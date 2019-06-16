package main

import (
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path"
	"runtime/debug"
	"runtime/pprof"
	"strconv"

	etcd "github.com/coreos/etcd/clientv3"
	units "github.com/docker/go-units"
	"github.com/pachyderm/pachyderm/src/client"
	adminclient "github.com/pachyderm/pachyderm/src/client/admin"
	authclient "github.com/pachyderm/pachyderm/src/client/auth"
	debugclient "github.com/pachyderm/pachyderm/src/client/debug"
	eprsclient "github.com/pachyderm/pachyderm/src/client/enterprise"
	healthclient "github.com/pachyderm/pachyderm/src/client/health"
	pfsclient "github.com/pachyderm/pachyderm/src/client/pfs"
	"github.com/pachyderm/pachyderm/src/client/pkg/discovery"
	"github.com/pachyderm/pachyderm/src/client/pkg/grpcutil"
	"github.com/pachyderm/pachyderm/src/client/pkg/shard"
	"github.com/pachyderm/pachyderm/src/client/pkg/tracing"
	ppsclient "github.com/pachyderm/pachyderm/src/client/pps"
	transactionclient "github.com/pachyderm/pachyderm/src/client/transaction"
	"github.com/pachyderm/pachyderm/src/client/version"
	"github.com/pachyderm/pachyderm/src/client/version/versionpb"
	adminserver "github.com/pachyderm/pachyderm/src/server/admin/server"
	authserver "github.com/pachyderm/pachyderm/src/server/auth/server"
	debugserver "github.com/pachyderm/pachyderm/src/server/debug/server"
	eprsserver "github.com/pachyderm/pachyderm/src/server/enterprise/server"
	"github.com/pachyderm/pachyderm/src/server/health"
	pach_http "github.com/pachyderm/pachyderm/src/server/http"
	"github.com/pachyderm/pachyderm/src/server/pfs/s3"
	pfs_server "github.com/pachyderm/pachyderm/src/server/pfs/server"
	cache_pb "github.com/pachyderm/pachyderm/src/server/pkg/cache/groupcachepb"
	cache_server "github.com/pachyderm/pachyderm/src/server/pkg/cache/server"
	"github.com/pachyderm/pachyderm/src/server/pkg/cmdutil"
	col "github.com/pachyderm/pachyderm/src/server/pkg/collection"
	"github.com/pachyderm/pachyderm/src/server/pkg/deploy/assets"
	"github.com/pachyderm/pachyderm/src/server/pkg/grpcserver"
	"github.com/pachyderm/pachyderm/src/server/pkg/hashtree"
	logutil "github.com/pachyderm/pachyderm/src/server/pkg/log"
	"github.com/pachyderm/pachyderm/src/server/pkg/metrics"
	"github.com/pachyderm/pachyderm/src/server/pkg/netutil"
	"github.com/pachyderm/pachyderm/src/server/pkg/serviceenv"
	txnenv "github.com/pachyderm/pachyderm/src/server/pkg/transactionenv"
	"github.com/pachyderm/pachyderm/src/server/pkg/uuid"
	pps_server "github.com/pachyderm/pachyderm/src/server/pps/server"
	"github.com/pachyderm/pachyderm/src/server/pps/server/githook"
	txnserver "github.com/pachyderm/pachyderm/src/server/transaction/server"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
	flag "github.com/spf13/pflag"
	"golang.org/x/net/context"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	v1 "k8s.io/api/core/v1"
)

const (
	defaultTreeCacheSize = 8
)

var mode string
var readiness bool

func init() {
	flag.StringVar(&mode, "mode", "full", "Pachd currently supports two modes: full and sidecar.  The former includes everything you need in a full pachd node.  The latter runs only PFS, the Auth service, and a stripped-down version of PPS.")
	flag.BoolVar(&readiness, "readiness", false, "Run readiness check.")
	flag.Parse()
}

func main() {
	log.SetFormatter(logutil.FormatterFunc(logutil.Pretty))

	switch {
	case readiness:
		cmdutil.Main(doReadinessCheck, &serviceenv.PachdFullConfiguration{})
	case mode == "full":
		cmdutil.Main(doFullMode, &serviceenv.PachdFullConfiguration{})
	case mode == "sidecar":
		cmdutil.Main(doSidecarMode, &serviceenv.PachdFullConfiguration{})
	default:
		fmt.Printf("unrecognized mode: %s\n", mode)
	}
}

func doReadinessCheck(config interface{}) error {
	env := serviceenv.InitPachOnlyEnv(serviceenv.NewConfiguration(config))
	return env.GetPachClient(context.Background()).Health()
}

func doSidecarMode(config interface{}) (retErr error) {
	defer func() {
		if retErr != nil {
			pprof.Lookup("goroutine").WriteTo(os.Stderr, 2)
		}
	}()
	tracing.InstallJaegerTracerFromEnv() // must run before InitWithKube
	env := serviceenv.InitWithKube(serviceenv.NewConfiguration(config))
	debug.SetGCPercent(50)
	go func() {
		log.Println(http.ListenAndServe(fmt.Sprintf(":%d", env.PProfPort), nil))
	}()
	switch env.LogLevel {
	case "debug":
		log.SetLevel(log.DebugLevel)
	case "info":
		log.SetLevel(log.InfoLevel)
	case "error":
		log.SetLevel(log.ErrorLevel)
	default:
		log.Errorf("Unrecognized log level %s, falling back to default of \"info\"", env.LogLevel)
		log.SetLevel(log.InfoLevel)
	}
	if env.EtcdPrefix == "" {
		env.EtcdPrefix = col.DefaultPrefix
	}
	clusterID, err := getClusterID(env.GetEtcdClient())
	if err != nil {
		return fmt.Errorf("getClusterID: %v", err)
	}
	var reporter *metrics.Reporter
	if env.Metrics {
		reporter = metrics.NewReporter(clusterID, env)
	}

	pfsCacheSize, err := strconv.Atoi(env.PFSCacheSize)
	if err != nil {
		return fmt.Errorf("Atoi: %v", err)
	}
	if pfsCacheSize == 0 {
		pfsCacheSize = defaultTreeCacheSize
	}
	treeCache, err := hashtree.NewCache(pfsCacheSize)
	if err != nil {
		return fmt.Errorf("lru.New: %v", err)
	}
	// The sidecar only needs to serve traffic on the peer port, as it only serves
	// traffic from the user container (the worker binary and occasionally user
	// pipelines)
	return grpcserver.Serve(
		grpcserver.ServerOptions{
			Port:       env.PeerPort,
			MaxMsgSize: grpcutil.MaxMsgSize,
			RegisterFunc: func(s *grpc.Server) error {
				txnEnv := &txnenv.TransactionEnv{}
				blockCacheBytes, err := units.RAMInBytes(env.BlockCacheBytes)
				if err != nil {
					return fmt.Errorf("units.RAMInBytes: %v", err)
				}
				blockAPIServer, err := pfs_server.NewBlockAPIServer(env.StorageRoot, blockCacheBytes, env.StorageBackend, net.JoinHostPort(env.EtcdHost, env.EtcdPort))
				if err != nil {
					return fmt.Errorf("pfs.NewBlockAPIServer: %v", err)
				}
				pfsclient.RegisterObjectAPIServer(s, blockAPIServer)

				memoryRequestBytes, err := units.RAMInBytes(env.MemoryRequest)
				if err != nil {
					return err
				}
				pfsAPIServer, err := pfs_server.NewAPIServer(
					env,
					txnEnv,
					path.Join(env.EtcdPrefix, env.PFSEtcdPrefix),
					treeCache,
					env.StorageRoot,
					memoryRequestBytes,
				)
				if err != nil {
					return fmt.Errorf("pfs.NewAPIServer: %v", err)
				}
				pfsclient.RegisterAPIServer(s, pfsAPIServer)

				ppsAPIServer, err := pps_server.NewSidecarAPIServer(
					env,
					path.Join(env.EtcdPrefix, env.PPSEtcdPrefix),
					env.IAMRole,
					reporter,
					env.PPSWorkerPort,
					env.PProfPort,
					env.HTTPPort,
					env.PeerPort,
				)
				if err != nil {
					return fmt.Errorf("pps.NewSidecarAPIServer: %v", err)
				}
				ppsclient.RegisterAPIServer(s, ppsAPIServer)

				authAPIServer, err := authserver.NewAuthServer(
					env,
					txnEnv,
					path.Join(env.EtcdPrefix, env.AuthEtcdPrefix),
					false,
				)
				if err != nil {
					return fmt.Errorf("NewAuthServer: %v", err)
				}
				authclient.RegisterAPIServer(s, authAPIServer)

				transactionAPIServer, err := txnserver.NewAPIServer(
					env,
					txnEnv,
					path.Join(env.EtcdPrefix, env.PFSEtcdPrefix),
				)
				if err != nil {
					return fmt.Errorf("transaction.NewAPIServer: %v", err)
				}
				transactionclient.RegisterAPIServer(s, transactionAPIServer)

				enterpriseAPIServer, err := eprsserver.NewEnterpriseServer(
					env, path.Join(env.EtcdPrefix, env.EnterpriseEtcdPrefix))
				if err != nil {
					return fmt.Errorf("NewEnterpriseServer: %v", err)
				}
				eprsclient.RegisterAPIServer(s, enterpriseAPIServer)

				healthclient.RegisterHealthServer(s, health.NewHealthServer())
				debugclient.RegisterDebugServer(s, debugserver.NewDebugServer(
					"", // no name for pachd servers
					env.GetEtcdClient(),
					path.Join(env.EtcdPrefix, env.PPSEtcdPrefix),
					env.PPSWorkerPort,
				))
				txnEnv.Initialize(env, transactionAPIServer, authAPIServer, pfsAPIServer)
				return nil
			},
		},
	)
}

func doFullMode(config interface{}) (retErr error) {
	defer func() {
		if retErr != nil {
			pprof.Lookup("goroutine").WriteTo(os.Stderr, 2)
		}
	}()
	tracing.InstallJaegerTracerFromEnv() // must run before InitWithKube
	env := serviceenv.InitWithKube(serviceenv.NewConfiguration(config))
	debug.SetGCPercent(50)
	go func() {
		log.Println(http.ListenAndServe(fmt.Sprintf(":%d", env.PProfPort), nil))
	}()
	switch env.LogLevel {
	case "debug":
		log.SetLevel(log.DebugLevel)
	case "info":
		log.SetLevel(log.InfoLevel)
	case "error":
		log.SetLevel(log.ErrorLevel)
	default:
		log.Errorf("Unrecognized log level %s, falling back to default of \"info\"", env.LogLevel)
		log.SetLevel(log.InfoLevel)
	}
	if env.EtcdPrefix == "" {
		env.EtcdPrefix = col.DefaultPrefix
	}
	clusterID, err := getClusterID(env.GetEtcdClient())
	if err != nil {
		return fmt.Errorf("getClusterID: %v", err)
	}
	var reporter *metrics.Reporter
	if env.Metrics {
		reporter = metrics.NewReporter(clusterID, env)
	}
	// (bryce) Do we have to use etcd client v2 here for sharder? Might want to re-visit this later.
	etcdAddress := fmt.Sprintf("http://%s", net.JoinHostPort(env.EtcdHost, env.EtcdPort))
	etcdClientV2 := getEtcdClient(etcdAddress)
	ip, err := netutil.ExternalIP()
	if err != nil {
		return fmt.Errorf("error getting pachd external ip: %v", err)
	}
	address := net.JoinHostPort(ip, fmt.Sprintf("%d", env.PeerPort))
	sharder := shard.NewSharder(
		etcdClientV2,
		env.NumShards,
		env.Namespace,
	)
	go func() {
		if err := sharder.AssignRoles(address, nil); err != nil {
			log.Printf("error from sharder.AssignRoles: %s", grpcutil.ScrubGRPC(err))
		}
	}()
	router := shard.NewRouter(
		sharder,
		grpcutil.NewDialer(
			grpc.WithInsecure(),
		),
		address,
	)
	pfsCacheSize, err := strconv.Atoi(env.PFSCacheSize)
	if err != nil {
		return fmt.Errorf("Atoi: %v", err)
	}
	if pfsCacheSize == 0 {
		pfsCacheSize = defaultTreeCacheSize
	}
	treeCache, err := hashtree.NewCache(pfsCacheSize)
	if err != nil {
		return fmt.Errorf("lru.New: %v", err)
	}
	kubeNamespace := getNamespace()
	publicHealthServer := health.NewHealthServer()
	peerHealthServer := health.NewHealthServer()

	// TODO(msteffen): We should not use an errorgroup here. Errorgroup waits
	// until *all* goroutines have run and then returns, but we want pachd to halt
	// if *any* of these serve functions returns an arror.
	var eg errgroup.Group
	eg.Go(func() error {
		httpServer, err := pach_http.NewHTTPServer(address)
		if err != nil {
			return err
		}
		err = http.ListenAndServe(fmt.Sprintf(":%v", env.HTTPPort), httpServer)
		if err != nil {
			log.Printf("error starting http server %v\n", err)
		}
		return fmt.Errorf("ListenAndServe: %v", err)
	})
	eg.Go(func() error {
		err := githook.RunGitHookServer(address, etcdAddress, path.Join(env.EtcdPrefix, env.PPSEtcdPrefix))
		if err != nil {
			log.Printf("error starting githook server %v\n", err)
		}
		return fmt.Errorf("RunGitHookServer: %v", err)
	})
	eg.Go(func() error {
		c, err := client.NewFromAddress(fmt.Sprintf("localhost:%d", env.Port))
		if err != nil {
			return fmt.Errorf("s3gateway gRPC client init: %v", err)
		}
		defer c.Close()
		server := s3.Server(c, env.S3GatewayPort)
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			return fmt.Errorf("s3gateway server: %v", err)
		}
		return nil
	})
	eg.Go(func() error {
		http.Handle("/metrics", promhttp.Handler())
		err := http.ListenAndServe(fmt.Sprintf(":%v", assets.PrometheusPort), nil)
		if err != nil {
			log.Printf("error starting prometheus server %v\n", err)
		}
		return fmt.Errorf("ListenAndServe: %v", err)
	})
	eg.Go(func() error {
		err := grpcserver.Serve(
			grpcserver.ServerOptions{
				Port:                 env.Port,
				MaxMsgSize:           grpcutil.MaxMsgSize,
				PublicPortTLSAllowed: true,
				RegisterFunc: func(s *grpc.Server) error {
					txnEnv := &txnenv.TransactionEnv{}
					memoryRequestBytes, err := units.RAMInBytes(env.MemoryRequest)
					if err != nil {
						return err
					}
					pfsAPIServer, err := pfs_server.NewAPIServer(env, txnEnv, path.Join(env.EtcdPrefix, env.PFSEtcdPrefix), treeCache, env.StorageRoot, memoryRequestBytes)
					if err != nil {
						return fmt.Errorf("pfs.NewAPIServer: %v", err)
					}
					pfsclient.RegisterAPIServer(s, pfsAPIServer)

					ppsAPIServer, err := pps_server.NewAPIServer(
						env,
						txnEnv,
						path.Join(env.EtcdPrefix, env.PPSEtcdPrefix),
						kubeNamespace,
						env.WorkerImage,
						env.WorkerSidecarImage,
						env.WorkerImagePullPolicy,
						env.StorageRoot,
						env.StorageBackend,
						env.StorageHostPath,
						env.IAMRole,
						env.ImagePullSecret,
						env.NoExposeDockerSocket,
						reporter,
						env.WorkerUsesRoot,
						env.PPSWorkerPort,
						env.Port,
						env.PProfPort,
						env.HTTPPort,
						env.PeerPort,
					)
					if err != nil {
						return fmt.Errorf("pps.NewAPIServer: %v", err)
					}
					ppsclient.RegisterAPIServer(s, ppsAPIServer)

					if env.ExposeObjectAPI {
						// Generally the object API should not be exposed publicly, but
						// TestGarbageCollection uses it and it may help with debugging
						blockAPIServer, err := pfs_server.NewBlockAPIServer(
							env.StorageRoot,
							0 /* = blockCacheBytes (disable cache) */, env.StorageBackend,
							etcdAddress)
						if err != nil {
							return fmt.Errorf("pfs.NewBlockAPIServer: %v", err)
						}
						pfsclient.RegisterObjectAPIServer(s, blockAPIServer)
					}

					authAPIServer, err := authserver.NewAuthServer(
						env, txnEnv, path.Join(env.EtcdPrefix, env.AuthEtcdPrefix), true)
					if err != nil {
						return fmt.Errorf("NewAuthServer: %v", err)
					}
					authclient.RegisterAPIServer(s, authAPIServer)

					transactionAPIServer, err := txnserver.NewAPIServer(
						env,
						txnEnv,
						path.Join(env.EtcdPrefix, env.PFSEtcdPrefix),
					)
					if err != nil {
						return fmt.Errorf("transaction.NewAPIServer: %v", err)
					}
					transactionclient.RegisterAPIServer(s, transactionAPIServer)

					enterpriseAPIServer, err := eprsserver.NewEnterpriseServer(
						env, path.Join(env.EtcdPrefix, env.EnterpriseEtcdPrefix))
					if err != nil {
						return fmt.Errorf("NewEnterpriseServer: %v", err)
					}
					eprsclient.RegisterAPIServer(s, enterpriseAPIServer)

					adminclient.RegisterAPIServer(s, adminserver.NewAPIServer(address, env.StorageRoot, &adminclient.ClusterInfo{ID: clusterID}))
					healthclient.RegisterHealthServer(s, publicHealthServer)
					versionpb.RegisterAPIServer(s, version.NewAPIServer(version.Version, version.APIServerOptions{}))
					debugclient.RegisterDebugServer(s, debugserver.NewDebugServer(
						"", // no name for pachd servers
						env.GetEtcdClient(),
						path.Join(env.EtcdPrefix, env.PPSEtcdPrefix),
						env.PPSWorkerPort,
					))
					txnEnv.Initialize(env, transactionAPIServer, authAPIServer, pfsAPIServer)
					return nil
				},
			},
		)
		if err != nil {
			log.Printf("error starting grpc server %v\n", err)
		}
		return err
	})
	// Unfortunately, calling Register___Server(x) twice on the same
	// struct x doesn't work--x will only serve requests from the first
	// grpc.Server it was registered with. So we create a second set of
	// APIServer structs here so we can serve the Pachyderm API on the
	// peer port
	eg.Go(func() error {
		err := grpcserver.Serve(
			grpcserver.ServerOptions{
				Port:       env.PeerPort,
				MaxMsgSize: grpcutil.MaxMsgSize,
				RegisterFunc: func(s *grpc.Server) error {
					txnEnv := &txnenv.TransactionEnv{}
					cacheServer := cache_server.NewCacheServer(router, env.NumShards)
					go func() {
						if err := sharder.RegisterFrontends(nil, address, []shard.Frontend{cacheServer}); err != nil {
							log.Printf("error from sharder.RegisterFrontend %s", grpcutil.ScrubGRPC(err))
						}
					}()
					go func() {
						if err := sharder.Register(nil, address, []shard.Server{cacheServer}); err != nil {
							log.Printf("error from sharder.Register %s", grpcutil.ScrubGRPC(err))
						}
					}()
					cache_pb.RegisterGroupCacheServer(s, cacheServer)

					blockCacheBytes, err := units.RAMInBytes(env.BlockCacheBytes)
					if err != nil {
						return fmt.Errorf("units.RAMInBytes: %v", err)
					}
					blockAPIServer, err := pfs_server.NewBlockAPIServer(
						env.StorageRoot, blockCacheBytes, env.StorageBackend, etcdAddress)
					if err != nil {
						return fmt.Errorf("pfs.NewBlockAPIServer: %v", err)
					}
					pfsclient.RegisterObjectAPIServer(s, blockAPIServer)

					memoryRequestBytes, err := units.RAMInBytes(env.MemoryRequest)
					if err != nil {
						return err
					}
					pfsAPIServer, err := pfs_server.NewAPIServer(
						env,
						txnEnv,
						path.Join(env.EtcdPrefix, env.PFSEtcdPrefix),
						treeCache,
						env.StorageRoot,
						memoryRequestBytes,
					)
					if err != nil {
						return fmt.Errorf("pfs.NewAPIServer: %v", err)
					}
					pfsclient.RegisterAPIServer(s, pfsAPIServer)

					ppsAPIServer, err := pps_server.NewAPIServer(
						env,
						txnEnv,
						path.Join(env.EtcdPrefix, env.PPSEtcdPrefix),
						kubeNamespace,
						env.WorkerImage,
						env.WorkerSidecarImage,
						env.WorkerImagePullPolicy,
						env.StorageRoot,
						env.StorageBackend,
						env.StorageHostPath,
						env.IAMRole,
						env.ImagePullSecret,
						env.NoExposeDockerSocket,
						reporter,
						env.WorkerUsesRoot,
						env.PPSWorkerPort,
						env.Port,
						env.PProfPort,
						env.HTTPPort,
						env.PeerPort,
					)
					if err != nil {
						return fmt.Errorf("pps.NewAPIServer: %v", err)
					}
					ppsclient.RegisterAPIServer(s, ppsAPIServer)

					authAPIServer, err := authserver.NewAuthServer(
						env,
						txnEnv,
						path.Join(env.EtcdPrefix, env.AuthEtcdPrefix),
						false,
					)
					if err != nil {
						return fmt.Errorf("NewAuthServer: %v", err)
					}
					authclient.RegisterAPIServer(s, authAPIServer)

					transactionAPIServer, err := txnserver.NewAPIServer(
						env,
						txnEnv,
						path.Join(env.EtcdPrefix, env.PFSEtcdPrefix),
					)
					if err != nil {
						return fmt.Errorf("transaction.NewAPIServer: %v", err)
					}
					transactionclient.RegisterAPIServer(s, transactionAPIServer)

					enterpriseAPIServer, err := eprsserver.NewEnterpriseServer(
						env, path.Join(env.EtcdPrefix, env.EnterpriseEtcdPrefix))
					if err != nil {
						return fmt.Errorf("NewEnterpriseServer: %v", err)
					}
					eprsclient.RegisterAPIServer(s, enterpriseAPIServer)

					healthclient.RegisterHealthServer(s, peerHealthServer)
					versionpb.RegisterAPIServer(s, version.NewAPIServer(version.Version, version.APIServerOptions{}))
					adminclient.RegisterAPIServer(s, adminserver.NewAPIServer(address, env.StorageRoot, &adminclient.ClusterInfo{ID: clusterID}))
					txnEnv.Initialize(env, transactionAPIServer, authAPIServer, pfsAPIServer)
					return nil
				},
			},
		)
		if err != nil {
			log.Printf("error starting grpc server %v\n", err)
		}
		return err
	})
	// TODO(msteffen): Is it really necessary to indicate that the peer service is
	// healthy? Presumably migrate() will call the peer service no matter what.
	publicHealthServer.Ready()
	peerHealthServer.Ready()
	return eg.Wait()
}

func getEtcdClient(etcdAddress string) discovery.Client {
	return discovery.NewEtcdClient(etcdAddress)
}

const clusterIDKey = "cluster-id"

func getClusterID(client *etcd.Client) (string, error) {
	resp, err := client.Get(context.Background(),
		clusterIDKey)

	// if it's a key not found error then we create the key
	if resp.Count == 0 {
		// This might error if it races with another pachd trying to set the
		// cluster id so we ignore the error.
		client.Put(context.Background(), clusterIDKey, uuid.NewWithoutDashes())
	} else if err != nil {
		return "", err
	} else {
		// We expect there to only be one value for this key
		id := string(resp.Kvs[0].Value)
		return id, nil
	}

	return getClusterID(client)
}

// getNamespace returns the kubernetes namespace that this pachd pod runs in
func getNamespace() string {
	namespace := os.Getenv("PACHD_POD_NAMESPACE")
	if namespace != "" {
		return namespace
	}
	return v1.NamespaceDefault
}

package local

import (
	gotls "crypto/tls"
	"fmt"
	"net"
	"net/http"
	"os"
	"path"
	"runtime/debug"
	"runtime/pprof"

	adminclient "github.com/pachyderm/pachyderm/v2/src/admin"
	authclient "github.com/pachyderm/pachyderm/v2/src/auth"
	"github.com/pachyderm/pachyderm/v2/src/client"
	debugclient "github.com/pachyderm/pachyderm/v2/src/debug"
	eprsclient "github.com/pachyderm/pachyderm/v2/src/enterprise"
	healthclient "github.com/pachyderm/pachyderm/v2/src/health"
	identityclient "github.com/pachyderm/pachyderm/v2/src/identity"
	"github.com/pachyderm/pachyderm/v2/src/internal/auth"
	"github.com/pachyderm/pachyderm/v2/src/internal/clusterstate"
	"github.com/pachyderm/pachyderm/v2/src/internal/cmdutil"
	col "github.com/pachyderm/pachyderm/v2/src/internal/collection"
	"github.com/pachyderm/pachyderm/v2/src/internal/deploy/assets"
	"github.com/pachyderm/pachyderm/v2/src/internal/errors"
	"github.com/pachyderm/pachyderm/v2/src/internal/grpcutil"
	"github.com/pachyderm/pachyderm/v2/src/internal/metrics"
	"github.com/pachyderm/pachyderm/v2/src/internal/migrations"
	"github.com/pachyderm/pachyderm/v2/src/internal/netutil"
	"github.com/pachyderm/pachyderm/v2/src/internal/serviceenv"
	"github.com/pachyderm/pachyderm/v2/src/internal/tls"
	"github.com/pachyderm/pachyderm/v2/src/internal/tracing"
	txnenv "github.com/pachyderm/pachyderm/v2/src/internal/transactionenv"
	licenseclient "github.com/pachyderm/pachyderm/v2/src/license"
	pfsclient "github.com/pachyderm/pachyderm/v2/src/pfs"
	ppsclient "github.com/pachyderm/pachyderm/v2/src/pps"
	adminserver "github.com/pachyderm/pachyderm/v2/src/server/admin/server"
	authserver "github.com/pachyderm/pachyderm/v2/src/server/auth/server"
	debugserver "github.com/pachyderm/pachyderm/v2/src/server/debug/server"
	eprsserver "github.com/pachyderm/pachyderm/v2/src/server/enterprise/server"
	"github.com/pachyderm/pachyderm/v2/src/server/health"
	identity_server "github.com/pachyderm/pachyderm/v2/src/server/identity/server"
	licenseserver "github.com/pachyderm/pachyderm/v2/src/server/license/server"
	"github.com/pachyderm/pachyderm/v2/src/server/pfs/s3"
	pfs_server "github.com/pachyderm/pachyderm/v2/src/server/pfs/server"
	pps_server "github.com/pachyderm/pachyderm/v2/src/server/pps/server"
	"github.com/pachyderm/pachyderm/v2/src/server/pps/server/githook"
	txnserver "github.com/pachyderm/pachyderm/v2/src/server/transaction/server"
	transactionclient "github.com/pachyderm/pachyderm/v2/src/transaction"
	"github.com/pachyderm/pachyderm/v2/src/version"
	"github.com/pachyderm/pachyderm/v2/src/version/versionpb"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// RunLocal runs an in-process Pachd instance that spawns pipeline and job
// workers via a Kube client connected to an external Kubernetes API Server
// (i.e., it does not run inside the Kubernetes cluster that it uses). This
// allows for very fast local development, as there's no need to build/push a
// new pachd image after every change.
func RunLocal() (retErr error) {
	config := &serviceenv.PachdFullConfiguration{}
	cmdutil.Populate(config)

	config.PostgresServiceSSL = "disable"

	f, err := os.OpenFile("/tmp/pach/pachd-log", os.O_WRONLY|os.O_CREATE, 0755)
	if err != nil {
		log.WithError(err).Error("unable to open log file")
	}
	defer f.Close()
	log.SetOutput(f)

	defer func() {
		if retErr != nil {
			log.Errorf("error: %v", retErr)
			pprof.Lookup("goroutine").WriteTo(os.Stderr, 2)
		}
	}()
	switch logLevel := os.Getenv("LOG_LEVEL"); logLevel {
	case "debug":
		log.SetLevel(log.DebugLevel)
	case "error":
		log.SetLevel(log.ErrorLevel)
	case "info", "":
		log.SetLevel(log.InfoLevel)
	default:
		log.Errorf("Unrecognized log level %s, falling back to default of \"info\"", logLevel)
		log.SetLevel(log.InfoLevel)
	}

	// must run InstallJaegerTracer before InitWithKube/pach client initialization
	if endpoint := tracing.InstallJaegerTracerFromEnv(); endpoint != "" {
		log.Printf("connecting to Jaeger at %q", endpoint)
	} else {
		log.Printf("no Jaeger collector found (JAEGER_COLLECTOR_SERVICE_HOST not set)")
	}
	env := serviceenv.InitWithKube(serviceenv.NewConfiguration(config))
	debug.SetGCPercent(env.Config().GCPercent)
	if env.Config().EtcdPrefix == "" {
		env.Config().EtcdPrefix = col.DefaultPrefix
	}

	// TODO: currently all pachds attempt to apply migrations, we should coordinate this
	if err := migrations.ApplyMigrations(context.Background(), env.GetDBClient(), migrations.Env{}, clusterstate.DesiredClusterState); err != nil {
		return err
	}
	if err := migrations.BlockUntil(context.Background(), env.GetDBClient(), clusterstate.DesiredClusterState); err != nil {
		return err
	}

	var reporter *metrics.Reporter
	if env.Config().Metrics {
		reporter = metrics.NewReporter(env)
	}
	etcdAddress := fmt.Sprintf("http://%s", net.JoinHostPort(env.Config().EtcdHost, env.Config().EtcdPort))
	ip, err := netutil.ExternalIP()
	if err != nil {
		return errors.Wrapf(err, "error getting pachd external ip")
	}
	address := net.JoinHostPort(ip, fmt.Sprintf("%d", env.Config().PeerPort))
	requireNoncriticalServers := !env.Config().RequireCriticalServersOnly

	// Setup External Pachd GRPC Server.
	authInterceptor := auth.NewInterceptor(env)
	externalServer, err := grpcutil.NewServer(
		context.Background(),
		true,
		grpc.ChainUnaryInterceptor(
			tracing.UnaryServerInterceptor(),
			authInterceptor.InterceptUnary,
		),
		grpc.ChainStreamInterceptor(
			tracing.StreamServerInterceptor(),
			authInterceptor.InterceptStream,
		),
	)

	if err != nil {
		return err
	}

	identityStorageProvider, err := identity_server.NewStorageProvider(env)
	if err != nil {
		return err
	}

	if err := logGRPCServerSetup("External Pachd", func() error {
		txnEnv := &txnenv.TransactionEnv{}
		var pfsAPIServer pfs_server.APIServer
		if err := logGRPCServerSetup("PFS API", func() error {
			pfsAPIServer, err = pfs_server.NewAPIServer(env, txnEnv, path.Join(env.Config().EtcdPrefix, env.Config().PFSEtcdPrefix))
			if err != nil {
				return err
			}
			pfsclient.RegisterAPIServer(externalServer.Server, pfsAPIServer)
			return nil
		}); err != nil {
			return err
		}
		var ppsAPIServer pps_server.APIServer
		if err := logGRPCServerSetup("PPS API", func() error {
			ppsAPIServer, err = pps_server.NewAPIServer(
				env,
				txnEnv,
				reporter,
			)
			if err != nil {
				return err
			}
			ppsclient.RegisterAPIServer(externalServer.Server, ppsAPIServer)
			return nil
		}); err != nil {
			return err
		}

		if err := logGRPCServerSetup("Identity API", func() error {
			idAPIServer := identity_server.NewIdentityServer(
				env,
				identityStorageProvider,
				true,
			)
			if err != nil {
				return err
			}
			identityclient.RegisterAPIServer(externalServer.Server, idAPIServer)
			return nil
		}); err != nil {
			return err
		}

		var authAPIServer authserver.APIServer
		if err := logGRPCServerSetup("Auth API", func() error {
			authAPIServer, err = authserver.NewAuthServer(
				env, txnEnv, path.Join(env.Config().EtcdPrefix, env.Config().AuthEtcdPrefix), true, requireNoncriticalServers, true)
			if err != nil {
				return err
			}
			authclient.RegisterAPIServer(externalServer.Server, authAPIServer)
			return nil
		}); err != nil {
			return err
		}
		var transactionAPIServer txnserver.APIServer
		if err := logGRPCServerSetup("Transaction API", func() error {
			transactionAPIServer, err = txnserver.NewAPIServer(
				env,
				txnEnv,
				path.Join(env.Config().EtcdPrefix, env.Config().PFSEtcdPrefix),
			)
			if err != nil {
				return err
			}
			transactionclient.RegisterAPIServer(externalServer.Server, transactionAPIServer)
			return nil
		}); err != nil {
			return err
		}
		if err := logGRPCServerSetup("Enterprise API", func() error {
			enterpriseAPIServer, err := eprsserver.NewEnterpriseServer(
				env, path.Join(env.Config().EtcdPrefix, env.Config().EnterpriseEtcdPrefix))
			if err != nil {
				return err
			}
			eprsclient.RegisterAPIServer(externalServer.Server, enterpriseAPIServer)
			return nil
		}); err != nil {
			return err
		}
		if err := logGRPCServerSetup("License API", func() error {
			licenseAPIServer, err := licenseserver.New(
				env, path.Join(env.Config().EtcdPrefix, env.Config().EnterpriseEtcdPrefix))
			if err != nil {
				return err
			}
			licenseclient.RegisterAPIServer(externalServer.Server, licenseAPIServer)
			return nil
		}); err != nil {
			return err
		}
		if err := logGRPCServerSetup("Admin API", func() error {
			adminclient.RegisterAPIServer(externalServer.Server, adminserver.NewAPIServer(env))
			return nil
		}); err != nil {
			return err
		}
		healthServer := health.NewHealthServer()
		if err := logGRPCServerSetup("Health", func() error {
			healthclient.RegisterHealthServer(externalServer.Server, healthServer)
			return nil
		}); err != nil {
			return err
		}
		if err := logGRPCServerSetup("Version API", func() error {
			versionpb.RegisterAPIServer(externalServer.Server, version.NewAPIServer(version.Version, version.APIServerOptions{}))
			return nil
		}); err != nil {
			return err
		}
		if err := logGRPCServerSetup("Debug", func() error {
			debugclient.RegisterDebugServer(externalServer.Server, debugserver.NewDebugServer(
				env,
				env.Config().PachdPodName,
				nil,
			))
			return nil
		}); err != nil {
			return err
		}
		txnEnv.Initialize(env, transactionAPIServer, authAPIServer, pfsAPIServer, ppsAPIServer)
		log.Printf("listening on %v", env.Config().Port)
		if _, err := externalServer.ListenTCP("", env.Config().Port); err != nil {
			return err
		}
		healthServer.Ready()
		return nil
	}); err != nil {
		return err
	}
	// Setup Internal Pachd GRPC Server.
	internalServer, err := grpcutil.NewServer(context.Background(), false, grpc.ChainUnaryInterceptor(tracing.UnaryServerInterceptor(), authInterceptor.InterceptUnary), grpc.StreamInterceptor(authInterceptor.InterceptStream))
	if err != nil {
		return err
	}
	if err := logGRPCServerSetup("Internal Pachd", func() error {
		txnEnv := &txnenv.TransactionEnv{}
		var pfsAPIServer pfs_server.APIServer
		if err := logGRPCServerSetup("PFS API", func() error {
			pfsAPIServer, err = pfs_server.NewAPIServer(
				env,
				txnEnv,
				path.Join(env.Config().EtcdPrefix, env.Config().PFSEtcdPrefix),
			)
			if err != nil {
				return err
			}
			pfsclient.RegisterAPIServer(internalServer.Server, pfsAPIServer)
			return nil
		}); err != nil {
			return err
		}
		var ppsAPIServer pps_server.APIServer
		if err := logGRPCServerSetup("PPS API", func() error {
			ppsAPIServer, err = pps_server.NewAPIServer(
				env,
				txnEnv,
				reporter,
			)
			if err != nil {
				return err
			}
			ppsclient.RegisterAPIServer(internalServer.Server, ppsAPIServer)
			return nil
		}); err != nil {
			return err
		}
		if err := logGRPCServerSetup("Identity API", func() error {
			idAPIServer := identity_server.NewIdentityServer(
				env,
				identityStorageProvider,
				false,
			)
			identityclient.RegisterAPIServer(internalServer.Server, idAPIServer)
			return nil
		}); err != nil {
			return err
		}
		var authAPIServer authserver.APIServer
		if err := logGRPCServerSetup("Auth API", func() error {
			authAPIServer, err = authserver.NewAuthServer(
				env,
				txnEnv,
				path.Join(env.Config().EtcdPrefix, env.Config().AuthEtcdPrefix),
				false,
				requireNoncriticalServers,
				true,
			)
			if err != nil {
				return err
			}
			authclient.RegisterAPIServer(internalServer.Server, authAPIServer)
			return nil
		}); err != nil {
			return err
		}
		var transactionAPIServer txnserver.APIServer
		if err := logGRPCServerSetup("Transaction API", func() error {
			transactionAPIServer, err = txnserver.NewAPIServer(
				env,
				txnEnv,
				path.Join(env.Config().EtcdPrefix, env.Config().PFSEtcdPrefix),
			)
			if err != nil {
				return err
			}
			transactionclient.RegisterAPIServer(internalServer.Server, transactionAPIServer)
			return nil
		}); err != nil {
			return err
		}
		if err := logGRPCServerSetup("Enterprise API", func() error {
			enterpriseAPIServer, err := eprsserver.NewEnterpriseServer(
				env, path.Join(env.Config().EtcdPrefix, env.Config().EnterpriseEtcdPrefix))
			if err != nil {
				return err
			}
			eprsclient.RegisterAPIServer(internalServer.Server, enterpriseAPIServer)
			return nil
		}); err != nil {
			return err
		}
		healthServer := health.NewHealthServer()
		if err := logGRPCServerSetup("Health", func() error {
			healthclient.RegisterHealthServer(internalServer.Server, healthServer)
			return nil
		}); err != nil {
			return err
		}
		if err := logGRPCServerSetup("Version API", func() error {
			versionpb.RegisterAPIServer(internalServer.Server, version.NewAPIServer(version.Version, version.APIServerOptions{}))
			return nil
		}); err != nil {
			return err
		}
		if err := logGRPCServerSetup("Admin API", func() error {
			adminclient.RegisterAPIServer(internalServer.Server, adminserver.NewAPIServer(env))
			return nil
		}); err != nil {
			return err
		}
		txnEnv.Initialize(env, transactionAPIServer, authAPIServer, pfsAPIServer, ppsAPIServer)
		if _, err := internalServer.ListenTCP("", env.Config().PeerPort); err != nil {
			return err
		}
		healthServer.Ready()
		return nil
	}); err != nil {
		return err
	}
	// Create the goroutines for the servers.
	// Any server error is considered critical and will cause Pachd to exit.
	// The first server that errors will have its error message logged.
	errChan := make(chan error, 1)
	go waitForError("External Pachd GRPC Server", errChan, true, func() error {
		return externalServer.Wait()
	})
	go waitForError("Internal Pachd GRPC Server", errChan, true, func() error {
		return internalServer.Wait()
	})
	// TODO: Make http server work with V2.
	//go waitForError("HTTP Server", errChan, requireNoncriticalServers, func() error {
	//	httpServer, err := pach_http.NewHTTPServer(address)
	//	if err != nil {
	//		return err
	//	}
	//	server := http.Server{
	//		Addr:    fmt.Sprintf(":%v", env.HTTPPort),
	//		Handler: httpServer,
	//	}

	//	certPath, keyPath, err := tls.GetCertPaths()
	//	if err != nil {
	//		log.Warnf("pfs-over-HTTP - TLS disabled: %v", err)
	//		return server.ListenAndServe()
	//	}

	//	cLoader := tls.NewCertLoader(certPath, keyPath, tls.CertCheckFrequency)
	//	err = cLoader.LoadAndStart()
	//	if err != nil {
	//		return errors.Wrapf(err, "couldn't load TLS cert for pfs-over-http: %v", err)
	//	}

	//	server.TLSConfig = &gotls.Config{GetCertificate: cLoader.GetCertificate}

	//	return server.ListenAndServeTLS(certPath, keyPath)
	//})
	go waitForError("Githook Server", errChan, requireNoncriticalServers, func() error {
		return githook.RunGitHookServer(address, etcdAddress, path.Join(env.Config().EtcdPrefix, env.Config().PPSEtcdPrefix))
	})
	go waitForError("S3 Server", errChan, requireNoncriticalServers, func() error {
		server, err := s3.Server(env.Config().S3GatewayPort, s3.NewMasterDriver(), func() (*client.APIClient, error) {
			return client.NewFromURI(fmt.Sprintf("localhost:%d", env.Config().PeerPort))
		})
		if err != nil {
			return err
		}
		certPath, keyPath, err := tls.GetCertPaths()
		if err != nil {
			log.Warnf("s3gateway TLS disabled: %v", err)
			return server.ListenAndServe()
		}
		cLoader := tls.NewCertLoader(certPath, keyPath, tls.CertCheckFrequency)
		// Read TLS cert and key
		err = cLoader.LoadAndStart()
		if err != nil {
			return errors.Wrapf(err, "couldn't load TLS cert for s3gateway: %v", err)
		}
		server.TLSConfig = &gotls.Config{GetCertificate: cLoader.GetCertificate}
		return server.ListenAndServeTLS(certPath, keyPath)
	})
	go waitForError("Prometheus Server", errChan, requireNoncriticalServers, func() error {
		http.Handle("/metrics", promhttp.Handler())
		return http.ListenAndServe(fmt.Sprintf(":%v", assets.PrometheusPort), nil)
	})
	return <-errChan
}

func logGRPCServerSetup(name string, f func() error) (retErr error) {
	log.Printf("started setting up %v GRPC Server", name)
	defer func() {
		if retErr != nil {
			retErr = errors.Wrapf(retErr, "error setting up %v GRPC Server", name)
		} else {
			log.Printf("finished setting up %v GRPC Server", name)
		}
	}()
	return f()
}

func waitForError(name string, errChan chan error, required bool, f func() error) {
	if err := f(); !errors.Is(err, http.ErrServerClosed) {
		if !required {
			log.Errorf("error setting up and/or running %v: %v", name, err)
		} else {
			errChan <- errors.Wrapf(err, "error setting up and/or running %v (use --require-critical-servers-only deploy flag to ignore errors from noncritical servers)", name)
		}
	}
}

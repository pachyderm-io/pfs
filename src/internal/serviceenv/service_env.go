package serviceenv

import (
	"fmt"
	"math"
	"net"
	"os"
	"time"

	"github.com/pachyderm/pachyderm/v2/src/client"
	"github.com/pachyderm/pachyderm/v2/src/internal/backoff"
	"github.com/pachyderm/pachyderm/v2/src/internal/dbutil"
	"github.com/pachyderm/pachyderm/v2/src/internal/errors"
	"github.com/pachyderm/pachyderm/v2/src/internal/uuid"

	etcd "github.com/coreos/etcd/clientv3"
	loki "github.com/grafana/loki/pkg/logcli/client"
	"github.com/jmoiron/sqlx"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"golang.org/x/sync/errgroup"
	kube "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const clusterIDKey = "cluster-id"

// ServiceEnv contains connections to other services in the cluster, as well as
// pachd's startup runtime configuration. These values (client connections and
// configuration) are initialized on ServiceEnv creation and do not change over
// the lifetime of the ServiceEnv. In particular, gRPC channels used by these
// clients are shared by all callers after they are initialized (per gRPC best
// practice).
//
// In pachd, there is only one instance of this struct, but tests may create
// multiple, to give each test its own isolated "cluster", served in a separate
// goroutine.
type ServiceEnv interface {

	// Config returns the (typically env-var-based) pachd runtime configuration
	// options. Like pachd's peer service clients, these are passed on startup and
	// do not change over the lifetime of the ServiceEnv (over the lifetime of pachd
	// in production, or over the lifetime of a test run in test).
	//
	// In pachd, pachd/main.go parses its environment variables to initialize
	// its Config, and in testing, many tests set these values manually
	Config() *Configuration

	// GetPachClient returns a pachyderm API client, that pachd can use to send
	// RPCs to peer pachd instances or to itself. Pachd uses these for
	// communication between its various APIs, e.g. PPS -> PFS -> FileSet, Auth,
	// etc. The clients returned by this method must share their underlying gRPC
	// channel, but each will each have its own context and auth token.
	GetPachClient(ctx context.Context) *client.APIClient

	// GetEtcdClient returns a client connected to a peer etcd instance, if any is
	// available on startup. These clients must share their underlying gRPC
	// channel, and all current implementations of GetEtcdClient return pointers
	// to a single client which is returned to (and therefore shared by) all
	// callers.
	GetEtcdClient() *etcd.Client

	// GetKubeClient returns a client connected to a peer Kubernetes API server,
	// if any is available on startup. All current implementations of
	// GetKubeClient return pointers to a single client which is returned to (and
	// therefore shared by) all callers.
	GetKubeClient() *kube.Clientset

	// GetLokiClient returns a client connected to a peer Loki instance, if any is
	// available on startup. All current implementations of GetLokiClient return
	// pointers to a single client which is returned to (and therefore shared by)
	// all callers.
	GetLokiClient() (*loki.Client, error)

	// GetDBClient returns a sqlx client connected to a SQL database (Postgres, in
	// pachd), if any is available on startup. All current implementations of
	// GetDBClient return pointers to a single client which is returned to (and
	// therefore shared by) all callers.
	GetDBClient() *sqlx.DB

	// ClusterID returns the Cluster ID associated with this ServiceEnv's
	// cluster. Like pachd's peer service clients, this is initialized on startup
	// (from etcd) and does not change over the lifetime of the ServiceEnv.
	ClusterID() string

	// Context returns the context associated with this ServiceEnv, which
	// tracks the lifetime of the ServiceEnv. The singleton ServiceEnv in pachd
	// is never cancelled, but the ServiceEnvs used by tests may be cancelled when
	// their corresponding test finishes.
	Context() context.Context

	// Logger returns a global logger associated with this ServiceEnv. Like
	// other ServiceEnv data, this does not change over the lifetime of the
	// ServiceEnv. Production pachd uses a stdout-based logger, but in tests this
	// may send logs to a test-specific file.
	Logger() *log.Logger

	// Close closes this ServiceEnv's client instances and frees the ServiceEnv's
	// resources. The singleton ServiceEnv in pachd is never closed, but the
	// ServiceEnvs used by tests may be closed when their corresponding test
	// finishes
	Close() error
}

// NonblockingServiceEnv is an implementation of ServiceEnv that initializes
// clients in the background, and blocks in the getters until they're ready.
type NonblockingServiceEnv struct {
	config *Configuration

	// pachAddress is the domain name or hostport where pachd can be reached
	pachAddress string
	// pachClient is the "template" client on which the other clients returned by
	// GetPachClient are based.
	//
	// Per gRPC best practice, ServiceEnv dials a single GRPC channel which is
	// shared by all clients returned from GetPachClient: each client is a
	// separate instance of the APIClient struct, which has its own context and
	// auth token, but points to shared protobuf-generated GRPC clients.
	pachClient *client.APIClient
	// pachEg coordinates the initialization of pachClient. Note that
	// NonblockingServiceEnv uses a separate error group for each client, rather
	// than one for all three clients, so that pachd can initialize a
	// NonblockingServiceEnv inside of its own initialization (if GetEtcdClient()
	// blocked on intialization of 'pachClient' and pachd/main.go couldn't start
	// the pachd server until GetEtcdClient() had returned, then pachd would be
	// unable to start)
	pachEg errgroup.Group

	// etcdAddress is the domain name or hostport where etcd can be reached
	etcdAddress string
	// etcdClient is an etcd client that's shared by all users of this environment
	etcdClient *etcd.Client
	// etcdEg coordinates the initialization of etcdClient (see pachdEg)
	etcdEg errgroup.Group

	// kubeClient is a kubernetes client that, if initialized, is shared by all
	// users of this environment
	kubeClient *kube.Clientset
	// kubeEg coordinates the initialization of kubeClient (see pachdEg)
	kubeEg errgroup.Group

	// lokiClient is a loki (log aggregator) client that is shared by all users
	// of this environment, it doesn't require an initialization funcion, so
	// there's no errgroup associated with it.
	lokiClient *loki.Client

	// clusterID is the unique ID for this pach cluster
	clusterID   string
	clusterIDEg errgroup.Group

	// dbClient is a database client.
	dbClient *sqlx.DB
	// dbEg coordinates the initialization of dbClient (see pachdEg)
	dbEg errgroup.Group

	// ctx is the background context for the environment that will be canceled
	// when the ServiceEnv is closed - this typically only happens for orderly
	// shutdown in tests
	ctx    context.Context
	cancel func()
}

// InitPachOnlyEnv initializes this service environment. This dials a GRPC
// connection to pachd only (in a background goroutine), and creates the
// template pachClient used by future calls to GetPachClient.
//
// This call returns immediately, but GetPachClient will block
// until the client is ready.
func InitPachOnlyEnv(config *Configuration) *NonblockingServiceEnv {
	ctx, cancel := context.WithCancel(context.Background())
	env := &NonblockingServiceEnv{config: config, ctx: ctx, cancel: cancel}
	env.pachAddress = net.JoinHostPort("127.0.0.1", fmt.Sprintf("%d", env.config.PeerPort))
	env.pachEg.Go(env.initPachClient)
	return env // env is not ready yet
}

// InitServiceEnv initializes this service environment. This dials a GRPC
// connection to pachd and etcd (in a background goroutine), and creates the
// template pachClient used by future calls to GetPachClient.
//
// This call returns immediately, but GetPachClient and GetEtcdClient block
// until their respective clients are ready.
func InitServiceEnv(config *Configuration) *NonblockingServiceEnv {
	env := InitPachOnlyEnv(config)
	env.etcdAddress = fmt.Sprintf("http://%s", net.JoinHostPort(env.config.EtcdHost, env.config.EtcdPort))
	env.etcdEg.Go(env.initEtcdClient)
	env.clusterIDEg.Go(env.initClusterID)
	env.dbEg.Go(env.initDBClient)
	if env.config.LokiHost != "" && env.config.LokiPort != "" {
		env.lokiClient = &loki.Client{
			Address: fmt.Sprintf("http://%s", net.JoinHostPort(env.config.LokiHost, env.config.LokiPort)),
		}
	}
	return env // env is not ready yet
}

// InitWithKube is like InitNonblockingServiceEnv, but also assumes that it's run inside
// a kubernetes cluster and tries to connect to the kubernetes API server.
func InitWithKube(config *Configuration) *NonblockingServiceEnv {
	env := InitServiceEnv(config)
	env.kubeEg.Go(env.initKubeClient)
	return env // env is not ready yet
}

// Config implements the corresponding ServiceEnv method for
// NonblockingServiceEnv
func (env *NonblockingServiceEnv) Config() *Configuration {
	return env.config
}

func (env *NonblockingServiceEnv) initClusterID() error {
	client := env.GetEtcdClient()
	for {
		resp, err := client.Get(context.Background(), clusterIDKey)

		// if it's a key not found error then we create the key
		if resp.Count == 0 {
			// This might error if it races with another pachd trying to set the
			// cluster id so we ignore the error.
			client.Put(context.Background(), clusterIDKey, uuid.NewWithoutDashes())
		} else if err != nil {
			return err
		} else {
			// We expect there to only be one value for this key
			env.clusterID = string(resp.Kvs[0].Value)
			return nil
		}
	}
}

func (env *NonblockingServiceEnv) initPachClient() error {
	// validate argument
	if env.pachAddress == "" {
		return errors.New("cannot initialize pach client with empty pach address")
	}
	// Initialize pach client
	return backoff.Retry(func() error {
		var err error
		env.pachClient, err = client.NewFromURI(env.pachAddress)
		if err != nil {
			return errors.Wrapf(err, "failed to initialize pach client")
		}
		return nil
	}, backoff.RetryEvery(time.Second).For(5*time.Minute))
}

func (env *NonblockingServiceEnv) initEtcdClient() error {
	// validate argument
	if env.etcdAddress == "" {
		return errors.New("cannot initialize pach client with empty pach address")
	}
	// Initialize etcd
	return backoff.Retry(func() error {
		var err error
		env.etcdClient, err = etcd.New(etcd.Config{
			Endpoints: []string{env.etcdAddress},
			// Use a long timeout with Etcd so that Pachyderm doesn't crash loop
			// while waiting for etcd to come up (makes startup net faster)
			DialTimeout:        3 * time.Minute,
			DialOptions:        client.DefaultDialOptions(), // SA1019 can't call grpc.Dial directly
			MaxCallSendMsgSize: math.MaxInt32,
			MaxCallRecvMsgSize: math.MaxInt32,
		})
		if err != nil {
			return errors.Wrapf(err, "failed to initialize etcd client")
		}
		return nil
	}, backoff.RetryEvery(time.Second).For(5*time.Minute))
}

func (env *NonblockingServiceEnv) initKubeClient() error {
	return backoff.Retry(func() error {
		// Get secure in-cluster config
		var kubeAddr string
		var ok bool
		cfg, err := rest.InClusterConfig()
		if err != nil {
			// InClusterConfig failed, fall back to insecure config
			log.Errorf("falling back to insecure kube client due to error from NewInCluster: %s", err)
			kubeAddr, ok = os.LookupEnv("KUBERNETES_PORT_443_TCP_ADDR")
			if !ok {
				return errors.Wrapf(err, "can't fall back to insecure kube client due to missing env var (failed to retrieve in-cluster config")
			}
			kubePort, ok := os.LookupEnv("KUBERNETES_PORT")
			if !ok {
				kubePort = ":443"
			}
			bearerToken, _ := os.LookupEnv("KUBERNETES_BEARER_TOKEN_FILE")
			cfg = &rest.Config{
				Host:            fmt.Sprintf("%s:%s", kubeAddr, kubePort),
				BearerTokenFile: bearerToken,
				TLSClientConfig: rest.TLSClientConfig{
					Insecure: true,
				},
			}
		}
		env.kubeClient, err = kube.NewForConfig(cfg)
		if err != nil {
			return errors.Wrapf(err, "could not initialize kube client")
		}
		return nil
	}, backoff.RetryEvery(time.Second).For(5*time.Minute))
}

func (env *NonblockingServiceEnv) initDBClient() error {
	return backoff.Retry(func() error {
		db, err := dbutil.NewDB(
			dbutil.WithHostPort(env.config.PostgresServiceHost, env.config.PostgresServicePort),
			dbutil.WithDBName(env.config.PostgresDBName),
		)
		if err != nil {
			return err
		}
		env.dbClient = db
		return nil
	}, backoff.RetryEvery(time.Second).For(5*time.Minute))
}

// GetPachClient returns a pachd client with the same authentication
// credentials and cancellation as 'ctx' (ensuring that auth credentials are
// propagated through downstream RPCs).
//
// Functions that receive RPCs should call this to convert their RPC context to
// a Pachyderm client, and internal Pachyderm calls should accept clients
// returned by this call.
//
// (Warning) Do not call this function during server setup unless it is in a goroutine.
// A Pachyderm client is not available until the server has been setup.
func (env *NonblockingServiceEnv) GetPachClient(ctx context.Context) *client.APIClient {
	if err := env.pachEg.Wait(); err != nil {
		panic(err) // If env can't connect, there's no sensible way to recover
	}
	return env.pachClient.WithCtx(ctx)
}

// GetEtcdClient returns the already connected etcd client without modification.
func (env *NonblockingServiceEnv) GetEtcdClient() *etcd.Client {
	if err := env.etcdEg.Wait(); err != nil {
		panic(err) // If env can't connect, there's no sensible way to recover
	}
	if env.etcdClient == nil {
		panic("service env never connected to etcd")
	}
	return env.etcdClient
}

// GetKubeClient returns the already connected Kubernetes API client without
// modification.
func (env *NonblockingServiceEnv) GetKubeClient() *kube.Clientset {
	if err := env.kubeEg.Wait(); err != nil {
		panic(err) // If env can't connect, there's no sensible way to recover
	}
	if env.kubeClient == nil {
		panic("service env never connected to kubernetes")
	}
	return env.kubeClient
}

// GetLokiClient returns the loki client, it doesn't require blocking on a
// connection because the client is just a dumb struct with no init function.
func (env *NonblockingServiceEnv) GetLokiClient() (*loki.Client, error) {
	if env.lokiClient == nil {
		return nil, errors.Errorf("loki not configured, is it running in the same namespace as pachd?")
	}
	return env.lokiClient, nil
}

// GetDBClient returns the already connected database client without modification.
func (env *NonblockingServiceEnv) GetDBClient() *sqlx.DB {
	if err := env.dbEg.Wait(); err != nil {
		panic(err) // If env can't connect, there's no sensible way to recover
	}
	if env.dbClient == nil {
		panic("service env never connected to the database")
	}
	return env.dbClient
}

// ClusterID implements the corresponding ServiceEnv method for
// NonblockingServiceEnv
func (env *NonblockingServiceEnv) ClusterID() string {
	if err := env.clusterIDEg.Wait(); err != nil {
		panic(err)
	}

	return env.clusterID
}

// Context implements the corresponding ServiceEnv method for
// NonblockingServiceEnv
func (env *NonblockingServiceEnv) Context() context.Context {
	return env.ctx
}

// Logger implements the corresponding ServiceEnv method for
// NonblockingServiceEnv
func (env *NonblockingServiceEnv) Logger() *log.Logger {
	return log.StandardLogger()
}

// Close implements the corresponding ServiceEnv method for
// NonblockingServiceEnv
func (env *NonblockingServiceEnv) Close() error {
	// Cancel anything using the ServiceEnv's context
	env.cancel()

	// Close all of the clients and return the first error.
	// Loki client and kube client do not have a Close method.
	eg := &errgroup.Group{}

	// There is a race condition here, although not too serious because this only
	// happens in tests and the errors should not propagate back to the clients -
	// ideally we would close the client connection first and wait for the server
	// RPCs to end before closing the underlying service connections (like
	// postgres and etcd), so we don't get spurious errors. Instead, some RPCs may
	// fail because of losing the database connection.
	eg.Go(env.GetPachClient(context.Background()).Close)
	eg.Go(env.GetEtcdClient().Close)
	eg.Go(env.GetDBClient().Close)
	return eg.Wait()
}

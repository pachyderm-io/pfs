package serviceenv

import (
	"fmt"
	"math"
	"net"
	"os"
	"time"

	"github.com/pachyderm/pachyderm/v2/src/client"
	"github.com/pachyderm/pachyderm/v2/src/internal/backoff"
	col "github.com/pachyderm/pachyderm/v2/src/internal/collection"
	"github.com/pachyderm/pachyderm/v2/src/internal/dbutil"
	"github.com/pachyderm/pachyderm/v2/src/internal/errors"
	"github.com/pachyderm/pachyderm/v2/src/internal/uuid"
	auth_server "github.com/pachyderm/pachyderm/v2/src/server/auth"
	pfs_server "github.com/pachyderm/pachyderm/v2/src/server/pfs"
	pps_server "github.com/pachyderm/pachyderm/v2/src/server/pps"

	etcd "github.com/coreos/etcd/clientv3"
	dex_storage "github.com/dexidp/dex/storage"
	loki "github.com/grafana/loki/pkg/logcli/client"
	"github.com/jmoiron/sqlx"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"golang.org/x/sync/errgroup"
	kube "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const clusterIDKey = "cluster-id"

// ServiceEnv contains connections to other services in the
// cluster. In pachd, there is only one instance of this struct, but tests may
// create more, if they want to create multiple pachyderm "clusters" served in
// separate goroutines.
type ServiceEnv interface {
	AuthServer() auth_server.APIServer
	PfsServer() pfs_server.APIServer
	PpsServer() pps_server.APIServer
	SetAuthServer(auth_server.APIServer)
	SetPfsServer(pfs_server.APIServer)
	SetPpsServer(pps_server.APIServer)

	Config() *Configuration
	GetPachClient(ctx context.Context) *client.APIClient
	GetEtcdClient() *etcd.Client
	GetKubeClient() *kube.Clientset
	GetLokiClient() (*loki.Client, error)
	GetDBClient() *sqlx.DB
	GetPostgresListener() *col.PostgresListener
	GetDexDB() dex_storage.Storage
	ClusterID() string
	Context() context.Context
	Logger() *log.Logger
	Close() error
}

// NonblockingServiceEnv is an implementation of ServiceEnv that initializes
// clients in the background, and blocks in the getters until they're ready.
type NonblockingServiceEnv struct {
	config *Configuration

	// pachAddress is the domain name or hostport where pachd can be reached
	pachAddress string
	// pachClient is the "template" client other clients returned by this library
	// are based on. It contains the original GRPC client connection and has no
	// ctx and therefore no auth credentials or cancellation
	pachClient *client.APIClient
	// pachEg coordinates the initialization of pachClient.  Note that NonblockingServiceEnv
	// uses a separate error group for each client, rather than one for all
	// three clients, so that pachd can initialize a NonblockingServiceEnv inside of its own
	// initialization (if GetEtcdClient() blocked on intialization of 'pachClient'
	// and pachd/main.go couldn't start the pachd server until GetEtcdClient() had
	// returned, then pachd would be unable to start)
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

	// clusterId is the unique ID for this pach cluster
	clusterId   string
	clusterIdEg errgroup.Group

	// dexDB is a dex_storage connected to postgres
	dexDB   dex_storage.Storage
	dexDBEg errgroup.Group

	// dbClient is a database client.
	dbClient *sqlx.DB
	// dbEg coordinates the initialization of dbClient (see pachdEg)
	dbEg errgroup.Group

	// listener is a special database client for listening for changes
	listener *col.PostgresListener
	// listenerEg coordinates the initialization of listener (see pachdEg)
	listenerEg errgroup.Group

	authServer auth_server.APIServer
	ppsServer  pps_server.APIServer
	pfsServer  pfs_server.APIServer

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
	env.clusterIdEg.Go(env.initClusterID)
	env.dbEg.Go(env.initDBClient)
	env.listenerEg.Go(env.initListener)
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
			env.clusterId = string(resp.Kvs[0].Value)
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
		pachClient, err := client.NewFromURI(env.pachAddress)
		if err != nil {
			return errors.Wrapf(err, "failed to initialize pach client")
		}
		env.pachClient = pachClient
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
			dbutil.WithUserPassword(env.config.PostgresUser, "elephantastic"),
			dbutil.WithHostPort(env.config.PostgresServiceHost, env.config.PostgresServicePort),
			dbutil.WithDBName(env.config.PostgresDB),
		)
		if err != nil {
			return err
		}
		env.dbClient = db
		return nil
	}, backoff.RetryEvery(time.Second).For(5*time.Minute))
}

func (env *NonblockingServiceEnv) initListener() error {
	dsn := dbutil.GetDSN(
		dbutil.WithHostPort(env.config.PostgresServiceHost, env.config.PostgresServicePort),
		dbutil.WithDBName(env.config.PostgresDB),
	)
	return backoff.Retry(func() error {
		// The PostgresListener is lazily initialized to avoid consuming too many
		// postgres resources by having idle client connections, so construction
		// can't fail
		env.listener = col.NewPostgresListener(dsn)
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

// GetPostgresListener returns the already constructed database client dedicated
// for listen operations without modification. Note that this listener lazily
// connects to the database on the first listen operation.
func (env *NonblockingServiceEnv) GetPostgresListener() *col.PostgresListener {
	if err := env.listenerEg.Wait(); err != nil {
		panic(err)
	}
	if env.listener == nil {
		panic("service env never constructed a postgres listener")
	}
	return env.listener
}

func (env *NonblockingServiceEnv) GetDexDB() dex_storage.Storage {
	if err := env.dexDBEg.Wait(); err != nil {
		panic(err) // If env can't connect, there's no sensible way to recover
	}
	if env.dexDB == nil {
		panic("service env never connected to the Dex database")
	}
	return env.dexDB
}

func (env *NonblockingServiceEnv) ClusterID() string {
	if err := env.clusterIdEg.Wait(); err != nil {
		panic(err)
	}

	return env.clusterId
}

func (env *NonblockingServiceEnv) Context() context.Context {
	return env.ctx
}

func (env *NonblockingServiceEnv) Logger() *log.Logger {
	return log.StandardLogger()
}

func (env *NonblockingServiceEnv) Close() error {
	// Cancel anything using the ServiceEnv's context
	env.cancel()

	// Close all of the clients and return the first error.
	// Loki client and kube client do not have a Close method.
	eg := &errgroup.Group{}
	eg.Go(env.GetPachClient(context.Background()).Close)
	eg.Go(env.GetEtcdClient().Close)
	eg.Go(env.GetDBClient().Close)
	eg.Go(env.GetPostgresListener().Close)
	return eg.Wait()
}

// AuthServer returns the registered Auth APIServer
func (env *NonblockingServiceEnv) AuthServer() auth_server.APIServer {
	return env.authServer
}

// SetAuthServer registers an Auth APIServer with this service env
func (env *NonblockingServiceEnv) SetAuthServer(s auth_server.APIServer) {
	env.authServer = s
}

// PpsServer returns the registered PPS APIServer
func (env *NonblockingServiceEnv) PpsServer() pps_server.APIServer {
	return env.ppsServer
}

// SetPpsServer registers a Pps APIServer with this service env
func (env *NonblockingServiceEnv) SetPpsServer(s pps_server.APIServer) {
	env.ppsServer = s
}

// PfsServer returns the registered PFS APIServer
func (env *NonblockingServiceEnv) PfsServer() pfs_server.APIServer {
	return env.pfsServer
}

// SetPfsServer registers a Pfs APIServer with this service env
func (env *NonblockingServiceEnv) SetPfsServer(s pfs_server.APIServer) {
	env.pfsServer = s
}

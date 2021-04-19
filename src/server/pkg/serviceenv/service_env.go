package serviceenv

import (
	"fmt"
	"math"
	"net"
	"os"
	"testing"
	"time"

	"github.com/pachyderm/pachyderm/src/client"
	"github.com/pachyderm/pachyderm/src/client/pkg/errors"
	"github.com/pachyderm/pachyderm/src/server/pkg/backoff"

	etcd "github.com/coreos/etcd/clientv3"
	loki "github.com/grafana/loki/pkg/logcli/client"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"golang.org/x/sync/errgroup"
	kube "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// ServiceEnv is a struct containing connections to other services in the
// cluster. In pachd, there is only one instance of this struct, but tests may
// create more, if they want to create multiple pachyderm "clusters" served in
// separate goroutines.
type ServiceEnv struct {
	*Configuration

	// pachAddress is the domain name or hostport where pachd can be reached
	pachAddress string
	// pachClient is the "template" client other clients returned by this library
	// are based on. It contains the original GRPC client connection and has no
	// ctx and therefore no auth credentials or cancellation
	pachClient *client.APIClient
	// pachEg coordinates the initialization of pachClient.  Note that ServiceEnv
	// uses a separate error group for each client, rather than one for all
	// three clients, so that pachd can initialize a ServiceEnv inside of its own
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
}

// InitPachOnlyEnv initializes this service environment. This dials a GRPC
// connection to pachd only (in a background goroutine), and creates the
// template pachClient used by future calls to GetPachClient.
//
// This call returns immediately, but GetPachClient will block
// until the client is ready.
func InitPachOnlyEnv(config *Configuration) *ServiceEnv {
	env := &ServiceEnv{Configuration: config}
	env.pachAddress = net.JoinHostPort("127.0.0.1", fmt.Sprintf("%d", env.PeerPort))
	env.pachEg.Go(env.initPachClient)
	return env // env is not ready yet
}

// InitServiceEnv initializes this service environment. This dials a GRPC
// connection to pachd and etcd (in a background goroutine), and creates the
// template pachClient used by future calls to GetPachClient.
//
// This call returns immediately, but GetPachClient and GetEtcdClient block
// until their respective clients are ready.
func InitServiceEnv(config *Configuration) *ServiceEnv {
	env := InitPachOnlyEnv(config)
	env.etcdAddress = fmt.Sprintf("http://%s", net.JoinHostPort(env.EtcdHost, env.EtcdPort))
	env.etcdEg.Go(env.initEtcdClient)
	if env.LokiHost != "" && env.LokiPort != "" {
		env.lokiClient = &loki.Client{
			Address: fmt.Sprintf("http://%s", net.JoinHostPort(env.LokiHost, env.LokiPort)),
		}
	}
	return env // env is not ready yet
}

// InitWithKube is like InitServiceEnv, but also assumes that it's run inside
// a kubernetes cluster and tries to connect to the kubernetes API server.
func InitWithKube(config *Configuration) *ServiceEnv {
	env := InitServiceEnv(config)
	env.kubeEg.Go(env.initKubeClient)
	return env // env is not ready yet
}

// InitPachOnlyTestEnv is like InitPachOnlyEnv, but initializes a pachd client
// for tests (using client.NewForTest())
func InitPachOnlyTestEnv(t *testing.T, config *Configuration) *ServiceEnv {
	env := &ServiceEnv{Configuration: config}

	var err error
	env.pachClient, err = client.NewForTest()
	if err != nil {
		t.Fatalf("could not intialize pach client for test: %v", err)
	}
	return env // env is not ready yet
}

func (env *ServiceEnv) initPachClient() error {
	// validate argument
	if env.pachAddress == "" {
		return errors.New("cannot initialize pach client with empty pach address")
	}
	// Initialize pach client
	return backoff.Retry(func() error {
		var err error
		env.pachClient, err = client.NewFromAddress(env.pachAddress)
		if err != nil {
			return errors.Wrapf(err, "failed to initialize pach client")
		}
		return nil
	}, backoff.RetryEvery(time.Second).For(5*time.Minute))
}

func (env *ServiceEnv) initEtcdClient() error {
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

func (env *ServiceEnv) initKubeClient() error {
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
			cfg = &rest.Config{
				Host: fmt.Sprintf("%s:443", kubeAddr),
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
func (env *ServiceEnv) GetPachClient(ctx context.Context) *client.APIClient {
	if err := env.pachEg.Wait(); err != nil {
		panic(err) // If env can't connect, there's no sensible way to recover
	}
	return env.pachClient.WithCtx(ctx)
}

// GetEtcdClient returns the already connected etcd client without modification.
func (env *ServiceEnv) GetEtcdClient() *etcd.Client {
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
func (env *ServiceEnv) GetKubeClient() *kube.Clientset {
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
func (env *ServiceEnv) GetLokiClient() (*loki.Client, error) {
	if env.lokiClient == nil {
		return nil, errors.Errorf("loki not configured, is it running in the same namespace as pachd?")
	}
	return env.lokiClient, nil
}

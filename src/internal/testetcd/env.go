package testetcd

import (
	"context"
	"fmt"
	"io/ioutil"
	"net"
	"net/url"
	"os"
	"path"

	etcd "github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/embed"
	etcdwal "github.com/coreos/etcd/wal"
	"github.com/coreos/pkg/capnslog"
	"github.com/jmoiron/sqlx"
	"golang.org/x/sync/errgroup"
	kube "k8s.io/client-go/kubernetes"

	loki "github.com/grafana/loki/pkg/logcli/client"
	"github.com/pachyderm/pachyderm/v2/src/client"
	"github.com/pachyderm/pachyderm/v2/src/internal/serviceenv"
)

// Env contains the basic setup for running end-to-end pachyderm tests entirely
// locally within the test process. It implements the serviceenv.ServiceEnv
// interface for initializing API servers with in-process etcd and pachd
// instances, and provides a temporary directory for storing data.
type Env struct {
	Context   context.Context
	Directory string

	config     serviceenv.Configuration
	etcd       *embed.Etcd
	etcdClient *etcd.Client
}

func (e *Env) Config() *serviceenv.Configuration {
	return &e.config
}

func (e *Env) GetPachClient(ctx context.Context) *client.APIClient {
	panic("testetcd/env does not support GetPachClient")
}

func (e *Env) GetEtcdClient() *etcd.Client {
	return e.etcdClient
}

func (e *Env) GetKubeClient() *kube.Clientset {
	panic("test env does not support GetKubeClient")
}

func (e *Env) GetLokiClient() (*loki.Client, error) {
	panic("test env does not support GetKubeClient")
}

func (e *Env) GetDBClient() *sqlx.DB {
	panic("test env does not support GetDBClient")
}

// WithEnv constructs a default Env for testing during the lifetime of
// the callback.
func WithEnv(cb func(*Env) error) (err error) {
	// Use an error group with a cancelable context to supervise every component
	// and cancel everything if one fails
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	eg, ctx := errgroup.WithContext(ctx)

	env := &Env{Context: ctx}

	// Cleanup any state when we return
	defer func() {
		// We return the first error that occurs during teardown, but still try to
		// close everything
		saveErr := func(e error) error {
			if e != nil && err == nil {
				err = e
			}
			return e
		}

		if env.etcdClient != nil {
			saveErr(env.etcdClient.Close())
		}

		if env.etcd != nil {
			env.etcd.Close()
		}

		saveErr(os.RemoveAll(env.Directory))
		cancel()
		saveErr(eg.Wait())
	}()

	dirBase := path.Join(os.TempDir(), "pachyderm_test")

	err = os.MkdirAll(dirBase, 0700)
	if err != nil {
		return err
	}

	env.Directory, err = ioutil.TempDir(dirBase, "")
	if err != nil {
		return err
	}

	// NOTE: this is changing a GLOBAL variable in etcd. This function should not
	// be run in the same process as production code where this may affect
	// performance (but there should be little risk of that as this is only for
	// test code).
	etcdwal.SegmentSizeBytes = 1 * 1000 * 1000 // 1 MB

	etcdConfig := embed.NewConfig()
	etcdConfig.LogOutput = "default"
	etcdConfig.MaxTxnOps = 10000

	// Create test dirs for etcd data
	etcdConfig.Dir = path.Join(env.Directory, "etcd_data")
	etcdConfig.WalDir = path.Join(env.Directory, "etcd_wal")

	// Speed up initial election, hopefully this has no other impact since there
	// is only one etcd instance
	etcdConfig.InitialElectionTickAdvance = false
	etcdConfig.TickMs = 10
	etcdConfig.ElectionMs = 50

	// We want to assign a random unused port to etcd, but etcd doesn't give us a
	// way to read it back out later. We can work around this by creating our own
	// listener on a random port, find out which port was used, close that
	// listener, and pass that port down for etcd to use. There is a small race
	// condition here where someone else can steal the port between these steps,
	// but it should be fairly minimal and depends on how the OS assigns
	// unallocated ports.
	listener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		return err
	}
	defer listener.Close()

	clientURL, err := url.Parse(fmt.Sprintf("http://%s", listener.Addr().String()))
	if err != nil {
		return err
	}

	etcdConfig.LPUrls = []url.URL{}
	etcdConfig.LCUrls = []url.URL{*clientURL}

	listener.Close()

	// Throw away noisy messages from etcd - comment these out if you need to debug
	// a failed start
	capnslog.SetGlobalLogLevel(capnslog.CRITICAL)

	env.etcd, err = embed.StartEtcd(etcdConfig)
	if err != nil {
		return err
	}

	capnslog.SetGlobalLogLevel(capnslog.CRITICAL)

	eg.Go(func() error {
		return errorWait(ctx, env.etcd.Err())
	})

	env.etcdClient, err = etcd.New(etcd.Config{
		Context:     env.Context,
		Endpoints:   []string{clientURL.String()},
		DialOptions: client.DefaultDialOptions(),
	})
	if err != nil {
		return err
	}

	// TODO: supervise the EtcdClient connection and error the errgroup if they
	// go down

	return cb(env)
}

func errorWait(ctx context.Context, errChan <-chan error) error {
	select {
	case <-ctx.Done():
		return nil
	case err := <-errChan:
		return err
	}
}

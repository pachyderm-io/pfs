package server

import (
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	etcd "github.com/coreos/etcd/clientv3"
	"github.com/pachyderm/pachyderm/v2/src/auth"
	"github.com/pachyderm/pachyderm/v2/src/client"
	"github.com/pachyderm/pachyderm/v2/src/internal/backoff"
	"github.com/pachyderm/pachyderm/v2/src/internal/dbutil"
	"github.com/pachyderm/pachyderm/v2/src/internal/errors"
	"github.com/pachyderm/pachyderm/v2/src/internal/grpcutil"
	"github.com/pachyderm/pachyderm/v2/src/internal/require"
	"github.com/pachyderm/pachyderm/v2/src/internal/serviceenv"
	txnenv "github.com/pachyderm/pachyderm/v2/src/internal/transactionenv"
	"github.com/pachyderm/pachyderm/v2/src/internal/uuid"
	"github.com/pachyderm/pachyderm/v2/src/pfs"
	authtesting "github.com/pachyderm/pachyderm/v2/src/server/auth/testing"
	"github.com/pachyderm/pachyderm/v2/src/version"
	"github.com/pachyderm/pachyderm/v2/src/version/versionpb"

	"golang.org/x/net/context"
)

const (
	etcdHost                   = "localhost"
	etcdPort                   = "32379"
	localBlockServerCacheBytes = 256 * 1024 * 1024
)

var (
	port          int32     = 30655 // Initial port on which pachd server processes will serve
	checkEtcdOnce sync.Once         // ensure we only test the etcd connection once
)

// generateRandomString is a helper function for getPachClient
func generateRandomString(n int) string {
	rand.Seed(time.Now().UnixNano())
	b := make([]byte, n)
	for i := range b {
		b[i] = byte('a' + rand.Intn(26))
	}
	return string(b)
}

// runServers starts serving requests for the given apiServer
// in a separate goroutine. Helper for getPachClient()
func runServers(t testing.TB, port int32, apiServer APIServer) {
	server, err := grpcutil.NewServer(context.Background(), false)
	require.NoError(t, err)

	pfs.RegisterAPIServer(server.Server, apiServer)
	auth.RegisterAPIServer(server.Server, &authtesting.InactiveAPIServer{}) // PFS server uses auth API
	versionpb.RegisterAPIServer(server.Server,
		version.NewAPIServer(version.Version, version.APIServerOptions{}))

	_, err = server.ListenTCP("", uint16(port))
	require.NoError(t, err)

	go func() {
		require.NoError(t, server.Wait())
	}()
}

// GetBasicConfig gets a basic service environment configuration for testing pachd.
func GetBasicConfig() *serviceenv.Configuration {
	config := serviceenv.NewConfiguration(&serviceenv.PachdFullConfiguration{})
	config.EtcdHost = etcdHost
	config.EtcdPort = etcdPort
	return config
}

// GetPachClient initializes a new PFSAPIServer and blockAPIServer and begins
// serving requests for them on a new port, and then returns a client connected
// to the new servers (allows PFS tests to run in parallel without conflict)
func GetPachClient(t testing.TB, config *serviceenv.Configuration) *client.APIClient {
	// src/server/pfs/server/driver.go expects an etcd server at "localhost:32379"
	// Try to establish a connection before proceeding with the test (which will
	// fail if the connection can't be established)
	checkEtcdOnce.Do(func() {
		require.NoError(t, backoff.Retry(func() error {
			_, err := etcd.New(etcd.Config{
				Endpoints:   []string{net.JoinHostPort(etcdHost, etcdPort)},
				DialOptions: client.DefaultDialOptions(),
			})
			if err != nil {
				return errors.Wrapf(err, "could not connect to etcd")
			}
			return nil
		}, backoff.NewTestingBackOff()))
	})

	root := "/tmp/pach_test/run" + uuid.NewWithoutDashes()[0:12]
	t.Logf("root %s", root)

	pfsPort := atomic.AddInt32(&port, 1)
	config.PeerPort = uint16(pfsPort)

	// initialize new BlockAPIServier
	env := serviceenv.InitServiceEnv(config)
	etcdPrefix := generateRandomString(32)

	txnEnv := &txnenv.TransactionEnv{}

	db := dbutil.NewTestDB(t)
	apiServer, err := newAPIServer(env, txnEnv, etcdPrefix, db)
	require.NoError(t, err)

	txnEnv.Initialize(env, nil, &authtesting.InactiveAPIServer{}, apiServer, txnenv.NewMockPpsTransactionServer())

	runServers(t, pfsPort, apiServer)
	return env.GetPachClient(context.Background())
}

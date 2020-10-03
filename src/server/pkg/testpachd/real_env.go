package testpachd

import (
	"net"
	"net/url"
	"os"
	"path"

	authserver "github.com/pachyderm/pachyderm/src/server/auth/server"
	authtesting "github.com/pachyderm/pachyderm/src/server/auth/testing"
	pfsserver "github.com/pachyderm/pachyderm/src/server/pfs/server"
	"github.com/pachyderm/pachyderm/src/server/pkg/hashtree"
	"github.com/pachyderm/pachyderm/src/server/pkg/serviceenv"
	txnenv "github.com/pachyderm/pachyderm/src/server/pkg/transactionenv"
	txnserver "github.com/pachyderm/pachyderm/src/server/transaction/server"
)

// RealEnv contains a setup for running end-to-end pachyderm tests locally.  It
// includes the base MockEnv struct as well as a real instance of the API server.
// These calls can still be mocked, but they default to calling into the real
// server endpoints.
type RealEnv struct {
	MockEnv

	ServiceEnv               *serviceenv.ServiceEnv
	AuthServer               authserver.APIServer
	PFSBlockServer           pfsserver.BlockAPIServer
	PFSServer                pfsserver.APIServer
	TransactionServer        txnserver.APIServer
	MockPPSTransactionServer *MockPPSTransactionServer
}

const (
	testingTreeCacheSize       = 8
	localBlockServerCacheBytes = 256 * 1024 * 1024
)

// WithRealEnv constructs a MockEnv, then forwards all API calls to go to API
// server instances for supported operations. PPS requires a kubernetes
// environment in order to spin up pipelines, which is not yet supported by this
// package, but the other API servers work.
func WithRealEnv(cb func(*RealEnv) error, customConfig ...*serviceenv.PachdFullConfiguration) error {
	// TODO: This means StorageV2 tests can not run in parallel with V1 test.
	if len(customConfig) > 0 && customConfig[0].StorageV2 {
		if err := os.Setenv("STORAGE_V2", "true"); err != nil {
			panic(err)
		}
		defer func() {
			if err := os.Unsetenv("STORAGE_V2"); err != nil {
				panic(err)
			}
		}()
	}

	return WithMockEnv(func(mockEnv *MockEnv) (err error) {
		realEnv := &RealEnv{MockEnv: *mockEnv}

		treeCache, err := hashtree.NewCache(testingTreeCacheSize)
		if err != nil {
			return err
		}
		defer treeCache.Close()

		config := serviceenv.NewConfiguration(&serviceenv.PachdFullConfiguration{})
		if len(customConfig) > 0 {
			config = serviceenv.NewConfiguration(customConfig[0])
		}

		etcdClientURL, err := url.Parse(realEnv.EtcdClient.Endpoints()[0])
		if err != nil {
			return err
		}
		config.EtcdHost = etcdClientURL.Hostname()
		config.EtcdPort = etcdClientURL.Port()
		config.PeerPort = uint16(realEnv.MockPachd.Addr.(*net.TCPAddr).Port)
		config.StorageRoot = path.Join(realEnv.Directory, "localStorage")
		realEnv.ServiceEnv = serviceenv.InitServiceEnv(config)

		realEnv.PFSBlockServer, err = pfsserver.NewBlockAPIServer(
			realEnv.ServiceEnv.StorageRoot,
			localBlockServerCacheBytes,
			pfsserver.LocalBackendEnvVar,
			net.JoinHostPort(config.EtcdHost, config.EtcdPort),
			true, // duplicate
		)
		if err != nil {
			return err
		}

		txnEnv := &txnenv.TransactionEnv{}

		realEnv.PFSServer, err = pfsserver.NewAPIServer(
			realEnv.ServiceEnv,
			txnEnv,
			realEnv.ServiceEnv.EtcdPrefix,
			treeCache,
			realEnv.ServiceEnv.StorageRoot,
			64*1024*1024,
		)
		if err != nil {
			return err
		}

		realEnv.AuthServer = &authtesting.InactiveAPIServer{}

		realEnv.TransactionServer, err = txnserver.NewAPIServer(realEnv.ServiceEnv, txnEnv, realEnv.ServiceEnv.EtcdPrefix)
		if err != nil {
			return err
		}

		realEnv.MockPPSTransactionServer = NewMockPPSTransactionServer()

		txnEnv.Initialize(realEnv.ServiceEnv, realEnv.TransactionServer, realEnv.AuthServer, realEnv.PFSServer, &realEnv.MockPPSTransactionServer.api)

		linkServers(&realEnv.MockPachd.Object, realEnv.PFSBlockServer)
		linkServers(&realEnv.MockPachd.PFS, realEnv.PFSServer)
		linkServers(&realEnv.MockPachd.Auth, realEnv.AuthServer)
		linkServers(&realEnv.MockPachd.Transaction, realEnv.TransactionServer)

		return cb(realEnv)
	})
}

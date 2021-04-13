package testpachd

import (
	"context"
	"net"
	"net/url"
	"path"

	"github.com/pachyderm/pachyderm/v2/src/internal/clusterstate"
	"github.com/pachyderm/pachyderm/v2/src/internal/migrations"
	"github.com/pachyderm/pachyderm/v2/src/internal/serviceenv"

	units "github.com/docker/go-units"
	"github.com/jmoiron/sqlx"
	txnenv "github.com/pachyderm/pachyderm/v2/src/internal/transactionenv"
	authserver "github.com/pachyderm/pachyderm/v2/src/server/auth/server"
	authtesting "github.com/pachyderm/pachyderm/v2/src/server/auth/testing"
	pfsserver "github.com/pachyderm/pachyderm/v2/src/server/pfs/server"
	txnserver "github.com/pachyderm/pachyderm/v2/src/server/transaction/server"
)

// RealEnv contains a setup for running end-to-end pachyderm tests locally.  It
// includes the base MockEnv struct as well as a real instance of the API server.
// These calls can still be mocked, but they default to calling into the real
// server endpoints.
type RealEnv struct {
	MockEnv

	LocalStorageDirectory    string
	AuthServer               authserver.APIServer
	PFSServer                pfsserver.APIServer
	TransactionServer        txnserver.APIServer
	MockPPSTransactionServer *MockPPSTransactionServer
}

// WithRealEnv constructs a MockEnv, then forwards all API calls to go to API
// server instances for supported operations. PPS requires a kubernetes
// environment in order to spin up pipelines, which is not yet supported by this
// package, but the other API servers work.
func WithRealEnv(db *sqlx.DB, cb func(*RealEnv) error, customConfig ...*serviceenv.PachdFullConfiguration) error {
	return WithMockEnv(func(mockEnv *MockEnv) (err error) {
		realEnv := &RealEnv{MockEnv: *mockEnv}
		if len(customConfig) > 0 {
			*realEnv.Config() = *serviceenv.NewConfiguration(customConfig[0])
		}

		etcdClientURL, err := url.Parse(realEnv.GetEtcdClient().Endpoints()[0])
		if err != nil {
			return err
		}
		realEnv.Config().EtcdHost = etcdClientURL.Hostname()
		realEnv.Config().EtcdPort = etcdClientURL.Port()
		realEnv.Config().PeerPort = uint16(realEnv.mockPachd.Addr.(*net.TCPAddr).Port)

		if err := migrations.ApplyMigrations(context.Background(), db, migrations.Env{}, clusterstate.DesiredClusterState); err != nil {
			return err
		}
		if err := migrations.BlockUntil(context.Background(), db, clusterstate.DesiredClusterState); err != nil {
			return err
		}

		realEnv.LocalStorageDirectory = path.Join(realEnv.Directory, "localStorage")
		realEnv.Config().StorageRoot = realEnv.LocalStorageDirectory

		etcdPrefix := ""

		txnEnv := &txnenv.TransactionEnv{}

		realEnv.PFSServer, err = pfsserver.NewAPIServer(
			realEnv,
			txnEnv,
			etcdPrefix,
			db,
		)
		if err != nil {
			return err
		}

		realEnv.AuthServer = &authtesting.InactiveAPIServer{}

		realEnv.TransactionServer, err = txnserver.NewAPIServer(realEnv, txnEnv, etcdPrefix)
		if err != nil {
			return err
		}

		realEnv.MockPPSTransactionServer = NewMockPPSTransactionServer()

		txnEnv.Initialize(realEnv, realEnv.TransactionServer, realEnv.AuthServer, realEnv.PFSServer, &realEnv.MockPPSTransactionServer.api)

		linkServers(&realEnv.mockPachd.PFS, realEnv.PFSServer)
		linkServers(&realEnv.mockPachd.Auth, realEnv.AuthServer)
		linkServers(&realEnv.mockPachd.Transaction, realEnv.TransactionServer)

		return cb(realEnv)
	})
}

// NewDefaultConfig creates a new default pachd configuration.
func NewDefaultConfig() *serviceenv.PachdFullConfiguration {
	config := &serviceenv.PachdFullConfiguration{}
	config.StorageMemoryThreshold = units.GB
	config.StorageShardThreshold = units.GB
	config.StorageLevelFactor = 10
	config.StorageGCPolling = "30s"
	config.StorageCompactionMaxFanIn = 50
	return config
}

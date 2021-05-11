package multipach

import (
	"encoding/base64"
	"fmt"
	"math"
	"net"
	"os"
	"path"
	"runtime/debug"
	"strings"
	"testing"
	"time"

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
	"github.com/pachyderm/pachyderm/v2/src/internal/errors"
	"github.com/pachyderm/pachyderm/v2/src/internal/grpcutil"
	"github.com/pachyderm/pachyderm/v2/src/internal/metrics"
	"github.com/pachyderm/pachyderm/v2/src/internal/migrations"
	"github.com/pachyderm/pachyderm/v2/src/internal/serviceenv"
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
	pfs_server "github.com/pachyderm/pachyderm/v2/src/server/pfs/server"
	pps_server "github.com/pachyderm/pachyderm/v2/src/server/pps/server"
	txnserver "github.com/pachyderm/pachyderm/v2/src/server/transaction/server"
	transactionclient "github.com/pachyderm/pachyderm/v2/src/transaction"
	"github.com/pachyderm/pachyderm/v2/src/version"
	"github.com/pachyderm/pachyderm/v2/src/version/versionpb"

	etcd "github.com/coreos/etcd/clientv3"
	dex_sql "github.com/dexidp/dex/storage/sql"
	"github.com/jmoiron/sqlx"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	kube "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/pachyderm/pachyderm/v2/src/internal/dbutil"
	"github.com/pachyderm/pachyderm/v2/src/internal/random"
	"github.com/pachyderm/pachyderm/v2/src/internal/require"
	"github.com/pachyderm/pachyderm/v2/src/internal/testutil"
)

// withDB creates a database connection that is scoped to the passed in callback.
func withDB(cb func(*sqlx.DB) error, opts ...dbutil.Option) (retErr error) {
	db, err := dbutil.NewDB(opts...)
	if err != nil {
		return err
	}
	defer func() {
		if err := db.Close(); retErr == nil {
			retErr = err
		}
	}()
	return cb(db)
}

// TestContext is provides an interface for integration tests to interact with the pach API
type TestContext interface {
	GetUnauthenticatedPachClient(tb testing.TB) *client.APIClient
	GetAuthenticatedPachClient(tb testing.TB, subject string) *client.APIClient
}

// InMemoryTestContext is a TestContext that points to an isolated in-memory pachd
type InMemoryTestContext struct {
	env *serviceenv.TestServiceEnv
}

// GetUnauthenticatedPachClient returns a pach client with no auth info
func (*RemoteTestContext) GetUnauthenticatedPachClient(tb testing.TB) *client.APIClient {
	return testutil.GetUnauthenticatedPachClient(tb)
}

// GetAuthenticatedPachClient returns a pach client which is authenticated as `subject`
func (*RemoteTestContext) GetAuthenticatedPachClient(tb testing.TB, subject string) *client.APIClient {
	return testutil.GetAuthenticatedPachClient(tb, subject)
}

// RemoteTestContext is a TestContext that points to a real pachd running in a k8s cluster
type RemoteTestContext struct{}

// GetTestContext should be called at the beginning of an integration test to get a
// TestContext. It will either spawn a dedicated in-memory pachd or return a reference
// to an external pachd depending on the environement.
func GetTestContext(t testing.TB, requiresRealDeployment bool) TestContext {
	t.Helper()

	if os.Getenv("PACH_INMEMORY") == "" {
		testutil.DeleteAll(t)
		t.Cleanup(func() { testutil.DeleteAll(t) })
		return &RemoteTestContext{}
	}

	if requiresRealDeployment {
		t.Skip("Skipping for in-memory tests")
	}

	testId := strings.ToLower(base64.RawURLEncoding.EncodeToString([]byte(random.String(20))))
	sharedVolume := os.Getenv("SHARED_DATA_DIR")
	dataDir := path.Join(sharedVolume, testId)
	fmt.Printf("Test context %s - %s\n", testId, dataDir)

	if ct, ok := t.(*testing.T); ok {
		ct.Parallel()
	}

	clientSocketPath := path.Join(dataDir, "pachd_socket_"+testId)
	os.Mkdir(dataDir, os.ModePerm)

	fullConfig := &serviceenv.PachdFullConfiguration{}
	require.NoError(t, cmdutil.Populate(fullConfig))
	config := serviceenv.NewConfiguration(fullConfig)

	options := []dbutil.Option{
		dbutil.WithHostPort(config.PostgresServiceHost, config.PostgresServicePort),
	}
	require.NoError(t, withDB(func(db *sqlx.DB) error {
		db.MustExec("CREATE DATABASE " + testId)
		db.MustExec("CREATE DATABASE " + testId + "_dex")
		t.Log("database", testId, "successfully created")
		return nil
	}, options...))
	/*
		t.Cleanup(func() {
			require.NoError(t, withDB(func(db *sqlx.DB) error {
				db.MustExec("DROP DATABASE " + testId)
				db.MustExec("DROP DATABASE " + testId + "_dex")
				t.Log("database", testId, "successfully deleted")
				return nil
			}))
		})
	*/

	db, err := dbutil.NewDB(append(options, dbutil.WithDBName(testId))...)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	config.StorageRoot = path.Join(dataDir, "pach_root")
	config.StorageHostPath = path.Join(dataDir, "pach_root")
	config.CacheRoot = path.Join(dataDir, "cache_root")
	config.EtcdPrefix = testId
	config.PostgresDBName = testId
	config.PipelineLabel = testId

	cfg := &rest.Config{
		Host:            os.Getenv("KUBERNETES_PORT_443_TCP_ADDR") + ":8443",
		BearerTokenFile: os.Getenv("KUBERNETES_BEARER_TOKEN_FILE"),
		TLSClientConfig: rest.TLSClientConfig{
			Insecure: true,
		},
	}

	etcdClient, err := etcd.New(etcd.Config{
		Endpoints: []string{net.JoinHostPort(config.EtcdHost, config.EtcdPort)},
		// Use a long timeout with Etcd so that Pachyderm doesn't crash loop
		// while waiting for etcd to come up (makes startup net faster)
		DialTimeout:        3 * time.Minute,
		DialOptions:        client.DefaultDialOptions(), // SA1019 can't call grpc.Dial directly
		MaxCallSendMsgSize: math.MaxInt32,
		MaxCallRecvMsgSize: math.MaxInt32,
	})
	require.NoError(t, err)

	kubeClient, err := kube.NewForConfig(cfg)
	require.NoError(t, err)

	logger := log.New()
	f, err := os.OpenFile(path.Join(dataDir, "pachd.log"), os.O_WRONLY|os.O_CREATE, 0755)
	require.NoError(t, err)
	logger.SetOutput(f)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	dexDB, err := (&dex_sql.Postgres{
		NetworkDB: dex_sql.NetworkDB{
			Database: testId + "_dex",
			User:     dbutil.DefaultUser,
			Password: dbutil.DefaultUser,
			Host:     config.PostgresServiceHost,
			Port:     uint16(config.PostgresServicePort),
		},
		SSL: dex_sql.SSL{
			Mode: "disable",
		},
	}).Open(log.NewEntry(log.New()).WithField("source", "identity-db"))
	require.NoError(t, err)
	t.Cleanup(func() { dexDB.Close() })

	senv := &serviceenv.TestServiceEnv{
		Configuration: config,
		EtcdClient:    etcdClient,
		DBClient:      db,
		Log:           logger,
		KubeClient:    kubeClient,
		Ctx:           ctx,
		DexDB:         dexDB,
		Ready:         make(chan interface{}),
	}
	require.NoError(t, setupServer(senv, clientSocketPath))

	senv.PachClient, err = client.NewFromURI("unix://" + clientSocketPath)
	require.NoError(t, err)
	close(senv.Ready)

	testCtx := &InMemoryTestContext{env: senv}
	t.Cleanup(func() { testCtx.GetAuthenticatedPachClient(t, authclient.RootUser).DeleteAll() })
	return testCtx
}

// GetUnauthenticatedPachClient returns a copy of the testing pach client authenticated as `subject`
func (c *InMemoryTestContext) GetAuthenticatedPachClient(tb testing.TB, subject string) *client.APIClient {
	tb.Helper()
	rootClient := c.GetUnauthenticatedPachClient(tb)
	testutil.ActivateAuthWithClient(tb, rootClient)
	rootClient.SetAuthToken(testutil.RootToken)
	if subject == authclient.RootUser {
		return rootClient
	}
	token, err := rootClient.GetRobotToken(rootClient.Ctx(), &authclient.GetRobotTokenRequest{Robot: subject})
	require.NoError(tb, err)
	client := c.GetUnauthenticatedPachClient(tb)
	client.SetAuthToken(token.Token)
	return client
}

// GetUnauthenticatedPachClient returns a copy of the testing pach client with no auth token
func (c *InMemoryTestContext) GetUnauthenticatedPachClient(tb testing.TB) *client.APIClient {
	tb.Helper()
	return c.env.PachClient.WithCtx(context.Background())
}

func setupServer(env serviceenv.ServiceEnv, socketPath string) error {
	debug.SetGCPercent(env.Config().GCPercent)

	var reporter *metrics.Reporter

	if err := migrations.ApplyMigrations(context.Background(), env.GetDBClient(), migrations.Env{}, clusterstate.DesiredClusterState); err != nil {
		return err
	}

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
			idAPIServer := identity_server.NewIdentityServer(env, false)
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
				env, txnEnv, path.Join(env.Config().EtcdPrefix, env.Config().AuthEtcdPrefix), false, false, true)
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
		if err := externalServer.ListenSocket(socketPath); err != nil {
			return err
		}
		healthServer.Ready()
		return nil
	}); err != nil {
		return err
	}
	return nil
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

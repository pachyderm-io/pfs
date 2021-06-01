package testutil

import (
	"crypto/rand"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/jmoiron/sqlx"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/pachyderm/pachyderm/v2/src/internal/backoff"
	col "github.com/pachyderm/pachyderm/v2/src/internal/collection"
	"github.com/pachyderm/pachyderm/v2/src/internal/dbutil"
	"github.com/pachyderm/pachyderm/v2/src/internal/deploy/assets"
	"github.com/pachyderm/pachyderm/v2/src/internal/require"
	"github.com/pachyderm/pachyderm/v2/src/internal/serde"
	"github.com/pachyderm/pachyderm/v2/src/internal/serviceenv"
)

const (
	PostgresDefaultHost = "127.0.0.1"
	PostgresDefaultPort = 5432
	PostgresDefaultUser = "postgres"
)

// set this to false if you want to keep the database around
var cleanup = true

const postgresMaxConnections = 100

// we want to divide the total number of connections we can have up among the
// concurrently running tests
var maxOpenConnsPerPool = postgresMaxConnections / runtime.GOMAXPROCS(0)

// TestDatabaseDeployment represents a deployment of postgres, and databases may
// be created for individual tests.
type TestDatabaseDeployment interface {
	NewDatabase(t testing.TB) (*sqlx.DB, *col.PostgresListener)
	NewDatabaseConfig(t testing.TB) serviceenv.ConfigOption
}

type postgresDeployment struct {
	db      *sqlx.DB
	address string
	port    int
}

// NewPostgresDeployment creates a kubernetes namespace containing a postgres
// instance. The namespace will be destroyed at the end of the test or test
// suite that called this.
func NewPostgresDeployment(t testing.TB) TestDatabaseDeployment {
	manifest := &strings.Builder{}
	encoder, err := serde.GetEncoder("json", manifest, serde.WithIndent(2), serde.WithOrigName(true))
	require.NoError(t, err)

	namespaceName := UniqueString("postgres-test-")
	assetOpts := &assets.AssetOpts{
		PostgresOpts: assets.PostgresOpts{
			Nodes:      1,
			MemRequest: "500M",
			CPURequest: "0.1",
		},
		Namespace: namespaceName,
	}

	configMap := assets.PostgresInitConfigMap(assetOpts)
	require.NoError(t, encoder.Encode(configMap))

	storageClass := assets.PostgresStorageClass(assetOpts, assets.LocalBackend)
	require.NoError(t, encoder.Encode(storageClass))

	headlessService := assets.PostgresHeadlessService(assetOpts)
	require.NoError(t, encoder.Encode(headlessService))

	statefulSet := assets.PostgresStatefulSet(assetOpts, assets.LocalBackend, 1)
	require.NoError(t, encoder.Encode(statefulSet))

	service := assets.PostgresService(assetOpts)
	service.Spec.Ports[0].NodePort = 0
	require.NoError(t, encoder.Encode(service))

	// Connect to kubernetes and create the postgres items in their namespace
	kubeClient := GetKubeClient(t)
	namespace := &v1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespaceName}}
	_, err = kubeClient.CoreV1().Namespaces().Create(namespace)
	require.NoError(t, err)

	if cleanup {
		t.Cleanup(func() {
			err := kubeClient.CoreV1().Namespaces().Delete(namespaceName, nil)
			require.NoError(t, err)
		})
	}

	cmd := Cmd("kubectl", "apply", "--namespace", namespaceName, "-f", "-")
	cmd.Stdin = strings.NewReader(manifest.String())
	err = cmd.Run()
	require.NoError(t, err)

	// Since persistent volumes are not tied to namespaces, use a label to delete
	// the volumes we created on shutdown.
	if cleanup {
		t.Cleanup(func() {
			selector := fmt.Sprintf("namespace=%s", namespaceName)
			err := kubeClient.CoreV1().PersistentVolumes().DeleteCollection(nil, metav1.ListOptions{LabelSelector: selector})
			require.NoError(t, err)
		})
	}

	// Read out the service details from kubernetes
	postgres, err := kubeClient.CoreV1().Services(namespaceName).Get("postgres", metav1.GetOptions{})
	require.NoError(t, err)

	var port int
	for _, servicePort := range postgres.Spec.Ports {
		if servicePort.Name == "client-port" {
			port = int(servicePort.NodePort)
		}
	}
	require.NotEqual(t, 0, port)

	// Get the IP address of the nodes (any _should_ work for the service port)
	nodes, err := kubeClient.CoreV1().Nodes().List(metav1.ListOptions{})
	require.NoError(t, err)

	// Minikube 'Hostname' address type didn't work when testing, use InternalIP
	var address string
	for _, addr := range nodes.Items[0].Status.Addresses {
		if addr.Type == "InternalIP" {
			address = addr.Address
		}
	}
	require.NotEqual(t, "", address)

	db, err := dbutil.NewDB(dbutil.WithHostPort(address, port))
	require.NoError(t, err)
	initConnection(t, db)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})

	return &postgresDeployment{db: db, address: address, port: port}
}

func initConnection(t testing.TB, db *sqlx.DB) {
	db.SetMaxOpenConns(maxOpenConnsPerPool)

	// Check that the connection works
	require.NoError(t, backoff.Retry(func() error {
		_, err := db.Exec("SELECT 1")
		return err
	}, backoff.NewTestingBackOff()))
}

func (pd *postgresDeployment) NewDatabaseConfig(t testing.TB) serviceenv.ConfigOption {
	dbName := createEphemeralDB(t)
	return func(config *serviceenv.Configuration) {
		serviceenv.WithPostgresHostPort(pd.address, pd.port)(config)
		config.PostgresDB = dbName
	}
}

func (pd *postgresDeployment) NewDatabase(t testing.TB) (*sqlx.DB, *col.PostgresListener) {
	dbName := createEphemeralDB(t)
	options := []dbutil.Option{
		dbutil.WithHostPort(pd.address, pd.port),
		dbutil.WithDBName(dbName),
	}
	db, err := dbutil.NewDB(options...)
	require.NoError(t, err)
	initConnection(t, db)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})

	listener := col.NewPostgresListener(dbutil.GetDSN(options...))
	t.Cleanup(func() {
		require.NoError(t, listener.Close())
	})

	return db, listener
}

func createEphemeralDB(t testing.TB) string {
	dbName := ephemeralDBName(t)
	opts := []dbutil.Option{
		dbutil.WithUserPassword(PostgresDefaultUser, "elephantastic"),
		dbutil.WithHostPort(dbHost(), dbPort()),
	}
	require.NoError(t, withDB(func(db *sqlx.DB) error {
		_, err := db.Exec("CREATE DATABASE " + dbName)
		require.NoError(t, err)
		t.Log("database", dbName, "successfully created")
		return nil
	}, opts...))
	if cleanup {
		t.Cleanup(func() {
			require.NoError(t, withDB(func(db *sqlx.DB) error {
				_, err := db.Exec("DROP DATABASE " + dbName)
				require.NoError(t, err)
				t.Log("database", dbName, "successfully deleted")
				return nil
			}, opts...))
		})
	}
	return dbName
}

func dbHost() string {
	if host, ok := os.LookupEnv("POSTGRES_SERVICE_HOST"); ok {
		return host
	}
	return PostgresDefaultHost
}

func dbPort() int {
	if port, ok := os.LookupEnv("POSTGRES_SERVICE_PORT"); ok {
		if portInt, err := strconv.Atoi(port); err == nil {
			return portInt
		}
	}
	return PostgresDefaultPort
}

// NewTestDB connects to postgres using the default settings, creates a database
// with a unique name then returns a sqlx.DB configured to use the newly created
// database. After the test or suite finishes, the database is dropped.
func NewTestDB(t testing.TB) *sqlx.DB {
	dbName := createEphemeralDB(t)
	db2, err := dbutil.NewDB(
		dbutil.WithUserPassword(PostgresDefaultUser, "elephantastic"),
		dbutil.WithHostPort(dbHost(), dbPort()),
		dbutil.WithDBName(dbName))
	require.NoError(t, err)
	db2.SetMaxOpenConns(maxOpenConnsPerPool)
	t.Cleanup(func() {
		require.NoError(t, db2.Close())
	})
	return db2
}

// NewTestDBConfig connects to postgres using the default settings, creates a
// database with a unique name then returns a ServiceEnv config option to
// connect to the new database. After test test or suite finishes, the database
// is dropped.
func NewTestDBConfig(t testing.TB) serviceenv.ConfigOption {
	dbName := createEphemeralDB(t)
	return func(config *serviceenv.Configuration) {
		config.PostgresUser = PostgresDefaultUser
		config.PostgresDB = dbName
		config.PostgresServiceHost = dbHost()
		config.PostgresServicePort = dbPort()
	}
}

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

func ephemeralDBName(t testing.TB) string {
	buf := [8]byte{}
	n, err := rand.Reader.Read(buf[:])
	require.NoError(t, err)
	require.Equal(t, n, 8)

	// TODO: it looks like postgres is truncating identifiers to 32 bytes,
	// it should be 64 but we might be passing the name as non-ascii, i'm not really sure.
	// for now just use a random int, but it would be nice to go back to names with a timestamp.
	return fmt.Sprintf("test_%08x", buf)
	//now := time.Now()
	// test_<date>T<time>_<random int>
	// return fmt.Sprintf("test_%04d%02d%02dT%02d%02d%02d_%04x",
	// 	now.Year(), now.Month(), now.Day(),
	// 	now.Hour(), now.Minute(), now.Second(),
	// 	rand.Uint32())
}

package client

import (
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/net/context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"

	types "github.com/gogo/protobuf/types"
	log "github.com/sirupsen/logrus"

	"github.com/pachyderm/pachyderm/src/client/admin"
	"github.com/pachyderm/pachyderm/src/client/auth"
	"github.com/pachyderm/pachyderm/src/client/debug"
	"github.com/pachyderm/pachyderm/src/client/enterprise"
	"github.com/pachyderm/pachyderm/src/client/health"
	"github.com/pachyderm/pachyderm/src/client/limit"
	"github.com/pachyderm/pachyderm/src/client/pfs"
	"github.com/pachyderm/pachyderm/src/client/pkg/config"
	"github.com/pachyderm/pachyderm/src/client/pkg/grpcutil"
	"github.com/pachyderm/pachyderm/src/client/pkg/tracing"
	"github.com/pachyderm/pachyderm/src/client/pps"
	"github.com/pachyderm/pachyderm/src/client/transaction"
	"github.com/pachyderm/pachyderm/src/client/version/versionpb"
)

const (
	// MaxListItemsLog specifies the maximum number of items we log in response to a List* API
	MaxListItemsLog = 10
	// StorageSecretName is the name of the Kubernetes secret in which
	// storage credentials are stored.
	StorageSecretName = "pachyderm-storage-secret"

	// DefaultPachdNodePort is the pachd kubernetes service's default
	// NodePort.Port setting.
	DefaultPachdNodePort = "30650"

	// DefaultPachdPort is the pachd kubernetes service's default
	// Port (often used with Pachyderm ELBs)
	DefaultPachdPort = "650"
)

// PfsAPIClient is an alias for pfs.APIClient.
type PfsAPIClient pfs.APIClient

// PpsAPIClient is an alias for pps.APIClient.
type PpsAPIClient pps.APIClient

// ObjectAPIClient is an alias for pfs.ObjectAPIClient
type ObjectAPIClient pfs.ObjectAPIClient

// AuthAPIClient is an alias of auth.APIClient
type AuthAPIClient auth.APIClient

// VersionAPIClient is an alias of versionpb.APIClient
type VersionAPIClient versionpb.APIClient

// AdminAPIClient is an alias of admin.APIClient
type AdminAPIClient admin.APIClient

// TransactionAPIClient is an alias of transaction.APIClient
type TransactionAPIClient transaction.APIClient

// DebugClient is an alias of debug.DebugClient
type DebugClient debug.DebugClient

// An APIClient is a wrapper around pfs, pps and block APIClients.
type APIClient struct {
	PfsAPIClient
	PpsAPIClient
	ObjectAPIClient
	AuthAPIClient
	VersionAPIClient
	AdminAPIClient
	TransactionAPIClient
	DebugClient
	Enterprise enterprise.APIClient // not embedded--method name conflicts with AuthAPIClient

	// addr is a "host:port" string pointing at a pachd endpoint
	addr string

	// The trusted CAs, for authenticating a pachd server over TLS
	caCerts *x509.CertPool

	// clientConn is a cached grpc connection to 'addr'
	clientConn *grpc.ClientConn

	// healthClient is a cached healthcheck client connected to 'addr'
	healthClient health.HealthClient

	// streamSemaphore limits the number of concurrent message streams between
	// this client and pachd
	limiter limit.ConcurrencyLimiter

	// metricsUserID is an identifier that is included in usage metrics sent to
	// Pachyderm Inc. and is used to count the number of unique Pachyderm users.
	// If unset, no usage metrics are sent back to Pachyderm Inc.
	metricsUserID string

	// metricsPrefix is used to send information from this client to Pachyderm Inc
	// for usage metrics
	metricsPrefix string

	// authenticationToken is an identifier that authenticates the caller in case
	// they want to access privileged data
	authenticationToken string

	// The context used in requests, can be set with WithCtx
	ctx context.Context

	portForwarder *PortForwarder
}

// GetAddress returns the pachd host:port with which 'c' is communicating. If
// 'c' was created using NewInCluster or NewOnUserMachine then this is how the
// address may be retrieved from the environment.
func (c *APIClient) GetAddress() string {
	return c.addr
}

// DefaultMaxConcurrentStreams defines the max number of Putfiles or Getfiles happening simultaneously
const DefaultMaxConcurrentStreams = 100

// DefaultDialTimeout is the max amount of time APIClient.connect() will wait
// for a connection to be established unless overridden by WithDialTimeout()
const DefaultDialTimeout = 30 * time.Second

type clientSettings struct {
	maxConcurrentStreams int
	dialTimeout          time.Duration
	caCerts              *x509.CertPool
}

// NewFromAddress constructs a new APIClient for the server at addr.
func NewFromAddress(addr string, options ...Option) (*APIClient, error) {
	// Apply creation options
	settings := clientSettings{
		maxConcurrentStreams: DefaultMaxConcurrentStreams,
		dialTimeout:          DefaultDialTimeout,
	}
	for _, option := range options {
		if err := option(&settings); err != nil {
			return nil, err
		}
	}
	c := &APIClient{
		addr:    addr,
		caCerts: settings.caCerts,
		limiter: limit.New(settings.maxConcurrentStreams),
	}
	if err := c.connect(settings.dialTimeout); err != nil {
		return nil, err
	}
	return c, nil
}

// Option is a client creation option that may be passed to NewOnUserMachine(), or NewInCluster()
type Option func(*clientSettings) error

// WithMaxConcurrentStreams instructs the New* functions to create client that
// can have at most 'streams' concurrent streams open with pachd at a time
func WithMaxConcurrentStreams(streams int) Option {
	return func(settings *clientSettings) error {
		settings.maxConcurrentStreams = streams
		return nil
	}
}

func addCertFromFile(pool *x509.CertPool, path string) error {
	bytes, err := ioutil.ReadFile(path)
	if err != nil {
		return fmt.Errorf("could not read x509 cert from \"%s\": %v", path, err)
	}
	if ok := pool.AppendCertsFromPEM(bytes); !ok {
		return fmt.Errorf("could not add %s to cert pool as PEM", path)
	}
	return nil
}

// WithRootCAs instructs the New* functions to create client that uses the
// given signed x509 certificates as the trusted root certificates (instead of
// the system certs). Introduced to pass certs provided via command-line flags
func WithRootCAs(path string) Option {
	return func(settings *clientSettings) error {
		settings.caCerts = x509.NewCertPool()
		return addCertFromFile(settings.caCerts, path)
	}
}

// WithAdditionalRootCAs instructs the New* functions to additionally trust the
// given base64-encoded, signed x509 certificates as root certificates.
// Introduced to pass certs in the Pachyderm config
func WithAdditionalRootCAs(pemBytes []byte) Option {
	return func(settings *clientSettings) error {
		// append certs from config
		if settings.caCerts == nil {
			settings.caCerts = x509.NewCertPool()
		}
		if ok := settings.caCerts.AppendCertsFromPEM(pemBytes); !ok {
			return fmt.Errorf("server CA certs are present in Pachyderm config, but could not be added to client")
		}
		return nil
	}
}

// WithDialTimeout instructs the New* functions to use 't' as the deadline to
// connect to pachd
func WithDialTimeout(t time.Duration) Option {
	return func(settings *clientSettings) error {
		settings.dialTimeout = t
		return nil
	}
}

// WithAdditionalPachdCert instructs the New* functions to additionally trust
// the signed cert mounted in Pachd's cert volume. This is used by Pachd
// when connecting to itself (if no cert is present, the clients cert pool
// will not be modified, so that if no other options have been passed, pachd
// will connect to itself over an insecure connection)
func WithAdditionalPachdCert() Option {
	return func(settings *clientSettings) error {
		if _, err := os.Stat(grpcutil.TLSVolumePath); err == nil {
			if settings.caCerts == nil {
				settings.caCerts = x509.NewCertPool()
			}
			return addCertFromFile(settings.caCerts, path.Join(grpcutil.TLSVolumePath, grpcutil.TLSCertFile))
		}
		return nil
	}
}

func getCertOptionsFromEnv() ([]Option, error) {
	var options []Option
	if certPaths, ok := os.LookupEnv("PACH_CA_CERTS"); ok {
		paths := strings.Split(certPaths, ",")
		for _, p := range paths {
			// Try to read all certs under 'p'--skip any that we can't read/stat
			if err := filepath.Walk(p, func(p string, info os.FileInfo, err error) error {
				if err != nil {
					log.Warnf("skipping \"%s\", could not stat path: %v", p, err)
					return nil // Don't try and fix any errors encountered by Walk() itself
				}
				if info.IsDir() {
					return nil // We'll just read the children of any directories when we traverse them
				}
				pemBytes, err := ioutil.ReadFile(p)
				if err != nil {
					log.Warnf("could not read server CA certs at %s: %v", p, err)
					return nil
				}
				options = append(options, WithAdditionalRootCAs(pemBytes))
				return nil
			}); err != nil {
				return nil, err
			}
		}
	}
	return options, nil
}

// getUserMachineAddrAndOpts is a helper for NewOnUserMachine that uses
// environment variables, config files, etc to figure out which address a user
// running a command should connect to.
func getUserMachineAddrAndOpts(cfg *config.Config) (string, []Option, error) {
	// 1) PACHD_ADDRESS environment variable (shell-local) overrides global config
	if envAddr, ok := os.LookupEnv("PACHD_ADDRESS"); ok {
		if !strings.Contains(envAddr, ":") {
			envAddr = fmt.Sprintf("%s:%s", envAddr, DefaultPachdNodePort)
		}
		options, err := getCertOptionsFromEnv()
		if err != nil {
			return "", nil, err
		}
		return envAddr, options, nil
	}

	// 2) Get target address from global config if possible
	if cfg != nil && cfg.V1 != nil && cfg.V1.PachdAddress != "" {
		// Also get cert info from config (if set)
		if cfg.V1.ServerCAs != "" {
			pemBytes, err := base64.StdEncoding.DecodeString(cfg.V1.ServerCAs)
			if err != nil {
				return "", nil, fmt.Errorf("could not decode server CA certs in config: %v", err)
			}
			return cfg.V1.PachdAddress, []Option{WithAdditionalRootCAs(pemBytes)}, nil
		}
		return cfg.V1.PachdAddress, nil, nil
	}

	// 3) Use default address (broadcast) if nothing else works
	options, err := getCertOptionsFromEnv()
	if err != nil {
		return "", nil, err
	}
	return "", options, nil
}

func portForwarder() *PortForwarder {
	log.Debugln("Attempting to implicitly enable port forwarding...")

	// NOTE: this will always use the default namespace; if a custom
	// namespace is required with port forwarding,
	// `pachctl port-forward` should be explicitly called.
	fw, err := NewPortForwarder("")
	if err != nil {
		log.Infof("Implicit port forwarding was not enabled because the kubernetes config could not be read: %v", err)
		return nil
	}
	if err = fw.Lock(); err != nil {
		log.Infof("Implicit port forwarding was not enabled because the pidfile could not be written to. Most likely this means that port forwarding is running in another instance of `pachctl`: %v", err)
		return nil
	}

	if err = fw.RunForDaemon(0, 0); err != nil {
		log.Debugf("Implicit port forwarding for the daemon failed: %v", err)
	}
	if err = fw.RunForSAMLACS(0); err != nil {
		log.Debugf("Implicit port forwarding for SAML ACS failed: %v", err)
	}

	return fw
}

// NewOnUserMachine constructs a new APIClient using env vars that may be set
// on a user's machine (i.e. PACHD_ADDRESS), as well as
// $HOME/.pachyderm/config if it  exists. This is primarily intended to be
// used with the pachctl binary, but may also be useful in tests.
//
// TODO(msteffen) this logic is fairly linux/unix specific, and makes the
// pachyderm client library incompatible with Windows. We may want to move this
// (and similar) logic into src/server and have it call a NewFromOptions()
// constructor.
func NewOnUserMachine(reportMetrics bool, portForward bool, prefix string, options ...Option) (*APIClient, error) {
	cfg, err := config.Read()
	if err != nil {
		// metrics errors are non fatal
		log.Warningf("error loading user config from ~/.pachyderm/config: %v", err)
	}

	// create new pachctl client
	var fw *PortForwarder
	addr, cfgOptions, err := getUserMachineAddrAndOpts(cfg)
	if err != nil {
		return nil, err
	}
	if addr == "" {
		addr = fmt.Sprintf("0.0.0.0:%s", DefaultPachdNodePort)

		if portForward {
			fw = portForwarder()
		}
	}

	client, err := NewFromAddress(addr, append(options, cfgOptions...)...)
	if err != nil {
		if strings.Contains(err.Error(), "context deadline exceeded") {
			// port always starts after last colon, but net.SplitHostPort returns an
			// error on a hostport without a colon, which this might be
			port := ""
			if colonIdx := strings.LastIndexByte(addr, ':'); colonIdx >= 0 {
				port = addr[colonIdx+1:]
			}
			// Check for errors in approximate order of helpfulness
			if port != "" && port != DefaultPachdPort && port != DefaultPachdNodePort {
				return nil, fmt.Errorf("could not connect (note: port is usually "+
					"%s or %s, but is currently set to %q--is this right?): %v", DefaultPachdNodePort, DefaultPachdPort, port, err)
			}
			if strings.HasPrefix(addr, "0.0.0.0") ||
				strings.HasPrefix(addr, "127.0.0.1") ||
				strings.HasPrefix(addr, "[::1]") ||
				strings.HasPrefix(addr, "localhost") {
				return nil, fmt.Errorf("could not connect (note: address %q looks "+
					"like loopback, check that 'pachctl port-forward' is running): %v",
					addr, err)
			}
			if port == "" {
				return nil, fmt.Errorf("could not connect (note: address %q does not "+
					"seem to be host:port): %v", addr, err)
			}
		}
		return nil, fmt.Errorf("could not connect to pachd at %q: %v", addr, err)
	}

	// Add metrics info & authentication token
	client.metricsPrefix = prefix
	if cfg != nil && cfg.UserID != "" && reportMetrics {
		client.metricsUserID = cfg.UserID
	}
	if cfg != nil && cfg.V1 != nil && cfg.V1.SessionToken != "" {
		client.authenticationToken = cfg.V1.SessionToken
	}

	// Add port forwarding. This will set it to nil if port forwarding is
	// disabled, or an address is explicitly set.
	client.portForwarder = fw

	return client, nil
}

// NewInCluster constructs a new APIClient using env vars that Kubernetes creates.
// This should be used to access Pachyderm from within a Kubernetes cluster
// with Pachyderm running on it.
func NewInCluster(options ...Option) (*APIClient, error) {
	host, ok := os.LookupEnv("PACHD_SERVICE_HOST")
	if !ok {
		return nil, fmt.Errorf("PACHD_SERVICE_HOST not set")
	}
	port, ok := os.LookupEnv("PACHD_SERVICE_PORT")
	if !ok {
		return nil, fmt.Errorf("PACHD_SERVICE_PORT not set")
	}
	// create new pachctl client
	return NewFromAddress(fmt.Sprintf("%s:%s", host, port), options...)
}

// Close the connection to gRPC
func (c *APIClient) Close() error {
	if err := c.clientConn.Close(); err != nil {
		return err
	}

	if c.portForwarder != nil {
		c.portForwarder.Close()
	}

	return nil
}

// DeleteAll deletes everything in the cluster.
// Use with caution, there is no undo.
// TODO: rewrite this to use transactions
func (c APIClient) DeleteAll() error {
	if _, err := c.AuthAPIClient.Deactivate(
		c.Ctx(),
		&auth.DeactivateRequest{},
	); err != nil && !auth.IsErrNotActivated(err) {
		return grpcutil.ScrubGRPC(err)
	}
	if _, err := c.PpsAPIClient.DeleteAll(
		c.Ctx(),
		&types.Empty{},
	); err != nil {
		return grpcutil.ScrubGRPC(err)
	}
	if _, err := c.PfsAPIClient.DeleteAll(
		c.Ctx(),
		&types.Empty{},
	); err != nil {
		return grpcutil.ScrubGRPC(err)
	}
	if _, err := c.TransactionAPIClient.DeleteAll(
		c.Ctx(),
		&transaction.DeleteAllRequest{},
	); err != nil {
		return grpcutil.ScrubGRPC(err)
	}
	return nil
}

// SetMaxConcurrentStreams Sets the maximum number of concurrent streams the
// client can have. It is not safe to call this operations while operations are
// outstanding.
func (c APIClient) SetMaxConcurrentStreams(n int) {
	c.limiter = limit.New(n)
}

// DefaultDialOptions is a helper returning a slice of grpc.Dial options
// such that grpc.Dial() is synchronous: the call doesn't return until
// the connection has been established and it's safe to send RPCs
func DefaultDialOptions() []grpc.DialOption {
	return []grpc.DialOption{
		// Don't return from Dial() until the connection has been established
		grpc.WithBlock(),

		// If no connection is established in 30s, fail the call
		grpc.WithTimeout(30 * time.Second),

		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(grpcutil.MaxMsgSize),
			grpc.MaxCallSendMsgSize(grpcutil.MaxMsgSize),
		),
	}
}

func (c *APIClient) connect(timeout time.Duration) error {
	keepaliveOpt := grpc.WithKeepaliveParams(keepalive.ClientParameters{
		Time:                20 * time.Second, // if 20s since last msg (any kind), ping
		Timeout:             20 * time.Second, // if no response to ping for 20s, reset
		PermitWithoutStream: true,             // send ping even if no active RPCs
	})
	dialOptions := append(DefaultDialOptions(), keepaliveOpt)
	if c.caCerts == nil {
		dialOptions = append(dialOptions, grpc.WithInsecure())
	} else {
		tlsCreds := credentials.NewClientTLSFromCert(c.caCerts, "")
		dialOptions = append(dialOptions, grpc.WithTransportCredentials(tlsCreds))
	}
	dialOptions = append(dialOptions,
		// TODO(msteffen) switch to grpc.DialContext instead
		grpc.WithTimeout(timeout),
	)
	if tracing.IsActive() {
		dialOptions = append(dialOptions,
			grpc.WithUnaryInterceptor(tracing.UnaryClientInterceptor()),
			grpc.WithStreamInterceptor(tracing.StreamClientInterceptor()),
		)
	}
	clientConn, err := grpc.Dial(c.addr, dialOptions...)
	if err != nil {
		return err
	}
	c.PfsAPIClient = pfs.NewAPIClient(clientConn)
	c.PpsAPIClient = pps.NewAPIClient(clientConn)
	c.ObjectAPIClient = pfs.NewObjectAPIClient(clientConn)
	c.AuthAPIClient = auth.NewAPIClient(clientConn)
	c.Enterprise = enterprise.NewAPIClient(clientConn)
	c.VersionAPIClient = versionpb.NewAPIClient(clientConn)
	c.AdminAPIClient = admin.NewAPIClient(clientConn)
	c.TransactionAPIClient = transaction.NewAPIClient(clientConn)
	c.DebugClient = debug.NewDebugClient(clientConn)
	c.clientConn = clientConn
	c.healthClient = health.NewHealthClient(clientConn)
	return nil
}

// AddMetadata adds necessary metadata (including authentication credentials)
// to the context 'ctx', preserving any metadata that is present in either the
// incoming or outgoing metadata of 'ctx'.
func (c *APIClient) AddMetadata(ctx context.Context) context.Context {
	// TODO(msteffen): There are several places in this client where it's possible
	// to set per-request metadata (specifically auth tokens): client.WithCtx(),
	// client.SetAuthToken(), etc. These should be consolidated, as this API
	// doesn't make it obvious how these settings are resolved when they conflict.
	clientData := make(map[string]string)
	if c.authenticationToken != "" {
		clientData[auth.ContextTokenKey] = c.authenticationToken
	}
	// metadata API downcases all the key names
	if c.metricsUserID != "" {
		clientData["userid"] = c.metricsUserID
		clientData["prefix"] = c.metricsPrefix
	}

	// Rescue any metadata pairs already in 'ctx' (otherwise
	// metadata.NewOutgoingContext() would drop them). Note that this is similar
	// to metadata.Join(), but distinct because it discards conflicting k/v pairs
	// instead of merging them)
	incomingMD, _ := metadata.FromIncomingContext(ctx)
	outgoingMD, _ := metadata.FromOutgoingContext(ctx)
	clientMD := metadata.New(clientData)
	finalMD := make(metadata.MD) // Collect k/v pairs
	for _, md := range []metadata.MD{incomingMD, outgoingMD, clientMD} {
		for k, v := range md {
			finalMD[k] = v
		}
	}
	return metadata.NewOutgoingContext(ctx, finalMD)
}

// Ctx is a convenience function that returns adds Pachyderm authn metadata
// to context.Background().
func (c *APIClient) Ctx() context.Context {
	if c.ctx == nil {
		return c.AddMetadata(context.Background())
	}
	return c.AddMetadata(c.ctx)
}

// WithCtx returns a new APIClient that uses ctx for requests it sends. Note
// that the new APIClient will still use the authentication token and metrics
// metadata of this client, so this is only useful for propagating other
// context-associated metadata.
func (c *APIClient) WithCtx(ctx context.Context) *APIClient {
	result := *c // copy c
	result.ctx = ctx
	return &result
}

// SetAuthToken sets the authentication token that will be used for all
// API calls for this client.
func (c *APIClient) SetAuthToken(token string) {
	c.authenticationToken = token
}

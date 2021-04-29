package server

import (
	"fmt"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/lib/pq"
	"github.com/pachyderm/pachyderm/v2/src/auth"
	enterpriseclient "github.com/pachyderm/pachyderm/v2/src/enterprise"
	internalauth "github.com/pachyderm/pachyderm/v2/src/internal/auth"
	"github.com/pachyderm/pachyderm/v2/src/internal/backoff"
	col "github.com/pachyderm/pachyderm/v2/src/internal/collection"
	"github.com/pachyderm/pachyderm/v2/src/internal/errors"
	"github.com/pachyderm/pachyderm/v2/src/internal/grpcutil"
	"github.com/pachyderm/pachyderm/v2/src/internal/keycache"
	"github.com/pachyderm/pachyderm/v2/src/internal/log"
	"github.com/pachyderm/pachyderm/v2/src/internal/ppsconsts"
	"github.com/pachyderm/pachyderm/v2/src/internal/serviceenv"
	txnenv "github.com/pachyderm/pachyderm/v2/src/internal/transactionenv"
	"github.com/pachyderm/pachyderm/v2/src/internal/uuid"

	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	logrus "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
)

const (
	tokensPrefix       = "/tokens"
	roleBindingsPrefix = "/role-bindings"
	membersPrefix      = "/members"
	groupsPrefix       = "/groups"
	configPrefix       = "/config"
	oidcAuthnPrefix    = "/oidc-authns"

	// defaultSessionTTLSecs is the lifetime of an auth token from Authenticate,
	defaultSessionTTLSecs = 30 * 24 * 60 * 60 // 30 days

	// configKey is a key (in etcd, in the config collection) that maps to the
	// auth configuration. This is the only key in that collection (due to
	// implemenation details of our config library, we can't use an empty key)
	configKey = "config"

	// clusterRoleBindingKey is a key in etcd, in the roleBindings collection,
	// that contains the set of role bindings for the cluster. These are frequently
	// accessed so we cache them.
	clusterRoleBindingKey = "CLUSTER:"

	// GitHookPort is 655
	// Prometheus uses 656

	// the length of interval between expired auth token cleanups
	cleanupIntervalHours = 24
)

// DefaultOIDCConfig is the default config for the auth API server
var DefaultOIDCConfig = auth.OIDCConfig{}

// APIServer represents an auth api server
type APIServer interface {
	auth.APIServer
	txnenv.AuthTransactionServer
}

// apiServer implements the public interface of the Pachyderm auth system,
// including all RPCs defined in the protobuf spec.
type apiServer struct {
	env        serviceenv.ServiceEnv
	txnEnv     *txnenv.TransactionEnv
	pachLogger log.Logger

	configCache             *keycache.Cache
	clusterRoleBindingCache *keycache.Cache

	// roleBindings is a collection of resource name -> role binding mappings.
	roleBindings col.Collection
	// members is a collection of username -> groups mappings.
	members col.Collection
	// groups is a collection of group -> usernames mappings.
	groups col.Collection
	// collection containing the auth config (under the key configKey)
	authConfig col.Collection
	// oidcStates  contains the set of OIDC nonces for requests that are in progress
	oidcStates col.Collection

	// This is a cache of the PPS master token. It's set once on startup and then
	// never updated
	ppsToken string

	// public addresses the fact that pachd in full mode initializes two auth
	// servers: one that exposes a public API, possibly over TLS, and one that
	// exposes a private API, for internal services. Only the public-facing auth
	// service should export the SAML ACS and Metadata services, so if public
	// is true and auth is active, this may export those SAML services
	public bool

	// watchesEnabled controls whether we cache the auth config and cluster role bindings
	// in the auth service, or whether we look them up each time. Watches are expensive in
	// postgres, so we can't afford to have each sidecar run watches. Pipelines always have
	// direct access to a repo anyways, so the cluster role bindings don't affect their access,
	// and the OIDC server doesn't run in the sidecar so the config doesn't matter.
	watchesEnabled bool
}

// LogReq is like log.Logger.Log(), but it assumes that it's being called from
// the top level of a GRPC method implementation, and correspondingly extracts
// the method name from the parent stack frame
func (a *apiServer) LogReq(request interface{}) {
	a.pachLogger.Log(request, nil, nil, 0)
}

// LogResp is like log.Logger.Log(). However,
// 1) It assumes that it's being called from a defer() statement in a GRPC
//    method , and correspondingly extracts the method name from the grandparent
//    stack frame
// 2) It logs NotActivatedError at DebugLevel instead of ErrorLevel, as, in most
//    cases, this error is expected, and logging it frequently may confuse users
func (a *apiServer) LogResp(request interface{}, response interface{}, err error, duration time.Duration) {
	if err == nil {
		a.pachLogger.LogAtLevelFromDepth(request, response, err, duration, logrus.InfoLevel, 4)
	} else if auth.IsErrNotActivated(err) {
		a.pachLogger.LogAtLevelFromDepth(request, response, err, duration, logrus.DebugLevel, 4)
	} else {
		a.pachLogger.LogAtLevelFromDepth(request, response, err, duration, logrus.ErrorLevel, 4)
	}
}

// NewAuthServer returns an implementation of auth.APIServer.
func NewAuthServer(
	env serviceenv.ServiceEnv,
	txnEnv *txnenv.TransactionEnv,
	etcdPrefix string,
	public bool,
	requireNoncriticalServers bool,
	watchesEnabled bool,
) (APIServer, error) {

	authConfig := col.NewCollection(
		env.GetEtcdClient(),
		path.Join(etcdPrefix, configKey),
		nil,
		&auth.OIDCConfig{},
		nil,
		nil,
	)
	roleBindings := col.NewCollection(
		env.GetEtcdClient(),
		path.Join(etcdPrefix, roleBindingsPrefix),
		nil,
		&auth.RoleBinding{},
		nil,
		nil,
	)
	s := &apiServer{
		env:        env,
		txnEnv:     txnEnv,
		pachLogger: log.NewLogger("auth.API", env.Logger()),
		members: col.NewCollection(
			env.GetEtcdClient(),
			path.Join(etcdPrefix, membersPrefix),
			nil,
			&auth.Groups{},
			nil,
			nil,
		),
		groups: col.NewCollection(
			env.GetEtcdClient(),
			path.Join(etcdPrefix, groupsPrefix),
			nil,
			&auth.Users{},
			nil,
			nil,
		),
		oidcStates: col.NewCollection(
			env.GetEtcdClient(),
			path.Join(oidcAuthnPrefix),
			nil,
			&auth.SessionInfo{},
			nil,
			nil,
		),
		authConfig:     authConfig,
		roleBindings:   roleBindings,
		public:         public,
		watchesEnabled: watchesEnabled,
	}
	go s.retrieveOrGeneratePPSToken()

	if public {
		// start OIDC service (won't respond to anything until config is set)
		go waitForError("OIDC HTTP Server", requireNoncriticalServers, s.serveOIDC)
	}

	if watchesEnabled {
		s.configCache = keycache.NewCache(env.Context(), authConfig, configKey, &DefaultOIDCConfig)
		s.clusterRoleBindingCache = keycache.NewCache(env.Context(), roleBindings, clusterRoleBindingKey, &auth.RoleBinding{})

		// Watch for new auth config options
		go s.configCache.Watch()

		// Watch for changes to the cluster role binding
		go s.clusterRoleBindingCache.Watch()
	}

	s.deleteExpiredTokensRoutine()

	return s, nil
}

func waitForError(name string, required bool, cb func() error) {
	if err := cb(); !errors.Is(err, http.ErrServerClosed) {
		if required {
			logrus.Fatalf("error setting up and/or running %v (use --require-critical-servers-only deploy flag to ignore errors from noncritical servers): %v", name, err)
		}
		logrus.Errorf("error setting up and/or running %v: %v", name, err)
	}
}

// getClusterRoleBinding attempts to get the current cluster role bindings,
// and returns an error if auth is not activated. This can require hitting
// postgres if watches are not enabled (in the worker sidecar).
func (a *apiServer) getClusterRoleBinding(ctx context.Context) (*auth.RoleBinding, error) {
	if a.watchesEnabled {
		bindings, ok := a.clusterRoleBindingCache.Load().(*auth.RoleBinding)
		if !ok {
			return nil, errors.New("cached cluster binding had unexpected type")
		}

		if bindings.Entries == nil {
			return nil, auth.ErrNotActivated
		}
		return bindings, nil
	}

	var binding auth.RoleBinding
	if err := a.roleBindings.ReadOnly(ctx).Get(clusterRoleBindingKey, &binding); err != nil {
		if col.IsErrNotFound(err) {
			return nil, auth.ErrNotActivated
		}
		return nil, err
	}
	return &binding, nil
}

func (a *apiServer) isActive(ctx context.Context) error {
	_, err := a.getClusterRoleBinding(ctx)
	return err
}

// Retrieve the PPS master token, or generate it and put it in etcd.
// TODO This is a hack. PPS and Auth communicate through etcd instead of
// an API, but we should define an internal API and use that instead.
func (a *apiServer) retrieveOrGeneratePPSToken() {
	var tokenProto types.StringValue // will contain PPS token
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	b := backoff.NewExponentialBackOff()
	b.MaxElapsedTime = 60 * time.Second
	b.MaxInterval = 5 * time.Second
	if err := backoff.Retry(func() error {
		if _, err := col.NewSTM(ctx, a.env.GetEtcdClient(), func(stm col.STM) error {
			superUserTokenCol := col.NewCollection(a.env.GetEtcdClient(), ppsconsts.PPSTokenKey, nil, &types.StringValue{}, nil, nil).ReadWrite(stm)
			// TODO(msteffen): Don't use an empty key, as it will not be erased by
			// superUserTokenCol.DeleteAll()
			err := superUserTokenCol.Get("", &tokenProto)
			if err == nil {
				return nil
			}
			if col.IsErrNotFound(err) {
				// no existing token yet -- generate token
				token := uuid.NewWithoutDashes()
				tokenProto.Value = token
				if err := superUserTokenCol.Create("", &tokenProto); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
		a.ppsToken = tokenProto.Value
		return nil
	}, b); err != nil {
		panic(fmt.Sprintf("couldn't create/retrieve PPS superuser token within 60s of starting up: %v", err))
	}
}

func (a *apiServer) getEnterpriseTokenState(ctx context.Context) (enterpriseclient.State, error) {
	pachClient := a.env.GetPachClient(ctx)
	resp, err := pachClient.Enterprise.GetState(pachClient.Ctx(),
		&enterpriseclient.GetStateRequest{})
	if err != nil {
		return 0, errors.Wrapf(grpcutil.ScrubGRPC(err), "could not get Enterprise status")
	}
	return resp.State, nil
}

// Activate implements the protobuf auth.Activate RPC
func (a *apiServer) Activate(ctx context.Context, req *auth.ActivateRequest) (resp *auth.ActivateResponse, retErr error) {
	pachClient := a.env.GetPachClient(ctx)
	ctx = pachClient.Ctx() // copy auth information
	// We don't want to actually log the request/response since they contain
	// credentials.
	defer func(start time.Time) { a.LogResp(nil, nil, retErr, time.Since(start)) }(time.Now())
	// If the cluster's Pachyderm Enterprise token isn't active, the auth system
	// cannot be activated
	state, err := a.getEnterpriseTokenState(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "error confirming Pachyderm Enterprise token")
	}
	if state != enterpriseclient.State_ACTIVE {
		return nil, errors.Errorf("Pachyderm Enterprise is not active in this " +
			"cluster, and the Pachyderm auth API is an Enterprise-level feature")
	}

	// Activating an already activated auth service should fail, because
	// otherwise anyone can just activate the service again and set
	// themselves as an admin.
	if err := a.isActive(ctx); err == nil {
		return nil, auth.ErrAlreadyActivated
	}

	// If the token hash was in the request, use it and return an empty response.
	// Otherwise generate a new random token.
	pachToken := req.RootToken
	if pachToken == "" {
		pachToken = uuid.NewWithoutDashes()
	}

	// Store a new Pachyderm token (as the caller is authenticating) and
	// initialize the root user as a cluster admin
	if _, err = col.NewSTM(ctx, a.env.GetEtcdClient(), func(stm col.STM) error {
		roleBindings := a.roleBindings.ReadWrite(stm)
		if err := roleBindings.Put(clusterRoleBindingKey, &auth.RoleBinding{
			Entries: map[string]*auth.Roles{
				auth.RootUser: &auth.Roles{Roles: map[string]bool{auth.ClusterAdminRole: true}},
			},
		}); err != nil {
			return err
		}
		// TODO(acohen4): workout the transactionality
		return a.insertAuthTokenNoTTL(ctx, auth.HashToken(pachToken), auth.RootUser)
	}); err != nil {
		return nil, err
	}

	// wait until the clusterRoleBinding watcher has updated the local cache
	// (changing the activation state), so that Activate() is less likely to
	// race with subsequent calls that expect auth to be activated.
	if err := backoff.Retry(func() error {
		if err := a.isActive(ctx); err != nil {
			return errors.Errorf("auth never activated")
		}
		return nil
	}, backoff.RetryEvery(100*time.Millisecond)); err != nil {
		return nil, err
	}
	return &auth.ActivateResponse{PachToken: pachToken}, nil
}

// Deactivate implements the protobuf auth.Deactivate RPC
func (a *apiServer) Deactivate(ctx context.Context, req *auth.DeactivateRequest) (resp *auth.DeactivateResponse, retErr error) {
	a.LogReq(req)
	defer func(start time.Time) { a.LogResp(req, resp, retErr, time.Since(start)) }(time.Now())

	_, err := col.NewSTM(ctx, a.env.GetEtcdClient(), func(stm col.STM) error {
		a.roleBindings.ReadWrite(stm).DeleteAll()
		a.deleteAllAuthTokens(ctx) // need to merge with transaction changes
		a.members.ReadWrite(stm).DeleteAll()
		a.groups.ReadWrite(stm).DeleteAll()
		a.authConfig.ReadWrite(stm).DeleteAll()
		return nil
	})
	if err != nil {
		return nil, err
	}

	// wait until the clusterRoleBinding watcher sees the deleted role binding,
	// so that Deactivate() is less likely to race with subsequent calls that
	// expect auth to be deactivated.
	if err := backoff.Retry(func() error {
		if err := a.isActive(ctx); err == nil {
			return errors.Errorf("auth still activated")
		}
		return nil
	}, backoff.RetryEvery(100*time.Millisecond)); err != nil {
		return nil, err
	}
	return &auth.DeactivateResponse{}, nil
}

// expiredEnterpriseCheck enforces that if the cluster's enterprise token is
// expired, users cannot log in. The root token can be used to access the cluster.
func (a *apiServer) expiredEnterpriseCheck(ctx context.Context) error {
	state, err := a.getEnterpriseTokenState(ctx)
	if err != nil {
		return errors.Wrapf(err, "error confirming Pachyderm Enterprise token")
	}

	if state == enterpriseclient.State_ACTIVE {
		return nil
	}

	callerInfo, err := a.getAuthenticatedUser(ctx)
	if err != nil {
		return err
	}

	if callerInfo.Subject == auth.RootUser || callerInfo.Subject == auth.PpsUser {
		return nil
	}

	return errors.New("Pachyderm Enterprise is not active in this " +
		"cluster (until Pachyderm Enterprise is re-activated or Pachyderm " +
		"auth is deactivated, users cannot log in)")
}

// Authenticate implements the protobuf auth.Authenticate RPC
func (a *apiServer) Authenticate(ctx context.Context, req *auth.AuthenticateRequest) (resp *auth.AuthenticateResponse, retErr error) {
	if err := a.isActive(ctx); err != nil {
		return nil, err
	}

	// We don't want to actually log the request/response since they contain
	// credentials.
	defer func(start time.Time) { a.LogResp(nil, nil, retErr, time.Since(start)) }(time.Now())

	// If the cluster's enterprise token is expired, login is disabled
	if err := a.expiredEnterpriseCheck(ctx); err != nil {
		return nil, err
	}

	// verify whatever credential the user has presented, and write a new
	// Pachyderm token for the user that their credential belongs to
	var pachToken string
	switch {
	case req.OIDCState != "":
		// Determine caller's Pachyderm/OIDC user info (email)
		email, err := a.OIDCStateToEmail(ctx, req.OIDCState)
		if err != nil {
			return nil, err
		}

		username := auth.UserPrefix + email

		// Generate a new Pachyderm token and write it
		token, err := a.generateAndInsertAuthToken(ctx, username, defaultSessionTTLSecs)
		if err != nil {
			return nil, errors.Wrapf(err, "error storing auth token for user \"%s\"", username)
		}
		pachToken = token

	case req.IdToken != "":
		// Determine caller's Pachyderm/OIDC user info (email)
		token, claims, err := a.validateIDToken(ctx, req.IdToken)
		if err != nil {
			return nil, err
		}

		username := auth.UserPrefix + claims.Email

		// Sync the user's group membership from the groups claim
		if err := a.syncGroupMembership(ctx, claims); err != nil {
			return nil, err
		}

		// Compute the remaining time before the ID token expires,
		// and limit the pach token to the same expiration time.
		// If the token would be longer-lived than the default pach token,
		// TTL clamp the expiration to the default TTL.
		expirationSecs := int64(time.Until(token.Expiry).Seconds())
		if expirationSecs > defaultSessionTTLSecs {
			expirationSecs = defaultSessionTTLSecs
		}

		t, err := a.generateAndInsertAuthToken(ctx, username, expirationSecs)
		if err != nil {
			return nil, errors.Wrapf(err, "error storing auth token for user \"%s\"", username)
		}
		pachToken = t

	default:
		return nil, errors.Errorf("unrecognized authentication mechanism (old pachd?)")
	}

	logrus.Info("Authentication checks successful, now returning pachToken")

	// Return new pachyderm token to caller
	return &auth.AuthenticateResponse{
		PachToken: pachToken,
	}, nil
}

func resourceKey(r *auth.Resource) string {
	if r.Type == auth.ResourceType_CLUSTER {
		return clusterRoleBindingKey
	}
	return fmt.Sprintf("%s:%s", r.Type, r.Name)
}

func (a *apiServer) evaluateRoleBindingInTransaction(txnCtx *txnenv.TransactionContext, principal string, resource *auth.Resource, permissions map[auth.Permission]bool) (*authorizeRequest, error) {
	binding, err := a.getClusterRoleBinding(txnCtx.ClientContext)
	if err != nil {
		return nil, err
	}

	request := newAuthorizeRequest(principal, permissions, a.getGroups)

	// Check the permissions at the cluster level
	if err := request.evaluateRoleBinding(txnCtx.ClientContext, binding); err != nil {
		return nil, err
	}

	// If all the permissions are satisfied by the cached cluster binding don't
	// retrieve the resource bindings. If the resource in question is the whole
	// cluster we should also exit early
	if request.isSatisfied() || resource.Type == auth.ResourceType_CLUSTER {
		return request, nil
	}

	// Get the role bindings for the resource to check
	var roleBinding auth.RoleBinding
	if err := a.roleBindings.ReadWrite(txnCtx.Stm).Get(resourceKey(resource), &roleBinding); err != nil {
		if col.IsErrNotFound(err) {
			return nil, &auth.ErrNoRoleBinding{*resource}
		}
		return nil, errors.Wrapf(err, "error getting role bindings for %s \"%s\"", resource.Type, resource.Name)
	}
	if err := request.evaluateRoleBinding(txnCtx.ClientContext, &roleBinding); err != nil {
		return nil, err
	}
	return request, nil
}

// AuthorizeInTransaction is identical to Authorize except that it can run
// inside an existing etcd STM transaction.  This is not an RPC.
func (a *apiServer) AuthorizeInTransaction(
	txnCtx *txnenv.TransactionContext,
	req *auth.AuthorizeRequest,
) (resp *auth.AuthorizeResponse, retErr error) {
	callerInfo, err := a.getAuthenticatedUser(txnCtx.ClientContext)
	if err != nil {
		return nil, err
	}

	permissions := make(map[auth.Permission]bool)
	for _, p := range req.Permissions {
		permissions[p] = true
	}

	request, err := a.evaluateRoleBindingInTransaction(txnCtx, callerInfo.Subject, req.Resource, permissions)
	if err != nil {
		return nil, err
	}

	return &auth.AuthorizeResponse{
		Principal:  callerInfo.Subject,
		Authorized: request.isSatisfied(),
		Missing:    request.missing(),
		Satisfied:  request.satisfiedPermissions,
	}, nil
}

// Authorize implements the protobuf auth.Authorize RPC
func (a *apiServer) Authorize(
	ctx context.Context,
	req *auth.AuthorizeRequest,
) (resp *auth.AuthorizeResponse, retErr error) {
	a.LogReq(req)
	defer func(start time.Time) { a.LogResp(req, resp, retErr, time.Since(start)) }(time.Now())

	var response *auth.AuthorizeResponse
	if err := a.txnEnv.WithReadContext(ctx, func(txnCtx *txnenv.TransactionContext) error {
		var err error
		response, err = a.AuthorizeInTransaction(txnCtx, req)
		return err
	}); err != nil {
		return nil, err
	}
	return response, nil
}

func (a *apiServer) GetPermissionsForPrincipal(ctx context.Context, req *auth.GetPermissionsForPrincipalRequest) (resp *auth.GetPermissionsResponse, retErr error) {
	a.LogReq(req)
	defer func(start time.Time) { a.LogResp(req, resp, retErr, time.Since(start)) }(time.Now())

	permissions := make(map[auth.Permission]bool)
	for p := range auth.Permission_name {
		permissions[auth.Permission(p)] = true
	}

	var request *authorizeRequest
	if err := a.txnEnv.WithReadContext(ctx, func(txnCtx *txnenv.TransactionContext) error {
		var err error
		request, err = a.evaluateRoleBindingInTransaction(txnCtx, req.Principal, req.Resource, permissions)
		return err
	}); err != nil {
		return nil, err
	}

	return &auth.GetPermissionsResponse{
		Roles:       request.roles(),
		Permissions: request.satisfied(),
	}, nil

}

// GetPermissions implements the protobuf auth.GetPermissions RPC
func (a *apiServer) GetPermissions(ctx context.Context, req *auth.GetPermissionsRequest) (resp *auth.GetPermissionsResponse, retErr error) {
	a.LogReq(req)
	defer func(start time.Time) { a.LogResp(req, resp, retErr, time.Since(start)) }(time.Now())

	callerInfo, err := a.getAuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}

	return a.GetPermissionsForPrincipal(ctx, &auth.GetPermissionsForPrincipalRequest{Principal: callerInfo.Subject, Resource: req.Resource})
}

// WhoAmI implements the protobuf auth.WhoAmI RPC
func (a *apiServer) WhoAmI(ctx context.Context, req *auth.WhoAmIRequest) (resp *auth.WhoAmIResponse, retErr error) {
	a.pachLogger.LogAtLevelFromDepth(req, nil, nil, 0, logrus.DebugLevel, 2)
	defer func(start time.Time) { a.LogResp(req, resp, retErr, time.Since(start)) }(time.Now())

	if err := a.isActive(ctx); err != nil {
		return nil, err
	}

	callerInfo, err := a.getAuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}

	return &auth.WhoAmIResponse{
		Username:   callerInfo.Subject,
		Expiration: callerInfo.Expiration,
	}, nil
}

// DeleteRoleBindingInTransaction is used to remove role bindings for resources when they're deleted in other services.
// It doesn't do any auth checks itself - the calling method should ensure the user is allowed to delete this resource.
// This is not an RPC, this is only called in-process.
func (a *apiServer) DeleteRoleBindingInTransaction(txnCtx *txnenv.TransactionContext, resource *auth.Resource) error {
	if err := a.isActive(txnCtx.ClientContext); err != nil {
		return err
	}

	if resource.Type == auth.ResourceType_CLUSTER {
		return fmt.Errorf("cannot delete cluster role binding")
	}

	key := resourceKey(resource)
	roleBindings := a.roleBindings.ReadWrite(txnCtx.Stm)
	if err := roleBindings.Delete(key); err != nil {
		return err
	}

	return nil
}

// rolesFromRoleSlice converts a slice of strings into *auth.Roles,
// validating that each role name is valid.
func rolesFromRoleSlice(rs []string) (*auth.Roles, error) {
	if len(rs) == 0 {
		return nil, nil
	}

	for _, r := range rs {
		if _, err := permissionsForRole(r); err != nil {
			return nil, err
		}
	}

	roles := &auth.Roles{Roles: make(map[string]bool)}
	for _, r := range rs {
		roles.Roles[r] = true
	}
	return roles, nil
}

// CreateRoleBindingInTransaction is an internal-only API to create a role binding for a new resource.
// It doesn't do any authorization checks itself - the calling method should ensure the user is allowed
// to create the resource. This is not an RPC.
func (a *apiServer) CreateRoleBindingInTransaction(txnCtx *txnenv.TransactionContext, principal string, roleSlice []string, resource *auth.Resource) error {
	bindings := &auth.RoleBinding{
		Entries: make(map[string]*auth.Roles),
	}

	if len(roleSlice) != 0 {
		// Check that the subject is in canonical form (`<type>:<principal>`).
		if err := a.checkCanonicalSubject(principal); err != nil {
			return err
		}

		roles, err := rolesFromRoleSlice(roleSlice)
		if err != nil {
			return err
		}

		bindings.Entries[principal] = roles
	}

	// Call Create, this will raise an error if the role binding already exists.
	key := resourceKey(resource)
	roleBindings := a.roleBindings.ReadWrite(txnCtx.Stm)
	if err := roleBindings.Create(key, bindings); err != nil {
		return err
	}

	return nil
}

// AddPipelineReaderToRepoInTransaction gives a pipeline access to read data from the specified source repo.
// This is distinct from ModifyRoleBinding because AddPipelineReader is a less expansive permission
// that is included in the repoReader role, versus being able to modify all role bindings which is
// part of repoOwner. This method is for internal use and is not exposed as an RPC.
func (a *apiServer) AddPipelineReaderToRepoInTransaction(txnCtx *txnenv.TransactionContext, sourceRepo, pipeline string) error {
	if err := CheckRepoIsAuthorizedInTransaction(txnCtx, sourceRepo, auth.Permission_REPO_ADD_PIPELINE_READER); err != nil {
		return err
	}

	return a.setUserRoleBindingInTransaction(txnCtx, &auth.Resource{Type: auth.ResourceType_REPO, Name: sourceRepo}, auth.PipelinePrefix+pipeline, []string{auth.RepoReaderRole})
}

// AddPipelineWriterToRepoInTransaction gives a pipeline access to write to it's own output repo.
// This is distinct from ModifyRoleBinding because AddPipelineWriter is a less expansive permission
// that is included in the repoWriter role, versus being able to modify all role bindings which is
// part of repoOwner. This method is for internal use and is not exposed as an RPC.
func (a *apiServer) AddPipelineWriterToRepoInTransaction(txnCtx *txnenv.TransactionContext, pipeline string) error {
	// Check that the user is allowed to add a pipeline to write to the output repo.
	if err := CheckRepoIsAuthorizedInTransaction(txnCtx, pipeline, auth.Permission_REPO_ADD_PIPELINE_WRITER); err != nil {
		return err
	}

	return a.setUserRoleBindingInTransaction(txnCtx, &auth.Resource{Type: auth.ResourceType_REPO, Name: pipeline}, auth.PipelinePrefix+pipeline, []string{auth.RepoWriterRole})
}

// RemovePipelineReaderFromRepo revokes a pipeline's access to read data from the specified source repo.
// This is distinct from ModifyRoleBinding because RemovePipelineReader is a less expansive permission
// that is included in the repoWriter role, versus being able to modify all role bindings which is
// part of repoOwner. This method is for internal use and is not exposed as an RPC.
func (a *apiServer) RemovePipelineReaderFromRepoInTransaction(txnCtx *txnenv.TransactionContext, sourceRepo, pipeline string) error {
	// Check that the user is allowed to remove input repos from the pipeline repo - this check is on the pipeline itself
	// and not sourceRepo because otherwise users could break piplines they don't have access to by revoking them from the
	// input repo.
	if err := CheckRepoIsAuthorizedInTransaction(txnCtx, pipeline, auth.Permission_REPO_REMOVE_PIPELINE_READER); err != nil && !auth.IsErrNoRoleBinding(err) {
		return err
	}

	return a.setUserRoleBindingInTransaction(txnCtx, &auth.Resource{Type: auth.ResourceType_REPO, Name: sourceRepo}, auth.PipelinePrefix+pipeline, []string{})
}

// ModifyRoleBindingInTransaction is identical to ModifyRoleBinding except that it can run inside
// an existing etcd STM transaction.  This is not an RPC.
func (a *apiServer) ModifyRoleBindingInTransaction(
	txnCtx *txnenv.TransactionContext,
	req *auth.ModifyRoleBindingRequest,
) (*auth.ModifyRoleBindingResponse, error) {
	if err := a.isActive(txnCtx.ClientContext); err != nil {
		return nil, err
	}

	if err := a.checkCanonicalSubject(req.Principal); err != nil {
		return nil, err
	}

	if strings.HasPrefix(req.Principal, auth.PachPrefix) && req.Resource.Type == auth.ResourceType_CLUSTER {
		return nil, fmt.Errorf("cannot modify cluster role bindings for pach: users")
	}

	// ModifyRoleBinding can be called for any type of resource,
	// and the permission required depends on the type of resource.
	switch req.Resource.Type {
	case auth.ResourceType_CLUSTER:
		if err := CheckClusterIsAuthorizedInTransaction(txnCtx, auth.Permission_CLUSTER_MODIFY_BINDINGS); err != nil {
			return nil, err
		}
	case auth.ResourceType_REPO:
		if err := CheckRepoIsAuthorizedInTransaction(txnCtx, req.Resource.Name, auth.Permission_REPO_MODIFY_BINDINGS); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unknown resource type %v", req.Resource.Type)
	}

	if err := a.setUserRoleBindingInTransaction(txnCtx, req.Resource, req.Principal, req.Roles); err != nil {
		return nil, err
	}

	return &auth.ModifyRoleBindingResponse{}, nil
}

func (a *apiServer) setUserRoleBindingInTransaction(txnCtx *txnenv.TransactionContext, resource *auth.Resource, principal string, roleSlice []string) error {
	roles, err := rolesFromRoleSlice(roleSlice)
	if err != nil {
		return err
	}

	key := resourceKey(resource)
	roleBindings := a.roleBindings.ReadWrite(txnCtx.Stm)
	var bindings auth.RoleBinding
	if err := roleBindings.Get(key, &bindings); err != nil {
		if col.IsErrNotFound(err) {
			return &auth.ErrNoRoleBinding{*resource}
		}
		return err
	}

	if bindings.Entries == nil {
		bindings.Entries = make(map[string]*auth.Roles)
	}

	if len(roleSlice) == 0 {
		delete(bindings.Entries, principal)
	} else {
		bindings.Entries[principal] = roles
	}
	return roleBindings.Put(key, &bindings)
}

// ModifyRoleBinding implements the protobuf auth.ModifyRoleBinding RPC
func (a *apiServer) ModifyRoleBinding(ctx context.Context, req *auth.ModifyRoleBindingRequest) (resp *auth.ModifyRoleBindingResponse, retErr error) {
	a.LogReq(req)
	defer func(start time.Time) { a.LogResp(req, resp, retErr, time.Since(start)) }(time.Now())

	var response *auth.ModifyRoleBindingResponse
	if err := a.txnEnv.WithTransaction(ctx, func(txn txnenv.Transaction) error {
		var err error
		response, err = txn.ModifyRoleBinding(req)
		return err
	}); err != nil {
		return nil, err
	}

	// If the request is not in a transaction, block until the cache is updated
	if req.Resource.Type == auth.ResourceType_CLUSTER {
		expected, err := rolesFromRoleSlice(req.Roles)
		if err != nil {
			return nil, err
		}
		if err := backoff.Retry(func() error {
			bindings, ok := a.clusterRoleBindingCache.Load().(*auth.RoleBinding)
			if !ok {
				return errors.New("cached cluster binding had unexpected type")
			}
			if !proto.Equal(bindings.Entries[req.Principal], expected) {
				return errors.New("waiting for cache to be updated")
			}
			return nil
		}, backoff.RetryEvery(100*time.Millisecond)); err != nil {
			return nil, err
		}
	}

	return response, nil
}

// GetRoleBindingInTransaction is identical to GetRoleBinding except that it can run inside
// an existing etcd STM transaction.  This is not an RPC.
func (a *apiServer) GetRoleBindingInTransaction(
	txnCtx *txnenv.TransactionContext,
	req *auth.GetRoleBindingRequest,
) (*auth.GetRoleBindingResponse, error) {
	if err := a.isActive(txnCtx.ClientContext); err != nil {
		return nil, err
	}

	// Read role bindings from etcd
	var roleBindings auth.RoleBinding
	if err := a.roleBindings.ReadWrite(txnCtx.Stm).Get(resourceKey(req.Resource), &roleBindings); err != nil && !col.IsErrNotFound(err) {
		return nil, err
	}

	if roleBindings.Entries == nil {
		roleBindings.Entries = make(map[string]*auth.Roles)
	}
	return &auth.GetRoleBindingResponse{
		Binding: &roleBindings,
	}, nil
}

// GetRoleBinding implements the protobuf auth.GetRoleBinding RPC
func (a *apiServer) GetRoleBinding(ctx context.Context, req *auth.GetRoleBindingRequest) (resp *auth.GetRoleBindingResponse, retErr error) {
	a.LogReq(req)
	defer func(start time.Time) { a.LogResp(req, resp, retErr, time.Since(start)) }(time.Now())

	var response *auth.GetRoleBindingResponse
	if err := a.txnEnv.WithReadContext(ctx, func(txnCtx *txnenv.TransactionContext) error {
		var err error
		response, err = a.GetRoleBindingInTransaction(txnCtx, req)
		return err
	}); err != nil {
		return nil, err
	}
	return response, nil
}

// GetRobotToken implements the protobuf auth.GetRobotToken RPC
func (a *apiServer) GetRobotToken(ctx context.Context, req *auth.GetRobotTokenRequest) (resp *auth.GetRobotTokenResponse, retErr error) {
	a.LogReq(req)
	defer func(start time.Time) { a.LogResp(req, resp, retErr, time.Since(start)) }(time.Now())

	// If the user specified a redundant robot: prefix, strip it. Colons are not permitted in robot names.
	subject := strings.TrimPrefix(req.Robot, auth.RobotPrefix)
	if strings.Contains(subject, ":") {
		return nil, errors.New("robot names cannot contain colons (':')")
	}

	subject = auth.RobotPrefix + subject

	// generate new token, and write to postgres
	var token string
	var err error
	if req.TTL > 0 {
		token, err = a.generateAndInsertAuthToken(ctx, subject, req.TTL)
	} else {
		token, err = a.generateAndInsertAuthTokenNoTTL(ctx, subject)
	}
	if err != nil {
		return nil, err
	}
	return &auth.GetRobotTokenResponse{
		Token: token,
	}, nil
}

// GetPipelineAuthTokenInTransaction is an internal API used to create a pipeline token for a given pipeline.
// Not an RPC.
func (a *apiServer) GetPipelineAuthTokenInTransaction(txnCtx *txnenv.TransactionContext, pipeline string) (string, error) {
	if err := a.isActive(txnCtx.ClientContext); err != nil {
		return "", err
	}

	token := uuid.NewWithoutDashes()
	// TODO(acohen4): look at transaction context
	if err := a.insertAuthTokenNoTTL(txnCtx.ClientContext, auth.HashToken(token), auth.PipelinePrefix+pipeline); err != nil {
		return "", errors.Wrapf(err, "error storing token")
	}
	return token, nil
}

// GetOIDCLogin implements the protobuf auth.GetOIDCLogin RPC
func (a *apiServer) GetOIDCLogin(ctx context.Context, req *auth.GetOIDCLoginRequest) (resp *auth.GetOIDCLoginResponse, retErr error) {
	a.LogReq(req)
	// Don't log response to avoid logging OIDC state token
	defer func(start time.Time) { a.LogResp(req, nil, retErr, time.Since(start)) }(time.Now())
	var err error

	authURL, state, err := a.GetOIDCLoginURL(ctx)
	if err != nil {
		return nil, err
	}
	return &auth.GetOIDCLoginResponse{
		LoginURL: authURL,
		State:    state,
	}, nil
}

// RevokeAuthToken implements the protobuf auth.RevokeAuthToken RPC
func (a *apiServer) RevokeAuthToken(ctx context.Context, req *auth.RevokeAuthTokenRequest) (resp *auth.RevokeAuthTokenResponse, retErr error) {
	a.LogReq(req)
	defer func(start time.Time) { a.LogResp(req, resp, retErr, time.Since(start)) }(time.Now())
	a.txnEnv.WithWriteContext(ctx, func(txnCtx *txnenv.TransactionContext) error {
		resp, retErr = a.RevokeAuthTokenInTransaction(txnCtx, req)
		return retErr
	})
	return resp, retErr
}

func (a *apiServer) RevokeAuthTokenInTransaction(txnCtx *txnenv.TransactionContext, req *auth.RevokeAuthTokenRequest) (resp *auth.RevokeAuthTokenResponse, retErr error) {
	if err := a.isActive(txnCtx.ClientContext); err != nil {
		return nil, err
	}

	if err := a.deleteAuthToken(txnCtx.ClientContext, auth.HashToken(req.Token)); err != nil {
		return nil, err
	}
	return &auth.RevokeAuthTokenResponse{}, nil
}

// setGroupsForUserInternal is a helper function used by SetGroupsForUser, and
// also by handleSAMLResponse and handleOIDCExchangeInternal (which updates
// group membership information based on signed SAML assertions or JWT claims).
// This does no auth checks, so the caller must do all relevant authorization.
func (a *apiServer) setGroupsForUserInternal(ctx context.Context, subject string, groups []string) error {
	_, err := col.NewSTM(ctx, a.env.GetEtcdClient(), func(stm col.STM) error {
		members := a.members.ReadWrite(stm)

		// Get groups to remove/add user from/to
		var removeGroups auth.Groups
		addGroups := addToSet(nil, groups...)
		if err := members.Get(subject, &removeGroups); err == nil {
			for _, group := range groups {
				if removeGroups.Groups[group] {
					removeGroups.Groups = removeFromSet(removeGroups.Groups, group)
					addGroups = removeFromSet(addGroups, group)
				}
			}
		}

		// Set groups for user
		if err := members.Put(subject, &auth.Groups{
			Groups: addToSet(nil, groups...),
		}); err != nil {
			return err
		}

		// Remove user from previous groups
		groups := a.groups.ReadWrite(stm)
		var membersProto auth.Users
		for group := range removeGroups.Groups {
			if err := groups.Upsert(group, &membersProto, func() error {
				membersProto.Usernames = removeFromSet(membersProto.Usernames, subject)
				return nil
			}); err != nil {
				return err
			}
		}

		// Add user to new groups
		for group := range addGroups {
			if err := groups.Upsert(group, &membersProto, func() error {
				membersProto.Usernames = addToSet(membersProto.Usernames, subject)
				return nil
			}); err != nil {
				return err
			}
		}

		return nil
	})
	return err
}

// SetGroupsForUser implements the protobuf auth.SetGroupsForUser RPC
func (a *apiServer) SetGroupsForUser(ctx context.Context, req *auth.SetGroupsForUserRequest) (resp *auth.SetGroupsForUserResponse, retErr error) {
	a.LogReq(req)
	defer func(start time.Time) { a.LogResp(req, resp, retErr, time.Since(start)) }(time.Now())

	if err := a.checkCanonicalSubject(req.Username); err != nil {
		return nil, err
	}
	// TODO(msteffen): canonicalize group names
	if err := a.setGroupsForUserInternal(ctx, req.Username, req.Groups); err != nil {
		return nil, err
	}
	return &auth.SetGroupsForUserResponse{}, nil
}

// ModifyMembers implements the protobuf auth.ModifyMembers RPC
func (a *apiServer) ModifyMembers(ctx context.Context, req *auth.ModifyMembersRequest) (resp *auth.ModifyMembersResponse, retErr error) {
	a.LogReq(req)
	defer func(start time.Time) { a.LogResp(req, resp, retErr, time.Since(start)) }(time.Now())

	if err := a.checkCanonicalSubjects(req.Add); err != nil {
		return nil, err
	}
	// TODO(bryce) Skip canonicalization if the users can be found.
	if err := a.checkCanonicalSubjects(req.Remove); err != nil {
		return nil, err
	}

	if _, err := col.NewSTM(ctx, a.env.GetEtcdClient(), func(stm col.STM) error {
		members := a.members.ReadWrite(stm)
		var groupsProto auth.Groups
		for _, username := range req.Add {
			if err := members.Upsert(username, &groupsProto, func() error {
				groupsProto.Groups = addToSet(groupsProto.Groups, req.Group)
				return nil
			}); err != nil {
				return err
			}
		}
		for _, username := range req.Remove {
			if err := members.Upsert(username, &groupsProto, func() error {
				groupsProto.Groups = removeFromSet(groupsProto.Groups, req.Group)
				return nil
			}); err != nil {
				return err
			}
		}

		groups := a.groups.ReadWrite(stm)
		var membersProto auth.Users
		if err := groups.Upsert(req.Group, &membersProto, func() error {
			membersProto.Usernames = addToSet(membersProto.Usernames, req.Add...)
			membersProto.Usernames = removeFromSet(membersProto.Usernames, req.Remove...)
			return nil
		}); err != nil {
			return err
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return &auth.ModifyMembersResponse{}, nil
}

func addToSet(set map[string]bool, elems ...string) map[string]bool {
	if set == nil {
		set = map[string]bool{}
	}

	for _, elem := range elems {
		set[elem] = true
	}
	return set
}

func removeFromSet(set map[string]bool, elems ...string) map[string]bool {
	if set != nil {
		for _, elem := range elems {
			delete(set, elem)
		}
	}

	return set
}

// getGroups is a helper function used primarily by the GRPC API GetGroups, but
// also by Authorize() and isAdmin().
func (a *apiServer) getGroups(ctx context.Context, subject string) ([]string, error) {
	members := a.members.ReadOnly(ctx)
	var groupsProto auth.Groups
	if err := members.Get(subject, &groupsProto); err != nil {
		if col.IsErrNotFound(err) {
			return []string{}, nil
		}
		return nil, err
	}
	return setToList(groupsProto.Groups), nil
}

// GetGroups implements the protobuf auth.GetGroups RPC
func (a *apiServer) GetGroups(ctx context.Context, req *auth.GetGroupsRequest) (resp *auth.GetGroupsResponse, retErr error) {
	a.LogReq(req)
	defer func(start time.Time) { a.LogResp(req, resp, retErr, time.Since(start)) }(time.Now())

	callerInfo, err := a.getAuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}

	groups, err := a.getGroups(ctx, callerInfo.Subject)
	if err != nil {
		return nil, err
	}
	return &auth.GetGroupsResponse{Groups: groups}, nil
}

// GetGroupsForPrincipal implements the protobuf auth.GetGroupsForPrincipal RPC
func (a *apiServer) GetGroupsForPrincipal(ctx context.Context, req *auth.GetGroupsForPrincipalRequest) (resp *auth.GetGroupsResponse, retErr error) {
	a.LogReq(req)
	defer func(start time.Time) { a.LogResp(req, resp, retErr, time.Since(start)) }(time.Now())

	groups, err := a.getGroups(ctx, req.Principal)
	if err != nil {
		return nil, err
	}
	return &auth.GetGroupsResponse{Groups: groups}, nil
}

// GetUsers implements the protobuf auth.GetUsers RPC
func (a *apiServer) GetUsers(ctx context.Context, req *auth.GetUsersRequest) (resp *auth.GetUsersResponse, retErr error) {
	a.LogReq(req)
	defer func(start time.Time) { a.LogResp(req, resp, retErr, time.Since(start)) }(time.Now())

	// Filter by group
	if req.Group != "" {
		var membersProto auth.Users
		if _, err := col.NewSTM(ctx, a.env.GetEtcdClient(), func(stm col.STM) error {
			groups := a.groups.ReadWrite(stm)
			if err := groups.Get(req.Group, &membersProto); err != nil {
				return err
			}
			return nil
		}); err != nil {
			return nil, err
		}

		return &auth.GetUsersResponse{Usernames: setToList(membersProto.Usernames)}, nil
	}

	membersCol := a.members.ReadOnly(ctx)
	groups := &auth.Groups{}
	var users []string
	if err := membersCol.List(groups, col.DefaultOptions, func(user string) error {
		users = append(users, user)
		return nil
	}); err != nil {
		return nil, err
	}
	return &auth.GetUsersResponse{Usernames: users}, nil
}

func setToList(set map[string]bool) []string {
	if set == nil {
		return []string{}
	}

	list := []string{}
	for elem := range set {
		list = append(list, elem)
	}
	return list
}

func (a *apiServer) getAuthenticatedUser(ctx context.Context) (*auth.TokenInfo, error) {
	if err := a.isActive(ctx); err != nil {
		return nil, err
	}

	token, err := auth.GetAuthToken(ctx)
	if err != nil {
		return nil, err
	}

	if token == a.ppsToken {
		// TODO(msteffen): This is a hack. The idea is that there is a logical user
		// entry mapping ppsToken to ppsUser. Soon, ppsUser will go away and
		// this check should happen in authorize
		return &auth.TokenInfo{
			Subject: auth.PpsUser,
		}, nil
	}

	// try to lookup pre-computed subject
	if subject := internalauth.GetWhoAmI(ctx); subject != "" {
		return &auth.TokenInfo{
			Subject: subject,
		}, nil
	}

	// Lookup the token
	tokenInfo, lookupErr := a.lookupAuthTokenInfo(ctx, auth.HashToken(token))
	if lookupErr != nil {
		if col.IsErrNotFound(lookupErr) {
			return nil, auth.ErrBadToken
		}
		return nil, lookupErr
	}
	// verify token hasn't expired
	if tokenInfo.Expiration != nil {
		if time.Now().After(*tokenInfo.Expiration) {
			return nil, auth.ErrExpiredToken
		}
	}
	return tokenInfo, nil
}

// checkCanonicalSubjects applies checkCanonicalSubject to a list
func (a *apiServer) checkCanonicalSubjects(subjects []string) error {
	for _, subject := range subjects {
		if err := a.checkCanonicalSubject(subject); err != nil {
			return err
		}
	}
	return nil
}

// checkCanonicalSubject returns an error if a subject doesn't have a prefix, or the prefix is
// not recognized
func (a *apiServer) checkCanonicalSubject(subject string) error {
	if subject == auth.AllClusterUsersSubject {
		return nil
	}

	colonIdx := strings.Index(subject, ":")
	if colonIdx < 0 {
		return errors.Errorf("subject has no prefix, must be of the form <type>:<name>")
	}
	prefix := subject[:colonIdx]

	// check against fixed prefixes
	prefix += ":" // append ":" to match constants
	switch prefix {
	case auth.PipelinePrefix, auth.RobotPrefix, auth.PachPrefix, auth.UserPrefix, auth.GroupPrefix:
		break
	default:
		return errors.Errorf("subject has unrecognized prefix: %s", subject[:colonIdx+1])
	}
	return nil
}

// GetConfiguration implements the protobuf auth.GetConfiguration RPC.
func (a *apiServer) GetConfiguration(ctx context.Context, req *auth.GetConfigurationRequest) (resp *auth.GetConfigurationResponse, retErr error) {
	a.LogReq(req)
	defer func(start time.Time) { a.LogResp(req, resp, retErr, time.Since(start)) }(time.Now())

	if !a.watchesEnabled {
		return nil, errors.New("watches are not enabled, unable to get current config")
	}

	config, ok := a.configCache.Load().(*auth.OIDCConfig)
	if !ok {
		return nil, errors.New("cached auth config had unexpected type")
	}

	return &auth.GetConfigurationResponse{
		Configuration: config,
	}, nil
}

// SetConfiguration implements the protobuf auth.SetConfiguration RPC
func (a *apiServer) SetConfiguration(ctx context.Context, req *auth.SetConfigurationRequest) (resp *auth.SetConfigurationResponse, retErr error) {
	a.LogReq(req)
	defer func(start time.Time) { a.LogResp(req, resp, retErr, time.Since(start)) }(time.Now())

	if !a.watchesEnabled {
		return nil, errors.New("watches are not enabled, unable to set config")
	}

	var configToStore *auth.OIDCConfig
	if req.Configuration != nil {
		// Validate new config
		if err := validateOIDCConfig(ctx, req.Configuration); err != nil {
			return nil, err
		}
		configToStore = req.Configuration
	} else {
		configToStore = proto.Clone(&DefaultOIDCConfig).(*auth.OIDCConfig)
	}

	// set the new config
	if _, err := col.NewSTM(ctx, a.env.GetEtcdClient(), func(stm col.STM) error {
		return a.authConfig.ReadWrite(stm).Put(configKey, configToStore)
	}); err != nil {
		return nil, err
	}

	// block until the watcher observes the write
	if err := backoff.Retry(func() error {
		record, ok := a.configCache.Load().(*auth.OIDCConfig)
		if !ok {
			return errors.Errorf("could not retrieve auth config from cache")
		}
		if !proto.Equal(record, configToStore) {
			return errors.Errorf("config in cache was not updated")
		}
		return nil
	}, backoff.RetryEvery(time.Second)); err != nil {
		return nil, err
	}

	return &auth.SetConfigurationResponse{}, nil
}

func (a *apiServer) ExtractAuthTokens(ctx context.Context, req *auth.ExtractAuthTokensRequest) (resp *auth.ExtractAuthTokensResponse, retErr error) {
	// We don't want to actually log the request/response since they contain
	// credentials.
	a.LogReq(req)
	defer func(start time.Time) { a.LogResp(nil, nil, retErr, time.Since(start)) }(time.Now())
	if err := a.isActive(ctx); err != nil {
		return nil, err
	}

	extracted, err := a.listRobotTokens(ctx)
	if err != nil {
		return nil, err
	}
	return &auth.ExtractAuthTokensResponse{Tokens: extracted}, nil
}

func (a *apiServer) RestoreAuthToken(ctx context.Context, req *auth.RestoreAuthTokenRequest) (resp *auth.RestoreAuthTokenResponse, retErr error) {
	// We don't want to actually log the request/response since they contain
	// credentials.
	defer func(start time.Time) { a.LogResp(nil, nil, retErr, time.Since(start)) }(time.Now())

	var ttl int64
	if req.Token.Expiration != nil {
		ttl = int64(time.Until(*req.Token.Expiration).Seconds())
		if ttl < 0 {
			return nil, auth.ErrExpiredToken
		}
	}

	if err := func() error {
		if ttl > 0 {
			return a.insertAuthToken(ctx, req.Token.HashedToken, req.Token.Subject, ttl)
		}
		return a.insertAuthTokenNoTTL(ctx, req.Token.HashedToken, req.Token.Subject)
	}(); err != nil {
		return nil, errors.Wrapf(err, "error restoring auth token")
	}

	return &auth.RestoreAuthTokenResponse{}, nil
}

// implements the protobuf auth.DeleteExpiredAuthTokens RPC
func (a *apiServer) DeleteExpiredAuthTokens(ctx context.Context, req *auth.DeleteExpiredAuthTokensRequest) (*auth.DeleteExpiredAuthTokensResponse, error) {
	logrus.Info("Deleting expired auth tokens")
	if _, err := a.env.GetDBClient().Exec(`DELETE FROM auth.auth_tokens WHERE NOW() > expiration`); err != nil {
		return nil, errors.Wrapf(err, "error deleting expired tokens")
	}
	return &auth.DeleteExpiredAuthTokensResponse{}, nil
}

func (a *apiServer) RevokeAuthTokensForUser(ctx context.Context, req *auth.RevokeAuthTokensForUserRequest) (resp *auth.RevokeAuthTokensForUserResponse, retErr error) {
	defer func(start time.Time) { a.LogResp(req, resp, retErr, time.Since(start)) }(time.Now())

	if _, err := a.env.GetDBClient().ExecContext(ctx, `DELETE FROM auth.auth_tokens WHERE subject = $1`, req.Username); err != nil {
		return nil, errors.Wrapf(err, "error deleting all auth tokens")
	}
	return &auth.RevokeAuthTokensForUserResponse{}, nil
}

func (a *apiServer) deleteExpiredTokensRoutine() {
	go func(ctx context.Context) {
		for {
			time.Sleep(time.Duration(cleanupIntervalHours) * time.Hour)
			a.DeleteExpiredAuthTokens(ctx, &auth.DeleteExpiredAuthTokensRequest{})
		}
	}(context.Background())
}

// we interpret an expiration value of NULL as "lives forever".
func (a *apiServer) lookupAuthTokenInfo(ctx context.Context, tokenHash string) (*auth.TokenInfo, error) {
	var tokenInfo auth.TokenInfo

	err := a.env.GetDBClient().GetContext(ctx, &tokenInfo, `SELECT subject, expiration FROM auth.auth_tokens WHERE token_hash = $1`, tokenHash)

	if err != nil {
		return nil, col.ErrNotFound{Type: "auth_tokens", Key: tokenHash}
	}

	return &tokenInfo, nil
}

// we will sometimes have expiration values set in the passed, since we only remove those values in the deleteExpiredTokensRoutine() goroutine
func (a *apiServer) listRobotTokens(ctx context.Context) ([]*auth.TokenInfo, error) {
	robotTokens := make([]*auth.TokenInfo, 0)
	if err := a.env.GetDBClient().SelectContext(ctx, &robotTokens,
		`SELECT token_hash, subject, expiration
		FROM auth.auth_tokens 
		WHERE subject LIKE $1 || '%'`, auth.RobotPrefix); err != nil {
		return nil, errors.Wrapf(err, "error querying token")
	}
	return robotTokens, nil
}

func (a *apiServer) generateAndInsertAuthToken(ctx context.Context, subject string, ttlSeconds int64) (string, error) {
	token := uuid.NewWithoutDashes()
	if err := a.insertAuthToken(ctx, auth.HashToken(token), subject, ttlSeconds); err != nil {
		return "", err
	}
	return token, nil
}

func (a *apiServer) generateAndInsertAuthTokenNoTTL(ctx context.Context, subject string) (string, error) {
	token := uuid.NewWithoutDashes()
	if err := a.insertAuthTokenNoTTL(ctx, auth.HashToken(token), subject); err != nil {
		return "", err
	}

	return token, nil
}

// generates a token, and stores it's hash and supporting data in postgres
func (a *apiServer) insertAuthToken(ctx context.Context, tokenHash string, subject string, ttlSeconds int64) error {
	// Register the pachd in the database
	if _, err := a.env.GetDBClient().ExecContext(ctx,
		`INSERT INTO auth.auth_tokens (token_hash, subject, expiration) 
		VALUES ($1, $2, NOW() + $3 * interval '1 sec')`, tokenHash, subject, ttlSeconds); err != nil {
		if pgErr, ok := err.(*pq.Error); ok {
			if pgErr.Code == pq.ErrorCode(pgerrcode.UniqueViolation) {
				return errors.New("cannot overwrite existing token with same hash")
			}
		}
		return errors.Wrapf(err, "error storing token")
	}
	return nil
}

func (a *apiServer) insertAuthTokenNoTTL(ctx context.Context, tokenHash string, subject string) error {
	// Register the pachd in the database
	if _, err := a.env.GetDBClient().ExecContext(ctx,
		`INSERT INTO auth.auth_tokens (token_hash, subject) 
		VALUES ($1, $2)`, tokenHash, subject); err != nil {
		if pgErr, ok := err.(*pq.Error); ok {
			if pgErr.Code == pq.ErrorCode(pgerrcode.UniqueViolation) {
				return errors.New("cannot overwrite existing token with same hash")
			}
		}
		return errors.Wrapf(err, "error storing token")
	}
	return nil
}

// TODO(acohen4): Transactionify
func (a *apiServer) deleteAllAuthTokens(ctx context.Context) error {
	if _, err := a.env.GetDBClient().ExecContext(ctx, `DELETE FROM auth.auth_tokens`); err != nil {
		return errors.Wrapf(err, "error deleting all auth tokens")
	}
	return nil
}

func (a *apiServer) deleteAuthToken(ctx context.Context, tokenHash string) error {
	if _, err := a.env.GetDBClient().ExecContext(ctx, `DELETE FROM auth.auth_tokens WHERE token_hash=$1`, tokenHash); err != nil {
		return errors.Wrapf(err, "error deleting token")
	}
	return nil
}

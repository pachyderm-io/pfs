package auth

import (
	"context"
	"fmt"

	"github.com/pachyderm/pachyderm/v2/src/auth"
	"github.com/pachyderm/pachyderm/v2/src/internal/serviceenv"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// authHandlers is a mapping of RPCs to authorization levels required to access them.
// This interceptor fails closed - whenever a new RPC is added, it's disabled
// until some authentication is added. Some RPCs may do additional auth checks
// beyond what's required by this interceptor.
var authHandlers = map[string]authHandler{
	//
	// Admin API
	//

	// Allow InspectCluster to succeed before a user logs in
	"/admin.API/InspectCluster": unauthenticated,

	//
	// Auth API
	//

	// Activate only has an effect when auth is not enabled
	// Authenticate, Authorize and WhoAmI check auth status themselves
	// GetOIDCLogin is necessary to authenticate
	"/auth.API/Activate":     unauthenticated,
	"/auth.API/Authenticate": unauthenticated,
	"/auth.API/Authorize":    unauthenticated,
	"/auth.API/WhoAmI":       unauthenticated,
	"/auth.API/GetOIDCLogin": unauthenticated,

	// TODO: restrict GetClusterRoleBinding to cluster admins?
	"/auth.API/CreateRoleBinding":       authenticated,
	"/auth.API/GetRoleBinding":          authenticated,
	"/auth.API/ModifyRoleBinding":       authenticated,
	"/auth.API/RevokeAuthToken":         authenticated,
	"/auth.API/RevokeAuthTokensForUser": authenticated,
	"/auth.API/GetGroups":               authenticated,
	"/auth.API/GetPermissions":          authenticated,

	"/auth.API/GetGroupsForPrincipal":      clusterPermissions(auth.Permission_CLUSTER_AUTH_GET_GROUPS),
	"/auth.API/GetPermissionsForPrincipal": clusterPermissions(auth.Permission_CLUSTER_AUTH_GET_PERMISSIONS_FOR_PRINCIPAL),
	"/auth.API/GetConfiguration":           clusterPermissions(auth.Permission_CLUSTER_AUTH_GET_CONFIG),
	"/auth.API/SetConfiguration":           clusterPermissions(auth.Permission_CLUSTER_AUTH_SET_CONFIG),
	"/auth.API/GetRobotToken":              clusterPermissions(auth.Permission_CLUSTER_AUTH_GET_ROBOT_TOKEN),
	"/auth.API/SetGroupsForUser":           clusterPermissions(auth.Permission_CLUSTER_AUTH_MODIFY_GROUP_MEMBERS),
	"/auth.API/ModifyMembers":              clusterPermissions(auth.Permission_CLUSTER_AUTH_MODIFY_GROUP_MEMBERS),
	"/auth.API/GetUsers":                   clusterPermissions(auth.Permission_CLUSTER_AUTH_GET_GROUP_USERS),
	"/auth.API/ExtractAuthTokens":          clusterPermissions(auth.Permission_CLUSTER_AUTH_EXTRACT_TOKENS),
	"/auth.API/RestoreAuthToken":           clusterPermissions(auth.Permission_CLUSTER_AUTH_RESTORE_TOKEN),
	"/auth.API/Deactivate":                 clusterPermissions(auth.Permission_CLUSTER_AUTH_DEACTIVATE),
	"/auth.API/DeleteExpiredAuthTokens":    clusterPermissions(auth.Permission_CLUSTER_AUTH_DELETE_EXPIRED_TOKENS),

	//
	// Debug API
	//

	"/debug.Debug/Profile": authDisabledOr(clusterPermissions(auth.Permission_CLUSTER_DEBUG_DUMP)),
	"/debug.Debug/Binary":  authDisabledOr(clusterPermissions(auth.Permission_CLUSTER_DEBUG_DUMP)),
	"/debug.Debug/Dump":    authDisabledOr(clusterPermissions(auth.Permission_CLUSTER_DEBUG_DUMP)),

	//
	// Enterprise API
	//

	"/enterprise.API/GetState":          unauthenticated,
	"/enterprise.API/Activate":          authDisabledOr(clusterPermissions(auth.Permission_CLUSTER_ENTERPRISE_ACTIVATE)),
	"/enterprise.API/GetActivationCode": authDisabledOr(clusterPermissions(auth.Permission_CLUSTER_ENTERPRISE_GET_CODE)),
	"/enterprise.API/Deactivate":        authDisabledOr(clusterPermissions(auth.Permission_CLUSTER_ENTERPRISE_DEACTIVATE)),
	"/enterprise.API/Heartbeat":         authDisabledOr(clusterPermissions(auth.Permission_CLUSTER_ENTERPRISE_HEARTBEAT)),

	//
	// Health API
	//
	"/health.Health/Health": unauthenticated,

	//
	// Identity API
	//
	"/identity.API/SetIdentityServerConfig": clusterPermissions(auth.Permission_CLUSTER_IDENTITY_SET_CONFIG),
	"/identity.API/GetIdentityServerConfig": clusterPermissions(auth.Permission_CLUSTER_IDENTITY_GET_CONFIG),
	"/identity.API/CreateIDPConnector":      clusterPermissions(auth.Permission_CLUSTER_IDENTITY_CREATE_IDP),
	"/identity.API/UpdateIDPConnector":      clusterPermissions(auth.Permission_CLUSTER_IDENTITY_UPDATE_IDP),
	"/identity.API/ListIDPConnectors":       clusterPermissions(auth.Permission_CLUSTER_IDENTITY_LIST_IDPS),
	"/identity.API/GetIDPConnector":         clusterPermissions(auth.Permission_CLUSTER_IDENTITY_GET_IDP),
	"/identity.API/DeleteIDPConnector":      clusterPermissions(auth.Permission_CLUSTER_IDENTITY_DELETE_IDP),
	"/identity.API/CreateOIDCClient":        clusterPermissions(auth.Permission_CLUSTER_IDENTITY_CREATE_OIDC_CLIENT),
	"/identity.API/UpdateOIDCClient":        clusterPermissions(auth.Permission_CLUSTER_IDENTITY_UPDATE_OIDC_CLIENT),
	"/identity.API/GetOIDCClient":           clusterPermissions(auth.Permission_CLUSTER_IDENTITY_GET_OIDC_CLIENT),
	"/identity.API/ListOIDCClients":         clusterPermissions(auth.Permission_CLUSTER_IDENTITY_LIST_OIDC_CLIENTS),
	"/identity.API/DeleteOIDCClient":        clusterPermissions(auth.Permission_CLUSTER_IDENTITY_DELETE_OIDC_CLIENT),
	"/identity.API/DeleteAll":               clusterPermissions(auth.Permission_CLUSTER_DELETE_ALL),

	//
	// License API
	//
	"/license.API/Activate":          authDisabledOr(clusterPermissions(auth.Permission_CLUSTER_LICENSE_ACTIVATE)),
	"/license.API/GetActivationCode": authDisabledOr(clusterPermissions(auth.Permission_CLUSTER_LICENSE_GET_CODE)),
	"/license.API/AddCluster":        authDisabledOr(clusterPermissions(auth.Permission_CLUSTER_LICENSE_ADD_CLUSTER)),
	"/license.API/UpdateCluster":     authDisabledOr(clusterPermissions(auth.Permission_CLUSTER_LICENSE_UPDATE_CLUSTER)),
	"/license.API/DeleteCluster":     authDisabledOr(clusterPermissions(auth.Permission_CLUSTER_LICENSE_DELETE_CLUSTER)),
	"/license.API/ListClusters":      authDisabledOr(clusterPermissions(auth.Permission_CLUSTER_LICENSE_LIST_CLUSTERS)),
	"/license.API/DeleteAll":         authDisabledOr(clusterPermissions(auth.Permission_CLUSTER_DELETE_ALL)),
	// Heartbeat relies on the shared secret generated at cluster registration-time
	"/license.API/Heartbeat": unauthenticated,

	//
	// PFS API
	//

	// TODO: Add methods to handle repo permissions
	"/pfs.API/ActivateAuth":    clusterPermissions(auth.Permission_CLUSTER_AUTH_ACTIVATE),
	"/pfs.API/CreateRepo":      authDisabledOr(authenticated),
	"/pfs.API/InspectRepo":     authDisabledOr(authenticated),
	"/pfs.API/ListRepo":        authDisabledOr(authenticated),
	"/pfs.API/DeleteRepo":      authDisabledOr(authenticated),
	"/pfs.API/StartCommit":     authDisabledOr(authenticated),
	"/pfs.API/FinishCommit":    authDisabledOr(authenticated),
	"/pfs.API/InspectCommit":   authDisabledOr(authenticated),
	"/pfs.API/ListCommit":      authDisabledOr(authenticated),
	"/pfs.API/SquashCommit":    authDisabledOr(authenticated),
	"/pfs.API/FlushCommit":     authDisabledOr(authenticated),
	"/pfs.API/SubscribeCommit": authDisabledOr(authenticated),
	"/pfs.API/ClearCommit":     authDisabledOr(authenticated),
	"/pfs.API/CreateBranch":    authDisabledOr(authenticated),
	"/pfs.API/InspectBranch":   authDisabledOr(authenticated),
	"/pfs.API/ListBranch":      authDisabledOr(authenticated),
	"/pfs.API/DeleteBranch":    authDisabledOr(authenticated),
	"/pfs.API/ModifyFile":      authDisabledOr(authenticated),
	"/pfs.API/GetFile":         authDisabledOr(authenticated),
	"/pfs.API/InspectFile":     authDisabledOr(authenticated),
	"/pfs.API/ListFile":        authDisabledOr(authenticated),
	"/pfs.API/WalkFile":        authDisabledOr(authenticated),
	"/pfs.API/GlobFile":        authDisabledOr(authenticated),
	"/pfs.API/DiffFile":        authDisabledOr(authenticated),
	"/pfs.API/DeleteAll":       authDisabledOr(authenticated),
	"/pfs.API/Fsck":            authDisabledOr(authenticated),
	"/pfs.API/CreateFileset":   authDisabledOr(authenticated),
	"/pfs.API/GetFileset":      authDisabledOr(authenticated),
	"/pfs.API/AddFileset":      authDisabledOr(authenticated),
	"/pfs.API/RenewFileset":    authDisabledOr(authenticated),

	//
	// PPS API
	//

	// TODO: Add per-repo permissions checks for these
	// TODO: split GetLogs into master and not-master and add check for pipeline permissions
	"/pps.API/CreateJob":       authDisabledOr(authenticated),
	"/pps.API/InspectJob":      authDisabledOr(authenticated),
	"/pps.API/ListJob":         authDisabledOr(authenticated),
	"/pps.API/ListJobStream":   authDisabledOr(authenticated),
	"/pps.API/FlushJob":        authDisabledOr(authenticated),
	"/pps.API/DeleteJob":       authDisabledOr(authenticated),
	"/pps.API/StopJob":         authDisabledOr(authenticated),
	"/pps.API/InspectDatum":    authDisabledOr(authenticated),
	"/pps.API/ListDatum":       authDisabledOr(authenticated),
	"/pps.API/ListDatumStream": authDisabledOr(authenticated),
	"/pps.API/RestartDatum":    authDisabledOr(authenticated),
	"/pps.API/CreatePipeline":  authDisabledOr(authenticated),
	"/pps.API/InspectPipeline": authDisabledOr(authenticated),
	"/pps.API/DeletePipeline":  authDisabledOr(authenticated),
	"/pps.API/StartPipeline":   authDisabledOr(authenticated),
	"/pps.API/StopPipeline":    authDisabledOr(authenticated),
	"/pps.API/RunPipeline":     authDisabledOr(authenticated),
	"/pps.API/RunCron":         authDisabledOr(authenticated),
	"/pps.API/CreateSecret":    authDisabledOr(authenticated),
	"/pps.API/DeleteSecret":    authDisabledOr(authenticated),
	"/pps.API/ListSecret":      authDisabledOr(authenticated),
	"/pps.API/InspectSecret":   authDisabledOr(authenticated),
	"/pps.API/GetLogs":         authDisabledOr(authenticated),
	"/pps.API/GarbageCollect":  authDisabledOr(authenticated),
	"/pps.API/UpdateJobState":  authDisabledOr(authenticated),
	"/pps.API/ListPipeline":    authDisabledOr(authenticated),
	"/pps.API/ActivateAuth":    clusterPermissions(auth.Permission_CLUSTER_AUTH_ACTIVATE),
	"/pps.API/DeleteAll":       authDisabledOr(clusterPermissions(auth.Permission_CLUSTER_DELETE_ALL)),

	//
	// TransactionAPI
	//

	"/transaction.API/BatchTransaction":   authDisabledOr(authenticated),
	"/transaction.API/StartTransaction":   authDisabledOr(authenticated),
	"/transaction.API/InspectTransaction": authDisabledOr(authenticated),
	"/transaction.API/DeleteTransaction":  authDisabledOr(authenticated),
	"/transaction.API/ListTransaction":    authDisabledOr(authenticated),
	"/transaction.API/FinishTransaction":  authDisabledOr(authenticated),
	"/transaction.API/DeleteAll":          authDisabledOr(clusterPermissions(auth.Permission_CLUSTER_DELETE_ALL)),

	//
	// Version API
	//

	"/versionpb.API/GetVersion": unauthenticated,
}

// NewInterceptor instantiates a new Interceptor
func NewInterceptor(env serviceenv.ServiceEnv) *Interceptor {
	return &Interceptor{
		env: env,
	}
}

// ServerStreamWrapper adds values into the stream's Go Context
type ServerStreamWrapper struct {
	stream grpc.ServerStream
	ctx    context.Context
}

// Context returns the stream's Go context
func (s ServerStreamWrapper) Context() context.Context {
	return s.ctx
}

// SetHeader sets a header in 's's gGRPC ServerStream
func (s ServerStreamWrapper) SetHeader(md metadata.MD) error {
	return s.stream.SetHeader(md)
}

// SendHeader sends a header through 's's gGRPC ServerStream
func (s ServerStreamWrapper) SendHeader(md metadata.MD) error {
	return s.stream.SendHeader(md)
}

// SetTrailer sets a trailiner in 's's gGRPC ServerStream
func (s ServerStreamWrapper) SetTrailer(md metadata.MD) {
	s.stream.SetTrailer(md)
}

// SendMsg sends a message through 's's gGRPC ServerStream
func (s ServerStreamWrapper) SendMsg(m interface{}) error {
	return s.stream.SendMsg(m)
}

// RecvMsg receives a message from 's's gGRPC ServerStream
func (s ServerStreamWrapper) RecvMsg(m interface{}) error {
	return s.stream.RecvMsg(m)
}

// Interceptor checks the authentication metadata in unary and streaming RPCs
// and prevents unknown or unauthorized calls.
type Interceptor struct {
	env serviceenv.ServiceEnv
}

// InterceptUnary applies authentication rules to unary RPCs
func (i *Interceptor) InterceptUnary(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	pachClient := i.env.GetPachClient(ctx)
	a, ok := authHandlers[info.FullMethod]
	if !ok {
		logrus.Errorf("no auth function for %q\n", info.FullMethod)
		return nil, fmt.Errorf("no auth function for %q, this is a bug", info.FullMethod)
	}

	username, err := a(pachClient, info.FullMethod)

	if err != nil {
		logrus.WithError(err).Errorf("denied unary call %q to user %v\n", info.FullMethod, nameOrUnauthenticated(username))
		return nil, err
	}

	if username != "" {
		ctx = setWhoAmI(ctx, username)
	}

	return handler(ctx, req)
}

// InterceptStream applies authentication rules to streaming RPCs
func (i *Interceptor) InterceptStream(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	ctx := stream.Context()
	pachClient := i.env.GetPachClient(ctx)
	a, ok := authHandlers[info.FullMethod]
	if !ok {
		logrus.Errorf("no auth function for %q\n", info.FullMethod)
		return fmt.Errorf("no auth function for %q, this is a bug", info.FullMethod)
	}

	username, err := a(pachClient, info.FullMethod)

	if err != nil {
		logrus.WithError(err).Errorf("denied streaming call %q to user %v\n", info.FullMethod, nameOrUnauthenticated(username))
		return err
	}

	if username != "" {
		newCtx := setWhoAmI(ctx, username)
		stream = ServerStreamWrapper{stream, newCtx}
	}
	return handler(srv, stream)
}

func nameOrUnauthenticated(name string) string {
	if name == "" {
		return "unauthenticated"
	}
	return name
}

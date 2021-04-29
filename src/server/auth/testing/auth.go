package testing

import (
	"golang.org/x/net/context"

	"github.com/pachyderm/pachyderm/v2/src/auth"
	txnenv "github.com/pachyderm/pachyderm/v2/src/internal/transactionenv"
)

// InactiveAPIServer (in the auth/testing package) is an implementation of the
// pachyderm auth api that returns NotActivatedError for all requests. This is
// meant to be used with local PFS and PPS servers for testing, and should
// never be used in a real Pachyderm cluster
type InactiveAPIServer struct{}

// Activate implements the Activate RPC, but just returns NotActivatedError
func (a *InactiveAPIServer) Activate(context.Context, *auth.ActivateRequest) (*auth.ActivateResponse, error) {
	return nil, auth.ErrNotActivated
}

// Deactivate implements the Deactivate RPC, but just returns NotActivatedError
func (a *InactiveAPIServer) Deactivate(context.Context, *auth.DeactivateRequest) (*auth.DeactivateResponse, error) {
	return nil, auth.ErrNotActivated
}

// GetRoleBinding implements the GetRoleBinding RPC, but just returns NotActivatedError
func (a *InactiveAPIServer) GetRoleBinding(context.Context, *auth.GetRoleBindingRequest) (*auth.GetRoleBindingResponse, error) {
	return nil, auth.ErrNotActivated
}

// ModifyRoleBinding implements the ModifyRoleBinding RPC, but just returns NotActivatedError
func (a *InactiveAPIServer) ModifyRoleBinding(context.Context, *auth.ModifyRoleBindingRequest) (*auth.ModifyRoleBindingResponse, error) {
	return nil, auth.ErrNotActivated
}

// GetRoleBindingInTransaction implements the GetRoleBinding RPC, but just returns NotActivatedError
func (a *InactiveAPIServer) GetRoleBindingInTransaction(*txnenv.TransactionContext, *auth.GetRoleBindingRequest) (*auth.GetRoleBindingResponse, error) {
	return nil, auth.ErrNotActivated
}

// ModifyRoleBindingInTransaction implements the ModifyRoleBinding RPC, but just returns NotActivatedError
func (a *InactiveAPIServer) ModifyRoleBindingInTransaction(*txnenv.TransactionContext, *auth.ModifyRoleBindingRequest) (*auth.ModifyRoleBindingResponse, error) {
	return nil, auth.ErrNotActivated
}

// AddPipelineReaderToRepoInTransaction implements the AddPipelineReaderToRepoInTransaction internal API
func (a *InactiveAPIServer) AddPipelineReaderToRepoInTransaction(txnCtx *txnenv.TransactionContext, sourceRepo, pipeline string) error {
	return auth.ErrNotActivated
}

// AddPipelineWriterToRepoInTransaction implements the AddPipelineWriterToRepoInTransaction internal API
func (a *InactiveAPIServer) AddPipelineWriterToRepoInTransaction(txnCtx *txnenv.TransactionContext, pipeline string) error {
	return auth.ErrNotActivated
}

// RemovePipelineReaderFromRepoInTransaction implements the RemovePipelineReaderFromRepoInTransaction internal API
func (a *InactiveAPIServer) RemovePipelineReaderFromRepoInTransaction(txnCtx *txnenv.TransactionContext, sourceRepo, pipeline string) error {
	return auth.ErrNotActivated
}

// CreateRoleBindingInTransaction implements the CreateRoleBinding RPC, but just returns NotActivatedError
func (a *InactiveAPIServer) CreateRoleBindingInTransaction(*txnenv.TransactionContext, string, []string, *auth.Resource) error {
	return auth.ErrNotActivated
}

// DeleteRoleBindingInTransaction implements the DeleteRoleBinding RPC, but just returns NotActivatedError
func (a *InactiveAPIServer) DeleteRoleBindingInTransaction(*txnenv.TransactionContext, *auth.Resource) error {
	return auth.ErrNotActivated
}

// Authenticate implements the Authenticate RPC, but just returns NotActivatedError
func (a *InactiveAPIServer) Authenticate(context.Context, *auth.AuthenticateRequest) (*auth.AuthenticateResponse, error) {
	return nil, auth.ErrNotActivated
}

// GetPermissions implements the GetPermissions RPC, but just returns NotActivatedError
func (a *InactiveAPIServer) GetPermissions(context.Context, *auth.GetPermissionsRequest) (*auth.GetPermissionsResponse, error) {
	return nil, auth.ErrNotActivated
}

// GetPermissionsForPrincipal implements the GetPermissions RPC, but just returns NotActivatedError
func (a *InactiveAPIServer) GetPermissionsForPrincipal(context.Context, *auth.GetPermissionsForPrincipalRequest) (*auth.GetPermissionsResponse, error) {
	return nil, auth.ErrNotActivated
}

// Authorize implements the Authorize RPC, but just returns NotActivatedError
func (a *InactiveAPIServer) Authorize(context.Context, *auth.AuthorizeRequest) (*auth.AuthorizeResponse, error) {
	return nil, auth.ErrNotActivated
}

// AuthorizeInTransaction is the same as the Authorize RPC but for use inside a
// running transaction.  It also returns a NotActivatedError.
func (a *InactiveAPIServer) AuthorizeInTransaction(*txnenv.TransactionContext, *auth.AuthorizeRequest) (*auth.AuthorizeResponse, error) {
	return nil, auth.ErrNotActivated
}

// WhoAmI implements the WhoAmI RPC, but just returns NotActivatedError
func (a *InactiveAPIServer) WhoAmI(context.Context, *auth.WhoAmIRequest) (*auth.WhoAmIResponse, error) {
	return nil, auth.ErrNotActivated
}

// GetRobotToken implements the GetRobotToken RPC, but just returns NotActivatedError
func (a *InactiveAPIServer) GetRobotToken(context.Context, *auth.GetRobotTokenRequest) (*auth.GetRobotTokenResponse, error) {
	return nil, auth.ErrNotActivated
}

// GetPipelineAuthTokenInTransaction is the same as GetAuthToken but for use inside a running transaction.
func (a *InactiveAPIServer) GetPipelineAuthTokenInTransaction(*txnenv.TransactionContext, string) (string, error) {
	return "", auth.ErrNotActivated
}

// GetOIDCLogin implements the GetOIDCLogin RPC, but just returns NotActivatedError
func (a *InactiveAPIServer) GetOIDCLogin(context.Context, *auth.GetOIDCLoginRequest) (*auth.GetOIDCLoginResponse, error) {
	return nil, auth.ErrNotActivated
}

// RevokeAuthToken implements the RevokeAuthToken RPC, but just returns NotActivatedError
func (a *InactiveAPIServer) RevokeAuthToken(context.Context, *auth.RevokeAuthTokenRequest) (*auth.RevokeAuthTokenResponse, error) {
	return nil, auth.ErrNotActivated
}

// RevokeAuthTokenInTransaction is the same as RevokeAuthToken but for use inside a running transaction
func (a *InactiveAPIServer) RevokeAuthTokenInTransaction(*txnenv.TransactionContext, *auth.RevokeAuthTokenRequest) (*auth.RevokeAuthTokenResponse, error) {
	return nil, auth.ErrNotActivated
}

// RevokeAuthTokensForUser implements the RevokeAuthTokensForUser RPC, but just returns NotActivatedError
func (a *InactiveAPIServer) RevokeAuthTokensForUser(context.Context, *auth.RevokeAuthTokensForUserRequest) (*auth.RevokeAuthTokensForUserResponse, error) {
	return nil, auth.ErrNotActivated
}

// SetGroupsForUser implements the SetGroupsForUser RPC, but just returns NotActivatedError
func (a *InactiveAPIServer) SetGroupsForUser(context.Context, *auth.SetGroupsForUserRequest) (*auth.SetGroupsForUserResponse, error) {
	return nil, auth.ErrNotActivated
}

// ModifyMembers implements the ModifyMembers RPC, but just returns NotActivatedError
func (a *InactiveAPIServer) ModifyMembers(context.Context, *auth.ModifyMembersRequest) (*auth.ModifyMembersResponse, error) {
	return nil, auth.ErrNotActivated
}

// GetGroups implements the GetGroups RPC, but just returns NotActivatedError
func (a *InactiveAPIServer) GetGroups(context.Context, *auth.GetGroupsRequest) (*auth.GetGroupsResponse, error) {
	return nil, auth.ErrNotActivated
}

// GetGroupsForPrincipal implements the GetGroups RPC, but just returns NotActivatedError
func (a *InactiveAPIServer) GetGroupsForPrincipal(context.Context, *auth.GetGroupsForPrincipalRequest) (*auth.GetGroupsResponse, error) {
	return nil, auth.ErrNotActivated
}

// GetUsers implements the GetUsers RPC, but just returns NotActivatedError
func (a *InactiveAPIServer) GetUsers(context.Context, *auth.GetUsersRequest) (*auth.GetUsersResponse, error) {
	return nil, auth.ErrNotActivated
}

// SetConfiguration implements the SetConfiguration RPC, but just returns NotActivatedError
func (a *InactiveAPIServer) SetConfiguration(context.Context, *auth.SetConfigurationRequest) (*auth.SetConfigurationResponse, error) {
	return nil, auth.ErrNotActivated
}

// GetConfiguration implements the GetConfiguration RPC, but just returns NotActivatedError
func (a *InactiveAPIServer) GetConfiguration(context.Context, *auth.GetConfigurationRequest) (*auth.GetConfigurationResponse, error) {
	return nil, auth.ErrNotActivated
}

// ExtractAuthTokens implements the ExtractAuthTokens RPC, but just returns NotActivatedError
func (a *InactiveAPIServer) ExtractAuthTokens(context.Context, *auth.ExtractAuthTokensRequest) (*auth.ExtractAuthTokensResponse, error) {
	return nil, auth.ErrNotActivated
}

// RestoreAuthToken implements the RestoreAuthToken RPC, but just returns NotActivatedError
func (a *InactiveAPIServer) RestoreAuthToken(context.Context, *auth.RestoreAuthTokenRequest) (*auth.RestoreAuthTokenResponse, error) {
	return nil, auth.ErrNotActivated
}

// DeleteExpiredAuthTokens implements the DeleteExpiredAuthTokens RPC, but just returns NotActivatedError
func (a *InactiveAPIServer) DeleteExpiredAuthTokens(context.Context, *auth.DeleteExpiredAuthTokensRequest) (*auth.DeleteExpiredAuthTokensResponse, error) {
	return nil, auth.ErrNotActivated
}

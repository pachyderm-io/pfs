package auth

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strings"

	"github.com/pachyderm/pachyderm/v2/src/internal/errors"

	oidc "github.com/coreos/go-oidc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	// ContextTokenKey is the key of the auth token in an
	// authenticated context
	ContextTokenKey = "authn-token"

	// The following constants are Subject prefixes. These are prepended to
	// subject names in ACLs, group membership, and any other references to subjects
	// to indicate what type of Subject or Principal they are (every Pachyderm
	// Subject has a logical Principal with the same name).

	// UserPrefix indicates that this Subject is a Pachyderm user synced from an IDP.
	UserPrefix = "user:"

	// RobotPrefix indicates that this Subject is a Pachyderm robot user. Any
	// string (with this prefix) is a logical Pachyderm robot user.
	RobotPrefix = "robot:"

	// PipelinePrefix indicates that this Subject is a PPS pipeline. Any string
	// (with this prefix) is a logical PPS pipeline (even though the pipeline may
	// not exist).
	PipelinePrefix = "pipeline:"

	// PachPrefix indicates that this Subject is an internal Pachyderm user.
	PachPrefix = "pach:"

	// GroupPrefix indicates that this Subject is a group.
	GroupPrefix = "group:"

	// RootUser is the user created when auth is initialized. Only one token
	// can be created for this user (during auth activation) and they cannot
	// be removed from the set of cluster super-admins.
	RootUser = "pach:root"

	// PpsUser is a special, unrevokable cluster administrator account used by PPS
	// to create pipeline tokens, close commits, and do other necessary PPS work.
	// It's not possible to authenticate as ppsUser (pps reads the auth token for
	// this user directly from etcd).
	PpsUser = `pach:pps`

	// ClusterAdminRole is the role for cluster admins, who have full access to all APIs
	ClusterAdminRole = "clusterAdmin"

	// RepoOwnerRole is a role which grants access to read, write and modify the role bindings for a repo
	RepoOwnerRole = "repoOwner"

	// RepoWriterRole is a role which grants ability to both read from and write to a repo
	RepoWriterRole = "repoWriter"

	// RepoReaderRole is a role which grants ability to both read from a repo
	RepoReaderRole = "repoReader"

	// IDPAdminRole is a role which grants the ability to configure OIDC apps.
	OIDCAppAdminRole = "oidcAppAdmin"

	// IDPAdminRole is a role which grants the ability to configure identity providers.
	IDPAdminRole = "idpAdmin"

	// IdentityAdmin is a role which grants the ability to configure the identity service.
	IdentityAdminRole = "identityAdmin"

	// DebuggerRole is a role which grants the ability to produce debug dumps.
	DebuggerRole = "debugger"

	// RobotUserRole is a role which grants the ability to generate tokens for robot
	// users.
	RobotUserRole = "robotUser"

	// LicenseAdminRole is a role which grants the ability to register new
	// pachds with the license server, manage pachds and update the enterprise license.
	LicenseAdminRole = "licenseAdmin"

	// AllClusterUsersSubject is a subject which applies a role binding to all authenticated users
	AllClusterUsersSubject = "allClusterUsers"
)

var (
	// ErrNotActivated is returned by an Auth API if the Auth service
	// has not been activated.
	//
	// Note: This error message string is matched in the UI. If edited,
	// it also needs to be updated in the UI code
	ErrNotActivated = status.Error(codes.Unimplemented, "the auth service is not activated")

	// ErrAlreadyActivated is returned by Activate if the Auth service
	// is already activated.
	ErrAlreadyActivated = status.Error(codes.Unimplemented, "the auth service is already activated")

	// ErrNotSignedIn indicates that the caller isn't signed in
	//
	// Note: This error message string is matched in the UI. If edited,
	// it also needs to be updated in the UI code
	ErrNotSignedIn = status.Error(codes.Unauthenticated, "no authentication token (try logging in)")

	// ErrNoMetadata is returned by the Auth API if the caller sent a request
	// containing no auth token.
	ErrNoMetadata = status.Error(codes.Internal, "no authentication metadata (try logging in)")

	// ErrBadToken is returned by the Auth API if the caller's token is corrupted
	// or has expired.
	ErrBadToken = status.Error(codes.Unauthenticated, "provided auth token is corrupted or has expired (try logging in again)")

	// ErrExpiredToken is returned by the Auth API if a restored token expired in
	// the past.
	ErrExpiredToken = status.Error(codes.Internal, "token expiration is in the past")
)

// DefaultOIDCScopes defines a list of the default OIDC scopes
var DefaultOIDCScopes = []string{"email", "profile", "groups", oidc.ScopeOpenID}

// IsErrAlreadyActivated checks if an error is a ErrAlreadyActivated
func IsErrAlreadyActivated(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), status.Convert(ErrAlreadyActivated).Message())
}

// IsErrNotActivated checks if an error is a ErrNotActivated
func IsErrNotActivated(err error) bool {
	if err == nil {
		return false
	}
	// TODO(msteffen) This is unstructured because we have no way to propagate
	// structured errors across GRPC boundaries. Fix
	return strings.Contains(err.Error(), status.Convert(ErrNotActivated).Message())
}

// IsErrNotSignedIn returns true if 'err' is a ErrNotSignedIn
func IsErrNotSignedIn(err error) bool {
	if err == nil {
		return false
	}
	// TODO(msteffen) This is unstructured because we have no way to propagate
	// structured errors across GRPC boundaries. Fix
	return strings.Contains(err.Error(), status.Convert(ErrNotSignedIn).Message())
}

// IsErrNoMetadata returns true if 'err' is an ErrNoMetadata (uses string
// comparison to work across RPC boundaries)
func IsErrNoMetadata(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), status.Convert(ErrNoMetadata).Message())
}

// IsErrBadToken returns true if 'err' is a ErrBadToken
func IsErrBadToken(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), status.Convert(ErrBadToken).Message())
}

// IsErrExpiredToken returns true if 'err' is a ErrExpiredToken
func IsErrExpiredToken(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), status.Convert(ErrExpiredToken).Message())
}

const errNoRoleBindingMsg = "no role binding exists for"

// ErrNoRoleBinding is returned if no role binding exists for a resource.
type ErrNoRoleBinding struct {
	Resource Resource
}

func (e *ErrNoRoleBinding) Error() string {
	return fmt.Sprintf("%v %v %v", errNoRoleBindingMsg, e.Resource.Type, e.Resource.Name)
}

// IsErrNoRoleBinding checks if an error is a ErrNoRoleBinding
func IsErrNoRoleBinding(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), errNoRoleBindingMsg)
}

// ErrNotAuthorized is returned if the user is not authorized to perform
// a certain operation.
type ErrNotAuthorized struct {
	Subject string // subject trying to perform blocked operation -- always set

	Resource Resource     // Resource that the user is attempting to access
	Required []Permission // Caller needs 'Required'-level access to 'Resource'
}

// This error message string is matched in the UI. If edited,
// it also needs to be updated in the UI code
const errNotAuthorizedMsg = "not authorized to perform this operation"

func (e *ErrNotAuthorized) Error() string {
	return fmt.Sprintf("%v is %v - needs permissions %v on %v %v", e.Subject, errNotAuthorizedMsg, e.Required, e.Resource.Type, e.Resource.Name)
}

// IsErrNotAuthorized checks if an error is a ErrNotAuthorized
func IsErrNotAuthorized(err error) bool {
	if err == nil {
		return false
	}
	// TODO(msteffen) This is unstructured because we have no way to propagate
	// structured errors across GRPC boundaries. Fix
	return strings.Contains(err.Error(), errNotAuthorizedMsg)
}

// ErrInvalidPrincipal indicates that a an argument to e.g. GetScope,
// SetScope, or SetACL is invalid
type ErrInvalidPrincipal struct {
	Principal string
}

func (e *ErrInvalidPrincipal) Error() string {
	return fmt.Sprintf("invalid principal \"%s\"; must start with one of \"pipeline:\", \"github:\", or \"robot:\", or have no \":\"", e.Principal)
}

// IsErrInvalidPrincipal returns true if 'err' is an ErrInvalidPrincipal
func IsErrInvalidPrincipal(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "invalid principal \"") &&
		strings.Contains(err.Error(), "\"; must start with one of \"pipeline:\", \"github:\", or \"robot:\", or have no \":\"")
}

// ErrTooShortTTL is returned by the ExtendAuthToken if request.Token already
// has a TTL longer than request.TTL.
type ErrTooShortTTL struct {
	RequestTTL, ExistingTTL int64
}

const errTooShortTTLMsg = "provided TTL (%d) is shorter than token's existing TTL (%d)"

func (e ErrTooShortTTL) Error() string {
	return fmt.Sprintf(errTooShortTTLMsg, e.RequestTTL, e.ExistingTTL)
}

// IsErrTooShortTTL returns true if 'err' is a ErrTooShortTTL
func IsErrTooShortTTL(err error) bool {
	if err == nil {
		return false
	}
	errMsg := err.Error()
	return strings.Contains(errMsg, "provided TTL (") &&
		strings.Contains(errMsg, ") is shorter than token's existing TTL (") &&
		strings.Contains(errMsg, ")")
}

// HashToken converts a token to a cryptographic hash.
// We don't want to store tokens verbatim in the database, as then whoever
// that has access to the database has access to all tokens.
func HashToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return fmt.Sprintf("%x", sum)
}

// GetAuthToken extracts the auth token embedded in 'ctx', if there is one
func GetAuthToken(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", ErrNoMetadata
	}
	if len(md[ContextTokenKey]) > 1 {
		return "", errors.Errorf("multiple authentication token keys found in context")
	} else if len(md[ContextTokenKey]) == 0 {
		return "", ErrNotSignedIn
	}
	return md[ContextTokenKey][0], nil
}

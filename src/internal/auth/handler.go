package auth

import (
	"context"

	"github.com/pachyderm/pachyderm/v2/src/auth"
	"github.com/pachyderm/pachyderm/v2/src/client"
)

// an authHandler can optionally return a username string that will be cached in the request's context
type authHandler func(*client.APIClient, string) (string, error)

// ContextKey is a key that the auth interceptor uses to inject and retrieve
// values from the golang context attached to an RPC (currently just the
// identity of the caller, cached in the context under 'WhoAmI', so that each
// RPC only needs to determine the caller's identity once)
type ContextKey string

const whoAmIResultKey = ContextKey("WhoAmI")

// authDisabledOr wraps an authHandler and permits the RPC if authHandler succeeds or
// if auth is disabled on the cluster
func authDisabledOr(h authHandler) authHandler {
	return func(pachClient *client.APIClient, fullMethod string) (string, error) {
		username, err := h(pachClient, fullMethod)

		if auth.IsErrNotActivated(err) {
			return "", nil
		}
		return username, err
	}
}

// unauthenticated permits any RPC even if the user has no authentication token
func unauthenticated(pachClient *client.APIClient, fullMethod string) (string, error) {
	return "", nil
}

// authenticated permits an RPC if auth is fully enabled and the user is authenticated
func authenticated(pachClient *client.APIClient, fullMethod string) (string, error) {
	r, err := pachClient.WhoAmI(pachClient.Ctx(), &auth.WhoAmIRequest{})
	var username string
	if err == nil {
		username = r.Username
	}
	return username, err
}

// clusterPermissions permits an RPC if the user is authorized with the given permissions on the cluster
func clusterPermissions(permissions ...auth.Permission) authHandler {
	return func(pachClient *client.APIClient, fullMethod string) (string, error) {
		resp, err := pachClient.Authorize(pachClient.Ctx(), &auth.AuthorizeRequest{
			Resource:    &auth.Resource{Type: auth.ResourceType_CLUSTER},
			Permissions: permissions,
		})
		if err != nil {
			return "", err
		}

		if resp.Authorized {
			return "", nil
		}

		return "", &auth.ErrNotAuthorized{
			Subject:  resp.Principal,
			Resource: auth.Resource{Type: auth.ResourceType_CLUSTER},
			Required: permissions,
		}
	}
}

// GetWhoAmI retrieves the caller's identity from 'ctx' if it's cached there.
func GetWhoAmI(ctx context.Context) string {
	if v := ctx.Value(whoAmIResultKey); v != nil {
		return v.(string)
	}
	return ""
}

func setWhoAmI(ctx context.Context, username string) context.Context {
	return context.WithValue(ctx, whoAmIResultKey, username)
}

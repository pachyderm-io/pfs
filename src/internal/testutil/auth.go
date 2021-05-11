package testutil

import (
	"context"
	"strings"
	"testing"

	"github.com/pachyderm/pachyderm/v2/src/auth"
	"github.com/pachyderm/pachyderm/v2/src/client"
	"github.com/pachyderm/pachyderm/v2/src/internal/require"
	"github.com/pachyderm/pachyderm/v2/src/pfs"
	"github.com/pachyderm/pachyderm/v2/src/pps"
)

const (
	// RootToken is the hard-coded admin token used on all activated test clusters
	RootToken = "iamroot"
)

// ActivateAuth activates the auth service in the test cluster, if it isn't already enabled
func ActivateAuth(tb testing.TB) {
	tb.Helper()
	client := GetPachClient(tb)
	ActivateAuthWithClient(tb, client)
}

// ActivateAuthWithClient activates the auth service using the provided client.APIClient
func ActivateAuthWithClient(tb testing.TB, client *client.APIClient) {
	tb.Helper()
	client = client.WithCtx(context.Background())
	client.SetAuthToken(RootToken)

	ActivateEnterprise(tb, client)

	_, err := client.Activate(client.Ctx(),
		&auth.ActivateRequest{RootToken: RootToken},
	)
	if err != nil && !strings.HasSuffix(err.Error(), "already activated") {
		tb.Fatalf("could not activate auth service: %v", err.Error())
	}

	// Activate auth for PPS
	_, err = client.PfsAPIClient.ActivateAuth(client.Ctx(), &pfs.ActivateAuthRequest{})
	require.NoError(tb, err)
	_, err = client.PpsAPIClient.ActivateAuth(client.Ctx(), &pps.ActivateAuthRequest{})
	require.NoError(tb, err)
}

// GetAuthenticatedPachClient activates auth, if it is not activated, and returns
// an authenticated client for the specified subject.
func GetAuthenticatedPachClient(tb testing.TB, subject string) *client.APIClient {
	tb.Helper()
	ActivateAuth(tb)
	rootClient := GetUnauthenticatedPachClient(tb)
	rootClient.SetAuthToken(RootToken)
	if subject == auth.RootUser {
		return rootClient
	}
	token, err := rootClient.GetRobotToken(rootClient.Ctx(), &auth.GetRobotTokenRequest{Robot: subject})
	require.NoError(tb, err)
	client := GetUnauthenticatedPachClient(tb)
	client.SetAuthToken(token.Token)
	return client
}

// GetUnauthenticatedPachClient returns a copy of the testing pach client with no auth token
func GetUnauthenticatedPachClient(tb testing.TB) *client.APIClient {
	tb.Helper()
	client := GetPachClient(tb)
	client = client.WithCtx(context.Background())
	client.SetAuthToken("")
	return client
}

// DeleteAll deletes all data in the cluster. This includes deleting all auth
// tokens, so all pachyderm clients must be recreated after calling deleteAll()
// (it should generally be called at the beginning or end of tests, before any
// clients have been created or after they're done being used).
func DeleteAll(tb testing.TB) {
	tb.Helper()

	// Setting the auth token has no effect if auth is disabled. If it's enabled,
	// the root user must be an admin so this will always succeed (unless auth was
	// activated with an unknown root token).
	client := GetUnauthenticatedPachClient(tb)
	client.SetAuthToken(RootToken)
	require.NoError(tb, client.DeleteAll())
}

package testutil

import (
	"context"
	"strings"
	"testing"

	"github.com/pachyderm/pachyderm/src/client"
	"github.com/pachyderm/pachyderm/src/client/auth"
	"github.com/pachyderm/pachyderm/src/client/pkg/config"
	"github.com/pachyderm/pachyderm/src/client/pkg/errors"
	"github.com/pachyderm/pachyderm/src/client/pkg/require"
	"github.com/pachyderm/pachyderm/src/client/pps"
	"github.com/pachyderm/pachyderm/src/server/pkg/backoff"
)

const (
	// RootToken is the hard-coded admin token used on all activated test clusters
	RootToken = "iamroot"
)

// ActivateAuth activates the auth service in the test cluster, if it isn't already enabled
func ActivateAuth(tb testing.TB) {
	tb.Helper()
	client := GetPachClient(tb)

	require.NoError(tb, ActivateEnterprise(tb, client))

	_, err := client.Activate(client.Ctx(),
		&auth.ActivateRequest{RootToken: RootToken},
	)
	if err != nil && !strings.HasSuffix(err.Error(), "already activated") {
		tb.Fatalf("could not activate auth service: %v", err.Error())
	}
	config.WritePachTokenToConfig(RootToken)

	// Wait for the Pachyderm Auth system to activate
	require.NoError(tb, backoff.Retry(func() error {
		if isActive, err := client.IsAuthActive(); err != nil {
			return err
		} else if isActive {
			return nil
		}
		return errors.Errorf("auth not active yet")
	}, backoff.NewTestingBackOff()))

	// Activate auth for PPS
	client = client.WithCtx(context.Background())
	client.SetAuthToken(RootToken)
	_, err = client.ActivateAuth(client.Ctx(), &pps.ActivateAuthRequest{})
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
	token, err := rootClient.GetAuthToken(rootClient.Ctx(), &auth.GetAuthTokenRequest{Subject: subject})
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

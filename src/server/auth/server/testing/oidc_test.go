package server

import (
	"testing"

	"github.com/pachyderm/pachyderm/v2/src/auth"
	"github.com/pachyderm/pachyderm/v2/src/internal/require"
	tu "github.com/pachyderm/pachyderm/v2/src/internal/testutil"
	"github.com/pachyderm/pachyderm/v2/src/internal/testutil/multipach"
)

// TestOIDCAuthCodeFlow tests that we can configure an OIDC provider and do the
// auth code flow
func TestOIDCAuthCodeFlow(t *testing.T) {
	multipach.GetTestContext(t, true)
	tu.ConfigureOIDCProvider(t)

	testClient := tu.GetUnauthenticatedPachClient(t)
	loginInfo, err := testClient.GetOIDCLogin(testClient.Ctx(), &auth.GetOIDCLoginRequest{})
	require.NoError(t, err)

	tu.DoOAuthExchange(t, testClient, testClient, loginInfo.LoginURL)
	// Check that pachd recorded the response from the redirect
	authResp, err := testClient.Authenticate(testClient.Ctx(),
		&auth.AuthenticateRequest{OIDCState: loginInfo.State})
	require.NoError(t, err)
	testClient.SetAuthToken(authResp.PachToken)

	// Check that testClient authenticated as the right user
	whoAmIResp, err := testClient.WhoAmI(testClient.Ctx(), &auth.WhoAmIRequest{})
	require.NoError(t, err)
	require.Equal(t, user(tu.DexMockConnectorEmail), whoAmIResp.Username)

	tu.DeleteAll(t)
}

// TestOIDCTrustedApp tests using an ID token issued to another OIDC app to authenticate.
func TestOIDCTrustedApp(t *testing.T) {
	multipach.GetTestContext(t, true)
	tu.ConfigureOIDCProvider(t)
	testClient := tu.GetUnauthenticatedPachClient(t)

	token := tu.GetOIDCTokenForTrustedApp(t)

	// Use the id token from the previous OAuth flow with Pach
	authResp, err := testClient.Authenticate(testClient.Ctx(),
		&auth.AuthenticateRequest{IdToken: token})
	require.NoError(t, err)
	testClient.SetAuthToken(authResp.PachToken)

	// Check that testClient authenticated as the right user
	whoAmIResp, err := testClient.WhoAmI(testClient.Ctx(), &auth.WhoAmIRequest{})
	require.NoError(t, err)
	require.Equal(t, user(tu.DexMockConnectorEmail), whoAmIResp.Username)

	tu.DeleteAll(t)
}

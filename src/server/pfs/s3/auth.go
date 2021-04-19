package s3

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.com/pachyderm/pachyderm/src/client/auth"
	"github.com/pachyderm/pachyderm/src/client/pkg/errors"
)

func (c *controller) SecretKey(r *http.Request, accessKey string, region *string) (*string, error) {
	c.logger.Debugf("SecretKey: %+v", region)

	pc := c.env.GetPachClient(r.Context())
	pc.SetAuthToken(accessKey)

	// WhoAmI will simultaneously check that auth is enabled, and that the
	// user is who they say they are
	_, err := pc.WhoAmI(pc.Ctx(), &auth.WhoAmIRequest{})
	if err != nil {
		// Some S3 clients (like minio) require the use of authenticated
		// requests, so in the case that auth is not enabled on pachyderm,
		// just allow any access credentials.
		if auth.IsErrNotActivated(err) {
			vars := mux.Vars(r)
			vars["s3gAuth"] = "disabled"
			return &accessKey, nil
		}

		// Auth failed, return nil secret key, signifying that the auth failed
		return nil, nil
	}

	// Auth succeeded, return the access key as the secret key
	return &accessKey, nil
}

func (c *controller) CustomAuth(r *http.Request) (bool, error) {
	c.logger.Debug("CustomAuth")

	pc := c.env.GetPachClient(r.Context())
	active, err := pc.IsAuthActive()
	if err != nil {
		return false, errors.Wrapf(err, "could not check whether auth is active")
	}

	// Allow custom auth (including no auth headers being sent) only if
	// pachyderm auth is disabled
	return !active, nil
}

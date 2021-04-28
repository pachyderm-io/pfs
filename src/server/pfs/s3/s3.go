package s3

import (
	"fmt"
	stdlog "log"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/pachyderm/pachyderm/v2/src/client"
	"github.com/pachyderm/pachyderm/v2/src/internal/serviceenv"

	"github.com/pachyderm/s2"
	"github.com/sirupsen/logrus"
)

// ClientFactory is a function called by s3g to create request-scoped
// pachyderm clients
type ClientFactory = func() (*client.APIClient, error)

const (
	multipartRepo        = "_s3gateway_multipart_"
	maxAllowedParts      = 10000
	maxRequestBodyLength = 128 * 1024 * 1024 //128mb
	requestTimeout       = 10 * time.Second
	readBodyTimeout      = 5 * time.Second

	// The S3 storage class that all PFS content will be reported to be stored in
	globalStorageClass = "STANDARD"

	// The S3 location served back
	globalLocation = "PACHYDERM"
)

// The S3 user associated with all PFS content
var defaultUser = s2.User{ID: "00000000000000000000000000000000", DisplayName: "pachyderm"}

type controller struct {
	// app environment -- clients to other services, env vars, etc
	env serviceenv.ServiceEnv

	// custom logger
	logger *logrus.Entry

	// name of the PFS repo holding multipart content
	repo string

	// the maximum number of allowed parts that can be associated with any
	// given file
	maxAllowedParts int

	driver Driver
}

// requestPachClient uses serviceenv to construct a request-scoped pachyderm
// client
func (c *controller) requestClient(r *http.Request) (*client.APIClient, error) {
	pc := c.env.GetPachClient(r.Context())

	vars := mux.Vars(r)
	if vars["s3gAuth"] != "disabled" {
		accessKey := vars["authAccessKey"]
		if accessKey != "" {
			pc.SetAuthToken(accessKey)
		}
	}

	return pc, nil
}

// Server runs an HTTP server with an S3-like API for PFS. This allows you to
// use s3 clients to access PFS contents.
//
// `inputBuckets` specifies which buckets should be served, referencing
// specific commit IDs. If nil, all PFS branches will be served as separate
// buckets, of the form `<branch name>.<bucket name>`. Some s3 features are
// enabled when all PFS branches are served as well; e.g. we add support for
// some s3 versioning functionality.
//
// This returns an `http.Server` instance. It is the responsibility of the
// caller to start the returned server. It's possible for the caller to
// gracefully shutdown the server if desired; see the `http` package for details.
//
// Note: server errors are redirected to logrus' standard log writer. The log
// writer is never closed. This should not be a problem with logrus' default
// configuration, which just writes to stdio. But if the standard logger is
// overwritten (e.g. to write to a socket), it's possible for this to cause
// problems.
//
// Note: In `s3cmd`, you must set the access key and secret key, even though
// this API will ignore them - otherwise, you'll get an opaque config error:
// https://github.com/s3tools/s3cmd/issues/845#issuecomment-464885959
func Server(env serviceenv.ServiceEnv, driver Driver) (*http.Server, error) {
	logger := logrus.WithFields(logrus.Fields{
		"source": "s3gateway",
	})

	c := &controller{
		env:             env,
		logger:          logger,
		repo:            multipartRepo,
		maxAllowedParts: maxAllowedParts,
		driver:          driver,
	}

	s3Server := s2.NewS2(logger, maxRequestBodyLength, readBodyTimeout)
	s3Server.Auth = c
	s3Server.Service = c
	s3Server.Bucket = c
	s3Server.Object = c
	s3Server.Multipart = c
	router := s3Server.Router()

	server := &http.Server{
		Addr:         fmt.Sprintf(":%d", env.Config().S3GatewayPort),
		ReadTimeout:  requestTimeout,
		WriteTimeout: requestTimeout,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Log that a request was made
			logger.Infof("http request: %s %s", r.Method, r.RequestURI)
			router.ServeHTTP(w, r)
		}),
		// NOTE: this is not closed. If the standard logger gets customized, this will need to be fixed
		ErrorLog: stdlog.New(logger.Writer(), "", 0),
	}

	return server, nil
}

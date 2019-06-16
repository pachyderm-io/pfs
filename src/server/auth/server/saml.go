package server

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"
	"time"

	"github.com/crewjam/saml"
	logrus "github.com/sirupsen/logrus"

	"github.com/pachyderm/pachyderm/src/client/auth"
	"github.com/pachyderm/pachyderm/src/server/pkg/backoff"
	"github.com/pachyderm/pachyderm/src/server/pkg/httperr"
)

// canonicalConfig contains the values specified in an auth.AuthConfig proto
// message, but as structured Go types. This is populated and returned by
// validateConfig
type canonicalConfig struct {
	Version int64

	// Currently, both IDP and SAMLSvc must be set. They are separate structs to
	// separate ambiguous names (e.g. IDP.MetadataURL is the ID provider's
	// metadata URL, while SAMLSvc.MetadataURL is Pachyderm's metadata URL,
	// configured by the cluster admin.
	//
	// Later, there may be multiple IDPs, or IDP may be set to a non-SAML source
	// and SAMLSvc may not be set, but those aren't supported yet
	IDP struct {
		Name           string
		Description    string
		MetadataURL    *url.URL
		Metadata       *saml.EntityDescriptor
		GroupAttribute string
	}

	SAMLSvc struct {
		ACSURL          *url.URL
		MetadataURL     *url.URL
		DashURL         *url.URL
		SessionDuration time.Duration
	}
}

func (c *canonicalConfig) ToProto() (*auth.AuthConfig, error) {
	// ToProto may be called on an empty canonical config if the user is setting
	// an empty config (the empty AuthConfig proto will be validated and then
	// reverted to a proto before being written to etcd)
	if c.IsEmpty() {
		return &auth.AuthConfig{}, nil
	}

	// Non-empty config case
	metadataBytes, err := xml.MarshalIndent(c.IDP.Metadata, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("could not marshal ID provider metadata: %v", err)
	}
	samlIDP := &auth.IDProvider{
		Name:        c.IDP.Name,
		Description: c.IDP.Description,
		SAML: &auth.IDProvider_SAMLOptions{
			MetadataXML:    metadataBytes,
			GroupAttribute: c.IDP.GroupAttribute,
		},
	}
	if c.IDP.MetadataURL != nil {
		samlIDP.SAML.MetadataURL = c.IDP.MetadataURL.String()
	}

	result := &auth.AuthConfig{
		IDProviders: []*auth.IDProvider{samlIDP},
		SAMLServiceOptions: &auth.AuthConfig_SAMLServiceOptions{
			ACSURL:      c.SAMLSvc.ACSURL.String(),
			MetadataURL: c.SAMLSvc.MetadataURL.String(),
		},
	}
	if c.SAMLSvc.DashURL != nil {
		result.SAMLServiceOptions.DashURL = c.SAMLSvc.DashURL.String()
	}
	if c.SAMLSvc.SessionDuration > 0 {
		result.SAMLServiceOptions.SessionDuration = c.SAMLSvc.SessionDuration.String()
	}
	return result, nil
}

func (c *canonicalConfig) IsEmpty() bool {
	return c == nil || c.IDP.Name == ""
}

// fetchRawIDPMetadata is a helper of validateConfig, below. It takes the URL
// of a SAML IdP's Metadata service, queries it, parses the result, and returns
// it as a struct the crewjam/saml library can use.
// This code is heavily based on the crewjam/saml/samlsp.Middleware constructor
func fetchRawIDPMetadata(name string, mdURL *url.URL) ([]byte, error) {
	c := http.DefaultClient
	req, err := http.NewRequest("GET", mdURL.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("could not retrieve IdP metadata for %q: %v", name, err)
	}
	req.Header.Set("User-Agent", "Golang; github.com/pachyderm/pachdyerm")

	var rawMetadata []byte
	b := backoff.NewInfiniteBackOff()
	b.MaxElapsedTime = 30 * time.Second
	b.MaxInterval = 2 * time.Second
	if err := backoff.RetryNotify(func() error {
		resp, err := c.Do(req)
		if err != nil {
			return err
		}

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("%d %s", resp.StatusCode, resp.Status)
		}
		rawMetadata, err = ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return fmt.Errorf("could not read IdP metadata response body: %v", err)
		}
		if len(rawMetadata) == 0 {
			return fmt.Errorf("empty metadata from IdP")
		}
		return nil
	}, b, func(err error, d time.Duration) error {
		logrus.Printf("error retrieving IdP metadata: %v; retrying in %v", err, d)
		return nil
	}); err != nil {
		return nil, err
	}

	// Successfully retrieved metadata
	return rawMetadata, nil
}

type configSource uint8

const (
	internal configSource = iota
	external
)

func validateIDP(idp *auth.IDProvider, src configSource, c *canonicalConfig) error {
	// Validate the ID Provider's name (must exist and must not be reserved)
	if idp.Name == "" {
		return errors.New("All ID providers must have a name specified (for use " +
			"during authorization)")
	}
	// TODO(msteffen): make sure we don't have to extend this every time we add
	// a new built-in backend.
	switch idp.Name + ":" {
	case auth.GitHubPrefix:
		return errors.New("cannot configure auth backend with reserved prefix " +
			auth.GitHubPrefix)
	case auth.RobotPrefix:
		return errors.New("cannot configure auth backend with reserved prefix " +
			auth.RobotPrefix)
	case auth.PipelinePrefix:
		return errors.New("cannot configure auth backend with reserved prefix " +
			auth.PipelinePrefix)
	}

	// Check if the IDP is a known type (right now the only type of IDP is SAML)
	if idp.SAML == nil {
		// render ID provider as json for error message
		idpConfigAsJSON, err := json.MarshalIndent(idp, "", "  ")
		idpConfigMsg := string(idpConfigAsJSON)
		if err != nil {
			idpConfigMsg = fmt.Sprintf("(could not marshal config json: %v)", err)
		}
		return fmt.Errorf("ID provider has unrecognized type: %v", idpConfigMsg)
	}

	// confirm that there is only one SAML IDP (requirement for now)
	if c.IDP.Name != "" {
		return fmt.Errorf("two SAML providers found in config, %q and %q, but "+
			"only one is allowed", idp.Name, c.IDP.Name)
	}
	c.IDP.Name = idp.Name
	c.IDP.Description = idp.Description
	c.IDP.GroupAttribute = idp.SAML.GroupAttribute

	// construct this SAML ID provider's metadata. There are three valid cases:
	// 1. This is a user-provided config (i.e. it's coming from an RPC), and the
	//    IDP's metadata was set directly in the config
	// 2. This is a user-provided config, and the IDP's metadata was not set
	//    in the config, but the config contains a URL where the IDP metadata
	//    can be retrieved
	// 3. This is an internal config (it has already been validated by a pachd
	//    worker, and it's coming from etcd)
	// Any other case should be rejected with an error
	//
	// Either download raw IDP metadata from metadata URL or get it from cfg
	var rawIDPMetadata []byte
	if idp.SAML.MetadataURL == "" {
		if len(idp.SAML.MetadataXML) == 0 {
			return fmt.Errorf("must set either metadata_xml or metadata_url for the "+
				"SAML ID provider %q", idp.Name)
		}
		rawIDPMetadata = idp.SAML.MetadataXML
	} else {
		// Parse URL even if this is an internal cfg and IDPMetadata is already
		// set, so that GetConfig can return it
		var err error
		c.IDP.MetadataURL, err = url.Parse(idp.SAML.MetadataURL)
		if err != nil {
			return fmt.Errorf("Could not parse SAML IDP metadata URL (%q) to query "+
				"it: %v", idp.SAML.MetadataURL, err)
		} else if c.IDP.MetadataURL.Scheme == "" {
			return fmt.Errorf("SAML IDP metadata URL %q is invalid (no scheme)",
				idp.SAML.MetadataURL)
		}

		switch src {
		case external: // user-provided config
			if len(idp.SAML.MetadataXML) > 0 {
				return fmt.Errorf("cannot set both metadata_xml and metadata_url for "+
					"the SAML ID provider %q", idp.Name)
			}
			rawIDPMetadata, err = fetchRawIDPMetadata(idp.Name, c.IDP.MetadataURL)
			if err != nil {
				return err
			}

		case internal: // config from etcd
			if len(idp.SAML.MetadataXML) == 0 {
				return fmt.Errorf("internal error: the SAML ID provider %q was "+
					"persisted without IDP metadata", idp.Name)
			}
			rawIDPMetadata = idp.SAML.MetadataXML
		}
	}

	// Parse IDP metadata. This code is heavily based on the
	// crewjam/saml/samlsp.Middleware constructor
	c.IDP.Metadata = &saml.EntityDescriptor{}
	err := xml.Unmarshal(rawIDPMetadata, c.IDP.Metadata)
	if err != nil {
		// this comparison is ugly, but it is how the error is generated in
		// encoding/xml
		if err.Error() != "expected element type <EntityDescriptor> but have <EntitiesDescriptor>" {
			return fmt.Errorf("could not unmarshal EntityDescriptor from IDP metadata: %v", err)
		}
		// Search through <EntitiesDescriptor> & find IDP entity
		entities := &saml.EntitiesDescriptor{}
		if err := xml.Unmarshal(rawIDPMetadata, entities); err != nil {
			return fmt.Errorf("could not unmarshal EntitiesDescriptor from IDP metadata: %v", err)
		}
		for i, e := range entities.EntityDescriptors {
			if len(e.IDPSSODescriptors) > 0 {
				c.IDP.Metadata = &entities.EntityDescriptors[i]
				break
			}
		}
		// Make sure we found an IDP entity descriptor
		if len(c.IDP.Metadata.IDPSSODescriptors) == 0 {
			return fmt.Errorf("no entity found with IDPSSODescriptor")
		}
	}
	return nil
}

func validateConfig(config *auth.AuthConfig, src configSource) (*canonicalConfig, error) {
	if config == nil {
		config = &auth.AuthConfig{}
	}
	c := &canonicalConfig{
		Version: config.LiveConfigVersion,
	}
	var err error

	// Validate all ID providers (and fetch IDP metadata for all SAML ID
	// providers)
	for _, idp := range config.IDProviders {
		if err := validateIDP(idp, src, c); err != nil {
			return nil, err
		}
	}

	// Make sure saml_svc_options are set if using SAML
	if c.IDP.Name != "" && config.SAMLServiceOptions == nil {
		return nil, errors.New("must set saml_svc_options if a SAML ID provider has been configured")
	}

	// Validate saml_svc_options
	if config.SAMLServiceOptions != nil {
		if c.IDP.Name == "" {
			return nil, errors.New("cannot set saml_svc_options without configuring a SAML ID provider")
		}
		sso := config.SAMLServiceOptions
		// parse ACS URL
		if sso.ACSURL == "" {
			return nil, errors.New("invalid SAML service options: must set ACS URL")
		}
		if c.SAMLSvc.ACSURL, err = url.Parse(sso.ACSURL); err != nil {
			return nil, fmt.Errorf("could not parse SAML config ACS URL (%q): %v", sso.ACSURL, err)
		} else if c.SAMLSvc.ACSURL.Scheme == "" {
			return nil, fmt.Errorf("ACS URL %q is invalid (no scheme)", sso.ACSURL)
		}

		// parse Metadata URL
		if sso.MetadataURL == "" {
			return nil, errors.New("invalid SAML service options: must set Metadata URL")
		}
		if c.SAMLSvc.MetadataURL, err = url.Parse(sso.MetadataURL); err != nil {
			return nil, fmt.Errorf("could not parse SAML config metadata URL (%q): %v", sso.MetadataURL, err)
		} else if c.SAMLSvc.MetadataURL.Scheme == "" {
			return nil, fmt.Errorf("Metadata URL %q is invalid (no scheme)", sso.MetadataURL)
		}

		// parse Dash URL
		if sso.DashURL != "" {
			if c.SAMLSvc.DashURL, err = url.Parse(sso.DashURL); err != nil {
				return nil, fmt.Errorf("could not parse Pachyderm dashboard URL (%q): %v", sso.DashURL, err)
			} else if c.SAMLSvc.DashURL.Scheme == "" {
				return nil, fmt.Errorf("Pachyderm dashboard URL %q is invalid (no scheme)", sso.DashURL)
			}
		}

		// parse session duration
		if sso.SessionDuration != "" {
			if c.SAMLSvc.SessionDuration, err = time.ParseDuration(sso.SessionDuration); err != nil {
				return nil, fmt.Errorf("could not parse SAML-based session duration: %v", err)
			}
		}
	}

	return c, nil
}

// updateConfig validates 'config', and if it valides successfully, loads it
// into the apiServer's config cache. The caller should already hold a.configMu
// and a.samlSPMu (as this updates a.samlSP)
func (a *apiServer) updateConfig(config *auth.AuthConfig) error {
	if config == nil {
		config = &auth.AuthConfig{}
	}
	newConfig, err := validateConfig(config, internal)
	if err != nil {
		return err
	}

	// It's possible that 'config' is non-nil, but empty (this is the case when
	// users clear the config from the command line). Therefore, check
	// newConfig.IDP.Name != "" instead of config != nil.
	if newConfig.IDP.Name != "" {
		a.configCache = newConfig
		// construct SAML handler
		a.samlSP = &saml.ServiceProvider{
			Logger:      logrus.New(),
			IDPMetadata: a.configCache.IDP.Metadata,
			AcsURL:      *a.configCache.SAMLSvc.ACSURL,
			MetadataURL: *a.configCache.SAMLSvc.MetadataURL,

			// Not set:
			// Key: Private key for Pachyderm ACS. Unclear if needed
			// Certificate: Public key for Pachyderm ACS. Unclear if needed
			// ForceAuthn: (whether users need to re-authenticate with the IdP, even
			//             if they already have a session--leaving this false)
			// AuthnNameIDFormat: (format the ACS expects the AuthnName to be in)
			// MetadataValidDuration: (how long the SP endpoints are valid? Returned
			//                        by the Metadata service)
		}
		a.redirectAddress = a.configCache.SAMLSvc.DashURL // Set redirect address from config as well
	} else {
		a.configCache = nil
		a.samlSP = nil
		a.redirectAddress = nil
	}
	return nil
}

var defaultDashRedirectURL = &url.URL{
	Scheme: "http",
	Host:   "localhost:30080",
	// leading slash is for equality checks in tests (HTTP lib inserts it)
	Path: path.Join("/", "auth", "autologin"),
}

// handleSAMLResponseInternal is a helper function called by handleSAMLResponse
func (a *apiServer) handleSAMLResponseInternal(req *http.Request) (string, string, *httperr.HTTPError) {
	a.configMu.Lock()
	defer a.configMu.Unlock()
	a.samlSPMu.Lock()
	defer a.samlSPMu.Unlock()

	if a.configCache == nil {
		return "", "", httperr.New(http.StatusConflict, "auth has no active config (either never set or disabled)")

	}
	if a.samlSP == nil {
		return "", "", httperr.New(http.StatusConflict, "SAML ACS has not been configured or was disabled")
	}
	sp := a.samlSP

	if err := req.ParseForm(); err != nil {
		return "", "", httperr.New(http.StatusConflict, "Could not parse request form: %v", err)
	}
	// No possible request IDs b/c only IdP-initiated auth is implemented for now
	assertion, err := sp.ParseResponse(req, []string{""})
	if err != nil {
		errMsg := fmt.Sprintf("Error parsing SAML response: %v", err)
		if invalidRespErr, ok := err.(*saml.InvalidResponseError); ok {
			errMsg += "\n(" + invalidRespErr.PrivateErr.Error() + ")"
		}
		return "", "", httperr.New(http.StatusBadRequest, errMsg)
	}

	// Make sure all the fields we need are present (avoid segfault)
	switch {
	case assertion == nil:
		return "", "", httperr.New(http.StatusConflict, "Error parsing SAML response: assertion is nil")
	case assertion.Subject == nil:
		return "", "", httperr.New(http.StatusConflict, "Error parsing SAML response: assertion.Subject is nil")
	case assertion.Subject.NameID == nil:
		return "", "", httperr.New(http.StatusConflict, "Error parsing SAML response: assertion.Subject.NameID is nil")
	case assertion.Subject.NameID.Value == "":
		return "", "", httperr.New(http.StatusConflict, "Error parsing SAML response: assertion.Subject.NameID.Value is unset")
	}

	// User is successfully authenticated
	subject := fmt.Sprintf("%s:%s", a.configCache.IDP.Name, assertion.Subject.NameID.Value)

	// Get new OTP for user (exp. from config if set, or default session duration)
	expiration := time.Now().Add(time.Duration(defaultSAMLTTLSecs) * time.Second)
	if a.configCache.SAMLSvc.SessionDuration != 0 {
		expiration = time.Now().Add(a.configCache.SAMLSvc.SessionDuration)
	}
	authCode, err := a.getOneTimePassword(req.Context(), subject, expiration)
	if err != nil {
		return "", "", httperr.New(http.StatusInternalServerError, err.Error())
	}

	// Update group memberships
	if a.configCache.IDP.GroupAttribute != "" {
		for _, attribute := range assertion.AttributeStatements {
			for _, attr := range attribute.Attributes {
				if attr.Name != a.configCache.IDP.GroupAttribute {
					continue
				}

				// Collect groups specified in this attribute and record them
				var groups []string
				for _, v := range attr.Values {
					groups = append(groups, fmt.Sprintf("group/%s:%s", a.configCache.IDP.Name, v.Value))
				}
				if err := a.setGroupsForUserInternal(context.Background(), subject, groups); err != nil {
					return "", "", httperr.New(http.StatusInternalServerError, err.Error())
				}
			}
		}
	}

	return subject, authCode, nil
}

// handleSAMLResponse is the HTTP handler for Pachyderm's ACS, which receives
// signed SAML assertions from this cluster's SAML ID provider (if one is
// configured)
func (a *apiServer) handleSAMLResponse(w http.ResponseWriter, req *http.Request) {
	var subject, authCode string
	var err *httperr.HTTPError

	logRequest := "SAML login request"
	a.LogReq(logRequest)
	defer func(start time.Time) {
		if subject != "" {
			logRequest = fmt.Sprintf("SAML login request for %s", subject)
		}
		a.LogResp(logRequest, httperr.PrettyPrintCode(err), err, time.Since(start))
	}(time.Now())

	subject, authCode, err = a.handleSAMLResponseInternal(req)
	if err != nil {
		http.Error(w, err.Error(), err.Code())
		return
	}

	// Redirect caller back to dash with auth code
	u := *defaultDashRedirectURL
	if a.redirectAddress != nil {
		u = *a.redirectAddress
	}
	u.RawQuery = url.Values{"auth_code": []string{authCode}}.Encode()
	w.Header().Set("Location", u.String())
	w.WriteHeader(http.StatusFound) // Send redirect
}

func (a *apiServer) handleMetadata(w http.ResponseWriter, req *http.Request) {
	a.samlSPMu.Lock()
	defer a.samlSPMu.Unlock()
	buf, _ := xml.MarshalIndent(a.samlSP.Metadata(), "", "  ")
	w.Header().Set("Content-Type", "application/samlmetadata+xml")
	w.Write(buf)
	return
}

func (a *apiServer) serveSAML() {
	samlMux := http.NewServeMux()
	samlMux.HandleFunc("/saml/acs", a.handleSAMLResponse)
	samlMux.HandleFunc("/saml/metadata", a.handleMetadata)
	samlMux.HandleFunc("/*", func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	})
	http.ListenAndServe(fmt.Sprintf(":%d", SamlPort), samlMux)
}

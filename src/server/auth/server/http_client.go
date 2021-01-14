package server

import (
	"fmt"
	"net/http"
	"net/url"
)

// localhostIdentityServer is the URL we can reach the embedded identity server at
const localhostIdentityServer = "http://localhost:658/"

// RewriteRoundTripper replaces the expected hostname with a new hostname.
// If a scheme is specified it's also replaced.
type RewriteRoundTripper struct {
	Expected *url.URL
	Rewrite  *url.URL
}

// LocalhostRewriteClient returns an http.Client which replaces the host and scheme from `expected` with
func LocalhostRewriteClient(expected string) (*http.Client, error) {
	expectedURL, err := url.Parse(expected)
	if err != nil {
		return nil, fmt.Errorf("unable to parse URL %q: %w", expected, err)
	}

	rewriteURL, err := url.Parse(localhostIdentityServer)
	if err != nil {
		return nil, fmt.Errorf("unable to parse URL %q: %w", localhostIdentityServer, err)
	}

	if rewriteURL.Host == "" || rewriteURL.Scheme == "" {
		return nil, fmt.Errorf("invalid URL %q is missing host or scheme", err)
	}

	return &http.Client{
		Transport: RewriteRoundTripper{
			Expected: expectedURL,
			Rewrite:  rewriteURL,
		},
	}, nil
}

// RoundTrip fulfills the http RoundTripper interface
func (rt RewriteRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.URL.Host == rt.Expected.Host {
		req.URL.Host = rt.Rewrite.Host
		req.URL.Scheme = rt.Rewrite.Scheme
	}
	return http.DefaultTransport.RoundTrip(req)
}

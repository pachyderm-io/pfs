package client

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"path"
	"time"
)

// ** Why this is here **
// We use a stripped down version of the loki client as importing
// the main client locks us to old module deps like k8s.io/client-go
var (
	queryRangePath = "/loki/api/v1/query_range"
)

// Client holds configuration for the loki
type Client struct {
	Address string
}

// QueryRange queries Loki in a given time range
func (c *Client) QueryRange(ctx context.Context, queryStr string, limit int, start, end time.Time, direction string, step, interval time.Duration, quiet bool) (*QueryResponse, error) {
	params := newQueryStringBuilder()
	params.SetString("query", queryStr)
	params.SetInt32("limit", limit)
	params.SetInt("start", start.UnixNano())
	params.SetInt("end", end.UnixNano())
	params.SetString("direction", direction)

	// The step is optional, so we do set it only if provided,
	// otherwise we do leverage on the API defaults
	if step != 0 {
		params.SetFloat("step", step.Seconds())
	}

	if interval != 0 {
		params.SetFloat("interval", interval.Seconds())
	}

	return c.doQuery(ctx, queryRangePath, params.Encode(), quiet)
}

func (c *Client) doQuery(ctx context.Context, path string, query string, quiet bool) (*QueryResponse, error) {
	var err error
	var r QueryResponse

	if err = c.doRequest(ctx, path, query, quiet, &r); err != nil {
		return nil, err
	}

	return &r, nil
}

func (c *Client) doRequest(ctx context.Context, path, query string, quiet bool, out interface{}) error {
	us, err := buildURL(c.Address, path, query)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, "GET", us, nil)
	if err != nil {
		return err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	return json.NewDecoder(resp.Body).Decode(out)
}

func buildURL(u, p, q string) (string, error) {
	url, err := url.Parse(u)
	if err != nil {
		return "", err
	}
	url.Path = path.Join(url.Path, p)
	url.RawQuery = q
	return url.String(), nil
}

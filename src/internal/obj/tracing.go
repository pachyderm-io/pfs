package obj

import (
	"context"
	"fmt"
	"io"

	"github.com/pachyderm/pachyderm/v2/src/internal/tracing"
)

func prettyProvider(provider string) string {
	switch provider {
	case "s3", Amazon:
		return "Amazon"
	case "gcs", "gs", Google:
		return "Google"
	case "as", "wasb", Microsoft:
		return "Microsoft"
	case "local", Local:
		return "Local"
	case Minio:
		return "Minio"
	}
	return "Unknown"
}

// TracingObjClient wraps the given object client 'c', adding tracing to all calls made
// by the returned interface
func TracingObjClient(provider string, c Client) Client {
	return &tracingObjClient{c, prettyProvider(provider)}
}

var _ Client = &tracingObjClient{}

type tracingObjClient struct {
	Client
	provider string
}

// Writer implements the corresponding method in the Client interface
func (o *tracingObjClient) Put(ctx context.Context, name string, r io.Reader) (retErr error) {
	span, ctx := tracing.AddSpanToAnyExisting(ctx, "/"+o.provider+".Writer/Connect", "name", name)
	defer func() {
		tracing.FinishAnySpan(span, "err", retErr)
	}()
	return o.Client.Put(ctx, name, r)
}

// Reader implements the corresponding method in the Client interface
func (o *tracingObjClient) Reader(ctx context.Context, name string, offset uint64, size uint64, w io.Writer) (retErr error) {
	span, ctx := tracing.AddSpanToAnyExisting(ctx, "/"+o.provider+".Reader/Connect",
		"name", name,
		"offset", fmt.Sprintf("%d", offset),
		"size", fmt.Sprintf("%d", size))
	defer func() {
		tracing.FinishAnySpan(span, "err", retErr)
	}()
	return o.Client.Get(ctx, name, w)
}

// Delete implements the corresponding method in the Client interface
func (o *tracingObjClient) Delete(ctx context.Context, name string) (retErr error) {
	span, ctx := tracing.AddSpanToAnyExisting(ctx, "/"+o.provider+"/Delete",
		"name", name)
	defer func() {
		tracing.FinishAnySpan(span, "err", retErr)
	}()
	return o.Client.Delete(ctx, name)
}

// Walk implements the corresponding method in the Client interface
func (o *tracingObjClient) Walk(ctx context.Context, prefix string, fn func(name string) error) (retErr error) {
	span, ctx := tracing.AddSpanToAnyExisting(ctx, "/"+o.provider+"/Walk",
		"prefix", prefix)
	defer func() {
		tracing.FinishAnySpan(span, "err", retErr)
	}()
	return o.Client.Walk(ctx, prefix, fn)
}

// Exists implements the corresponding method in the Client interface
func (o *tracingObjClient) Exists(ctx context.Context, name string) (retVal bool, retErr error) {
	span, ctx := tracing.AddSpanToAnyExisting(ctx, "/"+o.provider+"/Exists",
		"name", name)
	defer func() {
		tracing.FinishAnySpan(span, "exists", retVal)
	}()
	defer tracing.FinishAnySpan(span)
	return o.Client.Exists(ctx, name)
}

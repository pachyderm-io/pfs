package load

import (
	"context"
	"errors"
	"io"
	"math/rand"
	"time"

	"github.com/pachyderm/pachyderm/v2/src/client"
	"modernc.org/mathutil"
)

// Client is the standard interface for a load testing client.
// TODO: This should become the client.Client interface when we put the standard pach client behind an interface that
// takes a context as the first parameter for each method.
type Client interface {
	WithModifyFileClient(ctx context.Context, repo, commit string, cb func(client.ModifyFile) error) error
	GetFileTar(ctx context.Context, repo, commit, path string) (io.Reader, error)
}

type pachClient struct {
	client *client.APIClient
}

func NewPachClient(client *client.APIClient) Client {
	return &pachClient{client: client}
}

func (pc *pachClient) WithModifyFileClient(ctx context.Context, repo, commit string, cb func(client.ModifyFile) error) error {
	return pc.client.WithCtx(ctx).WithModifyFileClient(repo, commit, cb)
}

func (pc *pachClient) GetFileTar(ctx context.Context, repo, commit, path string) (io.Reader, error) {
	return pc.client.WithCtx(ctx).GetFileTar(repo, commit, path)
}

// ThroughputSpec specifies the throughput that can be achieved for fuzz
// operations
type ThroughputSpec struct {
	// Limit specifies the maximum throughput of an operation using this
	// ThroughputSpec, in B/s.
	Limit int `yaml:"limit,omitempty"`

	// Prob is the probably that this ThroughputSpec is selected for any given
	// operation.
	Prob float64 `yaml:"prob,omitempty"`
}

type throughputLimitClient struct {
	Client
	spec *ThroughputSpec
}

func NewThroughputLimitClient(client Client, spec *ThroughputSpec) Client {
	return &throughputLimitClient{
		Client: client,
		spec:   spec,
	}
}

func (tlc *throughputLimitClient) WithModifyFileClient(ctx context.Context, repo, commit string, cb func(client.ModifyFile) error) error {
	return tlc.Client.WithModifyFileClient(ctx, repo, commit, func(mf client.ModifyFile) error {
		return cb(&throughputLimitModifyFileClient{
			ModifyFile: mf,
			spec:       tlc.spec,
		})
	})
}

type throughputLimitModifyFileClient struct {
	client.ModifyFile
	spec *ThroughputSpec
}

func (tlmfc *throughputLimitModifyFileClient) PutFile(path string, r io.Reader, opts ...client.PutFileOption) error {
	if shouldExecute(tlmfc.spec.Prob) {
		r = &throughputLimitReader{
			r:              r,
			bytesPerSecond: tlmfc.spec.Limit,
		}
	}
	return tlmfc.ModifyFile.PutFile(path, r, opts...)
}

type throughputLimitReader struct {
	r                               io.Reader
	bytesSinceSleep, bytesPerSecond int
}

func (tlr *throughputLimitReader) Read(data []byte) (int, error) {
	var bytesRead int
	for len(data) > 0 {
		size := mathutil.Min(len(data), tlr.bytesPerSecond-tlr.bytesSinceSleep)
		n, err := tlr.r.Read(data[:size])
		data = data[n:]
		bytesRead += n
		tlr.bytesSinceSleep += n
		if err != nil {
			return bytesRead, err
		}
		if tlr.bytesSinceSleep == tlr.bytesPerSecond {
			time.Sleep(time.Second)
			tlr.bytesSinceSleep = 0
		}
	}
	return bytesRead, nil
}

type CancelSpec struct {
	MaxTime time.Duration `yaml:"maxTime,omitempty"`
	Prob    float64       `yaml:"prob,omitempty"`
}

type cancelClient struct {
	Client
	spec *CancelSpec
}

func NewCancelClient(client Client, spec *CancelSpec) Client {
	return &cancelClient{
		Client: client,
		spec:   spec,
	}
}

func (cc *cancelClient) WithModifyFileClient(ctx context.Context, repo, commit string, cb func(client.ModifyFile) error) (retErr error) {
	if shouldExecute(cc.spec.Prob) {
		var cancel context.CancelFunc
		cancelCtx, cancel := context.WithCancel(ctx)
		defer func() {
			if errors.Is(cancelCtx.Err(), context.Canceled) {
				retErr = nil
			}
		}()
		// TODO: This leaks, refactor into an errgroup.
		go func() {
			<-time.After(time.Duration(int64(float64(int64(cc.spec.MaxTime)) * rand.Float64())))
			cancel()
		}()
		ctx = cancelCtx
	}
	return cc.Client.WithModifyFileClient(ctx, repo, commit, func(mf client.ModifyFile) error {
		return cb(mf)
	})
}

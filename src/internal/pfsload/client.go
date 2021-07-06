package pfsload

import (
	"context"
	"errors"
	"io"
	"math/rand"
	"time"

	"github.com/pachyderm/pachyderm/v2/src/client"
	"github.com/pachyderm/pachyderm/v2/src/pfs"
	"modernc.org/mathutil"
)

// Client is the standard interface for a load testing client.
// TODO: This should become the client.Client interface when we put the standard pach client behind an interface that
// takes a context as the first parameter for each method.
type Client interface {
	WithModifyFileClient(ctx context.Context, commit *pfs.Commit, cb func(client.ModifyFile) error) error
	GetFileTAR(ctx context.Context, commit *pfs.Commit, path string) (io.Reader, error)
	Ctx() context.Context
}

type pachClient struct {
	client *client.APIClient
}

func NewPachClient(client *client.APIClient) Client {
	return &pachClient{client: client}
}

func (pc *pachClient) WithModifyFileClient(ctx context.Context, commit *pfs.Commit, cb func(client.ModifyFile) error) error {
	return pc.client.WithCtx(ctx).WithModifyFileClient(commit, cb)
}

func (pc *pachClient) GetFileTAR(ctx context.Context, commit *pfs.Commit, path string) (io.Reader, error) {
	return pc.client.WithCtx(ctx).GetFileTAR(commit, path)
}

func (pc *pachClient) Ctx() context.Context {
	return pc.client.Ctx()
}

type ThroughputSpec struct {
	Limit int `yaml:"limit,omitempty"`
	Prob  int `yaml:"prob,omitempty"`
}

type throughputLimitClient struct {
	Client
	spec   *ThroughputSpec
	random *rand.Rand
}

func NewThroughputLimitClient(client Client, spec *ThroughputSpec, random *rand.Rand) (Client, error) {
	if err := validateProb(spec.Prob); err != nil {
		return nil, err
	}
	return &throughputLimitClient{
		Client: client,
		spec:   spec,
		random: random,
	}, nil
}

func (tlc *throughputLimitClient) WithModifyFileClient(ctx context.Context, commit *pfs.Commit, cb func(client.ModifyFile) error) error {
	return tlc.Client.WithModifyFileClient(ctx, commit, func(mf client.ModifyFile) error {
		return cb(&throughputLimitModifyFileClient{
			ModifyFile: mf,
			spec:       tlc.spec,
			random:     tlc.random,
		})
	})
}

type throughputLimitModifyFileClient struct {
	client.ModifyFile
	spec   *ThroughputSpec
	random *rand.Rand
}

func (tlmfc *throughputLimitModifyFileClient) PutFile(path string, r io.Reader, opts ...client.PutFileOption) error {
	if shouldExecute(tlmfc.random, tlmfc.spec.Prob) {
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
	Prob    int           `yaml:"prob,omitempty"`
}

type cancelClient struct {
	Client
	spec   *CancelSpec
	random *rand.Rand
}

func NewCancelClient(client Client, spec *CancelSpec, random *rand.Rand) (Client, error) {
	if err := validateProb(spec.Prob); err != nil {
		return nil, err
	}
	return &cancelClient{
		Client: client,
		spec:   spec,
		random: random,
	}, nil
}

func (cc *cancelClient) WithModifyFileClient(ctx context.Context, commit *pfs.Commit, cb func(client.ModifyFile) error) (retErr error) {
	if shouldExecute(cc.random, cc.spec.Prob) {
		var cancel context.CancelFunc
		cancelCtx, cancel := context.WithCancel(ctx)
		defer func() {
			if errors.Is(cancelCtx.Err(), context.Canceled) {
				retErr = nil
			}
		}()
		// TODO: This leaks, refactor into an errgroup.
		go func() {
			<-time.After(time.Duration(int64(float64(int64(cc.spec.MaxTime)) * cc.random.Float64())))
			cancel()
		}()
		ctx = cancelCtx
	}
	return cc.Client.WithModifyFileClient(ctx, commit, func(mf client.ModifyFile) error {
		return cb(mf)
	})
}

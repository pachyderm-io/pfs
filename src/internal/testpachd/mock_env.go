package testpachd

import (
	"context"

	"github.com/pachyderm/pachyderm/v2/src/client"
	"github.com/pachyderm/pachyderm/v2/src/internal/testetcd"

	"golang.org/x/sync/errgroup"
)

// MockEnv contains the basic setup for running end-to-end pachyderm tests
// entirely locally within the test process. It provides a temporary directory
// for storing data, an embedded etcd server with a connected client, as well as
// a local mock pachd instance which allows a test to hook into any pachd calls.
type MockEnv struct {
	testetcd.Env
	mockPachd  *MockPachd
	pachClient *client.APIClient
}

func (e *MockEnv) GetPachClient(ctx context.Context) *client.APIClient {
	return e.pachClient.WithCtx(ctx)
}

// WithMockEnv sets up a MockEnv structure, passes it to the provided callback,
// then cleans up everything in the environment, regardless of if an assertion
// fails.
func WithMockEnv(cb func(*MockEnv) error) error {
	return testetcd.WithEnv(func(etcdEnv *testetcd.Env) (err error) {
		// Use an error group with a cancelable context to supervise every component
		// and cancel everything if one fails
		ctx, cancel := context.WithCancel(etcdEnv.Context)
		defer cancel()
		eg, ctx := errgroup.WithContext(ctx)

		mockEnv := &MockEnv{Env: *etcdEnv}
		mockEnv.Context = ctx

		// Cleanup any state when we return
		defer func() {
			saveErr := func(e error) error {
				if e != nil && err == nil {
					err = e
				}
				return e
			}

			if mockEnv.pachClient != nil {
				saveErr(mockEnv.pachClient.Close())
			}

			if mockEnv.mockPachd != nil {
				saveErr(mockEnv.mockPachd.Close())
			}

			cancel()
			saveErr(eg.Wait())
		}()

		mockEnv.mockPachd, err = NewMockPachd(mockEnv.Context)
		if err != nil {
			return err
		}

		eg.Go(func() error {
			return errorWait(ctx, mockEnv.mockPachd.Err())
		})
		mockEnv.pachClient, err = client.NewFromAddress(mockEnv.mockPachd.Addr.String())
		if err != nil {
			return err
		}

		// TODO: supervise the PachClient connection and error the errgroup if they
		// go down

		return cb(mockEnv)
	})
}

func errorWait(ctx context.Context, errChan <-chan error) error {
	select {
	case <-ctx.Done():
		return nil
	case err := <-errChan:
		return err
	}
}

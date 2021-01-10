package server

import (
	"github.com/gogo/protobuf/types"
	"golang.org/x/net/context"

	"github.com/pachyderm/pachyderm/src/client/pkg/errors"
	"github.com/pachyderm/pachyderm/src/client/pkg/grpcutil"
	"github.com/pachyderm/pachyderm/src/client/pps"
	"github.com/pachyderm/pachyderm/src/server/pkg/hashtree"
	"github.com/pachyderm/pachyderm/src/server/worker/driver"
	"github.com/pachyderm/pachyderm/src/server/worker/logs"
)

// WorkerInterface is an interface for getting or canceling the
// currently-running task in the worker process.
type WorkerInterface interface {
	GetStatus() (*pps.WorkerStatus, error)
	Cancel(jobID string, datumFilter []string) bool
}

// APIServer implements the worker API
type APIServer struct {
	driver          driver.Driver
	workerInterface WorkerInterface
	workerName      string // The k8s pod name of this worker
	logger          logs.TaggedLogger
}

// NewAPIServer creates an APIServer for a given pipeline
func NewAPIServer(driver driver.Driver, workerInterface WorkerInterface, workerName string) *APIServer {
	return &APIServer{
		driver:          driver,
		workerInterface: workerInterface,
		workerName:      workerName,
		logger:          logs.NewStatlessLogger(driver.PipelineInfo()),
	}
}

// Status returns the status of the current worker task.
func (a *APIServer) Status(ctx context.Context, _ *types.Empty) (*pps.WorkerStatus, error) {
	var status *pps.WorkerStatus
	if err := a.logger.LogStep("worker.API.Status", func() error {
		var err error
		status, err = a.workerInterface.GetStatus()
		if err != nil {
			return err
		}
		status.WorkerID = a.workerName
		return nil
	}); err != nil {
		return nil, err
	}
	return status, nil
}

// Cancel cancels the currently running datum
func (a *APIServer) Cancel(ctx context.Context, request *CancelRequest) (*CancelResponse, error) {
	var success bool
	if err := a.logger.LogStep("worker.API.Cancel", func() error {
		success = a.workerInterface.Cancel(request.JobID, request.DataFilters)
		return nil
	}); err != nil {
		return nil, err
	}
	return &CancelResponse{Success: success}, nil
}

// GetChunk returns the merged datum hashtrees of a particular chunk (if available)
func (a *APIServer) GetChunk(request *GetChunkRequest, server Worker_GetChunkServer) error {
	return a.logger.LogStep("worker.API.GetChunk", func() error {
		filter := hashtree.NewFilter(a.driver.NumShards(), request.Shard)
		if request.Stats {
			cache := a.driver.ChunkStatsCaches().GetCache(request.JobID)
			if cache != nil && cache.Has(request.ChunkID) {
				return cache.Get(request.ChunkID, grpcutil.NewStreamingBytesWriter(server), filter)
			}
		} else {
			cache := a.driver.ChunkCaches().GetCache(request.JobID)
			if cache != nil && cache.Has(request.ChunkID) {
				return cache.Get(request.ChunkID, grpcutil.NewStreamingBytesWriter(server), filter)
			}
		}
		return errors.New("hashtree chunk not found")
	})
}

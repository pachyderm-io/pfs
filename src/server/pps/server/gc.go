package server

import (
	"context"
	"time"

	"github.com/gogo/protobuf/types"
	ppsclient "github.com/pachyderm/pachyderm/src/client/pps"
	"github.com/pachyderm/pachyderm/src/server/pps/persist"

	log "github.com/Sirupsen/logrus"
	"k8s.io/kubernetes/pkg/api"
)

var (
	// DefaultGCPolicy is the default GC policy used by a pipeline if one is not
	// specified.
	DefaultGCPolicy = &ppsclient.GCPolicy{
		// a day
		Success: &types.Duration{
			Seconds: 24 * 60 * 60,
		},
		// a week
		Failure: &types.Duration{
			Seconds: 7 * 24 * 60 * 60,
		},
	}
)

func (a *apiServer) runGC(ctx context.Context, pipelineInfo *ppsclient.PipelineInfo) {
	dSuccess, _ := types.DurationFromProto(pipelineInfo.GcPolicy.Success)
	dFailure, _ := types.DurationFromProto(pipelineInfo.GcPolicy.Failure)
	successTick := time.Tick(dSuccess)
	failureTick := time.Tick(dFailure)
	// wait blocks until it's time to run GC again
	wait := func() {
		select {
		case <-successTick:
		case <-failureTick:
		}
	}
	for {
		client, err := a.getPersistClient()
		if err != nil {
			log.Errorf("error getting persist client: %s", err)
			wait()
			continue
		}
		jobIDs, err := client.ListGCJobs(ctx, &persist.ListGCJobsRequest{
			PipelineName: pipelineInfo.Pipeline.Name,
			GcPolicy:     pipelineInfo.GcPolicy,
		})
		if err != nil {
			log.Errorf("error listing jobs to GC: %s", err)
			wait()
			continue
		}
		for _, jobID := range jobIDs.Jobs {
			zero := int64(0)
			falseVal := false
			go func(jobID string) {
				if err := a.kubeClient.Extensions().Jobs(a.namespace).Delete(jobID, &api.DeleteOptions{
					GracePeriodSeconds: &zero,
					OrphanDependents:   &falseVal,
				}); err != nil {
					// TODO: if the error indicates that the job has already been
					// deleted, just proceed to the next step
					log.Errorf("error deleting kubernetes job %s: %s", jobID, err)
					return
				}
				if _, err := client.GCJob(ctx, &ppsclient.Job{jobID}); err != nil {
					log.Errorf("error marking job %s as GC-ed: %s", jobID, err)
				}
				return
			}(jobID)
		}
		wait()
	}
	panic("unreachable")
}

package worker

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"strings"
	"time"

	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/types"
	"github.com/montanaflynn/stats"

	"github.com/pachyderm/pachyderm/src/client"
	"github.com/pachyderm/pachyderm/src/client/pfs"
	"github.com/pachyderm/pachyderm/src/client/pkg/errutil"
	"github.com/pachyderm/pachyderm/src/client/pkg/pbutil"
	"github.com/pachyderm/pachyderm/src/client/pps"
	pfsserver "github.com/pachyderm/pachyderm/src/server/pfs"
	"github.com/pachyderm/pachyderm/src/server/pkg/backoff"
	col "github.com/pachyderm/pachyderm/src/server/pkg/collection"
	"github.com/pachyderm/pachyderm/src/server/pkg/dlock"
	"github.com/pachyderm/pachyderm/src/server/pkg/obj"
	"github.com/pachyderm/pachyderm/src/server/pkg/ppsconsts"
	"github.com/pachyderm/pachyderm/src/server/pkg/ppsutil"
	filesync "github.com/pachyderm/pachyderm/src/server/pkg/sync"
	pfs_sync "github.com/pachyderm/pachyderm/src/server/pkg/sync"
	"github.com/pachyderm/pachyderm/src/server/pkg/watch"
)

const (
	// maximumRetriesPerDatum is the maximum number of times each datum
	// can failed to be processed before we declare that the job has failed.
	maximumRetriesPerDatum = 3

	masterLockPath = "_master_worker_lock"

	// The number of datums the master caches
	numCachedDatums = 1000000

	ttl = int64(30)
)

func (a *APIServer) getMasterLogger() *taggedLogger {
	result := &taggedLogger{
		template:  a.logMsgTemplate, // Copy struct
		stderrLog: log.Logger{},
		marshaler: &jsonpb.Marshaler{},
	}
	result.stderrLog.SetOutput(os.Stderr)
	result.stderrLog.SetFlags(log.LstdFlags | log.Llongfile) // Log file/line
	result.template.Master = true
	return result
}

func (a *APIServer) getWorkerLogger() *taggedLogger {
	result := &taggedLogger{
		template:  a.logMsgTemplate, // Copy struct
		stderrLog: log.Logger{},
		marshaler: &jsonpb.Marshaler{},
	}
	result.stderrLog.SetOutput(os.Stderr)
	result.stderrLog.SetFlags(log.LstdFlags | log.Llongfile) // Log file/line
	return result
}

func (logger *taggedLogger) jobLogger(jobID string) *taggedLogger {
	result := logger.clone()
	result.template.JobID = jobID
	return result
}

func (a *APIServer) master(masterType string, spawner func(*client.APIClient) error) {
	masterLock := dlock.NewDLock(a.etcdClient, path.Join(a.etcdPrefix, masterLockPath, a.pipelineInfo.Pipeline.Name, a.pipelineInfo.Salt))
	logger := a.getMasterLogger()
	b := backoff.NewInfiniteBackOff()
	// Setting a high backoff so that when this master fails, the other
	// workers are more likely to become the master.
	// Also, we've observed race conditions where StopPipeline would cause
	// a master to restart before it's deleted.  PPS would then get confused
	// by the restart and create the workers again, because the restart would
	// bring the pipeline state from PAUSED to RUNNING.  By setting a high
	// retry interval, the master would be deleted before it gets a chance
	// to restart.
	b.InitialInterval = 10 * time.Second
	backoff.RetryNotify(func() error {
		// We use pachClient.Ctx here because it contains auth information.
		ctx, cancel := context.WithCancel(a.pachClient.Ctx())
		defer cancel() // make sure that everything this loop might spawn gets cleaned up
		ctx, err := masterLock.Lock(ctx)
		pachClient := a.pachClient.WithCtx(ctx)
		if err != nil {
			return err
		}
		defer masterLock.Unlock(ctx)
		logger.Logf("Launching %v master process", masterType)
		return spawner(pachClient)
	}, b, func(err error, d time.Duration) error {
		logger.Logf("master: error running the %v master process: %v; retrying in %v", masterType, err, d)
		return nil
	})
}

func (a *APIServer) jobSpawner(pachClient *client.APIClient) error {
	logger := a.getMasterLogger()
	// Listen for new commits, and create jobs when they arrive
	commitIter, err := pachClient.SubscribeCommit(a.pipelineInfo.Pipeline.Name, a.pipelineInfo.OutputBranch, "", pfs.CommitState_READY)
	if err != nil {
		return err
	}
	defer commitIter.Close()
	for {
		commitInfo, err := commitIter.Next()
		if err != nil {
			return err
		}
		if commitInfo.Finished != nil {
			continue
		}
		// Inspect the commit and check again if it has been finished (it may have
		// been closed since it was queued, e.g. by StopPipeline or StopJob)
		commitInfo, err = pachClient.InspectCommit(commitInfo.Commit.Repo.Name, commitInfo.Commit.ID)
		if err != nil {
			return err
		}
		if commitInfo.Finished != nil {
			continue // commit finished after queueing
		}

		// Check if a job was previously created for this commit. If not, make one
		var jobInfo *pps.JobInfo // job for commitInfo (new or old)
		jobInfos, err := pachClient.ListJob("", nil, commitInfo.Commit, -1, true)
		if err != nil {
			return err
		}
		if len(jobInfos) > 1 {
			return fmt.Errorf("multiple jobs found for commit: %s/%s", commitInfo.Commit.Repo.Name, commitInfo.Commit.ID)
		} else if len(jobInfos) < 1 {
			job, err := pachClient.CreateJob(a.pipelineInfo.Pipeline.Name, commitInfo.Commit)
			if err != nil {
				return err
			}
			logger.Logf("creating new job %q for output commit %q", job.ID, commitInfo.Commit.ID)
			// get jobInfo to look up spec commit, pipeline version, etc (if this
			// worker is stale and about to be killed, the new job may have a newer
			// pipeline version than the master. Or if the commit is stale, it may
			// have an older pipeline version than the master)
			jobInfo, err = pachClient.InspectJob(job.ID, false)
			if err != nil {
				return err
			}
		} else {
			// get latest job state
			jobInfo, err = pachClient.InspectJob(jobInfos[0].Job.ID, false)
			logger.Logf("found existing job %q for output commit %q", jobInfo.Job.ID, commitInfo.Commit.ID)
			if err != nil {
				return err
			}
		}

		switch {
		case ppsutil.IsTerminal(jobInfo.State):
			// ignore finished jobs (e.g. old pipeline & already killed)
			continue
		case jobInfo.PipelineVersion < a.pipelineInfo.Version:
			// kill unfinished jobs from old pipelines (should generally be cleaned
			// up by PPS master, but the PPS master can fail, and if these jobs
			// aren't killed, future jobs will hang indefinitely waiting for their
			// parents to finish)
			if err := a.updateJobState(pachClient.Ctx(), jobInfo, nil,
				pps.JobState_JOB_KILLED, "pipeline has been updated"); err != nil {
				return fmt.Errorf("could not kill stale job: %v", err)
			}
			continue
		case jobInfo.PipelineVersion > a.pipelineInfo.Version:
			return fmt.Errorf("job %s's version (%d) greater than pipeline's "+
				"version (%d), this should automatically resolve when the worker "+
				"is updated", jobInfo.Job.ID, jobInfo.PipelineVersion, a.pipelineInfo.Version)
			continue
		}

		// Now that the jobInfo is persisted, wait until all input commits are
		// ready, split the input datums into chunks and merge the results of
		// chunks as they're processed
		if err := a.waitJob(pachClient, jobInfo, logger); err != nil {
			return err
		}
	}
}

func (a *APIServer) spoutSpawner(pachClient *client.APIClient) error {
	ctx := pachClient.Ctx()

	var dir string

	logger, err := a.getTaggedLogger(pachClient, "spout", nil, false)
	puller := filesync.NewPuller()

	if err := a.unlinkData(nil); err != nil {
		return fmt.Errorf("unlinkData: %v", err)
	}
	// If this is our second time through the loop cleanup the old data.
	if dir != "" {
		if err := os.RemoveAll(dir); err != nil {
			return fmt.Errorf("os.RemoveAll: %v", err)
		}
	}
	dir, err = a.downloadData(pachClient, logger, nil, puller, &pps.ProcessStats{}, nil)
	if err != nil {
		return err
	}
	if err := a.linkData(nil, dir); err != nil {
		return fmt.Errorf("linkData: %v", err)
	}

	err = a.runService(ctx, logger)
	if err != nil {
		logger.Logf("error from runService: %+v", err)
	}
	return nil
}

func (a *APIServer) serviceSpawner(pachClient *client.APIClient) error {
	ctx := pachClient.Ctx()
	commitIter, err := pachClient.SubscribeCommit(a.pipelineInfo.Pipeline.Name, a.pipelineInfo.OutputBranch, "", pfs.CommitState_READY)
	if err != nil {
		return err
	}
	defer commitIter.Close()
	var serviceCtx context.Context
	var serviceCancel func()
	var dir string
	for {
		commitInfo, err := commitIter.Next()
		if err != nil {
			return err
		}
		if commitInfo.Finished != nil {
			continue
		}

		// Create a job document matching the service's output commit
		jobInput := ppsutil.JobInput(a.pipelineInfo, commitInfo)
		job, err := pachClient.CreateJob(a.pipelineInfo.Pipeline.Name, commitInfo.Commit)
		if err != nil {
			return err
		}
		df, err := NewDatumFactory(pachClient, jobInput)
		if err != nil {
			return err
		}
		if df.Len() != 1 {
			return fmt.Errorf("services must have a single datum")
		}
		data := df.Datum(0)
		logger, err := a.getTaggedLogger(pachClient, job.ID, data, false)
		puller := filesync.NewPuller()
		// If this is our second time through the loop cleanup the old data.
		if dir != "" {
			if err := a.unlinkData(data); err != nil {
				return fmt.Errorf("unlinkData: %v", err)
			}
			if err := os.RemoveAll(dir); err != nil {
				return fmt.Errorf("os.RemoveAll: %v", err)
			}
		}
		dir, err = a.downloadData(pachClient, logger, data, puller, &pps.ProcessStats{}, nil)
		if err != nil {
			return err
		}
		if err := os.MkdirAll(client.PPSInputPrefix, 0666); err != nil {
			return err
		}
		if err := a.linkData(data, dir); err != nil {
			return fmt.Errorf("linkData: %v", err)
		}
		if serviceCancel != nil {
			serviceCancel()
		}
		serviceCtx, serviceCancel = context.WithCancel(ctx)
		defer serviceCancel() // make go vet happy: infinite loop obviates 'defer'
		go func() {
			serviceCtx := serviceCtx
			if _, err := col.NewSTM(ctx, a.etcdClient, func(stm col.STM) error {
				jobs := a.jobs.ReadWrite(stm)
				jobPtr := &pps.EtcdJobInfo{}
				if err := jobs.Get(job.ID, jobPtr); err != nil {
					return err
				}
				return ppsutil.UpdateJobState(a.pipelines.ReadWrite(stm), a.jobs.ReadWrite(stm), jobPtr, pps.JobState_JOB_RUNNING, "")
			}); err != nil {
				logger.Logf("error updating job state: %+v", err)
			}
			err := a.runService(serviceCtx, logger)
			if err != nil {
				logger.Logf("error from runService: %+v", err)
			}
			select {
			case <-serviceCtx.Done():
				if _, err := col.NewSTM(ctx, a.etcdClient, func(stm col.STM) error {
					jobs := a.jobs.ReadWrite(stm)
					jobPtr := &pps.EtcdJobInfo{}
					if err := jobs.Get(job.ID, jobPtr); err != nil {
						return err
					}
					return ppsutil.UpdateJobState(a.pipelines.ReadWrite(stm), a.jobs.ReadWrite(stm), jobPtr, pps.JobState_JOB_SUCCESS, "")
				}); err != nil {
					logger.Logf("error updating job progress: %+v", err)
				}
				if err := pachClient.FinishCommit(commitInfo.Commit.Repo.Name, commitInfo.Commit.ID); err != nil {
					logger.Logf("could not finish output commit: %v", err)
				}
			default:
			}
		}()
	}
}

func plusDuration(x *types.Duration, y *types.Duration) (*types.Duration, error) {
	var xd time.Duration
	var yd time.Duration
	var err error
	if x != nil {
		xd, err = types.DurationFromProto(x)
		if err != nil {
			return nil, err
		}
	}
	if y != nil {
		yd, err = types.DurationFromProto(y)
		if err != nil {
			return nil, err
		}
	}
	return types.DurationProto(xd + yd), nil
}

func (a *APIServer) chunks(jobID string) col.Collection {
	return col.NewCollection(a.etcdClient, path.Join(a.etcdPrefix, chunkPrefix, jobID), nil, &ChunkState{}, nil, nil)
}

func (a *APIServer) merges(jobID string) col.Collection {
	return col.NewCollection(a.etcdClient, path.Join(a.etcdPrefix, mergePrefix, jobID), nil, &MergeState{}, nil, nil)
}

func newPlan(df DatumFactory, spec *pps.ChunkSpec, parallelism int, numHashtrees int64) *Plan {
	if spec == nil {
		spec = &pps.ChunkSpec{}
	}
	if spec.Number == 0 && spec.SizeBytes == 0 {
		spec.Number = int64(df.Len() / (parallelism * 10))
		if spec.Number == 0 {
			spec.Number = 1
		}
	}
	plan := &Plan{}
	if spec.Number != 0 {
		for i := spec.Number; i < int64(df.Len()); i += spec.Number {
			plan.Chunks = append(plan.Chunks, int64(i))
		}
	} else {
		size := int64(0)
		for i := 0; i < df.Len(); i++ {
			for _, input := range df.Datum(i) {
				size += int64(input.FileInfo.SizeBytes)
			}
			if size > spec.SizeBytes {
				plan.Chunks = append(plan.Chunks, int64(i))
			}
		}
	}
	plan.Chunks = append(plan.Chunks, int64(df.Len()))
	plan.Merges = numHashtrees
	return plan
}

func (a *APIServer) failedInputs(ctx context.Context, jobInfo *pps.JobInfo) ([]string, error) {
	var failedInputs []string
	var vistErr error
	blockCommit := func(name string, commit *pfs.Commit) {
		ci, err := a.pachClient.PfsAPIClient.InspectCommit(ctx,
			&pfs.InspectCommitRequest{
				Commit:     commit,
				BlockState: pfs.CommitState_FINISHED,
			})
		if err != nil && vistErr == nil {
			vistErr = fmt.Errorf("error blocking on commit %s/%s: %v",
				commit.Repo.Name, commit.ID, err)
			return
		}
		if ci.Tree == nil && ci.Trees == nil {
			failedInputs = append(failedInputs, name)
		}
	}
	pps.VisitInput(jobInfo.Input, func(input *pps.Input) {
		if input.Pfs != nil && input.Pfs.Commit != "" {
			blockCommit(input.Pfs.Name, client.NewCommit(input.Pfs.Repo, input.Pfs.Commit))
		}
		if input.Cron != nil && input.Cron.Commit != "" {
			blockCommit(input.Cron.Name, client.NewCommit(input.Cron.Repo, input.Cron.Commit))
		}
		if input.Git != nil && input.Git.Commit != "" {
			blockCommit(input.Git.Name, client.NewCommit(input.Git.Name, input.Git.Commit))
		}
	})
	return failedInputs, vistErr
}

// waitJob waits for the job in 'jobInfo' to finish, and then it collects the
// output from the job's workers and merges it into a commit (and may merge
// stats into a commit in the stats branch as well)
func (a *APIServer) waitJob(pachClient *client.APIClient, jobInfo *pps.JobInfo, logger *taggedLogger) (retErr error) {
	logger.Logf("waiting on job %q (pipeline version: %d, state: %s)", jobInfo.Job.ID, jobInfo.PipelineVersion, jobInfo.State)
	ctx, cancel := context.WithCancel(pachClient.Ctx())
	pachClient = pachClient.WithCtx(ctx)

	// Watch the output commit to see if it's terminated (KILLED, FAILED, or
	// SUCCESS) and if so, cancel the current context
	go func() {
		backoff.RetryNotify(func() error {
			commitInfo, err := pachClient.PfsAPIClient.InspectCommit(ctx,
				&pfs.InspectCommitRequest{
					Commit:     jobInfo.OutputCommit,
					BlockState: pfs.CommitState_FINISHED,
				})
			if err != nil {
				if pfsserver.IsCommitNotFoundErr(err) || pfsserver.IsCommitDeletedErr(err) {
					defer cancel() // whether we return error or nil, job is done
					// Output commit was deleted. Delete job as well
					if _, err := col.NewSTM(ctx, a.etcdClient, func(stm col.STM) error {
						// Delete the job if no other worker has deleted it yet
						jobPtr := &pps.EtcdJobInfo{}
						if err := a.jobs.ReadWrite(stm).Get(jobInfo.Job.ID, jobPtr); err != nil {
							return err
						}
						return a.deleteJob(stm, jobPtr)
					}); err != nil && !col.IsErrNotFound(err) {
						return err
					}
					return nil
				}
				return err
			}
			if commitInfo.Trees == nil {
				defer cancel() // whether job state update succeeds or not, job is done
				if _, err := col.NewSTM(ctx, a.etcdClient, func(stm col.STM) error {
					// Read an up to date version of the jobInfo so that we
					// don't overwrite changes that have happened since this
					// function started.
					jobPtr := &pps.EtcdJobInfo{}
					if err := a.jobs.ReadWrite(stm).Get(jobInfo.Job.ID, jobPtr); err != nil {
						return err
					}
					if !ppsutil.IsTerminal(jobPtr.State) {
						return ppsutil.UpdateJobState(a.pipelines.ReadWrite(stm), a.jobs.ReadWrite(stm), jobPtr, pps.JobState_JOB_KILLED, "")
					}
					return nil
				}); err != nil {
					return err
				}
			}
			return nil
		}, backoff.NewInfiniteBackOff(), func(err error, d time.Duration) error {
			if isDone(ctx) {
				return err // exit retry loop
			}
			return nil // retry again
		})
	}()
	if jobInfo.JobTimeout != nil {
		startTime, err := types.TimestampFromProto(jobInfo.Started)
		if err != nil {
			return err
		}
		timeout, err := types.DurationFromProto(jobInfo.JobTimeout)
		if err != nil {
			return err
		}
		afterTime := startTime.Add(timeout).Sub(time.Now())
		logger.Logf("cancelling job at: %+v", afterTime)
		timer := time.AfterFunc(afterTime, func() {
			if _, err := pachClient.PfsAPIClient.FinishCommit(ctx,
				&pfs.FinishCommitRequest{
					Commit: jobInfo.OutputCommit,
					Empty:  true,
				}); err != nil {
				logger.Logf("error from FinishCommit while timing out job: %+v", err)
			}
		})
		defer timer.Stop()
	}

	backoff.RetryNotify(func() (retErr error) {
		// block until job inputs are ready
		// TODO(bryce) This should be removed because it is no longer applicable.
		failedInputs, err := a.failedInputs(ctx, jobInfo)
		if err != nil {
			return err
		}
		if len(failedInputs) > 0 {
			reason := fmt.Sprintf("inputs %s failed", strings.Join(failedInputs, ", "))
			if err := a.updateJobState(ctx, jobInfo, nil, pps.JobState_JOB_FAILURE, reason); err != nil {
				return err
			}
			_, err := pachClient.PfsAPIClient.FinishCommit(ctx, &pfs.FinishCommitRequest{
				Commit: jobInfo.OutputCommit,
				Empty:  true,
			})
			return err
		}

		// Create a datum factory pointing at the job's inputs and split up the
		// input data into chunks
		df, err := NewDatumFactory(pachClient, jobInfo.Input)
		if err != nil {
			return err
		}
		parallelism, err := ppsutil.GetExpectedNumWorkers(a.kubeClient, a.pipelineInfo.ParallelismSpec)
		if err != nil {
			return fmt.Errorf("error from GetExpectedNumWorkers: %v", err)
		}
		numHashtrees, err := ppsutil.GetExpectedNumHashtrees(a.pipelineInfo.HashtreeSpec)
		if err != nil {
			return fmt.Errorf("error from GetExpectedNumHashtrees: %v", err)
		}
		plan := &Plan{}
		// Get stats commit
		var statsCommit *pfs.Commit
		var statsTrees []*pfs.Object
		var statsSize uint64
		if jobInfo.EnableStats {
			ci, err := pachClient.InspectCommit(jobInfo.OutputCommit.Repo.Name, jobInfo.OutputCommit.ID)
			if err != nil {
				return err
			}
			for _, commitRange := range ci.Subvenance {
				if commitRange.Lower.Repo.Name == jobInfo.OutputRepo.Name && commitRange.Upper.Repo.Name == jobInfo.OutputRepo.Name {
					statsCommit = commitRange.Lower
				}
			}
		}
		// Read the job document, and either resume (if we're recovering from a
		// crash) or mark it running. Also write the input chunks calculated above
		// into plansCol
		jobID := jobInfo.Job.ID
		if _, err := col.NewSTM(ctx, a.etcdClient, func(stm col.STM) error {
			jobs := a.jobs.ReadWrite(stm)
			jobPtr := &pps.EtcdJobInfo{}
			if err := jobs.Get(jobID, jobPtr); err != nil {
				return err
			}
			if jobPtr.State == pps.JobState_JOB_KILLED {
				return nil
			}
			jobPtr.DataTotal = int64(df.Len())
			jobPtr.StatsCommit = statsCommit
			if err := ppsutil.UpdateJobState(a.pipelines.ReadWrite(stm), a.jobs.ReadWrite(stm), jobPtr, pps.JobState_JOB_RUNNING, ""); err != nil {
				return err
			}
			plansCol := a.plans.ReadWrite(stm)
			if err := plansCol.Get(jobID, plan); err == nil {
				return nil
			}
			plan = newPlan(df, jobInfo.ChunkSpec, parallelism, numHashtrees)
			return plansCol.Put(jobID, plan)
		}); err != nil {
			return err
		}

		defer func() {
			if retErr == nil {
				if _, err := col.NewSTM(ctx, a.etcdClient, func(stm col.STM) error {
					chunksCol := a.chunks(jobID).ReadWrite(stm)
					chunksCol.DeleteAll()
					plansCol := a.plans.ReadWrite(stm)
					return plansCol.Delete(jobID)
				}); err != nil {
					retErr = err
				}
			}
		}()
		// Watch the chunks in order
		chunks := a.chunks(jobInfo.Job.ID).ReadOnly(ctx)
		var failedDatumID string
		for _, high := range plan.Chunks {
			chunkState := &ChunkState{}
			if err := chunks.WatchOneF(fmt.Sprint(high), func(e *watch.Event) error {
				var key string
				if err := e.Unmarshal(&key, chunkState); err != nil {
					return err
				}
				if key != fmt.Sprint(high) {
					return nil
				}
				if chunkState.State != State_RUNNING {
					if chunkState.State == State_FAILED {
						failedDatumID = chunkState.DatumID
					}
					return errutil.ErrBreak
				}
				return nil
			}); err != nil {
				return err
			}
		}
		if err := a.updateJobState(ctx, jobInfo, nil, pps.JobState_JOB_MERGING, ""); err != nil {
			return err
		}
		var trees []*pfs.Object
		var size uint64
		if failedDatumID == "" || jobInfo.EnableStats {
			// Wait for all merges to happen.
			merges := a.merges(jobInfo.Job.ID).ReadOnly(ctx)
			for merge := int64(0); merge < plan.Merges; merge++ {
				mergeState := &MergeState{}
				if err := merges.WatchOneF(fmt.Sprint(merge), func(e *watch.Event) error {
					var key string
					if err := e.Unmarshal(&key, mergeState); err != nil {
						return err
					}
					if key != fmt.Sprint(merge) {
						return nil
					}
					if mergeState.State != State_RUNNING {
						trees = append(trees, mergeState.Tree)
						size += mergeState.SizeBytes
						statsTrees = append(statsTrees, mergeState.StatsTree)
						statsSize += mergeState.StatsSizeBytes
						return errutil.ErrBreak
					}
					return nil
				}); err != nil {
					return err
				}
			}
		}
		if jobInfo.EnableStats {
			if _, err = pachClient.PfsAPIClient.FinishCommit(ctx, &pfs.FinishCommitRequest{
				Commit:    statsCommit,
				Trees:     statsTrees,
				SizeBytes: statsSize,
			}); err != nil {
				return err
			}
		}
		// If the job failed we finish the commit with an empty tree but only
		// after we've set the state, otherwise the job will be considered
		// killed.
		if failedDatumID != "" {
			reason := fmt.Sprintf("failed to process datum: %v", failedDatumID)
			if err := a.updateJobState(ctx, jobInfo, statsCommit, pps.JobState_JOB_FAILURE, reason); err != nil {
				return err
			}
			_, err = pachClient.PfsAPIClient.FinishCommit(ctx, &pfs.FinishCommitRequest{
				Commit: jobInfo.OutputCommit,
				Empty:  true,
			})
			return err
		}
		// Write out the datums processed/skipped and merged for this job
		buf := &bytes.Buffer{}
		pbw := pbutil.NewWriter(buf)
		for i := 0; i < df.Len(); i++ {
			files := df.Datum(i)
			datumHash := HashDatum(a.pipelineInfo.Pipeline.Name, a.pipelineInfo.Salt, files)
			if _, err := pbw.WriteBytes([]byte(datumHash)); err != nil {
				return err
			}
		}
		datums, _, err := pachClient.PutObject(buf)
		if err != nil {
			return err
		}
		// Finish the job's output commit
		_, err = pachClient.PfsAPIClient.FinishCommit(ctx, &pfs.FinishCommitRequest{
			Commit:    jobInfo.OutputCommit,
			Trees:     trees,
			SizeBytes: size,
			Datums:    datums,
		})
		if err != nil && !pfsserver.IsCommitFinishedErr(err) {
			if pfsserver.IsCommitNotFoundErr(err) || pfsserver.IsCommitDeletedErr(err) {
				// output commit was deleted during e.g. FinishCommit, which means this job
				// should be deleted. Goro from top of waitJob() will observe the deletion,
				// delete the jobPtr and call cancel()--wait for that.
				<-ctx.Done() // wait for cancel()
				return nil
			}
			return err
		}
		// Handle egress
		if err := a.egress(pachClient, logger, jobInfo); err != nil {
			reason := fmt.Sprintf("egress error: %v", err)
			return a.updateJobState(ctx, jobInfo, statsCommit, pps.JobState_JOB_FAILURE, reason)
		}
		return a.updateJobState(ctx, jobInfo, statsCommit, pps.JobState_JOB_SUCCESS, "")
	}, backoff.NewInfiniteBackOff(), func(err error, d time.Duration) error {
		logger.Logf("error in waitJob %v, retrying in %v", err, d)
		select {
		case <-ctx.Done():
			if err := ctx.Err(); err != nil {
				if err == context.DeadlineExceeded {
					reason := fmt.Sprintf("job exceeded timeout (%v)", jobInfo.JobTimeout)
					// Mark the job as failed.
					// Workers subscribe to etcd for this state change to cancel their work
					_, err := col.NewSTM(context.Background(), a.etcdClient, func(stm col.STM) error {
						jobs := a.jobs.ReadWrite(stm)
						jobID := jobInfo.Job.ID
						jobPtr := &pps.EtcdJobInfo{}
						if err := jobs.Get(jobID, jobPtr); err != nil {
							return err
						}
						err = ppsutil.UpdateJobState(a.pipelines.ReadWrite(stm), a.jobs.ReadWrite(stm), jobPtr, pps.JobState_JOB_FAILURE, reason)
						if err != nil {
							return nil
						}
						return nil
					})
					if err != nil {
						return err
					}
				}
				return err
			}
			return err
		default:
		}
		// Increment the job's restart count
		_, err = col.NewSTM(ctx, a.etcdClient, func(stm col.STM) error {
			jobs := a.jobs.ReadWrite(stm)
			jobID := jobInfo.Job.ID
			jobPtr := &pps.EtcdJobInfo{}
			if err := jobs.Get(jobID, jobPtr); err != nil {
				return err
			}
			jobPtr.Restart++
			return jobs.Put(jobID, jobPtr)
		})
		if err != nil {
			logger.Logf("error incrementing job %s's restart count", jobInfo.Job.ID)
		}
		return nil
	})
	return nil
}

func (a *APIServer) updateJobState(ctx context.Context, info *pps.JobInfo, stats *pfs.Commit, state pps.JobState, reason string) error {
	_, err := col.NewSTM(ctx, a.etcdClient, func(stm col.STM) error {
		jobs := a.jobs.ReadWrite(stm)
		jobID := info.Job.ID
		jobPtr := &pps.EtcdJobInfo{}
		if err := jobs.Get(jobID, jobPtr); err != nil {
			return err
		}
		if jobPtr.StatsCommit == nil {
			jobPtr.StatsCommit = stats
		}
		return ppsutil.UpdateJobState(a.pipelines.ReadWrite(stm), a.jobs.ReadWrite(stm), jobPtr, state, reason)
	})
	return err
}

// deleteJob is identical to updateJobState, except that jobPtr points to a job
// that should be deleted rather than marked failed. Jobs may be deleted if
// their output commit is deleted.
func (a *APIServer) deleteJob(stm col.STM, jobPtr *pps.EtcdJobInfo) error {
	pipelinePtr := &pps.EtcdPipelineInfo{}
	if err := a.pipelines.ReadWrite(stm).Update(jobPtr.Pipeline.Name, pipelinePtr, func() error {
		if pipelinePtr.JobCounts == nil {
			pipelinePtr.JobCounts = make(map[int32]int32)
		}
		if pipelinePtr.JobCounts[int32(jobPtr.State)] != 0 {
			pipelinePtr.JobCounts[int32(jobPtr.State)]--
		}
		return nil
	}); err != nil {
		return err
	}
	return a.jobs.ReadWrite(stm).Delete(jobPtr.Job.ID)
}

func (a *APIServer) egress(pachClient *client.APIClient, logger *taggedLogger, jobInfo *pps.JobInfo) error {
	// copy the pach client (preserving auth info) so we can set a different
	// number of concurrent streams
	pachClient = pachClient.WithCtx(pachClient.Ctx())
	pachClient.SetMaxConcurrentStreams(100)
	var egressFailureCount int
	return backoff.RetryNotify(func() (retErr error) {
		if jobInfo.Egress != nil {
			logger.Logf("Starting egress upload for job (%v)", jobInfo)
			start := time.Now()
			url, err := obj.ParseURL(jobInfo.Egress.URL)
			if err != nil {
				return err
			}
			objClient, err := obj.NewClientFromURLAndSecret(url, false)
			if err != nil {
				return err
			}
			if err := pfs_sync.PushObj(pachClient, jobInfo.OutputCommit, objClient, url.Object); err != nil {
				return err
			}
			logger.Logf("Completed egress upload for job (%v), duration (%v)", jobInfo, time.Since(start))
		}
		return nil
	}, backoff.NewInfiniteBackOff(), func(err error, d time.Duration) error {
		egressFailureCount++
		if egressFailureCount > 3 {
			return err
		}
		logger.Logf("egress failed: %v; retrying in %v", err, d)
		return nil
	})
}

func (a *APIServer) receiveSpout(ctx context.Context, logger *taggedLogger) error {
	return backoff.RetryNotify(func() error {
		repo := a.pipelineInfo.Pipeline.Name
		for {
			// this extra closure is so that we can scope the defer
			if err := func() (retErr error) {
				// open a read connection to the /pfs/out named pipe
				out, err := os.Open("/pfs/out")
				if err != nil {
					return err
				}
				// and close it at the end of each loop
				defer func() {
					if err := out.Close(); err != nil && retErr == nil {
						// this lets us pass the error through if Close fails
						retErr = err
					}
				}()
				outTar := tar.NewReader(out)

				// start commit
				commit, err := a.pachClient.PfsAPIClient.StartCommit(a.pachClient.Ctx(), &pfs.StartCommitRequest{
					Parent:     client.NewCommit(repo, ""),
					Branch:     a.pipelineInfo.OutputBranch,
					Provenance: []*pfs.CommitProvenance{client.NewCommitProvenance(ppsconsts.SpecRepo, a.pipelineInfo.OutputBranch, a.pipelineInfo.SpecCommit.ID)},
				})
				if err != nil {
					return err
				}

				// finish the commit even if there was an issue
				defer func() {
					if err := a.pachClient.FinishCommit(repo, commit.ID); err != nil && retErr == nil {
						// this lets us pass the error through if FinishCommit fails
						retErr = err
					}
				}()
				// this loops through all the files in the tar that we've read from /pfs/out
				for {
					fileHeader, err := outTar.Next()
					if err == io.EOF {
						break
					}
					if err != nil {
						return err
					}
					// put files into pachyderm
					if a.pipelineInfo.Spout.Overwrite {
						_, err = a.pachClient.PutFileOverwrite(repo, commit.ID, fileHeader.Name, outTar, 0)
						if err != nil {
							return err
						}
					} else {
						_, err = a.pachClient.PutFile(repo, commit.ID, fileHeader.Name, outTar)
						if err != nil {
							return err
						}
					}
				}
				return nil
			}(); err != nil {
				return err
			}
		}
	}, backoff.NewInfiniteBackOff(), func(err error, d time.Duration) error {
		select {
		case <-ctx.Done():
			return err
		default:
			logger.Logf("error running spout: %+v, retrying in: %+v", err, d)
			return nil
		}
	})
}

func (a *APIServer) runService(ctx context.Context, logger *taggedLogger) error {
	return backoff.RetryNotify(func() error {
		// if we have a spout, then asynchronously receive spout data
		if a.pipelineInfo.Spout != nil {
			go a.receiveSpout(ctx, logger)
		}
		return a.runUserCode(ctx, logger, nil, &pps.ProcessStats{}, nil)
	}, backoff.NewInfiniteBackOff(), func(err error, d time.Duration) error {
		select {
		case <-ctx.Done():
			return err
		default:
			logger.Logf("error running user code: %+v, retrying in: %+v", err, d)
			return nil
		}
	})
}

func (a *APIServer) aggregateProcessStats(stats []*pps.ProcessStats) (*pps.AggregateProcessStats, error) {
	var downloadTime []float64
	var processTime []float64
	var uploadTime []float64
	var downloadBytes []float64
	var uploadBytes []float64
	for _, s := range stats {
		dt, err := types.DurationFromProto(s.DownloadTime)
		if err != nil {
			return nil, err
		}
		downloadTime = append(downloadTime, float64(dt))
		pt, err := types.DurationFromProto(s.ProcessTime)
		if err != nil {
			return nil, err
		}
		processTime = append(processTime, float64(pt))
		ut, err := types.DurationFromProto(s.UploadTime)
		if err != nil {
			return nil, err
		}
		uploadTime = append(uploadTime, float64(ut))
		downloadBytes = append(downloadBytes, float64(s.DownloadBytes))
		uploadBytes = append(uploadBytes, float64(s.UploadBytes))

	}
	dtAgg, err := a.aggregate(downloadTime)
	if err != nil {
		return nil, err
	}
	ptAgg, err := a.aggregate(processTime)
	if err != nil {
		return nil, err
	}
	utAgg, err := a.aggregate(uploadTime)
	if err != nil {
		return nil, err
	}
	dbAgg, err := a.aggregate(downloadBytes)
	if err != nil {
		return nil, err
	}
	ubAgg, err := a.aggregate(uploadBytes)
	if err != nil {
		return nil, err
	}
	return &pps.AggregateProcessStats{
		DownloadTime:  dtAgg,
		ProcessTime:   ptAgg,
		UploadTime:    utAgg,
		DownloadBytes: dbAgg,
		UploadBytes:   ubAgg,
	}, nil
}

func (a *APIServer) aggregate(datums []float64) (*pps.Aggregate, error) {
	logger := a.getMasterLogger()
	mean, err := stats.Mean(datums)
	if err != nil {
		logger.Logf("error aggregating mean: %v", err)
	}
	stddev, err := stats.StandardDeviation(datums)
	if err != nil {
		logger.Logf("error aggregating std dev: %v", err)
	}
	fifth, err := stats.Percentile(datums, 5)
	if err != nil {
		logger.Logf("error aggregating 5th percentile: %v", err)
	}
	ninetyFifth, err := stats.Percentile(datums, 95)
	if err != nil {
		logger.Logf("error aggregating 95th percentile: %v", err)
	}
	return &pps.Aggregate{
		Count:                 int64(len(datums)),
		Mean:                  mean,
		Stddev:                stddev,
		FifthPercentile:       fifth,
		NinetyFifthPercentile: ninetyFifth,
	}, nil
}

func isNotFoundErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), "not found")
}

func now() *types.Timestamp {
	t, err := types.TimestampProto(time.Now())
	if err != nil {
		panic(err)
	}
	return t
}

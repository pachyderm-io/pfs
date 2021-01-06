package transform

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/gogo/protobuf/types"
	"github.com/pachyderm/pachyderm/src/client"
	"github.com/pachyderm/pachyderm/src/client/limit"
	"github.com/pachyderm/pachyderm/src/client/pfs"
	"github.com/pachyderm/pachyderm/src/client/pkg/errors"
	"github.com/pachyderm/pachyderm/src/client/pps"
	pfsserver "github.com/pachyderm/pachyderm/src/server/pfs"
	"github.com/pachyderm/pachyderm/src/server/pkg/backoff"
	col "github.com/pachyderm/pachyderm/src/server/pkg/collection"
	"github.com/pachyderm/pachyderm/src/server/pkg/errutil"
	"github.com/pachyderm/pachyderm/src/server/pkg/ppsutil"
	"github.com/pachyderm/pachyderm/src/server/pkg/storage/renew"
	"github.com/pachyderm/pachyderm/src/server/pkg/uuid"
	"github.com/pachyderm/pachyderm/src/server/pkg/work"
	ppsserver "github.com/pachyderm/pachyderm/src/server/pps"
	"github.com/pachyderm/pachyderm/src/server/worker/common"
	"github.com/pachyderm/pachyderm/src/server/worker/datum"
	"github.com/pachyderm/pachyderm/src/server/worker/driver"
	"github.com/pachyderm/pachyderm/src/server/worker/logs"
	"github.com/pachyderm/pachyderm/src/server/worker/pipeline/transform/chain"
	"golang.org/x/sync/errgroup"
)

type hasher struct {
	name string
	salt string
}

func (h *hasher) Hash(inputs []*common.Input) string {
	return common.HashDatum(h.name, h.salt, inputs)
}

type pendingJob struct {
	driver                     driver.Driver
	logger                     logs.TaggedLogger
	ji                         *pps.JobInfo
	commitInfo, metaCommitInfo *pfs.CommitInfo
	jdit                       *chain.JobDatumIterator
	taskMaster                 *work.Master
	cancel                     context.CancelFunc
}

func (pj *pendingJob) writeJobInfo() error {
	pj.logger.Logf("updating job info, state: %s", pj.ji.State)
	return writeJobInfo(pj.driver.PachClient(), pj.ji)
}

func writeJobInfo(pachClient *client.APIClient, jobInfo *pps.JobInfo) error {
	_, err := pachClient.PpsAPIClient.UpdateJobState(pachClient.Ctx(), &pps.UpdateJobStateRequest{
		Job:           jobInfo.Job,
		State:         jobInfo.State,
		Reason:        jobInfo.Reason,
		Restart:       jobInfo.Restart,
		DataProcessed: jobInfo.DataProcessed,
		DataSkipped:   jobInfo.DataSkipped,
		DataTotal:     jobInfo.DataTotal,
		DataFailed:    jobInfo.DataFailed,
		DataRecovered: jobInfo.DataRecovered,
		Stats:         jobInfo.Stats,
	})
	return err
}

// TODO: The job info should eventually just have a field with type *datum.Stats
func (pj *pendingJob) saveJobStats(stats *datum.Stats) {
	// TODO: Need to clean up the setup of process stats.
	if pj.ji.Stats == nil {
		pj.ji.Stats = &pps.ProcessStats{}
	}
	datum.MergeProcessStats(pj.ji.Stats, stats.ProcessStats)
	pj.ji.DataProcessed += stats.Processed
	pj.ji.DataSkipped += stats.Skipped
	pj.ji.DataFailed += stats.Failed
	pj.ji.DataRecovered += stats.Recovered
	pj.ji.DataTotal += stats.Processed + stats.Skipped + stats.Failed + stats.Recovered
}

func (pj *pendingJob) withDeleter(pachClient *client.APIClient, cb func() error) error {
	defer pj.jdit.SetDeleter(nil)
	// Setup file operation client for output Meta commit.
	metaCommit := pj.metaCommitInfo.Commit
	return pachClient.WithModifyFileClient(metaCommit.Repo.Name, metaCommit.ID, func(mfcMeta *client.ModifyFileClient) error {
		// Setup file operation client for output PFS commit.
		outputCommit := pj.commitInfo.Commit
		return pachClient.WithModifyFileClient(outputCommit.Repo.Name, outputCommit.ID, func(mfcPFS *client.ModifyFileClient) error {
			parentMetaCommit := pj.metaCommitInfo.ParentCommit
			metaFileWalker := func(path string) ([]string, error) {
				var files []string
				if err := pachClient.WalkFile(parentMetaCommit.Repo.Name, parentMetaCommit.ID, path, func(fi *pfs.FileInfo) error {
					if fi.FileType == pfs.FileType_FILE {
						files = append(files, fi.File.Path)
					}
					return nil
				}); err != nil {
					return nil, err
				}
				return files, nil
			}
			pj.jdit.SetDeleter(datum.NewDeleter(metaFileWalker, mfcMeta, mfcPFS))
			return cb()
		})
	})
}

type registry struct {
	driver      driver.Driver
	logger      logs.TaggedLogger
	taskQueue   *work.TaskQueue
	concurrency int64
	limiter     limit.ConcurrencyLimiter
	jobChain    *chain.JobChain
}

// TODO:
// s3 input / gateway stuff (need more information here).
// egress.
// Prometheus stats? (previously in the driver, which included testing we should reuse if possible)
// capture logs (reuse driver tests and reintroduce tagged logger).
func newRegistry(driver driver.Driver, logger logs.TaggedLogger) (*registry, error) {
	// Determine the maximum number of concurrent tasks we will allow
	concurrency, err := driver.ExpectedNumWorkers()
	if err != nil {
		return nil, err
	}
	taskQueue, err := driver.NewTaskQueue()
	if err != nil {
		return nil, err
	}
	return &registry{
		driver:      driver,
		logger:      logger,
		taskQueue:   taskQueue,
		concurrency: concurrency,
		limiter:     limit.New(int(concurrency)),
	}, nil
}

func (reg *registry) succeedJob(pj *pendingJob) error {
	var newState pps.JobState
	if pj.ji.Egress == nil {
		pj.logger.Logf("job successful, closing commits")
		newState = pps.JobState_JOB_SUCCESS
	} else {
		pj.logger.Logf("job successful, advancing to egress")
		newState = pps.JobState_JOB_EGRESSING
	}
	// Use the registry's driver so that the job's supervision goroutine cannot cancel us
	return finishJob(reg.driver.PipelineInfo(), reg.driver.PachClient(), pj, newState, "")
}

func (reg *registry) failJob(pj *pendingJob, reason string) error {
	pj.logger.Logf("failing job with reason: %s", reason)
	// Use the registry's driver so that the job's supervision goroutine cannot cancel us
	return finishJob(reg.driver.PipelineInfo(), reg.driver.PachClient(), pj, pps.JobState_JOB_FAILURE, reason)
}

func (reg *registry) killJob(pj *pendingJob, reason string) error {
	pj.logger.Logf("killing job with reason: %s", reason)
	// Use the registry's driver so that the job's supervision goroutine cannot cancel us
	return finishJob(reg.driver.PipelineInfo(), reg.driver.PachClient(), pj, pps.JobState_JOB_KILLED, reason)
}

func (reg *registry) initializeJobChain(metaCommit *pfs.Commit) error {
	if reg.jobChain == nil {
		hasher := &hasher{
			name: reg.driver.PipelineInfo().Pipeline.Name,
			salt: reg.driver.PipelineInfo().Salt,
		}
		pachClient := reg.driver.PachClient()
		metaCommitInfo, err := pachClient.PfsAPIClient.InspectCommit(pachClient.Ctx(),
			&pfs.InspectCommitRequest{
				Commit: metaCommit,
			})
		if err != nil {
			return err
		}
		if metaCommitInfo.ParentCommit == nil {
			reg.jobChain = chain.NewJobChain(hasher)
			return nil
		}
		parentMetaCommitInfo, err := pachClient.PfsAPIClient.InspectCommit(pachClient.Ctx(),
			&pfs.InspectCommitRequest{
				Commit:     metaCommitInfo.ParentCommit,
				BlockState: pfs.CommitState_FINISHED,
			})
		if err != nil {
			return err
		}
		commit := parentMetaCommitInfo.Commit
		reg.jobChain = chain.NewJobChain(
			hasher,
			datum.NewFileSetIterator(pachClient, commit.Repo.Name, commit.ID),
		)
	}
	return nil
}

// ensureJob loads an existing job for the given commit in the pipeline, or
// creates it if there is none. If more than one such job exists, an error will
// be generated.
func (reg *registry) ensureJob(commitInfo *pfs.CommitInfo, metaCommit *pfs.Commit) (*pps.JobInfo, error) {
	pachClient := reg.driver.PachClient()
	// Check if a job was previously created for this commit. If not, make one
	jobInfos, err := pachClient.ListJob("", nil, commitInfo.Commit, -1, true)
	if err != nil {
		return nil, err
	}
	if len(jobInfos) > 1 {
		return nil, errors.Errorf("multiple jobs found for commit: %s/%s", commitInfo.Commit.Repo.Name, commitInfo.Commit.ID)
	} else if len(jobInfos) < 1 {
		job, err := pachClient.CreateJob(reg.driver.PipelineInfo().Pipeline.Name, commitInfo.Commit, metaCommit)
		if err != nil {
			return nil, err
		}
		reg.logger.Logf("created new job %q for output commit %q", job.ID, commitInfo.Commit.ID)
		// get jobInfo to look up spec commit, pipeline version, etc (if this
		// worker is stale and about to be killed, the new job may have a newer
		// pipeline version than the master. Or if the commit is stale, it may
		// have an older pipeline version than the master)
		return pachClient.InspectJob(job.ID, false)
	}
	// Get latest job state.
	reg.logger.Logf("found existing job %q for output commit %q", jobInfos[0].Job.ID, commitInfo.Commit.ID)
	return pachClient.InspectJob(jobInfos[0].Job.ID, false)
}

func (reg *registry) startJob(commitInfo *pfs.CommitInfo, metaCommit *pfs.Commit) error {
	var asyncEg *errgroup.Group
	reg.limiter.Acquire()
	defer func() {
		if asyncEg == nil {
			// The async errgroup never got started, so give up the limiter lock
			reg.limiter.Release()
		}
	}()
	jobInfo, err := reg.ensureJob(commitInfo, metaCommit)
	if err != nil {
		return err
	}
	switch {
	case commitInfo.Finished != nil:
		return recoverJob(reg.driver.PipelineInfo(), reg.driver.PachClient(), jobInfo, true)
	case jobInfo.PipelineVersion < reg.driver.PipelineInfo().Version:
		jobInfo.State = pps.JobState_JOB_KILLED
		jobInfo.Reason = "pipeline has been updated"
		return recoverJob(reg.driver.PipelineInfo(), reg.driver.PachClient(), jobInfo, true)
	case jobInfo.PipelineVersion > reg.driver.PipelineInfo().Version:
		return errors.Errorf("job %s's version (%d) greater than pipeline's "+
			"version (%d), this should automatically resolve when the worker "+
			"is updated", jobInfo.Job.ID, jobInfo.PipelineVersion, reg.driver.PipelineInfo().Version)
	}
	if err := reg.initializeJobChain(metaCommit); err != nil {
		return err
	}
	var metaCommitInfo *pfs.CommitInfo
	if metaCommit != nil {
		metaCommitInfo, err = reg.driver.PachClient().InspectCommit(metaCommit.Repo.Name, metaCommit.ID)
		if err != nil {
			return err
		}
	}
	jobCtx, cancel := context.WithCancel(reg.driver.PachClient().Ctx())
	driver := reg.driver.WithContext(jobCtx)
	// Build the pending job to send out to workers - this will block if we have
	// too many already
	pj := &pendingJob{
		driver:         driver,
		ji:             jobInfo,
		logger:         reg.logger.WithJob(jobInfo.Job.ID),
		commitInfo:     commitInfo,
		metaCommitInfo: metaCommitInfo,
		cancel:         cancel,
	}
	// Inputs must be ready before we can construct a datum iterator, so do this
	// synchronously to ensure correct order in the jobChain.
	if err := pj.logger.LogStep("waiting for job inputs", func() error {
		return reg.processJobStarting(pj)
	}); err != nil {
		return err
	}
	// TODO: we should NOT start the job this way if it is in EGRESSING
	// TODO: This could probably be scoped to a callback, and we could move job specific features
	// in the chain package (timeouts for example).
	// TODO: I use the registry pachclient for the iterators, so I can reuse across jobs for skipping.
	pachClient := reg.driver.PachClient()
	dit, err := datum.NewIterator(pachClient, pj.ji.Input)
	if err != nil {
		return err
	}
	outputDit := datum.NewFileSetIterator(pachClient, pj.metaCommitInfo.Commit.Repo.Name, pj.metaCommitInfo.Commit.ID)
	pj.jdit = reg.jobChain.CreateJob(pj.driver.PachClient().Ctx(), pj.ji.Job.ID, dit, outputDit)
	var afterTime time.Duration
	if pj.ji.JobTimeout != nil {
		startTime, err := types.TimestampFromProto(pj.ji.Started)
		if err != nil {
			return err
		}
		timeout, err := types.DurationFromProto(pj.ji.JobTimeout)
		if err != nil {
			return err
		}
		afterTime = time.Until(startTime.Add(timeout))
	}
	asyncEg, jobCtx = errgroup.WithContext(pj.driver.PachClient().Ctx())
	pj.driver = reg.driver.WithContext(jobCtx)
	asyncEg.Go(func() error {
		defer pj.cancel()
		if pj.ji.JobTimeout != nil {
			pj.logger.Logf("cancelling job at: %+v", afterTime)
			timer := time.AfterFunc(afterTime, func() {
				reg.killJob(pj, "job timed out")
			})
			defer timer.Stop()
		}
		return backoff.RetryUntilCancel(pj.driver.PachClient().Ctx(), func() error {
			return reg.superviseJob(pj)
		}, backoff.NewInfiniteBackOff(), func(err error, d time.Duration) error {
			pj.logger.Logf("error in superviseJob: %v, retrying in %+v", err, d)
			return nil
		})
	})
	asyncEg.Go(func() error {
		defer pj.cancel()
		mutex := &sync.Mutex{}
		mutex.Lock()
		defer mutex.Unlock()
		// This runs the callback asynchronously, but we want to block the errgroup until it completes
		if err := reg.taskQueue.RunTask(pj.driver.PachClient().Ctx(), func(master *work.Master) {
			defer mutex.Unlock()
			pj.taskMaster = master
			backoff.RetryUntilCancel(pj.driver.PachClient().Ctx(), func() error {
				var err error
				for err == nil {
					err = reg.processJob(pj)
				}
				if errors.Is(err, errutil.ErrBreak) {
					return nil
				}
				return err
			}, backoff.NewInfiniteBackOff(), func(err error, d time.Duration) error {
				pj.logger.Logf("processJob error: %v, retrying in %v", err, d)
				for err != nil {
					if st, ok := err.(errors.StackTracer); ok {
						pj.logger.Logf("error stack: %+v", st.StackTrace())
					}
					err = errors.Unwrap(err)
				}
				// Get job state, increment restarts, write job state
				pj.ji, err = pj.driver.PachClient().InspectJob(pj.ji.Job.ID, false)
				if err != nil {
					return err
				}
				pj.ji.Restart++
				if err := pj.writeJobInfo(); err != nil {
					pj.logger.Logf("error incrementing restart count for job (%s): %v", pj.ji.Job.ID, err)
				}
				// Reload the job's commitInfo(s) as they may have changed and clear the state of the commit(s).
				pj.commitInfo, err = reg.driver.PachClient().InspectCommit(pj.commitInfo.Commit.Repo.Name, pj.commitInfo.Commit.ID)
				if err != nil {
					return err
				}
				if err := pj.driver.PachClient().ClearCommit(pj.commitInfo.Commit.Repo.Name, pj.commitInfo.Commit.ID); err != nil {
					return err
				}
				if metaCommit != nil {
					pj.metaCommitInfo, err = reg.driver.PachClient().InspectCommit(metaCommit.Repo.Name, metaCommit.ID)
					if err != nil {
						return err
					}
					if err := pj.driver.PachClient().ClearCommit(pj.metaCommitInfo.Commit.Repo.Name, pj.metaCommitInfo.Commit.ID); err != nil {
						return err
					}
				}
				return nil
			})
			pj.logger.Logf("master done running processJobs")
			// TODO: make sure that all paths close the commit correctly
		}); err != nil {
			return err
		}
		// This should block until the callback has completed
		mutex.Lock()
		return nil
	})
	go func() {
		defer reg.limiter.Release()
		// Make sure the job has been removed from the job chain.
		defer pj.jdit.Finish()
		if err := asyncEg.Wait(); err != nil {
			pj.logger.Logf("fatal job error: %v", err)
		}
	}()
	return nil
}

// superviseJob watches for the output commit closing and cancels the job, or
// deletes it if the output commit is removed.
func (reg *registry) superviseJob(pj *pendingJob) error {
	defer pj.cancel()
	ci, err := pj.driver.PachClient().PfsAPIClient.InspectCommit(pj.driver.PachClient().Ctx(),
		&pfs.InspectCommitRequest{
			Commit:     pj.ji.OutputCommit,
			BlockState: pfs.CommitState_FINISHED,
		})
	if err != nil {
		if pfsserver.IsCommitNotFoundErr(err) || pfsserver.IsCommitDeletedErr(err) {
			// Stop the job and clean up any job state in the registry
			if err := reg.killJob(pj, "output commit missing"); err != nil {
				return err
			}
			// Output commit was deleted. Delete job as well
			if _, err := pj.driver.NewSTM(func(stm col.STM) error {
				// Delete the job if no other worker has deleted it yet
				jobPtr := &pps.EtcdJobInfo{}
				if err := pj.driver.Jobs().ReadWrite(stm).Get(pj.ji.Job.ID, jobPtr); err != nil {
					return err
				}
				return pj.driver.DeleteJob(stm, jobPtr)
			}); err != nil && !col.IsErrNotFound(err) {
				return err
			}
			return nil
		}
		return err
	}
	if strings.Contains(ci.Description, pfs.EmptyStr) {
		return reg.killJob(pj, "output commit closed")
	}
	return nil

}

func (reg *registry) processJob(pj *pendingJob) error {
	state := pj.ji.State
	switch {
	case ppsutil.IsTerminal(state):
		pj.cancel()
		return errutil.ErrBreak
	case state == pps.JobState_JOB_STARTING:
		return errors.New("job should have been moved out of the STARTING state before processJob")
	case state == pps.JobState_JOB_RUNNING:
		return pj.logger.LogStep("processing job datums", func() error {
			return reg.processJobRunning(pj)
		})
	}
	pj.cancel()
	return errors.Errorf("unknown job state: %v", state)
}

func (reg *registry) processJobStarting(pj *pendingJob) error {
	// block until job inputs are ready
	failed, err := failedInputs(pj.driver.PachClient(), pj.ji)
	if err != nil {
		return err
	}
	if len(failed) > 0 {
		reason := fmt.Sprintf("inputs failed: %s", strings.Join(failed, ", "))
		return reg.failJob(pj, reason)
	}
	pj.ji.State = pps.JobState_JOB_RUNNING
	return pj.writeJobInfo()
}

// TODO:
// Need to put some more thought into the context use.
func (reg *registry) processJobRunning(pj *pendingJob) error {
	pachClient := pj.driver.PachClient()
	eg, ctx := errgroup.WithContext(pachClient.Ctx())
	pachClient = pachClient.WithCtx(ctx)
	stats := &datum.Stats{ProcessStats: &pps.ProcessStats{}}

	// TODO: This is a hack to ensure that deletions are generated before any output is uploaded.
	// This may be resolved by either explicitly generating deletes first (somewhat similar to this hack) or
	// relying on temporary fileset identifiers being associated with the commit after the datumsets have been
	// generated (and therefore after the deletes).
	if err := pj.withDeleter(pachClient, func() error {
		return pj.jdit.Iterate(func(_ *datum.Meta) error { return nil })
	}); err != nil {
		return err
	}

	// Setup datum set subtask channel.
	subtasks := make(chan *work.Task)
	if err := pachClient.WithRenewer(func(ctx context.Context, renewer *renew.StringSet) error {
		// Setup goroutine for creating datum set subtasks.
		// TODO: When the datum set spec is not set, evenly distribute the datums.
		eg.Go(func() error {
			defer close(subtasks)
			storageRoot := filepath.Join(pj.driver.InputDir(), client.PPSScratchSpace, uuid.NewWithoutDashes())
			var setSpec *datum.SetSpec
			if pj.driver.PipelineInfo().ChunkSpec != nil {
				setSpec = &datum.SetSpec{
					Number: int(pj.driver.PipelineInfo().ChunkSpec.Number),
				}
			}
			return datum.CreateSets(pj.jdit, storageRoot, setSpec, func(upload func(datum.AppendFileTarClient) error) error {
				subtask, err := createDatumSetSubtask(pachClient, pj, upload, renewer)
				if err != nil {
					return err
				}
				select {
				case subtasks <- subtask:
				case <-ctx.Done():
					return ctx.Err()
				}
				return nil
			})
		})
		// Setup goroutine for running and collecting datum set subtasks.
		eg.Go(func() error {
			return pj.logger.LogStep("running and collecting datum set subtasks", func() error {
				return pj.taskMaster.RunSubtasksChan(
					subtasks,
					func(ctx context.Context, taskInfo *work.TaskInfo) error {
						if taskInfo.State == work.State_FAILURE {
							return errors.Errorf("datum set subtask failed: %s", taskInfo.Reason)
						}
						data, err := deserializeDatumSet(taskInfo.Task.Data)
						if err != nil {
							return err
						}
						renewer.Remove(data.FileSet)
						return datum.MergeStats(stats, data.Stats)
					},
				)
			})
		})
		return eg.Wait()
	}); err != nil {
		return err
	}
	// TODO: This shouldn't be necessary.
	select {
	case <-pj.driver.PachClient().Ctx().Done():
		return pj.driver.PachClient().Ctx().Err()
	default:
	}
	pj.saveJobStats(pj.jdit.Stats())
	pj.saveJobStats(stats)
	if stats.FailedID != "" {
		return reg.failJob(pj, fmt.Sprintf("datum %v failed", stats.FailedID))
	}
	return reg.succeedJob(pj)
}

func createDatumSetSubtask(pachClient *client.APIClient, pj *pendingJob, upload func(datum.AppendFileTarClient) error, renewer *renew.StringSet) (*work.Task, error) {
	resp, err := pachClient.WithCreateFilesetClient(func(ctfsc *client.CreateFilesetClient) error {
		return upload(ctfsc)
	})
	if err != nil {
		return nil, err
	}
	renewer.Add(resp.FilesetId)
	data, err := serializeDatumSet(&DatumSet{
		JobID: pj.ji.Job.ID,
		// TODO: It might make sense for this to be a hash of the constituent datums?
		// That could make it possible to recover from a master restart.
		FileSet:      resp.FilesetId,
		OutputCommit: pj.commitInfo.Commit,
		MetaCommit:   pj.metaCommitInfo.Commit,
	})
	if err != nil {
		return nil, err
	}
	return &work.Task{
		// TODO: Should this just be a uuid?
		ID:   uuid.NewWithoutDashes(),
		Data: data,
	}, nil
}

func serializeDatumSet(data *DatumSet) (*types.Any, error) {
	serialized, err := types.MarshalAny(data)
	if err != nil {
		return nil, err
	}
	return serialized, nil
}

func deserializeDatumSet(any *types.Any) (*DatumSet, error) {
	data := &DatumSet{}
	if err := types.UnmarshalAny(any, data); err != nil {
		return nil, err
	}
	return data, nil
}

func failedInputs(pachClient *client.APIClient, jobInfo *pps.JobInfo) ([]string, error) {
	var failed []string
	var vistErr error
	blockCommit := func(name string, commit *pfs.Commit) {
		ci, err := pachClient.PfsAPIClient.InspectCommit(pachClient.Ctx(),
			&pfs.InspectCommitRequest{
				Commit:     commit,
				BlockState: pfs.CommitState_FINISHED,
			})
		if err != nil {
			if vistErr == nil {
				vistErr = errors.Wrapf(err, "error blocking on commit %s/%s",
					commit.Repo.Name, commit.ID)
			}
			return
		}
		if strings.Contains(ci.Description, pfs.EmptyStr) {
			failed = append(failed, name)
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
	return failed, vistErr
}

// TODO: Errors that can occur while finishing jobs needs more thought.
// TODO: Job failures are propagated through commits with pfs.EmptyStr in the description, would be better to have general purpose metadata associated with a commit.
func finishJob(pipelineInfo *pps.PipelineInfo, pachClient *client.APIClient, pj *pendingJob, state pps.JobState, reason string) error {
	jobInfo := pj.ji
	// Optimistically update the local state and reason - if any errors occur the
	// local state will be reloaded way up the stack
	jobInfo.State = state
	jobInfo.Reason = reason
	var empty bool
	if state == pps.JobState_JOB_FAILURE || state == pps.JobState_JOB_KILLED {
		empty = true
	}
	if _, err := pachClient.RunBatchInTransaction(func(builder *client.TransactionBuilder) error {
		if _, err := builder.PfsAPIClient.FinishCommit(pachClient.Ctx(), &pfs.FinishCommitRequest{
			Commit: jobInfo.StatsCommit,
			Empty:  empty,
		}); err != nil {
			return err
		}
		if _, err := builder.PfsAPIClient.FinishCommit(pachClient.Ctx(), &pfs.FinishCommitRequest{
			Commit: jobInfo.OutputCommit,
			Empty:  empty,
		}); err != nil {
			return err
		}
		return writeJobInfo(&builder.APIClient, jobInfo)
	}); err != nil {
		if pfsserver.IsCommitFinishedErr(err) || pfsserver.IsCommitNotFoundErr(err) || pfsserver.IsCommitDeletedErr(err) {
			if err := recoverJob(pipelineInfo, pachClient, jobInfo, empty); err != nil {
				return err
			}
			// TODO: How to handle errors without causing subsequent jobs to get stuck.
			pj.jdit.Finish()
			return nil
		}
		// For other types of errors, we want to fail the job supervision and let it
		// reattempt later
		return err
	}
	// TODO: How to handle errors without causing subsequent jobs to get stuck.
	pj.jdit.Finish()
	return nil
}

func recoverJob(pipelineInfo *pps.PipelineInfo, pachClient *client.APIClient, jobInfo *pps.JobInfo, empty bool) error {
	if _, err := pachClient.PfsAPIClient.FinishCommit(pachClient.Ctx(), &pfs.FinishCommitRequest{
		Commit: jobInfo.StatsCommit,
		Empty:  empty,
	}); err != nil {
		if !pfsserver.IsCommitFinishedErr(err) && !pfsserver.IsCommitNotFoundErr(err) && !pfsserver.IsCommitDeletedErr(err) {
			return err
		}
	}
	if _, err := pachClient.PfsAPIClient.FinishCommit(pachClient.Ctx(), &pfs.FinishCommitRequest{
		Commit: jobInfo.OutputCommit,
		Empty:  empty,
	}); err != nil {
		if !pfsserver.IsCommitFinishedErr(err) && !pfsserver.IsCommitNotFoundErr(err) && !pfsserver.IsCommitDeletedErr(err) {
			return err
		}
	}
	if err := writeJobInfo(pachClient, jobInfo); err != nil {
		if !ppsserver.IsJobFinishedErr(err) {
			return err
		}
	}
	return nil
}

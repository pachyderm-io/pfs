package transform

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/gogo/protobuf/types"
	"golang.org/x/sync/errgroup"

	"github.com/pachyderm/pachyderm/src/client"
	"github.com/pachyderm/pachyderm/src/client/limit"
	"github.com/pachyderm/pachyderm/src/client/pfs"
	"github.com/pachyderm/pachyderm/src/client/pkg/errors"
	"github.com/pachyderm/pachyderm/src/client/pkg/pbutil"
	"github.com/pachyderm/pachyderm/src/client/pps"
	pfsserver "github.com/pachyderm/pachyderm/src/server/pfs"
	"github.com/pachyderm/pachyderm/src/server/pkg/backoff"
	col "github.com/pachyderm/pachyderm/src/server/pkg/collection"
	"github.com/pachyderm/pachyderm/src/server/pkg/errutil"
	"github.com/pachyderm/pachyderm/src/server/pkg/ppsutil"
	"github.com/pachyderm/pachyderm/src/server/pkg/uuid"
	"github.com/pachyderm/pachyderm/src/server/pkg/work"
	ppsserver "github.com/pachyderm/pachyderm/src/server/pps"
	"github.com/pachyderm/pachyderm/src/server/worker/common"
	"github.com/pachyderm/pachyderm/src/server/worker/datum"
	"github.com/pachyderm/pachyderm/src/server/worker/driver"
	"github.com/pachyderm/pachyderm/src/server/worker/logs"
	"github.com/pachyderm/pachyderm/src/server/worker/pipeline/transform/chain"
)

const (
	jobTagArtifactPrefix = "job-artifacts"
)

// All tags that begin with this prefix (and their associated objects) will be
// cleaned up when the job ends. If any of these objects are leaked due to a
// crash between finishing the job and removing the artifacts, they can be
// removed using the `garbage-collect` command.
func jobTagPrefix(pipelineName string, jobID string) string {
	return filepath.Join(jobTagArtifactPrefix, fmt.Sprintf("pipeline-%s", pipelineName), fmt.Sprintf("job-%s", jobID))
}

// This generates a unique ID each time, as the datums present in a given chunk
// may change based on parallel jobs. Therefore, this is not usable to reload
// old datum chunks - this exists so that these tags will be removed when
// cleaning job artifacts.
func jobDatumsTag(pipelineName string, jobID string) string {
	return filepath.Join(jobTagPrefix(pipelineName, jobID), fmt.Sprintf("datum-chunk-%s", uuid.NewWithoutDashes()))
}

func jobHashtreesTag(pipelineName string, jobID string) string {
	return filepath.Join(jobTagPrefix(pipelineName, jobID), "hashtrees")
}

func jobRecoveredDatumTagsTag(pipelineName string, jobID string) string {
	return filepath.Join(jobTagPrefix(pipelineName, jobID), "recovered")
}

type pendingJob struct {
	driver          driver.Driver
	commitInfo      *pfs.CommitInfo
	statsCommitInfo *pfs.CommitInfo
	cancel          context.CancelFunc
	logger          logs.TaggedLogger
	ji              *pps.JobInfo
	jdit            chain.JobDatumIterator
	taskMaster      *work.Master

	// These are filled in when the RUNNING phase completes, but may be re-fetched
	// from object storage.
	chunkHashtrees []*HashtreeInfo
	statsHashtrees []*HashtreeInfo
}

type registry struct {
	driver      driver.Driver
	logger      logs.TaggedLogger
	taskQueue   *work.TaskQueue
	concurrency int64
	limiter     limit.ConcurrencyLimiter
	jobChain    chain.JobChain
}

type hasher struct {
	name string
	salt string
}

func (h *hasher) Hash(inputs []*common.Input) string {
	return common.HashDatum(h.name, h.salt, inputs)
}

// Returns the registry or lazily instantiates it
func newRegistry(
	logger logs.TaggedLogger,
	driver driver.Driver,
) (*registry, error) {
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
		concurrency: concurrency,
		taskQueue:   taskQueue,
		limiter:     limit.New(int(concurrency)),
		jobChain:    nil,
	}, nil
}

// Helper function for succeedJob and failJob, do not use directly
func finishJob(
	pipelineInfo *pps.PipelineInfo,
	pachClient *client.APIClient,
	jobInfo *pps.JobInfo,
	state pps.JobState,
	reason string,
	datums *pfs.Object,
	trees []*pfs.Object,
	size uint64,
	statsTrees []*pfs.Object,
	statsSize uint64,
) error {
	// Optimistically update the local state and reason - if any errors occur the
	// local state will be reloaded way up the stack
	jobInfo.State = state
	jobInfo.Reason = reason

	if _, err := pachClient.RunBatchInTransaction(func(builder *client.TransactionBuilder) error {
		if pipelineInfo.S3Out {
			if err := builder.FinishCommit(jobInfo.OutputCommit.Repo.Name, jobInfo.OutputCommit.ID); err != nil {
				return err
			}
		} else {
			if jobInfo.StatsCommit != nil {
				if _, err := builder.PfsAPIClient.FinishCommit(pachClient.Ctx(), &pfs.FinishCommitRequest{
					Commit:    jobInfo.StatsCommit,
					Empty:     statsTrees == nil,
					Trees:     statsTrees,
					SizeBytes: statsSize,
				}); err != nil {
					return err
				}
			}

			if _, err := builder.PfsAPIClient.FinishCommit(pachClient.Ctx(), &pfs.FinishCommitRequest{
				Commit:    jobInfo.OutputCommit,
				Empty:     trees == nil,
				Datums:    datums,
				Trees:     trees,
				SizeBytes: size,
			}); err != nil {
				return err
			}
		}

		return writeJobInfo(&builder.APIClient, jobInfo)
	}); err != nil {
		if pfsserver.IsCommitFinishedErr(err) || pfsserver.IsCommitNotFoundErr(err) || pfsserver.IsCommitDeletedErr(err) || ppsserver.IsJobFinishedErr(err) {
			// For certain types of errors, we want to reattempt these operations
			// outside of a transaction (in case the job or commits were affected by
			// some non-transactional code elsewhere, we can attempt to recover)
			return recoverFinishedJob(pipelineInfo, pachClient, jobInfo, state, reason, datums, trees, size, statsTrees, statsSize)
		}
		// For other types of errors, we want to fail the job supervision and let it
		// reattempt later
		return err
	}
	return nil
}

// recoverFinishedJob performs job and output commit updates outside of a
// transaction in an attempt to get everything in a consistent state if they
// were modified non-transactionally elsewhere.
func recoverFinishedJob(
	pipelineInfo *pps.PipelineInfo,
	pachClient *client.APIClient,
	jobInfo *pps.JobInfo,
	state pps.JobState,
	reason string,
	datums *pfs.Object,
	trees []*pfs.Object,
	size uint64,
	statsTrees []*pfs.Object,
	statsSize uint64,
) error {
	if pipelineInfo.S3Out {
		if err := pachClient.FinishCommit(jobInfo.OutputCommit.Repo.Name, jobInfo.OutputCommit.ID); err != nil {
			return err
		}
	} else {
		if jobInfo.StatsCommit != nil {
			if _, err := pachClient.PfsAPIClient.FinishCommit(pachClient.Ctx(), &pfs.FinishCommitRequest{
				Commit:    jobInfo.StatsCommit,
				Empty:     statsTrees == nil,
				Trees:     statsTrees,
				SizeBytes: statsSize,
			}); err != nil {
				if !pfsserver.IsCommitFinishedErr(err) && !pfsserver.IsCommitNotFoundErr(err) && !pfsserver.IsCommitDeletedErr(err) {
					return err
				}
			}
		}

		if _, err := pachClient.PfsAPIClient.FinishCommit(pachClient.Ctx(), &pfs.FinishCommitRequest{
			Commit:    jobInfo.OutputCommit,
			Empty:     trees == nil,
			Datums:    datums,
			Trees:     trees,
			SizeBytes: size,
		}); err != nil {
			if !pfsserver.IsCommitFinishedErr(err) && !pfsserver.IsCommitNotFoundErr(err) && !pfsserver.IsCommitDeletedErr(err) {
				return err
			}
		}
	}

	if err := writeJobInfo(pachClient, jobInfo); err != nil {
		if !ppsserver.IsJobFinishedErr(err) {
			return err
		}
	}
	return nil
}

// succeedJob will move a job to the successful state and propagate to any
// dependent jobs in the jobChain.
func (reg *registry) succeedJob(
	pj *pendingJob,
	trees []*pfs.Object,
	size uint64,
	statsTrees []*pfs.Object,
	statsSize uint64,
) error {
	datums, err := reg.storeJobDatums(pj)
	if err != nil {
		return err
	}

	var newState pps.JobState
	if pj.ji.Egress == nil {
		pj.logger.Logf("job successful, closing commits")
		newState = pps.JobState_JOB_SUCCESS
	} else {
		pj.logger.Logf("job successful, advancing to egress")
		newState = pps.JobState_JOB_EGRESSING
	}

	// Use the registry's driver so that the job's supervision goroutine cannot cancel us
	if err := finishJob(reg.driver.PipelineInfo(), reg.driver.PachClient(), pj.ji, newState, "", datums, trees, size, statsTrees, statsSize); err != nil {
		return err
	}

	if err := reg.jobChain.Succeed(pj); err != nil {
		return err
	}

	return reg.cleanJobArtifacts(pj.ji.Job)
}

func (reg *registry) failJob(
	pj *pendingJob,
	reason string,
	statsTrees []*pfs.Object,
	statsSize uint64,
) error {
	pj.logger.Logf("failing job with reason: %s", reason)

	// Use the registry's driver so that the job's supervision goroutine cannot cancel us
	if err := finishJob(reg.driver.PipelineInfo(), reg.driver.PachClient(), pj.ji, pps.JobState_JOB_FAILURE, reason, nil, nil, 0, statsTrees, statsSize); err != nil {
		return err
	}

	if err := reg.cleanJobArtifacts(pj.ji.Job); err != nil {
		return err
	}

	// Disregard job chain errors when failing the job - in case of egress, the
	// pending job should already have been removed from the chain.
	reg.jobChain.Fail(pj)
	return nil
}

func (reg *registry) killJob(
	pj *pendingJob,
	reason string,
) error {
	pj.logger.Logf("killing job with reason: %s", reason)

	// Use the registry's driver so that the job's supervision goroutine cannot cancel us
	if err := finishJob(reg.driver.PipelineInfo(), reg.driver.PachClient(), pj.ji, pps.JobState_JOB_KILLED, reason, nil, nil, 0, nil, 0); err != nil {
		return err
	}

	if err := reg.cleanJobArtifacts(pj.ji.Job); err != nil {
		return err
	}

	return reg.jobChain.Fail(pj)
}

func forEachTag(pachClient *client.APIClient, prefix string, cb func(tag *pfs.Tag, object *pfs.Object) error) error {
	listTagClient, err := pachClient.ListTags(
		pachClient.Ctx(),
		&pfs.ListTagsRequest{Prefix: prefix, IncludeObject: true},
	)
	if err != nil {
		return err
	}

	for {
		response, err := listTagClient.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if err := cb(response.Tag, response.Object); err != nil {
			if errors.Is(err, errutil.ErrBreak) {
				return nil
			}
			return err
		}
	}
}

func (reg *registry) cleanJobArtifacts(job *pps.Job) error {
	return reg.logger.WithJob(job.ID).LogStep("Cleaning job artifacts", func() error {
		prefix := jobTagPrefix(reg.driver.PipelineInfo().Pipeline.Name, job.ID) + string(filepath.Separator)
		tags := []*pfs.Tag{}
		objects := []*pfs.Object{}

		if err := forEachTag(reg.driver.PachClient(), prefix, func(tag *pfs.Tag, object *pfs.Object) error {
			tags = append(tags, tag)
			objects = append(objects, object)
			return nil
		}); err != nil {
			return err
		}

		_, err := reg.driver.PachClient().DeleteTags(
			reg.driver.PachClient().Ctx(),
			&pfs.DeleteTagsRequest{Tags: tags},
		)

		return err
	})
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

func (pj *pendingJob) writeJobInfo() error {
	pj.logger.Logf("updating job info, state: %s", pj.ji.State)
	return writeJobInfo(pj.driver.PachClient(), pj.ji)
}

func (reg *registry) initializeJobChain(commitInfo *pfs.CommitInfo) error {
	if reg.jobChain == nil {
		// Get the most recent successful commit starting from the given commit
		parentCommitInfo, err := reg.getParentCommitInfo(commitInfo)
		if err != nil {
			return err
		}

		var baseDatums chain.DatumSet
		if parentCommitInfo != nil {
			baseDatums, err = reg.getDatumSet(parentCommitInfo.Datums)
			if err != nil {
				return err
			}
		} else {
			baseDatums = make(chain.DatumSet)
		}

		if reg.driver.PipelineInfo().S3Out {
			// When running a pipeline with S3Out, we need to yield every datum for
			// every job, use a no-skip job chain for this.
			reg.jobChain = chain.NewNoSkipJobChain(
				&hasher{
					name: reg.driver.PipelineInfo().Pipeline.Name,
					salt: reg.driver.PipelineInfo().Salt,
				},
			)
		} else {
			reg.jobChain = chain.NewJobChain(
				&hasher{
					name: reg.driver.PipelineInfo().Pipeline.Name,
					salt: reg.driver.PipelineInfo().Salt,
				},
				baseDatums,
			)
		}
	}

	return nil
}

// Generate a datum task (and split it up into subtasks) for the added datums
// in the pending job.
func (reg *registry) sendDatumTasks(ctx context.Context, pj *pendingJob, numDatums int64, subtasks chan<- *work.Task) error {
	chunkSpec := pj.ji.ChunkSpec
	if chunkSpec == nil {
		chunkSpec = &pps.ChunkSpec{}
	}

	maxDatumsPerTask := int64(chunkSpec.Number)
	maxBytesPerTask := int64(chunkSpec.SizeBytes)
	driver := pj.driver.WithContext(ctx)
	var numTasks int64
	if numDatums < reg.concurrency {
		numTasks = numDatums
	} else if maxDatumsPerTask > 0 && numDatums/maxDatumsPerTask > reg.concurrency {
		numTasks = numDatums / maxDatumsPerTask
	} else {
		numTasks = reg.concurrency
	}
	datumsPerTask := int64(math.Ceil(float64(numDatums) / float64(numTasks)))

	datumsSize := int64(0)
	datums := []*DatumInputs{}

	// finishTask will finish the currently-writing object and append it to the
	// subtasks, then reset all the relevant variables
	finishTask := func() error {
		putObjectWriter, err := driver.PachClient().PutObjectAsync([]*pfs.Tag{{Name: jobDatumsTag(reg.driver.PipelineInfo().Pipeline.Name, pj.ji.Job.ID)}})
		if err != nil {
			return err
		}

		protoWriter := pbutil.NewWriter(putObjectWriter)
		if _, err := protoWriter.Write(&DatumInputsList{Datums: datums}); err != nil {
			putObjectWriter.Close()
			return err
		}

		if err := putObjectWriter.Close(); err != nil {
			return err
		}

		datumsObject, err := putObjectWriter.Object()
		if err != nil {
			return err
		}

		taskData, err := serializeDatumData(&DatumData{Datums: datumsObject, OutputCommit: pj.ji.OutputCommit, JobID: pj.ji.Job.ID})
		if err != nil {
			return err
		}

		select {
		case subtasks <- &work.Task{ID: uuid.NewWithoutDashes(), Data: taskData}:
		case <-ctx.Done():
			return ctx.Err()
		}

		datumsSize = 0
		datums = []*DatumInputs{}
		return nil
	}

	// Build up chunks to be put into work tasks from the datum iterator
	for i := int64(0); i < numDatums; i++ {
		inputs, index := pj.jdit.NextDatum()
		if inputs == nil {
			return errors.New("job datum iterator returned nil inputs")
		}

		datums = append(datums, &DatumInputs{Inputs: inputs, Index: index})

		// If we have enough input bytes, finish the task
		if maxBytesPerTask != 0 {
			for _, input := range inputs {
				datumsSize += int64(input.FileInfo.SizeBytes)
			}
			if datumsSize >= maxBytesPerTask {
				if err := finishTask(); err != nil {
					return err
				}
			}
		}

		// If we hit the upper threshold for task size, finish the task
		if int64(len(datums)) >= datumsPerTask {
			if err := finishTask(); err != nil {
				return err
			}
		}
	}

	if len(datums) > 0 {
		if err := finishTask(); err != nil {
			return err
		}
	}

	return nil
}

func serializeDatumData(data *DatumData) (*types.Any, error) {
	serialized, err := types.MarshalAny(data)
	if err != nil {
		return nil, err
	}
	return serialized, nil
}

func deserializeDatumData(any *types.Any) (*DatumData, error) {
	data := &DatumData{}
	if err := types.UnmarshalAny(any, data); err != nil {
		return nil, err
	}
	return data, nil
}

func serializeMergeData(data *MergeData) (*types.Any, error) {
	serialized, err := types.MarshalAny(data)
	if err != nil {
		return nil, err
	}
	return serialized, nil
}

func deserializeMergeData(any *types.Any) (*MergeData, error) {
	data := &MergeData{}
	if err := types.UnmarshalAny(any, data); err != nil {
		return nil, err
	}
	return data, nil
}

func (reg *registry) getDatumSet(datumsObj *pfs.Object) (_ chain.DatumSet, retErr error) {
	pachClient := reg.driver.PachClient()
	if datumsObj == nil {
		return nil, nil
	}
	r, err := pachClient.GetObjectReader(datumsObj.Hash)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := r.Close(); err != nil && retErr != nil {
			retErr = err
		}
	}()
	pbr := pbutil.NewReader(r)
	datums := make(chain.DatumSet)
	for {
		k, err := pbr.ReadBytes()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return datums, retErr
			}
			return nil, err
		}
		datums[string(k)]++
	}
}

// Walk from the given commit back to a successfully completed commit so we can
// get the initial state of datumsBase in the registry.
func (reg *registry) getParentCommitInfo(commitInfo *pfs.CommitInfo) (*pfs.CommitInfo, error) {
	pachClient := reg.driver.PachClient()
	// Walk up the commit chain to find a successfully finished commit
	for commitInfo.ParentCommit != nil {
		parentCommitInfo, err := pachClient.PfsAPIClient.InspectCommit(pachClient.Ctx(),
			&pfs.InspectCommitRequest{
				Commit: commitInfo.ParentCommit,
			})
		if err != nil {
			return nil, err
		}
		// If the parent commit isn't finished, then finish it and continue the traversal.
		// If the parent commit is finished and has output, then return it.
		if parentCommitInfo.Finished == nil {
			if _, err := pachClient.PfsAPIClient.FinishCommit(pachClient.Ctx(), &pfs.FinishCommitRequest{
				Commit: parentCommitInfo.Commit,
				Empty:  true,
			}); err != nil && !pfsserver.IsCommitFinishedErr(err) {
				return nil, err
			}
		} else if parentCommitInfo.Trees != nil {
			return parentCommitInfo, nil
		}
		commitInfo = parentCommitInfo
	}
	return nil, nil
}

// ensureJob loads an existing job for the given commit in the pipeline, or
// creates it if there is none. If more than one such job exists, an error will
// be generated.
func (reg *registry) ensureJob(
	commitInfo *pfs.CommitInfo,
	statsCommit *pfs.Commit,
) (*pps.JobInfo, error) {
	pachClient := reg.driver.PachClient()

	// Check if a job was previously created for this commit. If not, make one
	jobInfos, err := pachClient.ListJob("", nil, commitInfo.Commit, -1, true)
	if err != nil {
		return nil, err
	}
	if len(jobInfos) > 1 {
		return nil, errors.Errorf("multiple jobs found for commit: %s/%s", commitInfo.Commit.Repo.Name, commitInfo.Commit.ID)
	} else if len(jobInfos) < 1 {
		job, err := pachClient.CreateJob(reg.driver.PipelineInfo().Pipeline.Name, commitInfo.Commit, statsCommit)
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

	// get latest job state
	reg.logger.Logf("found existing job %q for output commit %q", jobInfos[0].Job.ID, commitInfo.Commit.ID)
	return pachClient.InspectJob(jobInfos[0].Job.ID, false)
}

func (reg *registry) startJob(commitInfo *pfs.CommitInfo, statsCommit *pfs.Commit) error {
	if err := reg.initializeJobChain(commitInfo); err != nil {
		return err
	}

	var asyncEg *errgroup.Group
	reg.limiter.Acquire()

	defer func() {
		if asyncEg == nil {
			// The async errgroup never got started, so give up the limiter lock
			reg.limiter.Release()
		}
	}()

	jobInfo, err := reg.ensureJob(commitInfo, statsCommit)
	if err != nil {
		return err
	}

	var statsCommitInfo *pfs.CommitInfo
	if statsCommit != nil {
		statsCommitInfo, err = reg.driver.PachClient().InspectCommit(statsCommit.Repo.Name, statsCommit.ID)
		if err != nil {
			return err
		}
	}

	jobCtx, cancel := context.WithCancel(reg.driver.PachClient().Ctx())
	driver := reg.driver.WithContext(jobCtx)

	// Build the pending job to send out to workers - this will block if we have
	// too many already
	pj := &pendingJob{
		driver:          driver,
		commitInfo:      commitInfo,
		statsCommitInfo: statsCommitInfo,
		logger:          reg.logger.WithJob(jobInfo.Job.ID),
		ji:              jobInfo,
		cancel:          cancel,
	}

	switch {
	case ppsutil.IsTerminal(jobInfo.State):
		// Make sure the output commits are closed
		if err := recoverFinishedJob(pj.driver.PipelineInfo(), pj.driver.PachClient(), pj.ji, jobInfo.State, "", nil, nil, 0, nil, 0); err != nil {
			return err
		}
		// ignore finished jobs (e.g. old pipeline & already killed)
		return nil
	case jobInfo.PipelineVersion < reg.driver.PipelineInfo().Version:
		// kill unfinished jobs from old pipelines (should generally be cleaned
		// up by PPS master, but the PPS master can fail, and if these jobs
		// aren't killed, future jobs will hang indefinitely waiting for their
		// parents to finish)
		pj.ji.State = pps.JobState_JOB_KILLED
		pj.ji.Reason = "pipeline has been updated"
		if err := pj.writeJobInfo(); err != nil {
			if !ppsserver.IsJobFinishedErr(err) {
				return errors.Wrap(err, "failed to kill stale job")
			}
		}
		return nil
	case jobInfo.PipelineVersion > reg.driver.PipelineInfo().Version:
		return errors.Errorf("job %s's version (%d) greater than pipeline's "+
			"version (%d), this should automatically resolve when the worker "+
			"is updated", jobInfo.Job.ID, jobInfo.PipelineVersion, reg.driver.PipelineInfo().Version)
	}

	// Inputs must be ready before we can construct a datum iterator, so do this
	// synchronously to ensure correct order in the jobChain.
	if pj.ji.State == pps.JobState_JOB_STARTING {
		if err := pj.logger.LogStep("waiting for job inputs", func() error {
			return reg.processJobStarting(pj)
		}); err != nil {
			return err
		}
	}

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

	// Use a separate context for egress - it should not be canceled when the
	// output commit is closed.
	egressCtx, egressCancel := context.WithCancel(reg.driver.PachClient().Ctx())

	// If the job is already in egressing, we need to skip the job chain
	if pj.ji.State != pps.JobState_JOB_EGRESSING {
		pj.jdit, err = reg.jobChain.Start(pj)
		if err != nil {
			return err
		}
		pj.ji.DataTotal = pj.jdit.MaxLen()
		if err := pj.writeJobInfo(); err != nil {
			return err
		}
	}

	asyncEg.Go(func() error {
		defer pj.cancel()

		if pj.ji.JobTimeout != nil {
			pj.logger.Logf("cancelling job at: %+v", afterTime)
			timer := time.AfterFunc(afterTime, func() {
				reg.killJob(pj, "job timed out")

				// We cancel egress after the timeout, but we don't cancel egress if the
				// job's output commit is closed - that is both how jobs complete and
				// how they are killed by user action, but there's no way to distinguish
				// the two at the moment.
				egressCancel()
			})
			defer timer.Stop()
		}

		// We don't cancel the errgroup if the supervise fails because the job will
		// be canceled anyway, and this makes it easier to filter out spurious
		// errors when waiting on the errgroup.
		backoff.RetryUntilCancel(pj.driver.PachClient().Ctx(), func() error {
			return reg.superviseJob(pj)
		}, backoff.NewInfiniteBackOff(), func(err error, d time.Duration) error {
			pj.logger.Logf("error in superviseJob: %v, retrying in %+v", err, d)
			return nil
		})

		return nil
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

				pj.jdit.Reset()

				// Get job state, increment restarts, write job state
				pj.ji, err = pj.driver.PachClient().InspectJob(pj.ji.Job.ID, false)
				if err != nil {
					return err
				}

				pj.ji.Restart++
				if err := pj.writeJobInfo(); err != nil {
					pj.logger.Logf("error incrementing restart count for job (%s): %v", pj.ji.Job.ID, err)
				}

				// Reload the job's commitInfo as it may have changed
				pj.commitInfo, err = reg.driver.PachClient().InspectCommit(pj.commitInfo.Commit.Repo.Name, pj.commitInfo.Commit.ID)
				if err != nil {
					return err
				}

				if statsCommit != nil {
					pj.statsCommitInfo, err = reg.driver.PachClient().InspectCommit(statsCommit.Repo.Name, statsCommit.ID)
					if err != nil {
						return err
					}
				}

				return nil
			})
			pj.logger.Logf("master done running processJobs")
		}); err != nil {
			return err
		}

		// This should block until the callback has completed
		mutex.Lock()
		return nil
	})

	go func() {
		defer reg.limiter.Release()

		// Make sure the job has been removed from the job chain, ignore any errors
		defer reg.jobChain.Fail(pj)

		if err := asyncEg.Wait(); err != nil {
			pj.logger.Logf("fatal job error: %v", err)
		}

		if pj.ji.State == pps.JobState_JOB_EGRESSING {
			// Set up the driver for the egress context, which has different cancel conditions
			pj.driver = reg.driver.WithContext(egressCtx)

			// If egress fails, there isn't much we can do - the output commit is
			// already done, so the job is 'complete' regardless.
			pj.logger.LogStep("egressing job data", func() error {
				return reg.processJobEgress(pj)
			})
		}
	}()

	return nil
}

// superviseJob watches for the output commit closing and cancels the job, or
// deletes it if the output commit is removed.
func (reg *registry) superviseJob(pj *pendingJob) error {
	commitInfo, err := pj.driver.PachClient().PfsAPIClient.InspectCommit(pj.driver.PachClient().Ctx(),
		&pfs.InspectCommitRequest{
			Commit:     pj.ji.OutputCommit,
			BlockState: pfs.CommitState_FINISHED,
		})
	if err != nil {
		if pfsserver.IsCommitNotFoundErr(err) || pfsserver.IsCommitDeletedErr(err) {
			defer pj.cancel() // whether we return error or nil, job is done

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
	// commitInfo.Trees is set by non-S3-output jobs, while commitInfo.Tree is
	// set by S3-output jobs
	// TODO: why don't we cancel in all cases?
	if commitInfo.Trees == nil && commitInfo.Tree == nil {
		defer pj.cancel() // whether job state update succeeds or not, job is done
		return reg.killJob(pj, "output commit closed")
	}
	return nil
}

func (reg *registry) processJob(pj *pendingJob) error {
	state := pj.ji.State
	switch {
	case ppsutil.IsTerminal(state):
		return errutil.ErrBreak
	case state == pps.JobState_JOB_STARTING:
		return errors.New("job should have been moved out of the STARTING state before processJob")
	case state == pps.JobState_JOB_RUNNING:
		return pj.logger.LogStep("processing job datums", func() error {
			return reg.processJobRunning(pj)
		})
	case state == pps.JobState_JOB_MERGING:
		return pj.logger.LogStep("merging job hashtrees", func() error {
			return reg.processJobMerging(pj)
		})
	case state == pps.JobState_JOB_EGRESSING:
		return errutil.ErrBreak
	}
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
		return reg.failJob(pj, reason, nil, 0)
	}

	if pj.driver.PipelineInfo().S3Out && pj.commitInfo.ParentCommit != nil {
		// We don't want S3-out pipelines to merge datum output with the parent
		// commit, so we create a PutFile record to delete "/". Doing it before
		// we move the job to the RUNNING state ensures that:
		// 1) workers can't process datums unless DeleteFile("/") has run
		// 2) DeleteFile("/") won't run after work has started
		if err := pj.driver.PachClient().DeleteFile(
			pj.commitInfo.Commit.Repo.Name,
			pj.commitInfo.Commit.ID,
			"/",
		); err != nil {
			return errors.Wrap(err, "couldn't prepare output commit for S3-out job")
		}
	}

	pj.ji.State = pps.JobState_JOB_RUNNING
	return nil
}

// Iterator fulfills the chain.JobData interface for pendingJob
func (pj *pendingJob) Iterator() (datum.Iterator, error) {
	var dit datum.Iterator
	err := pj.logger.LogStep("constructing datum iterator", func() (err error) {
		dit, err = datum.NewIterator(pj.driver.PachClient(), pj.ji.Input)
		return
	})
	return dit, err
}

func (reg *registry) processJobRunning(pj *pendingJob) error {
	pj.logger.Logf("processJobRunning creating task channel")
	subtasks := make(chan *work.Task, 10)

	eg, ctx := errgroup.WithContext(reg.driver.PachClient().Ctx())

	// Spawn a goroutine to emit tasks on the datum task channel
	eg.Go(func() error {
		defer close(subtasks)
		return pj.logger.LogStep("collecting datums for tasks", func() error {
			for {
				numDatums, err := pj.jdit.NextBatch(ctx)
				if err != nil {
					return err
				}
				if numDatums == 0 {
					return nil
				}

				if err := reg.sendDatumTasks(ctx, pj, numDatums, subtasks); err != nil {
					return err
				}
			}
		})
	})

	mutex := &sync.Mutex{}
	stats := &DatumStats{ProcessStats: &pps.ProcessStats{}}
	chunkHashtrees := []*HashtreeInfo{}
	statsHashtrees := []*HashtreeInfo{}
	recoveredTags := []string{}

	// Run subtasks until we are done
	eg.Go(func() error {
		return pj.logger.LogStep("running datum tasks", func() error {
			return pj.taskMaster.RunSubtasksChan(
				subtasks,
				func(ctx context.Context, taskInfo *work.TaskInfo) error {
					if taskInfo.State == work.State_FAILURE {
						return errors.Errorf("datum task failed: %s", taskInfo.Reason)
					}

					data, err := deserializeDatumData(taskInfo.Task.Data)
					if err != nil {
						return err
					}

					mutex.Lock()
					defer mutex.Unlock()

					mergeStats(stats, data.Stats)

					if data.ChunkHashtree != nil {
						chunkHashtrees = append(chunkHashtrees, data.ChunkHashtree)
					}
					if data.StatsHashtree != nil {
						statsHashtrees = append(statsHashtrees, data.StatsHashtree)
					}
					if data.RecoveredDatumsTag != "" {
						recoveredTags = append(recoveredTags, data.RecoveredDatumsTag)
					}
					// propagate the stats to etcd
					pj.saveJobStats(stats)
					return pj.writeJobInfo()
				},
			)
		})
	})

	err := eg.Wait()
	if err != nil {
		// If these was no failed datum, we can reattempt later
		return errors.Wrap(err, "process datum error")
	}

	if stats.FailedDatumID != "" {
		// A datum failed, but we still may need to merge stats - discard chunk hashtrees
		chunkHashtrees = []*HashtreeInfo{}
	}

	// S3Out pipelines don't use hashtrees, so skip over the MERGING state - this
	// will go to EGRESSING, if applicable.
	if pj.driver.PipelineInfo().S3Out {
		if stats.FailedDatumID != "" {
			return reg.failJob(pj, "datum failed", nil, 0)
		}
		pj.logger.Logf("processJobRunning succeeding s3out job, total stats: %v", stats)
		return reg.succeedJob(pj, nil, 0, nil, 0)
	}

	// Write the hashtrees list and recovered datums list to object storage
	if err := pj.storeHashtreeInfos(chunkHashtrees, statsHashtrees); err != nil {
		return err
	}
	if err := pj.storeRecoveredTags(recoveredTags); err != nil {
		return err
	}

	pj.logger.Logf("processJobRunning updating task to merging, total stats: %v", stats)
	pj.ji.State = pps.JobState_JOB_MERGING
	pj.finalizeJobStats()
	return pj.writeJobInfo()
}

func (pj *pendingJob) saveJobStats(stats *DatumStats) {
	// Any unaccounted-for datums were skipped in the job datum iterator
	pj.ji.DataSkipped = stats.DatumsSkipped
	pj.ji.DataProcessed = stats.DatumsProcessed
	pj.ji.DataFailed = stats.DatumsFailed
	pj.ji.DataRecovered = stats.DatumsRecovered
	pj.ji.DataTotal = int64(pj.jdit.MaxLen())
	pj.ji.Stats = stats.ProcessStats
}

func (pj *pendingJob) finalizeJobStats() {
	pj.ji.DataSkipped = int64(pj.jdit.MaxLen()) - pj.ji.DataProcessed - pj.ji.DataFailed - pj.ji.DataRecovered
}

func (pj *pendingJob) storeHashtreeInfos(chunks []*HashtreeInfo, stats []*HashtreeInfo) error {
	pj.chunkHashtrees = chunks
	pj.statsHashtrees = stats

	tags := &HashtreeTags{ChunkTags: []string{}, StatsTags: []string{}}
	for _, info := range chunks {
		tags.ChunkTags = append(tags.ChunkTags, info.Tag)
	}
	for _, info := range stats {
		tags.StatsTags = append(tags.StatsTags, info.Tag)
	}

	buf := &bytes.Buffer{}
	pbw := pbutil.NewWriter(buf)
	if _, err := pbw.Write(tags); err != nil {
		return nil
	}

	_, _, err := pj.driver.PachClient().PutObject(buf, jobHashtreesTag(pj.driver.PipelineInfo().Pipeline.Name, pj.ji.Job.ID))
	return err
}

func (pj *pendingJob) storeRecoveredTags(recoveredTags []string) error {
	if len(recoveredTags) == 0 {
		return nil
	}

	tags := &RecoveredDatumTags{Tags: recoveredTags}

	buf := &bytes.Buffer{}
	pbw := pbutil.NewWriter(buf)
	if _, err := pbw.Write(tags); err != nil {
		return nil
	}

	_, _, err := pj.driver.PachClient().PutObject(buf, jobRecoveredDatumTagsTag(pj.driver.PipelineInfo().Pipeline.Name, pj.ji.Job.ID))
	return err
}

func (pj *pendingJob) initializeHashtrees() error {
	if pj.chunkHashtrees == nil {
		// We are picking up an old job and don't have the hashtrees generated by
		// the 'running' state, load them from object storage
		buf := &bytes.Buffer{}
		if err := pj.driver.PachClient().GetTag(jobHashtreesTag(pj.driver.PipelineInfo().Pipeline.Name, pj.ji.Job.ID), buf); err != nil {
			return err
		}
		protoReader := pbutil.NewReader(buf)

		hashtreeTags := &HashtreeTags{}
		if err := protoReader.Read(hashtreeTags); err != nil {
			return err
		}

		pj.chunkHashtrees = []*HashtreeInfo{}
		for _, tag := range hashtreeTags.ChunkTags {
			pj.chunkHashtrees = append(pj.chunkHashtrees, &HashtreeInfo{Tag: tag})
		}

		pj.statsHashtrees = []*HashtreeInfo{}
		for _, tag := range hashtreeTags.StatsTags {
			pj.statsHashtrees = append(pj.statsHashtrees, &HashtreeInfo{Tag: tag})
		}
	}
	return nil
}

func (pj *pendingJob) loadRecoveredDatums() (chain.DatumSet, error) {
	datumSet := make(chain.DatumSet)

	// We are picking up an old job and don't have the recovered datums generated by
	// the 'running' state, load them from object storage
	buf := &bytes.Buffer{}
	if err := pj.driver.PachClient().GetTag(jobRecoveredDatumTagsTag(pj.driver.PipelineInfo().Pipeline.Name, pj.ji.Job.ID), buf); err != nil {
		if strings.Contains(err.Error(), "not found") {
			return datumSet, nil
		}
		return nil, err
	}
	protoReader := pbutil.NewReader(buf)

	recoveredTags := &RecoveredDatumTags{}
	if err := protoReader.Read(recoveredTags); err != nil {
		return nil, err
	}

	recoveredDatums := &RecoveredDatums{}

	for _, tag := range recoveredTags.Tags {
		buf := &bytes.Buffer{}
		if err := pj.driver.PachClient().GetTag(tag, buf); err != nil {
			return nil, err
		}

		protoReader := pbutil.NewReader(buf)
		if err := protoReader.Read(recoveredDatums); err != nil {
			return nil, err
		}

		for _, hash := range recoveredDatums.Hashes {
			datumSet[hash]++
		}
	}

	return datumSet, nil
}

func (reg *registry) makeMergeSubtasks(pj *pendingJob, commitInfo *pfs.CommitInfo, stats bool) ([]*work.Task, error) {
	hashtrees := pj.chunkHashtrees
	if stats {
		hashtrees = pj.statsHashtrees
	}

	// For jobs that can base their hashtree off of the parent hashtree, fetch the
	// object information for the parent hashtrees
	// TODO: this is risky - there's no check that the parent the jobChain is
	// thinking of is the same one we find.  Add some extra guarantees here if we can.
	var parentHashtrees []*pfs.Object
	if pj.jdit.AdditiveOnly() {
		parentCommitInfo, err := reg.getParentCommitInfo(commitInfo)
		if err != nil {
			return nil, err
		}

		if parentCommitInfo != nil {
			parentHashtrees = parentCommitInfo.Trees
			if len(parentHashtrees) != int(reg.driver.NumShards()) {
				return nil, errors.Errorf(
					"unexpected number of hashtrees between the parent commit (%d) and the pipeline spec (%d)",
					len(parentHashtrees), reg.driver.NumShards(),
				)
			}
		}
	}

	if stats {
		if parentHashtrees != nil {
			pj.logger.Logf("merging %d stats hashtrees with parent hashtree across %d shards", len(hashtrees), reg.driver.NumShards())
		} else {
			pj.logger.Logf("merging %d stats hashtrees across %d shards", len(hashtrees), reg.driver.NumShards())
		}
	} else {
		if parentHashtrees != nil {
			pj.logger.Logf("merging %d hashtrees with parent hashtree across %d shards", len(hashtrees), reg.driver.NumShards())
		} else {
			pj.logger.Logf("merging %d hashtrees across %d shards", len(hashtrees), reg.driver.NumShards())
		}
	}

	mergeSubtasks := []*work.Task{}
	for i := int64(0); i < reg.driver.NumShards(); i++ {
		mergeData := &MergeData{Hashtrees: hashtrees, Shard: i, JobID: pj.ji.Job.ID, Stats: stats}

		if parentHashtrees != nil {
			mergeData.Parent = parentHashtrees[i]
		}

		data, err := serializeMergeData(mergeData)
		if err != nil {
			return nil, errors.Wrap(err, "failed to serialize merge data")
		}

		mergeSubtasks = append(mergeSubtasks, &work.Task{
			ID:   uuid.NewWithoutDashes(),
			Data: data,
		})
	}

	return mergeSubtasks, nil
}

func (reg *registry) processJobMerging(pj *pendingJob) error {
	if err := pj.initializeHashtrees(); err != nil {
		return err
	}

	mutex := &sync.Mutex{}
	mergeSubtasks := []*work.Task{}

	if pj.ji.DataFailed == 0 {
		chunkMergeSubtasks, err := reg.makeMergeSubtasks(pj, pj.commitInfo, false)
		if err != nil {
			return err
		}
		mergeSubtasks = append(mergeSubtasks, chunkMergeSubtasks...)
	}

	if pj.statsCommitInfo != nil {
		statsMergeSubtasks, err := reg.makeMergeSubtasks(pj, pj.statsCommitInfo, true)
		if err != nil {
			return err
		}
		mergeSubtasks = append(mergeSubtasks, statsMergeSubtasks...)
	}

	trees := make([]*pfs.Object, reg.driver.NumShards())
	size := uint64(0)
	statsTrees := make([]*pfs.Object, reg.driver.NumShards())
	statsSize := uint64(0)

	pj.logger.Logf("sending out %d merge tasks", len(mergeSubtasks))

	// Run merge subtasks and wait for them to complete
	if err := pj.taskMaster.RunSubtasks(
		mergeSubtasks,
		func(ctx context.Context, taskInfo *work.TaskInfo) error {
			if taskInfo.State == work.State_FAILURE {
				return errors.Errorf("merge task failed: %s", taskInfo.Reason)
			}

			data, err := deserializeMergeData(taskInfo.Task.Data)
			if err != nil {
				return err
			}

			if data.Tree == nil {
				return errors.Errorf("merge task for shard %d failed, no tree returned", data.Shard)
			}

			mutex.Lock()
			defer mutex.Unlock()

			if data.Stats {
				statsTrees[data.Shard] = data.Tree
				statsSize += data.TreeSize
			} else {
				trees[data.Shard] = data.Tree
				size += data.TreeSize
			}
			return nil
		},
	); err != nil {
		// TODO: persist error to job?
		return errors.Wrap(err, "merge error")
	}

	pj.logger.Logf("merge results: %v trees (%d bytes), %v stats trees (%d bytes)", trees, size, statsTrees, statsSize)

	if pj.ji.DataFailed == 0 {
		if err := reg.succeedJob(pj, trees, size, statsTrees, statsSize); err != nil {
			return err
		}
	} else if err := reg.failJob(pj, "datum failed", statsTrees, statsSize); err != nil {
		return err
	}
	return nil
}

func (reg *registry) storeJobDatums(pj *pendingJob) (*pfs.Object, error) {
	// Update recovered datums through the job chain, which will update its internal DatumSet
	recoveredDatums, err := pj.loadRecoveredDatums()
	if err != nil {
		return nil, err
	}

	if err := reg.jobChain.RecoveredDatums(pj, recoveredDatums); err != nil {
		return nil, err
	}

	// Write out the datums processed/skipped and merged for this job
	buf := &bytes.Buffer{}
	pbw := pbutil.NewWriter(buf)
	for hash, count := range pj.jdit.DatumSet() {
		for i := int64(0); i < count; i++ {
			if _, err := pbw.WriteBytes([]byte(hash)); err != nil {
				return nil, err
			}
		}
	}
	datums, _, err := pj.driver.PachClient().PutObject(buf)
	return datums, err
}

func (reg *registry) processJobEgress(pj *pendingJob) error {
	if err := reg.egress(pj); err != nil {
		return reg.failJob(pj, fmt.Sprintf("egress error: %v", err), nil, 0)
	}

	pj.ji.State = pps.JobState_JOB_SUCCESS
	return pj.writeJobInfo()
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
		if ci.Tree == nil && ci.Trees == nil {
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

func (reg *registry) egress(pj *pendingJob) error {
	var egressFailureCount int
	return backoff.RetryNotify(func() (retErr error) {
		if pj.ji.Egress != nil {
			return pj.logger.LogStep("egress upload", func() error {
				return pj.driver.Egress(pj.ji.OutputCommit, pj.ji.Egress.URL)
			})
		}
		return nil
	}, backoff.NewInfiniteBackOff(), func(err error, d time.Duration) error {
		egressFailureCount++
		if egressFailureCount > 3 {
			return err
		}
		pj.logger.Logf("egress failed: %v; retrying in %v", err, d)
		return nil
	})
}

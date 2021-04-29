package client

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"time"

	"github.com/pachyderm/pachyderm/v2/src/internal/errors"
	"github.com/pachyderm/pachyderm/v2/src/internal/errutil"
	"github.com/pachyderm/pachyderm/v2/src/internal/grpcutil"
	"github.com/pachyderm/pachyderm/v2/src/pfs"
	"github.com/pachyderm/pachyderm/v2/src/pps"

	"github.com/gogo/protobuf/types"
)

const (
	// PPSEtcdPrefixEnv is the environment variable that specifies the etcd
	// prefix that PPS uses.
	PPSEtcdPrefixEnv = "PPS_ETCD_PREFIX"
	// PPSWorkerIPEnv is the environment variable that a worker can use to
	// see its own IP.  The IP address is made available through the
	// Kubernetes downward API.
	PPSWorkerIPEnv = "PPS_WORKER_IP"
	// PPSPodNameEnv is the environment variable that a pod can use to
	// see its own name.  The pod name is made available through the
	// Kubernetes downward API.
	PPSPodNameEnv = "PPS_POD_NAME"
	// PPSPipelineNameEnv is the env var that sets the name of the pipeline
	// that the workers are running.
	PPSPipelineNameEnv = "PPS_PIPELINE_NAME"
	// PPSJobIDEnv is the env var that sets the ID of the job that the
	// workers are running (if the workers belong to an orphan job, rather than a
	// pipeline).
	PPSJobIDEnv = "PPS_JOB_ID"
	// PPSSpecCommitEnv is the namespace in which pachyderm is deployed
	PPSSpecCommitEnv = "PPS_SPEC_COMMIT"
	// PPSInputPrefix is the prefix of the path where datums are downloaded
	// to.  A datum of an input named `XXX` is downloaded to `/pfs/XXX/`.
	PPSInputPrefix = "/pfs"
	// PPSScratchSpace is where pps workers store data while it's waiting to be
	// processed.
	PPSScratchSpace = ".scratch"
	// PPSWorkerPortEnv is environment variable name for the port that workers
	// use for their gRPC server
	PPSWorkerPortEnv = "PPS_WORKER_GRPC_PORT"
	// PPSWorkerVolume is the name of the volume in which workers store
	// data.
	PPSWorkerVolume = "pachyderm-worker"
	// PPSWorkerUserContainerName is the name of the container that runs
	// the user code to process data.
	PPSWorkerUserContainerName = "user"
	// PPSWorkerSidecarContainerName is the name of the sidecar container
	// that runs alongside of each worker container.
	PPSWorkerSidecarContainerName = "storage"
	// GCGenerationKey is the etcd key that stores a counter that the
	// GC utility increments when it runs, so as to invalidate all cache.
	GCGenerationKey = "gc-generation"
	// JobIDEnv is an env var that is added to the environment of user pipeline
	// code and indicates the id of the job currently being run.
	JobIDEnv = "PACH_JOB_ID"
	// OutputCommitIDEnv is an env var that is added to the environment of user
	// pipelined code and indicates the id of the output commit.
	OutputCommitIDEnv = "PACH_OUTPUT_COMMIT_ID"
	// PeerPortEnv is the env var that sets a custom peer port
	PeerPortEnv = "PEER_PORT"

	// ReprocessSpecUntilSuccess indicates that a pipeline will, in each job,
	// reprocess every datum that has not yet been processed successfully
	ReprocessSpecUntilSuccess = "until_success"

	// ReprocessSpecEveryJob indicates that a pipeline will, in each job,
	// reprocess every datum in that job's input, regardless of whether the datum
	// has been successfully processed previously. This is useful for pipelines
	// with side effects (e.g. deploying other resources, ingesting data, etc.)
	ReprocessSpecEveryJob = "every_job"
)

// NewJob creates a pps.Job.
func NewJob(jobID string) *pps.Job {
	return &pps.Job{ID: jobID}
}

// DatumTagPrefix hashes a pipeline salt to a string of a fixed size for use as
// the prefix for datum output trees. This prefix allows us to do garbage
// collection correctly.
func DatumTagPrefix(salt string) string {
	// We need to hash the salt because UUIDs are not necessarily
	// random in every bit.
	h := sha256.New()
	h.Write([]byte(salt))
	return hex.EncodeToString(h.Sum(nil))[:4]
}

// NewPFSInput returns a new PFS input. It only includes required options.
func NewPFSInput(repo string, glob string) *pps.Input {
	return &pps.Input{
		Pfs: &pps.PFSInput{
			Repo: repo,
			Glob: glob,
		},
	}
}

// NewPFSInputOpts returns a new PFS input. It includes all options.
func NewPFSInputOpts(name string, repo string, branch string, glob string, joinOn string, groupBy string, outerJoin bool, lazy bool, trigger *pfs.Trigger) *pps.Input {
	return &pps.Input{
		Pfs: &pps.PFSInput{
			Name:      name,
			Repo:      repo,
			Branch:    branch,
			Glob:      glob,
			JoinOn:    joinOn,
			OuterJoin: outerJoin,
			GroupBy:   groupBy,
			Lazy:      lazy,
			Trigger:   trigger,
		},
	}
}

// NewS3PFSInput returns a new PFS input with 'S3' set.
func NewS3PFSInput(name string, repo string, branch string) *pps.Input {
	return &pps.Input{
		Pfs: &pps.PFSInput{
			Name:   name,
			Repo:   repo,
			Branch: branch,
			S3:     true,
		},
	}
}

// NewCrossInput returns an input which is the cross product of other inputs.
// That means that all combination of datums will be seen by the job /
// pipeline.
func NewCrossInput(input ...*pps.Input) *pps.Input {
	return &pps.Input{
		Cross: input,
	}
}

// NewJoinInput returns an input which is the join of other inputs.
// That means that all combination of datums which match on `joinOn` will be seen by the job /
// pipeline.
func NewJoinInput(input ...*pps.Input) *pps.Input {
	return &pps.Input{
		Join: input,
	}
}

// NewUnionInput returns an input which is the union of other inputs. That
// means that all datums from any of the inputs will be seen individually by
// the job / pipeline.
func NewUnionInput(input ...*pps.Input) *pps.Input {
	return &pps.Input{
		Union: input,
	}
}

// NewGroupInput returns an input which groups the inputs by the GroupBy pattern.
// That means that it will return a datum for each group of input datums matching
// a particular GroupBy pattern
func NewGroupInput(input ...*pps.Input) *pps.Input {
	return &pps.Input{
		Group: input,
	}
}

// NewCronInput returns an input which will trigger based on a timed schedule.
// It uses cron syntax to specify the schedule. The input will be exposed to
// jobs as `/pfs/<name>/<timestamp>`. The timestamp uses the RFC 3339 format,
// e.g. `2006-01-02T15:04:05Z07:00`. It only takes required options.
func NewCronInput(name string, spec string) *pps.Input {
	return &pps.Input{
		Cron: &pps.CronInput{
			Name: name,
			Spec: spec,
		},
	}
}

// NewCronInputOpts returns an input which will trigger based on a timed schedule.
// It uses cron syntax to specify the schedule. The input will be exposed to
// jobs as `/pfs/<name>/<timestamp>`. The timestamp uses the RFC 3339 format,
// e.g. `2006-01-02T15:04:05Z07:00`. It includes all the options.
func NewCronInputOpts(name string, repo string, spec string, overwrite bool) *pps.Input {
	return &pps.Input{
		Cron: &pps.CronInput{
			Name:      name,
			Repo:      repo,
			Spec:      spec,
			Overwrite: overwrite,
		},
	}
}

// NewJobInput creates a pps.JobInput.
func NewJobInput(repoName string, commitID string, glob string) *pps.JobInput {
	return &pps.JobInput{
		Commit: NewCommit(repoName, commitID),
		Glob:   glob,
	}
}

// NewPipeline creates a pps.Pipeline.
func NewPipeline(pipelineName string) *pps.Pipeline {
	return &pps.Pipeline{Name: pipelineName}
}

// CreateJob creates and runs a job in PPS.
// This function is mostly useful internally, users should generally run work
// by creating pipelines as well.
func (c APIClient) CreateJob(pipeline string, outputCommit, statsCommit *pfs.Commit) (*pps.Job, error) {
	job, err := c.PpsAPIClient.CreateJob(
		c.Ctx(),
		&pps.CreateJobRequest{
			Pipeline:     NewPipeline(pipeline),
			OutputCommit: outputCommit,
			StatsCommit:  statsCommit,
		},
	)
	return job, grpcutil.ScrubGRPC(err)
}

// InspectJob returns info about a specific job.
// blockState will cause the call to block until the job reaches a terminal state (failure or success).
// full indicates that the full job info should be returned.
func (c APIClient) InspectJob(jobID string, blockState bool, full ...bool) (*pps.JobInfo, error) {
	req := &pps.InspectJobRequest{
		Job:        NewJob(jobID),
		BlockState: blockState,
	}
	if len(full) > 0 {
		req.Full = full[0]
	}
	jobInfo, err := c.PpsAPIClient.InspectJob(c.Ctx(), req)
	return jobInfo, grpcutil.ScrubGRPC(err)
}

// InspectJobOutputCommit returns info about a job that created a commit.
// blockState will cause the call to block until the job reaches a terminal state (failure or success).
func (c APIClient) InspectJobOutputCommit(repoName, commitID string, blockState bool) (*pps.JobInfo, error) {
	jobInfo, err := c.PpsAPIClient.InspectJob(
		c.Ctx(),
		&pps.InspectJobRequest{
			OutputCommit: NewCommit(repoName, commitID),
			BlockState:   blockState,
		})
	return jobInfo, grpcutil.ScrubGRPC(err)
}

// ListJob returns info about all jobs.
// If pipelineName is non empty then only jobs that were started by the named pipeline will be returned
// If inputCommit is non-nil then only jobs which took the specific commits as inputs will be returned.
// The order of the inputCommits doesn't matter.
// If outputCommit is non-nil then only the job which created that commit as output will be returned.
// 'history' controls whether jobs from historical versions of pipelines are returned, it has the following semantics:
// 0: Return jobs from the current version of the pipeline or pipelines.
// 1: Return the above and jobs from the next most recent version
// 2: etc.
//-1: Return jobs from all historical versions.
// 'includePipelineInfo' controls whether the JobInfo passed to 'f' includes
// details fromt the pipeline spec (e.g. the transform). Leaving this 'false'
// can improve performance.
func (c APIClient) ListJob(pipelineName string, inputCommit []*pfs.Commit, outputCommit *pfs.Commit, history int64, includePipelineInfo bool) ([]*pps.JobInfo, error) {
	var result []*pps.JobInfo
	if err := c.ListJobF(pipelineName, inputCommit, outputCommit, history,
		includePipelineInfo, func(ji *pps.JobInfo) error {
			result = append(result, ji)
			return nil
		}); err != nil {
		return nil, err
	}
	return result, nil
}

// ListJobF is a previous version of ListJobFilterF, returning info about all jobs
// and calling f on each JobInfo
func (c APIClient) ListJobF(pipelineName string, inputCommit []*pfs.Commit,
	outputCommit *pfs.Commit, history int64, includePipelineInfo bool,
	f func(*pps.JobInfo) error) error {
	return c.ListJobFilterF(pipelineName, inputCommit, outputCommit, history, includePipelineInfo, "", f)
}

// ListJobFilterF returns info about all jobs, calling f with each JobInfo.
// If f returns an error iteration of jobs will stop and ListJobF will return
// that error, unless the error is errutil.ErrBreak in which case it will
// return nil.
// If pipelineName is non empty then only jobs that were started by the named pipeline will be returned
// If inputCommit is non-nil then only jobs which took the specific commits as inputs will be returned.
// The order of the inputCommits doesn't matter.
// If outputCommit is non-nil then only the job which created that commit as output will be returned.
// 'history' controls whether jobs from historical versions of pipelines are returned, it has the following semantics:
// 0: Return jobs from the current version of the pipeline or pipelines.
// 1: Return the above and jobs from the next most recent version
// 2: etc.
//-1: Return jobs from all historical versions.
// 'includePipelineInfo' controls whether the JobInfo passed to 'f' includes
// details fromt the pipeline spec--setting this to 'false' can improve
// performance.
func (c APIClient) ListJobFilterF(pipelineName string, inputCommit []*pfs.Commit,
	outputCommit *pfs.Commit, history int64, includePipelineInfo bool, jqFilter string,
	f func(*pps.JobInfo) error) error {
	var pipeline *pps.Pipeline
	if pipelineName != "" {
		pipeline = NewPipeline(pipelineName)
	}
	client, err := c.PpsAPIClient.ListJob(
		c.Ctx(),
		&pps.ListJobRequest{
			Pipeline:     pipeline,
			InputCommit:  inputCommit,
			OutputCommit: outputCommit,
			History:      history,
			Full:         includePipelineInfo,
			JqFilter:     jqFilter,
		})
	if err != nil {
		return grpcutil.ScrubGRPC(err)
	}
	for {
		ji, err := client.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		} else if err != nil {
			return grpcutil.ScrubGRPC(err)
		}
		if err := f(ji); err != nil {
			if errors.Is(err, errutil.ErrBreak) {
				return nil
			}
			return err
		}
	}
}

// FlushJob calls f with all the jobs which were triggered by commits.
// If toPipelines is non-nil then only the jobs between commits and those
// pipelines in the DAG will be returned.
func (c APIClient) FlushJob(commits []*pfs.Commit, toPipelines []string, f func(*pps.JobInfo) error) error {
	req := &pps.FlushJobRequest{
		Commits: commits,
	}
	for _, pipeline := range toPipelines {
		req.ToPipelines = append(req.ToPipelines, NewPipeline(pipeline))
	}
	client, err := c.PpsAPIClient.FlushJob(c.Ctx(), req)
	if err != nil {
		return grpcutil.ScrubGRPC(err)
	}
	for {
		jobInfo, err := client.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return grpcutil.ScrubGRPC(err)
		}
		if err := f(jobInfo); err != nil {
			return err
		}
	}
}

// FlushJobAll returns all the jobs which were triggered by commits.
// If toPipelines is non-nil then only the jobs between commits and those
// pipelines in the DAG will be returned.
func (c APIClient) FlushJobAll(commits []*pfs.Commit, toPipelines []string) ([]*pps.JobInfo, error) {
	var result []*pps.JobInfo
	if err := c.FlushJob(commits, toPipelines, func(ji *pps.JobInfo) error {
		result = append(result, ji)
		return nil
	}); err != nil {
		return nil, err
	}
	return result, nil
}

// DeleteJob deletes a job.
func (c APIClient) DeleteJob(jobID string) error {
	_, err := c.PpsAPIClient.DeleteJob(
		c.Ctx(),
		&pps.DeleteJobRequest{
			Job: NewJob(jobID),
		},
	)
	return grpcutil.ScrubGRPC(err)
}

// StopJob stops a job.
func (c APIClient) StopJob(jobID string) error {
	_, err := c.PpsAPIClient.StopJob(
		c.Ctx(),
		&pps.StopJobRequest{
			Job: NewJob(jobID),
		},
	)
	return grpcutil.ScrubGRPC(err)
}

// StopJobOutputCommit stops a job associated with an output commit.
func (c APIClient) StopJobOutputCommit(repo, commit string) (retErr error) {
	defer func() {
		retErr = grpcutil.ScrubGRPC(retErr)
	}()
	_, err := c.PpsAPIClient.StopJob(
		c.Ctx(),
		&pps.StopJobRequest{
			OutputCommit: NewCommit(repo, commit),
		},
	)
	return err
}

// RestartDatum restarts a datum that's being processed as part of a job.
// datumFilter is a slice of strings which are matched against either the Path
// or Hash of the datum, the order of the strings in datumFilter is irrelevant.
func (c APIClient) RestartDatum(jobID string, datumFilter []string) error {
	_, err := c.PpsAPIClient.RestartDatum(
		c.Ctx(),
		&pps.RestartDatumRequest{
			Job:         NewJob(jobID),
			DataFilters: datumFilter,
		},
	)
	return grpcutil.ScrubGRPC(err)
}

// ListDatum returns info about datums in a job.
func (c APIClient) ListDatum(job string, cb func(*pps.DatumInfo) error) (retErr error) {
	defer func() {
		retErr = grpcutil.ScrubGRPC(retErr)
	}()
	client, err := c.PpsAPIClient.ListDatum(
		c.Ctx(),
		&pps.ListDatumRequest{
			Job: NewJob(job),
		},
	)
	if err != nil {
		return err
	}
	for {
		di, err := client.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if err := cb(di); err != nil {
			if errors.Is(err, errutil.ErrBreak) {
				return nil
			}
			return err
		}
	}
}

// ListDatumAll returns info about datums in a job.
func (c APIClient) ListDatumAll(job string) (_ []*pps.DatumInfo, retErr error) {
	defer func() {
		retErr = grpcutil.ScrubGRPC(retErr)
	}()
	var dis []*pps.DatumInfo
	if err := c.ListDatum(job, func(di *pps.DatumInfo) error {
		dis = append(dis, di)
		return nil
	}); err != nil {
		return nil, err
	}
	return dis, nil
}

// InspectDatum returns info about a single datum
func (c APIClient) InspectDatum(jobID string, datumID string) (*pps.DatumInfo, error) {
	datumInfo, err := c.PpsAPIClient.InspectDatum(
		c.Ctx(),
		&pps.InspectDatumRequest{
			Datum: &pps.Datum{
				ID:  datumID,
				Job: NewJob(jobID),
			},
		},
	)
	if err != nil {
		return nil, grpcutil.ScrubGRPC(err)
	}
	return datumInfo, nil
}

// LogsIter iterates through log messages returned from pps.GetLogs. Logs can
// be fetched with 'Next()'. The log message received can be examined with
// 'Message()', and any errors can be examined with 'Err()'.
type LogsIter struct {
	logsClient pps.API_GetLogsClient
	msg        *pps.LogMessage
	err        error
}

// Next retrieves the next relevant log message from pachd
func (l *LogsIter) Next() bool {
	if l.err != nil {
		l.msg = nil
		return false
	}
	l.msg, l.err = l.logsClient.Recv()
	return l.err == nil
}

// Message returns the most recently retrieve log message (as an annotated log
// line, in the form of a pps.LogMessage)
func (l *LogsIter) Message() *pps.LogMessage {
	return l.msg
}

// Err retrieves any errors encountered in the course of calling 'Next()'.
func (l *LogsIter) Err() error {
	if errors.Is(l.err, io.EOF) {
		return nil
	}
	return grpcutil.ScrubGRPC(l.err)
}

// GetLogs gets logs from a job (logs includes stdout and stderr). 'pipelineName',
// 'jobID', 'data', and 'datumID', are all filters. To forego any filter,
// simply pass an empty value, though one of 'pipelineName' and 'jobID'
// must be set. Responses are written to 'messages'
func (c APIClient) GetLogs(
	pipelineName string,
	jobID string,
	data []string,
	datumID string,
	master bool,
	follow bool,
	since time.Duration,
) *LogsIter {
	return c.getLogs(pipelineName, jobID, data, datumID, master, follow, since, false)
}

// GetLogsLoki gets logs from a job (logs includes stdout and stderr). 'pipelineName',
// 'jobID', 'data', and 'datumID', are all filters. To forego any filter,
// simply pass an empty value, though one of 'pipelineName' and 'jobID'
// must be set. Responses are written to 'messages'
func (c APIClient) GetLogsLoki(
	pipelineName string,
	jobID string,
	data []string,
	datumID string,
	master bool,
	follow bool,
	since time.Duration,
) *LogsIter {
	return c.getLogs(pipelineName, jobID, data, datumID, master, follow, since, true)
}

func (c APIClient) getLogs(
	pipelineName string,
	jobID string,
	data []string,
	datumID string,
	master bool,
	follow bool,
	since time.Duration,
	useLoki bool,
) *LogsIter {
	request := pps.GetLogsRequest{
		Master:         master,
		Follow:         follow,
		UseLokiBackend: useLoki,
		Since:          types.DurationProto(since),
	}
	if pipelineName != "" {
		request.Pipeline = NewPipeline(pipelineName)
	}
	if jobID != "" {
		request.Job = NewJob(jobID)
	}
	request.DataFilters = data
	if datumID != "" {
		request.Datum = &pps.Datum{
			Job: NewJob(jobID),
			ID:  datumID,
		}
	}
	resp := &LogsIter{}
	resp.logsClient, resp.err = c.PpsAPIClient.GetLogs(c.Ctx(), &request)
	resp.err = grpcutil.ScrubGRPC(resp.err)
	return resp
}

// CreatePipeline creates a new pipeline, pipelines are the main computation
// object in PPS they create a flow of data from a set of input Repos to an
// output Repo (which has the same name as the pipeline). Whenever new data is
// committed to one of the input repos the pipelines will create jobs to bring
// the output Repo up to data.
// image is the Docker image to run the jobs in.
// cmd is the command passed to the Docker run invocation.
// NOTE as with Docker cmd is not run inside a shell that means that things
// like wildcard globbing (*), pipes (|) and file redirects (> and >>) will not
// work. To get that behavior you should have your command be a shell of your
// choice and pass a shell script to stdin.
// stdin is a slice of lines that are sent to your command on stdin. Lines need
// not end in newline characters.
// parallelism is how many copies of your container should run in parallel. You
// may pass 0 for parallelism in which case PPS will set the parallelism based
// on available resources.
// input specifies a set of Repos that will be visible to the jobs during runtime.
// commits to these repos will cause the pipeline to create new jobs to process them.
// update indicates that you want to update an existing pipeline
func (c APIClient) CreatePipeline(
	name string,
	image string,
	cmd []string,
	stdin []string,
	parallelismSpec *pps.ParallelismSpec,
	input *pps.Input,
	outputBranch string,
	update bool,
) error {
	_, err := c.PpsAPIClient.CreatePipeline(
		c.Ctx(),
		&pps.CreatePipelineRequest{
			Pipeline: NewPipeline(name),
			Transform: &pps.Transform{
				Image: image,
				Cmd:   cmd,
				Stdin: stdin,
			},
			ParallelismSpec: parallelismSpec,
			Input:           input,
			OutputBranch:    outputBranch,
			Update:          update,
		},
	)
	return grpcutil.ScrubGRPC(err)
}

// InspectPipeline returns info about a specific pipeline.
func (c APIClient) InspectPipeline(pipelineName string) (*pps.PipelineInfo, error) {
	pipelineInfo, err := c.PpsAPIClient.InspectPipeline(
		c.Ctx(),
		&pps.InspectPipelineRequest{
			Pipeline: NewPipeline(pipelineName),
		},
	)
	return pipelineInfo, grpcutil.ScrubGRPC(err)
}

// ListPipeline returns info about all pipelines.
func (c APIClient) ListPipeline() ([]*pps.PipelineInfo, error) {
	pipelineInfos, err := c.PpsAPIClient.ListPipeline(
		c.Ctx(),
		&pps.ListPipelineRequest{},
	)
	if err != nil {
		return nil, grpcutil.ScrubGRPC(err)
	}
	return pipelineInfos.PipelineInfo, nil
}

// ListPipelineHistory returns historical information about pipelines.
// `pipeline` specifies which pipeline to return history about, if it's equal
// to "" then ListPipelineHistory returns historical information about all
// pipelines.
// `history` specifies how many historical revisions to return:
// 0: Return the current version of the pipeline or pipelines.
// 1: Return the above and the next most recent version
// 2: etc.
//-1: Return all historical versions.
func (c APIClient) ListPipelineHistory(pipeline string, history int64) ([]*pps.PipelineInfo, error) {
	var _pipeline *pps.Pipeline
	if pipeline != "" {
		_pipeline = NewPipeline(pipeline)
	}
	pipelineInfos, err := c.PpsAPIClient.ListPipeline(
		c.Ctx(),
		&pps.ListPipelineRequest{
			Pipeline: _pipeline,
			History:  history,
		},
	)
	if err != nil {
		return nil, grpcutil.ScrubGRPC(err)
	}
	return pipelineInfos.PipelineInfo, nil
}

// DeletePipeline deletes a pipeline along with its output Repo.
func (c APIClient) DeletePipeline(name string, force bool) error {
	req := &pps.DeletePipelineRequest{
		Pipeline: NewPipeline(name),
		Force:    force,
	}
	_, err := c.PpsAPIClient.DeletePipeline(
		c.Ctx(),
		req,
	)
	return grpcutil.ScrubGRPC(err)
}

// StartPipeline restarts a stopped pipeline.
func (c APIClient) StartPipeline(name string) error {
	_, err := c.PpsAPIClient.StartPipeline(
		c.Ctx(),
		&pps.StartPipelineRequest{
			Pipeline: NewPipeline(name),
		},
	)
	return grpcutil.ScrubGRPC(err)
}

// StopPipeline prevents a pipeline from processing things, it can be restarted
// with StartPipeline.
func (c APIClient) StopPipeline(name string) error {
	_, err := c.PpsAPIClient.StopPipeline(
		c.Ctx(),
		&pps.StopPipelineRequest{
			Pipeline: NewPipeline(name),
		},
	)
	return grpcutil.ScrubGRPC(err)
}

// RunPipeline runs a pipeline. It can be passed a list of commit provenance.
// This will trigger a new job provenant on those commits, effectively running the pipeline on the data in those commits.
func (c APIClient) RunPipeline(name string, provenance []*pfs.CommitProvenance, jobID string) error {
	_, err := c.PpsAPIClient.RunPipeline(
		c.Ctx(),
		&pps.RunPipelineRequest{
			Pipeline:   NewPipeline(name),
			Provenance: provenance,
			JobID:      jobID,
		},
	)
	return grpcutil.ScrubGRPC(err)
}

// RunCron runs a pipeline. It can be passed a list of commit provenance.
// This will trigger a new job provenant on those commits, effectively running the pipeline on the data in those commits.
func (c APIClient) RunCron(name string) error {
	_, err := c.PpsAPIClient.RunCron(
		c.Ctx(),
		&pps.RunCronRequest{
			Pipeline: NewPipeline(name),
		},
	)
	return grpcutil.ScrubGRPC(err)
}

// CreateSecret creates a secret on the cluster.
func (c APIClient) CreateSecret(file []byte) error {
	_, err := c.PpsAPIClient.CreateSecret(
		c.Ctx(),
		&pps.CreateSecretRequest{
			File: file,
		},
	)
	return grpcutil.ScrubGRPC(err)
}

// DeleteSecret deletes a secret from the cluster.
func (c APIClient) DeleteSecret(secret string) error {
	_, err := c.PpsAPIClient.DeleteSecret(
		c.Ctx(),
		&pps.DeleteSecretRequest{
			Secret: &pps.Secret{Name: secret},
		},
	)
	return grpcutil.ScrubGRPC(err)
}

// InspectSecret returns info about a specific secret.
func (c APIClient) InspectSecret(secret string) (*pps.SecretInfo, error) {
	secretInfo, err := c.PpsAPIClient.InspectSecret(
		c.Ctx(),
		&pps.InspectSecretRequest{
			Secret: &pps.Secret{Name: secret},
		},
	)
	return secretInfo, grpcutil.ScrubGRPC(err)
}

// ListSecret returns info about all Pachyderm secrets.
func (c APIClient) ListSecret() ([]*pps.SecretInfo, error) {
	secretInfos, err := c.PpsAPIClient.ListSecret(
		c.Ctx(),
		&types.Empty{},
	)
	if err != nil {
		return nil, grpcutil.ScrubGRPC(err)
	}
	return secretInfos.SecretInfo, nil
}

// CreatePipelineService creates a new pipeline service.
func (c APIClient) CreatePipelineService(
	name string,
	image string,
	cmd []string,
	stdin []string,
	parallelismSpec *pps.ParallelismSpec,
	input *pps.Input,
	update bool,
	internalPort int32,
	externalPort int32,
	annotations map[string]string,
) error {
	_, err := c.PpsAPIClient.CreatePipeline(
		c.Ctx(),
		&pps.CreatePipelineRequest{
			Pipeline: NewPipeline(name),
			Metadata: &pps.Metadata{
				Annotations: annotations,
			},
			Transform: &pps.Transform{
				Image: image,
				Cmd:   cmd,
				Stdin: stdin,
			},
			ParallelismSpec: parallelismSpec,
			Input:           input,
			Update:          update,
			Service: &pps.Service{
				InternalPort: internalPort,
				ExternalPort: externalPort,
			},
		},
	)
	return grpcutil.ScrubGRPC(err)
}

// GetDatumTotalTime sums the timing stats from a DatumInfo
func GetDatumTotalTime(s *pps.ProcessStats) time.Duration {
	totalDuration := time.Duration(0)
	duration, _ := types.DurationFromProto(s.DownloadTime)
	totalDuration += duration
	duration, _ = types.DurationFromProto(s.ProcessTime)
	totalDuration += duration
	duration, _ = types.DurationFromProto(s.UploadTime)
	totalDuration += duration
	return totalDuration
}

package client

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"

	"github.com/pachyderm/pachyderm/src/client/pfs"
	"github.com/pachyderm/pachyderm/src/client/pps"
)

// NewJob creates a pps.Job.
func NewJob(jobID string) *pps.Job {
	return &pps.Job{ID: jobID}
}

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
	// PPSInputPrefix is the prefix of the path where datums are downloaded
	// to.  A datum of an input named `XXX` is downloaded to `/pfs/XXX/`.
	PPSInputPrefix = "/pfs"
	// PPSOutputPath is the path where the user code is
	// expected to write its output to.
	PPSOutputPath = "/pfs/out"
	// PPSWorkerPort is the port that workers use for their gRPC server
	PPSWorkerPort = 80
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
)

// HashPipelineID hashes a pipeline ID to a string of a fixed size
func HashPipelineID(pipelineID string) string {
	// We need to hash the pipeline ID because UUIDs are not necessarily
	// random in every bit.
	pipelineIDHash := sha256.New()
	pipelineIDHash.Write([]byte(pipelineID))
	return hex.EncodeToString(pipelineIDHash.Sum(nil))[:4]
}

// NewAtomInput returns a new atom input. It only includes required options.
func NewAtomInput(repo string, glob string) *pps.Input {
	return &pps.Input{
		Atom: &pps.AtomInput{
			Repo: repo,
			Glob: glob,
		},
	}
}

// NewAtomInputOpts returns a new atom input. It includes all options.
func NewAtomInputOpts(name string, repo string, branch string, glob string, lazy bool, fromCommit string) *pps.Input {
	return &pps.Input{
		Atom: &pps.AtomInput{
			Name:       name,
			Repo:       repo,
			Branch:     branch,
			Glob:       glob,
			Lazy:       lazy,
			FromCommit: fromCommit,
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

// NewUnionInput returns an input which is the union of other inputs. That
// means that all datums from any of the inputs will be seen individually by
// the job / pipeline.
func NewUnionInput(input ...*pps.Input) *pps.Input {
	return &pps.Input{
		Union: input,
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

// NewPipelineInput creates a new pps.PipelineInput
func NewPipelineInput(repoName string, glob string) *pps.PipelineInput {
	return &pps.PipelineInput{
		Repo: NewRepo(repoName),
		Glob: glob,
	}
}

// CreateJob creates and runs a job in PPS.
// image is the Docker image to run the job in.
// cmd is the command passed to the Docker run invocation.
// NOTE as with Docker cmd is not run inside a shell that means that things
// like wildcard globbing (*), pipes (|) and file redirects (> and >>) will not
// work. To get that behavior you should have your command be a shell of your
// choice and pass a shell script to stdin.
// stdin is a slice of lines that are sent to your command on stdin. Lines need
// not end in newline characters.
// parallelism is how many copies of your container should run in parallel. You
// may pass 0 for parallelism in which case PPS will set the parallelism based
// on availabe resources.
// input specifies a set of Commits that will be visible to the job during runtime.
// parentJobID specifies the a job to use as a parent, it may be left empty in
// which case there is no parent job. If not left empty your job will use the
// parent Job's output commit as the parent of its output commit.
func (c APIClient) CreateJob(
	image string,
	cmd []string,
	stdin []string,
	parallelismSpec *pps.ParallelismSpec,
	input *pps.Input,
	internalPort int32,
	externalPort int32,
) (*pps.Job, error) {
	var service *pps.Service
	if internalPort != 0 {
		service = &pps.Service{
			InternalPort: internalPort,
		}
	}
	if externalPort != 0 {
		if internalPort == 0 {
			return nil, fmt.Errorf("external port specified without internal port")
		}
		service.ExternalPort = externalPort
	}
	job, err := c.PpsAPIClient.CreateJob(
		c.ctx(),
		&pps.CreateJobRequest{
			Transform: &pps.Transform{
				Image: image,
				Cmd:   cmd,
				Stdin: stdin,
			},
			ParallelismSpec: parallelismSpec,
			Input:           input,
			Service:         service,
		},
	)
	return job, sanitizeErr(err)
}

// InspectJob returns info about a specific job.
// blockOutput will cause the call to block until the job has been assigned an output commit.
// blockState will cause the call to block until the job reaches a terminal state (failure or success).
func (c APIClient) InspectJob(jobID string, blockState bool) (*pps.JobInfo, error) {
	jobInfo, err := c.PpsAPIClient.InspectJob(
		c.ctx(),
		&pps.InspectJobRequest{
			Job:        NewJob(jobID),
			BlockState: blockState,
		})
	return jobInfo, sanitizeErr(err)
}

// ListJob returns info about all jobs.
// If pipelineName is non empty then only jobs that were started by the named pipeline will be returned
// If inputCommit is non-nil then only jobs which took the specific commits as inputs will be returned.
// The order of the inputCommits doesn't matter.
func (c APIClient) ListJob(pipelineName string, inputCommit []*pfs.Commit) ([]*pps.JobInfo, error) {
	var pipeline *pps.Pipeline
	if pipelineName != "" {
		pipeline = NewPipeline(pipelineName)
	}
	jobInfos, err := c.PpsAPIClient.ListJob(
		c.ctx(),
		&pps.ListJobRequest{
			Pipeline:    pipeline,
			InputCommit: inputCommit,
		})
	if err != nil {
		return nil, sanitizeErr(err)
	}
	return jobInfos.JobInfo, nil
}

// DeleteJob deletes a job.
func (c APIClient) DeleteJob(jobID string) error {
	_, err := c.PpsAPIClient.DeleteJob(
		c.ctx(),
		&pps.DeleteJobRequest{
			Job: NewJob(jobID),
		},
	)
	return sanitizeErr(err)
}

// StopJob stops a job.
func (c APIClient) StopJob(jobID string) error {
	_, err := c.PpsAPIClient.StopJob(
		c.ctx(),
		&pps.StopJobRequest{
			Job: NewJob(jobID),
		},
	)
	return sanitizeErr(err)
}

// RestartDatum restarts a datum that's being processed as part of a job.
// datumFilter is a slice of strings which are matched against either the Path
// or Hash of the datum, the order of the strings in datumFilter is irrelevant.
func (c APIClient) RestartDatum(jobID string, datumFilter []string) error {
	_, err := c.PpsAPIClient.RestartDatum(
		c.ctx(),
		&pps.RestartDatumRequest{
			Job:         NewJob(jobID),
			DataFilters: datumFilter,
		},
	)
	return sanitizeErr(err)
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
	if l.err != nil {
		return false
	}
	return true
}

// Message returns the most recently retrieve log message (as an annotated log
// line, in the form of a pps.LogMessage)
func (l *LogsIter) Message() *pps.LogMessage {
	return l.msg
}

// Err retrieves any errors encountered in the course of calling 'Next()'.
func (l *LogsIter) Err() error {
	if l.err == io.EOF {
		return nil
	}
	return l.err
}

// GetLogs gets logs from a job (logs includes stdout and stderr). 'pipelineName',
// 'jobID', and 'data', are all filters. To forego any filter, simply pass an
// empty value, though one of 'pipelineName' and 'jobID' must be set. Responses
// are written to 'messages'
func (c APIClient) GetLogs(
	pipelineName string,
	jobID string,
	data []string,
) *LogsIter {
	request := pps.GetLogsRequest{}
	resp := &LogsIter{}
	if pipelineName != "" {
		request.Pipeline = &pps.Pipeline{pipelineName}
	}
	if jobID != "" {
		request.Job = &pps.Job{jobID}
	}
	request.DataFilters = data
	resp.logsClient, resp.err = c.PpsAPIClient.GetLogs(c.ctx(), &request)
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
// on availabe resources.
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
		c.ctx(),
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
	return sanitizeErr(err)
}

// InspectPipeline returns info about a specific pipeline.
func (c APIClient) InspectPipeline(pipelineName string) (*pps.PipelineInfo, error) {
	pipelineInfo, err := c.PpsAPIClient.InspectPipeline(
		c.ctx(),
		&pps.InspectPipelineRequest{
			Pipeline: NewPipeline(pipelineName),
		},
	)
	return pipelineInfo, sanitizeErr(err)
}

// ListPipeline returns info about all pipelines.
func (c APIClient) ListPipeline() ([]*pps.PipelineInfo, error) {
	pipelineInfos, err := c.PpsAPIClient.ListPipeline(
		c.ctx(),
		&pps.ListPipelineRequest{},
	)
	if err != nil {
		return nil, sanitizeErr(err)
	}
	return pipelineInfos.PipelineInfo, nil
}

// DeletePipeline deletes a pipeline along with its output Repo.
func (c APIClient) DeletePipeline(name string, deleteJobs bool) error {
	_, err := c.PpsAPIClient.DeletePipeline(
		c.ctx(),
		&pps.DeletePipelineRequest{
			Pipeline:   NewPipeline(name),
			DeleteJobs: deleteJobs,
		},
	)
	return sanitizeErr(err)
}

// StartPipeline restarts a stopped pipeline.
func (c APIClient) StartPipeline(name string) error {
	_, err := c.PpsAPIClient.StartPipeline(
		c.ctx(),
		&pps.StartPipelineRequest{
			Pipeline: NewPipeline(name),
		},
	)
	return sanitizeErr(err)
}

// StopPipeline prevents a pipeline from processing things, it can be restarted
// with StartPipeline.
func (c APIClient) StopPipeline(name string) error {
	_, err := c.PpsAPIClient.StopPipeline(
		c.ctx(),
		&pps.StopPipelineRequest{
			Pipeline: NewPipeline(name),
		},
	)
	return sanitizeErr(err)
}

// RerunPipeline reruns a pipeline over a given set of commits. Exclude and
// include are filters that either include or exclude the ancestors of the
// given commits.  A commit is considered the ancestor of itself. The behavior
// is the same as that of ListCommit.
func (c APIClient) RerunPipeline(name string, include []*pfs.Commit, exclude []*pfs.Commit) error {
	_, err := c.PpsAPIClient.RerunPipeline(
		c.ctx(),
		&pps.RerunPipelineRequest{
			Pipeline: NewPipeline(name),
			Include:  include,
			Exclude:  exclude,
		},
	)
	return sanitizeErr(err)
}

// FlushCommit iterates over a set of jobs that have the specified commits as
// inputs (or inputs of inputs).  It blocks until the jobs have completed.
//
// If toRepos is not nil then only the commits up to and including those
// repos will be considered, otherwise all repos are considered.
//
// Note that it's never necessary to call FlushCommit to run jobs, they'll
// run no matter what, FlushCommit just allows you to wait for them to
// complete and see their output once they do.
func (c APIClient) FlushCommit(commits []*pfs.Commit, toRepos []*pfs.Repo, f func(jobInfo *pps.JobInfo) error) error {
	ctx, cancel := context.WithCancel(c.ctx())
	defer cancel()
	stream, err := c.PpsAPIClient.FlushCommit(
		ctx,
		&pps.FlushCommitRequest{
			Commits: commits,
			ToRepos: toRepos,
		},
	)
	if err != nil {
		return sanitizeErr(err)
	}
	for {
		jobInfo, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return sanitizeErr(err)
		}
		if err := f(jobInfo); err != nil {
			return err
		}
	}
}

// FlushCommitAll is like FlushCommit except instead of iterating over a set of
// jobs as they're returned it blocks until they're all available and then
// returns them as a slice.
func (c APIClient) FlushCommitAll(commits []*pfs.Commit, toRepos []*pfs.Repo) ([]*pps.JobInfo, error) {
	var result []*pps.JobInfo
	if err := c.FlushCommit(commits, toRepos, func(jobInfo *pps.JobInfo) error {
		result = append(result, jobInfo)
		return nil
	}); err != nil {
		return nil, err
	}
	return result, nil
}

// GarbageCollect garbage collects unused data.  Currently GC needs to be
// run while no data is being added or removed (which, among other things,
// implies that there shouldn't be jobs actively running).
func (c APIClient) GarbageCollect() error {
	_, err := c.PpsAPIClient.GarbageCollect(
		c.ctx(),
		&pps.GarbageCollectRequest{},
	)
	return sanitizeErr(err)
}

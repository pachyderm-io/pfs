package server

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/golang/protobuf/ptypes"
	"github.com/pachyderm/pachyderm/src/client"
	"github.com/pachyderm/pachyderm/src/client/auth"
	"github.com/pachyderm/pachyderm/src/client/limit"
	"github.com/pachyderm/pachyderm/src/client/pfs"
	"github.com/pachyderm/pachyderm/src/client/pkg/errors"
	"github.com/pachyderm/pachyderm/src/client/pkg/grpcutil"
	"github.com/pachyderm/pachyderm/src/client/pkg/tracing"
	"github.com/pachyderm/pachyderm/src/client/pkg/tracing/extended"
	"github.com/pachyderm/pachyderm/src/client/pps"
	pfsServer "github.com/pachyderm/pachyderm/src/server/pfs"
	"github.com/pachyderm/pachyderm/src/server/pkg/ancestry"
	"github.com/pachyderm/pachyderm/src/server/pkg/backoff"
	col "github.com/pachyderm/pachyderm/src/server/pkg/collection"
	"github.com/pachyderm/pachyderm/src/server/pkg/hashtree"
	"github.com/pachyderm/pachyderm/src/server/pkg/log"
	"github.com/pachyderm/pachyderm/src/server/pkg/lokiutil"
	"github.com/pachyderm/pachyderm/src/server/pkg/metrics"
	"github.com/pachyderm/pachyderm/src/server/pkg/ppsconsts"
	"github.com/pachyderm/pachyderm/src/server/pkg/ppsdb"
	"github.com/pachyderm/pachyderm/src/server/pkg/ppsutil"
	"github.com/pachyderm/pachyderm/src/server/pkg/serviceenv"
	txnenv "github.com/pachyderm/pachyderm/src/server/pkg/transactionenv"
	"github.com/pachyderm/pachyderm/src/server/pkg/uuid"
	"github.com/pachyderm/pachyderm/src/server/pkg/watch"
	ppsServer "github.com/pachyderm/pachyderm/src/server/pps"
	"github.com/pachyderm/pachyderm/src/server/pps/server/githook"
	"github.com/pachyderm/pachyderm/src/server/worker/common"
	workercommon "github.com/pachyderm/pachyderm/src/server/worker/common"
	"github.com/pachyderm/pachyderm/src/server/worker/datum"
	workerserver "github.com/pachyderm/pachyderm/src/server/worker/server"

	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/types"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/robfig/cron"
	logrus "github.com/sirupsen/logrus"
	"github.com/willf/bloom"
	"golang.org/x/net/context"
	"golang.org/x/sync/errgroup"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kube "k8s.io/client-go/kubernetes"
)

const (
	// DefaultUserImage is the image used for jobs when the user does not specify
	// an image.
	DefaultUserImage = "ubuntu:16.04"
	// DefaultDatumTries is the default number of times a datum will be tried
	// before we give up and consider the job failed.
	DefaultDatumTries = 3
)

var (
	suite           = "pachyderm"
	defaultGCMemory = 20 * 1024 * 1024 // 20 MB
)

func newErrPipelineNotFound(pipeline string) error {
	return errors.Errorf("pipeline %v not found", pipeline)
}

func newErrPipelineExists(pipeline string) error {
	return errors.Errorf("pipeline %v already exists", pipeline)
}

func newErrPipelineUpdate(pipeline string, reason string) error {
	return errors.Errorf("pipeline %v update error: %s", pipeline, reason)
}

type errGithookServiceNotFound struct {
	error
}

// apiServer implements the public interface of the Pachyderm Pipeline System,
// including all RPCs defined in the protobuf spec.
type apiServer struct {
	log.Logger
	etcdPrefix             string
	env                    *serviceenv.ServiceEnv
	txnEnv                 *txnenv.TransactionEnv
	namespace              string
	workerImage            string
	workerSidecarImage     string
	workerImagePullPolicy  string
	storageRoot            string
	storageBackend         string
	storageHostPath        string
	iamRole                string
	imagePullSecret        string
	noExposeDockerSocket   bool
	reporter               *metrics.Reporter
	monitorCancelsMu       sync.Mutex
	monitorCancels         map[string]func()
	crashingMonitorCancels map[string]func()
	workerUsesRoot         bool
	workerGrpcPort         uint16
	port                   uint16
	httpPort               uint16
	peerPort               uint16
	gcPercent              int
	// collections
	pipelines col.Collection
	jobs      col.Collection
}

func merge(from, to map[string]bool) {
	for s := range from {
		to[s] = true
	}
}

func validateNames(names map[string]bool, input *pps.Input) error {
	switch {
	case input == nil:
		return nil // spouts can have nil input
	case input.Pfs != nil:
		if names[input.Pfs.Name] {
			return errors.Errorf(`name "%s" was used more than once`, input.Pfs.Name)
		}
		names[input.Pfs.Name] = true
	case input.Cron != nil:
		if names[input.Cron.Name] {
			return errors.Errorf(`name "%s" was used more than once`, input.Cron.Name)
		}
		names[input.Cron.Name] = true
	case input.Union != nil:
		for _, input := range input.Union {
			namesCopy := make(map[string]bool)
			merge(names, namesCopy)
			if err := validateNames(namesCopy, input); err != nil {
				return err
			}
			// we defer this because subinputs of a union input are allowed to
			// have conflicting names but other inputs that are, for example,
			// crossed with this union cannot conflict with any of the names it
			// might present
			defer merge(namesCopy, names)
		}
	case input.Cross != nil:
		for _, input := range input.Cross {
			if err := validateNames(names, input); err != nil {
				return err
			}
		}
	case input.Join != nil:
		for _, input := range input.Join {
			if err := validateNames(names, input); err != nil {
				return err
			}
		}
	case input.Git != nil:
		if names[input.Git.Name] {
			return errors.Errorf(`name "%s" was used more than once`, input.Git.Name)
		}
		names[input.Git.Name] = true
	}
	return nil
}

func (a *apiServer) validateInput(pachClient *client.APIClient, pipelineName string, input *pps.Input, job bool) error {
	if err := validateNames(make(map[string]bool), input); err != nil {
		return err
	}
	var result error
	pps.VisitInput(input, func(input *pps.Input) {
		if err := func() error {
			set := false
			if input.Pfs != nil {
				set = true
				switch {
				case len(input.Pfs.Name) == 0:
					return errors.Errorf("input must specify a name")
				case input.Pfs.Name == "out":
					return errors.Errorf("input cannot be named \"out\", as pachyderm " +
						"already creates /pfs/out to collect job output")
				case input.Pfs.Repo == "":
					return errors.Errorf("input must specify a repo")
				case input.Pfs.Repo == "out" && input.Pfs.Name == "":
					return errors.Errorf("inputs based on repos named \"out\" must have " +
						"'name' set, as pachyderm already creates /pfs/out to collect " +
						"job output")
				case input.Pfs.Branch == "" && !job:
					return errors.Errorf("input must specify a branch")
				case !input.Pfs.S3 && len(input.Pfs.Glob) == 0:
					return errors.Errorf("input must specify a glob")
				case input.Pfs.S3 && input.Pfs.Glob != "/":
					return errors.Errorf("inputs that set 's3' to 'true' must also set " +
						"'glob', to \"/\", as the S3 gateway is only able to expose data " +
						"at the commit level")
				case input.Pfs.S3 && input.Pfs.Lazy:
					return errors.Errorf("input cannot specify both 's3' and 'lazy', as " +
						"'s3' requires input data to be accessed via Pachyderm's S3 " +
						"gateway rather than the file system")
				case input.Pfs.S3 && input.Pfs.EmptyFiles:
					return errors.Errorf("input cannot specify both 's3' and " +
						"'empty_files', as 's3' requires input data to be accessed via " +
						"Pachyderm's S3 gateway rather than the file system")
				}
				// Note that input.Pfs.Commit is empty if a) this is a job b) one of
				// the job pipeline's input branches has no commits yet
				if job && input.Pfs.Commit != "" {
					// for jobs we check that the input commit exists
					if _, err := pachClient.InspectCommit(input.Pfs.Repo, input.Pfs.Commit); err != nil {
						return err
					}
				} else {
					// for pipelines we only check that the repo exists
					if _, err := pachClient.InspectRepo(input.Pfs.Repo); err != nil {
						return err
					}
				}
			}
			if input.Cross != nil {
				if set {
					return errors.Errorf("multiple input types set")
				}
				set = true
			}
			if input.Join != nil {
				if set {
					return errors.Errorf("multiple input types set")
				}
				set = true
				if ppsutil.ContainsS3Inputs(input) {
					// The best datum semantics for s3 inputs embedded in join expressions
					// are not yet clear, and we see no use case for them yet, so block
					// them until we know how they should work
					return errors.Errorf("S3 inputs in join expressions are not supported")
				}
			}
			if input.Union != nil {
				if set {
					return errors.Errorf("multiple input types set")
				}
				set = true
				if ppsutil.ContainsS3Inputs(input) {
					// See above for "joins"; block s3 inputs in union expressions until
					// we know how they should work
					return errors.Errorf("S3 inputs in union expressions are not supported")
				}
			}
			if input.Cron != nil {
				if set {
					return errors.Errorf("multiple input types set")
				}
				set = true
				if len(input.Cron.Name) == 0 {
					return errors.Errorf("input must specify a name")
				}
				if _, err := cron.ParseStandard(input.Cron.Spec); err != nil {
					return errors.Wrapf(err, "error parsing cron-spec")
				}
			}
			if input.Git != nil {
				if set {
					return errors.Errorf("multiple input types set")
				}
				logrus.Warn("githooks are deprecated and will be removed in a future version - see pipeline build steps for an alternative")
				set = true
				if err := pps.ValidateGitCloneURL(input.Git.URL); err != nil {
					return err
				}
			}
			if !set {
				return errors.Errorf("no input set")
			}
			return nil
		}(); err != nil && result == nil {
			result = err
		}
	})
	return result
}

func validateTransform(transform *pps.Transform) error {
	if transform == nil {
		return errors.Errorf("pipeline must specify a transform")
	}
	if transform.Image == "" {
		return errors.Errorf("pipeline transform must contain an image")
	}
	return nil
}

func (a *apiServer) validateKube() {
	errors := false
	kubeClient := a.env.GetKubeClient()
	_, err := kubeClient.CoreV1().Nodes().List(metav1.ListOptions{})
	if err != nil {
		errors = true
		logrus.Errorf("unable to access kubernetes nodeslist, Pachyderm will continue to work but it will not be possible to use COEFFICIENT parallelism. error: %v", err)
	}
	_, err = kubeClient.CoreV1().Pods(a.namespace).Watch(metav1.ListOptions{Watch: true})
	if err != nil {
		errors = true
		logrus.Errorf("unable to access kubernetes pods, Pachyderm will continue to work but certain pipeline errors will result in pipelines being stuck indefinitely in \"starting\" state. error: %v", err)
	}
	pods, err := a.rcPods("pachd")
	if err != nil || len(pods) == 0 {
		errors = true
		logrus.Errorf("unable to access kubernetes pods, Pachyderm will continue to work but 'pachctl logs' will not work. error: %v", err)
	} else {
		// No need to check all pods since we're just checking permissions.
		pod := pods[0]
		_, err = kubeClient.CoreV1().Pods(a.namespace).GetLogs(
			pod.ObjectMeta.Name, &v1.PodLogOptions{
				Container: "pachd",
			}).Timeout(10 * time.Second).Do().Raw()
		if err != nil {
			errors = true
			logrus.Errorf("unable to access kubernetes logs, Pachyderm will continue to work but 'pachctl logs' will not work. error: %v", err)
		}
	}
	name := uuid.NewWithoutDashes()
	labels := map[string]string{"app": name}
	rc := &v1.ReplicationController{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ReplicationController",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: labels,
		},
		Spec: v1.ReplicationControllerSpec{
			Selector: labels,
			Replicas: new(int32),
			Template: &v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Name:   name,
					Labels: labels,
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name:    "name",
							Image:   DefaultUserImage,
							Command: []string{"true"},
						},
					},
				},
			},
		},
	}
	if _, err := kubeClient.CoreV1().ReplicationControllers(a.namespace).Create(rc); err != nil {
		if err != nil {
			errors = true
			logrus.Errorf("unable to create kubernetes replication controllers, Pachyderm will not function properly until this is fixed. error: %v", err)
		}
	}
	if err := kubeClient.CoreV1().ReplicationControllers(a.namespace).Delete(name, nil); err != nil {
		if err != nil {
			errors = true
			logrus.Errorf("unable to delete kubernetes replication controllers, Pachyderm function properly but pipeline cleanup will not work. error: %v", err)
		}
	}
	if !errors {
		logrus.Infof("validating kubernetes access returned no errors")
	}
}

func checkLoggedIn(pachClient *client.APIClient) (context.Context, error) {
	ctx := pachClient.Ctx() // pachClient propagates auth info
	_, err := pachClient.WhoAmI(ctx, &auth.WhoAmIRequest{})
	if err != nil && !auth.IsErrNotActivated(err) {
		return nil, err
	}
	return ctx, nil
}

// authorizing a pipeline operation varies slightly depending on whether the
// pipeline is being created, updated, or deleted
type pipelineOperation uint8

const (
	// pipelineOpCreate is required for CreatePipeline
	pipelineOpCreate pipelineOperation = iota
	// pipelineOpListDatum is required for ListDatum
	pipelineOpListDatum
	// pipelineOpGetLogs is required for GetLogs
	pipelineOpGetLogs
	// pipelineOpUpdate is required for UpdatePipeline
	pipelineOpUpdate
	// pipelineOpUpdate is required for DeletePipeline
	pipelineOpDelete
)

// authorizePipelineOp checks if the user indicated by 'ctx' is authorized
// to perform 'operation' on the pipeline in 'info'
func (a *apiServer) authorizePipelineOp(pachClient *client.APIClient, operation pipelineOperation, input *pps.Input, output string) error {
	ctx := pachClient.Ctx()
	me, err := pachClient.WhoAmI(ctx, &auth.WhoAmIRequest{})
	if auth.IsErrNotActivated(err) {
		return nil // Auth isn't activated, skip authorization completely
	} else if err != nil {
		return err
	}

	if input != nil && operation != pipelineOpDelete {
		// Check that the user is authorized to read all input repos, and write to the
		// output repo (which the pipeline needs to be able to do on the user's
		// behalf)
		var eg errgroup.Group
		done := make(map[string]struct{}) // don't double-authorize repos
		pps.VisitInput(input, func(in *pps.Input) {
			var repo string

			if in.Pfs != nil {
				repo = in.Pfs.Repo
			} else {
				return
			}

			if _, ok := done[repo]; ok {
				return
			}
			done[repo] = struct{}{}
			eg.Go(func() error {
				resp, err := pachClient.Authorize(ctx, &auth.AuthorizeRequest{
					Repo:  repo,
					Scope: auth.Scope_READER,
				})
				if err != nil {
					return err
				}
				if !resp.Authorized {
					return &auth.ErrNotAuthorized{
						Subject:  me.Username,
						Repo:     repo,
						Required: auth.Scope_READER,
					}
				}
				return nil
			})
		})
		if err := eg.Wait(); err != nil {
			return err
		}
	}

	// Check that the user is authorized to write to the output repo.
	// Note: authorizePipelineOp is called before CreateRepo creates a
	// PipelineInfo proto in etcd, so PipelineManager won't have created an output
	// repo yet, and it's possible to check that the output repo doesn't exist
	// (if it did exist, we'd have to check that the user has permission to write
	// to it, and this is simpler)
	var required auth.Scope
	switch operation {
	case pipelineOpCreate:
		if _, err := pachClient.InspectRepo(output); err == nil {
			return errors.Errorf("cannot overwrite repo \"%s\" with new output repo", output)
		} else if !isNotFoundErr(err) {
			return err
		}
	case pipelineOpListDatum, pipelineOpGetLogs:
		required = auth.Scope_READER
	case pipelineOpUpdate:
		required = auth.Scope_WRITER
	case pipelineOpDelete:
		if _, err := pachClient.InspectRepo(output); isNotFoundErr(err) {
			// special case: the pipeline output repo has been deleted (so the
			// pipeline is now invalid). It should be possible to delete the pipeline.
			return nil
		}
		required = auth.Scope_OWNER
	default:
		return errors.Errorf("internal error, unrecognized operation %v", operation)
	}
	if required != auth.Scope_NONE {
		resp, err := pachClient.Authorize(ctx, &auth.AuthorizeRequest{
			Repo:  output,
			Scope: required,
		})
		if err != nil {
			return err
		}
		if !resp.Authorized {
			return &auth.ErrNotAuthorized{
				Subject:  me.Username,
				Repo:     output,
				Required: required,
			}
		}
	}
	return nil
}

func (a *apiServer) UpdateJobState(ctx context.Context, request *pps.UpdateJobStateRequest) (response *types.Empty, retErr error) {
	func() { a.Log(request, nil, nil, 0) }()
	defer func(start time.Time) { a.Log(request, response, retErr, time.Since(start)) }(time.Now())

	if err := a.txnEnv.WithTransaction(ctx, func(txn txnenv.Transaction) error {
		return txn.UpdateJobState(request)
	}); err != nil {
		return nil, err
	}

	return &types.Empty{}, nil
}

func (a *apiServer) UpdateJobStateInTransaction(txnCtx *txnenv.TransactionContext, request *pps.UpdateJobStateRequest) error {
	jobs := a.jobs.ReadWrite(txnCtx.Stm)

	jobPtr := &pps.EtcdJobInfo{}
	if err := jobs.Get(request.Job.ID, jobPtr); err != nil {
		return err
	}
	if ppsutil.IsTerminal(jobPtr.State) {
		return ppsServer.ErrJobFinished{jobPtr.Job}
	}

	jobPtr.Restart = request.Restart
	jobPtr.DataProcessed = request.DataProcessed
	jobPtr.DataSkipped = request.DataSkipped
	jobPtr.DataFailed = request.DataFailed
	jobPtr.DataRecovered = request.DataRecovered
	jobPtr.DataTotal = request.DataTotal
	jobPtr.Stats = request.Stats

	return ppsutil.UpdateJobState(a.pipelines.ReadWrite(txnCtx.Stm), jobs, jobPtr, request.State, request.Reason)
}

// CreateJob implements the protobuf pps.CreateJob RPC
func (a *apiServer) CreateJob(ctx context.Context, request *pps.CreateJobRequest) (response *pps.Job, retErr error) {
	func() { a.Log(request, nil, nil, 0) }()
	defer func(start time.Time) { a.Log(request, response, retErr, time.Since(start)) }(time.Now())
	pachClient := a.env.GetPachClient(ctx)
	ctx, err := checkLoggedIn(pachClient)
	if err != nil {
		return nil, err
	}

	commitInfo, err := pachClient.PfsAPIClient.InspectCommit(ctx, pfs.InspectCommitRequest{
		Commit:     request.OutputCommit,
		BlockState: pfs.CommitState_STARTED,
	})
	if err != nil {
		return nil, err
	}

	pipelineInfo, err := pachClient.InspectPipeline(request.Pipeline.Name)
	if err != nil {
		return nil, err
	}

	input := ppsutil.JobInput(pipelineInfo, commitInfo)
	dit, err := datum.NewIterator(pachClient, input)
	if err != nil {
		return nil, err
	}

	datumIDs := []string{}
	for i := 0; i < dit.Len(); i++ {
		datumIDs = append(datumIDs, common.DatumID(dit.DatumN(i)))
	}

	job := client.NewJob(uuid.NewWithoutDashes())
	if request.Stats == nil {
		request.Stats = &pps.ProcessStats{}
	}
	_, err = col.NewSTM(ctx, a.env.GetEtcdClient(), func(stm col.STM) error {
		jobPtr := &pps.EtcdJobInfo{
			Job:           job,
			OutputCommit:  request.OutputCommit,
			Pipeline:      request.Pipeline,
			Stats:         request.Stats,
			Restart:       request.Restart,
			DataProcessed: request.DataProcessed,
			DataSkipped:   request.DataSkipped,
			DataTotal:     request.DataTotal,
			DataFailed:    request.DataFailed,
			DataRecovered: request.DataRecovered,
			StatsCommit:   request.StatsCommit,
			Started:       request.Started,
			Finished:      request.Finished,
			DatumIDs:      datumIDs,
		}
		return ppsutil.UpdateJobState(a.pipelines.ReadWrite(stm), a.jobs.ReadWrite(stm), jobPtr, request.State, request.Reason)
	})
	if err != nil {
		return nil, err
	}
	return job, nil
}

// InspectJob implements the protobuf pps.InspectJob RPC
func (a *apiServer) InspectJob(ctx context.Context, request *pps.InspectJobRequest) (response *pps.JobInfo, retErr error) {
	func() { a.Log(request, nil, nil, 0) }()
	defer func(start time.Time) { a.Log(request, response, retErr, time.Since(start)) }(time.Now())
	pachClient := a.env.GetPachClient(ctx)
	ctx, err := checkLoggedIn(pachClient)
	if err != nil {
		return nil, err
	}
	if request.Job == nil && request.OutputCommit == nil {
		return nil, errors.Errorf("must specify either a Job or an OutputCommit")
	}
	jobs := a.jobs.ReadOnly(ctx)
	if request.OutputCommit != nil {
		if request.Job != nil {
			return nil, errors.Errorf("can't set both Job and OutputCommit")
		}
		ci, err := pachClient.InspectCommit(request.OutputCommit.Repo.Name, request.OutputCommit.ID)
		if err != nil {
			return nil, err
		}
		if err := a.listJob(pachClient, nil, ci.Commit, nil, -1, false, func(ji *pps.JobInfo) error {
			if request.Job != nil {
				return errors.Errorf("internal error, more than 1 Job has output commit: %v (this is likely a bug)", request.OutputCommit)
			}
			request.Job = ji.Job
			return nil
		}); err != nil {
			return nil, err
		}
		if request.Job == nil {
			return nil, errors.Errorf("job with output commit %s not found", request.OutputCommit.ID)
		}
	}
	if request.BlockState {
		watcher, err := jobs.WatchOne(request.Job.ID)
		if err != nil {
			return nil, err
		}
		defer watcher.Close()

		for {
			ev, ok := <-watcher.Watch()
			if !ok {
				return nil, errors.Errorf("the stream for job updates closed unexpectedly")
			}
			switch ev.Type {
			case watch.EventError:
				return nil, ev.Err
			case watch.EventDelete:
				return nil, errors.Errorf("job %s was deleted", request.Job.ID)
			case watch.EventPut:
				var jobID string
				jobPtr := &pps.EtcdJobInfo{}
				if err := ev.Unmarshal(&jobID, jobPtr); err != nil {
					return nil, err
				}
				if ppsutil.IsTerminal(jobPtr.State) {
					return a.jobInfoFromPtr(pachClient, jobPtr, true)
				}
			}
		}
	}
	jobPtr := &pps.EtcdJobInfo{}
	if err := jobs.Get(request.Job.ID, jobPtr); err != nil {
		return nil, err
	}
	jobInfo, err := a.jobInfoFromPtr(pachClient, jobPtr, true)
	if err != nil {
		return nil, err
	}
	if request.Full {
		// If the job is running we fill in WorkerStatus field, otherwise we just
		// return the jobInfo.
		if jobInfo.State != pps.JobState_JOB_RUNNING {
			return jobInfo, nil
		}
		workerPoolID := ppsutil.PipelineRcName(jobInfo.Pipeline.Name, jobInfo.PipelineVersion)
		workerStatus, err := workerserver.Status(ctx, workerPoolID, a.env.GetEtcdClient(), a.etcdPrefix, a.workerGrpcPort)
		if err != nil {
			logrus.Errorf("failed to get worker status with err: %s", err.Error())
		} else {
			// It's possible that the workers might be working on datums for other
			// jobs, we omit those since they're not part of the status for this
			// job.
			for _, status := range workerStatus {
				if status.JobID == jobInfo.Job.ID {
					jobInfo.WorkerStatus = append(jobInfo.WorkerStatus, status)
					jobInfo.DataProcessed += status.DataProcessed
				}
			}
		}
	}
	return jobInfo, nil
}

// listJob is the internal implementation of ListJob shared between ListJob and
// ListJobStream. When ListJob is removed, this should be inlined into
// ListJobStream.
func (a *apiServer) listJob(pachClient *client.APIClient, pipeline *pps.Pipeline,
	outputCommit *pfs.Commit, inputCommits []*pfs.Commit, history int64, full bool,
	f func(*pps.JobInfo) error) error {
	authIsActive := true
	me, err := pachClient.WhoAmI(pachClient.Ctx(), &auth.WhoAmIRequest{})
	if auth.IsErrNotActivated(err) {
		authIsActive = false
	} else if err != nil {
		return err
	}
	if authIsActive && pipeline != nil {
		// If 'pipeline is set, check that caller has access to the pipeline's
		// output repo; currently, that's all that's required for ListJob.
		//
		// If 'pipeline' isn't set, then we don't return an error (otherwise, a
		// caller without access to a single pipeline's output repo couldn't run
		// `pachctl list job` at all) and instead silently skip jobs where the user
		// doesn't have access to the job's output repo.
		resp, err := pachClient.Authorize(pachClient.Ctx(), &auth.AuthorizeRequest{
			Repo:  pipeline.Name,
			Scope: auth.Scope_READER,
		})
		if err != nil {
			return err
		}
		if !resp.Authorized {
			return &auth.ErrNotAuthorized{
				Subject:  me.Username,
				Repo:     pipeline.Name,
				Required: auth.Scope_READER,
			}
		}
	}
	if outputCommit != nil {
		outputCommit, err = a.resolveCommit(pachClient, outputCommit)
		if err != nil {
			return err
		}
	}
	for i, inputCommit := range inputCommits {
		inputCommits[i], err = a.resolveCommit(pachClient, inputCommit)
		if err != nil {
			return err
		}
	}
	// specCommits holds the specCommits of pipelines that we're interested in
	specCommits := make(map[string]bool)
	if err := a.listPipelinePtr(pachClient, pipeline, history,
		func(name string, ptr *pps.EtcdPipelineInfo) error {
			specCommits[ptr.SpecCommit.ID] = true
			return nil
		}); err != nil {
		return err
	}
	jobs := a.jobs.ReadOnly(pachClient.Ctx())
	jobPtr := &pps.EtcdJobInfo{}
	_f := func(string) error {
		jobInfo, err := a.jobInfoFromPtr(pachClient, jobPtr,
			len(inputCommits) > 0 || full)
		if err != nil {
			if isNotFoundErr(err) {
				// This can happen if a user deletes an upstream commit and thereby
				// deletes this job's output commit, but doesn't delete the etcdJobInfo.
				// In this case, the job is effectively deleted, but isn't removed from
				// etcd yet.
				return nil
			} else if auth.IsErrNotAuthorized(err) {
				return nil // skip job--see note under 'authIsActive && pipeline != nil'
			}
			return err
		}
		if len(inputCommits) > 0 {
			found := make([]bool, len(inputCommits))
			pps.VisitInput(jobInfo.Input, func(in *pps.Input) {
				if in.Pfs != nil {
					for i, inputCommit := range inputCommits {
						if in.Pfs.Commit == inputCommit.ID {
							found[i] = true
						}
					}
				}
			})
			for _, found := range found {
				if !found {
					return nil
				}
			}
		}
		if !specCommits[jobInfo.SpecCommit.ID] {
			return nil
		}
		return f(jobInfo)
	}
	if pipeline != nil {
		return jobs.GetByIndex(ppsdb.JobsPipelineIndex, pipeline, jobPtr, col.DefaultOptions, _f)
	} else if outputCommit != nil {
		return jobs.GetByIndex(ppsdb.JobsOutputIndex, outputCommit, jobPtr, col.DefaultOptions, _f)
	} else {
		return jobs.List(jobPtr, col.DefaultOptions, _f)
	}
}

func (a *apiServer) jobInfoFromPtr(pachClient *client.APIClient, jobPtr *pps.EtcdJobInfo, full bool) (*pps.JobInfo, error) {
	result := &pps.JobInfo{
		Job:           jobPtr.Job,
		Pipeline:      jobPtr.Pipeline,
		OutputRepo:    &pfs.Repo{Name: jobPtr.Pipeline.Name},
		OutputCommit:  jobPtr.OutputCommit,
		Restart:       jobPtr.Restart,
		DataProcessed: jobPtr.DataProcessed,
		DataSkipped:   jobPtr.DataSkipped,
		DataTotal:     jobPtr.DataTotal,
		DataFailed:    jobPtr.DataFailed,
		DataRecovered: jobPtr.DataRecovered,
		Stats:         jobPtr.Stats,
		StatsCommit:   jobPtr.StatsCommit,
		State:         jobPtr.State,
		Reason:        jobPtr.Reason,
		Started:       jobPtr.Started,
		Finished:      jobPtr.Finished,
		DatumIDs:      jobPtr.DatumIDs,
	}
	commitInfo, err := pachClient.InspectCommit(jobPtr.OutputCommit.Repo.Name, jobPtr.OutputCommit.ID)
	if err != nil {
		if isNotFoundErr(err) {
			if _, err := a.DeleteJob(pachClient.Ctx(), &pps.DeleteJobRequest{Job: jobPtr.Job}); err != nil {
				return nil, err
			}
			return nil, errors.Errorf("job %s not found", jobPtr.Job.ID)
		}
		return nil, err
	}
	var specCommit *pfs.Commit
	for _, prov := range commitInfo.Provenance {
		if prov.Commit.Repo.Name == ppsconsts.SpecRepo && prov.Branch.Name == jobPtr.Pipeline.Name {
			specCommit = prov.Commit
			break
		}
	}
	if specCommit == nil {
		return nil, errors.Errorf("couldn't find spec commit for job %s, (this is likely a bug)", jobPtr.Job.ID)
	}
	result.SpecCommit = specCommit
	pipelinePtr := &pps.EtcdPipelineInfo{}
	if err := a.pipelines.ReadOnly(pachClient.Ctx()).Get(jobPtr.Pipeline.Name, pipelinePtr); err != nil {
		return nil, err
	}
	// Override the SpecCommit for the pipeline to be what it was when this job
	// was created, this prevents races between updating a pipeline and
	// previous jobs running.
	pipelinePtr.SpecCommit = specCommit
	if full {
		pipelineInfo, err := ppsutil.GetPipelineInfo(pachClient, jobPtr.Pipeline.Name, pipelinePtr)
		if err != nil {
			return nil, err
		}
		result.Transform = pipelineInfo.Transform
		result.PipelineVersion = pipelineInfo.Version
		result.ParallelismSpec = pipelineInfo.ParallelismSpec
		result.Egress = pipelineInfo.Egress
		result.Service = pipelineInfo.Service
		result.Spout = pipelineInfo.Spout
		result.OutputBranch = pipelineInfo.OutputBranch
		result.ResourceRequests = pipelineInfo.ResourceRequests
		result.ResourceLimits = pipelineInfo.ResourceLimits
		result.SidecarResourceLimits = pipelineInfo.SidecarResourceLimits
		result.Input = ppsutil.JobInput(pipelineInfo, commitInfo)
		result.EnableStats = pipelineInfo.EnableStats
		result.Salt = pipelineInfo.Salt
		result.ChunkSpec = pipelineInfo.ChunkSpec
		result.DatumTimeout = pipelineInfo.DatumTimeout
		result.JobTimeout = pipelineInfo.JobTimeout
		result.DatumTries = pipelineInfo.DatumTries
		result.SchedulingSpec = pipelineInfo.SchedulingSpec
		result.PodSpec = pipelineInfo.PodSpec
		result.PodPatch = pipelineInfo.PodPatch
	}
	return result, nil
}

// ListJob implements the protobuf pps.ListJob RPC
func (a *apiServer) ListJob(ctx context.Context, request *pps.ListJobRequest) (response *pps.JobInfos, retErr error) {
	func() { a.Log(request, nil, nil, 0) }()
	defer func(start time.Time) {
		if response != nil && len(response.JobInfo) > client.MaxListItemsLog {
			logrus.Infof("Response contains %d objects; logging the first %d", len(response.JobInfo), client.MaxListItemsLog)
			a.Log(request, &pps.JobInfos{JobInfo: response.JobInfo[:client.MaxListItemsLog]}, retErr, time.Since(start))
		} else {
			a.Log(request, response, retErr, time.Since(start))
		}
	}(time.Now())
	pachClient := a.env.GetPachClient(ctx)
	var jobInfos []*pps.JobInfo
	if err := a.listJob(pachClient, request.Pipeline, request.OutputCommit, request.InputCommit, request.History, request.Full, func(ji *pps.JobInfo) error {
		jobInfos = append(jobInfos, ji)
		return nil
	}); err != nil {
		return nil, err
	}
	return &pps.JobInfos{JobInfo: jobInfos}, nil
}

// ListJobStream implements the protobuf pps.ListJobStream RPC
func (a *apiServer) ListJobStream(request *pps.ListJobRequest, resp pps.API_ListJobStreamServer) (retErr error) {
	func() { a.Log(request, nil, nil, 0) }()
	sent := 0
	defer func(start time.Time) {
		a.Log(request, fmt.Sprintf("stream containing %d JobInfos", sent), retErr, time.Since(start))
	}(time.Now())
	pachClient := a.env.GetPachClient(resp.Context())
	return a.listJob(pachClient, request.Pipeline, request.OutputCommit, request.InputCommit, request.History, request.Full, func(ji *pps.JobInfo) error {
		if err := resp.Send(ji); err != nil {
			return err
		}
		sent++
		return nil
	})
}

// FlushJob implements the protobuf pps.FlushJob RPC
func (a *apiServer) FlushJob(request *pps.FlushJobRequest, resp pps.API_FlushJobServer) (retErr error) {
	func() { a.Log(request, nil, nil, 0) }()
	sent := 0
	defer func(start time.Time) {
		a.Log(request, fmt.Sprintf("stream containing %d JobInfos", sent), retErr, time.Since(start))
	}(time.Now())
	pachClient := a.env.GetPachClient(resp.Context())
	ctx, err := checkLoggedIn(pachClient)
	if err != nil {
		return err
	}
	var toRepos []*pfs.Repo
	for _, pipeline := range request.ToPipelines {
		toRepos = append(toRepos, client.NewRepo(pipeline.Name))
	}
	return pachClient.FlushCommitF(request.Commits, toRepos, func(ci *pfs.CommitInfo) error {
		var jis []*pps.JobInfo
		// FlushJob passes -1 for history because we don't know which version
		// of the pipeline created the output commit.
		if err := a.listJob(pachClient, nil, ci.Commit, nil, -1, false, func(ji *pps.JobInfo) error {
			jis = append(jis, ji)
			return nil
		}); err != nil {
			return err
		}
		if len(jis) == 0 {
			// This is possible because the commit may be part of the stats
			// branch of a pipeline, in which case it's not the output commit
			// of any job, thus we ignore it, the job will be returned in
			// another call to this function, the one for the job's output
			// commit.
			return nil
		}
		if len(jis) > 1 {
			return errors.Errorf("found too many jobs (%d) for output commit: %s/%s", len(jis), ci.Commit.Repo.Name, ci.Commit.ID)
		}
		// Even though the commit has been finished the job isn't necessarily
		// finished yet, so we block on its state as well.
		ji, err := a.InspectJob(ctx, &pps.InspectJobRequest{Job: jis[0].Job, BlockState: true})
		if err != nil {
			return err
		}
		return resp.Send(ji)
	})
}

// DeleteJob implements the protobuf pps.DeleteJob RPC
func (a *apiServer) DeleteJob(ctx context.Context, request *pps.DeleteJobRequest) (response *types.Empty, retErr error) {
	func() { a.Log(request, nil, nil, 0) }()
	defer func(start time.Time) { a.Log(request, response, retErr, time.Since(start)) }(time.Now())
	pachClient := a.env.GetPachClient(ctx)
	ctx, err := checkLoggedIn(pachClient)
	if err != nil {
		return nil, err
	}

	_, err = col.NewSTM(ctx, a.env.GetEtcdClient(), func(stm col.STM) error {
		return a.jobs.ReadWrite(stm).Delete(request.Job.ID)
	})
	if err != nil {
		return nil, err
	}
	return &types.Empty{}, nil
}

// StopJob implements the protobuf pps.StopJob RPC
func (a *apiServer) StopJob(ctx context.Context, request *pps.StopJobRequest) (response *types.Empty, retErr error) {
	func() { a.Log(request, nil, nil, 0) }()
	defer func(start time.Time) { a.Log(request, response, retErr, time.Since(start)) }(time.Now())
	pachClient := a.env.GetPachClient(ctx)
	ctx, err := checkLoggedIn(pachClient)
	if err != nil {
		return nil, err
	}

	// Lookup jobInfo
	jobPtr := &pps.EtcdJobInfo{}
	if err := a.jobs.ReadOnly(ctx).Get(request.Job.ID, jobPtr); err != nil {
		return nil, err
	}
	// Finish the job's output commit without a tree -- worker/master will mark
	// the job 'killed'
	if _, err := pachClient.PfsAPIClient.FinishCommit(ctx,
		&pfs.FinishCommitRequest{
			Commit: jobPtr.OutputCommit,
			Empty:  true,
		}); err != nil {
		return nil, err
	}
	return &types.Empty{}, nil
}

// RestartDatum implements the protobuf pps.RestartDatum RPC
func (a *apiServer) RestartDatum(ctx context.Context, request *pps.RestartDatumRequest) (response *types.Empty, retErr error) {
	func() { a.Log(request, nil, nil, 0) }()
	defer func(start time.Time) { a.Log(request, response, retErr, time.Since(start)) }(time.Now())
	pachClient := a.env.GetPachClient(ctx)
	ctx, err := checkLoggedIn(pachClient)
	if err != nil {
		return nil, err
	}

	jobInfo, err := a.InspectJob(ctx, &pps.InspectJobRequest{
		Job: request.Job,
	})
	if err != nil {
		return nil, err
	}
	workerPoolID := ppsutil.PipelineRcName(jobInfo.Pipeline.Name, jobInfo.PipelineVersion)
	if err := workerserver.Cancel(ctx, workerPoolID, a.env.GetEtcdClient(), a.etcdPrefix, a.workerGrpcPort, request.Job.ID, request.DataFilters); err != nil {
		return nil, err
	}
	return &types.Empty{}, nil
}

// listDatum contains our internal implementation of ListDatum, which is shared
// between ListDatum and ListDatumStream. When ListDatum is removed, this should
// be inlined into ListDatumStream
func (a *apiServer) listDatum(pachClient *client.APIClient, job *pps.Job, page, pageSize int64) (response *pps.ListDatumResponse, retErr error) {
	if _, err := checkLoggedIn(pachClient); err != nil {
		return nil, err
	}
	response = &pps.ListDatumResponse{}
	ctx := pachClient.Ctx()
	pfsClient := pachClient.PfsAPIClient

	// get information about 'job'
	jobInfo, err := a.InspectJob(ctx, &pps.InspectJobRequest{
		Job: &pps.Job{
			ID: job.ID,
		},
	})
	if err != nil {
		return nil, err
	}

	// authorize ListDatum (must have READER access to all inputs)
	if err := a.authorizePipelineOp(pachClient,
		pipelineOpListDatum,
		jobInfo.Input,
		jobInfo.Pipeline.Name,
	); err != nil {
		return nil, err
	}

	// helper functions for pagination
	getTotalPages := func(totalSize int) int64 {
		return (int64(totalSize) + pageSize - 1) / pageSize // == ceil(totalSize/pageSize)
	}
	getPageBounds := func(totalSize int) (int, int, error) {
		start := int(page * pageSize)
		end := int((page + 1) * pageSize)
		switch {
		case totalSize <= start:
			return 0, 0, io.EOF
		case totalSize <= end:
			return start, totalSize, nil
		case end < totalSize:
			return start, end, nil
		}
		return 0, 0, errors.New("getPageBounds: unreachable code")
	}

	dit, err := datum.NewIterator(pachClient, jobInfo.Input)
	if err != nil {
		return nil, err
	}

	var statsCommitInfo *pfs.CommitInfo
	if jobInfo.StatsCommit != nil {
		statsCommitInfo, err = pachClient.InspectCommit(jobInfo.StatsCommit.Repo.Name, jobInfo.StatsCommit.ID)
		if err != nil {
			return nil, err
		}
	}

	// If the stats commit is not closed, compute datums using jobInfo
	if statsCommitInfo == nil || statsCommitInfo.Finished == nil {
		start := 0
		end := dit.Len()
		if pageSize > 0 {
			var err error
			start, end, err = getPageBounds(dit.Len())
			if err != nil {
				return nil, err
			}
			response.Page = page
			response.TotalPages = getTotalPages(dit.Len())
		}
		var datumInfos []*pps.DatumInfo
		for i := start; i < end; i++ {
			datum := dit.DatumN(i) // flattened slice of *worker.Input to job
			id := workercommon.HashDatum(jobInfo.Pipeline.Name, jobInfo.Salt, datum)
			datumInfo := &pps.DatumInfo{
				Datum: &pps.Datum{
					ID:  id,
					Job: jobInfo.Job,
				},
				State: pps.DatumState_STARTING,
			}
			for _, input := range datum {
				datumInfo.Data = append(datumInfo.Data, input.FileInfo)
			}
			datumInfos = append(datumInfos, datumInfo)
		}
		response.DatumInfos = datumInfos
		return response, nil
	}

	// The stats commit is closed -- job is finished
	// List the files under / in the stats branch to get all the datums
	file := &pfs.File{
		Commit: jobInfo.StatsCommit,
		Path:   "/",
	}

	var datumFileInfos []*pfs.FileInfo
	fs, err := pfsClient.ListFileStream(ctx,
		&pfs.ListFileRequest{File: file, Full: true})
	if err != nil {
		return nil, grpcutil.ScrubGRPC(err)
	}
	// Omit files at the top level that correspond to aggregate job stats
	blacklist := map[string]bool{
		"stats": true,
		"logs":  true,
		"pfs":   true,
	}
	pathToDatumHash := func(path string) (string, error) {
		_, datumHash := filepath.Split(path)
		if _, ok := blacklist[datumHash]; ok {
			return "", errors.Errorf("value %v is not a datum hash", datumHash)
		}
		return datumHash, nil
	}
	for {
		f, err := fs.Recv()
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return nil, grpcutil.ScrubGRPC(err)
		}
		if _, err := pathToDatumHash(f.File.Path); err != nil {
			// not a datum
			continue
		}
		datumFileInfos = append(datumFileInfos, f)
	}
	var egGetDatums errgroup.Group
	limiter := limit.New(200)
	datumInfos := make([]*pps.DatumInfo, len(datumFileInfos))
	for index, fileInfo := range datumFileInfos {
		fileInfo := fileInfo
		index := index
		egGetDatums.Go(func() error {
			limiter.Acquire()
			defer limiter.Release()
			datumHash, err := pathToDatumHash(fileInfo.File.Path)
			if err != nil {
				// not a datum
				return nil
			}
			datum, err := a.getDatum(pachClient, jobInfo.StatsCommit.Repo.Name, jobInfo.StatsCommit, job.ID, datumHash, dit)
			if err != nil {
				return err
			}
			datumInfos[index] = datum
			return nil
		})
	}
	if err = egGetDatums.Wait(); err != nil {
		return nil, err
	}
	// Sort results (failed first)
	sort.Slice(datumInfos, func(i, j int) bool {
		return datumInfos[i].State < datumInfos[j].State
	})
	if pageSize > 0 {
		response.Page = page
		response.TotalPages = getTotalPages(len(datumInfos))
		start, end, err := getPageBounds(len(datumInfos))
		if err != nil {
			return nil, err
		}
		datumInfos = datumInfos[start:end]
	}
	response.DatumInfos = datumInfos
	return response, nil
}

// ListDatum implements the protobuf pps.ListDatum RPC
func (a *apiServer) ListDatum(ctx context.Context, request *pps.ListDatumRequest) (response *pps.ListDatumResponse, retErr error) {
	func() { a.Log(request, nil, nil, 0) }()
	defer func(start time.Time) {
		if response != nil && len(response.DatumInfos) > client.MaxListItemsLog {
			logrus.Infof("Response contains %d objects; logging the first %d", len(response.DatumInfos), client.MaxListItemsLog)
			logResponse := &pps.ListDatumResponse{
				TotalPages: response.TotalPages,
				Page:       response.Page,
				DatumInfos: response.DatumInfos[:client.MaxListItemsLog],
			}
			a.Log(request, logResponse, retErr, time.Since(start))
		} else {
			a.Log(request, response, retErr, time.Since(start))
		}
	}(time.Now())
	return a.listDatum(a.env.GetPachClient(ctx), request.Job, request.Page, request.PageSize)
}

// ListDatumStream implements the protobuf pps.ListDatumStream RPC
func (a *apiServer) ListDatumStream(req *pps.ListDatumRequest, resp pps.API_ListDatumStreamServer) (retErr error) {
	func() { a.Log(req, nil, nil, 0) }()
	sent := 0
	defer func(start time.Time) {
		a.Log(req, fmt.Sprintf("stream containing %d DatumInfos", sent), retErr, time.Since(start))
	}(time.Now())
	ldr, err := a.listDatum(a.env.GetPachClient(resp.Context()), req.Job, req.Page, req.PageSize)
	if err != nil {
		return err
	}
	first := true
	for _, di := range ldr.DatumInfos {
		r := &pps.ListDatumStreamResponse{}
		if first {
			r.Page = ldr.Page
			r.TotalPages = ldr.TotalPages
			first = false
		}
		r.DatumInfo = di
		if err := resp.Send(r); err != nil {
			return err
		}
		sent++
	}
	return nil
}

func (a *apiServer) getDatum(pachClient *client.APIClient, repo string, commit *pfs.Commit, jobID string, datumID string, dit datum.Iterator) (datumInfo *pps.DatumInfo, retErr error) {
	datumInfo = &pps.DatumInfo{
		Datum: &pps.Datum{
			ID:  datumID,
			Job: client.NewJob(jobID),
		},
		State: pps.DatumState_SUCCESS,
	}
	ctx := pachClient.Ctx()
	pfsClient := pachClient.PfsAPIClient

	// Check if skipped
	fileInfos, err := pachClient.GlobFile(commit.Repo.Name, commit.ID, fmt.Sprintf("/%v/job:*", datumID))
	if err != nil {
		return nil, err
	}
	if len(fileInfos) != 1 {
		return nil, errors.Errorf("couldn't find job file")
	}
	if strings.Split(fileInfos[0].File.Path, ":")[1] != jobID {
		datumInfo.State = pps.DatumState_SKIPPED
	}

	// Check if failed
	stateFile := &pfs.File{
		Commit: commit,
		Path:   fmt.Sprintf("/%v/failure", datumID),
	}
	_, err = pfsClient.InspectFile(ctx, &pfs.InspectFileRequest{File: stateFile})
	if err == nil {
		datumInfo.State = pps.DatumState_FAILED
	} else if !isNotFoundErr(err) {
		return nil, err
	}

	// Populate stats
	var buffer bytes.Buffer
	if err := pachClient.GetFile(commit.Repo.Name, commit.ID, fmt.Sprintf("/%v/stats", datumID), 0, 0, &buffer); err != nil {
		return nil, err
	}
	stats := &pps.ProcessStats{}
	err = jsonpb.Unmarshal(&buffer, stats)
	if err != nil {
		return nil, err
	}
	datumInfo.Stats = stats
	buffer.Reset()
	if err := pachClient.GetFile(commit.Repo.Name, commit.ID, fmt.Sprintf("/%v/index", datumID), 0, 0, &buffer); err != nil {
		return nil, err
	}
	i, err := strconv.Atoi(buffer.String())
	if err != nil {
		return nil, err
	}
	if i >= dit.Len() {
		return nil, errors.Errorf("index %d out of range", i)
	}
	inputs := dit.DatumN(i)
	for _, input := range inputs {
		datumInfo.Data = append(datumInfo.Data, input.FileInfo)
	}
	datumInfo.PfsState = &pfs.File{
		Commit: commit,
		Path:   fmt.Sprintf("/%v/pfs", datumID),
	}

	return datumInfo, nil
}

// InspectDatum implements the protobuf pps.InspectDatum RPC
func (a *apiServer) InspectDatum(ctx context.Context, request *pps.InspectDatumRequest) (response *pps.DatumInfo, retErr error) {
	func() { a.Log(request, nil, nil, 0) }()
	defer func(start time.Time) { a.Log(request, response, retErr, time.Since(start)) }(time.Now())
	pachClient := a.env.GetPachClient(ctx)
	ctx, err := checkLoggedIn(pachClient)
	if err != nil {
		return nil, err
	}
	jobInfo, err := a.InspectJob(ctx, &pps.InspectJobRequest{
		Job: &pps.Job{
			ID: request.Datum.Job.ID,
		},
	})
	if err != nil {
		return nil, err
	}

	if !jobInfo.EnableStats {
		return nil, errors.Errorf("stats not enabled on %v", jobInfo.Pipeline.Name)
	}
	if jobInfo.StatsCommit == nil {
		return nil, errors.Errorf("job not finished, no stats output yet")
	}
	dit, err := datum.NewIterator(pachClient, jobInfo.Input)
	if err != nil {
		return nil, err
	}

	// Populate datumInfo given a path
	datumInfo, err := a.getDatum(pachClient, jobInfo.StatsCommit.Repo.Name, jobInfo.StatsCommit, request.Datum.Job.ID, request.Datum.ID, dit)
	if err != nil {
		return nil, err
	}

	return datumInfo, nil
}

// GetLogs implements the protobuf pps.GetLogs RPC
func (a *apiServer) GetLogs(request *pps.GetLogsRequest, apiGetLogsServer pps.API_GetLogsServer) (retErr error) {
	if a.env.LokiLogging || request.UseLokiBackend {
		return a.getLogsLoki(request, apiGetLogsServer)
	}
	func() { a.Log(request, nil, nil, 0) }()
	defer func(start time.Time) { a.Log(request, nil, retErr, time.Since(start)) }(time.Now())
	pachClient := a.env.GetPachClient(apiGetLogsServer.Context())
	ctx := pachClient.Ctx() // pachClient will propagate auth info

	// Authorize request and get list of pods containing logs we're interested in
	// (based on pipeline and job filters)
	var rcName, containerName string
	if request.Pipeline == nil && request.Job == nil {
		if len(request.DataFilters) > 0 || request.Datum != nil {
			return errors.Errorf("must specify the Job or Pipeline that the datum is from to get logs for it")
		}
		// no authorization is done to get logs from master
		containerName, rcName = "pachd", "pachd"
	} else {
		containerName = client.PPSWorkerUserContainerName

		// 1) Lookup the PipelineInfo for this pipeline/job, for auth and to get the
		// RC name
		var pipelineInfo *pps.PipelineInfo
		var statsCommit *pfs.Commit
		var err error
		if request.Pipeline != nil {
			pipelineInfo, err = a.inspectPipeline(pachClient, request.Pipeline.Name)
			if err != nil {
				return errors.Wrapf(err, "could not get pipeline information for %s", request.Pipeline.Name)
			}
		} else if request.Job != nil {
			// If user provides a job, lookup the pipeline from the job info, and then
			// get the pipeline RC
			var jobPtr pps.EtcdJobInfo
			err = a.jobs.ReadOnly(ctx).Get(request.Job.ID, &jobPtr)
			if err != nil {
				return errors.Wrapf(err, "could not get job information for \"%s\"", request.Job.ID)
			}
			statsCommit = jobPtr.StatsCommit
			pipelineInfo, err = a.inspectPipeline(pachClient, jobPtr.Pipeline.Name)
			if err != nil {
				return errors.Wrapf(err, "could not get pipeline information for %s", jobPtr.Pipeline.Name)
			}
		}

		// 2) Check whether the caller is authorized to get logs from this pipeline/job
		if err := a.authorizePipelineOp(pachClient, pipelineOpGetLogs, pipelineInfo.Input, pipelineInfo.Pipeline.Name); err != nil {
			return err
		}

		// If the job had stats enabled, we use the logs from the stats
		// commit since that's likely to yield better results.
		if statsCommit != nil {
			ci, err := pachClient.InspectCommit(statsCommit.Repo.Name, statsCommit.ID)
			if err != nil {
				return err
			}
			if ci.Finished != nil {
				return a.getLogsFromStats(pachClient, request, apiGetLogsServer, statsCommit)
			}
		}

		// 3) Get rcName for this pipeline
		rcName = ppsutil.PipelineRcName(pipelineInfo.Pipeline.Name, pipelineInfo.Version)
		if err != nil {
			return err
		}
	}

	// Get pods managed by the RC we're scraping (either pipeline or pachd)
	pods, err := a.rcPods(rcName)
	if err != nil {
		return errors.Wrapf(err, "could not get pods in rc \"%s\" containing logs", rcName)
	}
	if len(pods) == 0 {
		return errors.Errorf("no pods belonging to the rc \"%s\" were found", rcName)
	}

	// Spawn one goroutine per pod. Each goro writes its pod's logs to a channel
	// and channels are read into the output server in a stable order.
	// (sort the pods to make sure that the order of log lines is stable)
	sort.Sort(podSlice(pods))
	logCh := make(chan *pps.LogMessage)
	var eg errgroup.Group
	var mu sync.Mutex
	eg.Go(func() error {
		for _, pod := range pods {
			pod := pod
			if !request.Follow {
				mu.Lock()
			}
			eg.Go(func() (retErr error) {
				if !request.Follow {
					defer mu.Unlock()
				}
				tailLines := &request.Tail
				if *tailLines <= 0 {
					tailLines = nil
				}
				// Get full set of logs from pod i
				stream, err := a.env.GetKubeClient().CoreV1().Pods(a.namespace).GetLogs(
					pod.ObjectMeta.Name, &v1.PodLogOptions{
						Container: containerName,
						Follow:    request.Follow,
						TailLines: tailLines,
					}).Timeout(10 * time.Second).Stream()
				if err != nil {
					return err
				}
				defer func() {
					if err := stream.Close(); err != nil && retErr == nil {
						retErr = err
					}
				}()

				// Parse pods' log lines, and filter out irrelevant ones
				scanner := bufio.NewScanner(stream)
				for scanner.Scan() {
					msg := new(pps.LogMessage)
					if containerName == "pachd" {
						msg.Message = scanner.Text()
					} else {
						logBytes := scanner.Bytes()
						if err := jsonpb.Unmarshal(bytes.NewReader(logBytes), msg); err != nil {
							continue
						}

						// Filter out log lines that don't match on pipeline or job
						if request.Pipeline != nil && request.Pipeline.Name != msg.PipelineName {
							continue
						}
						if request.Job != nil && request.Job.ID != msg.JobID {
							continue
						}
						if request.Datum != nil && request.Datum.ID != msg.DatumID {
							continue
						}
						if request.Master != msg.Master {
							continue
						}
						if !workercommon.MatchDatum(request.DataFilters, msg.Data) {
							continue
						}
					}
					msg.Message = strings.TrimSuffix(msg.Message, "\n")

					// Log message passes all filters -- return it
					select {
					case logCh <- msg:
					case <-ctx.Done():
						return nil
					}
				}
				return nil
			})
		}
		return nil
	})
	var egErr error
	go func() {
		egErr = eg.Wait()
		close(logCh)
	}()

	for msg := range logCh {
		if err := apiGetLogsServer.Send(msg); err != nil {
			return err
		}
	}
	return egErr
}

func (a *apiServer) getLogsFromStats(pachClient *client.APIClient, request *pps.GetLogsRequest, apiGetLogsServer pps.API_GetLogsServer, statsCommit *pfs.Commit) error {
	pfsClient := pachClient.PfsAPIClient
	fs, err := pfsClient.GlobFileStream(pachClient.Ctx(), &pfs.GlobFileRequest{
		Commit:  statsCommit,
		Pattern: "*/logs", // this is the path where logs reside
	})
	if err != nil {
		return grpcutil.ScrubGRPC(err)
	}

	limiter := limit.New(20)
	var eg errgroup.Group
	var mu sync.Mutex
	for {
		fileInfo, err := fs.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		eg.Go(func() error {
			if err != nil {
				return err
			}
			limiter.Acquire()
			defer limiter.Release()
			var buf bytes.Buffer
			if err := pachClient.GetFile(fileInfo.File.Commit.Repo.Name, fileInfo.File.Commit.ID, fileInfo.File.Path, 0, 0, &buf); err != nil {
				return err
			}
			// Parse pods' log lines, and filter out irrelevant ones
			scanner := bufio.NewScanner(&buf)
			for scanner.Scan() {
				logBytes := scanner.Bytes()
				msg := new(pps.LogMessage)
				if err := jsonpb.Unmarshal(bytes.NewReader(logBytes), msg); err != nil {
					continue
				}
				if request.Pipeline != nil && request.Pipeline.Name != msg.PipelineName {
					continue
				}
				if request.Job != nil && request.Job.ID != msg.JobID {
					continue
				}
				if request.Datum != nil && request.Datum.ID != msg.DatumID {
					continue
				}
				if request.Master != msg.Master {
					continue
				}
				if !workercommon.MatchDatum(request.DataFilters, msg.Data) {
					continue
				}

				mu.Lock()
				if err := apiGetLogsServer.Send(msg); err != nil {
					mu.Unlock()
					return err
				}
				mu.Unlock()
			}
			return nil
		})
	}
	return eg.Wait()
}

func (a *apiServer) getLogsLoki(request *pps.GetLogsRequest, apiGetLogsServer pps.API_GetLogsServer) (retErr error) {
	func() { a.Log(request, nil, nil, 0) }()
	defer func(start time.Time) { a.Log(request, nil, retErr, time.Since(start)) }(time.Now())
	pachClient := a.env.GetPachClient(apiGetLogsServer.Context())
	ctx := pachClient.Ctx() // pachClient will propagate auth info

	// Authorize request and get list of pods containing logs we're interested in
	// (based on pipeline and job filters)
	loki, err := a.env.GetLokiClient()
	if err != nil {
		return err
	}
	if request.Pipeline == nil && request.Job == nil {
		if len(request.DataFilters) > 0 || request.Datum != nil {
			return errors.Errorf("must specify the Job or Pipeline that the datum is from to get logs for it")
		}
		// no authorization is done to get logs from master
		return lokiutil.QueryRange(loki, `{app="pachd"}`, time.Time{}, time.Now(), func(t time.Time, line string) error {
			return apiGetLogsServer.Send(&pps.LogMessage{
				Message: strings.TrimSuffix(line, "\n"),
			})
		})
	}

	// 1) Lookup the PipelineInfo for this pipeline/job, for auth and to get the
	// RC name
	var pipelineInfo *pps.PipelineInfo
	if request.Pipeline != nil {
		pipelineInfo, err = a.inspectPipeline(pachClient, request.Pipeline.Name)
		if err != nil {
			return errors.Wrapf(err, "could not get pipeline information for %s", request.Pipeline.Name)
		}
	} else if request.Job != nil {
		// If user provides a job, lookup the pipeline from the job info, and then
		// get the pipeline RC
		var jobPtr pps.EtcdJobInfo
		err = a.jobs.ReadOnly(ctx).Get(request.Job.ID, &jobPtr)
		if err != nil {
			return errors.Wrapf(err, "could not get job information for \"%s\"", request.Job.ID)
		}
		pipelineInfo, err = a.inspectPipeline(pachClient, jobPtr.Pipeline.Name)
		if err != nil {
			return errors.Wrapf(err, "could not get pipeline information for %s", jobPtr.Pipeline.Name)
		}
	}

	// 2) Check whether the caller is authorized to get logs from this pipeline/job
	if err := a.authorizePipelineOp(pachClient, pipelineOpGetLogs, pipelineInfo.Input, pipelineInfo.Pipeline.Name); err != nil {
		return err
	}
	query := fmt.Sprintf(`{pipelineName=%q, container="user"}`, pipelineInfo.Pipeline.Name)
	if request.Master {
		query += contains("master")
	}
	if request.Job != nil {
		query += contains(request.Job.ID)
	}
	if request.Datum != nil {
		query += contains(request.Datum.ID)
	}
	for _, filter := range request.DataFilters {
		query += contains(filter)
	}
	return lokiutil.QueryRange(loki, query, time.Time{}, time.Now(), func(t time.Time, line string) error {
		msg := &pps.LogMessage{}
		// These filters are almost always unnecessary because we apply
		// them in the Loki request, but many of them are just done with
		// string matching so there technically could be some false
		// positive matches (although it's pretty unlikely), checking here
		// just makes sure we don't accidentally intersperse unrelated log
		// messages.
		if err := jsonpb.Unmarshal(strings.NewReader(line), msg); err != nil {
			return nil
		}
		if request.Pipeline != nil && request.Pipeline.Name != msg.PipelineName {
			return nil
		}
		if request.Job != nil && request.Job.ID != msg.JobID {
			return nil
		}
		if request.Datum != nil && request.Datum.ID != msg.DatumID {
			return nil
		}
		if request.Master != msg.Master {
			return nil
		}
		if !workercommon.MatchDatum(request.DataFilters, msg.Data) {
			return nil
		}
		msg.Message = strings.TrimSuffix(msg.Message, "\n")
		return apiGetLogsServer.Send(msg)
	})
}

func contains(s string) string {
	return fmt.Sprintf(" |= %q", s)
}

func (a *apiServer) validatePipelineRequest(request *pps.CreatePipelineRequest) error {
	if request.Pipeline == nil {
		return errors.New("invalid pipeline spec: request.Pipeline cannot be nil")
	}
	if request.Pipeline.Name == "" {
		return errors.New("invalid pipeline spec: request.Pipeline.Name cannot be empty")
	}
	if err := ancestry.ValidateName(request.Pipeline.Name); err != nil {
		return errors.Wrapf(err, "invalid pipeline name")
	}
	if len(request.Pipeline.Name) > 63 {
		return errors.Errorf("pipeline name is %d characters long, but must have at most 63: %q",
			len(request.Pipeline.Name), request.Pipeline.Name)
	}
	// TODO(msteffen) eventually TFJob and Transform will be alternatives, but
	// currently TFJob isn't supported
	if request.TFJob != nil {
		return errors.New("embedding TFJobs in pipelines is not supported yet")
	}
	if request.S3Out && ((request.Service != nil) || (request.Spout != nil)) {
		return errors.New("s3 output is not supported in spouts or services")
	}
	if request.S3Out && request.EnableStats {
		return errors.New("stats are not supported for pipelines that output via Pachyderm's S3 gateway")
	}
	if request.Transform == nil {
		return errors.Errorf("pipeline must specify a transform")
	}
	return nil
}

func (a *apiServer) validatePipeline(pachClient *client.APIClient, pipelineInfo *pps.PipelineInfo) error {
	if pipelineInfo.Pipeline == nil {
		return errors.New("invalid pipeline spec: Pipeline field cannot be nil")
	}
	if pipelineInfo.Pipeline.Name == "" {
		return errors.New("invalid pipeline spec: Pipeline.Name cannot be empty")
	}
	if err := ancestry.ValidateName(pipelineInfo.Pipeline.Name); err != nil {
		return errors.Wrapf(err, "invalid pipeline name")
	}
	first := rune(pipelineInfo.Pipeline.Name[0])
	if !unicode.IsLetter(first) && !unicode.IsDigit(first) {
		return errors.Errorf("pipeline names must start with an alphanumeric character")
	}
	if len(pipelineInfo.Pipeline.Name) > 63 {
		return errors.Errorf("pipeline name is %d characters long, but must have at most 63: %q",
			len(pipelineInfo.Pipeline.Name), pipelineInfo.Pipeline.Name)
	}
	if err := validateTransform(pipelineInfo.Transform); err != nil {
		return errors.Wrapf(err, "invalid transform")
	}
	if err := a.validateInput(pachClient, pipelineInfo.Pipeline.Name, pipelineInfo.Input, false); err != nil {
		return err
	}
	if pipelineInfo.ParallelismSpec != nil {
		if pipelineInfo.ParallelismSpec.Coefficient < 0 {
			return errors.New("ParallelismSpec.Coefficient cannot be negative")
		}
		if pipelineInfo.ParallelismSpec.Constant != 0 &&
			pipelineInfo.ParallelismSpec.Coefficient != 0 {
			return errors.New("contradictory parallelism strategies: must set at " +
				"most one of ParallelismSpec.Constant and ParallelismSpec.Coefficient")
		}
		if pipelineInfo.Service != nil && pipelineInfo.ParallelismSpec.Constant != 1 {
			return errors.New("services can only be run with a constant parallelism of 1")
		}
	}
	if pipelineInfo.HashtreeSpec != nil {
		if pipelineInfo.HashtreeSpec.Constant == 0 {
			return errors.New("invalid pipeline spec: HashtreeSpec.Constant must be > 0")
		}
	}
	if pipelineInfo.OutputBranch == "" {
		return errors.New("pipeline needs to specify an output branch")
	}
	if _, err := resource.ParseQuantity(pipelineInfo.CacheSize); err != nil {
		return errors.Wrapf(err, "could not parse cacheSize '%s'", pipelineInfo.CacheSize)
	}
	if pipelineInfo.JobTimeout != nil {
		_, err := types.DurationFromProto(pipelineInfo.JobTimeout)
		if err != nil {
			return err
		}
	}
	if pipelineInfo.DatumTimeout != nil {
		_, err := types.DurationFromProto(pipelineInfo.DatumTimeout)
		if err != nil {
			return err
		}
	}
	if pipelineInfo.PodSpec != "" && !json.Valid([]byte(pipelineInfo.PodSpec)) {
		return errors.Errorf("malformed PodSpec")
	}
	if pipelineInfo.PodPatch != "" && !json.Valid([]byte(pipelineInfo.PodPatch)) {
		return errors.Errorf("malformed PodPatch")
	}
	if pipelineInfo.Service != nil {
		validServiceTypes := map[v1.ServiceType]bool{
			v1.ServiceTypeClusterIP:    true,
			v1.ServiceTypeLoadBalancer: true,
			v1.ServiceTypeNodePort:     true,
		}

		if !validServiceTypes[v1.ServiceType(pipelineInfo.Service.Type)] {
			return errors.Errorf("the following service type %s is not allowed", pipelineInfo.Service.Type)
		}
	}
	if pipelineInfo.Spout != nil {
		if pipelineInfo.EnableStats {
			return errors.Errorf("spouts are not allowed to have a stats branch")
		}
		if pipelineInfo.Spout.Marker != "" {
			// we need to make sure the marker name is also a valid file name, since it is used in file names
			if err := hashtree.ValidatePath(pipelineInfo.Spout.Marker); err != nil || pipelineInfo.Spout.Marker == "out" {
				return errors.Errorf("the spout marker name must be a valid filename: %v", pipelineInfo.Spout.Marker)
			}
		}
		if pipelineInfo.Spout.Service == nil && pipelineInfo.Input != nil {
			return errors.Errorf("spout pipelines (without a service) must not have an input")
		}
	}
	return nil
}

func branchProvenance(input *pps.Input) []*pfs.Branch {
	var result []*pfs.Branch
	pps.VisitInput(input, func(input *pps.Input) {
		if input.Pfs != nil {
			result = append(result, client.NewBranch(input.Pfs.Repo, input.Pfs.Branch))
		}
		if input.Cron != nil {
			result = append(result, client.NewBranch(input.Cron.Repo, "master"))
		}
		if input.Git != nil {
			result = append(result, client.NewBranch(input.Git.Name, input.Git.Branch))
		}
	})
	return result
}

// hardStopPipeline does essentially the same thing as StopPipeline (deletes the
// pipeline's branch provenance, deletes any open commits, deletes any k8s
// workers), but does it immediately. This is to avoid races between operations
// that will do subsequent work (e.g. UpdatePipeline and DeletePipeline) and the
// PPS master
func (a *apiServer) hardStopPipeline(pachClient *client.APIClient, pipelineInfo *pps.PipelineInfo) error {
	// Remove the output branch's provenance so that no new jobs can be created
	if err := pachClient.CreateBranch(
		pipelineInfo.Pipeline.Name,
		pipelineInfo.OutputBranch,
		pipelineInfo.OutputBranch,
		nil,
	); err != nil && !isNotFoundErr(err) {
		return errors.Wrapf(err, "could not recreate original output branch")
	}

	// Now that new commits won't be created on the master branch, enumerate
	// existing commits and close any open ones.
	iter, err := pachClient.ListCommitStream(pachClient.Ctx(), &pfs.ListCommitRequest{
		Repo: client.NewRepo(pipelineInfo.Pipeline.Name),
		To:   client.NewCommit(pipelineInfo.Pipeline.Name, pipelineInfo.OutputBranch),
	})
	if err != nil {
		return errors.Wrapf(err, "couldn't get open commits on '%s'", pipelineInfo.OutputBranch)
	}
	// Finish all open commits, most recent first (so that we finish the
	// current job's output commit--the oldest--last, and unblock the master
	// only after all other commits are also finished, preventing any new jobs)
	for {
		ci, err := iter.Recv()
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return err
		}
		if ci.Finished == nil {
			// Finish the commit and don't pass a tree
			pachClient.PfsAPIClient.FinishCommit(pachClient.Ctx(), &pfs.FinishCommitRequest{
				Commit: ci.Commit,
				Empty:  true,
			})
		}
	}
	return nil
}

var (
	// superUserToken is the cached auth token used by PPS to write to the spec
	// repo, create pipeline subjects, and
	superUserToken string

	// superUserTokenOnce ensures that ppsToken is only read from etcd once. These are
	// read/written by apiServer#sudo()
	superUserTokenOnce sync.Once
)

// sudo is a helper function that copies 'pachClient' grants it PPS's superuser
// token, and calls 'f' with the superuser client. This helps isolate PPS's use
// of its superuser token so that it's not widely copied and is unlikely to
// leak authority to parts of the code that aren't supposed to have it.
//
// Note that because the argument to 'f' is a superuser client, it should not
// be used to make any calls with unvalidated user input. Any such use could be
// exploited to make PPS a confused deputy
func (a *apiServer) sudo(pachClient *client.APIClient, f func(*client.APIClient) error) error {
	// Get PPS auth token
	superUserTokenOnce.Do(func() {
		b := backoff.NewExponentialBackOff()
		b.MaxElapsedTime = 60 * time.Second
		b.MaxInterval = 5 * time.Second
		if err := backoff.Retry(func() error {
			superUserTokenCol := col.NewCollection(a.env.GetEtcdClient(), ppsconsts.PPSTokenKey, nil, &types.StringValue{}, nil, nil).ReadOnly(pachClient.Ctx())
			var result types.StringValue
			if err := superUserTokenCol.Get("", &result); err != nil {
				return err
			}
			superUserToken = result.Value
			return nil
		}, b); err != nil {
			panic(fmt.Sprintf("couldn't get PPS superuser token: %v", err))
		}
	})

	// Copy pach client, but keep ctx (to propagate cancellation). Replace token
	// with superUserToken
	superUserClient := pachClient.WithCtx(pachClient.Ctx())
	superUserClient.SetAuthToken(superUserToken)
	return f(superUserClient)
}

// makePipelineInfoCommit is a helper for CreatePipeline that creates a commit
// with 'pipelineInfo' in SpecRepo (in PFS). It's called in both the case where
// a user is updating a pipeline and the case where a user is creating a new
// pipeline.
func (a *apiServer) makePipelineInfoCommit(pachClient *client.APIClient, pipelineInfo *pps.PipelineInfo) (result *pfs.Commit, retErr error) {
	pipelineName := pipelineInfo.Pipeline.Name
	var commit *pfs.Commit
	if err := a.sudo(pachClient, func(superUserClient *client.APIClient) error {
		data, err := pipelineInfo.Marshal()
		if err != nil {
			return errors.Wrapf(err, "could not marshal PipelineInfo")
		}
		if _, err = superUserClient.PutFileOverwrite(ppsconsts.SpecRepo, pipelineName, ppsconsts.SpecFile, bytes.NewReader(data), 0); err != nil {
			return err
		}
		branchInfo, err := superUserClient.InspectBranch(ppsconsts.SpecRepo, pipelineName)
		if err != nil {
			return err
		}
		commit = branchInfo.Head
		return nil
	}); err != nil {
		return nil, err
	}
	return commit, nil
}

func (a *apiServer) fixPipelineInputRepoACLs(pachClient *client.APIClient, pipelineInfo *pps.PipelineInfo, prevPipelineInfo *pps.PipelineInfo) error {
	add := make(map[string]struct{})
	remove := make(map[string]struct{})
	var pipelineName string
	// Figure out which repos 'pipeline' might no longer be using
	if prevPipelineInfo != nil {
		pipelineName = prevPipelineInfo.Pipeline.Name
		pps.VisitInput(prevPipelineInfo.Input, func(input *pps.Input) {
			var repo string
			switch {
			case input.Pfs != nil:
				repo = input.Pfs.Repo
			case input.Cron != nil:
				repo = input.Cron.Repo
			case input.Git != nil:
				repo = input.Git.Name
			default:
				return // no scope to set: input is not a repo
			}
			remove[repo] = struct{}{}
		})
	}

	// Figure out which repos 'pipeline' is using
	if pipelineInfo != nil {
		// also check that pipeline name is consistent
		if pipelineName == "" {
			pipelineName = pipelineInfo.Pipeline.Name
		} else if pipelineInfo.Pipeline.Name != pipelineName {
			return errors.Errorf("pipelineInfo (%s) and prevPipelineInfo (%s) do not "+
				"belong to matching pipelines; this is a bug",
				pipelineInfo.Pipeline.Name, prevPipelineInfo.Pipeline.Name)
		}

		// collect inputs (remove redundant inputs from 'remove', but don't
		// bother authorizing 'pipeline' twice)
		pps.VisitInput(pipelineInfo.Input, func(input *pps.Input) {
			var repo string
			switch {
			case input.Pfs != nil:
				repo = input.Pfs.Repo
			case input.Cron != nil:
				repo = input.Cron.Repo
			case input.Git != nil:
				repo = input.Git.Name
			default:
				return // no scope to set: input is not a repo
			}
			if _, ok := remove[repo]; ok {
				delete(remove, repo)
			} else {
				add[repo] = struct{}{}
			}
		})
	}
	if pipelineName == "" {
		return errors.Errorf("fixPipelineInputRepoACLs called with both current and " +
			"previous pipelineInfos == to nil; this is a bug")
	}

	var eg errgroup.Group
	// Remove pipeline from old, unused inputs
	for repo := range remove {
		repo := repo
		eg.Go(func() error {
			return a.sudo(pachClient, func(superUserClient *client.APIClient) error {
				_, err := superUserClient.SetScope(superUserClient.Ctx(), &auth.SetScopeRequest{
					Repo:     repo,
					Username: auth.PipelinePrefix + pipelineName,
					Scope:    auth.Scope_NONE,
				})
				if isNotFoundErr(err) {
					// can happen if input repo is force-deleted; nothing to remove
					return nil
				}
				return grpcutil.ScrubGRPC(err)
			})
		})
	}
	// Add pipeline to every new input's ACL as a READER
	for repo := range add {
		repo := repo
		eg.Go(func() error {
			return a.sudo(pachClient, func(superUserClient *client.APIClient) error {
				_, err := superUserClient.SetScope(superUserClient.Ctx(), &auth.SetScopeRequest{
					Repo:     repo,
					Username: auth.PipelinePrefix + pipelineName,
					Scope:    auth.Scope_READER,
				})
				return grpcutil.ScrubGRPC(err)
			})
		})
	}
	// Add pipeline to its output repo's ACL as a WRITER if it's new
	if prevPipelineInfo == nil {
		eg.Go(func() error {
			return a.sudo(pachClient, func(superUserClient *client.APIClient) error {
				_, err := superUserClient.SetScope(superUserClient.Ctx(), &auth.SetScopeRequest{
					Repo:     pipelineName,
					Username: auth.PipelinePrefix + pipelineName,
					Scope:    auth.Scope_WRITER,
				})
				return grpcutil.ScrubGRPC(err)
			})
		})
	}
	if err := eg.Wait(); err != nil {
		return errors.Wrapf(grpcutil.ScrubGRPC(err), "error fixing ACLs on \"%s\"'s input repos", pipelineName)
	}
	return nil
}

// getExpectedNumWorkers is a helper function for CreatePipeline that transforms
// the parallelism spec in CreatePipelineRequest.Parallelism into a constant
// that can be stored in EtcdPipelineInfo.Parallelism
func getExpectedNumWorkers(kc *kube.Clientset, pipelineInfo *pps.PipelineInfo) (int, error) {
	switch pspec := pipelineInfo.ParallelismSpec; {
	case pspec == nil, pspec.Constant == 0 && pspec.Coefficient == 0:
		return 1, nil
	case pspec.Constant > 0 && pspec.Coefficient == 0:
		return int(pspec.Constant), nil
	case pspec.Constant == 0 && pspec.Coefficient > 0:
		// Start ('coefficient' * 'nodes') workers. Determine number of workers
		nodeList, err := kc.CoreV1().Nodes().List(metav1.ListOptions{})
		if err != nil {
			return 0, errors.Wrapf(err, "unable to retrieve node list from k8s to determine parallelism")
		}
		if len(nodeList.Items) == 0 {
			return 0, errors.Errorf("unable to determine parallelism for %q: no k8s nodes found",
				pipelineInfo.Pipeline.Name)
		}
		numNodes := len(nodeList.Items)
		floatParallelism := math.Floor(pspec.Coefficient * float64(numNodes))
		return int(math.Max(floatParallelism, 1)), nil
	default:
		return 0, errors.Errorf("unable to interpret ParallelismSpec %+v", pspec)
	}
}

// CreatePipeline implements the protobuf pps.CreatePipeline RPC
//
// Implementation note:
// - CreatePipeline always creates pipeline output branches such that the
//   pipeline's spec branch is in the pipeline output branch's provenance
// - CreatePipeline will always create a new output commit, but that's done
//   by CreateBranch at the bottom of the function, which sets the new output
//   branch provenance, rather than makePipelineInfoCommit higher up.
// - This is because CreatePipeline calls hardStopPipeline towards the top,
// 	 breakng the provenance connection from the spec branch to the output branch
// - For straightforward pipeline updates (e.g. new pipeline image)
//   stopping + updating + starting the pipeline isn't necessary
// - However it is necessary in many slightly atypical cases  (e.g. the
//   pipeline input changed: if the spec commit is created while the
//   output branch has its old provenance, or the output branch gets new
//   provenance while the old spec commit is the HEAD of the spec branch,
//   then an output commit will be created with provenance that doesn't
//   match its spec's PipelineInfo.Input. Another example is when
//   request.Reprocess == true).
// - Rather than try to enumerate every case where we can't create a spec
//   commit without stopping the pipeline, we just always stop the pipeline
func (a *apiServer) CreatePipeline(ctx context.Context, request *pps.CreatePipelineRequest) (response *types.Empty, retErr error) {
	func() { a.Log(request, nil, nil, 0) }()
	defer func(start time.Time) { a.Log(request, response, retErr, time.Since(start)) }(time.Now())
	metricsFn := metrics.ReportUserAction(ctx, a.reporter, "CreatePipeline")
	defer func(start time.Time) { metricsFn(start, retErr) }(time.Now())

	// Validate request
	if err := a.validatePipelineRequest(request); err != nil {
		return nil, err
	}

	// Annotate current span with pipeline name
	span := opentracing.SpanFromContext(ctx)
	tracing.TagAnySpan(span, "pipeline", request.Pipeline.Name)
	defer func() {
		tracing.TagAnySpan(span, "err", retErr)
	}()

	// propagate trace info (doesn't affect intra-RPC trace)
	ctx = extended.TraceIn2Out(ctx)
	pachClient := a.env.GetPachClient(ctx)
	ctx = pachClient.Ctx() // GetPachClient propagates auth info to inner ctx
	pfsClient := pachClient.PfsAPIClient
	// Reprocess overrides the salt in the request
	if request.Salt == "" || request.Reprocess {
		request.Salt = uuid.NewWithoutDashes()
	}
	pipelineInfo := &pps.PipelineInfo{
		Pipeline:              request.Pipeline,
		Version:               1,
		Transform:             request.Transform,
		TFJob:                 request.TFJob,
		ParallelismSpec:       request.ParallelismSpec,
		HashtreeSpec:          request.HashtreeSpec,
		Input:                 request.Input,
		OutputBranch:          request.OutputBranch,
		Egress:                request.Egress,
		CreatedAt:             now(),
		ResourceRequests:      request.ResourceRequests,
		ResourceLimits:        request.ResourceLimits,
		SidecarResourceLimits: request.SidecarResourceLimits,
		Description:           request.Description,
		CacheSize:             request.CacheSize,
		EnableStats:           request.EnableStats,
		Salt:                  request.Salt,
		MaxQueueSize:          request.MaxQueueSize,
		Service:               request.Service,
		Spout:                 request.Spout,
		ChunkSpec:             request.ChunkSpec,
		DatumTimeout:          request.DatumTimeout,
		JobTimeout:            request.JobTimeout,
		Standby:               request.Standby,
		DatumTries:            request.DatumTries,
		SchedulingSpec:        request.SchedulingSpec,
		PodSpec:               request.PodSpec,
		PodPatch:              request.PodPatch,
		S3Out:                 request.S3Out,
		Metadata:              request.Metadata,
	}
	if err := setPipelineDefaults(pipelineInfo); err != nil {
		return nil, err
	}
	// Validate final PipelineInfo (now that defaults have been populated)
	if err := a.validatePipeline(pachClient, pipelineInfo); err != nil {
		return nil, err
	}

	var visitErr error
	pps.VisitInput(pipelineInfo.Input, func(input *pps.Input) {
		if input.Cron != nil {
			if _, err := pachClient.PfsAPIClient.CreateRepo(pachClient.Ctx(),
				&pfs.CreateRepoRequest{
					Repo:        client.NewRepo(input.Cron.Repo),
					Description: fmt.Sprintf("Cron tick repo for pipeline %s.", request.Pipeline.Name),
				}); err != nil && !isAlreadyExistsErr(err) {
				visitErr = err
			}
		}
		if input.Git != nil {
			if _, err := pachClient.PfsAPIClient.CreateRepo(pachClient.Ctx(),
				&pfs.CreateRepoRequest{
					Repo:        client.NewRepo(input.Git.Name),
					Description: fmt.Sprintf("Git input repo for pipeline %s.", request.Pipeline.Name),
				}); err != nil && !isAlreadyExistsErr(err) {
				visitErr = err
			}
		}
	})
	if visitErr != nil {
		return nil, visitErr
	}

	// Authorize pipeline creation
	operation := pipelineOpCreate
	if request.Update {
		operation = pipelineOpUpdate
	}
	if err := a.authorizePipelineOp(pachClient, operation, pipelineInfo.Input, pipelineInfo.Pipeline.Name); err != nil {
		return nil, err
	}
	pipelineName := pipelineInfo.Pipeline.Name
	pps.SortInput(pipelineInfo.Input) // Makes datum hashes comparable
	update := false
	if request.Update {
		// inspect the pipeline to see if this is a real update
		if _, err := a.inspectPipeline(pachClient, request.Pipeline.Name); err == nil {
			update = true
		}
	}
	var (
		// provenance for the pipeline's output branch (includes the spec branch)
		provenance = append(branchProvenance(pipelineInfo.Input),
			client.NewBranch(ppsconsts.SpecRepo, pipelineName))
		outputBranch     = client.NewBranch(pipelineName, pipelineInfo.OutputBranch)
		statsBranch      = client.NewBranch(pipelineName, "stats")
		markerBranch     = client.NewBranch(pipelineName, ppsconsts.SpoutMarkerBranch)
		outputBranchHead *pfs.Commit
		statsBranchHead  *pfs.Commit
		markerBranchHead *pfs.Commit
	)

	// Get the expected number of workers for this pipeline
	parallelism, err := getExpectedNumWorkers(a.env.GetKubeClient(), pipelineInfo)
	if err != nil {
		return nil, err
	}

	if update {
		// Help user fix inconsistency if previous UpdatePipeline call failed
		if ci, err := pachClient.InspectCommit(ppsconsts.SpecRepo, pipelineName); err != nil {
			return nil, err
		} else if ci.Finished == nil {
			return nil, errors.Errorf("the HEAD commit of this pipeline's spec branch " +
				"is open. Either another CreatePipeline call is running or a previous " +
				"call crashed. If you're sure no other CreatePipeline commands are " +
				"running, you can run 'pachctl update pipeline --clean' which will " +
				"delete this open commit")
		}

		// Remove provenance from existing output branch, so that creating a new
		// spec commit doesn't create an output commit in the old output branch.
		if err := a.hardStopPipeline(pachClient, pipelineInfo); err != nil {
			return nil, err
		}

		// Look up existing pipelineInfo and update it, writing updated
		// pipelineInfo back to PFS in a new commit. Do this inside an etcd
		// transaction as PFS doesn't support transactions and this prevents
		// concurrent UpdatePipeline calls from racing
		var (
			pipelinePtr     pps.EtcdPipelineInfo
			oldPipelineInfo *pps.PipelineInfo
		)
		if _, err := col.NewSTM(ctx, a.env.GetEtcdClient(), func(stm col.STM) error {
			// Read existing PipelineInfo from PFS output repo
			return a.pipelines.ReadWrite(stm).Update(pipelineName, &pipelinePtr, func() error {
				var err error
				oldPipelineInfo, err = ppsutil.GetPipelineInfo(pachClient, pipelineName, &pipelinePtr)
				if err != nil {
					return err
				}

				// Cannot disable stats after it has been enabled.
				if oldPipelineInfo.EnableStats && !pipelineInfo.EnableStats {
					return newErrPipelineUpdate(pipelineInfo.Pipeline.Name, "cannot disable stats")
				}

				// Modify pipelineInfo (increment Version, and *preserve Stopped* so
				// that updating a pipeline doesn't restart it)
				pipelineInfo.Version = oldPipelineInfo.Version + 1
				if oldPipelineInfo.Stopped {
					provenance = nil // CreateBranch() below shouldn't create new output
					pipelineInfo.Stopped = true
				}
				if !request.Reprocess {
					pipelineInfo.Salt = oldPipelineInfo.Salt
				}
				// Must create spec commit before restoring output branch provenance, so
				// that no commits are created with a mismatched spec commit
				specCommit, err := a.makePipelineInfoCommit(pachClient, pipelineInfo)
				if err != nil {
					return err
				}
				// Update pipelinePtr to point to new commit
				pipelinePtr.SpecCommit = specCommit
				// Reset pipeline state (PPS master/pipeline controller recreates RC)
				pipelinePtr.State = pps.PipelineState_PIPELINE_STARTING
				// Clear any failure reasons
				pipelinePtr.Reason = ""
				// Update pipeline parallelism
				pipelinePtr.Parallelism = uint64(parallelism)
				return nil
			})
		}); err != nil {
			return nil, err
		}

		if !request.Reprocess {
			// don't branch the output/stats/marker commit chain from the old pipeline (re-use old branch HEAD)
			// However it's valid to set request.Update == true even if no pipeline exists, so only
			// set outputBranchHead if there's an old pipeline to update
			_, err := pfsClient.InspectBranch(ctx, &pfs.InspectBranchRequest{Branch: outputBranch})
			if err != nil && !isNotFoundErr(err) {
				return nil, err
			} else if err == nil {
				outputBranchHead = client.NewCommit(pipelineName, pipelineInfo.OutputBranch)
			}

			_, err = pfsClient.InspectBranch(ctx, &pfs.InspectBranchRequest{Branch: statsBranch})
			if err != nil && !isNotFoundErr(err) {
				return nil, err
			} else if err == nil {
				statsBranchHead = client.NewCommit(pipelineName, "stats")
			}

			_, err = pfsClient.InspectBranch(ctx, &pfs.InspectBranchRequest{Branch: markerBranch})
			if err != nil && !isNotFoundErr(err) {
				return nil, err
			} else if err == nil {
				markerBranchHead = client.NewCommit(pipelineName, ppsconsts.SpoutMarkerBranch)
			}
		}

		if pipelinePtr.AuthToken != "" {
			if err := a.fixPipelineInputRepoACLs(pachClient, pipelineInfo, oldPipelineInfo); err != nil {
				return nil, err
			}
		}
	} else {
		// Create output repo, pipeline output, and stats
		if _, err := pachClient.PfsAPIClient.CreateRepo(pachClient.Ctx(),
			&pfs.CreateRepoRequest{
				Repo:        client.NewRepo(pipelineName),
				Description: fmt.Sprintf("Output repo for pipeline %s.", request.Pipeline.Name),
			}); err != nil && !isAlreadyExistsErr(err) {
			return nil, err
		}

		// Must create spec commit before restoring output branch provenance, so
		// that no commits are created with a missing spec commit
		var commit *pfs.Commit
		if request.SpecCommit != nil {
			// Make sure that the spec commit actually exists
			commitInfo, err := pachClient.PfsAPIClient.InspectCommit(pachClient.Ctx(), &pfs.InspectCommitRequest{Commit: request.SpecCommit})
			if err != nil {
				return nil, errors.Wrap(err, "error inspecting spec commit")
			}
			// It does, so we use that as the spec commit, rather than making a new one
			commit = commitInfo.Commit
			// We also use the existing head for the branches, rather than making a new one.
			outputBranchHead = client.NewCommit(pipelineName, pipelineInfo.OutputBranch)
			statsBranchHead = client.NewCommit(pipelineName, "stats")
		} else {
			var err error
			commit, err = a.makePipelineInfoCommit(pachClient, pipelineInfo)
			if err != nil {
				return nil, err
			}
		}

		// pipelinePtr will be written to etcd, pointing at 'commit'. May include an
		// auth token
		pipelinePtr := &pps.EtcdPipelineInfo{
			SpecCommit:  commit,
			State:       pps.PipelineState_PIPELINE_STARTING,
			Parallelism: uint64(parallelism),
		}

		// Generate pipeline's auth token & add pipeline to the ACLs of input/output
		// repos
		if err := a.sudo(pachClient, func(superUserClient *client.APIClient) error {
			tokenResp, err := superUserClient.GetAuthToken(superUserClient.Ctx(), &auth.GetAuthTokenRequest{
				Subject: auth.PipelinePrefix + request.Pipeline.Name,
			})
			if err != nil {
				if auth.IsErrNotActivated(err) {
					return nil // no auth work to do
				}
				return grpcutil.ScrubGRPC(err)
			}
			pipelinePtr.AuthToken = tokenResp.Token
			return nil
		}); err != nil {
			return nil, err
		}

		// Put a pointer to the new PipelineInfo commit into etcd
		if _, err := col.NewSTM(ctx, a.env.GetEtcdClient(), func(stm col.STM) error {
			err := a.pipelines.ReadWrite(stm).Create(pipelineName, pipelinePtr)
			if isAlreadyExistsErr(err) {
				if err := a.sudo(pachClient, func(superUserClient *client.APIClient) error {
					return superUserClient.DeleteCommit(ppsconsts.SpecRepo, commit.ID)
				}); err != nil {
					return errors.Wrapf(grpcutil.ScrubGRPC(err), "couldn't clean up orphaned spec commit")
				}
				return newErrPipelineExists(pipelineName)
			}
			return err
		}); err != nil {
			return nil, err
		}
		if pipelinePtr.AuthToken != "" {
			if err := a.fixPipelineInputRepoACLs(pachClient, pipelineInfo, nil); err != nil {
				return nil, err
			}
		}
	}

	// spouts don't need to keep track of branch provenance since they are essentially inputs
	if request.Spout != nil {
		provenance = nil
	}

	// Create/update output branch (creating new output commit for the pipeline
	// and restarting the pipeline)
	if _, err := pfsClient.CreateBranch(ctx, &pfs.CreateBranchRequest{
		Branch:     outputBranch,
		Provenance: provenance,
		Head:       outputBranchHead,
	}); err != nil {
		return nil, errors.Wrapf(err, "could not create/update output branch")
	}
	if pipelineInfo.EnableStats {
		if _, err := pfsClient.CreateBranch(ctx, &pfs.CreateBranchRequest{
			Branch:     client.NewBranch(pipelineName, "stats"),
			Provenance: []*pfs.Branch{outputBranch},
			Head:       statsBranchHead,
		}); err != nil {
			return nil, errors.Wrapf(err, "could not create/update stats branch")
		}
	}
	if pipelineInfo.Spout != nil && pipelineInfo.Spout.Marker != "" {
		if _, err := pfsClient.CreateBranch(ctx, &pfs.CreateBranchRequest{
			Branch: markerBranch,
			Head:   markerBranchHead,
		}); err != nil {
			return nil, errors.Wrapf(err, "could not create/update marker branch")
		}
	}

	return &types.Empty{}, nil
}

// setPipelineDefaults sets the default values for a pipeline info
func setPipelineDefaults(pipelineInfo *pps.PipelineInfo) error {
	now := time.Now()
	if pipelineInfo.Transform.Image == "" {
		pipelineInfo.Transform.Image = DefaultUserImage
	}
	pps.VisitInput(pipelineInfo.Input, func(input *pps.Input) {
		if input.Pfs != nil {
			if input.Pfs.Branch == "" {
				input.Pfs.Branch = "master"
			}
			if input.Pfs.Name == "" {
				input.Pfs.Name = input.Pfs.Repo
			}
		}
		if input.Cron != nil {
			if input.Cron.Start == nil {
				start, _ := types.TimestampProto(now)
				input.Cron.Start = start
			}
			if input.Cron.Repo == "" {
				input.Cron.Repo = fmt.Sprintf("%s_%s", pipelineInfo.Pipeline.Name, input.Cron.Name)
			}
		}
		if input.Git != nil {
			if input.Git.Branch == "" {
				input.Git.Branch = "master"
			}
			if input.Git.Name == "" {
				// We know URL looks like:
				// "https://github.com/sjezewski/testgithook.git",
				tokens := strings.Split(path.Base(input.Git.URL), ".")
				input.Git.Name = tokens[0]
			}
		}
	})
	if pipelineInfo.OutputBranch == "" {
		// Output branches default to master
		pipelineInfo.OutputBranch = "master"
	}
	if pipelineInfo.CacheSize == "" {
		pipelineInfo.CacheSize = "64M"
	}
	if pipelineInfo.MaxQueueSize < 1 {
		pipelineInfo.MaxQueueSize = 1
	}
	if pipelineInfo.DatumTries == 0 {
		pipelineInfo.DatumTries = DefaultDatumTries
	}
	if pipelineInfo.Service != nil {
		if pipelineInfo.Service.Type == "" {
			pipelineInfo.Service.Type = string(v1.ServiceTypeNodePort)
		}
	}
	if pipelineInfo.Spout != nil && pipelineInfo.Spout.Service != nil && pipelineInfo.Spout.Service.Type == "" {
		pipelineInfo.Spout.Service.Type = string(v1.ServiceTypeNodePort)
	}
	return nil
}

// InspectPipeline implements the protobuf pps.InspectPipeline RPC
func (a *apiServer) InspectPipeline(ctx context.Context, request *pps.InspectPipelineRequest) (response *pps.PipelineInfo, retErr error) {
	func() { a.Log(request, nil, nil, 0) }()
	defer func(start time.Time) { a.Log(request, response, retErr, time.Since(start)) }(time.Now())
	pachClient := a.env.GetPachClient(ctx)
	return a.inspectPipeline(pachClient, request.Pipeline.Name)
}

// inspectPipeline contains the functional implementation of InspectPipeline.
// Many functions (GetLogs, ListPipeline, CreateJob) need to inspect a pipeline,
// so they call this instead of making an RPC
func (a *apiServer) inspectPipeline(pachClient *client.APIClient, name string) (*pps.PipelineInfo, error) {
	if _, err := checkLoggedIn(pachClient); err != nil {
		return nil, err
	}
	kubeClient := a.env.GetKubeClient()
	name, ancestors, err := ancestry.Parse(name)
	if err != nil {
		return nil, err
	}
	pipelinePtr := pps.EtcdPipelineInfo{}
	if err := a.pipelines.ReadOnly(pachClient.Ctx()).Get(name, &pipelinePtr); err != nil {
		if col.IsErrNotFound(err) {
			return nil, errors.Errorf("pipeline \"%s\" not found", name)
		}
		return nil, err
	}
	pipelinePtr.SpecCommit.ID = ancestry.Add(pipelinePtr.SpecCommit.ID, ancestors)
	pipelineInfo, err := ppsutil.GetPipelineInfo(pachClient, name, &pipelinePtr)
	if err != nil {
		return nil, err
	}
	if pipelineInfo.Service != nil {
		rcName := ppsutil.PipelineRcName(pipelineInfo.Pipeline.Name, pipelineInfo.Version)
		if err != nil {
			return nil, err
		}
		service, err := kubeClient.CoreV1().Services(a.namespace).Get(fmt.Sprintf("%s-user", rcName), metav1.GetOptions{})
		if err != nil {
			if !isNotFoundErr(err) {
				return nil, err
			}
		} else {
			pipelineInfo.Service.IP = service.Spec.ClusterIP
		}
	}
	var hasGitInput bool
	pps.VisitInput(pipelineInfo.Input, func(input *pps.Input) {
		if input.Git != nil {
			hasGitInput = true
		}
	})
	if hasGitInput {
		pipelineInfo.GithookURL = "pending"
		svc, err := getGithookService(kubeClient, a.namespace)
		if err != nil {
			return pipelineInfo, nil
		}
		numIPs := len(svc.Status.LoadBalancer.Ingress)
		if numIPs == 0 {
			// When running locally, no external IP is set
			return pipelineInfo, nil
		}
		if numIPs != 1 {
			return nil, errors.Errorf("unexpected number of external IPs set for githook service")
		}
		ingress := svc.Status.LoadBalancer.Ingress[0]
		if ingress.IP != "" {
			// GKE load balancing
			pipelineInfo.GithookURL = githook.URLFromDomain(ingress.IP)
		} else if ingress.Hostname != "" {
			// AWS load balancing
			pipelineInfo.GithookURL = githook.URLFromDomain(ingress.Hostname)
		}
	}

	workerPoolID := ppsutil.PipelineRcName(pipelineInfo.Pipeline.Name, pipelineInfo.Version)
	workerStatus, err := workerserver.Status(pachClient.Ctx(), workerPoolID, a.env.GetEtcdClient(), a.etcdPrefix, a.workerGrpcPort)
	if err != nil {
		logrus.Errorf("failed to get worker status with err: %s", err.Error())
	} else {
		pipelineInfo.WorkersAvailable = int64(len(workerStatus))
		pipelineInfo.WorkersRequested = int64(pipelinePtr.Parallelism)
	}
	return pipelineInfo, nil
}

// ListPipeline implements the protobuf pps.ListPipeline RPC
func (a *apiServer) ListPipeline(ctx context.Context, request *pps.ListPipelineRequest) (response *pps.PipelineInfos, retErr error) {
	func() { a.Log(request, nil, nil, 0) }()
	defer func(start time.Time) {
		if response != nil && len(response.PipelineInfo) > client.MaxListItemsLog {
			logrus.Infof("Response contains %d objects; logging the first %d", len(response.PipelineInfo), client.MaxListItemsLog)
			a.Log(request, &pps.PipelineInfos{PipelineInfo: response.PipelineInfo[:client.MaxListItemsLog]}, retErr, time.Since(start))
		} else {
			a.Log(request, response, retErr, time.Since(start))
		}
	}(time.Now())
	pachClient := a.env.GetPachClient(ctx)
	_, err := checkLoggedIn(pachClient)
	if err != nil {
		return nil, err
	}
	pipelineInfos := &pps.PipelineInfos{}
	if err := a.listPipeline(pachClient, request, func(pi *pps.PipelineInfo) error {
		pipelineInfos.PipelineInfo = append(pipelineInfos.PipelineInfo, pi)
		return nil
	}); err != nil {
		return nil, err
	}
	return pipelineInfos, nil
}

func (a *apiServer) listPipeline(pachClient *client.APIClient, request *pps.ListPipelineRequest, f func(*pps.PipelineInfo) error) error {
	return a.listPipelinePtr(pachClient, request.Pipeline, request.History,
		func(name string, ptr *pps.EtcdPipelineInfo) error {
			pipelineInfo, err := ppsutil.GetPipelineInfo(pachClient, name, ptr)
			if err != nil {
				return err
			}
			return f(pipelineInfo)
		})
}

// listPipelinePtr enumerates all PPS pipelines in etcd, filters them based on
// 'request', and then calls 'f' on each value
func (a *apiServer) listPipelinePtr(pachClient *client.APIClient,
	pipeline *pps.Pipeline, history int64, f func(string, *pps.EtcdPipelineInfo) error) error {
	p := &pps.EtcdPipelineInfo{}
	forEachPipeline := func(name string) error {
		for i := int64(0); i <= history || history == -1; i++ {
			if err := f(name, p); err != nil {
				return err
			}
			ci, err := pachClient.InspectCommit(ppsconsts.SpecRepo, p.SpecCommit.ID)
			switch {
			case err != nil:
				return err
			case ci.ParentCommit == nil:
				return nil
			default:
				p.SpecCommit = ci.ParentCommit
			}
		}
		return nil // shouldn't happen
	}
	if pipeline == nil {
		if err := a.pipelines.ReadOnly(pachClient.Ctx()).List(p, col.DefaultOptions, func(name string) error {
			return forEachPipeline(name)
		}); err != nil {
			return err
		}
	} else {
		if err := a.pipelines.ReadOnly(pachClient.Ctx()).Get(pipeline.Name, p); err != nil {
			if col.IsErrNotFound(err) {
				return errors.Errorf("pipeline \"%s\" not found", pipeline.Name)
			}
			return err
		}
		if err := forEachPipeline(pipeline.Name); err != nil {
			return err
		}
	}
	return nil
}

// DeletePipeline implements the protobuf pps.DeletePipeline RPC
func (a *apiServer) DeletePipeline(ctx context.Context, request *pps.DeletePipelineRequest) (response *types.Empty, retErr error) {
	func() { a.Log(request, nil, nil, 0) }()
	defer func(start time.Time) { a.Log(request, response, retErr, time.Since(start)) }(time.Now())
	pachClient := a.env.GetPachClient(ctx)
	ctx, err := checkLoggedIn(pachClient)
	if err != nil {
		return nil, err
	}

	// Possibly list pipelines in etcd (skip PFS read--don't need it) and delete them
	if request.All {
		request.Pipeline = &pps.Pipeline{}
		pipelinePtr := &pps.EtcdPipelineInfo{}
		if err := a.pipelines.ReadOnly(ctx).List(pipelinePtr, col.DefaultOptions, func(pipelineName string) error {
			request.Pipeline.Name = pipelineName
			_, err := a.deletePipeline(pachClient, request)
			return err
		}); err != nil {
			return nil, err
		}
		return &types.Empty{}, nil
	}

	// Otherwise delete single pipeline from request
	return a.deletePipeline(pachClient, request)
}

// cleanUpSpecBranch handles the corner case where a spec branch was created for
// a new pipeline, but the etcdPipelineInfo was never created successfully (and
// the pipeline is in an inconsistent state). It's called if a pipeline's
// etcdPipelineInfo wasn't found, checks if an orphaned branch exists, and if
// so, deletes the orphaned branch.
func (a *apiServer) cleanUpSpecBranch(pachClient *client.APIClient, pipeline string) error {
	specBranchInfo, err := pachClient.InspectBranch(ppsconsts.SpecRepo, pipeline)
	if err != nil && (specBranchInfo != nil && specBranchInfo.Head != nil) {
		// No spec branch (and no etcd pointer) => the pipeline doesn't exist
		return errors.Wrapf(err, "pipeline %v was not found", pipeline)
	}
	// branch exists but head is nil => pipeline creation never finished/
	// pps state is invalid. Delete nil branch
	return grpcutil.ScrubGRPC(a.sudo(pachClient, func(superUserClient *client.APIClient) error {
		return superUserClient.DeleteBranch(ppsconsts.SpecRepo, pipeline, true)
	}))
}

func (a *apiServer) deletePipeline(pachClient *client.APIClient, request *pps.DeletePipelineRequest) (response *types.Empty, retErr error) {
	ctx := pachClient.Ctx() // pachClient will propagate auth info

	// Check if there's an EtcdPipelineInfo for this pipeline. If not, we can't
	// authorize, and must return something here
	pipelinePtr := pps.EtcdPipelineInfo{}
	if err := a.pipelines.ReadOnly(ctx).Get(request.Pipeline.Name, &pipelinePtr); err != nil {
		if col.IsErrNotFound(err) {
			if err := a.cleanUpSpecBranch(pachClient, request.Pipeline.Name); err != nil {
				return nil, err
			}
			return &types.Empty{}, nil

		}
		return nil, err
	}

	// Get current pipeline info from:
	// - etcdPipelineInfo
	// - spec commit in etcdPipelineInfo (which may not be the HEAD of the
	//   pipeline's spec branch_
	// - kubernetes services (for service pipelines, githook pipelines, etc)
	pipelineInfo, err := a.inspectPipeline(pachClient, request.Pipeline.Name)
	if err != nil {
		logrus.Errorf("error inspecting pipeline: %v", err)
		pipelineInfo = &pps.PipelineInfo{Pipeline: request.Pipeline, OutputBranch: "master"}
	}

	// check if the output repo exists--if not, the pipeline is non-functional and
	// the rest of the delete operation continues without any auth checks
	if _, err := pachClient.InspectRepo(request.Pipeline.Name); err != nil && !isNotFoundErr(err) {
		return nil, err
	} else if !isNotFoundErr(err) {
		// Check if the caller is authorized to delete this pipeline. This must be
		// done after cleaning up the spec branch HEAD commit, because the
		// authorization condition depends on the pipeline's PipelineInfo
		if err := a.authorizePipelineOp(pachClient, pipelineOpDelete, pipelineInfo.Input, pipelineInfo.Pipeline.Name); err != nil {
			return nil, err
		}
		if request.KeepRepo {
			// Remove branch provenance (pass branch twice so that it continues to point
			// at the same commit, but also pass empty provenance slice)
			if err := pachClient.CreateBranch(
				request.Pipeline.Name,
				pipelineInfo.OutputBranch,
				pipelineInfo.OutputBranch,
				nil,
			); err != nil {
				return nil, err
			}
		} else {
			// delete the pipeline's output repo
			if err := pachClient.DeleteRepo(request.Pipeline.Name, request.Force); err != nil {
				return nil, err
			}
		}
	}

	// Delete pipeline's workers
	if err := a.deletePipelineResources(ctx, request.Pipeline.Name); err != nil {
		return nil, errors.Wrapf(err, "error deleting workers")
	}

	// If necessary, revoke the pipeline's auth token and remove it from its
	// inputs' ACLs
	if pipelinePtr.AuthToken != "" {
		// If auth was deactivated after the pipeline was created, don't bother
		// revoking
		if _, err := pachClient.WhoAmI(pachClient.Ctx(), &auth.WhoAmIRequest{}); err == nil {
			if err := a.sudo(pachClient, func(superUserClient *client.APIClient) error {
				// 'pipelineInfo' == nil => remove pipeline from all input repos
				if err := a.fixPipelineInputRepoACLs(superUserClient, nil, pipelineInfo); err != nil {
					return grpcutil.ScrubGRPC(err)
				}
				_, err := superUserClient.RevokeAuthToken(superUserClient.Ctx(),
					&auth.RevokeAuthTokenRequest{
						Token: pipelinePtr.AuthToken,
					})
				return grpcutil.ScrubGRPC(err)
			}); err != nil {
				return nil, errors.Wrapf(err, "error revoking old auth token")
			}
		}
	}

	// Kill or delete all of the pipeline's jobs
	var eg errgroup.Group
	jobPtr := &pps.EtcdJobInfo{}
	if err := a.jobs.ReadOnly(ctx).GetByIndex(ppsdb.JobsPipelineIndex, request.Pipeline, jobPtr, col.DefaultOptions, func(jobID string) error {
		eg.Go(func() error {
			_, err := a.DeleteJob(ctx, &pps.DeleteJobRequest{Job: client.NewJob(jobID)})
			if isNotFoundErr(err) {
				return nil
			}
			return err
		})
		return nil
	}); err != nil {
		return nil, err
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}

	eg = errgroup.Group{}
	// Delete pipeline branch in SpecRepo (leave commits, to preserve downstream
	// commits)
	eg.Go(func() error {
		return a.sudo(pachClient, func(superUserClient *client.APIClient) error {
			return grpcutil.ScrubGRPC(superUserClient.DeleteBranch(ppsconsts.SpecRepo, request.Pipeline.Name, request.Force))
		})
	})
	// Delete EtcdPipelineInfo
	eg.Go(func() error {
		if _, err := col.NewSTM(ctx, a.env.GetEtcdClient(), func(stm col.STM) error {
			return a.pipelines.ReadWrite(stm).Delete(request.Pipeline.Name)
		}); err != nil {
			return errors.Wrapf(err, "collection.Delete")
		}
		return nil
	})
	// Delete cron input repos
	if !request.KeepRepo {
		pps.VisitInput(pipelineInfo.Input, func(input *pps.Input) {
			if input.Cron != nil {
				eg.Go(func() error {
					return pachClient.DeleteRepo(input.Cron.Repo, request.Force)
				})
			}
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	return &types.Empty{}, nil
}

// StartPipeline implements the protobuf pps.StartPipeline RPC
func (a *apiServer) StartPipeline(ctx context.Context, request *pps.StartPipelineRequest) (response *types.Empty, retErr error) {
	func() { a.Log(request, nil, nil, 0) }()
	defer func(start time.Time) { a.Log(request, response, retErr, time.Since(start)) }(time.Now())
	pachClient := a.env.GetPachClient(ctx)

	// Get request.Pipeline's info
	pipelineInfo, err := a.inspectPipeline(pachClient, request.Pipeline.Name)
	if err != nil {
		return nil, err
	}

	// check if the caller is authorized to update this pipeline
	if err := a.authorizePipelineOp(pachClient, pipelineOpUpdate, pipelineInfo.Input, pipelineInfo.Pipeline.Name); err != nil {
		return nil, err
	}

	// Remove 'Stopped' from the pipeline spec
	pipelineInfo.Stopped = false
	commit, err := a.makePipelineInfoCommit(pachClient, pipelineInfo)
	if err != nil {
		return nil, err
	}
	if a.updatePipelineSpecCommit(pachClient, request.Pipeline.Name, commit); err != nil {
		return nil, err
	}

	// Replace missing branch provenance (removed by StopPipeline)
	provenance := append(branchProvenance(pipelineInfo.Input),
		client.NewBranch(ppsconsts.SpecRepo, pipelineInfo.Pipeline.Name))
	if err := pachClient.CreateBranch(
		request.Pipeline.Name,
		pipelineInfo.OutputBranch,
		pipelineInfo.OutputBranch,
		provenance,
	); err != nil {
		return nil, err
	}
	return &types.Empty{}, nil
}

// StopPipeline implements the protobuf pps.StopPipeline RPC
func (a *apiServer) StopPipeline(ctx context.Context, request *pps.StopPipelineRequest) (response *types.Empty, retErr error) {
	func() { a.Log(request, nil, nil, 0) }()
	defer func(start time.Time) { a.Log(request, response, retErr, time.Since(start)) }(time.Now())
	pachClient := a.env.GetPachClient(ctx)

	// Get request.Pipeline's info
	pipelineInfo, err := a.inspectPipeline(pachClient, request.Pipeline.Name)
	if err != nil {
		return nil, err
	}

	// check if the caller is authorized to update this pipeline
	if err := a.authorizePipelineOp(pachClient, pipelineOpUpdate, pipelineInfo.Input, pipelineInfo.Pipeline.Name); err != nil {
		return nil, err
	}

	// Remove branch provenance (pass branch twice so that it continues to point
	// at the same commit, but also pass empty provenance slice)
	if err := pachClient.CreateBranch(
		request.Pipeline.Name,
		pipelineInfo.OutputBranch,
		pipelineInfo.OutputBranch,
		nil,
	); err != nil {
		return nil, err
	}

	// Update PipelineInfo with new state
	pipelineInfo.Stopped = true
	commit, err := a.makePipelineInfoCommit(pachClient, pipelineInfo)
	if err != nil {
		return nil, err
	}
	if a.updatePipelineSpecCommit(pachClient, request.Pipeline.Name, commit); err != nil {
		return nil, err
	}
	return &types.Empty{}, nil
}

func (a *apiServer) RunPipeline(ctx context.Context, request *pps.RunPipelineRequest) (response *types.Empty, retErr error) {
	func() { a.Log(request, nil, nil, 0) }()
	defer func(start time.Time) { a.Log(request, response, retErr, time.Since(start)) }(time.Now())

	pachClient := a.env.GetPachClient(ctx)
	ctx = pachClient.Ctx() // pachClient will propagate auth info
	pfsClient := pachClient.PfsAPIClient
	ppsClient := pachClient.PpsAPIClient

	pipelineInfo, err := a.inspectPipeline(pachClient, request.Pipeline.Name)
	if err != nil {
		return nil, err
	}
	// make sure the user isn't trying to run pipeline on an empty branch
	branch, err := pfsClient.InspectBranch(ctx, &pfs.InspectBranchRequest{
		Branch: client.NewBranch(request.Pipeline.Name, pipelineInfo.OutputBranch),
	})
	if err != nil {
		return nil, err
	}
	if branch.Head == nil {
		return nil, errors.Errorf("run pipeline needs a pipeline with existing data to run\nnew commits will trigger the pipeline automatically, so this only needs to be used if you need to run the pipeline on an old version of the data, or to rerun an job")
	}

	key := path.Join
	branchProvMap := make(map[string]bool)

	// include the branch and its provenance in the branch provenance map
	branchProvMap[key(branch.Branch.Repo.Name, branch.Name)] = true
	for _, b := range branch.Provenance {
		branchProvMap[key(b.Repo.Name, b.Name)] = true
	}
	if branch.Head != nil {
		headCommitInfo, err := pfsClient.InspectCommit(ctx, &pfs.InspectCommitRequest{
			Commit: client.NewCommit(branch.Branch.Repo.Name, branch.Head.ID),
		})
		if err != nil {
			return nil, err
		}
		for _, prov := range headCommitInfo.Provenance {
			branchProvMap[key(prov.Branch.Repo.Name, prov.Branch.Name)] = true
		}
	}

	// we need to inspect the commit in order to resolve the commit ID, which may have an ancestry tag
	specCommit, err := pfsClient.InspectCommit(ctx, &pfs.InspectCommitRequest{
		Commit: pipelineInfo.SpecCommit,
	})
	if err != nil {
		return nil, err
	}
	provenance := request.Provenance
	provenanceMap := make(map[string]*pfs.CommitProvenance)

	if request.JobID != "" {
		jobInfo, err := ppsClient.InspectJob(ctx, &pps.InspectJobRequest{
			Job: client.NewJob(request.JobID),
		})
		if err != nil {
			return nil, err
		}
		jobOutputCommit, err := pfsClient.InspectCommit(ctx, &pfs.InspectCommitRequest{
			Commit: jobInfo.OutputCommit,
		})
		if err != nil {
			return nil, err
		}
		provenance = append(provenance, jobOutputCommit.Provenance...)
	}

	for _, prov := range provenance {
		if prov == nil {
			return nil, errors.Errorf("request should not contain nil provenance")
		}
		branch := prov.Branch
		if branch == nil {
			if prov.Commit == nil {
				return nil, errors.Errorf("request provenance cannot have both a nil commit and nil branch")
			}
			provCommit, err := pfsClient.InspectCommit(ctx, &pfs.InspectCommitRequest{
				Commit: prov.Commit,
			})
			if err != nil {
				return nil, err
			}
			branch = provCommit.Branch
			prov.Commit = provCommit.Commit
		}
		if branch.Repo == nil {
			return nil, errors.Errorf("request provenance branch must have a non nil repo")
		}
		_, err := pfsClient.InspectRepo(ctx, &pfs.InspectRepoRequest{Repo: branch.Repo})
		if err != nil {
			return nil, err
		}

		// ensure the commit provenance is consistent with the branch provenance
		if len(branchProvMap) != 0 {
			if branch.Repo.Name != ppsconsts.SpecRepo && !branchProvMap[key(branch.Repo.Name, branch.Name)] {
				return nil, errors.Errorf("the commit provenance contains a branch which the pipeline's branch is not provenant on")
			}
		}
		provenanceMap[key(branch.Repo.Name, branch.Name)] = prov
	}

	for _, branchProv := range append(branch.Provenance, branch.Branch) {
		if _, ok := provenanceMap[key(branchProv.Repo.Name, branchProv.Name)]; !ok {
			branchInfo, err := pfsClient.InspectBranch(ctx, &pfs.InspectBranchRequest{
				Branch: client.NewBranch(branchProv.Repo.Name, branchProv.Name),
			})
			if err != nil {
				return nil, err
			}
			if branchInfo.Head == nil {
				continue
			}
			headCommit, err := pfsClient.InspectCommit(ctx, &pfs.InspectCommitRequest{
				Commit: client.NewCommit(branchProv.Repo.Name, branchInfo.Head.ID),
			})
			if err != nil {
				return nil, err
			}
			for _, headProv := range headCommit.Provenance {
				if _, ok := provenanceMap[key(headProv.Branch.Repo.Name, headProv.Branch.Name)]; !ok {
					provenance = append(provenance, headProv)
				}
			}
		}
	}
	// we need to include the spec commit in the provenance, so that the new job is represented by the correct spec commit
	specProvenance := client.NewCommitProvenance(ppsconsts.SpecRepo, request.Pipeline.Name, specCommit.Commit.ID)
	provenance = append(provenance, specProvenance)

	if _, err := pachClient.ExecuteInTransaction(func(txnClient *client.APIClient) error {
		newCommit, err := txnClient.PfsAPIClient.StartCommit(txnClient.Ctx(), &pfs.StartCommitRequest{
			Parent: &pfs.Commit{
				Repo: &pfs.Repo{
					Name: request.Pipeline.Name,
				},
			},
			Provenance: provenance,
		})
		if err != nil {
			return err
		}

		// if stats are enabled, then create a stats commit for the job as well
		if pipelineInfo.EnableStats {
			// it needs to additionally be provenant on the commit we just created
			newCommitProv := client.NewCommitProvenance(newCommit.Repo.Name, "", newCommit.ID)
			_, err = txnClient.PfsAPIClient.StartCommit(txnClient.Ctx(), &pfs.StartCommitRequest{
				Parent: &pfs.Commit{
					Repo: &pfs.Repo{
						Name: request.Pipeline.Name,
					},
				},
				Branch:     "stats",
				Provenance: append(provenance, newCommitProv),
			})
			if err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return &types.Empty{}, nil
}

func (a *apiServer) RunCron(ctx context.Context, request *pps.RunCronRequest) (response *types.Empty, retErr error) {
	func() { a.Log(request, nil, nil, 0) }()
	defer func(start time.Time) { a.Log(request, response, retErr, time.Since(start)) }(time.Now())

	pachClient := a.env.GetPachClient(ctx)

	pipelineInfo, err := a.inspectPipeline(pachClient, request.Pipeline.Name)
	if err != nil {
		return nil, err
	}

	if pipelineInfo.Input == nil {
		return nil, errors.Errorf("pipeline must have a cron input")
	}

	// find any cron inputs
	var crons []*pps.CronInput
	pps.VisitInput(pipelineInfo.Input, func(in *pps.Input) {
		if in.Cron != nil {
			crons = append(crons, in.Cron)
		}
	})

	if len(crons) < 1 {
		return nil, errors.Errorf("pipeline must have a cron input")
	}

	txn, err := pachClient.StartTransaction()
	if err != nil {
		return nil, err
	}

	// We need all the DeleteFile and the PutFile requests to happen atomicly
	txnClient := pachClient.WithTransaction(txn)

	// make a tick on each cron input
	for _, cron := range crons {
		// Use a PutFileClient
		pfc, err := txnClient.NewPutFileClient()
		if err != nil {
			return nil, err
		}
		if cron.Overwrite {
			// get rid of any files, so the new file "overwrites" previous runs
			err = pfc.DeleteFile(cron.Repo, "master", "")
			if err != nil && !isNotFoundErr(err) && !pfsServer.IsNoHeadErr(err) {
				return nil, errors.Wrapf(err, "delete error")
			}
		}

		// Put in an empty file named by the timestamp
		_, err = pfc.PutFile(cron.Repo, "master", time.Now().Format(time.RFC3339), strings.NewReader(""))
		if err != nil {
			return nil, errors.Wrapf(err, "put error")
		}

		err = pfc.Close()
		if err != nil {
			return nil, err
		}
	}

	_, err = txnClient.FinishTransaction(txn)
	if err != nil {
		return nil, err
	}
	return &types.Empty{}, nil
}

// CreateSecret implements the protobuf pps.CreateSecret RPC
func (a *apiServer) CreateSecret(ctx context.Context, request *pps.CreateSecretRequest) (response *types.Empty, retErr error) {
	func() { a.Log(request, nil, nil, 0) }()
	defer func(start time.Time) { a.Log(request, response, retErr, time.Since(start)) }(time.Now())
	metricsFn := metrics.ReportUserAction(ctx, a.reporter, "CreateSecret")
	defer func(start time.Time) { metricsFn(start, retErr) }(time.Now())

	var s v1.Secret
	err := json.Unmarshal(request.GetFile(), &s)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to unmarshal secret")
	}

	labels := s.GetLabels()
	if labels["suite"] != "" && labels["suite"] != "pachyderm" {
		return nil, errors.Errorf("invalid suite label set on secret: suite=%s", labels["suite"])
	}
	if labels == nil {
		labels = map[string]string{}
	}
	labels["suite"] = "pachyderm"
	labels["secret-source"] = "pachyderm-user"
	s.SetLabels(labels)

	if _, err = a.env.GetKubeClient().CoreV1().Secrets(a.namespace).Create(&s); err != nil {
		return nil, errors.Wrapf(err, "failed to create secret")
	}
	return &types.Empty{}, nil
}

// DeleteSecret implements the protobuf pps.DeleteSecret RPC
func (a *apiServer) DeleteSecret(ctx context.Context, request *pps.DeleteSecretRequest) (response *types.Empty, retErr error) {
	func() { a.Log(request, nil, nil, 0) }()
	defer func(start time.Time) { a.Log(request, response, retErr, time.Since(start)) }(time.Now())
	metricsFn := metrics.ReportUserAction(ctx, a.reporter, "DeleteSecret")
	defer func(start time.Time) { metricsFn(start, retErr) }(time.Now())

	if err := a.env.GetKubeClient().CoreV1().Secrets(a.namespace).Delete(request.Secret.Name, &metav1.DeleteOptions{}); err != nil {
		return nil, errors.Wrapf(err, "failed to delete secret")
	}
	return &types.Empty{}, nil
}

// InspectSecret implements the protobuf pps.InspectSecret RPC
func (a *apiServer) InspectSecret(ctx context.Context, request *pps.InspectSecretRequest) (response *pps.SecretInfo, retErr error) {
	func() { a.Log(request, nil, nil, 0) }()
	defer func(start time.Time) { a.Log(request, response, retErr, time.Since(start)) }(time.Now())
	metricsFn := metrics.ReportUserAction(ctx, a.reporter, "InspectSecret")
	defer func(start time.Time) { metricsFn(start, retErr) }(time.Now())

	secret, err := a.env.GetKubeClient().CoreV1().Secrets(a.namespace).Get(request.Secret.Name, metav1.GetOptions{})
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get secret")
	}
	creationTimestamp, err := ptypes.TimestampProto(secret.GetCreationTimestamp().Time)
	if err != nil {
		return nil, errors.Errorf("failed to parse creation timestamp")
	}
	return &pps.SecretInfo{
		Secret: &pps.Secret{
			Name: secret.Name,
		},
		Type: string(secret.Type),
		CreationTimestamp: &types.Timestamp{
			Seconds: creationTimestamp.GetSeconds(),
			Nanos:   creationTimestamp.GetNanos(),
		},
	}, nil
}

// ListSecret implements the protobuf pps.ListSecret RPC
func (a *apiServer) ListSecret(ctx context.Context, in *types.Empty) (response *pps.SecretInfos, retErr error) {
	func() { a.Log(nil, nil, nil, 0) }()
	defer func(start time.Time) { a.Log(nil, response, retErr, time.Since(start)) }(time.Now())
	metricsFn := metrics.ReportUserAction(ctx, a.reporter, "ListSecret")
	defer func(start time.Time) { metricsFn(start, retErr) }(time.Now())

	secrets, err := a.env.GetKubeClient().CoreV1().Secrets(a.namespace).List(metav1.ListOptions{
		LabelSelector: "secret-source=pachyderm-user",
	})
	if err != nil {
		return nil, errors.Wrapf(err, "failed to list secrets")
	}
	secretInfos := []*pps.SecretInfo{}
	for _, s := range secrets.Items {
		creationTimestamp, err := ptypes.TimestampProto(s.GetCreationTimestamp().Time)
		if err != nil {
			return nil, errors.Errorf("failed to parse creation timestamp")
		}
		secretInfos = append(secretInfos, &pps.SecretInfo{
			Secret: &pps.Secret{
				Name: s.Name,
			},
			Type: string(s.Type),
			CreationTimestamp: &types.Timestamp{
				Seconds: creationTimestamp.GetSeconds(),
				Nanos:   creationTimestamp.GetNanos(),
			},
		})
	}

	return &pps.SecretInfos{
		SecretInfo: secretInfos,
	}, nil
}

// DeleteAll implements the protobuf pps.DeleteAll RPC
func (a *apiServer) DeleteAll(ctx context.Context, request *types.Empty) (response *types.Empty, retErr error) {
	func() { a.Log(request, nil, nil, 0) }()
	defer func(start time.Time) { a.Log(request, response, retErr, time.Since(start)) }(time.Now())
	pachClient := a.env.GetPachClient(ctx)
	ctx = pachClient.Ctx() // pachClient will propagate auth info

	// check if the caller is authorized -- they must be an admin
	if me, err := pachClient.WhoAmI(ctx, &auth.WhoAmIRequest{}); err == nil {
		var isAdmin bool
		for _, s := range me.ClusterRoles.Roles {
			if s == auth.ClusterRole_SUPER {
				isAdmin = true
				break
			}
		}

		if !isAdmin {
			return nil, &auth.ErrNotAuthorized{
				Subject: me.Username,
				AdminOp: "DeleteAll",
			}
		}
	} else if !auth.IsErrNotActivated(err) {
		return nil, errors.Wrapf(err, "error during authorization check")
	}

	if _, err := a.DeletePipeline(ctx, &pps.DeletePipelineRequest{All: true, Force: true}); err != nil {
		return nil, err
	}

	if err := a.env.GetKubeClient().CoreV1().Secrets(a.namespace).DeleteCollection(&metav1.DeleteOptions{}, metav1.ListOptions{
		LabelSelector: "secret-source=pachyderm-user",
	}); err != nil {
		return nil, err
	}

	// PFS doesn't delete the spec repo, so do it here
	if err := pachClient.DeleteRepo(ppsconsts.SpecRepo, true); err != nil && !isNotFoundErr(err) {
		return nil, err
	}
	if _, err := pachClient.PfsAPIClient.CreateRepo(
		pachClient.Ctx(),
		&pfs.CreateRepoRequest{
			Repo:        client.NewRepo(ppsconsts.SpecRepo),
			Update:      true,
			Description: ppsconsts.SpecRepoDesc,
		},
	); err != nil {
		return nil, err
	}
	return &types.Empty{}, nil
}

// ActiveStat contains stats about the object objects and tags in the
// filesystem. It is returned by CollectActiveObjectsAndTags.
type ActiveStat struct {
	Objects  *bloom.BloomFilter
	NObjects int
	Tags     *bloom.BloomFilter
	NTags    int
}

// CollectActiveObjectsAndTags collects all objects/tags that are not deleted
// or eligible for garbage collection
func CollectActiveObjectsAndTags(ctx context.Context, pachClient *client.APIClient, repoInfos []*pfs.RepoInfo, pipelineInfos []*pps.PipelineInfo, memoryAllowance int, storageRoot string) (*ActiveStat, error) {
	if memoryAllowance == 0 {
		memoryAllowance = defaultGCMemory
	}
	result := &ActiveStat{
		// Each bloom filter gets half the memory allowance, times 8 to convert
		// from bytes to bits.
		Objects: bloom.New(uint(memoryAllowance*8/2), 10),
		Tags:    bloom.New(uint(memoryAllowance*8/2), 10),
	}
	var activeObjectsMu sync.Mutex
	// A helper function for adding active objects in a thread-safe way
	addActiveObjects := func(objects ...*pfs.Object) {
		activeObjectsMu.Lock()
		defer activeObjectsMu.Unlock()
		for _, object := range objects {
			if object != nil {
				result.NObjects++
				result.Objects.AddString(object.Hash)
			}
		}
	}
	// A helper function for adding objects that are actually hash trees,
	// which in turn contain active objects.
	addActiveTree := func(object *pfs.Object) error {
		if object == nil {
			return nil
		}
		addActiveObjects(object)
		tree, err := hashtree.GetHashTreeObject(pachClient, storageRoot, object)
		if err != nil {
			return err
		}
		return tree.Walk("/", func(path string, node *hashtree.NodeProto) error {
			if node.FileNode != nil {
				addActiveObjects(node.FileNode.Objects...)
			}
			return nil
		})
	}

	// Get all commit trees
	limiter := limit.New(100)
	var eg errgroup.Group
	for _, repo := range repoInfos {
		repo := repo
		client, err := pachClient.ListCommitStream(ctx, &pfs.ListCommitRequest{
			Repo: repo.Repo,
		})
		if err != nil {
			return nil, err
		}
		for {
			ci, err := client.Recv()
			if errors.Is(err, io.EOF) {
				break
			} else if err != nil {
				return nil, grpcutil.ScrubGRPC(err)
			}
			limiter.Acquire()
			eg.Go(func() error {
				defer limiter.Release()
				// (bryce) This needs some notion of active blockrefs since these trees do not use objects
				addActiveObjects(ci.Trees...)
				addActiveObjects(ci.Datums)
				return addActiveTree(ci.Tree)
			})
		}
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}

	eg = errgroup.Group{}
	for _, pipelineInfo := range pipelineInfos {
		tags, err := pachClient.ObjectAPIClient.ListTags(pachClient.Ctx(), &pfs.ListTagsRequest{
			Prefix:        client.DatumTagPrefix(pipelineInfo.Salt),
			IncludeObject: true,
		})
		if err != nil {
			return nil, errors.Wrapf(err, "error listing tagged objects")
		}

		for resp, err := tags.Recv(); !errors.Is(err, io.EOF); resp, err = tags.Recv() {
			resp := resp
			if err != nil {
				return nil, err
			}
			result.Tags.AddString(resp.Tag.Name)
			result.NTags++
			limiter.Acquire()
			eg.Go(func() error {
				defer limiter.Release()
				// (bryce) Same as above
				addActiveObjects(resp.Object)
				return nil
			})
		}
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}

	return result, nil
}

// GarbageCollect implements the protobuf pps.GarbageCollect RPC
func (a *apiServer) GarbageCollect(ctx context.Context, request *pps.GarbageCollectRequest) (response *pps.GarbageCollectResponse, retErr error) {
	func() { a.Log(request, nil, nil, 0) }()
	defer func(start time.Time) { a.Log(request, response, retErr, time.Since(start)) }(time.Now())
	pachClient := a.env.GetPachClient(ctx)
	ctx, err := checkLoggedIn(pachClient)
	if err != nil {
		return nil, err
	}
	pipelineInfos, err := a.ListPipeline(ctx, &pps.ListPipelineRequest{})
	if err != nil {
		return nil, err
	}

	for _, pi := range pipelineInfos.PipelineInfo {
		if pi.State != pps.PipelineState_PIPELINE_PAUSED && pi.State != pps.PipelineState_PIPELINE_FAILURE {
			return nil, errors.Errorf("all pipelines must be stopped to run garbage collection, pipeline: %s is not", pi.Pipeline.Name)
		}
		selector := fmt.Sprintf("pipelineName=%s", pi.Pipeline.Name)
		pods, err := a.env.GetKubeClient().CoreV1().Pods(a.namespace).List(metav1.ListOptions{LabelSelector: selector})
		if err != nil {
			return nil, err
		}
		if len(pods.Items) != 0 {
			return nil, errors.Errorf("pipeline %s is paused, but still has running workers, this should resolve itself, if it doesn't you can manually delete them with kubectl delete", pi.Pipeline.Name)
		}
	}
	ctx = pachClient.Ctx() // pachClient will propagate auth info
	pfsClient := pachClient.PfsAPIClient
	objClient := pachClient.ObjectAPIClient

	// Get all repos
	repoInfos, err := pfsClient.ListRepo(ctx, &pfs.ListRepoRequest{})
	if err != nil {
		return nil, err
	}
	specRepoInfo, err := pachClient.InspectRepo(ppsconsts.SpecRepo)
	if err != nil {
		return nil, err
	}
	activeStat, err := CollectActiveObjectsAndTags(ctx, pachClient, append(repoInfos.RepoInfo, specRepoInfo), pipelineInfos.PipelineInfo, int(request.MemoryBytes), a.storageRoot)
	if err != nil {
		return nil, err
	}

	// Iterate through all objects.  If they are not active, delete them.
	objects, err := objClient.ListObjects(ctx, &pfs.ListObjectsRequest{})
	if err != nil {
		return nil, err
	}

	var objectsToDelete []*pfs.Object
	deleteObjectsIfMoreThan := func(n int) error {
		if len(objectsToDelete) > n {
			if _, err := objClient.DeleteObjects(ctx, &pfs.DeleteObjectsRequest{
				Objects: objectsToDelete,
			}); err != nil {
				return errors.Wrapf(err, "error deleting objects")
			}
			objectsToDelete = []*pfs.Object{}
		}
		return nil
	}
	for oi, err := objects.Recv(); !errors.Is(err, io.EOF); oi, err = objects.Recv() {
		if err != nil {
			return nil, errors.Wrapf(err, "error receiving objects from ListObjects")
		}
		if !activeStat.Objects.TestString(oi.Object.Hash) {
			objectsToDelete = append(objectsToDelete, oi.Object)
		}
		// Delete objects in batches
		if err := deleteObjectsIfMoreThan(100); err != nil {
			return nil, err
		}
	}
	if err := deleteObjectsIfMoreThan(0); err != nil {
		return nil, err
	}

	// Iterate through all tags.  If they are not active, delete them
	tags, err := objClient.ListTags(ctx, &pfs.ListTagsRequest{})
	if err != nil {
		return nil, err
	}
	var tagsToDelete []*pfs.Tag
	deleteTagsIfMoreThan := func(n int) error {
		if len(tagsToDelete) > n {
			if _, err := objClient.DeleteTags(ctx, &pfs.DeleteTagsRequest{
				Tags: tagsToDelete,
			}); err != nil {
				return errors.Wrapf(err, "error deleting tags")
			}
			tagsToDelete = []*pfs.Tag{}
		}
		return nil
	}
	for resp, err := tags.Recv(); !errors.Is(err, io.EOF); resp, err = tags.Recv() {
		if err != nil {
			return nil, errors.Wrapf(err, "error receiving tags from ListTags")
		}
		if !activeStat.Tags.TestString(resp.Tag.Name) {
			tagsToDelete = append(tagsToDelete, resp.Tag)
		}
		if err := deleteTagsIfMoreThan(100); err != nil {
			return nil, err
		}
	}
	if err := deleteTagsIfMoreThan(0); err != nil {
		return nil, err
	}

	if err := a.incrementGCGeneration(ctx); err != nil {
		return nil, err
	}

	return &pps.GarbageCollectResponse{}, nil
}

// ActivateAuth implements the protobuf pps.ActivateAuth RPC
func (a *apiServer) ActivateAuth(ctx context.Context, req *pps.ActivateAuthRequest) (resp *pps.ActivateAuthResponse, retErr error) {
	func() { a.Log(req, nil, nil, 0) }()
	defer func(start time.Time) { a.Log(req, resp, retErr, time.Since(start)) }(time.Now())
	pachClient := a.env.GetPachClient(ctx)
	ctx, err := checkLoggedIn(pachClient)
	if err != nil {
		return nil, err
	}

	// Unauthenticated users can't create new pipelines or repos, and users can't
	// log in while auth is in an intermediate state, so 'pipelines' is exhaustive
	var pipelines []*pps.PipelineInfo
	if err := a.sudo(pachClient, func(superUserClient *client.APIClient) error {
		var err error
		pipelines, err = superUserClient.ListPipeline()
		if err != nil {
			return errors.Wrapf(grpcutil.ScrubGRPC(err), "cannot get list of pipelines to update")
		}
		return nil
	}); err != nil {
		return nil, err
	}

	var eg errgroup.Group
	for _, pipeline := range pipelines {
		pipeline := pipeline
		pipelineName := pipeline.Pipeline.Name
		// 1) Create a new auth token for 'pipeline' and attach it, so that the
		// pipeline can authenticate as itself when it needs to read input data
		eg.Go(func() error {
			return a.sudo(pachClient, func(superUserClient *client.APIClient) error {
				tokenResp, err := superUserClient.GetAuthToken(superUserClient.Ctx(), &auth.GetAuthTokenRequest{
					Subject: auth.PipelinePrefix + pipelineName,
				})
				if err != nil {
					return errors.Wrapf(grpcutil.ScrubGRPC(err), "could not generate pipeline auth token")
				}
				_, err = col.NewSTM(ctx, a.env.GetEtcdClient(), func(stm col.STM) error {
					var pipelinePtr pps.EtcdPipelineInfo
					if err := a.pipelines.ReadWrite(stm).Update(pipelineName, &pipelinePtr, func() error {
						pipelinePtr.AuthToken = tokenResp.Token
						return nil
					}); err != nil {
						return errors.Wrapf(err, "could not update \"%s\" with new auth token", pipelineName)
					}
					return nil
				})
				return err
			})
		})
		// put 'pipeline' on relevant ACLs
		if err := a.fixPipelineInputRepoACLs(pachClient, pipeline, nil); err != nil {
			return nil, err
		}
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	return &pps.ActivateAuthResponse{}, nil
}

// incrementGCGeneration increments the GC generation number in etcd
func (a *apiServer) incrementGCGeneration(ctx context.Context) error {
	resp, err := a.env.GetEtcdClient().Get(ctx, client.GCGenerationKey)
	if err != nil {
		return err
	}

	if resp.Count == 0 {
		// If the generation number does not exist, create it.
		// It's important that the new generation is 1, as the first
		// generation is assumed to be 0.
		if _, err := a.env.GetEtcdClient().Put(ctx, client.GCGenerationKey, "1"); err != nil {
			return err
		}
	} else {
		oldGen, err := strconv.Atoi(string(resp.Kvs[0].Value))
		if err != nil {
			return err
		}
		newGen := oldGen + 1
		if _, err := a.env.GetEtcdClient().Put(ctx, client.GCGenerationKey, strconv.Itoa(newGen)); err != nil {
			return err
		}
	}
	return nil
}

func isAlreadyExistsErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), "already exists")
}

func isNotFoundErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), "not found")
}

func (a *apiServer) updatePipelineSpecCommit(pachClient *client.APIClient, pipelineName string, commit *pfs.Commit) error {
	_, err := col.NewSTM(pachClient.Ctx(), a.env.GetEtcdClient(), func(stm col.STM) error {
		pipelines := a.pipelines.ReadWrite(stm)
		pipelinePtr := &pps.EtcdPipelineInfo{}
		if err := pipelines.Get(pipelineName, pipelinePtr); err != nil {
			return err
		}
		pipelinePtr.SpecCommit = commit
		return pipelines.Put(pipelineName, pipelinePtr)
	})
	if isNotFoundErr(err) {
		return newErrPipelineNotFound(pipelineName)
	}
	return err
}

// RepoNameToEnvString is a helper which uppercases a repo name for
// use in environment variable names.
func RepoNameToEnvString(repoName string) string {
	return strings.ToUpper(repoName)
}

func (a *apiServer) rcPods(rcName string) ([]v1.Pod, error) {
	podList, err := a.env.GetKubeClient().CoreV1().Pods(a.namespace).List(metav1.ListOptions{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ListOptions",
			APIVersion: "v1",
		},
		LabelSelector: metav1.FormatLabelSelector(metav1.SetAsLabelSelector(map[string]string{"app": rcName})),
	})
	if err != nil {
		return nil, err
	}
	return podList.Items, nil
}

func (a *apiServer) resolveCommit(pachClient *client.APIClient, commit *pfs.Commit) (*pfs.Commit, error) {
	ci, err := pachClient.InspectCommit(commit.Repo.Name, commit.ID)
	if err != nil {
		return nil, err
	}
	return ci.Commit, nil
}

func labels(app string) map[string]string {
	return map[string]string{
		"app":       app,
		"suite":     suite,
		"component": "worker",
	}
}

type podSlice []v1.Pod

func (s podSlice) Len() int {
	return len(s)
}
func (s podSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s podSlice) Less(i, j int) bool {
	return s[i].ObjectMeta.Name < s[j].ObjectMeta.Name
}

func now() *types.Timestamp {
	t, err := types.TimestampProto(time.Now())
	if err != nil {
		panic(err)
	}
	return t
}

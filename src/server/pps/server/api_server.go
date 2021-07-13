package server

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"path"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	"github.com/golang/protobuf/ptypes"
	"github.com/itchyny/gojq"
	"github.com/jmoiron/sqlx"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/robfig/cron"
	logrus "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"golang.org/x/sync/errgroup"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kube "k8s.io/client-go/kubernetes"

	"github.com/pachyderm/pachyderm/v2/src/auth"
	"github.com/pachyderm/pachyderm/v2/src/client"
	enterpriseclient "github.com/pachyderm/pachyderm/v2/src/enterprise"
	"github.com/pachyderm/pachyderm/v2/src/internal/ancestry"
	col "github.com/pachyderm/pachyderm/v2/src/internal/collection"
	"github.com/pachyderm/pachyderm/v2/src/internal/errors"
	"github.com/pachyderm/pachyderm/v2/src/internal/errutil"
	"github.com/pachyderm/pachyderm/v2/src/internal/grpcutil"
	"github.com/pachyderm/pachyderm/v2/src/internal/log"
	"github.com/pachyderm/pachyderm/v2/src/internal/lokiutil"
	"github.com/pachyderm/pachyderm/v2/src/internal/metrics"
	"github.com/pachyderm/pachyderm/v2/src/internal/ppsconsts"
	"github.com/pachyderm/pachyderm/v2/src/internal/ppsdb"
	"github.com/pachyderm/pachyderm/v2/src/internal/ppsutil"
	"github.com/pachyderm/pachyderm/v2/src/internal/serde"
	"github.com/pachyderm/pachyderm/v2/src/internal/serviceenv"
	"github.com/pachyderm/pachyderm/v2/src/internal/tracing"
	"github.com/pachyderm/pachyderm/v2/src/internal/tracing/extended"
	txnenv "github.com/pachyderm/pachyderm/v2/src/internal/transactionenv"
	"github.com/pachyderm/pachyderm/v2/src/internal/transactionenv/txncontext"
	"github.com/pachyderm/pachyderm/v2/src/internal/uuid"
	"github.com/pachyderm/pachyderm/v2/src/internal/watch"
	"github.com/pachyderm/pachyderm/v2/src/internal/work"
	"github.com/pachyderm/pachyderm/v2/src/pfs"
	"github.com/pachyderm/pachyderm/v2/src/pps"
	enterpriselimits "github.com/pachyderm/pachyderm/v2/src/server/enterprise/limits"
	enterprisemetrics "github.com/pachyderm/pachyderm/v2/src/server/enterprise/metrics"
	enterprisetext "github.com/pachyderm/pachyderm/v2/src/server/enterprise/text"
	pfsServer "github.com/pachyderm/pachyderm/v2/src/server/pfs"
	ppsServer "github.com/pachyderm/pachyderm/v2/src/server/pps"
	"github.com/pachyderm/pachyderm/v2/src/server/worker/common"
	"github.com/pachyderm/pachyderm/v2/src/server/worker/datum"
	"github.com/pachyderm/pachyderm/v2/src/server/worker/driver"
	workerserver "github.com/pachyderm/pachyderm/v2/src/server/worker/server"
)

const (
	// DefaultUserImage is the image used for jobs when the user does not specify
	// an image.
	DefaultUserImage = "ubuntu:20.04"
	// DefaultDatumTries is the default number of times a datum will be tried
	// before we give up and consider the job failed.
	DefaultDatumTries = 3

	// DefaultLogsFrom is the default duration to return logs from, i.e. by
	// default we return logs from up to 24 hours ago.
	DefaultLogsFrom = time.Hour * 24
)

var (
	suite = "pachyderm"
)

func newErrPipelineExists(pipeline string) error {
	return errors.Errorf("pipeline %v already exists", pipeline)
}

// apiServer implements the public interface of the Pachyderm Pipeline System,
// including all RPCs defined in the protobuf spec.
type apiServer struct {
	log.Logger
	etcdPrefix            string
	env                   serviceenv.ServiceEnv
	txnEnv                *txnenv.TransactionEnv
	namespace             string
	workerImage           string
	workerSidecarImage    string
	workerImagePullPolicy string
	storageRoot           string
	storageBackend        string
	storageHostPath       string
	imagePullSecret       string
	reporter              *metrics.Reporter
	workerUsesRoot        bool
	workerGrpcPort        uint16
	port                  uint16
	peerPort              uint16
	gcPercent             int
	// collections
	pipelines col.PostgresCollection
	jobs      col.PostgresCollection
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
	case input.Group != nil:
		for _, input := range input.Group {
			if err := validateNames(names, input); err != nil {
				return err
			}
		}
	}
	return nil
}

func (a *apiServer) validateInput(pipelineName string, input *pps.Input) error {
	if err := validateNames(make(map[string]bool), input); err != nil {
		return err
	}
	return pps.VisitInput(input, func(input *pps.Input) error {
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
			case input.Pfs.Branch == "":
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
		if input.Group != nil {
			if set {
				return errors.Errorf("multiple input types set")
			}
			set = true
			if ppsutil.ContainsS3Inputs(input) {
				// See above for "joins"; block s3 inputs in group expressions until
				// we know how they should work
				return errors.Errorf("S3 inputs in group expressions are not supported")
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
		if !set {
			return errors.Errorf("no input set")
		}
		return nil
	})
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
	_, err := kubeClient.CoreV1().Pods(a.namespace).Watch(metav1.ListOptions{Watch: true})
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
func (a *apiServer) authorizePipelineOp(ctx context.Context, operation pipelineOperation, input *pps.Input, output string) error {
	return a.txnEnv.WithReadContext(ctx, func(txnCtx *txncontext.TransactionContext) error {
		return a.authorizePipelineOpInTransaction(txnCtx, operation, input, output)
	})
}

// authorizePipelineOpInTransaction is identical to authorizePipelineOp, but runs in the provided transaction
func (a *apiServer) authorizePipelineOpInTransaction(txnCtx *txncontext.TransactionContext, operation pipelineOperation, input *pps.Input, output string) error {
	_, err := a.env.AuthServer().WhoAmI(txnCtx.ClientContext, &auth.WhoAmIRequest{})
	if auth.IsErrNotActivated(err) {
		return nil // Auth isn't activated, skip authorization completely
	} else if err != nil {
		return err
	}

	if input != nil && operation != pipelineOpDelete {
		// Check that the user is authorized to read all input repos, and write to the
		// output repo (which the pipeline needs to be able to do on the user's
		// behalf)
		done := make(map[string]struct{}) // don't double-authorize repos
		if err := pps.VisitInput(input, func(in *pps.Input) error {
			var repo string
			if in.Pfs != nil {
				repo = in.Pfs.Repo
			} else {
				return nil
			}

			if _, ok := done[repo]; ok {
				return nil
			}
			done[repo] = struct{}{}
			return a.env.AuthServer().CheckRepoIsAuthorizedInTransaction(txnCtx, &pfs.Repo{Type: pfs.UserRepoType, Name: repo}, auth.Permission_REPO_READ)
		}); err != nil {
			return err
		}
	}

	// Check that the user is authorized to write to the output repo.
	// Note: authorizePipelineOp is called before CreateRepo creates a
	// PipelineInfo proto in etcd, so PipelineManager won't have created an output
	// repo yet, and it's possible to check that the output repo doesn't exist
	// (if it did exist, we'd have to check that the user has permission to write
	// to it, and this is simpler)
	if output != "" {
		var required auth.Permission
		switch operation {
		case pipelineOpCreate:
			if _, err := a.env.PfsServer().InspectRepoInTransaction(txnCtx, &pfs.InspectRepoRequest{
				Repo: client.NewRepo(output),
			}); err == nil {
				// the repo already exists, so we need the same permissions as update
				required = auth.Permission_REPO_WRITE
			} else if errutil.IsNotFoundError(err) {
				return nil
			} else {
				return err
			}
		case pipelineOpListDatum, pipelineOpGetLogs:
			required = auth.Permission_REPO_READ
		case pipelineOpUpdate:
			required = auth.Permission_REPO_WRITE
		case pipelineOpDelete:
			if _, err := a.env.PfsServer().InspectRepoInTransaction(txnCtx, &pfs.InspectRepoRequest{
				Repo: client.NewRepo(output),
			}); errutil.IsNotFoundError(err) {
				// special case: the pipeline output repo has been deleted (so the
				// pipeline is now invalid). It should be possible to delete the pipeline.
				return nil
			}
			required = auth.Permission_REPO_DELETE
		default:
			return errors.Errorf("internal error, unrecognized operation %v", operation)
		}
		if err := a.env.AuthServer().CheckRepoIsAuthorizedInTransaction(txnCtx, &pfs.Repo{Type: pfs.UserRepoType, Name: output}, required); err != nil {
			return err
		}
	}
	return nil
}

func (a *apiServer) UpdateJobState(ctx context.Context, request *pps.UpdateJobStateRequest) (response *types.Empty, retErr error) {
	func() { a.Log(request, nil, nil, 0) }()
	defer func(start time.Time) { a.Log(request, response, retErr, time.Since(start)) }(time.Now())

	if err := a.txnEnv.WithTransaction(ctx, func(txn txnenv.Transaction) error {
		return txn.UpdateJobState(request)
	}, nil); err != nil {
		return nil, err
	}

	return &types.Empty{}, nil
}

func (a *apiServer) UpdateJobStateInTransaction(txnCtx *txncontext.TransactionContext, request *pps.UpdateJobStateRequest) error {
	jobs := a.jobs.ReadWrite(txnCtx.SqlTx)
	jobInfo := &pps.JobInfo{}
	if err := jobs.Get(ppsdb.JobKey(request.Job), jobInfo); err != nil {
		return err
	}
	if ppsutil.IsTerminal(jobInfo.State) {
		return ppsServer.ErrJobFinished{Job: jobInfo.Job}
	}

	jobInfo.Restart = request.Restart
	jobInfo.DataProcessed = request.DataProcessed
	jobInfo.DataSkipped = request.DataSkipped
	jobInfo.DataFailed = request.DataFailed
	jobInfo.DataRecovered = request.DataRecovered
	jobInfo.DataTotal = request.DataTotal
	jobInfo.Stats = request.Stats

	return ppsutil.UpdateJobState(a.pipelines.ReadWrite(txnCtx.SqlTx), jobs, jobInfo, request.State, request.Reason)
}

// InspectJob implements the protobuf pps.InspectJob RPC
func (a *apiServer) InspectJob(ctx context.Context, request *pps.InspectJobRequest) (response *pps.JobInfo, retErr error) {
	func() { a.Log(request, nil, nil, 0) }()
	defer func(start time.Time) { a.Log(request, response, retErr, time.Since(start)) }(time.Now())
	if request.Job == nil {
		return nil, errors.Errorf("must specify a job")
	}
	jobs := a.jobs.ReadOnly(ctx)
	// Make sure the job exists
	// TODO: there's a race condition between this check and the watch below where
	// a deleted job could make this block forever.
	jobInfo := &pps.JobInfo{}
	if err := jobs.Get(ppsdb.JobKey(request.Job), jobInfo); err != nil {
		return nil, err
	}
	if request.Wait {
		watcher, err := jobs.WatchOne(ppsdb.JobKey(request.Job))
		if err != nil {
			return nil, err
		}
		defer watcher.Close()

	watchLoop:
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
				if err := ev.Unmarshal(&jobID, jobInfo); err != nil {
					return nil, err
				}
				if ppsutil.IsTerminal(jobInfo.State) {
					break watchLoop
				}
			}
		}
	}

	if request.Details {
		if err := a.getJobDetails(ctx, jobInfo); err != nil {
			return nil, err
		}
	}
	return jobInfo, nil
}

// InspectJobSet implements the protobuf pps.InspectJobSet RPC
func (a *apiServer) InspectJobSet(request *pps.InspectJobSetRequest, server pps.API_InspectJobSetServer) (retErr error) {
	func() { a.Log(request, nil, nil, 0) }()
	defer func(start time.Time) { a.Log(request, nil, retErr, time.Since(start)) }(time.Now())
	pachClient := a.env.GetPachClient(server.Context())

	cb := func(ci *pfs.CommitInfo) error {
		if ci.Commit.Branch.Repo.Type != pfs.UserRepoType || ci.Origin.Kind == pfs.OriginKind_ALIAS {
			return nil
		}
		jobInfo, err := pachClient.InspectJob(ci.Commit.Branch.Repo.Name, ci.Commit.ID, request.Details)
		if err != nil {
			// Not all commits are guaranteed to have an associated job - skip over it
			if errutil.IsNotFoundError(err) {
				return nil
			}
			return err
		}
		return server.Send(jobInfo)
	}

	// Note that while this will return jobs in the same topological sort as the
	// commitset, it will block on commits that don't have a job associated with
	// them (aliases and input commits, for example).
	if request.Wait {
		return pachClient.WaitCommitSet(request.JobSet.ID, cb)
	}
	commitInfos, err := pachClient.InspectCommitSet(request.JobSet.ID)
	if err != nil {
		return err
	}
	for _, ci := range commitInfos {
		if err := cb(ci); err != nil {
			return err
		}
	}
	return nil
}

// ListJobSet implements the protobuf pps.ListJobSet RPC
func (a *apiServer) ListJobSet(request *pps.ListJobSetRequest, serv pps.API_ListJobSetServer) (retErr error) {
	func() { a.Log(request, nil, nil, 0) }()
	sent := 0
	defer func(start time.Time) {
		a.Log(request, fmt.Sprintf("stream containing %d JobSetInfos", sent), retErr, time.Since(start))
	}(time.Now())

	pachClient := a.env.GetPachClient(serv.Context())

	// Track the jobsets we've already processed
	seen := map[string]struct{}{}

	// Return jobsets by the newest job in each set (which can be at a different
	// timestamp due to triggers or deferred processing)
	jobInfo := &pps.JobInfo{}
	return a.jobs.ReadOnly(serv.Context()).List(jobInfo, col.DefaultOptions(), func(string) error {
		if _, ok := seen[jobInfo.Job.ID]; ok {
			return nil
		}
		seen[jobInfo.Job.ID] = struct{}{}

		jobInfos, err := pachClient.InspectJobSet(jobInfo.Job.ID, request.Details)
		if err != nil {
			return err
		}

		sent++
		return serv.Send(&pps.JobSetInfo{
			JobSet: client.NewJobSet(jobInfo.Job.ID),
			Jobs:   jobInfos,
		})
	})
}

// intersectCommitSets finds all commitsets which involve the specified commits
// (or aliases of the specified commits)
// TODO(global ids): this assumes that all aliases are equivalent to their first
// ancestor non-alias commit, but that may not be true if the ancestor has been
// squashed.  We may need to recursively squash commitsets to prevent this.
func (a *apiServer) intersectCommitSets(ctx context.Context, commits []*pfs.Commit) (map[string]struct{}, error) {
	walkCommits := func(startCommit *pfs.Commit) (map[string]struct{}, error) {
		result := map[string]struct{}{} // key is the commitset id
		queue := []*pfs.Commit{}

		// Walk upwards until finding a concrete commit
		cursor := startCommit
		for {
			commitInfo, err := a.resolveCommit(ctx, cursor)
			if err != nil {
				return nil, err
			}
			if commitInfo.Origin.Kind != pfs.OriginKind_ALIAS || commitInfo.ParentCommit == nil {
				result[cursor.ID] = struct{}{}
				queue = append(queue, commitInfo.ChildCommits...)
				break
			}
			cursor = commitInfo.ParentCommit
		}

		// Now find all descendent aliases
		for len(queue) > 0 {
			cursor = queue[0]
			queue = queue[1:]

			commitInfo, err := a.resolveCommit(ctx, cursor)
			if err != nil {
				return nil, err
			}
			if commitInfo.Origin.Kind == pfs.OriginKind_ALIAS {
				result[cursor.ID] = struct{}{}
				queue = append(queue, commitInfo.ChildCommits...)
			}
		}
		return result, nil
	}

	var intersection map[string]struct{}
	for _, commit := range commits {
		result, err := walkCommits(commit)
		if err != nil {
			return nil, err
		}
		if intersection == nil {
			intersection = result
		} else {
			newIntersection := map[string]struct{}{}
			for commitsetID := range result {
				if _, ok := intersection[commitsetID]; ok {
					newIntersection[commitsetID] = struct{}{}
				}
			}
			intersection = newIntersection
		}
	}
	return intersection, nil
}

// listJob is the internal implementation of ListJob shared between ListJob and
// ListJobStream. When ListJob is removed, this should be inlined into
// ListJobStream.
func (a *apiServer) listJob(
	ctx context.Context,
	pipeline *pps.Pipeline,
	inputCommits []*pfs.Commit,
	history int64,
	details bool,
	jqFilter string,
	f func(*pps.JobInfo) error,
) error {
	if pipeline != nil {
		// If 'pipeline is set, check that caller has access to the pipeline's
		// output repo; currently, that's all that's required for ListJob.
		//
		// If 'pipeline' isn't set, then we don't return an error (otherwise, a
		// caller without access to a single pipeline's output repo couldn't run
		// `pachctl list job` at all) and instead silently skip jobs where the user
		// doesn't have access to the job's output repo.
		if err := a.env.AuthServer().CheckRepoIsAuthorized(ctx, &pfs.Repo{Type: pfs.UserRepoType, Name: pipeline.Name}, auth.Permission_PIPELINE_LIST_JOB); err != nil && !auth.IsErrNotActivated(err) {
			return err
		}
	}

	// For each specified input commit, build the set of commitset IDs which
	// belong to all of them.
	commitsets, err := a.intersectCommitSets(ctx, inputCommits)
	if err != nil {
		return err
	}

	var jqCode *gojq.Code
	var enc serde.Encoder
	var jsonBuffer bytes.Buffer
	if jqFilter != "" {
		jqQuery, err := gojq.Parse(jqFilter)
		if err != nil {
			return err
		}
		jqCode, err = gojq.Compile(jqQuery)
		if err != nil {
			return err
		}
		// ensure field names and enum values match with --raw output
		enc = serde.NewJSONEncoder(&jsonBuffer, serde.WithOrigName(true))
	}

	// pipelineVersions holds the versions of pipelines that we're interested in
	versionKey := func(name string, version uint64) string {
		return fmt.Sprintf("%s-%v", name, version)
	}
	pipelineVersions := make(map[string]bool)
	if err := a.listPipelineInfo(ctx, pipeline, history,
		func(ptr *pps.PipelineInfo) error {
			pipelineVersions[versionKey(ptr.Pipeline.Name, ptr.Version)] = true
			return nil
		}); err != nil {
		return err
	}

	jobs := a.jobs.ReadOnly(ctx)
	jobInfo := &pps.JobInfo{}
	_f := func(string) error {
		if details {
			if err := a.getJobDetails(ctx, jobInfo); err != nil {
				if auth.IsErrNotAuthorized(err) {
					return nil // skip job--see note at top of function
				}
				return err
			}
		}

		if len(inputCommits) > 0 {
			// Only include the job if it's in the set of intersected commitset IDs
			if _, ok := commitsets[jobInfo.Job.ID]; !ok {
				return nil
			}
		}

		if !pipelineVersions[versionKey(jobInfo.Job.Pipeline.Name, jobInfo.PipelineVersion)] {
			return nil
		}

		if jqCode != nil {
			jsonBuffer.Reset()
			// convert jobInfo to a map[string]interface{} for use with gojq
			enc.EncodeProto(jobInfo)
			var jobInterface interface{}
			json.Unmarshal(jsonBuffer.Bytes(), &jobInterface)
			iter := jqCode.Run(jobInterface)
			// treat either jq false-y value as rejection
			if v, _ := iter.Next(); v == false || v == nil {
				return nil
			}
		}

		return f(jobInfo)
	}
	if pipeline != nil {
		return jobs.GetByIndex(ppsdb.JobsPipelineIndex, pipeline.Name, jobInfo, col.DefaultOptions(), _f)
	} else {
		return jobs.List(jobInfo, col.DefaultOptions(), _f)
	}
}

func (a *apiServer) getJobDetails(ctx context.Context, jobInfo *pps.JobInfo) error {
	if err := a.env.AuthServer().CheckRepoIsAuthorized(ctx, &pfs.Repo{Type: pfs.UserRepoType, Name: jobInfo.Job.Pipeline.Name}, auth.Permission_PIPELINE_LIST_JOB); err != nil && !auth.IsErrNotActivated(err) {
		return err
	}

	pipelineInfo := &pps.PipelineInfo{}
	if err := a.pipelines.ReadOnly(ctx).Get(jobInfo.Job.Pipeline.Name, pipelineInfo); err != nil {
		return err
	}

	// Override the SpecCommit for the pipeline to be what it was when this job
	// was created, this prevents races between updating a pipeline and
	// previous jobs running.
	specCommit := client.NewSystemRepo(jobInfo.Job.Pipeline.Name, pfs.SpecRepoType).NewCommit("master", jobInfo.OutputCommit.ID)
	pipelineInfo.SpecCommit = specCommit

	pachClient := a.env.GetPachClient(ctx)
	// If the commits for the job have been squashed, the pipeline spec can no
	// longer be read for this job
	if err := ppsutil.GetPipelineDetails(pachClient, pipelineInfo); err != nil {
		return err
	}

	details := &pps.JobInfo_Details{}
	details.Transform = pipelineInfo.Details.Transform
	details.ParallelismSpec = pipelineInfo.Details.ParallelismSpec
	details.Egress = pipelineInfo.Details.Egress
	details.Service = pipelineInfo.Details.Service
	details.Spout = pipelineInfo.Details.Spout
	details.ResourceRequests = pipelineInfo.Details.ResourceRequests
	details.ResourceLimits = pipelineInfo.Details.ResourceLimits
	details.SidecarResourceLimits = pipelineInfo.Details.SidecarResourceLimits
	details.Input = ppsutil.JobInput(pipelineInfo, jobInfo.OutputCommit)
	details.Salt = pipelineInfo.Details.Salt
	details.DatumSetSpec = pipelineInfo.Details.DatumSetSpec
	details.DatumTimeout = pipelineInfo.Details.DatumTimeout
	details.JobTimeout = pipelineInfo.Details.JobTimeout
	details.DatumTries = pipelineInfo.Details.DatumTries
	details.SchedulingSpec = pipelineInfo.Details.SchedulingSpec
	details.PodSpec = pipelineInfo.Details.PodSpec
	details.PodPatch = pipelineInfo.Details.PodPatch

	// If the job is running, we fill in WorkerStatus field, otherwise
	// we just return the jobInfo.
	if jobInfo.State == pps.JobState_JOB_RUNNING {
		workerPoolID := ppsutil.PipelineRcName(jobInfo.Job.Pipeline.Name, jobInfo.PipelineVersion)
		workerStatus, err := workerserver.Status(ctx, workerPoolID, a.env.GetEtcdClient(), a.etcdPrefix, a.workerGrpcPort)
		if err != nil {
			logrus.Errorf("failed to get worker status with err: %s", err.Error())
		} else {
			// It's possible that the workers might be working on datums for other
			// jobs, we omit those since they're not part of the status for this
			// job.
			for _, status := range workerStatus {
				if status.JobID == jobInfo.Job.ID {
					details.WorkerStatus = append(details.WorkerStatus, status)
				}
			}
		}
	}

	jobInfo.Details = details
	return nil
}

// ListJob implements the protobuf pps.ListJob RPC
func (a *apiServer) ListJob(request *pps.ListJobRequest, resp pps.API_ListJobServer) (retErr error) {
	func() { a.Log(request, nil, nil, 0) }()
	sent := 0
	defer func(start time.Time) {
		a.Log(request, fmt.Sprintf("stream containing %d JobInfos", sent), retErr, time.Since(start))
	}(time.Now())
	return a.listJob(resp.Context(), request.Pipeline, request.InputCommit, request.History, request.Details, request.JqFilter, func(ji *pps.JobInfo) error {
		if err := resp.Send(ji); err != nil {
			return err
		}
		sent++
		return nil
	})
}

// SubscribeJob implements the protobuf pps.SubscribeJob RPC
func (a *apiServer) SubscribeJob(request *pps.SubscribeJobRequest, stream pps.API_SubscribeJobServer) (retErr error) {
	func() { a.Log(request, nil, nil, 0) }()
	defer func(start time.Time) { a.Log(request, nil, retErr, time.Since(start)) }(time.Now())
	ctx := stream.Context()

	// Validate arguments
	if request.Pipeline == nil || request.Pipeline.Name == "" {
		return errors.New("pipeline must be specified")
	}

	if err := a.env.AuthServer().CheckRepoIsAuthorized(ctx, &pfs.Repo{Type: pfs.UserRepoType, Name: request.Pipeline.Name}, auth.Permission_PIPELINE_LIST_JOB); err != nil && !auth.IsErrNotActivated(err) {
		return err
	}

	// keep track of the jobs that have been sent
	seen := map[string]struct{}{}

	return a.jobs.ReadOnly(ctx).WatchByIndexF(ppsdb.JobsTerminalIndex, ppsdb.JobTerminalKey(request.Pipeline, false), func(ev *watch.Event) error {
		var key string
		jobInfo := &pps.JobInfo{}
		if err := ev.Unmarshal(&key, jobInfo); err != nil {
			return errors.Wrapf(err, "unmarshal")
		}

		if _, ok := seen[key]; ok {
			return nil
		}
		seen[key] = struct{}{}

		if request.Details {
			if err := a.getJobDetails(ctx, jobInfo); err != nil {
				return err
			}
		}
		return stream.Send(jobInfo)
	}, watch.WithSort(col.SortByCreateRevision, col.SortAscend), watch.IgnoreDelete)
}

// DeleteJob implements the protobuf pps.DeleteJob RPC
func (a *apiServer) DeleteJob(ctx context.Context, request *pps.DeleteJobRequest) (response *types.Empty, retErr error) {
	func() { a.Log(request, nil, nil, 0) }()
	defer func(start time.Time) { a.Log(request, response, retErr, time.Since(start)) }(time.Now())
	if request.Job == nil {
		return nil, errors.New("job cannot be nil")
	}
	if err := a.txnEnv.WithWriteContext(ctx, func(txnCtx *txncontext.TransactionContext) error {
		if err := a.stopJob(txnCtx, request.Job, "job deleted"); err != nil {
			return err
		}
		return a.jobs.ReadWrite(txnCtx.SqlTx).Delete(ppsdb.JobKey(request.Job))
	}); err != nil {
		return nil, err
	}
	return &types.Empty{}, nil
}

// StopJob implements the protobuf pps.StopJob RPC
func (a *apiServer) StopJob(ctx context.Context, request *pps.StopJobRequest) (response *types.Empty, retErr error) {
	func() { a.Log(request, nil, nil, 0) }()
	defer func(start time.Time) { a.Log(request, response, retErr, time.Since(start)) }(time.Now())
	if err := a.txnEnv.WithTransaction(ctx, func(txn txnenv.Transaction) error {
		return txn.StopJob(request)
	}, nil); err != nil {
		return nil, err
	}
	return &types.Empty{}, nil
}

// StopJobInTransaction is identical to StopJob except that it can run inside an
// existing postgres transaction.  This is not an RPC.
func (a *apiServer) StopJobInTransaction(txnCtx *txncontext.TransactionContext, request *pps.StopJobRequest) error {
	reason := request.Reason
	if reason == "" {
		reason = "job stopped"
	}
	return a.stopJob(txnCtx, request.Job, reason)
}

func (a *apiServer) stopJob(txnCtx *txncontext.TransactionContext, job *pps.Job, reason string) error {
	jobs := a.jobs.ReadWrite(txnCtx.SqlTx)
	if job == nil {
		return errors.New("Job must be specified")
	}

	jobInfo := &pps.JobInfo{}
	if err := jobs.Get(ppsdb.JobKey(job), jobInfo); err != nil {
		return err
	}

	commitInfo, err := a.env.PfsServer().InspectCommitInTransaction(txnCtx, &pfs.InspectCommitRequest{
		Commit: jobInfo.OutputCommit,
	})
	if err != nil && !pfsServer.IsCommitNotFoundErr(err) && !pfsServer.IsCommitDeletedErr(err) {
		return err
	}

	if commitInfo != nil {
		if err := a.env.PfsServer().FinishCommitInTransaction(txnCtx, &pfs.FinishCommitRequest{
			Commit: commitInfo.Commit,
			Error:  true,
			Force:  true,
		}); err != nil && !pfsServer.IsCommitNotFoundErr(err) && !pfsServer.IsCommitDeletedErr(err) && !pfsServer.IsCommitFinishedErr(err) {
			return err
		}

		if err := a.env.PfsServer().FinishCommitInTransaction(txnCtx, &pfs.FinishCommitRequest{
			Commit: ppsutil.MetaCommit(commitInfo.Commit),
			Error:  true,
			Force:  true,
		}); err != nil && !pfsServer.IsCommitNotFoundErr(err) && !pfsServer.IsCommitDeletedErr(err) && !pfsServer.IsCommitFinishedErr(err) {
			return err
		}
	}

	// TODO: We can still not update a job's state if we fail here. This is
	// probably fine for now since we are likely to have a more comprehensive
	// solution to this with global ids.
	if err := ppsutil.UpdateJobState(a.pipelines.ReadWrite(txnCtx.SqlTx), jobs, jobInfo, pps.JobState_JOB_KILLED, reason); err != nil && !ppsServer.IsJobFinishedErr(err) {
		return err
	}
	return nil
}

// RestartDatum implements the protobuf pps.RestartDatum RPC
func (a *apiServer) RestartDatum(ctx context.Context, request *pps.RestartDatumRequest) (response *types.Empty, retErr error) {
	func() { a.Log(request, nil, nil, 0) }()
	defer func(start time.Time) { a.Log(request, response, retErr, time.Since(start)) }(time.Now())
	jobInfo, err := a.InspectJob(ctx, &pps.InspectJobRequest{
		Job: request.Job,
	})
	if err != nil {
		return nil, err
	}
	workerPoolID := ppsutil.PipelineRcName(jobInfo.Job.Pipeline.Name, jobInfo.PipelineVersion)
	if err := workerserver.Cancel(ctx, workerPoolID, a.env.GetEtcdClient(), a.etcdPrefix, a.workerGrpcPort, request.Job.ID, request.DataFilters); err != nil {
		return nil, err
	}
	return &types.Empty{}, nil
}

func (a *apiServer) InspectDatum(ctx context.Context, request *pps.InspectDatumRequest) (response *pps.DatumInfo, retErr error) {
	func() { a.Log(request, nil, nil, 0) }()
	defer func(start time.Time) { a.Log(request, response, retErr, time.Since(start)) }(time.Now())
	// TODO: Auth?
	if err := a.collectDatums(ctx, request.Datum.Job, func(meta *datum.Meta, pfsState *pfs.File) error {
		if common.DatumID(meta.Inputs) == request.Datum.ID {
			response = convertDatumMetaToInfo(meta, request.Datum.Job)
			response.PfsState = pfsState
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return response, nil
}

func (a *apiServer) ListDatum(request *pps.ListDatumRequest, server pps.API_ListDatumServer) (retErr error) {
	func() { a.Log(request, nil, nil, 0) }()
	defer func(start time.Time) { a.Log(request, nil, retErr, time.Since(start)) }(time.Now())
	// TODO: Auth?
	if request.Input != nil {
		return a.listDatumInput(server.Context(), request.Input, func(meta *datum.Meta) error {
			di := convertDatumMetaToInfo(meta, nil)
			di.State = pps.DatumState_UNKNOWN
			di.Datum.ID = ""
			return server.Send(di)
		})
	}
	return a.collectDatums(server.Context(), request.Job, func(meta *datum.Meta, _ *pfs.File) error {
		return server.Send(convertDatumMetaToInfo(meta, request.Job))
	})
}

func (a *apiServer) listDatumInput(ctx context.Context, input *pps.Input, cb func(*datum.Meta) error) error {
	setInputDefaults("", input)
	if visitErr := pps.VisitInput(input, func(input *pps.Input) error {
		if input.Pfs != nil {
			pachClient := a.env.GetPachClient(ctx)
			ci, err := pachClient.InspectCommit(input.Pfs.Repo, input.Pfs.Branch, "")
			if err != nil {
				return err
			}
			input.Pfs.Commit = ci.Commit.ID
		}
		if input.Cron != nil {
			return errors.Errorf("can't list datums with a cron input, there will be no datums until the pipeline is created")
		}
		return nil
	}); visitErr != nil {
		return visitErr
	}
	pachClient := a.env.GetPachClient(ctx)
	di, err := datum.NewIterator(pachClient, input)
	if err != nil {
		return err
	}
	return di.Iterate(func(meta *datum.Meta) error {
		return cb(meta)
	})
}

func convertDatumMetaToInfo(meta *datum.Meta, sourceJob *pps.Job) *pps.DatumInfo {
	di := &pps.DatumInfo{
		Datum: &pps.Datum{
			Job: meta.Job,
			ID:  common.DatumID(meta.Inputs),
		},
		State: convertDatumState(meta.State),
		Stats: meta.Stats,
	}
	for _, input := range meta.Inputs {
		di.Data = append(di.Data, input.FileInfo)
	}
	if meta.Job != nil && !proto.Equal(meta.Job, sourceJob) {
		di.State = pps.DatumState_SKIPPED
	}
	return di
}

// TODO: this is a bit wonky, but it is necessary based on the dependency graph.
func convertDatumState(state datum.State) pps.DatumState {
	switch state {
	case datum.State_FAILED:
		return pps.DatumState_FAILED
	case datum.State_RECOVERED:
		return pps.DatumState_RECOVERED
	default:
		return pps.DatumState_SUCCESS
	}
}

func (a *apiServer) collectDatums(ctx context.Context, job *pps.Job, cb func(*datum.Meta, *pfs.File) error) error {
	jobInfo, err := a.InspectJob(ctx, &pps.InspectJobRequest{
		Job: job,
	})
	if err != nil {
		return err
	}
	pachClient := a.env.GetPachClient(ctx)
	metaCommit := ppsutil.MetaCommit(jobInfo.OutputCommit)
	fsi := datum.NewCommitIterator(pachClient, metaCommit)
	return fsi.Iterate(func(meta *datum.Meta) error {
		// TODO: Potentially refactor into datum package (at least the path).
		pfsState := &pfs.File{
			Commit: metaCommit,
			Path:   "/" + path.Join(datum.PFSPrefix, common.DatumID(meta.Inputs)),
		}
		return cb(meta, pfsState)
	})
}

func (a *apiServer) GetLogs(request *pps.GetLogsRequest, apiGetLogsServer pps.API_GetLogsServer) (retErr error) {
	// Set the default for the `Since` field.
	if request.Since == nil || (request.Since.Seconds == 0 && request.Since.Nanos == 0) {
		request.Since = types.DurationProto(DefaultLogsFrom)
	}
	if a.env.Config().LokiLogging || request.UseLokiBackend {
		pachClient := a.env.GetPachClient(apiGetLogsServer.Context())
		resp, err := pachClient.Enterprise.GetState(pachClient.Ctx(),
			&enterpriseclient.GetStateRequest{})
		if err != nil {
			return errors.Wrapf(grpcutil.ScrubGRPC(err), "could not get enterprise status")
		}
		if resp.State == enterpriseclient.State_ACTIVE {
			return a.getLogsLoki(request, apiGetLogsServer)
		}
		enterprisemetrics.IncEnterpriseFailures()
		return errors.Errorf("%s requires an activation key to use Loki for logs. %s\n\n%s",
			enterprisetext.OpenSourceProduct, enterprisetext.ActivateCTA, enterprisetext.RegisterCTA)
	}
	func() { a.Log(request, nil, nil, 0) }()
	defer func(start time.Time) { a.Log(request, nil, retErr, time.Since(start)) }(time.Now())

	// Authorize request and get list of pods containing logs we're interested in
	// (based on pipeline and job filters)
	var rcName, containerName string
	if request.Pipeline == nil && request.Job == nil {
		if len(request.DataFilters) > 0 || request.Datum != nil {
			return errors.Errorf("must specify the Job or Pipeline that the datum is from to get logs for it")
		}
		if err := a.env.AuthServer().CheckClusterIsAuthorized(apiGetLogsServer.Context(), auth.Permission_CLUSTER_GET_PACHD_LOGS); err != nil {
			return err
		}
		containerName, rcName = "pachd", "pachd"
	} else if request.Job != nil && request.Job.GetPipeline().GetName() == "" {
		return errors.Errorf("pipeline must be specified for the given job")
	} else if request.Job != nil && request.Pipeline != nil && !proto.Equal(request.Job.Pipeline, request.Pipeline) {
		return errors.Errorf("job is from the wrong pipeline")
	} else {
		containerName = client.PPSWorkerUserContainerName

		// 1) Lookup the PipelineInfo for this pipeline/job, for auth and to get the
		// RC name
		var pipelineInfo *pps.PipelineInfo
		var err error
		if request.Pipeline != nil && request.Job == nil {
			pipelineInfo, err = a.inspectPipeline(apiGetLogsServer.Context(), request.Pipeline.Name, true)
			if err != nil {
				return errors.Wrapf(err, "could not get pipeline information for %s", request.Pipeline.Name)
			}
		} else if request.Job != nil {
			// If user provides a job, lookup the pipeline from the JobInfo, and then
			// get the pipeline RC
			jobInfo := &pps.JobInfo{}
			err = a.jobs.ReadOnly(apiGetLogsServer.Context()).Get(ppsdb.JobKey(request.Job), jobInfo)
			if err != nil {
				return errors.Wrapf(err, "could not get job information for \"%s\"", request.Job.ID)
			}
			pipelineInfo, err = a.inspectPipeline(apiGetLogsServer.Context(), jobInfo.Job.Pipeline.Name, true)
			if err != nil {
				return errors.Wrapf(err, "could not get pipeline information for %s", jobInfo.Job.Pipeline.Name)
			}
		}

		// 2) Check whether the caller is authorized to get logs from this pipeline/job
		if err := a.authorizePipelineOp(apiGetLogsServer.Context(), pipelineOpGetLogs, pipelineInfo.Details.Input, pipelineInfo.Pipeline.Name); err != nil {
			return err
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
	// Convert request.From to a usable timestamp.
	since, err := types.DurationFromProto(request.Since)
	if err != nil {
		return errors.Wrapf(err, "invalid from time")
	}
	sinceSeconds := int64(since.Seconds())

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
						Container:    containerName,
						Follow:       request.Follow,
						TailLines:    tailLines,
						SinceSeconds: &sinceSeconds,
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
						if request.Job != nil && (request.Job.ID != msg.JobID || request.Job.Pipeline.Name != msg.PipelineName) {
							continue
						}
						if request.Datum != nil && request.Datum.ID != msg.DatumID {
							continue
						}
						if request.Master != msg.Master {
							continue
						}
						if !common.MatchDatum(request.DataFilters, msg.Data) {
							continue
						}
					}
					msg.Message = strings.TrimSuffix(msg.Message, "\n")

					// Log message passes all filters -- return it
					select {
					case logCh <- msg:
					case <-apiGetLogsServer.Context().Done():
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

func (a *apiServer) getLogsLoki(request *pps.GetLogsRequest, apiGetLogsServer pps.API_GetLogsServer) (retErr error) {
	func() { a.Log(request, nil, nil, 0) }()
	defer func(start time.Time) { a.Log(request, nil, retErr, time.Since(start)) }(time.Now())

	// Authorize request and get list of pods containing logs we're interested in
	// (based on pipeline and job filters)
	loki, err := a.env.GetLokiClient()
	if err != nil {
		return err
	}
	since, err := types.DurationFromProto(request.Since)
	if err != nil {
		return errors.Wrapf(err, "invalid from time")
	}
	if request.Pipeline == nil && request.Job == nil {
		if len(request.DataFilters) > 0 || request.Datum != nil {
			return errors.Errorf("must specify the Job or Pipeline that the datum is from to get logs for it")
		}
		if err := a.env.AuthServer().CheckClusterIsAuthorized(apiGetLogsServer.Context(), auth.Permission_CLUSTER_GET_PACHD_LOGS); err != nil {
			return err
		}
		return lokiutil.QueryRange(apiGetLogsServer.Context(), loki, `{app="pachd"}`, time.Now().Add(-since), time.Now(), request.Follow, func(t time.Time, line string) error {
			return apiGetLogsServer.Send(&pps.LogMessage{
				Message: strings.TrimSuffix(line, "\n"),
			})
		})
	} else if request.Job != nil && request.Pipeline != nil && !proto.Equal(request.Job.Pipeline, request.Pipeline) {
		return errors.Errorf("job is from the wrong pipeline")
	}

	// 1) Lookup the PipelineInfo for this pipeline/job, for auth and to get the
	// RC name
	var pipelineInfo *pps.PipelineInfo

	if request.Pipeline != nil {
		pipelineInfo, err = a.inspectPipeline(apiGetLogsServer.Context(), request.Pipeline.Name, true)
		if err != nil {
			return errors.Wrapf(err, "could not get pipeline information for %s", request.Pipeline.Name)
		}
	} else if request.Job != nil {
		// If user provides a job, lookup the pipeline from the JobInfo, and then
		// get the pipeline RC
		jobInfo := &pps.JobInfo{}
		err = a.jobs.ReadOnly(apiGetLogsServer.Context()).Get(ppsdb.JobKey(request.Job), jobInfo)
		if err != nil {
			return errors.Wrapf(err, "could not get job information for \"%s\"", request.Job.ID)
		}
		pipelineInfo, err = a.inspectPipeline(apiGetLogsServer.Context(), jobInfo.Job.Pipeline.Name, true)
		if err != nil {
			return errors.Wrapf(err, "could not get pipeline information for %s", jobInfo.Job.Pipeline.Name)
		}
	}

	// 2) Check whether the caller is authorized to get logs from this pipeline/job
	if err := a.authorizePipelineOp(apiGetLogsServer.Context(), pipelineOpGetLogs, pipelineInfo.Details.Input, pipelineInfo.Pipeline.Name); err != nil {
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
	return lokiutil.QueryRange(apiGetLogsServer.Context(), loki, query, time.Now().Add(-since), time.Now(), request.Follow, func(t time.Time, line string) error {
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
		if request.Job != nil && (request.Job.ID != msg.JobID || request.Job.Pipeline.Name != msg.PipelineName) {
			return nil
		}
		if request.Datum != nil && request.Datum.ID != msg.DatumID {
			return nil
		}
		if request.Master != msg.Master {
			return nil
		}
		if !common.MatchDatum(request.DataFilters, msg.Data) {
			return nil
		}
		msg.Message = strings.TrimSuffix(msg.Message, "\n")
		return apiGetLogsServer.Send(msg)
	})
}

func contains(s string) string {
	return fmt.Sprintf(" |= %q", s)
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
	if request.Transform == nil {
		return errors.Errorf("pipeline must specify a transform")
	}
	if request.ReprocessSpec != "" &&
		request.ReprocessSpec != client.ReprocessSpecUntilSuccess &&
		request.ReprocessSpec != client.ReprocessSpecEveryJob {
		return errors.Errorf("invalid pipeline spec: ReprocessSpec must be one of '%s' or '%s'",
			client.ReprocessSpecUntilSuccess, client.ReprocessSpecEveryJob)
	}
	if request.Spout != nil && request.Autoscaling {
		return errors.Errorf("autoscaling can't be used with spouts (spouts aren't triggered externally)")
	}
	return nil
}

func (a *apiServer) validateEnterpriseChecks(ctx context.Context, pipelineInfo *pps.PipelineInfo) error {
	pachClient := a.env.GetPachClient(ctx)
	pipelines := a.pipelines.ReadOnly(ctx)
	if err := pipelines.Get(pipelineInfo.Pipeline.Name, &pps.PipelineInfo{}); err == nil {
		// Pipeline already exists so we allow people to update it even if
		// they're over the limits.
		return nil
	} else if !col.IsErrNotFound(err) {
		return err
	}
	resp, err := pachClient.Enterprise.GetState(pachClient.Ctx(),
		&enterpriseclient.GetStateRequest{})
	if err != nil {
		return errors.Wrapf(grpcutil.ScrubGRPC(err), "could not get enterprise status")
	}
	if resp.State == enterpriseclient.State_ACTIVE {
		// Enterprise is enabled so anything goes.
		return nil
	}
	pipelineCount, err := pipelines.Count()
	if err != nil {
		return err
	}
	if pipelineCount >= enterpriselimits.Pipelines {
		enterprisemetrics.IncEnterpriseFailures()
		return errors.Errorf("%s requires an activation key to create more than %d total pipelines (you have %d). %s\n\n%s",
			enterprisetext.OpenSourceProduct, enterpriselimits.Pipelines, pipelineCount, enterprisetext.ActivateCTA, enterprisetext.RegisterCTA)
	}
	if pipelineInfo.Details.ParallelismSpec != nil && pipelineInfo.Details.ParallelismSpec.Constant > enterpriselimits.Parallelism {
		enterprisemetrics.IncEnterpriseFailures()
		return errors.Errorf("%s requires an activation key to create pipelines with parallelism more than %d. %s\n\n%s",
			enterprisetext.OpenSourceProduct, enterpriselimits.Parallelism, enterprisetext.ActivateCTA, enterprisetext.RegisterCTA)
	}
	return nil
}

func (a *apiServer) validatePipeline(pipelineInfo *pps.PipelineInfo) error {
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
	if err := validateTransform(pipelineInfo.Details.Transform); err != nil {
		return errors.Wrapf(err, "invalid transform")
	}
	if err := a.validateInput(pipelineInfo.Pipeline.Name, pipelineInfo.Details.Input); err != nil {
		return err
	}
	if pipelineInfo.Details.ParallelismSpec != nil {
		if pipelineInfo.Details.Service != nil && pipelineInfo.Details.ParallelismSpec.Constant != 1 {
			return errors.New("services can only be run with a constant parallelism of 1")
		}
	}
	if pipelineInfo.Details.OutputBranch == "" {
		return errors.New("pipeline needs to specify an output branch")
	}
	if pipelineInfo.Details.JobTimeout != nil {
		_, err := types.DurationFromProto(pipelineInfo.Details.JobTimeout)
		if err != nil {
			return err
		}
	}
	if pipelineInfo.Details.DatumTimeout != nil {
		_, err := types.DurationFromProto(pipelineInfo.Details.DatumTimeout)
		if err != nil {
			return err
		}
	}
	if pipelineInfo.Details.PodSpec != "" && !json.Valid([]byte(pipelineInfo.Details.PodSpec)) {
		return errors.Errorf("malformed PodSpec")
	}
	if pipelineInfo.Details.PodPatch != "" && !json.Valid([]byte(pipelineInfo.Details.PodPatch)) {
		return errors.Errorf("malformed PodPatch")
	}
	if pipelineInfo.Details.Service != nil {
		validServiceTypes := map[v1.ServiceType]bool{
			v1.ServiceTypeClusterIP:    true,
			v1.ServiceTypeLoadBalancer: true,
			v1.ServiceTypeNodePort:     true,
		}

		if !validServiceTypes[v1.ServiceType(pipelineInfo.Details.Service.Type)] {
			return errors.Errorf("the following service type %s is not allowed", pipelineInfo.Details.Service.Type)
		}
	}
	if pipelineInfo.Details.Spout != nil {
		if pipelineInfo.Details.Spout.Service == nil && pipelineInfo.Details.Input != nil {
			return errors.Errorf("spout pipelines (without a service) must not have an input")
		}
	}
	return nil
}

func branchProvenance(input *pps.Input) []*pfs.Branch {
	var result []*pfs.Branch
	pps.VisitInput(input, func(input *pps.Input) error {
		if input.Pfs != nil {
			result = append(result, client.NewBranch(input.Pfs.Repo, input.Pfs.Branch))
		}
		if input.Cron != nil {
			result = append(result, client.NewBranch(input.Cron.Repo, "master"))
		}
		return nil
	})
	return result
}

func (a *apiServer) writePipelineInfoToFileSet(ctx context.Context, pipelineInfo *pps.PipelineInfo) (string, error) {
	data, err := pipelineInfo.Marshal()
	if err != nil {
		return "", errors.Wrapf(err, "could not marshal PipelineInfo")
	}
	pachClient := a.env.GetPachClient(ctx)
	resp, err := pachClient.WithCreateFileSetClient(func(mf client.ModifyFile) error {
		err := mf.PutFile(ppsconsts.SpecFile, bytes.NewReader(data))
		return err
	})
	if err != nil {
		return "", err
	}
	return resp.FileSetId, nil
}

// commitPipelineInfoFromFileSet is a helper for all pipeline updates that
// creates a commit with 'pipelineInfo' in SpecRepo (in PFS). It's called in
// both the case where a user is updating a pipeline and the case where a user
// is creating a new pipeline.
// pipelineInfo.Version is used to ensure that (in the absence of
// transactionality) the new pipelineInfo is only applied on top of the previous
// pipelineInfo - if the version of the pipeline has changed, this operation
// will error out.
func (a *apiServer) commitPipelineInfoFromFileSet(
	txnCtx *txncontext.TransactionContext,
	pipelineName string,
	filesetID string,
	prevPipelineVersion uint64,
) (*pfs.Commit, error) {
	// Make sure the PipelineInfo is the same version we expect so we don't clobber another pipeline update
	if prevPipelineVersion != 0 {
		latestPipelineInfo, err := a.latestPipelineInfo(txnCtx, pipelineName)
		if err != nil {
			return nil, err
		}
		if prevPipelineVersion != latestPipelineInfo.Version {
			return nil, errors.Errorf("pipeline operation aborted due to concurrent pipeline update")
		}
	}

	commit, err := a.env.PfsServer().StartCommitInTransaction(txnCtx, &pfs.StartCommitRequest{
		Branch: client.NewSystemRepo(pipelineName, pfs.SpecRepoType).NewBranch("master"),
	})
	if err != nil {
		return nil, errors.Wrapf(err, "could not marshal PipelineInfo")
	}

	if err := a.env.PfsServer().AddFileSetInTransaction(txnCtx, &pfs.AddFileSetRequest{
		Commit:    commit,
		FileSetId: filesetID,
	}); err != nil {
		return nil, err
	}

	if err := a.env.PfsServer().FinishCommitInTransaction(txnCtx, &pfs.FinishCommitRequest{
		Commit: commit,
	}); err != nil {
		return nil, err
	}

	return commit, nil
}

func (a *apiServer) fixPipelineInputRepoACLs(ctx context.Context, pipelineInfo *pps.PipelineInfo, prevPipelineInfo *pps.PipelineInfo) error {
	return a.txnEnv.WithWriteContext(ctx, func(txnCtx *txncontext.TransactionContext) error {
		return a.fixPipelineInputRepoACLsInTransaction(txnCtx, pipelineInfo, prevPipelineInfo)
	})
}

func (a *apiServer) fixPipelineInputRepoACLsInTransaction(txnCtx *txncontext.TransactionContext, pipelineInfo *pps.PipelineInfo, prevPipelineInfo *pps.PipelineInfo) (retErr error) {
	add := make(map[string]struct{})
	remove := make(map[string]struct{})
	var pipelineName string
	// Figure out which repos 'pipeline' might no longer be using
	if prevPipelineInfo != nil {
		pipelineName = prevPipelineInfo.Pipeline.Name
		pps.VisitInput(prevPipelineInfo.Details.Input, func(input *pps.Input) error {
			var repo string
			switch {
			case input.Pfs != nil:
				repo = input.Pfs.Repo
			case input.Cron != nil:
				repo = input.Cron.Repo
			default:
				return nil // no scope to set: input is not a repo
			}
			remove[repo] = struct{}{}
			return nil
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
		pps.VisitInput(pipelineInfo.Details.Input, func(input *pps.Input) error {
			var repo string
			switch {
			case input.Pfs != nil:
				repo = input.Pfs.Repo
			case input.Cron != nil:
				repo = input.Cron.Repo
			default:
				return nil // no scope to set: input is not a repo
			}
			if _, ok := remove[repo]; ok {
				delete(remove, repo)
			} else {
				add[repo] = struct{}{}
			}
			return nil
		})
	}
	if pipelineName == "" {
		return errors.Errorf("fixPipelineInputRepoACLs called with both current and " +
			"previous pipelineInfos == to nil; this is a bug")
	}

	// make sure we don't touch the pipeline's permissions on its output repo
	delete(remove, pipelineName)
	delete(add, pipelineName)

	defer func() {
		retErr = errors.Wrapf(retErr, "error fixing ACLs on \"%s\"'s input repos", pipelineName)
	}()

	// Remove pipeline from old, unused inputs
	for repo := range remove {
		// If we get an `ErrNoRoleBinding` that means the input repo no longer exists - we're removing it anyways, so we don't care.
		if err := a.env.AuthServer().RemovePipelineReaderFromRepoInTransaction(txnCtx, repo, pipelineName); err != nil && !auth.IsErrNoRoleBinding(err) {
			return err
		}
	}
	// Add pipeline to every new input's ACL as a READER
	for repo := range add {
		// This raises an error if the input repo doesn't exist, or if the user doesn't have permissions to add a pipeline as a reader on the input repo
		if err := a.env.AuthServer().AddPipelineReaderToRepoInTransaction(txnCtx, repo, pipelineName); err != nil {
			return err
		}
	}
	// Add pipeline to its output repo's ACL as a WRITER if it's new
	if prevPipelineInfo == nil {
		if err := a.env.AuthServer().AddPipelineWriterToRepoInTransaction(txnCtx, pipelineName); err != nil {
			return err
		}
	}
	return nil
}

// getExpectedNumWorkers is a helper function for CreatePipeline that transforms
// the parallelism spec in CreatePipelineRequest.Parallelism into a constant
// that can be stored in PipelineInfo.Parallelism
func getExpectedNumWorkers(kc *kube.Clientset, pipelineInfo *pps.PipelineInfo) (int, error) {
	switch pspec := pipelineInfo.Details.ParallelismSpec; {
	case pspec == nil, pspec.Constant == 0:
		return 1, nil
	case pspec.Constant > 0:
		return int(pspec.Constant), nil
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
//   branch provenance, rather than commitPipelineInfoFromFileSet higher up.
// - This is because CreatePipeline calls hardStopPipeline towards the top,
// 	 breaking the provenance connection from the spec branch to the output branch
// - For straightforward pipeline updates (e.g. new pipeline image)
//   stopping + updating + starting the pipeline isn't necessary
// - However it is necessary in many slightly atypical cases  (e.g. the
//   pipeline input changed: if the spec commit is created while the
//   output branch has its old provenance, or the output branch gets new
//   provenance while the old spec commit is the HEAD of the spec branch,
//   then an output commit will be created with provenance that doesn't
//   match its spec's PipelineInfo.Details.Input. Another example is when
//   request.Reprocess == true).
// - Rather than try to enumerate every case where we can't create a spec
//   commit without stopping the pipeline, we just always stop the pipeline
func (a *apiServer) CreatePipeline(ctx context.Context, request *pps.CreatePipelineRequest) (response *types.Empty, retErr error) {
	func() { a.Log(request, nil, nil, 0) }()
	defer func(start time.Time) { a.Log(request, response, retErr, time.Since(start)) }(time.Now())
	metricsFn := metrics.ReportUserAction(ctx, a.reporter, "CreatePipeline")
	defer func(start time.Time) { metricsFn(start, retErr) }(time.Now())

	if request.Pipeline == nil {
		return nil, errors.New("request.Pipeline cannot be nil")
	}

	// Annotate current span with pipeline & persist any extended trace to etcd
	span := opentracing.SpanFromContext(ctx)
	tracing.TagAnySpan(span, "pipeline", request.Pipeline.Name)
	defer func() {
		tracing.TagAnySpan(span, "err", retErr)
	}()
	extended.PersistAny(ctx, a.env.GetEtcdClient(), request.Pipeline.Name)

	// Don't provide a fileset and the transaction env will generate it
	filesetID := ""
	prevPipelineVersion := uint64(0)
	if err := a.txnEnv.WithTransaction(ctx, func(txn txnenv.Transaction) error {
		return txn.CreatePipeline(request, &filesetID, &prevPipelineVersion)
	}, nil); err != nil {
		return nil, err
	}
	return &types.Empty{}, nil
}

func (a *apiServer) initializePipelineInfo(request *pps.CreatePipelineRequest, oldPipelineInfo *pps.PipelineInfo) (*pps.PipelineInfo, error) {
	if err := a.validatePipelineRequest(request); err != nil {
		return nil, err
	}

	// Reprocess overrides the salt in the request
	if request.Salt == "" || request.Reprocess {
		request.Salt = uuid.NewWithoutDashes()
	}

	pipelineInfo := &pps.PipelineInfo{
		Pipeline: request.Pipeline,
		Version:  1,
		Details: &pps.PipelineInfo_Details{
			Transform:             request.Transform,
			TFJob:                 request.TFJob,
			ParallelismSpec:       request.ParallelismSpec,
			Input:                 request.Input,
			OutputBranch:          request.OutputBranch,
			Egress:                request.Egress,
			CreatedAt:             now(),
			ResourceRequests:      request.ResourceRequests,
			ResourceLimits:        request.ResourceLimits,
			SidecarResourceLimits: request.SidecarResourceLimits,
			Description:           request.Description,
			Salt:                  request.Salt,
			Service:               request.Service,
			Spout:                 request.Spout,
			DatumSetSpec:          request.DatumSetSpec,
			DatumTimeout:          request.DatumTimeout,
			JobTimeout:            request.JobTimeout,
			DatumTries:            request.DatumTries,
			SchedulingSpec:        request.SchedulingSpec,
			PodSpec:               request.PodSpec,
			PodPatch:              request.PodPatch,
			S3Out:                 request.S3Out,
			Metadata:              request.Metadata,
			ReprocessSpec:         request.ReprocessSpec,
			Autoscaling:           request.Autoscaling,
		},
	}

	if err := setPipelineDefaults(pipelineInfo); err != nil {
		return nil, err
	}
	// Validate final PipelineInfo (now that defaults have been populated)
	if err := a.validatePipeline(pipelineInfo); err != nil {
		return nil, err
	}

	pps.SortInput(pipelineInfo.Details.Input) // Makes datum hashes comparable

	if oldPipelineInfo != nil {
		// Modify pipelineInfo (increment Version, and *preserve Stopped* so
		// that updating a pipeline doesn't restart it)
		pipelineInfo.Version = oldPipelineInfo.Version + 1
		if oldPipelineInfo.Stopped {
			pipelineInfo.Stopped = true
		}
		if !request.Reprocess {
			pipelineInfo.Details.Salt = oldPipelineInfo.Details.Salt
		}
	}

	return pipelineInfo, nil
}

// latestPipelineInfo doesn't actually work transactionally because
// ppsutil.GetPipelineInfo needs to use pfs.GetFile to read the spec commit,
// which does not support transactions.
func (a *apiServer) latestPipelineInfo(txnCtx *txncontext.TransactionContext, pipelineName string) (*pps.PipelineInfo, error) {
	pipelineInfo := &pps.PipelineInfo{}
	if err := a.pipelines.ReadWrite(txnCtx.SqlTx).Get(pipelineName, pipelineInfo); err != nil {
		return nil, err
	}
	// the spec commit must already exist outside of the transaction, so we can retrieve it normally
	if err := ppsutil.GetPipelineDetails(a.env.GetPachClient(txnCtx.ClientContext), pipelineInfo); err != nil {
		return nil, err
	}
	return pipelineInfo, nil
}

func (a *apiServer) pipelineInfosForUpdate(txnCtx *txncontext.TransactionContext, request *pps.CreatePipelineRequest) (*pps.PipelineInfo, *pps.PipelineInfo, error) {
	if request.Pipeline == nil {
		return nil, nil, errors.New("invalid pipeline spec: request.Pipeline cannot be nil")
	}

	// We can't recover from an incomplete pipeline info here because
	// modifying the spec repo depends on being able to access the previous
	// commit. We therefore use `GetPipelineInfo` which will error if the
	// spec commit isn't working.
	oldPipelineInfo, err := a.latestPipelineInfo(txnCtx, request.Pipeline.Name)
	if err != nil {
		if !col.IsErrNotFound(err) {
			return nil, nil, err
		}
		// Continue with a 'nil' oldPipelineInfo if it does not already exist
	}

	newPipelineInfo, err := a.initializePipelineInfo(request, oldPipelineInfo)
	if err != nil {
		return nil, nil, err
	}

	if err := a.validateEnterpriseChecks(txnCtx.ClientContext, newPipelineInfo); err != nil {
		return nil, nil, err
	}

	return oldPipelineInfo, newPipelineInfo, nil
}

func (a *apiServer) CreatePipelineInTransaction(
	txnCtx *txncontext.TransactionContext,
	request *pps.CreatePipelineRequest,
	specFileSetID *string,
	prevPipelineVersion *uint64,
) error {
	oldPipelineInfo, newPipelineInfo, err := a.pipelineInfosForUpdate(txnCtx, request)
	if err != nil {
		return err
	}
	pipelineName := request.Pipeline.Name

	if *specFileSetID != "" {
		// If we already have a fileset, try to renew it - if that fails, invalidate it
		if err := a.env.GetPachClient(txnCtx.ClientContext).RenewFileSet(*specFileSetID, 600*time.Second); err != nil {
			*specFileSetID = ""
		}
	}

	// If the expected pipeline version doesn't match up with oldPipelineInfo, we
	// need to recreate the fileset
	staleFileSet := false
	if oldPipelineInfo == nil {
		staleFileSet = (*prevPipelineVersion != 0)
	} else {
		staleFileSet = oldPipelineInfo.Version != *prevPipelineVersion
	}

	if staleFileSet || *specFileSetID == "" {
		// No existing fileset or the old one expired, create a new fileset - the
		// pipeline spec to be written into a fileset outside of the transaction.
		*specFileSetID, err = a.writePipelineInfoToFileSet(txnCtx.ClientContext, newPipelineInfo)
		if err != nil {
			return err
		}
		if oldPipelineInfo != nil {
			*prevPipelineVersion = oldPipelineInfo.Version
		}

		// The transaction cannot continue because it cannot see the fileset - abort and retry
		return &col.ErrTransactionConflict{}
	}

	// Verify that all input repos exist (create cron and git repos if necessary)
	if visitErr := pps.VisitInput(newPipelineInfo.Details.Input, func(input *pps.Input) error {
		if input.Pfs != nil {
			if _, err := a.env.PfsServer().InspectRepoInTransaction(txnCtx,
				&pfs.InspectRepoRequest{
					Repo: client.NewSystemRepo(input.Pfs.Repo, input.Pfs.RepoType),
				},
			); err != nil {
				return err
			}
		}
		if input.Cron != nil {
			if err := a.env.PfsServer().CreateRepoInTransaction(txnCtx,
				&pfs.CreateRepoRequest{
					Repo:        client.NewRepo(input.Cron.Repo),
					Description: fmt.Sprintf("Cron tick repo for pipeline %s.", request.Pipeline.Name),
				},
			); err != nil && !errutil.IsAlreadyExistError(err) {
				return err
			}
		}
		return nil
	}); visitErr != nil {
		return visitErr
	}

	// Authorize pipeline creation
	operation := pipelineOpCreate
	if request.Update {
		operation = pipelineOpUpdate
	}
	if err := a.authorizePipelineOpInTransaction(txnCtx, operation, newPipelineInfo.Details.Input, newPipelineInfo.Pipeline.Name); err != nil {
		return err
	}
	update := false
	if request.Update {
		// check if the pipeline already exists, meaning this is a real update
		if err := a.pipelines.ReadWrite(txnCtx.SqlTx).Get(request.Pipeline.Name, &pps.PipelineInfo{}); err == nil {
			update = true
		} else if !col.IsErrNotFound(err) {
			return err
		}
	}
	var (
		// provenance for the pipeline's output branch (includes the spec branch)
		provenance = append(branchProvenance(newPipelineInfo.Details.Input),
			client.NewSystemRepo(pipelineName, pfs.SpecRepoType).NewBranch("master"))
		outputBranch = client.NewBranch(pipelineName, newPipelineInfo.Details.OutputBranch)
		statsBranch  = client.NewSystemRepo(pipelineName, pfs.MetaRepoType).NewBranch(newPipelineInfo.Details.OutputBranch)
		specCommit   *pfs.Commit
	)

	// Get the expected number of workers for this pipeline
	parallelism, err := getExpectedNumWorkers(a.env.GetKubeClient(), newPipelineInfo)
	if err != nil {
		return err
	}

	if update {
		// Help user fix inconsistency if previous UpdatePipeline call failed
		if ci, err := a.env.PfsServer().InspectCommitInTransaction(txnCtx, &pfs.InspectCommitRequest{
			Commit: client.NewSystemRepo(pipelineName, pfs.SpecRepoType).NewCommit("master", ""),
		}); err != nil {
			return err
		} else if ci.Finished == nil {
			return errors.Errorf("the HEAD commit of this pipeline's spec branch " +
				"is open. Either another CreatePipeline call is running or a previous " +
				"call crashed. If you're sure no other CreatePipeline commands are " +
				"running, you can run 'pachctl update pipeline --clean' which will " +
				"delete this open commit")
		}
	} else {
		// Create output and spec repos
		if err := a.env.PfsServer().CreateRepoInTransaction(txnCtx,
			&pfs.CreateRepoRequest{
				Repo:        client.NewRepo(pipelineName),
				Description: fmt.Sprintf("Output repo for pipeline %s.", request.Pipeline.Name),
			}); err != nil && !errutil.IsAlreadyExistError(err) {
			return errors.Wrapf(err, "error creating output repo for %s", pipelineName)
		}
		if err := a.env.PfsServer().CreateRepoInTransaction(txnCtx,
			&pfs.CreateRepoRequest{
				Repo:        client.NewSystemRepo(pipelineName, pfs.SpecRepoType),
				Description: fmt.Sprintf("Spec repo for pipeline %s.", request.Pipeline.Name),
				Update:      true,
			}); err != nil && !errutil.IsAlreadyExistError(err) {
			return errors.Wrapf(err, "error creating spec repo for %s", pipelineName)
		}
	}

	if request.SpecCommit != nil {
		if update {
			// request.SpecCommit indicates we're restoring from an extracted cluster
			// state and should not do any updates
			return errors.New("Cannot update a pipeline and provide a spec commit at the same time")
		}
		// Check if there is an existing spec commit
		commitInfo, err := a.env.PfsServer().InspectCommitInTransaction(txnCtx, &pfs.InspectCommitRequest{
			Commit: request.SpecCommit,
		})
		if err != nil {
			return errors.Wrap(err, "error inspecting spec commit")
		}
		// There is, so we use that as the spec commit, rather than making a new one
		specCommit = commitInfo.Commit
	} else {
		specCommit, err = a.commitPipelineInfoFromFileSet(txnCtx, pipelineName, *specFileSetID, *prevPipelineVersion)
		if err != nil {
			return err
		}
	}

	if update {
		// Kill all unfinished jobs (as those are for the previous version and will
		// no longer be completed)
		if err := a.stopAllJobsInPipeline(txnCtx, request.Pipeline); err != nil {
			return err
		}

		// Update the existing PipelineInfo in the collection
		pipelinePtr := &pps.PipelineInfo{}
		if err := a.pipelines.ReadWrite(txnCtx.SqlTx).Update(pipelineName, pipelinePtr, func() error {
			if oldPipelineInfo.Stopped {
				provenance = nil // CreateBranch() below shouldn't create new output
			}

			// Update pipelinePtr version
			pipelinePtr.Version = newPipelineInfo.Version
			// Update pipelinePtr to point to the new commit
			pipelinePtr.SpecCommit = specCommit
			// Reset pipeline state (PPS master/pipeline controller recreates RC)
			pipelinePtr.State = pps.PipelineState_PIPELINE_STARTING
			// Clear any failure reasons
			pipelinePtr.Reason = ""
			// Update pipeline parallelism
			pipelinePtr.Parallelism = uint64(parallelism)
			// Update the stored type
			pipelinePtr.Type = pipelineTypeFromInfo(newPipelineInfo)

			// Generate new pipeline auth token (added due to & add pipeline to the ACLs of input/output repos
			if err := func() error {
				oldAuthToken := pipelinePtr.AuthToken
				token, err := a.env.AuthServer().GetPipelineAuthTokenInTransaction(txnCtx, request.Pipeline.Name)
				if err != nil {
					if auth.IsErrNotActivated(err) {
						return nil // no auth work to do
					}
					return grpcutil.ScrubGRPC(err)
				}
				pipelinePtr.AuthToken = token

				// If getting a new auth token worked, we should revoke the old one
				if oldAuthToken != "" {
					_, err := a.env.AuthServer().RevokeAuthTokenInTransaction(txnCtx,
						&auth.RevokeAuthTokenRequest{
							Token: oldAuthToken,
						})
					if err != nil {
						if auth.IsErrNotActivated(err) {
							return nil // no auth work to do
						}
						return grpcutil.ScrubGRPC(err)
					}
				}
				return nil
			}(); err != nil {
				return err
			}
			return nil
		}); err != nil {
			return err
		}

		if pipelinePtr.AuthToken != "" {
			if err := a.fixPipelineInputRepoACLsInTransaction(txnCtx, newPipelineInfo, oldPipelineInfo); err != nil {
				return err
			}
		}
	} else {
		// pipelinePtr will be written to the collection, pointing at 'commit'. May
		// include an auth token
		pipelinePtr := &pps.PipelineInfo{
			Pipeline:    request.Pipeline,
			Version:     newPipelineInfo.Version,
			SpecCommit:  specCommit,
			State:       pps.PipelineState_PIPELINE_STARTING,
			Parallelism: uint64(parallelism),
			Type:        pipelineTypeFromInfo(newPipelineInfo),
		}

		// Generate pipeline's auth token & add pipeline to the ACLs of input/output
		// repos
		if err := func() error {
			token, err := a.env.AuthServer().GetPipelineAuthTokenInTransaction(txnCtx, request.Pipeline.Name)
			if err != nil {
				if auth.IsErrNotActivated(err) {
					return nil // no auth work to do
				}
				return grpcutil.ScrubGRPC(err)
			}

			pipelinePtr.AuthToken = token
			return nil
		}(); err != nil {
			return err
		}

		// Put a pointer to the new PipelineInfo commit into the collection
		err := a.pipelines.ReadWrite(txnCtx.SqlTx).Create(pipelineName, pipelinePtr)
		if errutil.IsAlreadyExistError(err) {
			return newErrPipelineExists(pipelineName)
		} else if err != nil {
			return err
		}

		if pipelinePtr.AuthToken != "" {
			if err := a.fixPipelineInputRepoACLsInTransaction(txnCtx, newPipelineInfo, nil); err != nil {
				return err
			}
		}
	}

	// A stopped pipeline should not have its provenance restored until it is
	// restarted.
	if oldPipelineInfo != nil && oldPipelineInfo.Stopped {
		provenance = nil
	}

	// Create or update the output branch (creating new output commit for the pipeline
	// and restarting the pipeline)
	if err := a.env.PfsServer().CreateBranchInTransaction(txnCtx, &pfs.CreateBranchRequest{
		Branch:     outputBranch,
		Provenance: provenance,
	}); err != nil {
		return errors.Wrapf(err, "could not create/update output branch")
	}

	if visitErr := pps.VisitInput(request.Input, func(input *pps.Input) error {
		if input.Pfs != nil && input.Pfs.Trigger != nil {
			var prevHead *pfs.Commit
			if branchInfo, err := a.env.PfsServer().InspectBranchInTransaction(txnCtx, &pfs.InspectBranchRequest{
				Branch: client.NewBranch(input.Pfs.Repo, input.Pfs.Branch),
			}); err != nil {
				if !errutil.IsNotFoundError(err) {
					return err
				}
			} else {
				prevHead = branchInfo.Head
			}

			return a.env.PfsServer().CreateBranchInTransaction(txnCtx, &pfs.CreateBranchRequest{
				Branch:  client.NewBranch(input.Pfs.Repo, input.Pfs.Branch),
				Head:    prevHead,
				Trigger: input.Pfs.Trigger,
			})
		}
		return nil
	}); visitErr != nil {
		return errors.Wrapf(visitErr, "could not create/update trigger branch")
	}

	if request.Service == nil && request.Spout == nil {
		if err := a.env.PfsServer().CreateRepoInTransaction(txnCtx, &pfs.CreateRepoRequest{
			Repo:        statsBranch.Repo,
			Description: fmt.Sprint("Meta repo for pipeline ", pipelineName),
		}); err != nil && !errutil.IsAlreadyExistError(err) {
			return errors.Wrap(err, "could not create meta repo")
		}
		if err := a.env.PfsServer().CreateBranchInTransaction(txnCtx, &pfs.CreateBranchRequest{
			Branch:     statsBranch,
			Provenance: provenance, // same provenance as output branch
		}); err != nil {
			return errors.Wrapf(err, "could not create/update meta branch")
		}
	}
	return nil
}

func pipelineTypeFromInfo(pipelineInfo *pps.PipelineInfo) pps.PipelineInfo_PipelineType {
	if pipelineInfo.Details.Spout != nil {
		return pps.PipelineInfo_PIPELINE_TYPE_SPOUT
	} else if pipelineInfo.Details.Service != nil {
		return pps.PipelineInfo_PIPELINE_TYPE_SERVICE
	}
	return pps.PipelineInfo_PIPELINE_TYPE_TRANSFORM
}

// setPipelineDefaults sets the default values for a pipeline info
func setPipelineDefaults(pipelineInfo *pps.PipelineInfo) error {
	if pipelineInfo.Details.Transform.Image == "" {
		pipelineInfo.Details.Transform.Image = DefaultUserImage
	}
	setInputDefaults(pipelineInfo.Pipeline.Name, pipelineInfo.Details.Input)
	if pipelineInfo.Details.OutputBranch == "" {
		// Output branches default to master
		pipelineInfo.Details.OutputBranch = "master"
	}
	if pipelineInfo.Details.DatumTries == 0 {
		pipelineInfo.Details.DatumTries = DefaultDatumTries
	}
	if pipelineInfo.Details.Service != nil {
		if pipelineInfo.Details.Service.Type == "" {
			pipelineInfo.Details.Service.Type = string(v1.ServiceTypeNodePort)
		}
	}
	if pipelineInfo.Details.Spout != nil && pipelineInfo.Details.Spout.Service != nil && pipelineInfo.Details.Spout.Service.Type == "" {
		pipelineInfo.Details.Spout.Service.Type = string(v1.ServiceTypeNodePort)
	}
	if pipelineInfo.Details.ReprocessSpec == "" {
		pipelineInfo.Details.ReprocessSpec = client.ReprocessSpecUntilSuccess
	}
	return nil
}

func setInputDefaults(pipelineName string, input *pps.Input) {
	now := time.Now()
	nCreatedBranches := make(map[string]int)
	pps.VisitInput(input, func(input *pps.Input) error {
		if input.Pfs != nil {
			if input.Pfs.Branch == "" {
				if input.Pfs.Trigger != nil {
					// We start counting trigger branches at 1
					nCreatedBranches[input.Pfs.Repo]++
					input.Pfs.Branch = fmt.Sprintf("%s-trigger-%d", pipelineName, nCreatedBranches[input.Pfs.Repo])
					if input.Pfs.Trigger.Branch == "" {
						input.Pfs.Trigger.Branch = "master"
					}
				} else {
					input.Pfs.Branch = "master"
				}
			}
			if input.Pfs.Name == "" {
				input.Pfs.Name = input.Pfs.Repo
			}
			if input.Pfs.RepoType == "" {
				input.Pfs.RepoType = pfs.UserRepoType
			}
		}
		if input.Cron != nil {
			if input.Cron.Start == nil {
				start, _ := types.TimestampProto(now)
				input.Cron.Start = start
			}
			if input.Cron.Repo == "" {
				input.Cron.Repo = fmt.Sprintf("%s_%s", pipelineName, input.Cron.Name)
			}
		}
		return nil
	})
}

func (a *apiServer) stopAllJobsInPipeline(txnCtx *txncontext.TransactionContext, pipeline *pps.Pipeline) error {
	// Using ReadWrite here may load a large number of jobs inline in the
	// transaction, but doing an inconsistent read outside of the transaction
	// would be pretty sketchy (and we'd have to worry about trying to get another
	// postgres connection and possibly deadlocking).
	jobInfo := &pps.JobInfo{}
	sort := &col.Options{Target: col.SortByCreateRevision, Order: col.SortAscend}
	return a.jobs.ReadWrite(txnCtx.SqlTx).GetByIndex(ppsdb.JobsTerminalIndex, ppsdb.JobTerminalKey(pipeline, false), jobInfo, sort, func(string) error {
		return a.stopJob(txnCtx, jobInfo.Job, "pipeline updated")
	})
}

// InspectPipeline implements the protobuf pps.InspectPipeline RPC
func (a *apiServer) InspectPipeline(ctx context.Context, request *pps.InspectPipelineRequest) (response *pps.PipelineInfo, retErr error) {
	func() { a.Log(request, nil, nil, 0) }()
	defer func(start time.Time) { a.Log(request, response, retErr, time.Since(start)) }(time.Now())
	return a.inspectPipeline(ctx, request.Pipeline.Name, request.Details)
}

// inspectPipeline contains the functional implementation of InspectPipeline.
// Many functions (GetLogs, ListPipeline) need to inspect a pipeline, so they
// call this instead of making an RPC
func (a *apiServer) inspectPipeline(ctx context.Context, name string, details bool) (*pps.PipelineInfo, error) {
	var response *pps.PipelineInfo
	if err := a.txnEnv.WithReadContext(ctx, func(txnCtx *txncontext.TransactionContext) error {
		var err error
		response, err = a.inspectPipelineInTransaction(txnCtx, name, details)
		return err
	}); err != nil {
		return nil, err
	}
	return response, nil
}

func (a *apiServer) inspectPipelineInTransaction(txnCtx *txncontext.TransactionContext, name string, details bool) (*pps.PipelineInfo, error) {
	kubeClient := a.env.GetKubeClient()
	name, ancestors, err := ancestry.Parse(name)
	if err != nil {
		return nil, err
	}
	pipelineInfo := &pps.PipelineInfo{}
	if err := a.pipelines.ReadWrite(txnCtx.SqlTx).Get(name, pipelineInfo); err != nil {
		if col.IsErrNotFound(err) {
			return nil, errors.Errorf("pipeline \"%s\" not found", name)
		}
		return nil, err
	}
	// Erase any AuthToken - this shouldn't be returned to anyone (the workers
	// won't use this function to get their auth token)
	pipelineInfo.AuthToken = ""
	// TODO(global ids): how should we treat ancestry here?  Alias commits are going to gum it up.
	pipelineInfo.SpecCommit.ID = ancestry.Add(pipelineInfo.SpecCommit.ID, ancestors)
	if details {
		// the spec commit must already exist outside of the transaction, so we can retrieve it normally
		pachClient := a.env.GetPachClient(txnCtx.ClientContext)
		if err := ppsutil.GetPipelineDetails(pachClient, pipelineInfo); err != nil {
			return nil, err
		}
		if pipelineInfo.Details.Service != nil {
			rcName := ppsutil.PipelineRcName(pipelineInfo.Pipeline.Name, pipelineInfo.Version)
			if err != nil {
				return nil, err
			}
			service, err := kubeClient.CoreV1().Services(a.namespace).Get(fmt.Sprintf("%s-user", rcName), metav1.GetOptions{})
			if err != nil {
				if !errutil.IsNotFoundError(err) {
					return nil, err
				}
			} else {
				pipelineInfo.Details.Service.IP = service.Spec.ClusterIP
			}
		}

		// TODO: move this into ppsutil.GetPipelineDetails?
		workerPoolID := ppsutil.PipelineRcName(pipelineInfo.Pipeline.Name, pipelineInfo.Version)
		workerStatus, err := workerserver.Status(txnCtx.ClientContext, workerPoolID, a.env.GetEtcdClient(), a.etcdPrefix, a.workerGrpcPort)
		if err != nil {
			logrus.Errorf("failed to get worker status with err: %s", err.Error())
		} else {
			pipelineInfo.Details.WorkersAvailable = int64(len(workerStatus))
			pipelineInfo.Details.WorkersRequested = int64(pipelineInfo.Parallelism)
		}
		tasks, claims, err := work.NewWorker(
			a.env.GetEtcdClient(),
			a.etcdPrefix,
			driver.WorkNamespace(pipelineInfo),
		).TaskCount(txnCtx.ClientContext)
		if err != nil {
			return nil, err
		}
		pipelineInfo.Details.UnclaimedTasks = tasks - claims
	}
	return pipelineInfo, nil
}

// ListPipeline implements the protobuf pps.ListPipeline RPC
func (a *apiServer) ListPipeline(request *pps.ListPipelineRequest, srv pps.API_ListPipelineServer) (retErr error) {
	func() { a.Log(request, nil, nil, 0) }()
	defer func(start time.Time) {
		a.Log(request, nil, retErr, time.Since(start))
	}(time.Now())
	return a.listPipeline(srv.Context(), request, srv.Send)
}

func (a *apiServer) listPipeline(ctx context.Context, request *pps.ListPipelineRequest, f func(*pps.PipelineInfo) error) error {
	var jqCode *gojq.Code
	var enc serde.Encoder
	var jsonBuffer bytes.Buffer
	if request.JqFilter != "" {
		jqQuery, err := gojq.Parse(request.JqFilter)
		if err != nil {
			return err
		}
		jqCode, err = gojq.Compile(jqQuery)
		if err != nil {
			return err
		}
		// ensure field names and enum values match with --raw output
		enc = serde.NewJSONEncoder(&jsonBuffer, serde.WithOrigName(true))
	}
	filterPipeline := func(pipelineInfo *pps.PipelineInfo) bool {
		if jqCode != nil {
			jsonBuffer.Reset()
			// convert pipelineInfo to a map[string]interface{} for use with gojq
			enc.EncodeProto(pipelineInfo)
			var pipelineInterface interface{}
			json.Unmarshal(jsonBuffer.Bytes(), &pipelineInterface)
			iter := jqCode.Run(pipelineInterface)
			// treat either jq false-y value as rejection
			if v, _ := iter.Next(); v == false || v == nil {
				return false
			}
		}
		return true
	}
	// the mess below is so we can lookup the PFS info for each pipeline concurrently.
	eg, ctx := errgroup.WithContext(ctx)
	infos := make(chan *pps.PipelineInfo)
	// stream these out of etcd
	eg.Go(func() error {
		defer close(infos)
		return a.listPipelineInfo(ctx, request.Pipeline, request.History, func(ptr *pps.PipelineInfo) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case infos <- proto.Clone(ptr).(*pps.PipelineInfo):
				return nil
			}
		})
	})
	// spin up goroutines to get the PFS info, and then use the mutex to call f all synchronized like.
	var mu sync.Mutex
	var fHasErrored bool
	for i := 0; i < 20; i++ {
		eg.Go(func() error {
			for info := range infos {
				pachClient := a.env.GetPachClient(ctx)
				if request.Details {
					if err := ppsutil.GetPipelineDetails(pachClient, info); err != nil {
						return err
					}
				}
				if err := func() error {
					mu.Lock()
					defer mu.Unlock()
					if fHasErrored {
						return nil
					}
					// the filtering shares that buffer thing, and it's CPU bound so why not do it with the lock
					if !filterPipeline(info) {
						return nil
					}
					if err := f(info); err != nil {
						fHasErrored = true
						return err
					}
					return nil
				}(); err != nil {
					return err
				}
			}
			return nil
		})
	}
	return eg.Wait()
}

// listPipelineInfo enumerates all PPS pipelines in the database, filters them
// based on 'request', and then calls 'f' on each value
func (a *apiServer) listPipelineInfo(ctx context.Context,
	pipeline *pps.Pipeline, history int64, f func(*pps.PipelineInfo) error) error {
	p := &pps.PipelineInfo{}
	forEachPipeline := func() error {
		// Erase any AuthToken - this shouldn't be returned to anyone (the workers
		// won't use this function to get their auth token)
		p.AuthToken = ""
		// TODO: this is kind of silly - callers should just make a version range for each pipeline?
		lastVersionToSend := int64(p.Version) - history
		if history == -1 || lastVersionToSend < 1 {
			lastVersionToSend = 1
		}
		for int64(p.Version) >= lastVersionToSend {
			if err := f(p); err != nil {
				return err
			}
			p.Version -= 1
		}
		return nil
	}
	if pipeline == nil {
		if err := a.pipelines.ReadOnly(ctx).List(p, col.DefaultOptions(), func(string) error {
			return forEachPipeline()
		}); err != nil {
			return err
		}
	} else {
		if err := a.pipelines.ReadOnly(ctx).Get(pipeline.Name, p); err != nil {
			if col.IsErrNotFound(err) {
				return errors.Errorf("pipeline \"%s\" not found", pipeline.Name)
			}
			return err
		}
		if err := forEachPipeline(); err != nil {
			return err
		}
	}
	return nil
}

// DeletePipeline implements the protobuf pps.DeletePipeline RPC
func (a *apiServer) DeletePipeline(ctx context.Context, request *pps.DeletePipelineRequest) (response *types.Empty, retErr error) {
	func() { a.Log(request, nil, nil, 0) }()
	defer func(start time.Time) { a.Log(request, response, retErr, time.Since(start)) }(time.Now())

	if request.All {
		request.Pipeline = &pps.Pipeline{}
		pipelineInfo := &pps.PipelineInfo{}
		if err := a.pipelines.ReadOnly(ctx).List(pipelineInfo, col.DefaultOptions(), func(string) error {
			request.Pipeline.Name = pipelineInfo.Pipeline.Name
			return a.deletePipeline(ctx, request)
		}); err != nil {
			return nil, err
		}
	} else if err := a.deletePipeline(ctx, request); err != nil {
		return nil, err
	}

	return &types.Empty{}, nil
}

func (a *apiServer) deletePipeline(ctx context.Context, request *pps.DeletePipelineRequest) error {
	// Check if there's a PipelineInfo for this pipeline. If not, we can't
	// authorize, and must return something here
	pipelineInfo := &pps.PipelineInfo{}
	if err := a.pipelines.ReadOnly(ctx).Get(request.Pipeline.Name, pipelineInfo); err != nil {
		if col.IsErrNotFound(err) {
			return nil
		}
		return err
	}

	if _, err := a.StopPipeline(ctx, &pps.StopPipelineRequest{Pipeline: request.Pipeline}); err != nil {
		return errors.Wrapf(err, "error stopping pipeline %s", request.Pipeline.Name)
	}

	// Load pipeline details so we can do some cleanup tasks based on certain
	// input types and the output branch.
	pachClient := a.env.GetPachClient(ctx)
	if err := ppsutil.GetPipelineDetails(pachClient, pipelineInfo); err != nil {
		return err
	}

	// check if the output repo exists--if not, the pipeline is non-functional and
	// the rest of the delete operation continues without any auth checks
	if _, err := pachClient.InspectRepo(request.Pipeline.Name); err != nil && !errutil.IsNotFoundError(err) && !auth.IsErrNoRoleBinding(err) {
		return err
	} else if err == nil {
		// Check if the caller is authorized to delete this pipeline
		if err := a.authorizePipelineOp(ctx, pipelineOpDelete, pipelineInfo.Details.Input, pipelineInfo.Pipeline.Name); err != nil {
			return err
		}
		if request.KeepRepo {
			// Remove branch provenance
			if err := pachClient.CreateBranch(
				request.Pipeline.Name,
				pipelineInfo.Details.OutputBranch,
				"",
				"",
				nil,
			); err != nil {
				return err
			}
		} else {
			// delete the pipeline's output repo
			if err := pachClient.DeleteRepo(request.Pipeline.Name, request.Force); err != nil {
				return err
			}
		}
	}

	// If necessary, revoke the pipeline's auth token and remove it from its
	// inputs' ACLs
	if pipelineInfo.AuthToken != "" {
		// If auth was deactivated after the pipeline was created, don't bother
		// revoking
		if _, err := pachClient.WhoAmI(pachClient.Ctx(), &auth.WhoAmIRequest{}); err == nil {
			// 'pipelineInfo' == nil => remove pipeline from all input repos
			if err := a.fixPipelineInputRepoACLs(ctx, nil, pipelineInfo); err != nil {
				return grpcutil.ScrubGRPC(err)
			}
			if _, err := pachClient.RevokeAuthToken(pachClient.Ctx(),
				&auth.RevokeAuthTokenRequest{
					Token: pipelineInfo.AuthToken,
				}); err != nil {

				return grpcutil.ScrubGRPC(err)
			}
		}
	}

	// Delete all of the pipeline's jobs - we shouldn't need to worry about any
	// new jobs since the output repo has already been deleted or disconnected.
	var eg errgroup.Group
	jobInfo := &pps.JobInfo{}
	if err := a.jobs.ReadOnly(ctx).GetByIndex(ppsdb.JobsPipelineIndex, request.Pipeline.Name, jobInfo, col.DefaultOptions(), func(string) error {
		job := proto.Clone(jobInfo.Job).(*pps.Job)
		eg.Go(func() error {
			_, err := a.DeleteJob(ctx, &pps.DeleteJobRequest{Job: job})
			if errutil.IsNotFoundError(err) || auth.IsErrNoRoleBinding(err) {
				return nil
			}
			return err
		})
		return nil
	}); err != nil {
		return err
	}
	if err := eg.Wait(); err != nil {
		return err
	}

	eg = errgroup.Group{}
	// Delete cron input repos
	if !request.KeepRepo {
		pps.VisitInput(pipelineInfo.Details.Input, func(input *pps.Input) error {
			if input.Cron != nil {
				eg.Go(func() error {
					return pachClient.DeleteRepo(input.Cron.Repo, request.Force)
				})
			}
			return nil
		})
	}
	// Delete PipelineInfo
	eg.Go(func() error {
		if err := col.NewSQLTx(ctx, a.env.GetDBClient(), func(sqlTx *sqlx.Tx) error {
			return a.pipelines.ReadWrite(sqlTx).Delete(request.Pipeline.Name)
		}); err != nil {
			return errors.Wrapf(err, "collection.Delete")
		}
		return nil
	})
	return eg.Wait()
}

// StartPipeline implements the protobuf pps.StartPipeline RPC
func (a *apiServer) StartPipeline(ctx context.Context, request *pps.StartPipelineRequest) (response *types.Empty, retErr error) {
	func() { a.Log(request, nil, nil, 0) }()
	defer func(start time.Time) { a.Log(request, response, retErr, time.Since(start)) }(time.Now())
	if request.Pipeline == nil {
		return nil, errors.New("request.Pipeline cannot be nil")
	}

	if err := a.txnEnv.WithWriteContext(ctx, func(txnCtx *txncontext.TransactionContext) error {
		// This reads the pipeline spec from PFS which is not transactional, so we
		// may have to abort and reattempt.  We only need this to authorize the
		// update.
		pipelineInfo, err := a.latestPipelineInfo(txnCtx, request.Pipeline.Name)
		if err != nil {
			return err
		}

		// check if the caller is authorized to update this pipeline
		if err := a.authorizePipelineOpInTransaction(txnCtx, pipelineOpUpdate, pipelineInfo.Details.Input, pipelineInfo.Pipeline.Name); err != nil {
			return err
		}

		// Restore branch provenance, which may create a new output commit/job
		provenance := append(branchProvenance(pipelineInfo.Details.Input),
			client.NewSystemRepo(pipelineInfo.Pipeline.Name, pfs.SpecRepoType).NewBranch("master"))
		if err := a.env.PfsServer().CreateBranchInTransaction(txnCtx, &pfs.CreateBranchRequest{
			Branch:     client.NewBranch(pipelineInfo.Pipeline.Name, pipelineInfo.Details.OutputBranch),
			Provenance: provenance,
		}); err != nil {
			return err
		}
		// restore same provenance to meta repo
		if err := a.env.PfsServer().CreateBranchInTransaction(txnCtx, &pfs.CreateBranchRequest{
			Branch:     client.NewSystemRepo(pipelineInfo.Pipeline.Name, pfs.MetaRepoType).NewBranch(pipelineInfo.Details.OutputBranch),
			Provenance: provenance,
		}); err != nil {
			return err
		}

		newPipelineInfo := &pps.PipelineInfo{}
		return a.pipelines.ReadWrite(txnCtx.SqlTx).Update(pipelineInfo.Pipeline.Name, newPipelineInfo, func() error {
			if newPipelineInfo.Version != pipelineInfo.Version {
				// If the pipeline has changed, restart the transaction and try again
				return col.ErrTransactionConflict{}
			}
			newPipelineInfo.Stopped = false
			return nil
		})
	}); err != nil {
		return nil, err
	}
	return &types.Empty{}, nil
}

// StopPipeline implements the protobuf pps.StopPipeline RPC
func (a *apiServer) StopPipeline(ctx context.Context, request *pps.StopPipelineRequest) (response *types.Empty, retErr error) {
	func() { a.Log(request, nil, nil, 0) }()
	defer func(start time.Time) { a.Log(request, response, retErr, time.Since(start)) }(time.Now())
	if request.Pipeline == nil {
		return nil, errors.New("request.Pipeline cannot be nil")
	}

	if err := a.txnEnv.WithWriteContext(ctx, func(txnCtx *txncontext.TransactionContext) error {
		// This reads the pipeline spec from PFS which is not transactional, so we
		// may have to abort and reattempt.  We only need this to authorize the
		// update.
		pipelineInfo, err := a.latestPipelineInfo(txnCtx, request.Pipeline.Name)
		if err != nil {
			return err
		}

		// check if the caller is authorized to update this pipeline
		if err := a.authorizePipelineOpInTransaction(txnCtx, pipelineOpUpdate, pipelineInfo.Details.Input, pipelineInfo.Pipeline.Name); err != nil {
			return err
		}

		// Remove branch provenance to prevent new output and meta commits from being created
		if err := a.env.PfsServer().CreateBranchInTransaction(txnCtx, &pfs.CreateBranchRequest{
			Branch:     client.NewBranch(pipelineInfo.Pipeline.Name, pipelineInfo.Details.OutputBranch),
			Provenance: nil,
		}); err != nil {
			return err
		}
		if err := a.env.PfsServer().CreateBranchInTransaction(txnCtx, &pfs.CreateBranchRequest{
			Branch:     client.NewSystemRepo(pipelineInfo.Pipeline.Name, pfs.MetaRepoType).NewBranch(pipelineInfo.Details.OutputBranch),
			Provenance: nil,
		}); err != nil && !errutil.IsNotFoundError(err) {
			// don't error if we're stopping a spout or service pipeline
			return err
		}

		newPipelineInfo := &pps.PipelineInfo{}
		if err := a.pipelines.ReadWrite(txnCtx.SqlTx).Update(pipelineInfo.Pipeline.Name, newPipelineInfo, func() error {
			if newPipelineInfo.Version != pipelineInfo.Version {
				// If the pipeline has changed, restart the transaction and try again
				return col.ErrTransactionConflict{}
			}
			newPipelineInfo.Stopped = true
			return nil
		}); err != nil {
			return err
		}

		// Kill any remaining jobs
		return a.stopAllJobsInPipeline(txnCtx, request.Pipeline)
	}); err != nil {
		return nil, err
	}
	return &types.Empty{}, nil
}

func (a *apiServer) RunPipeline(ctx context.Context, request *pps.RunPipelineRequest) (response *types.Empty, retErr error) {
	func() { a.Log(request, nil, nil, 0) }()
	defer func(start time.Time) { a.Log(request, response, retErr, time.Since(start)) }(time.Now())

	return nil, errors.New("unimplemented")
}

func (a *apiServer) RunCron(ctx context.Context, request *pps.RunCronRequest) (response *types.Empty, retErr error) {
	func() { a.Log(request, nil, nil, 0) }()
	defer func(start time.Time) { a.Log(request, response, retErr, time.Since(start)) }(time.Now())

	pipelineInfo, err := a.inspectPipeline(ctx, request.Pipeline.Name, true)
	if err != nil {
		return nil, err
	}

	if pipelineInfo.Details.Input == nil {
		return nil, errors.Errorf("pipeline must have a cron input")
	}

	// find any cron inputs
	var crons []*pps.CronInput
	pps.VisitInput(pipelineInfo.Details.Input, func(in *pps.Input) error {
		if in.Cron != nil {
			crons = append(crons, in.Cron)
		}
		return nil
	})

	if len(crons) < 1 {
		return nil, errors.Errorf("pipeline must have a cron input")
	}

	pachClient := a.env.GetPachClient(ctx)
	txn, err := pachClient.StartTransaction()
	if err != nil {
		return nil, err
	}

	// We need all the DeleteFile and the PutFile requests to happen atomicly
	txnClient := pachClient.WithTransaction(txn)

	// make a tick on each cron input
	for _, cron := range crons {
		// TODO: This isn't transactional, we could support a transactional modify file through the fileset API though.
		if err := txnClient.WithModifyFileClient(client.NewCommit(cron.Repo, "master", ""), func(mf client.ModifyFile) error {
			if cron.Overwrite {
				// get rid of any files, so the new file "overwrites" previous runs
				err = mf.DeleteFile("/")
				if err != nil && !errutil.IsNotFoundError(err) {
					return errors.Wrapf(err, "delete error")
				}
			}
			// Put in an empty file named by the timestamp
			if err := mf.PutFile(time.Now().Format(time.RFC3339), strings.NewReader("")); err != nil {
				return errors.Wrapf(err, "put error")
			}
			return nil
		}); err != nil {
			return nil, err
		}
	}

	_, err = txnClient.FinishTransaction(txn)
	if err != nil {
		return nil, err
	}
	return &types.Empty{}, nil
}

func (a *apiServer) propagateJobs(txnCtx *txncontext.TransactionContext) error {
	commitInfos, err := a.env.PfsServer().InspectCommitSetInTransaction(txnCtx, client.NewCommitSet(txnCtx.CommitSetID))
	if err != nil {
		return err
	}

	for _, commitInfo := range commitInfos {
		// Skip alias commits and any commits which have already been finished
		if commitInfo.Origin.Kind == pfs.OriginKind_ALIAS || commitInfo.Finished != nil {
			continue
		}

		// Skip commits from system repos
		if commitInfo.Commit.Branch.Repo.Type != pfs.UserRepoType {
			continue
		}

		// Skip commits from repos that have no associated pipeline
		pipelineInfo := &pps.PipelineInfo{}
		if err := a.pipelines.ReadWrite(txnCtx.SqlTx).Get(commitInfo.Commit.Branch.Repo.Name, pipelineInfo); err != nil {
			if col.IsErrNotFound(err) {
				continue
			}
			return err
		}

		// Don't create jobs for spouts
		if pipelineInfo.Type == pps.PipelineInfo_PIPELINE_TYPE_SPOUT {
			continue
		}

		// Check if there is an existing job for the output commit
		job := client.NewJob(pipelineInfo.Pipeline.Name, txnCtx.CommitSetID)
		jobInfo := &pps.JobInfo{}
		if err := a.jobs.ReadWrite(txnCtx.SqlTx).Get(ppsdb.JobKey(job), jobInfo); err == nil {
			continue // Job already exists, skip it
		} else if !col.IsErrNotFound(err) {
			return err
		}

		pipelines := a.pipelines.ReadWrite(txnCtx.SqlTx)
		jobs := a.jobs.ReadWrite(txnCtx.SqlTx)
		jobPtr := &pps.JobInfo{
			Job:             job,
			PipelineVersion: pipelineInfo.Version,
			OutputCommit:    commitInfo.Commit,
			Stats:           &pps.ProcessStats{},
			Created:         types.TimestampNow(),
		}
		if err := ppsutil.UpdateJobState(pipelines, jobs, jobPtr, pps.JobState_JOB_CREATED, ""); err != nil {
			return err
		}
	}
	return nil
}

// CreateSecret implements the protobuf pps.CreateSecret RPC
func (a *apiServer) CreateSecret(ctx context.Context, request *pps.CreateSecretRequest) (response *types.Empty, retErr error) {
	func() { a.Log(request, nil, nil, 0) }()
	defer func(start time.Time) { a.Log(request, response, retErr, time.Since(start)) }(time.Now())
	metricsFn := metrics.ReportUserAction(ctx, a.reporter, "CreateSecret")
	defer func(start time.Time) { metricsFn(start, retErr) }(time.Now())

	var s v1.Secret
	if err := json.Unmarshal(request.GetFile(), &s); err != nil {
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

	if _, err := a.env.GetKubeClient().CoreV1().Secrets(a.namespace).Create(&s); err != nil {
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

	if _, err := a.DeletePipeline(ctx, &pps.DeletePipelineRequest{All: true, Force: true}); err != nil {
		return nil, err
	}

	if err := a.env.GetKubeClient().CoreV1().Secrets(a.namespace).DeleteCollection(&metav1.DeleteOptions{}, metav1.ListOptions{
		LabelSelector: "secret-source=pachyderm-user",
	}); err != nil {
		return nil, err
	}
	return &types.Empty{}, nil
}

// ActivateAuth implements the protobuf pps.ActivateAuth RPC
func (a *apiServer) ActivateAuth(ctx context.Context, req *pps.ActivateAuthRequest) (resp *pps.ActivateAuthResponse, retErr error) {
	func() { a.Log(req, nil, nil, 0) }()
	defer func(start time.Time) { a.Log(req, resp, retErr, time.Since(start)) }(time.Now())
	pachClient := a.env.GetPachClient(ctx)

	// Unauthenticated users can't create new pipelines or repos, and users can't
	// log in while auth is in an intermediate state, so 'pipelines' is exhaustive
	var pipelines []*pps.PipelineInfo
	pipelines, err := pachClient.ListPipeline(true)
	if err != nil {
		return nil, errors.Wrapf(grpcutil.ScrubGRPC(err), "cannot get list of pipelines to update")
	}

	var eg errgroup.Group
	for _, pipeline := range pipelines {
		pipeline := pipeline
		pipelineName := pipeline.Pipeline.Name
		// 1) Create a new auth token for 'pipeline' and attach it, so that the
		// pipeline can authenticate as itself when it needs to read input data
		eg.Go(func() error {
			return a.txnEnv.WithWriteContext(ctx, func(txnCtx *txncontext.TransactionContext) error {
				token, err := a.env.AuthServer().GetPipelineAuthTokenInTransaction(txnCtx, pipelineName)
				if err != nil {
					return errors.Wrapf(grpcutil.ScrubGRPC(err), "could not generate pipeline auth token")
				}

				pipelineInfo := &pps.PipelineInfo{}
				if err := a.pipelines.ReadWrite(txnCtx.SqlTx).Update(pipelineName, pipelineInfo, func() error {
					pipelineInfo.AuthToken = token
					return nil
				}); err != nil {
					return errors.Wrapf(err, "could not update \"%s\" with new auth token", pipelineName)
				}
				return nil
			})
		})
		// put 'pipeline' on relevant ACLs
		if err := a.fixPipelineInputRepoACLs(ctx, pipeline, nil); err != nil {
			return nil, err
		}
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	return &pps.ActivateAuthResponse{}, nil
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

func (a *apiServer) resolveCommit(ctx context.Context, commit *pfs.Commit) (*pfs.CommitInfo, error) {
	pachClient := a.env.GetPachClient(ctx)
	ci, err := pachClient.PfsAPIClient.InspectCommit(
		pachClient.Ctx(),
		&pfs.InspectCommitRequest{
			Commit: commit,
			Wait:   pfs.CommitState_STARTED,
		})
	if err != nil {
		return nil, grpcutil.ScrubGRPC(err)
	}
	return ci, nil
}

func labels(app string) map[string]string {
	return map[string]string{
		"app":       app,
		"suite":     suite,
		"component": "worker",
	}
}

// Package ppsutil contains utilities for various PPS-related tasks, which are
// shared by both the PPS API and the worker binary. These utilities include:
// - Getting the RC name and querying k8s reguarding pipelines
// - Reading and writing pipeline resource requests and limits
// - Reading and writing StoredPipelineInfos and PipelineInfos[1]
//
// [1] Note that PipelineInfo in particular is complicated because it contains
// fields that are not always set or are stored in multiple places
// ('job_state', for example, is not stored in PFS along with the rest of each
// PipelineInfo, because this field is volatile and we cannot commit to PFS
// every time it changes. 'job_counts' is the same, and 'reason' is in etcd
// because it is only updated alongside 'job_state').  As of 12/7/2017, these
// are the only fields not stored in PFS.
package ppsutil

import (
	"bytes"
	"fmt"
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	"github.com/jmoiron/sqlx"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/pachyderm/pachyderm/v2/src/client"
	col "github.com/pachyderm/pachyderm/v2/src/internal/collection"
	"github.com/pachyderm/pachyderm/v2/src/internal/errors"
	"github.com/pachyderm/pachyderm/v2/src/internal/errutil"
	"github.com/pachyderm/pachyderm/v2/src/internal/pfsdb"
	"github.com/pachyderm/pachyderm/v2/src/internal/ppsconsts"
	"github.com/pachyderm/pachyderm/v2/src/internal/tracing"
	"github.com/pachyderm/pachyderm/v2/src/pfs"
	"github.com/pachyderm/pachyderm/v2/src/pps"
	ppsServer "github.com/pachyderm/pachyderm/v2/src/server/pps"
)

// PipelineRepo creates a pfs repo for a given pipeline.
func PipelineRepo(pipeline *pps.Pipeline) *pfs.Repo {
	return client.NewRepo(pipeline.Name)
}

// PipelineRcName generates the name of the k8s replication controller that
// manages a pipeline's workers
func PipelineRcName(name string, version uint64) string {
	// k8s won't allow RC names that contain upper-case letters
	// or underscores
	// TODO: deal with name collision
	name = strings.Replace(name, "_", "-", -1)
	return fmt.Sprintf("pipeline-%s-v%d", strings.ToLower(name), version)
}

// GetRequestsResourceListFromPipeline returns a list of resources that the pipeline,
// minimally requires.
func GetRequestsResourceListFromPipeline(pipelineInfo *pps.PipelineInfo) (*v1.ResourceList, error) {
	return getResourceListFromSpec(pipelineInfo.ResourceRequests)
}

func getResourceListFromSpec(resources *pps.ResourceSpec) (*v1.ResourceList, error) {
	result := make(v1.ResourceList)

	if resources.Cpu != 0 {
		cpuStr := fmt.Sprintf("%f", resources.Cpu)
		cpuQuantity, err := resource.ParseQuantity(cpuStr)
		if err != nil {
			log.Warnf("error parsing cpu string: %s: %+v", cpuStr, err)
		} else {
			result[v1.ResourceCPU] = cpuQuantity
		}
	}

	if resources.Memory != "" {
		memQuantity, err := resource.ParseQuantity(resources.Memory)
		if err != nil {
			log.Warnf("error parsing memory string: %s: %+v", resources.Memory, err)
		} else {
			result[v1.ResourceMemory] = memQuantity
		}
	}

	if resources.Disk != "" { // needed because not all versions of k8s support disk resources
		diskQuantity, err := resource.ParseQuantity(resources.Disk)
		if err != nil {
			log.Warnf("error parsing disk string: %s: %+v", resources.Disk, err)
		} else {
			result[v1.ResourceEphemeralStorage] = diskQuantity
		}
	}

	if resources.Gpu != nil {
		gpuStr := fmt.Sprintf("%d", resources.Gpu.Number)
		gpuQuantity, err := resource.ParseQuantity(gpuStr)
		if err != nil {
			log.Warnf("error parsing gpu string: %s: %+v", gpuStr, err)
		} else {
			result[v1.ResourceName(resources.Gpu.Type)] = gpuQuantity
		}
	}

	return &result, nil
}

// GetLimitsResourceList returns a list of resources from a pipeline
// ResourceSpec that it is maximally limited to.
func GetLimitsResourceList(limits *pps.ResourceSpec) (*v1.ResourceList, error) {
	return getResourceListFromSpec(limits)
}

// GetPipelineInfoAllowIncomplete retrieves and returns a PipelineInfo from PFS,
// or a sparsely-populated PipelineInfo if the spec data cannot be found in PPS
// (e.g. due to corruption or a missing block). It does the PFS
// read/unmarshalling of bytes as well as filling in missing fields
func GetPipelineInfoAllowIncomplete(pachClient *client.APIClient, ptr *pps.StoredPipelineInfo) (*pps.PipelineInfo, error) {
	result := &pps.PipelineInfo{}
	buf := bytes.Buffer{}
	if err := pachClient.GetFile(ptr.SpecCommit, ppsconsts.SpecFile, &buf); err != nil {
		log.Error(errors.Wrapf(err, "could not read existing PipelineInfo from PFS"))
	} else {
		if err := result.Unmarshal(buf.Bytes()); err != nil {
			return nil, errors.Wrapf(err, "could not unmarshal PipelineInfo bytes from PFS")
		}
	}

	if result.Pipeline == nil {
		result.Pipeline = ptr.Pipeline
	}
	result.State = ptr.State
	result.Reason = ptr.Reason
	result.JobCounts = ptr.JobCounts
	result.LastJobState = ptr.LastJobState
	result.SpecCommit = ptr.SpecCommit
	return result, nil
}

// GetPipelineInfo retrieves and returns a valid PipelineInfo from PFS. It does
// the PFS read/unmarshalling of bytes as well as filling in missing fields
func GetPipelineInfo(pachClient *client.APIClient, ptr *pps.StoredPipelineInfo) (*pps.PipelineInfo, error) {
	result, err := GetPipelineInfoAllowIncomplete(pachClient, ptr)
	if err == nil && result.Transform == nil {
		return nil, errors.Wrapf(err, "could not retrieve pipeline spec file from PFS for pipeline '%s', there may be a problem reaching object storage, or the pipeline may need to be deleted and recreated", result.Pipeline.Name)
	}
	return result, err
}

// FailPipeline updates the pipeline's state to failed and sets the failure reason
func FailPipeline(ctx context.Context, db *sqlx.DB, pipelinesCollection col.PostgresCollection, pipelineName string, reason string) error {
	return SetPipelineState(ctx, db, pipelinesCollection, pipelineName,
		nil, pps.PipelineState_PIPELINE_FAILURE, reason)
}

// CrashingPipeline updates the pipeline's state to crashing and sets the reason
func CrashingPipeline(ctx context.Context, db *sqlx.DB, pipelinesCollection col.PostgresCollection, pipelineName string, reason string) error {
	return SetPipelineState(ctx, db, pipelinesCollection, pipelineName,
		nil, pps.PipelineState_PIPELINE_CRASHING, reason)
}

// PipelineTransitionError represents an error transitioning a pipeline from
// one state to another.
type PipelineTransitionError struct {
	Pipeline        string
	Expected        []pps.PipelineState
	Target, Current pps.PipelineState
}

func (p PipelineTransitionError) Error() string {
	var froms bytes.Buffer
	for i, state := range p.Expected {
		if i > 0 {
			froms.WriteString(", ")
		}
		froms.WriteString(state.String())
	}
	return fmt.Sprintf("could not transition %q from any of [%s] -> %s, as it is in %s",
		p.Pipeline, froms.String(), p.Target, p.Current)
}

// SetPipelineState does a lot of conditional logging, and converts 'from' and
// 'to' to strings, so the construction of its log message is factored into this
// helper.
func logSetPipelineState(pipeline string, from []pps.PipelineState, to pps.PipelineState, reason string) {
	var logMsg strings.Builder
	logMsg.Grow(300) // approx. max length of this log msg if len(from) <= ~2
	logMsg.WriteString("SetPipelineState attempting to move \"")
	logMsg.WriteString(pipeline)
	logMsg.WriteString("\" ")
	if len(from) > 0 {
		logMsg.WriteString("from one of {")
		logMsg.WriteString(from[0].String())
		for _, s := range from[1:] {
			logMsg.WriteByte(',')
			logMsg.WriteString(s.String())
		}
		logMsg.WriteString("} ")
	}
	logMsg.WriteString("to ")
	logMsg.WriteString(to.String())
	if reason != "" {
		logMsg.WriteString(" (reason: \"")
		logMsg.WriteString(reason)
		logMsg.WriteString("\")")
	}
	log.Info(logMsg.String())
}

// SetPipelineState is a helper that moves the state of 'pipeline' from any of
// the states in 'from' (if not nil) to 'to'. It will annotate any trace in
// 'ctx' with information about 'pipeline' that it reads.
//
// This function logs a lot for a library function, but it's mostly (maybe
// exclusively?) called by the PPS master
func SetPipelineState(ctx context.Context, db *sqlx.DB, pipelinesCollection col.PostgresCollection, pipeline string, from []pps.PipelineState, to pps.PipelineState, reason string) (retErr error) {
	logSetPipelineState(pipeline, from, to, reason)
	err := col.NewSQLTx(ctx, db, func(sqlTx *sqlx.Tx) error {
		pipelines := pipelinesCollection.ReadWrite(sqlTx)
		pipelinePtr := &pps.StoredPipelineInfo{}
		if err := pipelines.Get(pipeline, pipelinePtr); err != nil {
			return err
		}
		tracing.TagAnySpan(ctx, "old-state", pipelinePtr.State)
		// Only UpdatePipeline can bring a pipeline out of failure
		// TODO(msteffen): apply the same logic for CRASHING?
		if pipelinePtr.State == pps.PipelineState_PIPELINE_FAILURE {
			if to != pps.PipelineState_PIPELINE_FAILURE {
				log.Warningf("cannot move pipeline %q to %s when it is already in FAILURE", pipeline, to)
			}
			return nil
		}
		// Don't allow a transition from STANDBY to CRASHING if we receive events out of order
		if pipelinePtr.State == pps.PipelineState_PIPELINE_STANDBY && to == pps.PipelineState_PIPELINE_CRASHING {
			log.Warningf("cannot move pipeline %q to CRASHING when it is in STANDBY", pipeline)
			return nil
		}

		// transitionPipelineState case: error if pipeline is in an unexpected
		// state.
		//
		// allow transitionPipelineState to send a pipeline state to its target
		// repeatedly (thus pipelinePtr.State == to yields no error). This will
		// trigger additional etcd write events, but will not trigger an error.
		if len(from) > 0 {
			var isInFromState bool
			for _, fromState := range from {
				if pipelinePtr.State == fromState {
					isInFromState = true
					break
				}
			}
			if !isInFromState && pipelinePtr.State != to {
				return PipelineTransitionError{
					Pipeline: pipeline,
					Expected: from,
					Target:   to,
					Current:  pipelinePtr.State,
				}
			}
		}
		log.Infof("SetPipelineState moving pipeline %s from %s to %s", pipeline, pipelinePtr.State, to)
		pipelinePtr.State = to
		pipelinePtr.Reason = reason
		return pipelines.Put(pipeline, pipelinePtr)
	})
	return err
}

// PipelineJobInput fills in the commits for an Input
func PipelineJobInput(pipelineInfo *pps.PipelineInfo, outputCommitInfo *pfs.CommitInfo) *pps.Input {
	// branchToCommit maps strings of the form "<repo>/<branch>" to PFS commits
	branchToCommit := make(map[string]*pfs.Commit)
	key := pfsdb.BranchKey
	// for a given branch, the commit assigned to it will be the latest commit on that branch
	// this is ensured by the way we sort the commit provenance when creating the outputCommit
	for _, prov := range outputCommitInfo.Provenance {
		branchToCommit[key(prov.Commit.Branch)] = prov.Commit
	}
	jobInput := proto.Clone(pipelineInfo.Input).(*pps.Input)
	pps.VisitInput(jobInput, func(input *pps.Input) error {
		if input.Pfs != nil {
			pfsBranch := client.NewSystemRepo(input.Pfs.Repo, input.Pfs.RepoType).NewBranch(input.Pfs.Branch)
			if commit, ok := branchToCommit[key(pfsBranch)]; ok {
				input.Pfs.Commit = commit.ID
			}
		}
		if input.Cron != nil {
			if commit, ok := branchToCommit[key(client.NewBranch(input.Cron.Repo, "master"))]; ok {
				input.Cron.Commit = commit.ID
			}
		}
		if input.Git != nil {
			if commit, ok := branchToCommit[key(client.NewBranch(input.Git.Name, input.Git.Branch))]; ok {
				input.Git.Commit = commit.ID
			}
		}
		return nil
	})
	return jobInput
}

// PipelineReqFromInfo converts a PipelineInfo into a CreatePipelineRequest.
func PipelineReqFromInfo(pipelineInfo *pps.PipelineInfo) *pps.CreatePipelineRequest {
	return &pps.CreatePipelineRequest{
		Pipeline:              pipelineInfo.Pipeline,
		Transform:             pipelineInfo.Transform,
		ParallelismSpec:       pipelineInfo.ParallelismSpec,
		Egress:                pipelineInfo.Egress,
		OutputBranch:          pipelineInfo.OutputBranch,
		ResourceRequests:      pipelineInfo.ResourceRequests,
		ResourceLimits:        pipelineInfo.ResourceLimits,
		SidecarResourceLimits: pipelineInfo.SidecarResourceLimits,
		Input:                 pipelineInfo.Input,
		Description:           pipelineInfo.Description,
		CacheSize:             pipelineInfo.CacheSize,
		EnableStats:           pipelineInfo.EnableStats,
		MaxQueueSize:          pipelineInfo.MaxQueueSize,
		Service:               pipelineInfo.Service,
		ChunkSpec:             pipelineInfo.ChunkSpec,
		DatumTimeout:          pipelineInfo.DatumTimeout,
		JobTimeout:            pipelineInfo.JobTimeout,
		Salt:                  pipelineInfo.Salt,
		PodSpec:               pipelineInfo.PodSpec,
		PodPatch:              pipelineInfo.PodPatch,
		Spout:                 pipelineInfo.Spout,
		SchedulingSpec:        pipelineInfo.SchedulingSpec,
		DatumTries:            pipelineInfo.DatumTries,
		Standby:               pipelineInfo.Standby,
		S3Out:                 pipelineInfo.S3Out,
		Metadata:              pipelineInfo.Metadata,
		ReprocessSpec:         pipelineInfo.ReprocessSpec,
	}
}

// IsTerminal returns 'true' if 'state' indicates that the job is done (i.e.
// the state will not change later: SUCCESS, FAILURE, KILLED) and 'false'
// otherwise.
func IsTerminal(state pps.PipelineJobState) bool {
	switch state {
	case pps.PipelineJobState_JOB_SUCCESS, pps.PipelineJobState_JOB_FAILURE, pps.PipelineJobState_JOB_KILLED:
		return true
	case pps.PipelineJobState_JOB_STARTING, pps.PipelineJobState_JOB_RUNNING, pps.PipelineJobState_JOB_EGRESSING:
		return false
	default:
		panic(fmt.Sprintf("unrecognized job state: %s", state))
	}
}

// UpdatePipelineJobState performs the operations involved with a job state transition.
func UpdatePipelineJobState(pipelines col.ReadWriteCollection, pipelineJobs col.ReadWriteCollection, pipelineJobPtr *pps.StoredPipelineJobInfo, state pps.PipelineJobState, reason string) error {
	if IsTerminal(pipelineJobPtr.State) {
		return ppsServer.ErrPipelineJobFinished{pipelineJobPtr.PipelineJob}
	}

	// Update pipeline
	pipelinePtr := &pps.StoredPipelineInfo{}
	if err := pipelines.Get(pipelineJobPtr.Pipeline.Name, pipelinePtr); err != nil {
		return err
	}
	if pipelinePtr.JobCounts == nil {
		pipelinePtr.JobCounts = make(map[int32]int32)
	}
	if pipelinePtr.JobCounts[int32(pipelineJobPtr.State)] != 0 {
		pipelinePtr.JobCounts[int32(pipelineJobPtr.State)]--
	}
	pipelinePtr.JobCounts[int32(state)]++
	pipelinePtr.LastJobState = state
	if err := pipelines.Put(pipelineJobPtr.Pipeline.Name, pipelinePtr); err != nil {
		return err
	}

	// Update job info
	var err error
	if state == pps.PipelineJobState_JOB_STARTING {
		pipelineJobPtr.Started, err = types.TimestampProto(time.Now())
	} else if IsTerminal(state) {
		pipelineJobPtr.Finished, err = types.TimestampProto(time.Now())
	}
	if err != nil {
		return err
	}
	pipelineJobPtr.State = state
	pipelineJobPtr.Reason = reason
	return pipelineJobs.Put(pipelineJobPtr.PipelineJob.ID, pipelineJobPtr)
}

func FinishPipelineJob(pachClient *client.APIClient, pipelineJobInfo *pps.PipelineJobInfo, state pps.PipelineJobState, reason string) error {
	pipelineJobInfo.State = state
	pipelineJobInfo.Reason = reason
	var empty bool
	if state == pps.PipelineJobState_JOB_FAILURE || state == pps.PipelineJobState_JOB_KILLED {
		empty = true
	}
	_, err := pachClient.RunBatchInTransaction(func(builder *client.TransactionBuilder) error {
		if _, err := builder.PfsAPIClient.FinishCommit(pachClient.Ctx(), &pfs.FinishCommitRequest{
			Commit: pipelineJobInfo.OutputCommit,
			Empty:  empty,
		}); err != nil {
			return err
		}
		if _, err := builder.PfsAPIClient.FinishCommit(pachClient.Ctx(), &pfs.FinishCommitRequest{
			Commit: pipelineJobInfo.StatsCommit,
			Empty:  empty,
		}); err != nil {
			return err
		}
		return WriteJobInfo(&builder.APIClient, pipelineJobInfo)
	})
	return err
}

func WriteJobInfo(pachClient *client.APIClient, pipelineJobInfo *pps.PipelineJobInfo) error {
	_, err := pachClient.PpsAPIClient.UpdatePipelineJobState(pachClient.Ctx(), &pps.UpdatePipelineJobStateRequest{
		PipelineJob:   pipelineJobInfo.PipelineJob,
		State:         pipelineJobInfo.State,
		Reason:        pipelineJobInfo.Reason,
		Restart:       pipelineJobInfo.Restart,
		DataProcessed: pipelineJobInfo.DataProcessed,
		DataSkipped:   pipelineJobInfo.DataSkipped,
		DataTotal:     pipelineJobInfo.DataTotal,
		DataFailed:    pipelineJobInfo.DataFailed,
		DataRecovered: pipelineJobInfo.DataRecovered,
		Stats:         pipelineJobInfo.Stats,
	})
	return err
}

func GetStatsCommit(commitInfo *pfs.CommitInfo) *pfs.Commit {
	outputRepo := commitInfo.Commit.Branch.Repo.Name
	for _, commitRange := range commitInfo.Subvenance {
		repo := commitRange.Lower.Branch.Repo
		if repo.Type == pfs.MetaRepoType && repo.Name == outputRepo {
			return commitRange.Lower
		}
	}
	// TODO: Getting here would be a bug in 2.0, log?
	return nil
}

// ContainsS3Inputs returns 'true' if 'in' is or contains any PFS inputs with
// 'S3' set to true. Any pipelines with s3 inputs lj
func ContainsS3Inputs(in *pps.Input) bool {
	var found bool
	pps.VisitInput(in, func(in *pps.Input) error {
		if in.Pfs != nil && in.Pfs.S3 {
			found = true
			return errutil.ErrBreak
		}
		return nil
	})
	return found
}

// SidecarS3GatewayService returns the name of the kubernetes service created
// for the job 'pipelineJobID' to hand sidecar s3 gateway requests. This helper
// is in ppsutil because both PPS (which creates the service, in the s3 gateway
// sidecar server) and the worker (which passes the endpoint to the user code)
// need to know it.
func SidecarS3GatewayService(pipelineJobID string) string {
	return "s3-" + pipelineJobID
}

// ErrorState returns true if s is an error state for a pipeline, that is, a
// state that users should be aware of and one which will have a "Reason" set
// for why it's in this state.
func ErrorState(s pps.PipelineState) bool {
	return map[pps.PipelineState]bool{
		pps.PipelineState_PIPELINE_FAILURE:    true,
		pps.PipelineState_PIPELINE_CRASHING:   true,
		pps.PipelineState_PIPELINE_RESTARTING: true,
	}[s]
}

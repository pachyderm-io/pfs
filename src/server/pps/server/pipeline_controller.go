package server

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/pachyderm/pachyderm/v2/src/auth"
	"github.com/pachyderm/pachyderm/v2/src/client"
	"github.com/pachyderm/pachyderm/v2/src/internal/backoff"
	"github.com/pachyderm/pachyderm/v2/src/internal/errors"
	"github.com/pachyderm/pachyderm/v2/src/internal/errutil"
	"github.com/pachyderm/pachyderm/v2/src/internal/ppsutil"
	"github.com/pachyderm/pachyderm/v2/src/internal/tracing"
	"github.com/pachyderm/pachyderm/v2/src/internal/tracing/extended"
	"github.com/pachyderm/pachyderm/v2/src/pfs"
	"github.com/pachyderm/pachyderm/v2/src/pps"
	"github.com/pachyderm/pachyderm/v2/src/server/pfs/pretty"
	"github.com/pachyderm/pachyderm/v2/src/version"

	opentracing "github.com/opentracing/opentracing-go"
	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type rcExpectation byte

const (
	noExpectation rcExpectation = iota
	noRCExpected
	rcExpected
)

func max(is ...int) int {
	if len(is) == 0 {
		return 0
	}
	max := is[0]
	for _, i := range is {
		if i > max {
			max = i
		}
	}
	return max
}

// pipelineOp contains all of the relevent current state for a pipeline. It's
// used by step() to take any necessary actions
type pipelineOp struct {
	m *ppsMaster
	// a pachyderm client wrapping this operation's context (child of the PPS
	// master's context, and cancelled at the end of step())
	ctx          context.Context
	pipelineInfo *pps.PipelineInfo
	rc           *v1.ReplicationController
}

var (
	errRCNotFound   = errors.New("RC not found")
	errUnexpectedRC = errors.New("unexpected RC")
	errTooManyRCs   = errors.New("multiple RCs found for pipeline")
	errStaleRC      = errors.New("RC doesn't match pipeline version (likely stale)")
)

// step takes 'pipelineInfo', a newly-changed pipeline pointer in the pipeline collection, and
// 1. retrieves its full pipeline spec and RC into the 'Details' field
// 2. makes whatever changes are needed to bring the RC in line with the (new) spec
// 3. updates 'pipelineInfo', if needed, to reflect the action it just took
func (m *ppsMaster) step(pipeline string, keyVer, keyRev int64) (retErr error) {
	log.Infof("PPS master: processing event for %q", pipeline)

	// Initialize op ctx (cancelled at the end of step(), to avoid leaking
	// resources), whereas masterClient is passed by the
	// PPS master and used in case a monitor needs to be spawned for 'pipeline',
	// whose lifetime is tied to the master rather than this op.
	opCtx, cancel := context.WithCancel(m.masterCtx)
	defer cancel()

	// Handle tracing
	span, opCtx := extended.AddSpanToAnyPipelineTrace(opCtx,
		m.a.env.GetEtcdClient(), pipeline, "/pps.Master/ProcessPipelineUpdate")
	if keyVer != 0 || keyRev != 0 {
		tracing.TagAnySpan(span, "key-version", keyVer, "mod-revision", keyRev)
	} else {
		tracing.TagAnySpan(span, "pollpipelines-event", "true")
	}
	defer func() {
		tracing.FinishAnySpan(span, "err", retErr)
	}()

	// Retrieve pipelineInfo from the spec repo
	op, err := m.newPipelineOp(opCtx, pipeline)
	if err != nil {
		// Don't put the pipeline in a failing state if we're in the middle
		// of activating auth, retry in a bit
		if auth.IsErrNotAuthorized(err) || auth.IsErrNotSignedIn(err) {
			return stepError{
				error:        errors.Wrap(err, "couldn't initialize pipeline op"),
				failPipeline: false,
			}
		}

		// otherwise fail immediately without retry
		return stepError{
			error:        errors.Wrap(err, "couldn't initialize pipeline op"),
			failPipeline: true,
		}
	}
	// set op.rc
	// TODO(msteffen) should this fail the pipeline? (currently getRC will restart
	// the pipeline indefinitely)
	if err := op.getRC(noExpectation); err != nil && !errors.Is(err, errRCNotFound) {
		return err
	}

	// Process the pipeline event
	return op.run()
}

func (m *ppsMaster) newPipelineOp(ctx context.Context, pipeline string) (*pipelineOp, error) {
	op := &pipelineOp{
		m:            m,
		ctx:          ctx,
		pipelineInfo: &pps.PipelineInfo{},
	}
	// get latest PipelineInfo (events can pile up, so that the current state
	// doesn't match the event being processed)
	if err := m.a.pipelines.ReadOnly(ctx).Get(pipeline, op.pipelineInfo); err != nil {
		return nil, errors.Wrapf(err, "could not retrieve pipeline info for %q", pipeline)
	}
	tracing.TagAnySpan(ctx,
		"current-state", op.pipelineInfo.State.String(),
		"spec-commit", pretty.CompactPrintCommitSafe(op.pipelineInfo.SpecCommit))
	// set op.pipelineInfo.Details

	// this reads the pipelineInfo associated with 'op's pipeline, as most other
	// methods (e.g.  getRC, though not failPipeline) assume that
	// op.pipelineInfo.Details is set.
	pachClient := op.m.a.env.GetPachClient(op.ctx)
	if err := ppsutil.GetPipelineDetails(pachClient, op.pipelineInfo); err != nil {
		return nil, newRetriableError(err, "error retrieving spec")
	}
	return op, nil
}

func (op *pipelineOp) run() error {
	// Bring 'pipeline' into the correct state by taking appropriate action
	switch op.pipelineInfo.State {
	case pps.PipelineState_PIPELINE_STARTING, pps.PipelineState_PIPELINE_RESTARTING:
		if op.rc != nil && !op.rcIsFresh() {
			// old RC is not down yet
			return op.restartPipeline("stale RC") // step() will be called again after collection write
		} else if op.rc == nil {
			// default: old RC (if any) is down but new RC is not up yet
			if err := op.createPipelineResources(); err != nil {
				return err
			}
		}
		if op.pipelineInfo.Stopped {
			return op.setPipelineState(pps.PipelineState_PIPELINE_PAUSED, "")
		}
		// trigger another event
		op.stopCrashingPipelineMonitor()
		return op.setPipelineState(pps.PipelineState_PIPELINE_RUNNING, "")
	case pps.PipelineState_PIPELINE_RUNNING:
		if !op.rcIsFresh() {
			return op.restartPipeline("stale RC") // step() will be called again after collection write
		}
		if op.pipelineInfo.Stopped {
			return op.setPipelineState(pps.PipelineState_PIPELINE_PAUSED, "")
		}

		op.stopCrashingPipelineMonitor()
		op.startPipelineMonitor()
		// default: scale up if pipeline start hasn't propagated to the collection yet
		// Note: mostly this should do nothing, as this runs several times per job
		return op.scaleUpPipeline()
	case pps.PipelineState_PIPELINE_STANDBY:
		if !op.rcIsFresh() {
			return op.restartPipeline("stale RC") // step() will be called again after collection write
		}
		if op.pipelineInfo.Stopped {
			return op.setPipelineState(pps.PipelineState_PIPELINE_PAUSED, "")
		}

		op.stopCrashingPipelineMonitor()
		// Make sure pipelineMonitor is running to pull it out of standby
		op.startPipelineMonitor()
		// default: scale down if standby hasn't propagated to kube RC yet
		return op.scaleDownPipeline()
	case pps.PipelineState_PIPELINE_PAUSED:
		if !op.rcIsFresh() {
			return op.restartPipeline("stale RC") // step() will be called again after collection write
		}
		if !op.pipelineInfo.Stopped {
			// StartPipeline has been called (so spec commit is updated), but new spec
			// commit hasn't been propagated to PipelineInfo or RC yet
			if err := op.scaleUpPipeline(); err != nil {
				return err
			}
			return op.setPipelineState(pps.PipelineState_PIPELINE_RUNNING, "")
		}
		// don't want cron commits or STANDBY state changes while pipeline is
		// stopped
		op.stopPipelineMonitor()
		op.stopCrashingPipelineMonitor()
		// default: scale down if pause/standby hasn't propagated to collection yet
		return op.scaleDownPipeline()
	case pps.PipelineState_PIPELINE_FAILURE:
		// pipeline fails if it encounters an unrecoverable error
		if err := op.finishPipelineOutputCommits(); err != nil {
			return err
		}
		// deletePipelineResources calls cancelMonitor() and cancelCrashingMonitor()
		// in addition to deleting the RC, so those calls aren't necessary here.
		if err := op.deletePipelineResources(); err != nil {
			// retry, but the pipeline has already failed
			return stepError{
				error: errors.Wrap(err, "error deleting resources for failing pipeline"),
				retry: true,
			}
		}
		return nil
	case pps.PipelineState_PIPELINE_CRASHING:
		if !op.rcIsFresh() {
			return op.restartPipeline("stale RC") // step() will be called again after collection write
		}
		if op.pipelineInfo.Stopped {
			return op.setPipelineState(pps.PipelineState_PIPELINE_PAUSED, "")
		}
		// start a monitor to poll k8s and update us when it goes into a running state
		op.startPipelineMonitor()
		op.startCrashingPipelineMonitor()
		// Surprisingly, scaleUpPipeline() is necessary, in case a pipelines is
		// quickly transitioned to CRASHING after coming out of STANDBY. Because the
		// pipeline controller reads the current state of the pipeline after each
		// event (to avoid getting backlogged), it might never actually see the
		// pipeline in RUNNING. However, if the RC is never scaled up, the pipeline
		// can never come out of CRASHING, so do it here in case it never happened.
		//
		// In general, CRASHING is actually almost identical to RUNNING (except for
		// the monitorCrashing goro)
		return op.scaleUpPipeline()
	}
	return nil
}

// getRC reads the RC associated with 'op's pipeline. op.pipelineInfo must be
// set already. 'expectation' indicates whether the PPS master expects an RC to
// exist--if set to 'rcExpected', getRC will restart the pipeline if no RC is
// found after three retries. If set to 'noRCExpected', then getRC will return
// after the first "not found" error. If set to noExpectation, then getRC will
// retry the kubeclient.List() RPC, but will not restart the pipeline if no RC
// is found
//
// Unlike other functions in this file, getRC takes responsibility for restarting
// op's pipeline if it can't read the pipeline's RC (or if the RC is stale or
// redundant), and then returns an error to the caller to indicate that the
// caller shouldn't continue with other operations
func (op *pipelineOp) getRC(expectation rcExpectation) (retErr error) {
	span, _ := tracing.AddSpanToAnyExisting(op.ctx,
		"/pps.Master/GetRC", "pipeline", op.pipelineInfo.Pipeline.Name)
	defer func(span opentracing.Span) {
		tracing.TagAnySpan(span, "err", fmt.Sprintf("%v", retErr))
		tracing.FinishAnySpan(span)
	}(span)

	kubeClient := op.m.a.env.GetKubeClient()
	namespace := op.m.a.namespace
	selector := fmt.Sprintf("%s=%s", pipelineNameLabel, op.pipelineInfo.Pipeline.Name)

	// count error types separately, so that this only errors if the pipeline is
	// stuck and not changing
	var notFoundErrCount, unexpectedErrCount, staleErrCount, tooManyErrCount,
		otherErrCount int
	return backoff.RetryNotify(func() error {
		// List all RCs, so stale RCs from old pipelines are noticed and deleted
		rcs, err := kubeClient.CoreV1().ReplicationControllers(namespace).List(
			metav1.ListOptions{LabelSelector: selector})
		if err != nil && !errutil.IsNotFoundError(err) {
			return err
		}
		if len(rcs.Items) == 0 {
			op.rc = nil
			return errRCNotFound
		}

		op.rc = &rcs.Items[0]
		switch {
		case len(rcs.Items) > 1:
			// select stale RC if possible, so that we delete it in restartPipeline
			for i := range rcs.Items {
				op.rc = &rcs.Items[i]
				if !op.rcIsFresh() {
					break
				}
			}
			return errTooManyRCs
		case !op.rcIsFresh():
			return errStaleRC
		case expectation == noRCExpected:
			return errUnexpectedRC
		default:
			return nil
		}
	}, backoff.NewInfiniteBackOff(), func(err error, d time.Duration) error {
		if expectation == noRCExpected && errors.Is(err, errRCNotFound) {
			return err // rc has come down successfully--no need to keep looking
		}
		switch {
		case errors.Is(err, errRCNotFound):
			notFoundErrCount++
		case errors.Is(err, errUnexpectedRC):
			unexpectedErrCount++
		case errors.Is(err, errTooManyRCs):
			tooManyErrCount++
		case errors.Is(err, errStaleRC):
			staleErrCount++ // don't return immediately b/c RC might be changing
		default:
			otherErrCount++
		}
		errCount := max(notFoundErrCount, unexpectedErrCount, staleErrCount,
			tooManyErrCount, otherErrCount)
		if errCount >= maxErrCount {
			missingExpectedRC := expectation == rcExpected && errors.Is(err, errRCNotFound)
			invalidRCState := errors.Is(err, errTooManyRCs) || errors.Is(err, errStaleRC)
			if missingExpectedRC || invalidRCState {
				return op.restartPipeline(fmt.Sprintf("could not get RC after %d attempts: %v", errCount, err))
			}
			return err //return whatever the most recent error was
		}
		log.Errorf("PPS master: error retrieving RC for %q: %v; retrying in %v", op.pipelineInfo.Pipeline.Name, err, d)
		return nil
	})
}

// rcIsFresh returns a boolean indicating whether op.rc has the right labels
// corresponding to op.pipelineInfo. If this returns false, it likely means the
// current RC is using e.g. an old spec commit or something.
func (op *pipelineOp) rcIsFresh() bool {
	if op.rc == nil {
		log.Errorf("PPS master: RC for %q is nil", op.pipelineInfo.Pipeline.Name)
		return false
	}
	expectedName := ""
	if op.pipelineInfo != nil {
		expectedName = ppsutil.PipelineRcName(op.pipelineInfo.Pipeline.Name, op.pipelineInfo.Version)
	}

	// establish current RC properties
	rcName := op.rc.ObjectMeta.Name
	rcPachVersion := op.rc.ObjectMeta.Annotations[pachVersionAnnotation]
	rcAuthTokenHash := op.rc.ObjectMeta.Annotations[hashedAuthTokenAnnotation]
	rcPipelineVersion := op.rc.ObjectMeta.Annotations[pipelineVersionAnnotation]
	switch {
	case rcAuthTokenHash != hashAuthToken(op.pipelineInfo.AuthToken):
		log.Errorf("PPS master: auth token in %q is stale %s != %s",
			op.pipelineInfo.Pipeline.Name, rcAuthTokenHash, hashAuthToken(op.pipelineInfo.AuthToken))
		return false
	case rcPipelineVersion != strconv.FormatUint(op.pipelineInfo.Version, 10):
		log.Errorf("PPS master: pipeline version in %q looks stale %s != %d",
			op.pipelineInfo.Pipeline.Name, rcPipelineVersion, op.pipelineInfo.Version)
		return false
	case rcPachVersion != version.PrettyVersion():
		log.Errorf("PPS master: %q is using stale pachd v%s != current v%s",
			op.pipelineInfo.Pipeline.Name, rcPachVersion, version.PrettyVersion())
		return false
	case expectedName != "" && rcName != expectedName:
		log.Errorf("PPS master: %q has an unexpected (likely stale) name %q != %q",
			op.pipelineInfo.Pipeline.Name, rcName, expectedName)
	}
	return true
}

// setPipelineState set's op's state in the collection to 'state'. This will trigger a
// collection watch event and cause step() to eventually run again.
func (op *pipelineOp) setPipelineState(state pps.PipelineState, reason string) error {
	if err := op.m.a.setPipelineState(op.ctx,
		op.pipelineInfo.Pipeline.Name, state, reason); err != nil {
		// don't bother failing if we can't set the state
		return stepError{
			error: errors.Wrapf(err, "could not set pipeline state to %v"+
				"(you may need to restart pachd to un-stick the pipeline)", state),
			retry: true,
		}
	}
	return nil
}

// createPipelineResources creates the RC and any services for op's pipeline.
func (op *pipelineOp) createPipelineResources() error {
	log.Infof("PPS master: creating resources for pipeline %q", op.pipelineInfo.Pipeline.Name)
	if err := op.m.a.createWorkerSvcAndRc(op.ctx, op.pipelineInfo); err != nil {
		if errors.As(err, &noValidOptionsErr{}) {
			// these errors indicate invalid pipelineInfo, don't retry
			return stepError{
				error:        errors.Wrap(err, "could not generate RC options"),
				failPipeline: true,
			}
		}
		return newRetriableError(err, "error creating resources")
	}
	return nil
}

// startPipelineMonitor spawns a monitorPipeline() goro for this pipeline (if
// one doesn't exist already), which manages standby and cron inputs, and
// updates the the pipeline state.
// Note: this is called by every run through step(), so must be idempotent
func (op *pipelineOp) startPipelineMonitor() {
	op.stopCrashingPipelineMonitor()
	op.m.startMonitor(op.pipelineInfo)
	op.pipelineInfo.Details.WorkerRc = op.rc.ObjectMeta.Name
}

func (op *pipelineOp) startCrashingPipelineMonitor() {
	op.m.startCrashingMonitor(op.pipelineInfo)
}

func (op *pipelineOp) stopPipelineMonitor() {
	op.m.cancelMonitor(op.pipelineInfo.Pipeline.Name)
}

func (op *pipelineOp) stopCrashingPipelineMonitor() {
	op.m.cancelCrashingMonitor(op.pipelineInfo.Pipeline.Name)
}

// finishPipelineOutputCommits finishes any output commits of
// 'pipelineInfo.Pipeline' with an empty tree.
// TODO(msteffen) Note that if the pipeline has any jobs (which can happen if
// the user manually deletes the pipeline's RC, failing the pipeline, after it
// has created jobs) those will not be updated, but they should be FAILED
//
// Unlike other functions in this file, finishPipelineOutputCommits doesn't
// cause retries if it encounters an error. Currently. it's only called by step()
// in the case where op's pipeline is already in FAILURE. If it returns an error in
// that case, the pps master will log the error and move on to the next pipeline
// event. This pipeline's output commits will stay open until another watch
// event arrives for the pipeline and finishPipelineOutputCommits is retried.
func (op *pipelineOp) finishPipelineOutputCommits() (retErr error) {
	log.Infof("PPS master: finishing output commits for pipeline %q", op.pipelineInfo.Pipeline.Name)

	pachClient := op.m.a.env.GetPachClient(op.ctx)
	if span, _ctx := tracing.AddSpanToAnyExisting(op.ctx,
		"/pps.Master/FinishPipelineOutputCommits", "pipeline", op.pipelineInfo.Pipeline.Name); span != nil {
		pachClient = pachClient.WithCtx(_ctx) // copy span back into pachClient
		defer func() {
			tracing.TagAnySpan(span, "err", fmt.Sprintf("%v", retErr))
			tracing.FinishAnySpan(span)
		}()
	}
	pachClient.SetAuthToken(op.pipelineInfo.AuthToken)

	if err := pachClient.ListCommitF(client.NewRepo(op.pipelineInfo.Pipeline.Name), client.NewCommit(op.pipelineInfo.Pipeline.Name, op.pipelineInfo.Details.OutputBranch, ""), nil, 0, false, func(commitInfo *pfs.CommitInfo) error {
		return pachClient.StopJob(op.pipelineInfo.Pipeline.Name, commitInfo.Commit.ID)
	}); err != nil {
		if errutil.IsNotFoundError(err) {
			return nil // already deleted
		}
		return errors.Wrapf(err, "could not finish output commits of pipeline %q", op.pipelineInfo.Pipeline.Name)
	}
	return nil
}

// deletePipelineResources deletes the RC and services associated with op's
// pipeline. It doesn't return a stepError, leaving retry behavior to the caller
func (op *pipelineOp) deletePipelineResources() error {
	if err := op.m.deletePipelineResources(op.pipelineInfo.Pipeline.Name); err != nil {
		return err
	}
	return nil
}

// updateRC is a helper for {scaleUp,scaleDown}Pipeline. It includes all of the
// logic for writing an updated RC spec to kubernetes, and updating/retrying if
// k8s rejects the write. It presents a strange API, since the the RC being
// updated is already available to the caller in op.rc, but update() may be
// called muliple times if the k8s write fails. It may be helpful to think of
// the rc passed to update() as mutable, while op.rc is immutable.
func (op *pipelineOp) updateRC(update func(rc *v1.ReplicationController)) error {
	kubeClient := op.m.a.env.GetKubeClient()
	namespace := op.m.a.namespace
	rc := kubeClient.CoreV1().ReplicationControllers(namespace)

	newRC := *op.rc
	// Apply op's update to rc
	update(&newRC)
	// write updated RC to k8s
	if _, err := rc.Update(&newRC); err != nil {
		return newRetriableError(err, "error updating RC")
	}
	return nil
}

// scaleUpPipeline edits the RC associated with op's pipeline & spins up the
// configured number of workers.
func (op *pipelineOp) scaleUpPipeline() (retErr error) {
	log.Infof("PPS master: scaling up workers for %q", op.pipelineInfo.Pipeline.Name)
	span, _ := tracing.AddSpanToAnyExisting(op.ctx,
		"/pps.Master/ScaleUpPipeline", "pipeline", op.pipelineInfo.Pipeline.Name)
	defer func() {
		if retErr != nil {
			log.Errorf("PPS master: error scaling up: %v", retErr)
		}
		tracing.TagAnySpan(span, "err", retErr)
		tracing.FinishAnySpan(span)
	}()

	// compute target pipeline parallelism
	parallelism := uint64(1)
	if op.pipelineInfo.Details.ParallelismSpec != nil {
		parallelism = op.pipelineInfo.Details.ParallelismSpec.Constant
	}

	// update pipeline RC
	return op.updateRC(func(rc *v1.ReplicationController) {
		if rc.Spec.Replicas != nil && *op.rc.Spec.Replicas > 0 {
			return // prior attempt succeeded
		}
		rc.Spec.Replicas = new(int32)
		if op.pipelineInfo.Details.Autoscaling {
			*rc.Spec.Replicas = 1
		} else {
			*rc.Spec.Replicas = int32(parallelism)
		}
	})
}

// scaleDownPipeline edits the RC associated with op's pipeline & spins down the
// configured number of workers.
func (op *pipelineOp) scaleDownPipeline() (retErr error) {
	log.Infof("PPS master: scaling down workers for %q", op.pipelineInfo.Pipeline.Name)
	span, _ := tracing.AddSpanToAnyExisting(op.ctx,
		"/pps.Master/ScaleDownPipeline", "pipeline", op.pipelineInfo.Pipeline.Name)
	defer func() {
		if retErr != nil {
			log.Errorf("PPS master: error scaling down: %v", retErr)
		}
		tracing.TagAnySpan(span, "err", retErr)
		tracing.FinishAnySpan(span)
	}()

	return op.updateRC(func(rc *v1.ReplicationController) {
		if rc.Spec.Replicas != nil && *op.rc.Spec.Replicas == 0 {
			return // prior attempt succeeded
		}
		rc.Spec.Replicas = &zero
	})
}

// restartPipeline updates the RC/service associated with op's pipeline, and
// then sets its state to RESTARTING. Note that restartPipeline only deletes
// op.rc if it's stale--a prior bug was that it would delete all of op's
// resources, and then get stuck in a loop deleting and recreating op's RC if
// the cluster was busy and the RC was taking too long to start.
//
// restartPipeline is an error-handling
// codepath, so it's guaranteed to return an error (typically wrapping 'reason',
// though if the restart process fails that error will take precendence) so that
// callers can use it like so:
//
// if errorState {
//   return op.restartPipeline("entered error state")
// }
func (op *pipelineOp) restartPipeline(reason string) error {
	if op.rc != nil && !op.rcIsFresh() {
		// delete old RC, monitorPipeline goro, and worker service
		if err := op.deletePipelineResources(); err != nil {
			return newRetriableError(err, "error deleting resources for restart")
		}
	}
	// create up-to-date RC
	if err := op.createPipelineResources(); err != nil {
		return errors.Wrap(err, "error creating resources for restart")
	}
	if err := op.setPipelineState(pps.PipelineState_PIPELINE_RESTARTING, ""); err != nil {
		return errors.Wrap(err, "error restarting pipeline")
	}

	return errors.Errorf("restarting pipeline %q: %s", op.pipelineInfo.Pipeline.Name, reason)
}

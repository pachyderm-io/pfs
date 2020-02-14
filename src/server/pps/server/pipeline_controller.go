package server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/pachyderm/pachyderm/src/client"

	opentracing "github.com/opentracing/opentracing-go"
	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/pachyderm/pachyderm/src/client/pfs"
	"github.com/pachyderm/pachyderm/src/client/pkg/tracing"
	"github.com/pachyderm/pachyderm/src/client/pkg/tracing/extended"
	"github.com/pachyderm/pachyderm/src/client/pps"
	"github.com/pachyderm/pachyderm/src/client/version"
	"github.com/pachyderm/pachyderm/src/server/pkg/backoff"
	"github.com/pachyderm/pachyderm/src/server/pkg/ppsutil"
)

const maxErrCount = 3 // gives all retried operations ~4.5s total to finish

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
	apiServer    *apiServer
	pachClient   *client.APIClient
	ptr          *pps.EtcdPipelineInfo
	name         string // also in pipelineInfo, but that may not be set initially
	pipelineInfo *pps.PipelineInfo
	rc           *v1.ReplicationController
}

var (
	errRCNotFound   = errors.New("RC not found")
	errUnexpectedRC = errors.New("unexpected RC")
	errTooManyRCs   = errors.New("multiple RCs found for pipeline")
	errStaleRC      = errors.New("RC doesn't match pipeline version (likely stale)")
)

// step takes 'ptr', a newly-changed pipeline pointer in etcd, and
// 1. retrieves its full pipeline spec and RC
// 2. makes whatever changes are needed to bring the RC in line with the (new) spec
// 3. updates 'ptr', if needed, to reflect the action it just took
func (a *apiServer) step(pachClient *client.APIClient, pipeline string, keyVer, keyRev int64) error {
	log.Infof("PPS master: processing event for %q", pipeline)

	// Retrieve pipelineInfo from the spec repo
	op, err := a.newPipelineOp(pachClient, pipeline)
	if err != nil {
		// op is nil, so can't use op.failPipeline
		return a.setPipelineFailure(pachClient.Ctx(), pipeline,
			fmt.Sprintf("couldn't initialize pipeline op: %v", err))
	}
	// set op.rc
	// TODO(msteffen) should this fail the pipeline? (currently getRC will restart
	// the pipeline indefinitely)
	if err := op.getRC(noExpectation); err != nil && err != errRCNotFound {
		return err
	}

	// Handle tracing
	span, ctx := extended.AddPipelineSpanToAnyTrace(pachClient.Ctx(),
		a.env.GetEtcdClient(), pipeline, "/pps.Master/ProcessPipelineUpdate",
		"key-version", keyVer,
		"mod-revision", keyRev,
		"state", op.ptr.State.String(),
		"spec-commit", op.ptr.SpecCommit)
	if span != nil {
		defer tracing.FinishAnySpan(span)
		pachClient = pachClient.WithCtx(ctx) //lint:ignore SA4006 pachClient is unused but we want the right one in scope in case someone uses it below in the future
	}

	// Bring 'pipeline' into the correct state by taking appropriate action
	switch op.ptr.State {
	case pps.PipelineState_PIPELINE_STARTING, pps.PipelineState_PIPELINE_RESTARTING:
		if op.rc != nil && !op.rcIsFresh() {
			// old RC is not down yet
			return op.restartPipeline("stale RC") // step() will be called again after etcd write
		} else if op.rc == nil {
			// default: old RC (if any) is down but new RC is not up yet
			if err := op.createPipelineResources(); err != nil {
				return err
			}
		}
		// trigger another event--once pipeline is RUNNING, step() will scale it up
		if op.ptr.Stopped {
			if err := op.setPipelineState(pps.PipelineState_PIPELINE_PAUSED); err != nil {
				return err
			}
		} else {
			if err := op.setPipelineState(pps.PipelineState_PIPELINE_RUNNING); err != nil {
				return err
			}
		}
	case pps.PipelineState_PIPELINE_RUNNING:
		if !op.rcIsFresh() {
			return op.restartPipeline("stale RC") // step() will be called again after etcd write
		}
		op.startPipelineMonitor()

		if op.ptr.Stopped {
			// StopPipeline has been called, but pipeline hasn't been paused yet
			if err := op.scaleDownPipeline(); err != nil {
				return err
			}
			return op.setPipelineState(pps.PipelineState_PIPELINE_PAUSED)
		}
		// default: scale up if pipeline start hasn't propagated to etcd yet
		// Note: mostly this should do nothing, as this runs several times per job
		return op.scaleUpPipeline()
	case pps.PipelineState_PIPELINE_STANDBY, pps.PipelineState_PIPELINE_PAUSED:
		if !op.rcIsFresh() {
			return op.restartPipeline("stale RC") // step() will be called again after etcd write
		}
		op.startPipelineMonitor()

		if op.ptr.State == pps.PipelineState_PIPELINE_PAUSED && !op.ptr.Stopped {
			// StartPipeline has been called, but pipeline hasn't been started yet
			if err := op.scaleUpPipeline(); err != nil {
				return err
			}
			return op.setPipelineState(pps.PipelineState_PIPELINE_RUNNING)
		}
		// default: scale down if pause/standby hasn't propagated to etcd yet
		return op.scaleDownPipeline()
	case pps.PipelineState_PIPELINE_FAILURE:
		// pipeline fails if docker image isn't found
		if err := op.finishPipelineOutputCommits(); err != nil {
			return err
		}
		return op.deletePipelineResources()
	}
	return nil
}

func (a *apiServer) newPipelineOp(pachClient *client.APIClient, pipeline string) (*pipelineOp, error) {
	op := &pipelineOp{
		apiServer:  a,
		pachClient: pachClient,
		ptr:        &pps.EtcdPipelineInfo{},
		name:       pipeline,
	}
	// get latest EtcdPipelineInfo (events can pile up, so that the current state
	// doesn't match the event being processed)
	if err := a.pipelines.ReadOnly(pachClient.Ctx()).Get(pipeline, op.ptr); err != nil {
		return nil, fmt.Errorf("could not retrieve etcd pipeline info for %q: %v", pipeline, err)
	}
	// set op.pipelineInfo
	if err := op.getPipelineInfo(); err != nil {
		return nil, err
	}
	return op, nil
}

// getPipelineInfo reads the pipelineInfo associated with 'op's pipeline. This
// should be one of the first calls made on 'op', as most other methods (e.g.
// getRC, though not failPipeline) assume that op.pipelineInfo is set.
//
// Like other functions in this file, it takes responsibility for failing op's
// pipeline if it can't read the pipeline's info, and then returns an error to
// the caller to indicate that the caller shouldn't continue.
func (op *pipelineOp) getPipelineInfo() error {
	var errCount int
	return backoff.RetryNotify(func() error {
		return op.apiServer.sudo(op.pachClient, func(superUserClient *client.APIClient) error {
			var err error
			op.pipelineInfo, err = ppsutil.GetPipelineInfo(superUserClient, op.ptr)
			return err
		})
	}, backoff.NewInfiniteBackOff(), func(err error, d time.Duration) error {
		if errCount++; errCount >= maxErrCount {
			// don't restart PPS master, which might not fix the problem (crashloop)
			return op.failPipeline(fmt.Sprintf("error retrieving spec for %q after %d attempts: %v",
				op.name, maxErrCount, err))
		}
		log.Errorf("PPS master: error retrieving spec for %q: %v; retrying in %v", op.name, err, d)
		return nil
	})
}

// getRC reads the RC associated with 'op's pipeline. op.pipelineInfo must be
// set already. 'expectation' indicates whether the PPS master expects an RC to
// exist--if set to 'rcExpected', getRC will restart the pipeline if no RC is
// found after three retries. If set to 'noRCExpected', then getRC will return
// after the first "not found" error. If set to noExpectation, then getRC will
// retry the kubeclient.List() RPC, but will no restart the pipeline if no RC is
// found
//
// Like other functions in this file, it takes responsibility for restarting
// op's pipeline if it can't read the pipeline's RC (or if the RC is stale or
// redundant), and then returns an error to the caller to indicate that the
// caller shouldn't continue with other operations
func (op *pipelineOp) getRC(expectation rcExpectation) (retErr error) {
	span, _ := tracing.AddSpanToAnyExisting(op.pachClient.Ctx(),
		"/pps.Master/GetRC", "pipeline", op.name)
	defer func(span opentracing.Span) {
		tracing.TagAnySpan(span, "err", fmt.Sprintf("%v", retErr))
		tracing.FinishAnySpan(span)
	}(span)

	kubeClient := op.apiServer.env.GetKubeClient()
	namespace := op.apiServer.namespace
	selector := fmt.Sprintf("%s=%s", pipelineNameLabel, op.name)

	// count error types separately, so that this only errors if the pipeline is
	// stuck and not changing
	var notFoundErrCount, unexpectedErrCount, staleErrCount, tooManyErrCount,
		otherErrCount int
	return backoff.RetryNotify(func() error {
		// List all RCs, so stale RCs from old pipelines are noticed and deleted
		rcs, err := kubeClient.CoreV1().ReplicationControllers(namespace).List(
			metav1.ListOptions{LabelSelector: selector})
		if err != nil && !isNotFoundErr(err) {
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
		if expectation == noRCExpected && err == errRCNotFound {
			return err // rc has come down successfully--no need to keep looking
		}
		switch err {
		case errRCNotFound:
			notFoundErrCount++
		case errUnexpectedRC:
			unexpectedErrCount++
		case errTooManyRCs:
			tooManyErrCount++
		case errStaleRC:
			staleErrCount++ // don't return immediately b/c RC might be changing
		default:
			otherErrCount++
		}
		errCount := max(notFoundErrCount, unexpectedErrCount, staleErrCount,
			tooManyErrCount, otherErrCount)
		if errCount >= maxErrCount {
			missingExpectedRC := expectation == rcExpected && err == errRCNotFound
			invalidRCState := err == errTooManyRCs || err == errStaleRC
			if missingExpectedRC || invalidRCState {
				return op.restartPipeline(fmt.Sprintf("could not get RC after %d attempts: %v", errCount, err))
			}
			return err //return whatever the most recent error was
		}
		log.Errorf("PPS master: error retrieving RC for %q: %v; retrying in %v", op.name, err, d)
		return nil
	})
}

// rcIsFresh returns a boolean indicating whether op.rc has the right labels
// corresponding to op.ptr. If this returns false, it likely means the current
// RC is using e.g. an old spec commit or something.
func (op *pipelineOp) rcIsFresh() bool {
	if op.rc == nil {
		log.Errorf("PPS master: RC for %q is nil", op.name)
		return false
	}
	expectedName := ""
	if op.pipelineInfo != nil {
		expectedName = ppsutil.PipelineRcName(op.name, op.pipelineInfo.Version)
	}

	// establish current RC properties
	rcName := op.rc.ObjectMeta.Name
	rcPachVersion := op.rc.ObjectMeta.Annotations[pachVersionAnnotation]
	rcAuthTokenHash := op.rc.ObjectMeta.Annotations[hashedAuthTokenAnnotation]
	rcSpecCommit := op.rc.ObjectMeta.Annotations[specCommitAnnotation]
	switch {
	case rcAuthTokenHash != hashAuthToken(op.ptr.AuthToken):
		log.Errorf("PPS master: auth token in %q is stale %s != %s",
			op.name, rcAuthTokenHash, hashAuthToken(op.ptr.AuthToken))
		return false
	case rcSpecCommit != op.ptr.SpecCommit.ID:
		log.Errorf("PPS master: spec commit in %q looks stale %s != %s",
			op.name, rcSpecCommit, op.ptr.SpecCommit.ID)
		return false
	case rcPachVersion != version.PrettyVersion():
		log.Errorf("PPS master: %q is using stale pachd v%s != current v%s",
			op.name, rcPachVersion, version.PrettyVersion())
		return false
	case expectedName != "" && rcName != expectedName:
		log.Errorf("PPS master: %q has an unexpected (likely stale) name %q != %q",
			op.name, rcName, expectedName)
	}
	return true
}

// setPipelineState set's op's state in etcd to 'state'. This will trigger an
// etcd watch event and cause step() to eventually run again.
//
// Like other functions in this file, setPipelineState handles its own retries,
// though if it can't eventually update the pipeline state, it just returns an
// error (to indicate to the caller that it shouldn't continue with other
// operations) but doesn't fail the pipeline as the pipeline state is already
// unsettable.
func (op *pipelineOp) setPipelineState(state pps.PipelineState) error {
	var errCount int
	return backoff.RetryNotify(func() error {
		return op.apiServer.setPipelineState(op.pachClient, op.pipelineInfo, state, "")
	}, backoff.NewInfiniteBackOff(), func(err error, d time.Duration) error {
		if errCount++; errCount >= maxErrCount {
			return fmt.Errorf("could not set pipeline state for %q to %v: %v "+
				"(you may need to restart pachd to un-stick the pipeline)", op.name, state, err)
		}
		return nil
	})
}

// createPipelineResources creates the RC and any services for op's pipeline.
//
// Like other functions in this file, it takes responsibility for failing op's
// pipeline if it can't create the resources, and then returns an error to the
// caller to indicate that the caller shouldn't continue.
func (op *pipelineOp) createPipelineResources() error {
	log.Infof("PPS master: creating resources for pipeline %q", op.name)
	var errCount int
	return backoff.RetryNotify(func() error {
		return op.apiServer.createWorkerSvcAndRc(op.pachClient.Ctx(), op.ptr, op.pipelineInfo)
	}, backoff.NewInfiniteBackOff(), func(err error, d time.Duration) error {
		_, invalidOpts := err.(noValidOptionsErr)
		errCount++
		switch {
		case invalidOpts:
			// these errors indicate invalid pipelineInfo
			return op.failPipeline(fmt.Sprintf("could not generate RC options: %v", err))
		case errCount >= maxErrCount:
			return op.failPipeline(fmt.Sprintf(
				"failed to create RC/service after %d attempts: %v", errCount, err))
		default:
			log.Errorf("PPS master: error creating resources for pipeline %q: %v; retrying in %v",
				op.name, err, d)
			return nil
		}
	})
}

// startPipelineMonitor spawns a monitorPipeline() goro for this pipeline (if
// one doesn't exist already), which manages standby and cron inputs, and
// updates the the pipeline state.
// Note: this is called by every run through step(), so must be idempotent
func (op *pipelineOp) startPipelineMonitor() {
	op.apiServer.monitorCancelsMu.Lock()
	defer op.apiServer.monitorCancelsMu.Unlock()
	if _, ok := op.apiServer.monitorCancels[op.name]; !ok {
		// use context.Background because we expect this goro to run for the rest of
		// pachd's lifetime
		ctx, cancel := context.WithCancel(context.Background())
		op.apiServer.monitorCancels[op.name] = cancel
		go op.apiServer.sudo(op.apiServer.env.GetPachClient(ctx),
			func(superUserClient *client.APIClient) error {
				op.apiServer.monitorPipeline(superUserClient, op.pipelineInfo)
				return nil
			})
	}
}

// finishPipelineOutputCommits finishes any output commits of
// 'pipelineInfo.Pipeline' with an empty tree.
// TODO(msteffen) Note that if the pipeline has any jobs (which can happen if
// the user manually deletes the pipeline's RC, failing the pipeline, after it
// has created jobs) those will not be updated, but they should be FAILED
//
// Unlike other functions in this file, finishPipelineOutputCommits doesn't
// retry if it encounters an error. Currently. it's only called by step() in the
// case where op's pipeline is already in FAILURE. If it returns an error in
// that case, the pps master will log the error and move on to the next pipeline
// event. This pipeline's output commits will stay open until another watch
// event arrives for the pipeline and finishPipelineOutputCommits is retried.
func (op *pipelineOp) finishPipelineOutputCommits() (retErr error) {
	log.Infof("PPS master: finishing output commits for pipeline %q", op.name)

	var pachClient *client.APIClient
	if span, _ctx := tracing.AddSpanToAnyExisting(op.pachClient.Ctx(),
		"/pps.Master/FinishPipelineOutputCommits", "pipeline", op.name); span != nil {
		pachClient = op.pachClient.WithCtx(_ctx) // copy span back into pachClient
		defer func() {
			tracing.TagAnySpan(span, "err", fmt.Sprintf("%v", retErr))
			tracing.FinishAnySpan(span)
		}()
	} else {
		pachClient = op.pachClient
	}

	return op.apiServer.sudo(pachClient, func(superUserClient *client.APIClient) error {
		commitInfos, err := superUserClient.ListCommit(op.name, op.pipelineInfo.OutputBranch, "", 0)
		if isNotFoundErr(err) {
			return nil // already deleted
		}
		if err != nil {
			return fmt.Errorf("could not list output commits of %q to finish them: %v", op.name, err)
		}

		var finishCommitErr error
		for _, ci := range commitInfos {
			if ci.Finished != nil {
				continue // nothing needs to be done
			}
			if _, err := superUserClient.PfsAPIClient.FinishCommit(superUserClient.Ctx(),
				&pfs.FinishCommitRequest{
					Commit: client.NewCommit(op.name, ci.Commit.ID),
					Empty:  true,
				}); err != nil && finishCommitErr == nil {
				finishCommitErr = err
			}
		}
		return finishCommitErr
	})
}

// deletePipelineResources deletes the RC and services associated with op's
// pipeline.
//
// Unlike other functions in this file, deletePipelineResources doesn't retry.
// It's called in two places:
// - step(), if the pipeline is in FAILURE(). In this case, the error will be
//   logged and the pipeline's resources will be left around until a new etcd
//   event arrives for the pipeline.
// - op.restartPipeline(). Because restartPipeline does retry,
//   deletePipelineResources will be retried a limited number of times and then
//   the pipeline will be failed. If the pipeline's resources still can't be
//   deleted, then (per step() above) the error will be logged and the PPS
//   master will move on
func (op *pipelineOp) deletePipelineResources() error {
	return op.apiServer.deletePipelineResources(op.pachClient.Ctx(), op.name)
}

// updateRC is a helper for {scaleUp,scaleDown}Pipeline. It includes all of the
// logic for writing an updated RC spec to kubernetes, and updating/retrying if
// k8s rejects the write. It presents a strange API, since the the RC being
// updated is already available to the caller in op.rc, but update() may be
// called muliple times if the k8s write fails. It may be helpful to think of
// the rc passed to update() as mutable, while op.rc is immutable.
//
// Like other functions in this file, it takes responsibility for
// failing/restarting op's pipeline if it can't update its RC. If this happens,
// it will return an error to the caller to indicate that the caller shouldn't
// continue with further operations
func (op *pipelineOp) updateRC(update func(rc *v1.ReplicationController)) error {
	kubeClient := op.apiServer.env.GetKubeClient()
	namespace := op.apiServer.namespace
	rc := kubeClient.CoreV1().ReplicationControllers(namespace)

	var errCount int
	return backoff.RetryNotify(func() error {
		newRC := *op.rc
		// Apply op's update to rc
		update(&newRC)
		// write updated RC to k8s
		_, err := rc.Update(&newRC)
		return err
	}, backoff.NewInfiniteBackOff(), func(err error, d time.Duration) error {
		errCount++
		if strings.Contains(err.Error(), "try again") {
			// refresh RC--sometimes kubernetes complains that the RC is stale
			if err := op.getRC(rcExpected); err != nil {
				return err // getRC will log & restart pipeline--just don't proceed
			}
		} else if errCount >= maxErrCount {
			return op.failPipeline(fmt.Sprintf("failed to update RC after %d attempts: %v",
				errCount, err))
		}
		log.Errorf("PPS master: error updating RC for pipeline %q: %v; retrying in %v", op.name, err, d)
		return nil
	})
}

// scaleUpPipeline edits the RC associated with op's pipeline & spins up the
// configured number of workers.
//
// Like other functions in this file, it takes responsibility for
// failing/restarting op's pipeline if it can't update its RC (via updateRC)
func (op *pipelineOp) scaleUpPipeline() (retErr error) {
	log.Infof("PPS master: scaling up workers for %q", op.name)
	span, _ := tracing.AddSpanToAnyExisting(op.pachClient.Ctx(),
		"/pps.Master/ScaleUpPipeline", "pipeline", op.name)
	defer func() {
		if retErr != nil {
			log.Errorf("PPS master: error scaling up: %v", retErr)
		}
		tracing.TagAnySpan(span, "err", retErr)
		tracing.FinishAnySpan(span)
	}()

	// compute target pipeline parallelism
	kubeClient := op.apiServer.env.GetKubeClient()
	parallelism, err := ppsutil.GetExpectedNumWorkers(kubeClient, op.pipelineInfo.ParallelismSpec)
	if err != nil {
		log.Errorf("PPS master: error getting number of workers (defaulting to 1 worker): %v", err)
		parallelism = 1
	}

	// update pipeline RC
	return op.updateRC(func(rc *v1.ReplicationController) {
		if rc.Spec.Replicas != nil && *op.rc.Spec.Replicas == int32(parallelism) {
			return // prior attempt succeeded
		}
		rc.Spec.Replicas = new(int32)
		*rc.Spec.Replicas = int32(parallelism)
	})
}

// scaleDownPipeline edits the RC associated with op's pipeline & spins down the
// configured number of workers.
//
// Like other functions in this file, it takes responsibility for
// failing/restarting op's pipeline if it can't update its RC (via updateRC)
func (op *pipelineOp) scaleDownPipeline() (retErr error) {
	log.Infof("PPS master: scaling down workers for %q", op.name)
	span, _ := tracing.AddSpanToAnyExisting(op.pachClient.Ctx(),
		"/pps.Master/ScaleDownPipeline", "pipeline", op.name)
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
//
// Like other functions in this file, restartPipeline takes responsibility for
// retrying and eventually failing op's pipeline if restartPipeline can't
// restart it.
func (op *pipelineOp) restartPipeline(reason string) error {
	kubeClient := op.apiServer.env.GetKubeClient()
	namespace := op.apiServer.namespace
	var errCount int
	if err := backoff.RetryNotify(func() error {
		if op.rc != nil && !op.rcIsFresh() {
			// Cancel any running monitorPipeline call
			op.apiServer.cancelMonitor(op.name)
			// delete stale RC
			err := kubeClient.CoreV1().ReplicationControllers(namespace).Delete(
				op.rc.Name, &metav1.DeleteOptions{OrphanDependents: &falseVal})
			if err != nil && !isNotFoundErr(err) {
				return fmt.Errorf("could not delete RC %q: %v", op.rc.Name, err)
			}
		}
		// create up-to-date RC
		if err := op.createPipelineResources(); err != nil {
			return err
		}
		return op.setPipelineState(pps.PipelineState_PIPELINE_RESTARTING)
	}, backoff.NewInfiniteBackOff(), func(err error, d time.Duration) error {
		if errCount++; errCount >= maxErrCount {
			return err
		}
		log.Errorf("PPS master: error restarting pipeline %q: %v; retrying in %v", op.name, err, d)
		return nil
	}); err != nil {
		return op.failPipeline(fmt.Sprintf("could not restart after %d attempts: %v", errCount, err))
	}
	return fmt.Errorf("restarting pipeline %q: %v", op.name, reason)
}

// failPipeline fails op's pipeline. failPipeline is an error-handling codepath,
// so it's guaranteed to return an error (typically wrapping 'reason', though if
// the restart process fails that error will take precendence) so that callers
// can use it like so:
//
// if errorState {
//   return op.failPipeline("entered error state")
// }
//
// Like other functions in this file, failPipeline takes responsibility for
// retrying.
func (op *pipelineOp) failPipeline(reason string) error {
	if err := op.apiServer.setPipelineFailure(op.pachClient.Ctx(), op.name, reason); err != nil {
		return fmt.Errorf("error failing pipeline %q: %v", op.name, err)
	}
	return fmt.Errorf("failing pipeline %q: %v", op.name, reason)
}

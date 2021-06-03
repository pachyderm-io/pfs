package server

import (
	"context"
	"fmt"
	"path"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/pachyderm/pachyderm/src/client"
	"github.com/pachyderm/pachyderm/src/client/pkg/errors"
	"github.com/pachyderm/pachyderm/src/client/pkg/tracing"
	"github.com/pachyderm/pachyderm/src/client/pps"
	"github.com/pachyderm/pachyderm/src/server/pkg/backoff"
	"github.com/pachyderm/pachyderm/src/server/pkg/dlock"
	"github.com/pachyderm/pachyderm/src/server/pkg/ppsutil"
)

const (
	masterLockPath = "_master_lock"
)

var (
	failures = map[string]bool{
		"InvalidImageName": true,
		"ErrImagePull":     true,
		"Unschedulable":    true,
	}

	zero     int32 // used to turn down RCs in scaleDownWorkersForPipeline
	falseVal bool  // used to delete RCs in deletePipelineResources and restartPipeline()
)

type eventType int

const (
	writeEv eventType = iota
	deleteEv
)

type pipelineEvent struct {
	eventType
	pipeline string

	// etcdVer and etcdRev record the etcd version and key revision at which a
	// write/delete was observed. These are recorded in traces and are useful for
	// debugging (e.g. if two consecutive spans have non-consecutive versions,
	// that would indicate a concurrent write). These will not be set for events
	// created by pollPipelines.
	etcdVer int64
	etcdRev int64
}

type ppsMaster struct {
	// The PPS APIServer that owns this struct
	a *apiServer

	// masterClient is a pachyderm client containing a context that is cancelled if
	// the current pps master loses its master status
	// Note: 'masterClient' is unauthenticated.  This uses the PPS token (via
	// a.sudo()) to authenticate requests as needed.
	masterClient *client.APIClient

	// fields for monitorPipeline goros, monitorCrashingPipeline goros, etc.
	monitorCancelsMu       sync.Mutex
	monitorCancels         map[string]func() // protected by monitorCancelsMu
	crashingMonitorCancels map[string]func() // also protected by monitorCancelsMu

	// fields for the pollPipelines and pollPipelinePods goros
	pollPipelinesMu sync.Mutex
	pollCancel      func() // protected by pollPipelinesMu
	pollPodsCancel  func() // protected by pollPipelinesMu
	pollEtcdCancel  func() // protected by pollPipelinesMu

	// channel through which pipeline events are passed
	eventCh chan *pipelineEvent
}

// The master process is responsible for creating/deleting workers as
// pipelines are created/removed.
func (a *apiServer) master() {
	m := &ppsMaster{
		a:                      a,
		monitorCancels:         make(map[string]func()),
		crashingMonitorCancels: make(map[string]func()),
	}

	masterLock := dlock.NewDLock(a.env.GetEtcdClient(), path.Join(a.etcdPrefix, masterLockPath))
	backoff.RetryNotify(func() error {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ctx, err := masterLock.Lock(ctx)
		if err != nil {
			return err
		}
		defer masterLock.Unlock(ctx)

		log.Infof("PPS master: launching master process")
		m.masterClient = a.env.GetPachClient(ctx)
		m.run()
		return errors.Wrapf(ctx.Err(), "ppsMaster.Run() exited unexpectedly")
	}, backoff.NewInfiniteBackOff(), func(err error, d time.Duration) error {
		log.Errorf("PPS master: error running the master process: %v; retrying in %v", err, d)
		return nil
	})
	panic("internal error: PPS master has somehow exited. Restarting pod...")
}

func (a *apiServer) setPipelineFailure(ctx context.Context, pipelineName string, reason string) error {
	return a.setPipelineState(ctx, pipelineName, pps.PipelineState_PIPELINE_FAILURE, reason)
}

func (a *apiServer) setPipelineCrashing(ctx context.Context, pipelineName string, reason string) error {
	return a.setPipelineState(ctx, pipelineName, pps.PipelineState_PIPELINE_CRASHING, reason)
}

func (m *ppsMaster) run() {
	// close m.eventCh after all cancels have returned and therefore all pollers
	// (which are what write to m.eventCh) have exited
	m.eventCh = make(chan *pipelineEvent, 1)
	defer close(m.eventCh)
	defer m.cancelAllMonitorsAndCrashingMonitors()
	// start pollers in the background--cancel functions ensure poll/monitor
	// goroutines all definitely stop (either because cancelXYZ returns or because
	// the binary panics)
	m.startPipelinePoller()
	defer m.cancelPipelinePoller()
	m.startPipelinePodsPoller()
	defer m.cancelPipelinePodsPoller()
	m.startPipelineEtcdPoller()
	defer m.cancelPipelineEtcdPoller()

	masterCtx := m.masterClient.Ctx()
eventLoop:
	for {
		select {
		case e := <-m.eventCh:
			switch e.eventType {
			case writeEv:
				// Create/Modify/Delete pipeline resources as needed per new state
				if err := m.step(e.pipeline, e.etcdVer, e.etcdRev); err != nil {
					log.Errorf("PPS master: could not update resources for pipeline %q: %v",
						e.pipeline, err)
				}
			case deleteEv:
				// TODO(msteffen) trace this call
				if err := m.deletePipelineResources(e.pipeline); err != nil {
					log.Errorf("PPS master: could not delete resources for pipeline %q: %v",
						e.pipeline, err)
				}
			}
		case <-masterCtx.Done():
			break eventLoop
		}
	}
}

func (m *ppsMaster) deletePipelineResources(pipelineName string) (retErr error) {
	log.Infof("PPS master: deleting resources for pipeline %q", pipelineName)
	span, ctx := tracing.AddSpanToAnyExisting(m.masterClient.Ctx(),
		"/pps.Master/DeletePipelineResources", "pipeline", pipelineName)
	defer func() {
		tracing.TagAnySpan(ctx, "err", retErr)
		tracing.FinishAnySpan(span)
	}()

	// Cancel any running monitorPipeline call
	m.cancelMonitor(pipelineName)
	// Same for cancelCrashingMonitor
	m.cancelCrashingMonitor(pipelineName)

	kubeClient := m.a.env.GetKubeClient()
	namespace := m.a.namespace

	// Delete any services associated with op.pipeline
	selector := fmt.Sprintf("%s=%s", pipelineNameLabel, pipelineName)
	opts := metav1.DeleteOptions{
		OrphanDependents: &falseVal,
	}

	services, err := kubeClient.CoreV1().Services(namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return errors.Wrapf(err, "could not list services")
	}
	for _, service := range services.Items {
		if err := kubeClient.CoreV1().Services(namespace).Delete(ctx, service.Name, opts); err != nil {
			if !isNotFoundErr(err) {
				return errors.Wrapf(err, "could not delete service %q", service.Name)
			}
		}
	}

	// Delete any secrets associated with op.pipeline
	secrets, err := kubeClient.CoreV1().Secrets(namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return errors.Wrapf(err, "could not list secrets")
	}
	for _, secret := range secrets.Items {
		if err := kubeClient.CoreV1().Secrets(namespace).Delete(ctx, secret.Name, opts); err != nil {
			if !isNotFoundErr(err) {
				return errors.Wrapf(err, "could not delete secret %q", secret.Name)
			}
		}
	}

	// Finally, delete op.pipeline's RC, which will cause pollPipelines to stop
	// polling it.
	rcs, err := kubeClient.CoreV1().ReplicationControllers(namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return errors.Wrapf(err, "could not list RCs")
	}
	for _, rc := range rcs.Items {
		if err := kubeClient.CoreV1().ReplicationControllers(namespace).Delete(ctx, rc.Name, opts); err != nil {
			if !isNotFoundErr(err) {
				return errors.Wrapf(err, "could not delete RC %q: %v", rc.Name)
			}
		}
	}

	return nil
}

// setPipelineState is a PPS-master-specific helper that wraps
// ppsutil.SetPipelineState in a trace
func (a *apiServer) setPipelineState(ctx context.Context, pipeline string, state pps.PipelineState, reason string) (retErr error) {
	span, ctx := tracing.AddSpanToAnyExisting(ctx,
		"/pps.Master/SetPipelineState", "pipeline", pipeline, "new-state", state)
	defer func() {
		tracing.TagAnySpan(span, "err", retErr)
		tracing.FinishAnySpan(span)
	}()
	return ppsutil.SetPipelineState(ctx, a.env.GetEtcdClient(), a.pipelines,
		pipeline, nil, state, reason)
}

// transitionPipelineState is similar to setPipelineState, except that it sets
// 'from' and logs a different trace
func (a *apiServer) transitionPipelineState(ctx context.Context, pipeline string, from []pps.PipelineState, to pps.PipelineState, reason string) (retErr error) {
	span, ctx := tracing.AddSpanToAnyExisting(ctx,
		"/pps.Master/TransitionPipelineState", "pipeline", pipeline,
		"from-state", from, "to-state", to)
	defer func() {
		tracing.TagAnySpan(span, "err", retErr)
		tracing.FinishAnySpan(span)
	}()
	return ppsutil.SetPipelineState(ctx, a.env.GetEtcdClient(), a.pipelines,
		pipeline, from, to, reason)
}

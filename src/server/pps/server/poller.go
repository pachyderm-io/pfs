package server

import (
	"time"

	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	kube_err "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kube_watch "k8s.io/apimachinery/pkg/watch"

	"github.com/pachyderm/pachyderm/src/client"
	"github.com/pachyderm/pachyderm/src/client/pkg/errors"
	"github.com/pachyderm/pachyderm/src/client/pps"
	"github.com/pachyderm/pachyderm/src/server/pkg/backoff"
	"github.com/pachyderm/pachyderm/src/server/pkg/watch"
)

const pollBackoffTime = 2 * time.Second

// startPipelinePoller starts a new goroutine running pollPipelines
func (m *ppsMaster) startPipelinePoller() {
	m.pollPipelinesMu.Lock()
	defer m.pollPipelinesMu.Unlock()
	m.pollCancel = m.startMonitorThread("pollPipelines", m.pollPipelines)
}

func (m *ppsMaster) cancelPipelinePoller() {
	m.pollPipelinesMu.Lock()
	defer m.pollPipelinesMu.Unlock()
	if m.pollCancel != nil {
		m.pollCancel()
		m.pollCancel = nil
	}
}

// startPipelinePodsPoller starts a new goroutine running pollPipelinePods
func (m *ppsMaster) startPipelinePodsPoller() {
	m.pollPipelinesMu.Lock()
	defer m.pollPipelinesMu.Unlock()
	m.pollPodsCancel = m.startMonitorThread("pollPipelinePods", m.pollPipelinePods)
}

func (m *ppsMaster) cancelPipelinePodsPoller() {
	m.pollPipelinesMu.Lock()
	defer m.pollPipelinesMu.Unlock()
	if m.pollPodsCancel != nil {
		m.pollPodsCancel()
		m.pollPodsCancel = nil
	}
}

// startPipelineEtcdPoller starts a new goroutine running
// pollPipelinesEtcd
func (m *ppsMaster) startPipelineEtcdPoller() {
	m.pollPipelinesMu.Lock()
	defer m.pollPipelinesMu.Unlock()
	m.pollEtcdCancel = m.startMonitorThread("pollPipelinesEtcd", m.pollPipelinesEtcd)
}

func (m *ppsMaster) cancelPipelineEtcdPoller() {
	m.pollPipelinesMu.Lock()
	defer m.pollPipelinesMu.Unlock()
	if m.pollEtcdCancel != nil {
		m.pollEtcdCancel()
		m.pollEtcdCancel = nil
	}
}

//////////////////////////////////////////////////////////////////////////////
//                     PollPipelines Definition                             //
// - As in monitor.go, functions below should not call functions above, to  //
// avoid reentrancy deadlock.                                               //
//////////////////////////////////////////////////////////////////////////////

// pollPipelines generates regular updateEv and deleteEv events for each
// pipeline and sends them to ppsMaster.Run(). By scanning etcd and k8s
// regularly and generating events for them, it prevents pipelines from
// getting orphaned.
func (m *ppsMaster) pollPipelines(pollClient *client.APIClient) {
	ctx := pollClient.Ctx()
	etcdPipelines := map[string]bool{}
	if err := backoff.RetryUntilCancel(ctx, func() error {
		if len(etcdPipelines) == 0 {
			// 1. Get the current set of pipeline RCs.
			//
			// We'll delete any RCs that don't correspond to a live pipeline after
			// querying etcd to determine the set of live pipelines, but we query k8s
			// first to avoid a race (if we were to query etcd first, and
			// CreatePipeline(foo) were to run between querying etcd and querying k8s,
			// then we might delete the RC for brand-new pipeline 'foo'). Even if we
			// do delete a live pipeline's RC, it'll be fixed in the next cycle)
			kc := m.a.env.GetKubeClient().CoreV1().ReplicationControllers(m.a.env.Namespace)
			rcs, err := kc.List(ctx, metav1.ListOptions{
				LabelSelector: "suite=pachyderm,pipelineName",
			})
			if err != nil {
				// No sensible error recovery here (e.g .if we can't reach k8s). We'll
				// keep going, and just won't delete any RCs this round.
				log.Errorf("error polling pipeline RCs: %v", err)
			}

			// 2. Replenish 'etcdPipelines' with the set of pipelines currently in
			// etcd. Note that there may be zero, and etcdPipelines may be empty
			if err := m.a.listPipelinePtr(pollClient, nil, 0,
				func(pipeline string, _ *pps.EtcdPipelineInfo) error {
					etcdPipelines[pipeline] = true
					return nil
				}); err != nil {
				// listPipelinePtr results (etcdPipelines) are used by all remaining
				// steps, so if that didn't work, start over and try again
				etcdPipelines = map[string]bool{}
				return errors.Wrap(err, "error polling pipelines")
			}

			// 3. Generate a delete event for orphaned RCs
			if rcs != nil {
				for _, rc := range rcs.Items {
					pipeline, ok := rc.Labels["pipelineName"]
					if !ok {
						return errors.New("'pipelineName' label missing from rc " + rc.Name)
					}
					if !etcdPipelines[pipeline] {
						m.eventCh <- &pipelineEvent{eventType: deleteEv, pipeline: pipeline}
					}
				}
			}

			// 4. Retry if there are no etcd pipelines to read/write
			if len(etcdPipelines) == 0 {
				return backoff.ErrContinue
			}
		}

		// Generate one etcd event for a pipeline (to trigger the pipeline
		// controller) and remove this pipeline from etcdPipelines. Always choose
		// the lexicographically smallest pipeline so that pipelines are always
		// traversed in the same order and the period between polls is stable across
		// all pipelines.
		var pipeline string
		for p := range etcdPipelines {
			if pipeline == "" || p < pipeline {
				pipeline = p
			}
		}

		// always rm 'pipeline', to advance loop
		delete(etcdPipelines, pipeline)

		// generate a pipeline event for 'pipeline'
		log.Debugf("PPS master: polling pipeline %q", pipeline)
		select {
		case m.eventCh <- &pipelineEvent{eventType: writeEv, pipeline: pipeline}:
			break
		case <-ctx.Done():
			break
		}

		// 5. move to next pipeline (after 2s sleep)
		return backoff.ErrContinue
	}, backoff.NewConstantBackOff(pollBackoffTime),
		backoff.NotifyContinue("pollPipelines"),
	); err != nil && ctx.Err() == nil {
		log.Fatalf("pollPipelines is exiting prematurely which should not happen (error: %v); restarting container...", err)
	}
}

// pollPipelinePods creates a kubernetes watch, and for each event:
//   1) Checks if the event concerns a Pod
//   2) Checks if the Pod belongs to a pipeline (pipelineName annotation is set)
//   3) Checks if the Pod is failing
// If all three conditions are met, then the pipline (in 'pipelineName') is set
// to CRASHING
func (m *ppsMaster) pollPipelinePods(pollClient *client.APIClient) {
	ctx := pollClient.Ctx()
	if err := backoff.RetryUntilCancel(ctx, func() error {
		kubePipelineWatch, err := m.a.env.GetKubeClient().CoreV1().Pods(m.a.namespace).Watch(ctx,
			metav1.ListOptions{
				LabelSelector: metav1.FormatLabelSelector(metav1.SetAsLabelSelector(
					map[string]string{
						"component": "worker",
					})),
				Watch: true,
			})
		if err != nil {
			return errors.Wrap(err, "failed to watch kubernetes pods")
		}
		defer kubePipelineWatch.Stop()
		for {
			select {
			case <-ctx.Done():
				return nil
			case event := <-kubePipelineWatch.ResultChan():
				// if we get an error we restart the watch
				if event.Type == kube_watch.Error {
					return errors.Wrap(kube_err.FromObject(event.Object), "error while watching kubernetes pods")
				} else if event.Type == "" {
					// k8s watches seem to sometimes get stuck in a loop returning events
					// with Type = "". We treat these as errors as otherwise we get an
					// endless stream of them and can't do anything.
					return errors.New("error while watching kubernetes pods: empty event type")
				}
				pod, ok := event.Object.(*v1.Pod)
				if !ok {
					continue // irrelevant event
				}
				if pod.Status.Phase == v1.PodFailed {
					log.Errorf("pod failed because: %s", pod.Status.Message)
				}
				pipelineName := pod.ObjectMeta.Annotations["pipelineName"]
				for _, status := range pod.Status.ContainerStatuses {
					if status.State.Waiting != nil && failures[status.State.Waiting.Reason] {
						if err := m.a.setPipelineCrashing(ctx, pipelineName, status.State.Waiting.Message); err != nil {
							return errors.Wrap(err, "error moving pipeline to CRASHING")
						}
					}
				}
				for _, condition := range pod.Status.Conditions {
					if condition.Type == v1.PodScheduled &&
						condition.Status != v1.ConditionTrue && failures[condition.Reason] {
						if err := m.a.setPipelineCrashing(ctx, pipelineName, condition.Message); err != nil {
							return errors.Wrap(err, "error moving pipeline to CRASHING")
						}
					}
				}
			}
		}
	}, backoff.NewInfiniteBackOff(), backoff.NotifyContinue("pollPipelinePods"),
	); err != nil && ctx.Err() == nil {
		log.Fatalf("pollPipelinePods is exiting prematurely which should not happen (error: %v); restarting container...", err)
	}
}

// pollPipelinesEtcd watches the 'pipelines' collection in etcd and sends
// writeEv and deleteEv events to the PPS master when it sees them.
//
// pollPipelinesEtcd is unlike the other poll and monitor goroutines in that it
// sees the result of other poll/monitor goroutines' writes. For example, when
// pollPipelinePods (above) observes that a pipeline is crashing and updates its
// state in etcd, the flow for starting monitorPipelineCrashing is:
//
//  k8s watch ─> pollPipelinePods  ╭──> pollPipelinesEtcd  ╭──> m.run()
//                      │          │            │          │      │
//                      ↓          │            ↓          │      ↓
//                  etcd write─────╯       m.eventCh ──────╯   m.step()
//
// most of the other poll/monitor goroutines actually go through
// pollPipelinesEtcd (by writing to etcd, which is then observed by the etcd
// watch below)
func (m *ppsMaster) pollPipelinesEtcd(pollClient *client.APIClient) {
	ctx := pollClient.Ctx()
	if err := backoff.RetryUntilCancel(ctx, func() error {
		// TODO(msteffen) request only keys, since pipeline_controller.go reads
		// fresh values for each event anyway
		pipelineWatcher, err := m.a.pipelines.ReadOnly(ctx).Watch()
		if err != nil {
			return errors.Wrapf(err, "error creating watch")
		}
		defer pipelineWatcher.Close()

		for event := range pipelineWatcher.Watch() {
			if event.Err != nil {
				return errors.Wrapf(event.Err, "event err")
			}
			switch event.Type {
			case watch.EventPut:
				m.eventCh <- &pipelineEvent{
					eventType: writeEv,
					pipeline:  string(event.Key),
					etcdVer:   event.Ver,
					etcdRev:   event.Rev,
				}
			case watch.EventDelete:
				m.eventCh <- &pipelineEvent{
					eventType: deleteEv,
					pipeline:  string(event.Key),
					etcdVer:   event.Ver,
					etcdRev:   event.Rev,
				}
			}
		}
		return backoff.ErrContinue // reset until ctx is cancelled (RetryUntilCancel)
	}, &backoff.ZeroBackOff{}, backoff.NotifyContinue("pollPipelinesEtcd"),
	); err != nil && ctx.Err() == nil {
		log.Fatalf("pollPipelinesEtcd is exiting prematurely which should not happen (error: %v); restarting container...", err)
	}
}

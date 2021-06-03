package server

// monitor.go contains methods of s/s/pps/server/api_server.go:APIServer that
// pertain to these fields of APIServer:
//   - monitorCancels,
//   - crashingMonitorCancels,
//   - pollCancel, and
//   - monitorCancelsMu (which protects all of the other fields).
//
// Other functions of APIServer.go should not access any of
// these fields directly (particularly APIServer.monitorCancelsMu, to avoid
// deadlocks) and should instead interact with monitorPipeline and such via
// methods in this file.
//
// Likewise, to avoid reentrancy deadlocks (A -> B -> A), methods in this file
// should avoid calling other methods of APIServer defined outside this file and
// shouldn't call each other.

import (
	"context"
	"path"
	"strings"
	"time"

	"github.com/gogo/protobuf/types"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/robfig/cron"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/pachyderm/pachyderm/src/client"
	"github.com/pachyderm/pachyderm/src/client/pfs"
	"github.com/pachyderm/pachyderm/src/client/pkg/errors"
	"github.com/pachyderm/pachyderm/src/client/pkg/tracing"
	"github.com/pachyderm/pachyderm/src/client/pkg/tracing/extended"
	"github.com/pachyderm/pachyderm/src/client/pps"
	pfsserver "github.com/pachyderm/pachyderm/src/server/pfs"
	"github.com/pachyderm/pachyderm/src/server/pkg/backoff"
	"github.com/pachyderm/pachyderm/src/server/pkg/ppsconsts"
	"github.com/pachyderm/pachyderm/src/server/pkg/ppsutil"
	"github.com/pachyderm/pachyderm/src/server/pkg/work"
	"github.com/pachyderm/pachyderm/src/server/worker/driver"
	workerserver "github.com/pachyderm/pachyderm/src/server/worker/server"
)

const crashingBackoff = time.Second * 15
const scaleUpInterval = time.Second * 30

//////////////////////////////////////////////////////////////////////////////
//                     Locking Functions                                    //
// - These functions lock monitorCancelsMu in order to start or stop a      //
// monitor function (and store any cancel() functions in 'm'). They can     //
// call any of the monitor functions at the bottom of this file, but after  //
// locking, they should not call each other or any function outside of this //
// file (or else they will trigger a reentrancy deadlock):                  //
//         master.go -> monitor.go -> master.go -> monitor.go               //
//                   (lock succeeds)          (lock fails, deadlock)        //
//////////////////////////////////////////////////////////////////////////////

// startMonitor starts a new goroutine running monitorPipeline for
// 'pipelineInfo.Pipeline'.
//
// Every running pipeline with standby == true or a cron input has a
// corresponding goroutine running monitorPipeline() that puts the pipeline in
// and out of standby in response to new output commits appearing in that
// pipeline's output repo.
func (m *ppsMaster) startMonitor(pipelineInfo *pps.PipelineInfo) {
	pipeline := pipelineInfo.Pipeline.Name
	m.monitorCancelsMu.Lock()
	defer m.monitorCancelsMu.Unlock()
	if _, ok := m.monitorCancels[pipeline]; !ok {
		m.monitorCancels[pipeline] = m.startMonitorThread(
			"monitorPipeline for "+pipeline, func(pachClient *client.APIClient) {
				// monitorPipeline needs auth privileges to call subscribeCommit and
				// blockCommit
				// TODO(msteffen): run the pipeline monitor as the pipeline user, rather
				// than as the PPS superuser
				if err := m.a.sudo(pachClient, func(superUserClient *client.APIClient) error {
					m.monitorPipeline(superUserClient, pipelineInfo)
					return nil
				}); err != nil {
					log.Errorf("error monitoring %q: %v", pipeline, err)
				}
			})
	}
}

// cancelMonitor cancels the monitorPipeline goroutine for 'pipeline'. See
// m.startMonitor().
func (m *ppsMaster) cancelMonitor(pipeline string) {
	m.monitorCancelsMu.Lock()
	defer m.monitorCancelsMu.Unlock()
	if cancel, ok := m.monitorCancels[pipeline]; ok {
		cancel()
		delete(m.monitorCancels, pipeline)
	}
}

// startCrashingMonitor starts a new goroutine running monitorCrashingPipeline
// for 'pipelineInfo.Pipeline'
//
// Every crashing pipeline has a corresponding goro running
// monitorCrashingPipeline that checks to see if the issues have resolved
// themselves and moves the pipeline out of crashing if they have.
func (m *ppsMaster) startCrashingMonitor(parallelism uint64, pipelineInfo *pps.PipelineInfo) {
	pipeline := pipelineInfo.Pipeline.Name
	m.monitorCancelsMu.Lock()
	defer m.monitorCancelsMu.Unlock()
	if _, ok := m.crashingMonitorCancels[pipeline]; !ok {
		m.crashingMonitorCancels[pipeline] = m.startMonitorThread(
			"monitorCrashingPipeline for "+pipeline,
			func(pachClient *client.APIClient) {
				m.monitorCrashingPipeline(pachClient, parallelism, pipelineInfo)
			})
	}
}

// cancelCrashingMonitor cancels the monitorCrashingPipeline goroutine for
// 'pipeline'. See m.startCrashingMonitor().
func (m *ppsMaster) cancelCrashingMonitor(pipeline string) {
	m.monitorCancelsMu.Lock()
	defer m.monitorCancelsMu.Unlock()
	if cancel, ok := m.crashingMonitorCancels[pipeline]; ok {
		cancel()
		delete(m.crashingMonitorCancels, pipeline)
	}
}

// cancelAllMonitorsAndCrashingMonitors overlaps with cancelMonitor and
// cancelCrashingMonitor, but also iterates over the existing members of
// m.{crashingM,m}onitorCancels in the critical section, so that all monitors
// can be cancelled without the risk that a new monitor is added between
// cancellations.
//
// 'leave' indicates pipelines whose monitorPipeline goros shouldn't be
// cancelled. It's set by pollPipelines, which does not cancel any pipeline in
// etcd at the time that it runs
func (m *ppsMaster) cancelAllMonitorsAndCrashingMonitors() {
	m.monitorCancelsMu.Lock()
	defer m.monitorCancelsMu.Unlock()
	for _, monitorMap := range []map[string]func(){m.monitorCancels, m.crashingMonitorCancels} {
		for p := range monitorMap {
			cancel := monitorMap[p]
			cancel()
			delete(monitorMap, p)
		}
	}
}

//////////////////////////////////////////////////////////////////////////////
//                     Monitor Functions                                    //
// - These do not lock monitorCancelsMu, but they are called by the         //
// functions above, which do. They should not be called by functions        //
// outside monitor.go (to avoid data races). They can in turn call each     //
// other but also should not call any of the functions above or any         //
// functions outside this file (or else they will trigger a reentrancy      //
// deadlock):                                                               //
//         master.go -> monitor.go -> master.go -> monitor.go               //
//                   (lock succeeds)          (lock fails, deadlock)        //
//////////////////////////////////////////////////////////////////////////////

// startMonitorThread is a helper used by startMonitor, startCrashingMonitor,
// and startPipelinePoller (in poller.go). It doesn't manipulate any of
// APIServer's fields, just wrapps the passed function in a goroutine, and
// returns a cancel() fn to cancel it and block until it returns.
func (m *ppsMaster) startMonitorThread(name string, f func(pachClient *client.APIClient)) func() {
	ctx, cancel := context.WithCancel(m.masterClient.Ctx())
	done := make(chan struct{})
	go func() {
		f(m.a.env.GetPachClient(ctx))
		close(done)
	}()
	return func() {
		cancel()
		select {
		case <-done:
			return
		case <-time.After(time.Minute):
			// restart pod rather than permanently locking up the PPS master (which
			// would break the PPS API)
			panic(name + " blocked for over a minute after cancellation; restarting container")
		}
	}
}

func (m *ppsMaster) monitorPipeline(pachClient *client.APIClient, pipelineInfo *pps.PipelineInfo) {
	pipeline := pipelineInfo.Pipeline.Name
	log.Printf("PPS master: monitoring pipeline %q", pipeline)
	var eg errgroup.Group
	pps.VisitInput(pipelineInfo.Input, func(in *pps.Input) {
		if in.Cron != nil {
			eg.Go(func() error {
				return backoff.RetryNotify(func() error {
					return m.makeCronCommits(pachClient, in)
				}, backoff.NewInfiniteBackOff(),
					backoff.NotifyCtx(pachClient.Ctx(), "cron for "+in.Cron.Name))
			})
		}
	})
	if pipelineInfo.Standby || pipelineInfo.Autoscaling {
		// Capacity 1 gives us a bit of buffer so we don't needlessly go into
		// standby when SubscribeCommit takes too long to return.
		ciChan := make(chan *pfs.CommitInfo, 1)
		eg.Go(func() error {
			defer close(ciChan)
			return backoff.RetryNotify(func() error {
				return pachClient.SubscribeCommitF(pipeline, "",
					client.NewCommitProvenance(ppsconsts.SpecRepo, pipeline, pipelineInfo.SpecCommit.ID),
					"", pfs.CommitState_READY, func(ci *pfs.CommitInfo) error {
						ciChan <- ci
						return nil
					})
			}, backoff.NewInfiniteBackOff(),
				backoff.NotifyCtx(pachClient.Ctx(), "SubscribeCommit for "+pipeline))
		})
		eg.Go(func() error {
			return backoff.RetryNotify(func() error {
				var (
					oldCtx        = pachClient.Ctx()
					oldPachClient = pachClient
					childSpan     opentracing.Span
					ctx           context.Context
				)
				defer func() {
					// childSpan is overwritten so wrap in a lambda for late binding
					tracing.FinishAnySpan(childSpan)
				}()
				// start span to capture & contextualize etcd state transition
				childSpan, ctx = extended.AddSpanToAnyPipelineTrace(oldCtx,
					m.a.env.GetEtcdClient(), pipeline,
					"/pps.Master/MonitorPipeline/Begin")
				if childSpan != nil {
					pachClient = oldPachClient.WithCtx(ctx)
				}
				if err := m.a.transitionPipelineState(pachClient.Ctx(),
					pipeline,
					[]pps.PipelineState{
						pps.PipelineState_PIPELINE_RUNNING,
						pps.PipelineState_PIPELINE_CRASHING,
					}, pps.PipelineState_PIPELINE_STANDBY, ""); err != nil {

					pte := &ppsutil.PipelineTransitionError{}
					if errors.As(err, &pte) && pte.Current == pps.PipelineState_PIPELINE_PAUSED {
						// pipeline is stopped, exit monitorPipeline (which pausing the
						// pipeline should also do). monitorPipeline will be called when
						// it transitions back to running
						// TODO(msteffen): this should happen in the pipeline
						// controller
						return nil
					}
					return err
				}
				for {
					// finish span from previous loops
					tracing.FinishAnySpan(childSpan)
					childSpan = nil

					var ci *pfs.CommitInfo
					var ok bool
					select {
					case ci, ok = <-ciChan:
						if !ok {
							return nil // subscribeCommit exited, nothing left to do
						}
						if ci.Finished != nil {
							continue
						}
						childSpan, ctx = extended.AddSpanToAnyPipelineTrace(oldCtx,
							m.a.env.GetEtcdClient(), pipeline,
							"/pps.Master/MonitorPipeline/SpinUp",
							"commit", ci.Commit.ID)
						if childSpan != nil {
							pachClient = oldPachClient.WithCtx(ctx)
						}

						if err := m.a.transitionPipelineState(pachClient.Ctx(),
							pipeline,
							[]pps.PipelineState{pps.PipelineState_PIPELINE_STANDBY},
							pps.PipelineState_PIPELINE_RUNNING, ""); err != nil {

							pte := &ppsutil.PipelineTransitionError{}
							if errors.As(err, &pte) && pte.Current == pps.PipelineState_PIPELINE_PAUSED {
								// pipeline is stopped, exit monitorPipeline (see above)
								return nil
							}
							return err
						}

						// Stay running while commits are available
					running:
						for {
							// Wait for the commit to be finished before blocking on the
							// job because the job may not exist yet.
							if _, err := pachClient.BlockCommit(ci.Commit.Repo.Name, ci.Commit.ID); err != nil {
								return err
							}
							if _, err := pachClient.InspectJobOutputCommit(ci.Commit.Repo.Name, ci.Commit.ID, true); err != nil {
								return err
							}

							tracing.FinishAnySpan(childSpan)
							childSpan = nil
							select {
							case ci, ok = <-ciChan:
								if !ok {
									return nil // subscribeCommit exited, nothing left to do
								}
								childSpan, ctx = extended.AddSpanToAnyPipelineTrace(oldCtx,
									m.a.env.GetEtcdClient(), pipeline,
									"/pps.Master/MonitorPipeline/WatchNext",
									"commit", ci.Commit.ID)
								if childSpan != nil {
									pachClient = oldPachClient.WithCtx(ctx)
								}
							default:
								break running
							}
						}

						if err := m.a.transitionPipelineState(pachClient.Ctx(),
							pipeline,
							[]pps.PipelineState{
								pps.PipelineState_PIPELINE_RUNNING,
								pps.PipelineState_PIPELINE_CRASHING,
							}, pps.PipelineState_PIPELINE_STANDBY, ""); err != nil {

							pte := &ppsutil.PipelineTransitionError{}
							if errors.As(err, &pte) && pte.Current == pps.PipelineState_PIPELINE_PAUSED {
								// pipeline is stopped; monitorPipeline will be called when it
								// transitions back to running
								// TODO(msteffen): this should happen in the pipeline
								// controller
								return nil
							}
							return err
						}
					case <-pachClient.Ctx().Done():
						return pachClient.Ctx().Err()
					}
				}
			}, backoff.NewInfiniteBackOff(),
				backoff.NotifyCtx(pachClient.Ctx(), "monitorPipeline for "+pipeline))
		})
		if pipelineInfo.ParallelismSpec != nil && pipelineInfo.ParallelismSpec.Constant > 1 && pipelineInfo.Autoscaling {
			eg.Go(func() error {
				return backoff.RetryNotify(func() error {
					worker := work.NewWorker(
						m.a.env.GetEtcdClient(),
						m.a.etcdPrefix,
						driver.WorkNamespace(pipelineInfo),
					)
					for {
						unclaimedTasks, err := worker.UnclaimedTasks(pachClient.Ctx())
						if err != nil {
							return err
						}
						if unclaimedTasks > 0 {
							// TODO route this through the controller rather
							// than modifying the RC directly. This will
							// prevent a potential race condition.
							kubeClient := m.a.env.GetKubeClient()
							namespace := m.a.namespace
							rc := kubeClient.CoreV1().ReplicationControllers(namespace)
							ctx := context.TODO()
							scale, err := rc.GetScale(ctx, pipelineInfo.WorkerRc, metav1.GetOptions{})
							n := int64(scale.Spec.Replicas) + int64(unclaimedTasks)
							if n > int64(pipelineInfo.ParallelismSpec.Constant) {
								n = int64(pipelineInfo.ParallelismSpec.Constant)
							}
							if err != nil {
								return err
							}
							if int64(scale.Spec.Replicas) < n {
								scale.Spec.Replicas = int32(n)
								if _, err := rc.UpdateScale(ctx, pipelineInfo.WorkerRc, scale, metav1.UpdateOptions{}); err != nil {
									return err
								}
							}
						}
						select {
						case <-time.After(scaleUpInterval):
						case <-pachClient.Ctx().Done():
							return pachClient.Ctx().Err()
						}
					}
				}, backoff.NewInfiniteBackOff(),
					backoff.NotifyCtx(pachClient.Ctx(), "monitorPipeline for "+pipeline))
			})
		}
	}
	if err := eg.Wait(); err != nil {
		log.Printf("error in monitorPipeline: %v", err)
	}
}

func (m *ppsMaster) monitorCrashingPipeline(pachClient *client.APIClient, parallelism uint64, pipelineInfo *pps.PipelineInfo) {
	pipeline := pipelineInfo.Pipeline.Name
	ctx := pachClient.Ctx()
	if parallelism == 0 {
		parallelism = 1
	}
	pipelineRCName := ppsutil.PipelineRcName(pipeline, pipelineInfo.Version)
	if err := backoff.RetryUntilCancel(ctx, func() error {
		workerStatus, err := workerserver.Status(ctx, pipelineRCName,
			m.a.env.GetEtcdClient(), m.a.etcdPrefix, m.a.workerGrpcPort)
		if err != nil {
			return errors.Wrap(err, "could not check if all workers are up")
		}
		if int(parallelism) == len(workerStatus) {
			if err := m.a.transitionPipelineState(ctx, pipeline,
				[]pps.PipelineState{pps.PipelineState_PIPELINE_CRASHING},
				pps.PipelineState_PIPELINE_RUNNING, ""); err != nil {
				return errors.Wrap(err, "could not transition pipeline to RUNNING")
			}
			return nil // done--pipeline is out of CRASHING
		}
		return backoff.ErrContinue // loop again to check for new workers
	}, backoff.NewConstantBackOff(crashingBackoff),
		backoff.NotifyContinue("monitorCrashingPipeline for "+pipeline),
	); err != nil && ctx.Err() == nil {
		// retryUntilCancel should exit iff 'ctx' is cancelled, so this should be
		// unreachable (restart master if not)
		log.Fatalf("monitorCrashingPipeline is exiting prematurely which should not happen (error: %v); restarting container...", err)
	}
}

// makeCronCommits makes commits to a single cron input's repo. It's
// a helper function called by monitorPipeline.
func (m *ppsMaster) makeCronCommits(pachClient *client.APIClient, in *pps.Input) error {
	schedule, err := cron.ParseStandard(in.Cron.Spec)
	if err != nil {
		return err // Shouldn't happen, as the input is validated in CreatePipeline
	}
	// make sure there isn't an unfinished commit on the branch
	commitInfo, err := pachClient.InspectCommit(in.Cron.Repo, "master")
	if err != nil && !pfsserver.IsNoHeadErr(err) {
		return err
	} else if commitInfo != nil && commitInfo.Finished == nil {
		// and if there is, delete it
		if err = pachClient.DeleteCommit(in.Cron.Repo, commitInfo.Commit.ID); err != nil {
			return err
		}
	}

	latestTime, err := getLatestCronTime(pachClient, in)
	if err != nil {
		return err
	}

	for {
		// get the time of the next time from the latest time using the cron schedule
		next := schedule.Next(latestTime)
		// and wait until then to make the next commit
		select {
		case <-time.After(time.Until(next)):
		case <-pachClient.Ctx().Done():
			return pachClient.Ctx().Err()
		}
		if err != nil {
			return err
		}

		// We need the DeleteFile and the PutFile to happen in the same commit
		_, err = pachClient.StartCommit(in.Cron.Repo, "master")
		if err != nil {
			return err
		}
		if in.Cron.Overwrite {
			// get rid of any files, so the new file "overwrites" previous runs
			err = pachClient.DeleteFile(in.Cron.Repo, "master", "")
			if err != nil && !isNotFoundErr(err) && !pfsserver.IsNoHeadErr(err) {
				return errors.Wrapf(err, "delete error")
			}
		}

		// Put in an empty file named by the timestamp
		_, err = pachClient.PutFile(in.Cron.Repo, "master", next.Format(time.RFC3339), strings.NewReader(""))
		if err != nil {
			return errors.Wrapf(err, "put error")
		}

		err = pachClient.FinishCommit(in.Cron.Repo, "master")
		if err != nil {
			return err
		}

		// set latestTime to the next time
		latestTime = next
	}
}

// getLatestCronTime is a helper used by m.makeCronCommits. It figures out what
// 'in's most recently executed cron tick was and returns it (or, if no cron
// ticks are in 'in's cron repo, it retuns the 'Start' time set in 'in.Cron'
// (typically set by 'pachctl extract')
func getLatestCronTime(pachClient *client.APIClient, in *pps.Input) (time.Time, error) {
	var latestTime time.Time
	files, err := pachClient.ListFile(in.Cron.Repo, "master", "")
	if err != nil && !pfsserver.IsNoHeadErr(err) {
		return latestTime, err
	} else if err != nil || len(files) == 0 {
		// File not found, this happens the first time the pipeline is run
		latestTime, err = types.TimestampFromProto(in.Cron.Start)
		if err != nil {
			return latestTime, err
		}
	} else {
		// Take the name of the most recent file as the latest timestamp
		// ListFile returns the files in lexicographical order, and the RFC3339 format goes
		// from largest unit of time to smallest, so the most recent file will be the last one
		latestTime, err = time.Parse(time.RFC3339, path.Base(files[len(files)-1].File.Path))
		if err != nil {
			return latestTime, err
		}
	}
	return latestTime, nil
}

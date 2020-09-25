package server

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pachyderm/pachyderm/src/client"
	"github.com/pachyderm/pachyderm/src/client/pkg/errors"
	"github.com/pachyderm/pachyderm/src/client/pps"
	"github.com/pachyderm/pachyderm/src/server/pfs/s3"
	"github.com/pachyderm/pachyderm/src/server/pkg/backoff"
	col "github.com/pachyderm/pachyderm/src/server/pkg/collection"
	"github.com/pachyderm/pachyderm/src/server/pkg/dlock"
	"github.com/pachyderm/pachyderm/src/server/pkg/ppsconsts"
	"github.com/pachyderm/pachyderm/src/server/pkg/ppsdb"
	"github.com/pachyderm/pachyderm/src/server/pkg/ppsutil"
	"github.com/pachyderm/pachyderm/src/server/pkg/watch"
	logrus "github.com/sirupsen/logrus"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	s3gSidecarLockPath = "_s3g_sidecar_lock"
)

type sidecarS3G struct {
	apiServer    *apiServer
	pipelineInfo *pps.PipelineInfo
	pachClient   *client.APIClient

	serversMu sync.Mutex
	servers   map[string]*http.Server
}

func (a *apiServer) ServeSidecarS3G() {
	s := &sidecarS3G{
		apiServer:    a,
		pipelineInfo: &pps.PipelineInfo{}, // populate below
		pachClient:   a.env.GetPachClient(context.Background()),
		servers:      make(map[string]*http.Server),
	}

	// Read spec commit for this sidecar's pipeline, and set auth token for pach
	// client
	specCommit := a.env.PPSSpecCommitID
	if specCommit == "" {
		// This error is not recoverable
		panic("cannot serve sidecar S3 gateway if no spec commit is set")
	}
	if err := backoff.RetryNotify(func() error {
		retryCtx, retryCancel := context.WithCancel(context.Background())
		defer retryCancel()
		if err := a.sudo(s.pachClient.WithCtx(retryCtx), func(superUserClient *client.APIClient) error {
			s.pipelineInfo.Reset()
			buf := bytes.Buffer{}
			if err := superUserClient.GetFile(ppsconsts.SpecRepo, specCommit, ppsconsts.SpecFile, 0, 0, &buf); err != nil {
				return errors.Wrapf(err, "could not read existing PipelineInfo from PFS")
			}
			if err := s.pipelineInfo.Unmarshal(buf.Bytes()); err != nil {
				return errors.Wrapf(err, "could not unmarshal PipelineInfo bytes from PFS")
			}
			return nil
		}); err != nil {
			return errors.Wrapf(err, "sidecar s3 gateway: could not read pipeline spec commit")
		}
		if !ppsutil.ContainsS3Inputs(s.pipelineInfo.Input) && !s.pipelineInfo.S3Out {
			return nil // break out of backoff (nothing to serve via S3 gateway)
		}

		// Set auth token for s.pachClient (pipelinePtr.AuthToken will be empty if
		// auth is off)
		pipelineName := s.pipelineInfo.Pipeline.Name
		pipelinePtr := &pps.EtcdPipelineInfo{}
		err := a.pipelines.ReadOnly(retryCtx).Get(pipelineName, pipelinePtr)
		if err != nil {
			return errors.Wrapf(err, "could not get auth token from etcdPipelineInfo")
		}
		s.pachClient.SetAuthToken(pipelinePtr.AuthToken)
		return nil
	}, backoff.NewInfiniteBackOff(), func(err error, d time.Duration) error {
		logrus.Errorf("error starting sidecar s3 gateway: %v; retrying in %d", err, d)
		return nil
	}); err != nil {
		// This code should never run, but I hesitate to introduce a panic to new
		// code
		logrus.Errorf("restarting startup of sidecar s3 gateway: %v", err)
		a.ServeSidecarS3G()
	}
	if !ppsutil.ContainsS3Inputs(s.pipelineInfo.Input) && !s.pipelineInfo.S3Out {
		return // break early (nothing to serve via S3 gateway)
	}

	// begin creating k8s services and s3 gateway instances for each job
	done := make(chan string)
	go func() {
		s.createK8sServices()
		done <- "createK8sServices"
	}()
	go func() {
		s.serveS3Instances()
		done <- "serveS3Instances"
	}()
	finisher := <-done
	panic(
		fmt.Sprintf("sidecar s3 gateway: %s is exiting, which should never happen", finisher),
	)
}

type jobHandler interface {
	// OnCreate runs when a job is created. Should be idempotent.
	OnCreate(ctx context.Context, jobInfo *pps.JobInfo)

	// OnTerminate runs when a job ends. Should be idempotent.
	OnTerminate(ctx context.Context, jobInfo *pps.JobInfo)
}

func (s *sidecarS3G) serveS3Instances() {
	// Watch for new jobs & initialize s3g for each new job
	(&handleJobsCtx{
		s: s,
		h: &s3InstanceCreatingJobHandler{s},
	}).start()
}

func (s *sidecarS3G) createK8sServices() {
	logrus.Infof("Launching sidecar s3 gateway master process")
	// createK8sServices goes through master election so that only one k8s service
	// is created per pachyderm job running sidecar s3 gateway
	backoff.RetryNotify(func() error {
		masterLock := dlock.NewDLock(s.apiServer.env.GetEtcdClient(),
			path.Join(s.apiServer.etcdPrefix,
				s3gSidecarLockPath,
				s.pipelineInfo.Pipeline.Name,
				s.pipelineInfo.Salt))
		//lint:ignore SA4006 'env' is passed to 'Unlock' below
		ctx, err := masterLock.Lock(s.pachClient.Ctx())
		if err != nil {
			// retry obtaining lock
			return errors.Wrapf(err, "error obtaining mastership")
		}

		// Watch for new jobs & create kubernetes service for each new job
		(&handleJobsCtx{
			s: s,
			h: &k8sServiceCreatingJobHandler{s},
		}).start()

		// Retry the unlock inside the larger retry as other sidecars may not be
		// able to obtain mastership until the key expires if unlock is unsuccessful
		if err := backoff.RetryNotify(func() error {
			return masterLock.Unlock(ctx)
		}, backoff.NewInfiniteBackOff(), func(err error, d time.Duration) error {
			logrus.Errorf("Error releasing sidecar s3 gateway master lock: %v; retrying in %v", err, d)
			return nil // always retry
		}); err != nil {
			return errors.Wrapf(err, "permanent error releasing sidecar s3 gateway master lock")
		}
		return nil
	}, backoff.NewInfiniteBackOff(), func(err error, d time.Duration) error {
		logrus.Errorf("sidecar s3 gateway: %v; retrying in %v", err, d)
		return nil
	})
}

type s3InstanceCreatingJobHandler struct {
	s *sidecarS3G
}

func (s *s3InstanceCreatingJobHandler) OnCreate(ctx context.Context, jobInfo *pps.JobInfo) {
	jobID := jobInfo.Job.ID

	// serve new S3 gateway & add to s.servers
	s.s.serversMu.Lock()
	defer s.s.serversMu.Unlock()
	if _, ok := s.s.servers[jobID]; ok {
		return // s3g handler already created
	}

	// Initialize new S3 gateway
	var inputBuckets []*s3.Bucket
	pps.VisitInput(jobInfo.Input, func(in *pps.Input) {
		if in.Pfs != nil && in.Pfs.S3 {
			inputBuckets = append(inputBuckets, &s3.Bucket{
				Repo:   in.Pfs.Repo,
				Commit: in.Pfs.Commit,
				Name:   in.Pfs.Name,
			})
		}
	})
	var outputBucket *s3.Bucket
	if s.s.pipelineInfo.S3Out {
		outputBucket = &s3.Bucket{
			Repo:   jobInfo.OutputCommit.Repo.Name,
			Commit: jobInfo.OutputCommit.ID,
			Name:   "out",
		}
	}
	driver := s3.NewWorkerDriver(inputBuckets, outputBucket)
	// TODO(msteffen) always serve on the same port for now (there shouldn't be
	// more than one job in s.servers). When parallel jobs are implemented, the
	// servers in s.servers won't actually serve anymore, and instead parent
	// server will forward requests based on the request hostname
	port := s.s.apiServer.env.S3GatewayPort
	strport := strconv.FormatInt(int64(port), 10)
	var server *http.Server
	err := backoff.RetryNotify(func() error {
		var err error
		server, err = s3.Server(port, driver, func() (*client.APIClient, error) {
			return s.s.apiServer.env.GetPachClient(s.s.pachClient.Ctx()), nil // clones s.pachClient
		})
		if err != nil {
			return errors.Wrapf(err, "couldn't initialize s3 gateway server")
		}
		server.Addr = ":" + strport
		return nil
	}, backoff.NewExponentialBackOff(), func(err error, d time.Duration) error {
		logrus.Errorf("error creating sidecar s3 gateway handler for %q: %v; retrying in %v", jobID, err, d)
		return nil
	})
	if err != nil {
		logrus.Errorf("permanent error creating sidecar s3 gateway handler for %q: %v", jobID, err)
		return // give up. Worker will fail the job
	}
	go func() {
		for i := 0; i < 2; i++ { // If too many errors, the worker will fail the job
			err := server.ListenAndServe()
			if err == nil || errors.Is(err, http.ErrServerClosed) {
				break // server was shutdown/closed
			}
			logrus.Errorf("error serving sidecar s3 gateway handler for %q: %v; strike %d/3", jobID, err, i+1)
		}
	}()
	s.s.servers[jobID] = server
}

func (s *s3InstanceCreatingJobHandler) OnTerminate(jobCtx context.Context, jobInfo *pps.JobInfo) {
	jobID := jobInfo.Job.ID
	s.s.serversMu.Lock()
	defer s.s.serversMu.Unlock()
	server, ok := s.s.servers[jobID]
	if !ok {
		return // s3g handler already deleted
	}

	// kill server
	b := backoff.New60sBackOff()
	// be extra slow, because this panics if it can't release the port
	b.MaxElapsedTime = 2 * time.Minute
	if err := backoff.RetryNotify(func() error {
		timeoutCtx, cancel := context.WithTimeout(jobCtx, 10*time.Second)
		defer cancel()
		return server.Shutdown(timeoutCtx)
	}, b, func(err error, d time.Duration) error {
		logrus.Errorf("could not kill sidecar s3 gateway server for job %q: %v; retrying in %v", jobID, err, d)
		return nil
	}); err != nil {
		// last chance -- try calling Close(), and if that doesn't work, force
		// the http server to shut down by panicking
		if err := server.Close(); err != nil {
			// panic here instead of ignoring the error and moving on because
			// otherwise the worker process won't release the s3 gateway port and
			// all future s3 jobs will fail.
			panic(fmt.Sprintf("could not kill sidecar s3 gateway server for job %q: %v; giving up", jobID, err))
		}
	}
	delete(s.s.servers, jobID) // remove server from map no matter what
}

type k8sServiceCreatingJobHandler struct {
	s *sidecarS3G
}

func (s *k8sServiceCreatingJobHandler) S3G() *sidecarS3G {
	return s.s
}

func (s *k8sServiceCreatingJobHandler) OnCreate(ctx context.Context, jobInfo *pps.JobInfo) {
	// Create kubernetes service for the current job ('jobInfo')
	labels := map[string]string{
		"app":       ppsutil.PipelineRcName(jobInfo.Pipeline.Name, jobInfo.PipelineVersion),
		"suite":     "pachyderm",
		"component": "worker",
	}
	service := &v1.Service{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Service",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:   ppsutil.SidecarS3GatewayService(jobInfo.Job.ID, datumID),
			Labels: labels,
		},
		Spec: v1.ServiceSpec{
			Selector: labels,
			// Create a headless service so that the worker's kube proxy doesn't
			// have to get a routing path for the service IP (i.e. the worker kube
			// proxy can have stale routes and clients running inside the worker
			// can still connect)
			ClusterIP: "None",
			Ports: []v1.ServicePort{
				{
					Port: int32(s.s.apiServer.env.S3GatewayPort),
					Name: "s3-gateway-port",
				},
			},
		},
	}

	err := backoff.RetryNotify(func() error {
		_, err := s.s.apiServer.env.GetKubeClient().CoreV1().Services(s.s.apiServer.namespace).Create(service)
		if err != nil && strings.Contains(err.Error(), "already exists") {
			return nil // service already created
		}
		return err
	}, backoff.NewExponentialBackOff(), func(err error, d time.Duration) error {
		logrus.Errorf("error creating kubernetes service for s3 gateway sidecar: %v; retrying in %v", err, d)
		return nil
	})
	if err != nil {
		logrus.Errorf("could not create service for %q: %v", jobInfo.Job.ID, err)
	}
}

func (s *k8sServiceCreatingJobHandler) OnTerminate(_ context.Context, jobInfo *pps.JobInfo) {
	if !ppsutil.ContainsS3Inputs(s.s.pipelineInfo.Input) && !s.s.pipelineInfo.S3Out {
		return // Nothing to delete; this isn't an s3 pipeline (shouldn't happen)
	}

	if err := backoff.RetryNotify(func() error {
		err := s.s.apiServer.env.GetKubeClient().CoreV1().Services(s.s.apiServer.namespace).Delete(
			ppsutil.SidecarS3GatewayService(jobInfo.Job.ID, datumID),
			&metav1.DeleteOptions{OrphanDependents: new(bool) /* false */})
		if err != nil && strings.Contains(err.Error(), "not found") {
			return nil // service already deleted
		}
		return err
	}, backoff.NewExponentialBackOff(), func(err error, d time.Duration) error {
		logrus.Errorf("error deleting kubernetes service for s3 %q gateway sidecar: %v; retrying in %v", jobInfo.Job.ID, err, d)
		return nil
	}); err != nil {
		logrus.Errorf("permanent error deleting kubernetes service for %q s3 gateway sidecar: %v", jobInfo.Job.ID, err)
	}
}

type handleJobsCtx struct {
	s *sidecarS3G
	h jobHandler
}

func (h *handleJobsCtx) start() {
	defer func() {
		panic("sidecar s3 gateway: start() is exiting; this should never happen")
	}()
	for { // reestablish watch in a loop, in case there's a watch error
		var watcher watch.Watcher
		backoff.Retry(func() error {
			var err error
			watcher, err = h.s.apiServer.jobs.ReadOnly(context.Background()).WatchByIndex(
				ppsdb.JobsPipelineIndex, h.s.pipelineInfo.Pipeline)
			if err != nil {
				return errors.Wrapf(err, "error creating watch")
			}
			return nil
		}, backoff.NewInfiniteBackOff())
		defer watcher.Close()

		for e := range watcher.Watch() {
			jobID := string(e.Key)
			if e.Type == watch.EventError {
				logrus.Errorf("sidecar s3 gateway watch error: %v", e.Err)
				break // reestablish watch
			}

			// create new ctx for this job
			jobCtx, jobCancel := context.WithCancel(context.Background())
			h.processJobEvent(jobCtx, e.Type, jobID)
			// spin off handler for job termination. 'watcher' will not see any job
			// state updates after the first because job state updates don't update
			// the pipelines index, so this establishes a watcher that will.
			go h.end(jobCtx, jobCancel, jobID)
		}
	}
}

// end watches 'jobID' and calls h.OnTerminate() when the job finishes.
func (h *handleJobsCtx) end(ctx context.Context, cancel func(), jobID string) {
	defer cancel()
	for { // reestablish watch in a loop, in case there's a watch error
		var watcher watch.Watcher
		backoff.Retry(func() error {
			var err error
			watcher, err = h.s.apiServer.jobs.ReadOnly(ctx).WatchOne(jobID)
			if err != nil {
				return errors.Wrapf(err, "error creating watch")
			}
			return nil
		}, backoff.NewInfiniteBackOff())
		defer watcher.Close()

		for e := range watcher.Watch() {
			jobID := string(e.Key)
			if e.Type == watch.EventError {
				logrus.Errorf("sidecar s3 gateway watch job %q error: %v", jobID, e.Err)
				break // reestablish watch
			}
			h.processJobEvent(ctx, e.Type, jobID)
		}
	}
}

func (h *handleJobsCtx) processJobEvent(jobCtx context.Context, t watch.EventType, jobID string) {
	if t == watch.EventDelete {
		h.h.OnTerminate(jobCtx, jobID)
		return
	}
	// 'e' is a Put event (new or updated job)
	pachClient := h.s.pachClient.WithCtx(jobCtx)
	// Inspect the job and make sure it's relevant, as this worker may be old
	logrus.Infof("sidecar s3 gateway: inspecting job %q to begin serving inputs over s3 gateway", jobID)

	var jobInfo *pps.JobInfo
	if err := backoff.RetryNotify(func() error {
		var err error
		jobInfo, err = pachClient.InspectJob(jobID, false)
		if err != nil {
			if col.IsErrNotFound(err) {
				// TODO(msteffen): I'm not sure what this means--maybe that the service
				// was created and immediately deleted, and there's a pending deletion
				// event? In any case, without a job that exists there's nothing to act on
				logrus.Errorf("sidecar s3 gateway: job %q not found", jobID)
				return nil
			}
			return err
		}
		return nil
	}, backoff.NewExponentialBackOff(), func(err error, d time.Duration) error {
		logrus.Errorf("error inspecting job %q: %v; retrying in %v", jobID, err, d)
		return nil
	}); err != nil {
		logrus.Errorf("permanent error inspecting job %q: %v", jobID, err)
		return // leak the job; better than getting stuck?
	}
	if jobInfo.PipelineVersion < h.s.pipelineInfo.Version {
		logrus.Infof("skipping job %v as it uses old pipeline version %d", jobID, jobInfo.PipelineVersion)
		return
	}
	if jobInfo.PipelineVersion > h.s.pipelineInfo.Version {
		logrus.Infof("skipping job %q as its pipeline version version %d is "+
			"greater than this worker's pipeline version (%d), this should "+
			"automatically resolve when the worker is updated", jobID,
			jobInfo.PipelineVersion, h.s.pipelineInfo.Version)
		return
	}
	if ppsutil.IsTerminal(jobInfo.State) {
		h.h.OnTerminate(jobCtx, jobID)
		return
	}

	// 1. Read [datums object] for jobInfo
	// Q: What is [datums object]?
	// for _, datum := range ([datums object]) {
	h.h.OnCreate(jobCtx, jobInfo /*, datum */)
	// }
}

package main

import (
	"context"
	"path"
	"time"

	"github.com/pachyderm/pachyderm/src/client"
	debugclient "github.com/pachyderm/pachyderm/src/client/debug"
	"github.com/pachyderm/pachyderm/src/client/pkg/errors"
	"github.com/pachyderm/pachyderm/src/client/pkg/grpcutil"
	"github.com/pachyderm/pachyderm/src/client/pkg/tracing"
	"github.com/pachyderm/pachyderm/src/client/pps"
	"github.com/pachyderm/pachyderm/src/client/version"
	"github.com/pachyderm/pachyderm/src/client/version/versionpb"
	"github.com/pachyderm/pachyderm/src/server/cmd/worker/assets"
	debugserver "github.com/pachyderm/pachyderm/src/server/debug/server"
	"github.com/pachyderm/pachyderm/src/server/pkg/cmdutil"
	logutil "github.com/pachyderm/pachyderm/src/server/pkg/log"
	"github.com/pachyderm/pachyderm/src/server/pkg/ppsutil"
	"github.com/pachyderm/pachyderm/src/server/pkg/serviceenv"
	"github.com/pachyderm/pachyderm/src/server/worker"
	workerserver "github.com/pachyderm/pachyderm/src/server/worker/server"

	etcd "github.com/coreos/etcd/clientv3"
	log "github.com/sirupsen/logrus"
)

func main() {
	log.SetFormatter(logutil.FormatterFunc(logutil.Pretty))

	// Copy certs embedded via go-bindata to /etc/ssl/certs. Because the
	// container running this app is user-specified, we don't otherwise have
	// control over the certs that are available.
	//
	// If an error occurs, don't hard-fail, but do record if any certs are
	// known to be missing so we can inform the user.
	if err := assets.RestoreAssets("/", "etc/ssl/certs"); err != nil {
		log.Warnf("failed to inject TLS certs: %v", err)
	}

	cmdutil.Main(do, &serviceenv.WorkerFullConfiguration{})
}

// getPipelineInfo gets the PipelineInfo proto describing the pipeline that this
// worker is part of.
// getPipelineInfo has the side effect of adding auth to the passed pachClient
// which is necessary to get the PipelineInfo from pfs.
func getPipelineInfo(pachClient *client.APIClient, env *serviceenv.ServiceEnv) (*pps.PipelineInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	resp, err := env.GetEtcdClient().Get(ctx, path.Join(env.PPSEtcdPrefix, "pipelines", env.PPSPipelineName))
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) != 1 {
		return nil, errors.Errorf("expected to find 1 pipeline (%s), got %d: %v", env.PPSPipelineName, len(resp.Kvs), resp)
	}
	var pipelinePtr pps.EtcdPipelineInfo
	if err := pipelinePtr.Unmarshal(resp.Kvs[0].Value); err != nil {
		return nil, err
	}
	pachClient.SetAuthToken(pipelinePtr.AuthToken)
	// Notice we use the SpecCommitID from our env, not from etcd. This is
	// because the value in etcd might get updated while the worker pod is
	// being created and we don't want to run the transform of one version of
	// the pipeline in the image of a different verison.
	pipelinePtr.SpecCommit.ID = env.PPSSpecCommitID
	return ppsutil.GetPipelineInfo(pachClient, env.PPSPipelineName, &pipelinePtr)
}

func do(config interface{}) error {
	// must run InstallJaegerTracer before InitWithKube/pach client initialization
	tracing.InstallJaegerTracerFromEnv()
	env := serviceenv.InitServiceEnv(serviceenv.NewConfiguration(config))

	// Construct a client that connects to the sidecar.
	pachClient := env.GetPachClient(context.Background())
	pipelineInfo, err := getPipelineInfo(pachClient, env) // get pipeline creds for pachClient
	if err != nil {
		return errors.Wrapf(err, "error getting pipelineInfo")
	}

	// Construct worker API server.
	workerRcName := ppsutil.PipelineRcName(pipelineInfo.Pipeline.Name, pipelineInfo.Version)
	workerInstance, err := worker.NewWorker(env, pachClient, pipelineInfo, "/")
	if err != nil {
		return err
	}

	// Start worker api server
	server, err := grpcutil.NewServer(context.Background(), false)
	if err != nil {
		return err
	}

	workerserver.RegisterWorkerServer(server.Server, workerInstance.APIServer)
	versionpb.RegisterAPIServer(server.Server, version.NewAPIServer(version.Version, version.APIServerOptions{}))
	debugclient.RegisterDebugServer(server.Server, debugserver.NewDebugServer(env, env.PodName, pachClient))

	// Put our IP address into etcd, so pachd can discover us
	key := path.Join(env.PPSEtcdPrefix, workerserver.WorkerEtcdPrefix, workerRcName, env.PPSWorkerIP)

	// Prepare to write "key" into etcd by creating lease -- if worker dies, our
	// IP will be removed from etcd
	ctx, cancel := context.WithTimeout(pachClient.Ctx(), 10*time.Second)
	defer cancel()

	resp, err := env.GetEtcdClient().Grant(ctx, 10 /* seconds */)
	if err != nil {
		return errors.Wrapf(err, "error granting lease")
	}

	// keepalive forever
	if _, err := env.GetEtcdClient().KeepAlive(context.Background(), resp.ID); err != nil {
		return errors.Wrapf(err, "error with KeepAlive")
	}

	// Actually write "key" into etcd
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second) // new ctx
	defer cancel()
	if _, err := env.GetEtcdClient().Put(ctx, key, "", etcd.WithLease(resp.ID)); err != nil {
		return errors.Wrapf(err, "error putting IP address")
	}

	// If server ever exits, return error
	if _, err := server.ListenTCP("", env.PPSWorkerPort); err != nil {
		return err
	}
	return server.Wait()
}

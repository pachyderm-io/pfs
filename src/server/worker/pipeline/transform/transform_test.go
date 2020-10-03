package transform

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gogo/protobuf/types"
	"golang.org/x/sync/errgroup"

	"github.com/pachyderm/pachyderm/src/client"
	"github.com/pachyderm/pachyderm/src/client/pfs"
	"github.com/pachyderm/pachyderm/src/client/pkg/errors"
	"github.com/pachyderm/pachyderm/src/client/pkg/require"
	"github.com/pachyderm/pachyderm/src/client/pps"
	"github.com/pachyderm/pachyderm/src/server/pkg/backoff"
	col "github.com/pachyderm/pachyderm/src/server/pkg/collection"
	"github.com/pachyderm/pachyderm/src/server/pkg/obj"
	"github.com/pachyderm/pachyderm/src/server/pkg/ppsutil"
	txnenv "github.com/pachyderm/pachyderm/src/server/pkg/transactionenv"
	"github.com/pachyderm/pachyderm/src/server/pkg/uuid"
	"github.com/pachyderm/pachyderm/src/server/pkg/work"
)

func withWorkerSpawnerPair(pipelineInfo *pps.PipelineInfo, cb func(env *testEnv) error) error {
	// We only support simple pfs input pipelines in this test suite at the moment
	if pipelineInfo.Input == nil || pipelineInfo.Input.Pfs == nil {
		return errors.New("invalid pipeline, only a single PFS input is supported")
	}

	var eg *errgroup.Group

	err := withTestEnv(pipelineInfo, func(env *testEnv) error {
		var ctx context.Context
		eg, ctx = errgroup.WithContext(env.driver.PachClient().Ctx())
		env.driver = env.driver.WithContext(ctx)
		env.PachClient = env.driver.PachClient()

		// Set env vars that the object storage layer expects in the env
		// This is global but it should be fine because all tests use the same value.
		if err := os.Setenv(obj.StorageBackendEnvVar, obj.Local); err != nil {
			return err
		}

		if err := os.MkdirAll(env.ServiceEnv.StorageRoot, 0777); err != nil {
			return err
		}
		// TODO: this is global and complicates running tests in parallel
		if err := os.Setenv(obj.PachRootEnvVar, env.ServiceEnv.StorageRoot); err != nil {
			return err
		}

		// Set up repos and branches for the pipeline
		input := pipelineInfo.Input.Pfs
		if err := env.PachClient.CreateRepo(input.Repo); err != nil {
			return err
		}
		if err := env.PachClient.CreateBranch(input.Repo, input.Branch, "", nil); err != nil {
			return err
		}

		if err := env.PachClient.CreateBranch(pipelineInfo.SpecCommit.Repo.Name, pipelineInfo.Pipeline.Name, "", nil); err != nil {
			return err
		}
		commit, err := env.PachClient.StartCommit(pipelineInfo.SpecCommit.Repo.Name, pipelineInfo.Pipeline.Name)
		if err != nil {
			return err
		}
		pipelineInfo.SpecCommit = commit
		if err := env.PachClient.FinishCommit(pipelineInfo.SpecCommit.Repo.Name, commit.ID); err != nil {
			return err
		}
		if err := env.PachClient.CreateRepo(pipelineInfo.Pipeline.Name); err != nil {
			return err
		}
		if err := env.PachClient.CreateBranch(
			pipelineInfo.Pipeline.Name,
			pipelineInfo.OutputBranch,
			"",
			[]*pfs.Branch{
				client.NewBranch(input.Repo, input.Branch),
				client.NewBranch(pipelineInfo.SpecCommit.Repo.Name, pipelineInfo.Pipeline.Name),
			},
		); err != nil {
			return err
		}

		// Put the pipeline info into etcd (which is read by the master)
		if _, err = env.driver.NewSTM(func(stm col.STM) error {
			etcdPipelineInfo := &pps.EtcdPipelineInfo{
				State:       pps.PipelineState_PIPELINE_STARTING,
				SpecCommit:  pipelineInfo.SpecCommit,
				Parallelism: 1,
			}
			return env.driver.Pipelines().ReadWrite(stm).Put(pipelineInfo.Pipeline.Name, etcdPipelineInfo)
		}); err != nil {
			return err
		}

		eg.Go(func() error {
			err := Run(env.driver, env.logger)
			if err != nil && errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		})

		eg.Go(func() error {
			err := backoff.RetryUntilCancel(env.driver.PachClient().Ctx(), func() error {
				return env.driver.NewTaskWorker().Run(
					env.driver.PachClient().Ctx(),
					func(ctx context.Context, subtask *work.Task) error {
						status := &Status{}
						return Worker(env.driver, env.logger, subtask, status)
					},
				)
			}, &backoff.ZeroBackOff{}, func(err error, d time.Duration) error {
				env.logger.Logf("worker failed, retrying immediately, err: %v", err)
				return nil
			})
			if err != nil && errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		})

		return cb(env)
	})

	workerSpawnerErr := eg.Wait()
	if workerSpawnerErr != nil && errors.Is(workerSpawnerErr, context.Canceled) {
		return workerSpawnerErr
	}
	return err
}

func withTimeout(ctx context.Context, duration time.Duration) context.Context {
	// Create a context that the caller can wait on
	ctx, cancel := context.WithCancel(ctx)

	go func() {
		select {
		case <-ctx.Done():
		case <-time.After(duration):
			fmt.Printf("Canceling test after timeout\n")
			cancel()
		}
	}()

	return ctx
}

func mockBasicJob(t *testing.T, env *testEnv, pi *pps.PipelineInfo) (context.Context, *pps.EtcdJobInfo) {
	// Create a context that the caller can wait on
	ctx, cancel := context.WithCancel(env.PachClient.Ctx())

	// Mock out the initial ListJob, CreateJob, and InspectJob calls
	etcdJobInfo := &pps.EtcdJobInfo{Job: client.NewJob(uuid.NewWithoutDashes())}

	// TODO: use a 'real' pps if we can make one that doesn't need a real kube client
	env.MockPachd.PPS.ListJobStream.Use(func(*pps.ListJobRequest, pps.API_ListJobStreamServer) error {
		return nil
	})

	env.MockPachd.PPS.CreateJob.Use(func(ctx context.Context, request *pps.CreateJobRequest) (*pps.Job, error) {
		etcdJobInfo.OutputCommit = request.OutputCommit
		etcdJobInfo.Pipeline = request.Pipeline
		etcdJobInfo.Stats = request.Stats
		etcdJobInfo.Restart = request.Restart
		etcdJobInfo.DataProcessed = request.DataProcessed
		etcdJobInfo.DataSkipped = request.DataSkipped
		etcdJobInfo.DataTotal = request.DataTotal
		etcdJobInfo.DataFailed = request.DataFailed
		etcdJobInfo.DataRecovered = request.DataRecovered
		etcdJobInfo.StatsCommit = request.StatsCommit
		etcdJobInfo.Started = request.Started
		etcdJobInfo.Finished = request.Finished
		return etcdJobInfo.Job, nil
	})

	env.MockPachd.PPS.InspectJob.Use(func(ctx context.Context, request *pps.InspectJobRequest) (*pps.JobInfo, error) {
		outputCommitInfo, err := env.PachClient.InspectCommit(etcdJobInfo.OutputCommit.Repo.Name, etcdJobInfo.OutputCommit.ID)
		require.NoError(t, err)

		return &pps.JobInfo{
			Job:              etcdJobInfo.Job,
			Pipeline:         etcdJobInfo.Pipeline,
			OutputRepo:       &pfs.Repo{Name: etcdJobInfo.Pipeline.Name},
			OutputCommit:     etcdJobInfo.OutputCommit,
			Restart:          etcdJobInfo.Restart,
			DataProcessed:    etcdJobInfo.DataProcessed,
			DataSkipped:      etcdJobInfo.DataSkipped,
			DataTotal:        etcdJobInfo.DataTotal,
			DataFailed:       etcdJobInfo.DataFailed,
			DataRecovered:    etcdJobInfo.DataRecovered,
			Stats:            etcdJobInfo.Stats,
			StatsCommit:      etcdJobInfo.StatsCommit,
			State:            etcdJobInfo.State,
			Reason:           etcdJobInfo.Reason,
			Started:          etcdJobInfo.Started,
			Finished:         etcdJobInfo.Finished,
			Transform:        pi.Transform,
			PipelineVersion:  pi.Version,
			ParallelismSpec:  pi.ParallelismSpec,
			Egress:           pi.Egress,
			Service:          pi.Service,
			Spout:            pi.Spout,
			OutputBranch:     pi.OutputBranch,
			ResourceRequests: pi.ResourceRequests,
			ResourceLimits:   pi.ResourceLimits,
			Input:            ppsutil.JobInput(pi, outputCommitInfo),
			EnableStats:      pi.EnableStats,
			Salt:             pi.Salt,
			ChunkSpec:        pi.ChunkSpec,
			DatumTimeout:     pi.DatumTimeout,
			JobTimeout:       pi.JobTimeout,
			DatumTries:       pi.DatumTries,
			SchedulingSpec:   pi.SchedulingSpec,
			PodSpec:          pi.PodSpec,
			PodPatch:         pi.PodPatch,
		}, nil
	})

	updateJobState := func(request *pps.UpdateJobStateRequest) {
		if ppsutil.IsTerminal(etcdJobInfo.State) {
			return
		}

		etcdJobInfo.State = request.State
		etcdJobInfo.Reason = request.Reason

		// If setting the job to a terminal state, we are done
		if ppsutil.IsTerminal(request.State) {
			cancel()
		}
	}

	env.MockPPSTransactionServer.UpdateJobStateInTransaction.Use(func(txnctx *txnenv.TransactionContext, request *pps.UpdateJobStateRequest) error {
		updateJobState(request)
		return nil
	})

	env.MockPachd.PPS.UpdateJobState.Use(func(ctx context.Context, request *pps.UpdateJobStateRequest) (*types.Empty, error) {
		updateJobState(request)
		return &types.Empty{}, nil
	})

	return ctx, etcdJobInfo
}

type inputFile struct {
	path     string
	contents string
}

func newInput(path string, contents string) *inputFile {
	return &inputFile{
		path:     path,
		contents: contents,
	}
}

func triggerJob(t *testing.T, env *testEnv, pi *pps.PipelineInfo, files []*inputFile) {
	pfc, err := env.PachClient.NewPutFileClient()
	require.NoError(t, err)

	for _, f := range files {
		_, err := pfc.PutFile(pi.Input.Pfs.Repo, "master", f.path, strings.NewReader(f.contents))
		require.NoError(t, err)
	}
	require.NoError(t, pfc.Close())

	inputCommitInfo, err := env.PachClient.InspectCommit(pi.Input.Pfs.Repo, "master")
	require.NoError(t, err)
	require.Equal(t, int64(1), inputCommitInfo.SubvenantCommitsTotal)
	require.Equal(t, inputCommitInfo.Subvenance[0].Lower, inputCommitInfo.Subvenance[0].Upper)

	outputCommit := inputCommitInfo.Subvenance[0].Lower
	require.Equal(t, pi.Pipeline.Name, outputCommit.Repo.Name)
}

func TestJobSuccess(t *testing.T) {
	pi := defaultPipelineInfo()
	err := withWorkerSpawnerPair(pi, func(env *testEnv) error {
		ctx, etcdJobInfo := mockBasicJob(t, env, pi)
		triggerJob(t, env, pi, []*inputFile{newInput("file", "foobar")})
		ctx = withTimeout(ctx, 10*time.Second)
		<-ctx.Done()
		require.Equal(t, pps.JobState_JOB_SUCCESS, etcdJobInfo.State)

		// Ensure the output commit is successful
		outputCommitID := etcdJobInfo.OutputCommit.ID
		outputCommitInfo, err := env.PachClient.InspectCommit(pi.Pipeline.Name, outputCommitID)
		require.NoError(t, err)
		require.NotNil(t, outputCommitInfo.Finished)

		branchInfo, err := env.PachClient.InspectBranch(pi.Pipeline.Name, pi.OutputBranch)
		require.NoError(t, err)
		require.NotNil(t, branchInfo)

		// Find the output file in the output branch
		files, err := env.PachClient.ListFile(pi.Pipeline.Name, pi.OutputBranch, "/")
		require.NoError(t, err)
		require.Equal(t, 1, len(files))
		require.Equal(t, "/file", files[0].File.Path)
		require.Equal(t, uint64(6), files[0].SizeBytes)

		buffer := &bytes.Buffer{}
		err = env.PachClient.GetFile(pi.Pipeline.Name, pi.OutputBranch, "/file", 0, 0, buffer)
		require.NoError(t, err)
		require.Equal(t, "foobar", buffer.String())

		return nil
	})
	require.NoError(t, err)
}

func TestJobFailedDatum(t *testing.T) {
	pi := defaultPipelineInfo()
	pi.Transform.Cmd = []string{"bash", "-c", "(exit 1)"}
	err := withWorkerSpawnerPair(pi, func(env *testEnv) error {
		ctx, etcdJobInfo := mockBasicJob(t, env, pi)
		triggerJob(t, env, pi, []*inputFile{newInput("file", "foobar")})
		ctx = withTimeout(ctx, 10*time.Second)
		<-ctx.Done()
		require.Equal(t, pps.JobState_JOB_FAILURE, etcdJobInfo.State)
		// TODO: check job stats
		return nil
	})
	require.NoError(t, err)
}

func TestJobMultiDatum(t *testing.T) {
	pi := defaultPipelineInfo()
	err := withWorkerSpawnerPair(pi, func(env *testEnv) error {
		ctx, etcdJobInfo := mockBasicJob(t, env, pi)
		triggerJob(t, env, pi, []*inputFile{newInput("a", "foobar"), newInput("b", "barfoo")})
		ctx = withTimeout(ctx, 10*time.Second)
		<-ctx.Done()
		require.Equal(t, pps.JobState_JOB_SUCCESS, etcdJobInfo.State)

		// Ensure the output commit is successful
		outputCommitID := etcdJobInfo.OutputCommit.ID
		outputCommitInfo, err := env.PachClient.InspectCommit(pi.Pipeline.Name, outputCommitID)
		require.NoError(t, err)
		require.NotNil(t, outputCommitInfo.Finished)

		branchInfo, err := env.PachClient.InspectBranch(pi.Pipeline.Name, pi.OutputBranch)
		require.NoError(t, err)
		require.NotNil(t, branchInfo)

		// Find the output file in the output branch
		files, err := env.PachClient.ListFile(pi.Pipeline.Name, pi.OutputBranch, "/")
		require.NoError(t, err)
		require.Equal(t, 2, len(files))
		require.Equal(t, "/a", files[0].File.Path)
		require.Equal(t, uint64(6), files[0].SizeBytes)
		require.Equal(t, "/b", files[1].File.Path)
		require.Equal(t, uint64(6), files[1].SizeBytes)

		buffer := &bytes.Buffer{}
		err = env.PachClient.GetFile(pi.Pipeline.Name, pi.OutputBranch, "/a", 0, 0, buffer)
		require.NoError(t, err)
		require.Equal(t, "foobar", buffer.String())

		buffer = &bytes.Buffer{}
		err = env.PachClient.GetFile(pi.Pipeline.Name, pi.OutputBranch, "/b", 0, 0, buffer)
		require.NoError(t, err)
		require.Equal(t, "barfoo", buffer.String())

		return nil
	})
	require.NoError(t, err)
}

func TestJobSerial(t *testing.T) {
	pi := defaultPipelineInfo()
	err := withWorkerSpawnerPair(pi, func(env *testEnv) error {
		ctx, etcdJobInfo := mockBasicJob(t, env, pi)
		triggerJob(t, env, pi, []*inputFile{newInput("a", "foobar")})
		ctx = withTimeout(ctx, 10*time.Second)
		<-ctx.Done()
		require.Equal(t, pps.JobState_JOB_SUCCESS, etcdJobInfo.State)

		ctx, etcdJobInfo = mockBasicJob(t, env, pi)
		triggerJob(t, env, pi, []*inputFile{newInput("b", "barfoo")})
		ctx = withTimeout(ctx, 10*time.Second)
		<-ctx.Done()
		require.Equal(t, pps.JobState_JOB_SUCCESS, etcdJobInfo.State)

		// Ensure the output commit is successful
		outputCommitID := etcdJobInfo.OutputCommit.ID
		outputCommitInfo, err := env.PachClient.InspectCommit(pi.Pipeline.Name, outputCommitID)
		require.NoError(t, err)
		require.NotNil(t, outputCommitInfo.Finished)

		branchInfo, err := env.PachClient.InspectBranch(pi.Pipeline.Name, pi.OutputBranch)
		require.NoError(t, err)
		require.NotNil(t, branchInfo)

		// Find the output file in the output branch
		files, err := env.PachClient.ListFile(pi.Pipeline.Name, pi.OutputBranch, "/")
		require.NoError(t, err)
		require.Equal(t, 2, len(files))
		require.Equal(t, "/a", files[0].File.Path)
		require.Equal(t, uint64(6), files[0].SizeBytes)
		require.Equal(t, "/b", files[1].File.Path)
		require.Equal(t, uint64(6), files[1].SizeBytes)

		buffer := &bytes.Buffer{}
		err = env.PachClient.GetFile(pi.Pipeline.Name, pi.OutputBranch, "/a", 0, 0, buffer)
		require.NoError(t, err)
		require.Equal(t, "foobar", buffer.String())

		buffer = &bytes.Buffer{}
		err = env.PachClient.GetFile(pi.Pipeline.Name, pi.OutputBranch, "/b", 0, 0, buffer)
		require.NoError(t, err)
		require.Equal(t, "barfoo", buffer.String())

		return nil
	})
	require.NoError(t, err)
}

// TestJobEgress is identical to TestJobSuccess except it includes the egress
// stage. The implementation of egress is mocked to short-circuit success, so
// this really just tests the state machine progresses all the way through.
func TestJobEgress(t *testing.T) {
	pi := defaultPipelineInfo()
	pi.Egress = &pps.Egress{URL: "http://example.com"}
	err := withWorkerSpawnerPair(pi, func(env *testEnv) error {
		ctx, etcdJobInfo := mockBasicJob(t, env, pi)
		triggerJob(t, env, pi, []*inputFile{newInput("a", "foobar")})
		ctx = withTimeout(ctx, 10*time.Second)
		<-ctx.Done()
		require.Equal(t, pps.JobState_JOB_SUCCESS, etcdJobInfo.State)

		// Ensure the output commit is successful
		outputCommitID := etcdJobInfo.OutputCommit.ID
		outputCommitInfo, err := env.PachClient.InspectCommit(pi.Pipeline.Name, outputCommitID)
		require.NoError(t, err)
		require.NotNil(t, outputCommitInfo.Finished)

		branchInfo, err := env.PachClient.InspectBranch(pi.Pipeline.Name, pi.OutputBranch)
		require.NoError(t, err)
		require.NotNil(t, branchInfo)

		// Find the output file in the output branch
		files, err := env.PachClient.ListFile(pi.Pipeline.Name, pi.OutputBranch, "/")
		require.NoError(t, err)
		require.Equal(t, 1, len(files))
		require.Equal(t, "/a", files[0].File.Path)
		require.Equal(t, uint64(6), files[0].SizeBytes)

		buffer := &bytes.Buffer{}
		err = env.PachClient.GetFile(pi.Pipeline.Name, pi.OutputBranch, "/a", 0, 0, buffer)
		require.NoError(t, err)
		require.Equal(t, "foobar", buffer.String())

		return nil
	})
	require.NoError(t, err)
}

package transform

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	"github.com/jmoiron/sqlx"
	"golang.org/x/sync/errgroup"

	"github.com/pachyderm/pachyderm/v2/src/client"
	"github.com/pachyderm/pachyderm/v2/src/internal/backoff"
	"github.com/pachyderm/pachyderm/v2/src/internal/errors"
	"github.com/pachyderm/pachyderm/v2/src/internal/obj"
	"github.com/pachyderm/pachyderm/v2/src/internal/ppsutil"
	"github.com/pachyderm/pachyderm/v2/src/internal/require"
	"github.com/pachyderm/pachyderm/v2/src/internal/serviceenv"
	"github.com/pachyderm/pachyderm/v2/src/internal/tarutil"
	"github.com/pachyderm/pachyderm/v2/src/internal/testutil"
	"github.com/pachyderm/pachyderm/v2/src/internal/transactionenv/txncontext"
	"github.com/pachyderm/pachyderm/v2/src/internal/uuid"
	"github.com/pachyderm/pachyderm/v2/src/internal/work"
	"github.com/pachyderm/pachyderm/v2/src/pfs"
	"github.com/pachyderm/pachyderm/v2/src/pps"
)

func newWorkerSpawnerPair(t *testing.T, dbConfig serviceenv.ConfigOption, pipelineInfo *pps.PipelineInfo) *testEnv {
	// We only support simple pfs input pipelines in this test suite at the moment
	require.NotNil(t, pipelineInfo.Details.Input)
	require.NotNil(t, pipelineInfo.Details.Input.Pfs)

	env := newTestEnv(t, dbConfig, pipelineInfo)

	eg, ctx := errgroup.WithContext(env.driver.PachClient().Ctx())
	t.Cleanup(func() {
		require.NoError(t, eg.Wait())
	})

	env.driver = env.driver.WithContext(ctx)
	env.PachClient = env.driver.PachClient()

	// Set env vars that the object storage layer expects in the env
	// This is global but it should be fine because all tests use the same value.
	require.NoError(t, os.Setenv(obj.StorageBackendEnvVar, obj.Local))
	require.NoError(t, os.MkdirAll(env.ServiceEnv.Config().StorageRoot, 0777))

	// Set up the input repo and branch
	input := pipelineInfo.Details.Input.Pfs
	require.NoError(t, env.PachClient.CreateRepo(input.Repo))
	require.NoError(t, env.PachClient.CreateBranch(input.Repo, input.Branch, "", "", nil))

	// Create the output repo
	pipelineRepo := client.NewRepo(pipelineInfo.Pipeline.Name)
	_, err := env.PachClient.PfsAPIClient.CreateRepo(ctx, &pfs.CreateRepoRequest{Repo: pipelineRepo})
	require.NoError(t, err)

	// Create the spec system repo and create the initial spec commit
	specRepo := client.NewSystemRepo(pipelineInfo.Pipeline.Name, pfs.SpecRepoType)
	_, err = env.PachClient.PfsAPIClient.CreateRepo(ctx, &pfs.CreateRepoRequest{Repo: specRepo})
	require.NoError(t, err)
	specCommit, err := env.PachClient.PfsAPIClient.StartCommit(ctx, &pfs.StartCommitRequest{Branch: specRepo.NewBranch("master")})
	require.NoError(t, err)
	_, err = env.PachClient.PfsAPIClient.FinishCommit(ctx, &pfs.FinishCommitRequest{Commit: specCommit})
	require.NoError(t, err)

	// Make the output branch provenant on the spec and input branches
	_, err = env.PachClient.PfsAPIClient.CreateBranch(ctx, &pfs.CreateBranchRequest{
		Branch: pipelineRepo.NewBranch(pipelineInfo.Details.OutputBranch),
		Provenance: []*pfs.Branch{
			client.NewBranch(input.Repo, input.Branch),
			specRepo.NewBranch("master"),
		},
	})
	require.NoError(t, err)

	// Create the meta system repo and set up the branch provenance
	metaRepo := client.NewSystemRepo(pipelineInfo.Pipeline.Name, pfs.MetaRepoType)
	_, err = env.PachClient.PfsAPIClient.CreateRepo(ctx, &pfs.CreateRepoRequest{Repo: metaRepo})
	require.NoError(t, err)
	_, err = env.PachClient.PfsAPIClient.CreateBranch(ctx, &pfs.CreateBranchRequest{
		Branch:     metaRepo.NewBranch("master"),
		Provenance: []*pfs.Branch{pipelineRepo.NewBranch("master")},
	})
	require.NoError(t, err)

	// Put the pipeline info into the collection (which is read by the master)
	err = env.driver.NewSQLTx(func(sqlTx *sqlx.Tx) error {
		pipelineInfo := &pps.PipelineInfo{
			State:       pps.PipelineState_PIPELINE_STARTING,
			Version:     1,
			SpecCommit:  specCommit,
			Parallelism: 1,
		}
		return env.driver.Pipelines().ReadWrite(sqlTx).Put(pipelineInfo.Pipeline.Name, pipelineInfo)
	})
	require.NoError(t, err)

	eg.Go(func() error {
		if err := Run(env.driver, env.logger); err != nil && !errors.Is(err, context.Canceled) {
			return err
		}
		return nil
	})

	eg.Go(func() error {
		err := backoff.RetryUntilCancel(env.driver.PachClient().Ctx(), func() error {
			return env.driver.NewTaskWorker().Run(
				env.driver.PachClient().Ctx(),
				func(ctx context.Context, subtask *work.Task) (*types.Any, error) {
					status := &Status{}
					return nil, Worker(env.driver, env.logger, subtask, status)
				},
			)
		}, &backoff.ZeroBackOff{}, func(err error, d time.Duration) error {
			env.logger.Logf("worker failed, retrying immediately, err: %v", err)
			return nil
		})
		if err != nil && !errors.Is(err, context.Canceled) {
			return err
		}
		return nil
	})

	return env
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

func mockBasicJob(t *testing.T, env *testEnv, pi *pps.PipelineInfo) (context.Context, *pps.JobInfo) {
	// Create a context that the caller can wait on
	ctx, cancel := context.WithCancel(env.PachClient.Ctx())

	// Mock out the initial ListJob, and InspectJob calls
	jobInfo := &pps.JobInfo{Job: client.NewJob(pi.Pipeline.Name, uuid.NewWithoutDashes())}

	// TODO: use a 'real' pps if we can make one that doesn't need a real kube client
	env.MockPachd.PPS.ListJob.Use(func(*pps.ListJobRequest, pps.API_ListJobServer) error {
		return nil
	})

	env.MockPachd.PPS.InspectJob.Use(func(ctx context.Context, request *pps.InspectJobRequest) (*pps.JobInfo, error) {
		outputCommitInfo, err := env.PachClient.InspectCommit(jobInfo.OutputCommit.Branch.Repo.Name, jobInfo.OutputCommit.Branch.Name, jobInfo.OutputCommit.ID)
		require.NoError(t, err)

		result := proto.Clone(jobInfo).(*pps.JobInfo)
		result.Details = &pps.JobInfo_Details{
			Transform:        pi.Details.Transform,
			ParallelismSpec:  pi.Details.ParallelismSpec,
			Egress:           pi.Details.Egress,
			Service:          pi.Details.Service,
			Spout:            pi.Details.Spout,
			ResourceRequests: pi.Details.ResourceRequests,
			ResourceLimits:   pi.Details.ResourceLimits,
			Input:            ppsutil.JobInput(pi, outputCommitInfo.Commit),
			Salt:             pi.Details.Salt,
			DatumSetSpec:     pi.Details.DatumSetSpec,
			DatumTimeout:     pi.Details.DatumTimeout,
			JobTimeout:       pi.Details.JobTimeout,
			DatumTries:       pi.Details.DatumTries,
			SchedulingSpec:   pi.Details.SchedulingSpec,
			PodSpec:          pi.Details.PodSpec,
			PodPatch:         pi.Details.PodPatch,
		}
		return result, nil
	})

	updateJobState := func(request *pps.UpdateJobStateRequest) {
		if ppsutil.IsTerminal(jobInfo.State) {
			return
		}

		jobInfo.State = request.State
		jobInfo.Reason = request.Reason

		// If setting the job to a terminal state, we are done
		if ppsutil.IsTerminal(request.State) {
			cancel()
		}
	}

	env.MockPPSTransactionServer.UpdateJobStateInTransaction.Use(func(txnCtx *txncontext.TransactionContext, request *pps.UpdateJobStateRequest) error {
		updateJobState(request)
		return nil
	})

	env.MockPachd.PPS.UpdateJobState.Use(func(ctx context.Context, request *pps.UpdateJobStateRequest) (*types.Empty, error) {
		updateJobState(request)
		return &types.Empty{}, nil
	})

	return ctx, jobInfo
}

func triggerJob(t *testing.T, env *testEnv, pi *pps.PipelineInfo, files []tarutil.File) {
	commit, err := env.PachClient.StartCommit(pi.Details.Input.Pfs.Repo, "master")
	require.NoError(t, err)
	buf := &bytes.Buffer{}
	require.NoError(t, tarutil.WithWriter(buf, func(tw *tar.Writer) error {
		for _, f := range files {
			if err := tarutil.WriteFile(tw, f); err != nil {
				return err
			}
		}
		return nil
	}))
	require.NoError(t, env.PachClient.PutFileTAR(commit, buf, client.WithAppendPutFile()))
	require.NoError(t, env.PachClient.FinishCommit(pi.Details.Input.Pfs.Repo, commit.Branch.Name, commit.ID))
}

func testJobSuccess(t *testing.T, env *testEnv, pi *pps.PipelineInfo, files []tarutil.File) {
	ctx, jobInfo := mockBasicJob(t, env, pi)
	triggerJob(t, env, pi, files)
	ctx = withTimeout(ctx, 10*time.Second)
	<-ctx.Done()
	require.Equal(t, pps.JobState_JOB_SUCCESS, jobInfo.State)

	// Ensure the output commit is successful
	outputCommitID := jobInfo.OutputCommit.ID
	outputCommitInfo, err := env.PachClient.InspectCommit(pi.Pipeline.Name, jobInfo.OutputCommit.Branch.Name, outputCommitID)
	require.NoError(t, err)
	require.NotNil(t, outputCommitInfo.Finished)

	branchInfo, err := env.PachClient.InspectBranch(pi.Pipeline.Name, pi.Details.OutputBranch)
	require.NoError(t, err)
	require.NotNil(t, branchInfo)

	r, err := env.PachClient.GetFileTAR(jobInfo.OutputCommit, "/*")
	require.NoError(t, err)
	require.NoError(t, tarutil.Iterate(r, func(file tarutil.File) error {
		ok, err := tarutil.Equal(files[0], file)
		require.NoError(t, err)
		require.True(t, ok)
		files = files[1:]
		return nil
	}))
}

func TestTransformPipeline(suite *testing.T) {
	suite.Parallel()

	suite.Run("TestJobSuccess", func(t *testing.T) {
		t.Parallel()
		pi := defaultPipelineInfo()
		env := newWorkerSpawnerPair(t, testutil.NewTestDBConfig(t), pi)
		testJobSuccess(t, env, pi, []tarutil.File{
			tarutil.NewMemFile("/file", []byte("foobar")),
		})
	})

	suite.Run("TestJobSuccessEgress", func(t *testing.T) {
		t.Parallel()
		objC, bucket := testutil.NewObjectClient(t)
		pi := defaultPipelineInfo()
		pi.Details.Egress = &pps.Egress{URL: fmt.Sprintf("local://%s/", bucket)}
		env := newWorkerSpawnerPair(t, testutil.NewTestDBConfig(t), pi)

		files := []tarutil.File{
			tarutil.NewMemFile("/file1", []byte("foo")),
			tarutil.NewMemFile("/file2", []byte("bar")),
		}
		testJobSuccess(t, env, pi, files)
		for _, file := range files {
			hdr, err := file.Header()
			require.NoError(t, err)

			buf1 := &bytes.Buffer{}
			require.NoError(t, file.Content(buf1))

			buf2 := &bytes.Buffer{}
			err = objC.Get(context.Background(), hdr.Name, buf2)
			require.NoError(t, err)

			require.True(t, bytes.Equal(buf1.Bytes(), buf2.Bytes()))
		}
	})

	suite.Run("TestJobFailedDatum", func(t *testing.T) {
		t.Parallel()
		pi := defaultPipelineInfo()
		env := newWorkerSpawnerPair(t, testutil.NewTestDBConfig(t), pi)

		pi.Details.Transform.Cmd = []string{"bash", "-c", "(exit 1)"}
		ctx, jobInfo := mockBasicJob(t, env, pi)
		tarFiles := []tarutil.File{
			tarutil.NewMemFile("/file", []byte("foobar")),
		}
		triggerJob(t, env, pi, tarFiles)
		ctx = withTimeout(ctx, 10*time.Second)
		<-ctx.Done()
		require.Equal(t, pps.JobState_JOB_FAILURE, jobInfo.State)
		// TODO: check job stats
	})

	suite.Run("TestJobMultiDatum", func(t *testing.T) {
		t.Parallel()
		pi := defaultPipelineInfo()
		env := newWorkerSpawnerPair(t, testutil.NewTestDBConfig(t), pi)

		ctx, jobInfo := mockBasicJob(t, env, pi)
		tarFiles := []tarutil.File{
			tarutil.NewMemFile("/a", []byte("foobar")),
			tarutil.NewMemFile("/b", []byte("barfoo")),
		}
		triggerJob(t, env, pi, tarFiles)
		ctx = withTimeout(ctx, 10*time.Second)
		<-ctx.Done()
		require.Equal(t, pps.JobState_JOB_SUCCESS, jobInfo.State)

		// Ensure the output commit is successful.
		outputCommitID := jobInfo.OutputCommit.ID
		outputCommitInfo, err := env.PachClient.InspectCommit(pi.Pipeline.Name, jobInfo.OutputCommit.Branch.Name, outputCommitID)
		require.NoError(t, err)
		require.NotNil(t, outputCommitInfo.Finished)

		branchInfo, err := env.PachClient.InspectBranch(pi.Pipeline.Name, pi.Details.OutputBranch)
		require.NoError(t, err)
		require.NotNil(t, branchInfo)

		// Get the output files.
		r, err := env.PachClient.GetFileTAR(jobInfo.OutputCommit, "/*")
		require.NoError(t, err)
		require.NoError(t, tarutil.Iterate(r, func(file tarutil.File) error {
			ok, err := tarutil.Equal(tarFiles[0], file)
			require.NoError(t, err)
			require.True(t, ok)
			tarFiles = tarFiles[1:]
			return nil
		}))
	})

	suite.Run("TestJobSerial", func(t *testing.T) {
		t.Parallel()
		pi := defaultPipelineInfo()
		env := newWorkerSpawnerPair(t, testutil.NewTestDBConfig(t), pi)

		ctx, jobInfo := mockBasicJob(t, env, pi)
		tarFiles := []tarutil.File{
			tarutil.NewMemFile("/a", []byte("foobar")),
			tarutil.NewMemFile("/b", []byte("barfoo")),
		}
		triggerJob(t, env, pi, tarFiles[:1])
		ctx = withTimeout(ctx, 10*time.Second)
		<-ctx.Done()
		require.Equal(t, pps.JobState_JOB_SUCCESS, jobInfo.State)

		ctx, jobInfo = mockBasicJob(t, env, pi)
		triggerJob(t, env, pi, tarFiles[1:])
		ctx = withTimeout(ctx, 10*time.Second)
		<-ctx.Done()
		require.Equal(t, pps.JobState_JOB_SUCCESS, jobInfo.State)

		// Ensure the output commit is successful
		outputCommitID := jobInfo.OutputCommit.ID
		outputCommitInfo, err := env.PachClient.InspectCommit(pi.Pipeline.Name, jobInfo.OutputCommit.Branch.Name, outputCommitID)
		require.NoError(t, err)
		require.NotNil(t, outputCommitInfo.Finished)

		branchInfo, err := env.PachClient.InspectBranch(pi.Pipeline.Name, pi.Details.OutputBranch)
		require.NoError(t, err)
		require.NotNil(t, branchInfo)

		// Get the output files.
		r, err := env.PachClient.GetFileTAR(jobInfo.OutputCommit, "/*")
		require.NoError(t, err)
		require.NoError(t, tarutil.Iterate(r, func(file tarutil.File) error {
			ok, err := tarutil.Equal(tarFiles[0], file)
			require.NoError(t, err)
			require.True(t, ok)
			tarFiles = tarFiles[1:]
			return nil
		}))
	})

	suite.Run("TestJobSerialDelete", func(t *testing.T) {
		t.Parallel()
		pi := defaultPipelineInfo()
		env := newWorkerSpawnerPair(t, testutil.NewTestDBConfig(t), pi)

		ctx, jobInfo := mockBasicJob(t, env, pi)
		tarFiles := []tarutil.File{
			tarutil.NewMemFile("/a", []byte("foobar")),
			tarutil.NewMemFile("/b", []byte("barfoo")),
		}
		triggerJob(t, env, pi, tarFiles[:1])
		ctx = withTimeout(ctx, 10*time.Second)
		<-ctx.Done()
		require.Equal(t, pps.JobState_JOB_SUCCESS, jobInfo.State)

		ctx, jobInfo = mockBasicJob(t, env, pi)
		triggerJob(t, env, pi, tarFiles[1:])
		ctx = withTimeout(ctx, 10*time.Second)
		<-ctx.Done()
		require.Equal(t, pps.JobState_JOB_SUCCESS, jobInfo.State)

		ctx, jobInfo = mockBasicJob(t, env, pi)
		deleteFiles(t, env, pi, []string{"/a"})
		ctx = withTimeout(ctx, 10*time.Second)
		<-ctx.Done()
		require.Equal(t, pps.JobState_JOB_SUCCESS, jobInfo.State)

		// Ensure the output commit is successful
		outputCommitID := jobInfo.OutputCommit.ID
		outputCommitInfo, err := env.PachClient.InspectCommit(pi.Pipeline.Name, jobInfo.OutputCommit.Branch.Name, outputCommitID)
		require.NoError(t, err)
		require.NotNil(t, outputCommitInfo.Finished)

		branchInfo, err := env.PachClient.InspectBranch(pi.Pipeline.Name, pi.Details.OutputBranch)
		require.NoError(t, err)
		require.NotNil(t, branchInfo)

		// Get the output files.
		r, err := env.PachClient.GetFileTAR(jobInfo.OutputCommit, "/*")
		require.NoError(t, err)
		require.NoError(t, tarutil.Iterate(r, func(file tarutil.File) error {
			ok, err := tarutil.Equal(tarFiles[1], file)
			require.NoError(t, err)
			require.True(t, ok)
			return nil
		}))
	})
}

func deleteFiles(t *testing.T, env *testEnv, pi *pps.PipelineInfo, files []string) {
	commit, err := env.PachClient.StartCommit(pi.Details.Input.Pfs.Repo, "master")
	require.NoError(t, err)
	for _, file := range files {
		require.NoError(t, env.PachClient.DeleteFile(commit, file))
	}
	require.NoError(t, env.PachClient.FinishCommit(pi.Details.Input.Pfs.Repo, commit.Branch.Name, commit.ID))
}

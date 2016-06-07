package server

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/pachyderm/pachyderm"
	"github.com/pachyderm/pachyderm/src/client"
	pfsclient "github.com/pachyderm/pachyderm/src/client/pfs"
	"github.com/pachyderm/pachyderm/src/client/pkg/require"
	"github.com/pachyderm/pachyderm/src/client/pkg/uuid"
	ppsclient "github.com/pachyderm/pachyderm/src/client/pps"
	"github.com/pachyderm/pachyderm/src/server/pkg/workload"
	ppsserver "github.com/pachyderm/pachyderm/src/server/pps"
	pps_server "github.com/pachyderm/pachyderm/src/server/pps/server"
	"k8s.io/kubernetes/pkg/api"
	kube "k8s.io/kubernetes/pkg/client/unversioned"
)

const (
	NUMFILES = 25
	KB       = 1024 * 1024
)

func TestJob(t *testing.T) {
	testJob(t, 4)
}

func TestJobNoShard(t *testing.T) {
	testJob(t, 0)
}

func testJob(t *testing.T, shards int) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}
	t.Parallel()
	c := getPachClient(t)
	dataRepo := uniqueString("TestJob_data")
	require.NoError(t, c.CreateRepo(dataRepo))
	commit, err := c.StartCommit(dataRepo, "", "")
	require.NoError(t, err)
	fileContent := "foo\n"
	// We want to create lots of files so that each parallel job will be
	// started with some files
	numFiles := shards*100 + 100
	for i := 0; i < numFiles; i++ {
		_, err = c.PutFile(dataRepo, commit.ID, fmt.Sprintf("file-%d", i), strings.NewReader(fileContent))
		require.NoError(t, err)
	}
	require.NoError(t, c.FinishCommit(dataRepo, commit.ID))
	job, err := c.CreateJob(
		"",
		[]string{"bash"},
		[]string{fmt.Sprintf("cp %s %s", path.Join("/pfs", dataRepo, "*"), "/pfs/out")},
		uint64(shards),
		[]*ppsclient.JobInput{{
			Commit: commit,
			Method: client.ReduceMethod,
		}},
		"",
	)
	require.NoError(t, err)
	inspectJobRequest := &ppsclient.InspectJobRequest{
		Job:        job,
		BlockState: true,
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel() //cleanup resources
	jobInfo, err := c.PpsAPIClient.InspectJob(ctx, inspectJobRequest)
	require.NoError(t, err)
	require.Equal(t, ppsclient.JobState_JOB_SUCCESS.String(), jobInfo.State.String())
	require.True(t, jobInfo.Parallelism > 0)
	commitInfo, err := c.InspectCommit(jobInfo.OutputCommit.Repo.Name, jobInfo.OutputCommit.ID)
	require.NoError(t, err)
	require.Equal(t, pfsclient.CommitType_COMMIT_TYPE_READ, commitInfo.CommitType)
	for i := 0; i < numFiles; i++ {
		var buffer bytes.Buffer
		require.NoError(t, c.GetFile(jobInfo.OutputCommit.Repo.Name, jobInfo.OutputCommit.ID, fmt.Sprintf("file-%d", i), 0, 0, "", nil, &buffer))
		require.Equal(t, fileContent, buffer.String())
	}
}

func TestPachCommitIdEnvVarInJob(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}
	t.Parallel()

	shards := 0
	c := getPachClient(t)
	repos := []string{
		uniqueString("TestJob_FriarTuck"),
		uniqueString("TestJob_RobinHood"),
	}

	var commits []*pfsclient.Commit

	for _, repo := range repos {
		require.NoError(t, c.CreateRepo(repo))
		commit, err := c.StartCommit(repo, "", "")
		require.NoError(t, err)
		fileContent := "foo\n"

		_, err = c.PutFile(repo, commit.ID, "file", strings.NewReader(fileContent))
		require.NoError(t, err)

		require.NoError(t, c.FinishCommit(repo, commit.ID))
		commits = append(commits, commit)
	}

	job, err := c.CreateJob(
		"",
		[]string{"bash"},
		[]string{
			"echo $PACH_OUTPUT_COMMIT_ID > /pfs/out/id",
			fmt.Sprintf("echo $PACH_%v_COMMIT_ID > /pfs/out/input-id-%v", pps_server.RepoNameToEnvString(repos[0]), repos[0]),
			fmt.Sprintf("echo $PACH_%v_COMMIT_ID > /pfs/out/input-id-%v", pps_server.RepoNameToEnvString(repos[1]), repos[1]),
		},
		uint64(shards),
		[]*ppsclient.JobInput{
			{
				Commit: commits[0],
				Method: client.ReduceMethod,
			},
			{
				Commit: commits[1],
				Method: client.ReduceMethod,
			},
		},
		"",
	)
	require.NoError(t, err)
	inspectJobRequest := &ppsclient.InspectJobRequest{
		Job:        job,
		BlockState: true,
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel() //cleanup resources
	jobInfo, err := c.PpsAPIClient.InspectJob(ctx, inspectJobRequest)
	require.NoError(t, err)
	require.Equal(t, ppsclient.JobState_JOB_SUCCESS.String(), jobInfo.State.String())
	require.True(t, jobInfo.Parallelism > 0)
	commitInfo, err := c.InspectCommit(jobInfo.OutputCommit.Repo.Name, jobInfo.OutputCommit.ID)
	require.NoError(t, err)
	require.Equal(t, pfsclient.CommitType_COMMIT_TYPE_READ, commitInfo.CommitType)

	var buffer bytes.Buffer
	require.NoError(t, c.GetFile(jobInfo.OutputCommit.Repo.Name, jobInfo.OutputCommit.ID, "id", 0, 0, "", nil, &buffer))
	require.Equal(t, jobInfo.OutputCommit.ID, strings.TrimSpace(buffer.String()))

	buffer.Reset()
	require.NoError(t, c.GetFile(jobInfo.OutputCommit.Repo.Name, jobInfo.OutputCommit.ID, fmt.Sprintf("input-id-%v", repos[0]), 0, 0, "", nil, &buffer))
	require.Equal(t, jobInfo.Inputs[0].Commit.ID, strings.TrimSpace(buffer.String()))

	buffer.Reset()
	require.NoError(t, c.GetFile(jobInfo.OutputCommit.Repo.Name, jobInfo.OutputCommit.ID, fmt.Sprintf("input-id-%v", repos[1]), 0, 0, "", nil, &buffer))
	require.Equal(t, jobInfo.Inputs[1].Commit.ID, strings.TrimSpace(buffer.String()))
}

func TestDuplicatedJob(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}
	t.Parallel()
	c := getPachClient(t)

	dataRepo := uniqueString("TestDuplicatedJob_data")
	require.NoError(t, c.CreateRepo(dataRepo))

	commit, err := c.StartCommit(dataRepo, "", "")
	require.NoError(t, err)

	fileContent := "foo\n"
	_, err = c.PutFile(dataRepo, commit.ID, "file", strings.NewReader(fileContent))
	require.NoError(t, err)

	require.NoError(t, c.FinishCommit(dataRepo, commit.ID))

	pipelineName := uniqueString("TestDuplicatedJob_pipeline")
	_, err = c.PfsAPIClient.CreateRepo(
		context.Background(),
		&pfsclient.CreateRepoRequest{
			Repo:       client.NewRepo(pipelineName),
			Provenance: []*pfsclient.Repo{client.NewRepo(dataRepo)},
		},
	)
	require.NoError(t, err)

	cmd := []string{"cp", path.Join("/pfs", dataRepo, "file"), "/pfs/out/file"}
	// Now we manually create the same job
	req := &ppsclient.CreateJobRequest{
		Transform: &ppsclient.Transform{
			Cmd: cmd,
		},
		Pipeline: &ppsclient.Pipeline{
			Name: pipelineName,
		},
		Inputs: []*ppsclient.JobInput{{
			Commit: commit,
		}},
	}

	job1, err := c.PpsAPIClient.CreateJob(context.Background(), req)
	require.NoError(t, err)

	job2, err := c.PpsAPIClient.CreateJob(context.Background(), req)
	require.NoError(t, err)

	require.Equal(t, job1, job2)

	req.Force = true
	job3, err := c.PpsAPIClient.CreateJob(context.Background(), req)
	require.NoError(t, err)
	require.NotEqual(t, job1, job3)

	inspectJobRequest := &ppsclient.InspectJobRequest{
		Job:        job1,
		BlockState: true,
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel() //cleanup resources
	jobInfo, err := c.PpsAPIClient.InspectJob(ctx, inspectJobRequest)
	require.NoError(t, err)

	var buffer bytes.Buffer
	require.NoError(t, c.GetFile(jobInfo.OutputCommit.Repo.Name, jobInfo.OutputCommit.ID, "file", 0, 0, "", nil, &buffer))
	require.Equal(t, fileContent, buffer.String())
}

func TestLogs(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}
	t.Parallel()
	c := getPachClient(t)

	job, err := c.CreateJob(
		"",
		[]string{"echo", "foo"},
		nil,
		4,
		[]*ppsclient.JobInput{},
		"",
	)
	require.NoError(t, err)
	inspectJobRequest := &ppsclient.InspectJobRequest{
		Job:        job,
		BlockState: true,
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel() //cleanup resources
	_, err = c.PpsAPIClient.InspectJob(ctx, inspectJobRequest)
	require.NoError(t, err)
	// TODO we Sleep here because even though the job has completed kubernetes
	// might not have even noticed the container was created yet
	time.Sleep(10 * time.Second)
	var buffer bytes.Buffer
	require.NoError(t, c.GetLogs(job.ID, &buffer))
	require.Equal(t, "0 | foo\n1 | foo\n2 | foo\n3 | foo\n", buffer.String())

	// Should get an error if the job does not exist
	require.YesError(t, c.GetLogs("nonexistent", &buffer))
}

func TestGrep(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}
	t.Parallel()

	dataRepo := uniqueString("TestGrep_data")
	c := getPachClient(t)
	require.NoError(t, c.CreateRepo(dataRepo))
	commit, err := c.StartCommit(dataRepo, "", "")
	require.NoError(t, err)
	for i := 0; i < 100; i++ {
		_, err = c.PutFile(dataRepo, commit.ID, fmt.Sprintf("file%d", i), strings.NewReader("foo\nbar\nfizz\nbuzz\n"))
		require.NoError(t, err)
	}
	require.NoError(t, c.FinishCommit(dataRepo, commit.ID))
	job1, err := c.CreateJob(
		"",
		[]string{"bash"},
		[]string{fmt.Sprintf("grep foo /pfs/%s/* >/pfs/out/foo", dataRepo)},
		1,
		[]*ppsclient.JobInput{{Commit: commit}},
		"",
	)
	require.NoError(t, err)
	job2, err := c.CreateJob(
		"",
		[]string{"bash"},
		[]string{fmt.Sprintf("grep foo /pfs/%s/* >/pfs/out/foo", dataRepo)},
		4,
		[]*ppsclient.JobInput{{Commit: commit}},
		"",
	)
	require.NoError(t, err)
	inspectJobRequest := &ppsclient.InspectJobRequest{
		Job:        job1,
		BlockState: true,
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel() //cleanup resources
	job1Info, err := c.PpsAPIClient.InspectJob(ctx, inspectJobRequest)
	require.NoError(t, err)
	inspectJobRequest.Job = job2
	job2Info, err := c.PpsAPIClient.InspectJob(ctx, inspectJobRequest)
	require.NoError(t, err)
	repo1Info, err := c.InspectRepo(job1Info.OutputCommit.Repo.Name)
	require.NoError(t, err)
	repo2Info, err := c.InspectRepo(job2Info.OutputCommit.Repo.Name)
	require.NoError(t, err)
	require.Equal(t, repo1Info.SizeBytes, repo2Info.SizeBytes)
}

func TestJobLongOutputLine(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}
	t.Parallel()
	c := getPachClient(t)
	job, err := c.CreateJob(
		"",
		[]string{"sh"},
		[]string{"yes | tr -d '\\n' | head -c 1000000 > /pfs/out/file"},
		1,
		[]*ppsclient.JobInput{},
		"",
	)
	require.NoError(t, err)
	inspectJobRequest := &ppsclient.InspectJobRequest{
		Job:        job,
		BlockState: true,
	}
	jobInfo, err := c.PpsAPIClient.InspectJob(context.Background(), inspectJobRequest)
	require.NoError(t, err)
	require.Equal(t, ppsclient.JobState_JOB_SUCCESS.String(), jobInfo.State.String())
}

func TestPipeline(t *testing.T) {

	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}
	t.Parallel()

	c := getPachClient(t)
	// create repos
	dataRepo := uniqueString("TestPipeline_data")
	require.NoError(t, c.CreateRepo(dataRepo))
	// create pipeline
	pipelineName := uniqueString("pipeline")
	outRepo := ppsserver.PipelineRepo(client.NewPipeline(pipelineName))
	require.NoError(t, c.CreatePipeline(
		pipelineName,
		"",
		[]string{"cp", path.Join("/pfs", dataRepo, "file"), "/pfs/out/file"},
		nil,
		1,
		[]*ppsclient.PipelineInput{{Repo: &pfsclient.Repo{Name: dataRepo}}},
	))
	// Do first commit to repo
	commit1, err := c.StartCommit(dataRepo, "", "")
	require.NoError(t, err)
	_, err = c.PutFile(dataRepo, commit1.ID, "file", strings.NewReader("foo\n"))
	require.NoError(t, err)
	require.NoError(t, c.FinishCommit(dataRepo, commit1.ID))
	listCommitRequest := &pfsclient.ListCommitRequest{
		Repo:       []*pfsclient.Repo{outRepo},
		CommitType: pfsclient.CommitType_COMMIT_TYPE_READ,
		Block:      true,
	}
	listCommitResponse, err := c.PfsAPIClient.ListCommit(
		context.Background(),
		listCommitRequest,
	)
	require.NoError(t, err)
	outCommits := listCommitResponse.CommitInfo
	require.Equal(t, 1, len(outCommits))
	var buffer bytes.Buffer
	require.NoError(t, c.GetFile(outRepo.Name, outCommits[0].Commit.ID, "file", 0, 0, "", nil, &buffer))
	require.Equal(t, "foo\n", buffer.String())
	// Do second commit to repo
	commit2, err := c.StartCommit(dataRepo, commit1.ID, "")
	require.NoError(t, err)
	_, err = c.PutFile(dataRepo, commit2.ID, "file", strings.NewReader("bar\n"))
	require.NoError(t, err)
	require.NoError(t, c.FinishCommit(dataRepo, commit2.ID))
	listCommitRequest = &pfsclient.ListCommitRequest{
		Repo:       []*pfsclient.Repo{outRepo},
		FromCommit: []*pfsclient.Commit{outCommits[0].Commit},
		CommitType: pfsclient.CommitType_COMMIT_TYPE_READ,
		Block:      true,
	}
	listCommitResponse, err = c.PfsAPIClient.ListCommit(
		context.Background(),
		listCommitRequest,
	)
	require.NoError(t, err)
	require.NotNil(t, listCommitResponse.CommitInfo[0].ParentCommit)
	require.Equal(t, outCommits[0].Commit.ID, listCommitResponse.CommitInfo[0].ParentCommit.ID)
	outCommits = listCommitResponse.CommitInfo
	require.Equal(t, 1, len(outCommits))
	buffer = bytes.Buffer{}
	require.NoError(t, c.GetFile(outRepo.Name, outCommits[0].Commit.ID, "file", 0, 0, "", nil, &buffer))
	require.Equal(t, "foo\nbar\n", buffer.String())

	require.NoError(t, c.DeletePipeline(pipelineName))

	pipelineInfos, err := c.PpsAPIClient.ListPipeline(context.Background(), &ppsclient.ListPipelineRequest{})
	require.NoError(t, err)
	for _, pipelineInfo := range pipelineInfos.PipelineInfo {
		require.True(t, pipelineInfo.Pipeline.Name != pipelineName)
	}

	// Do third commit to repo; this time pipeline should not run since it's been deleted
	commit3, err := c.StartCommit(dataRepo, commit2.ID, "")
	require.NoError(t, err)
	_, err = c.PutFile(dataRepo, commit3.ID, "file", strings.NewReader("buzz\n"))
	require.NoError(t, err)
	require.NoError(t, c.FinishCommit(dataRepo, commit3.ID))

	// We will sleep a while to wait for the pipeline to actually get cancelled
	// Also if the pipeline didn't get cancelled (due to a bug), we sleep a while
	// to let the pipeline commit
	time.Sleep(5 * time.Second)
	listCommitRequest = &pfsclient.ListCommitRequest{
		Repo: []*pfsclient.Repo{outRepo},
	}
	listCommitResponse, err = c.PfsAPIClient.ListCommit(
		context.Background(),
		listCommitRequest,
	)
	require.NoError(t, err)
	// there should only be two commits in the pipeline
	require.Equal(t, 2, len(listCommitResponse.CommitInfo))
}

func TestPipelineWithTooMuchParallelism(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}
	t.Parallel()

	c := getPachClient(t)
	// create repos
	dataRepo := uniqueString("TestPipelineWithTooMuchParallelism_data")
	require.NoError(t, c.CreateRepo(dataRepo))
	// create pipeline
	pipelineName := uniqueString("pipeline")
	outRepo := ppsserver.PipelineRepo(client.NewPipeline(pipelineName))
	// This pipeline will fail if any pod sees empty input, since cp won't
	// be able to find the file.
	// We have parallelism set to 3 so that if we actually start 3 pods,
	// which would be a buggy behavior, some jobs don't see any files
	require.NoError(t, c.CreatePipeline(
		pipelineName,
		"",
		[]string{"cp", path.Join("/pfs", dataRepo, "file"), "/pfs/out/file"},
		nil,
		3,
		[]*ppsclient.PipelineInput{{
			Repo: &pfsclient.Repo{Name: dataRepo},
			// Use reduce method so only one pod gets the file
			Method: client.ReduceMethod,
		}},
	))
	// Do first commit to repo
	commit1, err := c.StartCommit(dataRepo, "", "")
	require.NoError(t, err)
	_, err = c.PutFile(dataRepo, commit1.ID, "file", strings.NewReader("foo\n"))
	require.NoError(t, err)
	require.NoError(t, c.FinishCommit(dataRepo, commit1.ID))
	listCommitRequest := &pfsclient.ListCommitRequest{
		Repo:       []*pfsclient.Repo{outRepo},
		CommitType: pfsclient.CommitType_COMMIT_TYPE_READ,
		Block:      true,
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel() //cleanup resources
	listCommitResponse, err := c.PfsAPIClient.ListCommit(
		ctx,
		listCommitRequest,
	)
	require.NoError(t, err)
	outCommits := listCommitResponse.CommitInfo
	require.Equal(t, 1, len(outCommits))
	var buffer bytes.Buffer
	require.NoError(t, c.GetFile(outRepo.Name, outCommits[0].Commit.ID, "file", 0, 0, "", nil, &buffer))
	require.Equal(t, "foo\n", buffer.String())
	require.Equal(t, false, outCommits[0].Cancelled)
}

func TestPipelineWithEmptyInputs(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}
	t.Parallel()

	c := getPachClient(t)
	// create pipeline
	pipelineName := uniqueString("pipeline")
	outRepo := ppsserver.PipelineRepo(client.NewPipeline(pipelineName))
	require.NoError(t, c.CreatePipeline(
		pipelineName,
		"",
		[]string{"sh"},
		[]string{
			"NEW_UUID=$(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 32 | head -n 1)",
			"echo foo > /pfs/out/$NEW_UUID",
		},
		3,
		nil,
	))

	// Manually trigger the pipeline
	job, err := c.PpsAPIClient.CreateJob(context.Background(), &ppsclient.CreateJobRequest{
		Pipeline: &ppsclient.Pipeline{
			Name: pipelineName,
		},
	})
	require.NoError(t, err)

	inspectJobRequest := &ppsclient.InspectJobRequest{
		Job:        job,
		BlockState: true,
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()
	jobInfo, err := c.PpsAPIClient.InspectJob(ctx, inspectJobRequest)
	require.NoError(t, err)
	require.Equal(t, ppsclient.JobState_JOB_SUCCESS.String(), jobInfo.State.String())
	require.Equal(t, 3, int(jobInfo.Parallelism))

	listCommitRequest := &pfsclient.ListCommitRequest{
		Repo:       []*pfsclient.Repo{outRepo},
		CommitType: pfsclient.CommitType_COMMIT_TYPE_READ,
		Block:      true,
	}
	listCommitResponse, err := c.PfsAPIClient.ListCommit(
		ctx,
		listCommitRequest,
	)
	require.NoError(t, err)
	outCommits := listCommitResponse.CommitInfo
	require.Equal(t, 1, len(outCommits))
	fileInfos, err := c.ListFile(outRepo.Name, outCommits[0].Commit.ID, "", "", nil, false)
	require.NoError(t, err)
	require.Equal(t, 3, len(fileInfos))

	// Make sure that each job gets a different ID
	job2, err := c.PpsAPIClient.CreateJob(context.Background(), &ppsclient.CreateJobRequest{
		Pipeline: &ppsclient.Pipeline{
			Name: pipelineName,
		},
	})
	require.NoError(t, err)
	require.True(t, job.ID != job2.ID)
}

func TestPipelineThatWritesToOneFile(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}
	t.Parallel()

	c := getPachClient(t)
	// create pipeline
	pipelineName := uniqueString("pipeline")
	outRepo := ppsserver.PipelineRepo(client.NewPipeline(pipelineName))
	require.NoError(t, c.CreatePipeline(
		pipelineName,
		"",
		[]string{"sh"},
		[]string{
			"dd if=/dev/zero of=/pfs/out/file bs=10 count=1",
		},
		3,
		nil,
	))

	// Manually trigger the pipeline
	_, err := c.PpsAPIClient.CreateJob(context.Background(), &ppsclient.CreateJobRequest{
		Pipeline: &ppsclient.Pipeline{
			Name: pipelineName,
		},
	})
	require.NoError(t, err)

	listCommitRequest := &pfsclient.ListCommitRequest{
		Repo:       []*pfsclient.Repo{outRepo},
		CommitType: pfsclient.CommitType_COMMIT_TYPE_READ,
		Block:      true,
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()
	listCommitResponse, err := c.PfsAPIClient.ListCommit(
		ctx,
		listCommitRequest,
	)
	require.NoError(t, err)
	outCommits := listCommitResponse.CommitInfo
	require.Equal(t, 1, len(outCommits))
	var buffer bytes.Buffer
	require.NoError(t, c.GetFile(outRepo.Name, outCommits[0].Commit.ID, "file", 0, 0, "", nil, &buffer))
	require.Equal(t, 30, buffer.Len())
}

func TestPipelineThatOverwritesFile(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}
	t.Parallel()

	c := getPachClient(t)
	// create pipeline
	pipelineName := uniqueString("pipeline")
	outRepo := ppsserver.PipelineRepo(client.NewPipeline(pipelineName))
	require.NoError(t, c.CreatePipeline(
		pipelineName,
		"",
		[]string{"sh"},
		[]string{
			"echo foo > /pfs/out/file",
		},
		3,
		nil,
	))

	// Manually trigger the pipeline
	job, err := c.PpsAPIClient.CreateJob(context.Background(), &ppsclient.CreateJobRequest{
		Pipeline: &ppsclient.Pipeline{
			Name: pipelineName,
		},
	})
	require.NoError(t, err)

	listCommitRequest := &pfsclient.ListCommitRequest{
		Repo:       []*pfsclient.Repo{outRepo},
		CommitType: pfsclient.CommitType_COMMIT_TYPE_READ,
		Block:      true,
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()
	listCommitResponse, err := c.PfsAPIClient.ListCommit(
		ctx,
		listCommitRequest,
	)
	require.NoError(t, err)
	outCommits := listCommitResponse.CommitInfo
	require.Equal(t, 1, len(outCommits))
	var buffer bytes.Buffer
	require.NoError(t, c.GetFile(outRepo.Name, outCommits[0].Commit.ID, "file", 0, 0, "", nil, &buffer))
	require.Equal(t, "foo\nfoo\nfoo\n", buffer.String())

	// Manually trigger the pipeline
	_, err = c.PpsAPIClient.CreateJob(context.Background(), &ppsclient.CreateJobRequest{
		Pipeline: &ppsclient.Pipeline{
			Name: pipelineName,
		},
		ParentJob: job,
	})
	require.NoError(t, err)

	listCommitRequest = &pfsclient.ListCommitRequest{
		Repo:       []*pfsclient.Repo{outRepo},
		CommitType: pfsclient.CommitType_COMMIT_TYPE_READ,
		FromCommit: []*pfsclient.Commit{outCommits[0].Commit},
		Block:      true,
	}
	listCommitResponse, err = c.PfsAPIClient.ListCommit(
		ctx,
		listCommitRequest,
	)
	require.NoError(t, err)
	outCommits = listCommitResponse.CommitInfo
	require.Equal(t, 1, len(outCommits))
	var buffer2 bytes.Buffer
	require.NoError(t, c.GetFile(outRepo.Name, outCommits[0].Commit.ID, "file", 0, 0, "", nil, &buffer2))
	require.Equal(t, "foo\nfoo\nfoo\nfoo\nfoo\nfoo\n", buffer2.String())
}

func TestPipelineThatAppendsToFile(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}
	t.Parallel()

	c := getPachClient(t)
	// create pipeline
	pipelineName := uniqueString("pipeline")
	outRepo := ppsserver.PipelineRepo(client.NewPipeline(pipelineName))
	require.NoError(t, c.CreatePipeline(
		pipelineName,
		"",
		[]string{"sh"},
		[]string{
			"echo foo >> /pfs/out/file",
		},
		3,
		nil,
	))

	// Manually trigger the pipeline
	job, err := c.PpsAPIClient.CreateJob(context.Background(), &ppsclient.CreateJobRequest{
		Pipeline: &ppsclient.Pipeline{
			Name: pipelineName,
		},
	})
	require.NoError(t, err)

	listCommitRequest := &pfsclient.ListCommitRequest{
		Repo:       []*pfsclient.Repo{outRepo},
		CommitType: pfsclient.CommitType_COMMIT_TYPE_READ,
		Block:      true,
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	listCommitResponse, err := c.PfsAPIClient.ListCommit(
		ctx,
		listCommitRequest,
	)
	require.NoError(t, err)
	outCommits := listCommitResponse.CommitInfo
	require.Equal(t, 1, len(outCommits))
	var buffer bytes.Buffer
	require.NoError(t, c.GetFile(outRepo.Name, outCommits[0].Commit.ID, "file", 0, 0, "", nil, &buffer))
	require.Equal(t, "foo\nfoo\nfoo\n", buffer.String())

	// Manually trigger the pipeline
	_, err = c.PpsAPIClient.CreateJob(context.Background(), &ppsclient.CreateJobRequest{
		Pipeline: &ppsclient.Pipeline{
			Name: pipelineName,
		},
		ParentJob: job,
	})
	require.NoError(t, err)

	listCommitRequest = &pfsclient.ListCommitRequest{
		Repo:       []*pfsclient.Repo{outRepo},
		CommitType: pfsclient.CommitType_COMMIT_TYPE_READ,
		Block:      true,
		FromCommit: []*pfsclient.Commit{outCommits[0].Commit},
	}
	listCommitResponse, err = c.PfsAPIClient.ListCommit(
		ctx,
		listCommitRequest,
	)
	require.NoError(t, err)
	outCommits = listCommitResponse.CommitInfo
	require.Equal(t, 1, len(outCommits))
	var buffer2 bytes.Buffer
	require.NoError(t, c.GetFile(outRepo.Name, outCommits[0].Commit.ID, "file", 0, 0, "", nil, &buffer2))
	require.Equal(t, "foo\nfoo\nfoo\nfoo\nfoo\nfoo\n", buffer2.String())
}

func TestRemoveAndAppend(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}
	t.Parallel()

	c := getPachClient(t)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel() //cleanup resources

	job1, err := c.PpsAPIClient.CreateJob(context.Background(), &ppsclient.CreateJobRequest{
		Transform: &ppsclient.Transform{
			Cmd: []string{"sh"},
			Stdin: []string{
				"echo foo > /pfs/out/file",
			},
		},
		Parallelism: 3,
	})
	require.NoError(t, err)

	inspectJobRequest1 := &ppsclient.InspectJobRequest{
		Job:        job1,
		BlockState: true,
	}
	jobInfo1, err := c.PpsAPIClient.InspectJob(ctx, inspectJobRequest1)
	require.NoError(t, err)
	require.Equal(t, ppsclient.JobState_JOB_SUCCESS, jobInfo1.State)

	var buffer bytes.Buffer
	require.NoError(t, c.GetFile(jobInfo1.OutputCommit.Repo.Name, jobInfo1.OutputCommit.ID, "file", 0, 0, "", nil, &buffer))
	require.Equal(t, "foo\nfoo\nfoo\n", buffer.String())

	job2, err := c.PpsAPIClient.CreateJob(context.Background(), &ppsclient.CreateJobRequest{
		Transform: &ppsclient.Transform{
			Cmd: []string{"sh"},
			Stdin: []string{
				"unlink /pfs/out/file && echo bar > /pfs/out/file",
			},
		},
		Parallelism: 3,
		ParentJob:   job1,
	})
	require.NoError(t, err)

	inspectJobRequest2 := &ppsclient.InspectJobRequest{
		Job:        job2,
		BlockState: true,
	}
	jobInfo2, err := c.PpsAPIClient.InspectJob(ctx, inspectJobRequest2)
	require.NoError(t, err)
	require.Equal(t, ppsclient.JobState_JOB_SUCCESS, jobInfo2.State)

	var buffer2 bytes.Buffer
	require.NoError(t, c.GetFile(jobInfo2.OutputCommit.Repo.Name, jobInfo2.OutputCommit.ID, "file", 0, 0, "", nil, &buffer2))
	require.Equal(t, "bar\nbar\nbar\n", buffer2.String())
}

func TestWorkload(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := getPachClient(t)
	seed := time.Now().UnixNano()
	require.NoError(t, workload.RunWorkload(c, rand.New(rand.NewSource(seed)), 100))
}

func TestSharding(t *testing.T) {

	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}
	t.Parallel()

	repo := uniqueString("TestSharding")
	c := getPachClient(t)
	err := c.CreateRepo(repo)
	require.NoError(t, err)
	commit, err := c.StartCommit(repo, "", "")
	require.NoError(t, err)
	var wg sync.WaitGroup
	for i := 0; i < NUMFILES; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			rand := rand.New(rand.NewSource(int64(i)))
			_, err = c.PutFile(repo, commit.ID, fmt.Sprintf("file%d", i), workload.NewReader(rand, KB))
			require.NoError(t, err)
		}()
	}
	wg.Wait()
	err = c.FinishCommit(repo, commit.ID)
	require.NoError(t, err)
	wg = sync.WaitGroup{}
	for i := 0; i < NUMFILES; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			var buffer1Shard bytes.Buffer
			var buffer4Shard bytes.Buffer
			shard := &pfsclient.Shard{FileModulus: 1, BlockModulus: 1}
			err := c.GetFile(repo, commit.ID,
				fmt.Sprintf("file%d", i), 0, 0, "", shard, &buffer1Shard)
			require.NoError(t, err)
			shard.BlockModulus = 4
			for blockNumber := uint64(0); blockNumber < 4; blockNumber++ {
				shard.BlockNumber = blockNumber
				err := c.GetFile(repo, commit.ID,
					fmt.Sprintf("file%d", i), 0, 0, "", shard, &buffer4Shard)
				require.NoError(t, err)
			}
			require.Equal(t, buffer1Shard.Len(), buffer4Shard.Len())
		}()
	}
	wg.Wait()
}

func TestFromCommit(t *testing.T) {

	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}
	t.Parallel()

	repo := uniqueString("TestFromCommit")
	c := getPachClient(t)
	seed := time.Now().UnixNano()
	rand := rand.New(rand.NewSource(seed))
	err := c.CreateRepo(repo)
	require.NoError(t, err)
	commit1, err := c.StartCommit(repo, "", "")
	require.NoError(t, err)
	_, err = c.PutFile(repo, commit1.ID, "file", workload.NewReader(rand, KB))
	require.NoError(t, err)
	err = c.FinishCommit(repo, commit1.ID)
	require.NoError(t, err)
	commit2, err := c.StartCommit(repo, commit1.ID, "")
	require.NoError(t, err)
	_, err = c.PutFile(repo, commit2.ID, "file", workload.NewReader(rand, KB))
	require.NoError(t, err)
	err = c.FinishCommit(repo, commit2.ID)
	require.NoError(t, err)
	var buffer bytes.Buffer
	require.NoError(t, c.GetFile(repo, commit2.ID, "file", 0, 0, commit1.ID, nil, &buffer))
	require.Equal(t, buffer.Len(), KB)
	buffer = bytes.Buffer{}
	require.NoError(t, c.GetFile(repo, commit2.ID, "file", 0, 0, "", nil, &buffer))
	require.Equal(t, buffer.Len(), 2*KB)
}

func TestSimple(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}
	t.Parallel()
	c := getPachClient(t)
	repo := uniqueString("TestSimple")
	require.NoError(t, c.CreateRepo(repo))
	commit1, err := c.StartCommit(repo, "", "")
	require.NoError(t, err)
	_, err = c.PutFile(repo, commit1.ID, "foo", strings.NewReader("foo\n"))
	require.NoError(t, err)
	require.NoError(t, c.FinishCommit(repo, commit1.ID))
	commitInfos, err := c.ListCommit([]string{repo}, nil, client.CommitTypeNone, false, false, nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(commitInfos))
	var buffer bytes.Buffer
	require.NoError(t, c.GetFile(repo, commit1.ID, "foo", 0, 0, "", nil, &buffer))
	require.Equal(t, "foo\n", buffer.String())
	commit2, err := c.StartCommit(repo, commit1.ID, "")
	require.NoError(t, err)
	_, err = c.PutFile(repo, commit2.ID, "foo", strings.NewReader("foo\n"))
	require.NoError(t, err)
	err = c.FinishCommit(repo, commit2.ID)
	require.NoError(t, err)
	buffer = bytes.Buffer{}
	require.NoError(t, c.GetFile(repo, commit1.ID, "foo", 0, 0, "", nil, &buffer))
	require.Equal(t, "foo\n", buffer.String())
	buffer = bytes.Buffer{}
	require.NoError(t, c.GetFile(repo, commit2.ID, "foo", 0, 0, "", nil, &buffer))
	require.Equal(t, "foo\nfoo\n", buffer.String())
}

func TestPipelineWithMultipleInputs(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}
	t.Parallel()
	c := getPachClient(t)

	inputRepo1 := uniqueString("inputRepo")
	require.NoError(t, c.CreateRepo(inputRepo1))
	inputRepo2 := uniqueString("inputRepo")
	require.NoError(t, c.CreateRepo(inputRepo2))

	pipelineName := uniqueString("pipeline")
	require.NoError(t, c.CreatePipeline(
		pipelineName,
		"",
		[]string{"bash"},
		[]string{fmt.Sprintf(`
repo1=%s
repo2=%s
echo $repo1
ls -1 /pfs/$repo1
echo $repo2
ls -1 /pfs/$repo2
for f1 in /pfs/$repo1/*
do
	for f2 in /pfs/$repo2/*
	do
		v1=$(<$f1)
		v2=$(<$f2)
		echo $v1$v2 > /pfs/out/file
	done
done
`, inputRepo1, inputRepo2)},
		4,
		[]*ppsclient.PipelineInput{
			{
				Repo:   &pfsclient.Repo{Name: inputRepo1},
				Method: client.IncrementalReduceMethod,
			},
			{
				Repo:   &pfsclient.Repo{Name: inputRepo2},
				Method: client.IncrementalReduceMethod,
			},
		},
	))

	content := "foo"
	numfiles := 10

	commit1, err := c.StartCommit(inputRepo1, "", "")
	for i := 0; i < numfiles; i++ {
		_, err = c.PutFile(inputRepo1, commit1.ID, fmt.Sprintf("file%d", i), strings.NewReader(content))
		require.NoError(t, err)
	}
	require.NoError(t, c.FinishCommit(inputRepo1, commit1.ID))

	commit2, err := c.StartCommit(inputRepo2, "", "")
	for i := 0; i < numfiles; i++ {
		_, err = c.PutFile(inputRepo2, commit2.ID, fmt.Sprintf("file%d", i), strings.NewReader(content))
		require.NoError(t, err)
	}
	require.NoError(t, c.FinishCommit(inputRepo2, commit2.ID))

	listCommitRequest := &pfsclient.ListCommitRequest{
		Repo:       []*pfsclient.Repo{{pipelineName}},
		CommitType: pfsclient.CommitType_COMMIT_TYPE_READ,
		Block:      true,
	}
	listCommitResponse, err := c.PfsAPIClient.ListCommit(
		context.Background(),
		listCommitRequest,
	)
	require.NoError(t, err)
	outCommits := listCommitResponse.CommitInfo
	require.Equal(t, 1, len(outCommits))

	fileInfos, err := c.ListFile(pipelineName, outCommits[0].Commit.ID, "", "", nil, false)
	require.NoError(t, err)
	require.Equal(t, 1, len(fileInfos))

	var buffer bytes.Buffer
	require.NoError(t, c.GetFile(pipelineName, outCommits[0].Commit.ID, "file", 0, 0, "", nil, &buffer))
	lines := strings.Split(strings.TrimSpace(buffer.String()), "\n")
	require.Equal(t, numfiles*numfiles, len(lines))
	for _, line := range lines {
		require.Equal(t, len(content)*2, len(line))
	}

	commit3, err := c.StartCommit(inputRepo1, commit1.ID, "")
	for i := 0; i < numfiles; i++ {
		_, err = c.PutFile(inputRepo1, commit3.ID, fmt.Sprintf("file2-%d", i), strings.NewReader(content))
		require.NoError(t, err)
	}
	require.NoError(t, c.FinishCommit(inputRepo1, commit3.ID))

	listCommitRequest.FromCommit = append(listCommitRequest.FromCommit, outCommits[0].Commit)
	listCommitResponse, err = c.PfsAPIClient.ListCommit(
		context.Background(),
		listCommitRequest,
	)
	require.NoError(t, err)
	outCommits = listCommitResponse.CommitInfo
	require.Equal(t, 1, len(outCommits))

	buffer.Reset()
	require.NoError(t, c.GetFile(pipelineName, outCommits[0].Commit.ID, "file", 0, 0, "", nil, &buffer))
	lines = strings.Split(strings.TrimSpace(buffer.String()), "\n")
	require.Equal(t, 2*numfiles*numfiles, len(lines))
	for _, line := range lines {
		require.Equal(t, len(content)*2, len(line))
	}

	commit4, err := c.StartCommit(inputRepo2, commit2.ID, "")
	for i := 0; i < numfiles; i++ {
		_, err = c.PutFile(inputRepo2, commit4.ID, fmt.Sprintf("file2-%d", i), strings.NewReader(content))
		require.NoError(t, err)
	}
	require.NoError(t, c.FinishCommit(inputRepo2, commit4.ID))

	listCommitRequest.FromCommit[0] = outCommits[0].Commit
	listCommitResponse, err = c.PfsAPIClient.ListCommit(
		context.Background(),
		listCommitRequest,
	)
	require.NoError(t, err)
	outCommits = listCommitResponse.CommitInfo
	require.Equal(t, 1, len(outCommits))

	buffer.Reset()
	require.NoError(t, c.GetFile(pipelineName, outCommits[0].Commit.ID, "file", 0, 0, "", nil, &buffer))
	lines = strings.Split(strings.TrimSpace(buffer.String()), "\n")
	require.Equal(t, 4*numfiles*numfiles, len(lines))
	for _, line := range lines {
		require.Equal(t, len(content)*2, len(line))
	}
}

func TestPipelineWithGlobalMethod(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}
	t.Parallel()
	c := getPachClient(t)

	globalRepo := uniqueString("inputRepo")
	require.NoError(t, c.CreateRepo(globalRepo))
	numfiles := 20

	pipelineName := uniqueString("pipeline")
	parallelism := 2
	require.NoError(t, c.CreatePipeline(
		pipelineName,
		"",
		[]string{"bash"},
		// this script simply outputs the number of files under the global repo
		[]string{fmt.Sprintf(`
numfiles=(/pfs/%s/*)
numfiles=${#numfiles[@]}
echo $numfiles > /pfs/out/file
`, globalRepo)},
		uint64(parallelism),
		[]*ppsclient.PipelineInput{
			{
				Repo:   &pfsclient.Repo{Name: globalRepo},
				Method: client.GlobalMethod,
			},
		},
	))

	content := "foo"

	commit, err := c.StartCommit(globalRepo, "", "")
	require.NoError(t, err)
	for i := 0; i < numfiles; i++ {
		_, err = c.PutFile(globalRepo, commit.ID, fmt.Sprintf("file%d", i), strings.NewReader(content))
		require.NoError(t, err)
	}
	require.NoError(t, c.FinishCommit(globalRepo, commit.ID))

	listCommitRequest := &pfsclient.ListCommitRequest{
		Repo:       []*pfsclient.Repo{{pipelineName}},
		CommitType: pfsclient.CommitType_COMMIT_TYPE_READ,
		Block:      true,
	}
	listCommitResponse, err := c.PfsAPIClient.ListCommit(
		context.Background(),
		listCommitRequest,
	)
	require.NoError(t, err)
	outCommits := listCommitResponse.CommitInfo
	require.Equal(t, 1, len(outCommits))

	fileInfos, err := c.ListFile(pipelineName, outCommits[0].Commit.ID, "", "", nil, false)
	require.NoError(t, err)
	require.Equal(t, 1, len(fileInfos))

	var buffer bytes.Buffer
	require.NoError(t, c.GetFile(pipelineName, outCommits[0].Commit.ID, "file", 0, 0, "", nil, &buffer))
	lines := strings.Split(strings.TrimSpace(buffer.String()), "\n")
	require.Equal(t, parallelism, len(lines)) // each job outputs one line
	for _, line := range lines {
		require.Equal(t, fmt.Sprintf("%d", numfiles), line)
	}
}

func TestPipelineWithPrevRepoAndIncrementalReduceMethod(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}
	t.Parallel()
	c := getPachClient(t)

	repo := uniqueString("repo")
	require.NoError(t, c.CreateRepo(repo))

	pipelineName := uniqueString("pipeline")
	require.NoError(t, c.CreatePipeline(
		pipelineName,
		"",
		[]string{"bash"},
		[]string{fmt.Sprintf(`
cp /pfs/%s/file /pfs/out/file
if [ -d "/pfs/prev" ]; then
  cp /pfs/prev/file /pfs/out/file
fi
`, repo)},
		1,
		[]*ppsclient.PipelineInput{
			{
				Repo:   &pfsclient.Repo{Name: repo},
				Method: client.IncrementalReduceMethod,
			},
		},
	))

	commit1, err := c.StartCommit(repo, "", "")
	require.NoError(t, err)
	_, err = c.PutFile(repo, commit1.ID, "file", strings.NewReader("foo\n"))
	require.NoError(t, c.FinishCommit(repo, commit1.ID))

	listCommitRequest := &pfsclient.ListCommitRequest{
		Repo:       []*pfsclient.Repo{{pipelineName}},
		CommitType: pfsclient.CommitType_COMMIT_TYPE_READ,
		Block:      true,
	}
	listCommitResponse, err := c.PfsAPIClient.ListCommit(
		context.Background(),
		listCommitRequest,
	)
	require.NoError(t, err)
	outCommits := listCommitResponse.CommitInfo
	require.Equal(t, 1, len(outCommits))

	var buffer bytes.Buffer
	require.NoError(t, c.GetFile(pipelineName, outCommits[0].Commit.ID, "file", 0, 0, "", nil, &buffer))
	lines := strings.Split(strings.TrimSpace(buffer.String()), "\n")
	require.Equal(t, 1, len(lines))
	require.Equal(t, "foo", lines[0])

	commit2, err := c.StartCommit(repo, commit1.ID, "")
	require.NoError(t, err)
	_, err = c.PutFile(repo, commit2.ID, "file", strings.NewReader("bar\n"))
	require.NoError(t, c.FinishCommit(repo, commit2.ID))

	listCommitRequest = &pfsclient.ListCommitRequest{
		Repo:       []*pfsclient.Repo{{pipelineName}},
		CommitType: pfsclient.CommitType_COMMIT_TYPE_READ,
		Block:      true,
		FromCommit: []*pfsclient.Commit{outCommits[0].Commit},
	}
	listCommitResponse, err = c.PfsAPIClient.ListCommit(
		context.Background(),
		listCommitRequest,
	)
	require.NoError(t, err)
	outCommits = listCommitResponse.CommitInfo
	require.Equal(t, 1, len(outCommits))

	var buffer2 bytes.Buffer
	require.NoError(t, c.GetFile(pipelineName, outCommits[0].Commit.ID, "file", 0, 0, "", nil, &buffer2))
	lines = strings.Split(strings.TrimSpace(buffer2.String()), "\n")
	require.Equal(t, 3, len(lines))
	require.Equal(t, "foo", lines[0])
	require.Equal(t, "bar", lines[1])
	require.Equal(t, "foo", lines[2])
}

func TestPipelineThatUseNonexistentInputs(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}
	t.Parallel()
	c := getPachClient(t)

	pipelineName := uniqueString("pipeline")
	require.YesError(t, c.CreatePipeline(
		pipelineName,
		"",
		[]string{"bash"},
		[]string{""},
		1,
		[]*ppsclient.PipelineInput{
			{
				Repo: &pfsclient.Repo{Name: "nonexistent"},
			},
		},
	))
}

func TestPipelineWhoseInputsGetDeleted(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}
	t.Parallel()
	c := getPachClient(t)

	repo := uniqueString("repo")
	require.NoError(t, c.CreateRepo(repo))

	pipelineName := uniqueString("pipeline")
	require.NoError(t, c.CreatePipeline(
		pipelineName,
		"",
		[]string{"bash"},
		[]string{fmt.Sprintf(``, repo)},
		1,
		[]*ppsclient.PipelineInput{
			{
				Repo: &pfsclient.Repo{Name: repo},
			},
		},
	))

	// Shouldn't be able to delete the input repo because the pipeline
	// is still running
	require.YesError(t, c.DeleteRepo(repo))

	// The correct flow to delete the input repo
	require.NoError(t, c.DeletePipeline(pipelineName))
	require.NoError(t, c.DeleteRepo(pipelineName))
	require.NoError(t, c.DeleteRepo(repo))
}

// This test fails if you updated some static assets (such as doc/pipeline_spec.md)
// that are used in code but forgot to run:
// $ make assets
func TestAssets(t *testing.T) {
	assetPaths := []string{"doc/pipeline_spec.md"}

	for _, path := range assetPaths {
		doc, err := ioutil.ReadFile(filepath.Join(os.Getenv("GOPATH"), "src/github.com/pachyderm/pachyderm/", path))
		if err != nil {
			t.Fatal(err)
		}

		asset, err := pachyderm.Asset(path)
		if err != nil {
			t.Fatal(err)
		}

		require.Equal(t, doc, asset)
	}
}

// TestProvenance creates a pipeline DAG that's not a transitive reduction
// It looks like this:
// A
// | \
// v  v
// B-->C
// When we commit to A we expect to see 1 commit on C rather than 2.
func TestProvenance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	t.Parallel()
	c := getPachClient(t)
	aRepo := uniqueString("A")
	require.NoError(t, c.CreateRepo(aRepo))
	bPipeline := uniqueString("B")
	require.NoError(t, c.CreatePipeline(
		bPipeline,
		"",
		[]string{"cp", path.Join("/pfs", aRepo, "file"), "/pfs/out/file"},
		nil,
		1,
		[]*ppsclient.PipelineInput{{Repo: client.NewRepo(aRepo)}},
	))
	cPipeline := uniqueString("C")
	require.NoError(t, c.CreatePipeline(
		cPipeline,
		"",
		[]string{"sh"},
		[]string{fmt.Sprintf("diff %s %s >/pfs/out/file",
			path.Join("/pfs", aRepo, "file"), path.Join("/pfs", bPipeline, "file"))},
		1,
		[]*ppsclient.PipelineInput{
			{Repo: client.NewRepo(aRepo)},
			{Repo: client.NewRepo(bPipeline)},
		},
	))
	// commit to aRepo
	commit1, err := c.StartCommit(aRepo, "", "master")
	require.NoError(t, err)
	_, err = c.PutFile(aRepo, commit1.ID, "file", strings.NewReader("foo\n"))
	require.NoError(t, err)
	require.NoError(t, c.FinishCommit(aRepo, commit1.ID))
	commitInfos, err := c.FlushCommit([]*pfsclient.Commit{client.NewCommit(aRepo, commit1.ID)}, nil)
	require.NoError(t, err)
	require.Equal(t, 2, len(commitInfos))

	commit2, err := c.StartCommit(aRepo, "", "master")
	require.NoError(t, err)
	_, err = c.PutFile(aRepo, commit2.ID, "file", strings.NewReader("bar\n"))
	require.NoError(t, err)
	require.NoError(t, c.FinishCommit(aRepo, commit2.ID))
	commitInfos, err = c.FlushCommit([]*pfsclient.Commit{client.NewCommit(aRepo, commit2.ID)}, nil)
	require.NoError(t, err)
	require.Equal(t, 2, len(commitInfos))

	// There should only be 2 commits on cRepo
	commitInfos, err = c.ListCommit([]string{cPipeline}, nil, pfsclient.CommitType_COMMIT_TYPE_READ, false, false, nil)
	require.NoError(t, err)
	require.Equal(t, 2, len(commitInfos))
	for _, commitInfo := range commitInfos {
		// C takes the diff of 2 files that should always be the same, so we
		// expect no output and thus no file
		_, err := c.InspectFile(cPipeline, commitInfo.Commit.ID, "file", "", nil)
		require.YesError(t, err)
	}
}

// TestFlushCommit
func TestFlushCommit(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	t.Parallel()
	c := getPachClient(t)
	prefix := uniqueString("repo")
	makeRepoName := func(i int) string {
		return fmt.Sprintf("%s_%d", prefix, i)
	}

	sourceRepo := makeRepoName(0)
	require.NoError(t, c.CreateRepo(sourceRepo))

	// Create a five-stage pipeline
	numStages := 5
	for i := 0; i < numStages; i++ {
		repo := makeRepoName(i)
		require.NoError(t, c.CreatePipeline(
			makeRepoName(i+1),
			"",
			[]string{"cp", path.Join("/pfs", repo, "file"), "/pfs/out/file"},
			nil,
			1,
			[]*ppsclient.PipelineInput{{Repo: client.NewRepo(repo)}},
		))
	}

	test := func(parent string) string {
		commit, err := c.StartCommit(sourceRepo, parent, "")
		require.NoError(t, err)
		_, err = c.PutFile(sourceRepo, commit.ID, "file", strings.NewReader("foo\n"))
		require.NoError(t, err)
		require.NoError(t, c.FinishCommit(sourceRepo, commit.ID))
		commitInfos, err := c.FlushCommit([]*pfsclient.Commit{client.NewCommit(sourceRepo, commit.ID)}, nil)
		require.NoError(t, err)
		require.Equal(t, numStages, len(commitInfos))
		return commit.ID
	}

	// Run the test twice, once on a orphan commit and another on
	// a commit with a parent
	commit := test("")
	test(commit)
}

// TestFlushCommitWithFailure is similar to TestFlushCommit except that
// the pipeline is designed to fail
func TestFlushCommitWithFailure(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	t.Parallel()
	c := getPachClient(t)
	prefix := uniqueString("repo")
	makeRepoName := func(i int) string {
		return fmt.Sprintf("%s_%d", prefix, i)
	}

	sourceRepo := makeRepoName(0)
	require.NoError(t, c.CreateRepo(sourceRepo))

	// Create a five-stage pipeline; the third stage is designed to fail
	numStages := 5
	for i := 0; i < numStages; i++ {
		fileName := "file"
		if i == 3 {
			fileName = "nonexistent"
		}
		repo := makeRepoName(i)
		require.NoError(t, c.CreatePipeline(
			makeRepoName(i+1),
			"",
			[]string{"cp", path.Join("/pfs", repo, fileName), "/pfs/out/file"},
			nil,
			1,
			[]*ppsclient.PipelineInput{{Repo: client.NewRepo(repo)}},
		))
	}

	commit, err := c.StartCommit(sourceRepo, "", "")
	require.NoError(t, err)
	_, err = c.PutFile(sourceRepo, commit.ID, "file", strings.NewReader("foo\n"))
	require.NoError(t, err)
	require.NoError(t, c.FinishCommit(sourceRepo, commit.ID))
	_, err = c.FlushCommit([]*pfsclient.Commit{client.NewCommit(sourceRepo, commit.ID)}, nil)
	fmt.Println(err.Error())
	require.YesError(t, err)
}

// TestRecreatingPipeline tracks #432
func TestRecreatePipeline(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	t.Parallel()
	c := getPachClient(t)
	repo := uniqueString("data")
	require.NoError(t, c.CreateRepo(repo))
	pipeline := uniqueString("pipeline")
	createPipelineAndRunJob := func() {
		require.NoError(t, c.CreatePipeline(
			pipeline,
			"",
			[]string{"cp", path.Join("/pfs", repo, "file"), "/pfs/out/file"},
			nil,
			1,
			[]*ppsclient.PipelineInput{{Repo: client.NewRepo(repo)}},
		))

		commit, err := c.StartCommit(repo, "", "")
		require.NoError(t, err)
		_, err = c.PutFile(repo, commit.ID, "file", strings.NewReader("foo"))
		require.NoError(t, err)
		require.NoError(t, c.FinishCommit(repo, commit.ID))

		listCommitRequest := &pfsclient.ListCommitRequest{
			Repo:       []*pfsclient.Repo{{pipeline}},
			CommitType: pfsclient.CommitType_COMMIT_TYPE_READ,
			Block:      true,
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
		defer cancel()
		listCommitResponse, err := c.PfsAPIClient.ListCommit(
			ctx,
			listCommitRequest,
		)
		require.NoError(t, err)
		outCommits := listCommitResponse.CommitInfo
		require.Equal(t, 1, len(outCommits))
	}

	// Do it twice.  We expect jobs to be created on both runs.
	createPipelineAndRunJob()
	require.NoError(t, c.DeleteRepo(pipeline))
	require.NoError(t, c.DeletePipeline(pipeline))
	createPipelineAndRunJob()
}

func TestPipelineState(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	t.Parallel()
	c := getPachClient(t)
	repo := uniqueString("data")
	require.NoError(t, c.CreateRepo(repo))
	pipeline := uniqueString("pipeline")
	require.NoError(t, c.CreatePipeline(
		pipeline,
		"",
		[]string{"cp", path.Join("/pfs", repo, "file"), "/pfs/out/file"},
		nil,
		1,
		[]*ppsclient.PipelineInput{{Repo: client.NewRepo(repo)}},
	))

	time.Sleep(5 * time.Second) // wait for this pipeline to get picked up
	pipelineInfo, err := c.InspectPipeline(pipeline)
	require.NoError(t, err)
	require.Equal(t, ppsclient.PipelineState_PIPELINE_RUNNING, pipelineInfo.State)

	// Now we introduce an error to the pipeline by removing its output repo
	// and starting a job
	require.NoError(t, c.DeleteRepo(pipeline))
	commit, err := c.StartCommit(repo, "", "")
	require.NoError(t, err)
	_, err = c.PutFile(repo, commit.ID, "file", strings.NewReader("foo"))
	require.NoError(t, err)
	require.NoError(t, c.FinishCommit(repo, commit.ID))

	// So the state of the pipeline will alternate between running and
	// restarting.  We just want to make sure that it has definitely restarted.
	var states []interface{}
	for i := 0; i < 10; i++ {
		time.Sleep(1 * time.Second)
		pipelineInfo, err = c.InspectPipeline(pipeline)
		require.NoError(t, err)
		states = append(states, pipelineInfo.State)

	}
	require.EqualOneOf(t, states, ppsclient.PipelineState_PIPELINE_RESTARTING)
}

func TestClusterFunctioningAfterMembershipChange(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	k := getKubeClient(t)
	scalePachd(t, k)
	// Wait for the cluster to stablize... ideally we shouldn't have to
	// do that.
	time.Sleep(30 * time.Second)
	TestJob(t)
}

// scalePachd scales the number of pachd nodes to anywhere from 1 to
// twice the original number
// It's guaranteed that the new replica number will be different from
// the original
func scalePachd(t *testing.T, k *kube.Client) {
	rc := k.ReplicationControllers(api.NamespaceDefault)
	pachdRc, err := rc.Get("pachd")
	require.NoError(t, err)
	originalReplicas := pachdRc.Spec.Replicas
	for {
		pachdRc.Spec.Replicas = rand.Intn(originalReplicas*2) + 1
		if pachdRc.Spec.Replicas != originalReplicas {
			break
		}
	}
	fmt.Printf("scaling pachd to %d replicas\n", pachdRc.Spec.Replicas)
	_, err = rc.Update(pachdRc)
	require.NoError(t, err)
}

func TestScrubbedErrors(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	t.Parallel()
	c := getPachClient(t)

	_, err := c.InspectPipeline("blah")
	require.Equal(t, "PipelineInfos blah not found", err.Error())

	err = c.CreatePipeline(
		"lskdjf$#%^ERTYC",
		"",
		[]string{},
		nil,
		1,
		[]*ppsclient.PipelineInput{{Repo: &pfsclient.Repo{Name: "test"}}},
	)
	require.Equal(t, "Repo test not found", err.Error())

	_, err = c.CreateJob("askjdfhgsdflkjh", []string{}, []string{}, 0, []*ppsclient.JobInput{client.NewJobInput("bogusRepo", "bogusCommit", client.DefaultMethod)}, "")
	require.Matches(t, "Repo job_.* not found", err.Error())

	_, err = c.InspectJob("blah", true)
	require.Equal(t, "JobInfos blah not found", err.Error())

	home := os.Getenv("HOME")
	f, err := os.Create(filepath.Join(home, "/tmpfile"))
	defer func() {
		os.Remove(filepath.Join(home, "/tmpfile"))
	}()
	require.NoError(t, err)
	err = c.GetLogs("bogusJobId", f)
	require.Equal(t, "Job bogusJobId not found", err.Error())

}

func getPachClient(t *testing.T) *client.APIClient {
	client, err := client.NewFromAddress("0.0.0.0:30650")
	require.NoError(t, err)
	return client
}

func getKubeClient(t *testing.T) *kube.Client {
	config := &kube.Config{
		Host:     "0.0.0.0:8080",
		Insecure: false,
	}
	k, err := kube.New(config)
	require.NoError(t, err)
	return k
}

func uniqueString(prefix string) string {
	return prefix + "_" + uuid.NewWithoutDashes()[0:12]
}

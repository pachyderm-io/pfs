package server

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net"
	"net/http"
	"os"
	"path"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/docker/go-units"
	"github.com/gogo/protobuf/proto"
	"github.com/pachyderm/pachyderm/v2/src/auth"
	"github.com/pachyderm/pachyderm/v2/src/client"
	"github.com/pachyderm/pachyderm/v2/src/internal/ancestry"
	"github.com/pachyderm/pachyderm/v2/src/internal/backoff"
	"github.com/pachyderm/pachyderm/v2/src/internal/errors"
	"github.com/pachyderm/pachyderm/v2/src/internal/errutil"
	"github.com/pachyderm/pachyderm/v2/src/internal/grpcutil"
	"github.com/pachyderm/pachyderm/v2/src/internal/ppsutil"
	"github.com/pachyderm/pachyderm/v2/src/internal/pretty"
	"github.com/pachyderm/pachyderm/v2/src/internal/require"
	tu "github.com/pachyderm/pachyderm/v2/src/internal/testutil"
	"github.com/pachyderm/pachyderm/v2/src/internal/testutil/random"
	"github.com/pachyderm/pachyderm/v2/src/internal/uuid"
	"github.com/pachyderm/pachyderm/v2/src/pfs"
	"github.com/pachyderm/pachyderm/v2/src/pps"
	pfspretty "github.com/pachyderm/pachyderm/v2/src/server/pfs/pretty"
	ppspretty "github.com/pachyderm/pachyderm/v2/src/server/pps/pretty"

	"github.com/gogo/protobuf/types"
	globlib "github.com/pachyderm/ohmyglob"
	"golang.org/x/sync/errgroup"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func newCountBreakFunc(maxCount int) func(func() error) error {
	var count int
	return func(cb func() error) error {
		if err := cb(); err != nil {
			return err
		}
		count++
		if count == maxCount {
			return errutil.ErrBreak
		}
		return nil
	}
}

func TestSimplePipeline(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())

	dataRepo := tu.UniqueString("TestSimplePipeline_data")
	require.NoError(t, c.CreateRepo(dataRepo))

	commit1, err := c.StartCommit(dataRepo, "master")
	require.NoError(t, err)
	require.NoError(t, c.PutFile(commit1, "file", strings.NewReader("foo"), client.WithAppendPutFile()))
	require.NoError(t, c.FinishCommit(dataRepo, commit1.Branch.Name, commit1.ID))

	pipeline := tu.UniqueString("TestSimplePipeline")
	require.NoError(t, c.CreatePipeline(
		pipeline,
		"",
		[]string{"bash"},
		[]string{
			fmt.Sprintf("cp /pfs/%s/* /pfs/out/", dataRepo),
		},
		&pps.ParallelismSpec{
			Constant: 1,
		},
		client.NewPFSInput(dataRepo, "/*"),
		"",
		false,
	))

	commitInfo, err := c.InspectCommit(pipeline, "master", "")
	require.NoError(t, err)
	commitInfos, err := c.WaitCommitSetAll(commitInfo.Commit.ID)
	require.NoError(t, err)
	// The commitset should have a commit in: data, spec, pipeline, meta
	// the last two are dependent upon the first two, so should come later
	// in topological ordering
	require.Equal(t, 4, len(commitInfos))
	var commitRepos []*pfs.Repo
	for _, info := range commitInfos {
		commitRepos = append(commitRepos, info.Commit.Branch.Repo)
	}
	require.EqualOneOf(t, commitRepos[:2], client.NewRepo(dataRepo))
	require.EqualOneOf(t, commitRepos[:2], client.NewSystemRepo(pipeline, pfs.SpecRepoType))
	require.EqualOneOf(t, commitRepos[2:], client.NewRepo(pipeline))
	require.EqualOneOf(t, commitRepos[2:], client.NewSystemRepo(pipeline, pfs.MetaRepoType))

	var buf bytes.Buffer
	for _, info := range commitInfos {
		if proto.Equal(info.Commit.Branch.Repo, client.NewRepo(pipeline)) {
			require.NoError(t, c.GetFile(info.Commit, "file", &buf))
			require.Equal(t, "foo", buf.String())
		}
	}
}

func TestRepoSize(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())

	// create a data repo
	dataRepo := tu.UniqueString("TestRepoSize_data")
	require.NoError(t, c.CreateRepo(dataRepo))

	// create a pipeline
	pipeline := tu.UniqueString("TestRepoSize")
	require.NoError(t, c.CreatePipeline(
		pipeline,
		"",
		[]string{"bash"},
		[]string{
			fmt.Sprintf("cp /pfs/%s/* /pfs/out/", dataRepo),
		},
		&pps.ParallelismSpec{
			Constant: 1,
		},
		client.NewPFSInput(dataRepo, "/*"),
		"",
		false,
	))

	// put a file without an open commit - should count towards repo size
	require.NoError(t, c.PutFile(client.NewCommit(dataRepo, "master", ""), "file2", strings.NewReader("foo"), client.WithAppendPutFile()))

	// put a file on another branch - should not count towards repo size
	require.NoError(t, c.PutFile(client.NewCommit(dataRepo, "develop", ""), "file3", strings.NewReader("foo"), client.WithAppendPutFile()))

	// put a file on an open commit - should count toward repo size
	commit1, err := c.StartCommit(dataRepo, "master")
	require.NoError(t, err)
	require.NoError(t, c.PutFile(commit1, "file1", strings.NewReader("foo"), client.WithAppendPutFile()))
	require.NoError(t, c.FinishCommit(dataRepo, commit1.Branch.Name, commit1.ID))

	// wait for everything to be processed
	commitInfos, err := c.WaitCommitSetAll(commit1.ID)
	require.NoError(t, err)
	require.Equal(t, 4, len(commitInfos))

	// check data repo size
	repoInfo, err := c.InspectRepo(dataRepo)
	require.NoError(t, err)
	require.Equal(t, uint64(6), repoInfo.SizeBytes)

	// check pipeline repo size
	repoInfo, err = c.InspectRepo(pipeline)
	require.NoError(t, err)
	require.Equal(t, uint64(6), repoInfo.SizeBytes)

	// ensure size is updated when we delete a commit
	require.NoError(t, c.SquashCommitSet(commit1.ID))
	repoInfo, err = c.InspectRepo(dataRepo)
	require.NoError(t, err)
	require.Equal(t, uint64(3), repoInfo.SizeBytes)
	repoInfo, err = c.InspectRepo(pipeline)
	require.NoError(t, err)
	require.Equal(t, uint64(3), repoInfo.SizeBytes)
}

func TestPFSPipeline(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())

	dataRepo := tu.UniqueString("TestPFSPipeline_data")
	require.NoError(t, c.CreateRepo(dataRepo))

	pipeline := tu.UniqueString("TestPFSPipeline")
	require.NoError(t, c.CreatePipeline(
		pipeline,
		"",
		[]string{"bash"},
		[]string{
			fmt.Sprintf("cp /pfs/%s/* /pfs/out/", dataRepo),
		},
		&pps.ParallelismSpec{
			Constant: 1,
		},
		client.NewPFSInput(dataRepo, "/*"),
		"",
		false,
	))

	commit1, err := c.StartCommit(dataRepo, "master")
	require.NoError(t, err)
	require.NoError(t, c.PutFile(commit1, "file", strings.NewReader("foo"), client.WithAppendPutFile()))
	require.NoError(t, c.FinishCommit(dataRepo, commit1.Branch.Name, commit1.ID))

	commitInfos, err := c.WaitCommitSetAll(commit1.ID)
	require.NoError(t, err)
	require.Equal(t, 4, len(commitInfos))

	var buf bytes.Buffer
	outputCommit := client.NewCommit(pipeline, "master", commit1.ID)
	require.NoError(t, c.GetFile(outputCommit, "file", &buf))
	require.Equal(t, "foo", buf.String())
}

func TestPipelineWithParallelism(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())

	dataRepo := tu.UniqueString("TestPipelineWithParallelism_data")
	require.NoError(t, c.CreateRepo(dataRepo))

	pipeline := tu.UniqueString("pipeline")
	require.NoError(t, c.CreatePipeline(
		pipeline,
		"",
		[]string{"bash"},
		[]string{
			fmt.Sprintf("cp /pfs/%s/* /pfs/out/", dataRepo),
		},
		&pps.ParallelismSpec{
			Constant: 4,
		},
		client.NewPFSInput(dataRepo, "/*"),
		"",
		false,
	))

	numFiles := 200
	commit1, err := c.StartCommit(dataRepo, "master")
	require.NoError(t, err)
	for i := 0; i < numFiles; i++ {
		require.NoError(t, c.PutFile(commit1, fmt.Sprintf("file-%d", i), strings.NewReader(fmt.Sprintf("%d", i)), client.WithAppendPutFile()))
	}
	require.NoError(t, c.FinishCommit(dataRepo, commit1.Branch.Name, commit1.ID))

	commitInfos, err := c.WaitCommitSetAll(commit1.ID)
	require.NoError(t, err)
	require.Equal(t, 4, len(commitInfos))

	outputCommit := client.NewCommit(pipeline, "master", commit1.ID)
	for i := 0; i < numFiles; i++ {
		var buf bytes.Buffer
		require.NoError(t, c.GetFile(outputCommit, fmt.Sprintf("file-%d", i), &buf))
		require.Equal(t, fmt.Sprintf("%d", i), buf.String())
	}
}

func TestPipelineWithLargeFiles(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())

	dataRepo := tu.UniqueString("TestPipelineWithLargeFiles_data")
	require.NoError(t, c.CreateRepo(dataRepo))

	pipeline := tu.UniqueString("pipeline")
	require.NoError(t, c.CreatePipeline(
		pipeline,
		"",
		[]string{"bash"},
		[]string{
			fmt.Sprintf("cp /pfs/%s/* /pfs/out/", dataRepo),
		},
		nil,
		client.NewPFSInput(dataRepo, "/*"),
		"",
		false,
	))

	random.SeedRand(99)
	numFiles := 10
	var fileContents []string
	commit1, err := c.StartCommit(dataRepo, "master")
	require.NoError(t, err)
	chunkSize := int(pfs.ChunkSize / 32) // We used to use a full ChunkSize, but it was increased which caused this test to take too long.
	for i := 0; i < numFiles; i++ {
		fileContent := random.String(chunkSize + i*units.MB)
		require.NoError(t, c.PutFile(commit1, fmt.Sprintf("file-%d", i), strings.NewReader(fileContent), client.WithAppendPutFile()))
		fileContents = append(fileContents, fileContent)
	}
	require.NoError(t, c.FinishCommit(dataRepo, commit1.Branch.Name, commit1.ID))

	commitInfos, err := c.WaitCommitSetAll(commit1.ID)
	require.NoError(t, err)
	require.Equal(t, 4, len(commitInfos))

	outputCommit := client.NewCommit(pipeline, "master", commit1.ID)

	for i := 0; i < numFiles; i++ {
		var buf bytes.Buffer
		fileName := fmt.Sprintf("file-%d", i)

		fileInfo, err := c.InspectFile(outputCommit, fileName)
		require.NoError(t, err)
		require.Equal(t, chunkSize+i*units.MB, int(fileInfo.Details.SizeBytes))

		require.NoError(t, c.GetFile(outputCommit, fileName, &buf))
		// we don't wanna use the `require` package here since it prints
		// the strings, which would clutter the output.
		if fileContents[i] != buf.String() {
			t.Fatalf("file content does not match")
		}
	}
}

func TestDatumDedup(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())

	dataRepo := tu.UniqueString("TestDatumDedup_data")
	require.NoError(t, c.CreateRepo(dataRepo))

	pipeline := tu.UniqueString("pipeline")
	// This pipeline sleeps for 10 secs per datum
	require.NoError(t, c.CreatePipeline(
		pipeline,
		"",
		[]string{"bash"},
		[]string{
			"sleep 10",
		},
		nil,
		client.NewPFSInput(dataRepo, "/*"),
		"",
		false,
	))

	commit1, err := c.StartCommit(dataRepo, "master")
	require.NoError(t, err)
	require.NoError(t, c.PutFile(commit1, "file", strings.NewReader("foo"), client.WithAppendPutFile()))
	require.NoError(t, c.FinishCommit(dataRepo, commit1.Branch.Name, commit1.ID))

	commitInfos, err := c.WaitCommitSetAll(commit1.ID)
	require.NoError(t, err)
	require.Equal(t, 4, len(commitInfos))

	commit2, err := c.StartCommit(dataRepo, "master")
	require.NoError(t, err)
	require.NoError(t, c.FinishCommit(dataRepo, commit2.Branch.Name, commit2.ID))

	// Since we did not change the datum, the datum should not be processed
	// again, which means that the job should complete instantly.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	_, err = c.WithCtx(ctx).WaitCommitSetAll(commit2.ID)
	require.NoError(t, err)
}

func TestPipelineInputDataModification(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())

	dataRepo := tu.UniqueString("TestPipelineInputDataModification_data")
	require.NoError(t, c.CreateRepo(dataRepo))

	pipeline := tu.UniqueString("pipeline")
	require.NoError(t, c.CreatePipeline(
		pipeline,
		"",
		[]string{"bash"},
		[]string{
			fmt.Sprintf("cp /pfs/%s/* /pfs/out/", dataRepo),
		},
		nil,
		client.NewPFSInput(dataRepo, "/*"),
		"",
		false,
	))

	commit1, err := c.StartCommit(dataRepo, "master")
	require.NoError(t, err)
	require.NoError(t, c.PutFile(commit1, "file", strings.NewReader("foo"), client.WithAppendPutFile()))
	require.NoError(t, c.FinishCommit(dataRepo, commit1.Branch.Name, commit1.ID))

	commitInfos, err := c.WaitCommitSetAll(commit1.ID)
	require.NoError(t, err)
	require.Equal(t, 4, len(commitInfos))

	var buf bytes.Buffer
	outputCommit := client.NewCommit(pipeline, "master", commit1.ID)
	require.NoError(t, c.GetFile(outputCommit, "file", &buf))
	require.Equal(t, "foo", buf.String())

	// replace the contents of 'file' in dataRepo (from "foo" to "bar")
	commit2, err := c.StartCommit(dataRepo, "master")
	require.NoError(t, err)
	require.NoError(t, c.DeleteFile(commit2, "file"))
	require.NoError(t, c.PutFile(commit2, "file", strings.NewReader("bar"), client.WithAppendPutFile()))
	require.NoError(t, c.FinishCommit(dataRepo, commit2.Branch.Name, commit2.ID))

	commitInfos, err = c.WaitCommitSetAll(commit2.ID)
	require.NoError(t, err)
	require.Equal(t, 4, len(commitInfos))

	buf.Reset()
	outputCommit = client.NewCommit(pipeline, "master", commit2.ID)
	require.NoError(t, c.GetFile(outputCommit, "file", &buf))
	require.Equal(t, "bar", buf.String())

	// Add a file to dataRepo
	commit3, err := c.StartCommit(dataRepo, "master")
	require.NoError(t, err)
	require.NoError(t, c.DeleteFile(commit3, "file"))
	require.NoError(t, c.PutFile(commit3, "file2", strings.NewReader("foo"), client.WithAppendPutFile()))
	require.NoError(t, c.FinishCommit(dataRepo, commit3.Branch.Name, commit3.ID))

	commitInfos, err = c.WaitCommitSetAll(commit3.ID)
	require.NoError(t, err)
	require.Equal(t, 4, len(commitInfos))

	outputCommit = client.NewCommit(pipeline, "master", commit3.ID)
	require.YesError(t, c.GetFile(outputCommit, "file", &buf))
	buf.Reset()
	require.NoError(t, c.GetFile(outputCommit, "file2", &buf))
	require.Equal(t, "foo", buf.String())

	commitInfos, err = c.ListCommit(client.NewRepo(pipeline), client.NewCommit(pipeline, "master", ""), nil, 0)
	require.NoError(t, err)
	require.Equal(t, 4, len(commitInfos))
}

func TestMultipleInputsFromTheSameBranch(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())

	dataRepo := tu.UniqueString("TestMultipleInputsFromTheSameBranch_data")
	require.NoError(t, c.CreateRepo(dataRepo))

	pipeline := tu.UniqueString("pipeline")
	require.NoError(t, c.CreatePipeline(
		pipeline,
		"",
		[]string{"bash"},
		[]string{
			"cat /pfs/out/file",
			"cat /pfs/dirA/dirA/file >> /pfs/out/file",
			"cat /pfs/dirB/dirB/file >> /pfs/out/file",
		},
		nil,
		client.NewCrossInput(
			client.NewPFSInputOpts("dirA", dataRepo, "", "/dirA/*", "", "", false, false, nil),
			client.NewPFSInputOpts("dirB", dataRepo, "", "/dirB/*", "", "", false, false, nil),
		),
		"",
		false,
	))

	commit1, err := c.StartCommit(dataRepo, "master")
	require.NoError(t, err)
	require.NoError(t, c.PutFile(commit1, "dirA/file", strings.NewReader("foo\n"), client.WithAppendPutFile()))
	require.NoError(t, c.PutFile(commit1, "dirB/file", strings.NewReader("foo\n"), client.WithAppendPutFile()))
	require.NoError(t, c.FinishCommit(dataRepo, commit1.Branch.Name, commit1.ID))

	commitInfos, err := c.WaitCommitSetAll(commit1.ID)
	require.NoError(t, err)
	require.Equal(t, 4, len(commitInfos))

	var buf bytes.Buffer
	outputCommit := client.NewCommit(pipeline, "master", commit1.ID)
	require.NoError(t, c.GetFile(outputCommit, "file", &buf))
	require.Equal(t, "foo\nfoo\n", buf.String())

	commit2, err := c.StartCommit(dataRepo, "master")
	require.NoError(t, err)
	require.NoError(t, c.PutFile(commit2, "dirA/file", strings.NewReader("bar\n"), client.WithAppendPutFile()))
	require.NoError(t, c.FinishCommit(dataRepo, commit2.Branch.Name, commit2.ID))

	commitInfos, err = c.WaitCommitSetAll(commit2.ID)
	require.NoError(t, err)
	require.Equal(t, 4, len(commitInfos))

	buf.Reset()
	outputCommit = client.NewCommit(pipeline, "master", commit2.ID)
	require.NoError(t, c.GetFile(outputCommit, "file", &buf))
	require.Equal(t, "foo\nbar\nfoo\n", buf.String())

	commit3, err := c.StartCommit(dataRepo, "master")
	require.NoError(t, err)
	require.NoError(t, c.PutFile(commit3, "dirB/file", strings.NewReader("buzz\n"), client.WithAppendPutFile()))
	require.NoError(t, c.FinishCommit(dataRepo, commit3.Branch.Name, commit3.ID))

	commitInfos, err = c.WaitCommitSetAll(commit3.ID)
	require.NoError(t, err)
	require.Equal(t, 4, len(commitInfos))

	buf.Reset()
	outputCommit = client.NewCommit(pipeline, "master", commit3.ID)
	require.NoError(t, c.GetFile(outputCommit, "file", &buf))
	require.Equal(t, "foo\nbar\nfoo\nbuzz\n", buf.String())

	commitInfos, err = c.ListCommit(client.NewRepo(pipeline), client.NewCommit(pipeline, "master", ""), nil, 0)
	require.NoError(t, err)
	require.Equal(t, 4, len(commitInfos))
}

func TestMultipleInputsFromTheSameRepoDifferentBranches(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())

	dataRepo := tu.UniqueString("TestMultipleInputsFromTheSameRepoDifferentBranches_data")
	require.NoError(t, c.CreateRepo(dataRepo))

	branchA := "branchA"
	branchB := "branchB"

	pipeline := tu.UniqueString("pipeline")
	// Creating this pipeline should error, because the two inputs are
	// from the same repo but they don't specify different names.
	require.NoError(t, c.CreatePipeline(
		pipeline,
		"",
		[]string{"bash"},
		[]string{
			"cat /pfs/branch-a/file >> /pfs/out/file",
			"cat /pfs/branch-b/file >> /pfs/out/file",
		},
		nil,
		client.NewCrossInput(
			client.NewPFSInputOpts("branch-a", dataRepo, branchA, "/*", "", "", false, false, nil),
			client.NewPFSInputOpts("branch-b", dataRepo, branchB, "/*", "", "", false, false, nil),
		),
		"",
		false,
	))

	commitA, err := c.StartCommit(dataRepo, branchA)
	require.NoError(t, err)
	c.PutFile(commitA, "/file", strings.NewReader("data A\n"), client.WithAppendPutFile())
	c.FinishCommit(dataRepo, commitA.Branch.Name, commitA.ID)

	commitB, err := c.StartCommit(dataRepo, branchB)
	require.NoError(t, err)
	c.PutFile(commitB, "/file", strings.NewReader("data B\n"), client.WithAppendPutFile())
	c.FinishCommit(dataRepo, commitB.Branch.Name, commitB.ID)

	commitInfos, err := c.WaitCommitSetAll(commitB.ID)
	require.NoError(t, err)
	require.Equal(t, 5, len(commitInfos))
	buffer := bytes.Buffer{}
	outputCommit := client.NewCommit(pipeline, "master", commitB.ID)
	require.NoError(t, c.GetFile(outputCommit, "file", &buffer))
	require.Equal(t, "data A\ndata B\n", buffer.String())
}

func TestRunPipeline(t *testing.T) {
	// TODO(2.0 optional): Run pipeline creates a dangling commit, but uses the stats branch for stats commits.
	// Since there is no relationship between the commits created by run pipeline, there should be no
	// relationship between the stats commits. So, the stats commits should also be dangling commits.
	// This might be easier to address when global IDs are implemented.
	t.Skip("Run pipeline does not work correctly with stats enabled")
	//if testing.Short() {
	//	t.Skip("Skipping integration tests in short mode")
	//}

	//c := tu.GetPachClient(t)
	//require.NoError(t, c.DeleteAll())

	//// Test on cross pipeline
	//t.Run("RunPipelineCross", func(t *testing.T) {
	//	dataRepo := tu.UniqueString("TestRunPipeline_data")
	//	require.NoError(t, c.CreateRepo(dataRepo))

	//	branchA := "branchA"
	//	branchB := "branchB"

	//	pipeline := tu.UniqueString("pipeline")
	//	require.NoError(t, c.CreatePipeline(
	//		pipeline,
	//		"",
	//		[]string{"bash"},
	//		[]string{
	//			"cat /pfs/branch-a/file >> /pfs/out/file",
	//			"cat /pfs/branch-b/file >> /pfs/out/file",
	//			"echo ran-pipeline",
	//		},
	//		nil,
	//		client.NewCrossInput(
	//			client.NewPFSInputOpts("branch-a", dataRepo, branchA, "/*", "", "", false, false, nil),
	//			client.NewPFSInputOpts("branch-b", dataRepo, branchB, "/*", "", "", false, false, nil),
	//		),
	//		"",
	//		false,
	//	))

	//	commitA, err := c.StartCommit(dataRepo, branchA)
	//	require.NoError(t, err)
	//	require.NoError(t, c.PutFile(dataRepo, commitA.Branch.Name, commitA.ID, "/file", strings.NewReader("data A\n"), client.WithAppendPutFile()))
	//	require.NoError(t, c.FinishCommit(dataRepo, commitA.Branch.Name, commitA.ID))

	//	commitB, err := c.StartCommit(dataRepo, branchB)
	//	require.NoError(t, err)
	//	require.NoError(t, c.PutFile(dataRepo, commitB.Branch.Name, commitB.ID, "/file", strings.NewReader("data B\n"), client.WithAppendPutFile()))
	//	require.NoError(t, c.FinishCommit(dataRepo, commitB.Branch.Name, commitB.ID))

	//	iter, err := c.FlushJob([]*pfs.Commit{commitA, commitB}, nil)
	//	require.NoError(t, err)
	//	require.Equal(t, 2, len(commits))
	//	buffer := bytes.Buffer{}
	//	require.NoError(t, c.GetFile(commits[0].Commit.Repo.Name, commits[0].Commit.Branch.Name, commits[0].Commit.ID, "file", &buffer))
	//	require.Equal(t, "data A\ndata B\n", buffer.String())

	//	commitM, err := c.StartCommit(dataRepo, "master")
	//	require.NoError(t, err)
	//	require.NoError(t, c.FinishCommit(dataRepo, commitM.Branch.Name, commitM.ID))

	//	// we should have two jobs
	//	ji, err := c.ListJob(pipeline, nil, nil, -1, true)
	//	require.NoError(t, err)
	//	require.Equal(t, 2, len(ji))
	//	// now run the pipeline
	//	require.NoError(t, c.RunPipeline(pipeline, nil, ""))
	//	// running the pipeline should create a new job
	//	require.NoError(t, backoff.Retry(func() error {
	//		jobInfos, err := c.ListJob(pipeline, nil, nil, -1, true)
	//		require.NoError(t, err)
	//		if len(jobInfos) != 3 {
	//			return errors.Errorf("expected 3 jobs, got %d", len(jobInfos))
	//		}
	//		return nil
	//	}, backoff.NewTestingBackOff()))

	//	// now run the pipeline with non-empty provenance
	//	require.NoError(t, backoff.Retry(func() error {
	//		return c.RunPipeline(pipeline, []*pfs.Commit{
	//			client.NewCommit(dataRepo, "branchA", commitA.ID),
	//		}, "")
	//	}, backoff.NewTestingBackOff()))

	//	// running the pipeline should create a new job
	//	require.NoError(t, backoff.Retry(func() error {
	//		jobInfos, err := c.ListJob(pipeline, nil, nil, -1, true)
	//		require.NoError(t, err)
	//		if len(jobInfos) != 4 {
	//			return errors.Errorf("expected 4 jobs, got %d", len(jobInfos))
	//		}
	//		return nil
	//	}, backoff.NewTestingBackOff()))

	//	// add some new commits with some new info
	//	commitA2, err := c.StartCommit(dataRepo, branchA)
	//	require.NoError(t, err)
	//	require.NoError(t, c.PutFile(dataRepo, commitA2.Branch.Name, commitA2.ID, "/file", strings.NewReader("data A2\n"), client.WithAppendPutFile()))
	//	require.NoError(t, c.FinishCommit(dataRepo, commitA2.Branch.Name, commitA2.ID))

	//	commitB2, err := c.StartCommit(dataRepo, branchB)
	//	require.NoError(t, err)
	//	require.NoError(t, c.PutFile(dataRepo, commitB2.Branch.Name, commitB2.ID, "/file", strings.NewReader("data B2\n"), client.WithAppendPutFile()))
	//	require.NoError(t, c.FinishCommit(dataRepo, commitB2.Branch.Name, commitB2.ID))

	//	// and make sure the output file is updated appropriately
	//	iter, err = c.FlushJob([]*pfs.Commit{commitA2, commitB2}, nil)
	//	require.NoError(t, err)
	//	require.Equal(t, 2, len(commits))
	//	buffer = bytes.Buffer{}
	//	require.NoError(t, c.GetFile(commits[0].Commit.Repo.Name, commits[0].Commit.Branch.Name, commits[0].Commit.ID, "file", &buffer))
	//	require.Equal(t, "data A\ndata A2\ndata B\ndata B2\n", buffer.String())

	//	// now run the pipeline provenant on the old commits
	//	require.NoError(t, c.RunPipeline(pipeline, []*pfs.Commit{
	//		client.NewCommit(dataRepo, "branchA", commitA.ID),
	//		client.NewCommit(dataRepo, "branchB", commitB2.ID),
	//	}, ""))

	//	// and ensure that the file now has the info from the correct versions of the commits
	//	iter, err = c.FlushJob([]*pfs.Commit{commitA, commitB2}, nil)
	//	require.NoError(t, err)
	//	require.Equal(t, 2, len(commits))
	//	buffer = bytes.Buffer{}
	//	require.NoError(t, c.GetFile(commits[0].Commit.Repo.Name, commits[0].Commit.Branch.Name, commits[0].Commit.ID, "file", &buffer))
	//	require.Equal(t, "data A\ndata B\ndata B2\n", buffer.String())

	//	// make sure no commits with this provenance combination exist
	//	iter, err = c.FlushJob([]*pfs.Commit{commitA2, commitB}, nil)
	//	require.NoError(t, err)
	//	require.Equal(t, 0, len(commits))
	//})

	//// Test on pipeline with no commits
	//t.Run("RunPipelineEmpty", func(t *testing.T) {
	//	dataRepo := tu.UniqueString("TestRunPipeline_data")
	//	require.NoError(t, c.CreateRepo(dataRepo))

	//	pipeline := tu.UniqueString("empty-pipeline")
	//	require.NoError(t, c.CreatePipeline(
	//		pipeline,
	//		"",
	//		nil,
	//		nil,
	//		nil,
	//		nil,
	//		"",
	//		false,
	//	))

	//	// we should have two jobs
	//	ji, err := c.ListJob(pipeline, nil, nil, -1, true)
	//	require.NoError(t, err)
	//	require.Equal(t, 0, len(ji))
	//	// now run the pipeline
	//	require.YesError(t, c.RunPipeline(pipeline, nil, ""))
	//})

	//// Test on unrelated branch
	//t.Run("RunPipelineUnrelated", func(t *testing.T) {
	//	dataRepo := tu.UniqueString("TestRunPipeline_data")
	//	require.NoError(t, c.CreateRepo(dataRepo))

	//	branchA := "branchA"
	//	branchB := "branchB"

	//	pipeline := tu.UniqueString("unrelated-pipeline")
	//	require.NoError(t, c.CreatePipeline(
	//		pipeline,
	//		"",
	//		[]string{"bash"},
	//		[]string{
	//			"cat /pfs/branch-a/file >> /pfs/out/file",
	//			"cat /pfs/branch-b/file >> /pfs/out/file",
	//			"echo ran-pipeline",
	//		},
	//		nil,
	//		client.NewCrossInput(
	//			client.NewPFSInputOpts("branch-a", dataRepo, branchA, "/*", "", "", false, false, nil),
	//			client.NewPFSInputOpts("branch-b", dataRepo, branchB, "/*", "", "", false, false, nil),
	//		),
	//		"",
	//		false,
	//	))
	//	commitA, err := c.StartCommit(dataRepo, branchA)
	//	require.NoError(t, err)
	//	c.PutFile(dataRepo, commitA.Branch.Name, commitA.ID, "/file", strings.NewReader("data A\n", client.WithAppendPutFile()))
	//	c.FinishCommit(dataRepo, commitA.Branch.Name, commitA.ID)

	//	commitM, err := c.StartCommit(dataRepo, "master")
	//	require.NoError(t, err)
	//	err = c.FinishCommit(dataRepo, commitM.Branch.Name, commitM.ID)
	//	require.NoError(t, err)

	//	require.NoError(t, c.CreateBranch(dataRepo, "unrelated", "", nil))
	//	commitU, err := c.StartCommit(dataRepo, "unrelated")
	//	require.NoError(t, err)
	//	err = c.FinishCommit(dataRepo, commitU.Branch.Name, commitU.ID)
	//	require.NoError(t, err)

	//	_, err = c.FlushJob([]*pfs.Commit{commitA, commitM, commitU}, nil)
	//	require.NoError(t, err)

	//	// now run the pipeline with unrelated provenance
	//	require.YesError(t, c.RunPipeline(pipeline, []*pfs.Commit{
	//		client.NewCommit(dataRepo, "unrelated", commitU.ID)}, ""))
	//})

	//// Test with downstream pipeline
	//t.Run("RunPipelineDownstream", func(t *testing.T) {
	//	dataRepo := tu.UniqueString("TestRunPipeline_data")
	//	require.NoError(t, c.CreateRepo(dataRepo))

	//	branchA := "branchA"
	//	branchB := "branchB"

	//	pipeline := tu.UniqueString("original-pipeline")
	//	require.NoError(t, c.CreatePipeline(
	//		pipeline,
	//		"",
	//		[]string{"bash"},
	//		[]string{
	//			"cat /pfs/branch-a/file >> /pfs/out/file",
	//			"cat /pfs/branch-b/file >> /pfs/out/file",
	//			"echo ran-pipeline",
	//		},
	//		nil,
	//		client.NewCrossInput(
	//			client.NewPFSInputOpts("branch-a", dataRepo, branchA, "/*", "", "", false, false, nil),
	//			client.NewPFSInputOpts("branch-b", dataRepo, branchB, "/*", "", "", false, false, nil),
	//		),
	//		"",
	//		false,
	//	))

	//	commitA, err := c.StartCommit(dataRepo, branchA)
	//	require.NoError(t, err)
	//	c.PutFile(dataRepo, commitA.Branch.Name, commitA.ID, "/file", strings.NewReader("data A\n", client.WithAppendPutFile()))
	//	c.FinishCommit(dataRepo, commitA.Branch.Name, commitA.ID)

	//	commitB, err := c.StartCommit(dataRepo, branchB)
	//	require.NoError(t, err)
	//	c.PutFile(dataRepo, commitB.Branch.Name, commitB.ID, "/file", strings.NewReader("data B\n", client.WithAppendPutFile()))
	//	c.FinishCommit(dataRepo, commitB.Branch.Name, commitB.ID)

	//	iter, err := c.FlushJob([]*pfs.Commit{commitA, commitB}, nil)
	//	require.NoError(t, err)
	//	require.Equal(t, 2, len(commits))
	//	buffer := bytes.Buffer{}
	//	require.NoError(t, c.GetFile(commits[0].Commit.Repo.Name, commits[0].Commit.Branch.Name, commits[0].Commit.ID, "file", &buffer))
	//	require.Equal(t, "data A\ndata B\n", buffer.String())

	//	// and make sure we can attatch a downstream pipeline
	//	downstreamPipeline := tu.UniqueString("downstream-pipeline")
	//	require.NoError(t, c.CreatePipeline(
	//		downstreamPipeline,
	//		"",
	//		[]string{"/bin/bash"},
	//		[]string{"cp " + fmt.Sprintf("/pfs/%s/*", pipeline) + " /pfs/out/"},
	//		nil,
	//		client.NewPFSInput(pipeline, "/*"),
	//		"",
	//		false,
	//	))

	//	commitA2, err := c.StartCommit(dataRepo, branchA)
	//	require.NoError(t, err)
	//	err = c.FinishCommit(dataRepo, commitA2.Branch.Name, commitA2.ID)
	//	require.NoError(t, err)

	//	// there should be one job on the old commit for downstreamPipeline
	//	jobInfos, err := c.FlushJobAll([]*pfs.Commit{commitA}, []string{downstreamPipeline})
	//	require.NoError(t, err)
	//	require.Equal(t, 1, len(jobInfos))

	//	// now run the pipeline
	//	require.NoError(t, backoff.Retry(func() error {
	//		return c.RunPipeline(pipeline, []*pfs.Commit{
	//			client.NewCommit(dataRepo, branchA, commitA.ID),
	//		}, "")
	//	}, backoff.NewTestingBackOff()))

	//	// the downstream pipeline shouldn't have any new jobs, since runpipeline jobs don't propagate
	//	jobInfos, err = c.FlushJobAll([]*pfs.Commit{commitA}, []string{downstreamPipeline})
	//	require.NoError(t, err)
	//	require.Equal(t, 1, len(jobInfos))

	//	// now rerun the one job that we saw
	//	require.NoError(t, backoff.Retry(func() error {
	//		return c.RunPipeline(downstreamPipeline, nil, jobInfos[0].Job.ID)
	//	}, backoff.NewTestingBackOff()))

	//	// we should now have two jobs
	//	jobInfos, err = c.FlushJobAll([]*pfs.Commit{commitA}, []string{downstreamPipeline})
	//	require.NoError(t, err)
	//	require.Equal(t, 2, len(jobInfos))
	//})

	//// Test with a downstream pipeline who's upstream has no datum, but where the downstream still needs to succeed
	//t.Run("RunPipelineEmptyUpstream", func(t *testing.T) {
	//	dataRepo := tu.UniqueString("TestRunPipeline_data")
	//	require.NoError(t, c.CreateRepo(dataRepo))

	//	branchA := "branchA"
	//	branchB := "branchB"

	//	pipeline := tu.UniqueString("pipeline-downstream")
	//	require.NoError(t, c.CreatePipeline(
	//		pipeline,
	//		"",
	//		[]string{"bash"},
	//		[]string{
	//			"cat /pfs/branch-a/file >> /pfs/out/file",
	//			"cat /pfs/branch-b/file >> /pfs/out/file",
	//			"echo ran-pipeline",
	//		},
	//		nil,
	//		client.NewCrossInput(
	//			client.NewPFSInputOpts("branch-a", dataRepo, branchA, "/*", "", "", false, false, nil),
	//			client.NewPFSInputOpts("branch-b", dataRepo, branchB, "/*", "", "", false, false, nil),
	//		),
	//		"",
	//		false,
	//	))

	//	commitA, err := c.StartCommit(dataRepo, branchA)
	//	require.NoError(t, err)
	//	c.PutFile(dataRepo, commitA.Branch.Name, commitA.ID, "/file", strings.NewReader("data A\n", client.WithAppendPutFile()))
	//	c.FinishCommit(dataRepo, commitA.Branch.Name, commitA.ID)

	//	iter, err := c.FlushJob([]*pfs.Commit{commitA}, nil)
	//	require.NoError(t, err)
	//	require.Equal(t, 2, len(commits))

	//	// no commit to branch-b so "file" should not exist
	//	buffer := bytes.Buffer{}
	//	require.YesError(t, c.GetFile(commits[0].Commit.Repo.Name, commits[0].Commit.Branch.Name, commits[0].Commit.ID, "file", &buffer))

	//	// and make sure we can attatch a downstream pipeline
	//	downstreamPipeline := tu.UniqueString("pipelinedownstream")
	//	require.NoError(t, c.CreatePipeline(
	//		downstreamPipeline,
	//		"",
	//		[]string{"/bin/bash"},
	//		[]string{
	//			"cat /pfs/branch-a/file >> /pfs/out/file",
	//			fmt.Sprintf("cat /pfs/%s/file >> /pfs/out/file", pipeline),
	//			"echo ran-pipeline",
	//		},
	//		nil,
	//		client.NewUnionInput(
	//			client.NewPFSInputOpts("branch-a", dataRepo, branchA, "/*", "", "", false, false, nil),
	//			client.NewPFSInput(pipeline, "/*"),
	//		),
	//		"",
	//		false,
	//	))

	//	commitA2, err := c.StartCommit(dataRepo, branchA)
	//	require.NoError(t, err)
	//	err = c.FinishCommit(dataRepo, commitA2.Branch.Name, commitA2.ID)
	//	require.NoError(t, err)

	//	// there should be one job on the old commit for downstreamPipeline
	//	jobInfos, err := c.FlushJobAll([]*pfs.Commit{commitA}, []string{downstreamPipeline})
	//	require.NoError(t, err)
	//	require.Equal(t, 1, len(jobInfos))

	//	// now run the pipeline
	//	require.NoError(t, backoff.Retry(func() error {
	//		return c.RunPipeline(pipeline, []*pfs.Commit{
	//			client.NewCommit(dataRepo, branchA, commitA.ID),
	//		}, "")
	//	}, backoff.NewTestingBackOff()))

	//	buffer2 := bytes.Buffer{}
	//	require.NoError(t, c.GetFile(jobInfos[0].OutputCommit.Repo.Name, jobInfos[0].OutputCommit.Branch.Name, jobInfos[0].OutputCommit.ID, "file", &buffer2))
	//	// the union of an empty output and datA should only return a file with "data A" in it.
	//	require.Equal(t, "data A\n", buffer2.String())

	//	// add another commit to see that we can successfully do the cross and union together
	//	commitB, err := c.StartCommit(dataRepo, branchB)
	//	require.NoError(t, err)
	//	c.PutFile(dataRepo, commitB.Branch.Name, commitB.ID, "/file", strings.NewReader("data B\n", client.WithAppendPutFile()))
	//	c.FinishCommit(dataRepo, commitB.Branch.Name, commitB.ID)

	//	_, err = c.FlushJob([]*pfs.Commit{commitA, commitB}, nil)
	//	require.NoError(t, err)

	//	jobInfos, err = c.FlushJobAll([]*pfs.Commit{commitB}, []string{downstreamPipeline})
	//	require.NoError(t, err)
	//	require.Equal(t, 1, len(jobInfos))

	//	buffer3 := bytes.Buffer{}
	//	require.NoError(t, c.GetFile(jobInfos[0].OutputCommit.Repo.Name, jobInfos[0].OutputCommit.Branch.Name, jobInfos[0].OutputCommit.ID, "file", &buffer3))
	//	// now that we've added data to the other branch of the cross, we should see the union of data A along with the the crossed data.
	//	require.Equal(t, "data A\ndata A\ndata B\n", buffer3.String())
	//})

	//// Test on commits from the same branch
	//t.Run("RunPipelineSameBranch", func(t *testing.T) {
	//	dataRepo := tu.UniqueString("TestRunPipeline_data")
	//	require.NoError(t, c.CreateRepo(dataRepo))

	//	branchA := "branchA"
	//	branchB := "branchB"

	//	pipeline := tu.UniqueString("sameBranch-pipeline")
	//	require.NoError(t, c.CreatePipeline(
	//		pipeline,
	//		"",
	//		[]string{"bash"},
	//		[]string{
	//			"cat /pfs/branch-a/file >> /pfs/out/file",
	//			"cat /pfs/branch-b/file >> /pfs/out/file",
	//			"echo ran-pipeline",
	//		},
	//		nil,
	//		client.NewCrossInput(
	//			client.NewPFSInputOpts("branch-a", dataRepo, branchA, "/*", "", "", false, false, nil),
	//			client.NewPFSInputOpts("branch-b", dataRepo, branchB, "/*", "", "", false, false, nil),
	//		),
	//		"",
	//		false,
	//	))
	//	commitA1, err := c.StartCommit(dataRepo, branchA)
	//	require.NoError(t, err)
	//	c.PutFile(dataRepo, commitA1.Branch.Name, commitA1.ID, "/file", strings.NewReader("data A1\n", client.WithAppendPutFile()))
	//	c.FinishCommit(dataRepo, commitA1.Branch.Name, commitA1.ID)

	//	commitA2, err := c.StartCommit(dataRepo, branchA)
	//	require.NoError(t, err)
	//	c.PutFile(dataRepo, commitA2.Branch.Name, commitA2.ID, "/file", strings.NewReader("data A2\n", client.WithAppendPutFile()))
	//	c.FinishCommit(dataRepo, commitA2.Branch.Name, commitA2.ID)

	//	_, err = c.FlushJob([]*pfs.Commit{commitA1, commitA2}, nil)
	//	require.NoError(t, err)

	//	// now run the pipeline with provenance from the same branch
	//	require.YesError(t, c.RunPipeline(pipeline, []*pfs.Commit{
	//		client.NewCommit(dataRepo, branchA, commitA1.ID),
	//		client.NewCommit(dataRepo, branchA, commitA2.ID),
	//	}, ""))
	//})
	//// Test on pipeline that should always fail
	//t.Run("RerunPipeline", func(t *testing.T) {
	//	dataRepo := tu.UniqueString("TestRerunPipeline_data")
	//	require.NoError(t, c.CreateRepo(dataRepo))

	//	// jobs on this pipeline should always fail
	//	pipeline := tu.UniqueString("rerun-pipeline")
	//	require.NoError(t, c.CreatePipeline(
	//		pipeline,
	//		"",
	//		[]string{"bash"},
	//		[]string{"false"},
	//		nil,
	//		client.NewPFSInputOpts("branch-a", dataRepo, "branchA", "/*", "", "", false, false, nil),
	//		"",
	//		false,
	//	))

	//	commitA1, err := c.StartCommit(dataRepo, "branchA")
	//	require.NoError(t, err)
	//	require.NoError(t, c.PutFile(dataRepo, commitA1.Branch.Name, commitA1.ID, "/file", strings.NewReader("data A1\n"), client.WithAppendPutFile()))
	//	require.NoError(t, c.FinishCommit(dataRepo, commitA1.Branch.Name, commitA1.ID))

	//	iter, err := c.FlushJob([]*pfs.Commit{commitA1}, nil)
	//	require.NoError(t, err)
	//	require.Equal(t, 2, len(commits))
	//	// now run the pipeline
	//	require.NoError(t, c.RunPipeline(pipeline, nil, ""))

	//	// running the pipeline should create a new job
	//	require.NoError(t, backoff.Retry(func() error {
	//		jobInfos, err := c.ListJob(pipeline, nil, nil, -1, true)
	//		require.NoError(t, err)
	//		if len(jobInfos) != 2 {
	//			return errors.Errorf("expected 2 jobs, got %d", len(jobInfos))
	//		}

	//		// but both of these jobs should fail
	//		for i, job := range jobInfos {
	//			if job.State.String() != "JOB_FAILURE" {
	//				return errors.Errorf("expected job %v to fail, but got %v", i, job.State.String())
	//			}
	//		}
	//		return nil
	//	}, backoff.NewTestingBackOff()))

	//	// Shouldn't error if you try to delete an already deleted pipeline
	//	require.NoError(t, c.DeletePipeline(pipeline, false))
	//	require.NoError(t, c.DeletePipeline(pipeline, false))
	//})
	//t.Run("RunPipelineStats", func(t *testing.T) {
	//	dataRepo := tu.UniqueString("TestRunPipeline_data")
	//	require.NoError(t, c.CreateRepo(dataRepo))

	//	branchA := "branchA"

	//	pipeline := tu.UniqueString("stats-pipeline")
	//	_, err := c.PpsAPIClient.CreatePipeline(
	//		context.Background(),
	//		&pps.CreatePipelineRequest{
	//			Pipeline: client.NewPipeline(pipeline),
	//			Transform: &pps.Transform{
	//				Cmd: []string{"bash"},
	//				Stdin: []string{
	//					"cat /pfs/branch-a/file >> /pfs/out/file",

	//					"echo ran-pipeline",
	//				},
	//			},
	//			Input:       client.NewPFSInputOpts("branch-a", dataRepo, branchA, "/*", "", "", false, false, nil),
	//		})
	//	require.NoError(t, err)

	//	commitA, err := c.StartCommit(dataRepo, branchA)
	//	require.NoError(t, err)
	//	c.PutFile(dataRepo, commitA.Branch.Name, commitA.ID, "/file", strings.NewReader("data A\n", client.WithAppendPutFile()))
	//	c.FinishCommit(dataRepo, commitA.Branch.Name, commitA.ID)

	//	// wait for the commit to finish before calling RunPipeline
	//	_, err = c.WaitCommitSetAll([]*pfs.Commit{client.NewCommit(dataRepo, commitA.ID)}, nil)
	//	require.NoError(t, err)

	//	// now run the pipeline
	//	require.NoError(t, backoff.Retry(func() error {
	//		return c.RunPipeline(pipeline, []*pfs.Commit{
	//			client.NewCommit(dataRepo, branchA, commitA.ID),
	//		}, "")
	//	}, backoff.NewTestingBackOff()))

	//	// make sure the pipeline didn't crash
	//	commitInfos, err := c.WaitCommitSetAll([]*pfs.Commit{client.NewCommit(dataRepo, commitA.ID)}, nil)
	//	require.NoError(t, err)

	//	// we'll know it crashed if this causes it to hang
	//	require.NoErrorWithinTRetry(t, 80*time.Second, func() error {
	//		return nil
	//	})
	//})
}

func TestPipelineFailure(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())

	dataRepo := tu.UniqueString("TestPipelineFailure_data")
	require.NoError(t, c.CreateRepo(dataRepo))

	commit, err := c.StartCommit(dataRepo, "master")
	require.NoError(t, err)
	require.NoError(t, c.PutFile(commit, "file", strings.NewReader("foo\n"), client.WithAppendPutFile()))
	require.NoError(t, c.FinishCommit(dataRepo, commit.Branch.Name, commit.ID))

	pipeline := tu.UniqueString("pipeline")
	require.NoError(t, c.CreatePipeline(
		pipeline,
		"",
		[]string{"exit 1"},
		nil,
		&pps.ParallelismSpec{
			Constant: 1,
		},
		client.NewPFSInput(dataRepo, "/*"),
		"",
		false,
	))
	var jobInfos []*pps.JobInfo
	require.NoError(t, backoff.Retry(func() error {
		jobInfos, err = c.ListJob(pipeline, nil, -1, true)
		require.NoError(t, err)
		if len(jobInfos) != 1 {
			return errors.Errorf("expected 1 jobs, got %d", len(jobInfos))
		}
		return nil
	}, backoff.NewTestingBackOff()))
	jobInfo, err := c.WaitJob(pipeline, jobInfos[0].Job.ID, false)
	require.NoError(t, err)
	require.Equal(t, pps.JobState_JOB_FAILURE, jobInfo.State)
	require.True(t, strings.Contains(jobInfo.Reason, "datum"))
}

func TestPipelineErrorHandling(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())

	t.Run("ErrCmd", func(t *testing.T) {

		dataRepo := tu.UniqueString("TestPipelineErrorHandling_data")
		require.NoError(t, c.CreateRepo(dataRepo))

		dataCommit := client.NewCommit(dataRepo, "master", "")
		require.NoError(t, c.PutFile(dataCommit, "file1", strings.NewReader("foo\n"), client.WithAppendPutFile()))
		require.NoError(t, c.PutFile(dataCommit, "file2", strings.NewReader("bar\n"), client.WithAppendPutFile()))
		require.NoError(t, c.PutFile(dataCommit, "file3", strings.NewReader("bar\n"), client.WithAppendPutFile()))

		// In this pipeline, we'll have a command that fails for files 2 and 3, and an error handler that fails for file 2
		pipeline := tu.UniqueString("pipeline1")
		_, err := c.PpsAPIClient.CreatePipeline(
			context.Background(),
			&pps.CreatePipelineRequest{
				Pipeline: client.NewPipeline(pipeline),
				Transform: &pps.Transform{
					Cmd:      []string{"bash"},
					Stdin:    []string{"if", fmt.Sprintf("[ -a pfs/%v/file1 ]", dataRepo), "then", "exit 0", "fi", "exit 1"},
					ErrCmd:   []string{"bash"},
					ErrStdin: []string{"if", fmt.Sprintf("[ -a pfs/%v/file3 ]", dataRepo), "then", "exit 0", "fi", "exit 1"},
				},
				Input: client.NewPFSInput(dataRepo, "/*"),
			})
		require.NoError(t, err)

		commitInfo, err := c.WaitCommit(pipeline, "master", "")
		require.NoError(t, err)
		jobInfo, err := c.InspectJob(pipeline, commitInfo.Commit.ID, false)
		require.NoError(t, err)

		// We expect the job to fail, and have 1 datum processed, recovered, and failed each
		require.Equal(t, pps.JobState_JOB_FAILURE, jobInfo.State)
		require.Equal(t, int64(1), jobInfo.DataProcessed)
		require.Equal(t, int64(1), jobInfo.DataRecovered)
		require.Equal(t, int64(1), jobInfo.DataFailed)

		// Now update this pipeline, we have the same command as before, but this time the error handling passes for all
		_, err = c.PpsAPIClient.CreatePipeline(
			context.Background(),
			&pps.CreatePipelineRequest{
				Pipeline: client.NewPipeline(pipeline),
				Transform: &pps.Transform{
					Cmd:    []string{"bash"},
					Stdin:  []string{"if", fmt.Sprintf("[ -a pfs/%v/file1 ]", dataRepo), "then", "exit 0", "fi", "exit 1"},
					ErrCmd: []string{"true"},
				},
				Input:  client.NewPFSInput(dataRepo, "/*"),
				Update: true,
			})
		require.NoError(t, err)

		commitInfo, err = c.WaitCommit(pipeline, "master", "")
		require.NoError(t, err)
		jobInfo, err = c.InspectJob(pipeline, commitInfo.Commit.ID, false)
		require.NoError(t, err)

		// so we expect the job to succeed, and to have recovered 2 datums
		require.Equal(t, pps.JobState_JOB_SUCCESS, jobInfo.State)
		require.Equal(t, int64(1), jobInfo.DataProcessed)
		require.Equal(t, int64(0), jobInfo.DataSkipped)
		require.Equal(t, int64(2), jobInfo.DataRecovered)
		require.Equal(t, int64(0), jobInfo.DataFailed)
	})
	t.Run("RecoveredDatums", func(t *testing.T) {
		dataRepo := tu.UniqueString("TestPipelineRecoveredDatums_data")
		require.NoError(t, c.CreateRepo(dataRepo))

		dataCommit := client.NewCommit(dataRepo, "master", "")
		require.NoError(t, c.PutFile(dataCommit, "foo", strings.NewReader("bar\n"), client.WithAppendPutFile()))

		// In this pipeline, we'll have a command that fails the datum, and then recovers it
		pipeline := tu.UniqueString("pipeline3")
		_, err := c.PpsAPIClient.CreatePipeline(
			context.Background(),
			&pps.CreatePipelineRequest{
				Pipeline: client.NewPipeline(pipeline),
				Transform: &pps.Transform{
					Cmd:      []string{"bash"},
					Stdin:    []string{"false"},
					ErrCmd:   []string{"bash"},
					ErrStdin: []string{"true"},
				},
				Input: client.NewPFSInput(dataRepo, "/*"),
			})
		require.NoError(t, err)

		commitInfo, err := c.WaitCommit(pipeline, "master", "")
		require.NoError(t, err)
		jobInfo, err := c.InspectJob(pipeline, commitInfo.Commit.ID, false)
		require.NoError(t, err)

		// We expect there to be one recovered datum
		require.Equal(t, pps.JobState_JOB_SUCCESS, jobInfo.State)
		require.Equal(t, int64(0), jobInfo.DataProcessed)
		require.Equal(t, int64(1), jobInfo.DataRecovered)
		require.Equal(t, int64(0), jobInfo.DataFailed)

		// Update the pipeline so that datums will now successfully be processed
		_, err = c.PpsAPIClient.CreatePipeline(
			context.Background(),
			&pps.CreatePipelineRequest{
				Pipeline: client.NewPipeline(pipeline),
				Transform: &pps.Transform{
					Cmd:   []string{"bash"},
					Stdin: []string{"true"},
				},
				Input:  client.NewPFSInput(dataRepo, "/*"),
				Update: true,
			})
		require.NoError(t, err)

		commitInfo, err = c.WaitCommit(pipeline, "master", "")
		require.NoError(t, err)
		jobInfo, err = c.InspectJob(pipeline, commitInfo.Commit.ID, false)
		require.NoError(t, err)

		// Now the recovered datum should have been processed
		require.Equal(t, pps.JobState_JOB_SUCCESS, jobInfo.State)
		require.Equal(t, int64(1), jobInfo.DataProcessed)
		require.Equal(t, int64(0), jobInfo.DataRecovered)
		require.Equal(t, int64(0), jobInfo.DataFailed)
	})
}

func TestLazyPipelinePropagation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}
	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())

	dataRepo := tu.UniqueString("TestLazyPipelinePropagation_data")
	require.NoError(t, c.CreateRepo(dataRepo))

	pipelineA := tu.UniqueString("pipeline-A")
	require.NoError(t, c.CreatePipeline(
		pipelineA,
		"",
		[]string{"cp", path.Join("/pfs", dataRepo, "file"), "/pfs/out/file"},
		nil,
		&pps.ParallelismSpec{
			Constant: 1,
		},
		client.NewPFSInputOpts("", dataRepo, "", "/*", "", "", false, true, nil),
		"",
		false,
	))
	pipelineB := tu.UniqueString("pipeline-B")
	require.NoError(t, c.CreatePipeline(
		pipelineB,
		"",
		[]string{"cp", path.Join("/pfs", pipelineA, "file"), "/pfs/out/file"},
		nil,
		&pps.ParallelismSpec{
			Constant: 1,
		},
		client.NewPFSInputOpts("", pipelineA, "", "/*", "", "", false, true, nil),
		"",
		false,
	))

	commit1, err := c.StartCommit(dataRepo, "master")
	require.NoError(t, err)
	require.NoError(t, c.PutFile(commit1, "file", strings.NewReader("foo\n"), client.WithAppendPutFile()))
	require.NoError(t, c.FinishCommit(dataRepo, commit1.Branch.Name, commit1.ID))

	_, err = c.WaitCommitSetAll(commit1.ID)
	require.NoError(t, err)

	jobInfos, err := c.ListJob(pipelineA, nil, -1, true)
	require.NoError(t, err)
	require.Equal(t, 2, len(jobInfos))
	require.NotNil(t, jobInfos[0].Details.Input.Pfs)
	require.Equal(t, true, jobInfos[0].Details.Input.Pfs.Lazy)

	jobInfos, err = c.ListJob(pipelineB, nil, -1, true)
	require.NoError(t, err)
	require.Equal(t, 2, len(jobInfos))
	require.NotNil(t, jobInfos[0].Details.Input.Pfs)
	require.Equal(t, true, jobInfos[0].Details.Input.Pfs.Lazy)
}

func TestLazyPipeline(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())
	// create repos
	dataRepo := tu.UniqueString("TestLazyPipeline_data")
	require.NoError(t, c.CreateRepo(dataRepo))
	dataCommit := client.NewCommit(dataRepo, "master", "")

	// create pipeline
	pipelineName := tu.UniqueString("pipeline")
	_, err := c.PpsAPIClient.CreatePipeline(
		context.Background(),
		&pps.CreatePipelineRequest{
			Pipeline: client.NewPipeline(pipelineName),
			Transform: &pps.Transform{
				Cmd: []string{"cp", path.Join("/pfs", dataRepo, "file"), "/pfs/out/file"},
			},
			ParallelismSpec: &pps.ParallelismSpec{
				Constant: 1,
			},
			Input: &pps.Input{
				Pfs: &pps.PFSInput{
					Repo: dataRepo,
					Glob: "/",
					Lazy: true,
				},
			},
		})
	require.NoError(t, err)

	// Do a commit
	commit, err := c.StartCommit(dataRepo, "master")
	require.NoError(t, err)
	require.NoError(t, c.PutFile(dataCommit, "file", strings.NewReader("foo\n"), client.WithAppendPutFile()))
	// We put 2 files, 1 of which will never be touched by the pipeline code.
	// This is an important part of the correctness of this test because the
	// job-shim sets up a goro for each pipe, pipes that are never opened will
	// leak but that shouldn't prevent the job from completing.
	require.NoError(t, c.PutFile(dataCommit, "file2", strings.NewReader("foo\n"), client.WithAppendPutFile()))
	require.NoError(t, c.FinishCommit(dataRepo, "master", ""))

	commitInfos, err := c.WaitCommitSetAll(commit.ID)
	require.NoError(t, err)
	require.Equal(t, 4, len(commitInfos))

	buffer := bytes.Buffer{}
	outputCommit := client.NewCommit(pipelineName, "master", commit.ID)
	require.NoError(t, c.GetFile(outputCommit, "file", &buffer))
	require.Equal(t, "foo\n", buffer.String())
}

func TestEmptyFiles(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())
	// create repos
	dataRepo := tu.UniqueString("TestShufflePipeline_data")
	require.NoError(t, c.CreateRepo(dataRepo))
	dataCommit := client.NewCommit(dataRepo, "master", "")

	// create pipeline
	pipelineName := tu.UniqueString("pipeline")
	_, err := c.PpsAPIClient.CreatePipeline(
		context.Background(),
		&pps.CreatePipelineRequest{
			Pipeline: client.NewPipeline(pipelineName),
			Transform: &pps.Transform{
				Cmd: []string{"bash"},
				Stdin: []string{
					fmt.Sprintf("if [ -s /pfs/%s/file ]; then exit 1; fi", dataRepo),
					fmt.Sprintf("ln -s /pfs/%s/file /pfs/out/file", dataRepo),
				},
			},
			ParallelismSpec: &pps.ParallelismSpec{
				Constant: 1,
			},
			Input: &pps.Input{
				Pfs: &pps.PFSInput{
					Repo:       dataRepo,
					Glob:       "/*",
					EmptyFiles: true,
				},
			},
		})
	require.NoError(t, err)

	// Do a commit
	commit, err := c.StartCommit(dataRepo, "master")
	require.NoError(t, err)
	require.NoError(t, c.PutFile(dataCommit, "file", strings.NewReader("foo\n"), client.WithAppendPutFile()))
	require.NoError(t, c.FinishCommit(dataRepo, "master", ""))

	commitInfos, err := c.WaitCommitSetAll(commit.ID)
	require.NoError(t, err)
	require.Equal(t, 4, len(commitInfos))

	buffer := bytes.Buffer{}
	outputCommit := client.NewCommit(pipelineName, "master", commit.ID)
	require.NoError(t, c.GetFile(outputCommit, "file", &buffer))
	require.Equal(t, "foo\n", buffer.String())
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

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())
	aRepo := tu.UniqueString("A")
	require.NoError(t, c.CreateRepo(aRepo))

	bPipeline := tu.UniqueString("B")
	require.NoError(t, c.CreatePipeline(
		bPipeline,
		"",
		[]string{"cp", path.Join("/pfs", aRepo, "file"), "/pfs/out/file"},
		nil,
		&pps.ParallelismSpec{
			Constant: 1,
		},
		client.NewPFSInput(aRepo, "/*"),
		"",
		false,
	))

	cPipeline := tu.UniqueString("C")
	require.NoError(t, c.CreatePipeline(
		cPipeline,
		"",
		[]string{"sh"},
		[]string{fmt.Sprintf("diff %s %s >/pfs/out/file",
			path.Join("/pfs", aRepo, "file"), path.Join("/pfs", bPipeline, "file"))},
		&pps.ParallelismSpec{
			Constant: 1,
		},
		client.NewCrossInput(
			client.NewPFSInput(aRepo, "/*"),
			client.NewPFSInput(bPipeline, "/*"),
		),
		"",
		false,
	))

	// commit to aRepo
	commit1, err := c.StartCommit(aRepo, "master")
	require.NoError(t, err)
	require.NoError(t, c.PutFile(commit1, "file", strings.NewReader("foo\n"), client.WithAppendPutFile()))
	require.NoError(t, c.FinishCommit(aRepo, commit1.Branch.Name, commit1.ID))

	commit2, err := c.StartCommit(aRepo, "master")
	require.NoError(t, err)
	require.NoError(t, c.PutFile(commit2, "file", strings.NewReader("bar\n"), client.WithAppendPutFile()))
	require.NoError(t, c.FinishCommit(aRepo, commit2.Branch.Name, commit2.ID))

	commitInfos, err := c.WaitCommitSetAll(commit2.ID)
	require.NoError(t, err)
	require.Equal(t, 7, len(commitInfos)) // input repo plus spec/output/meta for b and c pipelines

	for _, ci := range commitInfos {
		if ci.Commit.Branch.Repo.Name == cPipeline && ci.Commit.Branch.Repo.Type == pfs.UserRepoType {
			require.Equal(t, uint64(0), ci.Details.SizeBytes)
		}
	}

	// We should only see four commits in aRepo (bpipeline created, cpipeline created, commit1, commit2)
	commitInfos, err = c.ListCommit(client.NewRepo(aRepo), client.NewCommit(aRepo, "master", ""), nil, 0)
	require.NoError(t, err)
	require.Equal(t, 4, len(commitInfos))

	// There are four commits in the bPipeline repo (same as for aRepo)
	commitInfos, err = c.ListCommit(client.NewRepo(bPipeline), client.NewCommit(bPipeline, "master", ""), nil, 0)
	require.NoError(t, err)
	require.Equal(t, 4, len(commitInfos))

	// There are three commits in the cPipeline repo (one fewer because it was added after bPipeline creation)
	commitInfos, err = c.ListCommit(client.NewRepo(cPipeline), client.NewCommit(cPipeline, "master", ""), nil, 0)
	require.NoError(t, err)
	require.Equal(t, 3, len(commitInfos))
}

// TestProvenance2 tests the following DAG:
//   A
//  / \
// B   C
//  \ /
//   D
func TestProvenance2(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())
	aRepo := tu.UniqueString("A")
	require.NoError(t, c.CreateRepo(aRepo))

	bPipeline := tu.UniqueString("B")
	require.NoError(t, c.CreatePipeline(
		bPipeline,
		"",
		[]string{"cp", path.Join("/pfs", aRepo, "bfile"), "/pfs/out/bfile"},
		nil,
		&pps.ParallelismSpec{
			Constant: 1,
		},
		client.NewPFSInput(aRepo, "/b*"),
		"",
		false,
	))

	cPipeline := tu.UniqueString("C")
	require.NoError(t, c.CreatePipeline(
		cPipeline,
		"",
		[]string{"cp", path.Join("/pfs", aRepo, "cfile"), "/pfs/out/cfile"},
		nil,
		&pps.ParallelismSpec{
			Constant: 1,
		},
		client.NewPFSInput(aRepo, "/c*"),
		"",
		false,
	))

	dPipeline := tu.UniqueString("D")
	require.NoError(t, c.CreatePipeline(
		dPipeline,
		"",
		[]string{"sh"},
		[]string{
			fmt.Sprintf("diff /pfs/%s/bfile /pfs/%s/cfile >/pfs/out/file", bPipeline, cPipeline),
		},
		&pps.ParallelismSpec{
			Constant: 1,
		},
		client.NewCrossInput(
			client.NewPFSInput(bPipeline, "/*"),
			client.NewPFSInput(cPipeline, "/*"),
		),
		"",
		false,
	))

	// commit to aRepo
	commit1, err := c.StartCommit(aRepo, "master")
	require.NoError(t, err)
	require.NoError(t, c.PutFile(commit1, "bfile", strings.NewReader("foo\n"), client.WithAppendPutFile()))
	require.NoError(t, c.PutFile(commit1, "cfile", strings.NewReader("foo\n"), client.WithAppendPutFile()))
	require.NoError(t, c.FinishCommit(aRepo, commit1.Branch.Name, commit1.ID))

	commit2, err := c.StartCommit(aRepo, "master")
	require.NoError(t, err)
	require.NoError(t, c.PutFile(commit2, "bfile", strings.NewReader("bar\n"), client.WithAppendPutFile()))
	require.NoError(t, c.PutFile(commit2, "cfile", strings.NewReader("bar\n"), client.WithAppendPutFile()))
	require.NoError(t, c.FinishCommit(aRepo, commit2.Branch.Name, commit2.ID))

	_, err = c.WaitCommit(dPipeline, "", commit2.ID)
	require.NoError(t, err)

	// We should see 5 commits in aRepo (one per pipeline creation and the two user commits)
	commitInfos, err := c.ListCommit(client.NewRepo(aRepo), client.NewCommit(aRepo, "master", ""), nil, 0)
	require.NoError(t, err)
	require.Equal(t, 5, len(commitInfos))

	// We should see 4 commits in bPipeline (b and d pipeline creation and from the two user commits)
	commitInfos, err = c.ListCommit(client.NewRepo(bPipeline), client.NewCommit(bPipeline, "master", ""), nil, 0)
	require.NoError(t, err)
	require.Equal(t, 4, len(commitInfos))

	// We should see 4 commits in cPipeline (c and d pipeline creation and from the two user commits)
	commitInfos, err = c.ListCommit(client.NewRepo(cPipeline), client.NewCommit(cPipeline, "master", ""), nil, 0)
	require.NoError(t, err)
	require.Equal(t, 4, len(commitInfos))

	// We should see 3 commits in dPipeline (d pipeline creation and from the two user commits)
	commitInfos, err = c.ListCommit(client.NewRepo(dPipeline), client.NewCommit(dPipeline, "master", ""), nil, 0)
	require.NoError(t, err)
	require.Equal(t, 3, len(commitInfos))

	buffer := bytes.Buffer{}
	outputCommit := client.NewCommit(dPipeline, "master", commit1.ID)
	require.NoError(t, c.GetFile(outputCommit, "file", &buffer))
	require.Equal(t, "", buffer.String())

	buffer.Reset()
	outputCommit = client.NewCommit(dPipeline, "master", commit2.ID)
	require.NoError(t, c.GetFile(outputCommit, "file", &buffer))
	require.Equal(t, "", buffer.String())
}

// TestStopPipelineExtraCommit generates the following DAG:
// A -> B -> C
// and ensures that calling StopPipeline on B does not create an commit in C.
func TestStopPipelineExtraCommit(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())
	aRepo := tu.UniqueString("A")
	require.NoError(t, c.CreateRepo(aRepo))

	bPipeline := tu.UniqueString("B")
	require.NoError(t, c.CreatePipeline(
		bPipeline,
		"",
		[]string{"cp", path.Join("/pfs", aRepo, "file"), "/pfs/out/file"},
		nil,
		&pps.ParallelismSpec{
			Constant: 1,
		},
		client.NewPFSInput(aRepo, "/*"),
		"",
		false,
	))

	cPipeline := tu.UniqueString("C")
	require.NoError(t, c.CreatePipeline(
		cPipeline,
		"",
		[]string{"cp", path.Join("/pfs", aRepo, "file"), "/pfs/out/file"},
		nil,
		&pps.ParallelismSpec{
			Constant: 1,
		},
		client.NewPFSInput(bPipeline, "/*"),
		"",
		false,
	))

	// commit to aRepo
	commit1, err := c.StartCommit(aRepo, "master")
	require.NoError(t, err)
	require.NoError(t, c.PutFile(commit1, "file", strings.NewReader("foo\n"), client.WithAppendPutFile()))
	require.NoError(t, c.FinishCommit(aRepo, commit1.Branch.Name, commit1.ID))

	commitInfos, err := c.WaitCommitSetAll(commit1.ID)
	require.NoError(t, err)
	require.Equal(t, 7, len(commitInfos))

	// We should see 3 commits in aRepo (b and c pipeline creation and the user commit)
	commitInfos, err = c.ListCommit(client.NewRepo(aRepo), client.NewCommit(aRepo, "master", ""), nil, 0)
	require.NoError(t, err)
	require.Equal(t, 3, len(commitInfos))

	// We should see 3 commits in bPipeline (same as in aRepo)
	commitInfos, err = c.ListCommit(client.NewRepo(bPipeline), client.NewCommit(bPipeline, "master", ""), nil, 0)
	require.NoError(t, err)
	require.Equal(t, 3, len(commitInfos))

	// We should see 2 commits in cPipeline (c pipeline creation and from the user commit)
	commitInfos, err = c.ListCommit(client.NewRepo(cPipeline), client.NewCommit(cPipeline, "master", ""), nil, 0)
	require.NoError(t, err)
	require.Equal(t, 2, len(commitInfos))

	require.NoError(t, c.StopPipeline(bPipeline))
	commitInfos, err = c.ListCommit(client.NewRepo(cPipeline), client.NewCommit(cPipeline, "master", ""), nil, 0)
	require.NoError(t, err)
	require.Equal(t, 2, len(commitInfos))
}

func TestWaitJobSet(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())
	prefix := tu.UniqueString("repo")
	makeRepoName := func(i int) string {
		return fmt.Sprintf("%s-%d", prefix, i)
	}

	sourceRepo := makeRepoName(0)
	require.NoError(t, c.CreateRepo(sourceRepo))

	// Create a four-stage pipeline
	numStages := 4
	for i := 0; i < numStages; i++ {
		repo := makeRepoName(i)
		require.NoError(t, c.CreatePipeline(
			makeRepoName(i+1),
			"",
			[]string{"cp", path.Join("/pfs", repo, "file"), "/pfs/out/file"},
			nil,
			&pps.ParallelismSpec{
				Constant: 1,
			},
			client.NewPFSInput(repo, "/*"),
			"",
			false,
		))
	}

	for i := 0; i < 5; i++ {
		commit, err := c.StartCommit(sourceRepo, "master")
		require.NoError(t, err)
		require.NoError(t, c.PutFile(commit, "file", strings.NewReader("foo\n"), client.WithAppendPutFile()))
		require.NoError(t, c.FinishCommit(sourceRepo, commit.Branch.Name, commit.ID))

		commitInfos, err := c.WaitCommitSetAll(commit.ID)
		require.NoError(t, err)
		require.Equal(t, numStages*3+1, len(commitInfos))

		jobInfos, err := c.WaitJobSetAll(commit.ID, false)
		require.NoError(t, err)
		require.Equal(t, numStages, len(jobInfos))
	}
}

func TestWaitJobSetFailures(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())
	dataRepo := tu.UniqueString("TestWaitJobSetFailures")
	require.NoError(t, c.CreateRepo(dataRepo))
	prefix := tu.UniqueString("TestWaitJobSetFailures")
	pipelineName := func(i int) string { return prefix + fmt.Sprintf("%d", i) }

	require.NoError(t, c.CreatePipeline(
		pipelineName(0),
		"",
		[]string{"sh"},
		[]string{fmt.Sprintf("cp /pfs/%s/* /pfs/out/", dataRepo)},
		&pps.ParallelismSpec{
			Constant: 1,
		},
		client.NewPFSInput(dataRepo, "/*"),
		"",
		false,
	))
	require.NoError(t, c.CreatePipeline(
		pipelineName(1),
		"",
		[]string{"sh"},
		[]string{
			fmt.Sprintf("if [ -f /pfs/%s/file1 ]; then exit 1; fi", pipelineName(0)),
			fmt.Sprintf("cp /pfs/%s/* /pfs/out/", pipelineName(0)),
		},
		&pps.ParallelismSpec{
			Constant: 1,
		},
		client.NewPFSInput(pipelineName(0), "/*"),
		"",
		false,
	))
	require.NoError(t, c.CreatePipeline(
		pipelineName(2),
		"",
		[]string{"sh"},
		[]string{fmt.Sprintf("cp /pfs/%s/* /pfs/out/", pipelineName(1))},
		&pps.ParallelismSpec{
			Constant: 1,
		},
		client.NewPFSInput(pipelineName(1), "/*"),
		"",
		false,
	))

	for i := 0; i < 2; i++ {
		commit, err := c.StartCommit(dataRepo, "master")
		require.NoError(t, err)
		require.NoError(t, c.PutFile(commit, fmt.Sprintf("file%d", i), strings.NewReader("foo\n"), client.WithAppendPutFile()))
		require.NoError(t, c.FinishCommit(dataRepo, commit.Branch.Name, commit.ID))
		jobInfos, err := c.WaitJobSetAll(commit.ID, false)
		require.NoError(t, err)
		require.Equal(t, 3, len(jobInfos))
		if i == 0 {
			for _, ji := range jobInfos {
				require.Equal(t, pps.JobState_JOB_SUCCESS.String(), ji.State.String())
			}
		} else {
			for _, ji := range jobInfos {
				if ji.Job.Pipeline.Name != pipelineName(0) {
					require.Equal(t, pps.JobState_JOB_FAILURE.String(), ji.State.String())
				}
			}
		}
	}
}

func TestWaitCommitSetAfterCreatePipeline(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())
	repo := tu.UniqueString("data")
	require.NoError(t, c.CreateRepo(repo))

	var commit *pfs.Commit
	var err error
	for i := 0; i < 10; i++ {
		commit, err = c.StartCommit(repo, "dev")
		require.NoError(t, err)
		require.NoError(t, c.PutFile(commit, "file", strings.NewReader(fmt.Sprintf("foo%d\n", i)), client.WithAppendPutFile()))
		require.NoError(t, c.FinishCommit(repo, commit.Branch.Name, commit.ID))
	}
	require.NoError(t, c.CreateBranch(repo, "master", commit.Branch.Name, commit.ID, nil))

	pipeline := tu.UniqueString("pipeline")
	require.NoError(t, c.CreatePipeline(
		pipeline,
		"",
		[]string{"cp", path.Join("/pfs", repo, "file"), "/pfs/out/file"},
		nil,
		&pps.ParallelismSpec{
			Constant: 1,
		},
		client.NewPFSInput(repo, "/*"),
		"",
		false,
	))
	commitInfo, err := c.InspectCommit(repo, "master", "")
	require.NoError(t, err)
	_, err = c.WaitCommitSetAll(commitInfo.Commit.ID)
	require.NoError(t, err)
}

// TestRecreatePipeline tracks #432
func TestRecreatePipeline(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())
	repo := tu.UniqueString("data")
	require.NoError(t, c.CreateRepo(repo))
	commit, err := c.StartCommit(repo, "master")
	require.NoError(t, err)
	require.NoError(t, c.PutFile(commit, "file", strings.NewReader("foo"), client.WithAppendPutFile()))
	require.NoError(t, c.FinishCommit(repo, commit.Branch.Name, commit.ID))
	pipeline := tu.UniqueString("pipeline")
	createPipeline := func() {
		require.NoError(t, c.CreatePipeline(
			pipeline,
			"",
			[]string{"cp", path.Join("/pfs", repo, "file"), "/pfs/out/file"},
			nil,
			&pps.ParallelismSpec{
				Constant: 1,
			},
			client.NewPFSInput(repo, "/*"),
			"",
			false,
		))
		_, err := c.WaitCommitSetAll(commit.ID)
		require.NoError(t, err)
	}

	// Do it twice.  We expect jobs to be created on both runs.
	createPipeline()
	time.Sleep(5 * time.Second)
	require.NoError(t, c.DeletePipeline(pipeline, false))
	time.Sleep(5 * time.Second)
	createPipeline()
}

func TestDeletePipeline(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())

	repo := tu.UniqueString("data")
	require.NoError(t, c.CreateRepo(repo))
	commit, err := c.StartCommit(repo, "master")
	require.NoError(t, err)
	require.NoError(t, c.PutFile(commit, uuid.NewWithoutDashes(), strings.NewReader("foo"), client.WithAppendPutFile()))
	require.NoError(t, c.FinishCommit(repo, commit.Branch.Name, commit.ID))
	pipelines := []string{tu.UniqueString("TestDeletePipeline1"), tu.UniqueString("TestDeletePipeline2")}
	createPipelines := func() {
		require.NoError(t, c.CreatePipeline(
			pipelines[0],
			"",
			[]string{"sleep", "20"},
			nil,
			&pps.ParallelismSpec{
				Constant: 1,
			},
			client.NewPFSInput(repo, "/*"),
			"",
			false,
		))
		require.NoError(t, c.CreatePipeline(
			pipelines[1],
			"",
			[]string{"sleep", "20"},
			nil,
			&pps.ParallelismSpec{
				Constant: 1,
			},
			client.NewPFSInput(pipelines[0], "/*"),
			"",
			false,
		))
		time.Sleep(10 * time.Second)
		// Wait for the pipeline to start running
		require.NoErrorWithinTRetry(t, 90*time.Second, func() error {
			pipelineInfos, err := c.ListPipeline(false)
			if err != nil {
				return err
			}
			// Check number of pipelines
			names := make([]string, 0, len(pipelineInfos))
			for _, pi := range pipelineInfos {
				names = append(names, fmt.Sprintf("(%s, %s)", pi.Pipeline.Name, pi.State))
			}
			if len(pipelineInfos) != 2 {
				return errors.Errorf("Expected two pipelines, but got: %+v", names)
			}
			// make sure second pipeline is running
			pipelineInfo, err := c.InspectPipeline(pipelines[1], false)
			if err != nil {
				return err
			}
			if pipelineInfo.State != pps.PipelineState_PIPELINE_RUNNING {
				return errors.Errorf("no running pipeline (only %+v)", names)
			}
			return nil
		})
	}

	createPipelines()

	deletePipeline := func(pipeline string) {
		require.NoError(t, c.DeletePipeline(pipeline, false))
		time.Sleep(5 * time.Second)
		// Wait for the pipeline to disappear
		require.NoError(t, backoff.Retry(func() error {
			_, err := c.InspectPipeline(pipeline, false)
			if err == nil {
				return errors.Errorf("expected pipeline to be missing, but it's still present")
			}
			return nil
		}, backoff.NewTestingBackOff()))

	}
	// Can't delete a pipeline from the middle of the dag
	require.YesError(t, c.DeletePipeline(pipelines[0], false))

	deletePipeline(pipelines[1])
	deletePipeline(pipelines[0])

	// The jobs should be gone
	jobs, err := c.ListJob("", nil, -1, true)
	require.NoError(t, err)
	require.Equal(t, len(jobs), 0)

	// Listing jobs for a deleted pipeline should error
	_, err = c.ListJob(pipelines[0], nil, -1, true)
	require.YesError(t, err)

	createPipelines()

	// Can force delete pipelines from the middle of the dag.
	require.NoError(t, c.DeletePipeline(pipelines[0], true))
}

func TestPipelineState(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())
	repo := tu.UniqueString("data")
	require.NoError(t, c.CreateRepo(repo))
	pipeline := tu.UniqueString("pipeline")
	require.NoError(t, c.CreatePipeline(
		pipeline,
		"",
		[]string{"cp", path.Join("/pfs", repo, "file"), "/pfs/out/file"},
		nil,
		&pps.ParallelismSpec{
			Constant: 1,
		},
		client.NewPFSInput(repo, "/*"),
		"",
		false,
	))

	// Wait for pipeline to get picked up
	time.Sleep(15 * time.Second)
	require.NoError(t, backoff.Retry(func() error {
		pipelineInfo, err := c.InspectPipeline(pipeline, false)
		if err != nil {
			return err
		}
		if pipelineInfo.State != pps.PipelineState_PIPELINE_RUNNING {
			return errors.Errorf("pipeline should be in state running, not: %s", pipelineInfo.State.String())
		}
		return nil
	}, backoff.NewTestingBackOff()))

	// Stop pipeline and wait for the pipeline to pause
	require.NoError(t, c.StopPipeline(pipeline))
	time.Sleep(5 * time.Second)
	require.NoError(t, backoff.Retry(func() error {
		pipelineInfo, err := c.InspectPipeline(pipeline, false)
		if err != nil {
			return err
		}
		if !pipelineInfo.Stopped {
			return errors.Errorf("pipeline never paused, even though StopPipeline() was called, state: %s", pipelineInfo.State.String())
		}
		return nil
	}, backoff.NewTestingBackOff()))

	// Restart pipeline and wait for the pipeline to resume
	require.NoError(t, c.StartPipeline(pipeline))
	time.Sleep(15 * time.Second)
	require.NoError(t, backoff.Retry(func() error {
		pipelineInfo, err := c.InspectPipeline(pipeline, false)
		if err != nil {
			return err
		}
		if pipelineInfo.State != pps.PipelineState_PIPELINE_RUNNING {
			return errors.Errorf("pipeline never restarted, even though StartPipeline() was called, state: %s", pipelineInfo.State.String())
		}
		return nil
	}, backoff.NewTestingBackOff()))
}

func TestJobCounts(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())
	repo := tu.UniqueString("data")
	require.NoError(t, c.CreateRepo(repo))
	pipeline := tu.UniqueString("pipeline")
	require.NoError(t, c.CreatePipeline(
		pipeline,
		"",
		[]string{"cp", path.Join("/pfs", repo, "file"), "/pfs/out/file"},
		nil,
		&pps.ParallelismSpec{
			Constant: 1,
		},
		client.NewPFSInput(repo, "/*"),
		"",
		false,
	))

	// Trigger a job by creating a commit
	commit, err := c.StartCommit(repo, "master")
	require.NoError(t, err)
	require.NoError(t, c.PutFile(commit, "file", strings.NewReader("foo"), client.WithAppendPutFile()))
	require.NoError(t, c.FinishCommit(repo, commit.Branch.Name, commit.ID))
	_, err = c.WaitCommitSetAll(commit.ID)
	require.NoError(t, err)
	jobInfos, err := c.ListJob(pipeline, nil, -1, true)
	require.NoError(t, err)
	require.Equal(t, 2, len(jobInfos))
	require.Equal(t, commit.ID, jobInfos[0].Job.ID)
	ctx, cancel := context.WithTimeout(c.Ctx(), time.Second*30)
	defer cancel() //cleanup resources
	_, err = c.WithCtx(ctx).WaitJob(pipeline, jobInfos[0].Job.ID, false)
	require.NoError(t, err)

	// check that the job has been accounted for
	pipelineInfo, err := c.InspectPipeline(pipeline, false)
	require.NoError(t, err)
	require.Equal(t, int32(2), pipelineInfo.JobCounts[int32(pps.JobState_JOB_SUCCESS)])
}

// TestUpdatePipelineThatHasNoOutput tracks #1637
func TestUpdatePipelineThatHasNoOutput(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())

	dataRepo := tu.UniqueString("TestUpdatePipelineThatHasNoOutput")
	require.NoError(t, c.CreateRepo(dataRepo))

	commit, err := c.StartCommit(dataRepo, "master")
	require.NoError(t, err)
	require.NoError(t, c.PutFile(commit, "file", strings.NewReader("foo\n"), client.WithAppendPutFile()))
	require.NoError(t, c.FinishCommit(dataRepo, commit.Branch.Name, commit.ID))

	pipeline := tu.UniqueString("pipeline")
	require.NoError(t, c.CreatePipeline(
		pipeline,
		"",
		[]string{"sh"},
		[]string{"exit 1"},
		nil,
		client.NewPFSInput(dataRepo, "/"),
		"",
		false,
	))

	// Wait for job to spawn
	var jobInfos []*pps.JobInfo
	time.Sleep(10 * time.Second)
	require.NoError(t, backoff.Retry(func() error {
		var err error
		jobInfos, err = c.ListJob(pipeline, nil, -1, true)
		if err != nil {
			return err
		}
		if len(jobInfos) < 1 {
			return errors.Errorf("job not spawned")
		}
		return nil
	}, backoff.NewTestingBackOff()))

	jobInfo, err := c.WaitJob(pipeline, jobInfos[0].Job.ID, false)
	require.NoError(t, err)
	require.Equal(t, pps.JobState_JOB_FAILURE, jobInfo.State)

	// Now we update the pipeline
	require.NoError(t, c.CreatePipeline(
		pipeline,
		"",
		[]string{"sh"},
		[]string{"exit 1"},
		nil,
		client.NewPFSInput(dataRepo, "/"),
		"",
		true,
	))
}

func TestAcceptReturnCode(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())

	dataRepo := tu.UniqueString("TestAcceptReturnCode")
	require.NoError(t, c.CreateRepo(dataRepo))

	pipelineName := tu.UniqueString("pipeline")
	_, err := c.PpsAPIClient.CreatePipeline(
		context.Background(),
		&pps.CreatePipelineRequest{
			Pipeline: client.NewPipeline(pipelineName),
			Transform: &pps.Transform{
				Cmd:              []string{"sh"},
				Stdin:            []string{"exit 1"},
				AcceptReturnCode: []int64{1},
			},
			Input: client.NewPFSInput(dataRepo, "/*"),
		},
	)
	require.NoError(t, err)

	commit, err := c.StartCommit(dataRepo, "master")
	require.NoError(t, err)
	require.NoError(t, c.PutFile(commit, "file", strings.NewReader("foo\n"), client.WithAppendPutFile()))
	require.NoError(t, c.FinishCommit(dataRepo, commit.Branch.Name, commit.ID))

	commitInfos, err := c.WaitCommitSetAll(commit.ID)
	require.NoError(t, err)
	require.Equal(t, 4, len(commitInfos))

	jobInfos, err := c.ListJob(pipelineName, nil, -1, true)
	require.NoError(t, err)
	require.Equal(t, 2, len(jobInfos))
	require.Equal(t, commit.ID, jobInfos[0].Job.ID)

	jobInfo, err := c.WaitJob(pipelineName, jobInfos[0].Job.ID, false)
	require.NoError(t, err)
	require.Equal(t, pps.JobState_JOB_SUCCESS, jobInfo.State)
}

func TestPrettyPrinting(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())

	// create repos
	dataRepo := tu.UniqueString("TestPrettyPrinting_data")
	require.NoError(t, c.CreateRepo(dataRepo))

	// create pipeline
	pipelineName := tu.UniqueString("pipeline")
	_, err := c.PpsAPIClient.CreatePipeline(
		context.Background(),
		&pps.CreatePipelineRequest{
			Pipeline: client.NewPipeline(pipelineName),
			Transform: &pps.Transform{
				Cmd: []string{"cp", path.Join("/pfs", dataRepo, "file"), "/pfs/out/file"},
			},
			ParallelismSpec: &pps.ParallelismSpec{
				Constant: 1,
			},
			Input: client.NewPFSInput(dataRepo, "/*"),
		})
	require.NoError(t, err)

	// Do a commit to repo
	commit, err := c.StartCommit(dataRepo, "master")
	require.NoError(t, err)
	require.NoError(t, c.PutFile(commit, "file", strings.NewReader("foo\n"), client.WithAppendPutFile()))
	require.NoError(t, c.FinishCommit(dataRepo, commit.Branch.Name, commit.ID))

	commitInfos, err := c.WaitCommitSetAll(commit.ID)
	require.NoError(t, err)
	require.Equal(t, 4, len(commitInfos))

	repoInfo, err := c.InspectRepo(dataRepo)
	require.NoError(t, err)
	require.NoError(t, pfspretty.PrintDetailedRepoInfo(pfspretty.NewPrintableRepoInfo(repoInfo)))
	for _, c := range commitInfos {
		require.NoError(t, pfspretty.PrintDetailedCommitInfo(os.Stdout, pfspretty.NewPrintableCommitInfo(c)))
	}

	fileInfo, err := c.InspectFile(commit, "file")
	require.NoError(t, err)
	require.NoError(t, pfspretty.PrintDetailedFileInfo(fileInfo))
	pipelineInfo, err := c.InspectPipeline(pipelineName, true)
	require.NoError(t, err)
	require.NoError(t, ppspretty.PrintDetailedPipelineInfo(os.Stdout, ppspretty.NewPrintablePipelineInfo(pipelineInfo)))
	jobInfos, err := c.ListJob("", nil, -1, true)
	require.NoError(t, err)
	require.True(t, len(jobInfos) > 0)
	require.NoError(t, ppspretty.PrintDetailedJobInfo(os.Stdout, ppspretty.NewPrintableJobInfo(jobInfos[0])))
}

func TestDeleteAll(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}
	// this test cannot be run in parallel because it deletes everything
	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())
	// create repos
	dataRepo := tu.UniqueString("TestDeleteAll_data")
	require.NoError(t, c.CreateRepo(dataRepo))
	// create pipeline
	pipelineName := tu.UniqueString("pipeline")
	require.NoError(t, c.CreatePipeline(
		pipelineName,
		"",
		[]string{"cp", path.Join("/pfs", dataRepo, "file"), "/pfs/out/file"},
		nil,
		&pps.ParallelismSpec{
			Constant: 1,
		},
		client.NewPFSInput(dataRepo, "/"),
		"",
		false,
	))
	// Do commit to repo
	commit, err := c.StartCommit(dataRepo, "master")
	require.NoError(t, err)
	require.NoError(t, c.PutFile(commit, "file", strings.NewReader("foo\n"), client.WithAppendPutFile()))
	require.NoError(t, c.FinishCommit(dataRepo, commit.Branch.Name, commit.ID))
	_, err = c.WaitCommitSetAll(commit.ID)
	require.NoError(t, err)
	require.NoError(t, c.DeleteAll())
	repoInfos, err := c.ListRepo()
	require.NoError(t, err)
	require.Equal(t, 0, len(repoInfos))
	pipelineInfos, err := c.ListPipeline(false)
	require.NoError(t, err)
	require.Equal(t, 0, len(pipelineInfos))
	jobInfos, err := c.ListJob("", nil, -1, true)
	require.NoError(t, err)
	require.Equal(t, 0, len(jobInfos))
}

func TestRecursiveCp(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())
	// create repos
	dataRepo := tu.UniqueString("TestRecursiveCp_data")
	require.NoError(t, c.CreateRepo(dataRepo))
	// create pipeline
	pipelineName := tu.UniqueString("TestRecursiveCp")
	require.NoError(t, c.CreatePipeline(
		pipelineName,
		"",
		[]string{"sh"},
		[]string{
			fmt.Sprintf("cp -r /pfs/%s /pfs/out", dataRepo),
		},
		&pps.ParallelismSpec{
			Constant: 1,
		},
		client.NewPFSInput(dataRepo, "/*"),
		"",
		false,
	))
	// Do commit to repo
	commit, err := c.StartCommit(dataRepo, "master")
	require.NoError(t, err)
	for i := 0; i < 100; i++ {
		require.NoError(t, c.PutFile(
			commit,
			fmt.Sprintf("file%d", i),
			strings.NewReader(strings.Repeat("foo\n", 10000)),
		))
	}
	require.NoError(t, c.FinishCommit(dataRepo, commit.Branch.Name, commit.ID))
	_, err = c.WaitCommitSetAll(commit.ID)
	require.NoError(t, err)
}

func TestPipelineUniqueness(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())

	repo := tu.UniqueString("data")
	require.NoError(t, c.CreateRepo(repo))
	pipelineName := tu.UniqueString("pipeline")
	require.NoError(t, c.CreatePipeline(
		pipelineName,
		"",
		[]string{"bash"},
		[]string{""},
		&pps.ParallelismSpec{
			Constant: 1,
		},
		client.NewPFSInput(repo, "/"),
		"",
		false,
	))
	err := c.CreatePipeline(
		pipelineName,
		"",
		[]string{"bash"},
		[]string{""},
		&pps.ParallelismSpec{
			Constant: 1,
		},
		client.NewPFSInput(repo, "/"),
		"",
		false,
	)
	require.YesError(t, err)
	require.Matches(t, "pipeline .*? already exists", err.Error())
}

func TestUpdatePipeline(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())
	// create repos and create the pipeline
	dataRepo := tu.UniqueString("TestUpdatePipeline_data")
	require.NoError(t, c.CreateRepo(dataRepo))
	pipelineName := tu.UniqueString("pipeline")
	pipelineCommit := client.NewCommit(pipelineName, "master", "")
	require.NoError(t, c.CreatePipeline(
		pipelineName,
		"",
		[]string{"bash"},
		[]string{"echo foo >/pfs/out/file"},
		&pps.ParallelismSpec{
			Constant: 1,
		},
		client.NewPFSInput(dataRepo, "/*"),
		"",
		true,
	))

	commit, err := c.StartCommit(dataRepo, "master")
	require.NoError(t, err)
	require.NoError(t, c.PutFile(commit, "file", strings.NewReader("1"), client.WithAppendPutFile()))
	require.NoError(t, c.FinishCommit(dataRepo, "master", ""))

	_, err = c.WaitCommitSetAll(commit.ID)
	require.NoError(t, err)

	var buffer bytes.Buffer
	require.NoError(t, c.GetFile(pipelineCommit, "file", &buffer))
	require.Equal(t, "foo\n", buffer.String())

	// Update the pipeline
	require.NoError(t, c.CreatePipeline(
		pipelineName,
		"",
		[]string{"bash"},
		[]string{"echo bar >/pfs/out/file"},
		&pps.ParallelismSpec{
			Constant: 1,
		},
		client.NewPFSInput(dataRepo, "/*"),
		"",
		true,
	))

	// Confirm that k8s resources have been updated (fix #4071)
	require.NoErrorWithinTRetry(t, 60*time.Second, func() error {
		kc := tu.GetKubeClient(t)
		svcs, err := kc.CoreV1().Services("default").List(metav1.ListOptions{})
		require.NoError(t, err)
		var newServiceSeen bool
		for _, svc := range svcs.Items {
			switch svc.ObjectMeta.Name {
			case ppsutil.PipelineRcName(pipelineName, 1):
				return fmt.Errorf("stale service encountered: %q", svc.ObjectMeta.Name)
			case ppsutil.PipelineRcName(pipelineName, 2):
				newServiceSeen = true
			}
		}
		if !newServiceSeen {
			return fmt.Errorf("did not find new service: %q", ppsutil.PipelineRcName(pipelineName, 2))
		}
		rcs, err := kc.CoreV1().ReplicationControllers("default").List(metav1.ListOptions{})
		require.NoError(t, err)
		var newRCSeen bool
		for _, rc := range rcs.Items {
			switch rc.ObjectMeta.Name {
			case ppsutil.PipelineRcName(pipelineName, 1):
				return fmt.Errorf("stale RC encountered: %q", rc.ObjectMeta.Name)
			case ppsutil.PipelineRcName(pipelineName, 2):
				newRCSeen = true
			}
		}
		require.True(t, newRCSeen)
		if !newRCSeen {
			return fmt.Errorf("did not find new RC: %q", ppsutil.PipelineRcName(pipelineName, 2))
		}
		return nil
	})

	commit, err = c.StartCommit(dataRepo, "master")
	require.NoError(t, err)
	require.NoError(t, c.PutFile(commit, "file", strings.NewReader("2"), client.WithAppendPutFile()))
	require.NoError(t, c.FinishCommit(dataRepo, "master", ""))
	_, err = c.WaitCommitSetAll(commit.ID)
	require.NoError(t, err)

	buffer.Reset()
	require.NoError(t, c.GetFile(pipelineCommit, "file", &buffer))
	require.Equal(t, "bar\n", buffer.String())

	// Inspect the first job to make sure it hasn't changed
	jis, err := c.ListJob(pipelineName, nil, -1, true)
	require.NoError(t, err)
	require.Equal(t, 4, len(jis))
	require.Equal(t, "echo bar >/pfs/out/file", jis[0].Details.Transform.Stdin[0])
	require.Equal(t, "echo bar >/pfs/out/file", jis[1].Details.Transform.Stdin[0])
	require.Equal(t, "echo foo >/pfs/out/file", jis[2].Details.Transform.Stdin[0])
	require.Equal(t, "echo foo >/pfs/out/file", jis[3].Details.Transform.Stdin[0])

	// Update the pipeline again, this time with Reprocess: true set. Now we
	// should see a different output file
	_, err = c.PpsAPIClient.CreatePipeline(
		context.Background(),
		&pps.CreatePipelineRequest{
			Pipeline: client.NewPipeline(pipelineName),
			Transform: &pps.Transform{
				Cmd:   []string{"bash"},
				Stdin: []string{"echo buzz >/pfs/out/file"},
			},
			ParallelismSpec: &pps.ParallelismSpec{
				Constant: 1,
			},
			Input:     client.NewPFSInput(dataRepo, "/*"),
			Update:    true,
			Reprocess: true,
		})
	require.NoError(t, err)

	// Confirm that k8s resources have been updated (fix #4071)
	require.NoErrorWithinTRetry(t, 60*time.Second, func() error {
		kc := tu.GetKubeClient(t)
		svcs, err := kc.CoreV1().Services("default").List(metav1.ListOptions{})
		require.NoError(t, err)
		var newServiceSeen bool
		for _, svc := range svcs.Items {
			switch svc.ObjectMeta.Name {
			case ppsutil.PipelineRcName(pipelineName, 1):
				return fmt.Errorf("stale service encountered: %q", svc.ObjectMeta.Name)
			case ppsutil.PipelineRcName(pipelineName, 2):
				newServiceSeen = true
			}
		}
		if !newServiceSeen {
			return fmt.Errorf("did not find new service: %q", ppsutil.PipelineRcName(pipelineName, 2))
		}
		rcs, err := kc.CoreV1().ReplicationControllers("default").List(metav1.ListOptions{})
		require.NoError(t, err)
		var newRCSeen bool
		for _, rc := range rcs.Items {
			switch rc.ObjectMeta.Name {
			case ppsutil.PipelineRcName(pipelineName, 1):
				return fmt.Errorf("stale RC encountered: %q", rc.ObjectMeta.Name)
			case ppsutil.PipelineRcName(pipelineName, 2):
				newRCSeen = true
			}
		}
		require.True(t, newRCSeen)
		if !newRCSeen {
			return fmt.Errorf("did not find new RC: %q", ppsutil.PipelineRcName(pipelineName, 2))
		}
		return nil
	})

	_, err = c.WaitCommit(pipelineName, "master", "")
	require.NoError(t, err)
	buffer.Reset()
	require.NoError(t, c.GetFile(pipelineCommit, "file", &buffer))
	require.Equal(t, "buzz\n", buffer.String())
}

func TestUpdatePipelineWithInProgressCommitsAndStats(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}
	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())
	dataRepo := tu.UniqueString("TestUpdatePipelineWithInProgressCommitsAndStats_data")
	require.NoError(t, c.CreateRepo(dataRepo))

	pipeline := tu.UniqueString("pipeline")
	createPipeline := func() {
		_, err := c.PpsAPIClient.CreatePipeline(
			context.Background(),
			&pps.CreatePipelineRequest{
				Pipeline: client.NewPipeline(pipeline),
				Transform: &pps.Transform{
					Cmd:   []string{"bash"},
					Stdin: []string{"sleep 1"},
				},
				Input:  client.NewPFSInput(dataRepo, "/*"),
				Update: true,
			})
		require.NoError(t, err)
	}
	createPipeline()

	flushJob := func(commitNum int) {
		commit, err := c.StartCommit(dataRepo, "master")
		require.NoError(t, err)
		require.NoError(t, c.PutFile(commit, "file"+strconv.Itoa(commitNum), strings.NewReader("foo"), client.WithAppendPutFile()))
		require.NoError(t, c.FinishCommit(dataRepo, commit.Branch.Name, commit.ID))
		commitInfos, err := c.WaitCommitSetAll(commit.ID)
		require.NoError(t, err)
		require.Equal(t, 4, len(commitInfos))
	}
	// Create a new job that should succeed (both output and stats commits should be finished normally).
	flushJob(1)

	// Create multiple new commits.
	numCommits := 5
	for i := 1; i < numCommits; i++ {
		commit, err := c.StartCommit(dataRepo, "master")
		require.NoError(t, err)
		require.NoError(t, c.PutFile(commit, "file"+strconv.Itoa(i), strings.NewReader("foo"), client.WithAppendPutFile()))
		require.NoError(t, c.FinishCommit(dataRepo, commit.Branch.Name, commit.ID))
	}

	// Force the in progress commits to be finished.
	createPipeline()
	// Create a new job that should succeed (should not get blocked on an unfinished stats commit).
	flushJob(numCommits)
}

func TestUpdateFailedPipeline(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())
	// create repos
	dataRepo := tu.UniqueString("TestUpdateFailedPipeline_data")
	require.NoError(t, c.CreateRepo(dataRepo))
	pipelineName := tu.UniqueString("pipeline")
	require.NoError(t, c.CreatePipeline(
		pipelineName,
		"imagethatdoesntexist",
		[]string{"bash"},
		[]string{"echo foo >/pfs/out/file"},
		&pps.ParallelismSpec{
			Constant: 1,
		},
		client.NewPFSInput(dataRepo, "/*"),
		"",
		false,
	))
	commit, err := c.StartCommit(dataRepo, "master")
	require.NoError(t, err)
	require.NoError(t, c.PutFile(commit, "file", strings.NewReader("1"), client.WithAppendPutFile()))
	require.NoError(t, c.FinishCommit(dataRepo, "master", ""))

	// Wait for pod to try and pull the bad image
	require.NoErrorWithinTRetry(t, time.Second*30, func() error {
		pipelineInfo, err := c.InspectPipeline(pipelineName, false)
		require.NoError(t, err)
		if pipelineInfo.State != pps.PipelineState_PIPELINE_CRASHING {
			return errors.Errorf("expected pipeline to be in CRASHING state but got: %v\n", pipelineInfo.State)
		}
		return nil
	})

	require.NoError(t, c.CreatePipeline(
		pipelineName,
		"bash:4",
		[]string{"bash"},
		[]string{"echo bar >/pfs/out/file"},
		&pps.ParallelismSpec{
			Constant: 1,
		},
		client.NewPFSInput(dataRepo, "/*"),
		"",
		true,
	))
	require.NoErrorWithinTRetry(t, time.Second*30, func() error {
		pipelineInfo, err := c.InspectPipeline(pipelineName, false)
		require.NoError(t, err)
		if pipelineInfo.State != pps.PipelineState_PIPELINE_RUNNING {
			return errors.Errorf("expected pipeline to be in RUNNING state but got: %v\n", pipelineInfo.State)
		}
		return nil
	})

	// Sanity check run some actual data through the pipeline:
	commit, err = c.StartCommit(dataRepo, "master")
	require.NoError(t, err)
	require.NoError(t, c.PutFile(commit, "file", strings.NewReader("2"), client.WithAppendPutFile()))
	require.NoError(t, c.FinishCommit(dataRepo, "master", ""))
	_, err = c.WaitCommitSetAll(commit.ID)
	require.NoError(t, err)

	var buffer bytes.Buffer
	require.NoError(t, c.GetFile(client.NewCommit(pipelineName, "master", ""), "file", &buffer))
	require.Equal(t, "bar\n", buffer.String())
}

func TestUpdateStoppedPipeline(t *testing.T) {
	// Pipeline should be updated, but should not be restarted
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())
	// create repo & pipeline
	dataRepo := tu.UniqueString("TestUpdateStoppedPipeline_data")
	require.NoError(t, c.CreateRepo(dataRepo))
	dataCommit := client.NewCommit(dataRepo, "master", "")
	pipelineName := tu.UniqueString("pipeline")
	require.NoError(t, c.CreatePipeline(
		pipelineName,
		"",
		[]string{"bash"},
		[]string{"cp /pfs/*/file /pfs/out/file"},
		&pps.ParallelismSpec{
			Constant: 1,
		},
		client.NewPFSInput(dataRepo, "/*"),
		"",
		false,
	))

	commits, err := c.ListCommit(client.NewRepo(pipelineName), client.NewCommit(pipelineName, "master", ""), nil, 0)
	require.NoError(t, err)
	require.Equal(t, 1, len(commits))

	// Add input data
	require.NoError(t, c.PutFile(dataCommit, "file", strings.NewReader("foo"), client.WithAppendPutFile()))

	commits, err = c.ListCommit(client.NewRepo(pipelineName), client.NewCommit(pipelineName, "master", ""), nil, 0)
	require.NoError(t, err)
	require.Equal(t, 2, len(commits))

	// Make sure the pipeline runs once (i.e. it's all the way up)
	commitInfos, err := c.WaitCommitSetAll(commits[0].Commit.ID)
	require.NoError(t, err)
	require.Equal(t, 4, len(commitInfos))

	// Stop the pipeline (and confirm that it's stopped)
	require.NoError(t, c.StopPipeline(pipelineName))
	pipelineInfo, err := c.InspectPipeline(pipelineName, false)
	require.NoError(t, err)
	require.Equal(t, true, pipelineInfo.Stopped)
	require.NoError(t, backoff.Retry(func() error {
		pipelineInfo, err = c.InspectPipeline(pipelineName, false)
		if err != nil {
			return err
		}
		if pipelineInfo.State != pps.PipelineState_PIPELINE_PAUSED {
			return errors.Errorf("expected pipeline to be in state PAUSED, but was in %s",
				pipelineInfo.State)
		}
		return nil
	}, backoff.NewTestingBackOff()))

	commits, err = c.ListCommit(client.NewRepo(pipelineName), client.NewCommit(pipelineName, "master", ""), nil, 0)
	require.NoError(t, err)
	require.Equal(t, 2, len(commits))

	// Update shouldn't restart it (wait for version to increment)
	require.NoError(t, c.CreatePipeline(
		pipelineName,
		"",
		[]string{"bash"},
		[]string{"cp /pfs/*/file /pfs/out/file"},
		&pps.ParallelismSpec{
			Constant: 1,
		},
		client.NewPFSInput(dataRepo, "/*"),
		"",
		true,
	))
	time.Sleep(10 * time.Second)
	require.NoError(t, backoff.Retry(func() error {
		pipelineInfo, err = c.InspectPipeline(pipelineName, false)
		if err != nil {
			return err
		}
		if pipelineInfo.State != pps.PipelineState_PIPELINE_PAUSED {
			return errors.Errorf("expected pipeline to be in state PAUSED, but was in %s",
				pipelineInfo.State)
		}
		if pipelineInfo.Version != 2 {
			return errors.Errorf("expected pipeline to be on v2, but was on v%d",
				pipelineInfo.Version)
		}
		return nil
	}, backoff.NewTestingBackOff()))

	commits, err = c.ListCommit(client.NewRepo(pipelineName), client.NewCommit(pipelineName, "master", ""), nil, 0)
	require.NoError(t, err)
	require.Equal(t, 2, len(commits))

	// Create a commit (to give the pipeline pending work), then start the pipeline
	require.NoError(t, c.PutFile(dataCommit, "file", strings.NewReader("bar"), client.WithAppendPutFile()))
	require.NoError(t, c.StartPipeline(pipelineName))

	// Pipeline should start and create a job should succeed -- fix
	// https://github.com/pachyderm/pachyderm/v2/issues/3934)
	commitInfo, err := c.InspectCommit(dataRepo, "master", "")
	require.NoError(t, err)
	commitInfos, err = c.WaitCommitSetAll(commitInfo.Commit.ID)
	require.NoError(t, err)
	require.Equal(t, 4, len(commitInfos))
	commits, err = c.ListCommit(client.NewRepo(pipelineName), client.NewCommit(pipelineName, "master", ""), nil, 0)
	require.NoError(t, err)
	require.Equal(t, 3, len(commits))

	var buf bytes.Buffer
	outputCommit := client.NewCommit(pipelineName, "master", commitInfo.Commit.ID)
	require.NoError(t, c.GetFile(outputCommit, "file", &buf))
	require.Equal(t, "foobar", buf.String())
}

func TestUpdatePipelineRunningJob(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())
	// create repos
	dataRepo := tu.UniqueString("TestUpdatePipeline_data")
	require.NoError(t, c.CreateRepo(dataRepo))
	pipelineName := tu.UniqueString("pipeline")
	require.NoError(t, c.CreatePipeline(
		pipelineName,
		"",
		[]string{"bash"},
		[]string{"sleep 1000"},
		&pps.ParallelismSpec{
			Constant: 2,
		},
		client.NewPFSInput(dataRepo, "/*"),
		"",
		false,
	))

	numFiles := 50
	commit1, err := c.StartCommit(dataRepo, "master")
	require.NoError(t, err)
	for i := 0; i < numFiles; i++ {
		require.NoError(t, c.PutFile(commit1, fmt.Sprintf("file-%d", i), strings.NewReader(""), client.WithAppendPutFile()))
	}
	require.NoError(t, c.FinishCommit(dataRepo, commit1.Branch.Name, commit1.ID))

	commit2, err := c.StartCommit(dataRepo, "master")
	require.NoError(t, err)
	for i := 0; i < numFiles; i++ {
		require.NoError(t, c.PutFile(commit2, fmt.Sprintf("file-%d", i+numFiles), strings.NewReader(""), client.WithAppendPutFile()))
	}
	require.NoError(t, c.FinishCommit(dataRepo, commit2.Branch.Name, commit2.ID))

	b := backoff.NewTestingBackOff()
	b.MaxElapsedTime = 30 * time.Second
	require.NoError(t, backoff.Retry(func() error {
		jobInfos, err := c.ListJob(pipelineName, nil, -1, true)
		if err != nil {
			return err
		}
		if len(jobInfos) != 3 {
			return errors.Errorf("wrong number of jobs")
		}

		state := jobInfos[1].State
		if state != pps.JobState_JOB_RUNNING {
			return fmt.Errorf("wrong state: %v for %s", state, jobInfos[1].Job.ID)
		}

		state = jobInfos[0].State
		if state != pps.JobState_JOB_RUNNING {
			return errors.Errorf("wrong state: %v for %s", state, jobInfos[0].Job.ID)
		}
		return nil
	}, b))

	// Update the pipeline. This will not create a new pipeline as reprocess
	// isn't set to true.
	require.NoError(t, c.CreatePipeline(
		pipelineName,
		"",
		[]string{"bash"},
		[]string{"true"},
		&pps.ParallelismSpec{
			Constant: 2,
		},
		client.NewPFSInput(dataRepo, "/*"),
		"",
		true,
	))
	commitInfo, err := c.InspectCommit(dataRepo, "master", "")
	require.NoError(t, err)
	_, err = c.WaitCommitSetAll(commitInfo.Commit.ID)
	require.NoError(t, err)

	jobInfos, err := c.ListJob(pipelineName, nil, -1, true)
	require.NoError(t, err)
	require.Equal(t, 4, len(jobInfos))
	require.Equal(t, pps.JobState_JOB_SUCCESS.String(), jobInfos[0].State.String())
	require.Equal(t, pps.JobState_JOB_KILLED.String(), jobInfos[1].State.String())
	require.Equal(t, pps.JobState_JOB_KILLED.String(), jobInfos[2].State.String())
	require.Equal(t, pps.JobState_JOB_SUCCESS.String(), jobInfos[3].State.String())
}

func TestManyFilesSingleCommit(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}
	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())
	// create repos
	dataRepo := tu.UniqueString("TestManyFilesSingleCommit_data")
	require.NoError(t, c.CreateRepo(dataRepo))
	dataCommit := client.NewCommit(dataRepo, "master", "")

	numFiles := 20000
	_, err := c.StartCommit(dataRepo, "master")
	require.NoError(t, err)
	require.NoError(t, c.WithModifyFileClient(dataCommit, func(mfc client.ModifyFile) error {
		for i := 0; i < numFiles; i++ {
			require.NoError(t, mfc.PutFile(fmt.Sprintf("file-%d", i), strings.NewReader(""), client.WithAppendPutFile()))
		}
		return nil
	}))
	require.NoError(t, c.FinishCommit(dataRepo, "master", ""))
	fileInfos, err := c.ListFileAll(dataCommit, "")
	require.NoError(t, err)
	require.Equal(t, numFiles, len(fileInfos))
}

func TestManyFilesSingleOutputCommit(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}
	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())
	dataRepo := tu.UniqueString("TestManyFilesSingleOutputCommit_data")
	require.NoError(t, c.CreateRepo(dataRepo))
	file := "file"

	// Setup pipeline.
	pipelineName := tu.UniqueString("TestManyFilesSingleOutputCommit")
	_, err := c.PpsAPIClient.CreatePipeline(context.Background(),
		&pps.CreatePipelineRequest{
			Pipeline: client.NewPipeline(pipelineName),
			Transform: &pps.Transform{
				Cmd:   []string{"sh"},
				Stdin: []string{"while read line; do echo $line > /pfs/out/$line; done < " + path.Join("/pfs", dataRepo, file)},
			},
			Input: client.NewPFSInput(dataRepo, "/*"),
		},
	)
	require.NoError(t, err)

	// Setup input.
	commit, err := c.StartCommit(dataRepo, "master")
	require.NoError(t, err)
	numFiles := 20000
	var data string
	for i := 0; i < numFiles; i++ {
		data += strconv.Itoa(i) + "\n"
	}
	require.NoError(t, c.PutFile(commit, file, strings.NewReader(data), client.WithAppendPutFile()))
	require.NoError(t, c.FinishCommit(dataRepo, "master", commit.ID))

	// Check results.
	jis, err := c.WaitJobSetAll(commit.ID, false)
	require.NoError(t, err)
	require.Equal(t, 1, len(jis))
	fileInfos, err := c.ListFileAll(client.NewCommit(pipelineName, "master", ""), "")
	require.NoError(t, err)
	require.Equal(t, numFiles, len(fileInfos))
}

func TestStopPipeline(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())
	// create repos
	dataRepo := tu.UniqueString("TestPipeline_data")
	require.NoError(t, c.CreateRepo(dataRepo))
	// create pipeline
	pipelineName := tu.UniqueString("pipeline")
	require.NoError(t, c.CreatePipeline(
		pipelineName,
		"",
		[]string{"cp", path.Join("/pfs", dataRepo, "file"), "/pfs/out/file"},
		nil,
		&pps.ParallelismSpec{
			Constant: 1,
		},
		client.NewPFSInput(dataRepo, "/*"),
		"",
		false,
	))

	// We should just have the initial output branch head commit
	commits, err := c.ListCommit(client.NewRepo(pipelineName), client.NewCommit(pipelineName, "master", ""), nil, 0)
	require.NoError(t, err)
	require.Equal(t, len(commits), 1)

	// Stop the pipeline, so it doesn't process incoming commits
	require.NoError(t, c.StopPipeline(pipelineName))

	// Do first commit to repo
	commit1, err := c.StartCommit(dataRepo, "master")
	require.NoError(t, err)
	require.NoError(t, c.PutFile(commit1, "file", strings.NewReader("foo\n"), client.WithAppendPutFile()))
	require.NoError(t, c.FinishCommit(dataRepo, commit1.Branch.Name, commit1.ID))

	// wait for 10 seconds and check that no new commit has been outputted
	time.Sleep(10 * time.Second)
	commits, err = c.ListCommit(client.NewRepo(pipelineName), client.NewCommit(pipelineName, "master", ""), nil, 0)
	require.NoError(t, err)
	require.Equal(t, len(commits), 1)

	// Restart pipeline, and make sure a new output commit is generated
	require.NoError(t, c.StartPipeline(pipelineName))

	commits, err = c.ListCommit(client.NewRepo(pipelineName), client.NewCommit(pipelineName, "master", ""), nil, 0)
	require.NoError(t, err)
	require.Equal(t, len(commits), 2)

	commitInfo, err := c.WaitCommit(pipelineName, "master", "")
	require.NoError(t, err)

	var buffer bytes.Buffer
	require.NoError(t, c.GetFile(commitInfo.Commit, "file", &buffer))
	require.Equal(t, "foo\n", buffer.String())
}

func TestStandby(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	t.Run("ChainOf10", func(t *testing.T) {
		require.NoError(t, c.DeleteAll())

		dataRepo := tu.UniqueString("TestStandby_data")
		require.NoError(t, c.CreateRepo(dataRepo))
		dataCommit := client.NewCommit(dataRepo, "master", "")

		numPipelines := 5
		pipelines := make([]string, numPipelines)
		for i := 0; i < numPipelines; i++ {
			pipelines[i] = tu.UniqueString("TestStandby")
			input := dataRepo
			if i > 0 {
				input = pipelines[i-1]
			}
			_, err := c.PpsAPIClient.CreatePipeline(context.Background(),
				&pps.CreatePipelineRequest{
					Pipeline: client.NewPipeline(pipelines[i]),
					Transform: &pps.Transform{
						Cmd: []string{"cp", path.Join("/pfs", input, "file"), "/pfs/out/file"},
					},
					Input:       client.NewPFSInput(input, "/*"),
					Autoscaling: true,
				},
			)
			require.NoError(t, err)
		}

		require.NoErrorWithinTRetry(t, time.Second*60, func() error {
			pis, err := c.ListPipeline(false)
			require.NoError(t, err)
			var standby int
			for _, pi := range pis {
				if pi.State == pps.PipelineState_PIPELINE_STANDBY {
					standby++
				}
			}
			if standby != numPipelines {
				return errors.Errorf("should have %d pipelines in standby, not %d", numPipelines, standby)
			}
			return nil
		})

		require.NoError(t, c.PutFile(dataCommit, "file", strings.NewReader("foo")))

		var eg errgroup.Group
		var finished bool
		eg.Go(func() error {
			_, err := c.WaitCommitSetAll(dataCommit.ID)
			require.NoError(t, err)
			finished = true
			return nil
		})
		eg.Go(func() error {
			for !finished {
				pis, err := c.ListPipeline(false)
				require.NoError(t, err)
				var active int
				for _, pi := range pis {
					if pi.State != pps.PipelineState_PIPELINE_STANDBY {
						active++
					}
				}
				// We tolerate having 2 pipelines out of standby because there's
				// latency associated with entering and exiting standby.
				require.True(t, active <= 2, "active: %d", active)
			}
			return nil
		})
		eg.Wait()
	})
	t.Run("ManyCommits", func(t *testing.T) {
		require.NoError(t, c.DeleteAll())

		dataRepo := tu.UniqueString("TestStandby_data")
		dataCommit := client.NewCommit(dataRepo, "master", "")
		pipeline := tu.UniqueString("TestStandby")
		require.NoError(t, c.CreateRepo(dataRepo))
		_, err := c.PpsAPIClient.CreatePipeline(context.Background(),
			&pps.CreatePipelineRequest{
				Pipeline: client.NewPipeline(pipeline),
				Transform: &pps.Transform{
					Cmd:   []string{"sh"},
					Stdin: []string{"echo $PPS_POD_NAME >/pfs/out/pod"},
				},
				Input: client.NewPFSInput(dataRepo, "/"),
			},
		)
		require.NoError(t, err)
		numCommits := 10
		for i := 0; i < numCommits; i++ {
			require.NoError(t, c.PutFile(dataCommit, fmt.Sprintf("file-%d", i), strings.NewReader("foo")))
		}
		require.NoErrorWithinTRetry(t, 120*time.Second, func() error {
			// Let pipeline run
			commitInfo, err := c.InspectCommit(dataRepo, "master", "")
			require.NoError(t, err)
			commitInfos, err := c.WaitCommitSetAll(commitInfo.Commit.ID)
			require.NoError(t, err)
			require.Equal(t, 2, len(commitInfos))
			pod := ""
			commitInfos, err = c.ListCommit(client.NewRepo(pipeline), client.NewCommit(pipeline, "master", ""), nil, 0)
			require.NoError(t, err)
			for _, ci := range commitInfos {
				var buffer bytes.Buffer
				require.NoError(t, c.GetFile(ci.Commit, "pod", &buffer))
				if pod == "" {
					pod = buffer.String()
				} else {
					require.True(t, pod == buffer.String(), "multiple pods were used to process commits")
				}
			}
			return nil
		})
		pi, err := c.InspectPipeline(pipeline, false)
		require.NoError(t, err)
		require.Equal(t, pps.PipelineState_PIPELINE_STANDBY.String(), pi.State.String())
	})
}

func TestStopStandbyPipeline(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())

	dataRepo := tu.UniqueString(t.Name() + "_data")
	require.NoError(t, c.CreateRepo(dataRepo))
	dataCommit := client.NewCommit(dataRepo, "master", "")

	pipeline := tu.UniqueString(t.Name())
	_, err := c.PpsAPIClient.CreatePipeline(context.Background(),
		&pps.CreatePipelineRequest{
			Pipeline: client.NewPipeline(pipeline),
			Transform: &pps.Transform{
				Cmd: []string{"/bin/bash"},
				Stdin: []string{
					fmt.Sprintf("cp /pfs/%s/* /pfs/out", dataRepo),
				},
			},
			Input:       client.NewPFSInput(dataRepo, "/*"),
			Autoscaling: true,
		},
	)
	require.NoError(t, err)

	require.NoErrorWithinTRetry(t, 30*time.Second, func() error {
		pi, err := c.InspectPipeline(pipeline, false)
		require.NoError(t, err)
		if pi.State != pps.PipelineState_PIPELINE_STANDBY {
			return fmt.Errorf("expected %q to be in STANDBY, but was in %s", pipeline, pi.State)
		}
		return nil
	})

	// Run the pipeline once under normal conditions. It should run and then go
	// back into standby
	require.NoError(t, c.PutFile(dataCommit, "/foo", strings.NewReader("foo"), client.WithAppendPutFile()))
	require.NoErrorWithinTRetry(t, 60*time.Second, func() error {
		// Let pipeline run
		commitInfo, err := c.InspectCommit(dataRepo, "master", "")
		require.NoError(t, err)
		_, err = c.WaitCommitSetAll(commitInfo.Commit.ID)
		require.NoError(t, err)
		// check ending state
		pi, err := c.InspectPipeline(pipeline, false)
		require.NoError(t, err)
		if pi.State != pps.PipelineState_PIPELINE_STANDBY {
			return fmt.Errorf("expected %q to be in STANDBY, but was in %s", pipeline, pi.State)
		}
		return nil
	})

	// Stop the pipeline...
	require.NoError(t, c.StopPipeline(pipeline))
	require.NoErrorWithinTRetry(t, 60*time.Second, func() error {
		pi, err := c.InspectPipeline(pipeline, false)
		require.NoError(t, err)
		if pi.State != pps.PipelineState_PIPELINE_PAUSED {
			return fmt.Errorf("expected %q to be in PAUSED, but was in %s", pipeline,
				pi.State)
		}
		return nil
	})
	// ...and then create several new input commits. Pipeline shouldn't run.
	for i := 0; i < 3; i++ {
		file := fmt.Sprintf("bar-%d", i)
		require.NoError(t, c.PutFile(dataCommit, "/"+file, strings.NewReader(file), client.WithAppendPutFile()))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	for ctx.Err() == nil {
		pi, err := c.InspectPipeline(pipeline, false)
		require.NoError(t, err)
		require.NotEqual(t, pps.PipelineState_PIPELINE_RUNNING, pi.State)
	}
	cancel()

	// Start pipeline--it should run and then enter standby
	require.NoError(t, c.StartPipeline(pipeline))
	require.NoErrorWithinTRetry(t, 60*time.Second, func() error {
		// Let pipeline run
		commitInfo, err := c.InspectCommit(dataRepo, "master", "")
		require.NoError(t, err)
		_, err = c.WaitCommitSetAll(commitInfo.Commit.ID)
		require.NoError(t, err)
		// check ending state
		pi, err := c.InspectPipeline(pipeline, false)
		require.NoError(t, err)
		if pi.State != pps.PipelineState_PIPELINE_STANDBY {
			return fmt.Errorf("expected %q to be in STANDBY, but was in %s", pipeline, pi.State)
		}
		return nil
	})

	// Finally, check that there's only three output commits
	commitInfos, err := c.ListCommit(client.NewRepo(pipeline), client.NewCommit(pipeline, "master", ""), nil, 0)
	require.NoError(t, err)
	require.Equal(t, 3, len(commitInfos))
}

func TestPipelineEnv(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	// make a secret to reference
	k := tu.GetKubeClient(t)
	secretName := tu.UniqueString("test-secret")
	_, err := k.CoreV1().Secrets(v1.NamespaceDefault).Create(
		&v1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name: secretName,
			},
			Data: map[string][]byte{
				"foo": []byte("foo\n"),
			},
		},
	)
	require.NoError(t, err)
	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())

	// create repos
	dataRepo := tu.UniqueString("TestPipelineEnv_data")
	require.NoError(t, c.CreateRepo(dataRepo))

	// create pipeline
	pipelineName := tu.UniqueString("pipeline")
	_, err = c.PpsAPIClient.CreatePipeline(
		context.Background(),
		&pps.CreatePipelineRequest{
			Pipeline: client.NewPipeline(pipelineName),
			Transform: &pps.Transform{
				Cmd: []string{"sh"},
				Stdin: []string{
					"ls /var/secret",
					"cat /var/secret/foo > /pfs/out/foo",
					"echo $bar> /pfs/out/bar",
					"echo $foo> /pfs/out/foo_env",
					fmt.Sprintf("echo $%s >/pfs/out/job_id", client.JobIDEnv),
					fmt.Sprintf("echo $%s >/pfs/out/output_commit_id", client.OutputCommitIDEnv),
					fmt.Sprintf("echo $%s >/pfs/out/input", dataRepo),
					fmt.Sprintf("echo $%s_COMMIT >/pfs/out/input_commit", dataRepo),
				},
				Env: map[string]string{"bar": "bar"},
				Secrets: []*pps.SecretMount{
					{
						Name:      secretName,
						Key:       "foo",
						MountPath: "/var/secret",
						EnvVar:    "foo",
					},
				},
			},
			ParallelismSpec: &pps.ParallelismSpec{
				Constant: 1,
			},
			Input: client.NewPFSInput(dataRepo, "/*"),
		})
	require.NoError(t, err)

	// Do first commit to repo
	dataCommit := client.NewCommit(dataRepo, "master", "")
	require.NoError(t, c.PutFile(dataCommit, "file", strings.NewReader("foo\n"), client.WithAppendPutFile()))

	commitInfo, err := c.InspectCommit(dataRepo, "master", "")
	require.NoError(t, err)

	jis, err := c.WaitJobSetAll(commitInfo.Commit.ID, true)
	require.NoError(t, err)
	require.Equal(t, 1, len(jis))

	var buffer bytes.Buffer
	require.NoError(t, c.GetFile(jis[0].OutputCommit, "foo", &buffer))
	require.Equal(t, "foo\n", buffer.String())
	buffer.Reset()
	require.NoError(t, c.GetFile(jis[0].OutputCommit, "foo_env", &buffer))
	require.Equal(t, "foo\n", buffer.String())
	buffer.Reset()
	require.NoError(t, c.GetFile(jis[0].OutputCommit, "bar", &buffer))
	require.Equal(t, "bar\n", buffer.String())
	buffer.Reset()
	require.NoError(t, c.GetFile(jis[0].OutputCommit, "job_id", &buffer))
	require.Equal(t, fmt.Sprintf("%s\n", jis[0].Job.ID), buffer.String())
	buffer.Reset()
	require.NoError(t, c.GetFile(jis[0].OutputCommit, "output_commit_id", &buffer))
	require.Equal(t, fmt.Sprintf("%s\n", jis[0].OutputCommit.ID), buffer.String())
	buffer.Reset()
	require.NoError(t, c.GetFile(jis[0].OutputCommit, "input", &buffer))
	require.Equal(t, fmt.Sprintf("/pfs/%s/file\n", dataRepo), buffer.String())
	buffer.Reset()
	require.NoError(t, c.GetFile(jis[0].OutputCommit, "input_commit", &buffer))
	require.Equal(t, fmt.Sprintf("%s\n", jis[0].Details.Input.Pfs.Commit), buffer.String())
}

func TestPipelineWithFullObjects(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())
	// create repos
	dataRepo := tu.UniqueString("TestPipeline_data")
	require.NoError(t, c.CreateRepo(dataRepo))
	// create pipeline
	pipelineName := tu.UniqueString("pipeline")
	require.NoError(t, c.CreatePipeline(
		pipelineName,
		"",
		[]string{"cp", path.Join("/pfs", dataRepo, "file"), "/pfs/out/file"},
		nil,
		&pps.ParallelismSpec{
			Constant: 1,
		},
		client.NewPFSInput(dataRepo, "/*"),
		"",
		false,
	))

	// Do first commit to repo
	commit1, err := c.StartCommit(dataRepo, "master")
	require.NoError(t, err)
	require.NoError(t, c.PutFile(commit1, "file", strings.NewReader("foo\n"), client.WithAppendPutFile()))
	require.NoError(t, c.FinishCommit(dataRepo, commit1.Branch.Name, commit1.ID))
	commitInfos, err := c.WaitCommitSetAll(commit1.ID)
	require.NoError(t, err)
	require.Equal(t, 4, len(commitInfos))

	var buffer bytes.Buffer
	outputCommit := client.NewCommit(pipelineName, "master", commit1.ID)
	require.NoError(t, c.GetFile(outputCommit, "file", &buffer))
	require.Equal(t, "foo\n", buffer.String())

	// Do second commit to repo
	commit2, err := c.StartCommit(dataRepo, "master")
	require.NoError(t, err)
	require.NoError(t, c.PutFile(commit2, "file", strings.NewReader("bar\n"), client.WithAppendPutFile()))
	require.NoError(t, c.FinishCommit(dataRepo, commit2.Branch.Name, commit2.ID))
	commitInfos, err = c.WaitCommitSetAll(commit2.ID)
	require.NoError(t, err)
	require.Equal(t, 4, len(commitInfos))

	buffer = bytes.Buffer{}
	outputCommit = client.NewCommit(pipelineName, "master", commit2.ID)
	require.NoError(t, c.GetFile(outputCommit, "file", &buffer))
	require.Equal(t, "foo\nbar\n", buffer.String())
}

func TestPipelineWithExistingInputCommits(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())
	// create repos
	dataRepo := tu.UniqueString("TestPipeline_data")
	require.NoError(t, c.CreateRepo(dataRepo))

	// Do first commit to repo
	commit1, err := c.StartCommit(dataRepo, "master")
	require.NoError(t, err)
	require.NoError(t, c.PutFile(commit1, "file", strings.NewReader("foo\n"), client.WithAppendPutFile()))
	require.NoError(t, c.FinishCommit(dataRepo, commit1.Branch.Name, commit1.ID))

	// Do second commit to repo
	commit2, err := c.StartCommit(dataRepo, "master")
	require.NoError(t, err)
	require.NoError(t, c.PutFile(commit2, "file", strings.NewReader("bar\n"), client.WithAppendPutFile()))
	require.NoError(t, c.FinishCommit(dataRepo, commit2.Branch.Name, commit2.ID))

	// create pipeline
	pipelineName := tu.UniqueString("pipeline")
	require.NoError(t, c.CreatePipeline(
		pipelineName,
		"",
		[]string{"cp", path.Join("/pfs", dataRepo, "file"), "/pfs/out/file"},
		nil,
		&pps.ParallelismSpec{
			Constant: 1,
		},
		client.NewPFSInput(dataRepo, "/*"),
		"",
		false,
	))

	commitInfo, err := c.InspectCommit(dataRepo, "master", "")
	require.NoError(t, err)
	commitInfos, err := c.WaitCommitSetAll(commitInfo.Commit.ID)
	require.NoError(t, err)
	require.Equal(t, 4, len(commitInfos))

	buffer := bytes.Buffer{}
	outputCommit := client.NewCommit(pipelineName, "master", commitInfo.Commit.ID)
	require.NoError(t, c.GetFile(outputCommit, "file", &buffer))
	require.Equal(t, "foo\nbar\n", buffer.String())

	// Check that one output commit is created (processing the inputs' head commits)
	commitInfos, err = c.ListCommit(client.NewRepo(pipelineName), client.NewCommit(pipelineName, "master", ""), nil, 0)
	require.NoError(t, err)
	require.Equal(t, 1, len(commitInfos))
}

func TestPipelineThatSymlinks(t *testing.T) {
	// TODO(2.0 required): this pipeline is failing because the datum upload code
	// attempts to follow symlinks which do not point to valid files and this
	// causes the worker to retry indefinitely.
	t.Skip("hanging symlinks cause the job to error loop indefinitely")
	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())

	// create repos
	dataRepo := tu.UniqueString("TestPipeline_data")
	require.NoError(t, c.CreateRepo(dataRepo))

	// create pipeline
	pipelineName := tu.UniqueString("pipeline")
	require.NoError(t, c.CreatePipeline(
		pipelineName,
		"",
		[]string{"bash"},
		[]string{
			// Symlinks to input files
			fmt.Sprintf("ln -s /pfs/%s/foo /pfs/out/foo", dataRepo),
			fmt.Sprintf("ln -s /pfs/%s/dir1/bar /pfs/out/bar", dataRepo),
			"mkdir /pfs/out/dir",
			fmt.Sprintf("ln -s /pfs/%s/dir2 /pfs/out/dir/dir2", dataRepo),
			// Symlinks to external files
			"echo buzz > /tmp/buzz",
			"ln -s /tmp/buzz /pfs/out/buzz",
		},
		&pps.ParallelismSpec{
			Constant: 1,
		},
		client.NewPFSInput(dataRepo, "/"),
		"",
		false,
	))

	// Do first commit to repo
	commit, err := c.StartCommit(dataRepo, "master")
	require.NoError(t, err)
	require.NoError(t, c.PutFile(commit, "foo", strings.NewReader("foo"), client.WithAppendPutFile()))
	require.NoError(t, c.PutFile(commit, "dir1/bar", strings.NewReader("bar"), client.WithAppendPutFile()))
	require.NoError(t, c.PutFile(commit, "dir2/foo", strings.NewReader("foo"), client.WithAppendPutFile()))
	require.NoError(t, c.FinishCommit(dataRepo, commit.Branch.Name, commit.ID))

	commitInfo, err := c.InspectCommit(dataRepo, "master", "")
	require.NoError(t, err)
	commitInfos, err := c.WaitCommitSetAll(commitInfo.Commit.ID)
	require.NoError(t, err)
	require.Equal(t, 4, len(commitInfos))

	// Check that the output files are identical to the input files.
	buffer := bytes.Buffer{}
	outputCommit := client.NewCommit(pipelineName, "master", commitInfo.Commit.ID)
	require.NoError(t, c.GetFile(outputCommit, "foo", &buffer))
	require.Equal(t, "foo", buffer.String())
	buffer.Reset()
	require.NoError(t, c.GetFile(outputCommit, "bar", &buffer))
	require.Equal(t, "bar", buffer.String())
	buffer.Reset()
	require.NoError(t, c.GetFile(outputCommit, "dir/dir2/foo", &buffer))
	require.Equal(t, "foo", buffer.String())
	buffer.Reset()
	require.NoError(t, c.GetFile(outputCommit, "buzz", &buffer))
	require.Equal(t, "buzz\n", buffer.String())
}

// TestChainedPipelines tracks https://github.com/pachyderm/pachyderm/v2/issues/797
func TestChainedPipelines(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())
	aRepo := tu.UniqueString("A")
	require.NoError(t, c.CreateRepo(aRepo))

	dRepo := tu.UniqueString("D")
	require.NoError(t, c.CreateRepo(dRepo))

	aCommit, err := c.StartCommit(aRepo, "master")
	require.NoError(t, err)
	require.NoError(t, c.PutFile(aCommit, "file", strings.NewReader("foo\n"), client.WithAppendPutFile()))
	require.NoError(t, c.FinishCommit(aRepo, "master", ""))

	dCommit, err := c.StartCommit(dRepo, "master")
	require.NoError(t, err)
	require.NoError(t, c.PutFile(dCommit, "file", strings.NewReader("bar\n"), client.WithAppendPutFile()))
	require.NoError(t, c.FinishCommit(dRepo, "master", ""))

	bPipeline := tu.UniqueString("B")
	require.NoError(t, c.CreatePipeline(
		bPipeline,
		"",
		[]string{"cp", path.Join("/pfs", aRepo, "file"), "/pfs/out/file"},
		nil,
		&pps.ParallelismSpec{
			Constant: 1,
		},
		client.NewPFSInput(aRepo, "/"),
		"",
		false,
	))

	cPipeline := tu.UniqueString("C")
	require.NoError(t, c.CreatePipeline(
		cPipeline,
		"",
		[]string{"sh"},
		[]string{fmt.Sprintf("cp /pfs/%s/file /pfs/out/bFile", bPipeline),
			fmt.Sprintf("cp /pfs/%s/file /pfs/out/dFile", dRepo)},
		&pps.ParallelismSpec{
			Constant: 1,
		},
		client.NewCrossInput(
			client.NewPFSInput(bPipeline, "/"),
			client.NewPFSInput(dRepo, "/"),
		),
		"",
		false,
	))

	commitInfo, err := c.InspectCommit(cPipeline, "master", "")
	require.NoError(t, err)

	commitInfos, err := c.WaitCommitSetAll(commitInfo.Commit.ID)
	require.NoError(t, err)
	require.Equal(t, 7, len(commitInfos))

	var buf bytes.Buffer
	require.NoError(t, c.GetFile(commitInfo.Commit, "bFile", &buf))
	require.Equal(t, "foo\n", buf.String())

	buf.Reset()
	require.NoError(t, c.GetFile(commitInfo.Commit, "dFile", &buf))
	require.Equal(t, "bar\n", buf.String())
}

// DAG:
//
// A
// |
// B  E
// | /
// C
// |
// D
func TestChainedPipelinesNoDelay(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())
	aRepo := tu.UniqueString("A")
	require.NoError(t, c.CreateRepo(aRepo))

	eRepo := tu.UniqueString("E")
	require.NoError(t, c.CreateRepo(eRepo))

	aCommit, err := c.StartCommit(aRepo, "master")
	require.NoError(t, err)
	require.NoError(t, c.PutFile(aCommit, "file", strings.NewReader("foo\n"), client.WithAppendPutFile()))
	require.NoError(t, c.FinishCommit(aRepo, "", "master"))

	eCommit, err := c.StartCommit(eRepo, "master")
	require.NoError(t, err)
	require.NoError(t, c.PutFile(eCommit, "file", strings.NewReader("bar\n"), client.WithAppendPutFile()))
	require.NoError(t, c.FinishCommit(eRepo, "master", ""))

	bPipeline := tu.UniqueString("B")
	require.NoError(t, c.CreatePipeline(
		bPipeline,
		"",
		[]string{"cp", path.Join("/pfs", aRepo, "file"), "/pfs/out/file"},
		nil,
		&pps.ParallelismSpec{
			Constant: 1,
		},
		client.NewPFSInput(aRepo, "/"),
		"",
		false,
	))

	cPipeline := tu.UniqueString("C")
	require.NoError(t, c.CreatePipeline(
		cPipeline,
		"",
		[]string{"sh"},
		[]string{fmt.Sprintf("cp /pfs/%s/file /pfs/out/bFile", bPipeline),
			fmt.Sprintf("cp /pfs/%s/file /pfs/out/eFile", eRepo)},
		&pps.ParallelismSpec{
			Constant: 1,
		},
		client.NewCrossInput(
			client.NewPFSInput(bPipeline, "/"),
			client.NewPFSInput(eRepo, "/"),
		),
		"",
		false,
	))

	dPipeline := tu.UniqueString("D")
	require.NoError(t, c.CreatePipeline(
		dPipeline,
		"",
		[]string{"sh"},
		[]string{fmt.Sprintf("cp /pfs/%s/bFile /pfs/out/bFile", cPipeline),
			fmt.Sprintf("cp /pfs/%s/eFile /pfs/out/eFile", cPipeline)},
		&pps.ParallelismSpec{
			Constant: 1,
		},
		client.NewPFSInput(cPipeline, "/"),
		"",
		false,
	))

	commitInfo, err := c.InspectCommit(dPipeline, "master", "")
	require.NoError(t, err)
	commitInfos, err := c.WaitCommitSetAll(commitInfo.Commit.ID)
	require.NoError(t, err)
	require.Equal(t, 9, len(commitInfos))

	eCommit2, err := c.StartCommit(eRepo, "master")
	require.NoError(t, err)
	require.NoError(t, c.PutFile(eCommit2, "file", strings.NewReader("bar\n"), client.WithAppendPutFile()))
	require.NoError(t, c.FinishCommit(eRepo, "master", ""))

	commitInfos, err = c.WaitCommitSetAll(eCommit2.ID)
	require.NoError(t, err)
	require.Equal(t, 10, len(commitInfos))

	// Get number of jobs triggered in pipeline D
	jobInfos, err := c.ListJob(dPipeline, nil, -1, true)
	require.NoError(t, err)
	require.Equal(t, 2, len(jobInfos))
}

func TestJobDeletion(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())
	// create repos
	dataRepo := tu.UniqueString("TestPipeline_data")
	require.NoError(t, c.CreateRepo(dataRepo))
	// create pipeline
	pipelineName := tu.UniqueString("pipeline")
	require.NoError(t, c.CreatePipeline(
		pipelineName,
		"",
		[]string{"cp", path.Join("/pfs", dataRepo, "file"), "/pfs/out/file"},
		nil,
		&pps.ParallelismSpec{
			Constant: 1,
		},
		client.NewPFSInput(dataRepo, "/"),
		"",
		false,
	))

	commit, err := c.StartCommit(dataRepo, "master")
	require.NoError(t, err)
	require.NoError(t, c.PutFile(commit, "file", strings.NewReader("foo\n"), client.WithAppendPutFile()))
	require.NoError(t, c.FinishCommit(dataRepo, commit.Branch.Name, commit.ID))

	_, err = c.WaitCommitSetAll(commit.ID)
	require.NoError(t, err)

	err = c.DeleteJob(pipelineName, commit.ID)
	require.NoError(t, err)
}

func TestStopJob(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())
	// create repos
	dataRepo := tu.UniqueString("TestStopJob")
	require.NoError(t, c.CreateRepo(dataRepo))
	// create pipeline
	pipelineName := tu.UniqueString("pipeline-stop-job")
	require.NoError(t, c.CreatePipeline(
		pipelineName,
		"",
		[]string{"sleep", "20"},
		nil,
		&pps.ParallelismSpec{
			Constant: 1,
		},
		client.NewPFSInput(dataRepo, "/"),
		"",
		false,
	))

	// Create two input commits to trigger two jobs.
	// We will stop the first job midway through, and assert that the
	// second job finishes.
	commit1, err := c.StartCommit(dataRepo, "master")
	require.NoError(t, err)
	require.NoError(t, c.PutFile(commit1, "file", strings.NewReader("foo\n"), client.WithAppendPutFile()))
	require.NoError(t, c.FinishCommit(dataRepo, commit1.Branch.Name, commit1.ID))

	commit2, err := c.StartCommit(dataRepo, "master")
	require.NoError(t, err)
	require.NoError(t, c.PutFile(commit2, "file", strings.NewReader("foo\n"), client.WithAppendPutFile()))
	require.NoError(t, c.FinishCommit(dataRepo, commit2.Branch.Name, commit2.ID))

	jobInfos, err := c.ListJob(pipelineName, nil, -1, true)
	require.NoError(t, err)
	require.Equal(t, 3, len(jobInfos))
	require.Equal(t, commit2.ID, jobInfos[0].Job.ID)
	require.Equal(t, commit1.ID, jobInfos[1].Job.ID)

	require.NoError(t, backoff.Retry(func() error {
		jobInfo, err := c.InspectJob(pipelineName, commit1.ID, false)
		require.NoError(t, err)
		if jobInfo.State != pps.JobState_JOB_RUNNING {
			return errors.Errorf("jobInfos[0] has the wrong state")
		}
		return nil
	}, backoff.NewTestingBackOff()))

	// Now stop the first job
	err = c.StopJob(pipelineName, commit1.ID)
	require.NoError(t, err)
	jobInfo, err := c.WaitJob(pipelineName, commit1.ID, false)
	require.NoError(t, err)
	require.Equal(t, pps.JobState_JOB_KILLED, jobInfo.State)

	// Check that the second job completes
	jobInfo, err = c.WaitJob(pipelineName, commit2.ID, false)
	require.NoError(t, err)
	require.Equal(t, pps.JobState_JOB_SUCCESS, jobInfo.State)
}

func TestGetLogs(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())
	iter := c.GetLogs("", "", nil, "", false, false, 0)
	for iter.Next() {
	}
	require.NoError(t, iter.Err())
	// create repos
	dataRepo := tu.UniqueString("data")
	require.NoError(t, c.CreateRepo(dataRepo))
	dataCommit := client.NewCommit(dataRepo, "master", "")
	// create pipeline
	pipelineName := tu.UniqueString("pipeline")
	_, err := c.PpsAPIClient.CreatePipeline(context.Background(),
		&pps.CreatePipelineRequest{
			Pipeline: client.NewPipeline(pipelineName),
			Transform: &pps.Transform{
				Cmd: []string{"sh"},
				Stdin: []string{
					fmt.Sprintf("cp /pfs/%s/file /pfs/out/file", dataRepo),
					"echo foo",
					"echo %s", // %s tests a formatting bug we had (#2729)
				},
			},
			Input: client.NewPFSInput(dataRepo, "/*"),
			ParallelismSpec: &pps.ParallelismSpec{
				Constant: 4,
			},
		})
	require.NoError(t, err)

	// Commit data to repo and flush commit
	commit, err := c.StartCommit(dataRepo, "master")
	require.NoError(t, err)
	require.NoError(t, c.PutFile(commit, "file", strings.NewReader("foo\n"), client.WithAppendPutFile()))
	require.NoError(t, c.FinishCommit(dataRepo, "master", ""))
	_, err = c.WaitJobSetAll(commit.ID, false)
	require.NoError(t, err)

	// Get logs from pipeline, using a pipeline that doesn't exist. There should
	// be an error
	iter = c.GetLogs("__DOES_NOT_EXIST__", "", nil, "", false, false, 0)
	require.False(t, iter.Next())
	require.YesError(t, iter.Err())
	require.Matches(t, "could not get", iter.Err().Error())

	// Get logs from pipeline, using a job that doesn't exist. There should
	// be an error
	iter = c.GetLogs(pipelineName, "__DOES_NOT_EXIST__", nil, "", false, false, 0)
	require.False(t, iter.Next())
	require.YesError(t, iter.Err())
	require.Matches(t, "could not get", iter.Err().Error())

	// This is put in a backoff because there's the possibility that pod was
	// evicted from k8s and is being re-initialized, in which case `GetLogs`
	// will appropriately fail. With the loki logging backend enabled the
	// eviction worry goes away, but is replaced with there being a window when
	// Loki hasn't scraped the logs yet so they don't show up.
	require.NoError(t, backoff.Retry(func() error {
		// Get logs from pipeline, using pipeline
		iter = c.GetLogs(pipelineName, "", nil, "", false, false, 0)
		var numLogs int
		for iter.Next() {
			if !iter.Message().User {
				continue
			}
			numLogs++
			require.True(t, iter.Message().Message != "")
			require.False(t, strings.Contains(iter.Message().Message, "MISSING"), iter.Message().Message)
		}
		if numLogs < 2 {
			return fmt.Errorf("didn't get enough log lines")
		}
		if err := iter.Err(); err != nil {
			return err
		}

		// Get logs from pipeline, using job
		// (1) Get job ID, from pipeline that just ran
		jobInfos, err := c.ListJob(pipelineName, nil, -1, true)
		if err != nil {
			return err
		}
		require.Equal(t, 2, len(jobInfos))
		// (2) Get logs using extracted job ID
		// wait for logs to be collected
		time.Sleep(10 * time.Second)
		iter = c.GetLogs(pipelineName, jobInfos[0].Job.ID, nil, "", false, false, 0)
		numLogs = 0
		for iter.Next() {
			numLogs++
			require.True(t, iter.Message().Message != "")
		}
		// Make sure that we've seen some logs
		if err = iter.Err(); err != nil {
			return err
		}
		require.True(t, numLogs > 0)

		// Get logs for datums but don't specify pipeline or job. These should error
		iter = c.GetLogs("", "", []string{"/foo"}, "", false, false, 0)
		require.False(t, iter.Next())
		require.YesError(t, iter.Err())

		dis, err := c.ListDatumAll(jobInfos[0].Job.Pipeline.Name, jobInfos[0].Job.ID)
		if err != nil {
			return err
		}
		require.True(t, len(dis) > 0)
		iter = c.GetLogs("", "", nil, dis[0].Datum.ID, false, false, 0)
		require.False(t, iter.Next())
		require.YesError(t, iter.Err())

		// Filter logs based on input (using file that exists). Get logs using file
		// path, hex hash, and base64 hash, and make sure you get the same log lines
		fileInfo, err := c.InspectFile(dataCommit, "/file")
		if err != nil {
			return err
		}

		pathLog := c.GetLogs(pipelineName, jobInfos[0].Job.ID, []string{"/file"}, "", false, false, 0)

		base64Hash := "kstrTGrFE58QWlxEpCRBt3aT8NJPNY0rso6XK7a4+wM="
		require.Equal(t, base64Hash, base64.StdEncoding.EncodeToString(fileInfo.Details.Hash))
		base64Log := c.GetLogs(pipelineName, jobInfos[0].Job.ID, []string{base64Hash}, "", false, false, 0)

		numLogs = 0
		for {
			havePathLog, haveBase64Log := pathLog.Next(), base64Log.Next()
			if havePathLog != haveBase64Log {
				return errors.Errorf("Unequal log lengths")
			}
			if !havePathLog {
				break
			}
			numLogs++
			if pathLog.Message().Message != base64Log.Message().Message {
				return errors.Errorf(
					"unequal logs, pathLogs: \"%s\" base64Log: \"%s\"",
					pathLog.Message().Message,
					base64Log.Message().Message)
			}
		}
		for _, logsiter := range []*client.LogsIter{pathLog, base64Log} {
			if logsiter.Err() != nil {
				return logsiter.Err()
			}
		}
		if numLogs == 0 {
			return errors.Errorf("no logs found")
		}

		// Filter logs based on input (using file that doesn't exist). There should
		// be no logs
		iter = c.GetLogs(pipelineName, jobInfos[0].Job.ID, []string{"__DOES_NOT_EXIST__"}, "", false, false, 0)
		require.False(t, iter.Next())
		if err = iter.Err(); err != nil {
			return err
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		iter = c.WithCtx(ctx).GetLogs(pipelineName, "", nil, "", false, false, 0)
		numLogs = 0
		for iter.Next() {
			numLogs++
			if numLogs == 8 {
				// Do another commit so there's logs to receive with follow
				_, err = c.StartCommit(dataRepo, "master")
				if err != nil {
					return err
				}
				if err := c.PutFile(dataCommit, "file", strings.NewReader("bar\n"), client.WithAppendPutFile()); err != nil {
					return err
				}
				if err = c.FinishCommit(dataRepo, "master", ""); err != nil {
					return err
				}
			}
			require.True(t, iter.Message().Message != "")
			if numLogs == 16 {
				break
			}
		}
		if err := iter.Err(); err != nil {
			return err
		}

		time.Sleep(time.Second * 30)

		numLogs = 0
		iter = c.WithCtx(ctx).GetLogs(pipelineName, "", nil, "", false, false, 15*time.Second)
		for iter.Next() {
			numLogs++
		}
		if err := iter.Err(); err != nil {
			return err
		}
		if numLogs != 0 {
			return errors.Errorf("shouldn't return logs due to since time")
		}
		return nil
	}, backoff.NewTestingBackOff()))
}

func TestManyLogs(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}
	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())
	// create repos
	dataRepo := tu.UniqueString("data")
	require.NoError(t, c.CreateRepo(dataRepo))
	dataCommit := client.NewCommit(dataRepo, "master", "")
	require.NoError(t, c.PutFile(dataCommit, "file", strings.NewReader("foo\n"), client.WithAppendPutFile()))
	// create pipeline
	numLogs := 10000
	pipelineName := tu.UniqueString("pipeline")
	_, err := c.PpsAPIClient.CreatePipeline(context.Background(),
		&pps.CreatePipelineRequest{
			Pipeline: client.NewPipeline(pipelineName),
			Transform: &pps.Transform{
				Cmd: []string{"sh"},
				Stdin: []string{
					"i=0",
					fmt.Sprintf("while [ \"$i\" -lt %d ]", numLogs),
					"do",
					"	echo $i",
					"	i=`expr $i + 1`",
					"done",
				},
			},
			Input: client.NewPFSInput(dataRepo, "/*"),
		})
	require.NoError(t, err)

	commitInfo, err := c.InspectCommit(pipelineName, "master", "")
	require.NoError(t, err)

	jis, err := c.WaitJobSetAll(commitInfo.Commit.ID, false)
	require.NoError(t, err)
	require.Equal(t, 1, len(jis))

	require.NoErrorWithinTRetry(t, 30*time.Second, func() error {
		iter := c.GetLogs(pipelineName, jis[0].Job.ID, nil, "", false, false, 0)
		logsReceived := 0
		for iter.Next() {
			if iter.Message().User {
				logsReceived++
			}
		}
		if iter.Err() != nil {
			return iter.Err()
		}
		if numLogs != logsReceived {
			return fmt.Errorf("received: %d log lines, expected: %d", logsReceived, numLogs)
		}
		return nil
	})
}

func TestLokiLogs(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}
	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())
	tu.ActivateEnterprise(t, c)
	// create repos
	dataRepo := tu.UniqueString("data")
	require.NoError(t, c.CreateRepo(dataRepo))
	dataCommit := client.NewCommit(dataRepo, "master", "")
	numFiles := 10
	for i := 0; i < numFiles; i++ {
		require.NoError(t, c.PutFile(dataCommit, fmt.Sprintf("file-%d", i), strings.NewReader("foo\n"), client.WithAppendPutFile()))
	}

	// create pipeline
	pipelineName := tu.UniqueString("pipeline")
	_, err := c.PpsAPIClient.CreatePipeline(context.Background(),
		&pps.CreatePipelineRequest{
			Pipeline: client.NewPipeline(pipelineName),
			Transform: &pps.Transform{
				Cmd: []string{"echo", "foo"},
			},
			Input: client.NewPFSInput(dataRepo, "/*"),
		})
	require.NoError(t, err)

	commitInfo, err := c.InspectCommit(pipelineName, "master", "")
	require.NoError(t, err)

	jis, err := c.WaitJobSetAll(commitInfo.Commit.ID, false)
	require.NoError(t, err)
	require.Equal(t, 1, len(jis))

	// Follow the logs the make sure we get enough foos
	require.NoErrorWithinT(t, time.Minute, func() error {
		iter := c.GetLogsLoki(pipelineName, jis[0].Job.ID, nil, "", false, true, 0)
		foundFoos := 0
		for iter.Next() {
			if strings.Contains(iter.Message().Message, "foo") {
				foundFoos++
				if foundFoos == numFiles {
					break
				}
			}
		}
		if iter.Err() != nil {
			return iter.Err()
		}
		return nil
	})

	time.Sleep(5 * time.Second)

	iter := c.GetLogsLoki(pipelineName, jis[0].Job.ID, nil, "", false, false, 0)
	foundFoos := 0
	for iter.Next() {
		if strings.Contains(iter.Message().Message, "foo") {
			foundFoos++
		}
	}
	require.NoError(t, iter.Err())
	require.Equal(t, numFiles, foundFoos, "didn't receive enough log lines containing foo")
}

func TestAllDatumsAreProcessed(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())

	dataRepo1 := tu.UniqueString("TestAllDatumsAreProcessed_data1")
	require.NoError(t, c.CreateRepo(dataRepo1))
	dataRepo2 := tu.UniqueString("TestAllDatumsAreProcessed_data2")
	require.NoError(t, c.CreateRepo(dataRepo2))

	commit1, err := c.StartCommit(dataRepo1, "master")
	require.NoError(t, err)
	require.NoError(t, c.PutFile(commit1, "file1", strings.NewReader("foo\n"), client.WithAppendPutFile()))
	require.NoError(t, c.PutFile(commit1, "file2", strings.NewReader("foo\n"), client.WithAppendPutFile()))
	require.NoError(t, c.FinishCommit(dataRepo1, "master", ""))

	commit2, err := c.StartCommit(dataRepo2, "master")
	require.NoError(t, err)
	require.NoError(t, c.PutFile(commit2, "file1", strings.NewReader("foo\n"), client.WithAppendPutFile()))
	require.NoError(t, c.PutFile(commit2, "file2", strings.NewReader("foo\n"), client.WithAppendPutFile()))
	require.NoError(t, c.FinishCommit(dataRepo2, "master", ""))

	pipeline := tu.UniqueString("TestAllDatumsAreProcessed_pipelines")
	require.NoError(t, c.CreatePipeline(
		pipeline,
		"",
		[]string{"bash"},
		[]string{
			fmt.Sprintf("cat /pfs/%s/* /pfs/%s/* > /pfs/out/file", dataRepo1, dataRepo2),
		},
		nil,
		client.NewCrossInput(
			client.NewPFSInput(dataRepo1, "/*"),
			client.NewPFSInput(dataRepo2, "/*"),
		),
		"",
		false,
	))

	commitInfo, err := c.InspectCommit(pipeline, "master", "")
	require.NoError(t, err)
	commitInfos, err := c.WaitCommitSetAll(commitInfo.Commit.ID)
	require.NoError(t, err)
	require.Equal(t, 5, len(commitInfos))

	var buf bytes.Buffer
	require.NoError(t, c.GetFile(commitInfo.Commit, "file", &buf))
	// should be 8 because each file gets copied twice due to cross product
	require.Equal(t, strings.Repeat("foo\n", 8), buf.String())
}

func TestDatumStatusRestart(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())

	dataRepo := tu.UniqueString("TestDatumDedup_data")
	require.NoError(t, c.CreateRepo(dataRepo))

	pipeline := tu.UniqueString("pipeline")
	// This pipeline sleeps for 20 secs per datum
	require.NoError(t, c.CreatePipeline(
		pipeline,
		"",
		[]string{"bash"},
		[]string{
			"sleep 20",
		},
		nil,
		client.NewPFSInput(dataRepo, "/*"),
		"",
		false,
	))

	commit1, err := c.StartCommit(dataRepo, "master")
	require.NoError(t, err)
	require.NoError(t, c.PutFile(commit1, "file", strings.NewReader("foo"), client.WithAppendPutFile()))
	require.NoError(t, c.FinishCommit(dataRepo, commit1.Branch.Name, commit1.ID))

	// get the job status
	jobs, err := c.ListJob(pipeline, nil, -1, true)
	require.NoError(t, err)
	require.Equal(t, 2, len(jobs))

	var datumStarted time.Time
	// checkStatus waits for 'pipeline' to start and makes sure that each time
	// it's called, the datum being processes was started at a new and later time
	// (than the last time checkStatus was called)
	checkStatus := func() {
		require.NoError(t, backoff.Retry(func() error {
			jobInfo, err := c.InspectJob(pipeline, commit1.ID, true)
			require.NoError(t, err)
			if len(jobInfo.Details.WorkerStatus) == 0 {
				return errors.Errorf("no worker statuses")
			}
			workerStatus := jobInfo.Details.WorkerStatus[0]
			if workerStatus.JobID == jobInfo.Job.ID {
				if workerStatus.DatumStatus == nil {
					return errors.Errorf("no datum status")
				}
				// The first time this function is called, datumStarted is zero
				// so `Before` is true for any non-zero time.
				_datumStarted, err := types.TimestampFromProto(workerStatus.DatumStatus.Started)
				require.NoError(t, err)
				require.True(t, datumStarted.Before(_datumStarted))
				datumStarted = _datumStarted
				return nil
			}
			return errors.Errorf("worker status from wrong job")
		}, backoff.RetryEvery(time.Second).For(30*time.Second)))
	}
	checkStatus()
	require.NoError(t, c.RestartDatum(pipeline, commit1.ID, []string{"/file"}))
	checkStatus()

	commitInfos, err := c.WaitCommitSetAll(commit1.ID)
	require.NoError(t, err)
	require.Equal(t, 4, len(commitInfos))
}

// TestSystemResourceRequest doesn't create any jobs or pipelines, it
// just makes sure that when pachyderm is deployed, we give pachd,
// and etcd default resource requests. This prevents them from overloading
// nodes and getting evicted, which can slow down or break a cluster.
func TestSystemResourceRequests(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}
	kubeClient := tu.GetKubeClient(t)

	// Expected resource requests for pachyderm system pods:
	defaultLocalMem := map[string]string{
		"pachd": "512M",
		"etcd":  "512M",
	}
	defaultLocalCPU := map[string]string{
		"pachd": "250m",
		"etcd":  "250m",
	}
	defaultCloudMem := map[string]string{
		"pachd": "3G",
		"etcd":  "2G",
	}
	defaultCloudCPU := map[string]string{
		"pachd": "1",
		"etcd":  "1",
	}
	// Get Pod info for 'app' from k8s
	var c v1.Container
	for _, app := range []string{"pachd", "etcd"} {
		err := backoff.Retry(func() error {
			podList, err := kubeClient.CoreV1().Pods(v1.NamespaceDefault).List(
				metav1.ListOptions{
					LabelSelector: metav1.FormatLabelSelector(metav1.SetAsLabelSelector(
						map[string]string{"app": app, "suite": "pachyderm"},
					)),
				})
			if err != nil {
				return err
			}
			if len(podList.Items) < 1 {
				return errors.Errorf("could not find pod for %s", app) // retry
			}
			c = podList.Items[0].Spec.Containers[0]
			return nil
		}, backoff.NewTestingBackOff())
		require.NoError(t, err)

		// Make sure the pod's container has resource requests
		cpu, ok := c.Resources.Requests[v1.ResourceCPU]
		require.True(t, ok, "could not get CPU request for "+app)
		require.True(t, cpu.String() == defaultLocalCPU[app] ||
			cpu.String() == defaultCloudCPU[app])
		mem, ok := c.Resources.Requests[v1.ResourceMemory]
		require.True(t, ok, "could not get memory request for "+app)
		require.True(t, mem.String() == defaultLocalMem[app] ||
			mem.String() == defaultCloudMem[app])
	}
}

// TestPipelineResourceRequest creates a pipeline with a resource request, and
// makes sure that's passed to k8s (by inspecting the pipeline's pods)
func TestPipelineResourceRequest(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())
	// create repos
	dataRepo := tu.UniqueString("TestPipelineResourceRequest")
	pipelineName := tu.UniqueString("TestPipelineResourceRequest_Pipeline")
	require.NoError(t, c.CreateRepo(dataRepo))
	// Resources are not yet in client.CreatePipeline() (we may add them later)
	_, err := c.PpsAPIClient.CreatePipeline(
		context.Background(),
		&pps.CreatePipelineRequest{
			Pipeline: client.NewPipeline(pipelineName),
			Transform: &pps.Transform{
				Cmd: []string{"cp", path.Join("/pfs", dataRepo, "file"), "/pfs/out/file"},
			},
			ParallelismSpec: &pps.ParallelismSpec{
				Constant: 1,
			},
			ResourceRequests: &pps.ResourceSpec{
				Memory: "100M",
				Cpu:    0.5,
				Disk:   "10M",
			},
			Input: &pps.Input{
				Pfs: &pps.PFSInput{
					Repo:   dataRepo,
					Branch: "master",
					Glob:   "/*",
				},
			},
		})
	require.NoError(t, err)

	// Get info about the pipeline pods from k8s & check for resources
	pipelineInfo, err := c.InspectPipeline(pipelineName, false)
	require.NoError(t, err)

	var container v1.Container
	rcName := ppsutil.PipelineRcName(pipelineInfo.Pipeline.Name, pipelineInfo.Version)
	kubeClient := tu.GetKubeClient(t)
	require.NoError(t, backoff.Retry(func() error {
		podList, err := kubeClient.CoreV1().Pods(v1.NamespaceDefault).List(
			metav1.ListOptions{
				LabelSelector: metav1.FormatLabelSelector(metav1.SetAsLabelSelector(
					map[string]string{"app": rcName},
				)),
			})
		if err != nil {
			return err // retry
		}
		if len(podList.Items) != 1 || len(podList.Items[0].Spec.Containers) == 0 {
			return errors.Errorf("could not find single container for pipeline %s", pipelineInfo.Pipeline.Name)
		}
		container = podList.Items[0].Spec.Containers[0]
		return nil // no more retries
	}, backoff.NewTestingBackOff()))
	// Make sure a CPU and Memory request are both set
	cpu, ok := container.Resources.Requests[v1.ResourceCPU]
	require.True(t, ok)
	require.Equal(t, "500m", cpu.String())
	mem, ok := container.Resources.Requests[v1.ResourceMemory]
	require.True(t, ok)
	require.Equal(t, "100M", mem.String())
	disk, ok := container.Resources.Requests[v1.ResourceEphemeralStorage]
	require.True(t, ok)
	require.Equal(t, "10M", disk.String())
}

func TestPipelineResourceLimit(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())
	// create repos
	dataRepo := tu.UniqueString("TestPipelineResourceLimit")
	pipelineName := tu.UniqueString("TestPipelineResourceLimit_Pipeline")
	require.NoError(t, c.CreateRepo(dataRepo))
	// Resources are not yet in client.CreatePipeline() (we may add them later)
	_, err := c.PpsAPIClient.CreatePipeline(
		context.Background(),
		&pps.CreatePipelineRequest{
			Pipeline: client.NewPipeline(pipelineName),
			Transform: &pps.Transform{
				Cmd: []string{"cp", path.Join("/pfs", dataRepo, "file"), "/pfs/out/file"},
			},
			ParallelismSpec: &pps.ParallelismSpec{
				Constant: 1,
			},
			ResourceLimits: &pps.ResourceSpec{
				Memory: "100M",
				Cpu:    0.5,
			},
			Input: &pps.Input{
				Pfs: &pps.PFSInput{
					Repo:   dataRepo,
					Branch: "master",
					Glob:   "/*",
				},
			},
		})
	require.NoError(t, err)

	// Get info about the pipeline pods from k8s & check for resources
	pipelineInfo, err := c.InspectPipeline(pipelineName, false)
	require.NoError(t, err)

	var container v1.Container
	rcName := ppsutil.PipelineRcName(pipelineInfo.Pipeline.Name, pipelineInfo.Version)
	kubeClient := tu.GetKubeClient(t)
	err = backoff.Retry(func() error {
		podList, err := kubeClient.CoreV1().Pods(v1.NamespaceDefault).List(metav1.ListOptions{
			LabelSelector: metav1.FormatLabelSelector(metav1.SetAsLabelSelector(
				map[string]string{"app": rcName, "suite": "pachyderm"},
			)),
		})
		if err != nil {
			return err // retry
		}
		if len(podList.Items) != 1 || len(podList.Items[0].Spec.Containers) == 0 {
			return errors.Errorf("could not find single container for pipeline %s", pipelineInfo.Pipeline.Name)
		}
		container = podList.Items[0].Spec.Containers[0]
		return nil // no more retries
	}, backoff.NewTestingBackOff())
	require.NoError(t, err)
	// Make sure a CPU and Memory request are both set
	cpu, ok := container.Resources.Limits[v1.ResourceCPU]
	require.True(t, ok)
	require.Equal(t, "500m", cpu.String())
	mem, ok := container.Resources.Limits[v1.ResourceMemory]
	require.True(t, ok)
	require.Equal(t, "100M", mem.String())
}

func TestPipelineResourceLimitDefaults(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())
	// create repos
	dataRepo := tu.UniqueString("TestPipelineResourceLimit")
	pipelineName := tu.UniqueString("TestPipelineResourceLimit_Pipeline")
	require.NoError(t, c.CreateRepo(dataRepo))
	// Resources are not yet in client.CreatePipeline() (we may add them later)
	_, err := c.PpsAPIClient.CreatePipeline(
		context.Background(),
		&pps.CreatePipelineRequest{
			Pipeline: client.NewPipeline(pipelineName),
			Transform: &pps.Transform{
				Cmd: []string{"cp", path.Join("/pfs", dataRepo, "file"), "/pfs/out/file"},
			},
			ParallelismSpec: &pps.ParallelismSpec{
				Constant: 1,
			},
			Input: &pps.Input{
				Pfs: &pps.PFSInput{
					Repo:   dataRepo,
					Branch: "master",
					Glob:   "/*",
				},
			},
		})
	require.NoError(t, err)

	// Get info about the pipeline pods from k8s & check for resources
	pipelineInfo, err := c.InspectPipeline(pipelineName, false)
	require.NoError(t, err)

	var container v1.Container
	rcName := ppsutil.PipelineRcName(pipelineInfo.Pipeline.Name, pipelineInfo.Version)
	kubeClient := tu.GetKubeClient(t)
	err = backoff.Retry(func() error {
		podList, err := kubeClient.CoreV1().Pods(v1.NamespaceDefault).List(metav1.ListOptions{
			LabelSelector: metav1.FormatLabelSelector(metav1.SetAsLabelSelector(
				map[string]string{"app": rcName, "suite": "pachyderm"},
			)),
		})
		if err != nil {
			return err // retry
		}
		if len(podList.Items) != 1 || len(podList.Items[0].Spec.Containers) == 0 {
			return errors.Errorf("could not find single container for pipeline %s", pipelineInfo.Pipeline.Name)
		}
		container = podList.Items[0].Spec.Containers[0]
		return nil // no more retries
	}, backoff.NewTestingBackOff())
	require.NoError(t, err)
	require.Nil(t, container.Resources.Limits)
}

func TestPipelinePartialResourceRequest(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())
	// create repos
	dataRepo := tu.UniqueString("TestPipelinePartialResourceRequest")
	pipelineName := tu.UniqueString("pipeline")
	require.NoError(t, c.CreateRepo(dataRepo))
	// Resources are not yet in client.CreatePipeline() (we may add them later)
	_, err := c.PpsAPIClient.CreatePipeline(
		context.Background(),
		&pps.CreatePipelineRequest{
			Pipeline: client.NewPipeline(fmt.Sprintf("%s-%d", pipelineName, 0)),
			Transform: &pps.Transform{
				Cmd: []string{"true"},
			},
			ResourceRequests: &pps.ResourceSpec{
				Cpu:    0.25,
				Memory: "100M",
			},
			Input: &pps.Input{
				Pfs: &pps.PFSInput{
					Repo:   dataRepo,
					Branch: "master",
					Glob:   "/*",
				},
			},
		})
	require.NoError(t, err)
	_, err = c.PpsAPIClient.CreatePipeline(
		context.Background(),
		&pps.CreatePipelineRequest{
			Pipeline: client.NewPipeline(fmt.Sprintf("%s-%d", pipelineName, 1)),
			Transform: &pps.Transform{
				Cmd: []string{"true"},
			},
			ResourceRequests: &pps.ResourceSpec{
				Memory: "100M",
			},
			Input: &pps.Input{
				Pfs: &pps.PFSInput{
					Repo:   dataRepo,
					Branch: "master",
					Glob:   "/*",
				},
			},
		})
	require.NoError(t, err)
	_, err = c.PpsAPIClient.CreatePipeline(
		context.Background(),
		&pps.CreatePipelineRequest{
			Pipeline: client.NewPipeline(fmt.Sprintf("%s-%d", pipelineName, 2)),
			Transform: &pps.Transform{
				Cmd: []string{"true"},
			},
			ResourceRequests: &pps.ResourceSpec{},
			Input: &pps.Input{
				Pfs: &pps.PFSInput{
					Repo:   dataRepo,
					Branch: "master",
					Glob:   "/*",
				},
			},
		})
	require.NoError(t, err)
	require.NoError(t, backoff.Retry(func() error {
		for i := 0; i < 3; i++ {
			pipelineInfo, err := c.InspectPipeline(fmt.Sprintf("%s-%d", pipelineName, i), false)
			require.NoError(t, err)
			if pipelineInfo.State != pps.PipelineState_PIPELINE_RUNNING {
				return errors.Errorf("pipeline not in running state")
			}
		}
		return nil
	}, backoff.NewTestingBackOff()))
}

func TestPipelineCrashing(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())
	// create repos
	dataRepo := tu.UniqueString("TestPipelineCrashing_data")
	pipelineName := tu.UniqueString("TestPipelineCrashing_pipeline")
	require.NoError(t, c.CreateRepo(dataRepo))
	_, err := c.PpsAPIClient.CreatePipeline(
		context.Background(),
		&pps.CreatePipelineRequest{
			Pipeline: client.NewPipeline(pipelineName),
			Transform: &pps.Transform{
				Cmd: []string{"cp", path.Join("/pfs", dataRepo, "file"), "/pfs/out/file"},
			},
			ParallelismSpec: &pps.ParallelismSpec{
				Constant: 1,
			},
			ResourceLimits: &pps.ResourceSpec{
				Gpu: &pps.GPUSpec{
					Type:   "nvidia.com/gpu",
					Number: 1,
				},
			},
			Input: &pps.Input{
				Pfs: &pps.PFSInput{
					Repo:   dataRepo,
					Branch: "master",
					Glob:   "/*",
				},
			},
		})
	require.NoError(t, err)

	require.NoError(t, backoff.Retry(func() error {
		pi, err := c.InspectPipeline(pipelineName, false)
		require.NoError(t, err)
		if pi.State != pps.PipelineState_PIPELINE_CRASHING {
			return errors.Errorf("pipeline in wrong state: %s", pi.State.String())
		}
		require.True(t, pi.Reason != "")
		return nil
	}, backoff.NewTestingBackOff()))
}

func TestPodOpts(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())
	// create repos
	dataRepo := tu.UniqueString("TestPodSpecOpts_data")
	require.NoError(t, c.CreateRepo(dataRepo))
	t.Run("Validation", func(t *testing.T) {
		pipelineName := tu.UniqueString("TestPodSpecOpts")
		_, err := c.PpsAPIClient.CreatePipeline(
			context.Background(),
			&pps.CreatePipelineRequest{
				Pipeline: client.NewPipeline(pipelineName),
				Transform: &pps.Transform{
					Cmd: []string{"cp", path.Join("/pfs", dataRepo, "file"), "/pfs/out/file"},
				},
				Input: &pps.Input{
					Pfs: &pps.PFSInput{
						Repo:   dataRepo,
						Branch: "master",
						Glob:   "/*",
					},
				},
				PodSpec: "not-json",
			})
		require.YesError(t, err)
		_, err = c.PpsAPIClient.CreatePipeline(
			context.Background(),
			&pps.CreatePipelineRequest{
				Pipeline: client.NewPipeline(pipelineName),
				Transform: &pps.Transform{
					Cmd: []string{"cp", path.Join("/pfs", dataRepo, "file"), "/pfs/out/file"},
				},
				Input: &pps.Input{
					Pfs: &pps.PFSInput{
						Repo:   dataRepo,
						Branch: "master",
						Glob:   "/*",
					},
				},
				PodPatch: "also-not-json",
			})
		require.YesError(t, err)
	})
	t.Run("Spec", func(t *testing.T) {
		pipelineName := tu.UniqueString("TestPodSpecOpts")
		_, err := c.PpsAPIClient.CreatePipeline(
			context.Background(),
			&pps.CreatePipelineRequest{
				Pipeline: client.NewPipeline(pipelineName),
				Transform: &pps.Transform{
					Cmd: []string{"cp", path.Join("/pfs", dataRepo, "file"), "/pfs/out/file"},
				},
				ParallelismSpec: &pps.ParallelismSpec{
					Constant: 1,
				},
				Input: &pps.Input{
					Pfs: &pps.PFSInput{
						Repo:   dataRepo,
						Branch: "master",
						Glob:   "/*",
					},
				},
				SchedulingSpec: &pps.SchedulingSpec{
					// This NodeSelector will cause the worker pod to fail to
					// schedule, but the test can still pass because we just check
					// for values on the pod, it doesn't need to actually come up.
					NodeSelector: map[string]string{
						"foo": "bar",
					},
				},
				PodSpec: `{
				"hostname": "hostname"
			}`,
			})
		require.NoError(t, err)

		// Get info about the pipeline pods from k8s & check for resources
		pipelineInfo, err := c.InspectPipeline(pipelineName, false)
		require.NoError(t, err)

		var pod v1.Pod
		rcName := ppsutil.PipelineRcName(pipelineInfo.Pipeline.Name, pipelineInfo.Version)
		kubeClient := tu.GetKubeClient(t)
		err = backoff.Retry(func() error {
			podList, err := kubeClient.CoreV1().Pods(v1.NamespaceDefault).List(metav1.ListOptions{
				LabelSelector: metav1.FormatLabelSelector(metav1.SetAsLabelSelector(
					map[string]string{"app": rcName, "suite": "pachyderm"},
				)),
			})
			if err != nil {
				return err // retry
			}
			if len(podList.Items) != 1 || len(podList.Items[0].Spec.Containers) == 0 {
				return errors.Errorf("could not find single container for pipeline %s", pipelineInfo.Pipeline.Name)
			}
			pod = podList.Items[0]
			return nil // no more retries
		}, backoff.NewTestingBackOff())
		require.NoError(t, err)
		// Make sure a CPU and Memory request are both set
		require.Equal(t, "bar", pod.Spec.NodeSelector["foo"])
		require.Equal(t, "hostname", pod.Spec.Hostname)
	})
	t.Run("Patch", func(t *testing.T) {
		pipelineName := tu.UniqueString("TestPodSpecOpts")
		_, err := c.PpsAPIClient.CreatePipeline(
			context.Background(),
			&pps.CreatePipelineRequest{
				Pipeline: client.NewPipeline(pipelineName),
				Transform: &pps.Transform{
					Cmd: []string{"cp", path.Join("/pfs", dataRepo, "file"), "/pfs/out/file"},
				},
				ParallelismSpec: &pps.ParallelismSpec{
					Constant: 1,
				},
				Input: &pps.Input{
					Pfs: &pps.PFSInput{
						Repo:   dataRepo,
						Branch: "master",
						Glob:   "/*",
					},
				},
				SchedulingSpec: &pps.SchedulingSpec{
					// This NodeSelector will cause the worker pod to fail to
					// schedule, but the test can still pass because we just check
					// for values on the pod, it doesn't need to actually come up.
					NodeSelector: map[string]string{
						"foo": "bar",
					},
				},
				PodPatch: `[
					{ "op": "add", "path": "/hostname", "value": "hostname" }
			]`,
			})
		require.NoError(t, err)

		// Get info about the pipeline pods from k8s & check for resources
		pipelineInfo, err := c.InspectPipeline(pipelineName, false)
		require.NoError(t, err)

		var pod v1.Pod
		rcName := ppsutil.PipelineRcName(pipelineInfo.Pipeline.Name, pipelineInfo.Version)
		kubeClient := tu.GetKubeClient(t)
		err = backoff.Retry(func() error {
			podList, err := kubeClient.CoreV1().Pods(v1.NamespaceDefault).List(metav1.ListOptions{
				LabelSelector: metav1.FormatLabelSelector(metav1.SetAsLabelSelector(
					map[string]string{"app": rcName, "suite": "pachyderm"},
				)),
			})
			if err != nil {
				return err // retry
			}
			if len(podList.Items) != 1 || len(podList.Items[0].Spec.Containers) == 0 {
				return errors.Errorf("could not find single container for pipeline %s", pipelineInfo.Pipeline.Name)
			}
			pod = podList.Items[0]
			return nil // no more retries
		}, backoff.NewTestingBackOff())
		require.NoError(t, err)
		// Make sure a CPU and Memory request are both set
		require.Equal(t, "bar", pod.Spec.NodeSelector["foo"])
		require.Equal(t, "hostname", pod.Spec.Hostname)
	})
}

func TestPipelineLargeOutput(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())

	dataRepo := tu.UniqueString("TestPipelineInputDataModification_data")
	require.NoError(t, c.CreateRepo(dataRepo))

	pipeline := tu.UniqueString("pipeline")
	require.NoError(t, c.CreatePipeline(
		pipeline,
		"",
		[]string{"bash"},
		[]string{
			"for i in `seq 1 100`; do touch /pfs/out/$RANDOM; done",
		},
		&pps.ParallelismSpec{
			Constant: 4,
		},
		client.NewPFSInput(dataRepo, "/*"),
		"",
		false,
	))

	numFiles := 100
	commit1, err := c.StartCommit(dataRepo, "master")
	require.NoError(t, err)
	for i := 0; i < numFiles; i++ {
		require.NoError(t, c.PutFile(commit1, fmt.Sprintf("file-%d", i), strings.NewReader(""), client.WithAppendPutFile()))
	}
	require.NoError(t, c.FinishCommit(dataRepo, commit1.Branch.Name, commit1.ID))

	commitInfos, err := c.WaitCommitSetAll(commit1.ID)
	require.NoError(t, err)
	require.Equal(t, 4, len(commitInfos))
}

func TestJoinInput(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())

	var repos []string
	for i := 0; i < 2; i++ {
		repos = append(repos, tu.UniqueString(fmt.Sprintf("TestJoinInput%v", i)))
		require.NoError(t, c.CreateRepo(repos[i]))
	}

	numFiles := 16
	for r, repo := range repos {
		commit, err := c.StartCommit(repo, "master")
		require.NoError(t, err)
		for i := 0; i < numFiles; i++ {
			require.NoError(t, c.PutFile(commit, fmt.Sprintf("file-%v.%4b", r, i), strings.NewReader(fmt.Sprintf("%d\n", i)), client.WithAppendPutFile()))
		}
		require.NoError(t, c.FinishCommit(repo, "master", ""))
	}

	pipeline := tu.UniqueString("join-pipeline")
	require.NoError(t, c.CreatePipeline(
		pipeline,
		"",
		[]string{"bash"},
		[]string{
			fmt.Sprintf("touch /pfs/out/$(echo $(ls -r /pfs/%s/)$(ls -r /pfs/%s/))", repos[0], repos[1]),
		},
		&pps.ParallelismSpec{
			Constant: 1,
		},
		client.NewJoinInput(
			client.NewPFSInputOpts("", repos[0], "", "/file-?.(11*)", "$1", "", false, false, nil),
			client.NewPFSInputOpts("", repos[1], "", "/file-?.(*0)", "$1", "", false, false, nil),
		),
		"",
		false,
	))

	commitInfo, err := c.WaitCommit(pipeline, "master", "")
	require.NoError(t, err)
	fileInfos, err := c.ListFileAll(commitInfo.Commit, "")
	require.NoError(t, err)
	require.Equal(t, 2, len(fileInfos))
	expectedNames := []string{"/file-0.1100file-1.1100", "/file-0.1110file-1.1110"}
	for i, fi := range fileInfos {
		// 1 byte per repo
		require.Equal(t, expectedNames[i], fi.File.Path)
	}
}

func TestGroupInput(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())

	t.Run("Basic", func(t *testing.T) {
		repo := tu.UniqueString("TestGroupInput")
		require.NoError(t, c.CreateRepo(repo))
		commit := client.NewCommit(repo, "master", "")
		numFiles := 16
		for i := 0; i < numFiles; i++ {
			require.NoError(t, c.PutFile(commit, fmt.Sprintf("file.%4b", i), strings.NewReader(fmt.Sprintf("%d\n", i)), client.WithAppendPutFile()))
		}

		pipeline := "group-pipeline"
		require.NoError(t, c.CreatePipeline(
			pipeline,
			"",
			[]string{"bash"},
			[]string{},
			&pps.ParallelismSpec{
				Constant: 1,
			},
			client.NewGroupInput(
				client.NewPFSInputOpts("", repo, "", "/file.(?)(?)(?)(?)", "", "$3", false, false, nil),
			),
			"",
			false,
		))

		commitInfo, err := c.InspectCommit(pipeline, "master", "")
		require.NoError(t, err)

		jobs, err := c.WaitJobSetAll(commitInfo.Commit.ID, false)
		require.NoError(t, err)
		require.Equal(t, 1, len(jobs))

		// We're grouping by the third digit in the filename
		// for 0 and 1, this is just a space
		// then we should see the 8 files with a one there, and the 6 files with a zero there
		expected := [][]string{
			{"/file.   0",
				"/file.   1"},

			{"/file.  10",
				"/file.  11",
				"/file. 110",
				"/file. 111",
				"/file.1010",
				"/file.1011",
				"/file.1110",
				"/file.1111"},

			{"/file. 100",
				"/file. 101",
				"/file.1000",
				"/file.1001",
				"/file.1100",
				"/file.1101"}}
		actual := make([][]string, 0, 3)
		dis, err := c.ListDatumAll(jobs[0].Job.Pipeline.Name, jobs[0].Job.ID)
		require.NoError(t, err)
		sort.Slice(dis, func(i, j int) bool {
			return dis[i].Data[0].File.Path < dis[j].Data[0].File.Path
		})
		for _, di := range dis {
			sort.Slice(di.Data, func(i, j int) bool { return di.Data[i].File.Path < di.Data[j].File.Path })
			datumFiles := make([]string, 0)
			for _, fi := range di.Data {
				datumFiles = append(datumFiles, fi.File.Path)
			}
			actual = append(actual, datumFiles)
		}
		require.Equal(t, expected, actual)
	})

	t.Run("MultiInput", func(t *testing.T) {
		var repos []string
		for i := 0; i < 2; i++ {
			repos = append(repos, tu.UniqueString(fmt.Sprintf("TestGroupInput%v", i)))
			require.NoError(t, c.CreateRepo(repos[i]))
		}

		numFiles := 16
		for r, repo := range repos {
			for i := 0; i < numFiles; i++ {
				require.NoError(t, c.PutFile(client.NewCommit(repo, "master", ""), fmt.Sprintf("file-%v.%4b", r, i), strings.NewReader(fmt.Sprintf("%d\n", i)), client.WithAppendPutFile()))
			}
		}

		pipeline := "group-pipeline-multi-input"
		require.NoError(t, c.CreatePipeline(
			pipeline,
			"",
			[]string{"bash"},
			[]string{},
			&pps.ParallelismSpec{
				Constant: 1,
			},
			client.NewGroupInput(
				client.NewPFSInputOpts("", repos[0], "", "/file-?.(?)(?)(?)(?)", "", "$3", false, false, nil),
				client.NewPFSInputOpts("", repos[1], "", "/file-?.(?)(?)(?)(?)", "", "$2", false, false, nil),
			),
			"",
			false,
		))

		commitInfo, err := c.InspectCommit(pipeline, "master", "")
		require.NoError(t, err)

		jobs, err := c.WaitJobSetAll(commitInfo.Commit.ID, false)
		require.NoError(t, err)
		require.Equal(t, 1, len(jobs))

		// this time, we are grouping by the third digit in the 0 repo, and the second digit in the 1 repo
		// so the first group should have all the things from the 0 repo with a space in the third digit
		// and all the things from the 1 repo with a space in the second digit
		//
		// similarly for the second and third groups
		expected := [][]string{
			{"/file-0.   0",
				"/file-0.   1",
				"/file-1.   0",
				"/file-1.   1",
				"/file-1.  10",
				"/file-1.  11"},

			{"/file-0.  10",
				"/file-0.  11",
				"/file-0. 110",
				"/file-0. 111",
				"/file-0.1010",
				"/file-0.1011",
				"/file-0.1110",
				"/file-0.1111",
				"/file-1. 100",
				"/file-1. 101",
				"/file-1. 110",
				"/file-1. 111",
				"/file-1.1100",
				"/file-1.1101",
				"/file-1.1110",
				"/file-1.1111"},

			{"/file-0. 100",
				"/file-0. 101",
				"/file-0.1000",
				"/file-0.1001",
				"/file-0.1100",
				"/file-0.1101",
				"/file-1.1000",
				"/file-1.1001",
				"/file-1.1010",
				"/file-1.1011"}}
		actual := make([][]string, 0, 3)
		dis, err := c.ListDatumAll(jobs[0].Job.Pipeline.Name, jobs[0].Job.ID)
		require.NoError(t, err)
		sort.Slice(dis, func(i, j int) bool {
			return dis[i].Data[0].File.Path < dis[j].Data[0].File.Path
		})
		for _, di := range dis {
			sort.Slice(di.Data, func(i, j int) bool { return di.Data[i].File.Path < di.Data[j].File.Path })
			datumFiles := make([]string, 0)
			for _, fi := range di.Data {
				datumFiles = append(datumFiles, fi.File.Path)
			}
			actual = append(actual, datumFiles)
		}
		require.Equal(t, expected, actual)
	})

	t.Run("GroupJoinCombo", func(t *testing.T) {
		var repos []string
		for i := 0; i < 2; i++ {
			repos = append(repos, tu.UniqueString(fmt.Sprintf("TestGroupInput%v", i)))
			require.NoError(t, c.CreateRepo(repos[i]))
		}

		numFiles := 16
		for r, repo := range repos {
			for i := 0; i < numFiles; i++ {
				require.NoError(t, c.PutFile(client.NewCommit(repo, "master", ""), fmt.Sprintf("file-%v.%4b", r, i), strings.NewReader(fmt.Sprintf("%d\n", i)), client.WithAppendPutFile()))
			}
		}

		pipeline := "group-join-pipeline"
		require.NoError(t, c.CreatePipeline(
			pipeline,
			"",
			[]string{"bash"},
			[]string{},
			&pps.ParallelismSpec{
				Constant: 1,
			},
			client.NewGroupInput(
				client.NewJoinInput(
					client.NewPFSInputOpts("", repos[0], "", "/file-?.(?)(?)(?)(?)", "$1$2$3$4", "$3", false, false, nil),
					client.NewPFSInputOpts("", repos[1], "", "/file-?.(?)(?)(?)(?)", "$4$3$2$1", "$2", false, false, nil),
				),
			),
			"",
			false,
		))

		commitInfo, err := c.InspectCommit(pipeline, "master", "")
		require.NoError(t, err)

		jobs, err := c.WaitJobSetAll(commitInfo.Commit.ID, false)
		require.NoError(t, err)
		require.Equal(t, 1, len(jobs))

		// here, we're first doing a join to get pairs of files (one from each repo) that have the reverse numbers
		// we should see four pairs
		// then, we're grouping the files in these pairs by the third digit/second digit as before
		// this should regroup things into two groups of four
		expected := [][]string{
			{"/file-0.1001",
				"/file-0.1101",
				"/file-1.1001",
				"/file-1.1011"},
			{"/file-0.1011",
				"/file-0.1111",
				"/file-1.1101",
				"/file-1.1111"}}
		actual := make([][]string, 0, 2)
		dis, err := c.ListDatumAll(jobs[0].Job.Pipeline.Name, jobs[0].Job.ID)
		require.NoError(t, err)
		sort.Slice(dis, func(i, j int) bool {
			return dis[i].Data[0].File.Path < dis[j].Data[0].File.Path
		})
		for _, di := range dis {
			sort.Slice(di.Data, func(i, j int) bool { return di.Data[i].File.Path < di.Data[j].File.Path })
			datumFiles := make([]string, 0)
			for _, fi := range di.Data {
				datumFiles = append(datumFiles, fi.File.Path)
			}
			actual = append(actual, datumFiles)
		}
		require.Equal(t, expected, actual)
	})
	t.Run("Symlink", func(t *testing.T) {
		// Fix for the bug exhibited here: https://github.com/pachyderm/pachyderm/v2/tree/example-groupby/examples/group
		repo := tu.UniqueString("TestGroupInputSymlink")
		require.NoError(t, c.CreateRepo(repo))

		commit := client.NewCommit(repo, "master", "")

		require.NoError(t, c.PutFile(commit, "/T1606331395-LIPID-PATID2-CLIA24D9871327.txt", strings.NewReader(""), client.WithAppendPutFile()))
		require.NoError(t, c.PutFile(commit, "/T1606707579-LIPID-PATID3-CLIA24D9871327.txt", strings.NewReader(""), client.WithAppendPutFile()))
		require.NoError(t, c.PutFile(commit, "/T1606707597-LIPID-PATID4-CLIA24D9871327.txt", strings.NewReader(""), client.WithAppendPutFile()))
		require.NoError(t, c.PutFile(commit, "/T1606707557-LIPID-PATID1-CLIA24D9871327.txt", strings.NewReader(""), client.WithAppendPutFile()))
		require.NoError(t, c.PutFile(commit, "/T1606707613-LIPID-PATID1-CLIA24D9871328.txt", strings.NewReader(""), client.WithAppendPutFile()))
		require.NoError(t, c.PutFile(commit, "/T1606707635-LIPID-PATID3-CLIA24D9871328.txt", strings.NewReader(""), client.WithAppendPutFile()))

		pipeline := "group-pipeline-symlink"
		_, err := c.PpsAPIClient.CreatePipeline(context.Background(),
			&pps.CreatePipelineRequest{
				Pipeline: client.NewPipeline(pipeline),
				Transform: &pps.Transform{
					Cmd: []string{"bash"},
					Stdin: []string{"PATTERN=.*-PATID\\(.*\\)-.*.txt",
						fmt.Sprintf("FILES=/pfs/%v/*", repo),
						"for f in $FILES",
						"do",
						"[[ $(basename $f) =~ $PATTERN ]]",
						"mkdir -p /pfs/out/${BASH_REMATCH[1]}/",
						"cp $f /pfs/out/${BASH_REMATCH[1]}/",
						"done"},
				},
				Input: client.NewGroupInput(
					client.NewPFSInputOpts("", repo, "master", "/*-PATID(*)-*.txt", "", "$1", false, false, nil),
				),
				ParallelismSpec: &pps.ParallelismSpec{
					Constant: 1,
				},
			})
		require.NoError(t, err)

		commitInfo, err := c.InspectCommit(pipeline, "master", "")
		require.NoError(t, err)

		jobs, err := c.WaitJobSetAll(commitInfo.Commit.ID, false)
		require.NoError(t, err)
		require.Equal(t, 1, len(jobs))

		require.Equal(t, "JOB_SUCCESS", jobs[0].State.String())

		expected := [][]string{
			{"/T1606331395-LIPID-PATID2-CLIA24D9871327.txt"},
			{"/T1606707557-LIPID-PATID1-CLIA24D9871327.txt", "/T1606707613-LIPID-PATID1-CLIA24D9871328.txt"},
			{"/T1606707579-LIPID-PATID3-CLIA24D9871327.txt", "/T1606707635-LIPID-PATID3-CLIA24D9871328.txt"},
			{"/T1606707597-LIPID-PATID4-CLIA24D9871327.txt"},
		}
		actual := make([][]string, 0, 3)
		dis, err := c.ListDatumAll(jobs[0].Job.Pipeline.Name, jobs[0].Job.ID)
		require.NoError(t, err)
		// these don't come in a consistent order because group inputs use maps
		sort.Slice(dis, func(i, j int) bool {
			return dis[i].Data[0].File.Path < dis[j].Data[0].File.Path
		})
		for _, di := range dis {
			sort.Slice(di.Data, func(i, j int) bool { return di.Data[i].File.Path < di.Data[j].File.Path })
			datumFiles := make([]string, 0)
			for _, fi := range di.Data {
				datumFiles = append(datumFiles, fi.File.Path)
			}
			actual = append(actual, datumFiles)
		}
		require.Equal(t, expected, actual)
	})
}

func TestUnionInput(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())

	var repos []string
	for i := 0; i < 4; i++ {
		repos = append(repos, tu.UniqueString("TestUnionInput"))
		require.NoError(t, c.CreateRepo(repos[i]))
	}

	numFiles := 2
	for _, repo := range repos {
		commit, err := c.StartCommit(repo, "master")
		require.NoError(t, err)
		for i := 0; i < numFiles; i++ {
			require.NoError(t, c.PutFile(commit, fmt.Sprintf("file-%d", i), strings.NewReader(fmt.Sprintf("%d", i)), client.WithAppendPutFile()))
		}
		require.NoError(t, c.FinishCommit(repo, "master", ""))
	}

	t.Run("union all", func(t *testing.T) {
		pipeline := tu.UniqueString("pipeline")
		require.NoError(t, c.CreatePipeline(
			pipeline,
			"",
			[]string{"bash"},
			[]string{
				"cp /pfs/*/* /pfs/out",
			},
			&pps.ParallelismSpec{
				Constant: 1,
			},
			client.NewUnionInput(
				client.NewPFSInput(repos[0], "/*"),
				client.NewPFSInput(repos[1], "/*"),
				client.NewPFSInput(repos[2], "/*"),
				client.NewPFSInput(repos[3], "/*"),
			),
			"",
			false,
		))

		commitInfo, err := c.WaitCommit(pipeline, "master", "")
		require.NoError(t, err)
		fileInfos, err := c.ListFileAll(commitInfo.Commit, "")
		require.NoError(t, err)
		require.Equal(t, 8, len(fileInfos))
	})

	t.Run("union crosses", func(t *testing.T) {
		pipeline := tu.UniqueString("pipeline")
		require.NoError(t, c.CreatePipeline(
			pipeline,
			"",
			[]string{"bash"},
			[]string{
				"cp -r -L /pfs/TestUnionInput* /pfs/out",
			},
			&pps.ParallelismSpec{
				Constant: 1,
			},
			client.NewUnionInput(
				client.NewCrossInput(
					client.NewPFSInput(repos[0], "/*"),
					client.NewPFSInput(repos[1], "/*"),
				),
				client.NewCrossInput(
					client.NewPFSInput(repos[2], "/*"),
					client.NewPFSInput(repos[3], "/*"),
				),
			),
			"",
			false,
		))

		commitInfo, err := c.WaitCommit(pipeline, "master", "")
		require.NoError(t, err)
		for _, repo := range repos {
			fileInfos, err := c.ListFileAll(commitInfo.Commit, repo)
			require.NoError(t, err)
			require.Equal(t, 4, len(fileInfos))
		}
	})

	t.Run("cross unions", func(t *testing.T) {
		pipeline := tu.UniqueString("pipeline")
		require.NoError(t, c.CreatePipeline(
			pipeline,
			"",
			[]string{"bash"},
			[]string{
				"cp -r -L /pfs/TestUnionInput* /pfs/out",
			},
			&pps.ParallelismSpec{
				Constant: 1,
			},
			client.NewCrossInput(
				client.NewUnionInput(
					client.NewPFSInput(repos[0], "/*"),
					client.NewPFSInput(repos[1], "/*"),
				),
				client.NewUnionInput(
					client.NewPFSInput(repos[2], "/*"),
					client.NewPFSInput(repos[3], "/*"),
				),
			),
			"",
			false,
		))

		commitInfo, err := c.WaitCommit(pipeline, "master", "")
		require.NoError(t, err)
		for _, repo := range repos {
			fileInfos, err := c.ListFileAll(commitInfo.Commit, repo)
			require.NoError(t, err)
			require.Equal(t, 8, len(fileInfos))
		}
	})
}

func TestPipelineWithStats(t *testing.T) {
	// TODO(2.0 required): Change the semantics of the test.
	t.Skip("Stats semantics different in V2")
	//if testing.Short() {
	//	t.Skip("Skipping integration tests in short mode")
	//}

	//c := tu.GetPachClient(t)
	//require.NoError(t, c.DeleteAll())

	//dataRepo := tu.UniqueString("TestPipelineWithStats_data")
	//require.NoError(t, c.CreateRepo(dataRepo))

	//numFiles := 10
	//commit1, err := c.StartCommit(dataRepo, "master")
	//require.NoError(t, err)
	//for i := 0; i < numFiles; i++ {
	//	require.NoError(t, c.PutFile(dataRepo, commit1.ID, fmt.Sprintf("file-%d", i), strings.NewReader(strings.Repeat("foo\n", 100)), client.WithAppendPutFile()))
	//}
	//require.NoError(t, c.FinishCommit(dataRepo, commit1.ID))

	//pipeline := tu.UniqueString("pipeline")
	//_, err = c.PpsAPIClient.CreatePipeline(context.Background(),
	//	&pps.CreatePipelineRequest{
	//		Pipeline: client.NewPipeline(pipeline),
	//		Transform: &pps.Transform{
	//			Cmd: []string{"bash"},
	//			Stdin: []string{
	//				fmt.Sprintf("cp /pfs/%s/* /pfs/out/", dataRepo),
	//			},
	//		},
	//		Input:       client.NewPFSInput(dataRepo, "/*"),
	//		ParallelismSpec: &pps.ParallelismSpec{
	//			Constant: 4,
	//		},
	//	})
	//require.NoError(t, err)

	//commitInfos, err := c.WaitCommitSetAll([]*pfs.Commit{commit1}, nil)
	//require.NoError(t, err)
	//require.Equal(t, 2, len(commitInfos))

	//jobs, err := c.ListJob(pipeline, nil, nil, -1, true)
	//require.NoError(t, err)
	//require.Equal(t, 1, len(jobs))

	//// Check we can list datums before job completion
	//resp, err := c.ListDatumAll(jobs[0].Job.ID, 0, 0)
	//require.NoError(t, err)
	//require.Equal(t, numFiles, len(resp.DatumInfos))
	//require.Equal(t, 1, len(resp.DatumInfos[0].Data))

	//// Check we can list datums before job completion w pagination
	//resp, err = c.ListDatumAll(jobs[0].Job.ID)
	//require.NoError(t, err)
	//require.Equal(t, 5, len(resp.DatumInfos))
	//require.Equal(t, int64(numFiles/5), resp.TotalPages)
	//require.Equal(t, int64(0), resp.Page)

	//// Block on the job being complete before we call ListDatum again so we're
	//// sure the datums have actually been processed.
	//_, err = c.WaitJob(pipeline, jobs[0].Job.ID, false)
	//require.NoError(t, err)

	//resp, err = c.ListDatumAll(jobs[0].Job.ID, 0, 0)
	//require.NoError(t, err)
	//require.Equal(t, numFiles, len(resp.DatumInfos))
	//require.Equal(t, 1, len(resp.DatumInfos[0].Data))

	//for _, datum := range resp.DatumInfos {
	//	require.NoError(t, err)
	//	require.Equal(t, pps.DatumState_SUCCESS, datum.State)
	//}

	//// Make sure 'inspect datum' works
	//datum, err := c.InspectDatum(jobs[0].Job.ID, resp.DatumInfos[0].Datum.ID)
	//require.NoError(t, err)
	//require.Equal(t, pps.DatumState_SUCCESS, datum.State)
}

func TestPipelineWithStatsFailedDatums(t *testing.T) {
	// TODO(2.0 required): Change the semantics of the test.
	t.Skip("Stats semantics different in V2")
	//	if testing.Short() {
	//		t.Skip("Skipping integration tests in short mode")
	//	}
	//
	//	c := tu.GetPachClient(t)
	//	require.NoError(t, c.DeleteAll())
	//
	//	dataRepo := tu.UniqueString("TestPipelineWithStatsFailedDatums_data")
	//	require.NoError(t, c.CreateRepo(dataRepo))
	//
	//	numFiles := 10
	//	commit1, err := c.StartCommit(dataRepo, "master")
	//	require.NoError(t, err)
	//	for i := 0; i < numFiles; i++ {
	//		require.NoError(t, c.PutFile(dataRepo, commit1.ID, fmt.Sprintf("file-%d", i), strings.NewReader(strings.Repeat("foo\n", 100)), client.WithAppendPutFile()))
	//	}
	//	require.NoError(t, c.FinishCommit(dataRepo, commit1.ID))
	//
	//	pipeline := tu.UniqueString("pipeline")
	//	_, err = c.PpsAPIClient.CreatePipeline(context.Background(),
	//		&pps.CreatePipelineRequest{
	//			Pipeline: client.NewPipeline(pipeline),
	//			Transform: &pps.Transform{
	//				Cmd: []string{"bash"},
	//				Stdin: []string{
	//					fmt.Sprintf("if [ -f /pfs/%s/file-5 ]; then exit 1; fi", dataRepo),
	//					fmt.Sprintf("cp /pfs/%s/* /pfs/out/", dataRepo),
	//				},
	//			},
	//			Input:       client.NewPFSInput(dataRepo, "/*"),
	//			ParallelismSpec: &pps.ParallelismSpec{
	//				Constant: 4,
	//			},
	//		})
	//	require.NoError(t, err)
	//
	//	commitInfos, err := c.WaitCommitSetAll([]*pfs.Commit{commit1}, nil)
	//	require.NoError(t, err)
	//	require.Equal(t, 2, len(commitInfos))
	//
	//	jobs, err := c.ListJob(pipeline, nil, nil, -1, true)
	//	require.NoError(t, err)
	//	require.Equal(t, 1, len(jobs))
	//	// Block on the job being complete before we call ListDatum
	//	_, err = c.WaitJob(pipeline, jobs[0].Job.ID, false)
	//	require.NoError(t, err)
	//
	//	resp, err := c.ListDatumAll(pipeline, jobs[0].Job.ID, 0, 0)
	//	require.NoError(t, err)
	//	require.Equal(t, numFiles, len(resp.DatumInfos))
	//
	//	// First entry should be failed
	//	require.Equal(t, pps.DatumState_FAILED, resp.DatumInfos[0].State)
	//	// Last entry should be success
	//	require.Equal(t, pps.DatumState_SUCCESS, resp.DatumInfos[len(resp.DatumInfos)-1].State)
	//
	//	// Make sure 'inspect datum' works for failed state
	//	datum, err := c.InspectDatum(pipeline, jobs[0].Job.ID, resp.DatumInfos[0].Datum.ID)
	//	require.NoError(t, err)
	//	require.Equal(t, pps.DatumState_FAILED, datum.State)
}

func TestPipelineWithStatsPaginated(t *testing.T) {
	// TODO(2.0 optional): Implement datum pagination?.
	t.Skip("Datum pagination not implemented in V2")
	//	if testing.Short() {
	//		t.Skip("Skipping integration tests in short mode")
	//	}
	//
	//	c := tu.GetPachClient(t)
	//	require.NoError(t, c.DeleteAll())
	//
	//	dataRepo := tu.UniqueString("TestPipelineWithStatsPaginated_data")
	//	require.NoError(t, c.CreateRepo(dataRepo))
	//
	//	numPages := int64(2)
	//	pageSize := int64(10)
	//	numFiles := int(numPages * pageSize)
	//	commit1, err := c.StartCommit(dataRepo, "master")
	//	require.NoError(t, err)
	//	for i := 0; i < numFiles; i++ {
	//		require.NoError(t, c.PutFile(dataRepo, commit1.ID, fmt.Sprintf("file-%d", i), strings.NewReader(strings.Repeat("foo\n", 100)), client.WithAppendPutFile()))
	//	}
	//	require.NoError(t, c.FinishCommit(dataRepo, commit1.ID))
	//
	//	pipeline := tu.UniqueString("pipeline")
	//	_, err = c.PpsAPIClient.CreatePipeline(context.Background(),
	//		&pps.CreatePipelineRequest{
	//			Pipeline: client.NewPipeline(pipeline),
	//			Transform: &pps.Transform{
	//				Cmd: []string{"bash"},
	//				Stdin: []string{
	//					fmt.Sprintf("if [ -f /pfs/%s/file-5 ]; then exit 1; fi", dataRepo),
	//					fmt.Sprintf("cp /pfs/%s/* /pfs/out/", dataRepo),
	//				},
	//			},
	//			Input:       client.NewPFSInput(dataRepo, "/*"),
	//			ParallelismSpec: &pps.ParallelismSpec{
	//				Constant: 4,
	//			},
	//		})
	//
	//	commitInfos, err := c.WaitCommitSetAll([]*pfs.Commit{commit1}, nil)
	//	require.NoError(t, err)
	//	require.Equal(t, 2, len(commitInfos))
	//
	//	var jobs []*pps.JobInfo
	//	require.NoError(t, backoff.Retry(func() error {
	//		jobs, err = c.ListJob(pipeline, nil, nil, -1, true)
	//		require.NoError(t, err)
	//		if len(jobs) != 1 {
	//			return errors.Errorf("expected 1 jobs, got %d", len(jobs))
	//		}
	//		return nil
	//	}, backoff.NewTestingBackOff()))
	//
	//	// Block on the job being complete before we call ListDatum
	//	_, err = c.WaitJob(pipeline, jobs[0].Job.ID, false)
	//	require.NoError(t, err)
	//
	//	resp, err := c.ListDatumAll(jobs[0].Job.ID, pageSize, 0)
	//	require.NoError(t, err)
	//	require.Equal(t, pageSize, int64(len(resp.DatumInfos)))
	//	require.Equal(t, int64(numFiles)/pageSize, resp.TotalPages)
	//
	//	// First entry should be failed
	//	require.Equal(t, pps.DatumState_FAILED, resp.DatumInfos[0].State)
	//
	//	resp, err = c.ListDatumAll(jobs[0].Job.ID, pageSize, int64(numPages-1))
	//	require.NoError(t, err)
	//	require.Equal(t, pageSize, int64(len(resp.DatumInfos)))
	//	require.Equal(t, int64(int64(numFiles)/pageSize-1), resp.Page)
	//
	//	// Last entry should be success
	//	require.Equal(t, pps.DatumState_SUCCESS, resp.DatumInfos[len(resp.DatumInfos)-1].State)
	//
	//	// Make sure we get error when requesting pages too high
	//	_, err = c.ListDatumAll(jobs[0].Job.ID, pageSize, int64(numPages))
	//	require.YesError(t, err)
}

func TestPipelineWithStatsAcrossJobs(t *testing.T) {
	// TODO(2.0 required): Change semantics of test.
	t.Skip("Stats semantics different in V2")
	//	if testing.Short() {
	//		t.Skip("Skipping integration tests in short mode")
	//	}
	//
	//	c := tu.GetPachClient(t)
	//	require.NoError(t, c.DeleteAll())
	//
	//	dataRepo := tu.UniqueString("TestPipelineWithStatsAcrossJobs_data")
	//	require.NoError(t, c.CreateRepo(dataRepo))
	//
	//	numFiles := 10
	//	commit1, err := c.StartCommit(dataRepo, "master")
	//	require.NoError(t, err)
	//	for i := 0; i < numFiles; i++ {
	//		require.NoError(t, c.PutFile(dataRepo, commit1.ID, fmt.Sprintf("foo-%d", i), strings.NewReader(strings.Repeat("foo\n", 100)), client.WithAppendPutFile()))
	//	}
	//	require.NoError(t, c.FinishCommit(dataRepo, commit1.ID))
	//
	//	pipeline := tu.UniqueString("StatsAcrossJobs")
	//	_, err = c.PpsAPIClient.CreatePipeline(context.Background(),
	//		&pps.CreatePipelineRequest{
	//			Pipeline: client.NewPipeline(pipeline),
	//			Transform: &pps.Transform{
	//				Cmd: []string{"bash"},
	//				Stdin: []string{
	//					fmt.Sprintf("cp /pfs/%s/* /pfs/out/", dataRepo),
	//				},
	//			},
	//			Input:       client.NewPFSInput(dataRepo, "/*"),
	//			ParallelismSpec: &pps.ParallelismSpec{
	//				Constant: 1,
	//			},
	//		})
	//	require.NoError(t, err)
	//
	//	commitInfos, err := c.WaitCommitSetAll([]*pfs.Commit{commit1}, nil)
	//	require.NoError(t, err)
	//	require.Equal(t, 2, len(commitInfos))
	//
	//	jobs, err := c.ListJob(pipeline, nil, nil, -1, true)
	//	require.NoError(t, err)
	//	require.Equal(t, 1, len(jobs))
	//
	//	// Block on the job being complete before we call ListDatum
	//	_, err = c.WaitJob(pipeline, jobs[0].Job.ID, false)
	//	require.NoError(t, err)
	//
	//	resp, err := c.ListDatumAll(pipeline, jobs[0].Job.ID, 0, 0)
	//	require.NoError(t, err)
	//	require.Equal(t, numFiles, len(resp.DatumInfos))
	//
	//	datum, err := c.InspectDatum(pipeline, jobs[0].Job.ID, resp.DatumInfos[0].Datum.ID)
	//	require.NoError(t, err)
	//	require.Equal(t, pps.DatumState_SUCCESS, datum.State)
	//
	//	commit2, err := c.StartCommit(dataRepo, "master")
	//	require.NoError(t, err)
	//	for i := 0; i < numFiles; i++ {
	//		require.NoError(t, c.PutFile(dataRepo, commit2.ID, fmt.Sprintf("bar-%d", i), strings.NewReader(strings.Repeat("bar\n", 100)), client.WithAppendPutFile()))
	//	}
	//	require.NoError(t, c.FinishCommit(dataRepo, commit2.ID))
	//
	//	commitInfos, err = c.WaitCommitSetAll([]*pfs.Commit{commit2}, nil)
	//	require.NoError(t, err)
	//	require.Equal(t, 2, len(commitInfos))
	//
	//	jobs, err = c.ListJob(pipeline, nil, nil, -1, true)
	//	require.NoError(t, err)
	//	require.Equal(t, 2, len(jobs))
	//
	//	// Block on the job being complete before we call ListDatum
	//	_, err = c.WaitJob(pipeline, jobs[0].Job.ID, false)
	//	require.NoError(t, err)
	//
	//	resp, err = c.ListDatumAll(pipeline, jobs[0].Job.ID, 0, 0)
	//	require.NoError(t, err)
	//	// we should see all the datums from the first job (which should be skipped)
	//	// in addition to all the new datums processed in this job
	//	require.Equal(t, numFiles*2, len(resp.DatumInfos))
	//
	//	datum, err = c.InspectDatum(pipeline, jobs[0].Job.ID, resp.DatumInfos[0].Datum.ID)
	//	require.NoError(t, err)
	//	require.Equal(t, pps.DatumState_SUCCESS, datum.State)
	//	// Test datums marked as skipped correctly
	//	// (also tests list datums are sorted by state)
	//	datum, err = c.InspectDatum(pipeline, jobs[0].Job.ID, resp.DatumInfos[numFiles].Datum.ID)
	//	require.NoError(t, err)
	//	require.Equal(t, pps.DatumState_SKIPPED, datum.State)
}

func TestPipelineWithStatsSkippedEdgeCase(t *testing.T) {
	// TODO(2.0 required): Change semantics of test.
	t.Skip("Stats semantics different in V2")
	//	// If I add a file in commit1, delete it in commit2, add it again in commit 3 ...
	//	// the datum will be marked as success on the 3rd job, even though it should be marked as skipped
	//	if testing.Short() {
	//		t.Skip("Skipping integration tests in short mode")
	//	}
	//
	//	c := tu.GetPachClient(t)
	//	require.NoError(t, c.DeleteAll())
	//
	//	dataRepo := tu.UniqueString("TestPipelineWithStatsSkippedEdgeCase_data")
	//	require.NoError(t, c.CreateRepo(dataRepo))
	//
	//	numFiles := 10
	//	commit1, err := c.StartCommit(dataRepo, "master")
	//	require.NoError(t, err)
	//	for i := 0; i < numFiles; i++ {
	//		require.NoError(t, c.PutFile(dataRepo, commit1.ID, fmt.Sprintf("file-%d", i), strings.NewReader(strings.Repeat("foo\n", 100)), client.WithAppendPutFile()))
	//	}
	//	require.NoError(t, c.FinishCommit(dataRepo, commit1.ID))
	//
	//	pipeline := tu.UniqueString("StatsEdgeCase")
	//	_, err = c.PpsAPIClient.CreatePipeline(context.Background(),
	//		&pps.CreatePipelineRequest{
	//			Pipeline: client.NewPipeline(pipeline),
	//			Transform: &pps.Transform{
	//				Cmd: []string{"bash"},
	//				Stdin: []string{
	//					fmt.Sprintf("cp /pfs/%s/* /pfs/out/", dataRepo),
	//				},
	//			},
	//			Input:       client.NewPFSInput(dataRepo, "/*"),
	//			ParallelismSpec: &pps.ParallelismSpec{
	//				Constant: 1,
	//			},
	//		})
	//	require.NoError(t, err)
	//
	//	commitInfos, err := c.WaitCommitSetAll([]*pfs.Commit{commit1}, nil)
	//	require.NoError(t, err)
	//	require.Equal(t, 2, len(commitInfos))
	//
	//	jobs, err := c.ListJob(pipeline, nil, nil, -1, true)
	//	require.NoError(t, err)
	//	require.Equal(t, 1, len(jobs))
	//
	//	// Block on the job being complete before we call ListDatum
	//	_, err = c.WaitJob(pipeline, jobs[0].Job.ID, false)
	//	require.NoError(t, err)
	//	resp, err := c.ListDatumAll(pipeline, jobs[0].Job.ID, 0, 0)
	//	require.NoError(t, err)
	//	require.Equal(t, numFiles, len(resp.DatumInfos))
	//
	//	for _, datum := range resp.DatumInfos {
	//		require.NoError(t, err)
	//		require.Equal(t, pps.DatumState_SUCCESS, datum.State)
	//	}
	//
	//	// Make sure 'inspect datum' works
	//	datum, err := c.InspectDatum(pipeline, jobs[0].Job.ID, resp.DatumInfos[0].Datum.ID)
	//	require.NoError(t, err)
	//	require.Equal(t, pps.DatumState_SUCCESS, datum.State)
	//
	//	// Create a second commit that deletes a file in commit1
	//	commit2, err := c.StartCommit(dataRepo, "master")
	//	require.NoError(t, err)
	//	err = c.DeleteFile(dataRepo, commit2.ID, "file-0")
	//	require.NoError(t, err)
	//	require.NoError(t, c.FinishCommit(dataRepo, commit2.ID))
	//
	//	// Create a third commit that re-adds the file removed in commit2
	//	commit3, err := c.StartCommit(dataRepo, "master")
	//	require.NoError(t, err)
	//	require.NoError(t, c.PutFile(dataRepo, commit3.ID, "file-0", strings.NewReader(strings.Repeat("foo\n", 100)), client.WithAppendPutFile()))
	//	require.NoError(t, c.FinishCommit(dataRepo, commit3.ID))
	//
	//	commitInfos, err = c.WaitCommitSetAll([]*pfs.Commit{commit3}, nil)
	//	require.NoError(t, err)
	//	require.Equal(t, 2, len(commitInfos))
	//
	//	jobs, err = c.ListJob(pipeline, nil, nil, -1, true)
	//	require.NoError(t, err)
	//	require.Equal(t, 3, len(jobs))
	//
	//	// Block on the job being complete before we call ListDatum
	//	_, err = c.InspectJob(jobs[0].Job.Pipeline.Name, jobs[0].Job.ID, true)
	//	require.NoError(t, err)
	//	resp, err = c.ListDatumAll(jobs[0].Job.ID, 0, 0)
	//	require.NoError(t, err)
	//	require.Equal(t, numFiles, len(resp.DatumInfos))
	//
	//	for _, datum := range resp.DatumInfos {
	//		require.Equal(t, pps.DatumState_SKIPPED, datum.State)
	//	}
}

func TestPipelineOnStatsBranch(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())

	dataRepo := tu.UniqueString("TestPipelineOnStatsBranch_data")
	require.NoError(t, c.CreateRepo(dataRepo))

	commit, err := c.StartCommit(dataRepo, "master")
	require.NoError(t, err)
	require.NoError(t, c.PutFile(commit, "file", strings.NewReader("foo"), client.WithAppendPutFile()))
	require.NoError(t, c.FinishCommit(dataRepo, commit.Branch.Name, commit.ID))

	pipeline1, pipeline2 := tu.UniqueString("TestPipelineOnStatsBranch1"), tu.UniqueString("TestPipelineOnStatsBranch2")
	_, err = c.PpsAPIClient.CreatePipeline(context.Background(),
		&pps.CreatePipelineRequest{
			Pipeline: client.NewPipeline(pipeline1),
			Transform: &pps.Transform{
				Cmd: []string{"bash", "-c", "cp -r $(ls -d /pfs/*|grep -v /pfs/out) /pfs/out"},
			},
			Input: client.NewPFSInput(dataRepo, "/*"),
		})
	require.NoError(t, err)
	_, err = c.PpsAPIClient.CreatePipeline(context.Background(),
		&pps.CreatePipelineRequest{
			Pipeline: client.NewPipeline(pipeline2),
			Transform: &pps.Transform{
				Cmd: []string{"bash", "-c", "cp -r $(ls -d /pfs/*|grep -v /pfs/out) /pfs/out"},
			},
			Input: &pps.Input{
				Pfs: &pps.PFSInput{
					Repo:     pipeline1,
					RepoType: pfs.MetaRepoType,
					Branch:   "master",
					Glob:     "/*",
				},
			},
		})
	require.NoError(t, err)

	commitInfo, err := c.InspectCommit(pipeline2, "master", "")
	require.NoError(t, err)

	jobInfos, err := c.WaitJobSetAll(commitInfo.Commit.ID, false)
	require.NoError(t, err)
	require.Equal(t, 1, len(jobInfos))
	for _, ji := range jobInfos {
		require.Equal(t, ji.State.String(), pps.JobState_JOB_SUCCESS.String())
	}
}

func TestSkippedDatums(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())
	// create repos
	dataRepo := tu.UniqueString("TestPipeline_data")
	require.NoError(t, c.CreateRepo(dataRepo))

	// create pipeline
	pipelineName := tu.UniqueString("pipeline")
	//	require.NoError(t, c.CreatePipeline(
	_, err := c.PpsAPIClient.CreatePipeline(context.Background(),
		&pps.CreatePipelineRequest{
			Pipeline: client.NewPipeline(pipelineName),
			Transform: &pps.Transform{
				Cmd: []string{"bash"},
				Stdin: []string{
					fmt.Sprintf("cp /pfs/%s/* /pfs/out/", dataRepo),
				},
			},
			ParallelismSpec: &pps.ParallelismSpec{
				Constant: 1,
			},
			Input: client.NewPFSInput(dataRepo, "/*"),
		})
	require.NoError(t, err)

	// Do first commit to repo
	commit1, err := c.StartCommit(dataRepo, "master")
	require.NoError(t, err)
	require.NoError(t, c.PutFile(commit1, "file", strings.NewReader("foo\n"), client.WithAppendPutFile()))
	require.NoError(t, c.FinishCommit(dataRepo, commit1.Branch.Name, commit1.ID))
	jis, err := c.WaitJobSetAll(commit1.ID, false)
	require.NoError(t, err)
	require.Equal(t, 1, len(jis))
	ji := jis[0]
	require.Equal(t, ji.State, pps.JobState_JOB_SUCCESS)
	var buffer bytes.Buffer
	require.NoError(t, c.GetFile(ji.OutputCommit, "file", &buffer))
	require.Equal(t, "foo\n", buffer.String())

	// Do second commit to repo
	commit2, err := c.StartCommit(dataRepo, "master")
	require.NoError(t, err)
	require.NoError(t, c.PutFile(commit2, "file2", strings.NewReader("bar\n"), client.WithAppendPutFile()))
	require.NoError(t, c.FinishCommit(dataRepo, commit2.Branch.Name, commit2.ID))
	jis, err = c.WaitJobSetAll(commit2.ID, false)
	require.NoError(t, err)
	require.Equal(t, 1, len(jis))
	ji = jis[0]
	require.Equal(t, ji.State, pps.JobState_JOB_SUCCESS)

	/*
		jobs, err := c.ListJob(pipelineName, nil, nil, -1, true)
		require.NoError(t, err)
		require.Equal(t, 2, len(jobs))

		datums, err := c.ListDatumAll(jobs[1].Job.ID)
		require.NoError(t, err)
		require.Equal(t, 2, len(datums))

		datum, err := c.InspectDatum(jobs[1].Job.ID, datums[0].ID)
		require.NoError(t, err)
		require.Equal(t, pps.DatumState_SUCCESS, datum.State)
	*/
}

func TestCronPipeline(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())
	t.Run("SimpleCron", func(t *testing.T) {
		pipeline1 := tu.UniqueString("cron1-")
		require.NoError(t, c.CreatePipeline(
			pipeline1,
			"",
			[]string{"/bin/bash"},
			[]string{"cp /pfs/time/* /pfs/out/"},
			nil,
			client.NewCronInput("time", "@every 10s"),
			"",
			false,
		))
		pipeline2 := tu.UniqueString("cron2-")
		require.NoError(t, c.CreatePipeline(
			pipeline2,
			"",
			[]string{"/bin/bash"},
			[]string{"cp " + fmt.Sprintf("/pfs/%s/*", pipeline1) + " /pfs/out/"},
			nil,
			client.NewPFSInput(pipeline1, "/*"),
			"",
			false,
		))

		// subscribe to the pipeline2 cron repo and wait for inputs
		repo := client.NewRepo(pipeline2)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*120)
		defer cancel() //cleanup resources
		// We'll look at four commits - with one initial head and one created in each tick
		// We expect the first commit to have 0 files, the second to have 1 file, etc...
		countBreakFunc := newCountBreakFunc(4)
		count := 0
		require.NoError(t, c.WithCtx(ctx).SubscribeCommit(repo, "master", "", pfs.CommitState_STARTED, func(ci *pfs.CommitInfo) error {
			return countBreakFunc(func() error {
				_, err := c.WaitCommitSetAll(ci.Commit.ID)
				require.NoError(t, err)

				files, err := c.ListFileAll(ci.Commit, "")
				require.NoError(t, err)
				require.Equal(t, count, len(files))
				count++

				return nil
			})
		}))
	})

	// Test a CronInput with the overwrite flag set to true
	t.Run("CronOverwrite", func(t *testing.T) {
		pipeline3 := tu.UniqueString("cron3-")
		overwriteInput := client.NewCronInput("time", "@every 10s")
		overwriteInput.Cron.Overwrite = true
		require.NoError(t, c.CreatePipeline(
			pipeline3,
			"",
			[]string{"/bin/bash"},
			[]string{"cp /pfs/time/* /pfs/out/"},
			nil,
			overwriteInput,
			"",
			false,
		))
		repo := client.NewRepo(fmt.Sprintf("%s_time", pipeline3))
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*120)
		defer cancel() //cleanup resources
		// We'll look at four commits - with one created in each tick
		// We expect each of the commits to have just a single file in this case
		// except the first, which is the default branch head.
		countBreakFunc := newCountBreakFunc(3)
		count := 0
		require.NoError(t, c.WithCtx(ctx).SubscribeCommit(repo, "master", "", pfs.CommitState_STARTED, func(ci *pfs.CommitInfo) error {
			return countBreakFunc(func() error {
				commitInfos, err := c.WaitCommitSetAll(ci.Commit.ID)
				require.NoError(t, err)
				require.Equal(t, 4, len(commitInfos))

				files, err := c.ListFileAll(ci.Commit, "")
				require.NoError(t, err)
				require.Equal(t, count, len(files))
				count = 1

				return nil
			})
		}))
	})

	// Create a non-cron input repo, and test a pipeline with a cross of cron and
	// non-cron inputs
	t.Run("CronPFSCross", func(t *testing.T) {
		dataRepo := tu.UniqueString("TestCronPipeline_data")
		require.NoError(t, c.CreateRepo(dataRepo))
		pipeline4 := tu.UniqueString("cron4-")
		require.NoError(t, c.CreatePipeline(
			pipeline4,
			"",
			[]string{"bash"},
			[]string{
				"cp /pfs/time/time /pfs/out/time",
				fmt.Sprintf("cp /pfs/%s/file /pfs/out/file", dataRepo),
			},
			nil,
			client.NewCrossInput(
				client.NewCronInput("time", "@every 20s"),
				client.NewPFSInput(dataRepo, "/"),
			),
			"",
			false,
		))
		_, err := c.StartCommit(dataRepo, "master")
		require.NoError(t, err)
		require.NoError(t, c.PutFile(client.NewCommit(dataRepo, "master", ""), "file", strings.NewReader("file"), client.WithAppendPutFile()))
		require.NoError(t, c.FinishCommit(dataRepo, "master", ""))

		repo := client.NewRepo(fmt.Sprintf("%s_time", pipeline4))
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
		defer cancel() //cleanup resources
		countBreakFunc := newCountBreakFunc(2)
		require.NoError(t, c.WithCtx(ctx).SubscribeCommit(repo, "master", "", pfs.CommitState_STARTED, func(ci *pfs.CommitInfo) error {
			return countBreakFunc(func() error {
				commitInfos, err := c.WaitCommitSetAll(ci.Commit.ID)
				require.NoError(t, err)
				require.Equal(t, 5, len(commitInfos))

				return nil
			})
		}))
	})
	t.Run("RunCron", func(t *testing.T) {
		pipeline5 := tu.UniqueString("cron5-")
		require.NoError(t, c.CreatePipeline(
			pipeline5,
			"",
			[]string{"/bin/bash"},
			[]string{"cp /pfs/time/* /pfs/out/"},
			nil,
			client.NewCronInput("time", "@every 1h"),
			"",
			false,
		))
		pipeline6 := tu.UniqueString("cron6-")
		require.NoError(t, c.CreatePipeline(
			pipeline6,
			"",
			[]string{"/bin/bash"},
			[]string{"cp " + fmt.Sprintf("/pfs/%s/*", pipeline5) + " /pfs/out/"},
			nil,
			client.NewPFSInput(pipeline5, "/*"),
			"",
			false,
		))

		_, err := c.PpsAPIClient.RunCron(context.Background(), &pps.RunCronRequest{Pipeline: client.NewPipeline(pipeline5)})
		require.NoError(t, err)
		_, err = c.PpsAPIClient.RunCron(context.Background(), &pps.RunCronRequest{Pipeline: client.NewPipeline(pipeline5)})
		require.NoError(t, err)
		_, err = c.PpsAPIClient.RunCron(context.Background(), &pps.RunCronRequest{Pipeline: client.NewPipeline(pipeline5)})
		require.NoError(t, err)

		// subscribe to the pipeline6 cron repo and wait for inputs
		repo := client.NewRepo(pipeline6)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*120)
		defer cancel() //cleanup resources
		countBreakFunc := newCountBreakFunc(4)
		require.NoError(t, c.WithCtx(ctx).SubscribeCommit(repo, "master", "", pfs.CommitState_STARTED, func(ci *pfs.CommitInfo) error {
			return countBreakFunc(func() error {
				_, err := c.WaitCommit(pipeline6, "master", ci.Commit.ID)
				require.NoError(t, err)

				return nil
			})
		}))
	})
	t.Run("RunCronOverwrite", func(t *testing.T) {
		pipeline7 := tu.UniqueString("cron7-")
		require.NoError(t, c.CreatePipeline(
			pipeline7,
			"",
			[]string{"/bin/bash"},
			[]string{"cp /pfs/time/* /pfs/out/"},
			nil,
			client.NewCronInputOpts("time", "", "1-59/1 * * * *", true), // every minute
			"",
			false,
		))
		pipeline8 := tu.UniqueString("cron8-")
		require.NoError(t, c.CreatePipeline(
			pipeline8,
			"",
			[]string{"/bin/bash"},
			[]string{"cp " + fmt.Sprintf("/pfs/%s/*", pipeline7) + " /pfs/out/"},
			nil,
			client.NewPFSInput(pipeline7, "/*"),
			"",
			false,
		))

		// subscribe to the pipeline1 cron repo and wait for inputs
		repo := fmt.Sprintf("%s_%s", pipeline7, "time")
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*120)
		defer cancel() //cleanup resources
		countBreakFunc := newCountBreakFunc(1)
		require.NoError(t, c.WithCtx(ctx).SubscribeCommit(client.NewRepo(repo), "master", "", pfs.CommitState_FINISHED, func(ci *pfs.CommitInfo) error {
			return countBreakFunc(func() error {
				// if the runcron is run too soon, it will have the same timestamp and we won't hit the weird bug
				time.Sleep(2 * time.Second)

				_, err := c.PpsAPIClient.RunCron(context.Background(), &pps.RunCronRequest{Pipeline: client.NewPipeline(pipeline7)})
				require.NoError(t, err)
				_, err = c.PpsAPIClient.RunCron(context.Background(), &pps.RunCronRequest{Pipeline: client.NewPipeline(pipeline7)})
				require.NoError(t, err)
				_, err = c.PpsAPIClient.RunCron(context.Background(), &pps.RunCronRequest{Pipeline: client.NewPipeline(pipeline7)})
				require.NoError(t, err)

				ctx, cancel = context.WithTimeout(context.Background(), time.Second*120)
				defer cancel() //cleanup resources
				// We expect to see four commits, despite the schedule being every minute, and the timeout 120 seconds
				// We expect each of the commits to have just a single file in this case
				// We check four so that we can make sure the scheduled cron is not messed up by the run crons
				countBreakFunc := newCountBreakFunc(5) // TODO: Right now we receive an additional Alias Commit. change this back to 4 as soon as we can filter subscribeCommit by commitType
				require.NoError(t, c.WithCtx(ctx).SubscribeCommit(client.NewRepo(repo), "master", ci.Commit.ID, pfs.CommitState_STARTED, func(ci *pfs.CommitInfo) error {
					return countBreakFunc(func() error {
						_, err := c.WaitCommitSetAll(ci.Commit.ID)
						require.NoError(t, err)
						if ci.Origin.Kind != pfs.OriginKind_ALIAS {
							files, err := c.ListFileAll(ci.Commit, "/")
							require.NoError(t, err)
							require.Equal(t, 1, len(files))
						}
						return nil
					})
				}))
				commits, err := c.ListCommit(client.NewRepo(repo), nil, nil, 0)
				require.NoError(t, err)

				userCommitCount := 0
				for _, v := range commits {
					if v.Origin.Kind == pfs.OriginKind_USER {
						userCommitCount++
					}
				}
				require.Equal(t, 4, userCommitCount)
				return nil
			})
		}))
	})
	t.Run("RunCronCross", func(t *testing.T) {
		pipeline9 := tu.UniqueString("cron9-")
		require.NoError(t, c.CreatePipeline(
			pipeline9,
			"",
			[]string{"/bin/bash"},
			[]string{"echo 'tick'"},
			nil,
			client.NewCrossInput(
				client.NewCronInput("time1", "@every 3h"),
				client.NewCronInput("time2", "@every 2h"),
			),
			"",
			false,
		))

		_, err := c.PpsAPIClient.RunCron(context.Background(), &pps.RunCronRequest{Pipeline: client.NewPipeline(pipeline9)})
		require.NoError(t, err)
		_, err = c.PpsAPIClient.RunCron(context.Background(), &pps.RunCronRequest{Pipeline: client.NewPipeline(pipeline9)})
		require.NoError(t, err)
		_, err = c.PpsAPIClient.RunCron(context.Background(), &pps.RunCronRequest{Pipeline: client.NewPipeline(pipeline9)})
		require.NoError(t, err)

		// subscribe to the pipeline1 cron repo and wait for inputs
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*120)
		defer cancel() //cleanup resources
		// We expect to see at least three commits, despite the schedules not ticking until three hours, and the timeout 120 seconds
		repo := pipeline9
		countBreakFunc := newCountBreakFunc(3)
		require.NoError(t, c.WithCtx(ctx).SubscribeCommit(client.NewRepo(repo), "master", "", pfs.CommitState_STARTED, func(ci *pfs.CommitInfo) error {
			return countBreakFunc(func() error {
				return nil
			})
		}))
	})
}

func TestSelfReferentialPipeline(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())
	pipeline := tu.UniqueString("pipeline")
	require.YesError(t, c.CreatePipeline(
		pipeline,
		"",
		[]string{"true"},
		nil,
		nil,
		client.NewPFSInput(pipeline, "/"),
		"",
		false,
	))
}

func TestPipelineBadImage(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())
	pipeline1 := tu.UniqueString("bad_pipeline_1_")
	require.NoError(t, c.CreatePipeline(
		pipeline1,
		"BadImage",
		[]string{"true"},
		nil,
		nil,
		client.NewCronInput("time", "@every 20s"),
		"",
		false,
	))
	pipeline2 := tu.UniqueString("bad_pipeline_2_")
	require.NoError(t, c.CreatePipeline(
		pipeline2,
		"bs/badimage:vcrap",
		[]string{"true"},
		nil,
		nil,
		client.NewCronInput("time", "@every 20s"),
		"",
		false,
	))
	require.NoError(t, backoff.Retry(func() error {
		for _, pipeline := range []string{pipeline1, pipeline2} {
			pipelineInfo, err := c.InspectPipeline(pipeline, false)
			if err != nil {
				return err
			}
			if pipelineInfo.State != pps.PipelineState_PIPELINE_CRASHING {
				return errors.Errorf("pipeline %s should be in crashing", pipeline)
			}
			require.True(t, pipelineInfo.Reason != "")
		}
		return nil
	}, backoff.NewTestingBackOff()))
}

func TestFixPipeline(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())
	// create repos
	dataRepo := tu.UniqueString("TestFixPipeline_data")
	require.NoError(t, c.CreateRepo(dataRepo))
	commit, err := c.StartCommit(dataRepo, "master")
	require.NoError(t, err)
	require.NoError(t, c.PutFile(commit, "file", strings.NewReader("1"), client.WithAppendPutFile()))
	require.NoError(t, c.FinishCommit(dataRepo, "master", ""))
	pipelineName := tu.UniqueString("TestFixPipeline_pipeline")
	require.NoError(t, c.CreatePipeline(
		pipelineName,
		"",
		[]string{"exit 1"},
		nil,
		&pps.ParallelismSpec{
			Constant: 1,
		},
		client.NewPFSInput(dataRepo, "/*"),
		"",
		false,
	))

	require.NoError(t, backoff.Retry(func() error {
		jobInfos, err := c.ListJob(pipelineName, nil, -1, true)
		require.NoError(t, err)
		if len(jobInfos) != 1 {
			return errors.Errorf("expected 1 jobs, got %d", len(jobInfos))
		}
		jobInfo, err := c.WaitJob(jobInfos[0].Job.Pipeline.Name, jobInfos[0].Job.ID, false)
		require.NoError(t, err)
		require.Equal(t, pps.JobState_JOB_FAILURE, jobInfo.State)
		return nil
	}, backoff.NewTestingBackOff()))

	// Update the pipeline, this will not create a new pipeline as reprocess
	// isn't set to true.
	require.NoError(t, c.CreatePipeline(
		pipelineName,
		"",
		[]string{"bash"},
		[]string{"echo bar >/pfs/out/file"},
		&pps.ParallelismSpec{
			Constant: 1,
		},
		client.NewPFSInput(dataRepo, "/*"),
		"",
		true,
	))

	require.NoError(t, backoff.Retry(func() error {
		jobInfos, err := c.ListJob(pipelineName, nil, -1, true)
		require.NoError(t, err)
		if len(jobInfos) != 2 {
			return errors.Errorf("expected 2 jobs, got %d", len(jobInfos))
		}
		jobInfo, err := c.WaitJob(jobInfos[0].Job.Pipeline.Name, jobInfos[0].Job.ID, false)
		require.NoError(t, err)
		require.Equal(t, pps.JobState_JOB_SUCCESS, jobInfo.State)
		return nil
	}, backoff.NewTestingBackOff()))
}

func TestListJobTruncated(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}
	c := tu.GetPachClient(t)

	dataRepo := tu.UniqueString("TestListJobTruncated_data")
	require.NoError(t, c.CreateRepo(dataRepo))

	pipeline := tu.UniqueString("pipeline")
	require.NoError(t, c.CreatePipeline(
		pipeline,
		"",
		[]string{"bash"},
		[]string{
			fmt.Sprintf("cp /pfs/%s/* /pfs/out/", dataRepo),
		},
		nil,
		client.NewPFSInput(dataRepo, "/*"),
		"",
		false,
	))

	commit1, err := c.StartCommit(dataRepo, "master")
	require.NoError(t, err)
	require.NoError(t, c.PutFile(commit1, "file", strings.NewReader("foo"), client.WithAppendPutFile()))
	require.NoError(t, c.FinishCommit(dataRepo, commit1.Branch.Name, commit1.ID))

	commitInfos, err := c.WaitCommitSetAll(commit1.ID)
	require.NoError(t, err)
	require.Equal(t, 4, len(commitInfos))

	liteJobInfos, err := c.ListJob(pipeline, nil, 0, false)
	require.NoError(t, err)
	require.Equal(t, 2, len(liteJobInfos))
	require.Equal(t, commit1.ID, liteJobInfos[0].Job.ID)
	fullJobInfos, err := c.ListJob(pipeline, nil, 0, true)
	require.NoError(t, err)
	require.Equal(t, commit1.ID, fullJobInfos[0].Job.ID)

	// Check that details are missing when not requested
	require.Nil(t, liteJobInfos[0].Details)
	require.Equal(t, pipeline, liteJobInfos[0].Job.Pipeline.Name)

	// Check that all fields are present when requested
	require.NotNil(t, fullJobInfos[0].Details)
	require.NotNil(t, fullJobInfos[0].Details.Transform)
	require.NotNil(t, fullJobInfos[0].Details.Input)
	require.Equal(t, pipeline, fullJobInfos[0].Job.Pipeline.Name)
}

func TestPipelineEnvVarAlias(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())

	dataRepo := tu.UniqueString("TestPipelineEnvVarAlias_data")
	require.NoError(t, c.CreateRepo(dataRepo))

	pipeline := tu.UniqueString("pipeline")
	require.NoError(t, c.CreatePipeline(
		pipeline,
		"",
		[]string{"bash"},
		[]string{
			"env",
			fmt.Sprintf("cp $%s /pfs/out/", dataRepo),
		},
		nil,
		client.NewPFSInput(dataRepo, "/*"),
		"",
		false,
	))

	numFiles := 10
	commit1, err := c.StartCommit(dataRepo, "master")
	require.NoError(t, err)
	for i := 0; i < numFiles; i++ {
		require.NoError(t, c.PutFile(commit1, fmt.Sprintf("file-%d", i), strings.NewReader(fmt.Sprintf("%d", i)), client.WithAppendPutFile()))
	}
	require.NoError(t, c.FinishCommit(dataRepo, commit1.Branch.Name, commit1.ID))

	commitInfos, err := c.WaitCommitSetAll(commit1.ID)
	require.NoError(t, err)
	require.Equal(t, 4, len(commitInfos))

	outputCommit := client.NewCommit(pipeline, "master", commit1.ID)

	for i := 0; i < numFiles; i++ {
		var buf bytes.Buffer
		require.NoError(t, c.GetFile(outputCommit, fmt.Sprintf("file-%d", i), &buf))
		require.Equal(t, fmt.Sprintf("%d", i), buf.String())
	}
}

func TestService(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}
	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())

	dataRepo := tu.UniqueString("TestService_data")
	require.NoError(t, c.CreateRepo(dataRepo))

	commit1, err := c.StartCommit(dataRepo, "master")
	require.NoError(t, err)
	require.NoError(t, c.PutFile(commit1, "file1", strings.NewReader("foo"), client.WithAppendPutFile()))
	require.NoError(t, c.FinishCommit(dataRepo, commit1.Branch.Name, commit1.ID))

	annotations := map[string]string{"foo": "bar"}

	pipeline := tu.UniqueString("pipelineservice")
	// This pipeline sleeps for 10 secs per datum
	require.NoError(t, c.CreatePipelineService(
		pipeline,
		"trinitronx/python-simplehttpserver",
		[]string{"sh"},
		[]string{
			"cd /pfs",
			"exec python -m SimpleHTTPServer 8000",
		},
		&pps.ParallelismSpec{
			Constant: 1,
		},
		client.NewPFSInput(dataRepo, "/"),
		false,
		8000,
		31800,
		annotations,
	))
	time.Sleep(10 * time.Second)

	// Lookup the address for 'pipelineservice' (different inside vs outside k8s)
	serviceAddr := func() string {
		// Hack: detect if running inside the cluster by looking for this env var
		if _, ok := os.LookupEnv("KUBERNETES_PORT"); !ok {
			// Outside cluster: Re-use external IP and external port defined above
			host := c.GetAddress().Host
			return net.JoinHostPort(host, "31800")
		}
		// Get k8s service corresponding to pachyderm service above--must access
		// via internal cluster IP, but we don't know what that is
		var address string
		kubeClient := tu.GetKubeClient(t)
		backoff.Retry(func() error {
			svcs, err := kubeClient.CoreV1().Services("default").List(metav1.ListOptions{})
			require.NoError(t, err)
			for _, svc := range svcs.Items {
				// Pachyderm actually generates two services for pipelineservice: one
				// for pachyderm (a ClusterIP service) and one for the user container
				// (a NodePort service, which is the one we want)
				rightName := strings.Contains(svc.Name, "pipelineservice")
				rightType := svc.Spec.Type == v1.ServiceTypeNodePort
				if !rightName || !rightType {
					continue
				}
				host := svc.Spec.ClusterIP
				port := fmt.Sprintf("%d", svc.Spec.Ports[0].Port)
				address = net.JoinHostPort(host, port)

				actualAnnotations := svc.Annotations
				delete(actualAnnotations, "pipelineName")
				if !reflect.DeepEqual(actualAnnotations, annotations) {
					return errors.Errorf(
						"expected service annotations map %#v, got %#v",
						annotations,
						actualAnnotations,
					)
				}

				return nil
			}
			return errors.Errorf("no matching k8s service found")
		}, backoff.NewTestingBackOff())

		require.NotEqual(t, "", address)
		return address
	}()

	httpClient := &http.Client{
		Timeout: 3 * time.Second,
	}
	require.NoError(t, backoff.Retry(func() error {
		resp, err := httpClient.Get(fmt.Sprintf("http://%s/%s/file1", serviceAddr, dataRepo))
		if err != nil {
			return err
		}
		if resp.StatusCode != 200 {
			return errors.Errorf("GET returned %d", resp.StatusCode)
		}
		content, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		if string(content) != "foo" {
			return errors.Errorf("wrong content for file1: expected foo, got %s", string(content))
		}
		return nil
	}, backoff.NewTestingBackOff()))

}

func TestServiceEnvVars(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}
	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())

	dataRepo := tu.UniqueString(t.Name() + "-input")
	require.NoError(t, c.CreateRepo(dataRepo))

	require.NoError(t, c.PutFile(client.NewCommit(dataRepo, "master", ""), "file1", strings.NewReader("foo"), client.WithAppendPutFile()))

	pipeline := tu.UniqueString("pipelineservice")
	_, err := c.PpsAPIClient.CreatePipeline(
		c.Ctx(),
		&pps.CreatePipelineRequest{
			Pipeline: client.NewPipeline(pipeline),
			Transform: &pps.Transform{
				Image: "trinitronx/python-simplehttpserver",
				Cmd:   []string{"sh"},
				Stdin: []string{
					"echo ${CUSTOM_ENV_VAR} >/pfs/custom_env_var",
					"cd /pfs",
					"exec python -m SimpleHTTPServer 8000",
				},
				Env: map[string]string{
					"CUSTOM_ENV_VAR": "custom-value",
				},
			},
			ParallelismSpec: &pps.ParallelismSpec{
				Constant: 1,
			},
			Input:  client.NewPFSInput(dataRepo, "/"),
			Update: false,
			Service: &pps.Service{
				InternalPort: 8000,
				ExternalPort: 31800,
			},
		})
	require.NoError(t, err)

	// Lookup the address for 'pipelineservice' (different inside vs outside k8s)
	serviceAddr := func() string {
		// Hack: detect if running inside the cluster by looking for this env var
		if _, ok := os.LookupEnv("KUBERNETES_PORT"); !ok {
			// Outside cluster: Re-use external IP and external port defined above
			host := c.GetAddress().Host
			return net.JoinHostPort(host, "31800")
		}
		// Get k8s service corresponding to pachyderm service above--must access
		// via internal cluster IP, but we don't know what that is
		var address string
		kubeClient := tu.GetKubeClient(t)
		backoff.Retry(func() error {
			svcs, err := kubeClient.CoreV1().Services("default").List(metav1.ListOptions{})
			require.NoError(t, err)
			for _, svc := range svcs.Items {
				// Pachyderm actually generates two services for pipelineservice: one
				// for pachyderm (a ClusterIP service) and one for the user container
				// (a NodePort service, which is the one we want)
				rightName := strings.Contains(svc.Name, "pipelineservice")
				rightType := svc.Spec.Type == v1.ServiceTypeNodePort
				if !rightName || !rightType {
					continue
				}
				host := svc.Spec.ClusterIP
				port := fmt.Sprintf("%d", svc.Spec.Ports[0].Port)
				address = net.JoinHostPort(host, port)
				return nil
			}
			return fmt.Errorf("no matching k8s service found")
		}, backoff.NewTestingBackOff())

		require.NotEqual(t, "", address)
		return address
	}()

	var envValue []byte
	require.NoErrorWithinTRetry(t, 2*time.Minute, func() error {
		httpC := http.Client{
			Timeout: 3 * time.Second, // fail fast
		}
		resp, err := httpC.Get(fmt.Sprintf("http://%s/custom_env_var", serviceAddr))
		if err != nil {
			// sleep => don't spam retries. Seems to make test less flaky
			time.Sleep(time.Second)
			return err
		}
		if resp.StatusCode != 200 {
			return fmt.Errorf("GET returned %d", resp.StatusCode)
		}
		envValue, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		return nil
	})
	require.Equal(t, "custom-value", strings.TrimSpace(string(envValue)))
}

func TestDatumSetSpec(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())

	dataRepo := tu.UniqueString("TestDatumSetSpec_data")
	require.NoError(t, c.CreateRepo(dataRepo))

	commit1, err := c.StartCommit(dataRepo, "master")
	require.NoError(t, err)
	numFiles := 101
	for i := 0; i < numFiles; i++ {
		require.NoError(t, c.PutFile(commit1, fmt.Sprintf("file%d", i), strings.NewReader("foo"), client.WithAppendPutFile()))
	}
	require.NoError(t, c.FinishCommit(dataRepo, commit1.Branch.Name, commit1.ID))

	t.Run("number", func(t *testing.T) {
		pipeline := tu.UniqueString("TestDatumSetSpec")
		c.PpsAPIClient.CreatePipeline(context.Background(),
			&pps.CreatePipelineRequest{
				Pipeline: client.NewPipeline(pipeline),
				Transform: &pps.Transform{
					Cmd: []string{"bash"},
					Stdin: []string{
						fmt.Sprintf("cp /pfs/%s/* /pfs/out/", dataRepo),
					},
				},
				Input:        client.NewPFSInput(dataRepo, "/*"),
				DatumSetSpec: &pps.DatumSetSpec{Number: 1},
			})

		commitInfo, err := c.WaitCommit(pipeline, "master", "")
		require.NoError(t, err)

		for i := 0; i < numFiles; i++ {
			var buf bytes.Buffer
			require.NoError(t, c.GetFile(commitInfo.Commit, fmt.Sprintf("file%d", i), &buf))
			require.Equal(t, "foo", buf.String())
		}
	})
	t.Run("size", func(t *testing.T) {
		pipeline := tu.UniqueString("TestDatumSetSpec")
		c.PpsAPIClient.CreatePipeline(context.Background(),
			&pps.CreatePipelineRequest{
				Pipeline: client.NewPipeline(pipeline),
				Transform: &pps.Transform{
					Cmd: []string{"bash"},
					Stdin: []string{
						fmt.Sprintf("cp /pfs/%s/* /pfs/out/", dataRepo),
					},
				},
				Input:        client.NewPFSInput(dataRepo, "/*"),
				DatumSetSpec: &pps.DatumSetSpec{SizeBytes: 5},
			})

		commitInfo, err := c.WaitCommit(pipeline, "master", "")
		require.NoError(t, err)

		for i := 0; i < numFiles; i++ {
			var buf bytes.Buffer
			require.NoError(t, c.GetFile(commitInfo.Commit, fmt.Sprintf("file%d", i), &buf))
			require.Equal(t, "foo", buf.String())
		}
	})
}

func TestLongDatums(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())

	dataRepo := tu.UniqueString("TestLongDatums_data")
	require.NoError(t, c.CreateRepo(dataRepo))

	pipeline := tu.UniqueString("TestLongDatums")
	require.NoError(t, c.CreatePipeline(
		pipeline,
		"",
		[]string{"bash"},
		[]string{
			"sleep 2s",
			fmt.Sprintf("cp /pfs/%s/* /pfs/out/", dataRepo),
		},
		&pps.ParallelismSpec{
			Constant: 4,
		},
		client.NewPFSInput(dataRepo, "/*"),
		"",
		false,
	))

	commit1, err := c.StartCommit(dataRepo, "master")
	require.NoError(t, err)
	numFiles := 8
	for i := 0; i < numFiles; i++ {
		require.NoError(t, c.PutFile(commit1, fmt.Sprintf("file%d", i), strings.NewReader("foo"), client.WithAppendPutFile()))
	}
	require.NoError(t, c.FinishCommit(dataRepo, commit1.Branch.Name, commit1.ID))

	commitInfos, err := c.WaitCommitSetAll(commit1.ID)
	require.NoError(t, err)
	require.Equal(t, 4, len(commitInfos))

	outputCommit := client.NewCommit(pipeline, "master", commit1.ID)

	for i := 0; i < numFiles; i++ {
		var buf bytes.Buffer
		require.NoError(t, c.GetFile(outputCommit, fmt.Sprintf("file%d", i), &buf))
		require.Equal(t, "foo", buf.String())
	}
}

func TestPipelineWithDatumTimeout(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())

	dataRepo := tu.UniqueString("TestPipelineWithDatumTimeout_data")
	require.NoError(t, c.CreateRepo(dataRepo))

	commit1, err := c.StartCommit(dataRepo, "master")
	require.NoError(t, err)
	require.NoError(t, c.PutFile(commit1, "file", strings.NewReader("foo"), client.WithAppendPutFile()))
	require.NoError(t, c.FinishCommit(dataRepo, commit1.Branch.Name, commit1.ID))
	timeout := 20
	pipeline := tu.UniqueString("pipeline")
	duration, err := time.ParseDuration(fmt.Sprintf("%vs", timeout))
	require.NoError(t, err)
	_, err = c.PpsAPIClient.CreatePipeline(
		context.Background(),
		&pps.CreatePipelineRequest{
			Pipeline: client.NewPipeline(pipeline),
			Transform: &pps.Transform{
				Cmd: []string{"bash"},
				Stdin: []string{
					"while true; do sleep 1; date; done",
					fmt.Sprintf("cp /pfs/%s/* /pfs/out/", dataRepo),
				},
			},
			Input:        client.NewPFSInput(dataRepo, "/*"),
			DatumTimeout: types.DurationProto(duration),
		},
	)
	require.NoError(t, err)

	jobs, err := c.ListJob(pipeline, nil, -1, true)
	require.NoError(t, err)
	require.Equal(t, 1, len(jobs))
	// Block on the job being complete before we call ListDatum
	jobInfo, err := c.WaitJob(jobs[0].Job.Pipeline.Name, jobs[0].Job.ID, false)
	require.NoError(t, err)
	require.Equal(t, pps.JobState_JOB_FAILURE, jobInfo.State)

	// Now validate the datum timed out properly
	dis, err := c.ListDatumAll(jobs[0].Job.Pipeline.Name, jobs[0].Job.ID)
	require.NoError(t, err)
	require.Equal(t, 1, len(dis))

	datum, err := c.InspectDatum(jobs[0].Job.Pipeline.Name, jobs[0].Job.ID, dis[0].Datum.ID)
	require.NoError(t, err)
	require.Equal(t, pps.DatumState_FAILED, datum.State)
	// ProcessTime looks like "20 seconds"
	tokens := strings.Split(pretty.Duration(datum.Stats.ProcessTime), " ")
	require.Equal(t, 2, len(tokens))
	seconds, err := strconv.Atoi(tokens[0])
	require.NoError(t, err)
	require.Equal(t, timeout, seconds)
}

func TestListDatumDuringJob(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())

	dataRepo := tu.UniqueString("TestListDatumDuringJob_data")
	require.NoError(t, c.CreateRepo(dataRepo))

	commit1, err := c.StartCommit(dataRepo, "master")
	require.NoError(t, err)

	fileCount := 10
	for i := 0; i < fileCount; i++ {
		fileName := "file" + strconv.Itoa(i)
		require.NoError(t, c.PutFile(commit1, fileName, strings.NewReader("foo")))
	}

	require.NoError(t, c.FinishCommit(dataRepo, commit1.Branch.Name, commit1.ID))
	timeout := 20
	pipeline := tu.UniqueString("TestListDatumDuringJob_pipeline")
	duration, err := time.ParseDuration(fmt.Sprintf("%vs", timeout))
	require.NoError(t, err)
	_, err = c.PpsAPIClient.CreatePipeline(
		context.Background(),
		&pps.CreatePipelineRequest{
			Pipeline: client.NewPipeline(pipeline),
			Transform: &pps.Transform{
				Cmd: []string{"bash"},
				Stdin: []string{
					"sleep 5;",
					fmt.Sprintf("cp /pfs/%s/* /pfs/out/", dataRepo),
				},
			},
			Input:        client.NewPFSInput(dataRepo, "/*"),
			DatumTimeout: types.DurationProto(duration),
			DatumSetSpec: &pps.DatumSetSpec{
				Number: 2, // since we set the DatumSetSpec number to 2, we expect our datums to be processed in 5 datum sets (10 files / 2 files per set)
			},
		},
	)
	require.NoError(t, err)

	var jobInfo *pps.JobInfo
	require.NoErrorWithinT(t, 30*time.Second, func() error {
		return backoff.Retry(func() error {
			jobInfos, err := c.ListJob(pipeline, nil, -1, true)
			if err != nil {
				return err
			}
			if len(jobInfos) != 1 {
				return errors.Errorf("Expected one job, but got %d: %v", len(jobInfos), jobInfos)
			}
			jobInfo = jobInfos[0]
			return nil
		}, backoff.NewTestingBackOff())
	})

	// initially since no datum chunks have been processed, we receive 0 datums
	dis, err := c.ListDatumAll(jobInfo.Job.Pipeline.Name, jobInfo.Job.ID)
	require.NoError(t, err)
	require.Equal(t, 0, len(dis))

	// test job progress by waiting until some datums are returned, and verify that it's not all of them
	require.NoErrorWithinT(t, 60*time.Second, func() error {
		return backoff.Retry(func() error {
			dis, err = c.ListDatumAll(jobInfo.Job.Pipeline.Name, jobInfo.Job.ID)
			if err != nil {
				return err
			}
			if len(dis) == 0 {
				return errors.Errorf("expected some datums to be listed")
			}
			return nil
		}, backoff.NewTestingBackOff())
	})

	// fewer than all the datums have been processed
	require.True(t, len(dis) < fileCount)

	// wait until all datums are processed
	_, err = c.WaitCommitSetAll(jobInfo.Job.ID)
	require.NoError(t, err)

	dis, err = c.ListDatumAll(jobInfo.Job.Pipeline.Name, jobInfo.Job.ID)
	require.NoError(t, err)
	require.Equal(t, fileCount, len(dis))
}

func TestPipelineWithDatumTimeoutControl(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())

	dataRepo := tu.UniqueString("TestPipelineWithDatumTimeoutControl_data")
	require.NoError(t, c.CreateRepo(dataRepo))

	commit1, err := c.StartCommit(dataRepo, "master")
	require.NoError(t, err)
	require.NoError(t, c.PutFile(commit1, "file", strings.NewReader("foo"), client.WithAppendPutFile()))
	require.NoError(t, c.FinishCommit(dataRepo, commit1.Branch.Name, commit1.ID))

	timeout := 20
	pipeline := tu.UniqueString("pipeline")
	duration, err := time.ParseDuration(fmt.Sprintf("%vs", timeout))
	require.NoError(t, err)
	_, err = c.PpsAPIClient.CreatePipeline(
		context.Background(),
		&pps.CreatePipelineRequest{
			Pipeline: client.NewPipeline(pipeline),
			Transform: &pps.Transform{
				Cmd: []string{"bash"},
				Stdin: []string{
					fmt.Sprintf("sleep %v", timeout-10),
					fmt.Sprintf("cp /pfs/%s/* /pfs/out/", dataRepo),
				},
			},
			Input:        client.NewPFSInput(dataRepo, "/*"),
			DatumTimeout: types.DurationProto(duration),
		},
	)
	require.NoError(t, err)

	commitInfo, err := c.InspectCommit(pipeline, "master", "")
	require.NoError(t, err)
	commitInfos, err := c.WaitCommitSetAll(commitInfo.Commit.ID)
	require.NoError(t, err)
	require.Equal(t, 4, len(commitInfos))

	jobs, err := c.ListJob(pipeline, nil, -1, true)
	require.NoError(t, err)
	require.Equal(t, 1, len(jobs))

	// Block on the job being complete before we call ListDatum
	jobInfo, err := c.WaitJob(jobs[0].Job.Pipeline.Name, jobs[0].Job.ID, false)
	require.NoError(t, err)
	require.Equal(t, pps.JobState_JOB_SUCCESS, jobInfo.State)
}

func TestPipelineWithJobTimeout(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())

	dataRepo := tu.UniqueString("TestPipelineWithDatumTimeout_data")
	require.NoError(t, c.CreateRepo(dataRepo))

	commit1, err := c.StartCommit(dataRepo, "master")
	require.NoError(t, err)
	numFiles := 2
	for i := 0; i < numFiles; i++ {
		require.NoError(t, c.PutFile(commit1, fmt.Sprintf("file-%v", i), strings.NewReader("foo"), client.WithAppendPutFile()))
	}
	require.NoError(t, c.FinishCommit(dataRepo, commit1.Branch.Name, commit1.ID))
	timeout := 20
	pipeline := tu.UniqueString("pipeline")
	duration, err := time.ParseDuration(fmt.Sprintf("%vs", timeout))
	require.NoError(t, err)
	_, err = c.PpsAPIClient.CreatePipeline(
		context.Background(),
		&pps.CreatePipelineRequest{
			Pipeline: client.NewPipeline(pipeline),
			Transform: &pps.Transform{
				Cmd: []string{"bash"},
				Stdin: []string{
					fmt.Sprintf("sleep %v", timeout), // we have 2 datums, so the total exec time will more than double the timeout value
					fmt.Sprintf("cp /pfs/%s/* /pfs/out/", dataRepo),
				},
			},
			Input:      client.NewPFSInput(dataRepo, "/*"),
			JobTimeout: types.DurationProto(duration),
		},
	)
	require.NoError(t, err)

	// Wait for the job to get scheduled / appear in listjob
	// A sleep of 15s is insufficient
	time.Sleep(25 * time.Second)
	jobs, err := c.ListJob(pipeline, nil, -1, true)
	require.NoError(t, err)
	require.Equal(t, 1, len(jobs))

	// Block on the job being complete before we call ListDatum
	jobInfo, err := c.WaitJob(jobs[0].Job.Pipeline.Name, jobs[0].Job.ID, false)
	require.NoError(t, err)
	require.Equal(t, pps.JobState_JOB_KILLED.String(), jobInfo.State.String())
	started, err := types.TimestampFromProto(jobInfo.Started)
	require.NoError(t, err)
	finished, err := types.TimestampFromProto(jobInfo.Finished)
	require.NoError(t, err)
	require.True(t, math.Abs((finished.Sub(started)-(time.Second*20)).Seconds()) <= 1.0)
}

func TestCommitDescription(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	dataRepo := tu.UniqueString("TestCommitDescription")
	require.NoError(t, c.CreateRepo(dataRepo))

	// Test putting a message in StartCommit
	commit, err := c.PfsAPIClient.StartCommit(ctx, &pfs.StartCommitRequest{
		Branch:      client.NewBranch(dataRepo, "master"),
		Description: "test commit description in 'start commit'",
	})
	require.NoError(t, err)
	c.FinishCommit(dataRepo, commit.Branch.Name, commit.ID)
	commitInfo, err := c.InspectCommit(dataRepo, commit.Branch.Name, commit.ID)
	require.NoError(t, err)
	require.Equal(t, "test commit description in 'start commit'", commitInfo.Description)
	require.NoError(t, pfspretty.PrintDetailedCommitInfo(os.Stdout, pfspretty.NewPrintableCommitInfo(commitInfo)))

	// Test putting a message in FinishCommit
	commit, err = c.StartCommit(dataRepo, "master")
	require.NoError(t, err)
	c.PfsAPIClient.FinishCommit(ctx, &pfs.FinishCommitRequest{
		Commit:      commit,
		Description: "test commit description in 'finish commit'",
	})
	commitInfo, err = c.InspectCommit(dataRepo, commit.Branch.Name, commit.ID)
	require.NoError(t, err)
	require.Equal(t, "test commit description in 'finish commit'", commitInfo.Description)
	require.NoError(t, pfspretty.PrintDetailedCommitInfo(os.Stdout, pfspretty.NewPrintableCommitInfo(commitInfo)))

	// Test overwriting a commit message
	commit, err = c.PfsAPIClient.StartCommit(ctx, &pfs.StartCommitRequest{
		Branch:      client.NewBranch(dataRepo, "master"),
		Description: "test commit description in 'start commit'",
	})
	require.NoError(t, err)
	c.PfsAPIClient.FinishCommit(ctx, &pfs.FinishCommitRequest{
		Commit:      commit,
		Description: "test commit description in 'finish commit' that overwrites",
	})
	commitInfo, err = c.InspectCommit(dataRepo, commit.Branch.Name, commit.ID)
	require.NoError(t, err)
	require.Equal(t, "test commit description in 'finish commit' that overwrites", commitInfo.Description)
	require.NoError(t, pfspretty.PrintDetailedCommitInfo(os.Stdout, pfspretty.NewPrintableCommitInfo(commitInfo)))
}

func TestPipelineDescription(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())

	dataRepo := tu.UniqueString("TestPipelineDescription_data")
	require.NoError(t, c.CreateRepo(dataRepo))

	description := "pipeline description"
	pipeline := tu.UniqueString("TestPipelineDescription")
	_, err := c.PpsAPIClient.CreatePipeline(
		context.Background(),
		&pps.CreatePipelineRequest{
			Pipeline:    client.NewPipeline(pipeline),
			Transform:   &pps.Transform{Cmd: []string{"true"}},
			Description: description,
			Input:       client.NewPFSInput(dataRepo, "/"),
		})
	require.NoError(t, err)
	pi, err := c.InspectPipeline(pipeline, true)
	require.NoError(t, err)
	require.Equal(t, description, pi.Details.Description)
}

func TestListJobInputCommits(t *testing.T) {
	// TODO(optional 2.0): listing jobs by their input commits is not supported in 2.0
	t.Skip("Listing jobs by their input commits is not supported in 2.0")
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())

	aRepo := tu.UniqueString("TestListJobInputCommits_data_a")
	require.NoError(t, c.CreateRepo(aRepo))
	bRepo := tu.UniqueString("TestListJobInputCommits_data_b")
	require.NoError(t, c.CreateRepo(bRepo))

	pipeline := tu.UniqueString("TestListJobInputCommits")
	require.NoError(t, c.CreatePipeline(
		pipeline,
		"",
		[]string{"bash"},
		[]string{
			fmt.Sprintf("cp /pfs/%s/* /pfs/out/", aRepo),
			fmt.Sprintf("cp /pfs/%s/* /pfs/out/", bRepo),
		},
		&pps.ParallelismSpec{
			Constant: 1,
		},
		client.NewCrossInput(
			client.NewPFSInput(aRepo, "/*"),
			client.NewPFSInput(bRepo, "/*"),
		),
		"",
		false,
	))

	commita1, err := c.StartCommit(aRepo, "master")
	require.NoError(t, err)
	require.NoError(t, c.PutFile(commita1, "file", strings.NewReader("foo"), client.WithAppendPutFile()))
	require.NoError(t, c.FinishCommit(aRepo, "master", ""))

	commitb1, err := c.StartCommit(bRepo, "master")
	require.NoError(t, err)
	require.NoError(t, c.PutFile(commitb1, "file", strings.NewReader("foo"), client.WithAppendPutFile()))
	require.NoError(t, c.FinishCommit(bRepo, "master", ""))

	commitInfos, err := c.WaitCommitSetAll(commitb1.ID)
	require.NoError(t, err)
	require.Equal(t, 5, len(commitInfos))

	commita2, err := c.StartCommit(aRepo, "master")
	require.NoError(t, err)
	require.NoError(t, c.PutFile(commita2, "file", strings.NewReader("bar"), client.WithAppendPutFile()))
	require.NoError(t, c.FinishCommit(aRepo, "master", ""))

	commitInfos, err = c.WaitCommitSetAll(commita2.ID)
	require.NoError(t, err)
	require.Equal(t, 5, len(commitInfos))

	commitb2, err := c.StartCommit(bRepo, "master")
	require.NoError(t, err)
	require.NoError(t, c.PutFile(commitb2, "file", strings.NewReader("bar"), client.WithAppendPutFile()))
	require.NoError(t, c.FinishCommit(bRepo, "master", ""))

	commitInfos, err = c.WaitCommitSetAll(commitb2.ID)
	require.NoError(t, err)
	require.Equal(t, 5, len(commitInfos))

	jobInfos, err := c.ListJob("", []*pfs.Commit{commita1}, -1, true)
	require.NoError(t, err)
	require.Equal(t, 2, len(jobInfos)) // a1 + nil and a1 + b1

	jobInfos, err = c.ListJob("", []*pfs.Commit{commitb1}, -1, true)
	require.NoError(t, err)
	require.Equal(t, 2, len(jobInfos)) // a1 + b1 and a2 + b1

	jobInfos, err = c.ListJob("", []*pfs.Commit{commita2}, -1, true)
	require.NoError(t, err)
	require.Equal(t, 2, len(jobInfos)) // a2 + b1 and a2 + b2

	jobInfos, err = c.ListJob("", []*pfs.Commit{commitb2}, -1, true)
	require.NoError(t, err)
	require.Equal(t, 1, len(jobInfos)) // a2 + b2

	jobInfos, err = c.ListJob("", []*pfs.Commit{commita1, commitb1}, -1, true)
	require.NoError(t, err)
	require.Equal(t, 1, len(jobInfos))

	jobInfos, err = c.ListJob("", []*pfs.Commit{commita2, commitb1}, -1, true)
	require.NoError(t, err)
	require.Equal(t, 1, len(jobInfos))

	jobInfos, err = c.ListJob("", []*pfs.Commit{commita2, commitb2}, -1, true)
	require.NoError(t, err)
	require.Equal(t, 1, len(jobInfos))

	jobInfos, err = c.ListJob("", []*pfs.Commit{client.NewCommit(aRepo, "master", ""), client.NewCommit(bRepo, "master", "")}, -1, true)
	require.NoError(t, err)
	require.Equal(t, 1, len(jobInfos))
}

// TestCancelJob creates a long-running job and then kills it, testing
// that the user process is killed.
func TestCancelJob(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())

	// Create an input repo
	repo := tu.UniqueString("TestCancelJob")
	require.NoError(t, c.CreateRepo(repo))

	// Create an input commit
	commit, err := c.StartCommit(repo, "master")
	require.NoError(t, err)
	require.NoError(t, c.PutFile(commit, "/time", strings.NewReader("600"), client.WithAppendPutFile()))
	require.NoError(t, c.PutFile(commit, "/data", strings.NewReader("commit data"), client.WithAppendPutFile()))
	require.NoError(t, c.FinishCommit(repo, commit.Branch.Name, commit.ID))

	// Create sleep + copy pipeline
	pipeline := tu.UniqueString("pipeline")
	require.NoError(t, c.CreatePipeline(
		pipeline,
		"",
		[]string{"bash"},
		[]string{
			"sleep `cat /pfs/*/time`",
			"cp /pfs/*/data /pfs/out/",
		},
		&pps.ParallelismSpec{
			Constant: 1,
		},
		client.NewPFSInput(repo, "/"),
		"",
		false,
	))

	// Wait until PPS has started processing commit
	var jobInfo *pps.JobInfo
	require.NoErrorWithinT(t, 30*time.Second, func() error {
		return backoff.Retry(func() error {
			jobInfos, err := c.ListJob(pipeline, nil, -1, true)
			if err != nil {
				return err
			}
			if len(jobInfos) != 1 {
				return errors.Errorf("Expected one job, but got %d: %v", len(jobInfos), jobInfos)
			}
			jobInfo = jobInfos[0]
			return nil
		}, backoff.NewTestingBackOff())
	})

	// stop the job
	require.NoError(t, c.StopJob(jobInfo.Job.Pipeline.Name, jobInfo.Job.ID))

	// Wait until the job is cancelled
	require.NoErrorWithinT(t, 30*time.Second, func() error {
		return backoff.Retry(func() error {
			updatedJobInfo, err := c.InspectJob(jobInfo.Job.Pipeline.Name, jobInfo.Job.ID, false)
			if err != nil {
				return err
			}
			if updatedJobInfo.State != pps.JobState_JOB_KILLED {
				return errors.Errorf("job %s is still running, but should be KILLED", jobInfo.Job.ID)
			}
			return nil
		}, backoff.NewTestingBackOff())
	})

	// Create one more commit to make sure the pipeline can still process input
	// commits
	commit2, err := c.StartCommit(repo, "master")
	require.NoError(t, err)
	require.NoError(t, c.DeleteFile(commit2, "/time"))
	require.NoError(t, c.PutFile(commit2, "/time", strings.NewReader("1"), client.WithAppendPutFile()))
	require.NoError(t, c.DeleteFile(commit2, "/data"))
	require.NoError(t, c.PutFile(commit2, "/data", strings.NewReader("commit 2 data"), client.WithAppendPutFile()))
	require.NoError(t, c.FinishCommit(repo, commit2.Branch.Name, commit2.ID))

	// Flush commit2, and make sure the output is as expected
	commitInfo, err := c.WaitCommit(pipeline, "master", commit2.ID)
	require.NoError(t, err)

	buf := bytes.Buffer{}
	err = c.GetFile(commitInfo.Commit, "/data", &buf)
	require.NoError(t, err)
	require.Equal(t, "commit 2 data", buf.String())
}

// TestCancelManyJobs creates many jobs to test that the handling of many
// incoming job events is correct. Each job comes up (which tests that that
// cancelling job 'a' does not cancel subsequent job 'b'), must be the only job
// running (which tests that only one job can run at a time), and then is
// cancelled.
func TestCancelManyJobs(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())

	// Create an input repo
	repo := tu.UniqueString("TestCancelManyJobs")
	require.NoError(t, c.CreateRepo(repo))

	// Create sleep pipeline
	pipeline := tu.UniqueString("pipeline")
	require.NoError(t, c.CreatePipeline(
		pipeline,
		"",
		[]string{"sleep", "600"},
		nil,
		&pps.ParallelismSpec{
			Constant: 1,
		},
		client.NewPFSInput(repo, "/*"),
		"",
		false,
	))

	// Create 10 input commits, to spawn 10 jobs
	var commits []*pfs.Commit
	for i := 0; i < 10; i++ {
		commit, err := c.StartCommit(repo, "master")
		require.NoError(t, c.PutFile(commit, "file", strings.NewReader("foo")))
		require.NoError(t, err)
		require.NoError(t, c.FinishCommit(repo, commit.Branch.Name, commit.ID))
		commits = append(commits, commit)
	}

	// For each expected job: watch to make sure the input job comes up, make
	// sure that it's the only job running, then cancel it
	for _, commit := range commits {
		// Wait until PPS has started processing commit
		var jobInfo *pps.JobInfo
		require.NoErrorWithinT(t, 30*time.Second, func() error {
			return backoff.Retry(func() error {
				jobInfos, err := c.ListJob(pipeline, []*pfs.Commit{commit}, -1, true)
				if err != nil {
					return err
				}
				if len(jobInfos) != 1 {
					return errors.Errorf("Expected one job, but got %d: %v", len(jobInfos), jobInfos)
				}
				jobInfo = jobInfos[0]
				return nil
			}, backoff.NewTestingBackOff())
		})

		// Stop the job
		require.NoError(t, c.StopJob(jobInfo.Job.Pipeline.Name, jobInfo.Job.ID))

		// Check that the job is now killed
		require.NoErrorWithinT(t, 30*time.Second, func() error {
			return backoff.Retry(func() error {
				// TODO(msteffen): once github.com/pachyderm/pachyderm/v2/pull/2642 is
				// submitted, change ListJob here to filter on commit1 as the input commit,
				// rather than inspecting the input in the test
				updatedJobInfo, err := c.InspectJob(jobInfo.Job.Pipeline.Name, jobInfo.Job.ID, false)
				if err != nil {
					return err
				}
				if updatedJobInfo.State != pps.JobState_JOB_KILLED {
					return errors.Errorf("job %s is still running, but should be KILLED", jobInfo.Job.ID)
				}
				return nil
			}, backoff.NewTestingBackOff())
		})
	}
}

// TestSquashCommitSetPropagation deletes an input commit and makes sure all
// downstream commits are also deleted.
// DAG in this test: repo -> pipeline[0] -> pipeline[1]
func TestSquashCommitSetPropagation(t *testing.T) {
	// TODO(2.0 optional): Implement put file split in V2.
	t.Skip("Put file split not implemented in V2")
	// 	if testing.Short() {
	// 		t.Skip("Skipping integration tests in short mode")
	// 	}

	// 	c := tu.GetPachClient(t)
	// 	require.NoError(t, c.DeleteAll())

	// 	// Create an input repo
	// 	repo := tu.UniqueString("TestSquashCommitSetPropagation")
	// 	require.NoError(t, c.CreateRepo(repo))
	// 	_, err := c.PutFileSplit(repo, "master", "d", pfs.Delimiter_SQL, 0, 0, 0, false,
	// 		strings.NewReader(tu.TestPGDump))
	// 	require.NoError(t, err)

	// 	// Create a pipeline that roughly validates the header
	// 	pipeline := tu.UniqueString("TestSplitFileReprocessPL")
	// 	require.NoError(t, c.CreatePipeline(
	// 		pipeline,
	// 		"",
	// 		[]string{"/bin/bash"},
	// 		[]string{
	// 			`ls /pfs/*/d/*`, // for debugging
	// 			`cars_tables="$(grep "CREATE TABLE public.cars" /pfs/*/d/* | sort -u  | wc -l)"`,
	// 			`(( cars_tables == 1 )) && exit 0 || exit 1`,
	// 		},
	// 		&pps.ParallelismSpec{Constant: 1},
	// 		client.NewPFSInput(repo, "/d/*"),
	// 		"",
	// 		false,
	// 	))

	// 	// wait for job to run & check that all rows were processed
	// 	var jobCount int
	// 	c.FlushJob([]*pfs.Commit{client.NewCommit(repo, "master")}, nil,
	// 		func(jobInfo *pps.JobInfo) error {
	// 			jobCount++
	// 			require.Equal(t, 1, jobCount)
	// 			require.Equal(t, pps.JobState_JOB_SUCCESS, jobInfo.State)
	// 			require.Equal(t, int64(5), jobInfo.DataProcessed)
	// 			require.Equal(t, int64(0), jobInfo.DataSkipped)
	// 			return nil
	// 		})

	// 	// put empty dataset w/ new header
	// 	_, err = c.PutFileSplit(repo, "master", "d", pfs.Delimiter_SQL, 0, 0, 0, false,
	// 		strings.NewReader(tu.TestPGDumpNewHeader))
	// 	require.NoError(t, err)

	// 	// everything gets reprocessed (hashes all change even though the files
	// 	// themselves weren't altered)
	// 	jobCount = 0
	// 	c.FlushJob([]*pfs.Commit{client.NewCommit(repo, "master")}, nil,
	// 		func(jobInfo *pps.JobInfo) error {
	// 			jobCount++
	// 			require.Equal(t, 1, jobCount)
	// 			require.Equal(t, pps.JobState_JOB_SUCCESS, jobInfo.State)
	// 			require.Equal(t, int64(5), jobInfo.DataProcessed) // added 3 new rows
	// 			require.Equal(t, int64(0), jobInfo.DataSkipped)
	// 			return nil
	// 		})
}

// TestSquashCommitSetRunsJob creates an input repo, commits several times, and
// then creates a pipeline. Creating the pipeline will spawn a job and while
// that job is running, this test deletes the HEAD commit of the input branch,
// which deletes the job's output commit and cancels the job. This should start
// another job that processes the original input HEAD commit's parent.
func TestSquashCommitSetRunsJob(t *testing.T) {
	// TODO(required 2.0): this test depends on old squash commit behavior and may
	// not be possible to make work with global ids.  If we change squashing
	// commits to also squash aliased commitsets, this may make sense again.
	t.Skip("Squashing commitsets does not make jobs like this with global IDs")
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}
	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())

	// Create an input repo
	repo := tu.UniqueString("TestSquashCommitSetRunsJob")
	require.NoError(t, c.CreateRepo(repo))

	// Create two input commits. The input commit has two files: 'time' which
	// determines how long the processing job runs for, and 'data' which
	// determines the job's output. This ensures that the first job (processing
	// the second commit) runs for a long time, making it easy to cancel, while
	// the second job runs quickly, ensuring that the test finishes quickly
	commit1, err := c.StartCommit(repo, "master")
	require.NoError(t, err)
	require.NoError(t, c.PutFile(commit1, "/time", strings.NewReader("1"), client.WithAppendPutFile()))
	require.NoError(t, c.PutFile(commit1, "/data", strings.NewReader("commit 1 data"), client.WithAppendPutFile()))
	require.NoError(t, c.FinishCommit(repo, commit1.Branch.Name, commit1.ID))

	commit2, err := c.StartCommit(repo, "master")
	require.NoError(t, err)
	require.NoError(t, c.DeleteFile(commit2, "/time"))
	require.NoError(t, c.PutFile(commit2, "/time", strings.NewReader("600"), client.WithAppendPutFile()))
	require.NoError(t, c.DeleteFile(commit2, "/data"))
	require.NoError(t, c.PutFile(commit2, "/data", strings.NewReader("commit 2 data"), client.WithAppendPutFile()))
	require.NoError(t, c.FinishCommit(repo, commit2.Branch.Name, commit2.ID))

	// Create sleep + copy pipeline
	pipeline := tu.UniqueString("pipeline")
	require.NoError(t, c.CreatePipeline(
		pipeline,
		"",
		[]string{"bash"},
		[]string{
			"sleep `cat /pfs/*/time`",
			"cp /pfs/*/data /pfs/out/",
		},
		&pps.ParallelismSpec{
			Constant: 1,
		},
		client.NewPFSInput(repo, "/"),
		"",
		false,
	))

	// Check that there are no jobs for commit1
	jobInfos, err := c.ListJob(pipeline, []*pfs.Commit{commit1}, -1, true)
	require.NoError(t, err)
	require.Equal(t, 0, len(jobInfos))

	// Check that PPS has generated a job for commit2
	jobInfos, err = c.ListJob(pipeline, []*pfs.Commit{commit2}, -1, true)
	require.NoError(t, err)
	require.Equal(t, 1, len(jobInfos))

	// Delete the second commit in the input repo
	require.NoError(t, c.SquashCommitSet(commit2.ID))

	// Check that PPS has generated a job for commit1
	jobInfos, err = c.ListJob(pipeline, []*pfs.Commit{commit1}, -1, true)
	require.NoError(t, err)
	require.Equal(t, 1, len(jobInfos))

	commitInfo, err := c.WaitCommit(pipeline, "master", "")
	require.NoError(t, err)

	// Check that the job processed the right data
	buf := bytes.Buffer{}
	err = c.GetFile(commitInfo.Commit, "/data", &buf)
	require.NoError(t, err)
	require.Equal(t, "commit 1 data", buf.String())

	// Create one more commit to make sure the pipeline can still process input
	// commits
	commit3, err := c.StartCommit(repo, "master")
	require.NoError(t, err)
	require.NoError(t, c.DeleteFile(commit3, "/data"))
	require.NoError(t, c.PutFile(commit3, "/data", strings.NewReader("commit 3 data"), client.WithAppendPutFile()))
	require.NoError(t, c.FinishCommit(repo, commit3.Branch.Name, commit3.ID))

	// Flush commit3, and make sure the output is as expected
	commitInfo, err = c.WaitCommit(pipeline, "master", commit3.ID)
	require.NoError(t, err)

	buf.Reset()
	err = c.GetFile(commitInfo.Commit, "/data", &buf)
	require.NoError(t, err)
	require.Equal(t, "commit 3 data", buf.String())
}

func TestDeleteSpecRepo(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	dataRepo := tu.UniqueString("TestDeleteSpecRepo_data")
	require.NoError(t, c.CreateRepo(dataRepo))

	pipeline := tu.UniqueString("TestSimplePipeline")
	require.NoError(t, c.CreatePipeline(
		pipeline,
		"",
		[]string{"echo", "foo"},
		nil,
		&pps.ParallelismSpec{
			Constant: 1,
		},
		client.NewPFSInput(dataRepo, "/"),
		"",
		false,
	))
	_, err := c.PfsAPIClient.DeleteRepo(
		c.Ctx(),
		&pfs.DeleteRepoRequest{
			Repo: client.NewSystemRepo(pipeline, pfs.SpecRepoType),
		})
	require.YesError(t, err)
}

func TestDontReadStdin(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	dataRepo := tu.UniqueString("TestDontReadStdin_data")
	require.NoError(t, c.CreateRepo(dataRepo))

	pipeline := tu.UniqueString("TestDontReadStdin")
	require.NoError(t, c.CreatePipeline(
		pipeline,
		"",
		[]string{"true"},
		[]string{"stdin that will never be read"},
		&pps.ParallelismSpec{
			Constant: 1,
		},
		client.NewPFSInput(dataRepo, "/"),
		"",
		false,
	))
	numCommits := 20
	for i := 0; i < numCommits; i++ {
		commit, err := c.StartCommit(dataRepo, "master")
		require.NoError(t, err)
		require.NoError(t, c.FinishCommit(dataRepo, "master", ""))
		jobInfos, err := c.WaitJobSetAll(commit.ID, false)
		require.NoError(t, err)
		require.Equal(t, 1, len(jobInfos))
		require.Equal(t, jobInfos[0].State.String(), pps.JobState_JOB_SUCCESS.String())
	}
}

func TestStatsDeleteAll(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())

	dataRepo := tu.UniqueString("TestPipelineWithStats_data")
	require.NoError(t, c.CreateRepo(dataRepo))

	pipeline := tu.UniqueString("pipeline")
	_, err := c.PpsAPIClient.CreatePipeline(context.Background(),
		&pps.CreatePipelineRequest{
			Pipeline: client.NewPipeline(pipeline),
			Transform: &pps.Transform{
				Cmd: []string{"cp", fmt.Sprintf("/pfs/%s/file", dataRepo), "/pfs/out"},
			},
			Input: client.NewPFSInput(dataRepo, "/"),
		})
	require.NoError(t, err)

	commit, err := c.StartCommit(dataRepo, "master")
	require.NoError(t, err)
	require.NoError(t, c.PutFile(commit, "file", strings.NewReader("foo\n"), client.WithAppendPutFile()))
	require.NoError(t, c.FinishCommit(dataRepo, commit.Branch.Name, commit.ID))

	jis, err := c.WaitJobSetAll(commit.ID, false)
	require.NoError(t, err)
	require.Equal(t, 1, len(jis))
	require.Equal(t, pps.JobState_JOB_SUCCESS.String(), jis[0].State.String())
	require.NoError(t, c.DeleteAll())

	require.NoError(t, c.CreateRepo(dataRepo))

	_, err = c.PpsAPIClient.CreatePipeline(context.Background(),
		&pps.CreatePipelineRequest{
			Pipeline: client.NewPipeline(pipeline),
			Transform: &pps.Transform{
				Cmd: []string{"cp", fmt.Sprintf("/pfs/%s/file", dataRepo), "/pfs/out"},
			},
			Input: client.NewPFSInput(dataRepo, "/*"),
		})
	require.NoError(t, err)

	commit, err = c.StartCommit(dataRepo, "master")
	require.NoError(t, err)
	require.NoError(t, c.PutFile(commit, "file", strings.NewReader("foo\n"), client.WithAppendPutFile()))
	require.NoError(t, c.FinishCommit(dataRepo, commit.Branch.Name, commit.ID))

	jis, err = c.WaitJobSetAll(commit.ID, false)
	require.NoError(t, err)
	require.Equal(t, 1, len(jis))
	require.Equal(t, pps.JobState_JOB_SUCCESS.String(), jis[0].State.String())
	require.NoError(t, c.DeleteAll())
}

func TestRapidUpdatePipelines(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())
	pipeline := tu.UniqueString(t.Name() + "-pipeline-")
	cronInput := client.NewCronInput("time", "@every 20s")
	cronInput.Cron.Overwrite = true
	require.NoError(t, c.CreatePipeline(
		pipeline,
		"",
		[]string{"/bin/bash"},
		[]string{"cp /pfs/time/* /pfs/out/"},
		nil,
		cronInput,
		"",
		false,
	))
	// TODO(msteffen): remove all sleeps from tests
	time.Sleep(10 * time.Second)

	for i := 0; i < 20; i++ {
		_, err := c.PpsAPIClient.CreatePipeline(
			context.Background(),
			&pps.CreatePipelineRequest{
				Pipeline: client.NewPipeline(pipeline),
				Transform: &pps.Transform{
					Cmd:   []string{"/bin/bash"},
					Stdin: []string{"cp /pfs/time/* /pfs/out/"},
				},
				Input:     cronInput,
				Update:    true,
				Reprocess: true,
			})
		require.NoError(t, err)
	}
	// TODO ideally this test would not take 5 minutes (or even 3 minutes)
	require.NoErrorWithinTRetry(t, 5*time.Minute, func() error {
		jis, err := c.ListJob(pipeline, nil, -1, true)
		if err != nil {
			return err
		}
		if len(jis) < 6 {
			return errors.Errorf("should have more than 6 jobs in 5 minutes")
		}
		for i := 0; i < 6; i++ {
			if jis[i].Started == nil {
				return errors.Errorf("not enough jobs have been started yet")
			}
		}
		for i := 0; i < 5; i++ {
			difference := jis[i].Started.Seconds - jis[i+1].Started.Seconds
			if difference < 10 {
				return errors.Errorf("jobs too close together")
			} else if difference > 30 {
				return errors.Errorf("jobs too far apart")
			}
		}
		return nil
	})
}

func TestDatumTries(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())

	dataRepo := tu.UniqueString("TestDatumTries_data")
	require.NoError(t, c.CreateRepo(dataRepo))

	require.NoError(t, c.PutFile(client.NewCommit(dataRepo, "master", ""), "file", strings.NewReader("foo"), client.WithAppendPutFile()))

	tries := int64(5)
	pipeline := tu.UniqueString("TestSimplePipeline")
	_, err := c.PpsAPIClient.CreatePipeline(
		context.Background(),
		&pps.CreatePipelineRequest{
			Pipeline: client.NewPipeline(pipeline),
			Transform: &pps.Transform{
				Cmd: []string{"unknown"}, // Cmd fails because "unknown" isn't a known command.
			},
			Input:      client.NewPFSInput(dataRepo, "/"),
			DatumTries: tries,
		})
	require.NoError(t, err)

	commitInfo, err := c.InspectCommit(dataRepo, "master", "")
	require.NoError(t, err)

	jobInfos, err := c.WaitJobSetAll(commitInfo.Commit.ID, false)
	require.NoError(t, err)
	require.Equal(t, 1, len(jobInfos))

	iter := c.GetLogs(pipeline, jobInfos[0].Job.ID, nil, "", false, false, 0)
	var observedTries int64
	for iter.Next() {
		if strings.Contains(iter.Message().Message, "errored running user code after") {
			observedTries++
		}
	}
	require.Equal(t, tries, observedTries)
}

func TestInspectJob(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())

	_, err := c.PpsAPIClient.InspectJob(context.Background(), &pps.InspectJobRequest{})
	require.YesError(t, err)
	require.True(t, strings.Contains(err.Error(), "must specify a job"))

	repo := tu.UniqueString("TestInspectJob")
	require.NoError(t, c.CreateRepo(repo))
	require.NoError(t, c.PutFile(client.NewCommit(repo, "master", ""), "file", strings.NewReader("foo"), client.WithAppendPutFile()))
	ci, err := c.InspectCommit(repo, "master", "")
	require.NoError(t, err)

	_, err = c.InspectJob(repo, ci.Commit.ID, false)
	require.YesError(t, err)
	require.True(t, errutil.IsNotFoundError(err))
}

func TestPipelineVersions(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())

	dataRepo := tu.UniqueString("TestPipelineVersions_data")
	require.NoError(t, c.CreateRepo(dataRepo))

	pipeline := tu.UniqueString("TestPipelineVersions")
	nVersions := 5
	for i := 0; i < nVersions; i++ {
		require.NoError(t, c.CreatePipeline(
			pipeline,
			"",
			[]string{fmt.Sprintf("%d", i)}, // an obviously illegal command, but the pipeline will never run
			nil,
			&pps.ParallelismSpec{
				Constant: 1,
			},
			client.NewPFSInput(dataRepo, "/*"),
			"",
			i != 0,
		))
	}

	for i := 0; i < nVersions; i++ {
		pi, err := c.InspectPipeline(ancestry.Add(pipeline, nVersions-1-i), true)
		require.NoError(t, err)
		require.Equal(t, fmt.Sprintf("%d", i), pi.Details.Transform.Cmd[0])
	}
}

// TestSplitFileHeader tests putting data in Pachyderm with delimiter == SQL,
// and makes sure that every pipeline worker gets a copy of the file header. As
// well, adding more data with the same header should not change the contents of
// existing data.
func TestSplitFileHeader(t *testing.T) {
	// TODO(2.0 optional): Implement put file split in V2?
	t.Skip("Split file header not implemented in V2")
	//	if testing.Short() {
	//		t.Skip("Skipping integration tests in short mode")
	//	}
	//
	//	c := tu.GetPachClient(t)
	//	require.NoError(t, c.DeleteAll())
	//
	//	// put a SQL file w/ header
	//	repo := tu.UniqueString("TestSplitFileHeader")
	//	require.NoError(t, c.CreateRepo(repo))
	//	require.NoError(t, c.PutFileSplit(repo, "master", "d", pfs.Delimiter_SQL, 0, 0, 0, false, strings.NewReader(tu.TestPGDump), client.WithAppendPutFile()))
	//
	//	// Create a pipeline that roughly validates the header
	//	pipeline := tu.UniqueString("TestSplitFileHeaderPipeline")
	//	require.NoError(t, c.CreatePipeline(
	//		pipeline,
	//		"",
	//		[]string{"/bin/bash"},
	//		[]string{
	//			`ls /pfs/*/d/*`, // for debugging
	//			`cars_tables="$(grep "CREATE TABLE public.cars" /pfs/*/d/* | sort -u  | wc -l)"`,
	//			`(( cars_tables == 1 )) && exit 0 || exit 1`,
	//		},
	//		&pps.ParallelismSpec{Constant: 1},
	//		client.NewPFSInput(repo, "/d/*"),
	//		"",
	//		false,
	//	))
	//
	//	// wait for job to run & check that all rows were processed
	//	var jobCount int
	//	c.FlushJob([]*pfs.Commit{client.NewCommit(repo, "master")}, nil,
	//		func(jobInfo *pps.JobInfo) error {
	//			jobCount++
	//			require.Equal(t, 1, jobCount)
	//			require.Equal(t, pps.JobState_JOB_SUCCESS, jobInfo.State)
	//			require.Equal(t, int64(5), jobInfo.DataProcessed)
	//			require.Equal(t, int64(0), jobInfo.DataSkipped)
	//			return nil
	//		})
	//
	//	// Add new rows with same header data
	//	require.NoError(t, c.PutFileSplit(repo, "master", "d", pfs.Delimiter_SQL, 0, 0, 0, false, strings.NewReader(tu.TestPGDumpNewRows), client.WithAppendPutFile()))
	//
	//	// old data should be skipped, even though header was uploaded twice (new
	//	// header shouldn't append or change the hash or anything)
	//	jobCount = 0
	//	c.FlushJob([]*pfs.Commit{client.NewCommit(repo, "master")}, nil,
	//		func(jobInfo *pps.JobInfo) error {
	//			jobCount++
	//			require.Equal(t, 1, jobCount)
	//			require.Equal(t, pps.JobState_JOB_SUCCESS, jobInfo.State)
	//			require.Equal(t, int64(3), jobInfo.DataProcessed) // added 3 new rows
	//			require.Equal(t, int64(5), jobInfo.DataSkipped)
	//			return nil
	//		})
}

func TestNewHeaderCausesReprocess(t *testing.T) {
	// TODO(2.0 optional): Implement put file split in V2?
	t.Skip("Split file header not implemented in V2")
	//	if testing.Short() {
	//		t.Skip("Skipping integration tests in short mode")
	//	}
	//
	//	c := tu.GetPachClient(t)
	//	require.NoError(t, c.DeleteAll())
	//
	//	// put a SQL file w/ header
	//	repo := tu.UniqueString("TestSplitFileHeader")
	//	require.NoError(t, c.CreateRepo(repo))
	//	require.NoError(t, c.PutFileSplit(repo, "master", "d", pfs.Delimiter_SQL, 0, 0, 0, false, strings.NewReader(tu.TestPGDump), client.WithAppendPutFile()))
	//
	//	// Create a pipeline that roughly validates the header
	//	pipeline := tu.UniqueString("TestSplitFileReprocessPL")
	//	require.NoError(t, c.CreatePipeline(
	//		pipeline,
	//		"",
	//		[]string{"/bin/bash"},
	//		[]string{
	//			`ls /pfs/*/d/*`, // for debugging
	//			`cars_tables="$(grep "CREATE TABLE public.cars" /pfs/*/d/* | sort -u  | wc -l)"`,
	//			`(( cars_tables == 1 )) && exit 0 || exit 1`,
	//		},
	//		&pps.ParallelismSpec{Constant: 1},
	//		client.NewPFSInput(repo, "/d/*"),
	//		"",
	//		false,
	//	))
	//
	//	// wait for job to run & check that all rows were processed
	//	var jobCount int
	//	c.FlushJob([]*pfs.Commit{client.NewCommit(repo, "master")}, nil,
	//		func(jobInfo *pps.JobInfo) error {
	//			jobCount++
	//			require.Equal(t, 1, jobCount)
	//			require.Equal(t, pps.JobState_JOB_SUCCESS, jobInfo.State)
	//			require.Equal(t, int64(5), jobInfo.DataProcessed)
	//			require.Equal(t, int64(0), jobInfo.DataSkipped)
	//			return nil
	//		})
	//
	//	// put empty dataset w/ new header
	//	require.NoError(t, c.PutFileSplit(repo, "master", "d", pfs.Delimiter_SQL, 0, 0, 0, false, strings.NewReader(tu.TestPGDumpNewHeader), client.WithAppendPutFile()))
	//
	//	// everything gets reprocessed (hashes all change even though the files
	//	// themselves weren't altered)
	//	jobCount = 0
	//	c.FlushJob([]*pfs.Commit{client.NewCommit(repo, "master")}, nil,
	//		func(jobInfo *pps.JobInfo) error {
	//			jobCount++
	//			require.Equal(t, 1, jobCount)
	//			require.Equal(t, pps.JobState_JOB_SUCCESS, jobInfo.State)
	//			require.Equal(t, int64(5), jobInfo.DataProcessed) // added 3 new rows
	//			require.Equal(t, int64(0), jobInfo.DataSkipped)
	//			return nil
	//		})
}

// TestDeferredCross is a repro for https://github.com/pachyderm/pachyderm/v2/issues/5172
func TestDeferredCross(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())

	// make repo for our dataset
	dataSet := tu.UniqueString("dataset")
	require.NoError(t, c.CreateRepo(dataSet))
	dataCommit := client.NewCommit(dataSet, "master", "")

	downstreamPipeline := tu.UniqueString("downstream")
	_, err := c.PpsAPIClient.CreatePipeline(
		context.Background(),
		&pps.CreatePipelineRequest{
			Pipeline: client.NewPipeline(downstreamPipeline),
			Transform: &pps.Transform{
				Cmd: []string{"bash"},
				Stdin: []string{
					fmt.Sprintf("cp /pfs/%v/* /pfs/out", dataSet),
				},
			},
			Input: client.NewPFSInput(dataSet, "master"),

			OutputBranch: "master",
		})
	require.NoError(t, err)

	require.NoError(t, c.PutFile(dataCommit, "file1", strings.NewReader("foo"), client.WithAppendPutFile()))
	require.NoError(t, c.PutFile(dataCommit, "file2", strings.NewReader("foo"), client.WithAppendPutFile()))
	require.NoError(t, c.PutFile(dataCommit, "file3", strings.NewReader("foo"), client.WithAppendPutFile()))

	commitInfo, err := c.InspectCommit(dataSet, "master", "")
	require.NoError(t, err)
	_, err = c.WaitCommitSetAll(commitInfo.Commit.ID)
	require.NoError(t, err)

	err = c.CreateBranch(downstreamPipeline, "other", "", "master^", nil)
	require.NoError(t, err)

	// next, create an imputation pipeline which is a cross of the dataset with the union of two different freeze branches
	impPipeline := tu.UniqueString("imputed")
	_, err = c.PpsAPIClient.CreatePipeline(
		context.Background(),
		&pps.CreatePipelineRequest{
			Pipeline: client.NewPipeline(impPipeline),
			Transform: &pps.Transform{
				Cmd: []string{"bash"},
				Stdin: []string{
					"true",
				},
			},
			Input: client.NewCrossInput(
				client.NewUnionInput(
					client.NewPFSInputOpts("a", downstreamPipeline, "master", "/", "", "", false, false, nil),
					client.NewPFSInputOpts("b", downstreamPipeline, "other", "/", "", "", false, false, nil),
				),
				client.NewPFSInput(dataSet, "/"),
			),
			OutputBranch: "master",
		})
	require.NoError(t, err)

	// after all this, the imputation job should be using the master commit of the dataset repo
	_, err = c.WaitCommit(impPipeline, "master", "")
	require.NoError(t, err)

	jobs, err := c.ListJob(impPipeline, nil, 0, true)
	require.NoError(t, err)
	require.Equal(t, len(jobs), 1)

	jobInfo, err := c.InspectJob(jobs[0].Job.Pipeline.Name, jobs[0].Job.ID, true)
	require.NoError(t, err)

	headCommit, err := c.InspectCommit(dataSet, "master", "")
	require.NoError(t, err)

	pps.VisitInput(jobInfo.Details.Input, func(i *pps.Input) error {
		if i.Pfs != nil && i.Pfs.Repo == dataSet {
			require.Equal(t, i.Pfs.Commit, headCommit.Commit.ID)
		}
		return nil
	})
}

func TestDeferredProcessing(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())

	dataRepo := tu.UniqueString("TestDeferredProcessing_data")
	require.NoError(t, c.CreateRepo(dataRepo))

	pipeline1 := tu.UniqueString("TestDeferredProcessing1")
	_, err := c.PpsAPIClient.CreatePipeline(
		context.Background(),
		&pps.CreatePipelineRequest{
			Pipeline: client.NewPipeline(pipeline1),
			Transform: &pps.Transform{
				Cmd:   []string{"bash"},
				Stdin: []string{fmt.Sprintf("cp /pfs/%s/* /pfs/out/", dataRepo)},
			},
			Input:        client.NewPFSInput(dataRepo, "/*"),
			OutputBranch: "staging",
		})
	require.NoError(t, err)

	pipeline2 := tu.UniqueString("TestDeferredProcessing2")
	require.NoError(t, c.CreatePipeline(
		pipeline2,
		"",
		[]string{"bash"},
		[]string{
			fmt.Sprintf("cp /pfs/%s/* /pfs/out/", pipeline1),
		},
		&pps.ParallelismSpec{
			Constant: 1,
		},
		client.NewPFSInput(pipeline1, "/*"),
		"",
		false,
	))

	commit := client.NewCommit(dataRepo, "staging", "")
	require.NoError(t, c.PutFile(commit, "file", strings.NewReader("foo"), client.WithAppendPutFile()))

	commitInfo, err := c.InspectCommit(dataRepo, "staging", "")
	require.NoError(t, err)

	// The same commitset should be extended after each branch head move
	commitInfos, err := c.WaitCommitSetAll(commitInfo.Commit.ID)
	require.NoError(t, err)
	require.Equal(t, 1, len(commitInfos))

	require.NoError(t, c.CreateBranch(dataRepo, "master", "staging", "", nil))

	commitInfos, err = c.WaitCommitSetAll(commitInfo.Commit.ID)
	require.NoError(t, err)
	require.Equal(t, 5, len(commitInfos))

	require.NoError(t, c.CreateBranch(pipeline1, "master", "staging", "", nil))

	commitInfos, err = c.WaitCommitSetAll(commitInfo.Commit.ID)
	require.NoError(t, err)
	require.Equal(t, 9, len(commitInfos))
}

func TestPipelineHistory(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())
	// create repos
	dataRepo := tu.UniqueString("TestPipelineHistory_data")
	require.NoError(t, c.CreateRepo(dataRepo))
	pipelineName := tu.UniqueString("TestPipelineHistory")
	require.NoError(t, c.CreatePipeline(
		pipelineName,
		"",
		[]string{"bash"},
		[]string{"echo foo >/pfs/out/file"},
		&pps.ParallelismSpec{
			Constant: 1,
		},
		client.NewPFSInput(dataRepo, "/*"),
		"",
		true,
	))

	require.NoError(t, c.PutFile(client.NewCommit(dataRepo, "master", ""), "file", strings.NewReader("1"), client.WithAppendPutFile()))
	commitInfo, err := c.InspectCommit(dataRepo, "master", "")
	require.NoError(t, err)
	_, err = c.WaitCommitSetAll(commitInfo.Commit.ID)
	require.NoError(t, err)

	jis, err := c.ListJob(pipelineName, nil, 0, true)
	require.NoError(t, err)
	require.Equal(t, 2, len(jis))

	// Update the pipeline
	require.NoError(t, c.CreatePipeline(
		pipelineName,
		"",
		[]string{"bash"},
		[]string{"echo bar >/pfs/out/file"},
		&pps.ParallelismSpec{
			Constant: 1,
		},
		client.NewPFSInput(dataRepo, "/*"),
		"",
		true,
	))

	require.NoError(t, c.PutFile(client.NewCommit(dataRepo, "master", ""), "file", strings.NewReader("2"), client.WithAppendPutFile()))
	commitInfo, err = c.InspectCommit(dataRepo, "master", "")
	require.NoError(t, err)
	_, err = c.WaitCommitSetAll(commitInfo.Commit.ID)
	require.NoError(t, err)

	commitInfos, err := c.ListCommit(client.NewRepo(pipelineName), client.NewCommit(pipelineName, "master", ""), nil, 0)
	require.NoError(t, err)
	require.Equal(t, 4, len(commitInfos))

	jis, err = c.ListJob(pipelineName, nil, 0, true)
	require.NoError(t, err)
	require.Equal(t, 2, len(jis))
	jis, err = c.ListJob(pipelineName, nil, 1, true)
	require.NoError(t, err)
	require.Equal(t, 4, len(jis))
	jis, err = c.ListJob(pipelineName, nil, -1, true)
	require.NoError(t, err)
	require.Equal(t, 4, len(jis))

	// Update the pipeline again
	require.NoError(t, c.CreatePipeline(
		pipelineName,
		"",
		[]string{"bash"},
		[]string{"echo buzz >/pfs/out/file"},
		&pps.ParallelismSpec{
			Constant: 1,
		},
		client.NewPFSInput(dataRepo, "/*"),
		"",
		true,
	))
	commitInfo, err = c.InspectCommit(dataRepo, "master", "")
	require.NoError(t, err)
	_, err = c.WaitCommitSetAll(commitInfo.Commit.ID)
	require.NoError(t, err)

	jis, err = c.ListJob(pipelineName, nil, 0, true)
	require.NoError(t, err)
	require.Equal(t, 1, len(jis))
	jis, err = c.ListJob(pipelineName, nil, 1, true)
	require.NoError(t, err)
	require.Equal(t, 3, len(jis))
	jis, err = c.ListJob(pipelineName, nil, 2, true)
	require.NoError(t, err)
	require.Equal(t, 5, len(jis))
	jis, err = c.ListJob(pipelineName, nil, -1, true)
	require.NoError(t, err)
	require.Equal(t, 5, len(jis))

	// Add another pipeline, this shouldn't change the results of the above
	// commands.
	pipelineName2 := tu.UniqueString("TestPipelineHistory2")
	require.NoError(t, c.CreatePipeline(
		pipelineName2,
		"",
		[]string{"bash"},
		[]string{"echo foo >/pfs/out/file"},
		&pps.ParallelismSpec{
			Constant: 1,
		},
		client.NewPFSInput(dataRepo, "/*"),
		"",
		true,
	))
	commitInfo, err = c.InspectCommit(dataRepo, "master", "")
	require.NoError(t, err)
	_, err = c.WaitCommitSetAll(commitInfo.Commit.ID)
	require.NoError(t, err)

	jis, err = c.ListJob(pipelineName, nil, 0, true)
	require.NoError(t, err)
	require.Equal(t, 1, len(jis))
	jis, err = c.ListJob(pipelineName, nil, 1, true)
	require.NoError(t, err)
	require.Equal(t, 3, len(jis))
	jis, err = c.ListJob(pipelineName, nil, 2, true)
	require.NoError(t, err)
	require.Equal(t, 5, len(jis))
	jis, err = c.ListJob(pipelineName, nil, -1, true)
	require.NoError(t, err)
	require.Equal(t, 5, len(jis))

	pipelineInfos, err := c.ListPipeline(false)
	require.NoError(t, err)
	require.Equal(t, 2, len(pipelineInfos))

	pipelineInfos, err = c.ListPipelineHistory("", -1, false)
	require.NoError(t, err)
	require.Equal(t, 4, len(pipelineInfos))

	pipelineInfos, err = c.ListPipelineHistory("", 1, false)
	require.NoError(t, err)
	require.Equal(t, 3, len(pipelineInfos))

	pipelineInfos, err = c.ListPipelineHistory(pipelineName, -1, false)
	require.NoError(t, err)
	require.Equal(t, 3, len(pipelineInfos))

	pipelineInfos, err = c.ListPipelineHistory(pipelineName2, -1, false)
	require.NoError(t, err)
	require.Equal(t, 1, len(pipelineInfos))
}

func TestFileHistory(t *testing.T) {
	// TODO: Implement file history in V2?
	t.Skip("File history not implemented in V2")
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())

	dataRepo1 := tu.UniqueString("TestFileHistory_data1")
	require.NoError(t, c.CreateRepo(dataRepo1))
	dataRepo2 := tu.UniqueString("TestFileHistory_data2")
	require.NoError(t, c.CreateRepo(dataRepo2))

	pipeline := tu.UniqueString("TestFileHistory")
	require.NoError(t, c.CreatePipeline(
		pipeline,
		"",
		[]string{"bash"},
		[]string{
			fmt.Sprintf("for a in /pfs/%s/*", dataRepo1),
			"do",
			fmt.Sprintf("for b in /pfs/%s/*", dataRepo2),
			"do",
			"touch /pfs/out/$(basename $a)_$(basename $b)",
			"done",
			"done",
		},
		&pps.ParallelismSpec{
			Constant: 1,
		},
		client.NewCrossInput(
			client.NewPFSInput(dataRepo1, "/*"),
			client.NewPFSInput(dataRepo2, "/*"),
		),
		"",
		false,
	))

	commit1 := client.NewCommit(dataRepo1, "master", "")
	commit2 := client.NewCommit(dataRepo2, "master", "")

	require.NoError(t, c.PutFile(commit1, "A1", strings.NewReader(""), client.WithAppendPutFile()))
	require.NoError(t, c.PutFile(commit2, "B1", strings.NewReader(""), client.WithAppendPutFile()))

	require.NoError(t, c.PutFile(commit1, "A2", strings.NewReader(""), client.WithAppendPutFile()))
	require.NoError(t, c.PutFile(commit1, "A3", strings.NewReader(""), client.WithAppendPutFile()))
	require.NoError(t, c.PutFile(commit2, "B2", strings.NewReader(""), client.WithAppendPutFile()))
	require.NoError(t, c.PutFile(commit2, "B3", strings.NewReader(""), client.WithAppendPutFile()))

	commitInfo, err := c.InspectCommit(pipeline, "master", "")
	require.NoError(t, err)
	_, err = c.WaitCommitSetAll(commitInfo.Commit.ID)
	require.NoError(t, err)

	//_, err = c.ListFileHistory(pipeline, "master", "", -1)
	//require.NoError(t, err)
}

// TestCreatePipelineErrorNoTransform tests that sending a CreatePipeline
// requests to pachd with no 'pipeline' field doesn't kill pachd
func TestCreatePipelineErrorNoPipeline(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())

	// Create input repo
	dataRepo := tu.UniqueString(t.Name() + "-data")
	require.NoError(t, c.CreateRepo(dataRepo))

	// Create pipeline w/ no pipeline field--make sure we get a response
	_, err := c.PpsAPIClient.CreatePipeline(
		context.Background(),
		&pps.CreatePipelineRequest{
			Pipeline: nil,
			Transform: &pps.Transform{
				Cmd:   []string{"/bin/bash"},
				Stdin: []string{`cat foo >/pfs/out/file`},
			},
			Input: client.NewPFSInput(dataRepo, "/*"),
		})
	require.YesError(t, err)
	require.Matches(t, "request.Pipeline", err.Error())
}

// TestCreatePipelineErrorNoTransform tests that sending a CreatePipeline
// requests to pachd with no 'transform' or 'pipeline' field doesn't kill pachd
func TestCreatePipelineError(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())

	// Create input repo
	dataRepo := tu.UniqueString(t.Name() + "-data")
	require.NoError(t, c.CreateRepo(dataRepo))

	// Create pipeline w/ no transform--make sure we get a response (& make sure
	// it explains the problem)
	pipeline := tu.UniqueString("no-transform-")
	_, err := c.PpsAPIClient.CreatePipeline(
		context.Background(),
		&pps.CreatePipelineRequest{
			Pipeline:  client.NewPipeline(pipeline),
			Transform: nil,
			Input:     client.NewPFSInput(dataRepo, "/*"),
		})
	require.YesError(t, err)
	require.Matches(t, "transform", err.Error())
}

// TestCreatePipelineErrorNoCmd tests that sending a CreatePipeline request to
// pachd with no 'transform.cmd' field doesn't kill pachd
func TestCreatePipelineErrorNoCmd(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())

	// Create input data
	dataRepo := tu.UniqueString(t.Name() + "-data")
	require.NoError(t, c.CreateRepo(dataRepo))
	require.NoError(t, c.PutFile(client.NewCommit(dataRepo, "master", ""), "file", strings.NewReader("foo"), client.WithAppendPutFile()))

	// create pipeline
	pipeline := tu.UniqueString("no-cmd-")
	_, err := c.PpsAPIClient.CreatePipeline(
		context.Background(),
		&pps.CreatePipelineRequest{
			Pipeline: client.NewPipeline(pipeline),
			Transform: &pps.Transform{
				Cmd:   nil,
				Stdin: []string{`cat foo >/pfs/out/file`},
			},
			Input: client.NewPFSInput(dataRepo, "/*"),
		})
	require.NoError(t, err)
	time.Sleep(5 * time.Second) // give pipeline time to start

	require.NoErrorWithinTRetry(t, 30*time.Second, func() error {
		pipelineInfo, err := c.InspectPipeline(pipeline, false)
		if err != nil {
			return err
		}
		if pipelineInfo.State != pps.PipelineState_PIPELINE_FAILURE {
			return errors.Errorf("pipeline should be in state FAILURE, not: %s", pipelineInfo.State.String())
		}
		return nil
	})
}

func TestExtractPipeline(t *testing.T) {
	// TODO: Implement extract pipeline.
	t.Skip("Extract pipeline not implemented in V2")
	//	c := tu.GetPachClient(t)
	//	require.NoError(t, c.DeleteAll())
	//
	//	dataRepo := tu.UniqueString("TestExtractPipeline_data")
	//	require.NoError(t, c.CreateRepo(dataRepo))
	//	request := &pps.CreatePipelineRequest{}
	//	// Generate fake data
	//	gofakeit.Struct(&request)
	//
	//	// Now set a bunch of fields explicitly so the server will accept the request.
	//	// Override the input because otherwise the repo won't exist
	//	request.Input = client.NewPFSInput(dataRepo, "/*")
	//	// These must be set explicitly, because extract returns the default values
	//	// and we want them to match.
	//	request.Input.Pfs.Name = "input"
	//	request.Input.Pfs.Branch = "master"
	//	// If service, can only set as Constant:1
	//	request.ParallelismSpec.Constant = 1
	//	// CacheSize must parse as a memory value
	//	request.CacheSize = "1G"
	//	// Durations must be valid
	//	d := &types.Duration{Seconds: 1, Nanos: 1}
	//	request.JobTimeout = d
	//	request.DatumTimeout = d
	//	// PodSpec and PodPatch must parse as json
	//	request.PodSpec = "{}"
	//	request.PodPatch = "{}"
	//	request.Service.Type = string(v1.ServiceTypeClusterIP)
	//	// Don't want to explicitly set spec commit, since there's no valid commit
	//	// to set it to, and this is one of the few fields that shouldn't get
	//	// extracted back to us.
	//	request.SpecCommit = nil
	//	// MaxQueueSize gets set to 1 if it's negative, which will superficially
	//	// fail the test, so we set a real value.
	//	request.MaxQueueSize = 2
	//	// Update and reprocess don't get extracted back either so don't set it.
	//	request.Update = false
	//	request.Reprocess = false
	//
	//	// Create the pipeline
	//	_, err := c.PpsAPIClient.CreatePipeline(
	//		context.Background(),
	//		request)
	//	require.YesError(t, err)
	//	require.True(t, strings.Contains(err.Error(), "TFJob"))
	//	// TODO when TFJobs are supported the above should be deleted
	//
	//	// Set TFJob to nil so request can work
	//	request.TFJob = nil
	//	_, err = c.PpsAPIClient.CreatePipeline(
	//		context.Background(),
	//		request)
	//	require.NoError(t, err)
	//
	//	// Extract it and see if we get the same thing
	//	extractedRequest, err := c.ExtractPipeline(request.Pipeline.Name)
	//	require.NoError(t, err)
	//	// When this check fails it most likely means that you've added field to
	//	// pipelines and not set it up to be extract. PipelineReqFromInfo is the
	//	// function you'll need to add it to.
	//	if !proto.Equal(request, extractedRequest) {
	//		marshaller := &jsonpb.Marshaler{
	//			Indent:   "  ",
	//			OrigName: true,
	//		}
	//		requestString, err := marshaller.MarshalToString(request)
	//		require.NoError(t, err)
	//		extractedRequestString, err := marshaller.MarshalToString(extractedRequest)
	//		require.NoError(t, err)
	//		t.Errorf("Expected:\n%s\n, Got:\n%s\n", requestString, extractedRequestString)
	//	}
}

// TestPodPatchUnmarshalling tests the fix for issues #3483, by adding a
// PodPatch to a pipeline spec and making sure it's applied correctly
func TestPodPatchUnmarshalling(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())

	// Create input data
	dataRepo := tu.UniqueString(t.Name() + "-data-")
	require.NoError(t, c.CreateRepo(dataRepo))

	dataCommit := client.NewCommit(dataRepo, "master", "")
	require.NoError(t, c.PutFile(dataCommit, "file", strings.NewReader("foo"), client.WithAppendPutFile()))

	// create pipeline
	pipeline := tu.UniqueString("pod-patch-")
	_, err := c.PpsAPIClient.CreatePipeline(
		context.Background(),
		&pps.CreatePipelineRequest{
			Pipeline: client.NewPipeline(pipeline),
			Transform: &pps.Transform{
				Cmd:   []string{"bash"},
				Stdin: []string{"cp /pfs/in/* /pfs/out"},
			},
			Input: &pps.Input{Pfs: &pps.PFSInput{
				Name: "in", Repo: dataRepo, Glob: "/*",
			}},
			PodPatch: `[
				{
				  "op": "add",
				  "path": "/volumes/0",
				  "value": {
				    "name": "vol0",
				    "hostPath": {
				      "path": "/volumePath"
				}}}]`,
		})
	require.NoError(t, err)

	commitInfo, err := c.WaitCommit(pipeline, "master", "")
	require.NoError(t, err)

	var buf bytes.Buffer
	require.NoError(t, c.GetFile(commitInfo.Commit, "file", &buf))
	require.Equal(t, "foo", buf.String())

	pipelineInfo, err := c.InspectPipeline(pipeline, false)
	require.NoError(t, err)

	// make sure 'vol0' is correct in the pod spec
	var volumes []v1.Volume
	rcName := ppsutil.PipelineRcName(pipelineInfo.Pipeline.Name, pipelineInfo.Version)
	kubeClient := tu.GetKubeClient(t)
	require.NoError(t, backoff.Retry(func() error {
		podList, err := kubeClient.CoreV1().Pods(v1.NamespaceDefault).List(
			metav1.ListOptions{
				LabelSelector: metav1.FormatLabelSelector(metav1.SetAsLabelSelector(
					map[string]string{"app": rcName},
				)),
			})
		if err != nil {
			return err // retry
		}
		if len(podList.Items) != 1 || len(podList.Items[0].Spec.Volumes) == 0 {
			return errors.Errorf("could not find volumes for pipeline %s", pipelineInfo.Pipeline.Name)
		}
		volumes = podList.Items[0].Spec.Volumes
		return nil // no more retries
	}, backoff.NewTestingBackOff()))
	// Make sure a CPU and Memory request are both set
	for _, vol := range volumes {
		require.True(t,
			vol.VolumeSource.HostPath == nil || vol.VolumeSource.EmptyDir == nil)
		if vol.Name == "vol0" {
			require.True(t, vol.VolumeSource.HostPath.Path == "/volumePath")
		}
	}
}

func TestSecrets(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())

	b := []byte(
		`{
			"kind": "Secret",
			"apiVersion": "v1",
			"metadata": {
				"name": "test-secret",
				"creationTimestamp": null
			},
			"data": {
				"mykey": "bXktdmFsdWU="
			}
		}`)
	require.NoError(t, c.CreateSecret(b))

	secretInfo, err := c.InspectSecret("test-secret")
	secretInfo.CreationTimestamp = nil
	require.NoError(t, err)
	require.Equal(t, &pps.SecretInfo{
		Secret: &pps.Secret{
			Name: "test-secret",
		},
		Type:              "Opaque",
		CreationTimestamp: nil,
	}, secretInfo)

	secretInfos, err := c.ListSecret()
	require.NoError(t, err)
	initialLength := len(secretInfos)

	require.NoError(t, c.DeleteSecret("test-secret"))

	secretInfos, err = c.ListSecret()
	require.NoError(t, err)
	require.Equal(t, initialLength-1, len(secretInfos))

	_, err = c.InspectSecret("test-secret")
	require.YesError(t, err)
}

// Test that an unauthenticated user can't call secrets APIS
func TestSecretsUnauthenticated(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	// Enable auth on the cluster
	tu.DeleteAll(t)
	tu.GetAuthenticatedPachClient(t, auth.RootUser)
	defer tu.DeleteAll(t)

	// Get an unauthenticated client
	c := tu.GetPachClient(t)
	c.SetAuthToken("")

	b := []byte(
		`{
			"kind": "Secret",
			"apiVersion": "v1",
			"metadata": {
				"name": "test-secret",
				"creationTimestamp": null
			},
			"data": {
				"mykey": "bXktdmFsdWU="
			}
		}`)

	err := c.CreateSecret(b)
	require.YesError(t, err)
	require.Matches(t, "no authentication token", err.Error())

	_, err = c.InspectSecret("test-secret")
	require.YesError(t, err)
	require.Matches(t, "no authentication token", err.Error())

	_, err = c.ListSecret()
	require.YesError(t, err)
	require.Matches(t, "no authentication token", err.Error())

	err = c.DeleteSecret("test-secret")
	require.YesError(t, err)
	require.Matches(t, "no authentication token", err.Error())
}

func TestCopyOutToIn(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())

	dataRepo := tu.UniqueString("TestCopyOutToIn_data")
	require.NoError(t, c.CreateRepo(dataRepo))

	dataCommit := client.NewCommit(dataRepo, "master", "")
	require.NoError(t, c.PutFile(dataCommit, "file", strings.NewReader("foo"), client.WithAppendPutFile()))

	pipeline := tu.UniqueString("TestCopyOutToIn")
	pipelineCommit := client.NewCommit(pipeline, "master", "")
	require.NoError(t, c.CreatePipeline(
		pipeline,
		"",
		[]string{"bash"},
		[]string{
			fmt.Sprintf("cp -R /pfs/%s/* /pfs/out/", dataRepo),
		},
		&pps.ParallelismSpec{
			Constant: 1,
		},
		client.NewPFSInput(dataRepo, "/*"),
		"",
		false,
	))

	commitInfo, err := c.InspectCommit(dataRepo, "master", "")
	require.NoError(t, err)
	_, err = c.WaitCommitSetAll(commitInfo.Commit.ID)
	require.NoError(t, err)
	require.NoError(t, c.CopyFile(dataCommit, "file2", pipelineCommit, "file", client.WithAppendCopyFile()))
	commitInfo, err = c.InspectCommit(dataRepo, "master", "")
	require.NoError(t, err)
	_, err = c.WaitCommitSetAll(commitInfo.Commit.ID)
	require.NoError(t, err)

	require.NoError(t, c.PutFile(dataCommit, "file2", strings.NewReader("foo"), client.WithAppendPutFile()))

	var buf bytes.Buffer
	require.NoError(t, c.GetFile(pipelineCommit, "file2", &buf))
	require.Equal(t, "foo", buf.String())

	mfc, err := c.NewModifyFileClient(dataCommit)
	require.NoError(t, err)
	require.NoError(t, mfc.PutFile("dir/file3", strings.NewReader("foo"), client.WithAppendPutFile()))
	require.NoError(t, mfc.PutFile("dir/file4", strings.NewReader("bar"), client.WithAppendPutFile()))
	require.NoError(t, mfc.Close())

	commitInfo, err = c.InspectCommit(dataRepo, "master", "")
	require.NoError(t, err)
	_, err = c.WaitCommitSetAll(commitInfo.Commit.ID)
	require.NoError(t, err)

	require.NoError(t, c.CopyFile(dataCommit, "dir2", pipelineCommit, "dir", client.WithAppendCopyFile()))

	commitInfo, err = c.InspectCommit(dataRepo, "master", "")
	require.NoError(t, err)
	_, err = c.WaitCommitSetAll(commitInfo.Commit.ID)
	require.NoError(t, err)

	buf.Reset()
	require.NoError(t, c.GetFile(pipelineCommit, "dir/file3", &buf))
	require.Equal(t, "foo", buf.String())
	buf.Reset()
	require.NoError(t, c.GetFile(pipelineCommit, "dir/file4", &buf))
	require.Equal(t, "bar", buf.String())
}

func TestKeepRepo(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())

	dataRepo := tu.UniqueString("TestKeepRepo_data")
	require.NoError(t, c.CreateRepo(dataRepo))

	dataCommit := client.NewCommit(dataRepo, "master", "")
	require.NoError(t, c.PutFile(dataCommit, "file", strings.NewReader("foo"), client.WithAppendPutFile()))

	pipeline := tu.UniqueString("TestKeepRepo")
	pipelineCommit := client.NewCommit(pipeline, "master", "")
	require.NoError(t, c.CreatePipeline(
		pipeline,
		"",
		[]string{"bash"},
		[]string{
			fmt.Sprintf("cp /pfs/%s/* /pfs/out/", dataRepo),
		},
		&pps.ParallelismSpec{
			Constant: 1,
		},
		client.NewPFSInput(dataRepo, "/*"),
		"",
		false,
	))

	commitInfo, err := c.InspectCommit(dataRepo, "master", "")
	require.NoError(t, err)
	_, err = c.WaitCommitSetAll(commitInfo.Commit.ID)
	require.NoError(t, err)

	_, err = c.PpsAPIClient.DeletePipeline(c.Ctx(), &pps.DeletePipelineRequest{
		Pipeline: client.NewPipeline(pipeline),
		KeepRepo: true,
	})
	require.NoError(t, err)
	_, err = c.InspectRepo(pipeline)
	require.NoError(t, err)

	var buf bytes.Buffer
	require.NoError(t, c.GetFile(pipelineCommit, "file", &buf))
	require.Equal(t, "foo", buf.String())

	require.NoError(t, c.CreatePipeline(
		pipeline,
		"",
		[]string{"bash"},
		[]string{
			fmt.Sprintf("cp /pfs/%s/* /pfs/out/", dataRepo),
		},
		&pps.ParallelismSpec{
			Constant: 1,
		},
		client.NewPFSInput(dataRepo, "/*"),
		"",
		false,
	))

	require.NoError(t, c.PutFile(dataCommit, "file2", strings.NewReader("bar"), client.WithAppendPutFile()))
	commitInfo, err = c.InspectCommit(dataRepo, "master", "")
	require.NoError(t, err)
	_, err = c.WaitCommitSetAll(commitInfo.Commit.ID)
	require.NoError(t, err)

	buf.Reset()
	require.NoError(t, c.GetFile(pipelineCommit, "file", &buf))
	require.Equal(t, "foo", buf.String())
	buf.Reset()
	require.NoError(t, c.GetFile(pipelineCommit, "file2", &buf))
	require.Equal(t, "bar", buf.String())

	require.NoError(t, c.DeletePipeline(pipeline, false))
}

// Regression test to make sure that pipeline creation doesn't crash pachd due to missing fields
func TestMalformedPipeline(t *testing.T) {
	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())

	pipelineName := tu.UniqueString("MalformedPipeline")

	var err error
	_, err = c.PpsAPIClient.CreatePipeline(c.Ctx(), &pps.CreatePipelineRequest{})
	require.YesError(t, err)
	require.Matches(t, "request.Pipeline cannot be nil", err.Error())

	_, err = c.PpsAPIClient.CreatePipeline(c.Ctx(), &pps.CreatePipelineRequest{
		Pipeline: client.NewPipeline(pipelineName)},
	)
	require.YesError(t, err)
	require.Matches(t, "must specify a transform", err.Error())

	_, err = c.PpsAPIClient.CreatePipeline(c.Ctx(), &pps.CreatePipelineRequest{
		Pipeline:  client.NewPipeline(pipelineName),
		Transform: &pps.Transform{},
		Input:     &pps.Input{},
	})
	require.YesError(t, err)
	require.Matches(t, "no input set", err.Error())

	_, err = c.PpsAPIClient.CreatePipeline(c.Ctx(), &pps.CreatePipelineRequest{
		Pipeline:        client.NewPipeline(pipelineName),
		Transform:       &pps.Transform{},
		Service:         &pps.Service{},
		ParallelismSpec: &pps.ParallelismSpec{},
	})
	require.YesError(t, err)
	require.Matches(t, "services can only be run with a constant parallelism of 1", err.Error())

	// TODO(2.0 required): This error isn't triggered in V2?
	//_, err = c.PpsAPIClient.CreatePipeline(c.Ctx(), &pps.CreatePipelineRequest{
	//	Pipeline:   client.NewPipeline(pipelineName),
	//	Transform:  &pps.Transform{},
	//	SpecCommit: &pfs.Commit{},
	//})
	//require.YesError(t, err)
	//require.Matches(t, "cannot resolve commit with no repo", err.Error())

	dataRepo := tu.UniqueString("TestMalformedPipeline_data")
	require.NoError(t, c.CreateRepo(dataRepo))

	dataCommit := client.NewCommit(dataRepo, "master", "")
	require.NoError(t, c.PutFile(dataCommit, "file", strings.NewReader("foo"), client.WithAppendPutFile()))

	_, err = c.PpsAPIClient.CreatePipeline(c.Ctx(), &pps.CreatePipelineRequest{
		Pipeline:  client.NewPipeline(pipelineName),
		Transform: &pps.Transform{},
		Input:     &pps.Input{Pfs: &pps.PFSInput{}},
	})
	require.YesError(t, err)
	require.Matches(t, "input must specify a name", err.Error())

	_, err = c.PpsAPIClient.CreatePipeline(c.Ctx(), &pps.CreatePipelineRequest{
		Pipeline:  client.NewPipeline(pipelineName),
		Transform: &pps.Transform{},
		Input:     &pps.Input{Pfs: &pps.PFSInput{Name: "data"}},
	})
	require.YesError(t, err)
	require.Matches(t, "input must specify a repo", err.Error())

	_, err = c.PpsAPIClient.CreatePipeline(c.Ctx(), &pps.CreatePipelineRequest{
		Pipeline:  client.NewPipeline(pipelineName),
		Transform: &pps.Transform{},
		Input:     &pps.Input{Pfs: &pps.PFSInput{Repo: dataRepo}},
	})
	require.YesError(t, err)
	require.Matches(t, "input must specify a glob", err.Error())

	_, err = c.PpsAPIClient.CreatePipeline(c.Ctx(), &pps.CreatePipelineRequest{
		Pipeline:  client.NewPipeline(pipelineName),
		Transform: &pps.Transform{},
		Input:     client.NewPFSInput("out", "/*"),
	})
	require.YesError(t, err)
	require.Matches(t, "input cannot be named \"out\"", err.Error())

	_, err = c.PpsAPIClient.CreatePipeline(c.Ctx(), &pps.CreatePipelineRequest{
		Pipeline:  client.NewPipeline(pipelineName),
		Transform: &pps.Transform{},
		Input:     &pps.Input{Pfs: &pps.PFSInput{Name: "out", Repo: dataRepo, Glob: "/*"}},
	})
	require.YesError(t, err)
	require.Matches(t, "input cannot be named \"out\"", err.Error())

	_, err = c.PpsAPIClient.CreatePipeline(c.Ctx(), &pps.CreatePipelineRequest{
		Pipeline:  client.NewPipeline(pipelineName),
		Transform: &pps.Transform{},
		Input:     &pps.Input{Pfs: &pps.PFSInput{Name: "data", Repo: "dne", Glob: "/*"}},
	})
	require.YesError(t, err)
	require.Matches(t, "dne[^ ]* not found", err.Error())

	_, err = c.PpsAPIClient.CreatePipeline(c.Ctx(), &pps.CreatePipelineRequest{
		Pipeline:  client.NewPipeline(pipelineName),
		Transform: &pps.Transform{},
		Input: client.NewCrossInput(
			client.NewPFSInput("foo", "/*"),
			client.NewPFSInput("foo", "/*"),
		),
	})
	require.YesError(t, err)
	require.Matches(t, "name \"foo\" was used more than once", err.Error())

	_, err = c.PpsAPIClient.CreatePipeline(c.Ctx(), &pps.CreatePipelineRequest{
		Pipeline:  client.NewPipeline(pipelineName),
		Transform: &pps.Transform{},
		Input:     &pps.Input{Cron: &pps.CronInput{}},
	})
	require.YesError(t, err)
	require.Matches(t, "input must specify a name", err.Error())

	_, err = c.PpsAPIClient.CreatePipeline(c.Ctx(), &pps.CreatePipelineRequest{
		Pipeline:  client.NewPipeline(pipelineName),
		Transform: &pps.Transform{},
		Input:     &pps.Input{Cron: &pps.CronInput{Name: "cron"}},
	})
	require.YesError(t, err)
	require.Matches(t, "Empty spec string", err.Error())

	// TODO: Implement git inputs.
	//_, err = c.PpsAPIClient.CreatePipeline(c.Ctx(), &pps.CreatePipelineRequest{
	//	Pipeline:  client.NewPipeline(pipelineName),
	//	Transform: &pps.Transform{},
	//	Input:     &pps.Input{Git: &pps.GitInput{}},
	//})
	//require.YesError(t, err)
	//require.Matches(t, "clone URL is missing \\(", err.Error())

	//_, err = c.PpsAPIClient.CreatePipeline(c.Ctx(), &pps.CreatePipelineRequest{
	//	Pipeline:  client.NewPipeline(pipelineName),
	//	Transform: &pps.Transform{},
	//	Input:     &pps.Input{Git: &pps.GitInput{URL: "foobar"}},
	//})
	//require.YesError(t, err)
	//require.Matches(t, "clone URL is missing .git suffix", err.Error())

	//_, err = c.PpsAPIClient.CreatePipeline(c.Ctx(), &pps.CreatePipelineRequest{
	//	Pipeline:  client.NewPipeline(pipelineName),
	//	Transform: &pps.Transform{},
	//	Input:     &pps.Input{Git: &pps.GitInput{URL: "foobar.git"}},
	//})
	//require.YesError(t, err)
	//require.Matches(t, "clone URL must use https protocol", err.Error())

	_, err = c.PpsAPIClient.CreatePipeline(c.Ctx(), &pps.CreatePipelineRequest{
		Pipeline:  client.NewPipeline(pipelineName),
		Transform: &pps.Transform{},
		Input:     &pps.Input{Cross: []*pps.Input{}},
	})
	require.YesError(t, err)
	require.Matches(t, "no input set", err.Error())

	_, err = c.PpsAPIClient.CreatePipeline(c.Ctx(), &pps.CreatePipelineRequest{
		Pipeline:  client.NewPipeline(pipelineName),
		Transform: &pps.Transform{},
		Input:     &pps.Input{Union: []*pps.Input{}},
	})
	require.YesError(t, err)
	require.Matches(t, "no input set", err.Error())

	_, err = c.PpsAPIClient.CreatePipeline(c.Ctx(), &pps.CreatePipelineRequest{
		Pipeline:  client.NewPipeline(pipelineName),
		Transform: &pps.Transform{},
		Input:     &pps.Input{Join: []*pps.Input{}},
	})
	require.YesError(t, err)
	require.Matches(t, "no input set", err.Error())
}

func TestTrigger(t *testing.T) {
	// TODO(2.0 required): This test does not work with V2. It is not clear what the issue is yet. Something noteworthy is that the output
	// size does not increase for the second to last commit, which may have something to do with compaction (the trigger won't kick
	// off since it is based on the output size). The output size calculation is a bit questionable due to the compaction delay (a
	// file may be accounted for in the size even if it is deleted).
	t.Skip("Does not work with V2, needs investigation")
	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())

	dataRepo := tu.UniqueString("TestTrigger_data")
	require.NoError(t, c.CreateRepo(dataRepo))
	dataCommit := client.NewCommit(dataRepo, "master", "")
	pipeline1 := tu.UniqueString("TestTrigger1")
	pipelineCommit1 := client.NewCommit(pipeline1, "master", "")
	pipeline2 := tu.UniqueString("TestTrigger2")
	pipelineCommit2 := client.NewCommit(pipeline2, "master", "")
	require.NoError(t, c.CreatePipeline(
		pipeline1,
		"",
		[]string{"bash"},
		[]string{
			fmt.Sprintf("cp /pfs/%s/* /pfs/out/", dataRepo),
		},
		&pps.ParallelismSpec{
			Constant: 1,
		},
		client.NewPFSInputOpts(dataRepo, dataRepo, "trigger", "/*", "", "", false, false, &pfs.Trigger{
			Branch: "master",
			Size_:  "1K",
		}),
		"",
		false,
	))
	require.NoError(t, c.CreatePipeline(
		pipeline2,
		"",
		[]string{"bash"},
		[]string{
			fmt.Sprintf("cp /pfs/%s/* /pfs/out/", pipeline1),
		},
		&pps.ParallelismSpec{
			Constant: 1,
		},
		client.NewPFSInputOpts(pipeline1, pipeline1, "", "/*", "", "", false, false, &pfs.Trigger{
			Size_: "2K",
		}),
		"",
		false,
	))
	// 10 100 byte files = 1K, so the last file should trigger pipeline1, but
	// not pipeline2.
	numFiles := 10
	fileBytes := 100
	for i := 0; i < numFiles; i++ {
		require.NoError(t, c.PutFile(dataCommit, fmt.Sprintf("file%d", i), strings.NewReader(strings.Repeat("a", fileBytes)), client.WithAppendPutFile()))
	}
	// This should have given us a job, flush to let it complete.
	commitInfos, err := c.WaitCommitSetAll(dataCommit.ID)
	require.NoError(t, err)
	require.Equal(t, 2, len(commitInfos))
	for i := 0; i < numFiles; i++ {
		var buf bytes.Buffer
		require.NoError(t, c.GetFile(pipelineCommit1, fmt.Sprintf("file%d", i), &buf))
		require.Equal(t, strings.Repeat("a", fileBytes), buf.String())
	}
	_, err = c.ListCommit(client.NewRepo(pipeline1), client.NewCommit(pipeline1, "master", ""), nil, 0)
	require.NoError(t, err)
	// Another 10 100 byte files = 2K, so the last file should trigger both pipelines.
	for i := numFiles; i < 2*numFiles; i++ {
		require.NoError(t, c.PutFile(dataCommit, fmt.Sprintf("file%d", i), strings.NewReader(strings.Repeat("a", fileBytes)), client.WithAppendPutFile()))
		require.NoError(t, err)
	}
	commitInfo, err := c.InspectCommit(dataRepo, "master", "")
	require.NoError(t, err)
	commitInfos, err = c.WaitCommitSetAll(commitInfo.Commit.ID)
	require.NoError(t, err)
	require.Equal(t, 4, len(commitInfos))
	for i := 0; i < numFiles*2; i++ {
		var buf bytes.Buffer
		require.NoError(t, c.GetFile(pipelineCommit1, fmt.Sprintf("file%d", i), &buf))
		require.Equal(t, strings.Repeat("a", fileBytes), buf.String())
		buf.Reset()
		require.NoError(t, c.GetFile(pipelineCommit2, fmt.Sprintf("file%d", i), &buf))
		require.Equal(t, strings.Repeat("a", fileBytes), buf.String())
	}
	commitInfos, err = c.ListCommit(client.NewRepo(pipeline1), client.NewCommit(pipeline1, "master", ""), nil, 0)
	require.NoError(t, err)
	require.Equal(t, 2, len(commitInfos))
	commitInfos, err = c.ListCommit(client.NewRepo(pipeline2), client.NewCommit(pipeline2, "master", ""), nil, 0)
	require.NoError(t, err)
	require.Equal(t, 1, len(commitInfos))

	require.NoError(t, c.CreatePipeline(
		pipeline2,
		"",
		[]string{"bash"},
		[]string{
			fmt.Sprintf("cp /pfs/%s/* /pfs/out/", pipeline1),
		},
		&pps.ParallelismSpec{
			Constant: 1,
		},
		client.NewPFSInputOpts(pipeline1, pipeline1, "", "/*", "", "", false, false, &pfs.Trigger{
			Size_: "3K",
		}),
		"",
		true,
	))

	// Make sure that updating the pipeline reuses the previous branch name
	// rather than creating a new one.
	bis, err := c.ListBranch(pipeline1)
	require.NoError(t, err)
	require.Equal(t, 3, len(bis))

	commitInfos, err = c.ListCommit(client.NewRepo(pipeline2), client.NewCommit(pipeline2, "master", ""), nil, 0)
	require.NoError(t, err)
	require.Equal(t, 2, len(commitInfos))

	// Another 30 100 byte files = 3K, so the last file should trigger both pipelines.
	for i := 2 * numFiles; i < 5*numFiles; i++ {
		require.NoError(t, c.PutFile(dataCommit, fmt.Sprintf("file%d", i), strings.NewReader(strings.Repeat("a", fileBytes)), client.WithAppendPutFile()))
	}

	commitInfos, err = c.WaitCommitSetAll(dataCommit.ID)
	require.NoError(t, err)
	require.Equal(t, 4, len(commitInfos))

	commitInfos, err = c.ListCommit(client.NewRepo(pipeline2), client.NewCommit(pipeline2, "master", ""), nil, 0)
	require.NoError(t, err)
	require.Equal(t, 3, len(commitInfos))
}

func TestListDatum(t *testing.T) {
	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())

	repo1 := tu.UniqueString("TestListDatum1")
	repo2 := tu.UniqueString("TestListDatum2")

	require.NoError(t, c.CreateRepo(repo1))
	require.NoError(t, c.CreateRepo(repo2))

	numFiles := 5
	for i := 0; i < numFiles; i++ {
		require.NoError(t, c.PutFile(client.NewCommit(repo1, "master", ""), fmt.Sprintf("file-%d", i), strings.NewReader("foo"), client.WithAppendPutFile()))
		require.NoError(t, c.PutFile(client.NewCommit(repo2, "master", ""), fmt.Sprintf("file-%d", i), strings.NewReader("foo"), client.WithAppendPutFile()))
	}

	dis, err := c.ListDatumInputAll(&pps.Input{
		Cross: []*pps.Input{{
			Pfs: &pps.PFSInput{
				Repo: repo1,
				Glob: "/*",
			},
		}, {
			Pfs: &pps.PFSInput{
				Repo: repo2,
				Glob: "/*",
			},
		}},
	})
	require.NoError(t, err)
	require.Equal(t, 25, len(dis))
}

func TestDebug(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())

	dataRepo := tu.UniqueString("TestDebug_data")
	require.NoError(t, c.CreateRepo(dataRepo))

	expectedFiles := make(map[string]*globlib.Glob)
	// Record glob patterns for expected pachd files.
	for _, file := range []string{"version", "logs", "logs-previous**", "goroutine", "heap"} {
		pattern := path.Join("pachd", "*", "pachd", file)
		g, err := globlib.Compile(pattern, '/')
		require.NoError(t, err)
		expectedFiles[pattern] = g
	}
	pattern := path.Join("input-repos", dataRepo, "commits")
	g, err := globlib.Compile(pattern, '/')
	require.NoError(t, err)
	expectedFiles[pattern] = g
	for i := 0; i < 3; i++ {
		pipeline := tu.UniqueString("TestDebug")
		require.NoError(t, c.CreatePipeline(
			pipeline,
			"",
			[]string{"bash"},
			[]string{
				fmt.Sprintf("cp /pfs/%s/* /pfs/out/", dataRepo),
			},
			&pps.ParallelismSpec{
				Constant: 1,
			},
			client.NewPFSInput(dataRepo, "/*"),
			"",
			false,
		))
		// Record glob patterns for expected pipeline files.
		for _, container := range []string{"user", "storage"} {
			for _, file := range []string{"logs", "logs-previous**", "goroutine", "heap"} {
				pattern := path.Join("pipelines", pipeline, "pods", "*", container, file)
				g, err := globlib.Compile(pattern, '/')
				require.NoError(t, err)
				expectedFiles[pattern] = g
			}
		}
		for _, file := range []string{"spec", "commits", "jobs"} {
			pattern := path.Join("pipelines", pipeline, file)
			g, err := globlib.Compile(pattern, '/')
			require.NoError(t, err)
			expectedFiles[pattern] = g
		}
	}

	commit1, err := c.StartCommit(dataRepo, "master")
	require.NoError(t, err)
	require.NoError(t, c.PutFile(commit1, "file", strings.NewReader("foo"), client.WithAppendPutFile()))
	require.NoError(t, c.FinishCommit(dataRepo, commit1.Branch.Name, commit1.ID))

	commitInfos, err := c.WaitCommitSetAll(commit1.ID)
	require.NoError(t, err)
	require.Equal(t, 10, len(commitInfos))

	buf := &bytes.Buffer{}
	require.NoError(t, c.Dump(nil, 0, buf))
	gr, err := gzip.NewReader(buf)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, gr.Close())
	}()
	// Check that all of the expected files were returned.
	tr := tar.NewReader(gr)
	for {
		hdr, err := tr.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
		}
		for pattern, g := range expectedFiles {
			if g.Match(hdr.Name) {
				delete(expectedFiles, pattern)
				break
			}
		}
	}
	require.Equal(t, 0, len(expectedFiles))
}

func TestUpdateMultiplePipelinesInTransaction(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}
	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())
	input := tu.UniqueString("in")
	commit := client.NewCommit(input, "master", "")
	pipelineA := tu.UniqueString("A")
	pipelineB := tu.UniqueString("B")
	repoB := client.NewRepo(pipelineB)

	createPipeline := func(c *client.APIClient, input, pipeline string, update bool) error {
		return c.CreatePipeline(
			pipeline,
			"",
			[]string{"bash"},
			[]string{fmt.Sprintf("cp /pfs/%s/* /pfs/out/", input)},
			&pps.ParallelismSpec{
				Constant: 1,
			},
			client.NewPFSInput(input, "/*"),
			"",
			update,
		)
	}

	require.NoError(t, c.CreateRepo(input))
	require.NoError(t, c.PutFile(commit, "foo", strings.NewReader("bar"), client.WithAppendPutFile()))

	_, err := c.ExecuteInTransaction(func(txnClient *client.APIClient) error {
		require.NoError(t, createPipeline(txnClient, input, pipelineA, false))
		require.NoError(t, createPipeline(txnClient, pipelineA, pipelineB, false))
		return nil
	})
	require.NoError(t, err)
	_, err = c.WaitCommit(pipelineB, "master", "")
	require.NoError(t, err)

	// now update both
	_, err = c.ExecuteInTransaction(func(txnClient *client.APIClient) error {
		require.NoError(t, createPipeline(txnClient, input, pipelineA, true))
		require.NoError(t, createPipeline(txnClient, pipelineA, pipelineB, true))
		return nil
	})
	require.NoError(t, err)

	_, err = c.WaitCommit(pipelineB, "master", "")
	require.NoError(t, err)
	commits, err := c.ListCommitByRepo(repoB)
	require.NoError(t, err)
	require.Equal(t, 2, len(commits))

	jobInfos, err := c.ListJob(pipelineB, nil, -1, false)
	require.NoError(t, err)
	require.Equal(t, 2, len(jobInfos))
}

func TestInterruptedUpdatePipelineInTransaction(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())
	inputA := tu.UniqueString("A")
	inputB := tu.UniqueString("B")
	inputC := tu.UniqueString("C")
	pipeline := tu.UniqueString("pipeline")

	createPipeline := func(c *client.APIClient, input string, update bool) error {
		return c.CreatePipeline(
			pipeline,
			"",
			[]string{"bash"},
			[]string{fmt.Sprintf("cp /pfs/%s/* /pfs/out/", input)},
			&pps.ParallelismSpec{
				Constant: 1,
			},
			client.NewPFSInput(input, "/*"),
			"",
			update,
		)
	}

	require.NoError(t, c.CreateRepo(inputA))
	require.NoError(t, c.CreateRepo(inputB))
	require.NoError(t, c.CreateRepo(inputC))
	require.NoError(t, createPipeline(c, inputA, false))

	txn, err := c.StartTransaction()
	require.NoError(t, err)

	require.NoError(t, createPipeline(c.WithTransaction(txn), inputB, true))
	require.NoError(t, createPipeline(c, inputC, true))

	_, err = c.FinishTransaction(txn)
	require.NoError(t, err)
	// make sure the final pipeline is the third version, with the input from in the transaction
	pipelineInfo, err := c.InspectPipeline(pipeline, true)
	require.NoError(t, err)
	require.Equal(t, uint64(3), pipelineInfo.Version)
	require.NotNil(t, pipelineInfo.Details.Input.Pfs)
	require.Equal(t, inputB, pipelineInfo.Details.Input.Pfs.Repo)
}

func TestSystemRepoDependence(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())
	input := tu.UniqueString("in")
	pipeline := tu.UniqueString("pipeline")

	require.NoError(t, c.CreateRepo(input))
	require.NoError(t, c.CreatePipeline(
		pipeline,
		"",
		[]string{"bash"},
		[]string{fmt.Sprintf("cp /pfs/%s/* /pfs/out/", input)},
		&pps.ParallelismSpec{
			Constant: 1,
		},
		client.NewPFSInput(input, "/*"),
		"",
		false,
	))

	// meta repo has no subvenance
	_, err := c.PfsAPIClient.DeleteRepo(
		c.Ctx(),
		&pfs.DeleteRepoRequest{
			Repo: client.NewSystemRepo(pipeline, pfs.MetaRepoType),
		})
	require.NoError(t, err)

	// but spec repo does
	_, err = c.PfsAPIClient.DeleteRepo(
		c.Ctx(),
		&pfs.DeleteRepoRequest{
			Repo: client.NewSystemRepo(pipeline, pfs.SpecRepoType),
		})
	require.YesError(t, err)

	require.NoError(t, c.DeletePipeline(pipeline, false))

	_, err = c.PfsAPIClient.InspectRepo(
		c.Ctx(),
		&pfs.InspectRepoRequest{
			Repo: client.NewSystemRepo(pipeline, pfs.SpecRepoType),
		},
	)
	// spec repo should have been deleted
	require.YesError(t, err)
	require.True(t, errutil.IsNotFoundError(grpcutil.ScrubGRPC(err)))
}

func TestPipelineAutoscaling(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())

	dataRepo := tu.UniqueString("TestPipelineAutoscaling_data")
	require.NoError(t, c.CreateRepo(dataRepo))

	pipeline := tu.UniqueString("pipeline")
	_, err := c.PpsAPIClient.CreatePipeline(context.Background(),
		&pps.CreatePipelineRequest{
			Pipeline: client.NewPipeline(pipeline),
			Transform: &pps.Transform{
				Cmd: []string{"bash"},
				Stdin: []string{
					fmt.Sprintf("cp /pfs/%s/* /pfs/out/", dataRepo),
					"sleep 30",
				},
			},
			Input:           client.NewPFSInput(dataRepo, "/*"),
			ParallelismSpec: &pps.ParallelismSpec{Constant: 4},
			Autoscaling:     true,
		},
	)
	require.NoError(t, err)

	fileIndex := 0
	commitNFiles := func(n int) {
		commit1, err := c.StartCommit(dataRepo, "master")
		require.NoError(t, err)
		for i := fileIndex; i < fileIndex+n; i++ {
			err := c.PutFile(commit1, fmt.Sprintf("file-%d", i), strings.NewReader(fmt.Sprintf("%d", i)))
			require.NoError(t, err)
		}
		require.NoError(t, c.FinishCommit(dataRepo, "master", commit1.ID))
		fileIndex += n
		replicas := n
		if replicas > 4 {
			replicas = 4
		}
		monitorReplicas(t, pipeline, replicas)
	}
	commitNFiles(1)
	commitNFiles(3)
	commitNFiles(8)
}

func TestListDeletedDatums(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}
	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())

	twoRepo := tu.UniqueString("TestListDeletedDatums_Two")
	threeRepo := tu.UniqueString("TestListDeletedDatums_Three")
	require.NoError(t, c.CreateRepo(twoRepo))
	require.NoError(t, c.CreateRepo(threeRepo))

	input := client.NewJoinInput(
		client.NewPFSInput(twoRepo, "/(*)"),
		client.NewPFSInput(threeRepo, "/(*)"),
	)
	input.Join[0].Pfs.JoinOn = "$1"
	input.Join[1].Pfs.JoinOn = "$1"

	// put files into the repo based on divisibility by 2 and 3
	twoCommit, err := c.StartCommit(twoRepo, "master")
	require.NoError(t, err)
	threeCommit, err := c.StartCommit(threeRepo, "master")
	require.NoError(t, err)

	const fileLimit = 12
	for i := 0; i < fileLimit; i++ {
		if i%2 == 0 {
			require.NoError(t, c.PutFile(twoCommit, strconv.Itoa(i), strings.NewReader("fizz")))
		}
		if i%3 == 0 {
			require.NoError(t, c.PutFile(threeCommit, strconv.Itoa(i), strings.NewReader("buzz")))
		}
	}
	require.NoError(t, c.FinishCommit(twoRepo, "master", twoCommit.ID))
	require.NoError(t, c.FinishCommit(threeRepo, "master", threeCommit.ID))

	pipeline := tu.UniqueString("pipeline")
	createAndCheckJoin := func(twoOuter, threeOuter bool) {
		input.Join[0].Pfs.OuterJoin = twoOuter
		input.Join[1].Pfs.OuterJoin = threeOuter
		_, err := c.PpsAPIClient.CreatePipeline(context.Background(),
			&pps.CreatePipelineRequest{
				Pipeline: client.NewPipeline(pipeline),
				Transform: &pps.Transform{
					Cmd:   []string{"bash"},
					Stdin: []string{"touch pfs/out/ignore"},
				},
				Input:  input,
				Update: true,
			},
		)
		require.NoError(t, err)
		// wait for job and get its datums
		jobs, err := c.ListJob(pipeline, nil, 0, false)
		require.NoError(t, err)
		require.Equal(t, 1, len(jobs))
		id := jobs[0].Job.ID
		info, err := c.WaitJob(pipeline, id, false)
		require.NoError(t, err)
		require.Equal(t, pps.JobState_JOB_SUCCESS, info.State)
		datums, err := c.ListDatumAll(pipeline, id)
		require.NoError(t, err)

		// find which file numbers are present in the datums
		numSet := make(map[int]struct{})
		for _, d := range datums {
			key, err := strconv.Atoi(path.Base(d.Data[0].File.Path))
			require.NoError(t, err)
			numSet[key] = struct{}{}
		}
		// we shouldn't have any duplicate keys across datums for any of these joins
		require.Equal(t, len(numSet), len(datums))

		// now check against what should be included
		for i := 0; i < fileLimit; i++ {
			expected := (i%6 == 0) || (i%2 == 0 && twoOuter) || (i%3 == 0 && threeOuter)
			_, ok := numSet[i]
			require.Equal(t, expected, ok)
		}
	}

	// first a completely outer join with all datums
	createAndCheckJoin(true, true)
	// then the half-outer joins, which should be smaller
	createAndCheckJoin(true, false)
	createAndCheckJoin(false, true)
	// finally the smallest, should only have datums divisible by 6
	createAndCheckJoin(false, false)
}

// TestNonrootPipeline tests that a pipeline can work when it's running as a UID that's not 0 (root).
// This test doesn't work in a typical local deployment because the storage sidecar needs to
// run as root to access the host path where the chunks are stored (at /pach).
func TestNonrootPipeline(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	// Only run this test in CI since we can control the permissions on the hostpath
	if os.Getenv("CIRCLE_REPOSITORY_URL") == "" {
		t.Skip("Skipping non-root test because hostpath permissions can't be verified")
	}

	c := tu.GetPachClient(t)
	require.NoError(t, c.DeleteAll())

	dataRepo := tu.UniqueString("TestNonrootPipeline_data")
	require.NoError(t, c.CreateRepo(dataRepo))

	commit1, err := c.StartCommit(dataRepo, "master")
	require.NoError(t, err)
	require.NoError(t, c.PutFile(commit1, "file", strings.NewReader("foo"), client.WithAppendPutFile()))
	require.NoError(t, c.FinishCommit(dataRepo, commit1.Branch.Name, commit1.ID))

	pipeline := tu.UniqueString("TestNonrootPipeline")
	_, err = c.PpsAPIClient.CreatePipeline(
		c.Ctx(),
		&pps.CreatePipelineRequest{
			Pipeline: client.NewPipeline(pipeline),
			Transform: &pps.Transform{
				Cmd:   []string{"bash"},
				Stdin: []string{fmt.Sprintf("cp /pfs/%s/* /pfs/out/", dataRepo)},
			},
			ParallelismSpec: &pps.ParallelismSpec{
				Constant: 1,
			},
			Input:        client.NewPFSInput(dataRepo, "/*"),
			OutputBranch: "",
			Update:       false,
			PodPatch:     `[{"op": "add",  "path": "/securityContext/runAsUser",  "value": 1000}]`,
		},
	)
	require.NoError(t, err)

	commitInfo, err := c.InspectCommit(pipeline, "master", "")
	require.NoError(t, err)
	commitInfos, err := c.WaitCommitSetAll(commitInfo.Commit.ID)
	require.NoError(t, err)
	// The commitset should have a commit in: data, spec, pipeline, meta
	// the last two are dependent upon the first two, so should come later
	// in topological ordering
	require.Equal(t, 4, len(commitInfos))
	var commitRepos []*pfs.Repo
	for _, info := range commitInfos {
		commitRepos = append(commitRepos, info.Commit.Branch.Repo)
	}
	require.EqualOneOf(t, commitRepos[:2], client.NewRepo(dataRepo))
	require.EqualOneOf(t, commitRepos[:2], client.NewSystemRepo(pipeline, pfs.SpecRepoType))
	require.EqualOneOf(t, commitRepos[2:], client.NewRepo(pipeline))
	require.EqualOneOf(t, commitRepos[2:], client.NewSystemRepo(pipeline, pfs.MetaRepoType))

	var buf bytes.Buffer
	for _, info := range commitInfos {
		if proto.Equal(info.Commit.Branch.Repo, client.NewRepo(pipeline)) {
			require.NoError(t, c.GetFile(info.Commit, "file", &buf))
			require.Equal(t, "foo", buf.String())
		}
	}
}

func monitorReplicas(t testing.TB, pipeline string, n int) {
	c := tu.GetPachClient(t)
	kc := tu.GetKubeClient(t)
	rcName := ppsutil.PipelineRcName(pipeline, 1)
	enoughReplicas := false
	tooManyReplicas := false
	require.NoErrorWithinTRetry(t, 180*time.Second, func() error {
		for {
			scale, err := kc.CoreV1().ReplicationControllers("default").GetScale(rcName, metav1.GetOptions{})
			if err != nil {
				return err
			}
			if int(scale.Spec.Replicas) >= n {
				enoughReplicas = true
			}
			if int(scale.Spec.Replicas) > n {
				tooManyReplicas = true
			}
			ci, err := c.InspectCommit(pipeline, "master", "")
			require.NoError(t, err)
			if ci.Finished != nil {
				return nil
			}
			time.Sleep(time.Second * 2)
		}
	})
	require.True(t, enoughReplicas, "didn't get enough replicas, looking for: %d", n)
	require.False(t, tooManyReplicas, "got too many replicas, looking for: %d", n)
}

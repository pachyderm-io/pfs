package server

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gogo/protobuf/types"
	pclient "github.com/pachyderm/pachyderm/src/client"
	"github.com/pachyderm/pachyderm/src/client/pfs"
	"github.com/pachyderm/pachyderm/src/client/pkg/require"
	"github.com/pachyderm/pachyderm/src/server/pkg/ancestry"
	"github.com/pachyderm/pachyderm/src/server/pkg/backoff"
	"github.com/pachyderm/pachyderm/src/server/pkg/hashtree"
	"github.com/pachyderm/pachyderm/src/server/pkg/obj"
	"github.com/pachyderm/pachyderm/src/server/pkg/sql"
	pfssync "github.com/pachyderm/pachyderm/src/server/pkg/sync"
	tu "github.com/pachyderm/pachyderm/src/server/pkg/testutil"
	"github.com/pachyderm/pachyderm/src/server/pkg/uuid"

	"golang.org/x/net/context"
	"golang.org/x/sync/errgroup"
)

const (
	MB = 1024 * 1024
)

func collectCommitInfos(commitInfoIter pclient.CommitInfoIterator) ([]*pfs.CommitInfo, error) {
	var commitInfos []*pfs.CommitInfo
	for {
		commitInfo, err := commitInfoIter.Next()
		if err == io.EOF {
			return commitInfos, nil
		}
		if err != nil {
			return nil, err
		}
		commitInfos = append(commitInfos, commitInfo)
	}
}

func CommitToID(commit interface{}) interface{} {
	return commit.(*pfs.Commit).ID
}

func CommitInfoToID(commit interface{}) interface{} {
	return commit.(*pfs.CommitInfo).Commit.ID
}

func RepoInfoToName(repoInfo interface{}) interface{} {
	return repoInfo.(*pfs.RepoInfo).Repo.Name
}

func RepoToName(repo interface{}) interface{} {
	return repo.(*pfs.Repo).Name
}

func FileInfoToPath(fileInfo interface{}) interface{} {
	return fileInfo.(*pfs.FileInfo).File.Path
}

func TestInvalidRepo(t *testing.T) {
	client := GetPachClient(t)
	require.YesError(t, client.CreateRepo("/repo"))

	require.NoError(t, client.CreateRepo("lenny"))
	require.NoError(t, client.CreateRepo("lenny123"))
	require.NoError(t, client.CreateRepo("lenny_123"))
	require.NoError(t, client.CreateRepo("lenny-123"))

	require.YesError(t, client.CreateRepo("lenny.123"))
	require.YesError(t, client.CreateRepo("lenny:"))
	require.YesError(t, client.CreateRepo("lenny,"))
	require.YesError(t, client.CreateRepo("lenny#"))
}

func TestCreateSameRepoInParallel(t *testing.T) {
	client := GetPachClient(t)

	numGoros := 1000
	errCh := make(chan error)
	for i := 0; i < numGoros; i++ {
		go func() {
			errCh <- client.CreateRepo("repo")
		}()
	}
	successCount := 0
	totalCount := 0
	for err := range errCh {
		totalCount++
		if err == nil {
			successCount++
		}
		if totalCount == numGoros {
			break
		}
	}
	// When creating the same repo, precisiely one attempt should succeed
	require.Equal(t, 1, successCount)
}

func TestCreateDifferentRepoInParallel(t *testing.T) {
	client := GetPachClient(t)

	numGoros := 1000
	errCh := make(chan error)
	for i := 0; i < numGoros; i++ {
		i := i
		go func() {
			errCh <- client.CreateRepo(fmt.Sprintf("repo%d", i))
		}()
	}
	successCount := 0
	totalCount := 0
	for err := range errCh {
		totalCount++
		if err == nil {
			successCount++
		}
		if totalCount == numGoros {
			break
		}
	}
	require.Equal(t, numGoros, successCount)
}

func TestCreateRepoDeleteRepoRace(t *testing.T) {
	client := GetPachClient(t)

	for i := 0; i < 100; i++ {
		require.NoError(t, client.CreateRepo("foo"))
		require.NoError(t, client.CreateRepo("bar"))
		errCh := make(chan error)
		go func() {
			errCh <- client.DeleteRepo("foo", false)
		}()
		go func() {
			errCh <- client.CreateBranch("bar", "master", "", []*pfs.Branch{pclient.NewBranch("foo", "master")})
		}()
		err1 := <-errCh
		err2 := <-errCh
		// these two operations should never race in such a way that they
		// both succeed, leaving us with a repo bar that has a nonexistent
		// provenance foo
		require.True(t, err1 != nil || err2 != nil)
		client.DeleteRepo("bar", true)
		client.DeleteRepo("foo", true)
	}
}

func TestBranch(t *testing.T) {
	c := GetPachClient(t)

	repo := "repo"
	require.NoError(t, c.CreateRepo(repo))
	_, err := c.StartCommit(repo, "master")
	require.NoError(t, err)
	require.NoError(t, c.FinishCommit(repo, "master"))
	commitInfo, err := c.InspectCommit(repo, "master")
	require.NoError(t, err)
	require.Nil(t, commitInfo.ParentCommit)

	_, err = c.StartCommit(repo, "master")
	require.NoError(t, err)
	require.NoError(t, c.FinishCommit(repo, "master"))
	commitInfo, err = c.InspectCommit(repo, "master")
	require.NoError(t, err)
	require.NotNil(t, commitInfo.ParentCommit)
}

func TestCreateAndInspectRepo(t *testing.T) {
	client := GetPachClient(t)

	repo := "repo"
	require.NoError(t, client.CreateRepo(repo))

	repoInfo, err := client.InspectRepo(repo)
	require.NoError(t, err)
	require.Equal(t, repo, repoInfo.Repo.Name)
	require.NotNil(t, repoInfo.Created)
	require.Equal(t, 0, int(repoInfo.SizeBytes))

	require.YesError(t, client.CreateRepo(repo))
	_, err = client.InspectRepo("nonexistent")
	require.YesError(t, err)

	_, err = client.PfsAPIClient.CreateRepo(context.Background(), &pfs.CreateRepoRequest{
		Repo: pclient.NewRepo("somerepo1"),
	})
	require.NoError(t, err)
}

func TestListRepo(t *testing.T) {
	client := GetPachClient(t)

	numRepos := 10
	var repoNames []string
	for i := 0; i < numRepos; i++ {
		repo := fmt.Sprintf("repo%d", i)
		require.NoError(t, client.CreateRepo(repo))
		repoNames = append(repoNames, repo)
	}

	repoInfos, err := client.ListRepo()
	require.NoError(t, err)
	require.ElementsEqualUnderFn(t, repoNames, repoInfos, RepoInfoToName)
}

// Make sure that commits of deleted repos do not resurface
func TestCreateDeletedRepo(t *testing.T) {
	client := GetPachClient(t)

	repo := "repo"
	require.NoError(t, client.CreateRepo(repo))

	commit, err := client.StartCommit(repo, "")
	require.NoError(t, err)
	_, err = client.PutFile(repo, commit.ID, "foo", strings.NewReader("foo"))
	require.NoError(t, err)
	require.NoError(t, client.FinishCommit(repo, commit.ID))

	commitInfos, err := client.ListCommit(repo, "", "", 0)
	require.NoError(t, err)
	require.Equal(t, 1, len(commitInfos))

	require.NoError(t, client.DeleteRepo(repo, false))
	require.NoError(t, client.CreateRepo(repo))

	commitInfos, err = client.ListCommit(repo, "", "", 0)
	require.NoError(t, err)
	require.Equal(t, 0, len(commitInfos))
}

// The DAG looks like this before the update:
// prov1 prov2
//   \    /
//    repo
//   /    \
// d1      d2
//
// Looks like this after the update:
//
// prov2 prov3
//   \    /
//    repo
//   /    \
// d1      d2
func TestUpdateProvenance(t *testing.T) {
	client := GetPachClient(t)

	prov1 := "prov1"
	require.NoError(t, client.CreateRepo(prov1))
	prov2 := "prov2"
	require.NoError(t, client.CreateRepo(prov2))
	prov3 := "prov3"
	require.NoError(t, client.CreateRepo(prov3))

	repo := "repo"
	require.NoError(t, client.CreateRepo(repo))
	require.NoError(t, client.CreateBranch(repo, "master", "", []*pfs.Branch{pclient.NewBranch(prov1, "master"), pclient.NewBranch(prov2, "master")}))

	downstream1 := "downstream1"
	require.NoError(t, client.CreateRepo(downstream1))
	require.NoError(t, client.CreateBranch(downstream1, "master", "", []*pfs.Branch{pclient.NewBranch(repo, "master")}))

	downstream2 := "downstream2"
	require.NoError(t, client.CreateRepo(downstream2))
	require.NoError(t, client.CreateBranch(downstream2, "master", "", []*pfs.Branch{pclient.NewBranch(repo, "master")}))

	// Without the Update flag it should fail
	require.YesError(t, client.CreateRepo(repo))

	_, err := client.PfsAPIClient.CreateRepo(context.Background(), &pfs.CreateRepoRequest{
		Repo:   pclient.NewRepo(repo),
		Update: true,
	})
	require.NoError(t, err)

	require.NoError(t, client.CreateBranch(repo, "master", "", []*pfs.Branch{pclient.NewBranch(prov2, "master"), pclient.NewBranch(prov3, "master")}))

	// We should be able to delete prov1 since it's no longer the provenance
	// of other repos.
	require.NoError(t, client.DeleteRepo(prov1, false))

	// We shouldn't be able to delete prov3 since it's now a provenance
	// of other repos.
	require.YesError(t, client.DeleteRepo(prov3, false))
}

func TestPutFileIntoOpenCommit(t *testing.T) {
	client := GetPachClient(t)

	repo := "test"
	require.NoError(t, client.CreateRepo(repo))

	commit1, err := client.StartCommit(repo, "master")
	require.NoError(t, err)
	_, err = client.PutFile(repo, commit1.ID, "foo", strings.NewReader("foo\n"))
	require.NoError(t, err)
	require.NoError(t, client.FinishCommit(repo, commit1.ID))

	_, err = client.PutFile(repo, commit1.ID, "foo", strings.NewReader("foo\n"))
	require.YesError(t, err)

	commit2, err := client.StartCommit(repo, "master")
	require.NoError(t, err)
	_, err = client.PutFile(repo, "master", "foo", strings.NewReader("foo\n"))
	require.NoError(t, err)
	require.NoError(t, client.FinishCommit(repo, "master"))

	_, err = client.PutFile(repo, commit2.ID, "foo", strings.NewReader("foo\n"))
	require.YesError(t, err)
}

// Regression test: putting multiple files on an open commit would error out.
// See here for more details: https://github.com/pachyderm/pachyderm/pull/3346
func TestRegressionPutFileIntoOpenCommit(t *testing.T) {
	client := GetPachClient(t)
	require.NoError(t, client.CreateRepo("repo"))

	_, err := client.StartCommit("repo", "master")
	require.NoError(t, err)

	writer, err := client.NewPutFileClient()
	require.NoError(t, err)

	_, err = writer.PutFile("repo", "master", "foo", strings.NewReader("foo\n"))
	require.NoError(t, err)
	_, err = writer.PutFile("repo", "master", "bar", strings.NewReader("bar\n"))
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	var fileInfos []*pfs.FileInfo
	fileInfos, err = client.ListFile("repo", "master", "")
	require.NoError(t, err)
	require.Equal(t, 2, len(fileInfos))

	err = client.Close()
	require.NoError(t, err)
}

func TestPutFileDirectoryTraversal(t *testing.T) {
	var fileInfos []*pfs.FileInfo
	client := GetPachClient(t)
	require.NoError(t, client.CreateRepo("repo"))

	_, err := client.StartCommit("repo", "master")
	require.NoError(t, err)

	writer, err := client.NewPutFileClient()
	require.NoError(t, err)
	_, err = writer.PutFile("repo", "master", "../foo", strings.NewReader("foo\n"))
	require.NoError(t, err)
	err = writer.Close()
	require.YesError(t, err)

	fileInfos, err = client.ListFile("repo", "master", "")
	require.NoError(t, err)
	require.Equal(t, 0, len(fileInfos))

	writer, err = client.NewPutFileClient()
	require.NoError(t, err)
	_, err = writer.PutFile("repo", "master", "foo/../../bar", strings.NewReader("foo\n"))
	require.NoError(t, err)
	err = writer.Close()
	require.YesError(t, err)

	fileInfos, err = client.ListFile("repo", "master", "")
	require.NoError(t, err)
	require.Equal(t, 0, len(fileInfos))

	writer, err = client.NewPutFileClient()
	require.NoError(t, err)
	_, err = writer.PutFile("repo", "master", "foo/../bar", strings.NewReader("foo\n"))
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)

	fileInfos, err = client.ListFile("repo", "master", "")
	require.NoError(t, err)
	require.Equal(t, 1, len(fileInfos))

	err = client.Close()
	require.NoError(t, err)
}

func TestCreateInvalidBranchName(t *testing.T) {

	client := GetPachClient(t)

	repo := "test"
	require.NoError(t, client.CreateRepo(repo))

	// Create a branch that's the same length as a commit ID
	_, err := client.StartCommit(repo, uuid.NewWithoutDashes())
	require.YesError(t, err)
}

func TestDeleteRepo(t *testing.T) {
	client := GetPachClient(t)

	numRepos := 10
	repoNames := make(map[string]bool)
	for i := 0; i < numRepos; i++ {
		repo := fmt.Sprintf("repo%d", i)
		require.NoError(t, client.CreateRepo(repo))
		repoNames[repo] = true
	}

	reposToRemove := 5
	for i := 0; i < reposToRemove; i++ {
		// Pick one random element from repoNames
		for repoName := range repoNames {
			require.NoError(t, client.DeleteRepo(repoName, false))
			delete(repoNames, repoName)
			break
		}
	}

	repoInfos, err := client.ListRepo()
	require.NoError(t, err)

	for _, repoInfo := range repoInfos {
		require.True(t, repoNames[repoInfo.Repo.Name])
	}

	require.Equal(t, len(repoInfos), numRepos-reposToRemove)
}

func TestDeleteProvenanceRepo(t *testing.T) {
	client := GetPachClient(t)

	// Create two repos, one as another's provenance
	require.NoError(t, client.CreateRepo("A"))
	require.NoError(t, client.CreateRepo("B"))
	require.NoError(t, client.CreateBranch("B", "master", "", []*pfs.Branch{pclient.NewBranch("A", "master")}))

	// Delete the provenance repo; that should fail.
	require.YesError(t, client.DeleteRepo("A", false))

	// Delete the leaf repo, then the provenance repo; that should succeed
	require.NoError(t, client.DeleteRepo("B", false))
	require.NoError(t, client.DeleteRepo("A", false))

	repoInfos, err := client.ListRepo()
	require.NoError(t, err)
	require.Equal(t, 0, len(repoInfos))

	// Create two repos again
	require.NoError(t, client.CreateRepo("A"))
	require.NoError(t, client.CreateRepo("B"))
	require.NoError(t, client.CreateBranch("B", "master", "", []*pfs.Branch{pclient.NewBranch("A", "master")}))

	// Force delete should succeed
	require.NoError(t, client.DeleteRepo("A", true))

	repoInfos, err = client.ListRepo()
	require.NoError(t, err)
	require.Equal(t, 1, len(repoInfos))
}

func TestInspectCommit(t *testing.T) {
	client := GetPachClient(t)

	repo := "test"
	require.NoError(t, client.CreateRepo(repo))

	started := time.Now()
	commit, err := client.StartCommit(repo, "")
	require.NoError(t, err)

	fileContent := "foo\n"
	_, err = client.PutFile(repo, commit.ID, "foo", strings.NewReader(fileContent))
	require.NoError(t, err)

	commitInfo, err := client.InspectCommit(repo, commit.ID)
	require.NoError(t, err)

	tStarted, err := types.TimestampFromProto(commitInfo.Started)
	require.NoError(t, err)

	require.Equal(t, commit, commitInfo.Commit)
	require.Nil(t, commitInfo.Finished)
	// PutFile does not update commit size; only FinishCommit does
	require.Equal(t, 0, int(commitInfo.SizeBytes))
	require.True(t, started.Before(tStarted))
	require.Nil(t, commitInfo.Finished)

	require.NoError(t, client.FinishCommit(repo, commit.ID))
	finished := time.Now()

	commitInfo, err = client.InspectCommit(repo, commit.ID)
	require.NoError(t, err)

	tStarted, err = types.TimestampFromProto(commitInfo.Started)
	require.NoError(t, err)

	tFinished, err := types.TimestampFromProto(commitInfo.Finished)
	require.NoError(t, err)

	require.Equal(t, commit, commitInfo.Commit)
	require.NotNil(t, commitInfo.Finished)
	require.Equal(t, len(fileContent), int(commitInfo.SizeBytes))
	require.True(t, started.Before(tStarted))
	require.True(t, finished.After(tFinished))
}

func TestInspectCommitBlock(t *testing.T) {
	client := GetPachClient(t)

	repo := "TestInspectCommitBlock"
	require.NoError(t, client.CreateRepo(repo))
	commit, err := client.StartCommit(repo, "")
	require.NoError(t, err)

	go func() {
		time.Sleep(2 * time.Second)
		require.NoError(t, client.FinishCommit(repo, commit.ID))
	}()

	commitInfo, err := client.BlockCommit(commit.Repo.Name, commit.ID)
	require.NoError(t, err)
	require.NotNil(t, commitInfo.Finished)
}

func TestDeleteCommit(t *testing.T) {
	client := GetPachClient(t)

	repo := "test"
	require.NoError(t, client.CreateRepo(repo))

	commit1, err := client.StartCommit(repo, "master")
	require.NoError(t, err)

	fileContent := "foo\n"
	_, err = client.PutFile(repo, commit1.ID, "foo", strings.NewReader(fileContent))
	require.NoError(t, err)

	require.NoError(t, client.FinishCommit(repo, "master"))

	commit2, err := client.StartCommit(repo, "master")
	require.NoError(t, err)

	require.NoError(t, client.DeleteCommit(repo, commit2.ID))

	commitInfo, err := client.InspectCommit(repo, commit2.ID)
	require.YesError(t, err)

	// Check that the head has been set to the parent
	commitInfo, err = client.InspectCommit(repo, "master")
	require.NoError(t, err)
	require.Equal(t, commit1.ID, commitInfo.Commit.ID)

	// Check that the branch still exists
	branches, err := client.ListBranch(repo)
	require.NoError(t, err)
	require.Equal(t, 1, len(branches))
}

func TestDeleteCommitOnlyCommitInBranch(t *testing.T) {
	client := GetPachClient(t)

	repo := "test"
	require.NoError(t, client.CreateRepo(repo))

	commit, err := client.StartCommit(repo, "master")
	require.NoError(t, err)
	_, err = client.PutFile(repo, commit.ID, "foo", strings.NewReader("foo\n"))
	require.NoError(t, err)
	require.NoError(t, client.DeleteCommit(repo, "master"))

	// The branch has not been deleted, though it has no commits
	branches, err := client.ListBranch(repo)
	require.NoError(t, err)
	require.Equal(t, 1, len(branches))
	commits, err := client.ListCommit(repo, "master", "", 0)
	require.NoError(t, err)
	require.Equal(t, 0, len(commits))

	// Check that repo size is back to 0
	repoInfo, err := client.InspectRepo(repo)
	require.Equal(t, 0, int(repoInfo.SizeBytes))
}

func TestDeleteCommitFinished(t *testing.T) {
	client := GetPachClient(t)

	repo := "test"
	require.NoError(t, client.CreateRepo(repo))

	commit, err := client.StartCommit(repo, "master")
	require.NoError(t, err)
	_, err = client.PutFile(repo, commit.ID, "foo", strings.NewReader("foo\n"))
	require.NoError(t, err)
	require.NoError(t, client.FinishCommit(repo, commit.ID))
	require.NoError(t, client.DeleteCommit(repo, "master"))

	// The branch has not been deleted, though it has no commits
	branches, err := client.ListBranch(repo)
	require.NoError(t, err)
	require.Equal(t, 1, len(branches))
	commits, err := client.ListCommit(repo, "master", "", 0)
	require.NoError(t, err)
	require.Equal(t, 0, len(commits))

	// Check that repo size is back to 0
	repoInfo, err := client.InspectRepo(repo)
	require.Equal(t, 0, int(repoInfo.SizeBytes))
}

func TestCleanPath(t *testing.T) {
	c := GetPachClient(t)
	repo := "TestCleanPath"
	require.NoError(t, c.CreateRepo(repo))
	commit, err := c.StartCommit(repo, "master")
	require.NoError(t, err)
	_, err = c.PutFile(repo, commit.ID, "./././file", strings.NewReader("foo"))
	require.NoError(t, err)
	require.NoError(t, c.FinishCommit(repo, commit.ID))
	_, err = c.InspectFile(repo, commit.ID, "file")
	require.NoError(t, err)
}

func TestBasicFile(t *testing.T) {
	client := GetPachClient(t)

	repo := "repo"
	require.NoError(t, client.CreateRepo(repo))

	commit, err := client.StartCommit(repo, "")
	require.NoError(t, err)

	file := "file"
	data := "data"
	_, err = client.PutFile(repo, commit.ID, file, strings.NewReader(data))
	require.NoError(t, err)
	var b bytes.Buffer
	require.NoError(t, client.GetFile(repo, commit.ID, "file", 0, 0, &b))
	require.Equal(t, data, b.String())

	require.NoError(t, client.FinishCommit(repo, commit.ID))

	b.Reset()
	require.NoError(t, client.GetFile(repo, commit.ID, "file", 0, 0, &b))
	require.Equal(t, data, b.String())
}

func TestSimpleFile(t *testing.T) {
	client := GetPachClient(t)

	repo := "test"
	require.NoError(t, client.CreateRepo(repo))

	commit1, err := client.StartCommit(repo, "master")
	require.NoError(t, err)
	_, err = client.PutFile(repo, commit1.ID, "foo", strings.NewReader("foo\n"))
	require.NoError(t, err)
	var buffer bytes.Buffer
	require.NoError(t, client.GetFile(repo, commit1.ID, "foo", 0, 0, &buffer))
	require.Equal(t, "foo\n", buffer.String())
	require.NoError(t, client.FinishCommit(repo, commit1.ID))

	buffer.Reset()
	require.NoError(t, client.GetFile(repo, commit1.ID, "foo", 0, 0, &buffer))
	require.Equal(t, "foo\n", buffer.String())

	commit2, err := client.StartCommit(repo, "master")
	require.NoError(t, err)
	_, err = client.PutFile(repo, commit2.ID, "foo", strings.NewReader("foo\n"))
	require.NoError(t, err)
	buffer.Reset()
	require.NoError(t, client.GetFile(repo, commit1.ID, "foo", 0, 0, &buffer))
	require.Equal(t, "foo\n", buffer.String())
	buffer.Reset()
	require.NoError(t, client.GetFile(repo, commit2.ID, "foo", 0, 0, &buffer))
	require.Equal(t, "foo\nfoo\n", buffer.String())
	err = client.FinishCommit(repo, commit2.ID)
	require.NoError(t, err)

	buffer.Reset()
	require.NoError(t, client.GetFile(repo, commit1.ID, "foo", 0, 0, &buffer))
	require.Equal(t, "foo\n", buffer.String())
	buffer.Reset()
	require.NoError(t, client.GetFile(repo, commit2.ID, "foo", 0, 0, &buffer))
	require.Equal(t, "foo\nfoo\n", buffer.String())
}

func TestStartCommitWithUnfinishedParent(t *testing.T) {
	client := GetPachClient(t)

	repo := "test"
	require.NoError(t, client.CreateRepo(repo))

	commit1, err := client.StartCommit(repo, "master")
	require.NoError(t, err)
	_, err = client.StartCommit(repo, "master")
	// fails because the parent commit has not been finished
	require.YesError(t, err)

	require.NoError(t, client.FinishCommit(repo, commit1.ID))
	_, err = client.StartCommit(repo, "master")
	require.NoError(t, err)
}

func TestAncestrySyntax(t *testing.T) {
	client := GetPachClient(t)

	repo := "test"
	require.NoError(t, client.CreateRepo(repo))

	commit1, err := client.StartCommit(repo, "master")
	require.NoError(t, err)
	_, err = client.PutFileOverwrite(repo, "master", "file", strings.NewReader("1"), 0)
	require.NoError(t, err)
	require.NoError(t, client.FinishCommit(repo, commit1.ID))

	commit2, err := client.StartCommit(repo, "master")
	require.NoError(t, err)
	_, err = client.PutFileOverwrite(repo, commit2.ID, "file", strings.NewReader("2"), 0)
	require.NoError(t, err)
	require.NoError(t, client.FinishCommit(repo, commit2.ID))

	commit3, err := client.StartCommit(repo, "master")
	require.NoError(t, err)
	_, err = client.PutFileOverwrite(repo, commit3.ID, "file", strings.NewReader("3"), 0)
	require.NoError(t, err)
	require.NoError(t, client.FinishCommit(repo, commit3.ID))

	commitInfo, err := client.InspectCommit(repo, "master^")
	require.NoError(t, err)
	require.Equal(t, commit2, commitInfo.Commit)

	commitInfo, err = client.InspectCommit(repo, "master~")
	require.NoError(t, err)
	require.Equal(t, commit2, commitInfo.Commit)

	commitInfo, err = client.InspectCommit(repo, "master^1")
	require.NoError(t, err)
	require.Equal(t, commit2, commitInfo.Commit)

	commitInfo, err = client.InspectCommit(repo, "master~1")
	require.NoError(t, err)
	require.Equal(t, commit2, commitInfo.Commit)

	commitInfo, err = client.InspectCommit(repo, "master^^")
	require.NoError(t, err)
	require.Equal(t, commit1, commitInfo.Commit)

	commitInfo, err = client.InspectCommit(repo, "master~~")
	require.NoError(t, err)
	require.Equal(t, commit1, commitInfo.Commit)

	commitInfo, err = client.InspectCommit(repo, "master^2")
	require.NoError(t, err)
	require.Equal(t, commit1, commitInfo.Commit)

	commitInfo, err = client.InspectCommit(repo, "master~2")
	require.NoError(t, err)
	require.Equal(t, commit1, commitInfo.Commit)

	commitInfo, err = client.InspectCommit(repo, "master.1")
	require.NoError(t, err)
	require.Equal(t, commit1, commitInfo.Commit)

	commitInfo, err = client.InspectCommit(repo, "master.2")
	require.NoError(t, err)
	require.Equal(t, commit2, commitInfo.Commit)

	commitInfo, err = client.InspectCommit(repo, "master.3")
	require.NoError(t, err)
	require.Equal(t, commit3, commitInfo.Commit)

	commitInfo, err = client.InspectCommit(repo, "master^^^")
	require.YesError(t, err)

	commitInfo, err = client.InspectCommit(repo, "master~~~")
	require.YesError(t, err)

	commitInfo, err = client.InspectCommit(repo, "master^3")
	require.YesError(t, err)

	commitInfo, err = client.InspectCommit(repo, "master~3")
	require.YesError(t, err)

	for i := 1; i <= 2; i++ {
		_, err := client.InspectFile(repo, fmt.Sprintf("%v^%v", commit3.ID, 3-i), "file")
		require.NoError(t, err)
	}

	var buffer bytes.Buffer
	require.NoError(t, client.GetFile(repo, ancestry.Add("master", 0), "file", 0, 0, &buffer))
	require.Equal(t, "3", buffer.String())
	buffer.Reset()
	require.NoError(t, client.GetFile(repo, ancestry.Add("master", 1), "file", 0, 0, &buffer))
	require.Equal(t, "2", buffer.String())
	buffer.Reset()
	require.NoError(t, client.GetFile(repo, ancestry.Add("master", 2), "file", 0, 0, &buffer))
	require.Equal(t, "1", buffer.String())
	buffer.Reset()
	require.NoError(t, client.GetFile(repo, ancestry.Add("master", -1), "file", 0, 0, &buffer))
	require.Equal(t, "1", buffer.String())
	buffer.Reset()
	require.NoError(t, client.GetFile(repo, ancestry.Add("master", -2), "file", 0, 0, &buffer))
	require.Equal(t, "2", buffer.String())
	buffer.Reset()
	require.NoError(t, client.GetFile(repo, ancestry.Add("master", -3), "file", 0, 0, &buffer))
	require.Equal(t, "3", buffer.String())

	// Adding a bunch of commits to the head of the branch shouldn't change the forward references.
	// (It will change backward references.)
	for i := 0; i < 10; i++ {
		_, err = client.PutFileOverwrite(repo, "master", "file", strings.NewReader(fmt.Sprintf("%d", i+4)), 0)
		require.NoError(t, err)
	}
	commitInfo, err = client.InspectCommit(repo, "master.1")
	require.NoError(t, err)
	require.Equal(t, commit1, commitInfo.Commit)

	commitInfo, err = client.InspectCommit(repo, "master.2")
	require.NoError(t, err)
	require.Equal(t, commit2, commitInfo.Commit)

	commitInfo, err = client.InspectCommit(repo, "master.3")
	require.NoError(t, err)
	require.Equal(t, commit3, commitInfo.Commit)
}

// TestProvenance implements the following DAG
//  A ─▶ B ─▶ C ─▶ D
//            ▲
//  E ────────╯

func TestProvenance(t *testing.T) {
	client := GetPachClient(t)

	require.NoError(t, client.CreateRepo("A"))
	require.NoError(t, client.CreateRepo("B"))
	require.NoError(t, client.CreateRepo("C"))
	require.NoError(t, client.CreateRepo("D"))
	require.NoError(t, client.CreateRepo("E"))

	require.NoError(t, client.CreateBranch("B", "master", "", []*pfs.Branch{pclient.NewBranch("A", "master")}))
	require.NoError(t, client.CreateBranch("C", "master", "", []*pfs.Branch{pclient.NewBranch("B", "master"), pclient.NewBranch("E", "master")}))
	require.NoError(t, client.CreateBranch("D", "master", "", []*pfs.Branch{pclient.NewBranch("C", "master")}))

	branchInfo, err := client.InspectBranch("B", "master")
	require.NoError(t, err)
	require.Equal(t, 1, len(branchInfo.Provenance))
	branchInfo, err = client.InspectBranch("C", "master")
	require.NoError(t, err)
	require.Equal(t, 3, len(branchInfo.Provenance))
	branchInfo, err = client.InspectBranch("D", "master")
	require.NoError(t, err)
	require.Equal(t, 4, len(branchInfo.Provenance))

	ACommit, err := client.StartCommit("A", "master")
	require.NoError(t, err)
	require.NoError(t, client.FinishCommit("A", ACommit.ID))
	ECommit, err := client.StartCommit("E", "master")
	require.NoError(t, err)
	require.NoError(t, client.FinishCommit("E", ECommit.ID))

	commitInfo, err := client.InspectCommit("B", "master")
	require.NoError(t, err)
	require.Equal(t, 1, len(commitInfo.Provenance))

	commitInfo, err = client.InspectCommit("C", "master")
	require.NoError(t, err)
	require.Equal(t, 3, len(commitInfo.Provenance))

	commitInfo, err = client.InspectCommit("D", "master")
	require.NoError(t, err)
	require.Equal(t, 4, len(commitInfo.Provenance))
}

func TestCommitBranch(t *testing.T) {
	client := GetPachClient(t)

	require.NoError(t, client.CreateRepo("repo"))
	// Make two branches provenant on the master branch
	require.NoError(t, client.CreateBranch("repo", "A", "", []*pfs.Branch{pclient.NewBranch("repo", "master")}))
	require.NoError(t, client.CreateBranch("repo", "B", "", []*pfs.Branch{pclient.NewBranch("repo", "master")}))

	// Now make a commit on the master branch, which should trigger a downstream commit on each of the two branches
	masterCommit, err := client.StartCommit("repo", "master")
	require.NoError(t, err)
	require.NoError(t, client.FinishCommit("repo", masterCommit.ID))

	// Check that the commit in branch A has the information and provenance we expect
	commitInfo, err := client.InspectCommit("repo", "A")
	require.NoError(t, err)
	require.Equal(t, "A", commitInfo.Branch.Name)
	require.Equal(t, 1, len(commitInfo.Provenance))
	require.Equal(t, "master", commitInfo.Provenance[0].Branch.Name)

	// Check that the commit in branch B has the information and provenance we expect
	commitInfo, err = client.InspectCommit("repo", "B")
	require.NoError(t, err)
	require.Equal(t, "B", commitInfo.Branch.Name)
	require.Equal(t, 1, len(commitInfo.Provenance))
	require.Equal(t, "master", commitInfo.Provenance[0].Branch.Name)
}

func TestCommitOnTwoBranchesProvenance(t *testing.T) {
	client := GetPachClient(t)

	require.NoError(t, client.CreateRepo("repo"))

	parentCommit, err := client.StartCommit("repo", "master")
	require.NoError(t, err)
	require.NoError(t, client.FinishCommit("repo", parentCommit.ID))

	masterCommit, err := client.StartCommit("repo", "master")
	require.NoError(t, err)
	require.NoError(t, client.FinishCommit("repo", masterCommit.ID))

	// Make two branches provenant on the same commit on the master branch
	require.NoError(t, client.CreateBranch("repo", "A", masterCommit.ID, nil))
	require.NoError(t, client.CreateBranch("repo", "B", masterCommit.ID, nil))

	// Now create a branch provenant on both branches A and B
	require.NoError(t, client.CreateBranch("repo", "C", "", []*pfs.Branch{pclient.NewBranch("repo", "A"), pclient.NewBranch("repo", "B")}))

	// The head commit of the C branch should have branches A and B both represented in the provenance
	// This is important because jobInput looks up commits by branch
	ci, err := client.InspectCommit("repo", "C")
	require.NoError(t, err)
	require.Equal(t, 2, len(ci.Provenance))

	// We should also be able to delete the head commit of A
	require.NoError(t, client.DeleteCommit("repo", "A"))

	// And the head of branch B should go back to the parent of the deleted commit
	branchInfo, err := client.InspectBranch("repo", "B")
	require.NoError(t, err)
	require.Equal(t, parentCommit.ID, branchInfo.Head.ID)

	// We should also be able to delete the head commit of A
	require.NoError(t, client.DeleteCommit("repo", parentCommit.ID))

	// It should also be ok to make new commits on branches A and B
	aCommit, err := client.StartCommit("repo", "A")
	require.NoError(t, err)
	require.NoError(t, client.FinishCommit("repo", aCommit.ID))

	bCommit, err := client.StartCommit("repo", "B")
	require.NoError(t, err)
	require.NoError(t, client.FinishCommit("repo", bCommit.ID))
}

func TestSimple(t *testing.T) {
	client := GetPachClient(t)

	repo := "test"
	require.NoError(t, client.CreateRepo(repo))
	commit1, err := client.StartCommit(repo, "master")
	require.NoError(t, err)
	_, err = client.PutFile(repo, commit1.ID, "foo", strings.NewReader("foo\n"))
	require.NoError(t, err)
	require.NoError(t, client.FinishCommit(repo, commit1.ID))
	commitInfos, err := client.ListCommit(repo, "", "", 0)
	require.NoError(t, err)
	require.Equal(t, 1, len(commitInfos))
	var buffer bytes.Buffer
	require.NoError(t, client.GetFile(repo, commit1.ID, "foo", 0, 0, &buffer))
	require.Equal(t, "foo\n", buffer.String())
	commit2, err := client.StartCommit(repo, "master")
	require.NoError(t, err)
	_, err = client.PutFile(repo, commit2.ID, "foo", strings.NewReader("foo\n"))
	require.NoError(t, err)
	err = client.FinishCommit(repo, commit2.ID)
	require.NoError(t, err)
	buffer = bytes.Buffer{}
	require.NoError(t, client.GetFile(repo, commit1.ID, "foo", 0, 0, &buffer))
	require.Equal(t, "foo\n", buffer.String())
	buffer = bytes.Buffer{}
	require.NoError(t, client.GetFile(repo, commit2.ID, "foo", 0, 0, &buffer))
	require.Equal(t, "foo\nfoo\n", buffer.String())
}

func TestBranch1(t *testing.T) {
	client := GetPachClient(t)

	repo := "test"
	require.NoError(t, client.CreateRepo(repo))
	commit, err := client.StartCommit(repo, "master")
	require.NoError(t, err)
	_, err = client.PutFile(repo, "master", "foo", strings.NewReader("foo\n"))
	require.NoError(t, err)
	require.NoError(t, client.FinishCommit(repo, "master"))
	var buffer bytes.Buffer
	require.NoError(t, client.GetFile(repo, "master", "foo", 0, 0, &buffer))
	require.Equal(t, "foo\n", buffer.String())
	branches, err := client.ListBranch(repo)
	require.NoError(t, err)
	require.Equal(t, 1, len(branches))
	require.Equal(t, "master", branches[0].Name)

	_, err = client.StartCommit(repo, "master")
	require.NoError(t, err)
	_, err = client.PutFile(repo, "master", "foo", strings.NewReader("foo\n"))
	require.NoError(t, err)
	err = client.FinishCommit(repo, "master")
	require.NoError(t, err)
	buffer = bytes.Buffer{}
	require.NoError(t, client.GetFile(repo, "master", "foo", 0, 0, &buffer))
	require.Equal(t, "foo\nfoo\n", buffer.String())
	branches, err = client.ListBranch(repo)
	require.NoError(t, err)
	require.Equal(t, 1, len(branches))
	require.Equal(t, "master", branches[0].Name)

	require.NoError(t, client.SetBranch(repo, commit.ID, "master2"))

	branches, err = client.ListBranch(repo)
	require.NoError(t, err)
	require.Equal(t, 2, len(branches))
	require.Equal(t, "master2", branches[0].Name)
	require.Equal(t, "master", branches[1].Name)
}

func TestPutFileBig(t *testing.T) {
	client := GetPachClient(t)

	repo := "test"
	require.NoError(t, client.CreateRepo(repo))

	// Write a big blob that would normally not fit in a block
	fileSize := int(pfs.ChunkSize + 5*1024*1024)
	expectedOutputA := generateRandomString(fileSize)
	r := strings.NewReader(string(expectedOutputA))

	commit1, err := client.StartCommit(repo, "")
	require.NoError(t, err)
	_, err = client.PutFile(repo, commit1.ID, "foo", r)
	require.NoError(t, err)
	require.NoError(t, client.FinishCommit(repo, commit1.ID))

	fileInfo, err := client.InspectFile(repo, commit1.ID, "foo")
	require.NoError(t, err)
	require.Equal(t, fileSize, int(fileInfo.SizeBytes))

	var buffer bytes.Buffer
	require.NoError(t, client.GetFile(repo, commit1.ID, "foo", 0, 0, &buffer))
	require.Equal(t, string(expectedOutputA), buffer.String())
}

func TestPutFile(t *testing.T) {
	client := GetPachClient(t)

	repo := "test"
	require.NoError(t, client.CreateRepo(repo))

	// Detect file conflict
	commit1, err := client.StartCommit(repo, "")
	require.NoError(t, err)
	_, err = client.PutFile(repo, commit1.ID, "foo", strings.NewReader("foo\n"))
	require.NoError(t, err)
	_, err = client.PutFile(repo, commit1.ID, "foo/bar", strings.NewReader("foo\n"))
	require.NoError(t, err)
	require.YesError(t, client.FinishCommit(repo, commit1.ID))

	commit1, err = client.StartCommit(repo, "")
	require.NoError(t, err)
	_, err = client.PutFile(repo, commit1.ID, "foo", strings.NewReader("foo\n"))
	require.NoError(t, err)
	_, err = client.PutFile(repo, commit1.ID, "foo", strings.NewReader("foo\n"))
	require.NoError(t, err)
	require.NoError(t, client.FinishCommit(repo, commit1.ID))

	var buffer bytes.Buffer
	require.NoError(t, client.GetFile(repo, commit1.ID, "foo", 0, 0, &buffer))
	require.Equal(t, "foo\nfoo\n", buffer.String())

	commit2, err := client.StartCommitParent(repo, "", commit1.ID)
	require.NoError(t, err)
	// file conflicts with the previous commit
	_, err = client.PutFile(repo, commit2.ID, "foo/bar", strings.NewReader("foo\n"))
	require.NoError(t, err)
	_, err = client.PutFile(repo, commit2.ID, "/bar", strings.NewReader("bar\n"))
	require.NoError(t, err)
	require.YesError(t, client.FinishCommit(repo, commit2.ID))

	commit2, err = client.StartCommitParent(repo, "", commit1.ID)
	require.NoError(t, err)
	_, err = client.PutFile(repo, commit2.ID, "/bar", strings.NewReader("bar\n"))
	require.NoError(t, err)
	require.NoError(t, client.FinishCommit(repo, commit2.ID))

	commit3, err := client.StartCommitParent(repo, "", commit2.ID)
	require.NoError(t, err)
	_, err = client.PutFile(repo, commit3.ID, "dir1/foo", strings.NewReader("foo\n"))
	require.NoError(t, err) // because the directory dir does not exist
	require.NoError(t, client.FinishCommit(repo, commit3.ID))

	commit4, err := client.StartCommitParent(repo, "", commit3.ID)
	require.NoError(t, err)
	_, err = client.PutFile(repo, commit4.ID, "dir2/bar", strings.NewReader("bar\n"))
	require.NoError(t, err)
	require.NoError(t, client.FinishCommit(repo, commit4.ID))

	buffer = bytes.Buffer{}
	require.NoError(t, client.GetFile(repo, commit4.ID, "dir2/bar", 0, 0, &buffer))
	require.Equal(t, "bar\n", buffer.String())
	buffer = bytes.Buffer{}
	require.NoError(t, client.GetFile(repo, commit4.ID, "dir2", 0, 0, &buffer))
}

func TestPutFile2(t *testing.T) {
	client := GetPachClient(t)

	repo := "test"
	require.NoError(t, client.CreateRepo(repo))
	commit1, err := client.StartCommit(repo, "master")
	require.NoError(t, err)
	_, err = client.PutFile(repo, commit1.ID, "file", strings.NewReader("foo\n"))
	require.NoError(t, err)
	_, err = client.PutFile(repo, commit1.ID, "file", strings.NewReader("bar\n"))
	require.NoError(t, err)
	_, err = client.PutFile(repo, "master", "file", strings.NewReader("buzz\n"))
	require.NoError(t, err)
	require.NoError(t, client.FinishCommit(repo, commit1.ID))

	expected := "foo\nbar\nbuzz\n"
	buffer := &bytes.Buffer{}
	require.NoError(t, client.GetFile(repo, commit1.ID, "file", 0, 0, buffer))
	require.Equal(t, expected, buffer.String())
	buffer.Reset()
	require.NoError(t, client.GetFile(repo, "master", "file", 0, 0, buffer))
	require.Equal(t, expected, buffer.String())

	commit2, err := client.StartCommit(repo, "master")
	require.NoError(t, err)
	_, err = client.PutFile(repo, commit2.ID, "file", strings.NewReader("foo\n"))
	require.NoError(t, err)
	_, err = client.PutFile(repo, commit2.ID, "file", strings.NewReader("bar\n"))
	require.NoError(t, err)
	_, err = client.PutFile(repo, "master", "file", strings.NewReader("buzz\n"))
	require.NoError(t, err)
	require.NoError(t, client.FinishCommit(repo, "master"))

	expected = "foo\nbar\nbuzz\nfoo\nbar\nbuzz\n"
	buffer.Reset()
	require.NoError(t, client.GetFile(repo, commit2.ID, "file", 0, 0, buffer))
	require.Equal(t, expected, buffer.String())
	buffer.Reset()
	require.NoError(t, client.GetFile(repo, "master", "file", 0, 0, buffer))
	require.Equal(t, expected, buffer.String())

	commit3, err := client.StartCommit(repo, "master")
	require.NoError(t, err)
	require.NoError(t, client.SetBranch(repo, commit3.ID, "foo"))
	_, err = client.PutFile(repo, "foo", "file", strings.NewReader("foo\nbar\nbuzz\n"))
	require.NoError(t, client.FinishCommit(repo, "foo"))

	expected = "foo\nbar\nbuzz\nfoo\nbar\nbuzz\nfoo\nbar\nbuzz\n"
	buffer.Reset()
	require.NoError(t, client.GetFile(repo, "foo", "file", 0, 0, buffer))
	require.Equal(t, expected, buffer.String())
}

func TestPutFileLongName(t *testing.T) {
	client := GetPachClient(t)

	repo := "test"
	require.NoError(t, client.CreateRepo(repo))

	fileName := `oaidhzoshd()&)(@^$@(#)oandoancoasid1)(&@$)(@U)oaidhzoshd()&)(@^$@(#)oandoancoasid1)(&@$)(@U)oaidhzoshd()&)(@^$@(#)oandoancoasid1)(&@$)(@U)oaidhzoshd()&)(@^$@(#)oandoancoasid1)(&@$)(@U)oaidhzoshd()&)(@^$@(#)oandoancoasid1)(&@$)(@U)oaidhzoshd()&)(@^$@(#)oandoancoasid1)(&@$)(@U)oaidhzoshd()&)(@^$@(#)oandoancoasid1)(&@$)(@U)oaidhzoshd()&)(@^$@(#)oandoancoasid1)(&@$)(@U)oaidhzoshd()&)(@^$@(#)oandoancoasid1)(&@$)(@U)oaidhzoshd()&)(@^$@(#)oandoancoasid1)(&@$)(@U)oaidhzoshd()&)(@^$@(#)oandoancoasid1)(&@$)(@U)oaidhzoshd()&)(@^$@(#)oandoancoasid1)(&@$)(@U)oaidhzoshd()&)(@^$@(#)oandoancoasid1)(&@$)(@U)oaidhzoshd()&)(@^$@(#)oandoancoasid1)(&@$)(@U)oaidhzoshd()&)(@^$@(#)oandoancoasid1)(&@$)(@U)oaidhzoshd()&)(@^$@(#)oandoancoasid1)(&@$)(@U)oaidhzoshd()&)(@^$@(#)oandoancoasid1)(&@$)(@U)oaidhzoshd()&)(@^$@(#)oandoancoasid1)(&@$)(@U)oaidhzoshd()&)(@^$@(#)oandoancoasid1)(&@$)(@U)oaidhzoshd()&)(@^$@(#)oandoancoasid1)(&@$)(@U)oaidhzoshd()&)(@^$@(#)oandoancoasid1)(&@$)(@U)oaidhzoshd()&)(@^$@(#)oandoancoasid1)(&@$)(@U)oaidhzoshd()&)(@^$@(#)oandoancoasid1)(&@$)(@U)oaidhzoshd()&)(@^$@(#)oandoancoasid1)(&@$)(@U)oaidhzoshd()&)(@^$@(#)oandoancoasid1)(&@$)(@U)oaidhzoshd()&)(@^$@(#)oandoancoasid1)(&@$)(@U)`

	commit, err := client.StartCommit(repo, "")
	require.NoError(t, err)
	_, err = client.PutFile(repo, commit.ID, fileName, strings.NewReader("foo\n"))
	require.NoError(t, client.FinishCommit(repo, commit.ID))

	var buffer bytes.Buffer
	require.NoError(t, client.GetFile(repo, commit.ID, fileName, 0, 0, &buffer))
	require.Equal(t, "foo\n", buffer.String())
}

func TestPutSameFileInParallel(t *testing.T) {
	client := GetPachClient(t)

	repo := "test"
	require.NoError(t, client.CreateRepo(repo))

	commit, err := client.StartCommit(repo, "")
	require.NoError(t, err)
	var eg errgroup.Group
	for i := 0; i < 3; i++ {
		eg.Go(func() error {
			_, err = client.PutFile(repo, commit.ID, "foo", strings.NewReader("foo\n"))
			return err
		})
	}
	require.NoError(t, eg.Wait())
	require.NoError(t, client.FinishCommit(repo, commit.ID))

	var buffer bytes.Buffer
	require.NoError(t, client.GetFile(repo, commit.ID, "foo", 0, 0, &buffer))
	require.Equal(t, "foo\nfoo\nfoo\n", buffer.String())
}

func TestInspectFile(t *testing.T) {
	client := GetPachClient(t)

	repo := "test"
	require.NoError(t, client.CreateRepo(repo))

	fileContent1 := "foo\n"
	commit1, err := client.StartCommit(repo, "master")
	require.NoError(t, err)
	_, err = client.PutFile(repo, commit1.ID, "foo", strings.NewReader(fileContent1))
	require.NoError(t, err)

	fileInfo, err := client.InspectFile(repo, commit1.ID, "foo")
	require.NoError(t, err)
	require.Equal(t, pfs.FileType_FILE, fileInfo.FileType)
	require.Equal(t, len(fileContent1), int(fileInfo.SizeBytes))

	require.NoError(t, client.FinishCommit(repo, commit1.ID))

	fileInfo, err = client.InspectFile(repo, commit1.ID, "foo")
	require.NoError(t, err)
	require.Equal(t, pfs.FileType_FILE, fileInfo.FileType)
	require.Equal(t, len(fileContent1), int(fileInfo.SizeBytes))

	fileContent2 := "barbar\n"
	commit2, err := client.StartCommit(repo, "master")
	require.NoError(t, err)
	_, err = client.PutFile(repo, commit2.ID, "foo", strings.NewReader(fileContent2))
	require.NoError(t, err)

	fileInfo, err = client.InspectFile(repo, commit2.ID, "foo")
	require.NoError(t, err)
	require.Equal(t, pfs.FileType_FILE, fileInfo.FileType)
	require.Equal(t, len(fileContent1+fileContent2), int(fileInfo.SizeBytes))

	require.NoError(t, client.FinishCommit(repo, commit2.ID))

	fileInfo, err = client.InspectFile(repo, commit2.ID, "foo")
	require.NoError(t, err)
	require.Equal(t, pfs.FileType_FILE, fileInfo.FileType)
	require.Equal(t, len(fileContent1+fileContent2), int(fileInfo.SizeBytes))

	fileInfo, err = client.InspectFile(repo, commit2.ID, "foo")
	require.NoError(t, err)
	require.Equal(t, pfs.FileType_FILE, fileInfo.FileType)
	require.Equal(t, len(fileContent1)+len(fileContent2), int(fileInfo.SizeBytes))

	fileContent3 := "bar\n"
	commit3, err := client.StartCommit(repo, "master")
	require.NoError(t, err)
	_, err = client.PutFile(repo, commit3.ID, "bar", strings.NewReader(fileContent3))
	require.NoError(t, err)
	require.NoError(t, client.FinishCommit(repo, commit3.ID))

	fileInfos, err := client.ListFile(repo, commit3.ID, "")
	require.NoError(t, err)
	require.Equal(t, len(fileInfos), 2)
}

func TestInspectFile2(t *testing.T) {
	client := GetPachClient(t)

	repo := "test"
	require.NoError(t, client.CreateRepo(repo))

	fileContent1 := "foo\n"
	fileContent2 := "buzz\n"

	_, err := client.StartCommit(repo, "master")
	require.NoError(t, err)
	_, err = client.PutFile(repo, "master", "file", strings.NewReader(fileContent1))
	require.NoError(t, err)
	require.NoError(t, client.FinishCommit(repo, "master"))

	fileInfo, err := client.InspectFile(repo, "master", "/file")
	require.NoError(t, err)
	require.Equal(t, len(fileContent1), int(fileInfo.SizeBytes))
	require.Equal(t, "/file", fileInfo.File.Path)
	require.Equal(t, pfs.FileType_FILE, fileInfo.FileType)

	_, err = client.StartCommit(repo, "master")
	require.NoError(t, err)
	_, err = client.PutFile(repo, "master", "file", strings.NewReader(fileContent1))
	require.NoError(t, err)
	require.NoError(t, client.FinishCommit(repo, "master"))

	fileInfo, err = client.InspectFile(repo, "master", "file")
	require.NoError(t, err)
	require.Equal(t, len(fileContent1)*2, int(fileInfo.SizeBytes))
	require.Equal(t, "file", fileInfo.File.Path)

	_, err = client.StartCommit(repo, "master")
	require.NoError(t, err)
	err = client.DeleteFile(repo, "master", "file")
	require.NoError(t, err)
	_, err = client.PutFile(repo, "master", "file", strings.NewReader(fileContent2))
	require.NoError(t, err)
	require.NoError(t, client.FinishCommit(repo, "master"))

	fileInfo, err = client.InspectFile(repo, "master", "file")
	require.NoError(t, err)
	require.Equal(t, len(fileContent2), int(fileInfo.SizeBytes))
}

func TestInspectDir(t *testing.T) {
	client := GetPachClient(t)

	repo := "test"
	require.NoError(t, client.CreateRepo(repo))

	commit1, err := client.StartCommit(repo, "")
	require.NoError(t, err)

	fileContent := "foo\n"
	_, err = client.PutFile(repo, commit1.ID, "dir/foo", strings.NewReader(fileContent))
	require.NoError(t, err)

	fileInfo, err := client.InspectFile(repo, commit1.ID, "dir/foo")
	require.NoError(t, err)
	require.Equal(t, len(fileContent), int(fileInfo.SizeBytes))
	require.Equal(t, pfs.FileType_FILE, fileInfo.FileType)

	require.NoError(t, client.FinishCommit(repo, commit1.ID))

	fileInfo, err = client.InspectFile(repo, commit1.ID, "dir/foo")
	require.NoError(t, err)
	require.Equal(t, len(fileContent), int(fileInfo.SizeBytes))
	require.Equal(t, pfs.FileType_FILE, fileInfo.FileType)

	fileInfo, err = client.InspectFile(repo, commit1.ID, "dir")
	require.NoError(t, err)
	require.Equal(t, len(fileContent), int(fileInfo.SizeBytes))
	require.Equal(t, pfs.FileType_DIR, fileInfo.FileType)

	_, err = client.InspectFile(repo, commit1.ID, "")
	require.NoError(t, err)
	require.Equal(t, len(fileContent), int(fileInfo.SizeBytes))
	require.Equal(t, pfs.FileType_DIR, fileInfo.FileType)
}

func TestInspectDir2(t *testing.T) {
	client := GetPachClient(t)

	repo := "test"
	require.NoError(t, client.CreateRepo(repo))

	fileContent := "foo\n"

	_, err := client.StartCommit(repo, "master")
	require.NoError(t, err)
	_, err = client.PutFile(repo, "master", "dir/1", strings.NewReader(fileContent))
	require.NoError(t, err)
	_, err = client.PutFile(repo, "master", "dir/2", strings.NewReader(fileContent))
	require.NoError(t, err)

	fileInfo, err := client.InspectFile(repo, "master", "/dir")
	require.NoError(t, err)
	require.Equal(t, "/dir", fileInfo.File.Path)
	require.Equal(t, pfs.FileType_DIR, fileInfo.FileType)

	require.NoError(t, client.FinishCommit(repo, "master"))

	fileInfo, err = client.InspectFile(repo, "master", "/dir")
	require.NoError(t, err)
	require.Equal(t, "/dir", fileInfo.File.Path)
	require.Equal(t, pfs.FileType_DIR, fileInfo.FileType)

	_, err = client.StartCommit(repo, "master")
	require.NoError(t, err)
	_, err = client.PutFile(repo, "master", "dir/3", strings.NewReader(fileContent))
	require.NoError(t, err)
	fileInfo, err = client.InspectFile(repo, "master", "dir")
	require.NoError(t, err)

	require.NoError(t, client.FinishCommit(repo, "master"))

	fileInfo, err = client.InspectFile(repo, "master", "dir")
	require.NoError(t, err)

	_, err = client.StartCommit(repo, "master")
	require.NoError(t, err)
	err = client.DeleteFile(repo, "master", "dir/2")
	require.NoError(t, err)
	require.NoError(t, client.FinishCommit(repo, "master"))

	fileInfo, err = client.InspectFile(repo, "master", "dir")
	require.NoError(t, err)
}

func TestListFileTwoCommits(t *testing.T) {
	client := GetPachClient(t)

	repo := "test"
	require.NoError(t, client.CreateRepo(repo))

	numFiles := 5

	commit1, err := client.StartCommit(repo, "master")
	require.NoError(t, err)

	for i := 0; i < numFiles; i++ {
		_, err = client.PutFile(repo, commit1.ID, fmt.Sprintf("file%d", i), strings.NewReader("foo\n"))
		require.NoError(t, err)
	}

	fileInfos, err := client.ListFile(repo, "master", "")
	require.NoError(t, err)
	require.Equal(t, numFiles, len(fileInfos))

	require.NoError(t, client.FinishCommit(repo, commit1.ID))

	commit2, err := client.StartCommit(repo, "master")
	require.NoError(t, err)

	for i := 0; i < numFiles; i++ {
		_, err = client.PutFile(repo, commit2.ID, fmt.Sprintf("file2-%d", i), strings.NewReader("foo\n"))
		require.NoError(t, err)
	}

	fileInfos, err = client.ListFile(repo, commit2.ID, "")
	require.NoError(t, err)
	require.Equal(t, 2*numFiles, len(fileInfos))

	require.NoError(t, client.FinishCommit(repo, commit2.ID))

	fileInfos, err = client.ListFile(repo, commit1.ID, "")
	require.NoError(t, err)
	require.Equal(t, numFiles, len(fileInfos))

	fileInfos, err = client.ListFile(repo, commit2.ID, "")
	require.NoError(t, err)
	require.Equal(t, 2*numFiles, len(fileInfos))
}

func TestListFile(t *testing.T) {
	client := GetPachClient(t)

	repo := "test"
	require.NoError(t, client.CreateRepo(repo))

	commit, err := client.StartCommit(repo, "")
	require.NoError(t, err)

	fileContent1 := "foo\n"
	_, err = client.PutFile(repo, commit.ID, "dir/foo", strings.NewReader(fileContent1))
	require.NoError(t, err)

	fileContent2 := "bar\n"
	_, err = client.PutFile(repo, commit.ID, "dir/bar", strings.NewReader(fileContent2))
	require.NoError(t, err)

	fileInfos, err := client.ListFile(repo, commit.ID, "dir")
	require.NoError(t, err)
	require.Equal(t, 2, len(fileInfos))
	require.True(t, fileInfos[0].File.Path == "/dir/foo" && fileInfos[1].File.Path == "/dir/bar" || fileInfos[0].File.Path == "/dir/bar" && fileInfos[1].File.Path == "/dir/foo")
	require.True(t, fileInfos[0].SizeBytes == fileInfos[1].SizeBytes && fileInfos[0].SizeBytes == uint64(len(fileContent1)))

	require.NoError(t, client.FinishCommit(repo, commit.ID))

	fileInfos, err = client.ListFile(repo, commit.ID, "dir")
	require.NoError(t, err)
	require.Equal(t, 2, len(fileInfos))
	require.True(t, fileInfos[0].File.Path == "/dir/foo" && fileInfos[1].File.Path == "/dir/bar" || fileInfos[0].File.Path == "/dir/bar" && fileInfos[1].File.Path == "/dir/foo")
	require.True(t, fileInfos[0].SizeBytes == fileInfos[1].SizeBytes && fileInfos[0].SizeBytes == uint64(len(fileContent1)))
}

func TestListFile2(t *testing.T) {
	client := GetPachClient(t)

	repo := "test"
	require.NoError(t, client.CreateRepo(repo))

	fileContent := "foo\n"

	_, err := client.StartCommit(repo, "master")
	require.NoError(t, err)
	_, err = client.PutFile(repo, "master", "dir/1", strings.NewReader(fileContent))
	require.NoError(t, err)
	_, err = client.PutFile(repo, "master", "dir/2", strings.NewReader(fileContent))
	require.NoError(t, err)

	fileInfos, err := client.ListFile(repo, "master", "dir")
	require.NoError(t, err)
	require.Equal(t, 2, len(fileInfos))

	require.NoError(t, client.FinishCommit(repo, "master"))

	fileInfos, err = client.ListFile(repo, "master", "dir")
	require.NoError(t, err)
	require.Equal(t, 2, len(fileInfos))

	_, err = client.StartCommit(repo, "master")
	require.NoError(t, err)
	_, err = client.PutFile(repo, "master", "dir/3", strings.NewReader(fileContent))
	require.NoError(t, err)
	require.NoError(t, client.FinishCommit(repo, "master"))

	fileInfos, err = client.ListFile(repo, "master", "dir")
	require.NoError(t, err)
	require.Equal(t, 3, len(fileInfos))

	_, err = client.StartCommit(repo, "master")
	require.NoError(t, err)
	err = client.DeleteFile(repo, "master", "dir/2")
	require.NoError(t, err)
	require.NoError(t, client.FinishCommit(repo, "master"))

	fileInfos, err = client.ListFile(repo, "master", "dir")
	require.NoError(t, err)
	require.Equal(t, 2, len(fileInfos))
}

func TestListFile3(t *testing.T) {
	client := GetPachClient(t)

	repo := "test"
	require.NoError(t, client.CreateRepo(repo))

	fileContent := "foo\n"

	_, err := client.StartCommit(repo, "master")
	require.NoError(t, err)
	_, err = client.PutFile(repo, "master", "dir/1", strings.NewReader(fileContent))
	require.NoError(t, err)
	_, err = client.PutFile(repo, "master", "dir/2", strings.NewReader(fileContent))
	require.NoError(t, err)
	require.NoError(t, client.FinishCommit(repo, "master"))

	fileInfos, err := client.ListFile(repo, "master", "dir")
	require.NoError(t, err)
	require.Equal(t, 2, len(fileInfos))

	_, err = client.StartCommit(repo, "master")
	require.NoError(t, err)
	_, err = client.PutFile(repo, "master", "dir/3/foo", strings.NewReader(fileContent))
	require.NoError(t, err)
	_, err = client.PutFile(repo, "master", "dir/3/bar", strings.NewReader(fileContent))
	require.NoError(t, err)
	require.NoError(t, client.FinishCommit(repo, "master"))

	fileInfos, err = client.ListFile(repo, "master", "dir")
	require.NoError(t, err)
	require.Equal(t, 3, len(fileInfos))
	require.Equal(t, int(fileInfos[2].SizeBytes), len(fileContent)*2)

	_, err = client.StartCommit(repo, "master")
	require.NoError(t, err)
	err = client.DeleteFile(repo, "master", "dir/3/bar")
	require.NoError(t, err)
	require.NoError(t, client.FinishCommit(repo, "master"))

	fileInfos, err = client.ListFile(repo, "master", "dir")
	require.NoError(t, err)
	require.Equal(t, 3, len(fileInfos))
	require.Equal(t, int(fileInfos[2].SizeBytes), len(fileContent))

	_, err = client.StartCommit(repo, "master")
	require.NoError(t, err)
	_, err = client.PutFile(repo, "master", "file", strings.NewReader(fileContent))
	require.NoError(t, err)
	require.NoError(t, client.FinishCommit(repo, "master"))

	fileInfos, err = client.ListFile(repo, "master", "/")
	require.NoError(t, err)
	require.Equal(t, 2, len(fileInfos))
}

func TestPutFileTypeConflict(t *testing.T) {
	client := GetPachClient(t)

	repo := "test"
	require.NoError(t, client.CreateRepo(repo))

	fileContent := "foo\n"

	commit1, err := client.StartCommit(repo, "master")
	require.NoError(t, err)
	_, err = client.PutFile(repo, commit1.ID, "dir/1", strings.NewReader(fileContent))
	require.NoError(t, err)
	require.NoError(t, client.FinishCommit(repo, commit1.ID))

	commit2, err := client.StartCommit(repo, "master")
	require.NoError(t, err)
	_, err = client.PutFile(repo, commit2.ID, "dir", strings.NewReader(fileContent))
	require.NoError(t, err)
	require.YesError(t, client.FinishCommit(repo, commit2.ID))
}

func TestRootDirectory(t *testing.T) {
	client := GetPachClient(t)

	repo := "test"
	require.NoError(t, client.CreateRepo(repo))

	fileContent := "foo\n"

	commit, err := client.StartCommit(repo, "")
	require.NoError(t, err)
	_, err = client.PutFile(repo, commit.ID, "foo", strings.NewReader(fileContent))
	require.NoError(t, err)

	fileInfos, err := client.ListFile(repo, commit.ID, "")
	require.NoError(t, err)
	require.Equal(t, 1, len(fileInfos))

	require.NoError(t, client.FinishCommit(repo, commit.ID))

	fileInfos, err = client.ListFile(repo, commit.ID, "")
	require.NoError(t, err)
	require.Equal(t, 1, len(fileInfos))
}

func TestDeleteFile(t *testing.T) {
	client := GetPachClient(t)

	repo := "test"
	require.NoError(t, client.CreateRepo(repo))

	// Commit 1: Add two files; delete one file within the commit
	commit1, err := client.StartCommit(repo, "master")
	require.NoError(t, err)

	fileContent1 := "foo\n"
	_, err = client.PutFile(repo, commit1.ID, "foo", strings.NewReader(fileContent1))
	require.NoError(t, err)

	fileContent2 := "bar\n"
	_, err = client.PutFile(repo, commit1.ID, "bar", strings.NewReader(fileContent2))
	require.NoError(t, err)

	require.NoError(t, client.DeleteFile(repo, commit1.ID, "foo"))

	_, err = client.InspectFile(repo, commit1.ID, "foo")
	require.YesError(t, err)

	fileInfos, err := client.ListFile(repo, commit1.ID, "")
	require.NoError(t, err)
	require.Equal(t, 1, len(fileInfos))

	require.NoError(t, client.FinishCommit(repo, commit1.ID))

	_, err = client.InspectFile(repo, commit1.ID, "foo")
	require.YesError(t, err)

	// Should see one file
	fileInfos, err = client.ListFile(repo, commit1.ID, "")
	require.NoError(t, err)
	require.Equal(t, 1, len(fileInfos))

	// Deleting a file in a finished commit should result in an error
	require.YesError(t, client.DeleteFile(repo, commit1.ID, "bar"))

	// Empty commit
	commit2, err := client.StartCommit(repo, "master")
	require.NoError(t, err)
	require.NoError(t, client.FinishCommit(repo, commit2.ID))

	// Should still see one files
	fileInfos, err = client.ListFile(repo, commit2.ID, "")
	require.NoError(t, err)
	require.Equal(t, 1, len(fileInfos))

	// Delete bar
	commit3, err := client.StartCommit(repo, "master")
	require.NoError(t, err)
	require.NoError(t, client.DeleteFile(repo, commit3.ID, "bar"))

	// Should see no file
	fileInfos, err = client.ListFile(repo, commit3.ID, "")
	require.NoError(t, err)
	require.Equal(t, 0, len(fileInfos))

	_, err = client.InspectFile(repo, commit3.ID, "bar")
	require.YesError(t, err)

	require.NoError(t, client.FinishCommit(repo, commit3.ID))

	// Should see no file
	fileInfos, err = client.ListFile(repo, commit3.ID, "")
	require.NoError(t, err)
	require.Equal(t, 0, len(fileInfos))

	_, err = client.InspectFile(repo, commit3.ID, "bar")
	require.YesError(t, err)

	// Delete a nonexistent file; it should be no-op
	commit4, err := client.StartCommit(repo, "master")
	require.NoError(t, err)
	require.NoError(t, client.DeleteFile(repo, commit4.ID, "nonexistent"))
	require.NoError(t, client.FinishCommit(repo, commit4.ID))
}

func TestDeleteDir(t *testing.T) {
	client := GetPachClient(t)

	repo := "test"
	require.NoError(t, client.CreateRepo(repo))

	// Commit 1: Add two files into the same directory; delete the directory
	commit1, err := client.StartCommit(repo, "master")
	require.NoError(t, err)

	_, err = client.PutFile(repo, commit1.ID, "dir/foo", strings.NewReader("foo1"))
	require.NoError(t, err)

	_, err = client.PutFile(repo, commit1.ID, "dir/bar", strings.NewReader("bar1"))
	require.NoError(t, err)

	require.NoError(t, client.DeleteFile(repo, commit1.ID, "dir"))

	fileInfos, err := client.ListFile(repo, commit1.ID, "")
	require.NoError(t, err)
	require.Equal(t, 0, len(fileInfos))

	require.NoError(t, client.FinishCommit(repo, commit1.ID))

	fileInfos, err = client.ListFile(repo, commit1.ID, "")
	require.NoError(t, err)
	require.Equal(t, 0, len(fileInfos))

	// dir should not exist
	_, err = client.InspectFile(repo, commit1.ID, "dir")
	require.YesError(t, err)

	// Commit 2: Delete the directory and add the same two files
	// The two files should reflect the new content
	commit2, err := client.StartCommit(repo, "master")
	require.NoError(t, err)

	_, err = client.PutFile(repo, commit2.ID, "dir/foo", strings.NewReader("foo2"))
	require.NoError(t, err)

	_, err = client.PutFile(repo, commit2.ID, "dir/bar", strings.NewReader("bar2"))
	require.NoError(t, err)

	// Should see two files
	fileInfos, err = client.ListFile(repo, commit2.ID, "dir")
	require.NoError(t, err)
	require.Equal(t, 2, len(fileInfos))

	require.NoError(t, client.FinishCommit(repo, commit2.ID))

	// Should see two files
	fileInfos, err = client.ListFile(repo, commit2.ID, "dir")
	require.NoError(t, err)
	require.Equal(t, 2, len(fileInfos))

	var buffer bytes.Buffer
	require.NoError(t, client.GetFile(repo, commit2.ID, "dir/foo", 0, 0, &buffer))
	require.Equal(t, "foo2", buffer.String())

	var buffer2 bytes.Buffer
	require.NoError(t, client.GetFile(repo, commit2.ID, "dir/bar", 0, 0, &buffer2))
	require.Equal(t, "bar2", buffer2.String())

	// Commit 3: delete the directory
	commit3, err := client.StartCommit(repo, "master")
	require.NoError(t, err)

	require.NoError(t, client.DeleteFile(repo, commit3.ID, "dir"))

	// Should see zero files
	fileInfos, err = client.ListFile(repo, commit3.ID, "")
	require.NoError(t, err)
	require.Equal(t, 0, len(fileInfos))

	require.NoError(t, client.FinishCommit(repo, commit3.ID))

	// Should see zero files
	fileInfos, err = client.ListFile(repo, commit3.ID, "")
	require.NoError(t, err)
	require.Equal(t, 0, len(fileInfos))

	// TODO: test deleting "."
}

func TestDeleteFile2(t *testing.T) {
	client := GetPachClient(t)

	repo := "test"
	require.NoError(t, client.CreateRepo(repo))

	commit1, err := client.StartCommit(repo, "master")
	require.NoError(t, err)
	_, err = client.PutFile(repo, commit1.ID, "file", strings.NewReader("foo\n"))
	require.NoError(t, err)
	require.NoError(t, client.FinishCommit(repo, commit1.ID))

	commit2, err := client.StartCommit(repo, "master")
	require.NoError(t, err)
	err = client.DeleteFile(repo, commit2.ID, "file")
	require.NoError(t, err)
	_, err = client.PutFile(repo, commit2.ID, "file", strings.NewReader("bar\n"))
	require.NoError(t, err)
	require.NoError(t, client.FinishCommit(repo, commit2.ID))

	expected := "bar\n"
	var buffer bytes.Buffer
	require.NoError(t, client.GetFile(repo, "master", "file", 0, 0, &buffer))
	require.Equal(t, expected, buffer.String())

	commit3, err := client.StartCommit(repo, "master")
	require.NoError(t, err)
	_, err = client.PutFile(repo, commit3.ID, "file", strings.NewReader("buzz\n"))
	require.NoError(t, err)
	err = client.DeleteFile(repo, commit3.ID, "file")
	require.NoError(t, err)
	_, err = client.PutFile(repo, commit3.ID, "file", strings.NewReader("foo\n"))
	require.NoError(t, err)
	require.NoError(t, client.FinishCommit(repo, commit3.ID))

	expected = "foo\n"
	buffer.Reset()
	require.NoError(t, client.GetFile(repo, commit3.ID, "file", 0, 0, &buffer))
	require.Equal(t, expected, buffer.String())
}

func TestListCommit(t *testing.T) {
	client := GetPachClient(t)

	repo := "test"
	require.NoError(t, client.CreateRepo(repo))

	numCommits := 10

	var midCommitID string
	for i := 0; i < numCommits; i++ {
		commit, err := client.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, client.FinishCommit(repo, "master"))
		if i == numCommits/2 {
			midCommitID = commit.ID
		}
	}

	// list all commits
	commitInfos, err := client.ListCommit(repo, "", "", 0)
	require.NoError(t, err)
	require.Equal(t, numCommits, len(commitInfos))

	// Test that commits are sorted in newest-first order
	for i := 0; i < len(commitInfos)-1; i++ {
		require.Equal(t, commitInfos[i].ParentCommit, commitInfos[i+1].Commit)
	}

	// Now list all commits up to the last commit
	commitInfos, err = client.ListCommit(repo, "master", "", 0)
	require.NoError(t, err)
	require.Equal(t, numCommits, len(commitInfos))

	// Test that commits are sorted in newest-first order
	for i := 0; i < len(commitInfos)-1; i++ {
		require.Equal(t, commitInfos[i].ParentCommit, commitInfos[i+1].Commit)
	}

	// Now list all commits up to the mid commit, excluding the mid commit
	// itself
	commitInfos, err = client.ListCommit(repo, "master", midCommitID, 0)
	require.NoError(t, err)
	require.Equal(t, numCommits-numCommits/2-1, len(commitInfos))

	// Test that commits are sorted in newest-first order
	for i := 0; i < len(commitInfos)-1; i++ {
		require.Equal(t, commitInfos[i].ParentCommit, commitInfos[i+1].Commit)
	}

	// list commits by branch
	commitInfos, err = client.ListCommit(repo, "master", "", 0)
	require.NoError(t, err)
	require.Equal(t, numCommits, len(commitInfos))

	// Test that commits are sorted in newest-first order
	for i := 0; i < len(commitInfos)-1; i++ {
		require.Equal(t, commitInfos[i].ParentCommit, commitInfos[i+1].Commit)
	}
}

func TestOffsetRead(t *testing.T) {
	client := GetPachClient(t)

	repo := "TestOffsetRead"
	require.NoError(t, client.CreateRepo(repo))
	commit, err := client.StartCommit(repo, "")
	require.NoError(t, err)
	fileData := "foo\n"
	_, err = client.PutFile(repo, commit.ID, "foo", strings.NewReader(fileData))
	require.NoError(t, err)
	_, err = client.PutFile(repo, commit.ID, "foo", strings.NewReader(fileData))
	require.NoError(t, err)

	var buffer bytes.Buffer
	require.NoError(t, client.GetFile(repo, commit.ID, "foo", int64(len(fileData)*2)+1, 0, &buffer))
	require.Equal(t, "", buffer.String())

	require.NoError(t, client.FinishCommit(repo, commit.ID))

	buffer.Reset()
	require.NoError(t, client.GetFile(repo, commit.ID, "foo", int64(len(fileData)*2)+1, 0, &buffer))
	require.Equal(t, "", buffer.String())
}

func TestBranch2(t *testing.T) {
	client := GetPachClient(t)

	repo := "test"
	require.NoError(t, client.CreateRepo(repo))

	commit, err := client.StartCommit(repo, "")
	require.NoError(t, err)
	require.NoError(t, client.FinishCommit(repo, commit.ID))

	expectedBranches := []string{"branch1", "branch2", "branch3"}
	for _, branch := range expectedBranches {
		require.NoError(t, client.SetBranch(repo, commit.ID, branch))
	}

	branches, err := client.ListBranch(repo)
	require.Equal(t, len(expectedBranches), len(branches))
	for i, branch := range branches {
		// branches should return in newest-first order
		require.Equal(t, expectedBranches[len(branches)-i-1], branch.Name)
		require.Equal(t, commit, branch.Head)
	}

	commit2, err := client.StartCommit(repo, "branch1")
	require.NoError(t, err)
	require.NoError(t, client.FinishCommit(repo, "branch1"))

	commit2Info, err := client.InspectCommit(repo, "branch1")
	require.NoError(t, err)
	require.Equal(t, commit, commit2Info.ParentCommit)

	// delete the last branch
	var lastBranch string
	lastBranch = expectedBranches[len(expectedBranches)-1]
	require.NoError(t, client.DeleteBranch(repo, lastBranch, false))
	branches, err = client.ListBranch(repo)
	require.Equal(t, 2, len(branches))
	require.Equal(t, "branch2", branches[0].Name)
	require.Equal(t, commit, branches[0].Head)
	require.Equal(t, "branch1", branches[1].Name)
	require.Equal(t, commit2, branches[1].Head)
}

func TestDeleteNonexistantBranch(t *testing.T) {
	client := GetPachClient(t)

	repo := "TestDeleteNonexistantBranch"
	require.NoError(t, client.CreateRepo(repo))
	require.NoError(t, client.DeleteBranch(repo, "doesnt_exist", false))
}

func TestSubscribeCommit(t *testing.T) {
	client := GetPachClient(t)

	repo := "test"
	require.NoError(t, client.CreateRepo(repo))

	numCommits := 10

	// create some commits that shouldn't affect the below SubscribeCommit call
	// reproduces #2469
	for i := 0; i < numCommits; i++ {
		commit, err := client.StartCommit(repo, "master-v1")
		require.NoError(t, err)
		require.NoError(t, client.FinishCommit(repo, commit.ID))
	}

	var commits []*pfs.Commit
	for i := 0; i < numCommits; i++ {
		commit, err := client.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, client.FinishCommit(repo, commit.ID))
		commits = append(commits, commit)
	}

	commitIter, err := client.SubscribeCommit(repo, "master", "", pfs.CommitState_STARTED)
	require.NoError(t, err)
	for i := 0; i < numCommits; i++ {
		commitInfo, err := commitIter.Next()
		require.NoError(t, err)
		require.Equal(t, commits[i], commitInfo.Commit)
	}

	// Create another batch of commits
	commits = nil
	for i := 0; i < numCommits; i++ {
		commit, err := client.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, client.FinishCommit(repo, "master"))
		commits = append(commits, commit)
	}

	for i := 0; i < numCommits; i++ {
		commitInfo, err := commitIter.Next()
		require.NoError(t, err)
		require.Equal(t, commits[i], commitInfo.Commit)
	}

	commitIter.Close()
}

func TestInspectRepoSimple(t *testing.T) {
	client := GetPachClient(t)

	repo := "test"
	require.NoError(t, client.CreateRepo(repo))

	commit, err := client.StartCommit(repo, "")
	require.NoError(t, err)

	file1Content := "foo\n"
	_, err = client.PutFile(repo, commit.ID, "foo", strings.NewReader(file1Content))
	require.NoError(t, err)

	file2Content := "bar\n"
	_, err = client.PutFile(repo, commit.ID, "bar", strings.NewReader(file2Content))
	require.NoError(t, err)

	require.NoError(t, client.FinishCommit(repo, commit.ID))

	info, err := client.InspectRepo(repo)
	require.NoError(t, err)

	// Size should be 0 because the files were not added to master
	require.Equal(t, int(info.SizeBytes), 0)
}

func TestInspectRepoComplex(t *testing.T) {
	client := GetPachClient(t)

	repo := "test"
	require.NoError(t, client.CreateRepo(repo))

	commit, err := client.StartCommit(repo, "master")
	require.NoError(t, err)

	numFiles := 100
	minFileSize := 1000
	maxFileSize := 2000
	totalSize := 0

	for i := 0; i < numFiles; i++ {
		fileContent := generateRandomString(rand.Intn(maxFileSize-minFileSize) + minFileSize)
		fileContent += "\n"
		fileName := fmt.Sprintf("file_%d", i)
		totalSize += len(fileContent)

		_, err = client.PutFile(repo, commit.ID, fileName, strings.NewReader(fileContent))
		require.NoError(t, err)

	}

	require.NoError(t, client.FinishCommit(repo, commit.ID))

	info, err := client.InspectRepo(repo)
	require.NoError(t, err)

	require.Equal(t, int(info.SizeBytes), totalSize)

	infos, err := client.ListRepo()
	require.NoError(t, err)
	require.Equal(t, 1, len(infos))
	info = infos[0]

	require.Equal(t, int(info.SizeBytes), totalSize)
}

func TestCreate(t *testing.T) {
	client := GetPachClient(t)

	repo := "test"
	require.NoError(t, client.CreateRepo(repo))
	commit, err := client.StartCommit(repo, "")
	require.NoError(t, err)
	w, err := client.PutFileSplitWriter(repo, commit.ID, "foo", pfs.Delimiter_NONE, 0, 0, 0, false)
	require.NoError(t, err)
	require.NoError(t, w.Close())
	require.NoError(t, client.FinishCommit(repo, commit.ID))
	_, err = client.InspectFile(repo, commit.ID, "foo")
	require.NoError(t, err)
}

func TestGetFile(t *testing.T) {
	client := GetPachClient(t)
	repo := tu.UniqueString("test")
	require.NoError(t, client.CreateRepo(repo))
	commit, err := client.StartCommit(repo, "")
	require.NoError(t, err)
	_, err = client.PutFile(repo, commit.ID, "dir/file", strings.NewReader("foo\n"))
	require.NoError(t, err)
	require.NoError(t, client.FinishCommit(repo, commit.ID))
	var buffer bytes.Buffer
	require.NoError(t, client.GetFile(repo, commit.ID, "dir/file", 0, 0, &buffer))
	require.Equal(t, "foo\n", buffer.String())
	t.Run("InvalidCommit", func(t *testing.T) {
		buffer = bytes.Buffer{}
		err = client.GetFile(repo, "aninvalidcommitid", "dir/file", 0, 0, &buffer)
		require.YesError(t, err)
	})
	t.Run("Directory", func(t *testing.T) {
		buffer = bytes.Buffer{}
		err = client.GetFile(repo, commit.ID, "dir", 0, 0, &buffer)
		require.NoError(t, err)
	})
}

func TestManyPutsSingleFileSingleCommit(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping long tests in short mode")
	}
	client := GetPachClient(t)

	repo := "test"
	require.NoError(t, client.CreateRepo(repo))

	commit1, err := client.StartCommit(repo, "")
	require.NoError(t, err)

	rawMessage := `{
		"level":"debug",
		"message":{
			"thing":"foo"
		},
		"timing":[1,3,34,6,7]
	}`
	numObjs := 500
	numGoros := 10
	var expectedOutput []byte
	var wg sync.WaitGroup
	for j := 0; j < numGoros; j++ {
		wg.Add(1)
		go func() {
			for i := 0; i < numObjs/numGoros; i++ {
				_, err = client.PutFile(repo, commit1.ID, "foo", strings.NewReader(rawMessage))
				if err != nil {
					panic(err)
				}
			}
			wg.Done()
		}()
	}
	for i := 0; i < numObjs; i++ {
		expectedOutput = append(expectedOutput, []byte(rawMessage)...)
	}
	wg.Wait()
	require.NoError(t, client.FinishCommit(repo, commit1.ID))

	var buffer bytes.Buffer
	require.NoError(t, client.GetFile(repo, commit1.ID, "foo", 0, 0, &buffer))
	require.Equal(t, string(expectedOutput), buffer.String())
}

func TestPutFileValidCharacters(t *testing.T) {
	client := GetPachClient(t)

	repo := "test"
	require.NoError(t, client.CreateRepo(repo))

	commit, err := client.StartCommit(repo, "")
	require.NoError(t, err)

	_, err = client.PutFile(repo, commit.ID, "foo\x00bar", strings.NewReader("foobar\n"))
	// null characters error because when you `ls` files with null characters
	// they truncate things after the null character leading to strange results
	require.YesError(t, err)

	// Boundary tests for valid character range
	_, err = client.PutFile(repo, commit.ID, "\x1ffoobar", strings.NewReader("foobar\n"))
	require.YesError(t, err)
	_, err = client.PutFile(repo, commit.ID, "foo\x20bar", strings.NewReader("foobar\n"))
	require.NoError(t, err)
	_, err = client.PutFile(repo, commit.ID, "foobar\x7e", strings.NewReader("foobar\n"))
	require.NoError(t, err)
	_, err = client.PutFile(repo, commit.ID, "foo\x7fbar", strings.NewReader("foobar\n"))
	require.YesError(t, err)

	// Random character tests outside and inside valid character range
	_, err = client.PutFile(repo, commit.ID, "foobar\x0b", strings.NewReader("foobar\n"))
	require.YesError(t, err)
	_, err = client.PutFile(repo, commit.ID, "\x41foobar", strings.NewReader("foobar\n"))
	require.NoError(t, err)
	_, err = client.PutFile(repo, commit.ID, "foo\x90bar", strings.NewReader("foobar\n"))
	require.YesError(t, err)
}

func TestPutFileURL(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := GetPachClient(t)

	repo := "TestPutFileURL"
	require.NoError(t, c.CreateRepo(repo))
	commit, err := c.StartCommit(repo, "master")
	require.NoError(t, err)
	require.NoError(t, c.PutFileURL(repo, commit.ID, "readme", "https://raw.githubusercontent.com/pachyderm/pachyderm/master/README.md", false, false))
	require.NoError(t, c.FinishCommit(repo, commit.ID))
	fileInfo, err := c.InspectFile(repo, commit.ID, "readme")
	require.NoError(t, err)
	require.True(t, fileInfo.SizeBytes > 0)
}

func TestBigListFile(t *testing.T) {
	client := GetPachClient(t)

	repo := "TestBigListFile"
	require.NoError(t, client.CreateRepo(repo))
	commit, err := client.StartCommit(repo, "")
	require.NoError(t, err)
	var eg errgroup.Group
	for i := 0; i < 25; i++ {
		for j := 0; j < 25; j++ {
			i := i
			j := j
			eg.Go(func() error {
				_, err = client.PutFile(repo, commit.ID, fmt.Sprintf("dir%d/file%d", i, j), strings.NewReader("foo\n"))
				return err
			})
		}
	}
	require.NoError(t, eg.Wait())
	require.NoError(t, client.FinishCommit(repo, commit.ID))
	for i := 0; i < 25; i++ {
		files, err := client.ListFile(repo, commit.ID, fmt.Sprintf("dir%d", i))
		require.NoError(t, err)
		require.Equal(t, 25, len(files))
	}
}

func TestStartCommitLatestOnBranch(t *testing.T) {
	client := GetPachClient(t)

	repo := "test"
	require.NoError(t, client.CreateRepo(repo))

	commit1, err := client.StartCommit(repo, "master")
	require.NoError(t, err)
	require.NoError(t, client.FinishCommit(repo, commit1.ID))

	commit2, err := client.StartCommit(repo, "master")
	require.NoError(t, err)

	require.NoError(t, client.FinishCommit(repo, commit2.ID))

	commit3, err := client.StartCommit(repo, "master")
	require.NoError(t, err)
	require.NoError(t, client.FinishCommit(repo, commit3.ID))

	commitInfo, err := client.InspectCommit(repo, "master")
	require.Equal(t, commit3.ID, commitInfo.Commit.ID)
}

func TestSetBranchTwice(t *testing.T) {
	client := GetPachClient(t)

	repo := "test"
	require.NoError(t, client.CreateRepo(repo))

	commit1, err := client.StartCommit(repo, "")
	require.NoError(t, err)
	require.NoError(t, client.SetBranch(repo, commit1.ID, "master"))
	require.NoError(t, client.FinishCommit(repo, commit1.ID))

	commit2, err := client.StartCommit(repo, "")
	require.NoError(t, err)
	require.NoError(t, client.SetBranch(repo, commit2.ID, "master"))
	require.NoError(t, client.FinishCommit(repo, commit2.ID))

	branches, err := client.ListBranch(repo)
	require.NoError(t, err)

	require.Equal(t, 1, len(branches))
	require.Equal(t, "master", branches[0].Name)
	require.Equal(t, commit2.ID, branches[0].Head.ID)
}

func TestSyncPullPush(t *testing.T) {
	client := GetPachClient(t)

	repo1 := "repo1"
	require.NoError(t, client.CreateRepo(repo1))

	commit1, err := client.StartCommit(repo1, "master")
	require.NoError(t, err)
	_, err = client.PutFile(repo1, commit1.ID, "foo", strings.NewReader("foo\n"))
	require.NoError(t, err)
	_, err = client.PutFile(repo1, commit1.ID, "dir/bar", strings.NewReader("bar\n"))
	require.NoError(t, err)
	require.NoError(t, client.FinishCommit(repo1, commit1.ID))

	tmpDir, err := ioutil.TempDir("/tmp", "pfs")
	require.NoError(t, err)

	puller := pfssync.NewPuller()
	require.NoError(t, puller.Pull(client, tmpDir, repo1, commit1.ID, "/", false, false, 2, nil, ""))
	_, err = puller.CleanUp()
	require.NoError(t, err)

	repo2 := "repo2"
	require.NoError(t, client.CreateRepo(repo2))

	commit2, err := client.StartCommit(repo2, "master")
	require.NoError(t, err)

	require.NoError(t, pfssync.Push(client, tmpDir, commit2, false))
	require.NoError(t, client.FinishCommit(repo2, commit2.ID))

	var buffer bytes.Buffer
	require.NoError(t, client.GetFile(repo2, commit2.ID, "foo", 0, 0, &buffer))
	require.Equal(t, "foo\n", buffer.String())
	buffer.Reset()
	require.NoError(t, client.GetFile(repo2, commit2.ID, "dir/bar", 0, 0, &buffer))
	require.Equal(t, "bar\n", buffer.String())

	fileInfos, err := client.ListFile(repo2, commit2.ID, "")
	require.NoError(t, err)
	require.Equal(t, 2, len(fileInfos))

	commit3, err := client.StartCommit(repo2, "master")
	require.NoError(t, err)

	// Test the overwrite flag.
	// After this Push operation, all files should still look the same, since
	// the old files were overwritten.
	require.NoError(t, pfssync.Push(client, tmpDir, commit3, true))
	require.NoError(t, client.FinishCommit(repo2, commit3.ID))

	buffer.Reset()
	require.NoError(t, client.GetFile(repo2, commit3.ID, "foo", 0, 0, &buffer))
	require.Equal(t, "foo\n", buffer.String())
	buffer.Reset()
	require.NoError(t, client.GetFile(repo2, commit3.ID, "dir/bar", 0, 0, &buffer))
	require.Equal(t, "bar\n", buffer.String())

	fileInfos, err = client.ListFile(repo2, commit3.ID, "")
	require.NoError(t, err)
	require.Equal(t, 2, len(fileInfos))

	// Test Lazy files
	tmpDir2, err := ioutil.TempDir("/tmp", "pfs")
	require.NoError(t, err)

	puller = pfssync.NewPuller()
	require.NoError(t, puller.Pull(client, tmpDir2, repo1, "master", "/", true, false, 2, nil, ""))

	data, err := ioutil.ReadFile(path.Join(tmpDir2, "dir/bar"))
	require.NoError(t, err)
	require.Equal(t, "bar\n", string(data))

	_, err = puller.CleanUp()
	require.NoError(t, err)
}

func TestSyncFile(t *testing.T) {
	client := GetPachClient(t)

	repo := "repo"
	require.NoError(t, client.CreateRepo(repo))

	content1 := generateRandomString(int(pfs.ChunkSize))

	commit1, err := client.StartCommit(repo, "master")
	require.NoError(t, err)
	require.NoError(t, pfssync.PushFile(client, client, &pfs.File{
		Commit: commit1,
		Path:   "file",
	}, strings.NewReader(content1)))
	require.NoError(t, client.FinishCommit(repo, commit1.ID))

	var buffer bytes.Buffer
	require.NoError(t, client.GetFile(repo, commit1.ID, "file", 0, 0, &buffer))
	require.Equal(t, content1, buffer.String())

	content2 := generateRandomString(int(pfs.ChunkSize * 2))

	commit2, err := client.StartCommit(repo, "master")
	require.NoError(t, err)
	require.NoError(t, pfssync.PushFile(client, client, &pfs.File{
		Commit: commit2,
		Path:   "file",
	}, strings.NewReader(content2)))
	require.NoError(t, client.FinishCommit(repo, commit2.ID))

	buffer.Reset()
	require.NoError(t, client.GetFile(repo, commit2.ID, "file", 0, 0, &buffer))
	require.Equal(t, content2, buffer.String())

	content3 := content2 + generateRandomString(int(pfs.ChunkSize))

	commit3, err := client.StartCommit(repo, "master")
	require.NoError(t, err)
	require.NoError(t, pfssync.PushFile(client, client, &pfs.File{
		Commit: commit3,
		Path:   "file",
	}, strings.NewReader(content3)))
	require.NoError(t, client.FinishCommit(repo, commit3.ID))

	buffer.Reset()
	require.NoError(t, client.GetFile(repo, commit3.ID, "file", 0, 0, &buffer))
	require.Equal(t, content3, buffer.String())
}

func TestSyncEmptyDir(t *testing.T) {
	client := GetPachClient(t)

	repo := "repo"
	require.NoError(t, client.CreateRepo(repo))

	commit, err := client.StartCommit(repo, "master")
	require.NoError(t, err)
	require.NoError(t, client.FinishCommit(repo, commit.ID))

	tmpDir, err := ioutil.TempDir("/tmp", "pfs")
	require.NoError(t, err)

	// We want to make sure that Pull creates an empty directory
	// when the path that we are cloning is empty.
	dir := filepath.Join(tmpDir, "tmp")

	puller := pfssync.NewPuller()
	require.NoError(t, puller.Pull(client, dir, repo, commit.ID, "/", false, false, 0, nil, ""))
	_, err = os.Stat(dir)
	require.NoError(t, err)
	_, err = puller.CleanUp()
	require.NoError(t, err)
}

func TestFlush(t *testing.T) {
	client := GetPachClient(t)
	require.NoError(t, client.CreateRepo("A"))
	require.NoError(t, client.CreateRepo("B"))
	require.NoError(t, client.CreateBranch("B", "master", "", []*pfs.Branch{pclient.NewBranch("A", "master")}))
	ACommit, err := client.StartCommit("A", "master")
	require.NoError(t, err)
	require.NoError(t, client.FinishCommit("A", "master"))
	require.NoError(t, client.FinishCommit("B", "master"))
	commitInfoIter, err := client.FlushCommit([]*pfs.Commit{pclient.NewCommit("A", ACommit.ID)}, nil)
	require.NoError(t, err)
	commitInfos, err := collectCommitInfos(commitInfoIter)
	require.NoError(t, err)
	require.Equal(t, 1, len(commitInfos))
}

// TestFlush2 implements the following DAG:
// A ─▶ B ─▶ C ─▶ D
func TestFlush2(t *testing.T) {
	client := GetPachClient(t)
	require.NoError(t, client.CreateRepo("A"))
	require.NoError(t, client.CreateRepo("B"))
	require.NoError(t, client.CreateRepo("C"))
	require.NoError(t, client.CreateRepo("D"))
	require.NoError(t, client.CreateBranch("B", "master", "", []*pfs.Branch{pclient.NewBranch("A", "master")}))
	require.NoError(t, client.CreateBranch("C", "master", "", []*pfs.Branch{pclient.NewBranch("B", "master")}))
	require.NoError(t, client.CreateBranch("D", "master", "", []*pfs.Branch{pclient.NewBranch("C", "master")}))
	ACommit, err := client.StartCommit("A", "master")
	require.NoError(t, err)
	require.NoError(t, client.FinishCommit("A", "master"))

	// do the other commits in a goro so we can block for them
	go func() {
		require.NoError(t, client.FinishCommit("B", "master"))
		require.NoError(t, client.FinishCommit("C", "master"))
		require.NoError(t, client.FinishCommit("D", "master"))
	}()

	// Flush ACommit
	commitInfoIter, err := client.FlushCommit([]*pfs.Commit{pclient.NewCommit("A", ACommit.ID)}, nil)
	require.NoError(t, err)
	commitInfos, err := collectCommitInfos(commitInfoIter)
	require.NoError(t, err)
	require.Equal(t, 3, len(commitInfos))

	commitInfoIter, err = client.FlushCommit(
		[]*pfs.Commit{pclient.NewCommit("A", ACommit.ID)},
		[]*pfs.Repo{pclient.NewRepo("C")},
	)
	require.NoError(t, err)
	commitInfos, err = collectCommitInfos(commitInfoIter)
	require.NoError(t, err)
	require.Equal(t, 1, len(commitInfos))
}

// A
//  ╲
//   ◀
//    C
//   ◀
//  ╱
// B
func TestFlush3(t *testing.T) {
	client := GetPachClient(t)
	require.NoError(t, client.CreateRepo("A"))
	require.NoError(t, client.CreateRepo("B"))
	require.NoError(t, client.CreateRepo("C"))

	require.NoError(t, client.CreateBranch("C", "master", "", []*pfs.Branch{pclient.NewBranch("A", "master"), pclient.NewBranch("B", "master")}))

	ACommit, err := client.StartCommit("A", "master")
	require.NoError(t, err)
	require.NoError(t, client.FinishCommit("A", ACommit.ID))
	require.NoError(t, client.FinishCommit("C", "master"))
	BCommit, err := client.StartCommit("B", "master")
	require.NoError(t, err)
	require.NoError(t, client.FinishCommit("B", BCommit.ID))
	require.NoError(t, client.FinishCommit("C", "master"))

	BCommit, err = client.StartCommit("B", "master")
	require.NoError(t, err)
	require.NoError(t, client.FinishCommit("B", BCommit.ID))
	require.NoError(t, client.FinishCommit("C", "master"))

	commitIter, err := client.FlushCommit([]*pfs.Commit{pclient.NewCommit("B", BCommit.ID), pclient.NewCommit("A", ACommit.ID)}, nil)
	require.NoError(t, err)
	commitInfos, err := collectCommitInfos(commitIter)
	require.NoError(t, err)
	require.Equal(t, 1, len(commitInfos))

	require.Equal(t, commitInfos[0].Commit.Repo.Name, "C")
}

func TestFlushRedundant(t *testing.T) {
	client := GetPachClient(t)
	require.NoError(t, client.CreateRepo("A"))
	ACommit, err := client.StartCommit("A", "master")
	require.NoError(t, err)
	require.NoError(t, client.FinishCommit("A", "master"))
	commitInfoIter, err := client.FlushCommit([]*pfs.Commit{pclient.NewCommit("A", ACommit.ID), pclient.NewCommit("A", ACommit.ID)}, nil)
	require.NoError(t, err)
	commitInfos, err := collectCommitInfos(commitInfoIter)
	require.NoError(t, err)
	require.Equal(t, 0, len(commitInfos))
}

func TestFlushCommitWithNoDownstreamRepos(t *testing.T) {
	c := GetPachClient(t)
	repo := "test"
	require.NoError(t, c.CreateRepo(repo))
	commit, err := c.StartCommit(repo, "master")
	require.NoError(t, err)
	require.NoError(t, c.FinishCommit(repo, commit.ID))
	commitIter, err := c.FlushCommit([]*pfs.Commit{pclient.NewCommit(repo, commit.ID)}, nil)
	require.NoError(t, err)
	commitInfos, err := collectCommitInfos(commitIter)
	require.NoError(t, err)
	require.Equal(t, 0, len(commitInfos))
}

func TestFlushOpenCommit(t *testing.T) {

	client := GetPachClient(t)
	require.NoError(t, client.CreateRepo("A"))
	require.NoError(t, client.CreateRepo("B"))
	require.NoError(t, client.CreateBranch("B", "master", "", []*pfs.Branch{pclient.NewBranch("A", "master")}))
	ACommit, err := client.StartCommit("A", "master")
	require.NoError(t, err)

	// do the other commits in a goro so we can block for them
	go func() {
		time.Sleep(5 * time.Second)
		require.NoError(t, client.FinishCommit("A", "master"))
		require.NoError(t, client.FinishCommit("B", "master"))
	}()

	// Flush ACommit
	commitIter, err := client.FlushCommit([]*pfs.Commit{pclient.NewCommit("A", ACommit.ID)}, nil)
	require.NoError(t, err)
	commitInfos, err := collectCommitInfos(commitIter)
	require.NoError(t, err)
	require.Equal(t, 1, len(commitInfos))
}

func TestEmptyFlush(t *testing.T) {

	client := GetPachClient(t)
	commitIter, err := client.FlushCommit(nil, nil)
	require.NoError(t, err)
	_, err = collectCommitInfos(commitIter)
	require.YesError(t, err)
}

func TestFlushNonExistentCommit(t *testing.T) {

	c := GetPachClient(t)
	iter, err := c.FlushCommit([]*pfs.Commit{pclient.NewCommit("fake-repo", "fake-commit")}, nil)
	require.NoError(t, err)
	_, err = collectCommitInfos(iter)
	require.YesError(t, err)
	repo := "FlushNonExistentCommit"
	require.NoError(t, c.CreateRepo(repo))
	_, err = c.FlushCommit([]*pfs.Commit{pclient.NewCommit(repo, "fake-commit")}, nil)
	require.NoError(t, err)
	_, err = collectCommitInfos(iter)
	require.YesError(t, err)
}

func TestPutFileSplit(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := GetPachClient(t)
	// create repos
	repo := tu.UniqueString("TestPutFileSplit")
	require.NoError(t, c.CreateRepo(repo))
	commit, err := c.StartCommit(repo, "master")
	require.NoError(t, err)
	_, err = c.PutFileSplit(repo, commit.ID, "none", pfs.Delimiter_NONE, 0, 0, 0, false, strings.NewReader("foo\nbar\nbuz\n"))
	require.NoError(t, err)
	_, err = c.PutFileSplit(repo, commit.ID, "line", pfs.Delimiter_LINE, 0, 0, 0, false, strings.NewReader("foo\nbar\nbuz\n"))
	require.NoError(t, err)
	_, err = c.PutFileSplit(repo, commit.ID, "line", pfs.Delimiter_LINE, 0, 0, 0, false, strings.NewReader("foo\nbar\nbuz\n"))
	require.NoError(t, err)
	_, err = c.PutFileSplit(repo, commit.ID, "line2", pfs.Delimiter_LINE, 2, 0, 0, false, strings.NewReader("foo\nbar\nbuz\nfiz\n"))
	require.NoError(t, err)
	_, err = c.PutFileSplit(repo, commit.ID, "line3", pfs.Delimiter_LINE, 0, 8, 0, false, strings.NewReader("foo\nbar\nbuz\nfiz\n"))
	require.NoError(t, err)
	_, err = c.PutFileSplit(repo, commit.ID, "json", pfs.Delimiter_JSON, 0, 0, 0, false, strings.NewReader("{}{}{}{}{}{}{}{}{}{}"))
	require.NoError(t, err)
	_, err = c.PutFileSplit(repo, commit.ID, "json", pfs.Delimiter_JSON, 0, 0, 0, false, strings.NewReader("{}{}{}{}{}{}{}{}{}{}"))
	require.NoError(t, err)
	_, err = c.PutFileSplit(repo, commit.ID, "json2", pfs.Delimiter_JSON, 2, 0, 0, false, strings.NewReader("{}{}{}{}"))
	require.NoError(t, err)
	_, err = c.PutFileSplit(repo, commit.ID, "json3", pfs.Delimiter_JSON, 0, 4, 0, false, strings.NewReader("{}{}{}{}"))
	require.NoError(t, err)

	files, err := c.ListFile(repo, commit.ID, "line2")
	require.NoError(t, err)
	require.Equal(t, 2, len(files))
	for _, fileInfo := range files {
		require.Equal(t, uint64(8), fileInfo.SizeBytes)
	}

	require.NoError(t, c.FinishCommit(repo, commit.ID))
	commit2, err := c.StartCommit(repo, "master")
	require.NoError(t, err)
	_, err = c.PutFileSplit(repo, commit2.ID, "line", pfs.Delimiter_LINE, 0, 0, 0, false, strings.NewReader("foo\nbar\nbuz\n"))
	require.NoError(t, err)
	_, err = c.PutFileSplit(repo, commit2.ID, "json", pfs.Delimiter_JSON, 0, 0, 0, false, strings.NewReader("{}{}{}{}{}{}{}{}{}{}"))
	require.NoError(t, err)

	files, err = c.ListFile(repo, commit2.ID, "line")
	require.NoError(t, err)
	require.Equal(t, 9, len(files))
	for _, fileInfo := range files {
		require.Equal(t, uint64(4), fileInfo.SizeBytes)
	}

	require.NoError(t, c.FinishCommit(repo, commit2.ID))
	fileInfo, err := c.InspectFile(repo, commit.ID, "none")
	require.NoError(t, err)
	require.Equal(t, pfs.FileType_FILE, fileInfo.FileType)
	files, err = c.ListFile(repo, commit.ID, "line")
	require.NoError(t, err)
	require.Equal(t, 6, len(files))
	for _, fileInfo := range files {
		require.Equal(t, uint64(4), fileInfo.SizeBytes)
	}
	files, err = c.ListFile(repo, commit2.ID, "line")
	require.NoError(t, err)
	require.Equal(t, 9, len(files))
	for _, fileInfo := range files {
		require.Equal(t, uint64(4), fileInfo.SizeBytes)
	}
	files, err = c.ListFile(repo, commit.ID, "line2")
	require.NoError(t, err)
	require.Equal(t, 2, len(files))
	for _, fileInfo := range files {
		require.Equal(t, uint64(8), fileInfo.SizeBytes)
	}
	files, err = c.ListFile(repo, commit.ID, "line3")
	require.NoError(t, err)
	require.Equal(t, 2, len(files))
	for _, fileInfo := range files {
		require.Equal(t, uint64(8), fileInfo.SizeBytes)
	}
	files, err = c.ListFile(repo, commit.ID, "json")
	require.NoError(t, err)
	require.Equal(t, 20, len(files))
	for _, fileInfo := range files {
		require.Equal(t, uint64(2), fileInfo.SizeBytes)
	}
	files, err = c.ListFile(repo, commit2.ID, "json")
	require.NoError(t, err)
	require.Equal(t, 30, len(files))
	for _, fileInfo := range files {
		require.Equal(t, uint64(2), fileInfo.SizeBytes)
	}
	files, err = c.ListFile(repo, commit.ID, "json2")
	require.NoError(t, err)
	require.Equal(t, 2, len(files))
	for _, fileInfo := range files {
		require.Equal(t, uint64(4), fileInfo.SizeBytes)
	}
	files, err = c.ListFile(repo, commit.ID, "json3")
	require.NoError(t, err)
	require.Equal(t, 2, len(files))
	for _, fileInfo := range files {
		require.Equal(t, uint64(4), fileInfo.SizeBytes)
	}
}

func TestPutFileSplitBig(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := GetPachClient(t)
	// create repos
	repo := tu.UniqueString("TestPutFileSplitBig")
	require.NoError(t, c.CreateRepo(repo))
	commit, err := c.StartCommit(repo, "master")
	require.NoError(t, err)
	w, err := c.PutFileSplitWriter(repo, commit.ID, "line", pfs.Delimiter_LINE, 0, 0, 0, false)
	require.NoError(t, err)
	for i := 0; i < 1000; i++ {
		_, err = w.Write([]byte("foo\n"))
		require.NoError(t, err)
	}
	require.NoError(t, w.Close())
	require.NoError(t, c.FinishCommit(repo, commit.ID))
	files, err := c.ListFile(repo, commit.ID, "line")
	require.NoError(t, err)
	require.Equal(t, 1000, len(files))
	for _, fileInfo := range files {
		require.Equal(t, uint64(4), fileInfo.SizeBytes)
	}
}

func TestPutFileSplitCSV(t *testing.T) {
	c := GetPachClient(t)
	// create repos
	repo := tu.UniqueString("TestPutFileSplitCSV")
	require.NoError(t, c.CreateRepo(repo))
	_, err := c.PutFileSplit(repo, "master", "data", pfs.Delimiter_CSV, 0, 0, 0, false,
		// Weird, but this is actually two lines ("is\na" is quoted, so one cell)
		strings.NewReader("this,is,a,test\n"+
			"\"\"\"this\"\"\",\"is\nonly\",\"a,test\"\n"))
	require.NoError(t, err)
	fileInfos, err := c.ListFile(repo, "master", "/data")
	require.NoError(t, err)
	require.Equal(t, 2, len(fileInfos))
	var contents bytes.Buffer
	c.GetFile(repo, "master", "/data/0000000000000000", 0, 0, &contents)
	require.Equal(t, "this,is,a,test\n", contents.String())
	contents.Reset()
	c.GetFile(repo, "master", "/data/0000000000000001", 0, 0, &contents)
	require.Equal(t, "\"\"\"this\"\"\",\"is\nonly\",\"a,test\"\n", contents.String())
}

func TestPutFileSplitSQL(t *testing.T) {
	c := GetPachClient(t)
	// create repos
	repo := tu.UniqueString("TestPutFileSplitSQL")
	require.NoError(t, c.CreateRepo(repo))

	_, err := c.PutFileSplit(repo, "master", "/sql", pfs.Delimiter_SQL, 0, 0, 0,
		false, strings.NewReader(tu.TestPGDump))
	require.NoError(t, err)
	fileInfos, err := c.ListFile(repo, "master", "/sql")
	require.NoError(t, err)
	require.Equal(t, 5, len(fileInfos))

	// Get one of the SQL records & validate it
	var contents bytes.Buffer
	c.GetFile(repo, "master", "/sql/0000000000000000", 0, 0, &contents)
	// Validate that the recieved pgdump file creates the cars table
	require.Matches(t, "CREATE TABLE public\\.cars", contents.String())
	// Validate the SQL header more generally by passing the output of GetFile
	// back through the SQL library & confirm that it parses correctly but only
	// has one row
	pgReader := sql.NewPGDumpReader(bufio.NewReader(bytes.NewReader(contents.Bytes())))
	record, err := pgReader.ReadRow()
	require.NoError(t, err)
	require.Equal(t, "Tesla\tRoadster\t2008\tliterally a rocket\n", string(record))
	record, err = pgReader.ReadRow()
	require.YesError(t, err)
	require.Equal(t, io.EOF, err)

	// Create a new commit that overwrites all existing data & puts it back with
	// --header-records=1
	commit, err := c.StartCommit(repo, "master")
	require.NoError(t, err)
	require.NoError(t, c.DeleteFile(repo, commit.ID, "/sql"))
	_, err = c.PutFileSplit(repo, commit.ID, "/sql", pfs.Delimiter_SQL, 0, 0, 1,
		false, strings.NewReader(tu.TestPGDump))
	require.NoError(t, err)
	require.NoError(t, c.FinishCommit(repo, commit.ID))
	fileInfos, err = c.ListFile(repo, "master", "/sql")
	require.NoError(t, err)
	require.Equal(t, 4, len(fileInfos))

	// Get one of the SQL records & validate it
	contents.Reset()
	c.GetFile(repo, "master", "/sql/0000000000000003", 0, 0, &contents)
	// Validate a that the recieved pgdump file creates the cars table
	require.Matches(t, "CREATE TABLE public\\.cars", contents.String())
	// Validate the SQL header more generally by passing the output of GetFile
	// back through the SQL library & confirm that it parses correctly but only
	// has one row
	pgReader = sql.NewPGDumpReader(bufio.NewReader(strings.NewReader(contents.String())))
	record, err = pgReader.ReadRow()
	require.NoError(t, err)
	require.Equal(t, "Tesla\tRoadster\t2008\tliterally a rocket\n", string(record))
	record, err = pgReader.ReadRow()
	require.NoError(t, err)
	require.Equal(t, "Toyota\tCorolla\t2005\tgreatest car ever made\n", string(record))
	record, err = pgReader.ReadRow()
	require.YesError(t, err)
	require.Equal(t, io.EOF, err)
}

func TestPutFileHeaderRecordsBasic(t *testing.T) {
	c := GetPachClient(t)
	// create repos
	repo := tu.UniqueString("TestPutFileHeaderRecordsBasic")
	require.NoError(t, c.CreateRepo(repo))

	// Put simple CSV document, which should become a header and two records
	_, err := c.PutFileSplit(repo, "master", "data", pfs.Delimiter_CSV, 0, 0, 1, false,
		strings.NewReader("A,B,C,D\n"+
			"this,is,a,test\n"+
			"this,is,another,test\n"))
	require.NoError(t, err)
	fileInfos, err := c.ListFile(repo, "master", "/data")
	require.NoError(t, err)
	require.Equal(t, 2, len(fileInfos))

	// Header should be returned by GetFile, and should appear exactly once at the
	// beginning
	var contents bytes.Buffer
	c.GetFile(repo, "master", "/data/0000000000000000", 0, 0, &contents)
	require.Equal(t, "A,B,C,D\nthis,is,a,test\n", contents.String())
	contents.Reset()
	c.GetFile(repo, "master", "/data/0000000000000001", 0, 0, &contents)
	require.Equal(t, "A,B,C,D\nthis,is,another,test\n",
		contents.String())
	// Header only appears once, even though the contents of two files are
	// concatenated and returned
	contents.Reset()
	c.GetFile(repo, "master", "/data/*", 0, 0, &contents)
	require.Equal(t, "A,B,C,D\nthis,is,a,test\nthis,is,another,test\n",
		contents.String())

	// Header should be included in FileInfo
	fileOneInfo, err := c.InspectFile(repo, "master", "/data/0000000000000000")
	require.NoError(t, err)
	require.Equal(t, 2, len(fileOneInfo.Objects))
	fileTwoInfo, err := c.InspectFile(repo, "master", "/data/0000000000000001")
	require.NoError(t, err)
	require.Equal(t, 2, len(fileTwoInfo.Objects))

	// Both headers should be the same object
	require.Equal(t, fileOneInfo.Objects[0].Hash, fileTwoInfo.Objects[0].Hash)
	firstHeaderHash := fileOneInfo.Objects[0].Hash

	// Put a new CSV document with a new header (and no new rows). The number of
	// files should be unchanged, but GetFile should yield new results.
	// InspectFile should reveal the new headers
	_, err = c.PutFileSplit(repo, "master", "data", pfs.Delimiter_CSV, 0, 0, 1, false,
		strings.NewReader("h_one,h_two,h_three,h_four\n"))

	// New header from GetFile
	contents.Reset()
	c.GetFile(repo, "master", "/data/0000000000000000", 0, 0, &contents)
	require.Equal(t, "h_one,h_two,h_three,h_four\nthis,is,a,test\n", contents.String())
	contents.Reset()
	c.GetFile(repo, "master", "/data/0000000000000001", 0, 0, &contents)
	require.Equal(t, "h_one,h_two,h_three,h_four\nthis,is,another,test\n",
		contents.String())
	// Header only appears once, even though the contents of two files are
	// concatenated and returned
	contents.Reset()
	c.GetFile(repo, "master", "/data/*", 0, 0, &contents)
	require.Equal(t, "h_one,h_two,h_three,h_four\nthis,is,a,test\nthis,is,another,test\n",
		contents.String())

	// Header should be included in FileInfo
	fileOneInfo, err = c.InspectFile(repo, "master", "/data/0000000000000000")
	require.NoError(t, err)
	require.Equal(t, 2, len(fileOneInfo.Objects))
	fileTwoInfo, err = c.InspectFile(repo, "master", "/data/0000000000000001")
	require.NoError(t, err)
	require.Equal(t, 2, len(fileTwoInfo.Objects))

	// Both headers should be the same object, and distinct from the first header
	require.Equal(t, fileOneInfo.Objects[0].Hash, fileTwoInfo.Objects[0].Hash)
	require.NotEqual(t, firstHeaderHash, fileTwoInfo.Objects[0].Hash)
}

// TestGetFileGlobMultipleHeaders tests the case where a commit contains two
// header/footer directories, say a/* and b/*, and a user calls
// GetFile("/*/*"). We expect the data to come back
// a_header + a/1 + ... + a/N + a_footer + b_header + b/1 + ... + b/N + b_footer
func TestGetFileGlobMultipleHeaders(t *testing.T) {
	c := GetPachClient(t)
	// create repos
	repo := tu.UniqueString("TestGetFileGlobMultipleHeaders")
	require.NoError(t, c.CreateRepo(repo))

	// Put two header directories
	_, err := c.PutFileSplit(repo, "master", "/a", pfs.Delimiter_CSV, 0, 0, 1, false,
		strings.NewReader("AA,AB,AC,AD\n"+
			"a11,a12,a13,a14\n"+
			"a21,a22,a23,a24\n"))
	require.NoError(t, err)
	fileInfos, err := c.ListFile(repo, "master", "/a")
	require.NoError(t, err)
	require.Equal(t, 2, len(fileInfos))
	_, err = c.PutFileSplit(repo, "master", "/b", pfs.Delimiter_CSV, 0, 0, 1, false,
		strings.NewReader("BA,BB,BC,BD\n"+
			"b11,b12,b13,b14\n"+
			"b21,b22,b23,b24\n"))
	require.NoError(t, err)
	fileInfos, err = c.ListFile(repo, "master", "/b")
	require.NoError(t, err)
	require.Equal(t, 2, len(fileInfos))

	// Headers appear at the beinning of data from /a and /b
	var contents bytes.Buffer
	c.GetFile(repo, "master", "/a/*", 0, 0, &contents)
	require.Equal(t, "AA,AB,AC,AD\na11,a12,a13,a14\na21,a22,a23,a24\n",
		contents.String())
	contents.Reset()
	c.GetFile(repo, "master", "/b/*", 0, 0, &contents)
	require.Equal(t, "BA,BB,BC,BD\nb11,b12,b13,b14\nb21,b22,b23,b24\n",
		contents.String())

	// Getting data from both should yield text as described at the top
	contents.Reset()
	c.GetFile(repo, "master", "/*/*", 0, 0, &contents)
	require.Equal(t, "AA,AB,AC,AD\na11,a12,a13,a14\na21,a22,a23,a24\n"+
		"BA,BB,BC,BD\nb11,b12,b13,b14\nb21,b22,b23,b24\n",
		contents.String())
}

func TestDiff(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := GetPachClient(t)
	repo := tu.UniqueString("TestDiff")
	require.NoError(t, c.CreateRepo(repo))

	// Write foo
	_, err := c.StartCommit(repo, "master")
	require.NoError(t, err)
	_, err = c.PutFile(repo, "master", "foo", strings.NewReader("foo\n"))
	require.NoError(t, err)

	newFiles, oldFiles, err := c.DiffFile(repo, "master", "", "", "", "", false)
	require.NoError(t, err)
	require.Equal(t, 1, len(newFiles))
	require.Equal(t, "foo", newFiles[0].File.Path)
	require.Equal(t, 0, len(oldFiles))

	require.NoError(t, c.FinishCommit(repo, "master"))

	newFiles, oldFiles, err = c.DiffFile(repo, "master", "", "", "", "", false)
	require.NoError(t, err)
	require.Equal(t, 1, len(newFiles))
	require.Equal(t, "foo", newFiles[0].File.Path)
	require.Equal(t, 0, len(oldFiles))

	// Change the value of foo
	_, err = c.StartCommit(repo, "master")
	require.NoError(t, err)
	require.NoError(t, c.DeleteFile(repo, "master", "foo"))
	_, err = c.PutFile(repo, "master", "foo", strings.NewReader("not foo\n"))
	require.NoError(t, err)

	newFiles, oldFiles, err = c.DiffFile(repo, "master", "", "", "", "", false)
	require.NoError(t, err)
	require.Equal(t, 1, len(newFiles))
	require.Equal(t, "foo", newFiles[0].File.Path)
	require.Equal(t, 1, len(oldFiles))
	require.Equal(t, "foo", oldFiles[0].File.Path)

	require.NoError(t, c.FinishCommit(repo, "master"))

	newFiles, oldFiles, err = c.DiffFile(repo, "master", "", "", "", "", false)
	require.NoError(t, err)
	require.Equal(t, 1, len(newFiles))
	require.Equal(t, "foo", newFiles[0].File.Path)
	require.Equal(t, 1, len(oldFiles))
	require.Equal(t, "foo", oldFiles[0].File.Path)

	// Write bar
	_, err = c.StartCommit(repo, "master")
	require.NoError(t, err)
	_, err = c.PutFile(repo, "master", "bar", strings.NewReader("bar\n"))
	require.NoError(t, err)

	newFiles, oldFiles, err = c.DiffFile(repo, "master", "", "", "", "", false)
	require.NoError(t, err)
	require.Equal(t, 1, len(newFiles))
	require.Equal(t, "bar", newFiles[0].File.Path)
	require.Equal(t, 0, len(oldFiles))

	require.NoError(t, c.FinishCommit(repo, "master"))

	newFiles, oldFiles, err = c.DiffFile(repo, "master", "", "", "", "", false)
	require.NoError(t, err)
	require.Equal(t, 1, len(newFiles))
	require.Equal(t, "bar", newFiles[0].File.Path)
	require.Equal(t, 0, len(oldFiles))

	// Delete bar
	_, err = c.StartCommit(repo, "master")
	require.NoError(t, err)
	require.NoError(t, c.DeleteFile(repo, "master", "bar"))

	newFiles, oldFiles, err = c.DiffFile(repo, "master", "", "", "", "", false)
	require.NoError(t, err)
	require.Equal(t, 0, len(newFiles))
	require.Equal(t, 1, len(oldFiles))
	require.Equal(t, "bar", oldFiles[0].File.Path)

	require.NoError(t, c.FinishCommit(repo, "master"))

	newFiles, oldFiles, err = c.DiffFile(repo, "master", "", "", "", "", false)
	require.NoError(t, err)
	require.Equal(t, 0, len(newFiles))
	require.Equal(t, 1, len(oldFiles))
	require.Equal(t, "bar", oldFiles[0].File.Path)

	// Write dir/fizz and dir/buzz
	_, err = c.StartCommit(repo, "master")
	require.NoError(t, err)
	_, err = c.PutFile(repo, "master", "dir/fizz", strings.NewReader("fizz\n"))
	require.NoError(t, err)
	_, err = c.PutFile(repo, "master", "dir/buzz", strings.NewReader("buzz\n"))
	require.NoError(t, err)

	newFiles, oldFiles, err = c.DiffFile(repo, "master", "", "", "", "", false)
	require.NoError(t, err)
	require.Equal(t, 2, len(newFiles))
	require.Equal(t, 0, len(oldFiles))

	require.NoError(t, c.FinishCommit(repo, "master"))

	newFiles, oldFiles, err = c.DiffFile(repo, "master", "", "", "", "", false)
	require.NoError(t, err)
	require.Equal(t, 2, len(newFiles))
	require.Equal(t, 0, len(oldFiles))

	// Modify dir/fizz
	_, err = c.StartCommit(repo, "master")
	require.NoError(t, err)
	_, err = c.PutFile(repo, "master", "dir/fizz", strings.NewReader("fizz\n"))
	require.NoError(t, err)

	newFiles, oldFiles, err = c.DiffFile(repo, "master", "", "", "", "", false)
	require.NoError(t, err)
	require.Equal(t, 1, len(newFiles))
	require.Equal(t, "dir/fizz", newFiles[0].File.Path)
	require.Equal(t, 1, len(oldFiles))
	require.Equal(t, "dir/fizz", oldFiles[0].File.Path)

	require.NoError(t, c.FinishCommit(repo, "master"))

	newFiles, oldFiles, err = c.DiffFile(repo, "master", "", "", "", "", false)
	require.NoError(t, err)
	require.Equal(t, 1, len(newFiles))
	require.Equal(t, "dir/fizz", newFiles[0].File.Path)
	require.Equal(t, 1, len(oldFiles))
	require.Equal(t, "dir/fizz", oldFiles[0].File.Path)
}

func TestGlobFile(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := GetPachClient(t)
	repo := tu.UniqueString("TestGlobFile")
	require.NoError(t, c.CreateRepo(repo))

	// Write foo
	numFiles := 100
	_, err := c.StartCommit(repo, "master")
	require.NoError(t, err)
	for i := 0; i < numFiles; i++ {
		_, err = c.PutFile(repo, "master", fmt.Sprintf("file%d", i), strings.NewReader("1"))
		require.NoError(t, err)
		_, err = c.PutFile(repo, "master", fmt.Sprintf("dir1/file%d", i), strings.NewReader("2"))
		require.NoError(t, err)
		_, err = c.PutFile(repo, "master", fmt.Sprintf("dir2/dir3/file%d", i), strings.NewReader("3"))
		require.NoError(t, err)
	}

	fileInfos, err := c.GlobFile(repo, "master", "*")
	require.NoError(t, err)
	require.Equal(t, numFiles+2, len(fileInfos))
	fileInfos, err = c.GlobFile(repo, "master", "file*")
	require.NoError(t, err)
	require.Equal(t, numFiles, len(fileInfos))
	fileInfos, err = c.GlobFile(repo, "master", "dir1/*")
	require.NoError(t, err)
	require.Equal(t, numFiles, len(fileInfos))
	fileInfos, err = c.GlobFile(repo, "master", "/non-existent-glob*")
	require.NoError(t, err)
	require.Equal(t, 0, len(fileInfos))
	fileInfos, err = c.GlobFile(repo, "master", "/non-existent-file")
	require.NoError(t, err)
	require.Equal(t, 0, len(fileInfos))

	fileInfos, err = c.GlobFile(repo, "master", "dir2/dir3/*")
	require.NoError(t, err)
	require.Equal(t, numFiles, len(fileInfos))
	fileInfos, err = c.GlobFile(repo, "master", "*/*")
	require.NoError(t, err)
	require.Equal(t, numFiles+1, len(fileInfos))

	require.NoError(t, c.FinishCommit(repo, "master"))

	fileInfos, err = c.GlobFile(repo, "master", "*")
	require.NoError(t, err)
	require.Equal(t, numFiles+2, len(fileInfos))
	fileInfos, err = c.GlobFile(repo, "master", "file*")
	require.NoError(t, err)
	require.Equal(t, numFiles, len(fileInfos))
	fileInfos, err = c.GlobFile(repo, "master", "dir1/*")
	require.NoError(t, err)
	require.Equal(t, numFiles, len(fileInfos))
	fileInfos, err = c.GlobFile(repo, "master", "dir2/dir3/*")
	require.NoError(t, err)
	require.Equal(t, numFiles, len(fileInfos))
	fileInfos, err = c.GlobFile(repo, "master", "*/*")
	require.NoError(t, err)
	require.Equal(t, numFiles+1, len(fileInfos))

	// Test file glob
	fileInfos, err = c.ListFile(repo, "master", "*")
	require.NoError(t, err)
	require.Equal(t, numFiles*2+1, len(fileInfos))

	fileInfos, err = c.ListFile(repo, "master", "dir2/dir3/file1?")
	require.NoError(t, err)
	require.Equal(t, 10, len(fileInfos))

	fileInfos, err = c.ListFile(repo, "master", "dir?/*")
	require.NoError(t, err)
	require.Equal(t, numFiles*2, len(fileInfos))

	var output strings.Builder
	err = c.GetFile(repo, "master", "*", 0, 0, &output)
	require.Equal(t, numFiles, len(output.String()))

	output = strings.Builder{}
	err = c.GetFile(repo, "master", "dir2/dir3/file1?", 0, 0, &output)
	require.Equal(t, 10, len(output.String()))

	output = strings.Builder{}
	err = c.GetFile(repo, "master", "**file1?", 0, 0, &output)
	require.Equal(t, 30, len(output.String()))

	output = strings.Builder{}
	err = c.GetFile(repo, "master", "**file1", 0, 0, &output)
	require.True(t, strings.Contains(output.String(), "1"))
	require.True(t, strings.Contains(output.String(), "2"))
	require.True(t, strings.Contains(output.String(), "3"))

	output = strings.Builder{}
	err = c.GetFile(repo, "master", "**file1", 1, 1, &output)
	match, err := regexp.Match("[123]", []byte(output.String()))
	require.NoError(t, err)
	require.True(t, match)

	output = strings.Builder{}
	err = c.GetFile(repo, "master", "dir?", 0, 0, &output)
	require.NoError(t, err)

	output = strings.Builder{}
	err = c.GetFile(repo, "master", "", 0, 0, &output)
	require.NoError(t, err)

	output = strings.Builder{}
	err = c.GetFile(repo, "master", "garbage", 0, 0, &output)
	require.YesError(t, err)

	_, err = c.StartCommit(repo, "master")
	require.NoError(t, err)

	err = c.DeleteFile(repo, "master", "dir2/dir3/*")
	require.NoError(t, err)
	fileInfos, err = c.GlobFile(repo, "master", "**")
	require.NoError(t, err)
	require.Equal(t, numFiles*2+3, len(fileInfos))
	err = c.DeleteFile(repo, "master", "dir?/*")
	require.NoError(t, err)
	fileInfos, err = c.GlobFile(repo, "master", "**")
	require.NoError(t, err)
	require.Equal(t, numFiles+2, len(fileInfos))
	err = c.DeleteFile(repo, "master", "/")
	require.NoError(t, err)
	fileInfos, err = c.GlobFile(repo, "master", "**")
	require.NoError(t, err)
	require.Equal(t, 0, len(fileInfos))

	require.NoError(t, c.FinishCommit(repo, "master"))

	fileInfos, err = c.GlobFile(repo, "master", "**")
	require.NoError(t, err)
	require.Equal(t, 0, len(fileInfos))
}

func TestGlobFileF(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := GetPachClient(t)
	repo := tu.UniqueString("TestGlobFileF")
	require.NoError(t, c.CreateRepo(repo))

	_, err := c.StartCommit(repo, "master")
	require.NoError(t, err)
	expectedFileNames := []string{}
	for i := 0; i < 100; i++ {
		filename := fmt.Sprintf("/%d", i)
		_, err = c.PutFile(repo, "master", filename, strings.NewReader(filename))
		require.NoError(t, err)

		if strings.HasPrefix(filename, "/1") {
			expectedFileNames = append(expectedFileNames, filename)
		}
	}
	require.NoError(t, c.FinishCommit(repo, "master"))

	actualFileNames := []string{}
	err = c.GlobFileF(repo, "master", "/1*", func(fileInfo *pfs.FileInfo) error {
		actualFileNames = append(actualFileNames, fileInfo.File.Path)
		return nil
	})
	require.NoError(t, err)

	sort.Strings(expectedFileNames)
	sort.Strings(actualFileNames)
	require.Equal(t, expectedFileNames, actualFileNames)
}

// TestGetFileGlobOrder checks that GetFile(glob) streams data back in the
// right order. GetFile(glob) is supposed to return a stream of data of the
// form file1 + file2 + .. + fileN, where file1 is the lexicographically lowest
// file matching 'glob', file2 is the next lowest, etc.
func TestGetFileGlobOrder(t *testing.T) {
	c := GetPachClient(t)
	// create repos
	repo := tu.UniqueString("TestGetFileGlobOrder")
	require.NoError(t, c.CreateRepo(repo))

	var expected bytes.Buffer
	commit, err := c.StartCommit(repo, "master")
	require.NoError(t, err)
	for i := 0; i < 25; i++ {
		next := fmt.Sprintf("%d,%d,%d,%d\n", 4*i, (4*i)+1, (4*i)+2, (4*i)+3)
		expected.WriteString(next)
		c.PutFile(repo, commit.ID, fmt.Sprintf("/data/%010d", i), strings.NewReader(next))
	}
	require.NoError(t, c.FinishCommit(repo, commit.ID))

	var output bytes.Buffer
	require.NoError(t, c.GetFile(repo, "master", "/data/*", 0, 0, &output))
	require.Equal(t, expected.String(), output.String())
}

func TestApplyWriteOrder(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := GetPachClient(t)
	repo := tu.UniqueString("TestApplyWriteOrder")
	require.NoError(t, c.CreateRepo(repo))

	// Test that fails when records are applied in lexicographic order
	// rather than mod revision order.
	_, err := c.StartCommit(repo, "master")
	require.NoError(t, err)
	_, err = c.PutFile(repo, "master", "/file", strings.NewReader(""))
	require.NoError(t, err)
	err = c.DeleteFile(repo, "master", "/")
	require.NoError(t, err)
	require.NoError(t, c.FinishCommit(repo, "master"))
	fileInfos, err := c.GlobFile(repo, "master", "**")
	require.NoError(t, err)
	require.Equal(t, 0, len(fileInfos))
}

func TestOverwrite(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := GetPachClient(t)
	repo := tu.UniqueString("TestGlob")
	require.NoError(t, c.CreateRepo(repo))

	// Write foo
	_, err := c.StartCommit(repo, "master")
	require.NoError(t, err)
	_, err = c.PutFile(repo, "master", "file1", strings.NewReader("foo"))
	_, err = c.PutFileSplit(repo, "master", "file2", pfs.Delimiter_LINE, 0, 0, 0, false, strings.NewReader("foo\nbar\nbuz\n"))
	require.NoError(t, err)
	_, err = c.PutFileSplit(repo, "master", "file3", pfs.Delimiter_LINE, 0, 0, 0, false, strings.NewReader("foo\nbar\nbuz\n"))
	require.NoError(t, err)
	require.NoError(t, c.FinishCommit(repo, "master"))
	_, err = c.StartCommit(repo, "master")
	require.NoError(t, err)
	_, err = c.PutFileOverwrite(repo, "master", "file1", strings.NewReader("bar"), 0)
	require.NoError(t, err)
	_, err = c.PutFileOverwrite(repo, "master", "file2", strings.NewReader("buzz"), 0)
	require.NoError(t, err)
	_, err = c.PutFileSplit(repo, "master", "file3", pfs.Delimiter_LINE, 0, 0, 0, true, strings.NewReader("0\n1\n2\n"))
	require.NoError(t, err)
	require.NoError(t, c.FinishCommit(repo, "master"))
	var buffer bytes.Buffer
	require.NoError(t, c.GetFile(repo, "master", "file1", 0, 0, &buffer))
	require.Equal(t, "bar", buffer.String())
	buffer.Reset()
	require.NoError(t, c.GetFile(repo, "master", "file2", 0, 0, &buffer))
	require.Equal(t, "buzz", buffer.String())
	fileInfos, err := c.ListFile(repo, "master", "file3")
	require.NoError(t, err)
	require.Equal(t, 3, len(fileInfos))
	for i := 0; i < 3; i++ {
		buffer.Reset()
		require.NoError(t, c.GetFile(repo, "master", fmt.Sprintf("file3/%016x", i), 0, 0, &buffer))
		require.Equal(t, fmt.Sprintf("%d\n", i), buffer.String())
	}
}

func TestCopyFile(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := GetPachClient(t)
	repo := tu.UniqueString("TestCopyFile")
	require.NoError(t, c.CreateRepo(repo))
	_, err := c.StartCommit(repo, "master")
	require.NoError(t, err)
	numFiles := 5
	for i := 0; i < numFiles; i++ {
		_, err = c.PutFile(repo, "master", fmt.Sprintf("files/%d", i), strings.NewReader(fmt.Sprintf("foo %d\n", i)))
		require.NoError(t, err)
	}
	require.NoError(t, c.FinishCommit(repo, "master"))
	_, err = c.StartCommit(repo, "other")
	require.NoError(t, err)
	require.NoError(t, c.CopyFile(repo, "master", "files", repo, "other", "files", false))
	require.NoError(t, c.CopyFile(repo, "master", "files/0", repo, "other", "file0", false))
	require.NoError(t, c.FinishCommit(repo, "other"))
	for i := 0; i < numFiles; i++ {
		var b bytes.Buffer
		require.NoError(t, c.GetFile(repo, "other", fmt.Sprintf("files/%d", i), 0, 0, &b))
		require.Equal(t, fmt.Sprintf("foo %d\n", i), b.String())
	}
	var b bytes.Buffer
	require.NoError(t, c.GetFile(repo, "other", "file0", 0, 0, &b))
	require.Equal(t, "foo 0\n", b.String())
	_, err = c.StartCommit(repo, "other")
	require.NoError(t, c.CopyFile(repo, "other", "files/0", repo, "other", "files", true))
	require.NoError(t, c.FinishCommit(repo, "other"))
	b.Reset()
	require.NoError(t, c.GetFile(repo, "other", "files", 0, 0, &b))
	require.Equal(t, "foo 0\n", b.String())
}

func TestCopyFileHeaderFooter(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := GetPachClient(t)
	repo := tu.UniqueString("TestCopyFileHeaderFooterOne")
	require.NoError(t, c.CreateRepo(repo))
	_, err := c.PutFileSplit(repo, "master", "/data", pfs.Delimiter_CSV, 0, 0, 1, false,
		strings.NewReader("A,B,C,D\n"+
			"this,is,a,test\n"+
			"this,is,another,test\n"))

	// 1) Try copying the header/footer dir into an open commit
	_, err = c.StartCommit(repo, "target-branch-unfinished")
	require.NoError(t, err)
	require.NoError(t, c.CopyFile(repo, "master", "/data", repo, "target-branch-unfinished", "/data", false))
	require.NoError(t, c.FinishCommit(repo, "target-branch-unfinished"))

	fs, _ := c.ListFile(repo, "target-branch-unfinished", "/*")
	require.ElementsEqualUnderFn(t,
		[]string{"/data/0000000000000000", "/data/0000000000000001"},
		fs,
		func(fii interface{}) interface{} {
			fi := fii.(*pfs.FileInfo)
			return fi.File.Path
		})

	// In target branch, header should be preserved--should be returned by
	// GetFile, and should appear exactly once at the beginning
	var contents bytes.Buffer
	c.GetFile(repo, "target-branch-unfinished", "/data/0000000000000000", 0, 0, &contents)
	require.Equal(t, "A,B,C,D\nthis,is,a,test\n", contents.String())
	contents.Reset()
	c.GetFile(repo, "target-branch-unfinished", "/data/0000000000000001", 0, 0, &contents)
	require.Equal(t, "A,B,C,D\nthis,is,another,test\n",
		contents.String())
	// Confirm header only appears once in target-branch, even though the
	// contents of two files are concatenated and returned
	contents.Reset()
	c.GetFile(repo, "target-branch-unfinished", "/data/*", 0, 0, &contents)
	require.Equal(t, "A,B,C,D\nthis,is,a,test\nthis,is,another,test\n",
		contents.String())

	// 2) Try copying the header/footer dir into a branch
	err = c.CreateBranch(repo, "target-branch-finished", "", nil)
	require.NoError(t, err)
	require.NoError(t, c.CopyFile(repo, "master", "/data", repo, "target-branch-finished", "/data", false))

	fs, _ = c.ListFile(repo, "target-branch-finished", "/*")
	require.ElementsEqualUnderFn(t,
		[]string{"/data/0000000000000000", "/data/0000000000000001"},
		fs, FileInfoToPath)

	// In target branch, header should be preserved--should be returned by
	// GetFile, and should appear exactly once at the beginning
	contents.Reset()
	c.GetFile(repo, "target-branch-finished", "/data/0000000000000000", 0, 0, &contents)
	require.Equal(t, "A,B,C,D\nthis,is,a,test\n", contents.String())
	contents.Reset()
	c.GetFile(repo, "target-branch-finished", "/data/0000000000000001", 0, 0, &contents)
	require.Equal(t, "A,B,C,D\nthis,is,another,test\n",
		contents.String())
	// Confirm header only appears once in target-branch, even though the
	// contents of two files are concatenated and returned
	contents.Reset()
	c.GetFile(repo, "target-branch-finished", "/data/*", 0, 0, &contents)
	require.Equal(t, "A,B,C,D\nthis,is,a,test\nthis,is,another,test\n",
		contents.String())
}

func TestBuildCommit(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := GetPachClient(t)
	repo := tu.UniqueString("TestBuildCommit")
	require.NoError(t, c.CreateRepo(repo))

	tree1, err := hashtree.NewDBHashTree("")
	require.NoError(t, err)
	fooObj, fooSize, err := c.PutObject(strings.NewReader("foo\n"))
	require.NoError(t, err)
	require.NoError(t, tree1.PutFile("foo", []*pfs.Object{fooObj}, fooSize))
	require.NoError(t, tree1.Hash())
	tree1Obj, err := hashtree.PutHashTree(c, tree1)
	_, err = c.BuildCommit(repo, "master", "", tree1Obj.Hash)
	require.NoError(t, err)
	repoInfo, err := c.InspectRepo(repo)
	require.NoError(t, err)
	require.Equal(t, uint64(fooSize), repoInfo.SizeBytes)
	commitInfo, err := c.InspectCommit(repo, "master")
	require.NoError(t, err)
	require.Equal(t, uint64(fooSize), commitInfo.SizeBytes)

	barObj, barSize, err := c.PutObject(strings.NewReader("bar\n"))
	require.NoError(t, err)
	require.NoError(t, tree1.PutFile("bar", []*pfs.Object{barObj}, barSize))
	require.NoError(t, tree1.Hash())
	tree2Obj, err := hashtree.PutHashTree(c, tree1)
	_, err = c.BuildCommit(repo, "master", "", tree2Obj.Hash)
	require.NoError(t, err)
	repoInfo, err = c.InspectRepo(repo)
	require.NoError(t, err)
	require.Equal(t, uint64(fooSize+barSize), repoInfo.SizeBytes)
	commitInfo, err = c.InspectCommit(repo, "master")
	require.NoError(t, err)
	require.Equal(t, uint64(fooSize+barSize), commitInfo.SizeBytes)
}

func TestPropagateCommit(t *testing.T) {
	c := GetPachClient(t)
	repo1 := tu.UniqueString("TestPropagateCommit1")
	require.NoError(t, c.CreateRepo(repo1))
	repo2 := tu.UniqueString("TestPropagateCommit2")
	require.NoError(t, c.CreateRepo(repo2))
	require.NoError(t, c.CreateBranch(repo2, "master", "", []*pfs.Branch{pclient.NewBranch(repo1, "master")}))
	commit, err := c.StartCommit(repo1, "master")
	require.NoError(t, err)
	require.NoError(t, c.FinishCommit(repo1, commit.ID))
	commits, err := c.ListCommitByRepo(repo2)
	require.NoError(t, err)
	require.Equal(t, 1, len(commits))
}

// TestBackfillBranch implements the following DAG:
//
// A ──▶ C
//  ╲   ◀
//   ╲ ╱
//    ╳
//   ╱ ╲
// 	╱   ◀
// B ──▶ D
func TestBackfillBranch(t *testing.T) {
	c := GetPachClient(t)
	require.NoError(t, c.CreateRepo("A"))
	require.NoError(t, c.CreateRepo("B"))
	require.NoError(t, c.CreateRepo("C"))
	require.NoError(t, c.CreateRepo("D"))
	require.NoError(t, c.CreateBranch("C", "master", "", []*pfs.Branch{pclient.NewBranch("A", "master"), pclient.NewBranch("B", "master")}))
	_, err := c.StartCommit("A", "master")
	require.NoError(t, err)
	require.NoError(t, c.FinishCommit("A", "master"))
	require.NoError(t, c.FinishCommit("C", "master"))
	_, err = c.StartCommit("B", "master")
	require.NoError(t, err)
	require.NoError(t, c.FinishCommit("B", "master"))
	require.NoError(t, c.FinishCommit("C", "master"))
	commits, err := c.ListCommitByRepo("C")
	require.NoError(t, err)
	require.Equal(t, 2, len(commits))

	// Create a branch in D, it should receive a single commit for the heads of `A` and `B`.
	require.NoError(t, c.CreateBranch("D", "master", "", []*pfs.Branch{pclient.NewBranch("A", "master"), pclient.NewBranch("B", "master")}))
	commits, err = c.ListCommitByRepo("D")
	require.NoError(t, err)
	require.Equal(t, 1, len(commits))
}

// TestUpdateBranch tests the following DAG:
//
// A ─▶ B ─▶ C
//
// Then updates it to:
//
// A ─▶ B ─▶ C
//      ▲
// D ───╯
//
func TestUpdateBranch(t *testing.T) {
	c := GetPachClient(t)
	require.NoError(t, c.CreateRepo("A"))
	require.NoError(t, c.CreateRepo("B"))
	require.NoError(t, c.CreateRepo("C"))
	require.NoError(t, c.CreateRepo("D"))
	require.NoError(t, c.CreateBranch("B", "master", "", []*pfs.Branch{pclient.NewBranch("A", "master")}))
	require.NoError(t, c.CreateBranch("C", "master", "", []*pfs.Branch{pclient.NewBranch("B", "master")}))
	_, err := c.StartCommit("A", "master")
	require.NoError(t, err)
	require.NoError(t, c.FinishCommit("A", "master"))
	require.NoError(t, c.FinishCommit("B", "master"))
	require.NoError(t, c.FinishCommit("C", "master"))

	_, err = c.StartCommit("D", "master")
	require.NoError(t, err)
	require.NoError(t, c.FinishCommit("D", "master"))

	require.NoError(t, c.CreateBranch("B", "master", "", []*pfs.Branch{pclient.NewBranch("A", "master"), pclient.NewBranch("D", "master")}))
	require.NoError(t, c.FinishCommit("B", "master"))
	require.NoError(t, c.FinishCommit("C", "master"))
	cCommitInfo, err := c.InspectCommit("C", "master")
	require.NoError(t, err)
	require.Equal(t, 3, len(cCommitInfo.Provenance))
}

func TestBranchProvenance(t *testing.T) {
	c := GetPachClient(t)
	tests := [][]struct {
		name       string
		directProv []string
		err        bool
		expectProv map[string][]string
		expectSubv map[string][]string
	}{{
		{name: "A"},
		{name: "B", directProv: []string{"A"}},
		{name: "C", directProv: []string{"B"}},
		{name: "D", directProv: []string{"C", "A"},
			expectProv: map[string][]string{"A": nil, "B": {"A"}, "C": {"B", "A"}, "D": {"A", "B", "C"}},
			expectSubv: map[string][]string{"A": {"B", "C", "D"}, "B": {"C", "D"}, "C": {"D"}, "D": {}}},
		// A ─▶ B ─▶ C ─▶ D
		// ╰─────────────⬏
		{name: "B",
			expectProv: map[string][]string{"A": {}, "B": {}, "C": {"B"}, "D": {"A", "B", "C"}},
			expectSubv: map[string][]string{"A": {"D"}, "B": {"C", "D"}, "C": {"D"}, "D": {}}},
		// A    B ─▶ C ─▶ D
		// ╰─────────────⬏
	}, {
		{name: "A"},
		{name: "B", directProv: []string{"A"}},
		{name: "C", directProv: []string{"A", "B"}},
		{name: "D", directProv: []string{"C"},
			expectProv: map[string][]string{"A": {}, "B": {"A"}, "C": {"A", "B"}, "D": {"A", "B", "C"}},
			expectSubv: map[string][]string{"A": {"B", "C", "D"}, "B": {"C", "D"}, "C": {"D"}, "D": {}}},
		// A ─▶ B ─▶ C ─▶ D
		// ╰────────⬏
		{name: "C", directProv: []string{"B"},
			expectProv: map[string][]string{"A": {}, "B": {"A"}, "C": {"A", "B"}, "D": {"A", "B", "C"}},
			expectSubv: map[string][]string{"A": {"B", "C", "D"}, "B": {"C", "D"}, "C": {"D"}, "D": {}}},
		// A ─▶ B ─▶ C ─▶ D
	}, {
		{name: "A"},
		{name: "B"},
		{name: "C", directProv: []string{"A", "B"}},
		{name: "D", directProv: []string{"C"}},
		{name: "E", directProv: []string{"A", "D"},
			expectProv: map[string][]string{"A": {}, "B": {}, "C": {"A", "B"}, "D": {"A", "B", "C"}, "E": {"A", "B", "C", "D"}},
			expectSubv: map[string][]string{"A": {"C", "D", "E"}, "B": {"C", "D", "E"}, "C": {"D", "E"}, "D": {"E"}, "E": {}}},
		// A    B ─▶ C ─▶ D ─▶ E
		// ├────────⬏          ▲
		// ╰───────────────────╯
		{name: "C", directProv: []string{"B"},
			expectProv: map[string][]string{"A": {}, "B": {}, "C": {"B"}, "D": {"B", "C"}, "E": {"A", "B", "C", "D"}},
			expectSubv: map[string][]string{"A": {"E"}, "B": {"C", "D", "E"}, "C": {"D", "E"}, "D": {"E"}, "E": {}}},
		// A    B ─▶ C ─▶ D ─▶ E
		// ╰──────────────────⬏
	}, {
		{name: "A", directProv: []string{"A"}, err: true},
		{name: "A"},
		{name: "A", directProv: []string{"A"}, err: true},
		{name: "B", directProv: []string{"A"}},
		{name: "A", directProv: []string{"B"}, err: true},
	},
	}
	for i, test := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			repo := tu.UniqueString("repo")
			require.NoError(t, c.CreateRepo(repo))
			for iStep, step := range test {
				var provenance []*pfs.Branch
				for _, branch := range step.directProv {
					provenance = append(provenance, pclient.NewBranch(repo, branch))
				}
				err := c.CreateBranch(repo, step.name, "", provenance)
				if step.err {
					require.YesError(t, err, "%d> CreateBranch(\"%s\", %v)", iStep, step.name, step.directProv)
				} else {
					require.NoError(t, err, "%d> CreateBranch(\"%s\", %v)", iStep, step.name, step.directProv)
				}
				for branch, expectedProv := range step.expectProv {
					bi, err := c.InspectBranch(repo, branch)
					require.NoError(t, err)
					sort.Strings(expectedProv)
					require.Equal(t, len(expectedProv), len(bi.Provenance))
					for _, b := range bi.Provenance {
						i := sort.SearchStrings(expectedProv, b.Name)
						if i >= len(expectedProv) || expectedProv[i] != b.Name {
							t.Fatalf("provenance for %s contains: %s, but should only contain: %v", branch, b.Name, expectedProv)
						}
					}
				}
				for branch, expectedSubv := range step.expectSubv {
					bi, err := c.InspectBranch(repo, branch)
					require.NoError(t, err)
					sort.Strings(expectedSubv)
					require.Equal(t, len(expectedSubv), len(bi.Subvenance))
					for _, b := range bi.Subvenance {
						i := sort.SearchStrings(expectedSubv, b.Name)
						if i >= len(expectedSubv) || expectedSubv[i] != b.Name {
							t.Fatalf("subvenance for %s contains: %s, but should only contain: %v", branch, b.Name, expectedSubv)
						}
					}
				}
			}
		})
	}
	// t.Run("1", func(t *testing.T) {
	// 	c := getPachClient(t)
	// 	require.NoError(t, c.CreateRepo("A"))
	// 	require.NoError(t, c.CreateRepo("B"))
	// 	require.NoError(t, c.CreateRepo("C"))
	// 	require.NoError(t, c.CreateRepo("D"))

	// 	require.NoError(t, c.CreateBranch("B", "master", "", []*pfs.Branch{pclient.NewBranch("A", "master")}))
	// 	require.NoError(t, c.CreateBranch("C", "master", "", []*pfs.Branch{pclient.NewBranch("B", "master")}))
	// 	require.NoError(t, c.CreateBranch("D", "master", "", []*pfs.Branch{pclient.NewBranch("C", "master"), pclient.NewBranch("A", "master")}))

	// 	aMaster, err := c.InspectBranch("A", "master")
	// 	require.NoError(t, err)
	// 	require.Equal(t, 3, len(aMaster.Subvenance))

	// 	cMaster, err := c.InspectBranch("C", "master")
	// 	require.NoError(t, err)
	// 	require.Equal(t, 2, len(cMaster.Provenance))

	// 	dMaster, err := c.InspectBranch("D", "master")
	// 	require.NoError(t, err)
	// 	require.Equal(t, 3, len(dMaster.Provenance))

	// 	require.NoError(t, c.CreateBranch("B", "master", "", nil))

	// 	aMaster, err = c.InspectBranch("A", "master")
	// 	require.NoError(t, err)
	// 	require.Equal(t, 1, len(aMaster.Subvenance))

	// 	cMaster, err = c.InspectBranch("C", "master")
	// 	require.NoError(t, err)
	// 	require.Equal(t, 1, len(cMaster.Provenance))

	// 	dMaster, err = c.InspectBranch("D", "master")
	// 	require.NoError(t, err)
	// 	require.Equal(t, 3, len(dMaster.Provenance))
	// })
	// t.Run("2", func(t *testing.T) {
	// 	c := getPachClient(t)
	// 	require.NoError(t, c.CreateRepo("A"))
	// 	require.NoError(t, c.CreateRepo("B"))
	// 	require.NoError(t, c.CreateRepo("C"))
	// 	require.NoError(t, c.CreateRepo("D"))

	// 	require.NoError(t, c.CreateBranch("B", "master", "", []*pfs.Branch{pclient.NewBranch("A", "master")}))
	// 	require.NoError(t, c.CreateBranch("C", "master", "", []*pfs.Branch{pclient.NewBranch("B", "master"), pclient.NewBranch("A", "master")}))
	// 	require.NoError(t, c.CreateBranch("D", "master", "", []*pfs.Branch{pclient.NewBranch("C", "master")}))
	// })
}

func TestChildCommits(t *testing.T) {
	c := GetPachClient(t)
	require.NoError(t, c.CreateRepo("A"))
	require.NoError(t, c.CreateBranch("A", "master", "", nil))

	// Small helper function wrapping c.InspectCommit, because it's called a lot
	inspect := func(repo, commit string) *pfs.CommitInfo {
		commitInfo, err := c.InspectCommit(repo, commit)
		require.NoError(t, err)
		return commitInfo
	}

	commit1, err := c.StartCommit("A", "master")
	require.NoError(t, err)
	commits, err := c.ListCommit("A", "master", "", 0)
	t.Logf("%v", commits)
	require.NoError(t, c.FinishCommit("A", "master"))

	commit2, err := c.StartCommit("A", "master")
	require.NoError(t, err)

	// Inspect commit 1 and 2
	commit1Info, commit2Info := inspect("A", commit1.ID), inspect("A", commit2.ID)
	require.Equal(t, commit1.ID, commit2Info.ParentCommit.ID)
	require.ElementsEqualUnderFn(t, []string{commit2.ID}, commit1Info.ChildCommits, CommitToID)

	// Delete commit 2 and make sure it's removed from commit1.ChildCommits
	require.NoError(t, c.DeleteCommit("A", commit2.ID))
	commit1Info = inspect("A", commit1.ID)
	require.ElementsEqualUnderFn(t, nil, commit1Info.ChildCommits, CommitToID)

	// Re-create commit2, and create a third commit also extending from commit1.
	// Make sure both appear in commit1.children
	commit2, err = c.StartCommit("A", "master")
	require.NoError(t, err)
	require.NoError(t, c.FinishCommit("A", commit2.ID))
	commit3, err := c.PfsAPIClient.StartCommit(c.Ctx(), &pfs.StartCommitRequest{
		Parent: pclient.NewCommit("A", commit1.ID),
	})
	require.NoError(t, err)
	commit1Info = inspect("A", commit1.ID)
	require.ElementsEqualUnderFn(t, []string{commit2.ID, commit3.ID}, commit1Info.ChildCommits, CommitToID)

	// Delete commit3 and make sure commit1 has the right children
	require.NoError(t, c.DeleteCommit("A", commit3.ID))
	commit1Info = inspect("A", commit1.ID)
	require.ElementsEqualUnderFn(t, []string{commit2.ID}, commit1Info.ChildCommits, CommitToID)

	// Create a downstream branch in the same repo, then commit to "A" and make
	// sure the new HEAD commit is in the parent's children (i.e. test
	// propagateCommit)
	require.NoError(t, c.CreateBranch("A", "out", "", []*pfs.Branch{
		pclient.NewBranch("A", "master"),
	}))
	outCommit1ID := inspect("A", "out").Commit.ID
	commit3, err = c.StartCommit("A", "master")
	require.NoError(t, err)
	c.FinishCommit("A", commit3.ID)
	// Re-inspect outCommit1, which has been updated by StartCommit
	outCommit1, outCommit2 := inspect("A", outCommit1ID), inspect("A", "out")
	require.Equal(t, outCommit1.Commit.ID, outCommit2.ParentCommit.ID)
	require.ElementsEqualUnderFn(t, []string{outCommit2.Commit.ID}, outCommit1.ChildCommits, CommitToID)

	// create a new branch in a different repo and do the same test again
	require.NoError(t, c.CreateRepo("B"))
	require.NoError(t, c.CreateBranch("B", "master", "", []*pfs.Branch{
		pclient.NewBranch("A", "master"),
	}))
	bCommit1ID := inspect("B", "master").Commit.ID
	commit3, err = c.StartCommit("A", "master")
	require.NoError(t, err)
	c.FinishCommit("A", commit3.ID)
	// Re-inspect bCommit1, which has been updated by StartCommit
	bCommit1, bCommit2 := inspect("B", bCommit1ID), inspect("B", "master")
	require.Equal(t, bCommit1.Commit.ID, bCommit2.ParentCommit.ID)
	require.ElementsEqualUnderFn(t, []string{bCommit2.Commit.ID}, bCommit1.ChildCommits, CommitToID)

	// create a new branch in a different repo, then update it so that two commits
	// are generated. Make sure the second commit is in the parent's children
	require.NoError(t, c.CreateRepo("C"))
	require.NoError(t, c.CreateBranch("C", "master", "", []*pfs.Branch{
		pclient.NewBranch("A", "master"),
	}))
	cCommit1ID := inspect("C", "master").Commit.ID // Get new commit's ID
	require.NoError(t, c.CreateBranch("C", "master", "master", []*pfs.Branch{
		pclient.NewBranch("A", "master"),
		pclient.NewBranch("B", "master"),
	}))
	// Re-inspect cCommit1, which has been updated by CreateBranch
	cCommit1, cCommit2 := inspect("C", cCommit1ID), inspect("C", "master")
	require.Equal(t, cCommit1.Commit.ID, cCommit2.ParentCommit.ID)
	require.ElementsEqualUnderFn(t, []string{cCommit2.Commit.ID}, cCommit1.ChildCommits, CommitToID)
}

func TestStartCommitFork(t *testing.T) {
	c := GetPachClient(t)
	require.NoError(t, c.CreateRepo("A"))
	require.NoError(t, c.CreateBranch("A", "master", "", nil))
	commit, err := c.StartCommit("A", "master")
	require.NoError(t, err)
	c.FinishCommit("A", commit.ID)
	commit2, err := c.PfsAPIClient.StartCommit(c.Ctx(), &pfs.StartCommitRequest{
		Branch: "master2",
		Parent: pclient.NewCommit("A", "master"),
	})
	require.NoError(t, err)
	require.NoError(t, c.FinishCommit("A", commit2.ID))

	commits, err := c.ListCommit("A", "master2", "", 0)
	require.NoError(t, err)
	require.ElementsEqualUnderFn(t, []string{commit.ID, commit2.ID}, commits, CommitInfoToID)
}

// TestUpdateBranchNewOutputCommit tests the following corner case:
// A ──▶ C
// B
//
// Becomes:
//
// A  ╭▶ C
// B ─╯
//
// C should create a new output commit to process its unprocessed inputs in B
func TestUpdateBranchNewOutputCommit(t *testing.T) {
	c := GetPachClient(t)
	require.NoError(t, c.CreateRepo("A"))
	require.NoError(t, c.CreateRepo("B"))
	require.NoError(t, c.CreateRepo("C"))
	require.NoError(t, c.CreateBranch("A", "master", "", nil))
	require.NoError(t, c.CreateBranch("B", "master", "", nil))
	require.NoError(t, c.CreateBranch("C", "master", "",
		[]*pfs.Branch{pclient.NewBranch("A", "master")}))

	// Create commits in A and B
	commit, err := c.StartCommit("A", "master")
	require.NoError(t, err)
	c.FinishCommit("A", commit.ID)
	commit, err = c.StartCommit("B", "master")
	require.NoError(t, err)
	c.FinishCommit("A", commit.ID)

	// Check for first output commit in C
	commits, err := c.ListCommit("C", "master", "", 0)
	require.NoError(t, err)
	require.Equal(t, 1, len(commits))

	// Update the provenance of C/master and make sure it creates a new commit
	require.NoError(t, c.CreateBranch("C", "master", "master",
		[]*pfs.Branch{pclient.NewBranch("B", "master")}))
	commits, err = c.ListCommit("C", "master", "", 0)
	require.NoError(t, err)
	require.Equal(t, 2, len(commits))
}

// TestDeleteCommitBigSubvenance deletes a commit that is upstream of a large
// stack of pipeline outputs and makes sure that parenthood and such are handled
// correctly.
// DAG (dots are commits):
//  schema:
//   ...   ─────╮
//              │  pipeline:
//  logs:       ├─▶ .............
//   .......... ╯
// Tests:
//   there are four cases tested here, in this order (b/c easy setup)
// 1. Delete parent commit -> child rewritten to point to a live commit
// 2. Delete branch HEAD   -> output branch rewritten to point to a live commit
// 3. Delete branch HEAD   -> output branch rewritten to point to nil
// 4. Delete parent commit -> child rewritten to point to nil
func TestDeleteCommitBigSubvenance(t *testing.T) {
	c := GetPachClient(t)

	// two input repos, one with many commits (logs), and one with few (schema)
	require.NoError(t, c.CreateRepo("logs"))
	require.NoError(t, c.CreateRepo("schema"))

	// Commit to logs and schema (so that "pipeline" has an initial output commit,
	// and we can check that it updates this initial commit's child appropriately)
	for _, repo := range []string{"schema", "logs"} {
		commit, err := c.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, c.FinishCommit(repo, commit.ID))
	}

	// Create an output branch, in "pipeline"
	require.NoError(t, c.CreateRepo("pipeline"))
	require.NoError(t, c.CreateBranch("pipeline", "master", "", []*pfs.Branch{
		pclient.NewBranch("schema", "master"),
		pclient.NewBranch("logs", "master"),
	}))
	commits, err := c.ListCommit("pipeline", "master", "", 0)
	require.NoError(t, err)
	require.Equal(t, 1, len(commits))

	// Case 1
	// - Commit to "schema", creating a second output commit in 'pipeline' (this
	//   is bigSubvCommit)
	// - Commit to "logs" 10 more times, so that the commit to "schema" has 11
	//   commits in its subvenance
	// - Commit to "schema" again creating a 12th commit in 'pipeline'
	// - Delete bigSubvCommit
	// - Now there are 2 output commits in 'pipeline', and the parent of the first
	//   commit is the second commit (makes sure that the first commit's parent is
	//   rewritten back to the last live commit)
	bigSubvCommit, err := c.StartCommit("schema", "master")
	require.NoError(t, err)
	require.NoError(t, c.FinishCommit("schema", bigSubvCommit.ID))
	for i := 0; i < 10; i++ {
		commit, err := c.StartCommit("logs", "master")
		require.NoError(t, err)
		require.NoError(t, c.FinishCommit("logs", commit.ID))
	}
	commit, err := c.StartCommit("schema", "master")
	require.NoError(t, err)
	require.NoError(t, c.FinishCommit("schema", commit.ID))

	// Make sure there are 13 output commits in 'pipeline' to start (one from
	// creation, one from the second 'schema' commit, 10 from the 'logs' commits,
	// and one more from the third 'schema' commit)
	commits, err = c.ListCommit("pipeline", "master", "", 0)
	require.NoError(t, err)
	require.Equal(t, 13, len(commits))

	require.NoError(t, c.DeleteCommit("schema", bigSubvCommit.ID))

	commits, err = c.ListCommit("pipeline", "master", "", 0)
	require.NoError(t, err)
	require.Equal(t, 2, len(commits))
	require.Equal(t, commits[1].Commit.ID, commits[0].ParentCommit.ID)
	require.Equal(t, commits[1].ChildCommits[0].ID, commits[0].Commit.ID)

	// Case 2
	// - reset bigSubvCommit to be the head commit of 'schema/master'
	// - commit to 'logs' 10 more times
	// - delete bigSubvCommit
	// - Now there should be two commits in 'pipeline':
	//   - One started by DeleteCommit (with provenance schema/master and
	//     logs/masterand
	//   - The oldest commit in 'pipeline', from the setup
	// - The second commit is the parent of the first
	//
	// This makes sure that the branch pipeline/master is rewritten back to
	// the last live commit, and that it creates a new output commit when branches
	// have unprocesed HEAD commits
	commits, err = c.ListCommit("schema", "master", "", 0)
	require.NoError(t, err)
	require.Equal(t, 2, len(commits))
	bigSubvCommitInfo, err := c.InspectCommit("schema", "master")
	require.NoError(t, err)
	bigSubvCommit = bigSubvCommitInfo.Commit
	for i := 0; i < 10; i++ {
		commit, err = c.StartCommit("logs", "master")
		require.NoError(t, err)
		require.NoError(t, c.FinishCommit("logs", commit.ID))
	}

	require.NoError(t, c.DeleteCommit("schema", bigSubvCommit.ID))

	commits, err = c.ListCommit("pipeline", "master", "", 0)
	require.NoError(t, err)
	require.Equal(t, 2, len(commits))
	pipelineMaster, err := c.InspectCommit("pipeline", "master")
	require.NoError(t, err)
	require.Equal(t, pipelineMaster.Commit.ID, commits[0].Commit.ID)
	require.Equal(t, pipelineMaster.ParentCommit.ID, commits[1].Commit.ID)

	// Case 3
	// - reset bigSubvCommit to be the head of 'schema/master' (the only commit)
	// - commit to 'logs' 10 more times
	// - delete bigSubvCommit
	// - Now there shouldn't be any commits in 'pipeline'
	// - Further test: delete all commits in schema and logs, and make sure that
	//   'pipeline/master' still points to nil, as there are no input commits
	commits, err = c.ListCommit("schema", "master", "", 0)
	require.NoError(t, err)
	require.Equal(t, 1, len(commits))
	bigSubvCommit = commits[0].Commit

	for i := 0; i < 10; i++ {
		commit, err = c.StartCommit("logs", "master")
		require.NoError(t, err)
		require.NoError(t, c.FinishCommit("logs", commit.ID))
	}

	require.NoError(t, c.DeleteCommit("schema", bigSubvCommit.ID))

	commits, err = c.ListCommit("pipeline", "master", "", 0)
	require.NoError(t, err)
	require.Equal(t, 0, len(commits))

	// Delete all input commits--DeleteCommit should reset 'pipeline/master' to
	// nil, and should not create a new output commit this time
	commits, err = c.ListCommit("schema", "master", "", 0)
	require.NoError(t, err)
	for _, commitInfo := range commits {
		require.NoError(t, c.DeleteCommit("schema", commitInfo.Commit.ID))
	}
	commits, err = c.ListCommit("logs", "master", "", 0)
	require.NoError(t, err)
	for _, commitInfo := range commits {
		require.NoError(t, c.DeleteCommit("logs", commitInfo.Commit.ID))
	}
	commits, err = c.ListCommit("pipeline", "master", "", 0)
	require.NoError(t, err)
	require.Equal(t, 0, len(commits))
	pipelineMaster, err = c.InspectCommit("pipeline", "master")
	require.YesError(t, err)
	require.Matches(t, "has no head", err.Error())

	// Case 4
	// - commit to 'schema' and reset bigSubvCommit to be the head
	//   (bigSubvCommit is now the only commit in 'schema/master')
	// - Commit to 'logs' 10 more times
	// - Commit to schema again
	// - Delete bigSubvCommit
	// - Now there should be one commit in 'pipeline', and its parent is nil
	// (makes sure that the the commit is rewritten back to 'nil'
	// schema, logs, and pipeline are now all completely empty again
	commits, err = c.ListCommit("schema", "master", "", 0)
	require.NoError(t, err)
	require.Equal(t, 0, len(commits))
	bigSubvCommit, err = c.StartCommit("schema", "master")
	require.NoError(t, err)
	require.NoError(t, c.FinishCommit("schema", bigSubvCommit.ID))
	for i := 0; i < 10; i++ {
		commit, err = c.StartCommit("logs", "master")
		require.NoError(t, err)
		require.NoError(t, c.FinishCommit("logs", commit.ID))
	}
	commit, err = c.StartCommit("schema", "master")
	require.NoError(t, err)
	require.NoError(t, c.FinishCommit("schema", commit.ID))

	require.NoError(t, c.DeleteCommit("schema", bigSubvCommit.ID))

	commits, err = c.ListCommit("pipeline", "master", "", 0)
	require.NoError(t, err)
	require.Equal(t, 1, len(commits))
	pipelineMaster, err = c.InspectCommit("pipeline", "master")
	require.NoError(t, err)
	require.Nil(t, pipelineMaster.ParentCommit)
}

// TestDeleteCommitMultipleChildrenSingleCommit tests that when you have the
// following commit graph in a repo:
// c   d
//  ↘ ↙
//   b
//   ↓
//   a
//
// and you delete commit 'b', what you end up with is:
//
// c   d
//  ↘ ↙
//   a
func TestDeleteCommitMultipleChildrenSingleCommit(t *testing.T) {
	cli := GetPachClient(t)
	require.NoError(t, cli.CreateRepo("repo"))
	require.NoError(t, cli.CreateBranch("repo", "master", "", nil))

	// Create commits 'a' and 'b'
	a, err := cli.StartCommit("repo", "master")
	require.NoError(t, err)
	require.NoError(t, cli.FinishCommit("repo", a.ID))
	b, err := cli.StartCommit("repo", "master")
	require.NoError(t, err)
	require.NoError(t, cli.FinishCommit("repo", b.ID))

	// Create second branch
	require.NoError(t, cli.CreateBranch("repo", "master2", "master", nil))

	// Create commits 'c' and 'd'
	c, err := cli.StartCommit("repo", "master")
	require.NoError(t, err)
	require.NoError(t, cli.FinishCommit("repo", c.ID))
	d, err := cli.StartCommit("repo", "master2")
	require.NoError(t, err)
	require.NoError(t, cli.FinishCommit("repo", d.ID))

	// Collect info re: a, b, c, and d, and make sure that the parent/child
	// relationships are all correct
	aInfo, err := cli.InspectCommit("repo", a.ID)
	bInfo, err := cli.InspectCommit("repo", b.ID)
	cInfo, err := cli.InspectCommit("repo", c.ID)
	dInfo, err := cli.InspectCommit("repo", d.ID)

	require.Nil(t, aInfo.ParentCommit)
	require.ElementsEqualUnderFn(t, []string{b.ID}, aInfo.ChildCommits, CommitToID)

	require.Equal(t, a.ID, bInfo.ParentCommit.ID)
	require.ElementsEqualUnderFn(t, []string{c.ID, d.ID}, bInfo.ChildCommits, CommitToID)

	require.Equal(t, b.ID, cInfo.ParentCommit.ID)
	require.Equal(t, 0, len(cInfo.ChildCommits))

	require.Equal(t, b.ID, dInfo.ParentCommit.ID)
	require.Equal(t, 0, len(dInfo.ChildCommits))

	// Delete commit 'b'
	cli.DeleteCommit("repo", b.ID)

	// Collect info re: a, c, and d, and make sure that the parent/child
	// relationships are still correct
	aInfo, err = cli.InspectCommit("repo", a.ID)
	cInfo, err = cli.InspectCommit("repo", c.ID)
	dInfo, err = cli.InspectCommit("repo", d.ID)

	require.Nil(t, aInfo.ParentCommit)
	require.ElementsEqualUnderFn(t, []string{c.ID, d.ID}, aInfo.ChildCommits, CommitToID)

	require.Equal(t, a.ID, cInfo.ParentCommit.ID)
	require.Equal(t, 0, len(cInfo.ChildCommits))

	require.Equal(t, a.ID, dInfo.ParentCommit.ID)
	require.Equal(t, 0, len(dInfo.ChildCommits))
}

// TestDeleteCommitMultiLevelChildrenNilParent tests that when you have the
// following commit graph in a repo:
//
//    ↙f
//   c
//   ↓↙e
//   b
//   ↓↙d
//   a
//
// and you delete commits 'a', 'b' and 'c' (in a single call), what you end up
// with is:
//
// d e f
//  ↘↓↙
//  nil
func TestDeleteCommitMultiLevelChildrenNilParent(t *testing.T) {
	cli := GetPachClient(t)
	require.NoError(t, cli.CreateRepo("upstream1"))
	require.NoError(t, cli.CreateRepo("upstream2"))
	// commit to both inputs
	_, err := cli.StartCommit("upstream1", "master")
	require.NoError(t, err)
	require.NoError(t, cli.FinishCommit("upstream1", "master"))
	deleteMeCommit, err := cli.StartCommit("upstream2", "master")
	require.NoError(t, err)
	require.NoError(t, cli.FinishCommit("upstream2", "master"))

	// Create main repo (will have the commit graphs above
	require.NoError(t, cli.CreateRepo("repo"))
	require.NoError(t, cli.CreateBranch("repo", "master", "", []*pfs.Branch{
		pclient.NewBranch("upstream1", "master"),
		pclient.NewBranch("upstream2", "master"),
	}))

	// Create commit 'a'
	aInfo, err := cli.InspectCommit("repo", "master")
	require.NoError(t, err)
	a := aInfo.Commit
	require.NoError(t, cli.FinishCommit("repo", a.ID))

	// Create 'd'
	resp, err := cli.PfsAPIClient.StartCommit(cli.Ctx(), &pfs.StartCommitRequest{
		Parent: pclient.NewCommit("repo", a.ID),
	})
	require.NoError(t, err)
	d := pclient.NewCommit("repo", resp.ID)
	require.NoError(t, cli.FinishCommit("repo", resp.ID))

	// Create 'b'
	// (commit to upstream1, so that a & b have same prov commit in upstream2)
	_, err = cli.StartCommit("upstream1", "master")
	require.NoError(t, err)
	require.NoError(t, cli.FinishCommit("upstream1", "master"))
	bInfo, err := cli.InspectCommit("repo", "master")
	require.NoError(t, err)
	b := bInfo.Commit
	require.NoError(t, cli.FinishCommit("repo", b.ID))

	// Create 'e'
	resp, err = cli.PfsAPIClient.StartCommit(cli.Ctx(), &pfs.StartCommitRequest{
		Parent: pclient.NewCommit("repo", b.ID),
	})
	require.NoError(t, err)
	e := pclient.NewCommit("repo", resp.ID)
	require.NoError(t, cli.FinishCommit("repo", resp.ID))

	// Create 'c'
	// (commit to upstream1, so that a, b & c have same prov commit in upstream2)
	_, err = cli.StartCommit("upstream1", "master")
	require.NoError(t, err)
	require.NoError(t, cli.FinishCommit("upstream1", "master"))
	cInfo, err := cli.InspectCommit("repo", "master")
	require.NoError(t, err)
	c := cInfo.Commit
	require.NoError(t, cli.FinishCommit("repo", c.ID))

	// Create 'f'
	resp, err = cli.PfsAPIClient.StartCommit(cli.Ctx(), &pfs.StartCommitRequest{
		Parent: pclient.NewCommit("repo", c.ID),
	})
	require.NoError(t, err)
	f := pclient.NewCommit("repo", resp.ID)
	require.NoError(t, cli.FinishCommit("repo", resp.ID))

	// Make sure child/parent relationships are as shown in first diagram
	commits, err := cli.ListCommit("repo", "", "", 0)
	require.Equal(t, 6, len(commits))
	aInfo, err = cli.InspectCommit("repo", a.ID)
	require.NoError(t, err)
	bInfo, err = cli.InspectCommit("repo", b.ID)
	require.NoError(t, err)
	cInfo, err = cli.InspectCommit("repo", c.ID)
	require.NoError(t, err)
	dInfo, err := cli.InspectCommit("repo", d.ID)
	require.NoError(t, err)
	eInfo, err := cli.InspectCommit("repo", e.ID)
	require.NoError(t, err)
	fInfo, err := cli.InspectCommit("repo", f.ID)
	require.NoError(t, err)

	require.Nil(t, aInfo.ParentCommit)
	require.Equal(t, a.ID, bInfo.ParentCommit.ID)
	require.Equal(t, a.ID, dInfo.ParentCommit.ID)
	require.Equal(t, b.ID, cInfo.ParentCommit.ID)
	require.Equal(t, b.ID, eInfo.ParentCommit.ID)
	require.Equal(t, c.ID, fInfo.ParentCommit.ID)
	require.ElementsEqualUnderFn(t, []string{b.ID, d.ID}, aInfo.ChildCommits, CommitToID)
	require.ElementsEqualUnderFn(t, []string{c.ID, e.ID}, bInfo.ChildCommits, CommitToID)
	require.ElementsEqualUnderFn(t, []string{f.ID}, cInfo.ChildCommits, CommitToID)
	require.Nil(t, dInfo.ChildCommits)
	require.Nil(t, eInfo.ChildCommits)
	require.Nil(t, fInfo.ChildCommits)

	// Delete commit in upstream2, which deletes b & c
	require.NoError(t, cli.DeleteCommit("upstream2", deleteMeCommit.ID))

	// Re-read commit info to get new parents/children
	dInfo, err = cli.InspectCommit("repo", d.ID)
	require.NoError(t, err)
	eInfo, err = cli.InspectCommit("repo", e.ID)
	require.NoError(t, err)
	fInfo, err = cli.InspectCommit("repo", f.ID)
	require.NoError(t, err)

	// Make sure child/parent relationships are as shown in second diagram
	commits, err = cli.ListCommit("repo", "", "", 0)
	require.Equal(t, 3, len(commits))
	require.Nil(t, eInfo.ParentCommit)
	require.Nil(t, fInfo.ParentCommit)
	require.Nil(t, dInfo.ChildCommits)
	require.Nil(t, eInfo.ChildCommits)
	require.Nil(t, fInfo.ChildCommits)
}

// Tests that when you have the following commit graph in a *downstream* repo:
//
//    ↙f
//   c
//   ↓↙e
//   b
//   ↓↙d
//   a
//
// and you delete commits 'b' and 'c' (in a single call), what you end up with
// is:
//
// d e f
//  ↘↓↙
//   a
// This makes sure that multiple live children are re-pointed at a live parent
// if appropriate
func TestDeleteCommitMultiLevelChildren(t *testing.T) {
	cli := GetPachClient(t)
	require.NoError(t, cli.CreateRepo("upstream1"))
	require.NoError(t, cli.CreateRepo("upstream2"))
	// commit to both inputs
	_, err := cli.StartCommit("upstream1", "master")
	require.NoError(t, err)
	require.NoError(t, cli.FinishCommit("upstream1", "master"))
	_, err = cli.StartCommit("upstream2", "master")
	require.NoError(t, err)
	require.NoError(t, cli.FinishCommit("upstream2", "master"))

	// Create main repo (will have the commit graphs above
	require.NoError(t, cli.CreateRepo("repo"))
	require.NoError(t, cli.CreateBranch("repo", "master", "", []*pfs.Branch{
		pclient.NewBranch("upstream1", "master"),
		pclient.NewBranch("upstream2", "master"),
	}))

	// Create commit 'a'
	aInfo, err := cli.InspectCommit("repo", "master")
	require.NoError(t, err)
	a := aInfo.Commit
	require.NoError(t, cli.FinishCommit("repo", a.ID))

	// Create 'd'
	resp, err := cli.PfsAPIClient.StartCommit(cli.Ctx(), &pfs.StartCommitRequest{
		Parent: pclient.NewCommit("repo", a.ID),
	})
	require.NoError(t, err)
	d := pclient.NewCommit("repo", resp.ID)
	require.NoError(t, cli.FinishCommit("repo", resp.ID))

	// Create 'b'
	// (a & b have same prov commit in upstream2, so this is the commit that will
	// be deleted, as both b and c are provenant on it)
	deleteMeCommit, err := cli.StartCommit("upstream1", "master")
	require.NoError(t, err)
	require.NoError(t, cli.FinishCommit("upstream1", "master"))
	bInfo, err := cli.InspectCommit("repo", "master")
	require.NoError(t, err)
	b := bInfo.Commit
	require.NoError(t, cli.FinishCommit("repo", b.ID))

	// Create 'e'
	resp, err = cli.PfsAPIClient.StartCommit(cli.Ctx(), &pfs.StartCommitRequest{
		Parent: pclient.NewCommit("repo", b.ID),
	})
	require.NoError(t, err)
	e := pclient.NewCommit("repo", resp.ID)
	require.NoError(t, cli.FinishCommit("repo", resp.ID))

	// Create 'c'
	// (commit to upstream2, so that b & c have same prov commit in upstream1)
	_, err = cli.StartCommit("upstream2", "master")
	require.NoError(t, err)
	require.NoError(t, cli.FinishCommit("upstream2", "master"))
	cInfo, err := cli.InspectCommit("repo", "master")
	require.NoError(t, err)
	c := cInfo.Commit
	require.NoError(t, cli.FinishCommit("repo", c.ID))

	// Create 'f'
	resp, err = cli.PfsAPIClient.StartCommit(cli.Ctx(), &pfs.StartCommitRequest{
		Parent: pclient.NewCommit("repo", c.ID),
	})
	require.NoError(t, err)
	f := pclient.NewCommit("repo", resp.ID)
	require.NoError(t, cli.FinishCommit("repo", resp.ID))

	// Make sure child/parent relationships are as shown in first diagram
	commits, err := cli.ListCommit("repo", "", "", 0)
	require.Equal(t, 6, len(commits))
	aInfo, err = cli.InspectCommit("repo", a.ID)
	require.NoError(t, err)
	bInfo, err = cli.InspectCommit("repo", b.ID)
	require.NoError(t, err)
	cInfo, err = cli.InspectCommit("repo", c.ID)
	require.NoError(t, err)
	dInfo, err := cli.InspectCommit("repo", d.ID)
	require.NoError(t, err)
	eInfo, err := cli.InspectCommit("repo", e.ID)
	require.NoError(t, err)
	fInfo, err := cli.InspectCommit("repo", f.ID)
	require.NoError(t, err)

	require.Nil(t, aInfo.ParentCommit)
	require.Equal(t, a.ID, bInfo.ParentCommit.ID)
	require.Equal(t, a.ID, dInfo.ParentCommit.ID)
	require.Equal(t, b.ID, cInfo.ParentCommit.ID)
	require.Equal(t, b.ID, eInfo.ParentCommit.ID)
	require.Equal(t, c.ID, fInfo.ParentCommit.ID)
	require.ElementsEqualUnderFn(t, []string{b.ID, d.ID}, aInfo.ChildCommits, CommitToID)
	require.ElementsEqualUnderFn(t, []string{c.ID, e.ID}, bInfo.ChildCommits, CommitToID)
	require.ElementsEqualUnderFn(t, []string{f.ID}, cInfo.ChildCommits, CommitToID)
	require.Nil(t, dInfo.ChildCommits)
	require.Nil(t, eInfo.ChildCommits)
	require.Nil(t, fInfo.ChildCommits)

	// Delete second commit in upstream2, which deletes b & c
	require.NoError(t, cli.DeleteCommit("upstream1", deleteMeCommit.ID))

	// Re-read commit info to get new parents/children
	aInfo, err = cli.InspectCommit("repo", a.ID)
	require.NoError(t, err)
	dInfo, err = cli.InspectCommit("repo", d.ID)
	require.NoError(t, err)
	eInfo, err = cli.InspectCommit("repo", e.ID)
	require.NoError(t, err)
	fInfo, err = cli.InspectCommit("repo", f.ID)
	require.NoError(t, err)

	// Make sure child/parent relationships are as shown in second diagram. Note
	// that after 'b' and 'c' are deleted, DeleteCommit creates a new commit:
	// - 'repo/master' points to 'a'
	// - DeleteCommit starts a new output commit to process 'upstream1/master'
	//   and 'upstream2/master'
	// - The new output commit is started in 'repo/master' and is also a child of
	//   'a'
	commits, err = cli.ListCommit("repo", "", "", 0)
	require.Equal(t, 5, len(commits))
	require.Nil(t, aInfo.ParentCommit)
	require.Equal(t, a.ID, dInfo.ParentCommit.ID)
	require.Equal(t, a.ID, eInfo.ParentCommit.ID)
	require.Equal(t, a.ID, fInfo.ParentCommit.ID)
	newCommitInfo, err := cli.InspectCommit("repo", "master")
	require.NoError(t, err)
	require.ElementsEqualUnderFn(t,
		[]string{d.ID, e.ID, f.ID, newCommitInfo.Commit.ID},
		aInfo.ChildCommits, CommitToID)
	require.Nil(t, dInfo.ChildCommits)
	require.Nil(t, eInfo.ChildCommits)
	require.Nil(t, fInfo.ChildCommits)
}

// TestDeleteCommitShrinkSubvRange is like TestDeleteCommitBigSubvenance, but
// instead of deleting commits from "schema", this test deletes them from
// "logs", to make sure that the subvenance of "schema" commits is rewritten
// correctly. As before, there are four cases:
// 1. Subvenance "Lower" is increased
// 2. Subvenance "Upper" is decreased
// 3. Subvenance is not affected, because the deleted commit is between "Lower" and "Upper"
// 4. The entire subvenance range is deleted
func TestDeleteCommitShrinkSubvRange(t *testing.T) {
	c := GetPachClient(t)

	// two input repos, one with many commits (logs), and one with few (schema)
	require.NoError(t, c.CreateRepo("logs"))
	require.NoError(t, c.CreateRepo("schema"))

	// Commit to logs and schema
	logsCommit := make([]*pfs.Commit, 10)
	var err error
	logsCommit[0], err = c.StartCommit("logs", "master")
	require.NoError(t, err)
	require.NoError(t, c.FinishCommit("logs", logsCommit[0].ID))
	schemaCommit, err := c.StartCommit("schema", "master")
	require.NoError(t, err)
	require.NoError(t, c.FinishCommit("schema", schemaCommit.ID))

	// Create an output branch, in "pipeline"
	require.NoError(t, c.CreateRepo("pipeline"))
	require.NoError(t, c.CreateBranch("pipeline", "master", "", []*pfs.Branch{
		pclient.NewBranch("schema", "master"),
		pclient.NewBranch("logs", "master"),
	}))
	commits, err := c.ListCommit("pipeline", "master", "", 0)
	require.NoError(t, err)
	require.Equal(t, 1, len(commits))

	// Commit to "logs" 9 more times, so that the commit to "schema" has 10
	// commits in its subvenance
	for i := 1; i < 10; i++ {
		logsCommit[i], err = c.StartCommit("logs", "master")
		require.NoError(t, err)
		require.NoError(t, c.FinishCommit("logs", logsCommit[i].ID))
	}
	pipelineCommitInfos, err := c.ListCommit("pipeline", "master", "", 0)
	require.Equal(t, 10, len(pipelineCommitInfos))
	pipelineCommit := make([]string, 10)
	for i := range pipelineCommitInfos {
		// ListCommit sorts from newest to oldest, but we want the reverse (0 is
		// oldest, like how logsCommit[0] is the oldest)
		pipelineCommit[9-i] = pipelineCommitInfos[i].Commit.ID
	}
	require.NoError(t, err)
	// Make sure the subvenance of the one commit in "schema" includes all commits
	// in "pipeline"
	schemaCommitInfo, err := c.InspectCommit("schema", schemaCommit.ID)
	require.Equal(t, 1, len(schemaCommitInfo.Subvenance))
	require.Equal(t, pipelineCommit[0], schemaCommitInfo.Subvenance[0].Lower.ID)
	require.Equal(t, pipelineCommit[9], schemaCommitInfo.Subvenance[0].Upper.ID)

	// Case 1
	// - Delete the first commit in "logs" and make sure that the subvenance of
	//   the single commit in "schema" has increased its Lower value
	require.NoError(t, c.DeleteCommit("logs", logsCommit[0].ID))
	schemaCommitInfo, err = c.InspectCommit("schema", schemaCommit.ID)
	require.Equal(t, 1, len(schemaCommitInfo.Subvenance))
	require.Equal(t, pipelineCommit[1], schemaCommitInfo.Subvenance[0].Lower.ID)
	require.Equal(t, pipelineCommit[9], schemaCommitInfo.Subvenance[0].Upper.ID)

	// Case 2
	// - Delete the last commit in "logs" and make sure that the subvenance of
	//   the single commit in "schema" has decreased its Upper value
	require.NoError(t, c.DeleteCommit("logs", logsCommit[9].ID))
	schemaCommitInfo, err = c.InspectCommit("schema", schemaCommit.ID)
	require.Equal(t, 1, len(schemaCommitInfo.Subvenance))
	require.Equal(t, pipelineCommit[1], schemaCommitInfo.Subvenance[0].Lower.ID)
	require.Equal(t, pipelineCommit[8], schemaCommitInfo.Subvenance[0].Upper.ID)

	// Case 3
	// - Delete the middle commit in "logs" and make sure that the subvenance of
	//   the single commit in "schema" hasn't changed
	require.NoError(t, c.DeleteCommit("logs", logsCommit[5].ID))
	schemaCommitInfo, err = c.InspectCommit("schema", schemaCommit.ID)
	require.Equal(t, 1, len(schemaCommitInfo.Subvenance))
	require.Equal(t, pipelineCommit[1], schemaCommitInfo.Subvenance[0].Lower.ID)
	require.Equal(t, pipelineCommit[8], schemaCommitInfo.Subvenance[0].Upper.ID)

	// Case 4
	// - Delete the remaining commits in "logs" and make sure that the subvenance
	//   of the single commit in "schema" is now empty
	for _, i := range []int{1, 2, 3, 4, 6, 7, 8} {
		require.NoError(t, c.DeleteCommit("logs", logsCommit[i].ID))
	}
	schemaCommitInfo, err = c.InspectCommit("schema", schemaCommit.ID)
	require.Equal(t, 0, len(schemaCommitInfo.Subvenance))
}

func TestCommitState(t *testing.T) {
	c := GetPachClient(t)

	// two input repos, one with many commits (logs), and one with few (schema)
	require.NoError(t, c.CreateRepo("A"))
	require.NoError(t, c.CreateRepo("B"))

	require.NoError(t, c.CreateBranch("B", "master", "", []*pfs.Branch{pclient.NewBranch("A", "master")}))

	// Start a commit on A/master, this will create a non-ready commit on B/master.
	_, err := c.StartCommit("A", "master")
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	_, err = c.PfsAPIClient.InspectCommit(ctx, &pfs.InspectCommitRequest{
		Commit:     pclient.NewCommit("B", "master"),
		BlockState: pfs.CommitState_READY,
	})
	require.YesError(t, err)

	// Finish the commit on A/master, that will make the B/master ready.
	require.NoError(t, c.FinishCommit("A", "master"))

	ctx, cancel = context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	_, err = c.PfsAPIClient.InspectCommit(ctx, &pfs.InspectCommitRequest{
		Commit:     pclient.NewCommit("B", "master"),
		BlockState: pfs.CommitState_READY,
	})
	require.NoError(t, err)

	// Create a new branch C/master with A/master as provenance. It should start out ready.
	require.NoError(t, c.CreateRepo("C"))
	require.NoError(t, c.CreateBranch("C", "master", "", []*pfs.Branch{pclient.NewBranch("A", "master")}))

	ctx, cancel = context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	_, err = c.PfsAPIClient.InspectCommit(ctx, &pfs.InspectCommitRequest{
		Commit:     pclient.NewCommit("C", "master"),
		BlockState: pfs.CommitState_READY,
	})
	require.NoError(t, err)
}

func TestSubscribeStates(t *testing.T) {
	c := GetPachClient(t)

	require.NoError(t, c.CreateRepo("A"))
	require.NoError(t, c.CreateRepo("B"))
	require.NoError(t, c.CreateRepo("C"))

	require.NoError(t, c.CreateBranch("B", "master", "", []*pfs.Branch{pclient.NewBranch("A", "master")}))
	require.NoError(t, c.CreateBranch("C", "master", "", []*pfs.Branch{pclient.NewBranch("B", "master")}))

	ctx, cancel := context.WithCancel(c.Ctx())
	defer cancel()
	c = c.WithCtx(ctx)

	var readyCommits int64
	go func() {
		c.SubscribeCommitF("B", "master", "", pfs.CommitState_READY, func(ci *pfs.CommitInfo) error {
			atomic.AddInt64(&readyCommits, 1)
			return nil
		})
	}()
	go func() {
		c.SubscribeCommitF("C", "master", "", pfs.CommitState_READY, func(ci *pfs.CommitInfo) error {
			atomic.AddInt64(&readyCommits, 1)
			return nil
		})
	}()
	_, err := c.StartCommit("A", "master")
	require.NoError(t, err)
	require.NoError(t, c.FinishCommit("A", "master"))

	require.NoErrorWithinTRetry(t, time.Second*10, func() error {
		if atomic.LoadInt64(&readyCommits) != 1 {
			return fmt.Errorf("wrong number of ready commits")
		}
		return nil
	})

	require.NoError(t, c.FinishCommit("B", "master"))

	require.NoErrorWithinTRetry(t, time.Second*10, func() error {
		if atomic.LoadInt64(&readyCommits) != 2 {
			return fmt.Errorf("wrong number of ready commits")
		}
		return nil
	})
}

func TestPutFileCommit(t *testing.T) {
	c := GetPachClient(t)

	numFiles := 25
	repo := "repo"
	require.NoError(t, c.CreateRepo(repo))

	var eg errgroup.Group
	for i := 0; i < numFiles; i++ {
		i := i
		eg.Go(func() error {
			_, err := c.PutFile(repo, "master", fmt.Sprintf("%d", i), strings.NewReader(fmt.Sprintf("%d", i)))
			return err
		})
	}
	require.NoError(t, eg.Wait())

	for i := 0; i < numFiles; i++ {
		var b bytes.Buffer
		require.NoError(t, c.GetFile(repo, "master", fmt.Sprintf("%d", i), 0, 0, &b))
		require.Equal(t, fmt.Sprintf("%d", i), b.String())
	}

	bi, err := c.InspectBranch(repo, "master")
	require.NoError(t, err)

	eg = errgroup.Group{}
	for i := 0; i < numFiles; i++ {
		i := i
		eg.Go(func() error {
			return c.CopyFile(repo, bi.Head.ID, fmt.Sprintf("%d", i), repo, "master", fmt.Sprintf("%d", (i+1)%numFiles), true)
		})
	}
	require.NoError(t, eg.Wait())

	for i := 0; i < numFiles; i++ {
		var b bytes.Buffer
		require.NoError(t, c.GetFile(repo, "master", fmt.Sprintf("%d", (i+1)%numFiles), 0, 0, &b))
		require.Equal(t, fmt.Sprintf("%d", i), b.String())
	}

	eg = errgroup.Group{}
	for i := 0; i < numFiles; i++ {
		i := i
		eg.Go(func() error {
			return c.DeleteFile(repo, "master", fmt.Sprintf("%d", i))
		})
	}
	require.NoError(t, eg.Wait())

	fileInfos, err := c.ListFile(repo, "master", "")
	require.NoError(t, err)
	require.Equal(t, 0, len(fileInfos))
}

func TestPutFileCommitNilBranch(t *testing.T) {
	c := GetPachClient(t)
	repo := "repo"
	require.NoError(t, c.CreateRepo(repo))
	require.NoError(t, c.CreateBranch(repo, "master", "", nil))

	_, err := c.PutFile(repo, "master", "file", strings.NewReader("file"))
	require.NoError(t, err)
}

func TestPutFileCommitOverwrite(t *testing.T) {
	c := GetPachClient(t)

	numFiles := 5
	repo := "repo"
	require.NoError(t, c.CreateRepo(repo))

	for i := 0; i < numFiles; i++ {
		_, err := c.PutFileOverwrite(repo, "master", "file", strings.NewReader(fmt.Sprintf("%d", i)), 0)
		require.NoError(t, err)
	}

	var b bytes.Buffer
	require.NoError(t, c.GetFile(repo, "master", "file", 0, 0, &b))
	require.Equal(t, fmt.Sprintf("%d", numFiles-1), b.String())
}

func TestStartCommitOutputBranch(t *testing.T) {
	c := GetPachClient(t)
	require.NoError(t, c.CreateRepo("in"))
	require.NoError(t, c.CreateRepo("out"))
	require.NoError(t, c.CreateBranch("out", "master", "", []*pfs.Branch{pclient.NewBranch("in", "master")}))
	_, err := c.StartCommit("out", "master")
	require.YesError(t, err)
}

func TestWalk(t *testing.T) {
	c := GetPachClient(t)
	repo := "repo"
	require.NoError(t, c.CreateRepo(repo))
	_, err := c.PutFile(repo, "master", "dir/bar", strings.NewReader("bar"))
	require.NoError(t, err)
	_, err = c.PutFile(repo, "master", "dir/dir2/buzz", strings.NewReader("buzz"))
	require.NoError(t, err)
	_, err = c.PutFile(repo, "master", "foo", strings.NewReader("foo"))
	require.NoError(t, err)

	expectedPaths := []string{"/", "/dir", "/dir/bar", "/dir/dir2", "/dir/dir2/buzz", "/foo"}
	i := 0
	require.NoError(t, c.Walk(repo, "master", "", func(fi *pfs.FileInfo) error {
		require.Equal(t, expectedPaths[i], fi.File.Path)
		i++
		return nil
	}))
	require.Equal(t, len(expectedPaths), i)
}

func TestReadSizeLimited(t *testing.T) {
	c := GetPachClient(t)
	require.NoError(t, c.CreateRepo("test"))
	_, err := c.PutFile("test", "master", "file", strings.NewReader(strings.Repeat("a", 100*MB)))
	require.NoError(t, err)

	var b bytes.Buffer
	require.NoError(t, c.GetFile("test", "master", "file", 0, 2*MB, &b))
	require.Equal(t, 2*MB, b.Len())

	b.Reset()
	require.NoError(t, c.GetFile("test", "master", "file", 2*MB, 2*MB, &b))
	require.Equal(t, 2*MB, b.Len())
}

func TestPutFiles(t *testing.T) {
	c := GetPachClient(t)
	require.NoError(t, c.CreateRepo("repo"))
	pfclient, err := c.PfsAPIClient.PutFile(context.Background())
	require.NoError(t, err)
	paths := []string{"foo", "bar", "fizz", "buzz"}
	for _, path := range paths {
		require.NoError(t, pfclient.Send(&pfs.PutFileRequest{
			File:  pclient.NewFile("repo", "master", path),
			Value: []byte(path),
		}))
	}
	_, err = pfclient.CloseAndRecv()
	require.NoError(t, err)

	cis, err := c.ListCommit("repo", "", "", 0)
	require.Equal(t, 1, len(cis))

	for _, path := range paths {
		var b bytes.Buffer
		require.NoError(t, c.GetFile("repo", "master", path, 0, 0, &b))
		require.Equal(t, path, b.String())
	}
}

func TestPutFilesURL(t *testing.T) {
	c := GetPachClient(t)
	require.NoError(t, c.CreateRepo("repo"))
	pfclient, err := c.PfsAPIClient.PutFile(context.Background())
	require.NoError(t, err)
	paths := []string{"README.md", "CHANGELOG.md", "CONTRIBUTING.md"}
	for _, path := range paths {
		require.NoError(t, pfclient.Send(&pfs.PutFileRequest{
			File: pclient.NewFile("repo", "master", path),
			Url:  fmt.Sprintf("https://raw.githubusercontent.com/pachyderm/pachyderm/master/%s", path),
		}))
	}
	_, err = pfclient.CloseAndRecv()
	require.NoError(t, err)

	cis, err := c.ListCommit("repo", "", "", 0)
	require.Equal(t, 1, len(cis))

	for _, path := range paths {
		fileInfo, err := c.InspectFile("repo", "master", path)
		require.NoError(t, err)
		require.True(t, fileInfo.SizeBytes > 0)
	}
}

func writeObj(t *testing.T, c obj.Client, path, content string) {
	w, err := c.Writer(context.Background(), path)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, w.Close())
	}()
	_, err = w.Write([]byte(content))
	require.NoError(t, err)
}

func TestPutFilesObjURL(t *testing.T) {
	paths := []string{"files/foo", "files/bar", "files/fizz"}
	wd, err := os.Getwd()
	require.NoError(t, err)
	objC, err := obj.NewLocalClient(wd)
	require.NoError(t, err)
	for _, path := range paths {
		writeObj(t, objC, path, path)
	}
	defer func() {
		for _, path := range paths {
			// ignored error, this is just cleanup, not actually part of the test
			objC.Delete(context.Background(), path)
		}
	}()

	c := GetPachClient(t)
	require.NoError(t, c.CreateRepo("repo"))
	pfclient, err := c.PfsAPIClient.PutFile(context.Background())
	require.NoError(t, err)
	for _, path := range paths {
		require.NoError(t, pfclient.Send(&pfs.PutFileRequest{
			File: pclient.NewFile("repo", "master", path),
			Url:  fmt.Sprintf("local://%s/%s", wd, path),
		}))
	}
	require.NoError(t, pfclient.Send(&pfs.PutFileRequest{
		File:      pclient.NewFile("repo", "master", "recursive"),
		Url:       fmt.Sprintf("local://%s/files", wd),
		Recursive: true,
	}))
	_, err = pfclient.CloseAndRecv()
	require.NoError(t, err)

	cis, err := c.ListCommit("repo", "", "", 0)
	require.Equal(t, 1, len(cis))

	for _, path := range paths {
		var b bytes.Buffer
		require.NoError(t, c.GetFile("repo", "master", path, 0, 0, &b))
		require.Equal(t, path, b.String())
		b.Reset()
		require.NoError(t, c.GetFile("repo", "master", filepath.Join("recursive", filepath.Base(path)), 0, 0, &b))
		require.Equal(t, path, b.String())
	}
}

func TestFileHistory(t *testing.T) {
	client := GetPachClient(t)
	var err error

	repo := "test"
	require.NoError(t, client.CreateRepo(repo))
	numCommits := 10
	for i := 0; i < numCommits; i++ {
		_, err = client.PutFile(repo, "master", "file", strings.NewReader("foo\n"))
		require.NoError(t, err)
	}
	fileInfos, err := client.ListFileHistory(repo, "master", "file", -1)
	require.NoError(t, err)
	require.Equal(t, numCommits, len(fileInfos))

	for i := 1; i < numCommits; i++ {
		fileInfos, err := client.ListFileHistory(repo, "master", "file", int64(i))
		require.NoError(t, err)
		require.Equal(t, i, len(fileInfos))
	}

	require.NoError(t, client.DeleteFile(repo, "master", "file"))
	for i := 0; i < numCommits; i++ {
		_, err = client.PutFile(repo, "master", "file", strings.NewReader("foo\n"))
		require.NoError(t, err)
		_, err = client.PutFile(repo, "master", "unrelated", strings.NewReader("foo\n"))
		require.NoError(t, err)
	}
	fileInfos, err = client.ListFileHistory(repo, "master", "file", -1)
	require.NoError(t, err)
	require.Equal(t, numCommits, len(fileInfos))

	for i := 1; i < numCommits; i++ {
		fileInfos, err := client.ListFileHistory(repo, "master", "file", int64(i))
		require.NoError(t, err)
		require.Equal(t, i, len(fileInfos))
	}
}

func TestUpdateRepo(t *testing.T) {
	var err error
	client := GetPachClient(t)
	repo := "test"
	_, err = client.PfsAPIClient.CreateRepo(
		client.Ctx(),
		&pfs.CreateRepoRequest{
			Repo:   pclient.NewRepo(repo),
			Update: true,
		},
	)
	require.NoError(t, err)
	ri, err := client.InspectRepo(repo)
	require.NoError(t, err)
	created, err := types.TimestampFromProto(ri.Created)
	require.NoError(t, err)
	desc := "foo"
	_, err = client.PfsAPIClient.CreateRepo(
		client.Ctx(),
		&pfs.CreateRepoRequest{
			Repo:        pclient.NewRepo(repo),
			Update:      true,
			Description: desc,
		},
	)
	require.NoError(t, err)
	ri, err = client.InspectRepo(repo)
	require.NoError(t, err)
	newCreated, err := types.TimestampFromProto(ri.Created)
	require.Equal(t, created, newCreated)
	require.Equal(t, desc, ri.Description)
}

func TestPutObjectAsync(t *testing.T) {
	client := GetPachClient(t)
	// Write and tag an object greater than grpc max message size.
	tag := &pfs.Tag{Name: "tag"}
	w, err := client.PutObjectAsync([]*pfs.Tag{tag})
	require.NoError(t, err)
	expected := []byte(generateRandomString(30 * MB))
	_, err = w.Write(expected)
	require.NoError(t, err)
	require.NoError(t, w.Close())
	// Check actual results of write.
	actual := &bytes.Buffer{}
	require.NoError(t, client.GetTag(tag.Name, actual))
	require.Equal(t, expected, actual.Bytes())
}

func TestDeferredProcessing(t *testing.T) {
	client := GetPachClient(t)
	require.NoError(t, client.CreateRepo("input"))
	require.NoError(t, client.CreateRepo("output1"))
	require.NoError(t, client.CreateRepo("output2"))
	require.NoError(t, client.CreateBranch("output1", "staging", "", []*pfs.Branch{pclient.NewBranch("input", "master")}))
	require.NoError(t, client.CreateBranch("output2", "staging", "", []*pfs.Branch{pclient.NewBranch("output1", "master")}))
	_, err := client.PutFile("input", "staging", "file", strings.NewReader("foo"))
	require.NoError(t, err)

	commits, err := client.FlushCommitAll([]*pfs.Commit{pclient.NewCommit("input", "staging")}, nil)
	require.NoError(t, err)
	require.Equal(t, 0, len(commits))

	require.NoError(t, client.CreateBranch("input", "master", "staging", nil))
	require.NoError(t, client.FinishCommit("output1", "staging"))
	commits, err = client.FlushCommitAll([]*pfs.Commit{pclient.NewCommit("input", "staging")}, nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(commits))

	require.NoError(t, client.CreateBranch("output1", "master", "staging", nil))
	require.NoError(t, client.FinishCommit("output2", "staging"))
	commits, err = client.FlushCommitAll([]*pfs.Commit{pclient.NewCommit("input", "staging")}, nil)
	require.NoError(t, err)
	require.Equal(t, 2, len(commits))
}

func seedStr(seed int64) string {
	return fmt.Sprint("seed: ", strconv.FormatInt(seed, 10))
}

func monkeyRetry(t *testing.T, f func() error, errMsg string) {
	backoff.Retry(func() error {
		err := f()
		if err != nil {
			require.True(t, obj.IsMonkeyError(err), errMsg)
		}
		return err
	}, backoff.NewInfiniteBackOff())
}

func TestMonkeyObjectStorage(t *testing.T) {
	seed := time.Now().UTC().UnixNano()
	obj.InitMonkeyTest(seed)
	client := GetPachClient(t)
	iterations := 25
	repo := "input"
	require.NoError(t, client.CreateRepo(repo), seedStr(seed))
	filePrefix := "file"
	dataPrefix := "data"
	var commit *pfs.Commit
	var err error
	buf := &bytes.Buffer{}
	obj.EnableMonkeyTest()
	defer obj.DisableMonkeyTest()
	for i := 0; i < iterations; i++ {
		file := filePrefix + strconv.Itoa(i)
		data := dataPrefix + strconv.Itoa(i)
		// Retry start commit until it eventually succeeds.
		monkeyRetry(t, func() error {
			commit, err = client.StartCommit(repo, "")
			return err
		}, seedStr(seed))
		// Retry put file until it eventually succeeds.
		monkeyRetry(t, func() error {
			_, err = client.PutFile(repo, commit.ID, file, strings.NewReader(data))
			if err != nil {
				// Verify that the file does not exist if an error occurred.
				obj.DisableMonkeyTest()
				defer obj.EnableMonkeyTest()
				buf.Reset()
				err := client.GetFile(repo, commit.ID, file, 0, 0, buf)
				require.Matches(t, "not found", err.Error(), seedStr(seed))
			}
			return err
		}, seedStr(seed))
		// Retry get file until it eventually succeeds (before commit is finished).
		monkeyRetry(t, func() error {
			buf.Reset()
			if err = client.GetFile(repo, commit.ID, file, 0, 0, buf); err != nil {
				return err
			}
			require.Equal(t, data, buf.String(), seedStr(seed))
			return nil
		}, seedStr(seed))
		// Retry finish commit until it eventually succeeds.
		monkeyRetry(t, func() error {
			return client.FinishCommit(repo, commit.ID)
		}, seedStr(seed))
		// Retry get file until it eventually succeeds (after commit is finished).
		monkeyRetry(t, func() error {
			buf.Reset()
			if err = client.GetFile(repo, commit.ID, file, 0, 0, buf); err != nil {
				return err
			}
			require.Equal(t, data, buf.String(), seedStr(seed))
			return nil
		}, seedStr(seed))
	}
}

const (
	inputRepo          = iota // create a new input repo
	inputBranch               // create a new branch on an existing input repo
	deleteInputBranch         // delete an input branch
	commit                    // commit to an input branch
	deleteCommit              // delete a commit from an input branch
	outputRepo                // create a new output repo, with master branch subscribed to random other branches
	outputBranch              // create a new output branch on an existing output repo
	deleteOutputBranch        // delete an output branch
)

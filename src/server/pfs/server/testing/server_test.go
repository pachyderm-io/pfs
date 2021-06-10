package testing

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	units "github.com/docker/go-units"
	"github.com/gogo/protobuf/types"
	"github.com/pachyderm/pachyderm/v2/src/client"
	pclient "github.com/pachyderm/pachyderm/v2/src/client"
	"github.com/pachyderm/pachyderm/v2/src/internal/ancestry"
	"github.com/pachyderm/pachyderm/v2/src/internal/backoff"
	"github.com/pachyderm/pachyderm/v2/src/internal/errors"
	"github.com/pachyderm/pachyderm/v2/src/internal/errutil"
	"github.com/pachyderm/pachyderm/v2/src/internal/obj"
	"github.com/pachyderm/pachyderm/v2/src/internal/pfsdb"
	"github.com/pachyderm/pachyderm/v2/src/internal/require"
	"github.com/pachyderm/pachyderm/v2/src/internal/serviceenv"
	"github.com/pachyderm/pachyderm/v2/src/internal/storage/fileset"
	"github.com/pachyderm/pachyderm/v2/src/internal/tarutil"
	"github.com/pachyderm/pachyderm/v2/src/internal/testpachd"
	tu "github.com/pachyderm/pachyderm/v2/src/internal/testutil"
	"github.com/pachyderm/pachyderm/v2/src/internal/testutil/random"
	"github.com/pachyderm/pachyderm/v2/src/internal/uuid"
	"github.com/pachyderm/pachyderm/v2/src/pfs"
	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/errgroup"
)

func CommitToID(commit interface{}) interface{} {
	return commit.(*pfs.Commit).ID
}

func CommitInfoToID(commit interface{}) interface{} {
	return commit.(*pfs.CommitInfo).Commit.ID
}

func RepoInfoToName(repoInfo interface{}) interface{} {
	return repoInfo.(*pfs.RepoInfo).Repo.Name
}

func FileInfoToPath(fileInfo interface{}) interface{} {
	return fileInfo.(*pfs.FileInfo).File.Path
}

type fileSetSpec map[string]tarutil.File

func (fs fileSetSpec) makeTarStream() io.Reader {
	buf := &bytes.Buffer{}
	if err := tarutil.WithWriter(buf, func(tw *tar.Writer) error {
		for _, file := range fs {
			if err := tarutil.WriteFile(tw, file); err != nil {
				panic(err)
			}
		}
		return nil
	}); err != nil {
		panic(err)
	}
	return buf
}

func finfosToPaths(finfos []*pfs.FileInfo) (paths []string) {
	for _, finfo := range finfos {
		paths = append(paths, finfo.File.Path)
	}
	return paths
}

func TestPFS(suite *testing.T) {
	suite.Parallel()

	suite.Run("InvalidRepo", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		require.YesError(t, env.PachClient.CreateRepo("/repo"))

		require.NoError(t, env.PachClient.CreateRepo("lenny"))
		require.NoError(t, env.PachClient.CreateRepo("lenny123"))
		require.NoError(t, env.PachClient.CreateRepo("lenny_123"))
		require.NoError(t, env.PachClient.CreateRepo("lenny-123"))

		require.YesError(t, env.PachClient.CreateRepo("lenny.123"))
		require.YesError(t, env.PachClient.CreateRepo("lenny:"))
		require.YesError(t, env.PachClient.CreateRepo("lenny,"))
		require.YesError(t, env.PachClient.CreateRepo("lenny#"))
	})

	suite.Run("CreateSameRepoInParallel", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		numGoros := 1000
		errCh := make(chan error)
		for i := 0; i < numGoros; i++ {
			go func() {
				errCh <- env.PachClient.CreateRepo("repo")
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
	})

	suite.Run("CreateDifferentRepoInParallel", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		numGoros := 1000
		errCh := make(chan error)
		for i := 0; i < numGoros; i++ {
			i := i
			go func() {
				errCh <- env.PachClient.CreateRepo(fmt.Sprintf("repo%d", i))
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
	})

	suite.Run("CreateRepoDeleteRepoRace", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		for i := 0; i < 100; i++ {
			require.NoError(t, env.PachClient.CreateRepo("foo"))
			require.NoError(t, env.PachClient.CreateRepo("bar"))
			errCh := make(chan error)
			go func() {
				errCh <- env.PachClient.DeleteRepo("foo", false)
			}()
			go func() {
				errCh <- env.PachClient.CreateBranch("bar", "master", "", "", []*pfs.Branch{pclient.NewBranch("foo", "master")})
			}()
			err1 := <-errCh
			err2 := <-errCh
			// these two operations should never race in such a way that they
			// both succeed, leaving us with a repo bar that has a nonexistent
			// provenance foo
			require.True(t, err1 != nil || err2 != nil)
			env.PachClient.DeleteRepo("bar", true)
			env.PachClient.DeleteRepo("foo", true)
		}
	})

	suite.Run("Branch", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		repo := "repo"
		require.NoError(t, env.PachClient.CreateRepo(repo))
		_, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit(repo, "master", ""))
		commitInfo, err := env.PachClient.InspectCommit(repo, "master", "")
		require.NoError(t, err)
		require.Nil(t, commitInfo.ParentCommit)

		_, err = env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit(repo, "master", ""))
		commitInfo, err = env.PachClient.InspectCommit(repo, "master", "")
		require.NoError(t, err)
		require.NotNil(t, commitInfo.ParentCommit)
	})

	suite.Run("ToggleBranchProvenance", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		require.NoError(t, env.PachClient.CreateRepo("in"))
		require.NoError(t, env.PachClient.CreateRepo("out"))
		require.NoError(t, env.PachClient.CreateBranch("out", "master", "", "", []*pfs.Branch{
			pclient.NewBranch("in", "master"),
		}))
		outRepo := client.NewRepo("out")

		// Create initial input commit, and make sure we get an output commit
		require.NoError(t, env.PachClient.PutFile(client.NewCommit("in", "master", ""), "1", strings.NewReader("1")))
		cis, err := env.PachClient.ListCommit(outRepo, outRepo.NewCommit("master", ""), nil, 0)
		require.NoError(t, err)
		require.Equal(t, 1, len(cis))
		require.NoError(t, env.PachClient.FinishCommit("out", "master", ""))
		// make sure output commit has the right provenance
		ci, err := env.PachClient.InspectCommit("in", "master", "")
		require.NoError(t, err)
		expectedProv := map[string]bool{
			path.Join("in", ci.Commit.Branch.Name, ci.Commit.ID): true,
		}
		ci, err = env.PachClient.InspectCommit("out", "master", "")
		require.NoError(t, err)
		require.Equal(t, len(expectedProv), len(ci.Provenance))
		for _, c := range ci.Provenance {
			require.True(t, expectedProv[path.Join(c.Commit.Branch.Repo.Name, c.Commit.Branch.Name, c.Commit.ID)])
		}

		// Toggle out@master provenance off
		require.NoError(t, env.PachClient.CreateBranch("out", "master", "master", "", nil))

		// Create new input commit & make sure no new output commit is created
		require.NoError(t, env.PachClient.PutFile(client.NewCommit("in", "master", ""), "2", strings.NewReader("2")))
		cis, err = env.PachClient.ListCommit(outRepo, outRepo.NewCommit("master", ""), nil, 0)
		require.NoError(t, err)
		require.Equal(t, 1, len(cis))
		// make sure output commit still has the right provenance
		ci, err = env.PachClient.InspectCommit("in", "", "master~1") // old input commit
		require.NoError(t, err)
		expectedProv = map[string]bool{
			path.Join("in", ci.Commit.Branch.Name, ci.Commit.ID): true,
		}
		ci, err = env.PachClient.InspectCommit("out", "master", "")
		require.NoError(t, err)
		require.Equal(t, len(expectedProv), len(ci.Provenance))
		for _, c := range ci.Provenance {
			require.True(t, expectedProv[path.Join(c.Commit.Branch.Repo.Name, c.Commit.Branch.Name, c.Commit.ID)])
		}

		// Toggle out@master provenance back on, creating a new output commit
		require.NoError(t, env.PachClient.CreateBranch("out", "master", "master", "", []*pfs.Branch{
			pclient.NewBranch("in", "master"),
		}))
		cis, err = env.PachClient.ListCommit(outRepo, outRepo.NewCommit("master", ""), nil, 0)
		require.NoError(t, err)
		require.Equal(t, 2, len(cis))
		// make sure new output commit has the right provenance
		ci, err = env.PachClient.InspectCommit("in", "master", "") // newest input commit
		require.NoError(t, err)
		expectedProv = map[string]bool{
			path.Join("in", ci.Commit.Branch.Name, ci.Commit.ID): true,
		}
		ci, err = env.PachClient.InspectCommit("out", "master", "")
		require.NoError(t, err)
		require.Equal(t, len(expectedProv), len(ci.Provenance))
		for _, c := range ci.Provenance {
			require.True(t, expectedProv[path.Join(c.Commit.Branch.Repo.Name, c.Commit.Branch.Name, c.Commit.ID)])
		}
	})

	suite.Run("RecreateBranchProvenance", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		require.NoError(t, env.PachClient.CreateRepo("in"))
		require.NoError(t, env.PachClient.CreateRepo("out"))
		require.NoError(t, env.PachClient.CreateBranch("out", "master", "", "", []*pfs.Branch{pclient.NewBranch("in", "master")}))
		require.NoError(t, env.PachClient.PutFile(client.NewCommit("in", "master", ""), "foo", strings.NewReader("foo")))
		outRepo := client.NewRepo("out")
		cis, err := env.PachClient.ListCommit(outRepo, nil, nil, 0)
		require.NoError(t, err)
		require.Equal(t, 1, len(cis))
		id := cis[0].Commit.ID
		require.NoError(t, env.PachClient.DeleteBranch("out", "master", false))
		require.NoError(t, env.PachClient.CreateBranch("out", "master", "", id, []*pfs.Branch{pclient.NewBranch("in", "master")}))
		cis, err = env.PachClient.ListCommit(outRepo, nil, nil, 0)
		require.NoError(t, err)
		require.Equal(t, 1, len(cis))
		require.Equal(t, id, cis[0].Commit.ID)
	})

	suite.Run("CreateAndInspectRepo", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		repo := "repo"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		repoInfo, err := env.PachClient.InspectRepo(repo)
		require.NoError(t, err)
		require.Equal(t, repo, repoInfo.Repo.Name)
		require.NotNil(t, repoInfo.Created)
		require.Equal(t, 0, int(repoInfo.SizeBytes))

		require.YesError(t, env.PachClient.CreateRepo(repo))
		_, err = env.PachClient.InspectRepo("nonexistent")
		require.YesError(t, err)

		_, err = env.PachClient.PfsAPIClient.CreateRepo(context.Background(), &pfs.CreateRepoRequest{
			Repo: pclient.NewRepo("somerepo1"),
		})
		require.NoError(t, err)
	})

	suite.Run("ListRepo", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		numRepos := 10
		var repoNames []string
		for i := 0; i < numRepos; i++ {
			repo := fmt.Sprintf("repo%d", i)
			require.NoError(t, env.PachClient.CreateRepo(repo))
			repoNames = append(repoNames, repo)
		}

		repoInfos, err := env.PachClient.ListRepo()
		require.NoError(t, err)
		require.ElementsEqualUnderFn(t, repoNames, repoInfos, RepoInfoToName)
	})

	// Make sure that commits of deleted repos do not resurface
	suite.Run("CreateDeletedRepo", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		repo := "repo"
		require.NoError(t, env.PachClient.CreateRepo(repo))
		repoProto := client.NewRepo(repo)

		commit, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(commit, "foo", strings.NewReader("foo")))
		require.NoError(t, env.PachClient.FinishCommit(repo, commit.Branch.Name, commit.ID))

		commitInfos, err := env.PachClient.ListCommit(repoProto, nil, nil, 0)
		require.NoError(t, err)
		require.Equal(t, 1, len(commitInfos))

		require.NoError(t, env.PachClient.DeleteRepo(repo, false))
		require.NoError(t, env.PachClient.CreateRepo(repo))

		commitInfos, err = env.PachClient.ListCommit(repoProto, nil, nil, 0)
		require.NoError(t, err)
		require.Equal(t, 0, len(commitInfos))
	})

	// Make sure that commits of deleted repos do not resurface
	suite.Run("ListCommitLimit", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		repo := "repo"
		commit := client.NewCommit(repo, "master", "")
		require.NoError(t, env.PachClient.CreateRepo(repo))
		require.NoError(t, env.PachClient.PutFile(commit, "foo", strings.NewReader("foo")))
		require.NoError(t, env.PachClient.PutFile(commit, "bar", strings.NewReader("bar")))
		commitInfos, err := env.PachClient.ListCommit(commit.Branch.Repo, nil, nil, 1)
		require.NoError(t, err)
		require.Equal(t, 1, len(commitInfos))
	})

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
	suite.Run("UpdateProvenance", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		prov1 := "prov1"
		require.NoError(t, env.PachClient.CreateRepo(prov1))
		prov2 := "prov2"
		require.NoError(t, env.PachClient.CreateRepo(prov2))
		prov3 := "prov3"
		require.NoError(t, env.PachClient.CreateRepo(prov3))

		repo := "repo"
		require.NoError(t, env.PachClient.CreateRepo(repo))
		require.NoError(t, env.PachClient.CreateBranch(repo, "master", "", "", []*pfs.Branch{pclient.NewBranch(prov1, "master"), pclient.NewBranch(prov2, "master")}))

		downstream1 := "downstream1"
		require.NoError(t, env.PachClient.CreateRepo(downstream1))
		require.NoError(t, env.PachClient.CreateBranch(downstream1, "master", "", "", []*pfs.Branch{pclient.NewBranch(repo, "master")}))

		downstream2 := "downstream2"
		require.NoError(t, env.PachClient.CreateRepo(downstream2))
		require.NoError(t, env.PachClient.CreateBranch(downstream2, "master", "", "", []*pfs.Branch{pclient.NewBranch(repo, "master")}))

		// Without the Update flag it should fail
		require.YesError(t, env.PachClient.CreateRepo(repo))

		_, err := env.PachClient.PfsAPIClient.CreateRepo(context.Background(), &pfs.CreateRepoRequest{
			Repo:   pclient.NewRepo(repo),
			Update: true,
		})
		require.NoError(t, err)

		require.NoError(t, env.PachClient.CreateBranch(repo, "master", "", "", []*pfs.Branch{pclient.NewBranch(prov2, "master"), pclient.NewBranch(prov3, "master")}))

		// We should be able to delete prov1 since it's no longer the provenance
		// of other repos.
		require.NoError(t, env.PachClient.DeleteRepo(prov1, false))

		// We shouldn't be able to delete prov3 since it's now a provenance
		// of other repos.
		require.YesError(t, env.PachClient.DeleteRepo(prov3, false))
	})

	suite.Run("PutFileIntoOpenCommit", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		commit1, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(commit1, "foo", strings.NewReader("foo\n")))
		require.NoError(t, env.PachClient.FinishCommit(repo, commit1.Branch.Name, commit1.ID))

		require.YesError(t, env.PachClient.PutFile(commit1, "foo", strings.NewReader("foo\n")))

		commit2, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(commit2, "foo", strings.NewReader("foo\n")))
		require.NoError(t, env.PachClient.FinishCommit(repo, "master", ""))

		require.YesError(t, env.PachClient.PutFile(commit2, "foo", strings.NewReader("foo\n")))
	})

	suite.Run("PutFileDirectoryTraversal", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		require.NoError(t, env.PachClient.CreateRepo("repo"))

		_, err := env.PachClient.StartCommit("repo", "master")
		require.NoError(t, err)
		masterCommit := client.NewCommit("repo", "master", "")

		mfc, err := env.PachClient.NewModifyFileClient(masterCommit)
		require.NoError(t, err)
		require.NoError(t, mfc.PutFile("../foo", strings.NewReader("foo\n")))
		require.YesError(t, mfc.Close())

		fis, err := env.PachClient.ListFileAll(masterCommit, "")
		require.NoError(t, err)
		require.Equal(t, 0, len(fis))

		mfc, err = env.PachClient.NewModifyFileClient(masterCommit)
		require.NoError(t, err)
		require.NoError(t, mfc.PutFile("foo/../../bar", strings.NewReader("foo\n")))
		require.YesError(t, mfc.Close())

		mfc, err = env.PachClient.NewModifyFileClient(masterCommit)
		require.NoError(t, err)
		require.NoError(t, mfc.PutFile("foo/../bar", strings.NewReader("foo\n")))
		require.YesError(t, mfc.Close())

		fis, err = env.PachClient.ListFileAll(masterCommit, "")
		require.NoError(t, err)
		require.Equal(t, 0, len(fis))
	})

	suite.Run("CreateInvalidBranchName", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		// Create a branch that's the same length as a commit ID
		_, err := env.PachClient.StartCommit(repo, uuid.NewWithoutDashes())
		require.YesError(t, err)
	})

	suite.Run("DeleteRepo", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		numRepos := 10
		repoNames := make(map[string]bool)
		for i := 0; i < numRepos; i++ {
			repo := fmt.Sprintf("repo%d", i)
			require.NoError(t, env.PachClient.CreateRepo(repo))
			repoNames[repo] = true
		}

		reposToRemove := 5
		for i := 0; i < reposToRemove; i++ {
			// Pick one random element from repoNames
			for repoName := range repoNames {
				require.NoError(t, env.PachClient.DeleteRepo(repoName, false))
				delete(repoNames, repoName)
				break
			}
		}

		repoInfos, err := env.PachClient.ListRepo()
		require.NoError(t, err)

		for _, repoInfo := range repoInfos {
			require.True(t, repoNames[repoInfo.Repo.Name])
		}

		require.Equal(t, len(repoInfos), numRepos-reposToRemove)
	})

	suite.Run("DeleteRepoProvenance", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		// Create two repos, one as another's provenance
		require.NoError(t, env.PachClient.CreateRepo("A"))
		require.NoError(t, env.PachClient.CreateRepo("B"))
		require.NoError(t, env.PachClient.CreateBranch("B", "master", "", "", []*pfs.Branch{pclient.NewBranch("A", "master")}))

		commit, err := env.PachClient.StartCommit("A", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("A", commit.Branch.Name, commit.ID))

		// Delete the provenance repo; that should fail.
		require.YesError(t, env.PachClient.DeleteRepo("A", false))

		// Delete the leaf repo, then the provenance repo; that should succeed
		require.NoError(t, env.PachClient.DeleteRepo("B", false))

		// Should be in a consistent state after B is deleted
		require.NoError(t, env.PachClient.FsckFastExit())

		require.NoError(t, env.PachClient.DeleteRepo("A", false))

		repoInfos, err := env.PachClient.ListRepo()
		require.NoError(t, err)
		require.Equal(t, 0, len(repoInfos))

		// Create two repos again
		require.NoError(t, env.PachClient.CreateRepo("A"))
		require.NoError(t, env.PachClient.CreateRepo("B"))
		require.NoError(t, env.PachClient.CreateBranch("B", "master", "", "", []*pfs.Branch{pclient.NewBranch("A", "master")}))

		// Force delete should succeed
		require.NoError(t, env.PachClient.DeleteRepo("A", true))

		repoInfos, err = env.PachClient.ListRepo()
		require.NoError(t, err)
		require.Equal(t, 1, len(repoInfos))

		// Everything should be consistent
		require.NoError(t, env.PachClient.FsckFastExit())
	})

	suite.Run("InspectCommit", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		started := time.Now()
		commit, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)

		fileContent := "foo\n"
		require.NoError(t, env.PachClient.PutFile(commit, "foo", strings.NewReader(fileContent)))

		commitInfo, err := env.PachClient.InspectCommit(repo, commit.Branch.Name, commit.ID)
		require.NoError(t, err)

		tStarted, err := types.TimestampFromProto(commitInfo.Started)
		require.NoError(t, err)

		require.Equal(t, commit, commitInfo.Commit)
		require.Nil(t, commitInfo.Finished)
		// PutFile does not update commit size; only FinishCommit does
		require.Equal(t, 0, int(commitInfo.SizeBytes))
		require.True(t, started.Before(tStarted))
		require.Nil(t, commitInfo.Finished)

		require.NoError(t, env.PachClient.FinishCommit(repo, commit.Branch.Name, commit.ID))
		finished := time.Now()

		commitInfo, err = env.PachClient.InspectCommit(repo, commit.Branch.Name, commit.ID)
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
	})

	suite.Run("InspectCommitBlock", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))
		commit, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)

		var eg errgroup.Group
		eg.Go(func() error {
			time.Sleep(2 * time.Second)
			return env.PachClient.FinishCommit(repo, commit.Branch.Name, commit.ID)
		})

		commitInfo, err := env.PachClient.BlockCommit(commit.Branch.Repo.Name, commit.Branch.Name, commit.ID)
		require.NoError(t, err)
		require.NotNil(t, commitInfo.Finished)

		require.NoError(t, eg.Wait())
	})

	suite.Run("SquashCommit", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		commit1, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)

		fileContent := "foo\n"
		require.NoError(t, env.PachClient.PutFile(commit1, "foo", strings.NewReader(fileContent)))

		require.NoError(t, env.PachClient.FinishCommit(repo, "master", ""))

		commit2, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)

		require.NoError(t, env.PachClient.SquashCommit(repo, commit2.Branch.Name, commit2.ID))

		_, err = env.PachClient.InspectCommit(repo, commit2.Branch.Name, commit2.ID)
		require.YesError(t, err)

		// Check that the head has been set to the parent
		commitInfo, err := env.PachClient.InspectCommit(repo, "master", "")
		require.NoError(t, err)
		require.Equal(t, commit1.ID, commitInfo.Commit.ID)

		// Check that the branch still exists
		branchInfos, err := env.PachClient.ListBranch(repo)
		require.NoError(t, err)
		require.Equal(t, 1, len(branchInfos))
	})

	suite.Run("SquashCommitOnlyCommitInBranch", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))
		repoProto := client.NewRepo(repo)

		commit, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(commit, "foo", strings.NewReader("foo\n")))
		require.NoError(t, env.PachClient.SquashCommit(repo, "master", ""))

		// The branch has not been deleted, though it has no commits
		branchInfos, err := env.PachClient.ListBranch(repo)
		require.NoError(t, err)
		require.Equal(t, 1, len(branchInfos))
		commits, err := env.PachClient.ListCommit(repoProto, repoProto.NewCommit("master", ""), nil, 0)
		require.NoError(t, err)
		require.Equal(t, 0, len(commits))

		// Check that repo size is back to 0
		repoInfo, err := env.PachClient.InspectRepo(repo)
		require.NoError(t, err)
		require.Equal(t, 0, int(repoInfo.SizeBytes))
	})

	suite.Run("SquashCommitFinished", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))
		repoProto := client.NewRepo(repo)

		commit, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(commit, "foo", strings.NewReader("foo\n")))
		require.NoError(t, env.PachClient.FinishCommit(repo, commit.Branch.Name, commit.ID))
		require.NoError(t, env.PachClient.SquashCommit(repo, "master", ""))

		// The branch has not been deleted, though it has no commits
		branchInfos, err := env.PachClient.ListBranch(repo)
		require.NoError(t, err)
		require.Equal(t, 1, len(branchInfos))
		commits, err := env.PachClient.ListCommit(repoProto, repoProto.NewCommit("master", ""), nil, 0)
		require.NoError(t, err)
		require.Equal(t, 0, len(commits))

		// Check that repo size is back to 0
		repoInfo, err := env.PachClient.InspectRepo(repo)
		require.NoError(t, err)
		require.Equal(t, 0, int(repoInfo.SizeBytes))
	})

	suite.Run("BasicFile", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		repo := "repo"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		commit, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)

		file := "file"
		data := "data"
		require.NoError(t, env.PachClient.PutFile(commit, file, strings.NewReader(data)))

		require.NoError(t, env.PachClient.FinishCommit(repo, commit.Branch.Name, commit.ID))

		var b bytes.Buffer
		require.NoError(t, env.PachClient.GetFile(commit, "file", &b))
		require.Equal(t, data, b.String())
	})

	suite.Run("SimpleFile", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		commit1, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(commit1, "foo", strings.NewReader("foo\n"), pclient.WithAppendPutFile()))
		require.NoError(t, env.PachClient.FinishCommit(repo, commit1.Branch.Name, commit1.ID))

		var buffer bytes.Buffer
		require.NoError(t, env.PachClient.GetFile(commit1, "foo", &buffer))
		require.Equal(t, "foo\n", buffer.String())

		commit2, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(commit2, "foo", strings.NewReader("foo\n"), pclient.WithAppendPutFile()))
		err = env.PachClient.FinishCommit(repo, commit2.Branch.Name, commit2.ID)
		require.NoError(t, err)

		buffer.Reset()
		require.NoError(t, env.PachClient.GetFile(commit1, "foo", &buffer))
		require.Equal(t, "foo\n", buffer.String())
		buffer.Reset()
		require.NoError(t, env.PachClient.GetFile(commit2, "foo", &buffer))
		require.Equal(t, "foo\nfoo\n", buffer.String())
	})

	suite.Run("StartCommitWithUnfinishedParent", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		commit1, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		_, err = env.PachClient.StartCommit(repo, "master")
		// fails because the parent commit has not been finished
		require.YesError(t, err)

		require.NoError(t, env.PachClient.FinishCommit(repo, commit1.Branch.Name, commit1.ID))
		_, err = env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
	})

	suite.Run("StartCommitWithDuplicatedCommitProvenance", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		commit1, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)

		require.NoError(t, env.PachClient.FinishCommit(repo, commit1.Branch.Name, commit1.ID))

		commit2, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)

		_, err = env.PachClient.PfsAPIClient.StartCommit(env.PachClient.Ctx(), &pfs.StartCommitRequest{
			Branch:     pclient.NewBranch(repo, "foo"),
			Provenance: []*pfs.CommitProvenance{commit2.NewProvenance(), commit2.NewProvenance()},
		})

		require.NoError(t, err)
	})

	suite.Run("AncestrySyntax", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		commit1, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(commit1, "file", strings.NewReader("1")))
		require.NoError(t, env.PachClient.FinishCommit(repo, commit1.Branch.Name, commit1.ID))

		commit2, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(commit2, "file", strings.NewReader("2")))
		require.NoError(t, env.PachClient.FinishCommit(repo, commit2.Branch.Name, commit2.ID))

		commit3, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(commit3, "file", strings.NewReader("3")))
		require.NoError(t, env.PachClient.FinishCommit(repo, commit3.Branch.Name, commit3.ID))

		commitInfo, err := env.PachClient.InspectCommit(repo, "", "master^")
		require.NoError(t, err)
		require.Equal(t, commit2, commitInfo.Commit)

		commitInfo, err = env.PachClient.InspectCommit(repo, "", "master~")
		require.NoError(t, err)
		require.Equal(t, commit2, commitInfo.Commit)

		commitInfo, err = env.PachClient.InspectCommit(repo, "", "master^1")
		require.NoError(t, err)
		require.Equal(t, commit2, commitInfo.Commit)

		commitInfo, err = env.PachClient.InspectCommit(repo, "", "master~1")
		require.NoError(t, err)
		require.Equal(t, commit2, commitInfo.Commit)

		commitInfo, err = env.PachClient.InspectCommit(repo, "", "master^^")
		require.NoError(t, err)
		require.Equal(t, commit1, commitInfo.Commit)

		commitInfo, err = env.PachClient.InspectCommit(repo, "", "master~~")
		require.NoError(t, err)
		require.Equal(t, commit1, commitInfo.Commit)

		commitInfo, err = env.PachClient.InspectCommit(repo, "", "master^2")
		require.NoError(t, err)
		require.Equal(t, commit1, commitInfo.Commit)

		commitInfo, err = env.PachClient.InspectCommit(repo, "", "master~2")
		require.NoError(t, err)
		require.Equal(t, commit1, commitInfo.Commit)

		commitInfo, err = env.PachClient.InspectCommit(repo, "", "master.1")
		require.NoError(t, err)
		require.Equal(t, commit1, commitInfo.Commit)

		commitInfo, err = env.PachClient.InspectCommit(repo, "", "master.2")
		require.NoError(t, err)
		require.Equal(t, commit2, commitInfo.Commit)

		commitInfo, err = env.PachClient.InspectCommit(repo, "", "master.3")
		require.NoError(t, err)
		require.Equal(t, commit3, commitInfo.Commit)

		_, err = env.PachClient.InspectCommit(repo, "", "master^^^")
		require.YesError(t, err)

		_, err = env.PachClient.InspectCommit(repo, "", "master~~~")
		require.YesError(t, err)

		_, err = env.PachClient.InspectCommit(repo, "", "master^3")
		require.YesError(t, err)

		_, err = env.PachClient.InspectCommit(repo, "", "master~3")
		require.YesError(t, err)

		for i := 1; i <= 2; i++ {
			_, err := env.PachClient.InspectFile(client.NewCommit(repo, "", fmt.Sprintf("%v^%v", commit3.ID, 3-i)), "file")
			require.NoError(t, err)
		}

		var buffer bytes.Buffer
		require.NoError(t, env.PachClient.GetFile(client.NewCommit(repo, "", ancestry.Add("master", 0)), "file", &buffer))
		require.Equal(t, "3", buffer.String())
		buffer.Reset()
		require.NoError(t, env.PachClient.GetFile(client.NewCommit(repo, "", ancestry.Add("master", 1)), "file", &buffer))
		require.Equal(t, "2", buffer.String())
		buffer.Reset()
		require.NoError(t, env.PachClient.GetFile(client.NewCommit(repo, "", ancestry.Add("master", 2)), "file", &buffer))
		require.Equal(t, "1", buffer.String())
		buffer.Reset()
		require.NoError(t, env.PachClient.GetFile(client.NewCommit(repo, "", ancestry.Add("master", -1)), "file", &buffer))
		require.Equal(t, "1", buffer.String())
		buffer.Reset()
		require.NoError(t, env.PachClient.GetFile(client.NewCommit(repo, "", ancestry.Add("master", -2)), "file", &buffer))
		require.Equal(t, "2", buffer.String())
		buffer.Reset()
		require.NoError(t, env.PachClient.GetFile(client.NewCommit(repo, "", ancestry.Add("master", -3)), "file", &buffer))
		require.Equal(t, "3", buffer.String())

		// Adding a bunch of commits to the head of the branch shouldn't change the forward references.
		// (It will change backward references.)
		for i := 0; i < 10; i++ {
			require.NoError(t, env.PachClient.PutFile(client.NewCommit(repo, "master", ""), "file", strings.NewReader(fmt.Sprintf("%d", i+4))))
		}
		commitInfo, err = env.PachClient.InspectCommit(repo, "", "master.1")
		require.NoError(t, err)
		require.Equal(t, commit1, commitInfo.Commit)

		commitInfo, err = env.PachClient.InspectCommit(repo, "", "master.2")
		require.NoError(t, err)
		require.Equal(t, commit2, commitInfo.Commit)

		commitInfo, err = env.PachClient.InspectCommit(repo, "", "master.3")
		require.NoError(t, err)
		require.Equal(t, commit3, commitInfo.Commit)
	})

	// Provenance implements the following DAG
	//  A ─▶ B ─▶ C ─▶ D
	//            ▲
	//  E ────────╯

	suite.Run("Provenance", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		require.NoError(t, env.PachClient.CreateRepo("A"))
		require.NoError(t, env.PachClient.CreateRepo("B"))
		require.NoError(t, env.PachClient.CreateRepo("C"))
		require.NoError(t, env.PachClient.CreateRepo("D"))
		require.NoError(t, env.PachClient.CreateRepo("E"))

		require.NoError(t, env.PachClient.CreateBranch("B", "master", "", "", []*pfs.Branch{pclient.NewBranch("A", "master")}))
		require.NoError(t, env.PachClient.CreateBranch("C", "master", "", "", []*pfs.Branch{pclient.NewBranch("B", "master"), pclient.NewBranch("E", "master")}))
		require.NoError(t, env.PachClient.CreateBranch("D", "master", "", "", []*pfs.Branch{pclient.NewBranch("C", "master")}))

		branchInfo, err := env.PachClient.InspectBranch("B", "master")
		require.NoError(t, err)
		require.Equal(t, 1, len(branchInfo.Provenance))
		branchInfo, err = env.PachClient.InspectBranch("C", "master")
		require.NoError(t, err)
		require.Equal(t, 3, len(branchInfo.Provenance))
		branchInfo, err = env.PachClient.InspectBranch("D", "master")
		require.NoError(t, err)
		require.Equal(t, 4, len(branchInfo.Provenance))

		ACommit, err := env.PachClient.StartCommit("A", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("A", ACommit.Branch.Name, ACommit.ID))
		ECommit, err := env.PachClient.StartCommit("E", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("E", ECommit.Branch.Name, ECommit.ID))

		commitInfo, err := env.PachClient.InspectCommit("B", "master", "")
		require.NoError(t, err)
		require.Equal(t, 1, len(commitInfo.Provenance))

		commitInfo, err = env.PachClient.InspectCommit("C", "master", "")
		require.NoError(t, err)
		require.Equal(t, 3, len(commitInfo.Provenance))

		commitInfo, err = env.PachClient.InspectCommit("D", "master", "")
		require.NoError(t, err)
		require.Equal(t, 4, len(commitInfo.Provenance))
	})

	suite.Run("StartCommitWithBranchNameProvenance", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		require.NoError(t, env.PachClient.CreateRepo("A"))
		require.NoError(t, env.PachClient.CreateRepo("B"))
		require.NoError(t, env.PachClient.CreateRepo("C"))

		require.NoError(t, env.PachClient.CreateBranch("B", "master", "", "", []*pfs.Branch{pclient.NewBranch("A", "master")}))
		require.NoError(t, env.PachClient.CreateBranch("C", "master", "", "", []*pfs.Branch{pclient.NewBranch("B", "master")}))

		masterCommit, err := env.PachClient.StartCommit("A", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("A", masterCommit.Branch.Name, masterCommit.ID))

		masterCommitInfo, err := env.PachClient.InspectCommit(masterCommit.Branch.Repo.Name, masterCommit.Branch.Name, masterCommit.ID)
		require.NoError(t, err)

		bCommitInfo, err := env.PachClient.InspectCommit("B", "master", "")
		require.NoError(t, err)

		// We're specifying the same commit three times - once by branch name, once
		// by commit ID, and once indirectly through B, these should be collapsed to
		// one provenance entry.
		newCommit, err := env.PachClient.PfsAPIClient.StartCommit(env.Context, &pfs.StartCommitRequest{
			Branch: pclient.NewBranch("C", "foo"),
			Provenance: []*pfs.CommitProvenance{
				pclient.NewCommitProvenance("A", "master", ""),
				masterCommitInfo.Commit.NewProvenance(),
				bCommitInfo.Commit.NewProvenance(),
			},
		})
		require.NoError(t, err)

		newCommitInfo, err := env.PachClient.InspectCommit(newCommit.Branch.Repo.Name, newCommit.Branch.Name, newCommit.ID)
		require.NoError(t, err)

		// Stupid require.ElementsEqual can't handle arrays of pointers
		expectedProvenanceA := &pfs.CommitProvenance{Commit: masterCommitInfo.Commit}
		expectedProvenanceB := &pfs.CommitProvenance{Commit: bCommitInfo.Commit}
		require.Equal(t, 2, len(newCommitInfo.Provenance))
		if newCommitInfo.Provenance[0].Commit.Branch.Repo.Name == "A" {
			require.Equal(t, expectedProvenanceA, newCommitInfo.Provenance[0])
			require.Equal(t, expectedProvenanceB, newCommitInfo.Provenance[1])
		} else {
			require.Equal(t, expectedProvenanceB, newCommitInfo.Provenance[0])
			require.Equal(t, expectedProvenanceA, newCommitInfo.Provenance[1])
		}
	})

	suite.Run("CommitBranch", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		require.NoError(t, env.PachClient.CreateRepo("repo"))
		// Make two branches provenant on the master branch
		require.NoError(t, env.PachClient.CreateBranch("repo", "A", "", "", []*pfs.Branch{pclient.NewBranch("repo", "master")}))
		require.NoError(t, env.PachClient.CreateBranch("repo", "B", "", "", []*pfs.Branch{pclient.NewBranch("repo", "master")}))

		// Now make a commit on the master branch, which should trigger a downstream commit on each of the two branches
		masterCommit, err := env.PachClient.StartCommit("repo", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("repo", masterCommit.Branch.Name, masterCommit.ID))

		// Check that the commit in branch A has the information and provenance we expect
		commitInfo, err := env.PachClient.InspectCommit("repo", "A", "")
		require.NoError(t, err)
		require.Equal(t, "A", commitInfo.Commit.Branch.Name)
		require.Equal(t, 1, len(commitInfo.Provenance))
		require.Equal(t, "master", commitInfo.Provenance[0].Commit.Branch.Name)

		// Check that the commit in branch B has the information and provenance we expect
		commitInfo, err = env.PachClient.InspectCommit("repo", "B", "")
		require.NoError(t, err)
		require.Equal(t, "B", commitInfo.Commit.Branch.Name)
		require.Equal(t, 1, len(commitInfo.Provenance))
		require.Equal(t, "master", commitInfo.Provenance[0].Commit.Branch.Name)
	})

	suite.Run("CommitOnTwoBranchesProvenance", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		require.NoError(t, env.PachClient.CreateRepo("repo"))

		parentCommit, err := env.PachClient.StartCommit("repo", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("repo", parentCommit.Branch.Name, parentCommit.ID))

		masterCommit, err := env.PachClient.StartCommit("repo", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("repo", masterCommit.Branch.Name, masterCommit.ID))

		// Make two branches provenant on the same commit on the master branch
		require.NoError(t, env.PachClient.CreateBranch("repo", "A", "", masterCommit.ID, nil))
		require.NoError(t, env.PachClient.CreateBranch("repo", "B", "", masterCommit.ID, nil))

		// Now create a branch provenant on both branches A and B
		require.NoError(t, env.PachClient.CreateBranch("repo", "C", "", "", []*pfs.Branch{pclient.NewBranch("repo", "A"), pclient.NewBranch("repo", "B")}))

		// The head commit of the C branch should have branches A and B both represented in the provenance
		// This is important because jobInput looks up commits by branch
		ci, err := env.PachClient.InspectCommit("repo", "C", "")
		require.NoError(t, err)
		require.Equal(t, 2, len(ci.Provenance))

		// We should also be able to delete the head commit of A
		require.NoError(t, env.PachClient.SquashCommit("repo", "A", ""))

		// And the head of branch B should go back to the parent of the deleted commit
		branchInfo, err := env.PachClient.InspectBranch("repo", "B")
		require.NoError(t, err)
		require.Equal(t, parentCommit.ID, branchInfo.Head.ID)

		// We should also be able to delete the head commit of A
		require.NoError(t, env.PachClient.SquashCommit("repo", parentCommit.Branch.Name, parentCommit.ID))

		// It should also be ok to make new commits on branches A and B
		aCommit, err := env.PachClient.StartCommit("repo", "A")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("repo", aCommit.Branch.Name, aCommit.ID))

		bCommit, err := env.PachClient.StartCommit("repo", "B")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("repo", bCommit.Branch.Name, bCommit.ID))
	})

	suite.Run("Branch1", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))
		masterCommit := client.NewCommit(repo, "master", "")
		commit, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(masterCommit, "foo", strings.NewReader("foo\n"), pclient.WithAppendPutFile()))
		require.NoError(t, env.PachClient.FinishCommit(repo, "master", ""))
		var buffer bytes.Buffer
		require.NoError(t, env.PachClient.GetFile(masterCommit, "foo", &buffer))
		require.Equal(t, "foo\n", buffer.String())
		branchInfos, err := env.PachClient.ListBranch(repo)
		require.NoError(t, err)
		require.Equal(t, 1, len(branchInfos))
		require.Equal(t, "master", branchInfos[0].Branch.Name)

		_, err = env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(masterCommit, "foo", strings.NewReader("foo\n"), pclient.WithAppendPutFile()))
		require.NoError(t, env.PachClient.FinishCommit(repo, "master", ""))
		buffer = bytes.Buffer{}
		require.NoError(t, env.PachClient.GetFile(masterCommit, "foo", &buffer))
		require.Equal(t, "foo\nfoo\n", buffer.String())
		branchInfos, err = env.PachClient.ListBranch(repo)
		require.NoError(t, err)
		require.Equal(t, 1, len(branchInfos))
		require.Equal(t, "master", branchInfos[0].Branch.Name)

		require.NoError(t, env.PachClient.CreateBranch(repo, "master2", commit.Branch.Name, commit.ID, nil))
		require.NoError(t, env.PachClient.CreateBranch(repo, "master3", "", commit.ID, nil))

		branchInfos, err = env.PachClient.ListBranch(repo)
		require.NoError(t, err)
		require.Equal(t, 3, len(branchInfos))
		require.Equal(t, "master3", branchInfos[0].Branch.Name)
		require.Equal(t, "master2", branchInfos[1].Branch.Name)
		require.Equal(t, "master", branchInfos[2].Branch.Name)
	})

	suite.Run("PutFileBig", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		// Write a big blob that would normally not fit in a block
		fileSize := int(pfs.ChunkSize + 5*1024*1024)
		expectedOutputA := random.String(fileSize)
		r := strings.NewReader(string(expectedOutputA))

		commit1, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(commit1, "foo", r))
		require.NoError(t, env.PachClient.FinishCommit(repo, commit1.Branch.Name, commit1.ID))

		fileInfo, err := env.PachClient.InspectFile(commit1, "foo")
		require.NoError(t, err)
		require.Equal(t, fileSize, int(fileInfo.SizeBytes))

		var buffer bytes.Buffer
		require.NoError(t, env.PachClient.GetFile(commit1, "foo", &buffer))
		require.Equal(t, string(expectedOutputA), buffer.String())
	})

	suite.Run("PutFile", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))
		masterCommit := client.NewCommit(repo, "master", "")
		require.NoError(t, env.PachClient.PutFile(masterCommit, "file", strings.NewReader("foo")))
		var buf bytes.Buffer
		require.NoError(t, env.PachClient.GetFile(masterCommit, "file", &buf))
		require.Equal(t, "foo", buf.String())
		require.NoError(t, env.PachClient.PutFile(masterCommit, "file", strings.NewReader("bar")))
		buf.Reset()
		require.NoError(t, env.PachClient.GetFile(masterCommit, "file", &buf))
		require.Equal(t, "bar", buf.String())
		require.NoError(t, env.PachClient.DeleteFile(masterCommit, "file"))
		require.NoError(t, env.PachClient.PutFile(masterCommit, "file", strings.NewReader("buzz")))
		buf.Reset()
		require.NoError(t, env.PachClient.GetFile(masterCommit, "file", &buf))
		require.Equal(t, "buzz", buf.String())
	})

	suite.Run("PutFile2", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))
		commit1, err := env.PachClient.StartCommit(repo, "master")
		masterCommit := client.NewCommit(repo, "master", "")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(commit1, "file", strings.NewReader("foo\n"), pclient.WithAppendPutFile()))
		require.NoError(t, env.PachClient.PutFile(commit1, "file", strings.NewReader("bar\n"), pclient.WithAppendPutFile()))
		require.NoError(t, env.PachClient.PutFile(masterCommit, "file", strings.NewReader("buzz\n"), pclient.WithAppendPutFile()))
		require.NoError(t, env.PachClient.FinishCommit(repo, commit1.Branch.Name, commit1.ID))

		expected := "foo\nbar\nbuzz\n"
		buffer := &bytes.Buffer{}
		require.NoError(t, env.PachClient.GetFile(commit1, "file", buffer))
		require.Equal(t, expected, buffer.String())
		buffer.Reset()
		require.NoError(t, env.PachClient.GetFile(masterCommit, "file", buffer))
		require.Equal(t, expected, buffer.String())

		commit2, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(commit2, "file", strings.NewReader("foo\n"), pclient.WithAppendPutFile()))
		require.NoError(t, env.PachClient.PutFile(commit2, "file", strings.NewReader("bar\n"), pclient.WithAppendPutFile()))
		require.NoError(t, env.PachClient.PutFile(masterCommit, "file", strings.NewReader("buzz\n"), pclient.WithAppendPutFile()))
		require.NoError(t, env.PachClient.FinishCommit(repo, "master", ""))

		expected = "foo\nbar\nbuzz\nfoo\nbar\nbuzz\n"
		buffer.Reset()
		require.NoError(t, env.PachClient.GetFile(commit2, "file", buffer))
		require.Equal(t, expected, buffer.String())
		buffer.Reset()
		require.NoError(t, env.PachClient.GetFile(masterCommit, "file", buffer))
		require.Equal(t, expected, buffer.String())

		commit3, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.CreateBranch(repo, "foo", "", commit3.ID, nil))
		fooCommit := client.NewCommit(repo, "foo", "")
		require.NoError(t, env.PachClient.PutFile(fooCommit, "file", strings.NewReader("foo\nbar\nbuzz\n"), pclient.WithAppendPutFile()))
		require.NoError(t, env.PachClient.FinishCommit(repo, "foo", ""))

		expected = "foo\nbar\nbuzz\nfoo\nbar\nbuzz\nfoo\nbar\nbuzz\n"
		buffer.Reset()
		require.NoError(t, env.PachClient.GetFile(fooCommit, "file", buffer))
		require.Equal(t, expected, buffer.String())
	})

	suite.Run("PutFileBranchCommitID", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		err := env.PachClient.PutFile(client.NewCommit(repo, "", "master"), "foo", strings.NewReader("foo\n"), pclient.WithAppendPutFile())
		require.NoError(t, err)
	})

	suite.Run("PutSameFileInParallel", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		commit, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		var eg errgroup.Group
		for i := 0; i < 3; i++ {
			eg.Go(func() error {
				return env.PachClient.PutFile(commit, "foo", strings.NewReader("foo\n"), pclient.WithAppendPutFile())
			})
		}
		require.NoError(t, eg.Wait())
		require.NoError(t, env.PachClient.FinishCommit(repo, commit.Branch.Name, commit.ID))

		var buffer bytes.Buffer
		require.NoError(t, env.PachClient.GetFile(commit, "foo", &buffer))
		require.Equal(t, "foo\nfoo\nfoo\n", buffer.String())
	})

	suite.Run("InspectFile", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		fileContent1 := "foo\n"
		commit1, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(commit1, "foo", strings.NewReader(fileContent1), pclient.WithAppendPutFile()))
		checks := func() {
			fileInfo, err := env.PachClient.InspectFile(commit1, "foo")
			require.NoError(t, err)
			require.Equal(t, pfs.FileType_FILE, fileInfo.FileType)
			require.Equal(t, len(fileContent1), int(fileInfo.SizeBytes))
		}
		checks()
		require.NoError(t, env.PachClient.FinishCommit(repo, commit1.Branch.Name, commit1.ID))
		checks()

		fileContent2 := "barbar\n"
		commit2, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(commit2, "foo", strings.NewReader(fileContent2), pclient.WithAppendPutFile()))

		require.NoError(t, env.PachClient.FinishCommit(repo, commit2.Branch.Name, commit2.ID))

		fileInfo, err := env.PachClient.InspectFile(commit2, "foo")
		require.NoError(t, err)
		require.Equal(t, pfs.FileType_FILE, fileInfo.FileType)
		require.Equal(t, len(fileContent1+fileContent2), int(fileInfo.SizeBytes))

		fileInfo, err = env.PachClient.InspectFile(commit2, "foo")
		require.NoError(t, err)
		require.Equal(t, pfs.FileType_FILE, fileInfo.FileType)
		require.Equal(t, len(fileContent1)+len(fileContent2), int(fileInfo.SizeBytes))

		fileContent3 := "bar\n"
		commit3, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(commit3, "bar", strings.NewReader(fileContent3), pclient.WithAppendPutFile()))
		require.NoError(t, env.PachClient.FinishCommit(repo, commit3.Branch.Name, commit3.ID))

		fis, err := env.PachClient.ListFileAll(commit3, "")
		require.NoError(t, err)
		require.Equal(t, 2, len(fis))

		require.Equal(t, len(fis), 2)
	})

	suite.Run("InspectFile2", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))
		commit := client.NewCommit(repo, "master", "")

		fileContent1 := "foo\n"
		fileContent2 := "buzz\n"

		_, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(commit, "file", strings.NewReader(fileContent1), pclient.WithAppendPutFile()))
		require.NoError(t, env.PachClient.FinishCommit(repo, "master", ""))

		fileInfo, err := env.PachClient.InspectFile(commit, "/file")
		require.NoError(t, err)
		require.Equal(t, len(fileContent1), int(fileInfo.SizeBytes))
		require.Equal(t, "/file", fileInfo.File.Path)
		require.Equal(t, pfs.FileType_FILE, fileInfo.FileType)

		_, err = env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(commit, "file", strings.NewReader(fileContent1), pclient.WithAppendPutFile()))
		require.NoError(t, env.PachClient.FinishCommit(repo, "master", ""))

		fileInfo, err = env.PachClient.InspectFile(commit, "file")
		require.NoError(t, err)
		require.Equal(t, len(fileContent1)*2, int(fileInfo.SizeBytes))
		require.Equal(t, "/file", fileInfo.File.Path)

		_, err = env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.DeleteFile(commit, "file"))
		require.NoError(t, env.PachClient.PutFile(commit, "file", strings.NewReader(fileContent2), pclient.WithAppendPutFile()))
		require.NoError(t, env.PachClient.FinishCommit(repo, "master", ""))

		fileInfo, err = env.PachClient.InspectFile(commit, "file")
		require.NoError(t, err)
		require.Equal(t, len(fileContent2), int(fileInfo.SizeBytes))
	})

	suite.Run("InspectFile3", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		fileContent1 := "foo\n"
		commit1, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(commit1, "foo/bar", strings.NewReader(fileContent1)))
		fileInfo, err := env.PachClient.InspectFile(commit1, "foo")
		require.NoError(t, err)
		require.NotNil(t, fileInfo)

		require.NoError(t, env.PachClient.FinishCommit(repo, commit1.Branch.Name, commit1.ID))

		fi, err := env.PachClient.InspectFile(commit1, "foo/bar")
		require.NoError(t, err)
		require.NotNil(t, fi)

		fileContent2 := "barbar\n"
		commit2, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(commit2, "foo", strings.NewReader(fileContent2)))

		fileInfo, err = env.PachClient.InspectFile(commit2, "foo")
		require.NoError(t, err)
		require.NotNil(t, fileInfo)

		require.NoError(t, env.PachClient.FinishCommit(repo, commit2.Branch.Name, commit2.ID))

		fi, err = env.PachClient.InspectFile(commit2, "foo")
		require.NoError(t, err)
		require.NotNil(t, fi)

		fileContent3 := "bar\n"
		commit3, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(commit3, "bar", strings.NewReader(fileContent3)))
		require.NoError(t, env.PachClient.FinishCommit(repo, commit3.Branch.Name, commit3.ID))
		fi, err = env.PachClient.InspectFile(commit3, "bar")
		require.NoError(t, err)
		require.NotNil(t, fi)
	})

	suite.Run("InspectDir", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		commit1, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)

		fileContent := "foo\n"
		require.NoError(t, env.PachClient.PutFile(commit1, "dir/foo", strings.NewReader(fileContent)))

		require.NoError(t, env.PachClient.FinishCommit(repo, commit1.Branch.Name, commit1.ID))

		fileInfo, err := env.PachClient.InspectFile(commit1, "dir/foo")
		require.NoError(t, err)
		require.Equal(t, len(fileContent), int(fileInfo.SizeBytes))
		require.Equal(t, pfs.FileType_FILE, fileInfo.FileType)

		fileInfo, err = env.PachClient.InspectFile(commit1, "dir")
		require.NoError(t, err)
		require.Equal(t, len(fileContent), int(fileInfo.SizeBytes))
		require.Equal(t, pfs.FileType_DIR, fileInfo.FileType)

		_, err = env.PachClient.InspectFile(commit1, "")
		require.NoError(t, err)
		require.Equal(t, len(fileContent), int(fileInfo.SizeBytes))
		require.Equal(t, pfs.FileType_DIR, fileInfo.FileType)
	})

	suite.Run("InspectDir2", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))
		commit := client.NewCommit(repo, "master", "")

		fileContent := "foo\n"

		_, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(commit, "dir/1", strings.NewReader(fileContent)))
		require.NoError(t, env.PachClient.PutFile(commit, "dir/2", strings.NewReader(fileContent)))

		require.NoError(t, env.PachClient.FinishCommit(repo, "master", ""))

		fileInfo, err := env.PachClient.InspectFile(commit, "/dir")
		require.NoError(t, err)
		require.Equal(t, "/dir/", fileInfo.File.Path)
		require.Equal(t, pfs.FileType_DIR, fileInfo.FileType)

		_, err = env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(commit, "dir/3", strings.NewReader(fileContent)))

		require.NoError(t, env.PachClient.FinishCommit(repo, "master", ""))

		_, err = env.PachClient.InspectFile(commit, "dir")
		require.NoError(t, err)

		_, err = env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		err = env.PachClient.DeleteFile(commit, "dir/2")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit(repo, "master", ""))

		_, err = env.PachClient.InspectFile(commit, "dir")
		require.NoError(t, err)
	})

	suite.Run("ListFileTwoCommits", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		numFiles := 5

		commit1, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)

		for i := 0; i < numFiles; i++ {
			require.NoError(t, env.PachClient.PutFile(commit1, fmt.Sprintf("file%d", i), strings.NewReader("foo\n")))
		}

		require.NoError(t, env.PachClient.FinishCommit(repo, commit1.Branch.Name, commit1.ID))

		fis, err := env.PachClient.ListFileAll(commit1, "")
		require.NoError(t, err)
		require.Equal(t, numFiles, len(fis))

		commit2, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)

		for i := 0; i < numFiles; i++ {
			require.NoError(t, env.PachClient.PutFile(commit2, fmt.Sprintf("file2-%d", i), strings.NewReader("foo\n")))
		}

		require.NoError(t, env.PachClient.FinishCommit(repo, commit2.Branch.Name, commit2.ID))

		fis, err = env.PachClient.ListFileAll(commit2, "")
		require.NoError(t, err)
		require.Equal(t, 2*numFiles, len(fis))

		fis, err = env.PachClient.ListFileAll(commit1, "")
		require.NoError(t, err)
		require.Equal(t, numFiles, len(fis))

		fis, err = env.PachClient.ListFileAll(commit2, "")
		require.NoError(t, err)
		require.Equal(t, 2*numFiles, len(fis))
	})

	suite.Run("ListFile", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		commit, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)

		fileContent1 := "foo\n"
		require.NoError(t, env.PachClient.PutFile(commit, "dir/foo", strings.NewReader(fileContent1)))

		fileContent2 := "bar\n"
		require.NoError(t, env.PachClient.PutFile(commit, "dir/bar", strings.NewReader(fileContent2)))

		checks := func() {
			fileInfos, err := env.PachClient.ListFileAll(commit, "dir")
			require.NoError(t, err)
			require.Equal(t, 2, len(fileInfos))
			require.True(t, fileInfos[0].File.Path == "/dir/foo" && fileInfos[1].File.Path == "/dir/bar" || fileInfos[0].File.Path == "/dir/bar" && fileInfos[1].File.Path == "/dir/foo")
			require.True(t, fileInfos[0].SizeBytes == fileInfos[1].SizeBytes && fileInfos[0].SizeBytes == uint64(len(fileContent1)))

		}
		checks()
		require.NoError(t, env.PachClient.FinishCommit(repo, commit.Branch.Name, commit.ID))
		checks()
	})

	suite.Run("ListFile2", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))
		commit := client.NewCommit(repo, "master", "")

		fileContent := "foo\n"

		_, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(commit, "dir/1", strings.NewReader(fileContent)))
		require.NoError(t, env.PachClient.PutFile(commit, "dir/2", strings.NewReader(fileContent)))
		require.NoError(t, err)

		require.NoError(t, env.PachClient.FinishCommit(repo, "master", ""))

		fileInfos, err := env.PachClient.ListFileAll(commit, "dir")
		require.NoError(t, err)
		require.Equal(t, 2, len(fileInfos))

		_, err = env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(commit, "dir/3", strings.NewReader(fileContent)))
		require.NoError(t, env.PachClient.FinishCommit(repo, "master", ""))

		fileInfos, err = env.PachClient.ListFileAll(commit, "dir")
		require.NoError(t, err)
		require.Equal(t, 3, len(fileInfos))

		_, err = env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		err = env.PachClient.DeleteFile(commit, "dir/2")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit(repo, "master", ""))

		fileInfos, err = env.PachClient.ListFileAll(commit, "dir")
		require.NoError(t, err)
		require.Equal(t, 2, len(fileInfos))
	})

	suite.Run("ListFile3", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))
		commit := client.NewCommit(repo, "master", "")

		fileContent := "foo\n"

		_, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(commit, "dir/1", strings.NewReader(fileContent)))
		require.NoError(t, env.PachClient.PutFile(commit, "dir/2", strings.NewReader(fileContent)))
		require.NoError(t, env.PachClient.FinishCommit(repo, "master", ""))

		fileInfos, err := env.PachClient.ListFileAll(commit, "dir")
		require.NoError(t, err)
		require.Equal(t, 2, len(fileInfos))

		_, err = env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(commit, "dir/3/foo", strings.NewReader(fileContent)))
		require.NoError(t, env.PachClient.PutFile(commit, "dir/3/bar", strings.NewReader(fileContent)))
		require.NoError(t, env.PachClient.FinishCommit(repo, "master", ""))

		fileInfos, err = env.PachClient.ListFileAll(commit, "dir")
		require.NoError(t, err)
		require.Equal(t, 3, len(fileInfos))
		require.Equal(t, int(fileInfos[2].SizeBytes), len(fileContent)*2)

		_, err = env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		err = env.PachClient.DeleteFile(commit, "dir/3/bar")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit(repo, "master", ""))

		fileInfos, err = env.PachClient.ListFileAll(commit, "dir")
		require.NoError(t, err)
		require.Equal(t, 3, len(fileInfos))
		require.Equal(t, int(fileInfos[2].SizeBytes), len(fileContent))

		_, err = env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(commit, "file", strings.NewReader(fileContent)))
		require.NoError(t, env.PachClient.FinishCommit(repo, "master", ""))

		fileInfos, err = env.PachClient.ListFileAll(commit, "/")
		require.NoError(t, err)
		require.Equal(t, 2, len(fileInfos))
	})

	suite.Run("ListFile4", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		commit1, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)

		require.NoError(t, env.PachClient.PutFile(commit1, "/dir1/file1.1", &bytes.Buffer{}))
		require.NoError(t, env.PachClient.PutFile(commit1, "/dir1/file1.2", &bytes.Buffer{}))
		require.NoError(t, env.PachClient.PutFile(commit1, "/dir2/file2.1", &bytes.Buffer{}))
		require.NoError(t, env.PachClient.PutFile(commit1, "/dir2/file2.2", &bytes.Buffer{}))

		require.NoError(t, env.PachClient.FinishCommit(repo, commit1.Branch.Name, commit1.ID))
		// should list a directory but not siblings
		var fis []*pfs.FileInfo
		require.NoError(t, env.PachClient.ListFile(commit1, "/dir1", func(fi *pfs.FileInfo) error {
			fis = append(fis, fi)
			return nil
		}))
		require.ElementsEqual(t, []string{"/dir1/file1.1", "/dir1/file1.2"}, finfosToPaths(fis))
		// should list the root
		fis = nil
		require.NoError(t, env.PachClient.ListFile(commit1, "/", func(fi *pfs.FileInfo) error {
			fis = append(fis, fi)
			return nil
		}))
		require.ElementsEqual(t, []string{"/dir1/", "/dir2/"}, finfosToPaths(fis))
	})

	suite.Run("RootDirectory", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		fileContent := "foo\n"

		commit, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(commit, "foo", strings.NewReader(fileContent)))

		require.NoError(t, env.PachClient.FinishCommit(repo, commit.Branch.Name, commit.ID))

		fileInfos, err := env.PachClient.ListFileAll(commit, "")
		require.NoError(t, err)
		require.Equal(t, 1, len(fileInfos))
	})

	suite.Run("DeleteFile", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		// Commit 1: Add two files; delete one file within the commit
		commit1, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)

		fileContent1 := "foo\n"
		require.NoError(t, env.PachClient.PutFile(commit1, "foo", strings.NewReader(fileContent1)))

		fileContent2 := "bar\n"
		require.NoError(t, env.PachClient.PutFile(commit1, "bar", strings.NewReader(fileContent2)))

		require.NoError(t, env.PachClient.DeleteFile(commit1, "foo"))

		require.NoError(t, env.PachClient.FinishCommit(repo, commit1.Branch.Name, commit1.ID))

		_, err = env.PachClient.InspectFile(commit1, "foo")
		require.YesError(t, err)

		// Should see one file
		fileInfos, err := env.PachClient.ListFileAll(commit1, "")
		require.NoError(t, err)
		require.Equal(t, 1, len(fileInfos))

		// Deleting a file in a finished commit should result in an error
		require.YesError(t, env.PachClient.DeleteFile(commit1, "bar"))

		// Empty commit
		commit2, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit(repo, commit2.Branch.Name, commit2.ID))

		// Should still see one files
		fileInfos, err = env.PachClient.ListFileAll(commit2, "")
		require.NoError(t, err)
		require.Equal(t, 1, len(fileInfos))

		// Delete bar
		commit3, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.DeleteFile(commit3, "bar"))

		require.NoError(t, env.PachClient.FinishCommit(repo, commit3.Branch.Name, commit3.ID))

		// Should see no file
		fileInfos, err = env.PachClient.ListFileAll(commit3, "")
		require.NoError(t, err)
		require.Equal(t, 0, len(fileInfos))

		_, err = env.PachClient.InspectFile(commit3, "bar")
		require.YesError(t, err)

		// Delete a nonexistent file; it should be no-op
		commit4, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.DeleteFile(commit4, "nonexistent"))
		require.NoError(t, env.PachClient.FinishCommit(repo, commit4.Branch.Name, commit4.ID))
	})

	suite.Run("DeleteFile2", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		commit1, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(commit1, "file", strings.NewReader("foo\n")))
		require.NoError(t, env.PachClient.FinishCommit(repo, commit1.Branch.Name, commit1.ID))

		commit2, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		err = env.PachClient.DeleteFile(commit2, "file")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(commit2, "file", strings.NewReader("bar\n")))
		require.NoError(t, env.PachClient.FinishCommit(repo, commit2.Branch.Name, commit2.ID))

		expected := "bar\n"
		var buffer bytes.Buffer
		require.NoError(t, env.PachClient.GetFile(client.NewCommit(repo, "master", ""), "file", &buffer))
		require.Equal(t, expected, buffer.String())

		commit3, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(commit3, "file", strings.NewReader("buzz\n")))
		err = env.PachClient.DeleteFile(commit3, "file")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(commit3, "file", strings.NewReader("foo\n")))
		require.NoError(t, env.PachClient.FinishCommit(repo, commit3.Branch.Name, commit3.ID))

		expected = "foo\n"
		buffer.Reset()
		require.NoError(t, env.PachClient.GetFile(commit3, "file", &buffer))
		require.Equal(t, expected, buffer.String())
	})

	suite.Run("DeleteFile3", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))
		commit1, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		fileContent := "bar\n"
		require.NoError(t, env.PachClient.PutFile(commit1, "/bar", strings.NewReader(fileContent)))
		require.NoError(t, env.PachClient.PutFile(commit1, "/dir1/dir2/bar", strings.NewReader(fileContent)))
		require.NoError(t, env.PachClient.FinishCommit(repo, commit1.Branch.Name, commit1.ID))

		commit2, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.DeleteFile(commit2, "/"))
		require.NoError(t, env.PachClient.PutFile(commit2, "/bar", strings.NewReader(fileContent)))
		require.NoError(t, env.PachClient.PutFile(commit2, "/dir1/bar", strings.NewReader(fileContent)))
		require.NoError(t, env.PachClient.PutFile(commit2, "/dir1/dir2/bar", strings.NewReader(fileContent)))
		require.NoError(t, env.PachClient.PutFile(commit2, "/dir1/dir2/barbar", strings.NewReader(fileContent)))
		require.NoError(t, env.PachClient.FinishCommit(repo, commit2.Branch.Name, commit2.ID))

		commit3, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.DeleteFile(commit3, "/dir1/dir2/"))
		require.NoError(t, env.PachClient.FinishCommit(repo, commit3.Branch.Name, commit3.ID))

		_, err = env.PachClient.InspectFile(commit3, "/dir1")
		require.NoError(t, err)
		_, err = env.PachClient.InspectFile(commit3, "/dir1/bar")
		require.NoError(t, err)
		_, err = env.PachClient.InspectFile(commit3, "/dir1/dir2")
		require.YesError(t, err)
		_, err = env.PachClient.InspectFile(commit3, "/dir1/dir2/bar")
		require.YesError(t, err)
		_, err = env.PachClient.InspectFile(commit3, "/dir1/dir2/barbar")
		require.YesError(t, err)

		commit4, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(commit4, "/dir1/dir2/bar", strings.NewReader(fileContent)))
		require.NoError(t, env.PachClient.FinishCommit(repo, commit4.Branch.Name, commit4.ID))

		_, err = env.PachClient.InspectFile(commit4, "/dir1")
		require.NoError(t, err)
		_, err = env.PachClient.InspectFile(commit4, "/dir1/bar")
		require.NoError(t, err)
		_, err = env.PachClient.InspectFile(commit4, "/dir1/dir2")
		require.NoError(t, err)
		_, err = env.PachClient.InspectFile(commit4, "/dir1/dir2/bar")
		require.NoError(t, err)
	})

	suite.Run("DeleteDir", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		// Commit 1: Add two files into the same directory; delete the directory
		commit1, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)

		require.NoError(t, env.PachClient.PutFile(commit1, "dir/foo", strings.NewReader("foo1")))

		require.NoError(t, env.PachClient.PutFile(commit1, "dir/bar", strings.NewReader("bar1")))

		require.NoError(t, env.PachClient.DeleteFile(commit1, "/dir/"))

		require.NoError(t, env.PachClient.FinishCommit(repo, commit1.Branch.Name, commit1.ID))

		fileInfos, err := env.PachClient.ListFileAll(commit1, "")
		require.NoError(t, err)
		require.Equal(t, 0, len(fileInfos))

		// dir should not exist
		_, err = env.PachClient.InspectFile(commit1, "dir")
		require.YesError(t, err)

		// Commit 2: Delete the directory and add the same two files
		// The two files should reflect the new content
		commit2, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)

		require.NoError(t, env.PachClient.PutFile(commit2, "dir/foo", strings.NewReader("foo2")))

		require.NoError(t, env.PachClient.PutFile(commit2, "dir/bar", strings.NewReader("bar2")))

		require.NoError(t, env.PachClient.FinishCommit(repo, commit2.Branch.Name, commit2.ID))

		// Should see two files
		fileInfos, err = env.PachClient.ListFileAll(commit2, "dir")
		require.NoError(t, err)
		require.Equal(t, 2, len(fileInfos))

		var buffer bytes.Buffer
		require.NoError(t, env.PachClient.GetFile(commit2, "dir/foo", &buffer))
		require.Equal(t, "foo2", buffer.String())

		var buffer2 bytes.Buffer
		require.NoError(t, env.PachClient.GetFile(commit2, "dir/bar", &buffer2))
		require.Equal(t, "bar2", buffer2.String())

		// Commit 3: delete the directory
		commit3, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)

		require.NoError(t, env.PachClient.DeleteFile(commit3, "/dir/"))

		require.NoError(t, env.PachClient.FinishCommit(repo, commit3.Branch.Name, commit3.ID))

		// Should see zero files
		fileInfos, err = env.PachClient.ListFileAll(commit3, "")
		require.NoError(t, err)
		require.Equal(t, 0, len(fileInfos))

		// One-off commit directory deletion
		masterCommit := client.NewCommit(repo, "master", "")
		require.NoError(t, env.PachClient.PutFile(masterCommit, "/dir/foo", strings.NewReader("foo")))
		require.NoError(t, env.PachClient.DeleteFile(masterCommit, "/"))
		fileInfos, err = env.PachClient.ListFileAll(masterCommit, "/")
		require.NoError(t, err)
		require.Equal(t, 0, len(fileInfos))
	})

	suite.Run("ListCommit", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))
		repoProto := client.NewRepo(repo)
		masterCommit := repoProto.NewCommit("master", "")

		numCommits := 10

		var midCommitID string
		for i := 0; i < numCommits; i++ {
			commit, err := env.PachClient.StartCommit(repo, "master")
			require.NoError(t, err)
			require.NoError(t, env.PachClient.FinishCommit(repo, "master", ""))
			if i == numCommits/2 {
				midCommitID = commit.ID
			}
		}

		// list all commits
		commitInfos, err := env.PachClient.ListCommit(repoProto, nil, nil, 0)
		require.NoError(t, err)
		require.Equal(t, numCommits, len(commitInfos))

		// Test that commits are sorted in newest-first order
		for i := 0; i < len(commitInfos)-1; i++ {
			require.Equal(t, commitInfos[i].ParentCommit, commitInfos[i+1].Commit)
		}

		// Now list all commits up to the last commit
		commitInfos, err = env.PachClient.ListCommit(repoProto, masterCommit, nil, 0)
		require.NoError(t, err)
		require.Equal(t, numCommits, len(commitInfos))

		// Test that commits are sorted in newest-first order
		for i := 0; i < len(commitInfos)-1; i++ {
			require.Equal(t, commitInfos[i].ParentCommit, commitInfos[i+1].Commit)
		}

		// Now list all commits up to the mid commit, excluding the mid commit
		// itself
		commitInfos, err = env.PachClient.ListCommit(repoProto, masterCommit, repoProto.NewCommit("", midCommitID), 0)
		require.NoError(t, err)
		require.Equal(t, numCommits-numCommits/2-1, len(commitInfos))

		// Test that commits are sorted in newest-first order
		for i := 0; i < len(commitInfos)-1; i++ {
			require.Equal(t, commitInfos[i].ParentCommit, commitInfos[i+1].Commit)
		}

		// list commits by branch
		commitInfos, err = env.PachClient.ListCommit(repoProto, masterCommit, nil, 0)
		require.NoError(t, err)
		require.Equal(t, numCommits, len(commitInfos))

		// Test that commits are sorted in newest-first order
		for i := 0; i < len(commitInfos)-1; i++ {
			require.Equal(t, commitInfos[i].ParentCommit, commitInfos[i+1].Commit)
		}

		// Try listing the commits in reverse order
		commitInfos = nil
		require.NoError(t, env.PachClient.ListCommitF(repoProto, nil, nil, 0, true, func(ci *pfs.CommitInfo) error {
			commitInfos = append(commitInfos, ci)
			return nil
		}))
		for i := 1; i < len(commitInfos); i++ {
			require.Equal(t, commitInfos[i].ParentCommit, commitInfos[i-1].Commit)
		}
	})

	suite.Run("OffsetRead", func(t *testing.T) {
		// TODO(2.0 required): Decide on how to expose offset read.
		t.Skip("Offset read exists (inefficient), just need to decide on how to expose it in V2")
		//t.Parallel()
		//env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		//repo := "test"
		//require.NoError(t, env.PachClient.CreateRepo(repo))
		//commit, err := env.PachClient.StartCommit(repo, "")
		//require.NoError(t, err)
		//fileData := "foo\n"
		//require.NoError(t, env.PachClient.PutFile(commit, "foo", strings.NewReader(fileData)))
		//require.NoError(t, env.PachClient.PutFile(commit, "foo", strings.NewReader(fileData)))

		//var buffer bytes.Buffer
		//require.NoError(t, env.PachClient.GetFile(commit, "foo", int64(len(fileData)*2)+1, 0, &buffer))
		//require.Equal(t, "", buffer.String())

		//require.NoError(t, env.PachClient.FinishCommit(repo, commit.Branch.Name, commit.ID))

		//buffer.Reset()
		//require.NoError(t, env.PachClient.GetFile(commit, "foo", int64(len(fileData)*2)+1, 0, &buffer))
		//require.Equal(t, "", buffer.String())
	})

	suite.Run("Branch2", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		commit, err := env.PachClient.StartCommit(repo, "branch1")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit(repo, commit.Branch.Name, commit.ID))

		expectedBranches := []string{"branch1", "branch2", "branch3"}
		for _, branch := range expectedBranches {
			require.NoError(t, env.PachClient.CreateBranch(repo, branch, "", commit.ID, nil))
		}

		branchInfos, err := env.PachClient.ListBranch(repo)
		require.NoError(t, err)
		require.Equal(t, len(expectedBranches), len(branchInfos))
		for i, branchInfo := range branchInfos {
			// branches should return in newest-first order
			require.Equal(t, expectedBranches[len(branchInfos)-i-1], branchInfo.Branch.Name)
			require.Equal(t, commit, branchInfo.Head)
		}

		commit2, err := env.PachClient.StartCommit(repo, "branch1")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit(repo, "branch1", ""))

		commit2Info, err := env.PachClient.InspectCommit(repo, "branch1", "")
		require.NoError(t, err)
		require.Equal(t, commit, commit2Info.ParentCommit)

		// delete the last branch
		lastBranch := expectedBranches[len(expectedBranches)-1]
		require.NoError(t, env.PachClient.DeleteBranch(repo, lastBranch, false))
		branchInfos, err = env.PachClient.ListBranch(repo)
		require.NoError(t, err)
		require.Equal(t, 2, len(branchInfos))
		require.Equal(t, "branch2", branchInfos[0].Branch.Name)
		require.Equal(t, commit, branchInfos[0].Head)
		require.Equal(t, "branch1", branchInfos[1].Branch.Name)
		require.Equal(t, commit2, branchInfos[1].Head)
	})

	suite.Run("DeleteNonexistentBranch", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))
		require.NoError(t, env.PachClient.DeleteBranch(repo, "doesnt_exist", false))
	})

	suite.Run("SubscribeCommit", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		numCommits := 10

		// create some commits that shouldn't affect the below SubscribeCommit call
		// reproduces #2469
		for i := 0; i < numCommits; i++ {
			commit, err := env.PachClient.StartCommit(repo, "master-v1")
			require.NoError(t, err)
			require.NoError(t, env.PachClient.FinishCommit(repo, commit.Branch.Name, commit.ID))
		}

		require.NoErrorWithinT(t, 60*time.Second, func() error {
			var eg errgroup.Group
			nextCommitChan := make(chan *pfs.Commit, numCommits)
			eg.Go(func() error {
				var count int
				err := env.PachClient.SubscribeCommit(client.NewRepo(repo), "master", nil, "", pfs.CommitState_STARTED, func(ci *pfs.CommitInfo) error {
					commit := <-nextCommitChan
					require.Equal(t, commit, ci.Commit)
					count++
					if count == numCommits {
						return errutil.ErrBreak
					}
					return nil
				})
				return err
			})
			eg.Go(func() error {
				for i := 0; i < numCommits; i++ {
					commit, err := env.PachClient.StartCommit(repo, "master")
					require.NoError(t, err)
					require.NoError(t, env.PachClient.FinishCommit(repo, commit.Branch.Name, commit.ID))
					nextCommitChan <- commit
				}
				return nil
			})

			return eg.Wait()
		})
	})

	suite.Run("InspectRepoSimple", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		commit, err := env.PachClient.StartCommit(repo, "branch")
		require.NoError(t, err)

		file1Content := "foo\n"
		require.NoError(t, env.PachClient.PutFile(commit, "foo", strings.NewReader(file1Content)))

		file2Content := "bar\n"
		require.NoError(t, env.PachClient.PutFile(commit, "bar", strings.NewReader(file2Content)))

		require.NoError(t, env.PachClient.FinishCommit(repo, commit.Branch.Name, commit.ID))

		info, err := env.PachClient.InspectRepo(repo)
		require.NoError(t, err)

		// Size should be 0 because the files were not added to master
		require.Equal(t, int(info.SizeBytes), 0)
	})

	suite.Run("InspectRepoComplex", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		commit, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)

		numFiles := 100
		minFileSize := 1000
		maxFileSize := 2000
		totalSize := 0

		for i := 0; i < numFiles; i++ {
			fileContent := random.String(rand.Intn(maxFileSize-minFileSize) + minFileSize)
			fileContent += "\n"
			fileName := fmt.Sprintf("file_%d", i)
			totalSize += len(fileContent)

			require.NoError(t, env.PachClient.PutFile(commit, fileName, strings.NewReader(fileContent)))

		}

		require.NoError(t, env.PachClient.FinishCommit(repo, commit.Branch.Name, commit.ID))

		info, err := env.PachClient.InspectRepo(repo)
		require.NoError(t, err)

		require.Equal(t, totalSize, int(info.SizeBytes))

		infos, err := env.PachClient.ListRepo()
		require.NoError(t, err)
		require.Equal(t, 1, len(infos))
		info = infos[0]

		require.Equal(t, totalSize, int(info.SizeBytes))
	})

	suite.Run("Create", func(t *testing.T) {
		// TODO: Implement put file split writer in V2?
		t.Skip("Put file split writer not implemented in V2")
		//t.Parallel()
		//env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		//repo := "test"
		//require.NoError(t, env.PachClient.CreateRepo(repo))
		//commit, err := env.PachClient.StartCommit(repo, "")
		//require.NoError(t, err)
		//w, err := env.PachClient.PutFileSplitWriter(repo, commit.Branch.Name, commit.ID, "foo", pfs.Delimiter_NONE, 0, 0, 0, false)
		//require.NoError(t, err)
		//require.NoError(t, w.Close())
		//require.NoError(t, env.PachClient.FinishCommit(repo, commit.Branch.Name, commit.ID))
		//_, err = env.PachClient.InspectFile(commit, "foo")
		//require.NoError(t, err)
	})

	suite.Run("GetFile", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		repo := tu.UniqueString("test")
		require.NoError(t, env.PachClient.CreateRepo(repo))
		commit, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(commit, "dir/file", strings.NewReader("foo\n")))
		checks := func() {
			var buffer bytes.Buffer
			require.NoError(t, env.PachClient.GetFile(commit, "dir/file", &buffer))
			require.Equal(t, "foo\n", buffer.String())
		}
		checks()
		require.NoError(t, env.PachClient.FinishCommit(repo, commit.Branch.Name, commit.ID))
		checks()
		t.Run("InvalidCommit", func(t *testing.T) {
			buffer := bytes.Buffer{}
			err = env.PachClient.GetFile(client.NewCommit(repo, "", "aninvalidcommitid"), "dir/file", &buffer)
			require.YesError(t, err)
		})
		t.Run("Directory", func(t *testing.T) {
			buffer := bytes.Buffer{}
			err = env.PachClient.GetFile(commit, "dir", &buffer)
			require.NoError(t, err)
		})
	})

	suite.Run("ManyPutsSingleFileSingleCommit", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		if testing.Short() {
			t.Skip("Skipping long tests in short mode")
		}
		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		commit1, err := env.PachClient.StartCommit(repo, "master")
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
					if err := env.PachClient.PutFile(commit1, "foo", strings.NewReader(rawMessage), pclient.WithAppendPutFile()); err != nil {
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
		require.NoError(t, env.PachClient.FinishCommit(repo, commit1.Branch.Name, commit1.ID))

		var buffer bytes.Buffer
		require.NoError(t, env.PachClient.GetFile(commit1, "foo", &buffer))
		require.Equal(t, string(expectedOutput), buffer.String())
	})

	suite.Run("PutFileValidCharacters", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		commit, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)

		// null characters error because when you `ls` files with null characters
		// they truncate things after the null character leading to strange results
		require.YesError(t, env.PachClient.PutFile(commit, "foo\x00bar", strings.NewReader("foobar\n")))

		// Boundary tests for valid character range
		require.YesError(t, env.PachClient.PutFile(commit, "\x1ffoobar", strings.NewReader("foobar\n")))
		require.NoError(t, env.PachClient.PutFile(commit, "foo\x20bar", strings.NewReader("foobar\n")))
		require.NoError(t, env.PachClient.PutFile(commit, "foobar\x7e", strings.NewReader("foobar\n")))
		require.YesError(t, env.PachClient.PutFile(commit, "foo\x7fbar", strings.NewReader("foobar\n")))

		// Random character tests outside and inside valid character range
		require.YesError(t, env.PachClient.PutFile(commit, "foobar\x0b", strings.NewReader("foobar\n")))
		require.NoError(t, env.PachClient.PutFile(commit, "\x41foobar", strings.NewReader("foobar\n")))

		// Glob character test
		require.YesError(t, env.PachClient.PutFile(commit, "foobar*", strings.NewReader("foobar\n")))
	})

	suite.Run("BigListFile", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))
		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))
		commit, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		var eg errgroup.Group
		for i := 0; i < 25; i++ {
			for j := 0; j < 25; j++ {
				i := i
				j := j
				eg.Go(func() error {
					return env.PachClient.PutFile(commit, fmt.Sprintf("dir%d/file%d", i, j), strings.NewReader("foo\n"))
				})
			}
		}
		require.NoError(t, eg.Wait())
		require.NoError(t, env.PachClient.FinishCommit(repo, commit.Branch.Name, commit.ID))
		for i := 0; i < 25; i++ {
			files, err := env.PachClient.ListFileAll(commit, fmt.Sprintf("dir%d", i))
			require.NoError(t, err)
			require.Equal(t, 25, len(files))
		}
	})

	suite.Run("StartCommitLatestOnBranch", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		commit1, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit(repo, commit1.Branch.Name, commit1.ID))

		commit2, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)

		require.NoError(t, env.PachClient.FinishCommit(repo, commit2.Branch.Name, commit2.ID))

		commit3, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit(repo, commit3.Branch.Name, commit3.ID))

		commitInfo, err := env.PachClient.InspectCommit(repo, "master", "")
		require.NoError(t, err)
		require.Equal(t, commit3.ID, commitInfo.Commit.ID)
	})

	suite.Run("CreateBranchTwice", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		commit1, err := env.PachClient.StartCommit(repo, "foo")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.CreateBranch(repo, "master", "", commit1.ID, nil))
		require.NoError(t, env.PachClient.FinishCommit(repo, commit1.Branch.Name, commit1.ID))

		commit2, err := env.PachClient.StartCommit(repo, "foo")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.CreateBranch(repo, "master", "", commit2.ID, nil))
		require.NoError(t, env.PachClient.FinishCommit(repo, commit2.Branch.Name, commit2.ID))

		branchInfos, err := env.PachClient.ListBranch(repo)
		require.NoError(t, err)

		require.Equal(t, 2, len(branchInfos))
		require.ElementsEqual(t, []string{"master", "foo"}, []string{branchInfos[0].Branch.Name, branchInfos[1].Branch.Name})
		require.Equal(t, commit2.ID, branchInfos[0].Head.ID)
		require.Equal(t, commit2.ID, branchInfos[1].Head.ID)
	})

	suite.Run("Flush", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		require.NoError(t, env.PachClient.CreateRepo("A"))
		require.NoError(t, env.PachClient.CreateRepo("B"))
		require.NoError(t, env.PachClient.CreateBranch("B", "master", "", "", []*pfs.Branch{pclient.NewBranch("A", "master")}))
		ACommit, err := env.PachClient.StartCommit("A", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("A", "master", ""))
		require.NoError(t, env.PachClient.FinishCommit("B", "master", ""))
		commitInfos, err := env.PachClient.FlushCommitAll([]*pfs.Commit{ACommit}, nil)
		require.NoError(t, err)
		require.Equal(t, 1, len(commitInfos))
	})

	// Flush2 implements the following DAG:
	// A ─▶ B ─▶ C ─▶ D
	suite.Run("Flush2", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		require.NoError(t, env.PachClient.CreateRepo("A"))
		require.NoError(t, env.PachClient.CreateRepo("B"))
		require.NoError(t, env.PachClient.CreateRepo("C"))
		require.NoError(t, env.PachClient.CreateRepo("D"))
		require.NoError(t, env.PachClient.CreateBranch("B", "master", "", "", []*pfs.Branch{pclient.NewBranch("A", "master")}))
		require.NoError(t, env.PachClient.CreateBranch("C", "master", "", "", []*pfs.Branch{pclient.NewBranch("B", "master")}))
		require.NoError(t, env.PachClient.CreateBranch("D", "master", "", "", []*pfs.Branch{pclient.NewBranch("C", "master")}))
		ACommit, err := env.PachClient.StartCommit("A", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("A", "master", ""))

		// do the other commits in a goro so we can block for them
		go func() {
			require.NoError(t, env.PachClient.FinishCommit("B", "master", ""))
			require.NoError(t, env.PachClient.FinishCommit("C", "master", ""))
			require.NoError(t, env.PachClient.FinishCommit("D", "master", ""))
		}()

		// Flush ACommit
		commitInfos, err := env.PachClient.FlushCommitAll([]*pfs.Commit{ACommit}, nil)
		require.NoError(t, err)
		require.Equal(t, 3, len(commitInfos))

		commitInfos, err = env.PachClient.FlushCommitAll(
			[]*pfs.Commit{ACommit},
			[]*pfs.Repo{pclient.NewRepo("C")},
		)
		require.NoError(t, err)
		require.Equal(t, 1, len(commitInfos))
	})

	// A
	//  ╲
	//   ◀
	//    C
	//   ◀
	//  ╱
	// B
	suite.Run("Flush3", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		require.NoError(t, env.PachClient.CreateRepo("A"))
		require.NoError(t, env.PachClient.CreateRepo("B"))
		require.NoError(t, env.PachClient.CreateRepo("C"))

		require.NoError(t, env.PachClient.CreateBranch("C", "master", "", "", []*pfs.Branch{pclient.NewBranch("A", "master"), pclient.NewBranch("B", "master")}))

		ACommit, err := env.PachClient.StartCommit("A", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("A", ACommit.Branch.Name, ACommit.ID))
		require.NoError(t, env.PachClient.FinishCommit("C", "master", ""))
		BCommit, err := env.PachClient.StartCommit("B", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("B", BCommit.Branch.Name, BCommit.ID))
		require.NoError(t, env.PachClient.FinishCommit("C", "master", ""))

		BCommit, err = env.PachClient.StartCommit("B", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("B", BCommit.Branch.Name, BCommit.ID))
		require.NoError(t, env.PachClient.FinishCommit("C", "master", ""))

		commitInfos, err := env.PachClient.FlushCommitAll([]*pfs.Commit{BCommit, ACommit}, nil)
		require.NoError(t, err)
		require.Equal(t, 1, len(commitInfos))

		require.Equal(t, commitInfos[0].Commit.Branch.Repo.Name, "C")
	})

	suite.Run("FlushRedundant", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		require.NoError(t, env.PachClient.CreateRepo("A"))
		ACommit, err := env.PachClient.StartCommit("A", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("A", "master", ""))
		commitInfos, err := env.PachClient.FlushCommitAll([]*pfs.Commit{ACommit, ACommit}, nil)
		require.NoError(t, err)
		require.Equal(t, 0, len(commitInfos))
	})

	suite.Run("FlushCommitWithNoDownstreamRepos", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))
		commit, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit(repo, commit.Branch.Name, commit.ID))
		commitInfos, err := env.PachClient.FlushCommitAll([]*pfs.Commit{commit}, nil)
		require.NoError(t, err)
		require.Equal(t, 0, len(commitInfos))
	})

	suite.Run("FlushOpenCommit", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		repo1 := "test1"
		repo2 := "test2"
		require.NoError(t, env.PachClient.CreateRepo(repo1))
		require.NoError(t, env.PachClient.CreateRepo(repo2))
		require.NoError(t, env.PachClient.CreateBranch(repo2, "master", "", "", []*pfs.Branch{pclient.NewBranch(repo1, "master")}))
		commit, err := env.PachClient.StartCommit(repo1, "master")
		require.NoError(t, err)

		// do the other commits in a goro so we can block for them
		eg, _ := errgroup.WithContext(context.Background())
		eg.Go(func() error {
			time.Sleep(3 * time.Second)
			if err := env.PachClient.FinishCommit(repo1, "master", ""); err != nil {
				return err
			}
			return env.PachClient.FinishCommit(repo2, "master", "")
		})

		t.Cleanup(func() {
			require.NoError(t, eg.Wait())
		})

		// Flush commit
		commitInfos, err := env.PachClient.FlushCommitAll([]*pfs.Commit{commit}, nil)
		require.NoError(t, err)
		require.Equal(t, 1, len(commitInfos))
	})

	suite.Run("EmptyFlush", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		_, err := env.PachClient.FlushCommitAll(nil, nil)
		require.YesError(t, err)
	})

	suite.Run("FlushNonExistentCommit", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		_, err := env.PachClient.FlushCommitAll([]*pfs.Commit{pclient.NewCommit("fake-repo", "fake-branch", "fake-commit")}, nil)
		require.YesError(t, err)
		repo := "FlushNonExistentCommit"
		require.NoError(t, env.PachClient.CreateRepo(repo))
		_, err = env.PachClient.FlushCommitAll([]*pfs.Commit{pclient.NewCommit(repo, "fake-branch", "fake-commit")}, nil)
		require.YesError(t, err)
		_, err = env.PachClient.FlushCommitAll([]*pfs.Commit{pclient.NewCommit(repo, "", "fake-commit")}, nil)
		require.YesError(t, err)
		_, err = env.PachClient.FlushCommitAll([]*pfs.Commit{pclient.NewCommit(repo, "fake-branch", "")}, nil)
		require.YesError(t, err)
		_, err = env.PachClient.FlushCommitAll([]*pfs.Commit{pclient.NewCommit(repo, "", "")}, nil)
		require.YesError(t, err)
	})

	suite.Run("PutFileSplit", func(t *testing.T) {
		// TODO(2.0 optional): Implement put file split.
		t.Skip("Put file split not implemented in V2")
		//	t.Parallel()
		//  env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))
		//
		//	if testing.Short() {
		//		t.Skip("Skipping integration tests in short mode")
		//	}
		//
		//	repo := "test"
		//	require.NoError(t, env.PachClient.CreateRepo(repo))
		//	commit, err := env.PachClient.StartCommit(repo, "master")
		//	require.NoError(t, err)
		//	_, err = env.PachClient.PutFileSplit(repo, commit.ID, "none", pfs.Delimiter_NONE, 0, 0, 0, false, strings.NewReader("foo\nbar\nbuz\n"))
		//	require.NoError(t, err)
		//	_, err = env.PachClient.PutFileSplit(repo, commit.ID, "line", pfs.Delimiter_LINE, 0, 0, 0, false, strings.NewReader("foo\nbar\nbuz\n"))
		//	require.NoError(t, err)
		//	_, err = env.PachClient.PutFileSplit(repo, commit.ID, "line", pfs.Delimiter_LINE, 0, 0, 0, false, strings.NewReader("foo\nbar\nbuz\n"))
		//	require.NoError(t, err)
		//	_, err = env.PachClient.PutFileSplit(repo, commit.ID, "line2", pfs.Delimiter_LINE, 2, 0, 0, false, strings.NewReader("foo\nbar\nbuz\nfiz\n"))
		//	require.NoError(t, err)
		//	_, err = env.PachClient.PutFileSplit(repo, commit.ID, "line3", pfs.Delimiter_LINE, 0, 8, 0, false, strings.NewReader("foo\nbar\nbuz\nfiz\n"))
		//	require.NoError(t, err)
		//	_, err = env.PachClient.PutFileSplit(repo, commit.ID, "json", pfs.Delimiter_JSON, 0, 0, 0, false, strings.NewReader("{}{}{}{}{}{}{}{}{}{}"))
		//	require.NoError(t, err)
		//	_, err = env.PachClient.PutFileSplit(repo, commit.ID, "json", pfs.Delimiter_JSON, 0, 0, 0, false, strings.NewReader("{}{}{}{}{}{}{}{}{}{}"))
		//	require.NoError(t, err)
		//	_, err = env.PachClient.PutFileSplit(repo, commit.ID, "json2", pfs.Delimiter_JSON, 2, 0, 0, false, strings.NewReader("{}{}{}{}"))
		//	require.NoError(t, err)
		//	_, err = env.PachClient.PutFileSplit(repo, commit.ID, "json3", pfs.Delimiter_JSON, 0, 4, 0, false, strings.NewReader("{}{}{}{}"))
		//	require.NoError(t, err)
		//
		//	files, err := env.PachClient.ListFileAll(repo, commit.ID, "line2")
		//	require.NoError(t, err)
		//	require.Equal(t, 2, len(files))
		//	for _, fileInfo := range files {
		//		require.Equal(t, uint64(8), fileInfo.SizeBytes)
		//	}
		//
		//	require.NoError(t, env.PachClient.FinishCommit(repo, commit.ID))
		//	commit2, err := env.PachClient.StartCommit(repo, "master")
		//	require.NoError(t, err)
		//	_, err = env.PachClient.PutFileSplit(repo, commit2.ID, "line", pfs.Delimiter_LINE, 0, 0, 0, false, strings.NewReader("foo\nbar\nbuz\n"))
		//	require.NoError(t, err)
		//	_, err = env.PachClient.PutFileSplit(repo, commit2.ID, "json", pfs.Delimiter_JSON, 0, 0, 0, false, strings.NewReader("{}{}{}{}{}{}{}{}{}{}"))
		//	require.NoError(t, err)
		//
		//	files, err = env.PachClient.ListFileAll(repo, commit2.ID, "line")
		//	require.NoError(t, err)
		//	require.Equal(t, 9, len(files))
		//	for _, fileInfo := range files {
		//		require.Equal(t, uint64(4), fileInfo.SizeBytes)
		//	}
		//
		//	require.NoError(t, env.PachClient.FinishCommit(repo, commit2.ID))
		//	fileInfo, err := env.PachClient.InspectFile(repo, commit.ID, "none")
		//	require.NoError(t, err)
		//	require.Equal(t, pfs.FileType_FILE, fileInfo.FileType)
		//	files, err = env.PachClient.ListFileAll(repo, commit.ID, "line")
		//	require.NoError(t, err)
		//	require.Equal(t, 6, len(files))
		//	for _, fileInfo := range files {
		//		require.Equal(t, uint64(4), fileInfo.SizeBytes)
		//	}
		//	files, err = env.PachClient.ListFileAll(repo, commit2.ID, "line")
		//	require.NoError(t, err)
		//	require.Equal(t, 9, len(files))
		//	for _, fileInfo := range files {
		//		require.Equal(t, uint64(4), fileInfo.SizeBytes)
		//	}
		//	files, err = env.PachClient.ListFileAll(repo, commit.ID, "line2")
		//	require.NoError(t, err)
		//	require.Equal(t, 2, len(files))
		//	for _, fileInfo := range files {
		//		require.Equal(t, uint64(8), fileInfo.SizeBytes)
		//	}
		//	files, err = env.PachClient.ListFileAll(repo, commit.ID, "line3")
		//	require.NoError(t, err)
		//	require.Equal(t, 2, len(files))
		//	for _, fileInfo := range files {
		//		require.Equal(t, uint64(8), fileInfo.SizeBytes)
		//	}
		//	files, err = env.PachClient.ListFileAll(repo, commit.ID, "json")
		//	require.NoError(t, err)
		//	require.Equal(t, 20, len(files))
		//	for _, fileInfo := range files {
		//		require.Equal(t, uint64(2), fileInfo.SizeBytes)
		//	}
		//	files, err = env.PachClient.ListFileAll(repo, commit2.ID, "json")
		//	require.NoError(t, err)
		//	require.Equal(t, 30, len(files))
		//	for _, fileInfo := range files {
		//		require.Equal(t, uint64(2), fileInfo.SizeBytes)
		//	}
		//	files, err = env.PachClient.ListFileAll(repo, commit.ID, "json2")
		//	require.NoError(t, err)
		//	require.Equal(t, 2, len(files))
		//	for _, fileInfo := range files {
		//		require.Equal(t, uint64(4), fileInfo.SizeBytes)
		//	}
		//	files, err = env.PachClient.ListFileAll(repo, commit.ID, "json3")
		//	require.NoError(t, err)
		//	require.Equal(t, 2, len(files))
		//	for _, fileInfo := range files {
		//		require.Equal(t, uint64(4), fileInfo.SizeBytes)
		//	}
	})

	suite.Run("PutFileSplitBig", func(t *testing.T) {
		// TODO(2.0 optional): Implement put file split.
		t.Skip("Put file split not implemented in V2")
		//	t.Parallel()
		//  env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))
		//
		//	if testing.Short() {
		//		t.Skip("Skipping integration tests in short mode")
		//	}
		//
		//	// create repos
		//	repo := "test"
		//	require.NoError(t, env.PachClient.CreateRepo(repo))
		//	commit, err := env.PachClient.StartCommit(repo, "master")
		//	require.NoError(t, err)
		//	w, err := env.PachClient.PutFileSplitWriter(repo, commit.ID, "line", pfs.Delimiter_LINE, 0, 0, 0, false)
		//	require.NoError(t, err)
		//	for i := 0; i < 1000; i++ {
		//		_, err = w.Write([]byte("foo\n"))
		//		require.NoError(t, err)
		//	}
		//	require.NoError(t, w.Close())
		//	require.NoError(t, env.PachClient.FinishCommit(repo, commit.ID))
		//	files, err := env.PachClient.ListFileAll(repo, commit.ID, "line")
		//	require.NoError(t, err)
		//	require.Equal(t, 1000, len(files))
		//	for _, fileInfo := range files {
		//		require.Equal(t, uint64(4), fileInfo.SizeBytes)
		//	}
	})

	suite.Run("PutFileSplitCSV", func(t *testing.T) {
		// TODO(2.0 optional): Implement put file split.
		t.Skip("Put file split not implemented in V2")
		//	t.Parallel()
		//  env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))
		//
		//	// create repos
		//	repo := "test"
		//	require.NoError(t, env.PachClient.CreateRepo(repo))
		//	_, err := env.PachClient.PutFileSplit(repo, "master", "data", pfs.Delimiter_CSV, 0, 0, 0, false,
		//		// Weird, but this is actually two lines ("is\na" is quoted, so one cell)
		//		strings.NewReader("this,is,a,test\n"+
		//			"\"\"\"this\"\"\",\"is\nonly\",\"a,test\"\n"))
		//	require.NoError(t, err)
		//	fileInfos, err := env.PachClient.ListFileAll(repo, "master", "/data")
		//	require.NoError(t, err)
		//	require.Equal(t, 2, len(fileInfos))
		//	var contents bytes.Buffer
		//	env.PachClient.GetFile(repo, "master", "/data/0000000000000000", &contents)
		//	require.Equal(t, "this,is,a,test\n", contents.String())
		//	contents.Reset()
		//	env.PachClient.GetFile(repo, "master", "/data/0000000000000001", &contents)
		//	require.Equal(t, "\"\"\"this\"\"\",\"is\nonly\",\"a,test\"\n", contents.String())
	})

	suite.Run("PutFileSplitSQL", func(t *testing.T) {
		// TODO(2.0 optional): Implement put file split.
		t.Skip("Put file split not implemented in V2")
		//	t.Parallel()
		//  env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))
		//
		//	// create repos
		//	repo := "test"
		//	require.NoError(t, env.PachClient.CreateRepo(repo))
		//
		//	_, err := env.PachClient.PutFileSplit(repo, "master", "/sql", pfs.Delimiter_SQL, 0, 0, 0,
		//		false, strings.NewReader(tu.TestPGDump))
		//	require.NoError(t, err)
		//	fileInfos, err := env.PachClient.ListFileAll(repo, "master", "/sql")
		//	require.NoError(t, err)
		//	require.Equal(t, 5, len(fileInfos))
		//
		//	// Get one of the SQL records & validate it
		//	var contents bytes.Buffer
		//	env.PachClient.GetFile(repo, "master", "/sql/0000000000000000", &contents)
		//	// Validate that the recieved pgdump file creates the cars table
		//	require.Matches(t, "CREATE TABLE public\\.cars", contents.String())
		//	// Validate the SQL header more generally by passing the output of GetFile
		//	// back through the SQL library & confirm that it parses correctly but only
		//	// has one row
		//	pgReader := sql.NewPGDumpReader(bufio.NewReader(bytes.NewReader(contents.Bytes())))
		//	record, err := pgReader.ReadRow()
		//	require.NoError(t, err)
		//	require.Equal(t, "Tesla\tRoadster\t2008\tliterally a rocket\n", string(record))
		//	_, err = pgReader.ReadRow()
		//	require.YesError(t, err)
		//	require.True(t, errors.Is(err, io.EOF))
		//
		//	// Create a new commit that overwrites all existing data & puts it back with
		//	// --header-records=1
		//	commit, err := env.PachClient.StartCommit(repo, "master")
		//	require.NoError(t, err)
		//	require.NoError(t, env.PachClient.DeleteFile(repo, commit.ID, "/sql"))
		//	_, err = env.PachClient.PutFileSplit(repo, commit.ID, "/sql", pfs.Delimiter_SQL, 0, 0, 1,
		//		false, strings.NewReader(tu.TestPGDump))
		//	require.NoError(t, err)
		//	require.NoError(t, env.PachClient.FinishCommit(repo, commit.ID))
		//	fileInfos, err = env.PachClient.ListFileAll(repo, "master", "/sql")
		//	require.NoError(t, err)
		//	require.Equal(t, 4, len(fileInfos))
		//
		//	// Get one of the SQL records & validate it
		//	contents.Reset()
		//	env.PachClient.GetFile(repo, "master", "/sql/0000000000000003", &contents)
		//	// Validate a that the recieved pgdump file creates the cars table
		//	require.Matches(t, "CREATE TABLE public\\.cars", contents.String())
		//	// Validate the SQL header more generally by passing the output of GetFile
		//	// back through the SQL library & confirm that it parses correctly but only
		//	// has one row
		//	pgReader = sql.NewPGDumpReader(bufio.NewReader(strings.NewReader(contents.String())))
		//	record, err = pgReader.ReadRow()
		//	require.NoError(t, err)
		//	require.Equal(t, "Tesla\tRoadster\t2008\tliterally a rocket\n", string(record))
		//	record, err = pgReader.ReadRow()
		//	require.NoError(t, err)
		//	require.Equal(t, "Toyota\tCorolla\t2005\tgreatest car ever made\n", string(record))
		//	_, err = pgReader.ReadRow()
		//	require.YesError(t, err)
		//	require.True(t, errors.Is(err, io.EOF))
	})

	suite.Run("DiffFile", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		// Write foo
		c1, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(c1, "foo", strings.NewReader("foo\n"), pclient.WithAppendPutFile()))
		checks := func() {
			newFis, oldFis, err := env.PachClient.DiffFileAll(c1, "", nil, "", false)
			require.NoError(t, err)
			require.Equal(t, 0, len(oldFis))
			require.Equal(t, 2, len(newFis))
			require.Equal(t, "/foo", newFis[1].File.Path)
		}
		checks()
		require.NoError(t, env.PachClient.FinishCommit(repo, c1.Branch.Name, c1.ID))
		checks()

		// Change the value of foo
		c2, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.DeleteFile(c2, "/foo"))
		require.NoError(t, env.PachClient.PutFile(c2, "foo", strings.NewReader("not foo\n"), pclient.WithAppendPutFile()))
		checks = func() {
			newFis, oldFis, err := env.PachClient.DiffFileAll(c2, "", nil, "", false)
			require.NoError(t, err)
			require.Equal(t, 2, len(oldFis))
			require.Equal(t, "/foo", oldFis[1].File.Path)
			require.Equal(t, 2, len(newFis))
			require.Equal(t, "/foo", newFis[1].File.Path)
		}
		checks()
		require.NoError(t, env.PachClient.FinishCommit(repo, c2.Branch.Name, c2.ID))
		checks()

		// Write bar
		c3, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(c3, "/bar", strings.NewReader("bar\n"), pclient.WithAppendPutFile()))
		checks = func() {
			newFis, oldFis, err := env.PachClient.DiffFileAll(c3, "", nil, "", false)
			require.NoError(t, err)
			require.Equal(t, 1, len(oldFis))
			require.Equal(t, 2, len(newFis))
			require.Equal(t, "/bar", newFis[1].File.Path)
		}
		checks()
		require.NoError(t, env.PachClient.FinishCommit(repo, c3.Branch.Name, c3.ID))
		checks()

		// Delete bar
		c4, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.DeleteFile(c4, "/bar"))
		checks = func() {
			newFis, oldFis, err := env.PachClient.DiffFileAll(c4, "", nil, "", false)
			require.NoError(t, err)
			require.Equal(t, 2, len(oldFis))
			require.Equal(t, "/bar", oldFis[1].File.Path)
			require.Equal(t, 1, len(newFis))
		}
		checks()
		require.NoError(t, env.PachClient.FinishCommit(repo, c4.Branch.Name, c4.ID))
		checks()

		// Write dir/fizz and dir/buzz
		c5, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(c5, "/dir/fizz", strings.NewReader("fizz\n"), pclient.WithAppendPutFile()))
		require.NoError(t, env.PachClient.PutFile(c5, "/dir/buzz", strings.NewReader("buzz\n"), pclient.WithAppendPutFile()))
		checks = func() {
			newFis, oldFis, err := env.PachClient.DiffFileAll(c5, "", nil, "", false)
			require.NoError(t, err)
			require.Equal(t, 1, len(oldFis))
			require.Equal(t, 4, len(newFis))
		}
		checks()
		require.NoError(t, env.PachClient.FinishCommit(repo, c5.Branch.Name, c5.ID))
		checks()

		// Modify dir/fizz
		c6, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(c6, "/dir/fizz", strings.NewReader("fizz\n"), pclient.WithAppendPutFile()))
		checks = func() {
			newFis, oldFis, err := env.PachClient.DiffFileAll(c6, "", nil, "", false)
			require.NoError(t, err)
			require.Equal(t, 3, len(oldFis))
			require.Equal(t, "/dir/fizz", oldFis[2].File.Path)
			require.Equal(t, 3, len(newFis))
			require.Equal(t, "/dir/fizz", newFis[2].File.Path)
		}
		checks()
		require.NoError(t, env.PachClient.FinishCommit(repo, c6.Branch.Name, c6.ID))
		checks()
	})

	suite.Run("GlobFile", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		if testing.Short() {
			t.Skip("Skipping integration tests in short mode")
		}

		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))
		commit := client.NewCommit(repo, "master", "")

		// Write foo
		numFiles := 100
		_, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		for i := 0; i < numFiles; i++ {
			require.NoError(t, env.PachClient.PutFile(commit, fmt.Sprintf("file%d", i), strings.NewReader("1")))
			require.NoError(t, env.PachClient.PutFile(commit, fmt.Sprintf("dir1/file%d", i), strings.NewReader("2")))
			require.NoError(t, env.PachClient.PutFile(commit, fmt.Sprintf("dir2/dir3/file%d", i), strings.NewReader("3")))
		}
		checks := func() {
			fileInfos, err := env.PachClient.GlobFileAll(commit, "*")
			require.NoError(t, err)
			require.Equal(t, numFiles+2, len(fileInfos))
			fileInfos, err = env.PachClient.GlobFileAll(commit, "file*")
			require.NoError(t, err)
			require.Equal(t, numFiles, len(fileInfos))
			fileInfos, err = env.PachClient.GlobFileAll(commit, "dir1/*")
			require.NoError(t, err)
			require.Equal(t, numFiles, len(fileInfos))
			fileInfos, err = env.PachClient.GlobFileAll(commit, "dir2/dir3/*")
			require.NoError(t, err)
			require.Equal(t, numFiles, len(fileInfos))
			fileInfos, err = env.PachClient.GlobFileAll(commit, "*/*")
			require.NoError(t, err)
			require.Equal(t, numFiles+1, len(fileInfos))

			var output strings.Builder
			err = env.PachClient.GetFile(commit, "*", &output)
			require.NoError(t, err)
			require.Equal(t, numFiles*3, len(output.String()))

			output = strings.Builder{}
			err = env.PachClient.GetFile(commit, "dir2/dir3/file1?", &output)
			require.NoError(t, err)
			require.Equal(t, 10, len(output.String()))

			output = strings.Builder{}
			err = env.PachClient.GetFile(commit, "**file1?", &output)
			require.NoError(t, err)
			require.Equal(t, 30, len(output.String()))

			output = strings.Builder{}
			err = env.PachClient.GetFile(commit, "**file1", &output)
			require.NoError(t, err)
			require.True(t, strings.Contains(output.String(), "1"))
			require.True(t, strings.Contains(output.String(), "2"))
			require.True(t, strings.Contains(output.String(), "3"))

			output = strings.Builder{}
			err = env.PachClient.GetFile(commit, "**file1", &output)
			require.NoError(t, err)
			match, err := regexp.Match("[123]", []byte(output.String()))
			require.NoError(t, err)
			require.True(t, match)

			output = strings.Builder{}
			err = env.PachClient.GetFile(commit, "dir?", &output)
			require.NoError(t, err)

			output = strings.Builder{}
			err = env.PachClient.GetFile(commit, "", &output)
			require.NoError(t, err)

			output = strings.Builder{}
			err = env.PachClient.GetFile(commit, "garbage", &output)
			require.YesError(t, err)
		}
		checks()
		require.NoError(t, env.PachClient.FinishCommit(repo, "master", ""))
		checks()

		_, err = env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)

		err = env.PachClient.DeleteFile(commit, "dir2/dir3/*")
		require.NoError(t, err)
		err = env.PachClient.DeleteFile(commit, "dir?/*")
		require.NoError(t, err)
		err = env.PachClient.DeleteFile(commit, "/")
		require.NoError(t, err)
		checks = func() {
			fileInfos, err := env.PachClient.GlobFileAll(commit, "**")
			require.NoError(t, err)
			require.Equal(t, 0, len(fileInfos))
		}
		checks()
		require.NoError(t, env.PachClient.FinishCommit(repo, "master", ""))
		checks()
	})

	suite.Run("GlobFile2", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		if testing.Short() {
			t.Skip("Skipping integration tests in short mode")
		}

		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))
		commit := client.NewCommit(repo, "master", "")

		_, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		expectedFileNames := []string{}
		for i := 0; i < 100; i++ {
			filename := fmt.Sprintf("/%d", i)
			require.NoError(t, env.PachClient.PutFile(commit, filename, strings.NewReader(filename)))

			if strings.HasPrefix(filename, "/1") {
				expectedFileNames = append(expectedFileNames, filename)
			}
		}
		require.NoError(t, env.PachClient.FinishCommit(repo, "master", ""))

		actualFileNames := []string{}
		require.NoError(t, env.PachClient.GlobFile(commit, "/1*", func(fileInfo *pfs.FileInfo) error {
			actualFileNames = append(actualFileNames, fileInfo.File.Path)
			return nil
		}))

		sort.Strings(expectedFileNames)
		sort.Strings(actualFileNames)
		require.Equal(t, expectedFileNames, actualFileNames)
	})

	suite.Run("GlobFile3", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))
		commit1, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(commit1, "/dir1/file1.1", &bytes.Buffer{}))
		require.NoError(t, env.PachClient.PutFile(commit1, "/dir1/file1.2", &bytes.Buffer{}))
		require.NoError(t, env.PachClient.PutFile(commit1, "/dir2/file2.1", &bytes.Buffer{}))
		require.NoError(t, env.PachClient.PutFile(commit1, "/dir2/file2.2", &bytes.Buffer{}))
		require.NoError(t, env.PachClient.FinishCommit(repo, commit1.Branch.Name, commit1.ID))
		globFile := func(pattern string) []string {
			var fis []*pfs.FileInfo
			require.NoError(t, env.PachClient.GlobFile(commit1, pattern, func(fi *pfs.FileInfo) error {
				fis = append(fis, fi)
				return nil
			}))
			return finfosToPaths(fis)
		}
		assert.ElementsMatch(t, []string{"/dir1/file1.2", "/dir2/file2.2"}, globFile("**.2"))
		assert.ElementsMatch(t, []string{"/dir1/file1.1", "/dir1/file1.2"}, globFile("/dir1/*"))
		assert.ElementsMatch(t, []string{"/dir1/", "/dir2/"}, globFile("/*"))
		assert.ElementsMatch(t, []string{"/"}, globFile("/"))
	})

	// GetFileGlobOrder checks that GetFile(glob) streams data back in the
	// right order. GetFile(glob) is supposed to return a stream of data of the
	// form file1 + file2 + .. + fileN, where file1 is the lexicographically lowest
	// file matching 'glob', file2 is the next lowest, etc.
	suite.Run("GetFileGlobOrder", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		var expected bytes.Buffer
		commit, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		for i := 0; i < 25; i++ {
			next := fmt.Sprintf("%d,%d,%d,%d\n", 4*i, (4*i)+1, (4*i)+2, (4*i)+3)
			expected.WriteString(next)
			env.PachClient.PutFile(commit, fmt.Sprintf("/data/%010d", i), strings.NewReader(next))
		}
		require.NoError(t, env.PachClient.FinishCommit(repo, commit.Branch.Name, commit.ID))

		var output bytes.Buffer
		require.NoError(t, env.PachClient.GetFile(commit, "/data/*", &output))
		require.Equal(t, expected.String(), output.String())
	})

	suite.Run("ApplyWriteOrder", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		if testing.Short() {
			t.Skip("Skipping integration tests in short mode")
		}

		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))
		commit := client.NewCommit(repo, "master", "")

		// Test that fails when records are applied in lexicographic order
		// rather than mod revision order.
		_, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(commit, "/file", strings.NewReader("")))
		err = env.PachClient.DeleteFile(commit, "/")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit(repo, "master", ""))
		fileInfos, err := env.PachClient.GlobFileAll(commit, "**")
		require.NoError(t, err)
		require.Equal(t, 0, len(fileInfos))
	})

	suite.Run("Overwrite", func(t *testing.T) {
		// TODO(2.0 optional): Implement put file split.
		t.Skip("Put file split not implemented in V2")
		//	t.Parallel()
		//  env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))
		//
		//	if testing.Short() {
		//		t.Skip("Skipping integration tests in short mode")
		//	}
		//
		//	repo := "test"
		//	require.NoError(t, env.PachClient.CreateRepo(repo))
		//
		//	// Write foo
		//	_, err := env.PachClient.StartCommit(repo, "master")
		//	require.NoError(t, err)
		//	_, err = env.PachClient.PutFile(repo, "master", "file1", strings.NewReader("foo"))
		//	require.NoError(t, err)
		//	_, err = env.PachClient.PutFileSplit(repo, "master", "file2", pfs.Delimiter_LINE, 0, 0, 0, false, strings.NewReader("foo\nbar\nbuz\n"))
		//	require.NoError(t, err)
		//	_, err = env.PachClient.PutFileSplit(repo, "master", "file3", pfs.Delimiter_LINE, 0, 0, 0, false, strings.NewReader("foo\nbar\nbuz\n"))
		//	require.NoError(t, err)
		//	require.NoError(t, env.PachClient.FinishCommit(repo, "master"))
		//	_, err = env.PachClient.StartCommit(repo, "master")
		//	require.NoError(t, err)
		//	_, err = env.PachClient.PutFile(repo, "master", "file1", strings.NewReader("bar"))
		//	require.NoError(t, err)
		//	require.NoError(t, env.PachClient.PutFile(repo, "master", "file2", strings.NewReader("buzz")))
		//	require.NoError(t, err)
		//	_, err = env.PachClient.PutFileSplit(repo, "master", "file3", pfs.Delimiter_LINE, 0, 0, 0, true, strings.NewReader("0\n1\n2\n"))
		//	require.NoError(t, err)
		//	require.NoError(t, env.PachClient.FinishCommit(repo, "master"))
		//	var buffer bytes.Buffer
		//	require.NoError(t, env.PachClient.GetFile(repo, "master", "file1", &buffer))
		//	require.Equal(t, "bar", buffer.String())
		//	buffer.Reset()
		//	require.NoError(t, env.PachClient.GetFile(repo, "master", "file2", &buffer))
		//	require.Equal(t, "buzz", buffer.String())
		//	fileInfos, err := env.PachClient.ListFileAll(repo, "master", "file3")
		//	require.NoError(t, err)
		//	require.Equal(t, 3, len(fileInfos))
		//	for i := 0; i < 3; i++ {
		//		buffer.Reset()
		//		require.NoError(t, env.PachClient.GetFile(repo, "master", fmt.Sprintf("file3/%016x", i), &buffer))
		//		require.Equal(t, fmt.Sprintf("%d\n", i), buffer.String())
		//	}
	})

	suite.Run("CopyFile", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		masterCommit, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		numFiles := 5
		for i := 0; i < numFiles; i++ {
			require.NoError(t, env.PachClient.PutFile(masterCommit, fmt.Sprintf("files/%d", i), strings.NewReader(fmt.Sprintf("foo %d\n", i))))
		}
		require.NoError(t, env.PachClient.FinishCommit(repo, masterCommit.Branch.Name, masterCommit.ID))

		for i := 0; i < numFiles; i++ {
			_, err = env.PachClient.InspectFile(masterCommit, fmt.Sprintf("files/%d", i))
			require.NoError(t, err)
		}

		otherCommit, err := env.PachClient.StartCommit(repo, "other")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.CopyFile(otherCommit, "files", masterCommit, "files", pclient.WithAppendCopyFile()))
		require.NoError(t, env.PachClient.CopyFile(otherCommit, "file0", masterCommit, "files/0", pclient.WithAppendCopyFile()))
		require.NoError(t, env.PachClient.FinishCommit(repo, otherCommit.Branch.Name, otherCommit.ID))

		for i := 0; i < numFiles; i++ {
			_, err = env.PachClient.InspectFile(otherCommit, fmt.Sprintf("files/%d", i))
			require.NoError(t, err)
		}
		_, err = env.PachClient.InspectFile(otherCommit, "files/0")
		require.NoError(t, err)
	})

	suite.Run("PropagateCommit", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		repo1 := "test1"
		require.NoError(t, env.PachClient.CreateRepo(repo1))
		repo2 := "test2"
		require.NoError(t, env.PachClient.CreateRepo(repo2))
		require.NoError(t, env.PachClient.CreateBranch(repo2, "master", "", "", []*pfs.Branch{pclient.NewBranch(repo1, "master")}))
		commit, err := env.PachClient.StartCommit(repo1, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit(repo1, commit.Branch.Name, commit.ID))
		commits, err := env.PachClient.ListCommitByRepo(client.NewRepo(repo2))
		require.NoError(t, err)
		require.Equal(t, 1, len(commits))
	})

	// BackfillBranch implements the following DAG:
	//
	// A ──▶ C
	//  ╲   ◀
	//   ╲ ╱
	//    ╳
	//   ╱ ╲
	// 	╱   ◀
	// B ──▶ D
	suite.Run("BackfillBranch", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		require.NoError(t, env.PachClient.CreateRepo("A"))
		require.NoError(t, env.PachClient.CreateRepo("B"))
		require.NoError(t, env.PachClient.CreateRepo("C"))
		require.NoError(t, env.PachClient.CreateRepo("D"))
		require.NoError(t, env.PachClient.CreateBranch("C", "master", "", "", []*pfs.Branch{pclient.NewBranch("A", "master"), pclient.NewBranch("B", "master")}))
		_, err := env.PachClient.StartCommit("A", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("A", "master", ""))
		require.NoError(t, env.PachClient.FinishCommit("C", "master", ""))
		_, err = env.PachClient.StartCommit("B", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("B", "master", ""))
		require.NoError(t, env.PachClient.FinishCommit("C", "master", ""))
		commits, err := env.PachClient.ListCommitByRepo(client.NewRepo("C"))
		require.NoError(t, err)
		for _, commitInfo := range commits {
			fmt.Printf("commit: %s\n", pfsdb.CommitKey(commitInfo.Commit))
		}
		require.Equal(t, 2, len(commits))

		// Create a branch in D, it should receive a single commit for the heads of `A` and `B`.
		require.NoError(t, env.PachClient.CreateBranch("D", "master", "", "", []*pfs.Branch{pclient.NewBranch("A", "master"), pclient.NewBranch("B", "master")}))
		commits, err = env.PachClient.ListCommitByRepo(client.NewRepo("D"))
		require.NoError(t, err)
		require.Equal(t, 1, len(commits))
	})

	// UpdateBranch tests the following DAG:
	//
	// A ─▶ B ─▶ C
	//
	// Then updates it to:
	//
	// A ─▶ B ─▶ C
	//      ▲
	// D ───╯
	//
	suite.Run("UpdateBranch", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		require.NoError(t, env.PachClient.CreateRepo("A"))
		require.NoError(t, env.PachClient.CreateRepo("B"))
		require.NoError(t, env.PachClient.CreateRepo("C"))
		require.NoError(t, env.PachClient.CreateRepo("D"))
		require.NoError(t, env.PachClient.CreateBranch("B", "master", "", "", []*pfs.Branch{pclient.NewBranch("A", "master")}))
		require.NoError(t, env.PachClient.CreateBranch("C", "master", "", "", []*pfs.Branch{pclient.NewBranch("B", "master")}))
		_, err := env.PachClient.StartCommit("A", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("A", "master", ""))
		require.NoError(t, env.PachClient.FinishCommit("B", "master", ""))
		require.NoError(t, env.PachClient.FinishCommit("C", "master", ""))

		_, err = env.PachClient.StartCommit("D", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("D", "master", ""))

		require.NoError(t, env.PachClient.CreateBranch("B", "master", "", "", []*pfs.Branch{pclient.NewBranch("A", "master"), pclient.NewBranch("D", "master")}))
		require.NoError(t, env.PachClient.FinishCommit("B", "master", ""))
		require.NoError(t, env.PachClient.FinishCommit("C", "master", ""))
		cCommitInfo, err := env.PachClient.InspectCommit("C", "master", "")
		require.NoError(t, err)
		require.Equal(t, 3, len(cCommitInfo.Provenance))
	})

	suite.Run("BranchProvenance", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

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
				require.NoError(t, env.PachClient.CreateRepo(repo))
				for iStep, step := range test {
					var provenance []*pfs.Branch
					for _, branch := range step.directProv {
						provenance = append(provenance, pclient.NewBranch(repo, branch))
					}
					err := env.PachClient.CreateBranch(repo, step.name, "", "", provenance)
					if step.err {
						require.YesError(t, err, "%d> CreateBranch(\"%s\", %v)", iStep, step.name, step.directProv)
					} else {
						require.NoError(t, err, "%d> CreateBranch(\"%s\", %v)", iStep, step.name, step.directProv)
					}
					for branch, expectedProv := range step.expectProv {
						bi, err := env.PachClient.InspectBranch(repo, branch)
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
						bi, err := env.PachClient.InspectBranch(repo, branch)
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
		// 	require.NoError(t, env.PachClient.CreateRepo("A"))
		// 	require.NoError(t, env.PachClient.CreateRepo("B"))
		// 	require.NoError(t, env.PachClient.CreateRepo("C"))
		// 	require.NoError(t, env.PachClient.CreateRepo("D"))

		// 	require.NoError(t, env.PachClient.CreateBranch("B", "master", "", []*pfs.Branch{pclient.NewBranch("A", "master")}))
		// 	require.NoError(t, env.PachClient.CreateBranch("C", "master", "", []*pfs.Branch{pclient.NewBranch("B", "master")}))
		// 	require.NoError(t, env.PachClient.CreateBranch("D", "master", "", []*pfs.Branch{pclient.NewBranch("C", "master"), pclient.NewBranch("A", "master")}))

		// 	aMaster, err := env.PachClient.InspectBranch("A", "master")
		// 	require.NoError(t, err)
		// 	require.Equal(t, 3, len(aMaster.Subvenance))

		// 	cMaster, err := env.PachClient.InspectBranch("C", "master")
		// 	require.NoError(t, err)
		// 	require.Equal(t, 2, len(cMaster.Provenance))

		// 	dMaster, err := env.PachClient.InspectBranch("D", "master")
		// 	require.NoError(t, err)
		// 	require.Equal(t, 3, len(dMaster.Provenance))

		// 	require.NoError(t, env.PachClient.CreateBranch("B", "master", "", nil))

		// 	aMaster, err = env.PachClient.InspectBranch("A", "master")
		// 	require.NoError(t, err)
		// 	require.Equal(t, 1, len(aMaster.Subvenance))

		// 	cMaster, err = env.PachClient.InspectBranch("C", "master")
		// 	require.NoError(t, err)
		// 	require.Equal(t, 1, len(cMaster.Provenance))

		// 	dMaster, err = env.PachClient.InspectBranch("D", "master")
		// 	require.NoError(t, err)
		// 	require.Equal(t, 3, len(dMaster.Provenance))
		// })
		// t.Run("2", func(t *testing.T) {
		// 	require.NoError(t, env.PachClient.CreateRepo("A"))
		// 	require.NoError(t, env.PachClient.CreateRepo("B"))
		// 	require.NoError(t, env.PachClient.CreateRepo("C"))
		// 	require.NoError(t, env.PachClient.CreateRepo("D"))

		// 	require.NoError(t, env.PachClient.CreateBranch("B", "master", "", []*pfs.Branch{pclient.NewBranch("A", "master")}))
		// 	require.NoError(t, env.PachClient.CreateBranch("C", "master", "", []*pfs.Branch{pclient.NewBranch("B", "master"), pclient.NewBranch("A", "master")}))
		// 	require.NoError(t, env.PachClient.CreateBranch("D", "master", "", []*pfs.Branch{pclient.NewBranch("C", "master")}))
		// })
	})

	suite.Run("ChildCommits", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		require.NoError(t, env.PachClient.CreateRepo("A"))
		require.NoError(t, env.PachClient.CreateBranch("A", "master", "", "", nil))
		aRepo := client.NewRepo("A")

		// Small helper function wrapping env.PachClient.InspectCommit, because it's called a lot
		inspect := func(repo, branch, commit string) *pfs.CommitInfo {
			commitInfo, err := env.PachClient.InspectCommit(repo, branch, commit)
			require.NoError(t, err)
			return commitInfo
		}

		commit1, err := env.PachClient.StartCommit("A", "master")
		require.NoError(t, err)
		commits, err := env.PachClient.ListCommit(aRepo, aRepo.NewCommit("master", ""), nil, 0)
		require.NoError(t, err)
		t.Logf("%v", commits)
		require.NoError(t, env.PachClient.FinishCommit("A", "master", ""))

		commit2, err := env.PachClient.StartCommit("A", "master")
		require.NoError(t, err)

		// Inspect commit 1 and 2
		commit1Info, commit2Info := inspect("A", commit1.Branch.Name, commit1.ID), inspect("A", commit2.Branch.Name, commit2.ID)
		require.Equal(t, commit1.ID, commit2Info.ParentCommit.ID)
		require.ElementsEqualUnderFn(t, []string{commit2.ID}, commit1Info.ChildCommits, CommitToID)

		// Delete commit 2 and make sure it's removed from commit1.ChildCommits
		require.NoError(t, env.PachClient.SquashCommit("A", commit2.Branch.Name, commit2.ID))
		commit1Info = inspect("A", commit1.Branch.Name, commit1.ID)
		require.ElementsEqualUnderFn(t, nil, commit1Info.ChildCommits, CommitToID)

		// Re-create commit2, and create a third commit also extending from commit1.
		// Make sure both appear in commit1.children
		commit2, err = env.PachClient.StartCommit("A", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("A", commit2.Branch.Name, commit2.ID))
		commit3, err := env.PachClient.PfsAPIClient.StartCommit(env.PachClient.Ctx(), &pfs.StartCommitRequest{
			Branch: pclient.NewBranch("A", "foo"),
			Parent: commit1,
		})
		require.NoError(t, err)
		commit1Info = inspect("A", commit1.Branch.Name, commit1.ID)
		require.ElementsEqualUnderFn(t, []string{commit2.ID, commit3.ID}, commit1Info.ChildCommits, CommitToID)

		// Delete commit3 and make sure commit1 has the right children
		require.NoError(t, env.PachClient.SquashCommit("A", commit3.Branch.Name, commit3.ID))
		commit1Info = inspect("A", commit1.Branch.Name, commit1.ID)
		require.ElementsEqualUnderFn(t, []string{commit2.ID}, commit1Info.ChildCommits, CommitToID)

		// Create a downstream branch in the same repo, then commit to "A" and make
		// sure the new HEAD commit is in the parent's children (i.e. test
		// propagateCommit)
		require.NoError(t, env.PachClient.CreateBranch("A", "out", "", "", []*pfs.Branch{
			pclient.NewBranch("A", "master"),
		}))
		outCommit1 := inspect("A", "out", "")
		commit3, err = env.PachClient.StartCommit("A", "master")
		require.NoError(t, err)
		env.PachClient.FinishCommit("A", commit3.Branch.Name, commit3.ID)
		// Re-inspect outCommit1, which has been updated by StartCommit
		outCommit1, outCommit2 := inspect("A", outCommit1.Commit.Branch.Name, outCommit1.Commit.ID), inspect("A", "out", "")
		require.Equal(t, outCommit1.Commit.ID, outCommit2.ParentCommit.ID)
		require.ElementsEqualUnderFn(t, []string{outCommit2.Commit.ID}, outCommit1.ChildCommits, CommitToID)

		// create a new branch in a different repo and do the same test again
		require.NoError(t, env.PachClient.CreateRepo("B"))
		require.NoError(t, env.PachClient.CreateBranch("B", "master", "", "", []*pfs.Branch{
			pclient.NewBranch("A", "master"),
		}))
		bCommit1 := inspect("B", "master", "")
		commit3, err = env.PachClient.StartCommit("A", "master")
		require.NoError(t, err)
		env.PachClient.FinishCommit("A", commit3.Branch.Name, commit3.ID)
		// Re-inspect bCommit1, which has been updated by StartCommit
		bCommit1, bCommit2 := inspect("B", bCommit1.Commit.Branch.Name, bCommit1.Commit.ID), inspect("B", "master", "")
		require.Equal(t, bCommit1.Commit.ID, bCommit2.ParentCommit.ID)
		require.ElementsEqualUnderFn(t, []string{bCommit2.Commit.ID}, bCommit1.ChildCommits, CommitToID)

		// create a new branch in a different repo, then update it so that two commits
		// are generated. Make sure the second commit is in the parent's children
		require.NoError(t, env.PachClient.CreateRepo("C"))
		require.NoError(t, env.PachClient.CreateBranch("C", "master", "", "", []*pfs.Branch{
			pclient.NewBranch("A", "master"),
		}))
		cCommit1 := inspect("C", "master", "") // Get new commit's ID
		require.NoError(t, env.PachClient.CreateBranch("C", "master", "master", "", []*pfs.Branch{
			pclient.NewBranch("A", "master"),
			pclient.NewBranch("B", "master"),
		}))
		// Re-inspect cCommit1, which has been updated by CreateBranch
		cCommit1, cCommit2 := inspect("C", cCommit1.Commit.Branch.Name, cCommit1.Commit.ID), inspect("C", "master", "")
		require.Equal(t, cCommit1.Commit.ID, cCommit2.ParentCommit.ID)
		require.ElementsEqualUnderFn(t, []string{cCommit2.Commit.ID}, cCommit1.ChildCommits, CommitToID)
	})

	suite.Run("StartCommitFork", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		require.NoError(t, env.PachClient.CreateRepo("A"))
		require.NoError(t, env.PachClient.CreateBranch("A", "master", "", "", nil))
		commit, err := env.PachClient.StartCommit("A", "master")
		require.NoError(t, err)
		env.PachClient.FinishCommit("A", commit.Branch.Name, commit.ID)
		commit2, err := env.PachClient.PfsAPIClient.StartCommit(env.PachClient.Ctx(), &pfs.StartCommitRequest{
			Branch: pclient.NewBranch("A", "master2"),
			Parent: pclient.NewCommit("A", "master", ""),
		})
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("A", commit2.Branch.Name, commit2.ID))

		aRepo := client.NewRepo("A")
		commits, err := env.PachClient.ListCommit(aRepo, aRepo.NewCommit("master2", ""), nil, 0)
		require.NoError(t, err)
		require.ElementsEqualUnderFn(t, []string{commit.ID, commit2.ID}, commits, CommitInfoToID)
	})

	// UpdateBranchNewOutputCommit tests the following corner case:
	// A ──▶ C
	// B
	//
	// Becomes:
	//
	// A  ╭▶ C
	// B ─╯
	//
	// C should create a new output commit to process its unprocessed inputs in B
	suite.Run("UpdateBranchNewOutputCommit", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		require.NoError(t, env.PachClient.CreateRepo("A"))
		require.NoError(t, env.PachClient.CreateRepo("B"))
		require.NoError(t, env.PachClient.CreateRepo("C"))
		require.NoError(t, env.PachClient.CreateBranch("A", "master", "", "", nil))
		require.NoError(t, env.PachClient.CreateBranch("B", "master", "", "", nil))
		require.NoError(t, env.PachClient.CreateBranch("C", "master", "", "",
			[]*pfs.Branch{pclient.NewBranch("A", "master")}))

		// Create commits in A and B
		commit, err := env.PachClient.StartCommit("A", "master")
		require.NoError(t, err)
		env.PachClient.FinishCommit("A", commit.Branch.Name, commit.ID)
		commit, err = env.PachClient.StartCommit("B", "master")
		require.NoError(t, err)
		env.PachClient.FinishCommit("A", commit.Branch.Name, commit.ID)

		// Check for first output commit in C
		cRepo := client.NewRepo("C")
		commits, err := env.PachClient.ListCommit(cRepo, cRepo.NewCommit("master", ""), nil, 0)
		require.NoError(t, err)
		require.Equal(t, 1, len(commits))

		// Update the provenance of C/master and make sure it creates a new commit
		require.NoError(t, env.PachClient.CreateBranch("C", "master", "master", "",
			[]*pfs.Branch{pclient.NewBranch("B", "master")}))
		commits, err = env.PachClient.ListCommit(cRepo, cRepo.NewCommit("master", ""), nil, 0)
		require.NoError(t, err)
		require.Equal(t, 2, len(commits))
	})

	// SquashCommitBigSubvenance deletes a commit that is upstream of a large
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
	suite.Run("SquashCommitBigSubvenance", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		// two input repos, one with many commits (logs), and one with few (schema)
		require.NoError(t, env.PachClient.CreateRepo("logs"))
		require.NoError(t, env.PachClient.CreateRepo("schema"))
		schemaRepo := client.NewRepo("schema")
		schemaMaster := schemaRepo.NewCommit("master", "")

		// Commit to logs and schema (so that "pipeline" has an initial output commit,
		// and we can check that it updates this initial commit's child appropriately)
		for _, repo := range []string{"schema", "logs"} {
			commit, err := env.PachClient.StartCommit(repo, "master")
			require.NoError(t, err)
			require.NoError(t, env.PachClient.FinishCommit(repo, commit.Branch.Name, commit.ID))
		}

		// Create an output branch, in "pipeline"
		require.NoError(t, env.PachClient.CreateRepo("pipeline"))
		require.NoError(t, env.PachClient.CreateBranch("pipeline", "master", "", "", []*pfs.Branch{
			pclient.NewBranch("schema", "master"),
			pclient.NewBranch("logs", "master"),
		}))
		pipeline := client.NewRepo("pipeline")
		pipelineMaster := pipeline.NewCommit("master", "")
		commits, err := env.PachClient.ListCommit(pipeline, pipelineMaster, nil, 0)
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
		bigSubvCommit, err := env.PachClient.StartCommit("schema", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("schema", bigSubvCommit.Branch.Name, bigSubvCommit.ID))
		for i := 0; i < 10; i++ {
			commit, err := env.PachClient.StartCommit("logs", "master")
			require.NoError(t, err)
			require.NoError(t, env.PachClient.FinishCommit("logs", commit.Branch.Name, commit.ID))
		}
		commit, err := env.PachClient.StartCommit("schema", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("schema", commit.Branch.Name, commit.ID))

		// Make sure there are 13 output commits in 'pipeline' to start (one from
		// creation, one from the second 'schema' commit, 10 from the 'logs' commits,
		// and one more from the third 'schema' commit)
		commits, err = env.PachClient.ListCommit(pipeline, pipelineMaster, nil, 0)
		require.NoError(t, err)
		require.Equal(t, 13, len(commits))

		require.NoError(t, env.PachClient.SquashCommit("schema", bigSubvCommit.Branch.Name, bigSubvCommit.ID))

		commits, err = env.PachClient.ListCommit(pipeline, pipelineMaster, nil, 0)
		require.NoError(t, err)
		require.Equal(t, 2, len(commits))
		require.Equal(t, commits[1].Commit.ID, commits[0].ParentCommit.ID)
		require.Equal(t, commits[1].ChildCommits[0].ID, commits[0].Commit.ID)

		// Case 2
		// - reset bigSubvCommit to be the head commit of 'schema/master'
		// - commit to 'logs' 10 more times
		// - delete bigSubvCommit
		// - Now there should be two commits in 'pipeline':
		//   - One started by SquashCommit (with provenance schema/master and
		//     logs/masterand
		//   - The oldest commit in 'pipeline', from the setup
		// - The second commit is the parent of the first
		//
		// This makes sure that the branch pipeline/master is rewritten back to
		// the last live commit, and that it creates a new output commit when branches
		// have unprocesed HEAD commits
		commits, err = env.PachClient.ListCommit(schemaRepo, schemaMaster, nil, 0)
		require.NoError(t, err)
		require.Equal(t, 2, len(commits))
		bigSubvCommitInfo, err := env.PachClient.InspectCommit("schema", "master", "")
		require.NoError(t, err)
		bigSubvCommit = bigSubvCommitInfo.Commit
		for i := 0; i < 10; i++ {
			commit, err = env.PachClient.StartCommit("logs", "master")
			require.NoError(t, err)
			require.NoError(t, env.PachClient.FinishCommit("logs", commit.Branch.Name, commit.ID))
		}

		require.NoError(t, env.PachClient.SquashCommit("schema", bigSubvCommit.Branch.Name, bigSubvCommit.ID))

		commits, err = env.PachClient.ListCommit(pipeline, pipelineMaster, nil, 0)
		require.NoError(t, err)
		require.Equal(t, 2, len(commits))
		pipelineMasterInfo, err := env.PachClient.InspectCommit("pipeline", "master", "")
		require.NoError(t, err)
		require.Equal(t, pipelineMasterInfo.Commit.ID, commits[0].Commit.ID)
		require.Equal(t, pipelineMasterInfo.ParentCommit.ID, commits[1].Commit.ID)

		// Case 3
		// - reset bigSubvCommit to be the head of 'schema/master' (the only commit)
		// - commit to 'logs' 10 more times
		// - delete bigSubvCommit
		// - Now there shouldn't be any commits in 'pipeline'
		// - Further test: delete all commits in schema and logs, and make sure that
		//   'pipeline/master' still points to nil, as there are no input commits
		commits, err = env.PachClient.ListCommit(schemaRepo, schemaMaster, nil, 0)
		require.NoError(t, err)
		require.Equal(t, 1, len(commits))
		bigSubvCommit = commits[0].Commit

		for i := 0; i < 10; i++ {
			commit, err = env.PachClient.StartCommit("logs", "master")
			require.NoError(t, err)
			require.NoError(t, env.PachClient.FinishCommit("logs", commit.Branch.Name, commit.ID))
		}

		require.NoError(t, env.PachClient.SquashCommit("schema", bigSubvCommit.Branch.Name, bigSubvCommit.ID))

		commits, err = env.PachClient.ListCommit(pipeline, pipelineMaster, nil, 0)
		require.NoError(t, err)
		require.Equal(t, 0, len(commits))

		// Delete all input commits--SquashCommit should reset 'pipeline/master' to
		// nil, and should not create a new output commit this time
		commits, err = env.PachClient.ListCommit(schemaRepo, schemaMaster, nil, 0)
		require.NoError(t, err)
		for _, commitInfo := range commits {
			require.NoError(t, env.PachClient.SquashCommit("schema", commitInfo.Commit.Branch.Name, commitInfo.Commit.ID))
		}
		commits, err = env.PachClient.ListCommit(client.NewRepo("logs"), client.NewCommit("logs", "master", ""), nil, 0)
		require.NoError(t, err)
		for _, commitInfo := range commits {
			require.NoError(t, env.PachClient.SquashCommit("logs", commitInfo.Commit.Branch.Name, commitInfo.Commit.ID))
		}
		commits, err = env.PachClient.ListCommit(pipeline, pipelineMaster, nil, 0)
		require.NoError(t, err)
		require.Equal(t, 0, len(commits))
		_, err = env.PachClient.InspectCommit("pipeline", "master", "")
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
		commits, err = env.PachClient.ListCommit(schemaRepo, schemaMaster, nil, 0)
		require.NoError(t, err)
		require.Equal(t, 0, len(commits))
		bigSubvCommit, err = env.PachClient.StartCommit("schema", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("schema", bigSubvCommit.Branch.Name, bigSubvCommit.ID))
		for i := 0; i < 10; i++ {
			commit, err = env.PachClient.StartCommit("logs", "master")
			require.NoError(t, err)
			require.NoError(t, env.PachClient.FinishCommit("logs", commit.Branch.Name, commit.ID))
		}
		commit, err = env.PachClient.StartCommit("schema", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("schema", commit.Branch.Name, commit.ID))

		require.NoError(t, env.PachClient.SquashCommit("schema", bigSubvCommit.Branch.Name, bigSubvCommit.ID))

		commits, err = env.PachClient.ListCommit(pipeline, pipelineMaster, nil, 0)
		require.NoError(t, err)
		require.Equal(t, 1, len(commits))
		pipelineMasterInfo, err = env.PachClient.InspectCommit("pipeline", "master", "")
		require.NoError(t, err)
		require.Nil(t, pipelineMasterInfo.ParentCommit)
	})

	// SquashCommitMultipleChildrenSingleCommit tests that when you have the
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
	suite.Run("SquashCommitMultipleChildrenSingleCommit", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		require.NoError(t, env.PachClient.CreateRepo("repo"))
		require.NoError(t, env.PachClient.CreateBranch("repo", "master", "", "", nil))

		// Create commits 'a' and 'b'
		a, err := env.PachClient.StartCommit("repo", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("repo", a.Branch.Name, a.ID))
		b, err := env.PachClient.StartCommit("repo", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("repo", b.Branch.Name, b.ID))

		// Create second branch
		require.NoError(t, env.PachClient.CreateBranch("repo", "master2", "master", "", nil))

		// Create commits 'c' and 'd'
		c, err := env.PachClient.StartCommit("repo", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("repo", c.Branch.Name, c.ID))
		d, err := env.PachClient.StartCommit("repo", "master2")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("repo", d.Branch.Name, d.ID))

		// Collect info re: a, b, c, and d, and make sure that the parent/child
		// relationships are all correct
		aInfo, err := env.PachClient.InspectCommit("repo", a.Branch.Name, a.ID)
		require.NoError(t, err)
		bInfo, err := env.PachClient.InspectCommit("repo", b.Branch.Name, b.ID)
		require.NoError(t, err)
		cInfo, err := env.PachClient.InspectCommit("repo", c.Branch.Name, c.ID)
		require.NoError(t, err)
		dInfo, err := env.PachClient.InspectCommit("repo", d.Branch.Name, d.ID)
		require.NoError(t, err)

		require.Nil(t, aInfo.ParentCommit)
		require.ElementsEqualUnderFn(t, []string{b.ID}, aInfo.ChildCommits, CommitToID)

		require.Equal(t, a.ID, bInfo.ParentCommit.ID)
		require.ElementsEqualUnderFn(t, []string{c.ID, d.ID}, bInfo.ChildCommits, CommitToID)

		require.Equal(t, b.ID, cInfo.ParentCommit.ID)
		require.Equal(t, 0, len(cInfo.ChildCommits))

		require.Equal(t, b.ID, dInfo.ParentCommit.ID)
		require.Equal(t, 0, len(dInfo.ChildCommits))

		// Delete commit 'b'
		env.PachClient.SquashCommit("repo", b.Branch.Name, b.ID)

		// Collect info re: a, c, and d, and make sure that the parent/child
		// relationships are still correct
		aInfo, err = env.PachClient.InspectCommit("repo", a.Branch.Name, a.ID)
		require.NoError(t, err)
		cInfo, err = env.PachClient.InspectCommit("repo", c.Branch.Name, c.ID)
		require.NoError(t, err)
		dInfo, err = env.PachClient.InspectCommit("repo", d.Branch.Name, d.ID)
		require.NoError(t, err)

		require.Nil(t, aInfo.ParentCommit)
		require.ElementsEqualUnderFn(t, []string{c.ID, d.ID}, aInfo.ChildCommits, CommitToID)

		require.Equal(t, a.ID, cInfo.ParentCommit.ID)
		require.Equal(t, 0, len(cInfo.ChildCommits))

		require.Equal(t, a.ID, dInfo.ParentCommit.ID)
		require.Equal(t, 0, len(dInfo.ChildCommits))
	})

	// SquashCommitMultiLevelChildrenNilParent tests that when you have the
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
	suite.Run("SquashCommitMultiLevelChildrenNilParent", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		require.NoError(t, env.PachClient.CreateRepo("upstream1"))
		require.NoError(t, env.PachClient.CreateRepo("upstream2"))
		// commit to both inputs
		_, err := env.PachClient.StartCommit("upstream1", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("upstream1", "master", ""))
		deleteMeCommit, err := env.PachClient.StartCommit("upstream2", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("upstream2", "master", ""))

		// Create main repo (will have the commit graphs above
		require.NoError(t, env.PachClient.CreateRepo("repo"))
		require.NoError(t, env.PachClient.CreateBranch("repo", "master", "", "", []*pfs.Branch{
			pclient.NewBranch("upstream1", "master"),
			pclient.NewBranch("upstream2", "master"),
		}))
		repoProto := client.NewRepo("repo")

		// Create commit 'a'
		aInfo, err := env.PachClient.InspectCommit("repo", "master", "")
		require.NoError(t, err)
		a := aInfo.Commit
		require.NoError(t, env.PachClient.FinishCommit("repo", a.Branch.Name, a.ID))

		// Create 'd'
		resp, err := env.PachClient.PfsAPIClient.StartCommit(env.PachClient.Ctx(), &pfs.StartCommitRequest{
			Branch: pclient.NewBranch("repo", "fod"),
			Parent: a,
		})
		require.NoError(t, err)
		d := pclient.NewCommit("repo", resp.Branch.Name, resp.ID)
		require.NoError(t, env.PachClient.FinishCommit("repo", resp.Branch.Name, resp.ID))

		// Create 'b'
		// (commit to upstream1, so that a & b have same prov commit in upstream2)
		_, err = env.PachClient.StartCommit("upstream1", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("upstream1", "master", ""))
		bInfo, err := env.PachClient.InspectCommit("repo", "master", "")
		require.NoError(t, err)
		b := bInfo.Commit
		require.NoError(t, env.PachClient.FinishCommit("repo", b.Branch.Name, b.ID))

		// Create 'e'
		resp, err = env.PachClient.PfsAPIClient.StartCommit(env.PachClient.Ctx(), &pfs.StartCommitRequest{
			Branch: pclient.NewBranch("repo", "foe"),
			Parent: b,
		})
		require.NoError(t, err)
		e := pclient.NewCommit("repo", resp.Branch.Name, resp.ID)
		require.NoError(t, env.PachClient.FinishCommit("repo", resp.Branch.Name, resp.ID))

		// Create 'c'
		// (commit to upstream1, so that a, b & c have same prov commit in upstream2)
		_, err = env.PachClient.StartCommit("upstream1", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("upstream1", "master", ""))
		cInfo, err := env.PachClient.InspectCommit("repo", "master", "")
		require.NoError(t, err)
		c := cInfo.Commit
		require.NoError(t, env.PachClient.FinishCommit("repo", c.Branch.Name, c.ID))

		// Create 'f'
		resp, err = env.PachClient.PfsAPIClient.StartCommit(env.PachClient.Ctx(), &pfs.StartCommitRequest{
			Branch: pclient.NewBranch("repo", "fof"),
			Parent: c,
		})
		require.NoError(t, err)
		f := pclient.NewCommit("repo", resp.Branch.Name, resp.ID)
		require.NoError(t, env.PachClient.FinishCommit("repo", resp.Branch.Name, resp.ID))

		// Make sure child/parent relationships are as shown in first diagram
		commits, err := env.PachClient.ListCommit(repoProto, nil, nil, 0)
		require.NoError(t, err)
		require.Equal(t, 6, len(commits))
		aInfo, err = env.PachClient.InspectCommit("repo", a.Branch.Name, a.ID)
		require.NoError(t, err)
		bInfo, err = env.PachClient.InspectCommit("repo", b.Branch.Name, b.ID)
		require.NoError(t, err)
		cInfo, err = env.PachClient.InspectCommit("repo", c.Branch.Name, c.ID)
		require.NoError(t, err)
		dInfo, err := env.PachClient.InspectCommit("repo", d.Branch.Name, d.ID)
		require.NoError(t, err)
		eInfo, err := env.PachClient.InspectCommit("repo", e.Branch.Name, e.ID)
		require.NoError(t, err)
		fInfo, err := env.PachClient.InspectCommit("repo", f.Branch.Name, f.ID)
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
		require.NoError(t, env.PachClient.SquashCommit("upstream2", deleteMeCommit.Branch.Name, deleteMeCommit.ID))

		// Re-read commit info to get new parents/children
		dInfo, err = env.PachClient.InspectCommit("repo", d.Branch.Name, d.ID)
		require.NoError(t, err)
		eInfo, err = env.PachClient.InspectCommit("repo", e.Branch.Name, e.ID)
		require.NoError(t, err)
		fInfo, err = env.PachClient.InspectCommit("repo", f.Branch.Name, f.ID)
		require.NoError(t, err)

		// Make sure child/parent relationships are as shown in second diagram
		commits, err = env.PachClient.ListCommit(repoProto, nil, nil, 0)
		require.NoError(t, err)
		require.Equal(t, 3, len(commits))
		require.Nil(t, eInfo.ParentCommit)
		require.Nil(t, fInfo.ParentCommit)
		require.Nil(t, dInfo.ChildCommits)
		require.Nil(t, eInfo.ChildCommits)
		require.Nil(t, fInfo.ChildCommits)
	})

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
	suite.Run("SquashCommitMultiLevelChildren", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		require.NoError(t, env.PachClient.CreateRepo("upstream1"))
		require.NoError(t, env.PachClient.CreateRepo("upstream2"))
		// commit to both inputs
		_, err := env.PachClient.StartCommit("upstream1", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("upstream1", "master", ""))
		_, err = env.PachClient.StartCommit("upstream2", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("upstream2", "master", ""))

		// Create main repo (will have the commit graphs above
		require.NoError(t, env.PachClient.CreateRepo("repo"))
		require.NoError(t, env.PachClient.CreateBranch("repo", "master", "", "", []*pfs.Branch{
			pclient.NewBranch("upstream1", "master"),
			pclient.NewBranch("upstream2", "master"),
		}))
		repoProto := client.NewRepo("repo")

		// Create commit 'a'
		aInfo, err := env.PachClient.InspectCommit("repo", "master", "")
		require.NoError(t, err)
		a := aInfo.Commit
		require.NoError(t, env.PachClient.FinishCommit("repo", a.Branch.Name, a.ID))

		// Create 'd'
		resp, err := env.PachClient.PfsAPIClient.StartCommit(env.PachClient.Ctx(), &pfs.StartCommitRequest{
			Branch: pclient.NewBranch("repo", "fod"),
			Parent: a,
		})
		require.NoError(t, err)
		d := pclient.NewCommit("repo", resp.Branch.Name, resp.ID)
		require.NoError(t, env.PachClient.FinishCommit("repo", resp.Branch.Name, resp.ID))

		// Create 'b'
		// (a & b have same prov commit in upstream2, so this is the commit that will
		// be deleted, as both b and c are provenant on it)
		deleteMeCommit, err := env.PachClient.StartCommit("upstream1", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("upstream1", "master", ""))
		bInfo, err := env.PachClient.InspectCommit("repo", "master", "")
		require.NoError(t, err)
		b := bInfo.Commit
		require.NoError(t, env.PachClient.FinishCommit("repo", b.Branch.Name, b.ID))

		// Create 'e'
		resp, err = env.PachClient.PfsAPIClient.StartCommit(env.PachClient.Ctx(), &pfs.StartCommitRequest{
			Branch: pclient.NewBranch("repo", "foe"),
			Parent: b,
		})
		require.NoError(t, err)
		e := pclient.NewCommit("repo", resp.Branch.Name, resp.ID)
		require.NoError(t, env.PachClient.FinishCommit("repo", resp.Branch.Name, resp.ID))

		// Create 'c'
		// (commit to upstream2, so that b & c have same prov commit in upstream1)
		_, err = env.PachClient.StartCommit("upstream2", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("upstream2", "master", ""))
		cInfo, err := env.PachClient.InspectCommit("repo", "master", "")
		require.NoError(t, err)
		c := cInfo.Commit
		require.NoError(t, env.PachClient.FinishCommit("repo", c.Branch.Name, c.ID))

		// Create 'f'
		resp, err = env.PachClient.PfsAPIClient.StartCommit(env.PachClient.Ctx(), &pfs.StartCommitRequest{
			Branch: pclient.NewBranch("repo", "fof"),
			Parent: c,
		})
		require.NoError(t, err)
		f := pclient.NewCommit("repo", resp.Branch.Name, resp.ID)
		require.NoError(t, env.PachClient.FinishCommit("repo", resp.Branch.Name, resp.ID))

		// Make sure child/parent relationships are as shown in first diagram
		commits, err := env.PachClient.ListCommit(repoProto, nil, nil, 0)
		require.NoError(t, err)
		require.Equal(t, 6, len(commits))
		aInfo, err = env.PachClient.InspectCommit("repo", a.Branch.Name, a.ID)
		require.NoError(t, err)
		bInfo, err = env.PachClient.InspectCommit("repo", b.Branch.Name, b.ID)
		require.NoError(t, err)
		cInfo, err = env.PachClient.InspectCommit("repo", c.Branch.Name, c.ID)
		require.NoError(t, err)
		dInfo, err := env.PachClient.InspectCommit("repo", d.Branch.Name, d.ID)
		require.NoError(t, err)
		eInfo, err := env.PachClient.InspectCommit("repo", e.Branch.Name, e.ID)
		require.NoError(t, err)
		fInfo, err := env.PachClient.InspectCommit("repo", f.Branch.Name, f.ID)
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
		require.NoError(t, env.PachClient.SquashCommit("upstream1", deleteMeCommit.Branch.Name, deleteMeCommit.ID))

		// Re-read commit info to get new parents/children
		aInfo, err = env.PachClient.InspectCommit("repo", a.Branch.Name, a.ID)
		require.NoError(t, err)
		dInfo, err = env.PachClient.InspectCommit("repo", d.Branch.Name, d.ID)
		require.NoError(t, err)
		eInfo, err = env.PachClient.InspectCommit("repo", e.Branch.Name, e.ID)
		require.NoError(t, err)
		fInfo, err = env.PachClient.InspectCommit("repo", f.Branch.Name, f.ID)
		require.NoError(t, err)

		// Make sure child/parent relationships are as shown in second diagram. Note
		// that after 'b' and 'c' are deleted, SquashCommit creates a new commit:
		// - 'repo/master' points to 'a'
		// - SquashCommit starts a new output commit to process 'upstream1/master'
		//   and 'upstream2/master'
		// - The new output commit is started in 'repo/master' and is also a child of
		//   'a'
		commits, err = env.PachClient.ListCommit(repoProto, nil, nil, 0)
		require.NoError(t, err)
		require.Equal(t, 5, len(commits))
		require.Nil(t, aInfo.ParentCommit)
		require.Equal(t, a.ID, dInfo.ParentCommit.ID)
		require.Equal(t, a.ID, eInfo.ParentCommit.ID)
		require.Equal(t, a.ID, fInfo.ParentCommit.ID)
		newCommitInfo, err := env.PachClient.InspectCommit("repo", "master", "")
		require.NoError(t, err)
		require.ElementsEqualUnderFn(t,
			[]string{d.ID, e.ID, f.ID, newCommitInfo.Commit.ID},
			aInfo.ChildCommits, CommitToID)
		require.Nil(t, dInfo.ChildCommits)
		require.Nil(t, eInfo.ChildCommits)
		require.Nil(t, fInfo.ChildCommits)
	})

	// SquashCommitShrinkSubvRange is like SquashCommitBigSubvenance, but
	// instead of deleting commits from "schema", this test deletes them from
	// "logs", to make sure that the subvenance of "schema" commits is rewritten
	// correctly. As before, there are four cases:
	// 1. Subvenance "Lower" is increased
	// 2. Subvenance "Upper" is decreased
	// 3. Subvenance is not affected, because the deleted commit is between "Lower" and "Upper"
	// 4. The entire subvenance range is deleted
	suite.Run("SquashCommitShrinkSubvRange", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		// two input repos, one with many commits (logs), and one with few (schema)
		require.NoError(t, env.PachClient.CreateRepo("logs"))
		require.NoError(t, env.PachClient.CreateRepo("schema"))

		// Commit to logs and schema
		logsCommit := make([]*pfs.Commit, 10)
		var err error
		logsCommit[0], err = env.PachClient.StartCommit("logs", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("logs", logsCommit[0].Branch.Name, logsCommit[0].ID))
		schemaCommit, err := env.PachClient.StartCommit("schema", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("schema", schemaCommit.Branch.Name, schemaCommit.ID))

		// Create an output branch, in "pipeline"
		require.NoError(t, env.PachClient.CreateRepo("pipeline"))
		require.NoError(t, env.PachClient.CreateBranch("pipeline", "master", "", "", []*pfs.Branch{
			pclient.NewBranch("schema", "master"),
			pclient.NewBranch("logs", "master"),
		}))
		pipeline := client.NewRepo("pipeline")
		pipelineMaster := pipeline.NewCommit("master", "")
		commits, err := env.PachClient.ListCommit(pipeline, pipelineMaster, nil, 0)
		require.NoError(t, err)
		require.Equal(t, 1, len(commits))

		// Commit to "logs" 9 more times, so that the commit to "schema" has 10
		// commits in its subvenance
		for i := 1; i < 10; i++ {
			logsCommit[i], err = env.PachClient.StartCommit("logs", "master")
			require.NoError(t, err)
			require.NoError(t, env.PachClient.FinishCommit("logs", logsCommit[i].Branch.Name, logsCommit[i].ID))
		}
		pipelineCommitInfos, err := env.PachClient.ListCommit(pipeline, pipelineMaster, nil, 0)
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
		schemaCommitInfo, err := env.PachClient.InspectCommit("schema", schemaCommit.Branch.Name, schemaCommit.ID)
		require.NoError(t, err)
		require.Equal(t, 1, len(schemaCommitInfo.Subvenance))
		require.Equal(t, pipelineCommit[0], schemaCommitInfo.Subvenance[0].Lower.ID)
		require.Equal(t, pipelineCommit[9], schemaCommitInfo.Subvenance[0].Upper.ID)

		// Case 1
		// - Delete the first commit in "logs" and make sure that the subvenance of
		//   the single commit in "schema" has increased its Lower value
		require.NoError(t, env.PachClient.SquashCommit("logs", logsCommit[0].Branch.Name, logsCommit[0].ID))
		schemaCommitInfo, err = env.PachClient.InspectCommit("schema", schemaCommit.Branch.Name, schemaCommit.ID)
		require.NoError(t, err)
		require.Equal(t, 1, len(schemaCommitInfo.Subvenance))
		require.Equal(t, pipelineCommit[1], schemaCommitInfo.Subvenance[0].Lower.ID)
		require.Equal(t, pipelineCommit[9], schemaCommitInfo.Subvenance[0].Upper.ID)

		// Case 2
		// - Delete the last commit in "logs" and make sure that the subvenance of
		//   the single commit in "schema" has decreased its Upper value
		require.NoError(t, env.PachClient.SquashCommit("logs", logsCommit[9].Branch.Name, logsCommit[9].ID))
		schemaCommitInfo, err = env.PachClient.InspectCommit("schema", schemaCommit.Branch.Name, schemaCommit.ID)
		require.NoError(t, err)
		require.Equal(t, 1, len(schemaCommitInfo.Subvenance))
		require.Equal(t, pipelineCommit[1], schemaCommitInfo.Subvenance[0].Lower.ID)
		require.Equal(t, pipelineCommit[8], schemaCommitInfo.Subvenance[0].Upper.ID)

		// Case 3
		// - Delete the middle commit in "logs" and make sure that the subvenance of
		//   the single commit in "schema" hasn't changed
		require.NoError(t, env.PachClient.SquashCommit("logs", logsCommit[5].Branch.Name, logsCommit[5].ID))
		schemaCommitInfo, err = env.PachClient.InspectCommit("schema", schemaCommit.Branch.Name, schemaCommit.ID)
		require.NoError(t, err)
		require.Equal(t, 1, len(schemaCommitInfo.Subvenance))
		require.Equal(t, pipelineCommit[1], schemaCommitInfo.Subvenance[0].Lower.ID)
		require.Equal(t, pipelineCommit[8], schemaCommitInfo.Subvenance[0].Upper.ID)

		// Case 4
		// - Delete the remaining commits in "logs" and make sure that the subvenance
		//   of the single commit in "schema" is now empty
		for _, i := range []int{1, 2, 3, 4, 6, 7, 8} {
			require.NoError(t, env.PachClient.SquashCommit("logs", logsCommit[i].Branch.Name, logsCommit[i].ID))
		}
		schemaCommitInfo, err = env.PachClient.InspectCommit("schema", schemaCommit.Branch.Name, schemaCommit.ID)
		require.NoError(t, err)
		require.Equal(t, 0, len(schemaCommitInfo.Subvenance))
	})

	suite.Run("CommitState", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		// two input repos, one with many commits (logs), and one with few (schema)
		require.NoError(t, env.PachClient.CreateRepo("A"))
		require.NoError(t, env.PachClient.CreateRepo("B"))

		require.NoError(t, env.PachClient.CreateBranch("B", "master", "", "", []*pfs.Branch{pclient.NewBranch("A", "master")}))

		// Start a commit on A/master, this will create a non-ready commit on B/master.
		_, err := env.PachClient.StartCommit("A", "master")
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()
		_, err = env.PachClient.PfsAPIClient.InspectCommit(ctx, &pfs.InspectCommitRequest{
			Commit:     pclient.NewCommit("B", "master", ""),
			BlockState: pfs.CommitState_READY,
		})
		require.YesError(t, err)

		// Finish the commit on A/master, that will make the B/master ready.
		require.NoError(t, env.PachClient.FinishCommit("A", "master", ""))

		ctx, cancel = context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()
		_, err = env.PachClient.PfsAPIClient.InspectCommit(ctx, &pfs.InspectCommitRequest{
			Commit:     pclient.NewCommit("B", "master", ""),
			BlockState: pfs.CommitState_READY,
		})
		require.NoError(t, err)

		// Create a new branch C/master with A/master as provenance. It should start out ready.
		require.NoError(t, env.PachClient.CreateRepo("C"))
		require.NoError(t, env.PachClient.CreateBranch("C", "master", "", "", []*pfs.Branch{pclient.NewBranch("A", "master")}))

		ctx, cancel = context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()
		_, err = env.PachClient.PfsAPIClient.InspectCommit(ctx, &pfs.InspectCommitRequest{
			Commit:     pclient.NewCommit("C", "master", ""),
			BlockState: pfs.CommitState_READY,
		})
		require.NoError(t, err)
	})

	suite.Run("SubscribeStates", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		require.NoError(t, env.PachClient.CreateRepo("A"))
		require.NoError(t, env.PachClient.CreateRepo("B"))
		require.NoError(t, env.PachClient.CreateRepo("C"))

		require.NoError(t, env.PachClient.CreateBranch("B", "master", "", "", []*pfs.Branch{pclient.NewBranch("A", "master")}))
		require.NoError(t, env.PachClient.CreateBranch("C", "master", "", "", []*pfs.Branch{pclient.NewBranch("B", "master")}))

		ctx, cancel := context.WithCancel(env.PachClient.Ctx())
		defer cancel()
		pachClient := env.PachClient.WithCtx(ctx)

		var readyCommits int64
		go func() {
			pachClient.SubscribeCommit(client.NewRepo("B"), "master", nil, "", pfs.CommitState_READY, func(ci *pfs.CommitInfo) error {
				atomic.AddInt64(&readyCommits, 1)
				return nil
			})
		}()
		go func() {
			pachClient.SubscribeCommit(client.NewRepo("C"), "master", nil, "", pfs.CommitState_READY, func(ci *pfs.CommitInfo) error {
				atomic.AddInt64(&readyCommits, 1)
				return nil
			})
		}()
		_, err := pachClient.StartCommit("A", "master")
		require.NoError(t, err)
		require.NoError(t, pachClient.FinishCommit("A", "master", ""))

		require.NoErrorWithinTRetry(t, time.Second*10, func() error {
			if atomic.LoadInt64(&readyCommits) != 1 {
				return errors.Errorf("wrong number of ready commits")
			}
			return nil
		})

		require.NoError(t, pachClient.FinishCommit("B", "master", ""))

		require.NoErrorWithinTRetry(t, time.Second*10, func() error {
			if atomic.LoadInt64(&readyCommits) != 2 {
				return errors.Errorf("wrong number of ready commits")
			}
			return nil
		})
	})

	suite.Run("PutFileCommit", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		numFiles := 25
		repo := "repo"
		require.NoError(t, env.PachClient.CreateRepo(repo))
		commit := client.NewCommit(repo, "master", "")

		var eg errgroup.Group
		for i := 0; i < numFiles; i++ {
			i := i
			eg.Go(func() error {
				return env.PachClient.PutFile(commit, fmt.Sprintf("%d", i), strings.NewReader(fmt.Sprintf("%d", i)))
			})
		}
		require.NoError(t, eg.Wait())

		for i := 0; i < numFiles; i++ {
			var b bytes.Buffer
			require.NoError(t, env.PachClient.GetFile(commit, fmt.Sprintf("%d", i), &b))
			require.Equal(t, fmt.Sprintf("%d", i), b.String())
		}

		bi, err := env.PachClient.InspectBranch(repo, "master")
		require.NoError(t, err)

		eg = errgroup.Group{}
		for i := 0; i < numFiles; i++ {
			i := i
			eg.Go(func() error {
				return env.PachClient.CopyFile(commit, fmt.Sprintf("%d", (i+1)%numFiles), bi.Head, fmt.Sprintf("%d", i))
			})
		}
		require.NoError(t, eg.Wait())

		for i := 0; i < numFiles; i++ {
			var b bytes.Buffer
			require.NoError(t, env.PachClient.GetFile(commit, fmt.Sprintf("%d", (i+1)%numFiles), &b))
			require.Equal(t, fmt.Sprintf("%d", i), b.String())
		}

		eg = errgroup.Group{}
		for i := 0; i < numFiles; i++ {
			i := i
			eg.Go(func() error {
				return env.PachClient.DeleteFile(commit, fmt.Sprintf("%d", i))
			})
		}
		require.NoError(t, eg.Wait())

		fileInfos, err := env.PachClient.ListFileAll(commit, "")
		require.NoError(t, err)
		require.Equal(t, 0, len(fileInfos))
	})

	suite.Run("PutFileCommitNilBranch", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		repo := "repo"
		require.NoError(t, env.PachClient.CreateRepo(repo))
		require.NoError(t, env.PachClient.CreateBranch(repo, "master", "", "", nil))
		commit := client.NewCommit(repo, "master", "")

		require.NoError(t, env.PachClient.PutFile(commit, "file", strings.NewReader("file")))
	})

	suite.Run("PutFileCommitOverwrite", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		numFiles := 5
		repo := "repo"
		require.NoError(t, env.PachClient.CreateRepo(repo))
		commit := client.NewCommit(repo, "master", "")

		for i := 0; i < numFiles; i++ {
			require.NoError(t, env.PachClient.PutFile(commit, "file", strings.NewReader(fmt.Sprintf("%d", i))))
		}

		var b bytes.Buffer
		require.NoError(t, env.PachClient.GetFile(commit, "file", &b))
		require.Equal(t, fmt.Sprintf("%d", numFiles-1), b.String())
	})

	suite.Run("WalkFile", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))
		commit, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(commit, "dir/bar", strings.NewReader("bar")))
		require.NoError(t, env.PachClient.PutFile(commit, "dir/dir2/buzz", strings.NewReader("buzz")))
		require.NoError(t, env.PachClient.PutFile(commit, "foo", strings.NewReader("foo")))

		expectedPaths := []string{"/", "/dir/", "/dir/bar", "/dir/dir2/", "/dir/dir2/buzz", "/foo"}
		checks := func() {
			i := 0
			require.NoError(t, env.PachClient.WalkFile(commit, "", func(fi *pfs.FileInfo) error {
				require.Equal(t, expectedPaths[i], fi.File.Path)
				i++
				return nil
			}))
			require.Equal(t, len(expectedPaths), i)
		}
		checks()
		require.NoError(t, env.PachClient.FinishCommit(repo, commit.Branch.Name, commit.ID))
		checks()
	})

	suite.Run("WalkFile2", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		repo := "WalkFile2"
		require.NoError(t, env.PachClient.CreateRepo(repo))
		commit1, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(commit1, "/dir1/file1.1", &bytes.Buffer{}))
		require.NoError(t, env.PachClient.PutFile(commit1, "/dir1/file1.2", &bytes.Buffer{}))
		require.NoError(t, env.PachClient.PutFile(commit1, "/dir2/file2.1", &bytes.Buffer{}))
		require.NoError(t, env.PachClient.PutFile(commit1, "/dir2/file2.2", &bytes.Buffer{}))
		require.NoError(t, env.PachClient.FinishCommit(repo, commit1.Branch.Name, commit1.ID))
		walkFile := func(path string) []string {
			var fis []*pfs.FileInfo
			require.NoError(t, env.PachClient.WalkFile(commit1, path, func(fi *pfs.FileInfo) error {
				fis = append(fis, fi)
				return nil
			}))
			return finfosToPaths(fis)
		}
		assert.ElementsMatch(t, []string{"/dir1/", "/dir1/file1.1", "/dir1/file1.2"}, walkFile("/dir1"))
		assert.ElementsMatch(t, []string{"/dir1/file1.1"}, walkFile("/dir1/file1.1"))
		assert.Len(t, walkFile("/"), 7)
	})

	suite.Run("WalkFileEmpty", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		repo := "test"
		latestCommit := client.NewCommit(repo, "master", "")
		checks := func() {
			cb := func(fi *pfs.FileInfo) error {
				if assert.Equal(t, fi.FileType, pfs.FileType_DIR) && assert.Equal(t, fi.File.Path, "/") {
					return nil
				}
				return errors.New("should not have returned any file results for an empty commit")
			}
			checkNotFound := func(path string) {
				err := env.PachClient.WalkFile(latestCommit, path, cb)
				require.YesError(t, err)
				require.Matches(t, "file .* not found in repo", err.Error())
			}
			require.NoError(t, env.PachClient.WalkFile(latestCommit, "", cb))
			require.NoError(t, env.PachClient.WalkFile(latestCommit, "/", cb))
			checkNotFound("foo")
			checkNotFound("/foo")
			checkNotFound("foo/bar")
			checkNotFound("/foo/bar")
		}

		require.NoError(t, env.PachClient.CreateRepo(repo))
		// TODO(global ids): uncomment this code once global ids land and branches have default heads
		//require.NoError(t, env.PachClient.CreateBranch(repo, "master", "", "", nil))
		//checks() // Test the default empty head commit

		_, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		checks() // Test an empty open commit

		require.NoError(t, env.PachClient.FinishCommit(repo, "master", ""))
		checks() // Test an empty closed commit
	})

	suite.Run("ReadSizeLimited", func(t *testing.T) {
		// TODO(2.0 required): Decide on how to expose offset read.
		t.Skip("Offset read exists (inefficient), just need to decide on how to expose it in V2")
		//	t.Parallel()
		//  env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))
		//
		//	require.NoError(t, env.PachClient.CreateRepo("test"))
		//	require.NoError(t, env.PachClient.PutFile("test", "master", "", "file", strings.NewReader(strings.Repeat("a", 100*units.MB))))
		//
		//	var b bytes.Buffer
		//	require.NoError(t, env.PachClient.GetFile("test", "master", "", "file", 0, 2*units.MB, &b))
		//	require.Equal(t, 2*units.MB, b.Len())
		//
		//	b.Reset()
		//	require.NoError(t, env.PachClient.GetFile("test", "master", "", "file", 2*units.MB, 2*units.MB, &b))
		//	require.Equal(t, 2*units.MB, b.Len())
	})

	suite.Run("PutFileURL", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		if testing.Short() {
			t.Skip("Skipping integration tests in short mode")
		}

		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))
		commit, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFileURL(commit, "readme", "https://raw.githubusercontent.com/pachyderm/pachyderm/master/README.md", false))
		check := func() {
			fileInfo, err := env.PachClient.InspectFile(commit, "readme")
			require.NoError(t, err)
			require.True(t, fileInfo.SizeBytes > 0)
		}
		check()
		require.NoError(t, env.PachClient.FinishCommit(repo, commit.Branch.Name, commit.ID))
		check()
	})

	suite.Run("PutFilesURL", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		repo := "repo"
		repoProto := client.NewRepo(repo)
		require.NoError(t, env.PachClient.CreateRepo(repo))
		commit, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		paths := []string{"README.md", "CHANGELOG.md", "CONTRIBUTING.md"}
		for _, path := range paths {
			url := fmt.Sprintf("https://raw.githubusercontent.com/pachyderm/pachyderm/master/%s", path)
			require.NoError(t, env.PachClient.PutFileURL(commit, path, url, false))
		}
		check := func() {
			cis, err := env.PachClient.ListCommit(repoProto, nil, nil, 0)
			require.NoError(t, err)
			require.Equal(t, 1, len(cis))

			for _, path := range paths {
				fileInfo, err := env.PachClient.InspectFile(repoProto.NewCommit("master", ""), path)
				require.NoError(t, err)
				require.True(t, fileInfo.SizeBytes > 0)
			}
		}
		check()
		require.NoError(t, env.PachClient.FinishCommit(repo, commit.Branch.Name, commit.ID))
		check()
	})

	suite.Run("PutFilesObjURL", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		repo := "repo"
		require.NoError(t, env.PachClient.CreateRepo(repo))
		commit, err := env.PachClient.StartCommit(repo, "master")
		masterCommit := client.NewCommit(repo, "master", "")
		require.NoError(t, err)
		objC, bucket := obj.NewTestClient(t)
		paths := []string{"files/foo", "files/bar", "files/fizz"}
		for _, path := range paths {
			writeObj(t, objC, path, path)
		}
		for _, path := range paths {
			url := fmt.Sprintf("local://%s/%s", bucket, path)
			require.NoError(t, env.PachClient.PutFileURL(commit, path, url, false))
		}
		url := fmt.Sprintf("local://%s/files", bucket)
		require.NoError(t, env.PachClient.PutFileURL(commit, "recursive", url, true))
		check := func() {
			cis, err := env.PachClient.ListCommit(client.NewRepo(repo), nil, nil, 0)
			require.NoError(t, err)
			require.Equal(t, 1, len(cis))

			for _, path := range paths {
				var b bytes.Buffer
				require.NoError(t, env.PachClient.GetFile(masterCommit, path, &b))
				require.Equal(t, path, b.String())
				b.Reset()
				require.NoError(t, env.PachClient.GetFile(masterCommit, filepath.Join("recursive", filepath.Base(path)), &b))
				require.Equal(t, path, b.String())
			}
		}
		check()
		require.NoError(t, env.PachClient.FinishCommit(repo, commit.Branch.Name, commit.ID))
		check()
	})

	suite.Run("GetFilesObjURL", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		repo := "repo"
		require.NoError(t, env.PachClient.CreateRepo(repo))
		commit, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		paths := []string{"files/foo", "files/bar", "files/fizz"}
		for _, path := range paths {
			require.NoError(t, env.PachClient.PutFile(commit, path, strings.NewReader(path)))
		}
		check := func() {
			objC, bucket := tu.NewObjectClient(t)
			for _, path := range paths {
				url := fmt.Sprintf("local://%s/", bucket)
				require.NoError(t, env.PachClient.GetFileURL(commit, path, url))
			}
			for _, path := range paths {
				buf := &bytes.Buffer{}
				err := objC.Get(context.Background(), path, buf)
				require.NoError(t, err)
				require.True(t, bytes.Equal([]byte(path), buf.Bytes()))
			}
		}
		check()
		require.NoError(t, env.PachClient.FinishCommit(repo, commit.Branch.Name, commit.ID))
		check()
	})

	suite.Run("PutFileOutputRepo", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		inputRepo, outputRepo := "input", "output"
		require.NoError(t, env.PachClient.CreateRepo(inputRepo))
		require.NoError(t, env.PachClient.CreateRepo(outputRepo))
		inCommit := client.NewCommit(inputRepo, "master", "")
		outCommit := client.NewCommit(outputRepo, "master", "")
		require.NoError(t, env.PachClient.CreateBranch(outputRepo, "master", "", "", []*pfs.Branch{pclient.NewBranch(inputRepo, "master")}))
		require.NoError(t, env.PachClient.PutFile(inCommit, "foo", strings.NewReader("foo\n")))
		require.NoError(t, env.PachClient.PutFile(outCommit, "bar", strings.NewReader("bar\n")))
		require.NoError(t, env.PachClient.FinishCommit(outputRepo, "master", ""))
		fileInfos, err := env.PachClient.ListFileAll(outCommit, "")
		require.NoError(t, err)
		require.Equal(t, 1, len(fileInfos))
		buf := &bytes.Buffer{}
		require.NoError(t, env.PachClient.GetFile(outCommit, "bar", buf))
		require.Equal(t, "bar\n", buf.String())
	})

	suite.Run("FileHistory", func(t *testing.T) {
		// TODO: There is no notion of file history in V2. We could potentially implement this, but
		// we would need to spend some time thinking about the performance characteristics.
		t.Skip("File history is not implemented in V2")
		//	t.Parallel()
		//  env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))
		//
		//	var err error
		//
		//	repo := "test"
		//	require.NoError(t, env.PachClient.CreateRepo(repo))
		//	numCommits := 10
		//	for i := 0; i < numCommits; i++ {
		//		_, err = env.PachClient.PutFile(repo, "master", "file", strings.NewReader("foo\n"))
		//		require.NoError(t, err)
		//	}
		//	fileInfos, err := env.PachClient.ListFileHistory(repo, "master", "file", -1)
		//	require.NoError(t, err)
		//	require.Equal(t, numCommits, len(fileInfos))
		//
		//	for i := 1; i < numCommits; i++ {
		//		fileInfos, err := env.PachClient.ListFileHistory(repo, "master", "file", int64(i))
		//		require.NoError(t, err)
		//		require.Equal(t, i, len(fileInfos))
		//	}
		//
		//	require.NoError(t, env.PachClient.DeleteFile(repo, "master", "file"))
		//	for i := 0; i < numCommits; i++ {
		//		_, err = env.PachClient.PutFile(repo, "master", "file", strings.NewReader("foo\n"))
		//		require.NoError(t, err)
		//		_, err = env.PachClient.PutFile(repo, "master", "unrelated", strings.NewReader("foo\n"))
		//		require.NoError(t, err)
		//	}
		//	fileInfos, err = env.PachClient.ListFileHistory(repo, "master", "file", -1)
		//	require.NoError(t, err)
		//	require.Equal(t, numCommits, len(fileInfos))
		//
		//	for i := 1; i < numCommits; i++ {
		//		fileInfos, err := env.PachClient.ListFileHistory(repo, "master", "file", int64(i))
		//		require.NoError(t, err)
		//		require.Equal(t, i, len(fileInfos))
		//	}
	})

	suite.Run("UpdateRepo", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		var err error
		repo := "test"
		_, err = env.PachClient.PfsAPIClient.CreateRepo(
			env.PachClient.Ctx(),
			&pfs.CreateRepoRequest{
				Repo:   pclient.NewRepo(repo),
				Update: true,
			},
		)
		require.NoError(t, err)
		ri, err := env.PachClient.InspectRepo(repo)
		require.NoError(t, err)
		created, err := types.TimestampFromProto(ri.Created)
		require.NoError(t, err)
		desc := "foo"
		_, err = env.PachClient.PfsAPIClient.CreateRepo(
			env.PachClient.Ctx(),
			&pfs.CreateRepoRequest{
				Repo:        pclient.NewRepo(repo),
				Update:      true,
				Description: desc,
			},
		)
		require.NoError(t, err)
		ri, err = env.PachClient.InspectRepo(repo)
		require.NoError(t, err)
		newCreated, err := types.TimestampFromProto(ri.Created)
		require.NoError(t, err)
		require.Equal(t, created, newCreated)
		require.Equal(t, desc, ri.Description)
	})

	suite.Run("DeferredProcessing", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		require.NoError(t, env.PachClient.CreateRepo("input"))
		require.NoError(t, env.PachClient.CreateRepo("output1"))
		require.NoError(t, env.PachClient.CreateRepo("output2"))
		require.NoError(t, env.PachClient.CreateBranch("output1", "staging", "", "", []*pfs.Branch{pclient.NewBranch("input", "master")}))
		require.NoError(t, env.PachClient.CreateBranch("output2", "staging", "", "", []*pfs.Branch{pclient.NewBranch("output1", "master")}))
		require.NoError(t, env.PachClient.PutFile(client.NewCommit("input", "staging", ""), "file", strings.NewReader("foo")))

		commits, err := env.PachClient.FlushCommitAll([]*pfs.Commit{pclient.NewCommit("input", "staging", "")}, nil)
		require.NoError(t, err)
		require.Equal(t, 0, len(commits))

		require.NoError(t, env.PachClient.CreateBranch("input", "master", "staging", "", nil))
		require.NoError(t, env.PachClient.FinishCommit("output1", "staging", ""))
		commits, err = env.PachClient.FlushCommitAll([]*pfs.Commit{pclient.NewCommit("input", "staging", "")}, nil)
		require.NoError(t, err)
		require.Equal(t, 1, len(commits))

		require.NoError(t, env.PachClient.CreateBranch("output1", "master", "staging", "", nil))
		require.NoError(t, env.PachClient.FinishCommit("output2", "staging", ""))
		commits, err = env.PachClient.FlushCommitAll([]*pfs.Commit{pclient.NewCommit("input", "staging", "")}, nil)
		require.NoError(t, err)
		require.Equal(t, 2, len(commits))
	})

	// MultiInputWithDeferredProcessing tests this DAG:
	//
	// input1 ─▶ deferred-output ─▶ final-output
	//                              ▲
	// input2 ──────────────────────╯
	//
	// For this test to pass, commits in 'final-output' must include commits from
	// the provenance of 'deferred-output', *even if 'deferred-output@master' isn't
	// the branch being propagated*
	suite.Run("MultiInputWithDeferredProcessing", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		require.NoError(t, env.PachClient.CreateRepo("input1"))
		require.NoError(t, env.PachClient.CreateRepo("deferred-output"))
		require.NoError(t, env.PachClient.CreateRepo("input2"))
		require.NoError(t, env.PachClient.CreateRepo("final-output"))
		require.NoError(t, env.PachClient.CreateBranch("deferred-output", "staging", "", "",
			[]*pfs.Branch{pclient.NewBranch("input1", "master")}))
		require.NoError(t, env.PachClient.CreateBranch("final-output", "master", "", "",
			[]*pfs.Branch{
				pclient.NewBranch("input2", "master"),
				pclient.NewBranch("deferred-output", "master"),
			}))
		require.NoError(t, env.PachClient.PutFile(client.NewCommit("input1", "master", ""), "1", strings.NewReader("1")))
		require.NoError(t, env.PachClient.PutFile(client.NewCommit("input2", "master", ""), "2", strings.NewReader("2")))

		deferred := client.NewRepo("deferred-output")
		deferredMaster := deferred.NewCommit("master", "")
		deferredStaging := deferred.NewCommit("staging", "")
		final := client.NewRepo("final-output")
		finalMaster := final.NewCommit("master", "")
		// There should be an open commit in "staging" but not "master"
		cis, err := env.PachClient.ListCommit(deferred, deferredStaging, nil, 0)
		require.NoError(t, err)
		require.Equal(t, 1, len(cis))
		require.NoError(t, env.PachClient.FinishCommit("deferred-output", "staging", ""))
		cis, err = env.PachClient.ListCommit(deferred, deferredMaster, nil, 0)
		require.NoError(t, err)
		require.Equal(t, 0, len(cis))

		// There shouldn't be one output commit in "final-output@master", but with no
		// provenance in deferred-output or input1 (only in input2)
		cis, err = env.PachClient.ListCommit(final, finalMaster, nil, 0)
		require.NoError(t, err)
		require.Equal(t, 1, len(cis))
		require.NoError(t, env.PachClient.FinishCommit("final-output", "master", ""))
		ci, err := env.PachClient.InspectCommit("input2", "master", "")
		require.NoError(t, err)
		expectedProv := map[string]bool{
			path.Join("input2", ci.Commit.Branch.Name, ci.Commit.ID): true,
		}
		ci, err = env.PachClient.InspectCommit("final-output", "master", "")
		require.NoError(t, err)
		require.Equal(t, len(expectedProv), len(ci.Provenance))
		for _, c := range ci.Provenance {
			require.True(t, expectedProv[path.Join(c.Commit.Branch.Repo.Name, c.Commit.Branch.Name, c.Commit.ID)])
		}

		// 1) Move master branch and create second output commit (first w/ full prov)
		require.NoError(t, env.PachClient.CreateBranch("deferred-output", "master", "staging", "", nil))
		require.NoError(t, err)
		cis, err = env.PachClient.ListCommit(final, finalMaster, nil, 0)
		require.NoError(t, err)
		require.Equal(t, 2, len(cis))
		require.NoError(t, env.PachClient.FinishCommit("final-output", "master", ""))

		// Make sure the final output (triggered by deferred-downstream) has the right
		// commit provenance
		expectedProv = make(map[string]bool)
		for _, r := range []string{"input1", "input2", "deferred-output"} {
			ci, err := env.PachClient.InspectCommit(r, "master", "")
			require.NoError(t, err)
			expectedProv[path.Join(r, "master", ci.Commit.ID)] = true
		}
		ci, err = env.PachClient.InspectCommit("final-output", "master", "")
		require.NoError(t, err)
		require.Equal(t, len(expectedProv), len(ci.Provenance))
		for _, c := range ci.Provenance {
			require.True(t, expectedProv[path.Join(c.Commit.Branch.Repo.Name, c.Commit.Branch.Name, c.Commit.ID)])
		}

		// 2) Commit to input2 and create second output commit
		require.NoError(t, env.PachClient.PutFile(client.NewCommit("input2", "master", ""), "3", strings.NewReader("3")))
		cis, err = env.PachClient.ListCommit(final, finalMaster, nil, 0)
		require.NoError(t, err)
		require.Equal(t, 3, len(cis))
		require.NoError(t, env.PachClient.FinishCommit("final-output", "master", ""))

		// Make sure the final output (triggered by second input) has the right
		// commit provenance
		expectedProv = make(map[string]bool)
		for _, r := range []string{"input1", "input2", "deferred-output"} {
			ci, err := env.PachClient.InspectCommit(r, "master", "")
			require.NoError(t, err)
			expectedProv[path.Join(r, "master", ci.Commit.ID)] = true
		}
		ci, err = env.PachClient.InspectCommit("final-output", "master", "")
		require.NoError(t, err)
		require.Equal(t, len(expectedProv), len(ci.Provenance))
		for _, c := range ci.Provenance {
			require.True(t, expectedProv[path.Join(c.Commit.Branch.Repo.Name, c.Commit.Branch.Name, c.Commit.ID)])
		}
	})

	suite.Run("SquashCommitEmptyChild", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		repo := "repo"
		file := "foo"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		commitA, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(repo, "master", "", file, strings.NewReader("foo")))
		require.NoError(t, env.PachClient.FinishCommit(repo, "master", ""))

		_, err = env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit(repo, "master", ""))

		require.NoError(t, env.PachClient.SquashCommit(repo, commitA.Branch.Name, commitA.ID))

		var b bytes.Buffer
		require.NoError(t, env.PachClient.GetFile(repo, "master", "", file, &b))
		require.Equal(t, "foo", b.String())
	})

	suite.Run("CommitProgress", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		require.NoError(t, env.PachClient.CreateRepo("in"))
		require.NoError(t, env.PachClient.CreateRepo("out"))
		require.NoError(t, env.PachClient.CreateBranch("out", "master", "", "", []*pfs.Branch{pclient.NewBranch("in", "master")}))
		require.NoError(t, env.PachClient.PutFile(client.NewCommit("in", "master", ""), "foo", strings.NewReader("foo")))
		ci, err := env.PachClient.InspectCommit("in", "master", "")
		require.NoError(t, err)
		require.Equal(t, int64(1), ci.SubvenantCommitsTotal)
		require.Equal(t, int64(0), ci.SubvenantCommitsSuccess)
		require.Equal(t, int64(0), ci.SubvenantCommitsFailure)
		require.NoError(t, env.PachClient.FinishCommit("out", "master", ""))
		ci, err = env.PachClient.InspectCommit("in", "master", "")
		require.NoError(t, err)
		require.Equal(t, int64(1), ci.SubvenantCommitsTotal)
		require.Equal(t, int64(1), ci.SubvenantCommitsSuccess)
		require.Equal(t, int64(0), ci.SubvenantCommitsFailure)
	})

	suite.Run("ListAll", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		require.NoError(t, env.PachClient.CreateRepo("repo1"))
		require.NoError(t, env.PachClient.CreateRepo("repo2"))
		commit1 := client.NewCommit("repo1", "master", "")
		commit2 := client.NewCommit("repo2", "master", "")
		require.NoError(t, env.PachClient.PutFile(commit1, "file1", strings.NewReader("1")))
		require.NoError(t, env.PachClient.PutFile(commit2, "file2", strings.NewReader("2")))
		require.NoError(t, env.PachClient.PutFile(commit1, "file3", strings.NewReader("3")))
		require.NoError(t, env.PachClient.PutFile(commit2, "file4", strings.NewReader("4")))

		cis, err := env.PachClient.ListCommitByRepo(client.NewRepo(""))
		require.NoError(t, err)
		require.Equal(t, 4, len(cis))

		bis, err := env.PachClient.ListBranch("")
		require.NoError(t, err)
		require.Equal(t, 2, len(bis))
	})

	suite.Run("MonkeyObjectStorage", func(t *testing.T) {
		// This test cannot be done in parallel because the monkey object client
		// modifies global state.
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))
		seedStr := func(seed int64) string {
			return fmt.Sprint("seed: ", strconv.FormatInt(seed, 10))
		}
		monkeyRetry := func(t *testing.T, f func() error, errMsg string) {
			backoff.Retry(func() error {
				err := f()
				if err != nil {
					require.True(t, obj.IsMonkeyError(err), "Expected monkey error (%s), %s", err.Error(), errMsg)
				}
				return err
			}, backoff.NewInfiniteBackOff())
		}
		seed := time.Now().UTC().UnixNano()
		obj.InitMonkeyTest(seed)
		iterations := 25
		repo := "input"
		require.NoError(t, env.PachClient.CreateRepo(repo), seedStr(seed))
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
				commit, err = env.PachClient.StartCommit(repo, "master")
				return err
			}, seedStr(seed))
			// Retry put file until it eventually succeeds.
			monkeyRetry(t, func() error {
				if err := env.PachClient.PutFile(commit, file, strings.NewReader(data)); err != nil {
					// Verify that the file does not exist if an error occurred.
					obj.DisableMonkeyTest()
					defer obj.EnableMonkeyTest()
					buf.Reset()
					err := env.PachClient.GetFile(commit, file, buf)
					require.Matches(t, "not found", err.Error(), seedStr(seed))
				}
				return err
			}, seedStr(seed))
			// Retry get file until it eventually succeeds (before commit is finished).
			monkeyRetry(t, func() error {
				buf.Reset()
				if err = env.PachClient.GetFile(commit, file, buf); err != nil {
					return err
				}
				require.Equal(t, data, buf.String(), seedStr(seed))
				return nil
			}, seedStr(seed))
			// Retry finish commit until it eventually succeeds.
			monkeyRetry(t, func() error {
				return env.PachClient.FinishCommit(repo, commit.Branch.Name, commit.ID)
			}, seedStr(seed))
			// Retry get file until it eventually succeeds (after commit is finished).
			monkeyRetry(t, func() error {
				buf.Reset()
				if err = env.PachClient.GetFile(commit, file, buf); err != nil {
					return err
				}
				require.Equal(t, data, buf.String(), seedStr(seed))
				return nil
			}, seedStr(seed))
		}
	})

	suite.Run("FsckFix", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		input := "input"
		output1 := "output1"
		output2 := "output2"
		require.NoError(t, env.PachClient.CreateRepo(input))
		require.NoError(t, env.PachClient.CreateRepo(output1))
		require.NoError(t, env.PachClient.CreateRepo(output2))
		require.NoError(t, env.PachClient.CreateBranch(output1, "master", "", "", []*pfs.Branch{pclient.NewBranch(input, "master")}))
		require.NoError(t, env.PachClient.CreateBranch(output2, "master", "", "", []*pfs.Branch{pclient.NewBranch(output1, "master")}))
		numCommits := 10
		for i := 0; i < numCommits; i++ {
			require.NoError(t, env.PachClient.PutFile(client.NewCommit(input, "master", ""), "file", strings.NewReader("1")))
		}
		require.NoError(t, env.PachClient.DeleteRepo(input, true))
		require.NoError(t, env.PachClient.CreateRepo(input))
		require.NoError(t, env.PachClient.CreateBranch(input, "master", "", "", nil))
		require.YesError(t, env.PachClient.FsckFastExit())
		// Deleting both repos should error, because they were broken by deleting the upstream repo.
		require.YesError(t, env.PachClient.DeleteRepo(output2, false))
		require.YesError(t, env.PachClient.DeleteRepo(output1, false))
		require.NoError(t, env.PachClient.Fsck(true, func(resp *pfs.FsckResponse) error { return nil }))
		// Deleting should now work due to fixing, must delete 2 before 1 though.
		require.NoError(t, env.PachClient.DeleteRepo(output2, false))
		require.NoError(t, env.PachClient.DeleteRepo(output1, false))
	})

	suite.Run("PutFileAtomic", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		c := env.PachClient
		test := "test"
		require.NoError(t, c.CreateRepo(test))
		testRepo := client.NewRepo(test)
		commit := testRepo.NewCommit("master", "")

		mfc, err := c.NewModifyFileClient(commit)
		require.NoError(t, err)
		require.NoError(t, mfc.PutFile("file1", strings.NewReader("1")))
		require.NoError(t, mfc.PutFile("file2", strings.NewReader("2")))
		require.NoError(t, mfc.Close())

		cis, err := c.ListCommit(testRepo, commit, nil, 0)
		require.NoError(t, err)
		require.Equal(t, 1, len(cis))
		var b bytes.Buffer
		require.NoError(t, c.GetFile(commit, "file1", &b))
		require.Equal(t, "1", b.String())
		b.Reset()
		require.NoError(t, c.GetFile(commit, "file2", &b))
		require.Equal(t, "2", b.String())

		mfc, err = c.NewModifyFileClient(commit)
		require.NoError(t, err)
		require.NoError(t, mfc.PutFile("file3", strings.NewReader("3")))
		require.NoError(t, err)
		require.NoError(t, mfc.DeleteFile("file1"))
		require.NoError(t, mfc.Close())

		cis, err = c.ListCommit(testRepo, commit, nil, 0)
		require.NoError(t, err)
		require.Equal(t, 2, len(cis))
		b.Reset()
		require.NoError(t, c.GetFile(commit, "file3", &b))
		require.Equal(t, "3", b.String())
		b.Reset()
		require.YesError(t, c.GetFile(commit, "file1", &b))

		// TODO(2.0 required): Should this behavior be kept in 2.0? If so, we need
		// to lazily make the one-off commit.
		// Empty ModifyFileClients shouldn't error or create commits
		//mfc, err = c.NewModifyFileClient(test, "master")
		//require.NoError(t, err)
		//require.NoError(t, mfc.Close())
		//cis, err = c.ListCommit(test, "master", "", 0)
		//require.NoError(t, err)
		//require.Equal(t, 2, len(cis))
	})

	const (
		inputRepo          = iota // create a new input repo
		inputBranch               // create a new branch on an existing input repo
		deleteInputBranch         // delete an input branch
		commit                    // commit to an input branch
		squashCommit              // squash commit from an input branch
		outputRepo                // create a new output repo, with master branch subscribed to random other branches
		outputBranch              // create a new output branch on an existing output repo
		deleteOutputBranch        // delete an output branch
	)

	suite.Run("FuzzProvenance", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		seed := time.Now().UnixNano()
		t.Log("Random seed is", seed)
		r := rand.New(rand.NewSource(seed))

		_, err := env.PachClient.PfsAPIClient.DeleteAll(env.PachClient.Ctx(), &types.Empty{})
		require.NoError(t, err)
		nOps := 300
		opShares := []int{
			1, // inputRepo
			1, // inputBranch
			1, // deleteInputBranch
			5, // commit
			3, // squashCommit
			1, // outputRepo
			2, // outputBranch
			1, // deleteOutputBranch
		}
		total := 0
		for _, v := range opShares {
			total += v
		}
		var (
			inputRepos     []string
			inputBranches  []*pfs.Branch
			commits        []*pfs.Commit
			outputRepos    []string
			outputBranches []*pfs.Branch
		)
	OpLoop:
		for i := 0; i < nOps; i++ {
			roll := r.Intn(total)
			if i < 0 {
				roll = inputRepo
			}
			var op int
			for _op, v := range opShares {
				roll -= v
				if roll < 0 {
					op = _op
					break
				}
			}
			switch op {
			case inputRepo:
				repo := tu.UniqueString("repo")
				require.NoError(t, env.PachClient.CreateRepo(repo))
				inputRepos = append(inputRepos, repo)
				require.NoError(t, env.PachClient.CreateBranch(repo, "master", "", "", nil))
				inputBranches = append(inputBranches, pclient.NewBranch(repo, "master"))
			case inputBranch:
				if len(inputRepos) == 0 {
					continue OpLoop
				}
				repo := inputRepos[r.Intn(len(inputRepos))]
				branch := tu.UniqueString("branch")
				require.NoError(t, env.PachClient.CreateBranch(repo, branch, "", "", nil))
				inputBranches = append(inputBranches, pclient.NewBranch(repo, branch))
			case deleteInputBranch:
				if len(inputBranches) == 0 {
					continue OpLoop
				}
				i := r.Intn(len(inputBranches))
				branch := inputBranches[i]
				inputBranches = append(inputBranches[:i], inputBranches[i+1:]...)
				err = env.PachClient.DeleteBranch(branch.Repo.Name, branch.Name, false)
				// don't fail if the error was just that it couldn't delete the branch without breaking subvenance
				if err != nil && !strings.Contains(err.Error(), "break") {
					require.NoError(t, err)
				}
			case commit:
				if len(inputBranches) == 0 {
					continue OpLoop
				}
				branch := inputBranches[r.Intn(len(inputBranches))]
				commit, err := env.PachClient.StartCommit(branch.Repo.Name, branch.Name)
				require.NoError(t, err)
				require.NoError(t, env.PachClient.FinishCommit(branch.Repo.Name, branch.Name, ""))
				commits = append(commits, commit)
			case squashCommit:
				if len(commits) == 0 {
					continue OpLoop
				}
				i := r.Intn(len(commits))
				commit := commits[i]
				commits = append(commits[:i], commits[i+1:]...)
				require.NoError(t, env.PachClient.SquashCommit(commit.Branch.Repo.Name, commit.Branch.Name, commit.ID))
			case outputRepo:
				if len(inputBranches) == 0 {
					continue OpLoop
				}
				repo := tu.UniqueString("repo")
				require.NoError(t, env.PachClient.CreateRepo(repo))
				outputRepos = append(outputRepos, repo)
				var provBranches []*pfs.Branch
				for num, i := range r.Perm(len(inputBranches))[:r.Intn(len(inputBranches))] {
					provBranches = append(provBranches, inputBranches[i])
					if num > 1 {
						break
					}
				}

				require.NoError(t, env.PachClient.CreateBranch(repo, "master", "", "", provBranches))
				outputBranches = append(outputBranches, pclient.NewBranch(repo, "master"))
			case outputBranch:
				if len(outputRepos) == 0 {
					continue OpLoop
				}
				if len(inputBranches) == 0 {
					continue OpLoop
				}
				repo := outputRepos[r.Intn(len(outputRepos))]
				branch := tu.UniqueString("branch")
				var provBranches []*pfs.Branch
				for num, i := range r.Perm(len(inputBranches))[:r.Intn(len(inputBranches))] {
					provBranches = append(provBranches, inputBranches[i])
					if num > 1 {
						break
					}
				}

				if len(outputBranches) > 0 {
					for num, i := range r.Perm(len(outputBranches))[:r.Intn(len(outputBranches))] {
						provBranches = append(provBranches, outputBranches[i])
						if num > 1 {
							break
						}
					}
				}
				require.NoError(t, env.PachClient.CreateBranch(repo, branch, "", "", provBranches))
				outputBranches = append(outputBranches, pclient.NewBranch(repo, branch))
			case deleteOutputBranch:
				if len(outputBranches) == 0 {
					continue OpLoop
				}
				i := r.Intn(len(outputBranches))
				branch := outputBranches[i]
				outputBranches = append(outputBranches[:i], outputBranches[i+1:]...)
				err = env.PachClient.DeleteBranch(branch.Repo.Name, branch.Name, false)
				// don't fail if the error was just that it couldn't delete the branch without breaking subvenance
				if err != nil && !strings.Contains(err.Error(), "break") {
					require.NoError(t, err)
				}
			}
			require.NoError(t, env.PachClient.FsckFastExit())
		}
	})

	// TestAtomicHistory repeatedly writes to a file while concurrently reading
	// its history. This checks for a regression where the repo would sometimes
	// lock.
	suite.Run("AtomicHistory", func(t *testing.T) {
		// TODO: There is no notion of file history in V2. We could potentially implement this, but
		// we would need to spend some time thinking about the performance characteristics.
		t.Skip("File history is not implemented in V2")
		//	t.Parallel()
		//  env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))
		//
		//	repo := "test"
		//	require.NoError(t, env.PachClient.CreateRepo(repo))
		//	require.NoError(t, env.PachClient.CreateBranch(repo, "master", "", nil))
		//	aSize := 1 * 1024 * 1024
		//	bSize := aSize + 1024
		//
		//	for i := 0; i < 10; i++ {
		//		// create a file of all A's
		//		a := strings.Repeat("A", aSize)
		//		_, err := env.PachClient.PutFile(repo, "master", "/file", strings.NewReader(a))
		//		require.NoError(t, err)
		//
		//		// sllowwwllly replace it with all B's
		//		ctx, cancel := context.WithCancel(context.Background())
		//		eg, ctx := errgroup.WithContext(ctx)
		//		eg.Go(func() error {
		//			b := strings.Repeat("B", bSize)
		//			r := SlowReader{underlying: strings.NewReader(b)}
		//			_, err := env.PachClient.PutFile(repo, "master", "/file", &r)
		//			cancel()
		//			return err
		//		})
		//
		//		// should pull /file when it's all A's
		//		eg.Go(func() error {
		//			for {
		//				fileInfos, err := env.PachClient.ListFileHistory(repo, "master", "/file", 1)
		//				require.NoError(t, err)
		//				require.Equal(t, len(fileInfos), 1)
		//
		//				// stop once B's have been written
		//				select {
		//				case <-ctx.Done():
		//					return nil
		//				default:
		//					time.Sleep(1 * time.Millisecond)
		//				}
		//			}
		//		})
		//
		//		require.NoError(t, eg.Wait())
		//
		//		// should pull /file when it's all B's
		//		fileInfos, err := env.PachClient.ListFileHistory(repo, "master", "/file", 1)
		//		require.NoError(t, err)
		//		require.Equal(t, 1, len(fileInfos))
		//		require.Equal(t, bSize, int(fileInfos[0].SizeBytes))
		//	}
	})

	// TestTrigger tests branch triggers
	// TODO(2.0 required): This test does not work with V2. It is not clear what the issue is yet. Something noteworthy is that the output
	// size does not increase for the second to last commit, which may have something to do with compaction (the trigger won't kick
	// off since it is based on the output size). The output size calculation is a bit questionable due to the compaction delay (a
	// file may be accounted for in the size even if it is deleted).
	suite.Run("Trigger", func(t *testing.T) {
		t.Skip("Does not work with V2, needs investigation")
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		c := env.PachClient
		t.Run("Simple", func(t *testing.T) {
			require.NoError(t, c.CreateRepo("test"))
			require.NoError(t, c.CreateBranchTrigger("test", "master", "", "", &pfs.Trigger{
				Branch: "staging",
				Size_:  "1B",
			}))
			require.NoError(t, c.PutFile(client.NewCommit("test", "staging", ""), "file", strings.NewReader("small")))
		})
		t.Run("SizeWithProvenance", func(t *testing.T) {
			require.NoError(t, c.CreateRepo("in"))
			require.NoError(t, c.CreateBranchTrigger("in", "trigger", "", "", &pfs.Trigger{
				Branch: "master",
				Size_:  "1K",
			}))
			inCommit := client.NewCommit("in", "master", "")
			bis, err := c.ListBranch("in")
			require.NoError(t, err)
			require.Equal(t, 1, len(bis))
			require.Nil(t, bis[0].Head)

			// Create a downstream branch
			require.NoError(t, c.CreateRepo("out"))
			require.NoError(t, c.CreateBranch("out", "master", "", "", []*pfs.Branch{pclient.NewBranch("in", "trigger")}))
			require.NoError(t, c.CreateBranchTrigger("out", "trigger", "", "", &pfs.Trigger{
				Branch: "master",
				Size_:  "1K",
			}))

			// Write a small file, too small to trigger
			require.NoError(t, c.PutFile(inCommit, "file", strings.NewReader("small")))
			bi, err := c.InspectBranch("in", "trigger")
			require.NoError(t, err)
			require.Nil(t, bi.Head)
			bi, err = c.InspectBranch("out", "master")
			require.NoError(t, err)
			require.Nil(t, bi.Head)
			bi, err = c.InspectBranch("out", "trigger")
			require.NoError(t, err)
			require.Nil(t, bi.Head)

			require.NoError(t, c.PutFile(inCommit, "file", strings.NewReader(strings.Repeat("a", units.KB))))

			bi, err = c.InspectBranch("in", "trigger")
			require.NoError(t, err)
			require.NotNil(t, bi.Head)

			// Output branch should have a commit now
			bi, err = c.InspectBranch("out", "master")
			require.NoError(t, err)
			require.NotNil(t, bi.Head)

			// Put a file that will cause the trigger to go off
			require.NoError(t, c.PutFile(client.NewCommit("out", "master", ""), "file", strings.NewReader(strings.Repeat("a", units.KB))))
			require.NoError(t, env.PachClient.FinishCommit("out", "master", ""))

			// Output trigger should have triggered
			bi, err = c.InspectBranch("out", "trigger")
			require.NoError(t, err)
			require.NotNil(t, bi.Head)
		})
		t.Run("Cron", func(t *testing.T) {
			require.NoError(t, c.CreateRepo("cron"))
			require.NoError(t, c.CreateBranchTrigger("cron", "trigger", "", "", &pfs.Trigger{
				Branch:   "master",
				CronSpec: "* * * * *", // every minute
			}))
			cronCommit := client.NewCommit("cron", "master", "")
			// The first commit should always trigger a cron
			require.NoError(t, c.PutFile(cronCommit, "file1", strings.NewReader("foo")))
			bi, err := c.InspectBranch("cron", "trigger")
			require.NoError(t, err)
			require.NotNil(t, bi.Head)
			head := bi.Head.ID

			// Second commit should not trigger the cron because less than a
			// minute has passed
			require.NoError(t, c.PutFile(cronCommit, "file2", strings.NewReader("bar")))
			bi, err = c.InspectBranch("cron", "trigger")
			require.NoError(t, err)
			require.Equal(t, head, bi.Head.ID)

			time.Sleep(time.Minute)
			// Third commit should trigger the cron because a minute has passed
			require.NoError(t, c.PutFile(cronCommit, "file3", strings.NewReader("fizz")))
			bi, err = c.InspectBranch("cron", "trigger")
			require.NoError(t, err)
			require.NotEqual(t, head, bi.Head.ID)
		})
		t.Run("Count", func(t *testing.T) {
			require.NoError(t, c.CreateRepo("count"))
			require.NoError(t, c.CreateBranchTrigger("count", "trigger", "", "", &pfs.Trigger{
				Branch:  "master",
				Commits: 2, // trigger every 2 commits
			}))
			countCommit := client.NewCommit("count", "master", "")
			// The first commit shouldn't trigger
			require.NoError(t, c.PutFile(countCommit, "file1", strings.NewReader("foo")))
			bi, err := c.InspectBranch("count", "trigger")
			require.NoError(t, err)
			require.Nil(t, bi.Head)

			// Second commit should trigger
			require.NoError(t, c.PutFile(countCommit, "file2", strings.NewReader("bar")))
			bi, err = c.InspectBranch("count", "trigger")
			require.NoError(t, err)
			require.NotNil(t, bi.Head)
			head := bi.Head.ID

			// Third commit shouldn't trigger
			require.NoError(t, c.PutFile(countCommit, "file3", strings.NewReader("fizz")))
			bi, err = c.InspectBranch("count", "trigger")
			require.NoError(t, err)
			require.Equal(t, head, bi.Head.ID)

			// Fourth commit should trigger
			require.NoError(t, c.PutFile(countCommit, "file4", strings.NewReader("buzz")))
			bi, err = c.InspectBranch("count", "trigger")
			require.NoError(t, err)
			require.NotEqual(t, head, bi.Head.ID)
		})
		t.Run("Or", func(t *testing.T) {
			require.NoError(t, c.CreateRepo("or"))
			require.NoError(t, c.CreateBranchTrigger("or", "trigger", "", "", &pfs.Trigger{
				Branch:   "master",
				CronSpec: "* * * * *",
				Size_:    "100",
				Commits:  3,
			}))
			orCommit := client.NewCommit("or", "master", "")
			// This triggers, because the cron is satisfied
			require.NoError(t, c.PutFile(orCommit, "file1", strings.NewReader(strings.Repeat("a", 1))))
			bi, err := c.InspectBranch("or", "trigger")
			require.NoError(t, err)
			require.NotNil(t, bi.Head)
			head := bi.Head.ID
			// This one doesn't because none of them are satisfied
			require.NoError(t, c.PutFile(orCommit, "file2", strings.NewReader(strings.Repeat("a", 50))))
			bi, err = c.InspectBranch("or", "trigger")
			require.NoError(t, err)
			require.Equal(t, head, bi.Head.ID)
			// This one triggers because we hit 100 bytes
			require.NoError(t, c.PutFile(orCommit, "file3", strings.NewReader(strings.Repeat("a", 50))))
			bi, err = c.InspectBranch("or", "trigger")
			require.NoError(t, err)
			require.NotEqual(t, head, bi.Head.ID)
			head = bi.Head.ID

			// This one doesn't trigger
			require.NoError(t, c.PutFile(orCommit, "file4", strings.NewReader(strings.Repeat("a", 1))))
			bi, err = c.InspectBranch("or", "trigger")
			require.NoError(t, err)
			require.Equal(t, head, bi.Head.ID)
			// This one neither
			require.NoError(t, c.PutFile(orCommit, "file5", strings.NewReader(strings.Repeat("a", 1))))
			bi, err = c.InspectBranch("or", "trigger")
			require.NoError(t, err)
			require.Equal(t, head, bi.Head.ID)
			// This one does, because it's 3 commits
			require.NoError(t, c.PutFile(orCommit, "file6", strings.NewReader(strings.Repeat("a", 1))))
			bi, err = c.InspectBranch("or", "trigger")
			require.NoError(t, err)
			require.NotEqual(t, head, bi.Head.ID)
			head = bi.Head.ID

			// This one doesn't trigger
			require.NoError(t, c.PutFile(orCommit, "file7", strings.NewReader(strings.Repeat("a", 1))))
			bi, err = c.InspectBranch("or", "trigger")
			require.NoError(t, err)
			require.Equal(t, head, bi.Head.ID)

			time.Sleep(time.Minute)

			require.NoError(t, c.PutFile(orCommit, "file8", strings.NewReader(strings.Repeat("a", 1))))
			bi, err = c.InspectBranch("or", "trigger")
			require.NoError(t, err)
			require.NotEqual(t, head, bi.Head.ID)
		})
		t.Run("And", func(t *testing.T) {
			require.NoError(t, c.CreateRepo("and"))
			require.NoError(t, c.CreateBranchTrigger("and", "trigger", "", "", &pfs.Trigger{
				Branch:   "master",
				All:      true,
				CronSpec: "* * * * *",
				Size_:    "100",
				Commits:  3,
			}))
			andCommit := client.NewCommit("and", "master", "")
			// Doesn't trigger because all 3 conditions must be met
			require.NoError(t, c.PutFile(andCommit, "file1", strings.NewReader(strings.Repeat("a", 100))))
			bi, err := c.InspectBranch("and", "trigger")
			require.NoError(t, err)
			require.Nil(t, bi.Head)

			// Still doesn't trigger
			require.NoError(t, c.PutFile(andCommit, "file2", strings.NewReader(strings.Repeat("a", 100))))
			bi, err = c.InspectBranch("and", "trigger")
			require.NoError(t, err)
			require.Nil(t, bi.Head)

			// Finally triggers because we have 3 commits, 100 bytes and Cron
			// Spec (since epoch) is satisfied.
			require.NoError(t, c.PutFile(andCommit, "file3", strings.NewReader(strings.Repeat("a", 100))))
			bi, err = c.InspectBranch("and", "trigger")
			require.NoError(t, err)
			require.NotNil(t, bi.Head)
			head := bi.Head.ID

			// Doesn't trigger because all 3 conditions must be met
			require.NoError(t, c.PutFile(andCommit, "file4", strings.NewReader(strings.Repeat("a", 100))))
			bi, err = c.InspectBranch("and", "trigger")
			require.NoError(t, err)
			require.Equal(t, head, bi.Head.ID)

			// Still no trigger, not enough time or commits
			require.NoError(t, c.PutFile(andCommit, "file5", strings.NewReader(strings.Repeat("a", 100))))
			bi, err = c.InspectBranch("and", "trigger")
			require.NoError(t, err)
			require.Equal(t, head, bi.Head.ID)

			// Still no trigger, not enough time
			require.NoError(t, c.PutFile(andCommit, "file6", strings.NewReader(strings.Repeat("a", 100))))
			bi, err = c.InspectBranch("and", "trigger")
			require.NoError(t, err)
			require.Equal(t, head, bi.Head.ID)

			time.Sleep(time.Minute)

			// Finally triggers, all triggers have been met
			require.NoError(t, c.PutFile(andCommit, "file7", strings.NewReader(strings.Repeat("a", 100))))
			bi, err = c.InspectBranch("and", "trigger")
			require.NoError(t, err)
			require.NotEqual(t, head, bi.Head.ID)
		})
		t.Run("Chain", func(t *testing.T) {
			// a triggers b which triggers c
			require.NoError(t, c.CreateRepo("chain"))
			require.NoError(t, c.CreateBranchTrigger("chain", "b", "", "", &pfs.Trigger{
				Branch: "a",
				Size_:  "100",
			}))
			require.NoError(t, c.CreateBranchTrigger("chain", "c", "", "", &pfs.Trigger{
				Branch: "b",
				Size_:  "200",
			}))
			aCommit := client.NewCommit("chain", "a", "")
			// Triggers nothing
			require.NoError(t, c.PutFile(aCommit, "file1", strings.NewReader(strings.Repeat("a", 50))))
			bi, err := c.InspectBranch("chain", "b")
			require.NoError(t, err)
			require.Nil(t, bi.Head)
			bi, err = c.InspectBranch("chain", "c")
			require.NoError(t, err)
			require.Nil(t, bi.Head)

			// Triggers b, but not c
			require.NoError(t, c.PutFile(aCommit, "file2", strings.NewReader(strings.Repeat("a", 50))))
			bi, err = c.InspectBranch("chain", "b")
			require.NoError(t, err)
			require.NotNil(t, bi.Head)
			bHead := bi.Head.ID
			bi, err = c.InspectBranch("chain", "c")
			require.NoError(t, err)
			require.Nil(t, bi.Head)

			// Triggers nothing
			require.NoError(t, c.PutFile(aCommit, "file3", strings.NewReader(strings.Repeat("a", 50))))
			bi, err = c.InspectBranch("chain", "b")
			require.NoError(t, err)
			require.NotNil(t, bi.Head)
			require.Equal(t, bHead, bi.Head.ID)
			bi, err = c.InspectBranch("chain", "c")
			require.NoError(t, err)
			require.Nil(t, bi.Head)

			// Triggers a and c
			require.NoError(t, c.PutFile(aCommit, "file4", strings.NewReader(strings.Repeat("a", 50))))
			bi, err = c.InspectBranch("chain", "b")
			require.NoError(t, err)
			require.NotNil(t, bi.Head)
			require.NotEqual(t, bHead, bi.Head.ID)
			bHead = bi.Head.ID
			bi, err = c.InspectBranch("chain", "c")
			require.NoError(t, err)
			require.NotNil(t, bi.Head)
			cHead := bi.Head.ID

			// Triggers nothing
			require.NoError(t, c.PutFile(aCommit, "file5", strings.NewReader(strings.Repeat("a", 50))))
			bi, err = c.InspectBranch("chain", "b")
			require.NoError(t, err)
			require.NotNil(t, bi.Head)
			require.Equal(t, bHead, bi.Head.ID)
			bi, err = c.InspectBranch("chain", "c")
			require.NoError(t, err)
			require.NotNil(t, bi.Head)
			require.Equal(t, cHead, bi.Head.ID)
		})
		t.Run("BranchMovement", func(t *testing.T) {
			require.NoError(t, c.CreateRepo("branch-movement"))
			require.NoError(t, c.CreateBranchTrigger("branch-movement", "c", "", "", &pfs.Trigger{
				Branch: "b",
				Size_:  "100",
			}))
			moveCommit := client.NewCommit("branch-movement", "a", "")

			require.NoError(t, c.PutFile(moveCommit, "file1", strings.NewReader(strings.Repeat("a", 50))))
			require.NoError(t, c.CreateBranch("branch-movement", "b", "a", "", nil))
			bi, err := c.InspectBranch("branch-movement", "c")
			require.NoError(t, err)
			require.Nil(t, bi.Head)

			require.NoError(t, c.PutFile(moveCommit, "file2", strings.NewReader(strings.Repeat("a", 50))))
			require.NoError(t, c.CreateBranch("branch-movement", "b", "a", "", nil))
			bi, err = c.InspectBranch("branch-movement", "c")
			require.NoError(t, err)
			require.NotNil(t, bi.Head)
			cHead := bi.Head.ID

			require.NoError(t, c.PutFile(moveCommit, "file3", strings.NewReader(strings.Repeat("a", 50))))
			require.NoError(t, c.CreateBranch("branch-movement", "b", "a", "", nil))
			bi, err = c.InspectBranch("branch-movement", "c")
			require.NoError(t, err)
			require.NotNil(t, bi.Head)
			require.Equal(t, cHead, bi.Head.ID)
		})
	})

	// TriggerValidation tests branch trigger validation
	suite.Run("TriggerValidation", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		c := env.PachClient
		require.NoError(t, c.CreateRepo("repo"))
		// Must specify a branch
		require.YesError(t, c.CreateBranchTrigger("repo", "master", "", "", &pfs.Trigger{
			Branch: "",
			Size_:  "1K",
		}))
		// Can't trigger a branch on itself
		require.YesError(t, c.CreateBranchTrigger("repo", "master", "", "", &pfs.Trigger{
			Branch: "master",
			Size_:  "1K",
		}))
		// Size doesn't parse
		require.YesError(t, c.CreateBranchTrigger("repo", "trigger", "", "", &pfs.Trigger{
			Branch: "master",
			Size_:  "this is not a size",
		}))
		// Can't have negative commit count
		require.YesError(t, c.CreateBranchTrigger("repo", "trigger", "", "", &pfs.Trigger{
			Branch:  "master",
			Commits: -1,
		}))

		// a -> b (valid, sets up the next test)
		require.NoError(t, c.CreateBranchTrigger("repo", "b", "", "", &pfs.Trigger{
			Branch: "a",
			Size_:  "1K",
		}))
		// Can't have circular triggers
		require.YesError(t, c.CreateBranchTrigger("repo", "a", "", "", &pfs.Trigger{
			Branch: "b",
			Size_:  "1K",
		}))
		// CronSpec doesn't parse
		require.YesError(t, c.CreateBranchTrigger("repo", "trigger", "", "", &pfs.Trigger{
			Branch:   "master",
			CronSpec: "this is not a cron spec",
		}))
		// Can't use a trigger and provenance together
		require.NoError(t, c.CreateRepo("in"))
		_, err := c.PfsAPIClient.CreateBranch(c.Ctx(),
			&pfs.CreateBranchRequest{
				Branch: pclient.NewBranch("repo", "master"),
				Trigger: &pfs.Trigger{
					Branch: "master",
					Size_:  "1K",
				},
				Provenance: []*pfs.Branch{pclient.NewBranch("in", "master")},
			})
		require.YesError(t, err)
	})

	suite.Run("RegressionOrphanedFile", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		fsclient, err := env.PachClient.NewCreateFilesetClient()
		require.NoError(t, err)
		data := []byte("test data")
		spec := fileSetSpec{
			"file1.txt": tarutil.NewMemFile("file1.txt", data),
			"file2.txt": tarutil.NewMemFile("file2.txt", data),
		}
		require.NoError(t, fsclient.PutFileTar(spec.makeTarStream()))
		resp, err := fsclient.Close()
		require.NoError(t, err)
		t.Logf("tmp fileset id: %s", resp.FilesetId)
		require.NoError(t, env.PachClient.RenewFileSet(resp.FilesetId, 60*time.Second))
		fis, err := env.PachClient.ListFileAll(client.NewCommit(pclient.FileSetsRepoName, "", resp.FilesetId), "/")
		require.NoError(t, err)
		require.Equal(t, 2, len(fis))
	})

	suite.Run("Compaction", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, func(config *serviceenv.Configuration) {
			config.StorageCompactionMaxFanIn = 10
		}, tu.NewTestDBConfig(t))

		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))
		commit1, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)

		const (
			nFileSets   = 100
			filesPer    = 10
			fileSetSize = 1e3
		)
		for i := 0; i < nFileSets; i++ {
			fsSpec := fileSetSpec{}
			for j := 0; j < filesPer; j++ {
				name := fmt.Sprintf("file%02d", j)
				data, err := ioutil.ReadAll(randomReader(fileSetSize))
				require.NoError(t, err)
				file := tarutil.NewMemFile(name, data)
				hdr, err := file.Header()
				require.NoError(t, err)
				fsSpec[hdr.Name] = file
			}
			require.NoError(t, env.PachClient.PutFileTar(commit1, fsSpec.makeTarStream()))
			runtime.GC()
		}
		require.NoError(t, env.PachClient.FinishCommit(repo, commit1.Branch.Name, commit1.ID))
	})

	suite.Run("ModifyFileGRPC", func(subsuite *testing.T) {
		subsuite.Parallel()

		subsuite.Run("EmptyFile", func(t *testing.T) {
			t.Parallel()
			env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))
			repo := "test"
			require.NoError(t, env.PachClient.CreateRepo(repo))
			c, err := env.PachClient.PfsAPIClient.ModifyFile(context.Background())
			require.NoError(t, err)
			files := []string{"/empty-1", "/empty-2"}
			for _, file := range files {
				require.NoError(t, c.Send(&pfs.ModifyFileRequest{
					Commit: pclient.NewCommit(repo, "master", ""),
					Modification: &pfs.ModifyFileRequest_PutFile{
						PutFile: &pfs.PutFile{
							Source: &pfs.PutFile_RawFileSource{
								RawFileSource: &pfs.RawFileSource{
									Path: file,
									EOF:  true,
								},
							},
						},
					},
				}))
			}
			_, err = c.CloseAndRecv()
			require.NoError(t, err)
			require.NoError(t, env.PachClient.ListFile(client.NewCommit(repo, "master", ""), "/", func(fi *pfs.FileInfo) error {
				require.True(t, files[0] == fi.File.Path)
				files = files[1:]
				return nil
			}))
			require.Equal(t, 0, len(files))
		})

		subsuite.Run("SingleMessageFile", func(t *testing.T) {
			t.Parallel()
			env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))
			repo := "test"
			require.NoError(t, env.PachClient.CreateRepo(repo))
			filePath := "file"
			fileContent := "foo"
			c, err := env.PachClient.PfsAPIClient.ModifyFile(context.Background())
			require.NoError(t, err)
			require.NoError(t, c.Send(&pfs.ModifyFileRequest{
				Commit: pclient.NewCommit(repo, "master", ""),
				Modification: &pfs.ModifyFileRequest_PutFile{
					PutFile: &pfs.PutFile{
						Source: &pfs.PutFile_RawFileSource{
							RawFileSource: &pfs.RawFileSource{
								Path: filePath,
								Data: []byte(fileContent),
								EOF:  true,
							},
						},
					},
				},
			}))
			_, err = c.CloseAndRecv()
			require.NoError(t, err)
			buf := &bytes.Buffer{}
			require.NoError(t, env.PachClient.GetFile(client.NewCommit(repo, "master", ""), filePath, buf))
			require.Equal(t, fileContent, buf.String())
		})
	})

	suite.Run("TestPanicOnNilArgs", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))
		c := env.PachClient
		requireNoPanic := func(err error) {
			t.Helper()
			if err != nil {
				// if a "transport is closing" error happened, pachd abruptly
				// closed the connection. Most likely this is caused by a panic.
				require.False(t, strings.Contains(err.Error(), "transport is closing"), err.Error())
			}
		}
		_, err := c.PfsAPIClient.CreateRepo(c.Ctx(), &pfs.CreateRepoRequest{})
		requireNoPanic(err)
		_, err = c.PfsAPIClient.InspectRepo(c.Ctx(), &pfs.InspectRepoRequest{})
		requireNoPanic(err)
		_, err = c.PfsAPIClient.ListRepo(c.Ctx(), &pfs.ListRepoRequest{})
		requireNoPanic(err)
		_, err = c.PfsAPIClient.DeleteRepo(c.Ctx(), &pfs.DeleteRepoRequest{})
		requireNoPanic(err)
		_, err = c.PfsAPIClient.StartCommit(c.Ctx(), &pfs.StartCommitRequest{})
		requireNoPanic(err)
		_, err = c.PfsAPIClient.FinishCommit(c.Ctx(), &pfs.FinishCommitRequest{})
		requireNoPanic(err)
		_, err = c.PfsAPIClient.InspectCommit(c.Ctx(), &pfs.InspectCommitRequest{})
		requireNoPanic(err)
		_, err = c.PfsAPIClient.ListCommit(c.Ctx(), &pfs.ListCommitRequest{})
		requireNoPanic(err)
		_, err = c.PfsAPIClient.SquashCommit(c.Ctx(), &pfs.SquashCommitRequest{})
		requireNoPanic(err)
		_, err = c.PfsAPIClient.FlushCommit(c.Ctx(), &pfs.FlushCommitRequest{})
		requireNoPanic(err)
		_, err = c.PfsAPIClient.SubscribeCommit(c.Ctx(), &pfs.SubscribeCommitRequest{})
		requireNoPanic(err)
		_, err = c.PfsAPIClient.CreateBranch(c.Ctx(), &pfs.CreateBranchRequest{})
		requireNoPanic(err)
		_, err = c.PfsAPIClient.InspectBranch(c.Ctx(), &pfs.InspectBranchRequest{})
		requireNoPanic(err)
		_, err = c.PfsAPIClient.ListBranch(c.Ctx(), &pfs.ListBranchRequest{})
		requireNoPanic(err)
		_, err = c.PfsAPIClient.DeleteBranch(c.Ctx(), &pfs.DeleteBranchRequest{})
		requireNoPanic(err)
		_, err = c.PfsAPIClient.GetFileTAR(c.Ctx(), &pfs.GetFileRequest{})
		requireNoPanic(err)
		_, err = c.PfsAPIClient.InspectFile(c.Ctx(), &pfs.InspectFileRequest{})
		requireNoPanic(err)
		_, err = c.PfsAPIClient.ListFile(c.Ctx(), &pfs.ListFileRequest{})
		requireNoPanic(err)
		_, err = c.PfsAPIClient.WalkFile(c.Ctx(), &pfs.WalkFileRequest{})
		requireNoPanic(err)
		_, err = c.PfsAPIClient.GlobFile(c.Ctx(), &pfs.GlobFileRequest{})
		requireNoPanic(err)
		_, err = c.PfsAPIClient.DiffFile(c.Ctx(), &pfs.DiffFileRequest{})
		requireNoPanic(err)
		_, err = c.PfsAPIClient.Fsck(c.Ctx(), &pfs.FsckRequest{})
		requireNoPanic(err)
	})

	suite.Run("DuplicateFileDifferentTag", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))
		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))
		masterCommit := client.NewCommit(repo, "master", "")
		require.NoError(t, env.PachClient.WithModifyFileClient(masterCommit, func(mf pclient.ModifyFile) error {
			require.NoError(t, mf.PutFile("foo", strings.NewReader("foo\n"), pclient.WithTagPutFile("tag1")))
			require.NoError(t, mf.PutFile("foo", strings.NewReader("foo\n"), pclient.WithTagPutFile("tag2")))
			require.NoError(t, mf.PutFile("bar", strings.NewReader("bar\n")))
			return nil
		}))
		newFile := func(repo, branch, commit, path, tag string) *pfs.File {
			file := pclient.NewFile(repo, branch, commit, path)
			file.Tag = tag
			return file
		}
		expected := []*pfs.File{newFile(repo, "master", "", "/bar", fileset.DefaultFileTag), newFile(repo, "master", "", "/foo", "tag1"), newFile(repo, "master", "", "/foo", "tag2")}
		require.NoError(t, env.PachClient.ListFile(masterCommit, "", func(fi *pfs.FileInfo) error {
			require.Equal(t, expected[0].Path, fi.File.Path)
			require.Equal(t, expected[0].Tag, fi.File.Tag)
			expected = expected[1:]
			return nil
		}))
		require.Equal(t, 0, len(expected))
	})
}

var (
	randSeed = int64(0)
	randMu   sync.Mutex
)

func writeObj(t *testing.T, c obj.Client, path, content string) {
	err := c.Put(context.Background(), path, strings.NewReader(content))
	require.NoError(t, err)
}

type SlowReader struct {
	underlying io.Reader
	delay      time.Duration
}

func (r *SlowReader) Read(p []byte) (n int, err error) {
	n, err = r.underlying.Read(p)
	if r.delay == 0 {
		time.Sleep(1 * time.Millisecond)
	} else {
		time.Sleep(r.delay)
	}
	return
}

func getRand() *rand.Rand {
	randMu.Lock()
	seed := randSeed
	randSeed++
	randMu.Unlock()
	return rand.New(rand.NewSource(seed))
}

func randomReader(n int) io.Reader {
	return io.LimitReader(getRand(), int64(n))
}

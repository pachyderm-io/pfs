package testing

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
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
	pfsserver "github.com/pachyderm/pachyderm/v2/src/server/pfs"
	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func CommitToID(commit interface{}) interface{} {
	return pfsdb.CommitKey(commit.(*pfs.Commit))
}

func CommitInfoToID(commit interface{}) interface{} {
	return pfsdb.CommitKey(commit.(*pfs.CommitInfo).Commit)
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
				errCh <- env.PachClient.CreateBranch("bar", "master", "", "", []*pfs.Branch{client.NewBranch("foo", "master")})
			}()
			err1 := <-errCh
			err2 := <-errCh
			// these two operations should never race in such a way that they
			// both succeed, leaving us with a repo bar that has a nonexistent
			// provenance foo
			require.True(t, err1 != nil || err2 != nil)
			require.NoError(t, env.PachClient.DeleteRepo("bar", true))
			require.NoError(t, env.PachClient.DeleteRepo("foo", true))
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
		require.NoError(t, env.PachClient.CreateBranch("out", "master", "", "", []*pfs.Branch{client.NewBranch("in", "master")}))
		require.NoError(t, env.PachClient.FinishCommit("out", "master", ""))
		outRepo := client.NewRepo("out")

		// Create initial input commit, and make sure we get an output commit
		require.NoError(t, env.PachClient.PutFile(client.NewCommit("in", "master", ""), "1", strings.NewReader("1")))
		cis, err := env.PachClient.ListCommit(outRepo, outRepo.NewCommit("master", ""), nil, 0)
		require.NoError(t, err)
		require.Equal(t, 2, len(cis))
		require.NoError(t, env.PachClient.FinishCommit("out", "master", ""))
		// make sure the output commit and input commit have the same ID
		inCommitInfo, err := env.PachClient.InspectCommit("in", "master", "")
		require.NoError(t, err)
		outCommitInfo, err := env.PachClient.InspectCommit("out", "master", "")
		require.NoError(t, err)
		require.Equal(t, inCommitInfo.Commit.ID, outCommitInfo.Commit.ID)

		// Toggle out@master provenance off
		require.NoError(t, env.PachClient.CreateBranch("out", "master", "master", "", nil))

		// Create new input commit & make sure no new output commit is created
		require.NoError(t, env.PachClient.PutFile(client.NewCommit("in", "master", ""), "2", strings.NewReader("2")))
		cis, err = env.PachClient.ListCommit(outRepo, outRepo.NewCommit("master", ""), nil, 0)
		require.NoError(t, err)
		require.Equal(t, 2, len(cis))
		// make sure output commit still matches the old input commit
		inCommitInfo, err = env.PachClient.InspectCommit("in", "", "master~1") // old input commit
		require.NoError(t, err)
		outCommitInfo, err = env.PachClient.InspectCommit("out", "master", "")
		require.NoError(t, err)
		require.Equal(t, inCommitInfo.Commit.ID, outCommitInfo.Commit.ID)

		// Toggle out@master provenance back on, creating a new output commit
		require.NoError(t, env.PachClient.CreateBranch("out", "master", "", "", []*pfs.Branch{
			client.NewBranch("in", "master"),
		}))
		cis, err = env.PachClient.ListCommit(outRepo, outRepo.NewCommit("master", ""), nil, 0)
		require.NoError(t, err)
		require.Equal(t, 3, len(cis))

		// make sure new output commit has the same ID as the new input commit
		inCommitInfo, err = env.PachClient.InspectCommit("in", "master", "")
		require.NoError(t, err)
		outCommitInfo, err = env.PachClient.InspectCommit("out", "master", "")
		require.NoError(t, err)
		require.Equal(t, inCommitInfo.Commit.ID, outCommitInfo.Commit.ID)
	})

	suite.Run("RecreateBranchProvenance", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		require.NoError(t, env.PachClient.CreateRepo("in"))
		require.NoError(t, env.PachClient.CreateRepo("out"))
		require.NoError(t, env.PachClient.CreateBranch("out", "master", "", "", []*pfs.Branch{client.NewBranch("in", "master")}))
		require.NoError(t, env.PachClient.FinishCommit("out", "master", ""))
		require.NoError(t, env.PachClient.PutFile(client.NewCommit("in", "master", ""), "foo", strings.NewReader("foo")))
		outRepo := client.NewRepo("out")
		cis, err := env.PachClient.ListCommit(outRepo, nil, nil, 0)
		require.NoError(t, err)
		require.Equal(t, 2, len(cis))
		commit1 := cis[0].Commit
		require.NoError(t, env.PachClient.DeleteBranch("out", "master", false))
		require.NoError(t, env.PachClient.FinishCommit("out", "", commit1.ID))
		require.NoError(t, env.PachClient.CreateBranch("out", "master", "", commit1.ID, []*pfs.Branch{client.NewBranch("in", "master")}))
		cis, err = env.PachClient.ListCommit(outRepo, nil, nil, 0)
		require.NoError(t, err)
		require.Equal(t, 2, len(cis))
		require.Equal(t, commit1, cis[0].Commit)
	})

	suite.Run("RewindBranch", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		require.NoError(t, env.PachClient.CreateRepo("a"))
		require.NoError(t, env.PachClient.CreateRepo("b"))
		require.NoError(t, env.PachClient.CreateRepo("c"))
		provB := []*pfs.Branch{client.NewBranch("a", "master")}
		require.NoError(t, env.PachClient.CreateBranch("b", "master", "", "", provB))
		provC := []*pfs.Branch{client.NewBranch("b", "master")}
		require.NoError(t, env.PachClient.CreateBranch("c", "master", "", "", provC))

		commit1, err := env.PachClient.StartCommit("a", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(commit1, "file", strings.NewReader("1")))
		require.NoError(t, env.PachClient.FinishCommit(commit1.Branch.Repo.Name, commit1.Branch.Name, commit1.ID))
		commit2, err := env.PachClient.StartCommit("a", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(commit2, "file", strings.NewReader("2")))
		require.NoError(t, env.PachClient.FinishCommit(commit2.Branch.Repo.Name, commit1.Branch.Name, commit2.ID))
		commit3, err := env.PachClient.StartCommit("a", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(commit3, "file", strings.NewReader("3")))
		require.NoError(t, env.PachClient.FinishCommit(commit3.Branch.Repo.Name, commit1.Branch.Name, commit3.ID))

		checkRepoCommits := func(commits []*pfs.Commit) {
			ids := []string{}
			for _, c := range commits {
				ids = append(ids, c.ID)
			}
			for _, repo := range []string{"a", "b", "c"} {
				cis, err := env.PachClient.ListCommit(client.NewRepo(repo), nil, nil, 0)
				require.NoError(t, err)
				// There will be some empty commits on each branch from creation, ignore
				// those and just check that the latest commits match.
				require.True(t, len(ids) <= len(cis))
				for i, id := range ids {
					require.Equal(t, id, cis[i].Commit.ID)
				}
			}
		}

		checkRepoCommits([]*pfs.Commit{commit3, commit2, commit1})

		// Rewind branch b.master to commit2 - fails because this commitset already has an entry on 'master'
		require.YesError(t, env.PachClient.CreateBranch("b", "master", "master", commit2.ID, provB))
		// But we can create a new commit by aliasing the old commit and setting it as the head
		_, err = env.PachClient.PfsAPIClient.CreateBranch(env.PachClient.Ctx(), &pfs.CreateBranchRequest{
			Head:         client.NewCommit("b", "master", commit2.ID),
			Branch:       client.NewBranch("b", "master"),
			NewCommitSet: true,
			Provenance:   provB,
		})
		require.NoError(t, err)
		ci, err := env.PachClient.InspectCommit("b", "master", "")
		require.NoError(t, err)
		commit4 := ci.Commit

		checkRepoCommits([]*pfs.Commit{commit4, commit3, commit2, commit1})

		// The commit4 data in "a" should be the same as what we wrote into commit2 (as that's the source data in "b")
		commit4a := client.NewCommit("a", "master", commit4.ID)
		var b bytes.Buffer
		require.NoError(t, env.PachClient.GetFile(commit4a, "file", &b))
		require.Equal(t, "2", b.String())
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
			Repo: client.NewRepo("somerepo1"),
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

	// Make sure that artifacts of deleted repos do not resurface
	suite.Run("CreateDeletedRepo", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		repo := "repo"
		require.NoError(t, env.PachClient.CreateRepo(repo))
		repoProto := client.NewRepo(repo)

		systemRepo := client.NewSystemRepo(repo, pfs.MetaRepoType)
		_, err := env.PachClient.PfsAPIClient.CreateRepo(env.PachClient.Ctx(), &pfs.CreateRepoRequest{
			Repo: systemRepo,
		})
		require.NoError(t, err)

		commit, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(commit, "foo", strings.NewReader("foo")))
		require.NoError(t, env.PachClient.FinishCommit(repo, commit.Branch.Name, commit.ID))

		_, err = env.PachClient.PfsAPIClient.StartCommit(env.PachClient.Ctx(), &pfs.StartCommitRequest{
			Branch: systemRepo.NewBranch("master"),
		})
		require.NoError(t, err)

		commitInfos, err := env.PachClient.ListCommit(repoProto, nil, nil, 0)
		require.NoError(t, err)
		require.Equal(t, 1, len(commitInfos))

		commitInfos, err = env.PachClient.ListCommit(systemRepo, nil, nil, 0)
		require.NoError(t, err)
		require.Equal(t, 1, len(commitInfos))

		require.NoError(t, env.PachClient.DeleteRepo(repo, false))
		require.NoError(t, env.PachClient.CreateRepo(repo))

		commitInfos, err = env.PachClient.ListCommit(repoProto, nil, nil, 0)
		require.NoError(t, err)
		require.Equal(t, 0, len(commitInfos))

		branchInfos, err := env.PachClient.ListBranch(repo)
		require.NoError(t, err)
		require.Equal(t, 0, len(branchInfos))

		repoInfos, err := env.PachClient.ListRepoByType("")
		require.NoError(t, err)
		require.Equal(t, 1, len(repoInfos))

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
		require.NoError(t, env.PachClient.CreateBranch(repo, "master", "", "", []*pfs.Branch{client.NewBranch(prov1, "master"), client.NewBranch(prov2, "master")}))

		downstream1 := "downstream1"
		require.NoError(t, env.PachClient.CreateRepo(downstream1))
		require.NoError(t, env.PachClient.CreateBranch(downstream1, "master", "", "", []*pfs.Branch{client.NewBranch(repo, "master")}))

		downstream2 := "downstream2"
		require.NoError(t, env.PachClient.CreateRepo(downstream2))
		require.NoError(t, env.PachClient.CreateBranch(downstream2, "master", "", "", []*pfs.Branch{client.NewBranch(repo, "master")}))

		// Without the Update flag it should fail
		require.YesError(t, env.PachClient.CreateRepo(repo))

		_, err := env.PachClient.PfsAPIClient.CreateRepo(context.Background(), &pfs.CreateRepoRequest{
			Repo:   client.NewRepo(repo),
			Update: true,
		})
		require.NoError(t, err)

		require.NoError(t, env.PachClient.CreateBranch(repo, "master", "", "", []*pfs.Branch{client.NewBranch(prov2, "master"), client.NewBranch(prov3, "master")}))

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

	suite.Run("CreateBranchHeadOnOtherRepo", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		// create two repos, and create a branch on one that tries to point on another's existing branch
		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))
		_, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		masterCommit := client.NewCommit(repo, "master", "")

		otherRepo := "other"
		require.NoError(t, env.PachClient.CreateRepo(otherRepo))
		_, err = env.PachClient.StartCommit(otherRepo, "master")
		require.NoError(t, err)
		otherMasterCommit := client.NewCommit(otherRepo, "master", "")

		mfc, err := env.PachClient.NewModifyFileClient(masterCommit)
		require.NoError(t, err)
		require.NoError(t, mfc.PutFile("/foo", strings.NewReader("foo\n")))
		require.NoError(t, mfc.Close())

		mfc, err = env.PachClient.NewModifyFileClient(otherMasterCommit)
		require.NoError(t, err)
		require.NoError(t, mfc.PutFile("/bar", strings.NewReader("bar\n")))
		require.NoError(t, mfc.Close())

		// Create a branch on one repo that points to a branch on another repo
		_, err = env.PachClient.PfsAPIClient.CreateBranch(
			env.PachClient.Ctx(),
			&pfs.CreateBranchRequest{
				Branch: client.NewBranch(repo, "test"),
				Head:   client.NewCommit(otherRepo, "master", ""),
			},
		)
		require.YesError(t, err)
		require.True(t, strings.Contains(err.Error(), "branch and head commit must belong to the same repo"))
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
		require.NoError(t, env.PachClient.CreateBranch("B", "master", "", "", []*pfs.Branch{client.NewBranch("A", "master")}))

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
		require.NoError(t, env.PachClient.CreateBranch("B", "master", "", "", []*pfs.Branch{client.NewBranch("A", "master")}))

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
		require.Nil(t, commitInfo.Details) // no details for an unfinished commit
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
		require.Equal(t, len(fileContent), int(commitInfo.Details.SizeBytes))
		require.True(t, started.Before(tStarted))
		require.True(t, finished.After(tFinished))
	})

	suite.Run("InspectCommitWait", func(t *testing.T) {
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

		commitInfo, err := env.PachClient.WaitCommit(commit.Branch.Repo.Name, commit.Branch.Name, commit.ID)
		require.NoError(t, err)
		require.NotNil(t, commitInfo.Finished)

		require.NoError(t, eg.Wait())
	})

	suite.Run("SquashCommitSet", func(t *testing.T) {
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

		require.NoError(t, env.PachClient.SquashCommitSet(commit2.ID))

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

	suite.Run("SquashCommitSetOnlyCommitInBranch", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))
		repoProto := client.NewRepo(repo)

		commit, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(commit, "foo", strings.NewReader("foo\n")))

		commitInfos, err := env.PachClient.ListCommit(repoProto, repoProto.NewCommit("master", ""), nil, 0)
		require.NoError(t, err)
		require.Equal(t, 1, len(commitInfos))
		require.Equal(t, commit, commitInfos[0].Commit)

		require.NoError(t, env.PachClient.SquashCommitSet(commit.ID))

		// The branch has not been deleted, though its head has been replaced with an empty commit
		branchInfos, err := env.PachClient.ListBranch(repo)
		require.NoError(t, err)
		require.Equal(t, 1, len(branchInfos))
		commitInfos, err = env.PachClient.ListCommit(repoProto, repoProto.NewCommit("master", ""), nil, 0)
		require.NoError(t, err)
		require.Equal(t, 1, len(commitInfos))
		require.Equal(t, branchInfos[0].Head, commitInfos[0].Commit)

		commitInfo, err := env.PachClient.InspectCommit(repo, "master", "")
		require.NoError(t, err)
		require.Equal(t, uint64(0), commitInfo.Details.SizeBytes)

		// Check that repo size is back to 0
		repoInfo, err := env.PachClient.InspectRepo(repo)
		require.NoError(t, err)
		require.Equal(t, uint64(0), repoInfo.SizeBytes)
	})

	suite.Run("SquashCommitSetFinished", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))
		repoProto := client.NewRepo(repo)

		commit, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(commit, "foo", strings.NewReader("foo\n")))
		require.NoError(t, env.PachClient.FinishCommit(repo, commit.Branch.Name, commit.ID))

		commitInfos, err := env.PachClient.ListCommit(repoProto, repoProto.NewCommit("master", ""), nil, 0)
		require.NoError(t, err)
		require.Equal(t, 1, len(commitInfos))
		require.Equal(t, commit, commitInfos[0].Commit)
		// TODO(2.0 required)?: ListCommit doesn't get the actual size of the
		// commits (even if they're finished) - do an inspect commit instead.
		commitInfo, err := env.PachClient.InspectCommit(repo, commit.Branch.Name, commit.ID)
		require.NoError(t, err)
		require.Equal(t, uint64(4), commitInfo.Details.SizeBytes)

		require.NoError(t, env.PachClient.SquashCommitSet(commit.ID))

		// The branch has not been deleted, though it only has an empty commit
		branchInfos, err := env.PachClient.ListBranch(repo)
		require.NoError(t, err)
		require.Equal(t, 1, len(branchInfos))
		commitInfos, err = env.PachClient.ListCommit(repoProto, repoProto.NewCommit("master", ""), nil, 0)
		require.NoError(t, err)
		require.Equal(t, 1, len(commitInfos))
		require.Equal(t, branchInfos[0].Head, commitInfos[0].Commit)
		require.NotEqual(t, commit, commitInfos[0].Commit)

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
		require.NoError(t, env.PachClient.PutFile(commit1, "foo", strings.NewReader("foo\n"), client.WithAppendPutFile()))
		require.NoError(t, env.PachClient.FinishCommit(repo, commit1.Branch.Name, commit1.ID))

		var buffer bytes.Buffer
		require.NoError(t, env.PachClient.GetFile(commit1, "foo", &buffer))
		require.Equal(t, "foo\n", buffer.String())

		commit2, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(commit2, "foo", strings.NewReader("foo\n"), client.WithAppendPutFile()))
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

	suite.Run("ProvenanceWithinSingleRepoDisallowed", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		repo := "repo"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		// TODO: implement in terms of branch provenance
		// test: repo -> repo
		// test: a -> b -> a
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

		require.NoError(t, env.PachClient.CreateBranch("B", "master", "", "", []*pfs.Branch{client.NewBranch("A", "master")}))
		require.NoError(t, env.PachClient.CreateBranch("C", "master", "", "", []*pfs.Branch{client.NewBranch("B", "master"), client.NewBranch("E", "master")}))
		require.NoError(t, env.PachClient.CreateBranch("D", "master", "", "", []*pfs.Branch{client.NewBranch("C", "master")}))

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

		commitInfo, err := env.PachClient.InspectCommit("B", "master", "")
		require.NoError(t, err)
		require.Equal(t, ACommit.ID, commitInfo.Commit.ID)

		commitInfo, err = env.PachClient.InspectCommit("C", "master", "")
		require.NoError(t, err)
		require.Equal(t, ACommit.ID, commitInfo.Commit.ID)

		commitInfo, err = env.PachClient.InspectCommit("D", "master", "")
		require.NoError(t, err)
		require.Equal(t, ACommit.ID, commitInfo.Commit.ID)

		ECommit, err := env.PachClient.StartCommit("E", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("E", ECommit.Branch.Name, ECommit.ID))

		commitInfo, err = env.PachClient.InspectCommit("B", "master", "")
		require.NoError(t, err)
		require.Equal(t, ECommit.ID, commitInfo.Commit.ID)

		commitInfo, err = env.PachClient.InspectCommit("C", "master", "")
		require.NoError(t, err)
		require.Equal(t, ECommit.ID, commitInfo.Commit.ID)

		commitInfo, err = env.PachClient.InspectCommit("D", "master", "")
		require.NoError(t, err)
		require.Equal(t, ECommit.ID, commitInfo.Commit.ID)
	})

	suite.Run("CommitBranch", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		require.NoError(t, env.PachClient.CreateRepo("input"))
		require.NoError(t, env.PachClient.CreateRepo("output"))
		// Make two branches provenant on the master branch
		require.NoError(t, env.PachClient.CreateBranch("output", "A", "", "", []*pfs.Branch{client.NewBranch("input", "master")}))
		require.NoError(t, env.PachClient.CreateBranch("output", "B", "", "", []*pfs.Branch{client.NewBranch("input", "master")}))

		// Now make a commit on the master branch, which should trigger a downstream commit on each of the two branches
		masterCommit, err := env.PachClient.StartCommit("input", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("input", masterCommit.Branch.Name, masterCommit.ID))

		// Check that the commit in branch A has the information and provenance we expect
		commitInfo, err := env.PachClient.InspectCommit("output", "A", "")
		require.NoError(t, err)
		require.Equal(t, "A", commitInfo.Commit.Branch.Name)
		require.Equal(t, masterCommit.ID, commitInfo.Commit.ID)

		// Check that the commit in branch B has the information and provenance we expect
		commitInfo, err = env.PachClient.InspectCommit("output", "B", "")
		require.NoError(t, err)
		require.Equal(t, "B", commitInfo.Commit.Branch.Name)
		require.Equal(t, masterCommit.ID, commitInfo.Commit.ID)
	})

	suite.Run("CommitOnTwoBranchesProvenance", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		require.NoError(t, env.PachClient.CreateRepo("input"))
		require.NoError(t, env.PachClient.CreateRepo("output"))

		parentCommit, err := env.PachClient.StartCommit("input", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("input", parentCommit.Branch.Name, parentCommit.ID))

		masterCommit, err := env.PachClient.StartCommit("input", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("input", masterCommit.Branch.Name, masterCommit.ID))

		// Make two branches provenant on the same commit on the master branch
		require.NoError(t, env.PachClient.CreateBranch("input", "A", masterCommit.Branch.Name, masterCommit.ID, nil))
		require.NoError(t, env.PachClient.CreateBranch("input", "B", masterCommit.Branch.Name, masterCommit.ID, nil))

		// Now create a branch provenant on both branches A and B
		require.NoError(t, env.PachClient.CreateBranch("output", "C", "", "", []*pfs.Branch{client.NewBranch("input", "A"), client.NewBranch("input", "B")}))

		// The head commit of the C branch should have the same ID as the new head
		// of branches A and B
		ci, err := env.PachClient.InspectCommit("output", "C", "")
		require.NoError(t, err)
		aHead, err := env.PachClient.InspectCommit("input", "A", "")
		require.NoError(t, err)
		bHead, err := env.PachClient.InspectCommit("input", "B", "")
		require.NoError(t, err)
		require.Equal(t, aHead.Commit.ID, ci.Commit.ID)
		require.Equal(t, bHead.Commit.ID, ci.Commit.ID)

		// We should be able to squash the parent commits of A and B Head
		require.NoError(t, env.PachClient.SquashCommitSet(aHead.ParentCommit.ID))

		// Now, squashing the head of A and B and C should leave each of them with just an empty head commit
		require.NoError(t, env.PachClient.SquashCommitSet(aHead.Commit.ID))

		_, err = env.PachClient.InspectCommit("output", "C", "")
		require.NoError(t, err)
		_, err = env.PachClient.InspectCommit("input", "A", "")
		require.NoError(t, err)
		_, err = env.PachClient.InspectCommit("input", "B", "")
		require.NoError(t, err)

		// It should also be ok to make new commits on branches A and B
		aCommit, err := env.PachClient.StartCommit("input", "A")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("input", aCommit.Branch.Name, aCommit.ID))

		bCommit, err := env.PachClient.StartCommit("input", "B")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("input", bCommit.Branch.Name, bCommit.ID))
	})

	suite.Run("Branch1", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))
		masterCommit := client.NewCommit(repo, "master", "")
		commit, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(masterCommit, "foo", strings.NewReader("foo\n"), client.WithAppendPutFile()))
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
		require.NoError(t, env.PachClient.PutFile(masterCommit, "foo", strings.NewReader("foo\n"), client.WithAppendPutFile()))
		require.NoError(t, env.PachClient.FinishCommit(repo, "master", ""))
		buffer = bytes.Buffer{}
		require.NoError(t, env.PachClient.GetFile(masterCommit, "foo", &buffer))
		require.Equal(t, "foo\nfoo\n", buffer.String())
		branchInfos, err = env.PachClient.ListBranch(repo)
		require.NoError(t, err)
		require.Equal(t, 1, len(branchInfos))
		require.Equal(t, "master", branchInfos[0].Branch.Name)

		// Check that moving the commit to other branches uses the same CommitSet ID and extends the existing CommitSet
		commitInfos, err := env.PachClient.InspectCommitSet(commit.ID)
		require.NoError(t, err)
		require.Equal(t, 1, len(commitInfos))

		require.NoError(t, env.PachClient.CreateBranch(repo, "master2", commit.Branch.Name, commit.ID, nil))

		commitInfos, err = env.PachClient.InspectCommitSet(commit.ID)
		require.NoError(t, err)
		require.Equal(t, 2, len(commitInfos))

		require.NoError(t, env.PachClient.CreateBranch(repo, "master3", commit.Branch.Name, commit.ID, nil))

		commitInfos, err = env.PachClient.InspectCommitSet(commit.ID)
		require.NoError(t, err)
		require.Equal(t, 3, len(commitInfos))

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
		require.Equal(t, fileSize, int(fileInfo.Details.SizeBytes))

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
		require.NoError(t, env.PachClient.PutFile(commit1, "file", strings.NewReader("foo\n"), client.WithAppendPutFile()))
		require.NoError(t, env.PachClient.PutFile(commit1, "file", strings.NewReader("bar\n"), client.WithAppendPutFile()))
		require.NoError(t, env.PachClient.PutFile(masterCommit, "file", strings.NewReader("buzz\n"), client.WithAppendPutFile()))
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
		require.NoError(t, env.PachClient.PutFile(commit2, "file", strings.NewReader("foo\n"), client.WithAppendPutFile()))
		require.NoError(t, env.PachClient.PutFile(commit2, "file", strings.NewReader("bar\n"), client.WithAppendPutFile()))
		require.NoError(t, env.PachClient.PutFile(masterCommit, "file", strings.NewReader("buzz\n"), client.WithAppendPutFile()))
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
		require.NoError(t, env.PachClient.FinishCommit(repo, commit3.Branch.Name, commit3.ID))
		require.NoError(t, env.PachClient.CreateBranch(repo, "foo", "", commit3.ID, nil))

		commit4, err := env.PachClient.StartCommit(repo, "foo")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(commit4, "file", strings.NewReader("foo\nbar\nbuzz\n"), client.WithAppendPutFile()))
		require.NoError(t, env.PachClient.FinishCommit(repo, commit4.Branch.Name, commit4.ID))

		// commit 3 should have remained unchanged
		buffer.Reset()
		require.NoError(t, env.PachClient.GetFile(commit3, "file", buffer))
		require.Equal(t, expected, buffer.String())

		expected = "foo\nbar\nbuzz\nfoo\nbar\nbuzz\nfoo\nbar\nbuzz\n"
		buffer.Reset()
		require.NoError(t, env.PachClient.GetFile(commit4, "file", buffer))
		require.Equal(t, expected, buffer.String())
	})

	suite.Run("PutFileBranchCommitID", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		err := env.PachClient.PutFile(client.NewCommit(repo, "", "master"), "foo", strings.NewReader("foo\n"), client.WithAppendPutFile())
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
				return env.PachClient.PutFile(commit, "foo", strings.NewReader("foo\n"), client.WithAppendPutFile())
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
		require.NoError(t, env.PachClient.PutFile(commit1, "foo", strings.NewReader(fileContent1), client.WithAppendPutFile()))
		checks := func() {
			fileInfo, err := env.PachClient.InspectFile(commit1, "foo")
			require.NoError(t, err)
			require.Equal(t, pfs.FileType_FILE, fileInfo.FileType)
			require.Equal(t, len(fileContent1), int(fileInfo.Details.SizeBytes))
		}
		checks()
		require.NoError(t, env.PachClient.FinishCommit(repo, commit1.Branch.Name, commit1.ID))
		checks()

		fileContent2 := "barbar\n"
		commit2, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(commit2, "foo", strings.NewReader(fileContent2), client.WithAppendPutFile()))

		require.NoError(t, env.PachClient.FinishCommit(repo, commit2.Branch.Name, commit2.ID))

		fileInfo, err := env.PachClient.InspectFile(commit2, "foo")
		require.NoError(t, err)
		require.Equal(t, pfs.FileType_FILE, fileInfo.FileType)
		require.Equal(t, len(fileContent1+fileContent2), int(fileInfo.Details.SizeBytes))

		fileInfo, err = env.PachClient.InspectFile(commit2, "foo")
		require.NoError(t, err)
		require.Equal(t, pfs.FileType_FILE, fileInfo.FileType)
		require.Equal(t, len(fileContent1)+len(fileContent2), int(fileInfo.Details.SizeBytes))

		fileContent3 := "bar\n"
		commit3, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(commit3, "bar", strings.NewReader(fileContent3), client.WithAppendPutFile()))
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
		require.NoError(t, env.PachClient.PutFile(commit, "file", strings.NewReader(fileContent1), client.WithAppendPutFile()))
		require.NoError(t, env.PachClient.FinishCommit(repo, "master", ""))

		fileInfo, err := env.PachClient.InspectFile(commit, "/file")
		require.NoError(t, err)
		require.Equal(t, len(fileContent1), int(fileInfo.Details.SizeBytes))
		require.Equal(t, "/file", fileInfo.File.Path)
		require.Equal(t, pfs.FileType_FILE, fileInfo.FileType)

		_, err = env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(commit, "file", strings.NewReader(fileContent1), client.WithAppendPutFile()))
		require.NoError(t, env.PachClient.FinishCommit(repo, "master", ""))

		fileInfo, err = env.PachClient.InspectFile(commit, "file")
		require.NoError(t, err)
		require.Equal(t, len(fileContent1)*2, int(fileInfo.Details.SizeBytes))
		require.Equal(t, "/file", fileInfo.File.Path)

		_, err = env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.DeleteFile(commit, "file"))
		require.NoError(t, env.PachClient.PutFile(commit, "file", strings.NewReader(fileContent2), client.WithAppendPutFile()))
		require.NoError(t, env.PachClient.FinishCommit(repo, "master", ""))

		fileInfo, err = env.PachClient.InspectFile(commit, "file")
		require.NoError(t, err)
		require.Equal(t, len(fileContent2), int(fileInfo.Details.SizeBytes))
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
		require.Equal(t, len(fileContent), int(fileInfo.Details.SizeBytes))
		require.Equal(t, pfs.FileType_FILE, fileInfo.FileType)

		fileInfo, err = env.PachClient.InspectFile(commit1, "dir")
		require.NoError(t, err)
		require.Equal(t, len(fileContent), int(fileInfo.Details.SizeBytes))
		require.Equal(t, pfs.FileType_DIR, fileInfo.FileType)

		_, err = env.PachClient.InspectFile(commit1, "")
		require.NoError(t, err)
		require.Equal(t, len(fileContent), int(fileInfo.Details.SizeBytes))
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
			require.True(t, fileInfos[0].Details.SizeBytes == fileInfos[1].Details.SizeBytes && fileInfos[0].Details.SizeBytes == uint64(len(fileContent1)))

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
		require.Equal(t, int(fileInfos[2].Details.SizeBytes), len(fileContent)*2)

		_, err = env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		err = env.PachClient.DeleteFile(commit, "dir/3/bar")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit(repo, "master", ""))

		fileInfos, err = env.PachClient.ListFileAll(commit, "dir")
		require.NoError(t, err)
		require.Equal(t, 3, len(fileInfos))
		require.Equal(t, int(fileInfos[2].Details.SizeBytes), len(fileContent))

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
		require.NoError(t, env.PachClient.PutFile(commit, "foo", strings.NewReader("bar")))
		require.NoError(t, env.PachClient.FinishCommit(repo, commit.Branch.Name, commit.ID))

		expectedBranches := []string{"branch1", "branch2", "branch3"}
		expectedCommits := []*pfs.Commit{}
		for _, branch := range expectedBranches {
			require.NoError(t, env.PachClient.CreateBranch(repo, branch, commit.Branch.Name, commit.ID, nil))
			commitInfo, err := env.PachClient.InspectCommit(repo, branch, "")
			require.NoError(t, err)
			expectedCommits = append(expectedCommits, commitInfo.Commit)
		}

		branchInfos, err := env.PachClient.ListBranch(repo)
		require.NoError(t, err)
		require.Equal(t, len(expectedBranches), len(branchInfos))
		for i, branchInfo := range branchInfos {
			// branches should return in newest-first order
			require.Equal(t, expectedBranches[len(branchInfos)-i-1], branchInfo.Branch.Name)

			// each branch should have a different commit id (from the transaction
			// that moved the branch head)
			headCommit := expectedCommits[len(branchInfos)-i-1]
			require.Equal(t, headCommit.Branch, branchInfo.Branch)
			require.Equal(t, headCommit, branchInfo.Head)

			// ensure that the branch has the file from the original commit
			var buffer bytes.Buffer
			require.NoError(t, env.PachClient.GetFile(headCommit, "foo", &buffer))
			require.Equal(t, "bar", buffer.String())
		}

		commit2, err := env.PachClient.StartCommit(repo, "branch1")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit(repo, "branch1", ""))

		commit2Info, err := env.PachClient.InspectCommit(repo, "branch1", "")
		require.NoError(t, err)
		require.Equal(t, expectedCommits[0], commit2Info.ParentCommit)

		// delete the last branch
		lastBranch := expectedBranches[len(expectedBranches)-1]
		require.NoError(t, env.PachClient.DeleteBranch(repo, lastBranch, false))
		branchInfos, err = env.PachClient.ListBranch(repo)
		require.NoError(t, err)
		require.Equal(t, 2, len(branchInfos))
		require.Equal(t, "branch2", branchInfos[0].Branch.Name)
		require.Equal(t, expectedCommits[1], branchInfos[0].Head)
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
				err := env.PachClient.SubscribeCommit(client.NewRepo(repo), "master", "", pfs.CommitState_STARTED, func(ci *pfs.CommitInfo) error {
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
			require.YesError(t, err)
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
					if err := env.PachClient.PutFile(commit1, "foo", strings.NewReader(rawMessage), client.WithAppendPutFile()); err != nil {
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
		require.NoError(t, env.PachClient.FinishCommit(repo, commit1.Branch.Name, commit1.ID))
		require.NoError(t, env.PachClient.CreateBranch(repo, "master", "", commit1.ID, nil))

		commit2, err := env.PachClient.StartCommit(repo, "foo")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit(repo, commit2.Branch.Name, commit2.ID))
		require.NoError(t, env.PachClient.CreateBranch(repo, "master", "", commit2.ID, nil))

		branchInfos, err := env.PachClient.ListBranch(repo)
		require.NoError(t, err)

		// branches should be returned newest-first
		require.Equal(t, 2, len(branchInfos))
		require.Equal(t, "master", branchInfos[0].Branch.Name)
		require.Equal(t, commit2.ID, branchInfos[0].Head.ID) // aliased branch should have the same commit ID
		require.Equal(t, "foo", branchInfos[1].Branch.Name)
		require.Equal(t, commit2.ID, branchInfos[1].Head.ID) // original branch should remain unchanged
	})

	suite.Run("WaitCommitSet", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		require.NoError(t, env.PachClient.CreateRepo("A"))
		require.NoError(t, env.PachClient.CreateRepo("B"))
		require.NoError(t, env.PachClient.CreateBranch("B", "master", "", "", []*pfs.Branch{client.NewBranch("A", "master")}))
		require.NoError(t, env.PachClient.FinishCommit("B", "master", ""))

		ACommit, err := env.PachClient.StartCommit("A", "master")
		require.NoError(t, err)
		BCommit := client.NewCommit("B", "master", ACommit.ID)
		require.NoError(t, env.PachClient.FinishCommit("A", "master", ""))
		require.NoError(t, env.PachClient.FinishCommit("B", "master", ""))

		commitInfos, err := env.PachClient.WaitCommitSetAll(ACommit.ID)
		require.NoError(t, err)
		require.Equal(t, 2, len(commitInfos))
		require.Equal(t, ACommit, commitInfos[0].Commit)
		require.Equal(t, BCommit, commitInfos[1].Commit)
	})

	// WaitCommitSet2 implements the following DAG:
	// A ─▶ B ─▶ C ─▶ D
	suite.Run("WaitCommitSet2", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		require.NoError(t, env.PachClient.CreateRepo("A"))
		require.NoError(t, env.PachClient.CreateRepo("B"))
		require.NoError(t, env.PachClient.CreateRepo("C"))
		require.NoError(t, env.PachClient.CreateRepo("D"))

		// Create branches and finish the default head commits on the downstream branches
		require.NoError(t, env.PachClient.CreateBranch("B", "master", "", "", []*pfs.Branch{client.NewBranch("A", "master")}))
		require.NoError(t, env.PachClient.FinishCommit("B", "master", ""))
		require.NoError(t, env.PachClient.CreateBranch("C", "master", "", "", []*pfs.Branch{client.NewBranch("B", "master")}))
		require.NoError(t, env.PachClient.FinishCommit("C", "master", ""))
		require.NoError(t, env.PachClient.CreateBranch("D", "master", "", "", []*pfs.Branch{client.NewBranch("C", "master")}))
		require.NoError(t, env.PachClient.FinishCommit("D", "master", ""))

		ACommit, err := env.PachClient.StartCommit("A", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("A", "master", ""))

		// do the other commits in a goro so we can block for them
		go func() {
			require.NoError(t, env.PachClient.FinishCommit("B", "master", ""))
			require.NoError(t, env.PachClient.FinishCommit("C", "master", ""))
			require.NoError(t, env.PachClient.FinishCommit("D", "master", ""))
		}()

		// Wait for the commits to finish
		commitInfos, err := env.PachClient.WaitCommitSetAll(ACommit.ID)
		require.NoError(t, err)
		BCommit := client.NewCommit("B", "master", ACommit.ID)
		CCommit := client.NewCommit("C", "master", ACommit.ID)
		DCommit := client.NewCommit("D", "master", ACommit.ID)
		require.Equal(t, 4, len(commitInfos))
		require.Equal(t, ACommit, commitInfos[0].Commit)
		require.Equal(t, BCommit, commitInfos[1].Commit)
		require.Equal(t, CCommit, commitInfos[2].Commit)
		require.Equal(t, DCommit, commitInfos[3].Commit)
	})

	// A
	//  ╲
	//   ◀
	//    C
	//   ◀
	//  ╱
	// B
	suite.Run("WaitCommitSet3", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		require.NoError(t, env.PachClient.CreateRepo("A"))
		require.NoError(t, env.PachClient.CreateRepo("B"))
		require.NoError(t, env.PachClient.CreateRepo("C"))

		require.NoError(t, env.PachClient.CreateBranch("C", "master", "", "", []*pfs.Branch{client.NewBranch("A", "master"), client.NewBranch("B", "master")}))
		require.NoError(t, env.PachClient.FinishCommit("C", "master", ""))

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

		// The first two commits will be A and B, but they aren't deterministically sorted
		commitInfos, err := env.PachClient.WaitCommitSetAll(ACommit.ID)
		require.NoError(t, err)
		require.Equal(t, 3, len(commitInfos))
		expected := []*pfs.Commit{ACommit, client.NewCommit("B", "master", ACommit.ID)}
		actual := []*pfs.Commit{commitInfos[0].Commit, commitInfos[1].Commit}
		require.ImagesEqual(t, expected, actual, CommitToID)
		require.Equal(t, client.NewCommit("C", "master", ACommit.ID), commitInfos[2].Commit)

		commitInfos, err = env.PachClient.WaitCommitSetAll(BCommit.ID)
		require.NoError(t, err)
		require.Equal(t, 3, len(commitInfos))
		expected = []*pfs.Commit{client.NewCommit("A", "master", BCommit.ID), BCommit}
		actual = []*pfs.Commit{commitInfos[0].Commit, commitInfos[1].Commit}
		require.ImagesEqual(t, expected, actual, CommitToID)
		require.Equal(t, client.NewCommit("C", "master", BCommit.ID), commitInfos[2].Commit)
	})

	suite.Run("WaitCommitSetWithNoDownstreamRepos", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))
		commit, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit(repo, commit.Branch.Name, commit.ID))
		commitInfos, err := env.PachClient.WaitCommitSetAll(commit.ID)
		require.NoError(t, err)
		require.Equal(t, 1, len(commitInfos))
		require.Equal(t, commit, commitInfos[0].Commit)
	})

	suite.Run("WaitOpenCommit", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		require.NoError(t, env.PachClient.CreateRepo("A"))
		require.NoError(t, env.PachClient.CreateRepo("B"))
		require.NoError(t, env.PachClient.CreateBranch("B", "master", "", "", []*pfs.Branch{client.NewBranch("A", "master")}))
		require.NoError(t, env.PachClient.FinishCommit("B", "master", ""))
		commit, err := env.PachClient.StartCommit("A", "master")
		require.NoError(t, err)

		// do the other commits in a goro so we can block for them
		eg, _ := errgroup.WithContext(context.Background())
		eg.Go(func() error {
			time.Sleep(3 * time.Second)
			if err := env.PachClient.FinishCommit("A", "master", ""); err != nil {
				return err
			}
			return env.PachClient.FinishCommit("B", "master", "")
		})

		t.Cleanup(func() {
			require.NoError(t, eg.Wait())
		})

		// Wait for the commit to finish
		commitInfos, err := env.PachClient.WaitCommitSetAll(commit.ID)
		require.NoError(t, err)
		require.Equal(t, 2, len(commitInfos))
		require.Equal(t, commit, commitInfos[0].Commit)
		require.Equal(t, client.NewCommit("B", "master", commit.ID), commitInfos[1].Commit)
	})

	suite.Run("WaitUninvolvedBranch", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		require.NoError(t, env.PachClient.CreateRepo("A"))
		require.NoError(t, env.PachClient.CreateRepo("B"))
		require.NoError(t, env.PachClient.CreateBranch("B", "master", "", "", nil))
		commit, err := env.PachClient.StartCommit("A", "master")
		require.NoError(t, err)

		// Blocking on a commit that doesn't exist does not work
		_, err = env.PachClient.WaitCommit("B", "master", commit.ID)
		require.YesError(t, err)
	})

	suite.Run("WaitNonExistentBranch", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		require.NoError(t, env.PachClient.CreateRepo("A"))
		commit, err := env.PachClient.StartCommit("A", "master")
		require.NoError(t, err)

		// Blocking on a branch that doesn't exist does not work
		_, err = env.PachClient.WaitCommit("A", "foo", commit.ID)
		require.YesError(t, err)

		_, err = env.PachClient.WaitCommit("A", "foo", "")
		require.YesError(t, err)
	})

	suite.Run("EmptyWait", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		_, err := env.PachClient.WaitCommitSetAll("")
		require.YesError(t, err)
	})

	suite.Run("WaitNonExistentCommitSet", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		_, err := env.PachClient.WaitCommitSetAll("fake-commitset")
		require.YesError(t, err)
		require.True(t, pfsserver.IsCommitSetNotFoundErr(err))
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
		require.NoError(t, env.PachClient.PutFile(c1, "foo", strings.NewReader("foo\n"), client.WithAppendPutFile()))
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
		require.NoError(t, env.PachClient.PutFile(c2, "foo", strings.NewReader("not foo\n"), client.WithAppendPutFile()))
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
		require.NoError(t, env.PachClient.PutFile(c3, "/bar", strings.NewReader("bar\n"), client.WithAppendPutFile()))
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
		require.NoError(t, env.PachClient.PutFile(c5, "/dir/fizz", strings.NewReader("fizz\n"), client.WithAppendPutFile()))
		require.NoError(t, env.PachClient.PutFile(c5, "/dir/buzz", strings.NewReader("buzz\n"), client.WithAppendPutFile()))
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
		require.NoError(t, env.PachClient.PutFile(c6, "/dir/fizz", strings.NewReader("fizz\n"), client.WithAppendPutFile()))
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

		require.NoError(t, env.PachClient.WithModifyFileClient(commit, func(mf client.ModifyFile) error {
			for i := 0; i < numFiles; i++ {
				require.NoError(t, mf.PutFile(fmt.Sprintf("file%d", i), strings.NewReader("1")))
				require.NoError(t, mf.PutFile(fmt.Sprintf("dir1/file%d", i), strings.NewReader("2")))
				require.NoError(t, mf.PutFile(fmt.Sprintf("dir2/dir3/file%d", i), strings.NewReader("3")))
			}
			return nil
		}))

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
			rc, err := env.PachClient.GetFileTAR(commit, "*")
			require.NoError(t, err)
			defer rc.Close()
			require.NoError(t, tarutil.ConcatFileContent(&output, rc))

			require.Equal(t, numFiles*3, len(output.String()))

			output = strings.Builder{}
			rc, err = env.PachClient.GetFileTAR(commit, "dir2/dir3/file1?")
			require.NoError(t, err)
			defer rc.Close()
			require.NoError(t, tarutil.ConcatFileContent(&output, rc))
			require.Equal(t, 10, len(output.String()))

			output = strings.Builder{}
			rc, err = env.PachClient.GetFileTAR(commit, "**file1?")
			require.NoError(t, err)
			defer rc.Close()
			require.NoError(t, tarutil.ConcatFileContent(&output, rc))
			require.Equal(t, 30, len(output.String()))

			output = strings.Builder{}
			rc, err = env.PachClient.GetFileTAR(commit, "**file1")
			require.NoError(t, err)
			defer rc.Close()
			require.NoError(t, tarutil.ConcatFileContent(&output, rc))
			require.True(t, strings.Contains(output.String(), "1"))
			require.True(t, strings.Contains(output.String(), "2"))
			require.True(t, strings.Contains(output.String(), "3"))

			output = strings.Builder{}
			rc, err = env.PachClient.GetFileTAR(commit, "**file1")
			require.NoError(t, err)
			defer rc.Close()
			require.NoError(t, tarutil.ConcatFileContent(&output, rc))
			match, err := regexp.Match("[123]", []byte(output.String()))
			require.NoError(t, err)
			require.True(t, match)

			output = strings.Builder{}
			rc, err = env.PachClient.GetFileTAR(commit, "dir?")
			require.NoError(t, err)
			defer rc.Close()
			require.NoError(t, tarutil.ConcatFileContent(&output, rc))

			output = strings.Builder{}
			rc, err = env.PachClient.GetFileTAR(commit, "")
			require.NoError(t, err)
			defer rc.Close()
			require.NoError(t, tarutil.ConcatFileContent(&output, rc))

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
	suite.Run("GetFileTARGlobOrder", func(t *testing.T) {
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
		rc, err := env.PachClient.GetFileTAR(commit, "/data/*")
		require.NoError(t, err)
		defer rc.Close()
		require.NoError(t, tarutil.ConcatFileContent(&output, rc))

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
		require.NoError(t, env.PachClient.CopyFile(otherCommit, "files", masterCommit, "files", client.WithAppendCopyFile()))
		require.NoError(t, env.PachClient.CopyFile(otherCommit, "file0", masterCommit, "files/0", client.WithAppendCopyFile()))
		require.NoError(t, env.PachClient.FinishCommit(repo, otherCommit.Branch.Name, otherCommit.ID))

		for i := 0; i < numFiles; i++ {
			_, err = env.PachClient.InspectFile(otherCommit, fmt.Sprintf("files/%d", i))
			require.NoError(t, err)
		}
		_, err = env.PachClient.InspectFile(otherCommit, "files/0")
		require.NoError(t, err)
	})

	suite.Run("PropagateBranch", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		repo1 := "test1"
		require.NoError(t, env.PachClient.CreateRepo(repo1))
		repo2 := "test2"
		require.NoError(t, env.PachClient.CreateRepo(repo2))
		require.NoError(t, env.PachClient.CreateBranch(repo2, "master", "", "", []*pfs.Branch{client.NewBranch(repo1, "master")}))
		commit, err := env.PachClient.StartCommit(repo1, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit(repo1, commit.Branch.Name, commit.ID))
		commits, err := env.PachClient.ListCommitByRepo(client.NewRepo(repo2))
		require.NoError(t, err)
		require.Equal(t, 2, len(commits))
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
		require.NoError(t, env.PachClient.CreateBranch("C", "master", "", "", []*pfs.Branch{client.NewBranch("A", "master"), client.NewBranch("B", "master")}))
		require.NoError(t, env.PachClient.FinishCommit("C", "master", ""))
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
		require.Equal(t, 3, len(commits))

		// Create a branch in D, it should receive a single commit for the heads of `A` and `B`.
		require.NoError(t, env.PachClient.CreateBranch("D", "master", "", "", []*pfs.Branch{client.NewBranch("A", "master"), client.NewBranch("B", "master")}))
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
		require.NoError(t, env.PachClient.CreateBranch("B", "master", "", "", []*pfs.Branch{client.NewBranch("A", "master")}))
		require.NoError(t, env.PachClient.FinishCommit("B", "master", ""))
		require.NoError(t, env.PachClient.CreateBranch("C", "master", "", "", []*pfs.Branch{client.NewBranch("B", "master")}))
		require.NoError(t, env.PachClient.FinishCommit("C", "master", ""))

		_, err := env.PachClient.StartCommit("A", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("A", "master", ""))
		require.NoError(t, env.PachClient.FinishCommit("B", "master", ""))
		require.NoError(t, env.PachClient.FinishCommit("C", "master", ""))

		_, err = env.PachClient.StartCommit("D", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("D", "master", ""))

		require.NoError(t, env.PachClient.CreateBranch("B", "master", "", "", []*pfs.Branch{client.NewBranch("A", "master"), client.NewBranch("D", "master")}))
		require.NoError(t, env.PachClient.FinishCommit("B", "master", ""))
		require.NoError(t, env.PachClient.FinishCommit("C", "master", ""))
		cCommitInfo, err := env.PachClient.InspectCommit("C", "master", "")
		require.NoError(t, err)

		commitInfos, err := env.PachClient.InspectCommitSet(cCommitInfo.Commit.ID)
		require.NoError(t, err)
		require.Equal(t, 4, len(commitInfos))
	})

	suite.Run("BranchProvenance", func(t *testing.T) {
		t.Parallel()

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
				t.Parallel()
				env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

				for _, repo := range []string{"A", "B", "C", "D", "E"} {
					require.NoError(t, env.PachClient.CreateRepo(repo))
				}
				for iStep, step := range test {
					var provenance []*pfs.Branch
					for _, repo := range step.directProv {
						provenance = append(provenance, client.NewBranch(repo, "master"))
					}
					err := env.PachClient.CreateBranch(step.name, "master", "", "", provenance)
					if step.err {
						require.YesError(t, err, "%d> CreateBranch(\"%s\", %v)", iStep, step.name, step.directProv)
					} else {
						require.NoError(t, err, "%d> CreateBranch(\"%s\", %v)", iStep, step.name, step.directProv)
					}
					require.NoError(t, env.PachClient.FsckFastExit())
					for repo, expectedProv := range step.expectProv {
						bi, err := env.PachClient.InspectBranch(repo, "master")
						require.NoError(t, err)
						sort.Strings(expectedProv)
						require.Equal(t, len(expectedProv), len(bi.Provenance))
						for _, b := range bi.Provenance {
							i := sort.SearchStrings(expectedProv, b.Name)
							if i >= len(expectedProv) || expectedProv[i] != b.Name {
								t.Fatalf("provenance for %s contains: %s, but should only contain: %v", repo, b, expectedProv)
							}
						}
					}
					for repo, expectedSubv := range step.expectSubv {
						bi, err := env.PachClient.InspectBranch(repo, "master")
						require.NoError(t, err)
						sort.Strings(expectedSubv)
						require.Equal(t, len(expectedSubv), len(bi.Subvenance))
						for _, b := range bi.Subvenance {
							i := sort.SearchStrings(expectedSubv, b.Name)
							if i >= len(expectedSubv) || expectedSubv[i] != b.Name {
								t.Fatalf("subvenance for %s contains: %s, but should only contain: %v", repo, b, expectedSubv)
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

		// 	require.NoError(t, env.PachClient.CreateBranch("B", "master", "", []*pfs.Branch{client.NewBranch("A", "master")}))
		// 	require.NoError(t, env.PachClient.CreateBranch("C", "master", "", []*pfs.Branch{client.NewBranch("B", "master")}))
		// 	require.NoError(t, env.PachClient.CreateBranch("D", "master", "", []*pfs.Branch{client.NewBranch("C", "master"), client.NewBranch("A", "master")}))

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

		// 	require.NoError(t, env.PachClient.CreateBranch("B", "master", "", []*pfs.Branch{client.NewBranch("A", "master")}))
		// 	require.NoError(t, env.PachClient.CreateBranch("C", "master", "", []*pfs.Branch{client.NewBranch("B", "master"), client.NewBranch("A", "master")}))
		// 	require.NoError(t, env.PachClient.CreateBranch("D", "master", "", []*pfs.Branch{client.NewBranch("C", "master")}))
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
		require.ImagesEqual(t, []*pfs.Commit{commit2}, commit1Info.ChildCommits, CommitToID)

		// Delete commit 2 and make sure it's removed from commit1.ChildCommits
		require.NoError(t, env.PachClient.SquashCommitSet(commit2.ID))
		commit1Info = inspect("A", commit1.Branch.Name, commit1.ID)
		require.ElementsEqualUnderFn(t, nil, commit1Info.ChildCommits, CommitToID)

		// Re-create commit2, and create a third commit also extending from commit1.
		// Make sure both appear in commit1.children
		commit2, err = env.PachClient.StartCommit("A", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("A", commit2.Branch.Name, commit2.ID))
		commit3, err := env.PachClient.PfsAPIClient.StartCommit(env.PachClient.Ctx(), &pfs.StartCommitRequest{
			Branch: client.NewBranch("A", "foo"),
			Parent: commit1,
		})
		require.NoError(t, err)
		commit1Info = inspect("A", commit1.Branch.Name, commit1.ID)
		require.ImagesEqual(t, []*pfs.Commit{commit2, commit3}, commit1Info.ChildCommits, CommitToID)

		// Delete commit3 and make sure commit1 has the right children
		require.NoError(t, env.PachClient.SquashCommitSet(commit3.ID))
		commit1Info = inspect("A", commit1.Branch.Name, commit1.ID)
		require.ImagesEqual(t, []*pfs.Commit{commit2}, commit1Info.ChildCommits, CommitToID)

		// Create a downstream branch in a different repo, then commit to "A" and
		// make sure the new HEAD commit is in the parent's children (i.e. test
		// propagateBranches)
		require.NoError(t, env.PachClient.CreateRepo("B"))
		require.NoError(t, env.PachClient.CreateBranch("B", "master", "", "", []*pfs.Branch{
			client.NewBranch("A", "master"),
		}))
		bCommit1 := inspect("B", "master", "")
		commit3, err = env.PachClient.StartCommit("A", "master")
		require.NoError(t, err)
		env.PachClient.FinishCommit("A", commit3.Branch.Name, commit3.ID)
		// Re-inspect bCommit1, which has been updated by StartCommit
		bCommit1, bCommit2 := inspect("B", bCommit1.Commit.Branch.Name, bCommit1.Commit.ID), inspect("B", "master", "")
		require.Equal(t, bCommit1.Commit.ID, bCommit2.ParentCommit.ID)
		require.ImagesEqual(t, []*pfs.Commit{bCommit2.Commit}, bCommit1.ChildCommits, CommitToID)

		// create a new branch in a different repo, then update it so that two commits
		// are generated. Make sure the second commit is in the parent's children
		require.NoError(t, env.PachClient.CreateRepo("C"))
		require.NoError(t, env.PachClient.CreateBranch("C", "master", "", "", []*pfs.Branch{
			client.NewBranch("A", "master"),
		}))
		cCommit1 := inspect("C", "master", "") // Get new commit's ID
		require.NoError(t, env.PachClient.CreateBranch("C", "master", "", "", []*pfs.Branch{
			client.NewBranch("A", "master"),
			client.NewBranch("B", "master"),
		}))
		// Re-inspect cCommit1, which has been updated by CreateBranch
		cCommit1, cCommit2 := inspect("C", cCommit1.Commit.Branch.Name, cCommit1.Commit.ID), inspect("C", "master", "")
		require.Equal(t, cCommit1.Commit.ID, cCommit2.ParentCommit.ID)
		require.ImagesEqual(t, []*pfs.Commit{cCommit2.Commit}, cCommit1.ChildCommits, CommitToID)
	})

	suite.Run("StartCommitFork", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		require.NoError(t, env.PachClient.CreateRepo("A"))
		commit, err := env.PachClient.StartCommit("A", "master")
		require.NoError(t, err)
		env.PachClient.FinishCommit("A", commit.Branch.Name, commit.ID)
		commit2, err := env.PachClient.PfsAPIClient.StartCommit(env.PachClient.Ctx(), &pfs.StartCommitRequest{
			Branch: client.NewBranch("A", "master2"),
			Parent: client.NewCommit("A", "master", ""),
		})
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("A", commit2.Branch.Name, commit2.ID))

		aRepo := client.NewRepo("A")
		commitInfos, err := env.PachClient.ListCommit(aRepo, nil, nil, 0)
		require.NoError(t, err)
		commits := []*pfs.Commit{}
		for _, ci := range commitInfos {
			commits = append(commits, ci.Commit)
		}
		require.ImagesEqual(t, []*pfs.Commit{commit, commit2}, commits, CommitToID)
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
			[]*pfs.Branch{client.NewBranch("A", "master")}))

		// Create commits in A and B
		commit, err := env.PachClient.StartCommit("A", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("A", commit.Branch.Name, commit.ID))
		commit, err = env.PachClient.StartCommit("B", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("B", commit.Branch.Name, commit.ID))

		// Check for first output commit in C (plus the old empty head commit)
		cRepo := client.NewRepo("C")
		commits, err := env.PachClient.ListCommit(cRepo, cRepo.NewCommit("master", ""), nil, 0)
		require.NoError(t, err)
		require.Equal(t, 2, len(commits))

		// Update the provenance of C/master and make sure it creates a new commit
		require.NoError(t, env.PachClient.CreateBranch("C", "master", "", "",
			[]*pfs.Branch{client.NewBranch("B", "master")}))
		commits, err = env.PachClient.ListCommit(cRepo, cRepo.NewCommit("master", ""), nil, 0)
		require.NoError(t, err)
		require.Equal(t, 3, len(commits))
	})

	// SquashCommitSetMultipleChildrenSingleCommit tests that when you have the
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
	suite.Run("SquashCommitSetMultipleChildrenSingleCommit", func(t *testing.T) {
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

		// Create 'd' by aliasing 'b' into another branch (force a new CommitSet rather than extending 'b')
		_, err = env.PachClient.PfsAPIClient.CreateBranch(env.PachClient.Ctx(), &pfs.CreateBranchRequest{
			Branch:       client.NewBranch("repo", "master2"),
			Head:         client.NewCommit("repo", "master", ""),
			NewCommitSet: true,
		})
		require.NoError(t, err)

		// Create 'c'
		c, err := env.PachClient.StartCommit("repo", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("repo", c.Branch.Name, c.ID))

		// Collect info re: a, b, c, and d, and make sure that the parent/child
		// relationships are all correct
		aInfo, err := env.PachClient.InspectCommit("repo", a.Branch.Name, a.ID)
		require.NoError(t, err)
		bInfo, err := env.PachClient.InspectCommit("repo", b.Branch.Name, b.ID)
		require.NoError(t, err)
		cInfo, err := env.PachClient.InspectCommit("repo", c.Branch.Name, c.ID)
		require.NoError(t, err)
		dInfo, err := env.PachClient.InspectCommit("repo", "master2", "")
		require.NoError(t, err)
		d := dInfo.Commit

		require.NotNil(t, aInfo.ParentCommit) // this should be the empty default head
		require.ImagesEqual(t, []*pfs.Commit{b}, aInfo.ChildCommits, CommitToID)

		require.Equal(t, a.ID, bInfo.ParentCommit.ID)
		require.ImagesEqual(t, []*pfs.Commit{c, d}, bInfo.ChildCommits, CommitToID)

		require.Equal(t, b.ID, cInfo.ParentCommit.ID)
		require.Equal(t, 0, len(cInfo.ChildCommits))

		require.Equal(t, b.ID, dInfo.ParentCommit.ID)
		require.Equal(t, 0, len(dInfo.ChildCommits))

		// Delete commit 'b'
		env.PachClient.SquashCommitSet(b.ID)

		// Collect info re: a, c, and d, and make sure that the parent/child
		// relationships are still correct
		aInfo, err = env.PachClient.InspectCommit("repo", a.Branch.Name, a.ID)
		require.NoError(t, err)
		cInfo, err = env.PachClient.InspectCommit("repo", c.Branch.Name, c.ID)
		require.NoError(t, err)
		dInfo, err = env.PachClient.InspectCommit("repo", d.Branch.Name, d.ID)
		require.NoError(t, err)

		require.NotNil(t, aInfo.ParentCommit)
		require.ImagesEqual(t, []*pfs.Commit{c, d}, aInfo.ChildCommits, CommitToID)

		require.Equal(t, a.ID, cInfo.ParentCommit.ID)
		require.Equal(t, 0, len(cInfo.ChildCommits))

		require.Equal(t, a.ID, dInfo.ParentCommit.ID)
		require.Equal(t, 0, len(dInfo.ChildCommits))
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
	//     f
	//     ↓
	// d e c
	//  ↘↓↙
	//   a
	// This makes sure that multiple live children are re-pointed at a live parent
	// if appropriate
	suite.Run("SquashCommitSetMultiLevelChildren", func(t *testing.T) {
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

		// Create main repo (will have the commit graphs above)
		require.NoError(t, env.PachClient.CreateRepo("repo"))
		require.NoError(t, env.PachClient.CreateBranch("repo", "master", "", "", []*pfs.Branch{
			client.NewBranch("upstream1", "master"),
			client.NewBranch("upstream2", "master"),
		}))
		repoProto := client.NewRepo("repo")

		// Create commit 'a'
		aInfo, err := env.PachClient.InspectCommit("repo", "master", "")
		require.NoError(t, err)
		a := aInfo.Commit
		require.NoError(t, env.PachClient.FinishCommit("repo", a.Branch.Name, a.ID))

		// Create 'd'
		resp, err := env.PachClient.PfsAPIClient.StartCommit(env.PachClient.Ctx(), &pfs.StartCommitRequest{
			Branch: client.NewBranch("repo", "fod"),
			Parent: a,
		})
		require.NoError(t, err)
		d := client.NewCommit("repo", resp.Branch.Name, resp.ID)
		require.NoError(t, env.PachClient.FinishCommit("repo", resp.Branch.Name, resp.ID))

		// Create 'b'
		// (a & b have same prov commit in upstream2, so this is the commit that will
		// be deleted, as both b and c are provenant on it)
		squashMeCommit, err := env.PachClient.StartCommit("upstream1", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("upstream1", "master", ""))
		bInfo, err := env.PachClient.InspectCommit("repo", "master", "")
		require.NoError(t, err)
		b := bInfo.Commit
		require.NoError(t, env.PachClient.FinishCommit("repo", b.Branch.Name, b.ID))
		require.Equal(t, b.ID, squashMeCommit.ID)

		// Create 'e'
		resp, err = env.PachClient.PfsAPIClient.StartCommit(env.PachClient.Ctx(), &pfs.StartCommitRequest{
			Branch: client.NewBranch("repo", "foe"),
			Parent: b,
		})
		require.NoError(t, err)
		e := client.NewCommit("repo", resp.Branch.Name, resp.ID)
		require.NoError(t, env.PachClient.FinishCommit("repo", resp.Branch.Name, resp.ID))

		// Create 'c'
		_, err = env.PachClient.StartCommit("upstream2", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("upstream2", "master", ""))
		cInfo, err := env.PachClient.InspectCommit("repo", "master", "")
		require.NoError(t, err)
		c := cInfo.Commit
		require.NoError(t, env.PachClient.FinishCommit("repo", c.Branch.Name, c.ID))

		// Create 'f'
		resp, err = env.PachClient.PfsAPIClient.StartCommit(env.PachClient.Ctx(), &pfs.StartCommitRequest{
			Branch: client.NewBranch("repo", "fof"),
			Parent: c,
		})
		require.NoError(t, err)
		f := client.NewCommit("repo", resp.Branch.Name, resp.ID)
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
		require.ImagesEqual(t, []*pfs.Commit{b, d}, aInfo.ChildCommits, CommitToID)
		require.ImagesEqual(t, []*pfs.Commit{c, e}, bInfo.ChildCommits, CommitToID)
		require.ImagesEqual(t, []*pfs.Commit{f}, cInfo.ChildCommits, CommitToID)
		require.Nil(t, dInfo.ChildCommits)
		require.Nil(t, eInfo.ChildCommits)
		require.Nil(t, fInfo.ChildCommits)

		// Delete second commit in upstream2, which deletes b
		require.NoError(t, env.PachClient.SquashCommitSet(squashMeCommit.ID))

		// Re-read commit info to get new parents/children
		aInfo, err = env.PachClient.InspectCommit("repo", a.Branch.Name, a.ID)
		require.NoError(t, err)
		cInfo, err = env.PachClient.InspectCommit("repo", c.Branch.Name, c.ID)
		require.NoError(t, err)
		dInfo, err = env.PachClient.InspectCommit("repo", d.Branch.Name, d.ID)
		require.NoError(t, err)
		eInfo, err = env.PachClient.InspectCommit("repo", e.Branch.Name, e.ID)
		require.NoError(t, err)
		fInfo, err = env.PachClient.InspectCommit("repo", f.Branch.Name, f.ID)
		require.NoError(t, err)

		// The head of master should be 'c'

		// Make sure child/parent relationships are as shown in second diagram. Note
		// that after 'b' is deleted, SquashCommitSet does not create a new commit (c has
		// an alias for the deleted commit in upstream1)
		commits, err = env.PachClient.ListCommit(client.NewRepo("repo"), nil, nil, 0)
		require.NoError(t, err)
		require.Equal(t, 5, len(commits))
		require.Nil(t, aInfo.ParentCommit)
		require.Equal(t, a.ID, cInfo.ParentCommit.ID)
		require.Equal(t, a.ID, dInfo.ParentCommit.ID)
		require.Equal(t, a.ID, eInfo.ParentCommit.ID)
		require.Equal(t, c.ID, fInfo.ParentCommit.ID)
		require.ImagesEqual(t, []*pfs.Commit{d, e, c}, aInfo.ChildCommits, CommitToID)
		require.ImagesEqual(t, []*pfs.Commit{f}, cInfo.ChildCommits, CommitToID)
		require.Nil(t, dInfo.ChildCommits)
		require.Nil(t, eInfo.ChildCommits)
		require.Nil(t, fInfo.ChildCommits)

		masterInfo, err := env.PachClient.InspectBranch("repo", "master")
		require.NoError(t, err)
		require.Equal(t, c.ID, masterInfo.Head.ID)
	})

	suite.Run("CommitState", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		// two input repos, one with many commits (logs), and one with few (schema)
		require.NoError(t, env.PachClient.CreateRepo("A"))
		require.NoError(t, env.PachClient.CreateRepo("B"))

		require.NoError(t, env.PachClient.CreateBranch("B", "master", "", "", []*pfs.Branch{client.NewBranch("A", "master")}))

		// Start a commit on A/master, this will create a non-ready commit on B/master.
		_, err := env.PachClient.StartCommit("A", "master")
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()
		_, err = env.PachClient.PfsAPIClient.InspectCommit(ctx, &pfs.InspectCommitRequest{
			Commit: client.NewCommit("B", "master", ""),
			Wait:   pfs.CommitState_READY,
		})
		require.YesError(t, err)

		// Finish the commit on A/master, that will make the B/master ready.
		require.NoError(t, env.PachClient.FinishCommit("A", "master", ""))

		ctx, cancel = context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()
		_, err = env.PachClient.PfsAPIClient.InspectCommit(ctx, &pfs.InspectCommitRequest{
			Commit: client.NewCommit("B", "master", ""),
			Wait:   pfs.CommitState_READY,
		})
		require.NoError(t, err)

		// Create a new branch C/master with A/master as provenance. It should start out ready.
		require.NoError(t, env.PachClient.CreateRepo("C"))
		require.NoError(t, env.PachClient.CreateBranch("C", "master", "", "", []*pfs.Branch{client.NewBranch("A", "master")}))

		ctx, cancel = context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()
		_, err = env.PachClient.PfsAPIClient.InspectCommit(ctx, &pfs.InspectCommitRequest{
			Commit: client.NewCommit("C", "master", ""),
			Wait:   pfs.CommitState_READY,
		})
		require.NoError(t, err)
	})

	suite.Run("SubscribeStates", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		require.NoError(t, env.PachClient.CreateRepo("A"))
		require.NoError(t, env.PachClient.CreateRepo("B"))
		require.NoError(t, env.PachClient.CreateRepo("C"))

		require.NoError(t, env.PachClient.CreateBranch("B", "master", "", "", []*pfs.Branch{client.NewBranch("A", "master")}))
		require.NoError(t, env.PachClient.FinishCommit("B", "master", ""))
		require.NoError(t, env.PachClient.CreateBranch("C", "master", "", "", []*pfs.Branch{client.NewBranch("B", "master")}))
		require.NoError(t, env.PachClient.FinishCommit("C", "master", ""))

		ctx, cancel := context.WithCancel(env.PachClient.Ctx())
		defer cancel()
		pachClient := env.PachClient.WithCtx(ctx)

		var readyCommits int64
		go func() {
			pachClient.SubscribeCommit(client.NewRepo("B"), "master", "", pfs.CommitState_READY, func(ci *pfs.CommitInfo) error {
				atomic.AddInt64(&readyCommits, 1)
				return nil
			})
		}()
		go func() {
			pachClient.SubscribeCommit(client.NewRepo("C"), "master", "", pfs.CommitState_READY, func(ci *pfs.CommitInfo) error {
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
				s := status.Convert(err)
				require.Equal(t, s.Code(), codes.NotFound)
			}
			require.NoError(t, env.PachClient.WalkFile(latestCommit, "", cb))
			require.NoError(t, env.PachClient.WalkFile(latestCommit, "/", cb))
			checkNotFound("foo")
			checkNotFound("/foo")
			checkNotFound("foo/bar")
			checkNotFound("/foo/bar")
		}

		require.NoError(t, env.PachClient.CreateRepo(repo))
		require.NoError(t, env.PachClient.CreateBranch(repo, "master", "", "", nil))
		checks() // Test the default empty head commit

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
			require.True(t, fileInfo.Details.SizeBytes > 0)
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
				require.True(t, fileInfo.Details.SizeBytes > 0)
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
		require.NoError(t, env.PachClient.CreateBranch(outputRepo, "master", "", "", []*pfs.Branch{client.NewBranch(inputRepo, "master")}))
		require.NoError(t, env.PachClient.FinishCommit(outputRepo, "master", ""))
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
				Repo:   client.NewRepo(repo),
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
				Repo:        client.NewRepo(repo),
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
		require.NoError(t, env.PachClient.CreateBranch("output1", "staging", "", "", []*pfs.Branch{client.NewBranch("input", "master")}))
		require.NoError(t, env.PachClient.FinishCommit("output1", "staging", ""))
		require.NoError(t, env.PachClient.CreateBranch("output2", "staging", "", "", []*pfs.Branch{client.NewBranch("output1", "master")}))
		require.NoError(t, env.PachClient.FinishCommit("output2", "staging", ""))
		require.NoError(t, env.PachClient.PutFile(client.NewCommit("input", "staging", ""), "file", strings.NewReader("foo")))
		commitInfoA, err := env.PachClient.InspectCommit("input", "staging", "")
		require.NoError(t, err)
		commitsetID := commitInfoA.Commit.ID

		commitInfos, err := env.PachClient.WaitCommitSetAll(commitsetID)
		require.NoError(t, err)
		require.Equal(t, 1, len(commitInfos))
		require.Equal(t, commitInfoA.Commit, commitInfos[0].Commit)

		require.NoError(t, env.PachClient.CreateBranch("input", "master", "staging", "", nil))
		require.NoError(t, env.PachClient.FinishCommit("output1", "staging", ""))
		commitInfoB, err := env.PachClient.InspectCommit("input", "master", "")
		require.NoError(t, err)
		require.Equal(t, commitsetID, commitInfoB.Commit.ID)

		commitInfos, err = env.PachClient.WaitCommitSetAll(commitsetID)
		require.NoError(t, err)
		require.Equal(t, 3, len(commitInfos))

		// The results _should_ be topologically sorted, but there are several
		// branches with equivalent topological depth
		expectedCommits := []string{
			pfsdb.CommitKey(client.NewCommit("input", "staging", commitsetID)),
			pfsdb.CommitKey(client.NewCommit("input", "master", commitsetID)),
			pfsdb.CommitKey(client.NewCommit("output1", "staging", commitsetID)),
		}
		require.ElementsEqualUnderFn(t, expectedCommits, commitInfos, CommitInfoToID)

		require.NoError(t, env.PachClient.CreateBranch("output1", "master", "staging", "", nil))
		require.NoError(t, env.PachClient.FinishCommit("output2", "staging", ""))
		commitInfoC, err := env.PachClient.InspectCommit("output1", "master", "")
		require.NoError(t, err)
		require.Equal(t, commitsetID, commitInfoC.Commit.ID)

		commitInfos, err = env.PachClient.WaitCommitSetAll(commitsetID)
		require.NoError(t, err)
		require.Equal(t, 5, len(commitInfos))
		expectedCommits = append(expectedCommits, []string{
			pfsdb.CommitKey(client.NewCommit("output1", "master", commitsetID)),
			pfsdb.CommitKey(client.NewCommit("output2", "staging", commitsetID)),
		}...)
		require.ElementsEqualUnderFn(t, expectedCommits, commitInfos, CommitInfoToID)
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
					require.True(t, errutil.IsNotFoundError(err), seedStr(seed))
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
		// TODO(optional 2.0): force-deleting the repo no longer creates dangling references
		t.Skip("this test no longer creates invalid metadata")
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		input := "input"
		output1 := "output1"
		output2 := "output2"
		require.NoError(t, env.PachClient.CreateRepo(input))
		require.NoError(t, env.PachClient.CreateRepo(output1))
		require.NoError(t, env.PachClient.CreateRepo(output2))
		require.NoError(t, env.PachClient.CreateBranch(output1, "master", "", "", []*pfs.Branch{client.NewBranch(input, "master")}))
		require.NoError(t, env.PachClient.CreateBranch(output2, "master", "", "", []*pfs.Branch{client.NewBranch(output1, "master")}))
		numCommits := 10
		for i := 0; i < numCommits; i++ {
			require.NoError(t, env.PachClient.PutFile(client.NewCommit(input, "master", ""), "file", strings.NewReader("1")))
		}
		require.NoError(t, env.PachClient.DeleteRepo(input, true))
		require.NoError(t, env.PachClient.CreateRepo(input))
		require.NoError(t, env.PachClient.CreateBranch(input, "master", "", "", nil))

		// Fsck should fail because ???
		require.YesError(t, env.PachClient.FsckFastExit())

		// Deleting output1 should fail because output2 is provenant on it
		require.YesError(t, env.PachClient.DeleteRepo(output1, false))

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

		mfc, err = c.NewModifyFileClient(commit)
		require.NoError(t, err)
		require.NoError(t, mfc.Close())
		cis, err = c.ListCommit(testRepo, commit, nil, 0)
		require.NoError(t, err)
		require.Equal(t, 3, len(cis))
	})

	const (
		inputRepo          = iota // create a new input repo
		inputBranch               // create a new branch on an existing input repo
		deleteInputBranch         // delete an input branch
		commit                    // commit to an input branch
		squashCommitSet           // squash a commitset from an input branch
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
			3, // squashCommitSet
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
				inputBranches = append(inputBranches, client.NewBranch(repo, "master"))
			case inputBranch:
				if len(inputRepos) == 0 {
					continue OpLoop
				}
				repo := inputRepos[r.Intn(len(inputRepos))]
				branch := tu.UniqueString("branch")
				require.NoError(t, env.PachClient.CreateBranch(repo, branch, "", "", nil))
				inputBranches = append(inputBranches, client.NewBranch(repo, branch))
			case deleteInputBranch:
				if len(inputBranches) == 0 {
					continue OpLoop
				}
				i := r.Intn(len(inputBranches))
				branch := inputBranches[i]
				err = env.PachClient.DeleteBranch(branch.Repo.Name, branch.Name, false)
				// don't fail if the error was just that it couldn't delete the branch without breaking subvenance
				inputBranches = append(inputBranches[:i], inputBranches[i+1:]...)
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
			case squashCommitSet:
				if len(commits) == 0 {
					continue OpLoop
				}
				i := r.Intn(len(commits))
				commit := commits[i]
				commits = append(commits[:i], commits[i+1:]...)
				require.NoError(t, env.PachClient.SquashCommitSet(commit.ID))
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

				err = env.PachClient.CreateBranch(repo, "master", "", "", provBranches)
				if err != nil && !strings.Contains(err.Error(), "cannot be in the provenance of its own branch") {
					require.NoError(t, err)
					outputBranches = append(outputBranches, client.NewBranch(repo, "master"))
				}
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

				err = env.PachClient.CreateBranch(repo, branch, "", "", provBranches)
				if err != nil && !strings.Contains(err.Error(), "cannot be in the provenance of its own branch") {
					require.NoError(t, err)
					outputBranches = append(outputBranches, client.NewBranch(repo, branch))
				}
			case deleteOutputBranch:
				if len(outputBranches) == 0 {
					continue OpLoop
				}
				i := r.Intn(len(outputBranches))
				branch := outputBranches[i]
				err = env.PachClient.DeleteBranch(branch.Repo.Name, branch.Name, false)
				// don't fail if the error was just that it couldn't delete the branch without breaking subvenance
				outputBranches = append(outputBranches[:i], outputBranches[i+1:]...)
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
			// TODO(2.0 required): fix size-based triggering (likely broken due to async compaction)
			t.Skip("Triggering based on size does not work with V2, needs investigation")
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
			require.NoError(t, c.CreateBranch("out", "master", "", "", []*pfs.Branch{client.NewBranch("in", "trigger")}))
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

			bi, err := c.InspectBranch("count", "trigger")
			require.NoError(t, err)
			head := bi.Head

			masterHead := client.NewCommit("count", "master", "")
			// The first commit shouldn't trigger
			require.NoError(t, c.PutFile(masterHead, "file1", strings.NewReader("foo")))
			bi, err = c.InspectBranch("count", "trigger")
			require.NoError(t, err)
			require.Equal(t, head, bi.Head)

			// Second commit should trigger
			require.NoError(t, c.PutFile(masterHead, "file2", strings.NewReader("bar")))
			bi, err = c.InspectBranch("count", "trigger")
			require.NoError(t, err)
			require.NotEqual(t, head, bi.Head)
			head = bi.Head

			// The trigger commit should have the same ID as the master commit
			bi, err = c.InspectBranch("count", "master")
			require.NoError(t, err)
			require.Equal(t, head.ID, bi.Head.ID)

			// Third commit shouldn't trigger
			require.NoError(t, c.PutFile(masterHead, "file3", strings.NewReader("fizz")))
			bi, err = c.InspectBranch("count", "trigger")
			require.NoError(t, err)
			require.Equal(t, head, bi.Head)

			// Fourth commit should trigger
			require.NoError(t, c.PutFile(masterHead, "file4", strings.NewReader("buzz")))
			bi, err = c.InspectBranch("count", "trigger")
			require.NoError(t, err)
			require.NotEqual(t, head, bi.Head)
			head = bi.Head

			// The trigger commit should have the same ID as the master commit
			bi, err = c.InspectBranch("count", "master")
			require.NoError(t, err)
			require.Equal(t, head.ID, bi.Head.ID)
		})

		t.Run("Or", func(t *testing.T) {
			// TODO(2.0 required): fix size-based triggering (likely broken due to async compaction)
			t.Skip("Triggering based on size does not work with V2, needs investigation")
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
			// TODO(2.0 required): fix size-based triggering (likely broken due to async compaction)
			t.Skip("Triggering based on size does not work with V2, needs investigation")
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
			// TODO(2.0 required): fix size-based triggering (likely broken due to async compaction)
			t.Skip("Triggering based on size does not work with V2, needs investigation")
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
			// TODO(2.0 required): fix size-based triggering (likely broken due to async compaction)
			t.Skip("Triggering based on size does not work with V2, needs investigation")
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
				Branch: client.NewBranch("repo", "master"),
				Trigger: &pfs.Trigger{
					Branch: "master",
					Size_:  "1K",
				},
				Provenance: []*pfs.Branch{client.NewBranch("in", "master")},
			})
		require.YesError(t, err)
	})

	suite.Run("RegressionOrphanedFile", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))

		fsclient, err := env.PachClient.NewCreateFileSetClient()
		require.NoError(t, err)
		data := []byte("test data")
		spec := fileSetSpec{
			"file1.txt": tarutil.NewMemFile("file1.txt", data),
			"file2.txt": tarutil.NewMemFile("file2.txt", data),
		}
		require.NoError(t, fsclient.PutFileTAR(spec.makeTarStream()))
		resp, err := fsclient.Close()
		require.NoError(t, err)
		t.Logf("tmp fileset id: %s", resp.FileSetId)
		require.NoError(t, env.PachClient.RenewFileSet(resp.FileSetId, 60*time.Second))
		fis, err := env.PachClient.ListFileAll(client.NewCommit(client.FileSetsRepoName, "", resp.FileSetId), "/")
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
			require.NoError(t, env.PachClient.PutFileTAR(commit1, fsSpec.makeTarStream()))
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
			require.NoError(t, c.Send(&pfs.ModifyFileRequest{
				Body: &pfs.ModifyFileRequest_SetCommit{SetCommit: client.NewCommit(repo, "master", "")},
			}))
			for _, file := range files {
				require.NoError(t, c.Send(&pfs.ModifyFileRequest{
					Body: &pfs.ModifyFileRequest_AddFile{
						AddFile: &pfs.AddFile{
							Path: file,
							Source: &pfs.AddFile_Raw{
								Raw: &types.BytesValue{},
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
				Body: &pfs.ModifyFileRequest_SetCommit{SetCommit: client.NewCommit(repo, "master", "")},
			}))
			require.NoError(t, c.Send(&pfs.ModifyFileRequest{
				Body: &pfs.ModifyFileRequest_AddFile{
					AddFile: &pfs.AddFile{
						Path: filePath,
						Source: &pfs.AddFile_Raw{
							Raw: &types.BytesValue{Value: []byte(fileContent)},
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
		ctx, cf := context.WithCancel(c.Ctx())
		defer cf()
		_, err := c.PfsAPIClient.CreateRepo(ctx, &pfs.CreateRepoRequest{})
		requireNoPanic(err)
		_, err = c.PfsAPIClient.InspectRepo(ctx, &pfs.InspectRepoRequest{})
		requireNoPanic(err)
		_, err = c.PfsAPIClient.ListRepo(ctx, &pfs.ListRepoRequest{})
		requireNoPanic(err)
		_, err = c.PfsAPIClient.DeleteRepo(ctx, &pfs.DeleteRepoRequest{})
		requireNoPanic(err)
		_, err = c.PfsAPIClient.StartCommit(ctx, &pfs.StartCommitRequest{})
		requireNoPanic(err)
		_, err = c.PfsAPIClient.FinishCommit(ctx, &pfs.FinishCommitRequest{})
		requireNoPanic(err)
		_, err = c.PfsAPIClient.InspectCommit(ctx, &pfs.InspectCommitRequest{})
		requireNoPanic(err)
		_, err = c.PfsAPIClient.ListCommit(ctx, &pfs.ListCommitRequest{})
		requireNoPanic(err)
		_, err = c.PfsAPIClient.SquashCommitSet(c.Ctx(), &pfs.SquashCommitSetRequest{})
		requireNoPanic(err)
		_, err = c.PfsAPIClient.InspectCommitSet(c.Ctx(), &pfs.InspectCommitSetRequest{})
		requireNoPanic(err)
		_, err = c.PfsAPIClient.SubscribeCommit(ctx, &pfs.SubscribeCommitRequest{})
		requireNoPanic(err)
		_, err = c.PfsAPIClient.CreateBranch(ctx, &pfs.CreateBranchRequest{})
		requireNoPanic(err)
		_, err = c.PfsAPIClient.InspectBranch(ctx, &pfs.InspectBranchRequest{})
		requireNoPanic(err)
		_, err = c.PfsAPIClient.ListBranch(ctx, &pfs.ListBranchRequest{})
		requireNoPanic(err)
		_, err = c.PfsAPIClient.DeleteBranch(ctx, &pfs.DeleteBranchRequest{})
		requireNoPanic(err)
		_, err = c.PfsAPIClient.GetFileTAR(ctx, &pfs.GetFileRequest{})
		requireNoPanic(err)
		_, err = c.PfsAPIClient.InspectFile(ctx, &pfs.InspectFileRequest{})
		requireNoPanic(err)
		_, err = c.PfsAPIClient.ListFile(ctx, &pfs.ListFileRequest{})
		requireNoPanic(err)
		_, err = c.PfsAPIClient.WalkFile(ctx, &pfs.WalkFileRequest{})
		requireNoPanic(err)
		_, err = c.PfsAPIClient.GlobFile(ctx, &pfs.GlobFileRequest{})
		requireNoPanic(err)
		_, err = c.PfsAPIClient.DiffFile(ctx, &pfs.DiffFileRequest{})
		requireNoPanic(err)
		_, err = c.PfsAPIClient.Fsck(ctx, &pfs.FsckRequest{})
		requireNoPanic(err)
	})

	suite.Run("DuplicateFileDifferentTag", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))
		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))
		masterCommit := client.NewCommit(repo, "master", "")
		require.NoError(t, env.PachClient.WithModifyFileClient(masterCommit, func(mf client.ModifyFile) error {
			require.NoError(t, mf.PutFile("foo", strings.NewReader("foo\n"), client.WithTagPutFile("tag1")))
			require.NoError(t, mf.PutFile("foo", strings.NewReader("foo\n"), client.WithTagPutFile("tag2")))
			require.NoError(t, mf.PutFile("bar", strings.NewReader("bar\n")))
			return nil
		}))
		newFile := func(repo, branch, commit, path, tag string) *pfs.File {
			file := client.NewFile(repo, branch, commit, path)
			file.Tag = tag
			return file
		}
		expected := []*pfs.File{
			newFile(repo, "master", "", "/bar", fileset.DefaultFileTag),
			newFile(repo, "master", "", "/foo", "tag1"),
			newFile(repo, "master", "", "/foo", "tag2"),
		}
		require.NoError(t, env.PachClient.ListFile(masterCommit, "", func(fi *pfs.FileInfo) error {
			require.Equal(t, expected[0].Path, fi.File.Path)
			require.Equal(t, expected[0].Tag, fi.File.Tag)
			expected = expected[1:]
			return nil
		}))
		require.Equal(t, 0, len(expected))
	})

	suite.Run("ErroredCommits", func(t *testing.T) {
		t.Parallel()
		env := testpachd.NewRealEnv(t, tu.NewTestDBConfig(t))
		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))
		checks := func(t *testing.T, branch string) {
			// First commit should contain the first file.
			branchCommit := client.NewCommit(repo, branch, "^2")
			expected := []string{"/f1"}
			require.NoError(t, env.PachClient.ListFile(branchCommit, "", func(fi *pfs.FileInfo) error {
				require.Equal(t, expected[0], fi.File.Path)
				expected = expected[1:]
				return nil
			}))
			require.Equal(t, 0, len(expected))
			// Second commit (errored commit) should still be readable with its content included.
			branchCommit = client.NewCommit(repo, branch, "^1")
			expected = []string{"/f1", "/f2"}
			require.NoError(t, env.PachClient.ListFile(branchCommit, "", func(fi *pfs.FileInfo) error {
				require.Equal(t, expected[0], fi.File.Path)
				expected = expected[1:]
				return nil
			}))
			require.Equal(t, 0, len(expected))
			// Third commit should exclude the errored parent commit.
			branchCommit = client.NewCommit(repo, branch, "")
			expected = []string{"/f1", "/f3"}
			require.NoError(t, env.PachClient.ListFile(branchCommit, "", func(fi *pfs.FileInfo) error {
				require.Equal(t, expected[0], fi.File.Path)
				expected = expected[1:]
				return nil
			}))
			require.Equal(t, 0, len(expected))
		}
		t.Run("FinishedErroredFinished", func(t *testing.T) {
			branch := uuid.New()
			require.NoError(t, env.PachClient.CreateBranch(repo, branch, "", "", nil))
			branchCommit := client.NewCommit(repo, branch, "")
			require.NoError(t, env.PachClient.PutFile(branchCommit, "f1", strings.NewReader("foo\n")))
			commit, err := env.PachClient.StartCommit(repo, branch)
			require.NoError(t, err)
			require.NoError(t, env.PachClient.PutFile(branchCommit, "f2", strings.NewReader("foo\n")))
			_, err = env.PachClient.PfsAPIClient.FinishCommit(context.Background(), &pfs.FinishCommitRequest{
				Commit: commit,
				Error:  true,
			})
			require.NoError(t, err)
			require.NoError(t, env.PachClient.PutFile(branchCommit, "f3", strings.NewReader("foo\n")))
			checks(t, branch)
		})
		t.Run("FinishedErroredOpen", func(t *testing.T) {
			branch := uuid.New()
			require.NoError(t, env.PachClient.CreateBranch(repo, branch, "", "", nil))
			branchCommit := client.NewCommit(repo, branch, "")
			require.NoError(t, env.PachClient.PutFile(branchCommit, "f1", strings.NewReader("foo\n")))
			commit, err := env.PachClient.StartCommit(repo, branch)
			require.NoError(t, err)
			require.NoError(t, env.PachClient.PutFile(branchCommit, "f2", strings.NewReader("foo\n")))
			_, err = env.PachClient.PfsAPIClient.FinishCommit(context.Background(), &pfs.FinishCommitRequest{
				Commit: commit,
				Error:  true,
			})
			require.NoError(t, err)
			_, err = env.PachClient.StartCommit(repo, branch)
			require.NoError(t, err)
			require.NoError(t, env.PachClient.PutFile(branchCommit, "f3", strings.NewReader("foo\n")))
			checks(t, branch)
		})
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

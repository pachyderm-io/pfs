package testing

import (
	"archive/tar"
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	units "github.com/docker/go-units"
	"github.com/gogo/protobuf/types"
	pclient "github.com/pachyderm/pachyderm/v2/src/client"
	"github.com/pachyderm/pachyderm/v2/src/internal/ancestry"
	"github.com/pachyderm/pachyderm/v2/src/internal/dbutil"
	"github.com/pachyderm/pachyderm/v2/src/internal/errors"
	"github.com/pachyderm/pachyderm/v2/src/internal/obj"
	"github.com/pachyderm/pachyderm/v2/src/internal/require"
	"github.com/pachyderm/pachyderm/v2/src/internal/sql"
	"github.com/pachyderm/pachyderm/v2/src/internal/tarutil"
	"github.com/pachyderm/pachyderm/v2/src/internal/testpachd"
	tu "github.com/pachyderm/pachyderm/v2/src/internal/testutil"
	"github.com/pachyderm/pachyderm/v2/src/internal/testutil/random"
	"github.com/pachyderm/pachyderm/v2/src/internal/uuid"
	"github.com/pachyderm/pachyderm/v2/src/pfs"
	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/errgroup"
)

func collectCommitInfos(commitInfoIter pclient.CommitInfoIterator) ([]*pfs.CommitInfo, error) {
	var commitInfos []*pfs.CommitInfo
	for {
		commitInfo, err := commitInfoIter.Next()
		if errors.Is(err, io.EOF) {
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

func TestInvalidRepo(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		require.YesError(t, env.PachClient.CreateRepo("/repo"))

		require.NoError(t, env.PachClient.CreateRepo("lenny"))
		require.NoError(t, env.PachClient.CreateRepo("lenny123"))
		require.NoError(t, env.PachClient.CreateRepo("lenny_123"))
		require.NoError(t, env.PachClient.CreateRepo("lenny-123"))

		require.YesError(t, env.PachClient.CreateRepo("lenny.123"))
		require.YesError(t, env.PachClient.CreateRepo("lenny:"))
		require.YesError(t, env.PachClient.CreateRepo("lenny,"))
		require.YesError(t, env.PachClient.CreateRepo("lenny#"))

		return nil
	}))
}

func TestCreateSameRepoInParallel(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
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

		return nil
	}))
}

func TestCreateDifferentRepoInParallel(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
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

		return nil
	}))
}

func TestCreateRepoDeleteRepoRace(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		for i := 0; i < 100; i++ {
			require.NoError(t, env.PachClient.CreateRepo("foo"))
			require.NoError(t, env.PachClient.CreateRepo("bar"))
			errCh := make(chan error)
			go func() {
				errCh <- env.PachClient.DeleteRepo("foo", false)
			}()
			go func() {
				errCh <- env.PachClient.CreateBranch("bar", "master", "", []*pfs.Branch{pclient.NewBranch("foo", "master")})
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

		return nil
	}))
}

func TestBranch(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		repo := "repo"
		require.NoError(t, env.PachClient.CreateRepo(repo))
		_, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit(repo, "master"))
		commitInfo, err := env.PachClient.InspectCommit(repo, "master")
		require.NoError(t, err)
		require.Nil(t, commitInfo.ParentCommit)

		_, err = env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit(repo, "master"))
		commitInfo, err = env.PachClient.InspectCommit(repo, "master")
		require.NoError(t, err)
		require.NotNil(t, commitInfo.ParentCommit)

		return nil
	}))
}

func TestToggleBranchProvenance(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		require.NoError(t, env.PachClient.CreateRepo("in"))
		require.NoError(t, env.PachClient.CreateRepo("out"))
		require.NoError(t, env.PachClient.CreateBranch("out", "master", "", []*pfs.Branch{
			pclient.NewBranch("in", "master"),
		}))

		// Create initial input commit, and make sure we get an output commit
		require.NoError(t, env.PachClient.PutFile("in", "master", "1", strings.NewReader("1")))
		cis, err := env.PachClient.ListCommit("out", "master", "", 0)
		require.NoError(t, err)
		require.Equal(t, 1, len(cis))
		require.NoError(t, env.PachClient.FinishCommit("out", "master"))
		// make sure output commit has the right provenance
		ci, err := env.PachClient.InspectCommit("in", "master")
		require.NoError(t, err)
		expectedProv := map[string]bool{
			path.Join("in", ci.Commit.ID): true,
		}
		ci, err = env.PachClient.InspectCommit("out", "master")
		require.NoError(t, err)
		require.Equal(t, len(expectedProv), len(ci.Provenance))
		for _, c := range ci.Provenance {
			require.True(t, expectedProv[path.Join(c.Commit.Repo.Name, c.Commit.ID)])
		}

		// Toggle out@master provenance off
		require.NoError(t, env.PachClient.CreateBranch("out", "master", "master", nil))

		// Create new input commit & make sure no new output commit is created
		require.NoError(t, env.PachClient.PutFile("in", "master", "2", strings.NewReader("2")))
		cis, err = env.PachClient.ListCommit("out", "master", "", 0)
		require.NoError(t, err)
		require.Equal(t, 1, len(cis))
		// make sure output commit still has the right provenance
		ci, err = env.PachClient.InspectCommit("in", "master~1") // old input commit
		require.NoError(t, err)
		expectedProv = map[string]bool{
			path.Join("in", ci.Commit.ID): true,
		}
		ci, err = env.PachClient.InspectCommit("out", "master")
		require.NoError(t, err)
		require.Equal(t, len(expectedProv), len(ci.Provenance))
		for _, c := range ci.Provenance {
			require.True(t, expectedProv[path.Join(c.Commit.Repo.Name, c.Commit.ID)])
		}

		// Toggle out@master provenance back on, creating a new output commit
		require.NoError(t, env.PachClient.CreateBranch("out", "master", "master", []*pfs.Branch{
			pclient.NewBranch("in", "master"),
		}))
		cis, err = env.PachClient.ListCommit("out", "master", "", 0)
		require.NoError(t, err)
		require.Equal(t, 2, len(cis))
		// make sure new output commit has the right provenance
		ci, err = env.PachClient.InspectCommit("in", "master") // newest input commit
		require.NoError(t, err)
		expectedProv = map[string]bool{
			path.Join("in", ci.Commit.ID): true,
		}
		ci, err = env.PachClient.InspectCommit("out", "master")
		require.NoError(t, err)
		require.Equal(t, len(expectedProv), len(ci.Provenance))
		for _, c := range ci.Provenance {
			require.True(t, expectedProv[path.Join(c.Commit.Repo.Name, c.Commit.ID)])
		}

		return nil
	}))
}

func TestRecreateBranchProvenance(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		require.NoError(t, env.PachClient.CreateRepo("in"))
		require.NoError(t, env.PachClient.CreateRepo("out"))
		require.NoError(t, env.PachClient.CreateBranch("out", "master", "", []*pfs.Branch{pclient.NewBranch("in", "master")}))
		require.NoError(t, env.PachClient.PutFile("in", "master", "foo", strings.NewReader("foo")))
		cis, err := env.PachClient.ListCommit("out", "", "", 0)
		require.NoError(t, err)
		id := cis[0].Commit.ID
		require.Equal(t, 1, len(cis))
		require.NoError(t, env.PachClient.DeleteBranch("out", "master", false))
		require.NoError(t, env.PachClient.CreateBranch("out", "master", id, []*pfs.Branch{pclient.NewBranch("in", "master")}))
		cis, err = env.PachClient.ListCommit("out", "", "", 0)
		require.NoError(t, err)
		require.Equal(t, 1, len(cis))
		require.Equal(t, id, cis[0].Commit.ID)

		return nil
	}))
}

func TestCreateAndInspectRepo(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
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

		return nil
	}))
}

func TestListRepo(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
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

		return nil
	}))
}

// Make sure that commits of deleted repos do not resurface
func TestCreateDeletedRepo(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		repo := "repo"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		commit, err := env.PachClient.StartCommit(repo, "")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(repo, commit.ID, "foo", strings.NewReader("foo")))
		require.NoError(t, env.PachClient.FinishCommit(repo, commit.ID))

		commitInfos, err := env.PachClient.ListCommit(repo, "", "", 0)
		require.NoError(t, err)
		require.Equal(t, 1, len(commitInfos))

		require.NoError(t, env.PachClient.DeleteRepo(repo, false))
		require.NoError(t, env.PachClient.CreateRepo(repo))

		commitInfos, err = env.PachClient.ListCommit(repo, "", "", 0)
		require.NoError(t, err)
		require.Equal(t, 0, len(commitInfos))

		return nil
	}))
}

// Make sure that commits of deleted repos do not resurface
func TestListCommitLimit(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		repo := "repo"
		require.NoError(t, env.PachClient.CreateRepo(repo))
		require.NoError(t, env.PachClient.PutFile(repo, "master", "foo", strings.NewReader("foo")))
		require.NoError(t, env.PachClient.PutFile(repo, "master", "bar", strings.NewReader("bar")))
		commitInfos, err := env.PachClient.ListCommit(repo, "", "", 1)
		require.NoError(t, err)
		require.Equal(t, 1, len(commitInfos))
		return nil
	}))
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
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		prov1 := "prov1"
		require.NoError(t, env.PachClient.CreateRepo(prov1))
		prov2 := "prov2"
		require.NoError(t, env.PachClient.CreateRepo(prov2))
		prov3 := "prov3"
		require.NoError(t, env.PachClient.CreateRepo(prov3))

		repo := "repo"
		require.NoError(t, env.PachClient.CreateRepo(repo))
		require.NoError(t, env.PachClient.CreateBranch(repo, "master", "", []*pfs.Branch{pclient.NewBranch(prov1, "master"), pclient.NewBranch(prov2, "master")}))

		downstream1 := "downstream1"
		require.NoError(t, env.PachClient.CreateRepo(downstream1))
		require.NoError(t, env.PachClient.CreateBranch(downstream1, "master", "", []*pfs.Branch{pclient.NewBranch(repo, "master")}))

		downstream2 := "downstream2"
		require.NoError(t, env.PachClient.CreateRepo(downstream2))
		require.NoError(t, env.PachClient.CreateBranch(downstream2, "master", "", []*pfs.Branch{pclient.NewBranch(repo, "master")}))

		// Without the Update flag it should fail
		require.YesError(t, env.PachClient.CreateRepo(repo))

		_, err := env.PachClient.PfsAPIClient.CreateRepo(context.Background(), &pfs.CreateRepoRequest{
			Repo:   pclient.NewRepo(repo),
			Update: true,
		})
		require.NoError(t, err)

		require.NoError(t, env.PachClient.CreateBranch(repo, "master", "", []*pfs.Branch{pclient.NewBranch(prov2, "master"), pclient.NewBranch(prov3, "master")}))

		// We should be able to delete prov1 since it's no longer the provenance
		// of other repos.
		require.NoError(t, env.PachClient.DeleteRepo(prov1, false))

		// We shouldn't be able to delete prov3 since it's now a provenance
		// of other repos.
		require.YesError(t, env.PachClient.DeleteRepo(prov3, false))

		return nil
	}))
}

func TestPutFileIntoOpenCommit(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		commit1, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(repo, commit1.ID, "foo", strings.NewReader("foo\n")))
		require.NoError(t, env.PachClient.FinishCommit(repo, commit1.ID))

		require.YesError(t, env.PachClient.PutFile(repo, commit1.ID, "foo", strings.NewReader("foo\n")))

		commit2, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(repo, "master", "foo", strings.NewReader("foo\n")))
		require.NoError(t, env.PachClient.FinishCommit(repo, "master"))

		require.YesError(t, env.PachClient.PutFile(repo, commit2.ID, "foo", strings.NewReader("foo\n")))

		return nil
	}))
}

// TODO: Make work with V2?
//func TestPutFileDirectoryTraversal(t *testing.T) {
//	t.Parallel()
//	db := dbutil.NewTestDB(t)
//	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
//		require.NoError(t, env.PachClient.CreateRepo("repo"))
//
//		_, err := env.PachClient.StartCommit("repo", "master")
//		require.NoError(t, err)
//
//		pfc, err := env.PachClient.NewPutFileClient()
//		require.NoError(t, err)
//		require.NoError(t, pfc.PutFile("repo", "master", "../foo", strings.NewReader("foo\n")))
//		require.YesError(t, pfc.Close())
//
//		fis, err := env.PachClient.ListFileAll("repo", "master", "")
//		require.NoError(t, err)
//		require.Equal(t, 0, len(fis))
//
//		pfc, err = env.PachClient.NewPutFileClient()
//		require.NoError(t, err)
//		require.NoError(t, pfc.PutFile("repo", "master", "foo/../../bar", strings.NewReader("foo\n")))
//		require.YesError(t, pfc.Close())
//
//		fis, err = env.PachClient.ListFileAll("repo", "master", "")
//		require.NoError(t, err)
//		require.Equal(t, 0, len(fis))
//
//		pfc, err = env.PachClient.NewPutFileClient()
//		require.NoError(t, err)
//		require.NoError(t, pfc.PutFile("repo", "master", "foo/../bar", strings.NewReader("foo\n")))
//		require.NoError(t, pfc.Close())
//
//		fis, err = env.PachClient.ListFileAll("repo", "master", "")
//		require.NoError(t, err)
//		require.Equal(t, 1, len(fis))
//
//		return nil
//	}))
//}

// TODO: Make work with V2?
//// TestPutFileOverlappingPaths tests the fix for
//// https://github.com/pachyderm/pachyderm/v2/issues/5345.
//// However, I can't get the test to fail without adding a sleep to
//// forEachPutFile in driver.go that induces the race (at:
//// `if req.Delete {...eg.Go(/*here*/...)}`). Setting GOMAXPROCS to 1 and 100
//// doesn't seem to help. In practice, that means we're still relying on
//// TestPipelineBuildLifecycle and pachyderm-in-Minikube to expose the race
//// (Minikube seems to induce more races), but maybe this test will be useful in
//// conjunction with e.g. some kind of race detector.
//// TODO(msteffen): Get this test to fail reliably in the presence of the race.
//func TestPutFileOverlappingPaths(t *testing.T) {
//	t.Parallel()
//	db := dbutil.NewTestDB(t)
//	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
//	files := []string{"/a", "/b", "/c"}
//	contents := []string{"foo", "bar", "baz"}
//		c := env.PachClient
//		c.CreateRepo("test")
//		for i := 0; i < 100; i++ {
//			pfc, err := c.NewPutFileClient()
//			require.NoError(t, err)
//			require.NoError(t, pfc.DeleteFile("test", "master", "/"))
//			for i, f := range files {
//				_, err := pfc.PutFile("test", "master", f, strings.NewReader(contents[i]))
//				require.NoError(t, err)
//			}
//			require.NoError(t, pfc.Close())
//			var actualFiles []string
//			c.ListFileF("test", "master", "/", 0, func(f *pfs.FileInfo) error {
//				actualFiles = append(actualFiles, f.File.Path)
//				return nil
//			})
//			require.ElementsEqual(t, files, actualFiles)
//		}
//		return nil
//	})
//	require.NoError(t, err)
//}

func TestCreateInvalidBranchName(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		// Create a branch that's the same length as a commit ID
		_, err := env.PachClient.StartCommit(repo, uuid.NewWithoutDashes())
		require.YesError(t, err)

		return nil
	}))
}

func TestDeleteRepo(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
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

		return nil
	}))
}

func TestDeleteRepoProvenance(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		// Create two repos, one as another's provenance
		require.NoError(t, env.PachClient.CreateRepo("A"))
		require.NoError(t, env.PachClient.CreateRepo("B"))
		require.NoError(t, env.PachClient.CreateBranch("B", "master", "", []*pfs.Branch{pclient.NewBranch("A", "master")}))

		commit, err := env.PachClient.StartCommit("A", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("A", commit.ID))

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
		require.NoError(t, env.PachClient.CreateBranch("B", "master", "", []*pfs.Branch{pclient.NewBranch("A", "master")}))

		// Force delete should succeed
		require.NoError(t, env.PachClient.DeleteRepo("A", true))

		repoInfos, err = env.PachClient.ListRepo()
		require.NoError(t, err)
		require.Equal(t, 1, len(repoInfos))

		// Everything should be consistent
		require.NoError(t, env.PachClient.FsckFastExit())

		return nil
	}))
}

func TestInspectCommit(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		started := time.Now()
		commit, err := env.PachClient.StartCommit(repo, "")
		require.NoError(t, err)

		fileContent := "foo\n"
		require.NoError(t, env.PachClient.PutFile(repo, commit.ID, "foo", strings.NewReader(fileContent)))

		commitInfo, err := env.PachClient.InspectCommit(repo, commit.ID)
		require.NoError(t, err)

		tStarted, err := types.TimestampFromProto(commitInfo.Started)
		require.NoError(t, err)

		require.Equal(t, commit, commitInfo.Commit)
		require.Nil(t, commitInfo.Finished)
		// PutFile does not update commit size; only FinishCommit does
		require.Equal(t, 0, int(commitInfo.SizeBytes))
		require.True(t, started.Before(tStarted))
		require.Nil(t, commitInfo.Finished)

		require.NoError(t, env.PachClient.FinishCommit(repo, commit.ID))
		finished := time.Now()

		commitInfo, err = env.PachClient.InspectCommit(repo, commit.ID)
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

		return nil
	}))
}

func TestInspectCommitBlock(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		repo := "TestInspectCommitBlock"
		require.NoError(t, env.PachClient.CreateRepo(repo))
		commit, err := env.PachClient.StartCommit(repo, "")
		require.NoError(t, err)

		var eg errgroup.Group
		eg.Go(func() error {
			time.Sleep(2 * time.Second)
			return env.PachClient.FinishCommit(repo, commit.ID)
		})

		commitInfo, err := env.PachClient.BlockCommit(commit.Repo.Name, commit.ID)
		require.NoError(t, err)
		require.NotNil(t, commitInfo.Finished)

		require.NoError(t, eg.Wait())

		return nil
	}))
}

func TestDeleteCommit(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		commit1, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)

		fileContent := "foo\n"
		require.NoError(t, env.PachClient.PutFile(repo, commit1.ID, "foo", strings.NewReader(fileContent)))

		require.NoError(t, env.PachClient.FinishCommit(repo, "master"))

		commit2, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)

		require.NoError(t, env.PachClient.DeleteCommit(repo, commit2.ID))

		_, err = env.PachClient.InspectCommit(repo, commit2.ID)
		require.YesError(t, err)

		// Check that the head has been set to the parent
		commitInfo, err := env.PachClient.InspectCommit(repo, "master")
		require.NoError(t, err)
		require.Equal(t, commit1.ID, commitInfo.Commit.ID)

		// Check that the branch still exists
		branches, err := env.PachClient.ListBranch(repo)
		require.NoError(t, err)
		require.Equal(t, 1, len(branches))

		return nil
	}))
}

func TestDeleteCommitOnlyCommitInBranch(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		commit, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(repo, commit.ID, "foo", strings.NewReader("foo\n")))
		require.NoError(t, env.PachClient.DeleteCommit(repo, "master"))

		// The branch has not been deleted, though it has no commits
		branches, err := env.PachClient.ListBranch(repo)
		require.NoError(t, err)
		require.Equal(t, 1, len(branches))
		commits, err := env.PachClient.ListCommit(repo, "master", "", 0)
		require.NoError(t, err)
		require.Equal(t, 0, len(commits))

		// Check that repo size is back to 0
		repoInfo, err := env.PachClient.InspectRepo(repo)
		require.NoError(t, err)
		require.Equal(t, 0, int(repoInfo.SizeBytes))

		return nil
	}))
}

func TestDeleteCommitFinished(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		commit, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(repo, commit.ID, "foo", strings.NewReader("foo\n")))
		require.NoError(t, env.PachClient.FinishCommit(repo, commit.ID))
		require.NoError(t, env.PachClient.DeleteCommit(repo, "master"))

		// The branch has not been deleted, though it has no commits
		branches, err := env.PachClient.ListBranch(repo)
		require.NoError(t, err)
		require.Equal(t, 1, len(branches))
		commits, err := env.PachClient.ListCommit(repo, "master", "", 0)
		require.NoError(t, err)
		require.Equal(t, 0, len(commits))

		// Check that repo size is back to 0
		repoInfo, err := env.PachClient.InspectRepo(repo)
		require.NoError(t, err)
		require.Equal(t, 0, int(repoInfo.SizeBytes))

		return nil
	}))
}

// TODO: Make work with V2?
//func TestCleanPath(t *testing.T) {
//	t.Parallel()
//	db := dbutil.NewTestDB(t)
//	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
//		repo := "TestCleanPath"
//		require.NoError(t, env.PachClient.CreateRepo(repo))
//		commit, err := env.PachClient.StartCommit(repo, "master")
//		require.NoError(t, err)
//		require.NoError(t, env.PachClient.PutFile(repo, commit.ID, "./././file", strings.NewReader("foo")))
//		require.NoError(t, env.PachClient.FinishCommit(repo, commit.ID))
//		_, err = env.PachClient.InspectFile(repo, commit.ID, "file")
//		require.NoError(t, err)
//
//		return nil
//	}))
//}

func TestBasicFile(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		repo := "repo"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		commit, err := env.PachClient.StartCommit(repo, "")
		require.NoError(t, err)

		file := "file"
		data := "data"
		require.NoError(t, env.PachClient.PutFile(repo, commit.ID, file, strings.NewReader(data)))

		require.NoError(t, env.PachClient.FinishCommit(repo, commit.ID))

		var b bytes.Buffer
		require.NoError(t, env.PachClient.GetFile(repo, commit.ID, "file", &b))
		require.Equal(t, data, b.String())

		return nil
	}))
}

func TestSimpleFile(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		commit1, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(repo, commit1.ID, "foo", strings.NewReader("foo\n")))
		require.NoError(t, env.PachClient.FinishCommit(repo, commit1.ID))

		var buffer bytes.Buffer
		require.NoError(t, env.PachClient.GetFile(repo, commit1.ID, "foo", &buffer))
		require.Equal(t, "foo\n", buffer.String())

		commit2, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(repo, commit2.ID, "foo", strings.NewReader("foo\n")))
		err = env.PachClient.FinishCommit(repo, commit2.ID)
		require.NoError(t, err)

		buffer.Reset()
		require.NoError(t, env.PachClient.GetFile(repo, commit1.ID, "foo", &buffer))
		require.Equal(t, "foo\n", buffer.String())
		buffer.Reset()
		require.NoError(t, env.PachClient.GetFile(repo, commit2.ID, "foo", &buffer))
		require.Equal(t, "foo\nfoo\n", buffer.String())

		return nil
	}))
}

func TestStartCommitWithUnfinishedParent(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		commit1, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		_, err = env.PachClient.StartCommit(repo, "master")
		// fails because the parent commit has not been finished
		require.YesError(t, err)

		require.NoError(t, env.PachClient.FinishCommit(repo, commit1.ID))
		_, err = env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)

		return nil
	}))
}

func TestStartCommitWithDuplicatedCommitProvenance(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		commit1, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)

		require.NoError(t, env.PachClient.FinishCommit(repo, commit1.ID))

		commit2, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)

		_, err = env.PachClient.PfsAPIClient.StartCommit(env.PachClient.Ctx(), &pfs.StartCommitRequest{
			Parent:     pclient.NewCommit(repo, ""),
			Provenance: []*pfs.CommitProvenance{pclient.NewCommitProvenance(repo, "master", commit2.ID), pclient.NewCommitProvenance(repo, "master", commit2.ID)},
		})

		require.NoError(t, err)

		return nil
	}))
}

func TestAncestrySyntax(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		commit1, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFileOverwrite(repo, "master", "file", strings.NewReader("1")))
		require.NoError(t, env.PachClient.FinishCommit(repo, commit1.ID))

		commit2, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFileOverwrite(repo, commit2.ID, "file", strings.NewReader("2")))
		require.NoError(t, env.PachClient.FinishCommit(repo, commit2.ID))

		commit3, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFileOverwrite(repo, commit3.ID, "file", strings.NewReader("3")))
		require.NoError(t, env.PachClient.FinishCommit(repo, commit3.ID))

		commitInfo, err := env.PachClient.InspectCommit(repo, "master^")
		require.NoError(t, err)
		require.Equal(t, commit2, commitInfo.Commit)

		commitInfo, err = env.PachClient.InspectCommit(repo, "master~")
		require.NoError(t, err)
		require.Equal(t, commit2, commitInfo.Commit)

		commitInfo, err = env.PachClient.InspectCommit(repo, "master^1")
		require.NoError(t, err)
		require.Equal(t, commit2, commitInfo.Commit)

		commitInfo, err = env.PachClient.InspectCommit(repo, "master~1")
		require.NoError(t, err)
		require.Equal(t, commit2, commitInfo.Commit)

		commitInfo, err = env.PachClient.InspectCommit(repo, "master^^")
		require.NoError(t, err)
		require.Equal(t, commit1, commitInfo.Commit)

		commitInfo, err = env.PachClient.InspectCommit(repo, "master~~")
		require.NoError(t, err)
		require.Equal(t, commit1, commitInfo.Commit)

		commitInfo, err = env.PachClient.InspectCommit(repo, "master^2")
		require.NoError(t, err)
		require.Equal(t, commit1, commitInfo.Commit)

		commitInfo, err = env.PachClient.InspectCommit(repo, "master~2")
		require.NoError(t, err)
		require.Equal(t, commit1, commitInfo.Commit)

		commitInfo, err = env.PachClient.InspectCommit(repo, "master.1")
		require.NoError(t, err)
		require.Equal(t, commit1, commitInfo.Commit)

		commitInfo, err = env.PachClient.InspectCommit(repo, "master.2")
		require.NoError(t, err)
		require.Equal(t, commit2, commitInfo.Commit)

		commitInfo, err = env.PachClient.InspectCommit(repo, "master.3")
		require.NoError(t, err)
		require.Equal(t, commit3, commitInfo.Commit)

		_, err = env.PachClient.InspectCommit(repo, "master^^^")
		require.YesError(t, err)

		_, err = env.PachClient.InspectCommit(repo, "master~~~")
		require.YesError(t, err)

		_, err = env.PachClient.InspectCommit(repo, "master^3")
		require.YesError(t, err)

		_, err = env.PachClient.InspectCommit(repo, "master~3")
		require.YesError(t, err)

		for i := 1; i <= 2; i++ {
			_, err := env.PachClient.InspectFile(repo, fmt.Sprintf("%v^%v", commit3.ID, 3-i), "file")
			require.NoError(t, err)
		}

		var buffer bytes.Buffer
		require.NoError(t, env.PachClient.GetFile(repo, ancestry.Add("master", 0), "file", &buffer))
		require.Equal(t, "3", buffer.String())
		buffer.Reset()
		require.NoError(t, env.PachClient.GetFile(repo, ancestry.Add("master", 1), "file", &buffer))
		require.Equal(t, "2", buffer.String())
		buffer.Reset()
		require.NoError(t, env.PachClient.GetFile(repo, ancestry.Add("master", 2), "file", &buffer))
		require.Equal(t, "1", buffer.String())
		buffer.Reset()
		require.NoError(t, env.PachClient.GetFile(repo, ancestry.Add("master", -1), "file", &buffer))
		require.Equal(t, "1", buffer.String())
		buffer.Reset()
		require.NoError(t, env.PachClient.GetFile(repo, ancestry.Add("master", -2), "file", &buffer))
		require.Equal(t, "2", buffer.String())
		buffer.Reset()
		require.NoError(t, env.PachClient.GetFile(repo, ancestry.Add("master", -3), "file", &buffer))
		require.Equal(t, "3", buffer.String())

		// Adding a bunch of commits to the head of the branch shouldn't change the forward references.
		// (It will change backward references.)
		for i := 0; i < 10; i++ {
			require.NoError(t, env.PachClient.PutFileOverwrite(repo, "master", "file", strings.NewReader(fmt.Sprintf("%d", i+4))))
		}
		commitInfo, err = env.PachClient.InspectCommit(repo, "master.1")
		require.NoError(t, err)
		require.Equal(t, commit1, commitInfo.Commit)

		commitInfo, err = env.PachClient.InspectCommit(repo, "master.2")
		require.NoError(t, err)
		require.Equal(t, commit2, commitInfo.Commit)

		commitInfo, err = env.PachClient.InspectCommit(repo, "master.3")
		require.NoError(t, err)
		require.Equal(t, commit3, commitInfo.Commit)

		return nil
	}))
}

// TestProvenance implements the following DAG
//  A ─▶ B ─▶ C ─▶ D
//            ▲
//  E ────────╯

func TestProvenance(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		require.NoError(t, env.PachClient.CreateRepo("A"))
		require.NoError(t, env.PachClient.CreateRepo("B"))
		require.NoError(t, env.PachClient.CreateRepo("C"))
		require.NoError(t, env.PachClient.CreateRepo("D"))
		require.NoError(t, env.PachClient.CreateRepo("E"))

		require.NoError(t, env.PachClient.CreateBranch("B", "master", "", []*pfs.Branch{pclient.NewBranch("A", "master")}))
		require.NoError(t, env.PachClient.CreateBranch("C", "master", "", []*pfs.Branch{pclient.NewBranch("B", "master"), pclient.NewBranch("E", "master")}))
		require.NoError(t, env.PachClient.CreateBranch("D", "master", "", []*pfs.Branch{pclient.NewBranch("C", "master")}))

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
		require.NoError(t, env.PachClient.FinishCommit("A", ACommit.ID))
		ECommit, err := env.PachClient.StartCommit("E", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("E", ECommit.ID))

		commitInfo, err := env.PachClient.InspectCommit("B", "master")
		require.NoError(t, err)
		require.Equal(t, 1, len(commitInfo.Provenance))

		commitInfo, err = env.PachClient.InspectCommit("C", "master")
		require.NoError(t, err)
		require.Equal(t, 3, len(commitInfo.Provenance))

		commitInfo, err = env.PachClient.InspectCommit("D", "master")
		require.NoError(t, err)
		require.Equal(t, 4, len(commitInfo.Provenance))

		return nil
	}))
}

func TestStartCommitWithBranchNameProvenance(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		require.NoError(t, env.PachClient.CreateRepo("A"))
		require.NoError(t, env.PachClient.CreateRepo("B"))
		require.NoError(t, env.PachClient.CreateRepo("C"))

		require.NoError(t, env.PachClient.CreateBranch("B", "master", "", []*pfs.Branch{pclient.NewBranch("A", "master")}))
		require.NoError(t, env.PachClient.CreateBranch("C", "master", "", []*pfs.Branch{pclient.NewBranch("B", "master")}))

		masterCommit, err := env.PachClient.StartCommit("A", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("A", masterCommit.ID))

		masterCommitInfo, err := env.PachClient.InspectCommit(masterCommit.Repo.Name, masterCommit.ID)
		require.NoError(t, err)

		bCommitInfo, err := env.PachClient.InspectCommit("B", "master")
		require.NoError(t, err)

		// We're specifying the same commit three times - once by branch name, once
		// by commit ID, and once indirectly through B, these should be collapsed to
		// one provenance entry.
		newCommit, err := env.PachClient.PfsAPIClient.StartCommit(env.Context, &pfs.StartCommitRequest{
			Parent: pclient.NewCommit("C", ""),
			Provenance: []*pfs.CommitProvenance{
				pclient.NewCommitProvenance("A", "master", "master"),
				pclient.NewCommitProvenance("A", "master", masterCommitInfo.Commit.ID),
				pclient.NewCommitProvenance("B", "master", bCommitInfo.Commit.ID),
			},
		})
		require.NoError(t, err)

		newCommitInfo, err := env.PachClient.InspectCommit(newCommit.Repo.Name, newCommit.ID)
		require.NoError(t, err)

		// Stupid require.ElementsEqual can't handle arrays of pointers
		expectedProvenanceA := &pfs.CommitProvenance{Commit: masterCommitInfo.Commit, Branch: masterCommitInfo.Branch}
		expectedProvenanceB := &pfs.CommitProvenance{Commit: bCommitInfo.Commit, Branch: bCommitInfo.Branch}
		require.Equal(t, 2, len(newCommitInfo.Provenance))
		if newCommitInfo.Provenance[0].Commit.Repo.Name == "A" {
			require.Equal(t, expectedProvenanceA, newCommitInfo.Provenance[0])
			require.Equal(t, expectedProvenanceB, newCommitInfo.Provenance[1])
		} else {
			require.Equal(t, expectedProvenanceB, newCommitInfo.Provenance[0])
			require.Equal(t, expectedProvenanceA, newCommitInfo.Provenance[1])
		}

		return nil
	}))
}

func TestCommitBranch(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		require.NoError(t, env.PachClient.CreateRepo("repo"))
		// Make two branches provenant on the master branch
		require.NoError(t, env.PachClient.CreateBranch("repo", "A", "", []*pfs.Branch{pclient.NewBranch("repo", "master")}))
		require.NoError(t, env.PachClient.CreateBranch("repo", "B", "", []*pfs.Branch{pclient.NewBranch("repo", "master")}))

		// Now make a commit on the master branch, which should trigger a downstream commit on each of the two branches
		masterCommit, err := env.PachClient.StartCommit("repo", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("repo", masterCommit.ID))

		// Check that the commit in branch A has the information and provenance we expect
		commitInfo, err := env.PachClient.InspectCommit("repo", "A")
		require.NoError(t, err)
		require.Equal(t, "A", commitInfo.Branch.Name)
		require.Equal(t, 1, len(commitInfo.Provenance))
		require.Equal(t, "master", commitInfo.Provenance[0].Branch.Name)

		// Check that the commit in branch B has the information and provenance we expect
		commitInfo, err = env.PachClient.InspectCommit("repo", "B")
		require.NoError(t, err)
		require.Equal(t, "B", commitInfo.Branch.Name)
		require.Equal(t, 1, len(commitInfo.Provenance))
		require.Equal(t, "master", commitInfo.Provenance[0].Branch.Name)

		return nil
	}))
}

func TestCommitOnTwoBranchesProvenance(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		require.NoError(t, env.PachClient.CreateRepo("repo"))

		parentCommit, err := env.PachClient.StartCommit("repo", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("repo", parentCommit.ID))

		masterCommit, err := env.PachClient.StartCommit("repo", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("repo", masterCommit.ID))

		// Make two branches provenant on the same commit on the master branch
		require.NoError(t, env.PachClient.CreateBranch("repo", "A", masterCommit.ID, nil))
		require.NoError(t, env.PachClient.CreateBranch("repo", "B", masterCommit.ID, nil))

		// Now create a branch provenant on both branches A and B
		require.NoError(t, env.PachClient.CreateBranch("repo", "C", "", []*pfs.Branch{pclient.NewBranch("repo", "A"), pclient.NewBranch("repo", "B")}))

		// The head commit of the C branch should have branches A and B both represented in the provenance
		// This is important because jobInput looks up commits by branch
		ci, err := env.PachClient.InspectCommit("repo", "C")
		require.NoError(t, err)
		require.Equal(t, 2, len(ci.Provenance))

		// We should also be able to delete the head commit of A
		require.NoError(t, env.PachClient.DeleteCommit("repo", "A"))

		// And the head of branch B should go back to the parent of the deleted commit
		branchInfo, err := env.PachClient.InspectBranch("repo", "B")
		require.NoError(t, err)
		require.Equal(t, parentCommit.ID, branchInfo.Head.ID)

		// We should also be able to delete the head commit of A
		require.NoError(t, env.PachClient.DeleteCommit("repo", parentCommit.ID))

		// It should also be ok to make new commits on branches A and B
		aCommit, err := env.PachClient.StartCommit("repo", "A")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("repo", aCommit.ID))

		bCommit, err := env.PachClient.StartCommit("repo", "B")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("repo", bCommit.ID))

		return nil
	}))
}

func TestSimple(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))
		commit1, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(repo, commit1.ID, "foo", strings.NewReader("foo\n")))
		require.NoError(t, env.PachClient.FinishCommit(repo, commit1.ID))
		commitInfos, err := env.PachClient.ListCommit(repo, "", "", 0)
		require.NoError(t, err)
		require.Equal(t, 1, len(commitInfos))
		var buffer bytes.Buffer
		require.NoError(t, env.PachClient.GetFile(repo, commit1.ID, "foo", &buffer))
		require.Equal(t, "foo\n", buffer.String())
		commit2, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(repo, commit2.ID, "foo", strings.NewReader("foo\n")))
		require.NoError(t, env.PachClient.FinishCommit(repo, commit2.ID))
		buffer = bytes.Buffer{}
		require.NoError(t, env.PachClient.GetFile(repo, commit1.ID, "foo", &buffer))
		require.Equal(t, "foo\n", buffer.String())
		buffer = bytes.Buffer{}
		require.NoError(t, env.PachClient.GetFile(repo, commit2.ID, "foo", &buffer))
		require.Equal(t, "foo\nfoo\n", buffer.String())

		return nil
	}))
}

func TestBranch1(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))
		commit, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(repo, "master", "foo", strings.NewReader("foo\n")))
		require.NoError(t, env.PachClient.FinishCommit(repo, "master"))
		var buffer bytes.Buffer
		require.NoError(t, env.PachClient.GetFile(repo, "master", "foo", &buffer))
		require.Equal(t, "foo\n", buffer.String())
		branches, err := env.PachClient.ListBranch(repo)
		require.NoError(t, err)
		require.Equal(t, 1, len(branches))
		require.Equal(t, "master", branches[0].Name)

		_, err = env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(repo, "master", "foo", strings.NewReader("foo\n")))
		require.NoError(t, env.PachClient.FinishCommit(repo, "master"))
		buffer = bytes.Buffer{}
		require.NoError(t, env.PachClient.GetFile(repo, "master", "foo", &buffer))
		require.Equal(t, "foo\nfoo\n", buffer.String())
		branches, err = env.PachClient.ListBranch(repo)
		require.NoError(t, err)
		require.Equal(t, 1, len(branches))
		require.Equal(t, "master", branches[0].Name)

		require.NoError(t, env.PachClient.SetBranch(repo, commit.ID, "master2"))

		branches, err = env.PachClient.ListBranch(repo)
		require.NoError(t, err)
		require.Equal(t, 2, len(branches))
		require.Equal(t, "master2", branches[0].Name)
		require.Equal(t, "master", branches[1].Name)

		return nil
	}))
}

func TestPutFileBig(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		// Write a big blob that would normally not fit in a block
		fileSize := int(pfs.ChunkSize + 5*1024*1024)
		expectedOutputA := random.String(fileSize)
		r := strings.NewReader(string(expectedOutputA))

		commit1, err := env.PachClient.StartCommit(repo, "")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(repo, commit1.ID, "foo", r))
		require.NoError(t, env.PachClient.FinishCommit(repo, commit1.ID))

		fileInfo, err := env.PachClient.InspectFile(repo, commit1.ID, "foo")
		require.NoError(t, err)
		require.Equal(t, fileSize, int(fileInfo.SizeBytes))

		var buffer bytes.Buffer
		require.NoError(t, env.PachClient.GetFile(repo, commit1.ID, "foo", &buffer))
		require.Equal(t, string(expectedOutputA), buffer.String())

		return nil
	}))
}

// TODO: Make work with V2?
//func TestPutFile(t *testing.T) {
//	t.Parallel()
//	db := dbutil.NewTestDB(t)
//	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
//		repo := "test"
//		require.NoError(t, env.PachClient.CreateRepo(repo))
//
//		// Detect file conflict
//		commit1, err := env.PachClient.StartCommit(repo, "")
//		require.NoError(t, err)
//		require.NoError(t, env.PachClient.PutFile(repo, commit1.ID, "foo", strings.NewReader("foo\n")))
//		require.NoError(t, env.PachClient.PutFile(repo, commit1.ID, "foo/bar", strings.NewReader("foo\n")))
//		require.YesError(t, env.PachClient.FinishCommit(repo, commit1.ID))
//
//		commit1, err = env.PachClient.StartCommit(repo, "")
//		require.NoError(t, err)
//		require.NoError(t, env.PachClient.PutFile(repo, commit1.ID, "foo", strings.NewReader("foo\n")))
//		require.NoError(t, env.PachClient.PutFile(repo, commit1.ID, "foo", strings.NewReader("foo\n")))
//		require.NoError(t, env.PachClient.FinishCommit(repo, commit1.ID))
//
//		var buffer bytes.Buffer
//		require.NoError(t, env.PachClient.GetFile(repo, commit1.ID, "foo", &buffer))
//		require.Equal(t, "foo\nfoo\n", buffer.String())
//
//		commit2, err := env.PachClient.StartCommitParent(repo, "", commit1.ID)
//		require.NoError(t, err)
//		// file conflicts with the previous commit
//		require.NoError(t, env.PachClient.PutFile(repo, commit2.ID, "foo/bar", strings.NewReader("foo\n")))
//		require.NoError(t, env.PachClient.PutFile(repo, commit2.ID, "/bar", strings.NewReader("bar\n")))
//		require.YesError(t, env.PachClient.FinishCommit(repo, commit2.ID))
//
//		commit2, err = env.PachClient.StartCommitParent(repo, "", commit1.ID)
//		require.NoError(t, err)
//		require.NoError(t, env.PachClient.PutFile(repo, commit2.ID, "/bar", strings.NewReader("bar\n")))
//		require.NoError(t, env.PachClient.FinishCommit(repo, commit2.ID))
//
//		commit3, err := env.PachClient.StartCommitParent(repo, "", commit2.ID)
//		require.NoError(t, err)
//		require.NoError(t, env.PachClient.PutFile(repo, commit3.ID, "dir1/foo", strings.NewReader("foo\n"))) // because the directory dir does not exist
//		require.NoError(t, env.PachClient.FinishCommit(repo, commit3.ID))
//
//		commit4, err := env.PachClient.StartCommitParent(repo, "", commit3.ID)
//		require.NoError(t, err)
//		require.NoError(t, env.PachClient.PutFile(repo, commit4.ID, "dir2/bar", strings.NewReader("bar\n")))
//		require.NoError(t, env.PachClient.FinishCommit(repo, commit4.ID))
//
//		buffer = bytes.Buffer{}
//		require.NoError(t, env.PachClient.GetFile(repo, commit4.ID, "dir2/bar", &buffer))
//		require.Equal(t, "bar\n", buffer.String())
//		buffer = bytes.Buffer{}
//		require.NoError(t, env.PachClient.GetFile(repo, commit4.ID, "dir2", &buffer))
//
//		return nil
//	}))
//}

func TestPutFile2(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))
		commit1, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(repo, commit1.ID, "file", strings.NewReader("foo\n")))
		require.NoError(t, env.PachClient.PutFile(repo, commit1.ID, "file", strings.NewReader("bar\n")))
		require.NoError(t, env.PachClient.PutFile(repo, "master", "file", strings.NewReader("buzz\n")))
		require.NoError(t, env.PachClient.FinishCommit(repo, commit1.ID))

		expected := "foo\nbar\nbuzz\n"
		buffer := &bytes.Buffer{}
		require.NoError(t, env.PachClient.GetFile(repo, commit1.ID, "file", buffer))
		require.Equal(t, expected, buffer.String())
		buffer.Reset()
		require.NoError(t, env.PachClient.GetFile(repo, "master", "file", buffer))
		require.Equal(t, expected, buffer.String())

		commit2, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(repo, commit2.ID, "file", strings.NewReader("foo\n")))
		require.NoError(t, env.PachClient.PutFile(repo, commit2.ID, "file", strings.NewReader("bar\n")))
		require.NoError(t, env.PachClient.PutFile(repo, "master", "file", strings.NewReader("buzz\n")))
		require.NoError(t, env.PachClient.FinishCommit(repo, "master"))

		expected = "foo\nbar\nbuzz\nfoo\nbar\nbuzz\n"
		buffer.Reset()
		require.NoError(t, env.PachClient.GetFile(repo, commit2.ID, "file", buffer))
		require.Equal(t, expected, buffer.String())
		buffer.Reset()
		require.NoError(t, env.PachClient.GetFile(repo, "master", "file", buffer))
		require.Equal(t, expected, buffer.String())

		commit3, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.SetBranch(repo, commit3.ID, "foo"))
		require.NoError(t, env.PachClient.PutFile(repo, "foo", "file", strings.NewReader("foo\nbar\nbuzz\n")))
		require.NoError(t, env.PachClient.FinishCommit(repo, "foo"))

		expected = "foo\nbar\nbuzz\nfoo\nbar\nbuzz\nfoo\nbar\nbuzz\n"
		buffer.Reset()
		require.NoError(t, env.PachClient.GetFile(repo, "foo", "file", buffer))
		require.Equal(t, expected, buffer.String())

		return nil
	}))
}

func TestPutFileOverwrite(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))
		require.NoError(t, env.PachClient.PutFileOverwrite(repo, "master", "file", strings.NewReader("foo")))
		var buf bytes.Buffer
		require.NoError(t, env.PachClient.GetFile(repo, "master", "file", &buf))
		require.Equal(t, "foo", buf.String())
		require.NoError(t, env.PachClient.PutFileOverwrite(repo, "master", "file", strings.NewReader("bar")))
		buf.Reset()
		require.NoError(t, env.PachClient.GetFile(repo, "master", "file", &buf))
		require.Equal(t, "bar", buf.String())
		require.NoError(t, env.PachClient.DeleteFile(repo, "master", "file"))
		require.NoError(t, env.PachClient.PutFile(repo, "master", "file", strings.NewReader("buzz")))
		buf.Reset()
		require.NoError(t, env.PachClient.GetFile(repo, "master", "file", &buf))
		require.Equal(t, "buzz", buf.String())
		return nil
	}))
}

func TestPutFileLongName(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		fileName := `oaidhzoshd<>&><%~$%<#>oandoancoasid1><&%$><%U>oaidhzoshd<>&><%~$%<#>oandoancoasid1><&%$><%U>oaidhzoshd<>&><%~$%<#>oandoancoasid1><&%$><%U>oaidhzoshd<>&><%~$%<#>oandoancoasid1><&%$><%U>oaidhzoshd<>&><%~$%<#>oandoancoasid1><&%$><%U>oaidhzoshd<>&><%~$%<#>oandoancoasid1><&%$><%U>oaidhzoshd<>&><%~$%<#>oandoancoasid1><&%$><%U>oaidhzoshd<>&><%~$%<#>oandoancoasid1><&%$><%U>oaidhzoshd<>&><%~$%<#>oandoancoasid1><&%$><%U>oaidhzoshd<>&><%~$%<#>oandoancoasid1><&%$><%U>oaidhzoshd<>&><%~$%<#>oandoancoasid1><&%$><%U>oaidhzoshd<>&><%~$%<#>oandoancoasid1><&%$><%U>oaidhzoshd<>&><%~$%<#>oandoancoasid1><&%$><%U>oaidhzoshd<>&><%~$%<#>oandoancoasid1><&%$><%U>oaidhzoshd<>&><%~$%<#>oandoancoasid1><&%$><%U>oaidhzoshd<>&><%~$%<#>oandoancoasid1><&%$><%U>oaidhzoshd<>&><%~$%<#>oandoancoasid1><&%$><%U>oaidhzoshd<>&><%~$%<#>oandoancoasid1><&%$><%U>oaidhzoshd<>&><%~$%<#>oandoancoasid1><&%$><%U>oaidhzoshd<>&><%~$%<#>oandoancoasid1><&%$><%U>oaidhzoshd<>&><%~$%<#>oandoancoasid1><&%$><%U>oaidhzoshd<>&><%~$%<#>oandoancoasid1><&%$><%U>oaidhzoshd<>&><%~$%<#>oandoancoasid1><&%$><%U>oaidhzoshd<>&><%~$%<#>oandoancoasid1><&%$><%U>oaidhzoshd<>&><%~$%<#>oandoancoasid1><&%$><%U>oaidhzoshd<>&><%~$%<#>oandoancoasid1><&%$><%U>`

		commit, err := env.PachClient.StartCommit(repo, "")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(repo, commit.ID, fileName, strings.NewReader("foo\n")))
		require.NoError(t, env.PachClient.FinishCommit(repo, commit.ID))

		var buffer bytes.Buffer
		require.NoError(t, env.PachClient.GetFile(repo, commit.ID, fileName, &buffer))
		require.Equal(t, "foo\n", buffer.String())

		return nil
	}))
}

func TestPutSameFileInParallel(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		commit, err := env.PachClient.StartCommit(repo, "")
		require.NoError(t, err)
		var eg errgroup.Group
		for i := 0; i < 3; i++ {
			eg.Go(func() error {
				return env.PachClient.PutFile(repo, commit.ID, "foo", strings.NewReader("foo\n"))
			})
		}
		require.NoError(t, eg.Wait())
		require.NoError(t, env.PachClient.FinishCommit(repo, commit.ID))

		var buffer bytes.Buffer
		require.NoError(t, env.PachClient.GetFile(repo, commit.ID, "foo", &buffer))
		require.Equal(t, "foo\nfoo\nfoo\n", buffer.String())

		return nil
	}))
}

func TestInspectFile(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		fileContent1 := "foo\n"
		commit1, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(repo, commit1.ID, "foo", strings.NewReader(fileContent1)))

		require.NoError(t, env.PachClient.FinishCommit(repo, commit1.ID))

		fileInfo, err := env.PachClient.InspectFile(repo, commit1.ID, "foo")
		require.NoError(t, err)
		require.Equal(t, pfs.FileType_FILE, fileInfo.FileType)
		require.Equal(t, len(fileContent1), int(fileInfo.SizeBytes))

		fileContent2 := "barbar\n"
		commit2, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(repo, commit2.ID, "foo", strings.NewReader(fileContent2)))

		require.NoError(t, env.PachClient.FinishCommit(repo, commit2.ID))

		fileInfo, err = env.PachClient.InspectFile(repo, commit2.ID, "foo")
		require.NoError(t, err)
		require.Equal(t, pfs.FileType_FILE, fileInfo.FileType)
		require.Equal(t, len(fileContent1+fileContent2), int(fileInfo.SizeBytes))

		fileInfo, err = env.PachClient.InspectFile(repo, commit2.ID, "foo")
		require.NoError(t, err)
		require.Equal(t, pfs.FileType_FILE, fileInfo.FileType)
		require.Equal(t, len(fileContent1)+len(fileContent2), int(fileInfo.SizeBytes))

		fileContent3 := "bar\n"
		commit3, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(repo, commit3.ID, "bar", strings.NewReader(fileContent3)))
		require.NoError(t, env.PachClient.FinishCommit(repo, commit3.ID))

		fis, err := env.PachClient.ListFileAll(repo, commit3.ID, "")
		require.NoError(t, err)
		require.Equal(t, 2, len(fis))

		require.Equal(t, len(fis), 2)

		return nil
	}))
}

func TestInspectFile2(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		fileContent1 := "foo\n"
		fileContent2 := "buzz\n"

		_, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(repo, "master", "file", strings.NewReader(fileContent1)))
		require.NoError(t, env.PachClient.FinishCommit(repo, "master"))

		fileInfo, err := env.PachClient.InspectFile(repo, "master", "/file")
		require.NoError(t, err)
		require.Equal(t, len(fileContent1), int(fileInfo.SizeBytes))
		require.Equal(t, "/file", fileInfo.File.Path)
		require.Equal(t, pfs.FileType_FILE, fileInfo.FileType)

		_, err = env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(repo, "master", "file", strings.NewReader(fileContent1)))
		require.NoError(t, env.PachClient.FinishCommit(repo, "master"))

		fileInfo, err = env.PachClient.InspectFile(repo, "master", "file")
		require.NoError(t, err)
		require.Equal(t, len(fileContent1)*2, int(fileInfo.SizeBytes))
		require.Equal(t, "/file", fileInfo.File.Path)

		_, err = env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.DeleteFile(repo, "master", "file"))
		require.NoError(t, env.PachClient.PutFile(repo, "master", "file", strings.NewReader(fileContent2)))
		require.NoError(t, env.PachClient.FinishCommit(repo, "master"))

		fileInfo, err = env.PachClient.InspectFile(repo, "master", "file")
		require.NoError(t, err)
		require.Equal(t, len(fileContent2), int(fileInfo.SizeBytes))

		return nil
	}))
}

func TestInspectFile3(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		repo := "TestInspectFile3"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		fileContent1 := "foo\n"
		commit1, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(repo, commit1.ID, "foo/bar", strings.NewReader(fileContent1)))
		// TODO: can't read uncommitted filesets yet.
		// fileInfo, err := env.PachClient.InspectFileV2(ctx, &pfs.InspectFileRequest{
		// 	File: &pfs.File{
		// 		Commit: commit1,
		// 		Path:   "foo",
		// 	},
		// })
		// require.NoError(t, err)
		// require.NotNil(t, fileInfo)

		require.NoError(t, env.PachClient.FinishCommit(repo, commit1.ID))

		fi, err := env.PachClient.InspectFile(repo, commit1.ID, "foo/bar")
		require.NoError(t, err)
		require.NotNil(t, fi)

		fileContent2 := "barbar\n"
		commit2, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(repo, commit2.ID, "foo", strings.NewReader(fileContent2)))

		// TODO: can't read uncommitted filesets yet.
		// fileInfo, err = env.PachClient.InspectFileV2(ctx, &pfs.InspectFileRequest{
		// 	File: &pfs.File{
		// 		Commit: commit2,
		// 		Path:   "foo",
		// 	},
		// })
		// require.NoError(t, err)
		// require.NotNil(t, fileInfo)

		require.NoError(t, env.PachClient.FinishCommit(repo, commit2.ID))

		fi, err = env.PachClient.InspectFile(repo, commit2.ID, "foo")
		require.NoError(t, err)
		require.NotNil(t, fi)

		fileContent3 := "bar\n"
		commit3, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(repo, commit3.ID, "bar", strings.NewReader(fileContent3)))
		require.NoError(t, env.PachClient.FinishCommit(repo, commit3.ID))
		fi, err = env.PachClient.InspectFile(repo, commit3.ID, "bar")
		require.NoError(t, err)
		require.NotNil(t, fi)
		return nil
	}))
}

func TestInspectDir(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		commit1, err := env.PachClient.StartCommit(repo, "")
		require.NoError(t, err)

		fileContent := "foo\n"
		require.NoError(t, env.PachClient.PutFile(repo, commit1.ID, "dir/foo", strings.NewReader(fileContent)))

		require.NoError(t, env.PachClient.FinishCommit(repo, commit1.ID))

		fileInfo, err := env.PachClient.InspectFile(repo, commit1.ID, "dir/foo")
		require.NoError(t, err)
		require.Equal(t, len(fileContent), int(fileInfo.SizeBytes))
		require.Equal(t, pfs.FileType_FILE, fileInfo.FileType)

		fileInfo, err = env.PachClient.InspectFile(repo, commit1.ID, "dir")
		require.NoError(t, err)
		require.Equal(t, len(fileContent), int(fileInfo.SizeBytes))
		require.Equal(t, pfs.FileType_DIR, fileInfo.FileType)

		_, err = env.PachClient.InspectFile(repo, commit1.ID, "")
		require.NoError(t, err)
		require.Equal(t, len(fileContent), int(fileInfo.SizeBytes))
		require.Equal(t, pfs.FileType_DIR, fileInfo.FileType)

		return nil
	}))
}

func TestInspectDir2(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		fileContent := "foo\n"

		_, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(repo, "master", "dir/1", strings.NewReader(fileContent)))
		require.NoError(t, env.PachClient.PutFile(repo, "master", "dir/2", strings.NewReader(fileContent)))

		require.NoError(t, env.PachClient.FinishCommit(repo, "master"))

		fileInfo, err := env.PachClient.InspectFile(repo, "master", "/dir")
		require.NoError(t, err)
		require.Equal(t, "/dir/", fileInfo.File.Path)
		require.Equal(t, pfs.FileType_DIR, fileInfo.FileType)

		_, err = env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(repo, "master", "dir/3", strings.NewReader(fileContent)))

		require.NoError(t, env.PachClient.FinishCommit(repo, "master"))

		_, err = env.PachClient.InspectFile(repo, "master", "dir")
		require.NoError(t, err)

		_, err = env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		err = env.PachClient.DeleteFile(repo, "master", "dir/2")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit(repo, "master"))

		_, err = env.PachClient.InspectFile(repo, "master", "dir")
		require.NoError(t, err)

		return nil
	}))
}

func TestListFileTwoCommits(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		numFiles := 5

		commit1, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)

		for i := 0; i < numFiles; i++ {
			require.NoError(t, env.PachClient.PutFile(repo, commit1.ID, fmt.Sprintf("file%d", i), strings.NewReader("foo\n")))
		}

		require.NoError(t, env.PachClient.FinishCommit(repo, commit1.ID))

		fis, err := env.PachClient.ListFileAll(repo, "master", "")
		require.NoError(t, err)
		require.Equal(t, numFiles, len(fis))

		commit2, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)

		for i := 0; i < numFiles; i++ {
			require.NoError(t, env.PachClient.PutFile(repo, commit2.ID, fmt.Sprintf("file2-%d", i), strings.NewReader("foo\n")))
		}

		require.NoError(t, env.PachClient.FinishCommit(repo, commit2.ID))

		fis, err = env.PachClient.ListFileAll(repo, commit2.ID, "")
		require.NoError(t, err)
		require.Equal(t, 2*numFiles, len(fis))

		fis, err = env.PachClient.ListFileAll(repo, commit1.ID, "")
		require.NoError(t, err)
		require.Equal(t, numFiles, len(fis))

		fis, err = env.PachClient.ListFileAll(repo, commit2.ID, "")
		require.NoError(t, err)
		require.Equal(t, 2*numFiles, len(fis))

		return nil
	}))
}

func TestListFile(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		commit, err := env.PachClient.StartCommit(repo, "")
		require.NoError(t, err)

		fileContent1 := "foo\n"
		require.NoError(t, env.PachClient.PutFile(repo, commit.ID, "dir/foo", strings.NewReader(fileContent1)))

		fileContent2 := "bar\n"
		require.NoError(t, env.PachClient.PutFile(repo, commit.ID, "dir/bar", strings.NewReader(fileContent2)))

		require.NoError(t, env.PachClient.FinishCommit(repo, commit.ID))

		fileInfos, err := env.PachClient.ListFileAll(repo, commit.ID, "dir")
		require.NoError(t, err)
		require.Equal(t, 2, len(fileInfos))
		require.True(t, fileInfos[0].File.Path == "/dir/foo" && fileInfos[1].File.Path == "/dir/bar" || fileInfos[0].File.Path == "/dir/bar" && fileInfos[1].File.Path == "/dir/foo")
		require.True(t, fileInfos[0].SizeBytes == fileInfos[1].SizeBytes && fileInfos[0].SizeBytes == uint64(len(fileContent1)))

		return nil
	}))
}

func TestListFile2(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		fileContent := "foo\n"

		_, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(repo, "master", "dir/1", strings.NewReader(fileContent)))
		require.NoError(t, env.PachClient.PutFile(repo, "master", "dir/2", strings.NewReader(fileContent)))
		require.NoError(t, err)

		require.NoError(t, env.PachClient.FinishCommit(repo, "master"))

		fileInfos, err := env.PachClient.ListFileAll(repo, "master", "dir")
		require.NoError(t, err)
		require.Equal(t, 2, len(fileInfos))

		_, err = env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(repo, "master", "dir/3", strings.NewReader(fileContent)))
		require.NoError(t, env.PachClient.FinishCommit(repo, "master"))

		fileInfos, err = env.PachClient.ListFileAll(repo, "master", "dir")
		require.NoError(t, err)
		require.Equal(t, 3, len(fileInfos))

		_, err = env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		err = env.PachClient.DeleteFile(repo, "master", "dir/2")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit(repo, "master"))

		fileInfos, err = env.PachClient.ListFileAll(repo, "master", "dir")
		require.NoError(t, err)
		require.Equal(t, 2, len(fileInfos))

		return nil
	}))
}

func TestListFile3(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		fileContent := "foo\n"

		_, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(repo, "master", "dir/1", strings.NewReader(fileContent)))
		require.NoError(t, env.PachClient.PutFile(repo, "master", "dir/2", strings.NewReader(fileContent)))
		require.NoError(t, env.PachClient.FinishCommit(repo, "master"))

		fileInfos, err := env.PachClient.ListFileAll(repo, "master", "dir")
		require.NoError(t, err)
		require.Equal(t, 2, len(fileInfos))

		_, err = env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(repo, "master", "dir/3/foo", strings.NewReader(fileContent)))
		require.NoError(t, env.PachClient.PutFile(repo, "master", "dir/3/bar", strings.NewReader(fileContent)))
		require.NoError(t, env.PachClient.FinishCommit(repo, "master"))

		fileInfos, err = env.PachClient.ListFileAll(repo, "master", "dir")
		require.NoError(t, err)
		require.Equal(t, 3, len(fileInfos))
		require.Equal(t, int(fileInfos[2].SizeBytes), len(fileContent)*2)

		_, err = env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		err = env.PachClient.DeleteFile(repo, "master", "dir/3/bar")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit(repo, "master"))

		fileInfos, err = env.PachClient.ListFileAll(repo, "master", "dir")
		require.NoError(t, err)
		require.Equal(t, 3, len(fileInfos))
		require.Equal(t, int(fileInfos[2].SizeBytes), len(fileContent))

		_, err = env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(repo, "master", "file", strings.NewReader(fileContent)))
		require.NoError(t, env.PachClient.FinishCommit(repo, "master"))

		fileInfos, err = env.PachClient.ListFileAll(repo, "master", "/")
		require.NoError(t, err)
		require.Equal(t, 2, len(fileInfos))

		return nil
	}))
}

func TestListFile4(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		repo := "TestListFile"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		commit1, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)

		require.NoError(t, env.PachClient.PutFile(repo, commit1.ID, "/dir1/file1.1", &bytes.Buffer{}))
		require.NoError(t, env.PachClient.PutFile(repo, commit1.ID, "/dir1/file1.2", &bytes.Buffer{}))
		require.NoError(t, env.PachClient.PutFile(repo, commit1.ID, "/dir2/file2.1", &bytes.Buffer{}))
		require.NoError(t, env.PachClient.PutFile(repo, commit1.ID, "/dir2/file2.2", &bytes.Buffer{}))

		require.NoError(t, env.PachClient.FinishCommit(repo, commit1.ID))
		// should list a directory but not siblings
		var fis []*pfs.FileInfo
		require.NoError(t, env.PachClient.ListFile(repo, commit1.ID, "/dir1", func(fi *pfs.FileInfo) error {
			fis = append(fis, fi)
			return nil
		}))
		require.ElementsEqual(t, []string{"/dir1/file1.1", "/dir1/file1.2"}, finfosToPaths(fis))
		// should list the root
		fis = nil
		require.NoError(t, env.PachClient.ListFile(repo, commit1.ID, "/", func(fi *pfs.FileInfo) error {
			fis = append(fis, fi)
			return nil
		}))
		require.ElementsEqual(t, []string{"/dir1/", "/dir2/"}, finfosToPaths(fis))

		return nil
	}))
}

// TODO: Make work with V2?
//func TestPutFileTypeConflict(t *testing.T) {
//	t.Parallel()
//	db := dbutil.NewTestDB(t)
//	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
//		repo := "test"
//		require.NoError(t, env.PachClient.CreateRepo(repo))
//
//		fileContent := "foo\n"
//
//		commit1, err := env.PachClient.StartCommit(repo, "master")
//		require.NoError(t, err)
//		require.NoError(t, env.PachClient.PutFile(repo, commit1.ID, "dir/1", strings.NewReader(fileContent)))
//		require.NoError(t, env.PachClient.FinishCommit(repo, commit1.ID))
//
//		commit2, err := env.PachClient.StartCommit(repo, "master")
//		require.NoError(t, err)
//		require.NoError(t, env.PachClient.PutFile(repo, commit2.ID, "dir", strings.NewReader(fileContent)))
//		require.YesError(t, env.PachClient.FinishCommit(repo, commit2.ID))
//
//		return nil
//	}))
//}

func TestRootDirectory(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		fileContent := "foo\n"

		commit, err := env.PachClient.StartCommit(repo, "")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(repo, commit.ID, "foo", strings.NewReader(fileContent)))

		require.NoError(t, env.PachClient.FinishCommit(repo, commit.ID))

		fileInfos, err := env.PachClient.ListFileAll(repo, commit.ID, "")
		require.NoError(t, err)
		require.Equal(t, 1, len(fileInfos))

		return nil
	}))
}

func TestDeleteFile(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		// Commit 1: Add two files; delete one file within the commit
		commit1, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)

		fileContent1 := "foo\n"
		require.NoError(t, env.PachClient.PutFile(repo, commit1.ID, "foo", strings.NewReader(fileContent1)))

		fileContent2 := "bar\n"
		require.NoError(t, env.PachClient.PutFile(repo, commit1.ID, "bar", strings.NewReader(fileContent2)))

		require.NoError(t, env.PachClient.DeleteFile(repo, commit1.ID, "foo"))

		require.NoError(t, env.PachClient.FinishCommit(repo, commit1.ID))

		_, err = env.PachClient.InspectFile(repo, commit1.ID, "foo")
		require.YesError(t, err)

		// Should see one file
		fileInfos, err := env.PachClient.ListFileAll(repo, commit1.ID, "")
		require.NoError(t, err)
		require.Equal(t, 1, len(fileInfos))

		// Deleting a file in a finished commit should result in an error
		require.YesError(t, env.PachClient.DeleteFile(repo, commit1.ID, "bar"))

		// Empty commit
		commit2, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit(repo, commit2.ID))

		// Should still see one files
		fileInfos, err = env.PachClient.ListFileAll(repo, commit2.ID, "")
		require.NoError(t, err)
		require.Equal(t, 1, len(fileInfos))

		// Delete bar
		commit3, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.DeleteFile(repo, commit3.ID, "bar"))

		require.NoError(t, env.PachClient.FinishCommit(repo, commit3.ID))

		// Should see no file
		fileInfos, err = env.PachClient.ListFileAll(repo, commit3.ID, "")
		require.NoError(t, err)
		require.Equal(t, 0, len(fileInfos))

		_, err = env.PachClient.InspectFile(repo, commit3.ID, "bar")
		require.YesError(t, err)

		// Delete a nonexistent file; it should be no-op
		commit4, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.DeleteFile(repo, commit4.ID, "nonexistent"))
		require.NoError(t, env.PachClient.FinishCommit(repo, commit4.ID))

		return nil
	}))
}

func TestDeleteFile2(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		commit1, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(repo, commit1.ID, "file", strings.NewReader("foo\n")))
		require.NoError(t, env.PachClient.FinishCommit(repo, commit1.ID))

		commit2, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		err = env.PachClient.DeleteFile(repo, commit2.ID, "file")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(repo, commit2.ID, "file", strings.NewReader("bar\n")))
		require.NoError(t, env.PachClient.FinishCommit(repo, commit2.ID))

		expected := "bar\n"
		var buffer bytes.Buffer
		require.NoError(t, env.PachClient.GetFile(repo, "master", "file", &buffer))
		require.Equal(t, expected, buffer.String())

		commit3, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(repo, commit3.ID, "file", strings.NewReader("buzz\n")))
		err = env.PachClient.DeleteFile(repo, commit3.ID, "file")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(repo, commit3.ID, "file", strings.NewReader("foo\n")))
		require.NoError(t, env.PachClient.FinishCommit(repo, commit3.ID))

		expected = "foo\n"
		buffer.Reset()
		require.NoError(t, env.PachClient.GetFile(repo, commit3.ID, "file", &buffer))
		require.Equal(t, expected, buffer.String())

		return nil
	}))
}

func TestDeleteFile3(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))
		commit1, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		fileContent := "bar\n"
		require.NoError(t, env.PachClient.PutFileOverwrite(repo, commit1.ID, "/bar", strings.NewReader(fileContent)))
		require.NoError(t, env.PachClient.PutFileOverwrite(repo, commit1.ID, "/dir1/dir2/bar", strings.NewReader(fileContent)))
		require.NoError(t, env.PachClient.FinishCommit(repo, commit1.ID))

		commit2, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.DeleteFile(repo, commit2.ID, "/"))
		require.NoError(t, env.PachClient.PutFileOverwrite(repo, commit2.ID, "/bar", strings.NewReader(fileContent)))
		require.NoError(t, env.PachClient.PutFileOverwrite(repo, commit2.ID, "/dir1/bar", strings.NewReader(fileContent)))
		require.NoError(t, env.PachClient.PutFileOverwrite(repo, commit2.ID, "/dir1/dir2/bar", strings.NewReader(fileContent)))
		require.NoError(t, env.PachClient.PutFileOverwrite(repo, commit2.ID, "/dir1/dir2/barbar", strings.NewReader(fileContent)))
		require.NoError(t, env.PachClient.FinishCommit(repo, commit2.ID))

		commit3, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.DeleteFile(repo, commit3.ID, "/dir1/dir2/"))
		require.NoError(t, env.PachClient.FinishCommit(repo, commit3.ID))

		_, err = env.PachClient.InspectFile(repo, commit3.ID, "/dir1")
		require.NoError(t, err)
		_, err = env.PachClient.InspectFile(repo, commit3.ID, "/dir1/bar")
		require.NoError(t, err)
		_, err = env.PachClient.InspectFile(repo, commit3.ID, "/dir1/dir2")
		require.YesError(t, err)
		_, err = env.PachClient.InspectFile(repo, commit3.ID, "/dir1/dir2/bar")
		require.YesError(t, err)
		_, err = env.PachClient.InspectFile(repo, commit3.ID, "/dir1/dir2/barbar")
		require.YesError(t, err)

		commit4, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFileOverwrite(repo, commit4.ID, "/dir1/dir2/bar", strings.NewReader(fileContent)))
		require.NoError(t, env.PachClient.FinishCommit(repo, commit4.ID))

		_, err = env.PachClient.InspectFile(repo, commit4.ID, "/dir1")
		require.NoError(t, err)
		_, err = env.PachClient.InspectFile(repo, commit4.ID, "/dir1/bar")
		require.NoError(t, err)
		_, err = env.PachClient.InspectFile(repo, commit4.ID, "/dir1/dir2")
		require.NoError(t, err)
		_, err = env.PachClient.InspectFile(repo, commit4.ID, "/dir1/dir2/bar")
		require.NoError(t, err)

		return nil
	}))
}

func TestDeleteDir(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		// Commit 1: Add two files into the same directory; delete the directory
		commit1, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)

		require.NoError(t, env.PachClient.PutFile(repo, commit1.ID, "dir/foo", strings.NewReader("foo1")))

		require.NoError(t, env.PachClient.PutFile(repo, commit1.ID, "dir/bar", strings.NewReader("bar1")))

		require.NoError(t, env.PachClient.DeleteFile(repo, commit1.ID, "/dir/"))

		require.NoError(t, env.PachClient.FinishCommit(repo, commit1.ID))

		fileInfos, err := env.PachClient.ListFileAll(repo, commit1.ID, "")
		require.NoError(t, err)
		require.Equal(t, 0, len(fileInfos))

		// dir should not exist
		_, err = env.PachClient.InspectFile(repo, commit1.ID, "dir")
		require.YesError(t, err)

		// Commit 2: Delete the directory and add the same two files
		// The two files should reflect the new content
		commit2, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)

		require.NoError(t, env.PachClient.PutFile(repo, commit2.ID, "dir/foo", strings.NewReader("foo2")))

		require.NoError(t, env.PachClient.PutFile(repo, commit2.ID, "dir/bar", strings.NewReader("bar2")))

		require.NoError(t, env.PachClient.FinishCommit(repo, commit2.ID))

		// Should see two files
		fileInfos, err = env.PachClient.ListFileAll(repo, commit2.ID, "dir")
		require.NoError(t, err)
		require.Equal(t, 2, len(fileInfos))

		var buffer bytes.Buffer
		require.NoError(t, env.PachClient.GetFile(repo, commit2.ID, "dir/foo", &buffer))
		require.Equal(t, "foo2", buffer.String())

		var buffer2 bytes.Buffer
		require.NoError(t, env.PachClient.GetFile(repo, commit2.ID, "dir/bar", &buffer2))
		require.Equal(t, "bar2", buffer2.String())

		// Commit 3: delete the directory
		commit3, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)

		require.NoError(t, env.PachClient.DeleteFile(repo, commit3.ID, "/dir/"))

		require.NoError(t, env.PachClient.FinishCommit(repo, commit3.ID))

		// Should see zero files
		fileInfos, err = env.PachClient.ListFileAll(repo, commit3.ID, "")
		require.NoError(t, err)
		require.Equal(t, 0, len(fileInfos))

		// TODO: test deleting "."

		return nil
	}))
}

func TestListCommit(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		numCommits := 10

		var midCommitID string
		for i := 0; i < numCommits; i++ {
			commit, err := env.PachClient.StartCommit(repo, "master")
			require.NoError(t, err)
			require.NoError(t, env.PachClient.FinishCommit(repo, "master"))
			if i == numCommits/2 {
				midCommitID = commit.ID
			}
		}

		// list all commits
		commitInfos, err := env.PachClient.ListCommit(repo, "", "", 0)
		require.NoError(t, err)
		require.Equal(t, numCommits, len(commitInfos))

		// Test that commits are sorted in newest-first order
		for i := 0; i < len(commitInfos)-1; i++ {
			require.Equal(t, commitInfos[i].ParentCommit, commitInfos[i+1].Commit)
		}

		// Now list all commits up to the last commit
		commitInfos, err = env.PachClient.ListCommit(repo, "master", "", 0)
		require.NoError(t, err)
		require.Equal(t, numCommits, len(commitInfos))

		// Test that commits are sorted in newest-first order
		for i := 0; i < len(commitInfos)-1; i++ {
			require.Equal(t, commitInfos[i].ParentCommit, commitInfos[i+1].Commit)
		}

		// Now list all commits up to the mid commit, excluding the mid commit
		// itself
		commitInfos, err = env.PachClient.ListCommit(repo, "master", midCommitID, 0)
		require.NoError(t, err)
		require.Equal(t, numCommits-numCommits/2-1, len(commitInfos))

		// Test that commits are sorted in newest-first order
		for i := 0; i < len(commitInfos)-1; i++ {
			require.Equal(t, commitInfos[i].ParentCommit, commitInfos[i+1].Commit)
		}

		// list commits by branch
		commitInfos, err = env.PachClient.ListCommit(repo, "master", "", 0)
		require.NoError(t, err)
		require.Equal(t, numCommits, len(commitInfos))

		// Test that commits are sorted in newest-first order
		for i := 0; i < len(commitInfos)-1; i++ {
			require.Equal(t, commitInfos[i].ParentCommit, commitInfos[i+1].Commit)
		}

		// Try listing the commits in reverse order
		commitInfos = nil
		require.NoError(t, env.PachClient.ListCommitF(repo, "", "", 0, true, func(ci *pfs.CommitInfo) error {
			commitInfos = append(commitInfos, ci)
			return nil
		}))
		for i := 1; i < len(commitInfos); i++ {
			require.Equal(t, commitInfos[i].ParentCommit, commitInfos[i-1].Commit)
		}

		return nil
	}))
}

// TODO: Make work with V2?
//func TestOffsetRead(t *testing.T) {
//	t.Parallel()
//	db := dbutil.NewTestDB(t)
//	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
//		repo := "TestOffsetRead"
//		require.NoError(t, env.PachClient.CreateRepo(repo))
//		commit, err := env.PachClient.StartCommit(repo, "")
//		require.NoError(t, err)
//		fileData := "foo\n"
//		require.NoError(t, env.PachClient.PutFile(repo, commit.ID, "foo", strings.NewReader(fileData)))
//		require.NoError(t, env.PachClient.PutFile(repo, commit.ID, "foo", strings.NewReader(fileData)))
//
//		var buffer bytes.Buffer
//		require.NoError(t, env.PachClient.GetFile(repo, commit.ID, "foo", int64(len(fileData)*2)+1, 0, &buffer))
//		require.Equal(t, "", buffer.String())
//
//		require.NoError(t, env.PachClient.FinishCommit(repo, commit.ID))
//
//		buffer.Reset()
//		require.NoError(t, env.PachClient.GetFile(repo, commit.ID, "foo", int64(len(fileData)*2)+1, 0, &buffer))
//		require.Equal(t, "", buffer.String())
//
//		return nil
//	}))
//}

func TestBranch2(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		commit, err := env.PachClient.StartCommit(repo, "")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit(repo, commit.ID))

		expectedBranches := []string{"branch1", "branch2", "branch3"}
		for _, branch := range expectedBranches {
			require.NoError(t, env.PachClient.SetBranch(repo, commit.ID, branch))
		}

		branches, err := env.PachClient.ListBranch(repo)
		require.NoError(t, err)
		require.Equal(t, len(expectedBranches), len(branches))
		for i, branch := range branches {
			// branches should return in newest-first order
			require.Equal(t, expectedBranches[len(branches)-i-1], branch.Name)
			require.Equal(t, commit, branch.Head)
		}

		commit2, err := env.PachClient.StartCommit(repo, "branch1")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit(repo, "branch1"))

		commit2Info, err := env.PachClient.InspectCommit(repo, "branch1")
		require.NoError(t, err)
		require.Equal(t, commit, commit2Info.ParentCommit)

		// delete the last branch
		lastBranch := expectedBranches[len(expectedBranches)-1]
		require.NoError(t, env.PachClient.DeleteBranch(repo, lastBranch, false))
		branches, err = env.PachClient.ListBranch(repo)
		require.NoError(t, err)
		require.Equal(t, 2, len(branches))
		require.Equal(t, "branch2", branches[0].Name)
		require.Equal(t, commit, branches[0].Head)
		require.Equal(t, "branch1", branches[1].Name)
		require.Equal(t, commit2, branches[1].Head)

		return nil
	}))
}

func TestDeleteNonexistentBranch(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		repo := "TestDeleteNonexistentBranch"
		require.NoError(t, env.PachClient.CreateRepo(repo))
		require.NoError(t, env.PachClient.DeleteBranch(repo, "doesnt_exist", false))

		return nil
	}))
}

func TestSubscribeCommit(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		numCommits := 10

		// create some commits that shouldn't affect the below SubscribeCommit call
		// reproduces #2469
		for i := 0; i < numCommits; i++ {
			commit, err := env.PachClient.StartCommit(repo, "master-v1")
			require.NoError(t, err)
			require.NoError(t, env.PachClient.FinishCommit(repo, commit.ID))
		}

		var commits []*pfs.Commit
		for i := 0; i < numCommits; i++ {
			commit, err := env.PachClient.StartCommit(repo, "master")
			require.NoError(t, err)
			require.NoError(t, env.PachClient.FinishCommit(repo, commit.ID))
			commits = append(commits, commit)
		}

		commitIter, err := env.PachClient.SubscribeCommit(repo, "master", nil, "", pfs.CommitState_STARTED)
		require.NoError(t, err)
		for i := 0; i < numCommits; i++ {
			commitInfo, err := commitIter.Next()
			require.NoError(t, err)
			require.Equal(t, commits[i], commitInfo.Commit)
		}

		// Create another batch of commits
		commits = nil
		for i := 0; i < numCommits; i++ {
			commit, err := env.PachClient.StartCommit(repo, "master")
			require.NoError(t, err)
			require.NoError(t, env.PachClient.FinishCommit(repo, "master"))
			commits = append(commits, commit)
		}

		for i := 0; i < numCommits; i++ {
			commitInfo, err := commitIter.Next()
			require.NoError(t, err)
			require.Equal(t, commits[i], commitInfo.Commit)
		}

		commitIter.Close()

		return nil
	}))
}

func TestInspectRepoSimple(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		commit, err := env.PachClient.StartCommit(repo, "")
		require.NoError(t, err)

		file1Content := "foo\n"
		require.NoError(t, env.PachClient.PutFile(repo, commit.ID, "foo", strings.NewReader(file1Content)))

		file2Content := "bar\n"
		require.NoError(t, env.PachClient.PutFile(repo, commit.ID, "bar", strings.NewReader(file2Content)))

		require.NoError(t, env.PachClient.FinishCommit(repo, commit.ID))

		info, err := env.PachClient.InspectRepo(repo)
		require.NoError(t, err)

		// Size should be 0 because the files were not added to master
		require.Equal(t, int(info.SizeBytes), 0)

		return nil
	}))
}

func TestInspectRepoComplex(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
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

			require.NoError(t, env.PachClient.PutFile(repo, commit.ID, fileName, strings.NewReader(fileContent)))

		}

		require.NoError(t, env.PachClient.FinishCommit(repo, commit.ID))

		info, err := env.PachClient.InspectRepo(repo)
		require.NoError(t, err)

		require.Equal(t, int(info.SizeBytes), totalSize)

		infos, err := env.PachClient.ListRepo()
		require.NoError(t, err)
		require.Equal(t, 1, len(infos))
		info = infos[0]

		require.Equal(t, int(info.SizeBytes), totalSize)

		return nil
	}))
}

// TODO: Make work with V2?
//func TestCreate(t *testing.T) {
//	t.Parallel()
//	db := dbutil.NewTestDB(t)
//	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
//		repo := "test"
//		require.NoError(t, env.PachClient.CreateRepo(repo))
//		commit, err := env.PachClient.StartCommit(repo, "")
//		require.NoError(t, err)
//		w, err := env.PachClient.PutFileSplitWriter(repo, commit.ID, "foo", pfs.Delimiter_NONE, 0, 0, 0, false)
//		require.NoError(t, err)
//		require.NoError(t, w.Close())
//		require.NoError(t, env.PachClient.FinishCommit(repo, commit.ID))
//		_, err = env.PachClient.InspectFile(repo, commit.ID, "foo")
//		require.NoError(t, err)
//
//		return nil
//	})
//	require.NoError(t, err)
//}

func TestGetFile(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		repo := tu.UniqueString("test")
		require.NoError(t, env.PachClient.CreateRepo(repo))
		commit, err := env.PachClient.StartCommit(repo, "")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(repo, commit.ID, "dir/file", strings.NewReader("foo\n")))
		require.NoError(t, env.PachClient.FinishCommit(repo, commit.ID))
		var buffer bytes.Buffer
		require.NoError(t, env.PachClient.GetFile(repo, commit.ID, "dir/file", &buffer))
		require.Equal(t, "foo\n", buffer.String())
		t.Run("InvalidCommit", func(t *testing.T) {
			buffer = bytes.Buffer{}
			err = env.PachClient.GetFile(repo, "aninvalidcommitid", "dir/file", &buffer)
			require.YesError(t, err)
		})
		t.Run("Directory", func(t *testing.T) {
			buffer = bytes.Buffer{}
			err = env.PachClient.GetFile(repo, commit.ID, "dir", &buffer)
			require.NoError(t, err)
		})

		return nil
	}))
}

func TestManyPutsSingleFileSingleCommit(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		if testing.Short() {
			t.Skip("Skipping long tests in short mode")
		}
		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		commit1, err := env.PachClient.StartCommit(repo, "")
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
					if err := env.PachClient.PutFile(repo, commit1.ID, "foo", strings.NewReader(rawMessage)); err != nil {
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
		require.NoError(t, env.PachClient.FinishCommit(repo, commit1.ID))

		var buffer bytes.Buffer
		require.NoError(t, env.PachClient.GetFile(repo, commit1.ID, "foo", &buffer))
		require.Equal(t, string(expectedOutput), buffer.String())

		return nil
	}))
}

// TODO: Make work with V2?
//func TestPutFileValidCharacters(t *testing.T) {
//	t.Parallel()
//	db := dbutil.NewTestDB(t)
//	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
//		repo := "test"
//		require.NoError(t, env.PachClient.CreateRepo(repo))
//
//		commit, err := env.PachClient.StartCommit(repo, "")
//		require.NoError(t, err)
//
//		require.NoError(t, env.PachClient.PutFile(repo, commit.ID, "foo\x00bar", strings.NewReader("foobar\n")))
//		// null characters error because when you `ls` files with null characters
//		// they truncate things after the null character leading to strange results
//		require.YesError(t, err)
//
//		// Boundary tests for valid character range
//		require.YesError(t, env.PachClient.PutFile(repo, commit.ID, "\x1ffoobar", strings.NewReader("foobar\n")))
//		require.NoError(t, env.PachClient.PutFile(repo, commit.ID, "foo\x20bar", strings.NewReader("foobar\n")))
//		require.NoError(t, env.PachClient.PutFile(repo, commit.ID, "foobar\x7e", strings.NewReader("foobar\n")))
//		require.YesError(t, env.PachClient.PutFile(repo, commit.ID, "foo\x7fbar", strings.NewReader("foobar\n")))
//
//		// Random character tests outside and inside valid character range
//		require.YesError(t, env.PachClient.PutFile(repo, commit.ID, "foobar\x0b", strings.NewReader("foobar\n")))
//		require.NoError(t, env.PachClient.PutFile(repo, commit.ID, "\x41foobar", strings.NewReader("foobar\n")))
//		require.YesError(t, env.PachClient.PutFile(repo, commit.ID, "foo\x90bar", strings.NewReader("foobar\n")))
//
//		return nil
//	}))
//}

func TestPutFileURL(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		if testing.Short() {
			t.Skip("Skipping integration tests in short mode")
		}

		repo := "TestPutFileURL"
		require.NoError(t, env.PachClient.CreateRepo(repo))
		commit, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFileURL(repo, commit.ID, "readme", "https://raw.githubusercontent.com/pachyderm/pachyderm/master/README.md", false, false))
		require.NoError(t, env.PachClient.FinishCommit(repo, commit.ID))
		fileInfo, err := env.PachClient.InspectFile(repo, commit.ID, "readme")
		require.NoError(t, err)
		require.True(t, fileInfo.SizeBytes > 0)

		return nil
	}))
}

// TODO: Make work with V2?
//func TestBigListFile(t *testing.T) {
//	t.Parallel()
//	db := dbutil.NewTestDB(t)
//	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
//		repo := "TestBigListFile"
//		require.NoError(t, env.PachClient.CreateRepo(repo))
//		commit, err := env.PachClient.StartCommit(repo, "")
//		require.NoError(t, err)
//		var eg errgroup.Group
//		for i := 0; i < 25; i++ {
//			for j := 0; j < 25; j++ {
//				i := i
//				j := j
//				eg.Go(func() error {
//					return env.PachClient.PutFile(repo, commit.ID, fmt.Sprintf("dir%d/file%d", i, j), strings.NewReader("foo\n"))
//				})
//			}
//		}
//		require.NoError(t, eg.Wait())
//		require.NoError(t, env.PachClient.FinishCommit(repo, commit.ID))
//		for i := 0; i < 25; i++ {
//			files, err := env.PachClient.ListFileAll(repo, commit.ID, fmt.Sprintf("dir%d", i))
//			require.NoError(t, err)
//			require.Equal(t, 25, len(files))
//		}
//
//		return nil
//	}))
//}

func TestStartCommitLatestOnBranch(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		commit1, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit(repo, commit1.ID))

		commit2, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)

		require.NoError(t, env.PachClient.FinishCommit(repo, commit2.ID))

		commit3, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit(repo, commit3.ID))

		commitInfo, err := env.PachClient.InspectCommit(repo, "master")
		require.NoError(t, err)
		require.Equal(t, commit3.ID, commitInfo.Commit.ID)

		return nil
	}))
}

func TestSetBranchTwice(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		commit1, err := env.PachClient.StartCommit(repo, "")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.SetBranch(repo, commit1.ID, "master"))
		require.NoError(t, env.PachClient.FinishCommit(repo, commit1.ID))

		commit2, err := env.PachClient.StartCommit(repo, "")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.SetBranch(repo, commit2.ID, "master"))
		require.NoError(t, env.PachClient.FinishCommit(repo, commit2.ID))

		branches, err := env.PachClient.ListBranch(repo)
		require.NoError(t, err)

		require.Equal(t, 1, len(branches))
		require.Equal(t, "master", branches[0].Name)
		require.Equal(t, commit2.ID, branches[0].Head.ID)

		return nil
	}))
}

// TODO: Make work with V2?
//func TestSyncPullPush(t *testing.T) {
//	t.Parallel()
//	db := dbutil.NewTestDB(t)
//	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
//		repo1 := "repo1"
//		require.NoError(t, env.PachClient.CreateRepo(repo1))
//
//		commit1, err := env.PachClient.StartCommit(repo1, "master")
//		require.NoError(t, err)
//		require.NoError(t, env.PachClient.PutFile(repo1, commit1.ID, "foo", strings.NewReader("foo\n")))
//		require.NoError(t, err)
//		require.NoError(t, env.PachClient.PutFile(repo1, commit1.ID, "dir/bar", strings.NewReader("bar\n")))
//		require.NoError(t, err)
//		require.NoError(t, env.PachClient.FinishCommit(repo1, commit1.ID))
//
//		tmpDir, err := ioutil.TempDir("/tmp", "pfs")
//		require.NoError(t, err)
//
//		puller := pfssync.NewPuller()
//		require.NoError(t, puller.Pull(env.PachClient, tmpDir, repo1, commit1.ID, "/", false, false, 2, nil, ""))
//		_, err = puller.CleanUp()
//		require.NoError(t, err)
//
//		repo2 := "repo2"
//		require.NoError(t, env.PachClient.CreateRepo(repo2))
//
//		commit2, err := env.PachClient.StartCommit(repo2, "master")
//		require.NoError(t, err)
//
//		require.NoError(t, pfssync.Push(env.PachClient, tmpDir, commit2, false))
//		require.NoError(t, env.PachClient.FinishCommit(repo2, commit2.ID))
//
//		var buffer bytes.Buffer
//		require.NoError(t, env.PachClient.GetFile(repo2, commit2.ID, "foo", &buffer))
//		require.Equal(t, "foo\n", buffer.String())
//		buffer.Reset()
//		require.NoError(t, env.PachClient.GetFile(repo2, commit2.ID, "dir/bar", &buffer))
//		require.Equal(t, "bar\n", buffer.String())
//
//		fileInfos, err := env.PachClient.ListFile(repo2, commit2.ID, "")
//		require.NoError(t, err)
//		require.Equal(t, 2, len(fileInfos))
//
//		commit3, err := env.PachClient.StartCommit(repo2, "master")
//		require.NoError(t, err)
//
//		// Test the overwrite flag.
//		// After this Push operation, all files should still look the same, since
//		// the old files were overwritten.
//		require.NoError(t, pfssync.Push(env.PachClient, tmpDir, commit3, true))
//		require.NoError(t, env.PachClient.FinishCommit(repo2, commit3.ID))
//
//		buffer.Reset()
//		require.NoError(t, env.PachClient.GetFile(repo2, commit3.ID, "foo", &buffer))
//		require.Equal(t, "foo\n", buffer.String())
//		buffer.Reset()
//		require.NoError(t, env.PachClient.GetFile(repo2, commit3.ID, "dir/bar", &buffer))
//		require.Equal(t, "bar\n", buffer.String())
//
//		fileInfos, err = env.PachClient.ListFile(repo2, commit3.ID, "")
//		require.NoError(t, err)
//		require.Equal(t, 2, len(fileInfos))
//
//		// Test Lazy files
//		tmpDir2, err := ioutil.TempDir("/tmp", "pfs")
//		require.NoError(t, err)
//
//		puller = pfssync.NewPuller()
//		require.NoError(t, puller.Pull(env.PachClient, tmpDir2, repo1, "master", "/", true, false, 2, nil, ""))
//
//		data, err := ioutil.ReadFile(path.Join(tmpDir2, "dir/bar"))
//		require.NoError(t, err)
//		require.Equal(t, "bar\n", string(data))
//
//		_, err = puller.CleanUp()
//		require.NoError(t, err)
//
//		return nil
//	})
//	require.NoError(t, err)
//}
//
//func TestSyncFile(t *testing.T) {
//	if os.Getenv("RUN_BAD_TESTS") == "" {
//		t.Skip("Skipping because RUN_BAD_TESTS was empty")
//	}
//	t.Parallel()
//	db := dbutil.NewTestDB(t)
//	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
//		repo := "repo"
//		require.NoError(t, env.PachClient.CreateRepo(repo))
//
//		content1 := random.String(int(pfs.ChunkSize))
//
//		commit1, err := env.PachClient.StartCommit(repo, "master")
//		require.NoError(t, err)
//		require.NoError(t, pfssync.PushFile(env.PachClient, env.PachClient, &pfs.File{
//			Commit: commit1,
//			Path:   "file",
//		}, strings.NewReader(content1)))
//		require.NoError(t, env.PachClient.FinishCommit(repo, commit1.ID))
//
//		var buffer bytes.Buffer
//		require.NoError(t, env.PachClient.GetFile(repo, commit1.ID, "file", &buffer))
//		require.Equal(t, content1, buffer.String())
//
//		content2 := random.String(int(pfs.ChunkSize * 2))
//
//		commit2, err := env.PachClient.StartCommit(repo, "master")
//		require.NoError(t, err)
//		require.NoError(t, pfssync.PushFile(env.PachClient, env.PachClient, &pfs.File{
//			Commit: commit2,
//			Path:   "file",
//		}, strings.NewReader(content2)))
//		require.NoError(t, env.PachClient.FinishCommit(repo, commit2.ID))
//
//		buffer.Reset()
//		require.NoError(t, env.PachClient.GetFile(repo, commit2.ID, "file", &buffer))
//		require.Equal(t, content2, buffer.String())
//
//		content3 := content2 + random.String(int(pfs.ChunkSize))
//
//		commit3, err := env.PachClient.StartCommit(repo, "master")
//		require.NoError(t, err)
//		require.NoError(t, pfssync.PushFile(env.PachClient, env.PachClient, &pfs.File{
//			Commit: commit3,
//			Path:   "file",
//		}, strings.NewReader(content3)))
//		require.NoError(t, env.PachClient.FinishCommit(repo, commit3.ID))
//
//		buffer.Reset()
//		require.NoError(t, env.PachClient.GetFile(repo, commit3.ID, "file", &buffer))
//		require.Equal(t, content3, buffer.String())
//
//		return nil
//	})
//	require.NoError(t, err)
//}
//
//func TestSyncEmptyDir(t *testing.T) {
//	t.Parallel()
//db := dbutil.NewTestDB(t)
//require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
//		repo := "repo"
//		require.NoError(t, env.PachClient.CreateRepo(repo))
//
//		commit, err := env.PachClient.StartCommit(repo, "master")
//		require.NoError(t, err)
//		require.NoError(t, env.PachClient.FinishCommit(repo, commit.ID))
//
//		tmpDir, err := ioutil.TempDir("/tmp", "pfs")
//		require.NoError(t, err)
//
//		// We want to make sure that Pull creates an empty directory
//		// when the path that we are cloning is empty.
//		dir := filepath.Join(tmpDir, "tmp")
//
//		puller := pfssync.NewPuller()
//		require.NoError(t, puller.Pull(env.PachClient, dir, repo, commit.ID, "/", false, false, 0, nil, ""))
//		_, err = os.Stat(dir)
//		require.NoError(t, err)
//		_, err = puller.CleanUp()
//		require.NoError(t, err)
//
//		return nil
//	})
//	require.NoError(t, err)
//}

func TestFlush(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		require.NoError(t, env.PachClient.CreateRepo("A"))
		require.NoError(t, env.PachClient.CreateRepo("B"))
		require.NoError(t, env.PachClient.CreateBranch("B", "master", "", []*pfs.Branch{pclient.NewBranch("A", "master")}))
		ACommit, err := env.PachClient.StartCommit("A", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("A", "master"))
		require.NoError(t, env.PachClient.FinishCommit("B", "master"))
		commitInfoIter, err := env.PachClient.FlushCommit([]*pfs.Commit{pclient.NewCommit("A", ACommit.ID)}, nil)
		require.NoError(t, err)
		commitInfos, err := collectCommitInfos(commitInfoIter)
		require.NoError(t, err)
		require.Equal(t, 1, len(commitInfos))

		return nil
	}))
}

// TestFlush2 implements the following DAG:
// A ─▶ B ─▶ C ─▶ D
func TestFlush2(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		require.NoError(t, env.PachClient.CreateRepo("A"))
		require.NoError(t, env.PachClient.CreateRepo("B"))
		require.NoError(t, env.PachClient.CreateRepo("C"))
		require.NoError(t, env.PachClient.CreateRepo("D"))
		require.NoError(t, env.PachClient.CreateBranch("B", "master", "", []*pfs.Branch{pclient.NewBranch("A", "master")}))
		require.NoError(t, env.PachClient.CreateBranch("C", "master", "", []*pfs.Branch{pclient.NewBranch("B", "master")}))
		require.NoError(t, env.PachClient.CreateBranch("D", "master", "", []*pfs.Branch{pclient.NewBranch("C", "master")}))
		ACommit, err := env.PachClient.StartCommit("A", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("A", "master"))

		// do the other commits in a goro so we can block for them
		go func() {
			require.NoError(t, env.PachClient.FinishCommit("B", "master"))
			require.NoError(t, env.PachClient.FinishCommit("C", "master"))
			require.NoError(t, env.PachClient.FinishCommit("D", "master"))
		}()

		// Flush ACommit
		commitInfoIter, err := env.PachClient.FlushCommit([]*pfs.Commit{pclient.NewCommit("A", ACommit.ID)}, nil)
		require.NoError(t, err)
		commitInfos, err := collectCommitInfos(commitInfoIter)
		require.NoError(t, err)
		require.Equal(t, 3, len(commitInfos))

		commitInfoIter, err = env.PachClient.FlushCommit(
			[]*pfs.Commit{pclient.NewCommit("A", ACommit.ID)},
			[]*pfs.Repo{pclient.NewRepo("C")},
		)
		require.NoError(t, err)
		commitInfos, err = collectCommitInfos(commitInfoIter)
		require.NoError(t, err)
		require.Equal(t, 1, len(commitInfos))

		return nil
	}))
}

// A
//  ╲
//   ◀
//    C
//   ◀
//  ╱
// B
func TestFlush3(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		require.NoError(t, env.PachClient.CreateRepo("A"))
		require.NoError(t, env.PachClient.CreateRepo("B"))
		require.NoError(t, env.PachClient.CreateRepo("C"))

		require.NoError(t, env.PachClient.CreateBranch("C", "master", "", []*pfs.Branch{pclient.NewBranch("A", "master"), pclient.NewBranch("B", "master")}))

		ACommit, err := env.PachClient.StartCommit("A", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("A", ACommit.ID))
		require.NoError(t, env.PachClient.FinishCommit("C", "master"))
		BCommit, err := env.PachClient.StartCommit("B", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("B", BCommit.ID))
		require.NoError(t, env.PachClient.FinishCommit("C", "master"))

		BCommit, err = env.PachClient.StartCommit("B", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("B", BCommit.ID))
		require.NoError(t, env.PachClient.FinishCommit("C", "master"))

		commitIter, err := env.PachClient.FlushCommit([]*pfs.Commit{pclient.NewCommit("B", BCommit.ID), pclient.NewCommit("A", ACommit.ID)}, nil)
		require.NoError(t, err)
		commitInfos, err := collectCommitInfos(commitIter)
		require.NoError(t, err)
		require.Equal(t, 1, len(commitInfos))

		require.Equal(t, commitInfos[0].Commit.Repo.Name, "C")

		return nil
	}))
}

func TestFlushRedundant(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		require.NoError(t, env.PachClient.CreateRepo("A"))
		ACommit, err := env.PachClient.StartCommit("A", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("A", "master"))
		commitInfoIter, err := env.PachClient.FlushCommit([]*pfs.Commit{pclient.NewCommit("A", ACommit.ID), pclient.NewCommit("A", ACommit.ID)}, nil)
		require.NoError(t, err)
		commitInfos, err := collectCommitInfos(commitInfoIter)
		require.NoError(t, err)
		require.Equal(t, 0, len(commitInfos))

		return nil
	}))
}

func TestFlushCommitWithNoDownstreamRepos(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		repo := "test"
		require.NoError(t, env.PachClient.CreateRepo(repo))
		commit, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit(repo, commit.ID))
		commitIter, err := env.PachClient.FlushCommit([]*pfs.Commit{pclient.NewCommit(repo, commit.ID)}, nil)
		require.NoError(t, err)
		commitInfos, err := collectCommitInfos(commitIter)
		require.NoError(t, err)
		require.Equal(t, 0, len(commitInfos))

		return nil
	}))
}

func TestFlushOpenCommit(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		require.NoError(t, env.PachClient.CreateRepo("A"))
		require.NoError(t, env.PachClient.CreateRepo("B"))
		require.NoError(t, env.PachClient.CreateBranch("B", "master", "", []*pfs.Branch{pclient.NewBranch("A", "master")}))
		ACommit, err := env.PachClient.StartCommit("A", "master")
		require.NoError(t, err)

		// do the other commits in a goro so we can block for them
		go func() {
			time.Sleep(5 * time.Second)
			require.NoError(t, env.PachClient.FinishCommit("A", "master"))
			require.NoError(t, env.PachClient.FinishCommit("B", "master"))
		}()

		// Flush ACommit
		commitIter, err := env.PachClient.FlushCommit([]*pfs.Commit{pclient.NewCommit("A", ACommit.ID)}, nil)
		require.NoError(t, err)
		commitInfos, err := collectCommitInfos(commitIter)
		require.NoError(t, err)
		require.Equal(t, 1, len(commitInfos))

		return nil
	}))
}

func TestEmptyFlush(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		commitIter, err := env.PachClient.FlushCommit(nil, nil)
		require.NoError(t, err)
		_, err = collectCommitInfos(commitIter)
		require.YesError(t, err)

		return nil
	}))
}

func TestFlushNonExistentCommit(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		iter, err := env.PachClient.FlushCommit([]*pfs.Commit{pclient.NewCommit("fake-repo", "fake-commit")}, nil)
		require.NoError(t, err)
		_, err = collectCommitInfos(iter)
		require.YesError(t, err)
		repo := "FlushNonExistentCommit"
		require.NoError(t, env.PachClient.CreateRepo(repo))
		_, err = env.PachClient.FlushCommit([]*pfs.Commit{pclient.NewCommit(repo, "fake-commit")}, nil)
		require.NoError(t, err)
		_, err = collectCommitInfos(iter)
		require.YesError(t, err)

		return nil
	}))
}

func TestPutFileSplit1(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		if testing.Short() {
			t.Skip("Skipping integration tests in short mode")
		}

		// create repos
		repo := tu.UniqueString("TestPutFileSplit")
		require.NoError(t, env.PachClient.CreateRepo(repo))
		commit, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFileSplit(repo, commit.ID, "none", pfs.Delimiter_NONE, 0, 0, false, strings.NewReader("foo\nbar\nbuz\n")))
		require.NoError(t, env.PachClient.PutFileSplit(repo, commit.ID, "line", pfs.Delimiter_LINE, 1, 0, false, strings.NewReader("foo\nbar\nbuz\n")))
		require.NoError(t, env.PachClient.PutFileSplit(repo, commit.ID, "line", pfs.Delimiter_LINE, 1, 0, false, strings.NewReader("foo\nbar\nbuz\n")))
		require.NoError(t, env.PachClient.PutFileSplit(repo, commit.ID, "line2", pfs.Delimiter_LINE, 2, 0, false, strings.NewReader("foo\nbar\nbuz\nfiz\n")))
		require.NoError(t, env.PachClient.PutFileSplit(repo, commit.ID, "line3", pfs.Delimiter_LINE, 0, 8, false, strings.NewReader("foo\nbar\nbuz\nfiz\n")))
		require.NoError(t, env.PachClient.PutFileSplit(repo, commit.ID, "json", pfs.Delimiter_JSON, 1, 0, false, strings.NewReader("{}{}{}{}{}{}{}{}{}{}")))
		require.NoError(t, env.PachClient.PutFileSplit(repo, commit.ID, "json", pfs.Delimiter_JSON, 1, 0, false, strings.NewReader("{}{}{}{}{}{}{}{}{}{}")))
		require.NoError(t, env.PachClient.PutFileSplit(repo, commit.ID, "json2", pfs.Delimiter_JSON, 2, 0, false, strings.NewReader("{}{}{}{}")))
		require.NoError(t, env.PachClient.PutFileSplit(repo, commit.ID, "json3", pfs.Delimiter_JSON, 0, 4, false, strings.NewReader("{}{}{}{}")))
		require.NoError(t, env.PachClient.FinishCommit(repo, commit.ID))

		files, err := env.PachClient.ListFileAll(repo, commit.ID, "line2")
		require.NoError(t, err)
		require.Equal(t, 2, len(files))
		for _, fileInfo := range files {
			require.Equal(t, uint64(8), fileInfo.SizeBytes)
		}

		commit2, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFileSplit(repo, commit2.ID, "line", pfs.Delimiter_LINE, 1, 0, false, strings.NewReader("foo\nbar\nbuz\n")))
		require.NoError(t, env.PachClient.PutFileSplit(repo, commit2.ID, "json", pfs.Delimiter_JSON, 1, 0, false, strings.NewReader("{}{}{}{}{}{}{}{}{}{}")))
		require.NoError(t, env.PachClient.FinishCommit(repo, commit2.ID))

		files, err = env.PachClient.ListFileAll(repo, commit2.ID, "line")
		require.NoError(t, err)
		for _, fileInfo := range files {
			fmt.Println(fileInfo.File.Path)
		}
		require.Equal(t, 9, len(files))
		for _, fileInfo := range files {
			require.Equal(t, uint64(4), fileInfo.SizeBytes)
		}

		fileInfo, err := env.PachClient.InspectFile(repo, commit.ID, "none")
		require.NoError(t, err)
		require.Equal(t, pfs.FileType_FILE, fileInfo.FileType)
		files, err = env.PachClient.ListFileAll(repo, commit.ID, "line")
		require.NoError(t, err)
		require.Equal(t, 6, len(files))
		for _, fileInfo := range files {
			require.Equal(t, uint64(4), fileInfo.SizeBytes)
		}
		files, err = env.PachClient.ListFileAll(repo, commit2.ID, "line")
		require.NoError(t, err)
		require.Equal(t, 9, len(files))
		for _, fileInfo := range files {
			require.Equal(t, uint64(4), fileInfo.SizeBytes)
		}
		files, err = env.PachClient.ListFileAll(repo, commit.ID, "line2")
		require.NoError(t, err)
		require.Equal(t, 2, len(files))
		for _, fileInfo := range files {
			require.Equal(t, uint64(8), fileInfo.SizeBytes)
		}
		files, err = env.PachClient.ListFileAll(repo, commit.ID, "line3")
		require.NoError(t, err)
		require.Equal(t, 2, len(files))
		for _, fileInfo := range files {
			require.Equal(t, uint64(8), fileInfo.SizeBytes)
		}
		files, err = env.PachClient.ListFileAll(repo, commit.ID, "json")
		require.NoError(t, err)
		require.Equal(t, 20, len(files))
		for _, fileInfo := range files {
			require.Equal(t, uint64(2), fileInfo.SizeBytes)
		}
		files, err = env.PachClient.ListFileAll(repo, commit2.ID, "json")
		require.NoError(t, err)
		require.Equal(t, 30, len(files))
		for _, fileInfo := range files {
			require.Equal(t, uint64(2), fileInfo.SizeBytes)
		}
		files, err = env.PachClient.ListFileAll(repo, commit.ID, "json2")
		require.NoError(t, err)
		require.Equal(t, 2, len(files))
		for _, fileInfo := range files {
			require.Equal(t, uint64(4), fileInfo.SizeBytes)
		}
		files, err = env.PachClient.ListFileAll(repo, commit.ID, "json3")
		require.NoError(t, err)
		require.Equal(t, 2, len(files))
		for _, fileInfo := range files {
			require.Equal(t, uint64(4), fileInfo.SizeBytes)
		}

		return nil
	}))
}

func TestPutFileSplitBig(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		if testing.Short() {
			t.Skip("Skipping integration tests in short mode")
		}

		// create repos
		repo := tu.UniqueString("TestPutFileSplitBig")
		require.NoError(t, env.PachClient.CreateRepo(repo))
		commit, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		buf := &bytes.Buffer{}
		for i := 0; i < 1000; i++ {
			_, err = buf.Write([]byte("foo\n"))
			require.NoError(t, err)
		}
		require.NoError(t, env.PachClient.PutFileSplit(repo, commit.ID, "line", pfs.Delimiter_LINE, 0, 0, false, buf))
		require.NoError(t, env.PachClient.FinishCommit(repo, commit.ID))
		files, err := env.PachClient.ListFileAll(repo, commit.ID, "line")
		require.NoError(t, err)
		require.Equal(t, 1000, len(files))
		for _, fileInfo := range files {
			require.Equal(t, uint64(4), fileInfo.SizeBytes)
		}

		return nil
	}))
}

func TestPutFileSplitCSV(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		// create repos
		repo := tu.UniqueString("TestPutFileSplitCSV")
		require.NoError(t, env.PachClient.CreateRepo(repo))
		require.NoError(t, env.PachClient.PutFileSplit(repo, "master", "data", pfs.Delimiter_CSV, 0, 0, false,
			// Weird, but this is actually two lines ("is\na" is quoted, so one cell)
			strings.NewReader("this,is,a,test\n"+
				"\"\"\"this\"\"\",\"is\nonly\",\"a,test\"\n")))
		fileInfos, err := env.PachClient.ListFileAll(repo, "master", "/data")
		require.NoError(t, err)
		require.Equal(t, 2, len(fileInfos))
		var contents bytes.Buffer
		env.PachClient.GetFile(repo, "master", "/data/0000000000000000", &contents)
		require.Equal(t, "this,is,a,test\n", contents.String())
		contents.Reset()
		env.PachClient.GetFile(repo, "master", "/data/0000000000000001", &contents)
		require.Equal(t, "\"\"\"this\"\"\",\"is\nonly\",\"a,test\"\n", contents.String())

		return nil
	}))
}

func TestPutFileSplitSQL(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		// create repos
		repo := tu.UniqueString("TestPutFileSplitSQL")
		require.NoError(t, env.PachClient.CreateRepo(repo))

		require.NoError(t, env.PachClient.PutFileSplit(repo, "master", "/sql", pfs.Delimiter_SQL, 0, 0, false, strings.NewReader(tu.TestPGDump)))
		fileInfos, err := env.PachClient.ListFileAll(repo, "master", "/sql")
		require.NoError(t, err)
		require.Equal(t, 5, len(fileInfos))

		// Get one of the SQL records & validate it
		var contents bytes.Buffer
		env.PachClient.GetFile(repo, "master", "/sql/0000000000000000", &contents)
		// Validate that the recieved pgdump file creates the cars table
		require.Matches(t, "CREATE TABLE public\\.cars", contents.String())
		// Validate the SQL header more generally by passing the output of GetFile
		// back through the SQL library & confirm that it parses correctly but only
		// has one row
		pgReader := sql.NewPGDumpReader(bufio.NewReader(bytes.NewReader(contents.Bytes())))
		record, err := pgReader.ReadRow()
		require.NoError(t, err)
		require.Equal(t, "Tesla\tRoadster\t2008\tliterally a rocket\n", string(record))
		_, err = pgReader.ReadRow()
		require.YesError(t, err)
		require.True(t, errors.Is(err, io.EOF))

		// Create a new commit that overwrites all existing data & puts it back with
		// --header-records=1
		commit, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.DeleteFile(repo, commit.ID, "/sql"))
		require.NoError(t, env.PachClient.PutFileSplit(repo, commit.ID, "/sql", pfs.Delimiter_SQL, 0, 0, false, strings.NewReader(tu.TestPGDump)))
		require.NoError(t, env.PachClient.FinishCommit(repo, commit.ID))
		fileInfos, err = env.PachClient.ListFileAll(repo, "master", "/sql")
		require.NoError(t, err)
		require.Equal(t, 4, len(fileInfos))

		// Get one of the SQL records & validate it
		contents.Reset()
		env.PachClient.GetFile(repo, "master", "/sql/0000000000000003", &contents)
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
		_, err = pgReader.ReadRow()
		require.YesError(t, err)
		require.True(t, errors.Is(err, io.EOF))

		return nil
	}))
}

func TestDiffFile(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		repo := "TestDiffFile2"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		// Write foo
		c1, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(repo, c1.ID, "foo", strings.NewReader("foo\n")))
		require.NoError(t, env.PachClient.FinishCommit(repo, c1.ID))

		newFis, oldFis, err := env.PachClient.DiffFileAll(repo, c1.ID, "", "", "", "", false)
		require.NoError(t, err)
		require.Equal(t, 0, len(oldFis))
		require.Equal(t, 2, len(newFis))
		require.Equal(t, "/foo", newFis[1].File.Path)

		// Change the value of foo
		c2, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.DeleteFile(repo, c2.ID, "/foo"))
		require.NoError(t, env.PachClient.PutFile(repo, c2.ID, "foo", strings.NewReader("not foo\n")))
		require.NoError(t, env.PachClient.FinishCommit(repo, c2.ID))

		newFis, oldFis, err = env.PachClient.DiffFileAll(repo, c2.ID, "", "", "", "", false)
		require.NoError(t, err)
		require.Equal(t, 2, len(oldFis))
		require.Equal(t, "/foo", oldFis[1].File.Path)
		require.Equal(t, 2, len(newFis))
		require.Equal(t, "/foo", newFis[1].File.Path)

		// Write bar
		c3, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(repo, c3.ID, "/bar", strings.NewReader("bar\n")))
		require.NoError(t, env.PachClient.FinishCommit(repo, c3.ID))

		newFis, oldFis, err = env.PachClient.DiffFileAll(repo, c3.ID, "", "", "", "", false)
		require.NoError(t, err)
		require.Equal(t, 1, len(oldFis))
		require.Equal(t, 2, len(newFis))
		require.Equal(t, "/bar", newFis[1].File.Path)

		// Delete bar
		c4, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.DeleteFile(repo, c4.ID, "/bar"))
		require.NoError(t, env.PachClient.FinishCommit(repo, c4.ID))

		newFis, oldFis, err = env.PachClient.DiffFileAll(repo, c4.ID, "", "", "", "", false)
		require.NoError(t, err)
		require.Equal(t, 2, len(oldFis))
		require.Equal(t, "/bar", oldFis[1].File.Path)
		require.Equal(t, 1, len(newFis))

		// Write dir/fizz and dir/buzz
		c5, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(repo, c5.ID, "/dir/fizz", strings.NewReader("fizz\n")))
		require.NoError(t, env.PachClient.PutFile(repo, c5.ID, "/dir/buzz", strings.NewReader("buzz\n")))
		require.NoError(t, env.PachClient.FinishCommit(repo, c5.ID))

		newFis, oldFis, err = env.PachClient.DiffFileAll(repo, c5.ID, "", "", "", "", false)
		require.NoError(t, err)
		require.Equal(t, 1, len(oldFis))
		require.Equal(t, 4, len(newFis))

		// Modify dir/fizz
		c6, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(repo, c6.ID, "/dir/fizz", strings.NewReader("fizz\n")))
		require.NoError(t, env.PachClient.FinishCommit(repo, c6.ID))

		newFis, oldFis, err = env.PachClient.DiffFileAll(repo, c6.ID, "", "", "", "", false)
		require.NoError(t, err)
		require.Equal(t, 3, len(oldFis))
		require.Equal(t, "/dir/fizz", oldFis[2].File.Path)
		require.Equal(t, 3, len(newFis))
		require.Equal(t, "/dir/fizz", newFis[2].File.Path)

		return nil
	}))
}

func TestGlobFile(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		if testing.Short() {
			t.Skip("Skipping integration tests in short mode")
		}

		repo := tu.UniqueString("TestGlobFile")
		require.NoError(t, env.PachClient.CreateRepo(repo))

		// Write foo
		numFiles := 100
		_, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		for i := 0; i < numFiles; i++ {
			require.NoError(t, env.PachClient.PutFile(repo, "master", fmt.Sprintf("file%d", i), strings.NewReader("1")))
			require.NoError(t, env.PachClient.PutFile(repo, "master", fmt.Sprintf("dir1/file%d", i), strings.NewReader("2")))
			require.NoError(t, env.PachClient.PutFile(repo, "master", fmt.Sprintf("dir2/dir3/file%d", i), strings.NewReader("3")))
		}
		require.NoError(t, env.PachClient.FinishCommit(repo, "master"))

		fileInfos, err := env.PachClient.GlobFileAll(repo, "master", "*")
		require.NoError(t, err)
		require.Equal(t, numFiles+2, len(fileInfos))
		fileInfos, err = env.PachClient.GlobFileAll(repo, "master", "file*")
		require.NoError(t, err)
		require.Equal(t, numFiles, len(fileInfos))
		fileInfos, err = env.PachClient.GlobFileAll(repo, "master", "dir1/*")
		require.NoError(t, err)
		require.Equal(t, numFiles, len(fileInfos))
		fileInfos, err = env.PachClient.GlobFileAll(repo, "master", "dir2/dir3/*")
		require.NoError(t, err)
		require.Equal(t, numFiles, len(fileInfos))
		fileInfos, err = env.PachClient.GlobFileAll(repo, "master", "*/*")
		require.NoError(t, err)
		require.Equal(t, numFiles+1, len(fileInfos))

		var output strings.Builder
		err = env.PachClient.GetFile(repo, "master", "*", &output)
		require.NoError(t, err)
		require.Equal(t, numFiles*3, len(output.String()))

		output = strings.Builder{}
		err = env.PachClient.GetFile(repo, "master", "dir2/dir3/file1?", &output)
		require.NoError(t, err)
		require.Equal(t, 10, len(output.String()))

		output = strings.Builder{}
		err = env.PachClient.GetFile(repo, "master", "**file1?", &output)
		require.NoError(t, err)
		require.Equal(t, 30, len(output.String()))

		output = strings.Builder{}
		err = env.PachClient.GetFile(repo, "master", "**file1", &output)
		require.NoError(t, err)
		require.True(t, strings.Contains(output.String(), "1"))
		require.True(t, strings.Contains(output.String(), "2"))
		require.True(t, strings.Contains(output.String(), "3"))

		output = strings.Builder{}
		err = env.PachClient.GetFile(repo, "master", "**file1", &output)
		require.NoError(t, err)
		match, err := regexp.Match("[123]", []byte(output.String()))
		require.NoError(t, err)
		require.True(t, match)

		output = strings.Builder{}
		err = env.PachClient.GetFile(repo, "master", "dir?", &output)
		require.NoError(t, err)

		output = strings.Builder{}
		err = env.PachClient.GetFile(repo, "master", "", &output)
		require.NoError(t, err)

		// TODO: File not found?
		//output = strings.Builder{}
		//err = env.PachClient.GetFile(repo, "master", "garbage", &output)
		//require.YesError(t, err)

		_, err = env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)

		err = env.PachClient.DeleteFile(repo, "master", "dir2/dir3/*")
		require.NoError(t, err)
		err = env.PachClient.DeleteFile(repo, "master", "dir?/*")
		require.NoError(t, err)
		err = env.PachClient.DeleteFile(repo, "master", "/")
		require.NoError(t, err)

		require.NoError(t, env.PachClient.FinishCommit(repo, "master"))

		fileInfos, err = env.PachClient.GlobFileAll(repo, "master", "**")
		require.NoError(t, err)
		require.Equal(t, 0, len(fileInfos))

		return nil
	}))
}

func TestGlobFile2(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		if testing.Short() {
			t.Skip("Skipping integration tests in short mode")
		}

		repo := tu.UniqueString("TestGlobFileF")
		require.NoError(t, env.PachClient.CreateRepo(repo))

		_, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		expectedFileNames := []string{}
		for i := 0; i < 100; i++ {
			filename := fmt.Sprintf("/%d", i)
			require.NoError(t, env.PachClient.PutFile(repo, "master", filename, strings.NewReader(filename)))

			if strings.HasPrefix(filename, "/1") {
				expectedFileNames = append(expectedFileNames, filename)
			}
		}
		require.NoError(t, env.PachClient.FinishCommit(repo, "master"))

		actualFileNames := []string{}
		require.NoError(t, env.PachClient.GlobFile(repo, "master", "/1*", func(fileInfo *pfs.FileInfo) error {
			actualFileNames = append(actualFileNames, fileInfo.File.Path)
			return nil
		}))

		sort.Strings(expectedFileNames)
		sort.Strings(actualFileNames)
		require.Equal(t, expectedFileNames, actualFileNames)

		return nil
	}))
}

func TestGlobFile3(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		repo := "TestGlobFile"
		require.NoError(t, env.PachClient.CreateRepo(repo))
		commit1, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(repo, commit1.ID, "/dir1/file1.1", &bytes.Buffer{}))
		require.NoError(t, env.PachClient.PutFile(repo, commit1.ID, "/dir1/file1.2", &bytes.Buffer{}))
		require.NoError(t, env.PachClient.PutFile(repo, commit1.ID, "/dir2/file2.1", &bytes.Buffer{}))
		require.NoError(t, env.PachClient.PutFile(repo, commit1.ID, "/dir2/file2.2", &bytes.Buffer{}))
		require.NoError(t, env.PachClient.FinishCommit(repo, commit1.ID))
		globFile := func(pattern string) []string {
			var fis []*pfs.FileInfo
			require.NoError(t, env.PachClient.GlobFile(repo, commit1.ID, pattern, func(fi *pfs.FileInfo) error {
				fis = append(fis, fi)
				return nil
			}))
			return finfosToPaths(fis)
		}
		assert.ElementsMatch(t, []string{"/dir1/file1.2", "/dir2/file2.2"}, globFile("**.2"))
		assert.ElementsMatch(t, []string{"/dir1/file1.1", "/dir1/file1.2"}, globFile("/dir1/*"))
		assert.ElementsMatch(t, []string{"/dir1/", "/dir2/"}, globFile("/*"))
		assert.ElementsMatch(t, []string{"/"}, globFile("/"))
		return nil
	}))
}

// TestGetFileGlobOrder checks that GetFile(glob) streams data back in the
// right order. GetFile(glob) is supposed to return a stream of data of the
// form file1 + file2 + .. + fileN, where file1 is the lexicographically lowest
// file matching 'glob', file2 is the next lowest, etc.
func TestGetFileGlobOrder(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		// create repos
		repo := tu.UniqueString("TestGetFileGlobOrder")
		require.NoError(t, env.PachClient.CreateRepo(repo))

		var expected bytes.Buffer
		commit, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		for i := 0; i < 25; i++ {
			next := fmt.Sprintf("%d,%d,%d,%d\n", 4*i, (4*i)+1, (4*i)+2, (4*i)+3)
			expected.WriteString(next)
			env.PachClient.PutFile(repo, commit.ID, fmt.Sprintf("/data/%010d", i), strings.NewReader(next))
		}
		require.NoError(t, env.PachClient.FinishCommit(repo, commit.ID))

		var output bytes.Buffer
		require.NoError(t, env.PachClient.GetFile(repo, "master", "/data/*", &output))
		require.Equal(t, expected.String(), output.String())

		return nil
	}))
}

func TestApplyWriteOrder(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		if testing.Short() {
			t.Skip("Skipping integration tests in short mode")
		}

		repo := tu.UniqueString("TestApplyWriteOrder")
		require.NoError(t, env.PachClient.CreateRepo(repo))

		// Test that fails when records are applied in lexicographic order
		// rather than mod revision order.
		_, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(repo, "master", "/file", strings.NewReader("")))
		err = env.PachClient.DeleteFile(repo, "master", "/")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit(repo, "master"))
		fileInfos, err := env.PachClient.GlobFileAll(repo, "master", "**")
		require.NoError(t, err)
		require.Equal(t, 0, len(fileInfos))

		return nil
	}))
}

// TODO: Make work with V2?
//func TestOverwrite(t *testing.T) {
//	t.Parallel()
//	db := dbutil.NewTestDB(t)
//	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
//		if testing.Short() {
//			t.Skip("Skipping integration tests in short mode")
//		}
//
//		repo := tu.UniqueString("TestGlob")
//		require.NoError(t, env.PachClient.CreateRepo(repo))
//
//		// Write foo
//		_, err := env.PachClient.StartCommit(repo, "master")
//		require.NoError(t, err)
//		require.NoError(t, env.PachClient.PutFile(repo, "master", "file1", strings.NewReader("foo")))
//		require.NoError(t, err)
//		require.NoError(t, env.PachClient.PutFileSplit(repo, "master", "file2", pfs.Delimiter_LINE, 0, 0, 0, false, strings.NewReader("foo\nbar\nbuz\n")))
//		require.NoError(t, err)
//		require.NoError(t, env.PachClient.PutFileSplit(repo, "master", "file3", pfs.Delimiter_LINE, 0, 0, 0, false, strings.NewReader("foo\nbar\nbuz\n")))
//		require.NoError(t, err)
//		require.NoError(t, env.PachClient.FinishCommit(repo, "master"))
//		_, err = env.PachClient.StartCommit(repo, "master")
//		require.NoError(t, err)
//		require.NoError(t, env.PachClient.PutFileOverwrite(repo, "master", "file1", strings.NewReader("bar")))
//		require.NoError(t, err)
//		require.NoError(t, env.PachClient.PutFileOverwrite(repo, "master", "file2", strings.NewReader("buzz")))
//		require.NoError(t, err)
//		require.NoError(t, env.PachClient.PutFileSplit(repo, "master", "file3", pfs.Delimiter_LINE, 0, 0, 0, true, strings.NewReader("0\n1\n2\n")))
//		require.NoError(t, err)
//		require.NoError(t, env.PachClient.FinishCommit(repo, "master"))
//		var buffer bytes.Buffer
//		require.NoError(t, env.PachClient.GetFile(repo, "master", "file1", &buffer))
//		require.Equal(t, "bar", buffer.String())
//		buffer.Reset()
//		require.NoError(t, env.PachClient.GetFile(repo, "master", "file2", &buffer))
//		require.Equal(t, "buzz", buffer.String())
//		fileInfos, err := env.PachClient.ListFileAll(repo, "master", "file3")
//		require.NoError(t, err)
//		require.Equal(t, 3, len(fileInfos))
//		for i := 0; i < 3; i++ {
//			buffer.Reset()
//			require.NoError(t, env.PachClient.GetFile(repo, "master", fmt.Sprintf("file3/%016x", i), &buffer))
//			require.Equal(t, fmt.Sprintf("%d\n", i), buffer.String())
//		}
//
//		return nil
//	}))
//}

func TestCopyFile(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		repo := "TestCopyFile"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		masterCommit, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		numFiles := 5
		for i := 0; i < numFiles; i++ {
			require.NoError(t, env.PachClient.PutFile(repo, masterCommit.ID, fmt.Sprintf("files/%d", i), strings.NewReader(fmt.Sprintf("foo %d\n", i))))
		}
		require.NoError(t, env.PachClient.FinishCommit(repo, masterCommit.ID))

		for i := 0; i < numFiles; i++ {
			_, err = env.PachClient.InspectFile(repo, masterCommit.ID, fmt.Sprintf("files/%d", i))
			require.NoError(t, err)
		}

		otherCommit, err := env.PachClient.StartCommit(repo, "other")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.CopyFile(repo, masterCommit.ID, "files", repo, otherCommit.ID, "files", false))
		require.NoError(t, env.PachClient.CopyFile(repo, masterCommit.ID, "files/0", repo, otherCommit.ID, "file0", false))
		require.NoError(t, env.PachClient.FinishCommit(repo, otherCommit.ID))

		for i := 0; i < numFiles; i++ {
			_, err = env.PachClient.InspectFile(repo, otherCommit.ID, fmt.Sprintf("files/%d", i))
			require.NoError(t, err)
		}
		_, err = env.PachClient.InspectFile(repo, otherCommit.ID, "files/0")
		require.NoError(t, err)
		return nil
	}))
}

func TestPropagateCommit(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		repo1 := tu.UniqueString("TestPropagateCommit1")
		require.NoError(t, env.PachClient.CreateRepo(repo1))
		repo2 := tu.UniqueString("TestPropagateCommit2")
		require.NoError(t, env.PachClient.CreateRepo(repo2))
		require.NoError(t, env.PachClient.CreateBranch(repo2, "master", "", []*pfs.Branch{pclient.NewBranch(repo1, "master")}))
		commit, err := env.PachClient.StartCommit(repo1, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit(repo1, commit.ID))
		commits, err := env.PachClient.ListCommitByRepo(repo2)
		require.NoError(t, err)
		require.Equal(t, 1, len(commits))

		return nil
	}))
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
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		require.NoError(t, env.PachClient.CreateRepo("A"))
		require.NoError(t, env.PachClient.CreateRepo("B"))
		require.NoError(t, env.PachClient.CreateRepo("C"))
		require.NoError(t, env.PachClient.CreateRepo("D"))
		require.NoError(t, env.PachClient.CreateBranch("C", "master", "", []*pfs.Branch{pclient.NewBranch("A", "master"), pclient.NewBranch("B", "master")}))
		_, err := env.PachClient.StartCommit("A", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("A", "master"))
		require.NoError(t, env.PachClient.FinishCommit("C", "master"))
		_, err = env.PachClient.StartCommit("B", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("B", "master"))
		require.NoError(t, env.PachClient.FinishCommit("C", "master"))
		commits, err := env.PachClient.ListCommitByRepo("C")
		require.NoError(t, err)
		require.Equal(t, 2, len(commits))

		// Create a branch in D, it should receive a single commit for the heads of `A` and `B`.
		require.NoError(t, env.PachClient.CreateBranch("D", "master", "", []*pfs.Branch{pclient.NewBranch("A", "master"), pclient.NewBranch("B", "master")}))
		commits, err = env.PachClient.ListCommitByRepo("D")
		require.NoError(t, err)
		require.Equal(t, 1, len(commits))

		return nil
	}))
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
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		require.NoError(t, env.PachClient.CreateRepo("A"))
		require.NoError(t, env.PachClient.CreateRepo("B"))
		require.NoError(t, env.PachClient.CreateRepo("C"))
		require.NoError(t, env.PachClient.CreateRepo("D"))
		require.NoError(t, env.PachClient.CreateBranch("B", "master", "", []*pfs.Branch{pclient.NewBranch("A", "master")}))
		require.NoError(t, env.PachClient.CreateBranch("C", "master", "", []*pfs.Branch{pclient.NewBranch("B", "master")}))
		_, err := env.PachClient.StartCommit("A", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("A", "master"))
		require.NoError(t, env.PachClient.FinishCommit("B", "master"))
		require.NoError(t, env.PachClient.FinishCommit("C", "master"))

		_, err = env.PachClient.StartCommit("D", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("D", "master"))

		require.NoError(t, env.PachClient.CreateBranch("B", "master", "", []*pfs.Branch{pclient.NewBranch("A", "master"), pclient.NewBranch("D", "master")}))
		require.NoError(t, env.PachClient.FinishCommit("B", "master"))
		require.NoError(t, env.PachClient.FinishCommit("C", "master"))
		cCommitInfo, err := env.PachClient.InspectCommit("C", "master")
		require.NoError(t, err)
		require.Equal(t, 3, len(cCommitInfo.Provenance))

		return nil
	}))
}

func TestBranchProvenance(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
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
					err := env.PachClient.CreateBranch(repo, step.name, "", provenance)
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

		return nil
	}))
}

func TestChildCommits(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		require.NoError(t, env.PachClient.CreateRepo("A"))
		require.NoError(t, env.PachClient.CreateBranch("A", "master", "", nil))

		// Small helper function wrapping env.PachClient.InspectCommit, because it's called a lot
		inspect := func(repo, commit string) *pfs.CommitInfo {
			commitInfo, err := env.PachClient.InspectCommit(repo, commit)
			require.NoError(t, err)
			return commitInfo
		}

		commit1, err := env.PachClient.StartCommit("A", "master")
		require.NoError(t, err)
		commits, err := env.PachClient.ListCommit("A", "master", "", 0)
		require.NoError(t, err)
		t.Logf("%v", commits)
		require.NoError(t, env.PachClient.FinishCommit("A", "master"))

		commit2, err := env.PachClient.StartCommit("A", "master")
		require.NoError(t, err)

		// Inspect commit 1 and 2
		commit1Info, commit2Info := inspect("A", commit1.ID), inspect("A", commit2.ID)
		require.Equal(t, commit1.ID, commit2Info.ParentCommit.ID)
		require.ElementsEqualUnderFn(t, []string{commit2.ID}, commit1Info.ChildCommits, CommitToID)

		// Delete commit 2 and make sure it's removed from commit1.ChildCommits
		require.NoError(t, env.PachClient.DeleteCommit("A", commit2.ID))
		commit1Info = inspect("A", commit1.ID)
		require.ElementsEqualUnderFn(t, nil, commit1Info.ChildCommits, CommitToID)

		// Re-create commit2, and create a third commit also extending from commit1.
		// Make sure both appear in commit1.children
		commit2, err = env.PachClient.StartCommit("A", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("A", commit2.ID))
		commit3, err := env.PachClient.PfsAPIClient.StartCommit(env.PachClient.Ctx(), &pfs.StartCommitRequest{
			Parent: pclient.NewCommit("A", commit1.ID),
		})
		require.NoError(t, err)
		commit1Info = inspect("A", commit1.ID)
		require.ElementsEqualUnderFn(t, []string{commit2.ID, commit3.ID}, commit1Info.ChildCommits, CommitToID)

		// Delete commit3 and make sure commit1 has the right children
		require.NoError(t, env.PachClient.DeleteCommit("A", commit3.ID))
		commit1Info = inspect("A", commit1.ID)
		require.ElementsEqualUnderFn(t, []string{commit2.ID}, commit1Info.ChildCommits, CommitToID)

		// Create a downstream branch in the same repo, then commit to "A" and make
		// sure the new HEAD commit is in the parent's children (i.e. test
		// propagateCommit)
		require.NoError(t, env.PachClient.CreateBranch("A", "out", "", []*pfs.Branch{
			pclient.NewBranch("A", "master"),
		}))
		outCommit1ID := inspect("A", "out").Commit.ID
		commit3, err = env.PachClient.StartCommit("A", "master")
		require.NoError(t, err)
		env.PachClient.FinishCommit("A", commit3.ID)
		// Re-inspect outCommit1, which has been updated by StartCommit
		outCommit1, outCommit2 := inspect("A", outCommit1ID), inspect("A", "out")
		require.Equal(t, outCommit1.Commit.ID, outCommit2.ParentCommit.ID)
		require.ElementsEqualUnderFn(t, []string{outCommit2.Commit.ID}, outCommit1.ChildCommits, CommitToID)

		// create a new branch in a different repo and do the same test again
		require.NoError(t, env.PachClient.CreateRepo("B"))
		require.NoError(t, env.PachClient.CreateBranch("B", "master", "", []*pfs.Branch{
			pclient.NewBranch("A", "master"),
		}))
		bCommit1ID := inspect("B", "master").Commit.ID
		commit3, err = env.PachClient.StartCommit("A", "master")
		require.NoError(t, err)
		env.PachClient.FinishCommit("A", commit3.ID)
		// Re-inspect bCommit1, which has been updated by StartCommit
		bCommit1, bCommit2 := inspect("B", bCommit1ID), inspect("B", "master")
		require.Equal(t, bCommit1.Commit.ID, bCommit2.ParentCommit.ID)
		require.ElementsEqualUnderFn(t, []string{bCommit2.Commit.ID}, bCommit1.ChildCommits, CommitToID)

		// create a new branch in a different repo, then update it so that two commits
		// are generated. Make sure the second commit is in the parent's children
		require.NoError(t, env.PachClient.CreateRepo("C"))
		require.NoError(t, env.PachClient.CreateBranch("C", "master", "", []*pfs.Branch{
			pclient.NewBranch("A", "master"),
		}))
		cCommit1ID := inspect("C", "master").Commit.ID // Get new commit's ID
		require.NoError(t, env.PachClient.CreateBranch("C", "master", "master", []*pfs.Branch{
			pclient.NewBranch("A", "master"),
			pclient.NewBranch("B", "master"),
		}))
		// Re-inspect cCommit1, which has been updated by CreateBranch
		cCommit1, cCommit2 := inspect("C", cCommit1ID), inspect("C", "master")
		require.Equal(t, cCommit1.Commit.ID, cCommit2.ParentCommit.ID)
		require.ElementsEqualUnderFn(t, []string{cCommit2.Commit.ID}, cCommit1.ChildCommits, CommitToID)

		return nil
	}))
}

func TestStartCommitFork(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		require.NoError(t, env.PachClient.CreateRepo("A"))
		require.NoError(t, env.PachClient.CreateBranch("A", "master", "", nil))
		commit, err := env.PachClient.StartCommit("A", "master")
		require.NoError(t, err)
		env.PachClient.FinishCommit("A", commit.ID)
		commit2, err := env.PachClient.PfsAPIClient.StartCommit(env.PachClient.Ctx(), &pfs.StartCommitRequest{
			Branch: "master2",
			Parent: pclient.NewCommit("A", "master"),
		})
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("A", commit2.ID))

		commits, err := env.PachClient.ListCommit("A", "master2", "", 0)
		require.NoError(t, err)
		require.ElementsEqualUnderFn(t, []string{commit.ID, commit2.ID}, commits, CommitInfoToID)

		return nil
	}))
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
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		require.NoError(t, env.PachClient.CreateRepo("A"))
		require.NoError(t, env.PachClient.CreateRepo("B"))
		require.NoError(t, env.PachClient.CreateRepo("C"))
		require.NoError(t, env.PachClient.CreateBranch("A", "master", "", nil))
		require.NoError(t, env.PachClient.CreateBranch("B", "master", "", nil))
		require.NoError(t, env.PachClient.CreateBranch("C", "master", "",
			[]*pfs.Branch{pclient.NewBranch("A", "master")}))

		// Create commits in A and B
		commit, err := env.PachClient.StartCommit("A", "master")
		require.NoError(t, err)
		env.PachClient.FinishCommit("A", commit.ID)
		commit, err = env.PachClient.StartCommit("B", "master")
		require.NoError(t, err)
		env.PachClient.FinishCommit("A", commit.ID)

		// Check for first output commit in C
		commits, err := env.PachClient.ListCommit("C", "master", "", 0)
		require.NoError(t, err)
		require.Equal(t, 1, len(commits))

		// Update the provenance of C/master and make sure it creates a new commit
		require.NoError(t, env.PachClient.CreateBranch("C", "master", "master",
			[]*pfs.Branch{pclient.NewBranch("B", "master")}))
		commits, err = env.PachClient.ListCommit("C", "master", "", 0)
		require.NoError(t, err)
		require.Equal(t, 2, len(commits))

		return nil
	}))
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
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		// two input repos, one with many commits (logs), and one with few (schema)
		require.NoError(t, env.PachClient.CreateRepo("logs"))
		require.NoError(t, env.PachClient.CreateRepo("schema"))

		// Commit to logs and schema (so that "pipeline" has an initial output commit,
		// and we can check that it updates this initial commit's child appropriately)
		for _, repo := range []string{"schema", "logs"} {
			commit, err := env.PachClient.StartCommit(repo, "master")
			require.NoError(t, err)
			require.NoError(t, env.PachClient.FinishCommit(repo, commit.ID))
		}

		// Create an output branch, in "pipeline"
		require.NoError(t, env.PachClient.CreateRepo("pipeline"))
		require.NoError(t, env.PachClient.CreateBranch("pipeline", "master", "", []*pfs.Branch{
			pclient.NewBranch("schema", "master"),
			pclient.NewBranch("logs", "master"),
		}))
		commits, err := env.PachClient.ListCommit("pipeline", "master", "", 0)
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
		require.NoError(t, env.PachClient.FinishCommit("schema", bigSubvCommit.ID))
		for i := 0; i < 10; i++ {
			commit, err := env.PachClient.StartCommit("logs", "master")
			require.NoError(t, err)
			require.NoError(t, env.PachClient.FinishCommit("logs", commit.ID))
		}
		commit, err := env.PachClient.StartCommit("schema", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("schema", commit.ID))

		// Make sure there are 13 output commits in 'pipeline' to start (one from
		// creation, one from the second 'schema' commit, 10 from the 'logs' commits,
		// and one more from the third 'schema' commit)
		commits, err = env.PachClient.ListCommit("pipeline", "master", "", 0)
		require.NoError(t, err)
		require.Equal(t, 13, len(commits))

		require.NoError(t, env.PachClient.DeleteCommit("schema", bigSubvCommit.ID))

		commits, err = env.PachClient.ListCommit("pipeline", "master", "", 0)
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
		commits, err = env.PachClient.ListCommit("schema", "master", "", 0)
		require.NoError(t, err)
		require.Equal(t, 2, len(commits))
		bigSubvCommitInfo, err := env.PachClient.InspectCommit("schema", "master")
		require.NoError(t, err)
		bigSubvCommit = bigSubvCommitInfo.Commit
		for i := 0; i < 10; i++ {
			commit, err = env.PachClient.StartCommit("logs", "master")
			require.NoError(t, err)
			require.NoError(t, env.PachClient.FinishCommit("logs", commit.ID))
		}

		require.NoError(t, env.PachClient.DeleteCommit("schema", bigSubvCommit.ID))

		commits, err = env.PachClient.ListCommit("pipeline", "master", "", 0)
		require.NoError(t, err)
		require.Equal(t, 2, len(commits))
		pipelineMaster, err := env.PachClient.InspectCommit("pipeline", "master")
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
		commits, err = env.PachClient.ListCommit("schema", "master", "", 0)
		require.NoError(t, err)
		require.Equal(t, 1, len(commits))
		bigSubvCommit = commits[0].Commit

		for i := 0; i < 10; i++ {
			commit, err = env.PachClient.StartCommit("logs", "master")
			require.NoError(t, err)
			require.NoError(t, env.PachClient.FinishCommit("logs", commit.ID))
		}

		require.NoError(t, env.PachClient.DeleteCommit("schema", bigSubvCommit.ID))

		commits, err = env.PachClient.ListCommit("pipeline", "master", "", 0)
		require.NoError(t, err)
		require.Equal(t, 0, len(commits))

		// Delete all input commits--DeleteCommit should reset 'pipeline/master' to
		// nil, and should not create a new output commit this time
		commits, err = env.PachClient.ListCommit("schema", "master", "", 0)
		require.NoError(t, err)
		for _, commitInfo := range commits {
			require.NoError(t, env.PachClient.DeleteCommit("schema", commitInfo.Commit.ID))
		}
		commits, err = env.PachClient.ListCommit("logs", "master", "", 0)
		require.NoError(t, err)
		for _, commitInfo := range commits {
			require.NoError(t, env.PachClient.DeleteCommit("logs", commitInfo.Commit.ID))
		}
		commits, err = env.PachClient.ListCommit("pipeline", "master", "", 0)
		require.NoError(t, err)
		require.Equal(t, 0, len(commits))
		_, err = env.PachClient.InspectCommit("pipeline", "master")
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
		commits, err = env.PachClient.ListCommit("schema", "master", "", 0)
		require.NoError(t, err)
		require.Equal(t, 0, len(commits))
		bigSubvCommit, err = env.PachClient.StartCommit("schema", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("schema", bigSubvCommit.ID))
		for i := 0; i < 10; i++ {
			commit, err = env.PachClient.StartCommit("logs", "master")
			require.NoError(t, err)
			require.NoError(t, env.PachClient.FinishCommit("logs", commit.ID))
		}
		commit, err = env.PachClient.StartCommit("schema", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("schema", commit.ID))

		require.NoError(t, env.PachClient.DeleteCommit("schema", bigSubvCommit.ID))

		commits, err = env.PachClient.ListCommit("pipeline", "master", "", 0)
		require.NoError(t, err)
		require.Equal(t, 1, len(commits))
		pipelineMaster, err = env.PachClient.InspectCommit("pipeline", "master")
		require.NoError(t, err)
		require.Nil(t, pipelineMaster.ParentCommit)

		return nil
	}))
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
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		require.NoError(t, env.PachClient.CreateRepo("repo"))
		require.NoError(t, env.PachClient.CreateBranch("repo", "master", "", nil))

		// Create commits 'a' and 'b'
		a, err := env.PachClient.StartCommit("repo", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("repo", a.ID))
		b, err := env.PachClient.StartCommit("repo", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("repo", b.ID))

		// Create second branch
		require.NoError(t, env.PachClient.CreateBranch("repo", "master2", "master", nil))

		// Create commits 'c' and 'd'
		c, err := env.PachClient.StartCommit("repo", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("repo", c.ID))
		d, err := env.PachClient.StartCommit("repo", "master2")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("repo", d.ID))

		// Collect info re: a, b, c, and d, and make sure that the parent/child
		// relationships are all correct
		aInfo, err := env.PachClient.InspectCommit("repo", a.ID)
		require.NoError(t, err)
		bInfo, err := env.PachClient.InspectCommit("repo", b.ID)
		require.NoError(t, err)
		cInfo, err := env.PachClient.InspectCommit("repo", c.ID)
		require.NoError(t, err)
		dInfo, err := env.PachClient.InspectCommit("repo", d.ID)
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
		env.PachClient.DeleteCommit("repo", b.ID)

		// Collect info re: a, c, and d, and make sure that the parent/child
		// relationships are still correct
		aInfo, err = env.PachClient.InspectCommit("repo", a.ID)
		require.NoError(t, err)
		cInfo, err = env.PachClient.InspectCommit("repo", c.ID)
		require.NoError(t, err)
		dInfo, err = env.PachClient.InspectCommit("repo", d.ID)
		require.NoError(t, err)

		require.Nil(t, aInfo.ParentCommit)
		require.ElementsEqualUnderFn(t, []string{c.ID, d.ID}, aInfo.ChildCommits, CommitToID)

		require.Equal(t, a.ID, cInfo.ParentCommit.ID)
		require.Equal(t, 0, len(cInfo.ChildCommits))

		require.Equal(t, a.ID, dInfo.ParentCommit.ID)
		require.Equal(t, 0, len(dInfo.ChildCommits))

		return nil
	}))
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
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		require.NoError(t, env.PachClient.CreateRepo("upstream1"))
		require.NoError(t, env.PachClient.CreateRepo("upstream2"))
		// commit to both inputs
		_, err := env.PachClient.StartCommit("upstream1", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("upstream1", "master"))
		deleteMeCommit, err := env.PachClient.StartCommit("upstream2", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("upstream2", "master"))

		// Create main repo (will have the commit graphs above
		require.NoError(t, env.PachClient.CreateRepo("repo"))
		require.NoError(t, env.PachClient.CreateBranch("repo", "master", "", []*pfs.Branch{
			pclient.NewBranch("upstream1", "master"),
			pclient.NewBranch("upstream2", "master"),
		}))

		// Create commit 'a'
		aInfo, err := env.PachClient.InspectCommit("repo", "master")
		require.NoError(t, err)
		a := aInfo.Commit
		require.NoError(t, env.PachClient.FinishCommit("repo", a.ID))

		// Create 'd'
		resp, err := env.PachClient.PfsAPIClient.StartCommit(env.PachClient.Ctx(), &pfs.StartCommitRequest{
			Parent: pclient.NewCommit("repo", a.ID),
		})
		require.NoError(t, err)
		d := pclient.NewCommit("repo", resp.ID)
		require.NoError(t, env.PachClient.FinishCommit("repo", resp.ID))

		// Create 'b'
		// (commit to upstream1, so that a & b have same prov commit in upstream2)
		_, err = env.PachClient.StartCommit("upstream1", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("upstream1", "master"))
		bInfo, err := env.PachClient.InspectCommit("repo", "master")
		require.NoError(t, err)
		b := bInfo.Commit
		require.NoError(t, env.PachClient.FinishCommit("repo", b.ID))

		// Create 'e'
		resp, err = env.PachClient.PfsAPIClient.StartCommit(env.PachClient.Ctx(), &pfs.StartCommitRequest{
			Parent: pclient.NewCommit("repo", b.ID),
		})
		require.NoError(t, err)
		e := pclient.NewCommit("repo", resp.ID)
		require.NoError(t, env.PachClient.FinishCommit("repo", resp.ID))

		// Create 'c'
		// (commit to upstream1, so that a, b & c have same prov commit in upstream2)
		_, err = env.PachClient.StartCommit("upstream1", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("upstream1", "master"))
		cInfo, err := env.PachClient.InspectCommit("repo", "master")
		require.NoError(t, err)
		c := cInfo.Commit
		require.NoError(t, env.PachClient.FinishCommit("repo", c.ID))

		// Create 'f'
		resp, err = env.PachClient.PfsAPIClient.StartCommit(env.PachClient.Ctx(), &pfs.StartCommitRequest{
			Parent: pclient.NewCommit("repo", c.ID),
		})
		require.NoError(t, err)
		f := pclient.NewCommit("repo", resp.ID)
		require.NoError(t, env.PachClient.FinishCommit("repo", resp.ID))

		// Make sure child/parent relationships are as shown in first diagram
		commits, err := env.PachClient.ListCommit("repo", "", "", 0)
		require.NoError(t, err)
		require.Equal(t, 6, len(commits))
		aInfo, err = env.PachClient.InspectCommit("repo", a.ID)
		require.NoError(t, err)
		bInfo, err = env.PachClient.InspectCommit("repo", b.ID)
		require.NoError(t, err)
		cInfo, err = env.PachClient.InspectCommit("repo", c.ID)
		require.NoError(t, err)
		dInfo, err := env.PachClient.InspectCommit("repo", d.ID)
		require.NoError(t, err)
		eInfo, err := env.PachClient.InspectCommit("repo", e.ID)
		require.NoError(t, err)
		fInfo, err := env.PachClient.InspectCommit("repo", f.ID)
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
		require.NoError(t, env.PachClient.DeleteCommit("upstream2", deleteMeCommit.ID))

		// Re-read commit info to get new parents/children
		dInfo, err = env.PachClient.InspectCommit("repo", d.ID)
		require.NoError(t, err)
		eInfo, err = env.PachClient.InspectCommit("repo", e.ID)
		require.NoError(t, err)
		fInfo, err = env.PachClient.InspectCommit("repo", f.ID)
		require.NoError(t, err)

		// Make sure child/parent relationships are as shown in second diagram
		commits, err = env.PachClient.ListCommit("repo", "", "", 0)
		require.NoError(t, err)
		require.Equal(t, 3, len(commits))
		require.Nil(t, eInfo.ParentCommit)
		require.Nil(t, fInfo.ParentCommit)
		require.Nil(t, dInfo.ChildCommits)
		require.Nil(t, eInfo.ChildCommits)
		require.Nil(t, fInfo.ChildCommits)

		return nil
	}))
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
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		require.NoError(t, env.PachClient.CreateRepo("upstream1"))
		require.NoError(t, env.PachClient.CreateRepo("upstream2"))
		// commit to both inputs
		_, err := env.PachClient.StartCommit("upstream1", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("upstream1", "master"))
		_, err = env.PachClient.StartCommit("upstream2", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("upstream2", "master"))

		// Create main repo (will have the commit graphs above
		require.NoError(t, env.PachClient.CreateRepo("repo"))
		require.NoError(t, env.PachClient.CreateBranch("repo", "master", "", []*pfs.Branch{
			pclient.NewBranch("upstream1", "master"),
			pclient.NewBranch("upstream2", "master"),
		}))

		// Create commit 'a'
		aInfo, err := env.PachClient.InspectCommit("repo", "master")
		require.NoError(t, err)
		a := aInfo.Commit
		require.NoError(t, env.PachClient.FinishCommit("repo", a.ID))

		// Create 'd'
		resp, err := env.PachClient.PfsAPIClient.StartCommit(env.PachClient.Ctx(), &pfs.StartCommitRequest{
			Parent: pclient.NewCommit("repo", a.ID),
		})
		require.NoError(t, err)
		d := pclient.NewCommit("repo", resp.ID)
		require.NoError(t, env.PachClient.FinishCommit("repo", resp.ID))

		// Create 'b'
		// (a & b have same prov commit in upstream2, so this is the commit that will
		// be deleted, as both b and c are provenant on it)
		deleteMeCommit, err := env.PachClient.StartCommit("upstream1", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("upstream1", "master"))
		bInfo, err := env.PachClient.InspectCommit("repo", "master")
		require.NoError(t, err)
		b := bInfo.Commit
		require.NoError(t, env.PachClient.FinishCommit("repo", b.ID))

		// Create 'e'
		resp, err = env.PachClient.PfsAPIClient.StartCommit(env.PachClient.Ctx(), &pfs.StartCommitRequest{
			Parent: pclient.NewCommit("repo", b.ID),
		})
		require.NoError(t, err)
		e := pclient.NewCommit("repo", resp.ID)
		require.NoError(t, env.PachClient.FinishCommit("repo", resp.ID))

		// Create 'c'
		// (commit to upstream2, so that b & c have same prov commit in upstream1)
		_, err = env.PachClient.StartCommit("upstream2", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("upstream2", "master"))
		cInfo, err := env.PachClient.InspectCommit("repo", "master")
		require.NoError(t, err)
		c := cInfo.Commit
		require.NoError(t, env.PachClient.FinishCommit("repo", c.ID))

		// Create 'f'
		resp, err = env.PachClient.PfsAPIClient.StartCommit(env.PachClient.Ctx(), &pfs.StartCommitRequest{
			Parent: pclient.NewCommit("repo", c.ID),
		})
		require.NoError(t, err)
		f := pclient.NewCommit("repo", resp.ID)
		require.NoError(t, env.PachClient.FinishCommit("repo", resp.ID))

		// Make sure child/parent relationships are as shown in first diagram
		commits, err := env.PachClient.ListCommit("repo", "", "", 0)
		require.NoError(t, err)
		require.Equal(t, 6, len(commits))
		aInfo, err = env.PachClient.InspectCommit("repo", a.ID)
		require.NoError(t, err)
		bInfo, err = env.PachClient.InspectCommit("repo", b.ID)
		require.NoError(t, err)
		cInfo, err = env.PachClient.InspectCommit("repo", c.ID)
		require.NoError(t, err)
		dInfo, err := env.PachClient.InspectCommit("repo", d.ID)
		require.NoError(t, err)
		eInfo, err := env.PachClient.InspectCommit("repo", e.ID)
		require.NoError(t, err)
		fInfo, err := env.PachClient.InspectCommit("repo", f.ID)
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
		require.NoError(t, env.PachClient.DeleteCommit("upstream1", deleteMeCommit.ID))

		// Re-read commit info to get new parents/children
		aInfo, err = env.PachClient.InspectCommit("repo", a.ID)
		require.NoError(t, err)
		dInfo, err = env.PachClient.InspectCommit("repo", d.ID)
		require.NoError(t, err)
		eInfo, err = env.PachClient.InspectCommit("repo", e.ID)
		require.NoError(t, err)
		fInfo, err = env.PachClient.InspectCommit("repo", f.ID)
		require.NoError(t, err)

		// Make sure child/parent relationships are as shown in second diagram. Note
		// that after 'b' and 'c' are deleted, DeleteCommit creates a new commit:
		// - 'repo/master' points to 'a'
		// - DeleteCommit starts a new output commit to process 'upstream1/master'
		//   and 'upstream2/master'
		// - The new output commit is started in 'repo/master' and is also a child of
		//   'a'
		commits, err = env.PachClient.ListCommit("repo", "", "", 0)
		require.NoError(t, err)
		require.Equal(t, 5, len(commits))
		require.Nil(t, aInfo.ParentCommit)
		require.Equal(t, a.ID, dInfo.ParentCommit.ID)
		require.Equal(t, a.ID, eInfo.ParentCommit.ID)
		require.Equal(t, a.ID, fInfo.ParentCommit.ID)
		newCommitInfo, err := env.PachClient.InspectCommit("repo", "master")
		require.NoError(t, err)
		require.ElementsEqualUnderFn(t,
			[]string{d.ID, e.ID, f.ID, newCommitInfo.Commit.ID},
			aInfo.ChildCommits, CommitToID)
		require.Nil(t, dInfo.ChildCommits)
		require.Nil(t, eInfo.ChildCommits)
		require.Nil(t, fInfo.ChildCommits)

		return nil
	}))
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
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		// two input repos, one with many commits (logs), and one with few (schema)
		require.NoError(t, env.PachClient.CreateRepo("logs"))
		require.NoError(t, env.PachClient.CreateRepo("schema"))

		// Commit to logs and schema
		logsCommit := make([]*pfs.Commit, 10)
		var err error
		logsCommit[0], err = env.PachClient.StartCommit("logs", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("logs", logsCommit[0].ID))
		schemaCommit, err := env.PachClient.StartCommit("schema", "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.FinishCommit("schema", schemaCommit.ID))

		// Create an output branch, in "pipeline"
		require.NoError(t, env.PachClient.CreateRepo("pipeline"))
		require.NoError(t, env.PachClient.CreateBranch("pipeline", "master", "", []*pfs.Branch{
			pclient.NewBranch("schema", "master"),
			pclient.NewBranch("logs", "master"),
		}))
		commits, err := env.PachClient.ListCommit("pipeline", "master", "", 0)
		require.NoError(t, err)
		require.Equal(t, 1, len(commits))

		// Commit to "logs" 9 more times, so that the commit to "schema" has 10
		// commits in its subvenance
		for i := 1; i < 10; i++ {
			logsCommit[i], err = env.PachClient.StartCommit("logs", "master")
			require.NoError(t, err)
			require.NoError(t, env.PachClient.FinishCommit("logs", logsCommit[i].ID))
		}
		pipelineCommitInfos, err := env.PachClient.ListCommit("pipeline", "master", "", 0)
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
		schemaCommitInfo, err := env.PachClient.InspectCommit("schema", schemaCommit.ID)
		require.NoError(t, err)
		require.Equal(t, 1, len(schemaCommitInfo.Subvenance))
		require.Equal(t, pipelineCommit[0], schemaCommitInfo.Subvenance[0].Lower.ID)
		require.Equal(t, pipelineCommit[9], schemaCommitInfo.Subvenance[0].Upper.ID)

		// Case 1
		// - Delete the first commit in "logs" and make sure that the subvenance of
		//   the single commit in "schema" has increased its Lower value
		require.NoError(t, env.PachClient.DeleteCommit("logs", logsCommit[0].ID))
		schemaCommitInfo, err = env.PachClient.InspectCommit("schema", schemaCommit.ID)
		require.NoError(t, err)
		require.Equal(t, 1, len(schemaCommitInfo.Subvenance))
		require.Equal(t, pipelineCommit[1], schemaCommitInfo.Subvenance[0].Lower.ID)
		require.Equal(t, pipelineCommit[9], schemaCommitInfo.Subvenance[0].Upper.ID)

		// Case 2
		// - Delete the last commit in "logs" and make sure that the subvenance of
		//   the single commit in "schema" has decreased its Upper value
		require.NoError(t, env.PachClient.DeleteCommit("logs", logsCommit[9].ID))
		schemaCommitInfo, err = env.PachClient.InspectCommit("schema", schemaCommit.ID)
		require.NoError(t, err)
		require.Equal(t, 1, len(schemaCommitInfo.Subvenance))
		require.Equal(t, pipelineCommit[1], schemaCommitInfo.Subvenance[0].Lower.ID)
		require.Equal(t, pipelineCommit[8], schemaCommitInfo.Subvenance[0].Upper.ID)

		// Case 3
		// - Delete the middle commit in "logs" and make sure that the subvenance of
		//   the single commit in "schema" hasn't changed
		require.NoError(t, env.PachClient.DeleteCommit("logs", logsCommit[5].ID))
		schemaCommitInfo, err = env.PachClient.InspectCommit("schema", schemaCommit.ID)
		require.NoError(t, err)
		require.Equal(t, 1, len(schemaCommitInfo.Subvenance))
		require.Equal(t, pipelineCommit[1], schemaCommitInfo.Subvenance[0].Lower.ID)
		require.Equal(t, pipelineCommit[8], schemaCommitInfo.Subvenance[0].Upper.ID)

		// Case 4
		// - Delete the remaining commits in "logs" and make sure that the subvenance
		//   of the single commit in "schema" is now empty
		for _, i := range []int{1, 2, 3, 4, 6, 7, 8} {
			require.NoError(t, env.PachClient.DeleteCommit("logs", logsCommit[i].ID))
		}
		schemaCommitInfo, err = env.PachClient.InspectCommit("schema", schemaCommit.ID)
		require.NoError(t, err)
		require.Equal(t, 0, len(schemaCommitInfo.Subvenance))

		return nil
	}))
}

func TestCommitState(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		// two input repos, one with many commits (logs), and one with few (schema)
		require.NoError(t, env.PachClient.CreateRepo("A"))
		require.NoError(t, env.PachClient.CreateRepo("B"))

		require.NoError(t, env.PachClient.CreateBranch("B", "master", "", []*pfs.Branch{pclient.NewBranch("A", "master")}))

		// Start a commit on A/master, this will create a non-ready commit on B/master.
		_, err := env.PachClient.StartCommit("A", "master")
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()
		_, err = env.PachClient.PfsAPIClient.InspectCommit(ctx, &pfs.InspectCommitRequest{
			Commit:     pclient.NewCommit("B", "master"),
			BlockState: pfs.CommitState_READY,
		})
		require.YesError(t, err)

		// Finish the commit on A/master, that will make the B/master ready.
		require.NoError(t, env.PachClient.FinishCommit("A", "master"))

		ctx, cancel = context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()
		_, err = env.PachClient.PfsAPIClient.InspectCommit(ctx, &pfs.InspectCommitRequest{
			Commit:     pclient.NewCommit("B", "master"),
			BlockState: pfs.CommitState_READY,
		})
		require.NoError(t, err)

		// Create a new branch C/master with A/master as provenance. It should start out ready.
		require.NoError(t, env.PachClient.CreateRepo("C"))
		require.NoError(t, env.PachClient.CreateBranch("C", "master", "", []*pfs.Branch{pclient.NewBranch("A", "master")}))

		ctx, cancel = context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()
		_, err = env.PachClient.PfsAPIClient.InspectCommit(ctx, &pfs.InspectCommitRequest{
			Commit:     pclient.NewCommit("C", "master"),
			BlockState: pfs.CommitState_READY,
		})
		require.NoError(t, err)

		return nil
	}))
}

func TestSubscribeStates(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		require.NoError(t, env.PachClient.CreateRepo("A"))
		require.NoError(t, env.PachClient.CreateRepo("B"))
		require.NoError(t, env.PachClient.CreateRepo("C"))

		require.NoError(t, env.PachClient.CreateBranch("B", "master", "", []*pfs.Branch{pclient.NewBranch("A", "master")}))
		require.NoError(t, env.PachClient.CreateBranch("C", "master", "", []*pfs.Branch{pclient.NewBranch("B", "master")}))

		ctx, cancel := context.WithCancel(env.PachClient.Ctx())
		defer cancel()
		client := env.PachClient.WithCtx(ctx)

		var readyCommits int64
		go func() {
			client.SubscribeCommitF("B", "master", nil, "", pfs.CommitState_READY, func(ci *pfs.CommitInfo) error {
				atomic.AddInt64(&readyCommits, 1)
				return nil
			})
		}()
		go func() {
			client.SubscribeCommitF("C", "master", nil, "", pfs.CommitState_READY, func(ci *pfs.CommitInfo) error {
				atomic.AddInt64(&readyCommits, 1)
				return nil
			})
		}()
		_, err := client.StartCommit("A", "master")
		require.NoError(t, err)
		require.NoError(t, client.FinishCommit("A", "master"))

		require.NoErrorWithinTRetry(t, time.Second*10, func() error {
			if atomic.LoadInt64(&readyCommits) != 1 {
				return errors.Errorf("wrong number of ready commits")
			}
			return nil
		})

		require.NoError(t, client.FinishCommit("B", "master"))

		require.NoErrorWithinTRetry(t, time.Second*10, func() error {
			if atomic.LoadInt64(&readyCommits) != 2 {
				return errors.Errorf("wrong number of ready commits")
			}
			return nil
		})

		return nil
	}))
}

// TODO: Make work with V2?
//func TestPutFileCommit(t *testing.T) {
//	t.Parallel()
//	db := dbutil.NewTestDB(t)
//	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
//		numFiles := 25
//		repo := "repo"
//		require.NoError(t, env.PachClient.CreateRepo(repo))
//
//		var eg errgroup.Group
//		for i := 0; i < numFiles; i++ {
//			i := i
//			eg.Go(func() error {
//				return env.PachClient.PutFile(repo, "master", fmt.Sprintf("%d", i), strings.NewReader(fmt.Sprintf("%d", i)))
//			})
//		}
//		require.NoError(t, eg.Wait())
//
//		for i := 0; i < numFiles; i++ {
//			var b bytes.Buffer
//			require.NoError(t, env.PachClient.GetFile(repo, "master", fmt.Sprintf("%d", i), &b))
//			require.Equal(t, fmt.Sprintf("%d", i), b.String())
//		}
//
//		bi, err := env.PachClient.InspectBranch(repo, "master")
//		require.NoError(t, err)
//
//		eg = errgroup.Group{}
//		for i := 0; i < numFiles; i++ {
//			i := i
//			eg.Go(func() error {
//				return env.PachClient.CopyFile(repo, bi.Head.ID, fmt.Sprintf("%d", i), repo, "master", fmt.Sprintf("%d", (i+1)%numFiles), true)
//			})
//		}
//		require.NoError(t, eg.Wait())
//
//		for i := 0; i < numFiles; i++ {
//			var b bytes.Buffer
//			require.NoError(t, env.PachClient.GetFile(repo, "master", fmt.Sprintf("%d", (i+1)%numFiles), &b))
//			require.Equal(t, fmt.Sprintf("%d", i), b.String())
//		}
//
//		eg = errgroup.Group{}
//		for i := 0; i < numFiles; i++ {
//			i := i
//			eg.Go(func() error {
//				return env.PachClient.DeleteFile(repo, "master", fmt.Sprintf("%d", i))
//			})
//		}
//		require.NoError(t, eg.Wait())
//
//		fileInfos, err := env.PachClient.ListFileAll(repo, "master", "")
//		require.NoError(t, err)
//		require.Equal(t, 0, len(fileInfos))
//
//		return nil
//	}))
//}

func TestPutFileCommitNilBranch(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		repo := "repo"
		require.NoError(t, env.PachClient.CreateRepo(repo))
		require.NoError(t, env.PachClient.CreateBranch(repo, "master", "", nil))

		require.NoError(t, env.PachClient.PutFile(repo, "master", "file", strings.NewReader("file")))

		return nil
	}))
}

func TestPutFileCommitOverwrite(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		numFiles := 5
		repo := "repo"
		require.NoError(t, env.PachClient.CreateRepo(repo))

		for i := 0; i < numFiles; i++ {
			require.NoError(t, env.PachClient.PutFileOverwrite(repo, "master", "file", strings.NewReader(fmt.Sprintf("%d", i))))
		}

		var b bytes.Buffer
		require.NoError(t, env.PachClient.GetFile(repo, "master", "file", &b))
		require.Equal(t, fmt.Sprintf("%d", numFiles-1), b.String())

		return nil
	}))
}

func TestStartCommitOutputBranch(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		require.NoError(t, env.PachClient.CreateRepo("in"))
		require.NoError(t, env.PachClient.CreateRepo("out"))
		require.NoError(t, env.PachClient.CreateBranch("out", "master", "", []*pfs.Branch{pclient.NewBranch("in", "master")}))
		_, err := env.PachClient.StartCommit("out", "master")
		require.YesError(t, err)

		return nil
	}))
}

func TestWalkFile(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		repo := "TestWalkFile"
		require.NoError(t, env.PachClient.CreateRepo(repo))
		require.NoError(t, env.PachClient.PutFile(repo, "master", "dir/bar", strings.NewReader("bar")))
		require.NoError(t, env.PachClient.PutFile(repo, "master", "dir/dir2/buzz", strings.NewReader("buzz")))
		require.NoError(t, env.PachClient.PutFile(repo, "master", "foo", strings.NewReader("foo")))

		expectedPaths := []string{"/", "/dir/", "/dir/bar", "/dir/dir2/", "/dir/dir2/buzz", "/foo"}
		i := 0
		require.NoError(t, env.PachClient.WalkFile(repo, "master", "", func(fi *pfs.FileInfo) error {
			require.Equal(t, expectedPaths[i], fi.File.Path)
			i++
			return nil
		}))
		require.Equal(t, len(expectedPaths), i)

		return nil
	}))
}

func TestWalkFile2(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		repo := "TestWalkFile2"
		require.NoError(t, env.PachClient.CreateRepo(repo))
		commit1, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		require.NoError(t, env.PachClient.PutFile(repo, commit1.ID, "/dir1/file1.1", &bytes.Buffer{}))
		require.NoError(t, env.PachClient.PutFile(repo, commit1.ID, "/dir1/file1.2", &bytes.Buffer{}))
		require.NoError(t, env.PachClient.PutFile(repo, commit1.ID, "/dir2/file2.1", &bytes.Buffer{}))
		require.NoError(t, env.PachClient.PutFile(repo, commit1.ID, "/dir2/file2.2", &bytes.Buffer{}))
		require.NoError(t, env.PachClient.FinishCommit(repo, commit1.ID))
		walkFile := func(path string) []string {
			var fis []*pfs.FileInfo
			require.NoError(t, env.PachClient.WalkFile(repo, commit1.ID, path, func(fi *pfs.FileInfo) error {
				fis = append(fis, fi)
				return nil
			}))
			return finfosToPaths(fis)
		}
		assert.ElementsMatch(t, []string{"/dir1/", "/dir1/file1.1", "/dir1/file1.2"}, walkFile("/dir1"))
		assert.ElementsMatch(t, []string{"/dir1/file1.1"}, walkFile("/dir1/file1.1"))
		assert.Len(t, walkFile("/"), 7)
		return nil
	}))
}

// TODO: Make work with V2?
//func TestReadSizeLimited(t *testing.T) {
//	t.Parallel()
//	db := dbutil.NewTestDB(t)
//	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
//		require.NoError(t, env.PachClient.CreateRepo("test"))
//		require.NoError(t, env.PachClient.PutFile("test", "master", "file", strings.NewReader(strings.Repeat("a", 100*units.MB))))
//
//		var b bytes.Buffer
//		require.NoError(t, env.PachClient.GetFile("test", "master", "file", 0, 2*units.MB, &b))
//		require.Equal(t, 2*units.MB, b.Len())
//
//		b.Reset()
//		require.NoError(t, env.PachClient.GetFile("test", "master", "file", 2*units.MB, 2*units.MB, &b))
//		require.Equal(t, 2*units.MB, b.Len())
//
//		return nil
//	}))
//}

// TODO: Make work with V2?
//func TestPutFiles(t *testing.T) {
//	t.Parallel()
//	db := dbutil.NewTestDB(t)
//	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
//		require.NoError(t, env.PachClient.CreateRepo("repo"))
//		pfclient, err := env.PachClient.PfsAPIClient.PutFile(context.Background())
//		require.NoError(t, err)
//		paths := []string{"foo", "bar", "fizz", "buzz"}
//		for _, path := range paths {
//			require.NoError(t, pfclient.Send(&pfs.PutFileRequest{
//				File:  pclient.NewFile("repo", "master", path),
//				Value: []byte(path),
//			}))
//		}
//		_, err = pfclient.CloseAndRecv()
//		require.NoError(t, err)
//
//		cis, err := env.PachClient.ListCommit("repo", "", "", 0)
//		require.NoError(t, err)
//		require.Equal(t, 1, len(cis))
//
//		for _, path := range paths {
//			var b bytes.Buffer
//			require.NoError(t, env.PachClient.GetFile("repo", "master", path, &b))
//			require.Equal(t, path, b.String())
//		}
//
//		return nil
//	}))
//}

func TestPutFilesURL(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		repo := "repo"
		require.NoError(t, env.PachClient.CreateRepo(repo))
		commit, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		paths := []string{"README.md", "CHANGELOG.md", "CONTRIBUTING.md"}
		for _, path := range paths {
			url := fmt.Sprintf("https://raw.githubusercontent.com/pachyderm/pachyderm/master/%s", path)
			require.NoError(t, env.PachClient.PutFileURL(repo, commit.ID, path, url, false, false))
		}
		require.NoError(t, env.PachClient.FinishCommit(repo, commit.ID))

		cis, err := env.PachClient.ListCommit("repo", "", "", 0)
		require.NoError(t, err)
		require.Equal(t, 1, len(cis))

		for _, path := range paths {
			fileInfo, err := env.PachClient.InspectFile("repo", "master", path)
			require.NoError(t, err)
			require.True(t, fileInfo.SizeBytes > 0)
		}

		return nil
	}))
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
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
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

		repo := "repo"
		require.NoError(t, env.PachClient.CreateRepo(repo))
		commit, err := env.PachClient.StartCommit(repo, "master")
		require.NoError(t, err)
		for _, path := range paths {
			url := fmt.Sprintf("local://%s/%s", wd, path)
			require.NoError(t, env.PachClient.PutFileURL(repo, commit.ID, path, url, false, false))
		}
		url := fmt.Sprintf("local://%s/files", wd)
		require.NoError(t, env.PachClient.PutFileURL(repo, commit.ID, "recursive", url, true, false))
		require.NoError(t, env.PachClient.FinishCommit(repo, commit.ID))

		cis, err := env.PachClient.ListCommit("repo", "", "", 0)
		require.NoError(t, err)
		require.Equal(t, 1, len(cis))

		for _, path := range paths {
			var b bytes.Buffer
			require.NoError(t, env.PachClient.GetFile("repo", "master", path, &b))
			require.Equal(t, path, b.String())
			b.Reset()
			require.NoError(t, env.PachClient.GetFile("repo", "master", filepath.Join("recursive", filepath.Base(path)), &b))
			require.Equal(t, path, b.String())
		}

		return nil
	}))
}

func TestPutFileOutputRepo(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		inputRepo, outputRepo := "input", "output"
		require.NoError(t, env.PachClient.CreateRepo(inputRepo))
		require.NoError(t, env.PachClient.CreateRepo(outputRepo))
		require.NoError(t, env.PachClient.CreateBranch(outputRepo, "master", "", []*pfs.Branch{pclient.NewBranch(inputRepo, "master")}))
		require.NoError(t, env.PachClient.PutFile(inputRepo, "master", "foo", strings.NewReader("foo\n")))
		require.NoError(t, env.PachClient.PutFile(outputRepo, "master", "bar", strings.NewReader("bar\n")))
		require.NoError(t, env.PachClient.FinishCommit(outputRepo, "master"))
		fileInfos, err := env.PachClient.ListFileAll(outputRepo, "master", "")
		require.NoError(t, err)
		require.Equal(t, 1, len(fileInfos))
		buf := &bytes.Buffer{}
		require.NoError(t, env.PachClient.GetFile(outputRepo, "master", "bar", buf))
		require.Equal(t, "bar\n", buf.String())

		return nil
	}))
}

// TODO: Make work with V2?
//func TestFileHistory(t *testing.T) {
//	t.Parallel()
//	db := dbutil.NewTestDB(t)
//	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
//		var err error
//
//		repo := "test"
//		require.NoError(t, env.PachClient.CreateRepo(repo))
//		numCommits := 10
//		for i := 0; i < numCommits; i++ {
//			require.NoError(t, env.PachClient.PutFile(repo, "master", "file", strings.NewReader("foo\n")))
//			require.NoError(t, err)
//		}
//		fileInfos, err := env.PachClient.ListFileHistory(repo, "master", "file", -1)
//		require.NoError(t, err)
//		require.Equal(t, numCommits, len(fileInfos))
//
//		for i := 1; i < numCommits; i++ {
//			fileInfos, err := env.PachClient.ListFileHistory(repo, "master", "file", int64(i))
//			require.NoError(t, err)
//			require.Equal(t, i, len(fileInfos))
//		}
//
//		require.NoError(t, env.PachClient.DeleteFile(repo, "master", "file"))
//		for i := 0; i < numCommits; i++ {
//			require.NoError(t, env.PachClient.PutFile(repo, "master", "file", strings.NewReader("foo\n")))
//			require.NoError(t, err)
//			require.NoError(t, env.PachClient.PutFile(repo, "master", "unrelated", strings.NewReader("foo\n")))
//			require.NoError(t, err)
//		}
//		fileInfos, err = env.PachClient.ListFileHistory(repo, "master", "file", -1)
//		require.NoError(t, err)
//		require.Equal(t, numCommits, len(fileInfos))
//
//		for i := 1; i < numCommits; i++ {
//			fileInfos, err := env.PachClient.ListFileHistory(repo, "master", "file", int64(i))
//			require.NoError(t, err)
//			require.Equal(t, i, len(fileInfos))
//		}
//
//		return nil
//	}))
//}

func TestUpdateRepo(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
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

		return nil
	}))
}

func TestDeferredProcessing(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		require.NoError(t, env.PachClient.CreateRepo("input"))
		require.NoError(t, env.PachClient.CreateRepo("output1"))
		require.NoError(t, env.PachClient.CreateRepo("output2"))
		require.NoError(t, env.PachClient.CreateBranch("output1", "staging", "", []*pfs.Branch{pclient.NewBranch("input", "master")}))
		require.NoError(t, env.PachClient.CreateBranch("output2", "staging", "", []*pfs.Branch{pclient.NewBranch("output1", "master")}))
		require.NoError(t, env.PachClient.PutFile("input", "staging", "file", strings.NewReader("foo")))

		commits, err := env.PachClient.FlushCommitAll([]*pfs.Commit{pclient.NewCommit("input", "staging")}, nil)
		require.NoError(t, err)
		require.Equal(t, 0, len(commits))

		require.NoError(t, env.PachClient.CreateBranch("input", "master", "staging", nil))
		require.NoError(t, env.PachClient.FinishCommit("output1", "staging"))
		commits, err = env.PachClient.FlushCommitAll([]*pfs.Commit{pclient.NewCommit("input", "staging")}, nil)
		require.NoError(t, err)
		require.Equal(t, 1, len(commits))

		require.NoError(t, env.PachClient.CreateBranch("output1", "master", "staging", nil))
		require.NoError(t, env.PachClient.FinishCommit("output2", "staging"))
		commits, err = env.PachClient.FlushCommitAll([]*pfs.Commit{pclient.NewCommit("input", "staging")}, nil)
		require.NoError(t, err)
		require.Equal(t, 2, len(commits))

		return nil
	}))
}

// TestMultiInputWithDeferredProcessing tests this DAG:
//
// input1 ─▶ deferred-output ─▶ final-output
//                              ▲
// input2 ──────────────────────╯
//
// For this test to pass, commits in 'final-output' must include commits from
// the provenance of 'deferred-output', *even if 'deferred-output@master' isn't
// the branch being propagated*
func TestMultiInputWithDeferredProcessing(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		require.NoError(t, env.PachClient.CreateRepo("input1"))
		require.NoError(t, env.PachClient.CreateRepo("deferred-output"))
		require.NoError(t, env.PachClient.CreateRepo("input2"))
		require.NoError(t, env.PachClient.CreateRepo("final-output"))
		require.NoError(t, env.PachClient.CreateBranch("deferred-output", "staging", "",
			[]*pfs.Branch{pclient.NewBranch("input1", "master")}))
		require.NoError(t, env.PachClient.CreateBranch("final-output", "master", "",
			[]*pfs.Branch{
				pclient.NewBranch("input2", "master"),
				pclient.NewBranch("deferred-output", "master"),
			}))
		require.NoError(t, env.PachClient.PutFile("input1", "master", "1", strings.NewReader("1")))
		require.NoError(t, env.PachClient.PutFile("input2", "master", "2", strings.NewReader("2")))

		// There should be an open commit in "staging" but not "master"
		cis, err := env.PachClient.ListCommit("deferred-output", "staging", "", 0)
		require.NoError(t, err)
		require.Equal(t, 1, len(cis))
		require.NoError(t, env.PachClient.FinishCommit("deferred-output", "staging"))
		cis, err = env.PachClient.ListCommit("deferred-output", "master", "", 0)
		require.NoError(t, err)
		require.Equal(t, 0, len(cis))

		// There shouldn't be one output commit in "final-output@master", but with no
		// provenance in deferred-output or input1 (only in input2)
		cis, err = env.PachClient.ListCommit("final-output", "master", "", 0)
		require.NoError(t, err)
		require.Equal(t, 1, len(cis))
		require.NoError(t, env.PachClient.FinishCommit("final-output", "master"))
		ci, err := env.PachClient.InspectCommit("input2", "master")
		require.NoError(t, err)
		expectedProv := map[string]bool{
			path.Join("input2", ci.Commit.ID): true,
		}
		ci, err = env.PachClient.InspectCommit("final-output", "master")
		require.NoError(t, err)
		require.Equal(t, len(expectedProv), len(ci.Provenance))
		for _, c := range ci.Provenance {
			require.True(t, expectedProv[path.Join(c.Commit.Repo.Name, c.Commit.ID)])
		}

		// 1) Move master branch and create second output commit (first w/ full prov)
		require.NoError(t, env.PachClient.CreateBranch("deferred-output", "master", "staging", nil))
		require.NoError(t, err)
		cis, err = env.PachClient.ListCommit("final-output", "master", "", 0)
		require.NoError(t, err)
		require.Equal(t, 2, len(cis))
		require.NoError(t, env.PachClient.FinishCommit("final-output", "master"))

		// Make sure the final output (triggered by deferred-downstream) has the right
		// commit provenance
		expectedProv = make(map[string]bool)
		for _, r := range []string{"input1", "input2", "deferred-output"} {
			ci, err := env.PachClient.InspectCommit(r, "master")
			require.NoError(t, err)
			expectedProv[path.Join(r, ci.Commit.ID)] = true
		}
		ci, err = env.PachClient.InspectCommit("final-output", "master")
		require.NoError(t, err)
		require.Equal(t, len(expectedProv), len(ci.Provenance))
		for _, c := range ci.Provenance {
			require.True(t, expectedProv[path.Join(c.Commit.Repo.Name, c.Commit.ID)])
		}

		// 2) Commit to input2 and create second output commit
		require.NoError(t, env.PachClient.PutFile("input2", "master", "3", strings.NewReader("3")))
		cis, err = env.PachClient.ListCommit("final-output", "master", "", 0)
		require.NoError(t, err)
		require.Equal(t, 3, len(cis))
		require.NoError(t, env.PachClient.FinishCommit("final-output", "master"))

		// Make sure the final output (triggered by second input) has the right
		// commit provenance
		expectedProv = make(map[string]bool)
		for _, r := range []string{"input1", "input2", "deferred-output"} {
			ci, err := env.PachClient.InspectCommit(r, "master")
			require.NoError(t, err)
			expectedProv[path.Join(r, ci.Commit.ID)] = true
		}
		ci, err = env.PachClient.InspectCommit("final-output", "master")
		require.NoError(t, err)
		require.Equal(t, len(expectedProv), len(ci.Provenance))
		for _, c := range ci.Provenance {
			require.True(t, expectedProv[path.Join(c.Commit.Repo.Name, c.Commit.ID)])
		}

		return nil
	}))
}

func TestCommitProgress(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		require.NoError(t, env.PachClient.CreateRepo("in"))
		require.NoError(t, env.PachClient.CreateRepo("out"))
		require.NoError(t, env.PachClient.CreateBranch("out", "master", "", []*pfs.Branch{pclient.NewBranch("in", "master")}))
		require.NoError(t, env.PachClient.PutFile("in", "master", "foo", strings.NewReader("foo")))
		ci, err := env.PachClient.InspectCommit("in", "master")
		require.NoError(t, err)
		require.Equal(t, int64(1), ci.SubvenantCommitsTotal)
		require.Equal(t, int64(0), ci.SubvenantCommitsSuccess)
		require.Equal(t, int64(0), ci.SubvenantCommitsFailure)
		require.NoError(t, env.PachClient.FinishCommit("out", "master"))
		ci, err = env.PachClient.InspectCommit("in", "master")
		require.NoError(t, err)
		require.Equal(t, int64(1), ci.SubvenantCommitsTotal)
		require.Equal(t, int64(1), ci.SubvenantCommitsSuccess)
		require.Equal(t, int64(0), ci.SubvenantCommitsFailure)

		return nil
	}))
}

func TestListAll(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		require.NoError(t, env.PachClient.CreateRepo("repo1"))
		require.NoError(t, env.PachClient.CreateRepo("repo2"))
		require.NoError(t, env.PachClient.PutFile("repo1", "master", "file1", strings.NewReader("1")))
		require.NoError(t, env.PachClient.PutFile("repo2", "master", "file2", strings.NewReader("2")))
		require.NoError(t, env.PachClient.PutFile("repo1", "master", "file3", strings.NewReader("3")))
		require.NoError(t, env.PachClient.PutFile("repo2", "master", "file4", strings.NewReader("4")))

		cis, err := env.PachClient.ListCommit("", "", "", 0)
		require.NoError(t, err)
		require.Equal(t, 4, len(cis))

		bis, err := env.PachClient.ListBranch("")
		require.NoError(t, err)
		require.Equal(t, 2, len(bis))

		return nil
	}))
}

// TODO: Make work with V2?
//func seedStr(seed int64) string {
//	return fmt.Sprint("seed: ", strconv.FormatInt(seed, 10))
//}
//
//func monkeyRetry(t *testing.T, f func() error, errMsg string) {
//	backoff.Retry(func() error {
//		err := f()
//		if err != nil {
//			require.True(t, obj.IsMonkeyError(err), "Expected monkey error (%s), %s", err.Error(), errMsg)
//		}
//		return err
//	}, backoff.NewInfiniteBackOff())
//}
//
//func TestMonkeyObjectStorage(t *testing.T) {
//	// This test cannot be done in parallel because the monkey object client
//	// modifies global state.
//	db := dbutil.NewTestDB(t)
//	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
//		seed := time.Now().UTC().UnixNano()
//		obj.InitMonkeyTest(seed)
//		iterations := 25
//		repo := "input"
//		require.NoError(t, env.PachClient.CreateRepo(repo), seedStr(seed))
//		filePrefix := "file"
//		dataPrefix := "data"
//		var commit *pfs.Commit
//		var err error
//		buf := &bytes.Buffer{}
//		obj.EnableMonkeyTest()
//		defer obj.DisableMonkeyTest()
//		for i := 0; i < iterations; i++ {
//			file := filePrefix + strconv.Itoa(i)
//			data := dataPrefix + strconv.Itoa(i)
//			// Retry start commit until it eventually succeeds.
//			monkeyRetry(t, func() error {
//				commit, err = env.PachClient.StartCommit(repo, "")
//				return err
//			}, seedStr(seed))
//			// Retry put file until it eventually succeeds.
//			monkeyRetry(t, func() error {
//				if err := env.PachClient.PutFile(repo, commit.ID, file, strings.NewReader(data)); err != nil {
//					// Verify that the file does not exist if an error occurred.
//					obj.DisableMonkeyTest()
//					defer obj.EnableMonkeyTest()
//					buf.Reset()
//					err := env.PachClient.GetFile(repo, commit.ID, file, buf)
//					require.Matches(t, "not found", err.Error(), seedStr(seed))
//				}
//				return err
//			}, seedStr(seed))
//			// Retry get file until it eventually succeeds (before commit is finished).
//			monkeyRetry(t, func() error {
//				buf.Reset()
//				if err = env.PachClient.GetFile(repo, commit.ID, file, buf); err != nil {
//					return err
//				}
//				require.Equal(t, data, buf.String(), seedStr(seed))
//				return nil
//			}, seedStr(seed))
//			// Retry finish commit until it eventually succeeds.
//			monkeyRetry(t, func() error {
//				return env.PachClient.FinishCommit(repo, commit.ID)
//			}, seedStr(seed))
//			// Retry get file until it eventually succeeds (after commit is finished).
//			monkeyRetry(t, func() error {
//				buf.Reset()
//				if err = env.PachClient.GetFile(repo, commit.ID, file, buf); err != nil {
//					return err
//				}
//				require.Equal(t, data, buf.String(), seedStr(seed))
//				return nil
//			}, seedStr(seed))
//		}
//
//		return nil
//	}))
//}

func TestFsckFix(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		input := "input"
		output1 := "output1"
		output2 := "output2"
		require.NoError(t, env.PachClient.CreateRepo(input))
		require.NoError(t, env.PachClient.CreateRepo(output1))
		require.NoError(t, env.PachClient.CreateRepo(output2))
		require.NoError(t, env.PachClient.CreateBranch(output1, "master", "", []*pfs.Branch{pclient.NewBranch(input, "master")}))
		require.NoError(t, env.PachClient.CreateBranch(output2, "master", "", []*pfs.Branch{pclient.NewBranch(output1, "master")}))
		numCommits := 10
		for i := 0; i < numCommits; i++ {
			require.NoError(t, env.PachClient.PutFile(input, "master", "file", strings.NewReader("1")))
		}
		require.NoError(t, env.PachClient.DeleteRepo(input, true))
		require.NoError(t, env.PachClient.CreateRepo(input))
		require.NoError(t, env.PachClient.CreateBranch(input, "master", "", nil))
		require.YesError(t, env.PachClient.FsckFastExit())
		// Deleting both repos should error, because they were broken by deleting the upstream repo.
		require.YesError(t, env.PachClient.DeleteRepo(output2, false))
		require.YesError(t, env.PachClient.DeleteRepo(output1, false))
		require.NoError(t, env.PachClient.Fsck(true, func(resp *pfs.FsckResponse) error { return nil }))
		// Deleting should now work due to fixing, must delete 2 before 1 though.
		require.NoError(t, env.PachClient.DeleteRepo(output2, false))
		require.NoError(t, env.PachClient.DeleteRepo(output1, false))

		return nil
	}))
}

// TODO: Make work with V2?
//func TestPutFileAtomic(t *testing.T) {
//	t.Parallel()
//	db := dbutil.NewTestDB(t)
//	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
//		c := env.PachClient
//		test := "test"
//		require.NoError(t, c.CreateRepo(test))
//
//		pfc, err := c.NewPutFileClient()
//		require.NoError(t, err)
//		require.NoError(t, pfc.PutFile(test, "master", "file1", strings.NewReader("1")))
//		require.NoError(t, pfc.PutFile(test, "master", "file2", strings.NewReader("2")))
//		require.NoError(t, pfc.Close())
//
//		cis, err := c.ListCommit(test, "master", "", 0)
//		require.NoError(t, err)
//		require.Equal(t, 1, len(cis))
//		var b bytes.Buffer
//		require.NoError(t, c.GetFile(test, "master", "file1", &b))
//		require.Equal(t, "1", b.String())
//		b.Reset()
//		require.NoError(t, c.GetFile(test, "master", "file2", &b))
//		require.Equal(t, "2", b.String())
//
//		pfc, err = c.NewPutFileClient()
//		require.NoError(t, err)
//		require.NoError(t, pfc.PutFile(test, "master", "file3", strings.NewReader("3")))
//		require.NoError(t, err)
//		require.NoError(t, pfc.DeleteFile(test, "master", "file1"))
//		require.NoError(t, pfc.Close())
//
//		cis, err = c.ListCommit(test, "master", "", 0)
//		require.NoError(t, err)
//		require.Equal(t, 2, len(cis))
//		b.Reset()
//		require.NoError(t, c.GetFile(test, "master", "file3", &b))
//		require.Equal(t, "3", b.String())
//		b.Reset()
//		require.YesError(t, c.GetFile(test, "master", "file1", &b))
//
//		// Empty PutFileClients shouldn't error or create commits
//		pfc, err = c.NewPutFileClient()
//		require.NoError(t, err)
//		require.NoError(t, pfc.Close())
//		cis, err = c.ListCommit(test, "master", "", 0)
//		require.NoError(t, err)
//		require.Equal(t, 2, len(cis))
//
//		return nil
//	}))
//}

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

func TestFuzzProvenance(t *testing.T) {
	if os.Getenv("RUN_BAD_TESTS") == "" {
		t.Skip("Skipping because RUN_BAD_TESTS was empty")
	}
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
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
			3, // deleteCommit
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
				require.NoError(t, env.PachClient.CreateBranch(repo, "master", "", nil))
				inputBranches = append(inputBranches, pclient.NewBranch(repo, "master"))
			case inputBranch:
				if len(inputRepos) == 0 {
					continue OpLoop
				}
				repo := inputRepos[r.Intn(len(inputRepos))]
				branch := tu.UniqueString("branch")
				require.NoError(t, env.PachClient.CreateBranch(repo, branch, "", nil))
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
				require.NoError(t, env.PachClient.FinishCommit(branch.Repo.Name, branch.Name))
				commits = append(commits, commit)
			case deleteCommit:
				if len(commits) == 0 {
					continue OpLoop
				}
				i := r.Intn(len(commits))
				commit := commits[i]
				commits = append(commits[:i], commits[i+1:]...)
				require.NoError(t, env.PachClient.DeleteCommit(commit.Repo.Name, commit.ID))
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

				require.NoError(t, env.PachClient.CreateBranch(repo, "master", "", provBranches))
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
				require.NoError(t, env.PachClient.CreateBranch(repo, branch, "", provBranches))
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

		return nil
	}))
}

// TestPutFileLeak is a regression test for when PutFile may result in a leaked
// goroutine in pachd that would use up a limiter and never release it, in the
// situation where the client starts uploading a file but causes a short-circuit
// by making a protocol error in the PutFile request.
// TODO: Make work with V2?
//func TestPutFileLeak(t *testing.T) {
//	t.Parallel()
//	pachdConfig := &serviceenv.PachdFullConfiguration{}
//	pachdConfig.StoragePutFileConcurrencyLimit = 10
//	db := dbutil.NewTestDB(t)
//	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
//		require.NoError(t, env.PachClient.CreateRepo("repo"))
//
//		for i := 0; i < 20; i++ {
//			pfc, err := env.PachClient.PfsAPIClient.PutFile(env.Context)
//			if err != nil {
//				return err
//			}
//
//			err = pfc.Send(&pfs.PutFileRequest{File: pclient.NewFile("repo", "master", "1")})
//			if err != nil {
//				return err
//			}
//
//			err = pfc.Send(&pfs.PutFileRequest{File: pclient.NewFile("repo", "faster", "2")})
//			require.NoError(t, err)
//
//			_, err = pfc.CloseAndRecv()
//			require.YesError(t, err)
//		}
//
//		return nil
//	}, pachdConfig))
//}

// TestAtomicHistory repeatedly writes to a file while concurrently reading
// its history. This checks for a regression where the repo would sometimes
// lock.
// TODO: Make work with V2?
//func TestAtomicHistory(t *testing.T) {
//	t.Parallel()
//	db := dbutil.NewTestDB(t)
//	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
//		repo := tu.UniqueString("TestAtomicHistory")
//		require.NoError(t, env.PachClient.CreateRepo(repo))
//		require.NoError(t, env.PachClient.CreateBranch(repo, "master", "", nil))
//		aSize := 1 * 1024 * 1024
//		bSize := aSize + 1024
//
//		for i := 0; i < 10; i++ {
//			// create a file of all A's
//			a := strings.Repeat("A", aSize)
//			_, err := env.PachClient.PutFileOverwrite(repo, "master", "/file", strings.NewReader(a))
//			require.NoError(t, err)
//
//			// sllowwwllly replace it with all B's
//			ctx, cancel := context.WithCancel(context.Background())
//			eg, ctx := errgroup.WithContext(ctx)
//			eg.Go(func() error {
//				b := strings.Repeat("B", bSize)
//				r := SlowReader{underlying: strings.NewReader(b)}
//				_, err := env.PachClient.PutFileOverwrite(repo, "master", "/file", &r)
//				cancel()
//				return err
//			})
//
//			// should pull /file when it's all A's
//			eg.Go(func() error {
//				for {
//					fileInfos, err := env.PachClient.ListFileHistory(repo, "master", "/file", 1)
//					require.NoError(t, err)
//					require.Equal(t, len(fileInfos), 1)
//
//					// stop once B's have been written
//					select {
//					case <-ctx.Done():
//						return nil
//					default:
//						time.Sleep(1 * time.Millisecond)
//					}
//				}
//			})
//
//			require.NoError(t, eg.Wait())
//
//			// should pull /file when it's all B's
//			fileInfos, err := env.PachClient.ListFileHistory(repo, "master", "/file", 1)
//			require.NoError(t, err)
//			require.Equal(t, 1, len(fileInfos))
//			require.Equal(t, bSize, int(fileInfos[0].SizeBytes))
//		}
//		return nil
//	}))
//}

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

// TestTrigger tests branch triggers
func TestTrigger(t *testing.T) {
	if os.Getenv("RUN_BAD_TESTS") == "" {
		t.Skip("Skipping because RUN_BAD_TESTS was empty")
	}
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		c := env.PachClient
		t.Run("Simple", func(t *testing.T) {
			require.NoError(t, c.CreateRepo("test"))
			require.NoError(t, c.CreateBranchTrigger("test", "master", "", &pfs.Trigger{
				Branch: "staging",
				Size_:  "1B",
			}))
			require.NoError(t, c.PutFile("test", "staging", "file", strings.NewReader("small")))
		})
		t.Run("SizeWithProvenance", func(t *testing.T) {
			require.NoError(t, c.CreateRepo("in"))
			require.NoError(t, c.CreateBranchTrigger("in", "trigger", "", &pfs.Trigger{
				Branch: "master",
				Size_:  "1K",
			}))
			bis, err := c.ListBranch("in")
			require.NoError(t, err)
			require.Equal(t, 1, len(bis))
			require.Nil(t, bis[0].Head)

			// Create a downstream branch
			require.NoError(t, c.CreateRepo("out"))
			require.NoError(t, c.CreateBranch("out", "master", "", []*pfs.Branch{pclient.NewBranch("in", "trigger")}))
			require.NoError(t, c.CreateBranchTrigger("out", "trigger", "", &pfs.Trigger{
				Branch: "master",
				Size_:  "1K",
			}))

			// Write a small file, too small to trigger
			require.NoError(t, c.PutFile("in", "master", "file", strings.NewReader("small")))
			bi, err := c.InspectBranch("in", "trigger")
			require.NoError(t, err)
			require.Nil(t, bi.Head)
			bi, err = c.InspectBranch("out", "master")
			require.NoError(t, err)
			require.Nil(t, bi.Head)
			bi, err = c.InspectBranch("out", "trigger")
			require.NoError(t, err)
			require.Nil(t, bi.Head)

			require.NoError(t, c.PutFile("in", "master", "file", strings.NewReader(strings.Repeat("a", units.KB))))

			bi, err = c.InspectBranch("in", "trigger")
			require.NoError(t, err)
			require.NotNil(t, bi.Head)

			// Output branch should have a commit now
			bi, err = c.InspectBranch("out", "master")
			require.NoError(t, err)
			require.NotNil(t, bi.Head)

			// Put a file that will cause the trigger to go off
			require.NoError(t, c.PutFile("out", "master", "file", strings.NewReader(strings.Repeat("a", units.KB))))
			require.NoError(t, env.PachClient.FinishCommit("out", "master"))

			// Output trigger should have triggered
			bi, err = c.InspectBranch("out", "trigger")
			require.NoError(t, err)
			require.NotNil(t, bi.Head)
		})
		t.Run("Cron", func(t *testing.T) {
			require.NoError(t, c.CreateRepo("cron"))
			require.NoError(t, c.CreateBranchTrigger("cron", "trigger", "", &pfs.Trigger{
				Branch:   "master",
				CronSpec: "* * * * *", // every minute
			}))
			// The first commit should always trigger a cron
			require.NoError(t, c.PutFile("cron", "master", "file1", strings.NewReader("foo")))
			bi, err := c.InspectBranch("cron", "trigger")
			require.NoError(t, err)
			require.NotNil(t, bi.Head)
			head := bi.Head.ID

			// Second commit should not trigger the cron because less than a
			// minute has passed
			require.NoError(t, c.PutFile("cron", "master", "file2", strings.NewReader("bar")))
			bi, err = c.InspectBranch("cron", "trigger")
			require.NoError(t, err)
			require.Equal(t, head, bi.Head.ID)

			time.Sleep(time.Minute)
			// Third commit should trigger the cron because a minute has passed
			require.NoError(t, c.PutFile("cron", "master", "file3", strings.NewReader("fizz")))
			bi, err = c.InspectBranch("cron", "trigger")
			require.NoError(t, err)
			require.NotEqual(t, head, bi.Head.ID)
		})
		t.Run("Count", func(t *testing.T) {
			require.NoError(t, c.CreateRepo("count"))
			require.NoError(t, c.CreateBranchTrigger("count", "trigger", "", &pfs.Trigger{
				Branch:  "master",
				Commits: 2, // trigger every 2 commits
			}))
			// The first commit shouldn't trigger
			require.NoError(t, c.PutFile("count", "master", "file1", strings.NewReader("foo")))
			bi, err := c.InspectBranch("count", "trigger")
			require.NoError(t, err)
			require.Nil(t, bi.Head)

			// Second commit should trigger
			require.NoError(t, c.PutFile("count", "master", "file2", strings.NewReader("bar")))
			bi, err = c.InspectBranch("count", "trigger")
			require.NoError(t, err)
			require.NotNil(t, bi.Head)
			head := bi.Head.ID

			// Third commit shouldn't trigger
			require.NoError(t, c.PutFile("count", "master", "file3", strings.NewReader("fizz")))
			bi, err = c.InspectBranch("count", "trigger")
			require.NoError(t, err)
			require.Equal(t, head, bi.Head.ID)

			// Fourth commit should trigger
			require.NoError(t, c.PutFile("count", "master", "file4", strings.NewReader("buzz")))
			bi, err = c.InspectBranch("count", "trigger")
			require.NoError(t, err)
			require.NotEqual(t, head, bi.Head.ID)
		})
		t.Run("Or", func(t *testing.T) {
			require.NoError(t, c.CreateRepo("or"))
			require.NoError(t, c.CreateBranchTrigger("or", "trigger", "", &pfs.Trigger{
				Branch:   "master",
				CronSpec: "* * * * *",
				Size_:    "100",
				Commits:  3,
			}))
			// This triggers, because the cron is satisfied
			require.NoError(t, c.PutFile("or", "master", "file1", strings.NewReader(strings.Repeat("a", 1))))
			bi, err := c.InspectBranch("or", "trigger")
			require.NoError(t, err)
			require.NotNil(t, bi.Head)
			head := bi.Head.ID
			// This one doesn't because none of them are satisfied
			require.NoError(t, c.PutFile("or", "master", "file2", strings.NewReader(strings.Repeat("a", 50))))
			bi, err = c.InspectBranch("or", "trigger")
			require.NoError(t, err)
			require.Equal(t, head, bi.Head.ID)
			// This one triggers because we hit 100 bytes
			require.NoError(t, c.PutFile("or", "master", "file3", strings.NewReader(strings.Repeat("a", 50))))
			bi, err = c.InspectBranch("or", "trigger")
			require.NoError(t, err)
			require.NotEqual(t, head, bi.Head.ID)
			head = bi.Head.ID

			// This one doesn't trigger
			require.NoError(t, c.PutFile("or", "master", "file4", strings.NewReader(strings.Repeat("a", 1))))
			bi, err = c.InspectBranch("or", "trigger")
			require.NoError(t, err)
			require.Equal(t, head, bi.Head.ID)
			// This one neither
			require.NoError(t, c.PutFile("or", "master", "file5", strings.NewReader(strings.Repeat("a", 1))))
			bi, err = c.InspectBranch("or", "trigger")
			require.NoError(t, err)
			require.Equal(t, head, bi.Head.ID)
			// This one does, because it's 3 commits
			require.NoError(t, c.PutFile("or", "master", "file6", strings.NewReader(strings.Repeat("a", 1))))
			bi, err = c.InspectBranch("or", "trigger")
			require.NoError(t, err)
			require.NotEqual(t, head, bi.Head.ID)
			head = bi.Head.ID

			// This one doesn't trigger
			require.NoError(t, c.PutFile("or", "master", "file7", strings.NewReader(strings.Repeat("a", 1))))
			bi, err = c.InspectBranch("or", "trigger")
			require.NoError(t, err)
			require.Equal(t, head, bi.Head.ID)

			time.Sleep(time.Minute)

			require.NoError(t, c.PutFile("or", "master", "file8", strings.NewReader(strings.Repeat("a", 1))))
			bi, err = c.InspectBranch("or", "trigger")
			require.NoError(t, err)
			require.NotEqual(t, head, bi.Head.ID)
		})
		t.Run("And", func(t *testing.T) {
			require.NoError(t, c.CreateRepo("and"))
			require.NoError(t, c.CreateBranchTrigger("and", "trigger", "", &pfs.Trigger{
				Branch:   "master",
				All:      true,
				CronSpec: "* * * * *",
				Size_:    "100",
				Commits:  3,
			}))
			// Doesn't trigger because all 3 conditions must be met
			require.NoError(t, c.PutFile("and", "master", "file1", strings.NewReader(strings.Repeat("a", 100))))
			bi, err := c.InspectBranch("and", "trigger")
			require.NoError(t, err)
			require.Nil(t, bi.Head)

			// Still doesn't trigger
			require.NoError(t, c.PutFile("and", "master", "file2", strings.NewReader(strings.Repeat("a", 100))))
			bi, err = c.InspectBranch("and", "trigger")
			require.NoError(t, err)
			require.Nil(t, bi.Head)

			// Finally triggers because we have 3 commits, 100 bytes and Cron
			// Spec (since epoch) is satisfied.
			require.NoError(t, c.PutFile("and", "master", "file3", strings.NewReader(strings.Repeat("a", 100))))
			bi, err = c.InspectBranch("and", "trigger")
			require.NoError(t, err)
			require.NotNil(t, bi.Head)
			head := bi.Head.ID

			// Doesn't trigger because all 3 conditions must be met
			require.NoError(t, c.PutFile("and", "master", "file4", strings.NewReader(strings.Repeat("a", 100))))
			bi, err = c.InspectBranch("and", "trigger")
			require.NoError(t, err)
			require.Equal(t, head, bi.Head.ID)

			// Still no trigger, not enough time or commits
			require.NoError(t, c.PutFile("and", "master", "file5", strings.NewReader(strings.Repeat("a", 100))))
			bi, err = c.InspectBranch("and", "trigger")
			require.NoError(t, err)
			require.Equal(t, head, bi.Head.ID)

			// Still no trigger, not enough time
			require.NoError(t, c.PutFile("and", "master", "file6", strings.NewReader(strings.Repeat("a", 100))))
			bi, err = c.InspectBranch("and", "trigger")
			require.NoError(t, err)
			require.Equal(t, head, bi.Head.ID)

			time.Sleep(time.Minute)

			// Finally triggers, all triggers have been met
			require.NoError(t, c.PutFile("and", "master", "file7", strings.NewReader(strings.Repeat("a", 100))))
			bi, err = c.InspectBranch("and", "trigger")
			require.NoError(t, err)
			require.NotEqual(t, head, bi.Head.ID)
		})
		t.Run("Chain", func(t *testing.T) {
			// a triggers b which triggers c
			require.NoError(t, c.CreateRepo("chain"))
			require.NoError(t, c.CreateBranchTrigger("chain", "b", "", &pfs.Trigger{
				Branch: "a",
				Size_:  "100",
			}))
			require.NoError(t, c.CreateBranchTrigger("chain", "c", "", &pfs.Trigger{
				Branch: "b",
				Size_:  "200",
			}))
			// Triggers nothing
			require.NoError(t, c.PutFile("chain", "a", "file1", strings.NewReader(strings.Repeat("a", 50))))
			bi, err := c.InspectBranch("chain", "b")
			require.NoError(t, err)
			require.Nil(t, bi.Head)
			bi, err = c.InspectBranch("chain", "c")
			require.NoError(t, err)
			require.Nil(t, bi.Head)

			// Triggers b, but not c
			require.NoError(t, c.PutFile("chain", "a", "file2", strings.NewReader(strings.Repeat("a", 50))))
			bi, err = c.InspectBranch("chain", "b")
			require.NoError(t, err)
			require.NotNil(t, bi.Head)
			bHead := bi.Head.ID
			bi, err = c.InspectBranch("chain", "c")
			require.NoError(t, err)
			require.Nil(t, bi.Head)

			// Triggers nothing
			require.NoError(t, c.PutFile("chain", "a", "file3", strings.NewReader(strings.Repeat("a", 50))))
			bi, err = c.InspectBranch("chain", "b")
			require.NoError(t, err)
			require.NotNil(t, bi.Head)
			require.Equal(t, bHead, bi.Head.ID)
			bi, err = c.InspectBranch("chain", "c")
			require.NoError(t, err)
			require.Nil(t, bi.Head)

			// Triggers a and c
			require.NoError(t, c.PutFile("chain", "a", "file4", strings.NewReader(strings.Repeat("a", 50))))
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
			require.NoError(t, c.PutFile("chain", "a", "file5", strings.NewReader(strings.Repeat("a", 50))))
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
			require.NoError(t, c.CreateBranchTrigger("branch-movement", "c", "", &pfs.Trigger{
				Branch: "b",
				Size_:  "100",
			}))

			require.NoError(t, c.PutFile("branch-movement", "a", "file1", strings.NewReader(strings.Repeat("a", 50))))
			require.NoError(t, c.CreateBranch("branch-movement", "b", "a", nil))
			bi, err := c.InspectBranch("branch-movement", "c")
			require.NoError(t, err)
			require.Nil(t, bi.Head)

			require.NoError(t, c.PutFile("branch-movement", "a", "file2", strings.NewReader(strings.Repeat("a", 50))))
			require.NoError(t, c.CreateBranch("branch-movement", "b", "a", nil))
			bi, err = c.InspectBranch("branch-movement", "c")
			require.NoError(t, err)
			require.NotNil(t, bi.Head)
			cHead := bi.Head.ID

			require.NoError(t, c.PutFile("branch-movement", "a", "file3", strings.NewReader(strings.Repeat("a", 50))))
			require.NoError(t, c.CreateBranch("branch-movement", "b", "a", nil))
			bi, err = c.InspectBranch("branch-movement", "c")
			require.NoError(t, err)
			require.NotNil(t, bi.Head)
			require.Equal(t, cHead, bi.Head.ID)
		})

		return nil
	}))
}

// TestTrigger tests branch trigger validation
func TestTriggerValidation(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		c := env.PachClient
		require.NoError(t, c.CreateRepo("repo"))
		// Must specify a branch
		require.YesError(t, c.CreateBranchTrigger("repo", "master", "", &pfs.Trigger{
			Branch: "",
			Size_:  "1K",
		}))
		// Can't trigger a branch on itself
		require.YesError(t, c.CreateBranchTrigger("repo", "master", "", &pfs.Trigger{
			Branch: "master",
			Size_:  "1K",
		}))
		// Size doesn't parse
		require.YesError(t, c.CreateBranchTrigger("repo", "trigger", "", &pfs.Trigger{
			Branch: "master",
			Size_:  "this is not a size",
		}))
		// Can't have negative commit count
		require.YesError(t, c.CreateBranchTrigger("repo", "trigger", "", &pfs.Trigger{
			Branch:  "master",
			Commits: -1,
		}))

		// a -> b (valid, sets up the next test)
		require.NoError(t, c.CreateBranchTrigger("repo", "b", "", &pfs.Trigger{
			Branch: "a",
			Size_:  "1K",
		}))
		// Can't have circular triggers
		require.YesError(t, c.CreateBranchTrigger("repo", "a", "", &pfs.Trigger{
			Branch: "b",
			Size_:  "1K",
		}))
		// CronSpec doesn't parse
		require.YesError(t, c.CreateBranchTrigger("repo", "trigger", "", &pfs.Trigger{
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
		return nil
	}))
}

// TODO: Reenable when PFS metadata is in postgres.
//func TestLargeDeleteRepo(t *testing.T) {
//	t.Parallel()
//	require.NoError(t, testpachd.WithRealEnv(func(env *testpachd.RealEnv) error {
//		numRepos := 10
//		numCommits := 1000
//		var repos []string
//		for i := 0; i < numRepos; i++ {
//			repo := fmt.Sprintf("repo-%d", i)
//			require.NoError(t, env.PachClient.CreateRepo(repo))
//			if i > 0 {
//				require.NoError(t, env.PachClient.CreateBranch(repo, "master", "", []*pfs.Branch{pclient.NewBranch(repos[i-1], "master")}))
//			}
//			repos = append(repos, repo)
//		}
//		for i := 0; i < numCommits; i++ {
//			_, err := env.PachClient.StartCommit(repos[0], "master")
//			require.NoError(t, err)
//			require.NoError(t, env.PachClient.FinishCommit(repos[0], "master"))
//		}
//		repo := repos[len(repos)-1]
//		ctx, cf := context.WithTimeout(context.Background(), time.Second)
//		defer cf()
//		require.YesError(t, env.PachClient.WithCtx(ctx).DeleteRepo(repo, false, true))
//		require.YesError(t, env.PachClient.CreateBranch(repo, "test", "", nil))
//		_, err := env.PachClient.StartCommit(repo, "master")
//		require.YesError(t, err)
//		for i := len(repos) - 1; i >= 0; i-- {
//			require.NoError(t, env.PachClient.DeleteRepo(repos[i], false, true))
//			require.NoError(t, env.PachClient.FsckFastExit())
//		}
//		_, err = env.PachClient.PfsAPIClient.DeleteAll(env.PachClient.Ctx(), &types.Empty{})
//		require.NoError(t, err)
//		return nil
//	}))
//}

func TestRegressionOrphanedFile(t *testing.T) {
	t.Parallel()
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		fsclient, err := env.PachClient.NewCreateFilesetClient()
		require.NoError(t, err)
		data := []byte("test data")
		spec := fileSetSpec{
			"file1.txt": tarutil.NewMemFile("file1.txt", data),
			"file2.txt": tarutil.NewMemFile("file2.txt", data),
		}
		require.NoError(t, fsclient.AppendFileTar(spec.makeTarStream()))
		resp, err := fsclient.Close()
		require.NoError(t, err)
		t.Logf("tmp fileset id: %s", resp.FilesetId)
		require.NoError(t, env.PachClient.RenewFileSet(resp.FilesetId, 60*time.Second))
		fis, err := env.PachClient.ListFileAll(pclient.TmpRepoName, resp.FilesetId, "/")
		require.NoError(t, err)
		require.Equal(t, 2, len(fis))
		return nil
	}))
}

func TestCompaction(t *testing.T) {
	t.Parallel()
	config := testpachd.NewDefaultConfig()
	config.StorageCompactionMaxFanIn = 10
	db := dbutil.NewTestDB(t)
	require.NoError(t, testpachd.WithRealEnv(db, func(env *testpachd.RealEnv) error {
		repo := "TestCompaction"
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
				if err != nil {
					return err
				}
				file := tarutil.NewMemFile(name, data)
				hdr, err := file.Header()
				if err != nil {
					return err
				}
				fsSpec[hdr.Name] = file
			}
			if err := env.PachClient.AppendFileTar(repo, commit1.ID, fsSpec.makeTarStream()); err != nil {
				return err
			}
			runtime.GC()
		}
		if err := env.PachClient.FinishCommit(repo, commit1.ID); err != nil {
			return err
		}
		return nil
	}, config))
}

var (
	randSeed = int64(0)
	randMu   sync.Mutex
)

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

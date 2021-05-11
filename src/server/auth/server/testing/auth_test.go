package server

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"fmt"
	"io"
	"net"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/pachyderm/pachyderm/v2/src/auth"
	"github.com/pachyderm/pachyderm/v2/src/client"
	"github.com/pachyderm/pachyderm/v2/src/internal/backoff"
	"github.com/pachyderm/pachyderm/v2/src/internal/errors"
	"github.com/pachyderm/pachyderm/v2/src/internal/require"
	tu "github.com/pachyderm/pachyderm/v2/src/internal/testutil"
	"github.com/pachyderm/pachyderm/v2/src/pfs"
	"github.com/pachyderm/pachyderm/v2/src/pps"

	minio "github.com/minio/minio-go/v6"
	globlib "github.com/pachyderm/ohmyglob"
)

func getRepoRoleBinding(t *testing.T, c *client.APIClient, repo string) *auth.RoleBinding {
	t.Helper()
	resp, err := c.GetRepoRoleBinding(repo)
	require.NoError(t, err)
	return resp
}

// CommitCnt uses 'c' to get the number of commits made to the repo 'repo'
func CommitCnt(t *testing.T, c *client.APIClient, repo string) int {
	t.Helper()
	commitList, err := c.ListCommitByRepo(repo)
	require.NoError(t, err)
	return len(commitList)
}

// PipelineNames returns the names of all pipelines that 'c' gets from
// ListPipeline
func PipelineNames(t *testing.T, c *client.APIClient) []string {
	t.Helper()
	ps, err := c.ListPipeline()
	require.NoError(t, err)
	result := make([]string, len(ps))
	for i, p := range ps {
		result[i] = p.Pipeline.Name
	}
	return result
}

// TestGetSetBasic creates two users, alice and bob, and gives bob gradually
// escalating privileges, checking what bob can and can't do after each change
func TestGetSetBasic(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}
	tu.DeleteAll(t)
	defer tu.DeleteAll(t)
	alice, bob := robot(tu.UniqueString("alice")), robot(tu.UniqueString("bob"))
	aliceClient, bobClient := tu.GetAuthenticatedPachClient(t, alice), tu.GetAuthenticatedPachClient(t, bob)

	// create repo, and check that alice is the owner of the new repo
	dataRepo := tu.UniqueString(t.Name())
	require.NoError(t, aliceClient.CreateRepo(dataRepo))
	require.Equal(t,
		buildBindings(alice, auth.RepoOwnerRole), getRepoRoleBinding(t, aliceClient, dataRepo))

	// Add data to repo (alice can write). Make sure alice can read also.
	err := aliceClient.PutFile(dataRepo, "master", "/file", strings.NewReader("1"), client.WithAppendPutFile())
	require.NoError(t, err)
	buf := &bytes.Buffer{}
	require.NoError(t, aliceClient.GetFile(dataRepo, "master", "/file", buf))
	require.Equal(t, "1", buf.String())

	//////////
	/// Initially, bob has no privileges
	// bob can't read
	err = bobClient.GetFile(dataRepo, "master", "/file", buf)
	require.YesError(t, err)
	require.Matches(t, "not authorized", err.Error())
	// bob can't write (check both the standalone form of PutFile and StartCommit)
	err = bobClient.PutFile(dataRepo, "master", "/file", strings.NewReader("lorem ipsum"), client.WithAppendPutFile())
	require.YesError(t, err)
	require.Matches(t, "not authorized", err.Error())
	require.Equal(t, 1, CommitCnt(t, aliceClient, dataRepo)) // check that no commits were created
	_, err = bobClient.StartCommit(dataRepo, "master")
	require.YesError(t, err)
	require.Matches(t, "not authorized", err.Error())
	require.Equal(t, 1, CommitCnt(t, aliceClient, dataRepo)) // check that no commits were created
	// bob can't update the ACL
	err = bobClient.ModifyRepoRoleBinding(dataRepo, robot("carol"), []string{auth.RepoReaderRole})
	require.YesError(t, err)
	require.Matches(t, "not authorized", err.Error())
	// check that ACL wasn't updated)
	require.Equal(t,
		buildBindings(alice, auth.RepoOwnerRole), getRepoRoleBinding(t, aliceClient, dataRepo))

	//////////
	/// alice adds bob to the ACL as a reader (alice can modify ACL)
	require.NoError(t, aliceClient.ModifyRepoRoleBinding(dataRepo, bob, []string{auth.RepoReaderRole}))
	// bob can read
	buf.Reset()
	require.NoError(t, bobClient.GetFile(dataRepo, "master", "/file", buf))
	require.Equal(t, "1", buf.String())
	// bob can't write
	err = bobClient.PutFile(dataRepo, "master", "/file", strings.NewReader("2"), client.WithAppendPutFile())
	require.YesError(t, err)
	require.Matches(t, "not authorized", err.Error())
	require.Equal(t, 1, CommitCnt(t, aliceClient, dataRepo)) // check that no commits were created
	_, err = bobClient.StartCommit(dataRepo, "master")
	require.YesError(t, err)
	require.Matches(t, "not authorized", err.Error())
	require.Equal(t, 1, CommitCnt(t, aliceClient, dataRepo)) // check that no commits were created
	// bob can't update the ACL
	err = bobClient.ModifyRepoRoleBinding(dataRepo, robot("carol"), []string{auth.RepoReaderRole})
	require.YesError(t, err)
	require.Matches(t, "not authorized", err.Error())
	// check that ACL wasn't updated)
	require.Equal(t,
		buildBindings(alice, auth.RepoOwnerRole, bob, auth.RepoReaderRole), getRepoRoleBinding(t, aliceClient, dataRepo))

	//////////
	/// alice adds bob to the ACL as a writer
	require.NoError(t, aliceClient.ModifyRepoRoleBinding(dataRepo, bob, []string{auth.RepoWriterRole}))
	// bob can read
	buf.Reset()
	require.NoError(t, bobClient.GetFile(dataRepo, "master", "/file", buf))
	require.Equal(t, "1", buf.String())
	// bob can write
	err = bobClient.PutFile(dataRepo, "master", "/file", strings.NewReader("2"), client.WithAppendPutFile())
	require.NoError(t, err)
	require.Equal(t, 2, CommitCnt(t, aliceClient, dataRepo)) // check that a new commit was created
	commit, err := bobClient.StartCommit(dataRepo, "master")
	require.NoError(t, err)
	require.NoError(t, bobClient.FinishCommit(dataRepo, commit.ID))
	require.Equal(t, 3, CommitCnt(t, aliceClient, dataRepo)) // check that a new commit was created
	// bob can't update the ACL
	err = bobClient.ModifyRepoRoleBinding(dataRepo, robot("carol"), []string{auth.RepoReaderRole})
	require.YesError(t, err)
	require.Matches(t, "not authorized", err.Error())
	// check that ACL wasn't updated)
	require.Equal(t,
		buildBindings(alice, auth.RepoOwnerRole, bob, auth.RepoWriterRole), getRepoRoleBinding(t, aliceClient, dataRepo))

	//////////
	/// alice adds bob to the ACL as an owner
	require.NoError(t, aliceClient.ModifyRepoRoleBinding(dataRepo, bob, []string{auth.RepoOwnerRole}))
	// bob can read
	buf.Reset()
	require.NoError(t, bobClient.GetFile(dataRepo, "master", "/file", buf))
	require.Equal(t, "12", buf.String())
	// bob can write
	err = bobClient.PutFile(dataRepo, "master", "/file", strings.NewReader("3"), client.WithAppendPutFile())
	require.NoError(t, err)
	require.Equal(t, 4, CommitCnt(t, aliceClient, dataRepo)) // check that a new commit was created
	commit, err = bobClient.StartCommit(dataRepo, "master")
	require.NoError(t, err)
	require.NoError(t, bobClient.FinishCommit(dataRepo, commit.ID))
	require.Equal(t, 5, CommitCnt(t, aliceClient, dataRepo)) // check that a new commit was created
	// bob can update the ACL
	require.NoError(t, bobClient.ModifyRepoRoleBinding(dataRepo, robot("carol"), []string{auth.RepoReaderRole}))
	// check that ACL was updated)
	require.Equal(t,
		buildBindings(alice, auth.RepoOwnerRole, bob, auth.RepoOwnerRole, robot("carol"), auth.RepoReaderRole),
		getRepoRoleBinding(t, aliceClient, dataRepo))
}

// TestGetSetReverse creates two users, alice and bob, and gives bob gradually
// shrinking privileges, checking what bob can and can't do after each change
func TestGetSetReverse(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}
	tu.DeleteAll(t)
	defer tu.DeleteAll(t)
	alice, bob := robot(tu.UniqueString("alice")), robot(tu.UniqueString("bob"))
	aliceClient, bobClient := tu.GetAuthenticatedPachClient(t, alice), tu.GetAuthenticatedPachClient(t, bob)

	// create repo, and check that alice is the owner of the new repo
	dataRepo := tu.UniqueString(t.Name())
	require.NoError(t, aliceClient.CreateRepo(dataRepo))
	require.Equal(t,
		buildBindings(alice, auth.RepoOwnerRole), getRepoRoleBinding(t, aliceClient, dataRepo))

	// Add data to repo (alice can write). Make sure alice can read also.
	commit, err := aliceClient.StartCommit(dataRepo, "master")
	require.NoError(t, err)
	err = aliceClient.PutFile(dataRepo, commit.ID, "/file", strings.NewReader("1"), client.WithAppendPutFile())
	require.NoError(t, err)
	require.NoError(t, aliceClient.FinishCommit(dataRepo, commit.ID)) // # commits = 1
	buf := &bytes.Buffer{}
	require.NoError(t, aliceClient.GetFile(dataRepo, "master", "/file", buf))
	require.Equal(t, "1", buf.String())

	//////////
	/// alice adds bob to the ACL as an owner
	require.NoError(t, aliceClient.ModifyRepoRoleBinding(dataRepo, bob, []string{auth.RepoOwnerRole}))
	// bob can read
	buf.Reset()
	require.NoError(t, bobClient.GetFile(dataRepo, "master", "/file", buf))
	require.Equal(t, "1", buf.String())
	// bob can write
	err = bobClient.PutFile(dataRepo, "master", "/file", strings.NewReader("2"), client.WithAppendPutFile())
	require.NoError(t, err)
	require.Equal(t, 2, CommitCnt(t, aliceClient, dataRepo)) // check that a new commit was created
	commit, err = bobClient.StartCommit(dataRepo, "master")
	require.NoError(t, err)
	require.NoError(t, bobClient.FinishCommit(dataRepo, commit.ID))
	require.Equal(t, 3, CommitCnt(t, aliceClient, dataRepo)) // check that a new commit was created
	// bob can update the ACL
	require.NoError(t, bobClient.ModifyRepoRoleBinding(dataRepo, robot("carol"), []string{auth.RepoReaderRole}))
	// check that ACL was updated)
	require.Equal(t,
		buildBindings(alice, auth.RepoOwnerRole, bob, auth.RepoOwnerRole, robot("carol"), auth.RepoReaderRole),
		getRepoRoleBinding(t, aliceClient, dataRepo))

	// clear carol
	require.NoError(t, aliceClient.ModifyRepoRoleBinding(dataRepo, robot("carol"), []string{}))
	require.Equal(t,
		buildBindings(alice, auth.RepoOwnerRole, bob, auth.RepoOwnerRole), getRepoRoleBinding(t, aliceClient, dataRepo))

	//////////
	/// alice adds bob to the ACL as a writer
	require.NoError(t, aliceClient.ModifyRepoRoleBinding(dataRepo, bob, []string{auth.RepoWriterRole}))
	// bob can read
	buf.Reset()
	require.NoError(t, bobClient.GetFile(dataRepo, "master", "/file", buf))
	require.Equal(t, "12", buf.String())
	// bob can write
	err = bobClient.PutFile(dataRepo, "master", "/file", strings.NewReader("3"), client.WithAppendPutFile())
	require.NoError(t, err)
	require.Equal(t, 4, CommitCnt(t, aliceClient, dataRepo)) // check that a new commit was created
	commit, err = bobClient.StartCommit(dataRepo, "master")
	require.NoError(t, err)
	require.NoError(t, bobClient.FinishCommit(dataRepo, commit.ID))
	require.Equal(t, 5, CommitCnt(t, aliceClient, dataRepo)) // check that a new commit was created
	// bob can't update the ACL
	err = bobClient.ModifyRepoRoleBinding(dataRepo, robot("carol"), []string{auth.RepoReaderRole})
	require.YesError(t, err)
	require.Matches(t, "not authorized", err.Error())
	// check that ACL wasn't updated)
	require.Equal(t,
		buildBindings(alice, auth.RepoOwnerRole, bob, auth.RepoWriterRole), getRepoRoleBinding(t, aliceClient, dataRepo))

	//////////
	/// alice adds bob to the ACL as a reader (alice can modify ACL)
	require.NoError(t, aliceClient.ModifyRepoRoleBinding(dataRepo, bob, []string{auth.RepoReaderRole}))
	// bob can read
	buf.Reset()
	require.NoError(t, bobClient.GetFile(dataRepo, "master", "/file", buf))
	require.Equal(t, "123", buf.String())
	// bob can't write
	err = bobClient.PutFile(dataRepo, "master", "/file", strings.NewReader("4"), client.WithAppendPutFile())
	require.YesError(t, err)
	require.Equal(t, 5, CommitCnt(t, aliceClient, dataRepo)) // check that a new commit was created
	_, err = bobClient.StartCommit(dataRepo, "master")
	require.YesError(t, err)
	require.Matches(t, "not authorized", err.Error())
	require.Equal(t, 5, CommitCnt(t, aliceClient, dataRepo)) // check that no commits were created
	// bob can't update the ACL
	err = bobClient.ModifyRepoRoleBinding(dataRepo, robot("carol"), []string{auth.RepoReaderRole})
	require.YesError(t, err)
	require.Matches(t, "not authorized", err.Error())
	// check that ACL wasn't updated)
	require.Equal(t,
		buildBindings(alice, auth.RepoOwnerRole, bob, auth.RepoReaderRole), getRepoRoleBinding(t, aliceClient, dataRepo))

	//////////
	/// alice revokes all of bob's privileges
	require.NoError(t, aliceClient.ModifyRepoRoleBinding(dataRepo, bob, []string{}))
	// bob can't read
	err = bobClient.GetFile(dataRepo, "master", "/file", buf)
	require.YesError(t, err)
	require.Matches(t, "not authorized", err.Error())
	// bob can't write
	err = bobClient.PutFile(dataRepo, "master", "/file", strings.NewReader("4"), client.WithAppendPutFile())
	require.YesError(t, err)
	require.Equal(t, 5, CommitCnt(t, aliceClient, dataRepo)) // check that a new commit was created
	_, err = bobClient.StartCommit(dataRepo, "master")
	require.YesError(t, err)
	require.Matches(t, "not authorized", err.Error())
	require.Equal(t, 5, CommitCnt(t, aliceClient, dataRepo)) // check that no commits were created
	// bob can't update the ACL
	err = bobClient.ModifyRepoRoleBinding(dataRepo, robot("carol"), []string{auth.RepoReaderRole})
	require.YesError(t, err)
	require.Matches(t, "not authorized", err.Error())
	// check that ACL wasn't updated)
	require.Equal(t,
		buildBindings(alice, auth.RepoOwnerRole), getRepoRoleBinding(t, aliceClient, dataRepo))
}

// TestCreateAndUpdateRepo tests that if CreateRepo(foo, update=true) is
// called, and foo exists, then the ACL for foo won't be modified.
func TestCreateAndUpdateRepo(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}
	tu.DeleteAll(t)
	defer tu.DeleteAll(t)
	alice, bob := robot(tu.UniqueString("alice")), robot(tu.UniqueString("bob"))
	aliceClient, bobClient := tu.GetAuthenticatedPachClient(t, alice), tu.GetAuthenticatedPachClient(t, bob)

	// create repo, and check that alice is the owner of the new repo
	dataRepo := tu.UniqueString(t.Name())
	require.NoError(t, aliceClient.CreateRepo(dataRepo))
	require.Equal(t,
		buildBindings(alice, auth.RepoOwnerRole), getRepoRoleBinding(t, aliceClient, dataRepo))

	// Add data to repo (alice can write). Make sure alice can read also.
	err := aliceClient.PutFile(dataRepo, "master", "/file", strings.NewReader("1"))
	require.NoError(t, err)
	buf := &bytes.Buffer{}
	require.NoError(t, aliceClient.GetFile(dataRepo, "master", "/file", buf))
	require.Equal(t, "1", buf.String())

	/// alice adds bob to the ACL as a reader (alice can modify ACL)
	require.NoError(t, aliceClient.ModifyRepoRoleBinding(dataRepo, bob, []string{auth.RepoReaderRole}))
	require.Equal(t,
		buildBindings(alice, auth.RepoOwnerRole, bob, auth.RepoReaderRole), getRepoRoleBinding(t, aliceClient, dataRepo))
	// bob can read
	buf.Reset()
	require.NoError(t, bobClient.GetFile(dataRepo, "master", "/file", buf))
	require.Equal(t, "1", buf.String())

	/// alice updates the repo
	description := "This request updates the description to force a write"
	_, err = aliceClient.PfsAPIClient.CreateRepo(aliceClient.Ctx(), &pfs.CreateRepoRequest{
		Repo:        client.NewRepo(dataRepo),
		Description: description,
		Update:      true,
	})
	require.NoError(t, err)
	repoInfo, err := aliceClient.InspectRepo(dataRepo)
	require.NoError(t, err)
	require.Equal(t, description, repoInfo.Description)
	// buildBindings haven't changed
	require.Equal(t,
		buildBindings(alice, auth.RepoOwnerRole, bob, auth.RepoReaderRole), getRepoRoleBinding(t, aliceClient, dataRepo))
	// bob can still read
	buf.Reset()
	require.NoError(t, bobClient.GetFile(dataRepo, "master", "/file", buf))
	require.Equal(t, "1", buf.String())
}

// TestCreateRepoWithUpdateFlag tests that if CreateRepo(foo, update=true) is
// called, and foo doesn't exist, then the ACL for foo will still be created and
// initialized to the correct value
func TestCreateRepoWithUpdateFlag(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}
	tu.DeleteAll(t)
	defer tu.DeleteAll(t)
	alice := robot(tu.UniqueString("alice"))
	aliceClient := tu.GetAuthenticatedPachClient(t, alice)

	// create repo, and check that alice is the owner of the new repo
	dataRepo := tu.UniqueString(t.Name())
	/// alice creates the repo with Update set
	_, err := aliceClient.PfsAPIClient.CreateRepo(aliceClient.Ctx(), &pfs.CreateRepoRequest{
		Repo:   client.NewRepo(dataRepo),
		Update: true,
	})
	require.NoError(t, err)
	require.Equal(t,
		buildBindings(alice, auth.RepoOwnerRole), getRepoRoleBinding(t, aliceClient, dataRepo))

	// Add data to repo (alice can write). Make sure alice can read also.
	err = aliceClient.PutFile(dataRepo, "master", "/file", strings.NewReader("1"))
	require.NoError(t, err)
	buf := &bytes.Buffer{}
	require.NoError(t, aliceClient.GetFile(dataRepo, "master", "/file", buf))
	require.Equal(t, "1", buf.String())
}

func TestCreateAndUpdatePipeline(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}
	tu.DeleteAll(t)
	defer tu.DeleteAll(t)
	type createArgs struct {
		client     *client.APIClient
		name, repo string
		update     bool
	}
	createPipeline := func(args createArgs) error {
		return args.client.CreatePipeline(
			args.name,
			"", // default image: ubuntu:18.04
			[]string{"bash"},
			[]string{"cp /pfs/*/* /pfs/out/"},
			&pps.ParallelismSpec{Constant: 1},
			client.NewPFSInput(args.repo, "/*"),
			"", // default output branch: master
			args.update,
		)
	}
	alice, bob := robot(tu.UniqueString("alice")), robot(tu.UniqueString("bob"))
	aliceClient, bobClient := tu.GetAuthenticatedPachClient(t, alice), tu.GetAuthenticatedPachClient(t, bob)

	// create repo, and check that alice is the owner of the new repo
	dataRepo := tu.UniqueString(t.Name())
	require.NoError(t, aliceClient.CreateRepo(dataRepo))
	require.Equal(t,
		buildBindings(alice, auth.RepoOwnerRole), getRepoRoleBinding(t, aliceClient, dataRepo))

	// alice can create a pipeline (she owns the input repo)
	pipeline := tu.UniqueString("alice-pipeline")
	require.NoError(t, createPipeline(createArgs{
		client: aliceClient,
		name:   pipeline,
		repo:   dataRepo,
	}))
	require.OneOfEquals(t, pipeline, PipelineNames(t, aliceClient))
	// check that alice owns the output repo too)
	require.Equal(t,
		buildBindings(alice, auth.RepoOwnerRole, pl(pipeline), auth.RepoWriterRole), getRepoRoleBinding(t, aliceClient, pipeline))

	// Make sure alice's pipeline runs successfully
	err := aliceClient.PutFile(dataRepo, "master", tu.UniqueString("/file"),
		strings.NewReader("test data"))
	require.NoError(t, err)
	require.NoErrorWithinT(t, 60*time.Second, func() error {
		_, err := aliceClient.FlushCommitAll(
			[]*pfs.Commit{client.NewCommit(dataRepo, "master")},
			[]*pfs.Repo{client.NewRepo(pipeline)},
		)
		return err
	})

	// bob can't create a pipeline
	badPipeline := tu.UniqueString("bob-bad")
	err = createPipeline(createArgs{
		client: bobClient,
		name:   badPipeline,
		repo:   dataRepo,
	})
	require.YesError(t, err)
	require.Matches(t, "not authorized", err.Error())
	require.NoneEquals(t, badPipeline, PipelineNames(t, aliceClient))

	// alice adds bob as a reader of the input repo
	require.NoError(t, aliceClient.ModifyRepoRoleBinding(dataRepo, bob, []string{auth.RepoReaderRole}))

	// now bob can create a pipeline
	goodPipeline := tu.UniqueString("bob-good")
	require.NoError(t, createPipeline(createArgs{
		client: bobClient,
		name:   goodPipeline,
		repo:   dataRepo,
	}))
	require.OneOfEquals(t, goodPipeline, PipelineNames(t, aliceClient))
	// check that bob owns the output repo too)
	require.Equal(t,
		buildBindings(bob, auth.RepoOwnerRole, pl(goodPipeline), auth.RepoWriterRole), getRepoRoleBinding(t, bobClient, goodPipeline))

	// Make sure bob's pipeline runs successfully
	err = aliceClient.PutFile(dataRepo, "master", tu.UniqueString("/file"),
		strings.NewReader("test data"))
	require.NoError(t, err)
	require.NoErrorWithinT(t, 60*time.Second, func() error {
		_, err := bobClient.FlushCommitAll(
			[]*pfs.Commit{client.NewCommit(dataRepo, "master")},
			[]*pfs.Repo{client.NewRepo(goodPipeline)},
		)
		return err
	})

	// bob can't update alice's pipeline
	infoBefore, err := aliceClient.InspectPipeline(pipeline)
	require.NoError(t, err)
	err = createPipeline(createArgs{
		client: bobClient,
		name:   pipeline,
		repo:   dataRepo,
		update: true,
	})
	require.YesError(t, err)
	require.Matches(t, "not authorized", err.Error())
	infoAfter, err := aliceClient.InspectPipeline(pipeline)
	require.NoError(t, err)
	require.Equal(t, infoBefore.Version, infoAfter.Version)

	// alice adds bob as a writer of the output repo, and removes him as a reader
	// of the input repo
	require.NoError(t, aliceClient.ModifyRepoRoleBinding(pipeline, bob, []string{auth.RepoWriterRole}))
	require.Equal(t,
		buildBindings(alice, auth.RepoOwnerRole, bob, auth.RepoWriterRole, pl(pipeline), auth.RepoWriterRole),
		getRepoRoleBinding(t, aliceClient, pipeline))

	require.NoError(t, aliceClient.ModifyRepoRoleBinding(dataRepo, bob, []string{}))
	require.Equal(t,
		buildBindings(alice, auth.RepoOwnerRole, pl(pipeline), auth.RepoReaderRole, pl(goodPipeline), auth.RepoReaderRole),
		getRepoRoleBinding(t, aliceClient, dataRepo))

	// bob still can't update alice's pipeline
	infoBefore, err = aliceClient.InspectPipeline(pipeline)
	require.NoError(t, err)
	err = createPipeline(createArgs{
		client: bobClient,
		name:   pipeline,
		repo:   dataRepo,
		update: true,
	})
	require.YesError(t, err)
	require.Matches(t, "not authorized", err.Error())
	infoAfter, err = aliceClient.InspectPipeline(pipeline)
	require.NoError(t, err)
	require.Equal(t, infoBefore.Version, infoAfter.Version)

	// alice re-adds bob as a reader of the input repo
	require.NoError(t, aliceClient.ModifyRepoRoleBinding(dataRepo, bob, []string{auth.RepoReaderRole}))
	require.Equal(t,
		buildBindings(alice, auth.RepoOwnerRole, bob, auth.RepoReaderRole, pl(pipeline), auth.RepoReaderRole, pl(goodPipeline), auth.RepoReaderRole),
		getRepoRoleBinding(t, aliceClient, dataRepo))

	// now bob can update alice's pipeline
	infoBefore, err = aliceClient.InspectPipeline(pipeline)
	require.NoError(t, err)
	err = createPipeline(createArgs{
		client: bobClient,
		name:   pipeline,
		repo:   dataRepo,
		update: true,
	})
	require.NoError(t, err)
	infoAfter, err = aliceClient.InspectPipeline(pipeline)
	require.NoError(t, err)
	require.NotEqual(t, infoBefore.Version, infoAfter.Version)

	// Make sure the updated pipeline runs successfully
	err = aliceClient.PutFile(dataRepo, "master", tu.UniqueString("/file"),
		strings.NewReader("test data"))
	require.NoError(t, err)
	require.NoErrorWithinT(t, 60*time.Second, func() error {
		_, err := bobClient.FlushCommitAll(
			[]*pfs.Commit{client.NewCommit(dataRepo, "master")},
			[]*pfs.Repo{client.NewRepo(pipeline)},
		)
		return err
	})
}

func TestPipelineMultipleInputs(t *testing.T) {
	if os.Getenv("RUN_BAD_TESTS") == "" {
		t.Skip("Skipping because RUN_BAD_TESTS was empty")
	}
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}
	tu.DeleteAll(t)
	defer tu.DeleteAll(t)
	type createArgs struct {
		client *client.APIClient
		name   string
		input  *pps.Input
		update bool
	}
	createPipeline := func(args createArgs) error {
		return args.client.CreatePipeline(
			args.name,
			"", // default image: ubuntu:18.04
			[]string{"bash"},
			[]string{"echo \"work\" >/pfs/out/x"},
			&pps.ParallelismSpec{Constant: 1},
			args.input,
			"", // default output branch: master
			args.update,
		)
	}
	alice, bob := robot(tu.UniqueString("alice")), robot(tu.UniqueString("bob"))
	aliceClient, bobClient := tu.GetAuthenticatedPachClient(t, alice), tu.GetAuthenticatedPachClient(t, bob)

	// create two repos, and check that alice is the owner of the new repos
	dataRepo1 := tu.UniqueString(t.Name())
	dataRepo2 := tu.UniqueString(t.Name())
	require.NoError(t, aliceClient.CreateRepo(dataRepo1))
	require.NoError(t, aliceClient.CreateRepo(dataRepo2))
	require.Equal(t,
		buildBindings(alice, auth.RepoOwnerRole), getRepoRoleBinding(t, aliceClient, dataRepo1))
	require.Equal(t,
		buildBindings(alice, auth.RepoOwnerRole), getRepoRoleBinding(t, aliceClient, dataRepo2))

	// alice can create a cross-pipeline with both inputs
	aliceCrossPipeline := tu.UniqueString("alice-cross")
	require.NoError(t, createPipeline(createArgs{
		client: aliceClient,
		name:   aliceCrossPipeline,
		input: client.NewCrossInput(
			client.NewPFSInput(dataRepo1, "/*"),
			client.NewPFSInput(dataRepo2, "/*"),
		),
	}))
	require.OneOfEquals(t, aliceCrossPipeline, PipelineNames(t, aliceClient))
	// check that alice owns the output repo too)
	require.Equal(t,
		buildBindings(alice, auth.RepoOwnerRole, pl(aliceCrossPipeline), auth.RepoWriterRole), getRepoRoleBinding(t, aliceClient, aliceCrossPipeline))

	// alice can create a union-pipeline with both inputs
	aliceUnionPipeline := tu.UniqueString("alice-union")
	require.NoError(t, createPipeline(createArgs{
		client: aliceClient,
		name:   aliceUnionPipeline,
		input: client.NewUnionInput(
			client.NewPFSInput(dataRepo1, "/*"),
			client.NewPFSInput(dataRepo2, "/*"),
		),
	}))
	require.OneOfEquals(t, aliceUnionPipeline, PipelineNames(t, aliceClient))
	// check that alice owns the output repo too)
	require.Equal(t,
		buildBindings(alice, auth.RepoOwnerRole, pl(aliceUnionPipeline), auth.RepoWriterRole), getRepoRoleBinding(t, aliceClient, aliceUnionPipeline))

	// alice adds bob as a reader of one of the input repos, but not the other
	require.NoError(t, aliceClient.ModifyRepoRoleBinding(dataRepo1, bob, []string{auth.RepoReaderRole}))

	// bob cannot create a cross-pipeline with both inputs
	bobCrossPipeline := tu.UniqueString("bob-cross")
	err := createPipeline(createArgs{
		client: bobClient,
		name:   bobCrossPipeline,
		input: client.NewCrossInput(
			client.NewPFSInput(dataRepo1, "/*"),
			client.NewPFSInput(dataRepo2, "/*"),
		),
	})
	require.YesError(t, err)
	require.Matches(t, "not authorized", err.Error())
	require.NoneEquals(t, bobCrossPipeline, PipelineNames(t, aliceClient))

	// bob cannot create a union-pipeline with both inputs
	bobUnionPipeline := tu.UniqueString("bob-union")
	err = createPipeline(createArgs{
		client: bobClient,
		name:   bobUnionPipeline,
		input: client.NewUnionInput(
			client.NewPFSInput(dataRepo1, "/*"),
			client.NewPFSInput(dataRepo2, "/*"),
		),
	})
	require.YesError(t, err)
	require.Matches(t, "not authorized", err.Error())
	require.NoneEquals(t, bobUnionPipeline, PipelineNames(t, aliceClient))

	// alice adds bob as a writer of her pipeline's output
	require.NoError(t, aliceClient.ModifyRepoRoleBinding(aliceCrossPipeline, bob, []string{auth.RepoWriterRole}))

	// bob can update alice's pipeline if he removes one of the inputs
	infoBefore, err := aliceClient.InspectPipeline(aliceCrossPipeline)
	require.NoError(t, err)
	require.NoError(t, createPipeline(createArgs{
		client: bobClient,
		name:   aliceCrossPipeline,
		input: client.NewCrossInput(
			// This cross input deliberately only has one element, to make sure it's
			// not simply rejected for having a cross input
			client.NewPFSInput(dataRepo1, "/*"),
		),
		update: true,
	}))
	infoAfter, err := aliceClient.InspectPipeline(aliceCrossPipeline)
	require.NoError(t, err)
	require.NotEqual(t, infoBefore.Version, infoAfter.Version)

	// bob cannot update alice's to put the second input back
	infoBefore, err = aliceClient.InspectPipeline(aliceCrossPipeline)
	require.NoError(t, err)
	err = createPipeline(createArgs{
		client: bobClient,
		name:   aliceCrossPipeline,
		input: client.NewCrossInput(
			client.NewPFSInput(dataRepo1, "/*"),
			client.NewPFSInput(dataRepo2, "/*"),
		),
		update: true,
	})
	require.YesError(t, err)
	require.Matches(t, "not authorized", err.Error())
	infoAfter, err = aliceClient.InspectPipeline(aliceCrossPipeline)
	require.NoError(t, err)
	require.Equal(t, infoBefore.Version, infoAfter.Version)

	// alice adds bob as a reader of the second input
	require.NoError(t, aliceClient.ModifyRepoRoleBinding(dataRepo2, bob, []string{auth.RepoReaderRole}))

	// bob can now update alice's to put the second input back
	infoBefore, err = aliceClient.InspectPipeline(aliceCrossPipeline)
	require.NoError(t, err)
	require.NoError(t, createPipeline(createArgs{
		client: bobClient,
		name:   aliceCrossPipeline,
		input: client.NewCrossInput(
			client.NewPFSInput(dataRepo1, "/*"),
			client.NewPFSInput(dataRepo2, "/*"),
		),
		update: true,
	}))
	infoAfter, err = aliceClient.InspectPipeline(aliceCrossPipeline)
	require.NoError(t, err)
	require.NotEqual(t, infoBefore.Version, infoAfter.Version)

	// bob can create a cross-pipeline with both inputs
	require.NoError(t, createPipeline(createArgs{
		client: bobClient,
		name:   bobCrossPipeline,
		input: client.NewCrossInput(
			client.NewPFSInput(dataRepo1, "/*"),
			client.NewPFSInput(dataRepo2, "/*"),
		),
	}))
	require.OneOfEquals(t, bobCrossPipeline, PipelineNames(t, aliceClient))

	// bob can create a union-pipeline with both inputs
	require.NoError(t, createPipeline(createArgs{
		client: bobClient,
		name:   bobUnionPipeline,
		input: client.NewUnionInput(
			client.NewPFSInput(dataRepo1, "/*"),
			client.NewPFSInput(dataRepo2, "/*"),
		),
	}))
	require.OneOfEquals(t, bobUnionPipeline, PipelineNames(t, aliceClient))

}

// TestPipelineRevoke tests revoking the privileges of a pipeline's creator as
// well as revoking the pipeline itself.
//
// When pipelines inherited privileges from their creator, revoking the owner's
// access to the pipeline's inputs would cause pipelines to stop running, but
// now it does not. In general, this should actually be more secure--it used to
// be that any pipeline Bob created could access any repo that Bob could, even
// if the repo was unrelated to the pipeline (making pipelines a powerful
// vector for privilege escalation). Now pipelines are their own principals,
// and they can only read from their inputs and write to their outputs.
//
// Ideally both would be required: if either the pipeline's access to its inputs
// or bob's access to the pipeline's inputs are revoked, the pipeline should
// stop, but for now it's required to revoke the pipeline's access directly
func TestPipelineRevoke(t *testing.T) {
	t.Skip("TestPipelineRevoke is broken")
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}
	tu.DeleteAll(t)
	defer tu.DeleteAll(t)
	alice, bob := robot(tu.UniqueString("alice")), robot(tu.UniqueString("bob"))
	aliceClient, bobClient := tu.GetAuthenticatedPachClient(t, alice), tu.GetAuthenticatedPachClient(t, bob)

	// alice creates a repo, and adds bob as a reader
	repo := tu.UniqueString(t.Name())
	require.NoError(t, aliceClient.CreateRepo(repo))
	require.NoError(t, aliceClient.ModifyRepoRoleBinding(repo, bob, []string{auth.RepoReaderRole}))
	require.Equal(t,
		buildBindings(alice, auth.RepoOwnerRole, bob, auth.RepoReaderRole), getRepoRoleBinding(t, aliceClient, repo))

	// bob creates a pipeline
	pipeline := tu.UniqueString("bob-pipeline")
	require.NoError(t, bobClient.CreatePipeline(
		pipeline,
		"", // default image: ubuntu:18.04
		[]string{"bash"},
		[]string{"cp /pfs/*/* /pfs/out/"},
		&pps.ParallelismSpec{Constant: 1},
		client.NewPFSInput(repo, "/*"),
		"", // default output branch: master
		false,
	))
	require.Equal(t,
		buildBindings(bob, auth.RepoOwnerRole, pl(pipeline), auth.RepoWriterRole), getRepoRoleBinding(t, bobClient, pipeline))
	// bob adds alice as a reader of the pipeline's output repo, so alice can
	// flush input commits (which requires her to inspect commits in the output)
	// and update the pipeline
	require.NoError(t, bobClient.ModifyRepoRoleBinding(pipeline, alice, []string{auth.RepoWriterRole}))
	require.Equal(t,
		buildBindings(bob, auth.RepoOwnerRole, alice, auth.RepoWriterRole, pl(pipeline), auth.RepoWriterRole),
		getRepoRoleBinding(t, bobClient, pipeline))

	// alice commits to the input repo, and the pipeline runs successfully
	require.NoError(t, aliceClient.PutFile(repo, "master", "/file", strings.NewReader("test")))
	require.NoErrorWithinT(t, 45*time.Second, func() error {
		_, err := bobClient.FlushCommitAll(
			[]*pfs.Commit{client.NewCommit(repo, "master")},
			[]*pfs.Repo{client.NewRepo(pipeline)},
		)
		return err
	})

	// alice removes bob as a reader of her repo, and then commits to the input
	// repo, but bob's pipeline still runs (it has its own principal--it doesn't
	// inherit bob's privileges)
	require.NoError(t, aliceClient.ModifyRepoRoleBinding(repo, bob, []string{}))
	require.Equal(t,
		buildBindings(alice, auth.RepoOwnerRole, pl(pipeline), auth.RepoReaderRole), getRepoRoleBinding(t, aliceClient, repo))
	require.NoError(t, aliceClient.PutFile(repo, "master", "/file", strings.NewReader("test")))
	require.NoErrorWithinT(t, 45*time.Second, func() error {
		_, err := aliceClient.FlushCommitAll(
			[]*pfs.Commit{client.NewCommit(repo, "master")},
			[]*pfs.Repo{client.NewRepo(pipeline)},
		)
		return err
	})

	// alice revokes the pipeline's access to 'repo' directly, and the pipeline
	// stops running
	require.NoError(t, aliceClient.ModifyRepoRoleBinding(repo, pl(pipeline), []string{}))
	require.NoError(t, aliceClient.PutFile(repo, "master", "/file", strings.NewReader("test")))
	doneCh := make(chan struct{})
	go func() {
		defer close(doneCh)
		_, err := aliceClient.FlushCommitAll(
			[]*pfs.Commit{client.NewCommit(repo, "master")},
			[]*pfs.Repo{client.NewRepo(pipeline)},
		)
		require.NoError(t, err)
	}()
	select {
	case <-doneCh:
		t.Fatal("pipeline should not be able to finish with no access")
	case <-time.After(45 * time.Second):
	}

	// alice updates bob's pipline, but the pipeline still doesn't run
	require.NoError(t, aliceClient.CreatePipeline(
		pipeline,
		"", // default image: ubuntu:18.04
		[]string{"bash"},
		[]string{"cp /pfs/*/* /pfs/out/"},
		&pps.ParallelismSpec{Constant: 1},
		client.NewPFSInput(repo, "/*"),
		"", // default output branch: master
		true,
	))
	require.NoError(t, aliceClient.PutFile(repo, "master", "/file", strings.NewReader("test")))
	doneCh = make(chan struct{})
	go func() {
		defer close(doneCh)
		_, err := aliceClient.FlushCommitAll(
			[]*pfs.Commit{client.NewCommit(repo, "master")},
			[]*pfs.Repo{client.NewRepo(pipeline)},
		)
		require.NoError(t, err)
	}()
	select {
	case <-doneCh:
		t.Fatal("pipeline should not be able to finish with no access")
	case <-time.After(45 * time.Second):
	}

	// alice restores the pipeline's access to its input repo, and now the
	// pipeline runs successfully
	require.NoError(t, aliceClient.ModifyRepoRoleBinding(repo, pl(pipeline), []string{auth.RepoReaderRole}))
	require.NoErrorWithinT(t, 45*time.Second, func() error {
		_, err := aliceClient.FlushCommitAll(
			[]*pfs.Commit{client.NewCommit(repo, "master")},
			[]*pfs.Repo{client.NewRepo(pipeline)},
		)
		return err
	})
}

func TestStopAndDeletePipeline(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}
	tu.DeleteAll(t)
	defer tu.DeleteAll(t)
	alice, bob := robot(tu.UniqueString("alice")), robot(tu.UniqueString("bob"))
	aliceClient, bobClient := tu.GetAuthenticatedPachClient(t, alice), tu.GetAuthenticatedPachClient(t, bob)

	// alice creates a repo
	repo := tu.UniqueString(t.Name())
	require.NoError(t, aliceClient.CreateRepo(repo))
	require.Equal(t, buildBindings(alice, auth.RepoOwnerRole), getRepoRoleBinding(t, aliceClient, repo))

	// alice creates a pipeline
	pipeline := tu.UniqueString("alice-pipeline")
	require.NoError(t, aliceClient.CreatePipeline(
		pipeline,
		"", // default image: ubuntu:18.04
		[]string{"bash"},
		[]string{"cp /pfs/*/* /pfs/out/"},
		&pps.ParallelismSpec{Constant: 1},
		client.NewPFSInput(repo, "/*"),
		"", // default output branch: master
		false,
	))
	// Make sure the input and output repos have non-empty ACLs
	require.Equal(t,
		buildBindings(alice, auth.RepoOwnerRole, pl(pipeline), auth.RepoReaderRole), getRepoRoleBinding(t, aliceClient, repo))
	require.Equal(t,
		buildBindings(alice, auth.RepoOwnerRole, pl(pipeline), auth.RepoWriterRole), getRepoRoleBinding(t, aliceClient, pipeline))

	// alice stops the pipeline (owner of the input and output repos can stop)
	require.NoError(t, aliceClient.StopPipeline(pipeline))

	// Make sure the remaining input and output repos *still* have non-empty ACLs
	require.Equal(t,
		buildBindings(alice, auth.RepoOwnerRole, pl(pipeline), auth.RepoReaderRole), getRepoRoleBinding(t, aliceClient, repo))
	require.Equal(t,
		buildBindings(alice, auth.RepoOwnerRole, pl(pipeline), auth.RepoWriterRole), getRepoRoleBinding(t, aliceClient, pipeline))

	// alice deletes the pipeline (owner of the input and output repos can delete)
	require.NoError(t, aliceClient.DeletePipeline(pipeline, false))
	require.Nil(t, getRepoRoleBinding(t, aliceClient, pipeline).Entries)

	// alice deletes the input repo (make sure the input repo's ACL is gone)
	require.NoError(t, aliceClient.DeleteRepo(repo, false))
	require.Nil(t, getRepoRoleBinding(t, aliceClient, repo).Entries)

	// alice creates another repo
	repo = tu.UniqueString(t.Name())
	require.NoError(t, aliceClient.CreateRepo(repo))
	require.Equal(t, buildBindings(alice, auth.RepoOwnerRole), getRepoRoleBinding(t, aliceClient, repo))

	// alice creates another pipeline
	pipeline = tu.UniqueString("alice-pipeline")
	require.NoError(t, aliceClient.CreatePipeline(
		pipeline,
		"", // default image: ubuntu:18.04
		[]string{"bash"},
		[]string{"cp /pfs/*/* /pfs/out/"},
		&pps.ParallelismSpec{Constant: 1},
		client.NewPFSInput(repo, "/*"),
		"", // default output branch: master
		false,
	))

	// bob can't stop or delete alice's pipeline
	err := bobClient.StopPipeline(pipeline)
	require.YesError(t, err)
	require.Matches(t, "not authorized", err.Error())
	err = bobClient.DeletePipeline(pipeline, false)
	require.YesError(t, err)
	require.Matches(t, "not authorized", err.Error())

	// alice adds bob as a reader of the input repo
	require.NoError(t, aliceClient.ModifyRepoRoleBinding(repo, bob, []string{auth.RepoReaderRole}))
	require.Equal(t,
		buildBindings(alice, auth.RepoOwnerRole, bob, auth.RepoReaderRole, pl(pipeline), auth.RepoReaderRole),
		getRepoRoleBinding(t, aliceClient, repo))

	// bob still can't stop or delete alice's pipeline
	err = bobClient.StopPipeline(pipeline)
	require.YesError(t, err)
	require.Matches(t, "not authorized", err.Error())
	err = bobClient.DeletePipeline(pipeline, false)
	require.YesError(t, err)
	require.Matches(t, "not authorized", err.Error())

	// alice removes bob as a reader of the input repo and adds bob as a writer of
	// the output repo
	require.NoError(t, aliceClient.ModifyRepoRoleBinding(repo, bob, []string{}))

	require.Equal(t,
		buildBindings(alice, auth.RepoOwnerRole, pl(pipeline), auth.RepoReaderRole), getRepoRoleBinding(t, aliceClient, repo))
	require.NoError(t, aliceClient.ModifyRepoRoleBinding(pipeline, bob, []string{auth.RepoWriterRole}))
	require.Equal(t,
		buildBindings(alice, auth.RepoOwnerRole, bob, auth.RepoWriterRole, pl(pipeline), auth.RepoWriterRole),
		getRepoRoleBinding(t, aliceClient, pipeline))

	// bob still can't stop or delete alice's pipeline
	err = bobClient.StopPipeline(pipeline)
	require.YesError(t, err)
	require.Matches(t, "not authorized", err.Error())
	err = bobClient.DeletePipeline(pipeline, false)
	require.YesError(t, err)
	require.Matches(t, "not authorized", err.Error())

	// alice re-adds bob as a reader of the input repo
	require.NoError(t, aliceClient.ModifyRepoRoleBinding(repo, bob, []string{auth.RepoReaderRole}))
	require.Equal(t,
		buildBindings(alice, auth.RepoOwnerRole, bob, auth.RepoReaderRole, pl(pipeline), auth.RepoReaderRole),
		getRepoRoleBinding(t, aliceClient, repo))

	// bob can stop (and start) but not delete alice's pipeline
	err = bobClient.StopPipeline(pipeline)
	require.NoError(t, err)
	err = bobClient.StartPipeline(pipeline)
	require.NoError(t, err)
	err = bobClient.DeletePipeline(pipeline, false)
	require.YesError(t, err)
	require.Matches(t, "not authorized", err.Error())

	// alice adds bob as an owner of the output repo
	require.NoError(t, aliceClient.ModifyRepoRoleBinding(pipeline, bob, []string{auth.RepoOwnerRole}))
	require.Equal(t,
		buildBindings(alice, auth.RepoOwnerRole, bob, auth.RepoOwnerRole, pl(pipeline), auth.RepoWriterRole),
		getRepoRoleBinding(t, aliceClient, pipeline))

	// finally bob can stop and delete alice's pipeline
	err = bobClient.StopPipeline(pipeline)
	require.NoError(t, err)
	err = bobClient.DeletePipeline(pipeline, false)
	require.NoError(t, err)
}

// TestStopJob just confirms that the StopJob API works when auth is on
func TestStopJob(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}
	tu.DeleteAll(t)
	alice := robot(tu.UniqueString("alice"))
	aliceClient := tu.GetAuthenticatedPachClient(t, alice)

	// alice creates a repo
	repo := tu.UniqueString(t.Name())
	require.NoError(t, aliceClient.CreateRepo(repo))
	require.Equal(t, buildBindings(alice, auth.RepoOwnerRole), getRepoRoleBinding(t, aliceClient, repo))
	err := aliceClient.PutFile(repo, "master", "/file", strings.NewReader("test"))
	require.NoError(t, err)

	// alice creates a pipeline
	pipeline := tu.UniqueString("alice-pipeline")
	require.NoError(t, aliceClient.CreatePipeline(
		pipeline,
		"", // default image: ubuntu:18.04
		[]string{"bash"},
		[]string{"sleep 600"},
		&pps.ParallelismSpec{Constant: 1},
		client.NewPFSInput(repo, "/*"),
		"", // default output branch: master
		false,
	))
	// Make sure the input and output repos have non-empty ACLs
	require.Equal(t,
		buildBindings(alice, auth.RepoOwnerRole, pl(pipeline), auth.RepoReaderRole), getRepoRoleBinding(t, aliceClient, repo))
	require.Equal(t,
		buildBindings(alice, auth.RepoOwnerRole, pl(pipeline), auth.RepoWriterRole), getRepoRoleBinding(t, aliceClient, pipeline))

	// Stop the first job in 'pipeline'
	var jobID string
	require.NoErrorWithinTRetry(t, 30*time.Second, func() error {
		jobs, err := aliceClient.ListJob(pipeline, nil /*inputs*/, nil /*output*/, -1 /*history*/, true /* full */)
		if err != nil {
			return err
		}
		if len(jobs) != 1 {
			return errors.Errorf("expected one job but got %d", len(jobs))
		}
		jobID = jobs[0].Job.ID
		return nil
	})

	require.NoError(t, aliceClient.StopJob(jobID))
	require.NoErrorWithinTRetry(t, 30*time.Second, func() error {
		pji, err := aliceClient.InspectJob(jobID, false)
		if err != nil {
			return errors.Wrapf(err, "could not inspect job %q", jobID)
		}
		if pji.State != pps.JobState_JOB_KILLED {
			return errors.Errorf("expected job %q to be in JOB_KILLED but was in %s", jobID, pji.State.String())
		}
		return nil
	})
}

// Test ListRepo checks that the auth information returned by ListRepo and
// InspectRepo is correct.
// TODO(msteffen): This should maybe go in pachyderm_test, since ListRepo isn't
// an auth API call
func TestListAndInspectRepo(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}
	tu.DeleteAll(t)
	defer tu.DeleteAll(t)
	alice, bob := robot(tu.UniqueString("alice")), robot(tu.UniqueString("bob"))
	aliceClient, bobClient := tu.GetAuthenticatedPachClient(t, alice), tu.GetAuthenticatedPachClient(t, bob)

	// alice creates a repo and makes Bob a writer
	repoWriter := tu.UniqueString(t.Name())
	require.NoError(t, aliceClient.CreateRepo(repoWriter))
	require.NoError(t, aliceClient.ModifyRepoRoleBinding(repoWriter, bob, []string{auth.RepoWriterRole}))
	require.Equal(t,
		buildBindings(alice, auth.RepoOwnerRole, bob, auth.RepoWriterRole), getRepoRoleBinding(t, aliceClient, repoWriter))

	// alice creates a repo and makes Bob a reader
	repoReader := tu.UniqueString(t.Name())
	require.NoError(t, aliceClient.CreateRepo(repoReader))
	require.NoError(t, aliceClient.ModifyRepoRoleBinding(repoReader, bob, []string{auth.RepoReaderRole}))
	require.Equal(t,
		buildBindings(alice, auth.RepoOwnerRole, bob, auth.RepoReaderRole), getRepoRoleBinding(t, aliceClient, repoReader))

	// alice creates a repo and gives Bob no access privileges
	repoNone := tu.UniqueString(t.Name())
	require.NoError(t, aliceClient.CreateRepo(repoNone))
	require.Equal(t,
		buildBindings(alice, auth.RepoOwnerRole), getRepoRoleBinding(t, aliceClient, repoNone))

	// bob creates a repo
	repoOwner := tu.UniqueString(t.Name())
	require.NoError(t, bobClient.CreateRepo(repoOwner))
	require.Equal(t, buildBindings(bob, auth.RepoOwnerRole), getRepoRoleBinding(t, bobClient, repoOwner))

	// Bob calls ListRepo, and the response must indicate the correct access scope
	// for each repo (because other tests have run, we may see repos besides the
	// above. Bob's access to those should be NONE
	listResp, err := bobClient.PfsAPIClient.ListRepo(bobClient.Ctx(),
		&pfs.ListRepoRequest{})
	require.NoError(t, err)
	expectedPermissions := map[string][]auth.Permission{
		repoOwner: []auth.Permission{
			auth.Permission_REPO_READ,
			auth.Permission_REPO_WRITE,
			auth.Permission_REPO_MODIFY_BINDINGS,
			auth.Permission_REPO_DELETE,
			auth.Permission_REPO_INSPECT_COMMIT,
			auth.Permission_REPO_LIST_COMMIT,
			auth.Permission_REPO_DELETE_COMMIT,
			auth.Permission_REPO_CREATE_BRANCH,
			auth.Permission_REPO_LIST_BRANCH,
			auth.Permission_REPO_DELETE_BRANCH,
			auth.Permission_REPO_LIST_FILE,
			auth.Permission_REPO_ADD_PIPELINE_READER,
			auth.Permission_REPO_REMOVE_PIPELINE_READER,
			auth.Permission_REPO_ADD_PIPELINE_WRITER,
			auth.Permission_REPO_INSPECT_FILE,
			auth.Permission_PIPELINE_LIST_JOB,
		},
		repoWriter: []auth.Permission{
			auth.Permission_REPO_READ,
			auth.Permission_REPO_WRITE,
			auth.Permission_REPO_INSPECT_COMMIT,
			auth.Permission_REPO_LIST_COMMIT,
			auth.Permission_REPO_DELETE_COMMIT,
			auth.Permission_REPO_CREATE_BRANCH,
			auth.Permission_REPO_LIST_BRANCH,
			auth.Permission_REPO_DELETE_BRANCH,
			auth.Permission_REPO_LIST_FILE,
			auth.Permission_REPO_ADD_PIPELINE_READER,
			auth.Permission_REPO_REMOVE_PIPELINE_READER,
			auth.Permission_REPO_ADD_PIPELINE_WRITER,
			auth.Permission_REPO_INSPECT_FILE,
			auth.Permission_PIPELINE_LIST_JOB,
		},
		repoReader: []auth.Permission{
			auth.Permission_REPO_READ,
			auth.Permission_REPO_INSPECT_COMMIT,
			auth.Permission_REPO_LIST_COMMIT,
			auth.Permission_REPO_LIST_BRANCH,
			auth.Permission_REPO_LIST_FILE,
			auth.Permission_REPO_ADD_PIPELINE_READER,
			auth.Permission_REPO_REMOVE_PIPELINE_READER,
			auth.Permission_REPO_INSPECT_FILE,
			auth.Permission_PIPELINE_LIST_JOB,
		},
	}
	for _, info := range listResp.RepoInfo {
		require.ElementsEqual(t, expectedPermissions[info.Repo.Name], info.AuthInfo.Permissions)
	}

	for _, name := range []string{repoOwner, repoWriter, repoReader, repoNone} {
		inspectResp, err := bobClient.PfsAPIClient.InspectRepo(bobClient.Ctx(),
			&pfs.InspectRepoRequest{
				Repo: &pfs.Repo{Name: name},
			})
		require.NoError(t, err)
		require.ElementsEqual(t, expectedPermissions[name], inspectResp.AuthInfo.Permissions)
	}
}

// TestGetPermissions tests that GetPermissions and GetPermissionsForPrincipal work for repos and the cluster itself
func TestGetPermissions(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}
	tu.DeleteAll(t)
	defer tu.DeleteAll(t)
	alice, bob := robot(tu.UniqueString("alice")), robot(tu.UniqueString("bob"))
	aliceClient, rootClient := tu.GetAuthenticatedPachClient(t, alice), tu.GetAuthenticatedPachClient(t, auth.RootUser)

	// alice creates a repo and makes Bob a writer
	repo := tu.UniqueString(t.Name())
	require.NoError(t, aliceClient.CreateRepo(repo))
	require.NoError(t, aliceClient.ModifyRepoRoleBinding(repo, bob, []string{auth.RepoWriterRole}))

	// alice can get her own permissions on the cluster (none) and on the repo (repoOwner)
	permissions, err := aliceClient.GetPermissions(aliceClient.Ctx(), &auth.GetPermissionsRequest{Resource: &auth.Resource{Type: auth.ResourceType_CLUSTER}})
	require.NoError(t, err)
	require.Nil(t, permissions.Roles)

	permissions, err = aliceClient.GetPermissions(aliceClient.Ctx(), &auth.GetPermissionsRequest{Resource: &auth.Resource{Type: auth.ResourceType_REPO, Name: repo}})
	require.NoError(t, err)
	require.Equal(t, []string{"repoOwner"}, permissions.Roles)

	// the root user can get bob's permissions
	permissions, err = rootClient.GetPermissionsForPrincipal(rootClient.Ctx(), &auth.GetPermissionsForPrincipalRequest{Resource: &auth.Resource{Type: auth.ResourceType_CLUSTER}, Principal: bob})
	require.NoError(t, err)
	require.Nil(t, permissions.Roles)

	permissions, err = rootClient.GetPermissionsForPrincipal(rootClient.Ctx(), &auth.GetPermissionsForPrincipalRequest{Resource: &auth.Resource{Type: auth.ResourceType_REPO, Name: repo}, Principal: bob})
	require.NoError(t, err)
	require.Equal(t, []string{"repoWriter"}, permissions.Roles)

	// alice cannot get bob's permissions
	_, err = aliceClient.GetPermissionsForPrincipal(aliceClient.Ctx(), &auth.GetPermissionsForPrincipalRequest{Resource: &auth.Resource{Type: auth.ResourceType_CLUSTER}, Principal: bob})
	require.YesError(t, err)
	require.Matches(t, "is not authorized to perform this operation - needs permissions", err.Error())

	_, err = aliceClient.GetPermissionsForPrincipal(aliceClient.Ctx(), &auth.GetPermissionsForPrincipalRequest{Resource: &auth.Resource{Type: auth.ResourceType_REPO, Name: repo}, Principal: bob})
	require.YesError(t, err)
	require.Matches(t, "is not authorized to perform this operation - needs permissions", err.Error())
}

func TestUnprivilegedUserCannotMakeSelfOwner(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}
	tu.DeleteAll(t)
	defer tu.DeleteAll(t)
	alice, bob := robot(tu.UniqueString("alice")), robot(tu.UniqueString("bob"))
	aliceClient, bobClient := tu.GetAuthenticatedPachClient(t, alice), tu.GetAuthenticatedPachClient(t, bob)

	// alice creates a repo
	repo := tu.UniqueString(t.Name())
	require.NoError(t, aliceClient.CreateRepo(repo))
	require.Equal(t,
		buildBindings(alice, auth.RepoOwnerRole), getRepoRoleBinding(t, aliceClient, repo))

	// bob calls SetScope(bob, OWNER) on alice's repo. This should fail
	err := bobClient.ModifyRepoRoleBinding(repo, bob, []string{auth.RepoOwnerRole})
	require.YesError(t, err)
	// make sure ACL wasn't updated
	require.Equal(t, buildBindings(alice, auth.RepoOwnerRole), getRepoRoleBinding(t, aliceClient, repo))
}

// TestListRepoNotLoggedInError makes sure that if a user isn't logged in, and
// they call ListRepo(), they get an error.
func TestListRepoNotLoggedInError(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}
	tu.DeleteAll(t)
	defer tu.DeleteAll(t)
	alice := robot(tu.UniqueString("alice"))
	aliceClient, anonClient := tu.GetAuthenticatedPachClient(t, alice), tu.GetUnauthenticatedPachClient(t)

	// alice creates a repo
	repo := tu.UniqueString(t.Name())
	require.NoError(t, aliceClient.CreateRepo(repo))
	require.Equal(t,
		buildBindings(alice, auth.RepoOwnerRole), getRepoRoleBinding(t, aliceClient, repo))

	// Anon (non-logged-in user) calls ListRepo, and must receive an error
	_, err := anonClient.PfsAPIClient.ListRepo(anonClient.Ctx(),
		&pfs.ListRepoRequest{})
	require.YesError(t, err)
	require.Matches(t, "no authentication token", err.Error())
}

// TestListRepoNoAuthInfoIfDeactivated tests that if auth isn't activated, then
// ListRepo returns RepoInfos where AuthInfo isn't set (i.e. is nil)
func TestListRepoNoAuthInfoIfDeactivated(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}
	tu.DeleteAll(t)
	defer tu.DeleteAll(t)
	// Dont't run this test in parallel, since it deactivates the auth system
	// globally, so any tests running concurrently will fail
	alice, bob := robot(tu.UniqueString("alice")), robot(tu.UniqueString("bob"))
	aliceClient, bobClient := tu.GetAuthenticatedPachClient(t, alice), tu.GetAuthenticatedPachClient(t, bob)
	adminClient := tu.GetAuthenticatedPachClient(t, auth.RootUser)

	// alice creates a repo
	repo := tu.UniqueString(t.Name())
	require.NoError(t, aliceClient.CreateRepo(repo))

	// bob calls ListRepo, but has NONE access to all repos
	infos, err := bobClient.ListRepo()
	require.NoError(t, err)
	for _, info := range infos {
		require.Nil(t, info.AuthInfo.Permissions)
	}

	// Deactivate auth
	_, err = adminClient.Deactivate(adminClient.Ctx(), &auth.DeactivateRequest{})
	require.NoError(t, err)

	// Wait for auth to be deactivated
	require.NoError(t, backoff.Retry(func() error {
		_, err := aliceClient.WhoAmI(aliceClient.Ctx(), &auth.WhoAmIRequest{})
		if err != nil && auth.IsErrNotActivated(err) {
			return nil // WhoAmI should fail when auth is deactivated
		}
		return errors.New("auth is not yet deactivated")
	}, backoff.NewTestingBackOff()))

	// bob calls ListRepo, now AuthInfo isn't set anywhere
	infos, err = bobClient.ListRepo()
	require.NoError(t, err)
	for _, info := range infos {
		require.Nil(t, info.AuthInfo)
	}
}

// TestCreateRepoAlreadyExistsError tests that creating a repo that already
// exists gives you an error to that effect, even when auth is already
// activated (rather than "access denied")
func TestCreateRepoAlreadyExistsError(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}
	tu.DeleteAll(t)
	defer tu.DeleteAll(t)
	alice, bob := robot(tu.UniqueString("alice")), robot(tu.UniqueString("bob"))
	aliceClient, bobClient := tu.GetAuthenticatedPachClient(t, alice), tu.GetAuthenticatedPachClient(t, bob)

	// alice creates a repo
	repo := tu.UniqueString(t.Name())
	require.NoError(t, aliceClient.CreateRepo(repo))

	// bob creates the same repo, and should get an error to the effect that the
	// repo already exists (rather than "access denied")
	err := bobClient.CreateRepo(repo)
	require.YesError(t, err)
	require.Matches(t, "already exists", err.Error())
}

// TestCreateRepoNotLoggedInError makes sure that if a user isn't logged in, and
// they call CreateRepo(), they get an error.
func TestCreateRepoNotLoggedInError(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}
	tu.DeleteAll(t)
	defer tu.DeleteAll(t)
	tu.ActivateAuth(t)
	anonClient := tu.GetUnauthenticatedPachClient(t)

	// anonClient tries and fails to create a repo
	repo := tu.UniqueString(t.Name())
	err := anonClient.CreateRepo(repo)
	require.YesError(t, err)
	require.Matches(t, "no authentication token", err.Error())
}

// Creating a pipeline when the output repo already exists gives is allowed
// (assuming write permission)
// this used to return a specific error regardless of permissions, in contrast
// to the auth-disabled behavior
func TestCreatePipelineRepoAlreadyExistsPermissions(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}
	tu.DeleteAll(t)
	defer tu.DeleteAll(t)
	alice, bob := robot(tu.UniqueString("alice")), robot(tu.UniqueString("bob"))
	aliceClient, bobClient := tu.GetAuthenticatedPachClient(t, alice), tu.GetAuthenticatedPachClient(t, bob)

	// alice creates a repo
	inputRepo := tu.UniqueString(t.Name())
	require.NoError(t, aliceClient.CreateRepo(inputRepo))
	require.NoError(t, aliceClient.ModifyRepoRoleBinding(inputRepo, bob, []string{auth.RepoReaderRole}))
	pipeline := tu.UniqueString("pipeline")
	require.NoError(t, aliceClient.CreateRepo(pipeline))

	// bob creates a pipeline, and should get an "access denied" error
	err := bobClient.CreatePipeline(
		pipeline,
		"", // default image: ubuntu:18.04
		[]string{"bash"},
		[]string{"cp /pfs/*/* /pfs/out/"},
		&pps.ParallelismSpec{Constant: 1},
		client.NewPFSInput(inputRepo, "/*"),
		"",    // default output branch: master
		false, // Don't update -- we want an error
	)
	require.YesError(t, err)
	require.Matches(t, "not authorized", err.Error())

	// alice gives bob writer scope on pipeline output repo
	require.NoError(t, aliceClient.ModifyRepoRoleBinding(pipeline, bob, []string{auth.RepoWriterRole}))
	require.NoError(t, bobClient.CreatePipeline(
		pipeline,
		"", // default image: ubuntu:16.04
		[]string{"bash"},
		[]string{"cp /pfs/*/* /pfs/out/"},
		&pps.ParallelismSpec{Constant: 1},
		client.NewPFSInput(inputRepo, "/*"),
		"",    // default output branch: master
		false, // Don't update -- we want an error
	))
}

// TestAuthorizedEveryone tests that Authorized(user, repo, NONE) tests that the
// `allClusterUsers` binding  for an ACL sets the minimum authorized scope
func TestAuthorizedEveryone(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}
	tu.DeleteAll(t)
	defer tu.DeleteAll(t)

	alice, bob := robot(tu.UniqueString("alice")), robot(tu.UniqueString("bob"))
	aliceClient, bobClient := tu.GetAuthenticatedPachClient(t, alice), tu.GetAuthenticatedPachClient(t, bob)

	// alice creates a repo
	repo := tu.UniqueString(t.Name())
	require.NoError(t, aliceClient.CreateRepo(repo))

	// alice is authorized as `OWNER`
	resp, err := aliceClient.Authorize(aliceClient.Ctx(), &auth.AuthorizeRequest{
		Resource: &auth.Resource{Type: auth.ResourceType_REPO, Name: repo},
		Permissions: []auth.Permission{
			auth.Permission_REPO_MODIFY_BINDINGS,
			auth.Permission_REPO_WRITE,
			auth.Permission_REPO_READ,
		},
	})
	require.NoError(t, err)
	require.True(t, resp.Authorized)

	// bob is not authorized
	resp, err = bobClient.Authorize(bobClient.Ctx(), &auth.AuthorizeRequest{
		Resource: &auth.Resource{Type: auth.ResourceType_REPO, Name: repo},
		Permissions: []auth.Permission{
			auth.Permission_REPO_READ,
		},
	})
	require.NoError(t, err)
	require.False(t, resp.Authorized)

	// alice grants everybody WRITER access
	require.NoError(t, aliceClient.ModifyRepoRoleBinding(repo, "allClusterUsers", []string{auth.RepoWriterRole}))

	// alice is still authorized as `OWNER`
	resp, err = aliceClient.Authorize(aliceClient.Ctx(), &auth.AuthorizeRequest{
		Resource: &auth.Resource{Type: auth.ResourceType_REPO, Name: repo},
		Permissions: []auth.Permission{
			auth.Permission_REPO_MODIFY_BINDINGS,
			auth.Permission_REPO_WRITE,
			auth.Permission_REPO_READ,
		},
	})
	require.NoError(t, err)
	require.True(t, resp.Authorized)

	// bob is now authorized as WRITER
	resp, err = bobClient.Authorize(bobClient.Ctx(), &auth.AuthorizeRequest{
		Resource: &auth.Resource{Type: auth.ResourceType_REPO, Name: repo},
		Permissions: []auth.Permission{
			auth.Permission_REPO_WRITE,
			auth.Permission_REPO_READ,
		},
	})
	require.NoError(t, err)
	require.True(t, resp.Authorized)
}

// TestDeleteAll tests that you must be a cluster admin to call DeleteAll
func TestDeleteAll(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}
	tu.DeleteAll(t)
	defer tu.DeleteAll(t)

	alice := robot(tu.UniqueString("alice"))
	aliceClient, adminClient := tu.GetAuthenticatedPachClient(t, alice), tu.GetAuthenticatedPachClient(t, auth.RootUser)

	// alice creates a repo
	repo := tu.UniqueString(t.Name())
	require.NoError(t, adminClient.CreateRepo(repo))

	// alice calls DeleteAll, but it fails
	err := aliceClient.DeleteAll()
	require.YesError(t, err)
	require.Matches(t, "not authorized", err.Error())

	// admin makes alice an fs admin
	require.NoError(t, adminClient.ModifyClusterRoleBinding(alice, []string{auth.RepoOwnerRole}))

	// wait until alice shows up in admin list
	resp, err := aliceClient.GetClusterRoleBinding()
	require.NoError(t, err)
	require.Equal(t, buildClusterBindings(alice, auth.RepoOwnerRole), resp)

	// alice calls DeleteAll but it fails because she's only an fs admin
	err = aliceClient.DeleteAll()
	require.YesError(t, err)
	require.Matches(t, "not authorized", err.Error())

	// admin calls DeleteAll and succeeds
	require.NoError(t, adminClient.DeleteAll())
}

// TestListDatum tests that you must have READER access to all of job's
// input repos to call ListDatum on that job
func TestListDatum(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}
	tu.DeleteAll(t)
	defer tu.DeleteAll(t)
	alice, bob := robot(tu.UniqueString("alice")), robot(tu.UniqueString("bob"))
	aliceClient, bobClient := tu.GetAuthenticatedPachClient(t, alice), tu.GetAuthenticatedPachClient(t, bob)

	// alice creates a repo
	repoA := tu.UniqueString(t.Name())
	require.NoError(t, aliceClient.CreateRepo(repoA))
	repoB := tu.UniqueString(t.Name())
	require.NoError(t, aliceClient.CreateRepo(repoB))

	// alice creates a pipeline
	pipeline := tu.UniqueString("alice-pipeline")
	require.NoError(t, aliceClient.CreatePipeline(
		pipeline,
		"", // default image: ubuntu:18.04
		[]string{"bash"},
		[]string{"ls /pfs/*/*; cp /pfs/*/* /pfs/out/"},
		&pps.ParallelismSpec{Constant: 1},
		client.NewCrossInput(
			client.NewPFSInput(repoA, "/*"),
			client.NewPFSInput(repoB, "/*"),
		),
		"", // default output branch: master
		false,
	))

	// alice commits to the input repos, and the pipeline runs successfully
	for i, repo := range []string{repoA, repoB} {
		var err error
		file := fmt.Sprintf("/file%d", i+1)
		err = aliceClient.PutFile(repo, "master", file, strings.NewReader("test"))
		require.NoError(t, err)
	}
	require.NoErrorWithinT(t, 45*time.Second, func() error {
		_, err := aliceClient.FlushCommitAll(
			[]*pfs.Commit{client.NewCommit(repoB, "master")},
			[]*pfs.Repo{client.NewRepo(pipeline)},
		)
		return err
	})
	jobs, err := aliceClient.ListJob(pipeline, nil /*inputs*/, nil /*output*/, -1 /*history*/, true /* full */)
	require.NoError(t, err)
	require.Equal(t, 2, len(jobs))
	jobID := jobs[0].Job.ID

	// bob cannot call ListDatum
	_, err = bobClient.ListDatumAll(jobID)
	require.YesError(t, err)
	require.True(t, auth.IsErrNotAuthorized(err), err.Error())

	// alice adds bob to repoA, but bob still can't call GetLogs
	require.NoError(t, aliceClient.ModifyRepoRoleBinding(repoA, bob, []string{auth.RepoReaderRole}))
	_, err = bobClient.ListDatumAll(jobID)
	require.YesError(t, err)
	require.True(t, auth.IsErrNotAuthorized(err), err.Error())

	// alice removes bob from repoA and adds bob to repoB, but bob still can't
	// call ListDatum
	require.NoError(t, aliceClient.ModifyRepoRoleBinding(repoA, bob, []string{}))
	require.NoError(t, aliceClient.ModifyRepoRoleBinding(repoB, bob, []string{auth.RepoReaderRole}))
	_, err = bobClient.ListDatumAll(jobID)
	require.YesError(t, err)
	require.True(t, auth.IsErrNotAuthorized(err), err.Error())

	// alice adds bob to repoA, and now bob can call ListDatum
	require.NoError(t, aliceClient.ModifyRepoRoleBinding(repoA, bob, []string{auth.RepoReaderRole}))
	_, err = bobClient.ListDatumAll(jobID)
	require.YesError(t, err)
	require.True(t, auth.IsErrNotAuthorized(err), err.Error())

	// Finally, alice adds bob to the output repo, and now bob can call ListDatum
	require.NoError(t, aliceClient.ModifyRepoRoleBinding(pipeline, bob, []string{auth.RepoReaderRole}))
	dis, err := bobClient.ListDatumAll(jobID)
	require.NoError(t, err)
	files := make(map[string]struct{})
	for _, di := range dis {
		for _, f := range di.Data {
			files[path.Base(f.File.Path)] = struct{}{}
		}
	}
	require.Equal(t, map[string]struct{}{
		"file1": struct{}{},
		"file2": struct{}{},
	}, files)
}

// TestListJob tests that you must have READER access to a pipeline's output
// repo to call ListJob on that pipeline, but a blank ListJob always succeeds
// (but doesn't return a given job if you don't have access to the job's output
// repo)
func TestListJob(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}
	tu.DeleteAll(t)
	defer tu.DeleteAll(t)
	alice, bob := robot(tu.UniqueString("alice")), robot(tu.UniqueString("bob"))
	aliceClient, bobClient := tu.GetAuthenticatedPachClient(t, alice), tu.GetAuthenticatedPachClient(t, bob)

	// alice creates a repo
	repo := tu.UniqueString(t.Name())
	require.NoError(t, aliceClient.CreateRepo(repo))

	// alice creates a pipeline
	pipeline := tu.UniqueString("alice-pipeline")
	require.NoError(t, aliceClient.CreatePipeline(
		pipeline,
		"", // default image: ubuntu:18.04
		[]string{"bash"},
		[]string{"ls /pfs/*/*; cp /pfs/*/* /pfs/out/"},
		&pps.ParallelismSpec{Constant: 1},
		client.NewPFSInput(repo, "/*"),
		"", // default output branch: master
		false,
	))

	// alice commits to the input repos, and the pipeline runs successfully
	var err error
	err = aliceClient.PutFile(repo, "master", "/file", strings.NewReader("test"))
	require.NoError(t, err)
	require.NoErrorWithinT(t, 60*time.Second, func() error {
		_, err := aliceClient.FlushCommitAll(
			[]*pfs.Commit{client.NewCommit(repo, "master")},
			[]*pfs.Repo{client.NewRepo(pipeline)},
		)
		return err
	})
	jobs, err := aliceClient.ListJob(pipeline, nil /*inputs*/, nil /*output*/, -1 /*history*/, true)
	require.NoError(t, err)
	require.Equal(t, 1, len(jobs))
	jobID := jobs[0].Job.ID

	// bob cannot call ListJob on 'pipeline'
	_, err = bobClient.ListJob(pipeline, nil, nil, -1 /*history*/, true)
	require.YesError(t, err)
	require.True(t, auth.IsErrNotAuthorized(err), err.Error())
	// bob can call blank ListJob, but gets no results
	jobs, err = bobClient.ListJob("", nil, nil, -1 /*history*/, true)
	require.NoError(t, err)
	require.Equal(t, 0, len(jobs))

	// alice adds bob to repo, but bob still can't call ListJob on 'pipeline' or
	// get any output
	require.NoError(t, aliceClient.ModifyRepoRoleBinding(repo, bob, []string{auth.RepoReaderRole}))
	_, err = bobClient.ListJob(pipeline, nil, nil, -1 /*history*/, true)
	require.YesError(t, err)
	require.True(t, auth.IsErrNotAuthorized(err), err.Error())
	jobs, err = bobClient.ListJob("", nil, nil, -1 /*history*/, true)
	require.NoError(t, err)
	require.Equal(t, 0, len(jobs))

	// alice removes bob from repo and adds bob to 'pipeline', and now bob can
	// call listJob on 'pipeline', and gets results back from blank listJob
	require.NoError(t, aliceClient.ModifyRepoRoleBinding(repo, bob, []string{}))
	err = aliceClient.ModifyRepoRoleBinding(pipeline, bob, []string{auth.RepoReaderRole})
	require.NoError(t, err)
	jobs, err = bobClient.ListJob(pipeline, nil, nil, -1 /*history*/, true)
	require.NoError(t, err)
	require.Equal(t, 1, len(jobs))
	require.Equal(t, jobID, jobs[0].Job.ID)
	jobs, err = bobClient.ListJob("", nil, nil, -1 /*history*/, true)
	require.NoError(t, err)
	require.Equal(t, 1, len(jobs))
	require.Equal(t, jobID, jobs[0].Job.ID)
}

// TestInspectDatum tests InspectDatum runs even when auth is activated
func TestInspectDatum(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}
	tu.DeleteAll(t)
	defer tu.DeleteAll(t)
	alice := robot(tu.UniqueString("alice"))
	aliceClient := tu.GetAuthenticatedPachClient(t, alice)

	// alice creates a repo
	repo := tu.UniqueString(t.Name())
	require.NoError(t, aliceClient.CreateRepo(repo))

	// alice creates a pipeline (we must enable stats for InspectDatum, which
	// means calling the grpc client function directly)
	pipeline := tu.UniqueString("alice-pipeline")
	_, err := aliceClient.PpsAPIClient.CreatePipeline(aliceClient.Ctx(),
		&pps.CreatePipelineRequest{
			Pipeline: &pps.Pipeline{Name: pipeline},
			Transform: &pps.Transform{
				Cmd:   []string{"bash"},
				Stdin: []string{"cp /pfs/*/* /pfs/out/"},
			},
			ParallelismSpec: &pps.ParallelismSpec{Constant: 1},
			Input:           client.NewPFSInput(repo, "/*"),
			EnableStats:     true,
		})
	require.NoError(t, err)

	// alice commits to the input repo, and the pipeline runs successfully
	err = aliceClient.PutFile(repo, "master", "/file", strings.NewReader("test"))
	require.NoError(t, err)
	require.NoErrorWithinT(t, 60*time.Second, func() error {
		_, err := aliceClient.FlushCommitAll(
			[]*pfs.Commit{client.NewCommit(repo, "master")},
			[]*pfs.Repo{client.NewRepo(pipeline)},
		)
		return err
	})
	jobs, err := aliceClient.ListJob(pipeline, nil /*inputs*/, nil /*output*/, -1 /*history*/, true)
	require.NoError(t, err)
	require.Equal(t, 1, len(jobs))
	jobID := jobs[0].Job.ID

	// ListDatum seems like it may return inconsistent results, so sleep until
	// the /stats branch is written
	// TODO(msteffen): verify if this is true, and if so, why
	time.Sleep(5 * time.Second)
	dis, err := aliceClient.ListDatumAll(jobID)
	require.NoError(t, err)
	require.NoErrorWithinT(t, 60*time.Second, func() error {
		for _, di := range dis {
			if _, err := aliceClient.InspectDatum(jobID, di.Datum.ID); err != nil {
				continue
			}
		}
		return nil
	})
}

// TODO: Make logs work with V2.
// TestGetLogs tests that you must have READER access to all of a job's input
// repos and READER access to its output repo to call GetLogs()
func TestGetLogs(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}
	tu.DeleteAll(t)
	defer tu.DeleteAll(t)
	alice, bob := tu.UniqueString("alice"), tu.UniqueString("bob")
	aliceClient, bobClient := tu.GetAuthenticatedPachClient(t, alice), tu.GetAuthenticatedPachClient(t, bob)

	// alice creates a repo
	repo := tu.UniqueString(t.Name())
	require.NoError(t, aliceClient.CreateRepo(repo))

	// alice creates a pipeline
	pipeline := tu.UniqueString("pipeline")
	require.NoError(t, aliceClient.CreatePipeline(
		pipeline,
		"", // default image: ubuntu:18.04
		[]string{"bash"},
		[]string{"cp /pfs/*/* /pfs/out/"},
		&pps.ParallelismSpec{Constant: 1},
		client.NewPFSInput(repo, "/*"),
		"", // default output branch: master
		false,
	))

	// alice commits to the input repos, and the pipeline runs successfully
	_, err := aliceClient.PutFile(repo, "master", "/file1", strings.NewReader("test"))
	require.NoError(t, err)
	commitIter, err := aliceClient.FlushCommit(
		[]*pfs.Commit{client.NewCommit(repo, "master")},
		[]*pfs.Repo{client.NewRepo(pipeline)},
	)
	require.NoError(t, err)
	require.NoErrorWithinT(t, 60*time.Second, func() error {
		_, err := commitIter.Next()
		return err
	})

	// bob cannot call GetLogs
	iter := bobClient.GetLogs(pipeline, "", nil, "", false, false, 0)
	require.False(t, iter.Next())
	require.YesError(t, iter.Err())
	require.True(t, auth.IsErrNotAuthorized(iter.Err()), iter.Err().Error())

	// bob also can't call GetLogs for the master process
	iter = bobClient.GetLogs(pipeline, "", nil, "", true, false, 0)
	require.False(t, iter.Next())
	require.YesError(t, iter.Err())
	require.True(t, auth.IsErrNotAuthorized(iter.Err()), iter.Err().Error())

	// alice adds bob to the input repo, but bob still can't call GetLogs
	aliceClient.SetScope(aliceClient.Ctx(), &auth.SetScopeRequest{
		Username: bob,
		Scope:    auth.Scope_READER,
		Repo:     repo,
	})
	iter = bobClient.GetLogs(pipeline, "", nil, "", false, false, 0)
	require.False(t, iter.Next())
	require.YesError(t, iter.Err())
	require.True(t, auth.IsErrNotAuthorized(iter.Err()), iter.Err().Error())

	// alice removes bob from the input repo and adds bob to the output repo, but
	// bob still can't call GetLogs
	aliceClient.SetScope(aliceClient.Ctx(), &auth.SetScopeRequest{
		Username: bob,
		Scope:    auth.Scope_NONE,
		Repo:     repo,
	})
	aliceClient.SetScope(aliceClient.Ctx(), &auth.SetScopeRequest{
		Username: bob,
		Scope:    auth.Scope_READER,
		Repo:     pipeline,
	})
	iter = bobClient.GetLogs(pipeline, "", nil, "", false, false, 0)
	require.False(t, iter.Next())
	require.YesError(t, iter.Err())
	require.True(t, auth.IsErrNotAuthorized(iter.Err()), iter.Err().Error())

	// alice adds bob to the output repo, and now bob can call GetLogs
	aliceClient.SetScope(aliceClient.Ctx(), &auth.SetScopeRequest{
		Username: bob,
		Scope:    auth.Scope_READER,
		Repo:     repo,
	})
	iter = bobClient.GetLogs(pipeline, "", nil, "", false, false, 0)
	iter.Next()
	require.NoError(t, iter.Err())

	// bob can also call GetLogs for the master process
	iter = bobClient.GetLogs(pipeline, "", nil, "", true, false, 0)
	iter.Next()
	require.NoError(t, iter.Err())
}

// TestGetLogsFromStats tests that GetLogs still works even when stats are
// enabled
func TestGetLogsFromStats(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}
	tu.DeleteAll(t)
	defer tu.DeleteAll(t)
	alice := tu.UniqueString("alice")
	aliceClient := tu.GetAuthenticatedPachClient(t, alice)

	// alice creates a repo
	repo := tu.UniqueString(t.Name())
	require.NoError(t, aliceClient.CreateRepo(repo))

	// alice creates a pipeline (we must enable stats for InspectDatum, which
	// means calling the grpc client function directly)
	pipeline := tu.UniqueString("alice")
	_, err := aliceClient.PpsAPIClient.CreatePipeline(aliceClient.Ctx(),
		&pps.CreatePipelineRequest{
			Pipeline: &pps.Pipeline{Name: pipeline},
			Transform: &pps.Transform{
				Cmd:   []string{"bash"},
				Stdin: []string{"cp /pfs/*/* /pfs/out/"},
			},
			ParallelismSpec: &pps.ParallelismSpec{Constant: 1},
			Input:           client.NewPFSInput(repo, "/*"),
			EnableStats:     true,
		})
	require.NoError(t, err)

	// alice commits to the input repo, and the pipeline runs successfully
	_, err = aliceClient.PutFile(repo, "master", "/file1", strings.NewReader("test"))
	require.NoError(t, err)
	commitItr, err := aliceClient.FlushCommit(
		[]*pfs.Commit{client.NewCommit(repo, "master")},
		[]*pfs.Repo{client.NewRepo(pipeline)},
	)
	require.NoError(t, err)
	require.NoErrorWithinT(t, 3*time.Minute, func() error {
		_, err := commitItr.Next()
		return err
	})
	jobs, err := aliceClient.ListJob(pipeline, nil /*inputs*/, nil /*output*/, -1 /*history*/, true)
	require.NoError(t, err)
	require.Equal(t, 1, len(jobs))
	jobID := jobs[0].Job.ID

	iter := aliceClient.GetLogs("", jobID, nil, "", false, false, 0)
	require.True(t, iter.Next())
	require.NoError(t, iter.Err())

	iter = aliceClient.GetLogs("", jobID, nil, "", true, false, 0)
	iter.Next()
	require.NoError(t, iter.Err())
}

func TestPipelineNewInput(t *testing.T) {
	if os.Getenv("RUN_BAD_TESTS") == "" {
		t.Skip("Skipping because RUN_BAD_TESTS was empty")
	}
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}
	tu.DeleteAll(t)
	defer tu.DeleteAll(t)
	alice := robot(tu.UniqueString("alice"))
	aliceClient := tu.GetAuthenticatedPachClient(t, alice)

	// alice creates three repos and commits to them
	var repo []string
	for i := 0; i < 3; i++ {
		repo = append(repo, tu.UniqueString(fmt.Sprint("TestPipelineNewInput-", i, "-")))
		require.NoError(t, aliceClient.CreateRepo(repo[i]))
		require.Equal(t, buildBindings(alice, auth.RepoOwnerRole), getRepoRoleBinding(t, aliceClient, repo[i]))

		// Commit to repo
		err := aliceClient.PutFile(
			repo[i], "master", "/"+repo[i], strings.NewReader(repo[i]))
		require.NoError(t, err)
	}

	// alice creates a pipeline
	pipeline := tu.UniqueString("alice-pipeline")
	require.NoError(t, aliceClient.CreatePipeline(
		pipeline,
		"", // default image: ubuntu:18.04
		[]string{"bash"},
		[]string{"cp /pfs/*/* /pfs/out/"},
		&pps.ParallelismSpec{Constant: 1},
		client.NewUnionInput(
			client.NewPFSInput(repo[0], "/*"),
			client.NewPFSInput(repo[1], "/*"),
		),
		"", // default output branch: master
		false,
	))
	// Make sure the input and output repos have appropriate ACLs
	require.Equal(t,
		buildBindings(alice, auth.RepoOwnerRole, pl(pipeline), auth.RepoReaderRole), getRepoRoleBinding(t, aliceClient, repo[0]))
	require.Equal(t,
		buildBindings(alice, auth.RepoOwnerRole, pl(pipeline), auth.RepoReaderRole), getRepoRoleBinding(t, aliceClient, repo[1]))
	require.Equal(t,
		buildBindings(alice, auth.RepoOwnerRole, pl(pipeline), auth.RepoWriterRole), getRepoRoleBinding(t, aliceClient, pipeline))
	// repo[2] is not on pipeline -- doesn't include 'pipeline'
	require.Equal(t,
		buildBindings(alice, auth.RepoOwnerRole), getRepoRoleBinding(t, aliceClient, repo[2]))

	// make sure the pipeline runs
	require.NoErrorWithinT(t, time.Minute, func() error {
		_, err := aliceClient.FlushCommitAll(
			[]*pfs.Commit{client.NewCommit(repo[0], "master")}, nil)
		return err
	})

	// alice updates the pipeline to replace repo[0] with repo[2]
	require.NoError(t, aliceClient.CreatePipeline(
		pipeline,
		"", // default image: ubuntu:18.04
		[]string{"bash"},
		[]string{"cp /pfs/*/* /pfs/out/"},
		&pps.ParallelismSpec{Constant: 1},
		client.NewUnionInput(
			client.NewPFSInput(repo[1], "/*"),
			client.NewPFSInput(repo[2], "/*"),
		),
		"", // default output branch: master
		true,
	))
	// Make sure the input and output repos have appropriate ACLs
	require.Equal(t,
		buildBindings(alice, auth.RepoOwnerRole, pl(pipeline), auth.RepoReaderRole), getRepoRoleBinding(t, aliceClient, repo[1]))
	require.Equal(t,
		buildBindings(alice, auth.RepoOwnerRole, pl(pipeline), auth.RepoReaderRole), getRepoRoleBinding(t, aliceClient, repo[2]))
	require.Equal(t,
		buildBindings(alice, auth.RepoOwnerRole, pl(pipeline), auth.RepoWriterRole), getRepoRoleBinding(t, aliceClient, pipeline))
	// repo[0] is not on pipeline -- doesn't include 'pipeline'
	require.Equal(t,
		buildBindings(alice, auth.RepoOwnerRole), getRepoRoleBinding(t, aliceClient, repo[0]))

	// make sure the pipeline still runs
	require.NoErrorWithinT(t, time.Minute, func() error {
		_, err := aliceClient.FlushCommitAll(
			[]*pfs.Commit{client.NewCommit(repo[2], "master")}, nil)
		return err
	})
}

func TestModifyMembers(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}
	tu.DeleteAll(t)
	defer tu.DeleteAll(t)

	alice := robot(tu.UniqueString("alice"))
	bob := robot(tu.UniqueString("bob"))
	organization := tu.UniqueString("organization")
	engineering := tu.UniqueString("engineering")
	security := tu.UniqueString("security")

	adminClient := tu.GetAuthenticatedPachClient(t, auth.RootUser)

	// This is a sequence dependent list of tests
	tests := []struct {
		Requests []*auth.ModifyMembersRequest
		Expected map[string][]string
	}{
		{
			[]*auth.ModifyMembersRequest{
				&auth.ModifyMembersRequest{
					Add:   []string{alice},
					Group: organization,
				},
				&auth.ModifyMembersRequest{
					Add:   []string{alice},
					Group: organization,
				},
			},
			map[string][]string{
				alice: []string{organization},
			},
		},
		{
			[]*auth.ModifyMembersRequest{
				&auth.ModifyMembersRequest{
					Add:   []string{bob},
					Group: organization,
				},
				&auth.ModifyMembersRequest{
					Add:   []string{alice, bob},
					Group: engineering,
				},
				&auth.ModifyMembersRequest{
					Add:   []string{bob},
					Group: security,
				},
			},
			map[string][]string{
				alice: []string{organization, engineering},
				bob:   []string{organization, engineering, security},
			},
		},
		{
			[]*auth.ModifyMembersRequest{
				&auth.ModifyMembersRequest{
					Add:    []string{alice},
					Remove: []string{bob},
					Group:  security,
				},
				&auth.ModifyMembersRequest{
					Remove: []string{bob},
					Group:  engineering,
				},
			},
			map[string][]string{
				alice: []string{organization, engineering, security},
				bob:   []string{organization},
			},
		},
		{
			[]*auth.ModifyMembersRequest{
				&auth.ModifyMembersRequest{
					Remove: []string{alice, bob},
					Group:  organization,
				},
				&auth.ModifyMembersRequest{
					Remove: []string{alice, bob},
					Group:  security,
				},
				&auth.ModifyMembersRequest{
					Add:    []string{alice},
					Remove: []string{alice},
					Group:  organization,
				},
				&auth.ModifyMembersRequest{
					Add:    []string{},
					Remove: []string{},
					Group:  organization,
				},
			},
			map[string][]string{
				alice: []string{engineering},
				bob:   []string{},
			},
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			for _, req := range test.Requests {
				_, err := adminClient.ModifyMembers(adminClient.Ctx(), req)
				require.NoError(t, err)
			}

			for username, groups := range test.Expected {
				groupsActual, err := adminClient.GetGroupsForPrincipal(adminClient.Ctx(), &auth.GetGroupsForPrincipalRequest{
					Principal: username,
				})
				require.NoError(t, err)
				require.ElementsEqual(t, groups, groupsActual.Groups)

				for _, group := range groups {
					users, err := adminClient.GetUsers(adminClient.Ctx(), &auth.GetUsersRequest{
						Group: group,
					})
					require.NoError(t, err)
					require.OneOfEquals(t, username, users.Usernames)
				}
			}
		})
	}
}

func TestSetGroupsForUser(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}
	tu.DeleteAll(t)
	defer tu.DeleteAll(t)

	alice := robot(tu.UniqueString("alice"))
	organization := tu.UniqueString("organization")
	engineering := tu.UniqueString("engineering")
	security := tu.UniqueString("security")

	adminClient := tu.GetAuthenticatedPachClient(t, auth.RootUser)

	groups := []string{organization, engineering}
	_, err := adminClient.SetGroupsForUser(adminClient.Ctx(), &auth.SetGroupsForUserRequest{
		Username: alice,
		Groups:   groups,
	})
	require.NoError(t, err)
	groupsActual, err := adminClient.GetGroupsForPrincipal(adminClient.Ctx(), &auth.GetGroupsForPrincipalRequest{
		Principal: alice,
	})
	require.NoError(t, err)
	require.ElementsEqual(t, groups, groupsActual.Groups)
	for _, group := range groups {
		users, err := adminClient.GetUsers(adminClient.Ctx(), &auth.GetUsersRequest{
			Group: group,
		})
		require.NoError(t, err)
		require.OneOfEquals(t, alice, users.Usernames)
	}

	groups = append(groups, security)
	_, err = adminClient.SetGroupsForUser(adminClient.Ctx(), &auth.SetGroupsForUserRequest{
		Username: alice,
		Groups:   groups,
	})
	require.NoError(t, err)
	groupsActual, err = adminClient.GetGroupsForPrincipal(adminClient.Ctx(), &auth.GetGroupsForPrincipalRequest{
		Principal: alice,
	})
	require.NoError(t, err)
	require.ElementsEqual(t, groups, groupsActual.Groups)
	for _, group := range groups {
		users, err := adminClient.GetUsers(adminClient.Ctx(), &auth.GetUsersRequest{
			Group: group,
		})
		require.NoError(t, err)
		require.OneOfEquals(t, alice, users.Usernames)
	}

	groups = groups[:1]
	_, err = adminClient.SetGroupsForUser(adminClient.Ctx(), &auth.SetGroupsForUserRequest{
		Username: alice,
		Groups:   groups,
	})
	require.NoError(t, err)
	groupsActual, err = adminClient.GetGroupsForPrincipal(adminClient.Ctx(), &auth.GetGroupsForPrincipalRequest{
		Principal: alice,
	})
	require.NoError(t, err)
	require.ElementsEqual(t, groups, groupsActual.Groups)
	for _, group := range groups {
		users, err := adminClient.GetUsers(adminClient.Ctx(), &auth.GetUsersRequest{
			Group: group,
		})
		require.NoError(t, err)
		require.OneOfEquals(t, alice, users.Usernames)
	}

	groups = []string{}
	_, err = adminClient.SetGroupsForUser(adminClient.Ctx(), &auth.SetGroupsForUserRequest{
		Username: alice,
		Groups:   groups,
	})
	require.NoError(t, err)
	groupsActual, err = adminClient.GetGroupsForPrincipal(adminClient.Ctx(), &auth.GetGroupsForPrincipalRequest{
		Principal: alice,
	})
	require.NoError(t, err)
	require.ElementsEqual(t, groups, groupsActual.Groups)
	for _, group := range groups {
		users, err := adminClient.GetUsers(adminClient.Ctx(), &auth.GetUsersRequest{
			Group: group,
		})
		require.NoError(t, err)
		require.OneOfEquals(t, alice, users.Usernames)
	}
}

func TestGetOwnGroups(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}
	tu.DeleteAll(t)
	defer tu.DeleteAll(t)

	alice := robot(tu.UniqueString("alice"))
	organization := tu.UniqueString("organization")
	engineering := tu.UniqueString("engineering")
	security := tu.UniqueString("security")

	adminClient := tu.GetAuthenticatedPachClient(t, auth.RootUser)

	_, err := adminClient.SetGroupsForUser(adminClient.Ctx(), &auth.SetGroupsForUserRequest{
		Username: alice,
		Groups:   []string{organization, engineering, security},
	})
	require.NoError(t, err)

	aliceClient := tu.GetAuthenticatedPachClient(t, alice)
	groups, err := aliceClient.GetGroups(aliceClient.Ctx(), &auth.GetGroupsRequest{})
	require.NoError(t, err)
	require.ElementsEqual(t, []string{organization, engineering, security}, groups.Groups)

	groups, err = adminClient.GetGroups(adminClient.Ctx(), &auth.GetGroupsRequest{})
	require.NoError(t, err)
	require.Equal(t, 0, len(groups.Groups))
}

// TestGetJobsBugFix tests the fix for https://github.com/pachyderm/pachyderm/v2/issues/2879
// where calling pps.ListJob when not logged in would delete all old jobs
func TestGetJobsBugFix(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}
	tu.DeleteAll(t)
	defer tu.DeleteAll(t)
	alice := robot(tu.UniqueString("alice"))
	aliceClient, anonClient := tu.GetAuthenticatedPachClient(t, alice), tu.GetUnauthenticatedPachClient(t)

	// alice creates a repo
	repo := tu.UniqueString(t.Name())
	require.NoError(t, aliceClient.CreateRepo(repo))
	require.Equal(t, buildBindings(alice, auth.RepoOwnerRole), getRepoRoleBinding(t, aliceClient, repo))
	err := aliceClient.PutFile(repo, "master", "/file", strings.NewReader("lorem ipsum"))
	require.NoError(t, err)

	// alice creates a pipeline
	pipeline := tu.UniqueString("alice-pipeline")
	require.NoError(t, aliceClient.CreatePipeline(
		pipeline,
		"", // default image: ubuntu:18.04
		[]string{"bash"},
		[]string{"cp /pfs/*/* /pfs/out/"},
		&pps.ParallelismSpec{Constant: 1},
		client.NewPFSInput(repo, "/*"),
		"", // default output branch: master
		false,
	))

	// Wait for pipeline to finish
	_, err = aliceClient.FlushCommitAll(
		[]*pfs.Commit{client.NewCommit(repo, "master")},
		[]*pfs.Repo{client.NewRepo(pipeline)},
	)
	require.NoError(t, err)

	// alice calls 'list job'
	jobs, err := aliceClient.ListJob("", nil, nil, -1 /*history*/, true)
	require.NoError(t, err)
	require.Equal(t, 1, len(jobs))

	// anonClient calls 'list job'
	_, err = anonClient.ListJob("", nil, nil, -1 /*history*/, true)
	require.YesError(t, err)
	require.Matches(t, "no authentication token", err.Error())

	// alice calls 'list job' again, and the existing job must still be present
	jobs2, err := aliceClient.ListJob("", nil, nil, -1 /*history*/, true)
	require.NoError(t, err)
	require.Equal(t, 1, len(jobs2))
	require.Equal(t, jobs[0].Job.ID, jobs2[0].Job.ID)
}

func TestS3GatewayAuthRequests(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	// generate auth credentials
	adminClient := tu.GetAuthenticatedPachClient(t, auth.RootUser)
	alice := tu.UniqueString("alice")
	authResp, err := adminClient.GetRobotToken(adminClient.Ctx(), &auth.GetRobotTokenRequest{
		Robot: alice,
	})
	require.NoError(t, err)
	authToken := authResp.Token

	ip := os.Getenv("VM_IP")
	if ip == "" {
		ip = "127.0.0.1"
	}
	address := net.JoinHostPort(ip, "30600")

	// anon login via V2 - should fail
	minioClientV2, err := minio.NewV2(address, "", "", false)
	require.NoError(t, err)
	_, err = minioClientV2.ListBuckets()
	require.YesError(t, err)

	// anon login via V4 - should fail
	minioClientV4, err := minio.NewV4(address, "", "", false)
	require.NoError(t, err)
	_, err = minioClientV4.ListBuckets()
	require.YesError(t, err)

	// proper login via V2 - should succeed
	minioClientV2, err = minio.NewV2(address, authToken, authToken, false)
	require.NoError(t, err)
	_, err = minioClientV2.ListBuckets()
	require.NoError(t, err)

	// proper login via V4 - should succeed
	minioClientV2, err = minio.NewV4(address, authToken, authToken, false)
	require.NoError(t, err)
	_, err = minioClientV2.ListBuckets()
	require.NoError(t, err)
}

// TestDeleteFailedPipeline creates a pipeline with an invalid image and then
// tries to delete it (which shouldn't be blocked by the auth system)
func TestDeleteFailedPipeline(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}
	tu.DeleteAll(t)
	defer tu.DeleteAll(t)
	alice := robot(tu.UniqueString("alice"))
	aliceClient := tu.GetAuthenticatedPachClient(t, alice)

	// Create input repo w/ initial commit
	repo := tu.UniqueString(t.Name())
	require.NoError(t, aliceClient.CreateRepo(repo))
	err := aliceClient.PutFile(repo, "master", "/file", strings.NewReader("1"))
	require.NoError(t, err)

	// Create pipeline
	pipeline := tu.UniqueString("pipeline")
	require.NoError(t, aliceClient.CreatePipeline(
		pipeline,
		"does-not-exist", // nonexistant image
		[]string{"true"}, nil,
		&pps.ParallelismSpec{Constant: 1},
		client.NewPFSInput(repo, "/*"),
		"", // default output branch: master
		false,
	))
	require.NoError(t, aliceClient.DeletePipeline(pipeline, true))

	// make sure FlushCommit eventually returns (i.e. pipeline failure doesn't
	// block flushCommit indefinitely)
	require.NoErrorWithinT(t, 30*time.Second, func() error {
		_, err := aliceClient.FlushCommitAll(
			[]*pfs.Commit{client.NewCommit(repo, "master")},
			[]*pfs.Repo{client.NewRepo(pipeline)})
		return err
	})
}

// TestDeletePipelineMissingRepos creates a pipeline, force-deletes its input
// and output repos, and then confirms that DeletePipeline still works (i.e.
// the missing repos/ACLs don't cause an auth error).
func TestDeletePipelineMissingRepos(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}
	tu.DeleteAll(t)
	defer tu.DeleteAll(t)
	alice := robot(tu.UniqueString("alice"))
	aliceClient := tu.GetAuthenticatedPachClient(t, alice)

	// Create input repo w/ initial commit
	repo := tu.UniqueString(t.Name())
	require.NoError(t, aliceClient.CreateRepo(repo))
	err := aliceClient.PutFile(repo, "master", "/file", strings.NewReader("1"))
	require.NoError(t, err)

	// Create pipeline
	pipeline := tu.UniqueString("pipeline")
	require.NoError(t, aliceClient.CreatePipeline(
		pipeline,
		"does-not-exist", // nonexistant image
		[]string{"true"}, nil,
		&pps.ParallelismSpec{Constant: 1},
		client.NewPFSInput(repo, "/*"),
		"", // default output branch: master
		false,
	))

	// force-delete input and output repos
	require.NoError(t, aliceClient.DeleteRepo(repo, true))
	require.NoError(t, aliceClient.DeleteRepo(pipeline, true))

	// Attempt to delete the pipeline--must succeed
	require.NoError(t, aliceClient.DeletePipeline(pipeline, true))
	require.NoErrorWithinTRetry(t, 30*time.Second, func() error {
		pis, err := aliceClient.ListPipeline()
		if err != nil {
			return err
		}
		for _, pi := range pis {
			if pi.Pipeline.Name == pipeline {
				return errors.Errorf("Expected %q to be deleted, but still present", pipeline)
			}
		}
		return nil
	})
}

// TestDeactivateFSAdmin tests that users with the FS admin role can't call Deactivate
func TestDeactivateFSAdmin(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}
	alice := robot(tu.UniqueString("alice"))
	aliceClient, adminClient := tu.GetAuthenticatedPachClient(t, alice), tu.GetAuthenticatedPachClient(t, auth.RootUser)

	// admin makes alice an fs admin
	require.NoError(t, adminClient.ModifyClusterRoleBinding(alice, []string{auth.RepoOwnerRole}))

	// wait until alice shows up in admin list
	resp, err := aliceClient.GetClusterRoleBinding()
	require.NoError(t, err)
	require.Equal(t, buildClusterBindings(alice, auth.RepoOwnerRole), resp)

	// alice tries to deactivate, but doesn't have permission as an FS admin
	_, err = aliceClient.Deactivate(aliceClient.Ctx(), &auth.DeactivateRequest{})
	require.YesError(t, err)
	require.Matches(t, "not authorized", err.Error())
}

// TestExtractAuthToken tests that admins can extract hashed robot auth tokens
func TestExtractAuthToken(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}
	tu.DeleteAll(t)
	defer tu.DeleteAll(t)

	alice := robot(tu.UniqueString("alice"))
	aliceClient, adminClient := tu.GetAuthenticatedPachClient(t, alice), tu.GetAuthenticatedPachClient(t, auth.RootUser)

	// alice can't extract auth tokens because she's not an admin
	_, err := aliceClient.ExtractAuthTokens(aliceClient.Ctx(), &auth.ExtractAuthTokensRequest{})
	require.YesError(t, err)
	require.Matches(t, "not authorized", err.Error())

	// Create a token with a TTL and confirm it is extracted with an expiration
	tokenResp, err := adminClient.GetRobotToken(adminClient.Ctx(), &auth.GetRobotTokenRequest{Robot: "other", TTL: 1000})
	require.NoError(t, err)

	// Create a token without a TTL and confirm it is extracted
	tokenRespTwo, err := adminClient.GetRobotToken(adminClient.Ctx(), &auth.GetRobotTokenRequest{Robot: "otherTwo"})
	require.NoError(t, err)

	// admins can extract auth tokens
	resp, err := adminClient.ExtractAuthTokens(adminClient.Ctx(), &auth.ExtractAuthTokensRequest{})
	require.NoError(t, err)

	// only robot tokens are extracted, so only the admin token (not the alice one) should be included
	containsToken := func(plaintext, subject string, expires bool) error {
		hash := auth.HashToken(plaintext)
		for _, token := range resp.Tokens {
			if token.HashedToken == hash {
				require.Equal(t, subject, token.Subject)
				if expires {
					require.True(t, token.Expiration.After(time.Now()))
				} else {
					require.Nil(t, token.Expiration)
				}
				return nil
			}
		}
		return fmt.Errorf("didn't find a token with hash %q", hash)
	}

	require.NoError(t, containsToken(tokenResp.Token, "robot:other", true))
	require.NoError(t, containsToken(tokenRespTwo.Token, "robot:otherTwo", false))
}

// TestRestoreAuthToken tests that admins can restore hashed auth tokens that have been extracted
func TestRestoreAuthToken(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}
	tu.DeleteAll(t)
	defer tu.DeleteAll(t)

	// Create a request to restore a token with known plaintext
	req := &auth.RestoreAuthTokenRequest{
		Token: &auth.TokenInfo{
			HashedToken: fmt.Sprintf("%x", sha256.Sum256([]byte("an-auth-token"))),
			Subject:     "robot:restored",
		},
	}

	alice := robot(tu.UniqueString("alice"))
	aliceClient, adminClient := tu.GetAuthenticatedPachClient(t, alice), tu.GetAuthenticatedPachClient(t, auth.RootUser)

	// alice can't restore auth tokens because she's not an admin
	_, err := aliceClient.RestoreAuthToken(aliceClient.Ctx(), req)
	require.YesError(t, err)
	require.Matches(t, "not authorized", err.Error())

	// admins can restore auth tokens
	_, err = adminClient.RestoreAuthToken(adminClient.Ctx(), req)
	require.NoError(t, err)

	req.Token.Subject = "robot:overwritten"
	_, err = adminClient.RestoreAuthToken(adminClient.Ctx(), req)
	require.YesError(t, err)
	require.Equal(t, "rpc error: code = Unknown desc = error restoring auth token: cannot overwrite existing token with same hash", err.Error())

	// now we can authenticate with the restored token
	aliceClient.SetAuthToken("an-auth-token")
	whoAmIResp, err := aliceClient.WhoAmI(aliceClient.Ctx(), &auth.WhoAmIRequest{})
	require.NoError(t, err)
	require.Equal(t, "robot:restored", whoAmIResp.Username)
	require.Nil(t, whoAmIResp.Expiration)

	// restore a token with an expiration date in the past
	req.Token.HashedToken = fmt.Sprintf("%x", sha256.Sum256([]byte("expired-token")))
	pastExpiration := time.Now().Add(-1 * time.Minute)
	req.Token.Expiration = &pastExpiration

	_, err = adminClient.RestoreAuthToken(adminClient.Ctx(), req)
	require.YesError(t, err)
	require.True(t, auth.IsErrExpiredToken(err))

	// restore a token with an expiration date in the future
	req.Token.HashedToken = fmt.Sprintf("%x", sha256.Sum256([]byte("expiring-token")))
	futureExpiration := time.Now().Add(10 * time.Minute)
	req.Token.Expiration = &futureExpiration

	_, err = adminClient.RestoreAuthToken(adminClient.Ctx(), req)
	require.NoError(t, err)

	aliceClient.SetAuthToken("expiring-token")
	whoAmIResp, err = aliceClient.WhoAmI(aliceClient.Ctx(), &auth.WhoAmIRequest{})
	require.NoError(t, err)

	// Relying on time.Now is gross but the token should have a TTL in the
	// next 10 minutes
	require.True(t, whoAmIResp.Expiration.After(time.Now()))
	require.True(t, whoAmIResp.Expiration.Before(time.Now().Add(time.Duration(600)*time.Second)))
}

// TODO: This test mirrors TestDebug in src/server/pachyderm_test.go.
// Need to restructure testing such that we have the implementation of this
// test in one place while still being able to test auth enabled and disabled clusters.
func TestDebug(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}
	tu.DeleteAll(t)
	defer tu.DeleteAll(t)

	// Get all the authenticated clients at the beginning of the test.
	// GetAuthenticatedPachClient will always re-activate auth, which
	// causes PPS to rotate all the pipeline tokens. This makes the RCs
	// change and recreates all the pods, which used to race with collecting logs.
	adminClient := tu.GetAuthenticatedPachClient(t, auth.RootUser)
	alice := robot(tu.UniqueString("alice"))
	aliceClient := tu.GetAuthenticatedPachClient(t, alice)

	dataRepo := tu.UniqueString("TestDebug_data")
	require.NoError(t, aliceClient.CreateRepo(dataRepo))

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
		require.NoError(t, aliceClient.CreatePipeline(
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

	commit1, err := aliceClient.StartCommit(dataRepo, "master")
	require.NoError(t, err)
	err = aliceClient.PutFile(dataRepo, commit1.ID, "file", strings.NewReader("foo"))
	require.NoError(t, err)
	require.NoError(t, aliceClient.FinishCommit(dataRepo, commit1.ID))

	commitInfos, err := aliceClient.FlushCommitAll([]*pfs.Commit{commit1}, nil)
	require.NoError(t, err)
	require.Equal(t, 6, len(commitInfos))

	// Only admins can collect a debug dump.
	buf := &bytes.Buffer{}
	require.YesError(t, aliceClient.Dump(nil, 0, buf))
	require.NoError(t, adminClient.Dump(nil, 0, buf))
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

func TestDeleteExpiredAuthTokens(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}
	tu.DeleteAll(t)
	defer tu.DeleteAll(t)

	// generate auth credentials
	adminClient := tu.GetAuthenticatedPachClient(t, auth.RootUser)

	// create a token that will instantly expire, a token that will expire later, and a token that will never expire
	noExpirationResp, noExpErr := adminClient.GetRobotToken(adminClient.Ctx(), &auth.GetRobotTokenRequest{Robot: "robot:alice"})
	require.NoError(t, noExpErr)

	fastExpirationResp, fastExpErr := adminClient.GetRobotToken(adminClient.Ctx(), &auth.GetRobotTokenRequest{Robot: "robot:alice", TTL: 1})
	require.NoError(t, fastExpErr)

	slowExpirationResp, slowExpErr := adminClient.GetRobotToken(adminClient.Ctx(), &auth.GetRobotTokenRequest{Robot: "robot:alice", TTL: 1000})
	require.NoError(t, slowExpErr)

	contains := func(tokens []*auth.TokenInfo, hashedToken string) bool {
		for _, v := range tokens {
			if v.HashedToken == hashedToken {
				return true
			}
		}
		return false
	}

	// query all tokens to show that the instantly expired one is expired
	extractTokensResp, firstExtractErr := adminClient.ExtractAuthTokens(adminClient.Ctx(), &auth.ExtractAuthTokensRequest{})
	require.NoError(t, firstExtractErr)

	preDeleteTokens := extractTokensResp.Tokens
	require.Equal(t, 3, len(preDeleteTokens), "all three tokens should be returned")
	require.True(t, contains(preDeleteTokens, auth.HashToken(noExpirationResp.Token)), "robot token without expiration should be extracted")
	require.True(t, contains(preDeleteTokens, auth.HashToken(fastExpirationResp.Token)), "robot token without expiration should be extracted")
	require.True(t, contains(preDeleteTokens, auth.HashToken(slowExpirationResp.Token)), "robot token without expiration should be extracted")

	// wait for the one token to expire
	time.Sleep(time.Duration(2) * time.Second)

	// record admin token
	adminToken := adminClient.AuthToken()

	// before deleting, check that WhoAmI call still fails for existing & expired token
	adminClient.SetAuthToken(fastExpirationResp.Token)
	_, whoAmIErr := adminClient.WhoAmI(adminClient.Ctx(), &auth.WhoAmIRequest{})
	require.True(t, auth.IsErrExpiredToken(whoAmIErr))

	// run DeleteExpiredAuthTokens RPC and verify that only the instantly expired token is inaccessible
	adminClient.SetAuthToken(adminToken)
	_, deleteErr := adminClient.DeleteExpiredAuthTokens(adminClient.Ctx(), &auth.DeleteExpiredAuthTokensRequest{})
	require.NoError(t, deleteErr)

	extractTokensAfterDeleteResp, sndExtractErr := adminClient.ExtractAuthTokens(adminClient.Ctx(), &auth.ExtractAuthTokensRequest{})
	require.NoError(t, sndExtractErr)

	postDeleteTokens := extractTokensAfterDeleteResp.Tokens

	require.Equal(t, 2, len(postDeleteTokens), "only the two unexpired tokens should be returned.")
	require.True(t, contains(postDeleteTokens, auth.HashToken(noExpirationResp.Token)), "robot token without expiration should be extracted")
	require.True(t, contains(postDeleteTokens, auth.HashToken(slowExpirationResp.Token)), "robot token without expiration should be extracted")
}

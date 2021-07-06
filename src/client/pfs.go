package client

import (
	"context"
	"io"

	"github.com/pachyderm/pachyderm/v2/src/internal/clientsdk"
	"github.com/pachyderm/pachyderm/v2/src/internal/errors"
	"github.com/pachyderm/pachyderm/v2/src/internal/errutil"
	"github.com/pachyderm/pachyderm/v2/src/internal/grpcutil"
	"github.com/pachyderm/pachyderm/v2/src/pfs"
)

// NewCommitSet creates a pfs.CommitSet
func NewCommitSet(id string) *pfs.CommitSet {
	return &pfs.CommitSet{ID: id}
}

// NewRepo creates a pfs.Repo.
func NewRepo(repoName string) *pfs.Repo {
	return &pfs.Repo{Name: repoName, Type: pfs.UserRepoType}
}

// NewSystemRepo creates a pfs.Repo of the given type
func NewSystemRepo(repoName string, repoType string) *pfs.Repo {
	return &pfs.Repo{Name: repoName, Type: repoType}
}

// NewBranch creates a pfs.Branch
func NewBranch(repoName string, branchName string) *pfs.Branch {
	return &pfs.Branch{
		Repo: NewRepo(repoName),
		Name: branchName,
	}
}

// NewCommit creates a pfs.Commit.
func NewCommit(repoName string, branchName string, commitID string) *pfs.Commit {
	return &pfs.Commit{
		Branch: NewBranch(repoName, branchName),
		ID:     commitID,
	}
}

// NewFile creates a pfs.File.
func NewFile(repoName string, branchName string, commitID string, path string) *pfs.File {
	return &pfs.File{
		Commit: NewCommit(repoName, branchName, commitID),
		Path:   path,
	}
}

// CreateRepo creates a new Repo object in pfs with the given name. Repos are
// the top level data object in pfs and should be used to store data of a
// similar type. For example rather than having a single Repo for an entire
// project you might have separate Repos for logs, metrics, database dumps etc.
func (c APIClient) CreateRepo(repoName string) error {
	_, err := c.PfsAPIClient.CreateRepo(
		c.Ctx(),
		&pfs.CreateRepoRequest{
			Repo: NewRepo(repoName),
		},
	)
	return grpcutil.ScrubGRPC(err)
}

// UpdateRepo upserts a repo with the given name.
func (c APIClient) UpdateRepo(repoName string) error {
	_, err := c.PfsAPIClient.CreateRepo(
		c.Ctx(),
		&pfs.CreateRepoRequest{
			Repo:   NewRepo(repoName),
			Update: true,
		},
	)
	return grpcutil.ScrubGRPC(err)
}

// InspectRepo returns info about a specific Repo.
func (c APIClient) InspectRepo(repoName string) (_ *pfs.RepoInfo, retErr error) {
	defer func() {
		retErr = grpcutil.ScrubGRPC(retErr)
	}()
	return c.PfsAPIClient.InspectRepo(
		c.Ctx(),
		&pfs.InspectRepoRequest{
			Repo: NewRepo(repoName),
		},
	)
}

// ListRepo returns info about user Repos
func (c APIClient) ListRepo() ([]*pfs.RepoInfo, error) {
	return c.ListRepoByType(pfs.UserRepoType)
}

// ListRepoByType returns info about Repos of the given type
// The if repoType is empty, all Repos will be included
func (c APIClient) ListRepoByType(repoType string) (_ []*pfs.RepoInfo, retErr error) {
	ctx, cf := context.WithCancel(c.Ctx())
	defer cf()
	request := &pfs.ListRepoRequest{Type: repoType}
	client, err := c.PfsAPIClient.ListRepo(
		ctx,
		request,
	)
	if err != nil {
		return nil, grpcutil.ScrubGRPC(err)
	}
	return clientsdk.ListRepoInfo(client)
}

// DeleteRepo deletes a repo and reclaims the storage space it was using. Note
// that as of 1.0 we do not reclaim the blocks that the Repo was referencing,
// this is because they may also be referenced by other Repos and deleting them
// would make those Repos inaccessible. This will be resolved in later
// versions.
// If "force" is set to true, the repo will be removed regardless of errors.
// This argument should be used with care.
func (c APIClient) DeleteRepo(repoName string, force bool) error {
	request := &pfs.DeleteRepoRequest{
		Repo:  NewRepo(repoName),
		Force: force,
	}
	_, err := c.PfsAPIClient.DeleteRepo(
		c.Ctx(),
		request,
	)
	return grpcutil.ScrubGRPC(err)
}

// StartCommit begins the process of committing data to a Repo. Once started
// you can write to the Commit with PutFile and when all the data has been
// written you must finish the Commit with FinishCommit. NOTE, data is not
// persisted until FinishCommit is called.
// branch is a more convenient way to build linear chains of commits. When a
// commit is started with a non empty branch the value of branch becomes an
// alias for the created Commit. This enables a more intuitive access pattern.
// When the commit is started on a branch the previous head of the branch is
// used as the parent of the commit.
func (c APIClient) StartCommit(repoName string, branchName string) (_ *pfs.Commit, retErr error) {
	defer func() {
		retErr = grpcutil.ScrubGRPC(retErr)
	}()
	return c.PfsAPIClient.StartCommit(
		c.Ctx(),
		&pfs.StartCommitRequest{
			Branch: NewBranch(repoName, branchName),
		},
	)
}

// StartCommitParent begins the process of committing data to a Repo. Once started
// you can write to the Commit with PutFile and when all the data has been
// written you must finish the Commit with FinishCommit. NOTE, data is not
// persisted until FinishCommit is called.
// branch is a more convenient way to build linear chains of commits. When a
// commit is started with a non empty branch the value of branch becomes an
// alias for the created Commit. This enables a more intuitive access pattern.
// When the commit is started on a branch the previous head of the branch is
// used as the parent of the commit.
// parentCommit specifies the parent Commit, upon creation the new Commit will
// appear identical to the parent Commit, data can safely be added to the new
// commit without affecting the contents of the parent Commit. You may pass ""
// as parentCommit in which case the new Commit will have no parent and will
// initially appear empty.
func (c APIClient) StartCommitParent(repoName string, branchName string, parentBranch string, parentCommit string) (*pfs.Commit, error) {
	commit, err := c.PfsAPIClient.StartCommit(
		c.Ctx(),
		&pfs.StartCommitRequest{
			Parent: NewCommit(repoName, parentBranch, parentCommit),
			Branch: NewBranch(repoName, branchName),
		},
	)
	if err != nil {
		return nil, grpcutil.ScrubGRPC(err)
	}
	return commit, nil
}

// FinishCommit ends the process of committing data to a Repo and persists the
// Commit. Once a Commit is finished the data becomes immutable and future
// attempts to write to it with PutFile will error.
func (c APIClient) FinishCommit(repoName string, branchName string, commitID string) error {
	_, err := c.PfsAPIClient.FinishCommit(
		c.Ctx(),
		&pfs.FinishCommitRequest{
			Commit: NewCommit(repoName, branchName, commitID),
		},
	)
	return grpcutil.ScrubGRPC(err)
}

// InspectCommit returns info about a specific Commit.
func (c APIClient) InspectCommit(repoName string, branchName string, commitID string) (_ *pfs.CommitInfo, retErr error) {
	defer func() { retErr = grpcutil.ScrubGRPC(retErr) }()
	return c.inspectCommit(repoName, branchName, commitID, pfs.CommitState_STARTED)
}

// WaitCommit returns info about a specific Commit, but blocks until that
// commit has been finished.
func (c APIClient) WaitCommit(repoName string, branchName string, commitID string) (_ *pfs.CommitInfo, retErr error) {
	defer func() { retErr = grpcutil.ScrubGRPC(retErr) }()
	return c.inspectCommit(repoName, branchName, commitID, pfs.CommitState_FINISHED)
}

func (c APIClient) inspectCommit(repoName string, branchName string, commitID string, wait pfs.CommitState) (*pfs.CommitInfo, error) {
	commitInfo, err := c.PfsAPIClient.InspectCommit(
		c.Ctx(),
		&pfs.InspectCommitRequest{
			Commit: NewCommit(repoName, branchName, commitID),
			Wait:   wait,
		},
	)
	if err != nil {
		return nil, err
	}
	return commitInfo, nil
}

// ListCommit lists commits.
// If only `repo` is given, all commits in the repo are returned.
// If `to` is given, only the ancestors of `to`, including `to` itself,
// are considered.
// If `from` is given, only the descendents of `from`, including `from`
// itself, are considered.
// If `to` and `from` are the same commit, no commits will be returned.
// `number` determines how many commits are returned.  If `number` is 0,
// all commits that match the aforementioned criteria are returned.
func (c APIClient) ListCommit(repo *pfs.Repo, to, from *pfs.Commit, number uint64) ([]*pfs.CommitInfo, error) {
	var result []*pfs.CommitInfo
	if err := c.ListCommitF(repo, to, from, number, false, func(ci *pfs.CommitInfo) error {
		result = append(result, ci)
		return nil
	}); err != nil {
		return nil, err
	}
	return result, nil
}

// ListCommitF lists commits, calling f with each commit.
// If only `repo` is given, all commits in the repo are returned.
// If `to` is given, only the ancestors of `to`, including `to` itself,
// are considered.
// If `from` is given, only the descendents of `from`, including `from`
// itself, are considered.
// If `to` and `from` are the same commit, no commits will be returned.
// `number` determines how many commits are returned.  If `number` is 0,
// `reverse` lists the commits from oldest to newest, rather than newest to oldest
// all commits that match the aforementioned criteria are passed to f.
func (c APIClient) ListCommitF(repo *pfs.Repo, to, from *pfs.Commit, number uint64, reverse bool, f func(*pfs.CommitInfo) error) error {
	req := &pfs.ListCommitRequest{
		Repo:    repo,
		Number:  number,
		Reverse: reverse,
		To:      to,
		From:    from,
	}
	ctx, cf := context.WithCancel(c.Ctx())
	defer cf()
	stream, err := c.PfsAPIClient.ListCommit(ctx, req)
	if err != nil {
		return grpcutil.ScrubGRPC(err)
	}
	for {
		ci, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return grpcutil.ScrubGRPC(err)
		}
		if err := f(ci); err != nil {
			if errors.Is(err, errutil.ErrBreak) {
				return nil
			}
			return err
		}
	}
	return nil
}

// ListCommitByRepo lists all commits in a repo.
func (c APIClient) ListCommitByRepo(repo *pfs.Repo) ([]*pfs.CommitInfo, error) {
	return c.ListCommit(repo, nil, nil, 0)
}

// CreateBranch creates a new branch
func (c APIClient) CreateBranch(repoName string, branchName string, commitBranch string, commitID string, provenance []*pfs.Branch) error {
	var head *pfs.Commit
	if commitBranch != "" || commitID != "" {
		head = NewCommit(repoName, commitBranch, commitID)
	}
	_, err := c.PfsAPIClient.CreateBranch(
		c.Ctx(),
		&pfs.CreateBranchRequest{
			Branch:     NewBranch(repoName, branchName),
			Head:       head,
			Provenance: provenance,
		},
	)
	return grpcutil.ScrubGRPC(err)
}

// CreateBranchTrigger Creates a branch with a trigger. Note: triggers and
// provenance are mutually exclusive. See the docs on triggers to learn more
// about why this is.
func (c APIClient) CreateBranchTrigger(repoName string, branchName string, commitBranch string, commitID string, trigger *pfs.Trigger) error {
	var head *pfs.Commit
	if commitBranch != "" || commitID != "" {
		head = NewCommit(repoName, commitBranch, commitID)
	}
	_, err := c.PfsAPIClient.CreateBranch(
		c.Ctx(),
		&pfs.CreateBranchRequest{
			Branch:  NewBranch(repoName, branchName),
			Head:    head,
			Trigger: trigger,
		},
	)
	return grpcutil.ScrubGRPC(err)
}

// InspectBranch returns information on a specific PFS branch
func (c APIClient) InspectBranch(repoName string, branchName string) (*pfs.BranchInfo, error) {
	branchInfo, err := c.PfsAPIClient.InspectBranch(
		c.Ctx(),
		&pfs.InspectBranchRequest{
			Branch: NewBranch(repoName, branchName),
		},
	)
	return branchInfo, grpcutil.ScrubGRPC(err)
}

// ListBranch lists the active branches on a Repo.
func (c APIClient) ListBranch(repoName string) ([]*pfs.BranchInfo, error) {
	ctx, cf := context.WithCancel(c.Ctx())
	defer cf()
	client, err := c.PfsAPIClient.ListBranch(
		ctx,
		&pfs.ListBranchRequest{
			Repo: NewRepo(repoName),
		},
	)
	if err != nil {
		return nil, grpcutil.ScrubGRPC(err)
	}
	return clientsdk.ListBranchInfo(client)
}

// DeleteBranch deletes a branch, but leaves the commits themselves intact.
// In other words, those commits can still be accessed via commit IDs and
// other branches they happen to be on.
func (c APIClient) DeleteBranch(repoName string, branchName string, force bool) error {
	_, err := c.PfsAPIClient.DeleteBranch(
		c.Ctx(),
		&pfs.DeleteBranchRequest{
			Branch: NewBranch(repoName, branchName),
			Force:  force,
		},
	)
	return grpcutil.ScrubGRPC(err)
}

func (c APIClient) inspectCommitSet(id string, wait bool, cb func(*pfs.CommitInfo) error) error {
	req := &pfs.InspectCommitSetRequest{
		CommitSet: NewCommitSet(id),
		Wait:      wait,
	}
	ctx, cf := context.WithCancel(c.Ctx())
	defer cf()
	client, err := c.PfsAPIClient.InspectCommitSet(ctx, req)
	if err != nil {
		return err
	}
	for {
		ci, err := client.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if err := cb(ci); err != nil {
			if errors.Is(err, errutil.ErrBreak) {
				return nil
			}
			return err
		}
	}
}

// InspectCommitSet returns info about a specific CommitSet.
func (c APIClient) InspectCommitSet(id string) (_ []*pfs.CommitInfo, retErr error) {
	defer func() { retErr = grpcutil.ScrubGRPC(retErr) }()
	result := []*pfs.CommitInfo{}
	if err := c.inspectCommitSet(id, false, func(ci *pfs.CommitInfo) error {
		result = append(result, ci)
		return nil
	}); err != nil {
		return nil, err
	}
	return result, nil
}

// WaitCommitSetAll blocks until all of a CommitSet's commits are finished.  To
// wait for an individual commit, use WaitCommit instead.
func (c APIClient) WaitCommitSetAll(id string) (_ []*pfs.CommitInfo, retErr error) {
	defer func() { retErr = grpcutil.ScrubGRPC(retErr) }()
	result := []*pfs.CommitInfo{}
	if err := c.WaitCommitSet(id, func(ci *pfs.CommitInfo) error {
		result = append(result, ci)
		return nil
	}); err != nil {
		return nil, err
	}
	return result, nil
}

// WaitCommitSet blocks until each of a CommitSet's commits are finished,
// passing them to the given callback as they finish.  To wait for an individual
// commit, use WaitCommit instead.
func (c APIClient) WaitCommitSet(id string, cb func(*pfs.CommitInfo) error) (retErr error) {
	defer func() { retErr = grpcutil.ScrubGRPC(retErr) }()
	if err := c.inspectCommitSet(id, true, cb); err != nil {
		return err
	}
	return nil
}

// SquashCommitSet squashes the commits of a CommitSet into their children.
func (c APIClient) SquashCommitSet(id string) error {
	_, err := c.PfsAPIClient.SquashCommitSet(
		c.Ctx(),
		&pfs.SquashCommitSetRequest{
			CommitSet: NewCommitSet(id),
		},
	)
	return grpcutil.ScrubGRPC(err)
}

// SubscribeCommit is like ListCommit but it keeps listening for commits as
// they come in.
func (c APIClient) SubscribeCommit(repo *pfs.Repo, branchName string, from string, state pfs.CommitState, cb func(*pfs.CommitInfo) error) (retErr error) {
	defer func() {
		retErr = grpcutil.ScrubGRPC(retErr)
	}()
	req := &pfs.SubscribeCommitRequest{
		Repo:   repo,
		Branch: branchName,
		State:  state,
	}
	if from != "" {
		req.From = repo.NewCommit(branchName, from)
	}
	client, err := c.PfsAPIClient.SubscribeCommit(c.Ctx(), req)
	if err != nil {
		return err
	}
	for {
		ci, err := client.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if err := cb(ci); err != nil {
			if errors.Is(err, errutil.ErrBreak) {
				return nil
			}
			return err
		}
	}
}

// ClearCommit clears the state of an open commit.
func (c APIClient) ClearCommit(repoName string, branchName string, commitID string) (retErr error) {
	defer func() {
		retErr = grpcutil.ScrubGRPC(retErr)
	}()
	_, err := c.PfsAPIClient.ClearCommit(
		c.Ctx(),
		&pfs.ClearCommitRequest{
			Commit: NewCommit(repoName, branchName, commitID),
		},
	)
	return err
}

// Fsck performs checks on pfs. Errors that are encountered will be passed
// onError. These aren't errors in the traditional sense, in that they don't
// prevent the completion of fsck. Errors that do prevent completion will be
// returned from the function.
func (c APIClient) Fsck(fix bool, cb func(*pfs.FsckResponse) error) error {
	fsckClient, err := c.PfsAPIClient.Fsck(c.Ctx(), &pfs.FsckRequest{Fix: fix})
	if err != nil {
		return grpcutil.ScrubGRPC(err)
	}
	for {
		resp, err := fsckClient.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return grpcutil.ScrubGRPC(err)
		}
		if err := cb(resp); err != nil {
			if errors.Is(err, errutil.ErrBreak) {
				break
			}
			return err
		}
	}
	return nil
}

// FsckFastExit performs checks on pfs, similar to Fsck, except that it returns the
// first fsck error it encounters and exits.
func (c APIClient) FsckFastExit() error {
	ctx, cancel := context.WithCancel(c.Ctx())
	defer cancel()
	fsckClient, err := c.PfsAPIClient.Fsck(ctx, &pfs.FsckRequest{})
	if err != nil {
		return grpcutil.ScrubGRPC(err)
	}
	for {
		resp, err := fsckClient.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return grpcutil.ScrubGRPC(err)
		}
		if resp.Error != "" {
			return errors.Errorf(resp.Error)
		}
	}
}

// RunPFSLoadTest runs a PFS load test.
func (c APIClient) RunPFSLoadTest(spec []byte, seed ...int64) (_ *pfs.RunLoadTestResponse, retErr error) {
	defer func() {
		retErr = grpcutil.ScrubGRPC(retErr)
	}()
	req := &pfs.RunLoadTestRequest{
		Spec: spec,
	}
	if len(seed) > 0 {
		req.Seed = seed[0]
	}
	return c.PfsAPIClient.RunLoadTest(c.Ctx(), req)
}

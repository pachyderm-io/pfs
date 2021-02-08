package client

import (
	"bytes"
	"context"
	"io"

	"github.com/gogo/protobuf/types"
	"github.com/pachyderm/pachyderm/v2/src/internal/errors"
	"github.com/pachyderm/pachyderm/v2/src/internal/errutil"
	"github.com/pachyderm/pachyderm/v2/src/internal/grpcutil"
	"github.com/pachyderm/pachyderm/v2/src/internal/tarutil"
	"github.com/pachyderm/pachyderm/v2/src/pfs"
)

// NewRepo creates a pfs.Repo.
func NewRepo(repoName string) *pfs.Repo {
	return &pfs.Repo{Name: repoName}
}

// NewBranch creates a pfs.Branch
func NewBranch(repoName string, branchName string) *pfs.Branch {
	return &pfs.Branch{
		Repo: NewRepo(repoName),
		Name: branchName,
	}
}

// NewCommit creates a pfs.Commit.
func NewCommit(repoName string, commitID string) *pfs.Commit {
	return &pfs.Commit{
		Repo: NewRepo(repoName),
		ID:   commitID,
	}
}

// NewCommitProvenance creates a pfs.CommitProvenance.
func NewCommitProvenance(repoName string, branchName string, commitID string) *pfs.CommitProvenance {
	return &pfs.CommitProvenance{
		Commit: NewCommit(repoName, commitID),
		Branch: NewBranch(repoName, branchName),
	}
}

// NewFile creates a pfs.File.
func NewFile(repoName string, commitID string, path string) *pfs.File {
	return &pfs.File{
		Commit: NewCommit(repoName, commitID),
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
func (c APIClient) InspectRepo(repoName string) (*pfs.RepoInfo, error) {
	resp, err := c.PfsAPIClient.InspectRepo(
		c.Ctx(),
		&pfs.InspectRepoRequest{
			Repo: NewRepo(repoName),
		},
	)
	if err != nil {
		return nil, grpcutil.ScrubGRPC(err)
	}
	return resp, nil
}

// ListRepo returns info about all Repos.
// provenance specifies a set of provenance repos, only repos which have ALL of
// the specified repos as provenance will be returned unless provenance is nil
// in which case it is ignored.
func (c APIClient) ListRepo() ([]*pfs.RepoInfo, error) {
	request := &pfs.ListRepoRequest{}
	repoInfos, err := c.PfsAPIClient.ListRepo(
		c.Ctx(),
		request,
	)
	if err != nil {
		return nil, grpcutil.ScrubGRPC(err)
	}
	return repoInfos.RepoInfo, nil
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
func (c APIClient) StartCommit(repoName string, branch string) (*pfs.Commit, error) {
	commit, err := c.PfsAPIClient.StartCommit(
		c.Ctx(),
		&pfs.StartCommitRequest{
			Parent: &pfs.Commit{
				Repo: &pfs.Repo{
					Name: repoName,
				},
			},
			Branch: branch,
		},
	)
	if err != nil {
		return nil, grpcutil.ScrubGRPC(err)
	}
	return commit, nil
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
func (c APIClient) StartCommitParent(repoName string, branch string, parentCommit string) (*pfs.Commit, error) {
	commit, err := c.PfsAPIClient.StartCommit(
		c.Ctx(),
		&pfs.StartCommitRequest{
			Parent: &pfs.Commit{
				Repo: &pfs.Repo{
					Name: repoName,
				},
				ID: parentCommit,
			},
			Branch: branch,
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
func (c APIClient) FinishCommit(repoName string, commitID string) error {
	_, err := c.PfsAPIClient.FinishCommit(
		c.Ctx(),
		&pfs.FinishCommitRequest{
			Commit: NewCommit(repoName, commitID),
		},
	)
	return grpcutil.ScrubGRPC(err)
}

// InspectCommit returns info about a specific Commit.
func (c APIClient) InspectCommit(repoName string, commitID string) (*pfs.CommitInfo, error) {
	return c.inspectCommit(repoName, commitID, pfs.CommitState_STARTED)
}

// BlockCommit returns info about a specific Commit, but blocks until that
// commit has been finished.
func (c APIClient) BlockCommit(repoName string, commitID string) (*pfs.CommitInfo, error) {
	return c.inspectCommit(repoName, commitID, pfs.CommitState_FINISHED)
}

func (c APIClient) inspectCommit(repoName string, commitID string, blockState pfs.CommitState) (*pfs.CommitInfo, error) {
	commitInfo, err := c.PfsAPIClient.InspectCommit(
		c.Ctx(),
		&pfs.InspectCommitRequest{
			Commit:     NewCommit(repoName, commitID),
			BlockState: blockState,
		},
	)
	if err != nil {
		return nil, grpcutil.ScrubGRPC(err)
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
func (c APIClient) ListCommit(repoName string, to string, from string, number uint64) ([]*pfs.CommitInfo, error) {
	var result []*pfs.CommitInfo
	if err := c.ListCommitF(repoName, to, from, number, false, func(ci *pfs.CommitInfo) error {
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
func (c APIClient) ListCommitF(repoName string, to string, from string, number uint64, reverse bool, f func(*pfs.CommitInfo) error) error {
	req := &pfs.ListCommitRequest{
		// repoName may be "", but the repo object must exist
		Repo:    NewRepo(repoName),
		Number:  number,
		Reverse: reverse,
	}
	if from != "" {
		req.From = NewCommit(repoName, from)
	}
	if to != "" {
		req.To = NewCommit(repoName, to)
	}
	stream, err := c.PfsAPIClient.ListCommit(c.Ctx(), req)
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
func (c APIClient) ListCommitByRepo(repoName string) ([]*pfs.CommitInfo, error) {
	return c.ListCommit(repoName, "", "", 0)
}

// CreateBranch creates a new branch
func (c APIClient) CreateBranch(repoName string, branch string, commit string, provenance []*pfs.Branch) error {
	var head *pfs.Commit
	if commit != "" {
		head = NewCommit(repoName, commit)
	}
	_, err := c.PfsAPIClient.CreateBranch(
		c.Ctx(),
		&pfs.CreateBranchRequest{
			Branch:     NewBranch(repoName, branch),
			Head:       head,
			Provenance: provenance,
		},
	)
	return grpcutil.ScrubGRPC(err)
}

// CreateBranchTrigger Creates a branch with a trigger. Note: triggers and
// provenance are mutually exclusive. See the docs on triggers to learn more
// about why this is.
func (c APIClient) CreateBranchTrigger(repoName string, branch string, commit string, trigger *pfs.Trigger) error {
	var head *pfs.Commit
	if commit != "" {
		head = NewCommit(repoName, commit)
	}
	_, err := c.PfsAPIClient.CreateBranch(
		c.Ctx(),
		&pfs.CreateBranchRequest{
			Branch:  NewBranch(repoName, branch),
			Head:    head,
			Trigger: trigger,
		},
	)
	return grpcutil.ScrubGRPC(err)
}

// InspectBranch returns information on a specific PFS branch
func (c APIClient) InspectBranch(repoName string, branch string) (*pfs.BranchInfo, error) {
	branchInfo, err := c.PfsAPIClient.InspectBranch(
		c.Ctx(),
		&pfs.InspectBranchRequest{
			Branch: NewBranch(repoName, branch),
		},
	)
	return branchInfo, grpcutil.ScrubGRPC(err)
}

// ListBranch lists the active branches on a Repo.
func (c APIClient) ListBranch(repoName string) ([]*pfs.BranchInfo, error) {
	branchInfos, err := c.PfsAPIClient.ListBranch(
		c.Ctx(),
		&pfs.ListBranchRequest{
			Repo: NewRepo(repoName),
		},
	)
	if err != nil {
		return nil, grpcutil.ScrubGRPC(err)
	}
	return branchInfos.BranchInfo, nil
}

// SetBranch sets a commit and its ancestors as a branch.
// SetBranch is deprecated in favor of CreateBranch.
func (c APIClient) SetBranch(repoName string, commit string, branch string) error {
	return c.CreateBranch(repoName, branch, commit, nil)
}

// DeleteBranch deletes a branch, but leaves the commits themselves intact.
// In other words, those commits can still be accessed via commit IDs and
// other branches they happen to be on.
func (c APIClient) DeleteBranch(repoName string, branch string, force bool) error {
	_, err := c.PfsAPIClient.DeleteBranch(
		c.Ctx(),
		&pfs.DeleteBranchRequest{
			Branch: NewBranch(repoName, branch),
			Force:  force,
		},
	)
	return grpcutil.ScrubGRPC(err)
}

// DeleteCommit deletes a commit.
func (c APIClient) DeleteCommit(repoName string, commitID string) error {
	_, err := c.PfsAPIClient.DeleteCommit(
		c.Ctx(),
		&pfs.DeleteCommitRequest{
			Commit: NewCommit(repoName, commitID),
		},
	)
	return grpcutil.ScrubGRPC(err)
}

// FlushCommit returns an iterator that returns commits that have the
// specified `commits` as provenance.  Note that the iterator can block if
// jobs have not successfully completed. This in effect waits for all of the
// jobs that are triggered by a set of commits to complete.
//
// If toRepos is not nil then only the commits up to and including those
// repos will be considered, otherwise all repos are considered.
//
// Note that it's never necessary to call FlushCommit to run jobs, they'll
// run no matter what, FlushCommit just allows you to wait for them to
// complete and see their output once they do.
func (c APIClient) FlushCommit(commits []*pfs.Commit, toRepos []*pfs.Repo) (CommitInfoIterator, error) {
	ctx, cancel := context.WithCancel(c.Ctx())
	stream, err := c.PfsAPIClient.FlushCommit(
		ctx,
		&pfs.FlushCommitRequest{
			Commits: commits,
			ToRepos: toRepos,
		},
	)
	if err != nil {
		cancel()
		return nil, grpcutil.ScrubGRPC(err)
	}
	return &commitInfoIterator{stream, cancel}, nil
}

// FlushCommitF calls f with commits that have the specified `commits` as
// provenance. Note that it can block if jobs have not successfully
// completed. This in effect waits for all of the jobs that are triggered by a
// set of commits to complete.
//
// If toRepos is not nil then only the commits up to and including those repos
// will be considered, otherwise all repos are considered.
//
// Note that it's never necessary to call FlushCommit to run jobs, they'll run
// no matter what, FlushCommitF just allows you to wait for them to complete and
// see their output once they do.
func (c APIClient) FlushCommitF(commits []*pfs.Commit, toRepos []*pfs.Repo, f func(*pfs.CommitInfo) error) error {
	stream, err := c.PfsAPIClient.FlushCommit(
		c.Ctx(),
		&pfs.FlushCommitRequest{
			Commits: commits,
			ToRepos: toRepos,
		},
	)
	if err != nil {
		return grpcutil.ScrubGRPC(err)
	}
	for {
		ci, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return grpcutil.ScrubGRPC(err)
		}
		if err := f(ci); err != nil {
			return err
		}
	}
}

// FlushCommitAll returns commits that have the specified `commits` as
// provenance. Note that it can block if jobs have not successfully
// completed. This in effect waits for all of the jobs that are triggered by a
// set of commits to complete.
//
// If toRepos is not nil then only the commits up to and including those repos
// will be considered, otherwise all repos are considered.
//
// Note that it's never necessary to call FlushCommit to run jobs, they'll run
// no matter what, FlushCommitAll just allows you to wait for them to complete and
// see their output once they do.
func (c APIClient) FlushCommitAll(commits []*pfs.Commit, toRepos []*pfs.Repo) ([]*pfs.CommitInfo, error) {
	var result []*pfs.CommitInfo
	if err := c.FlushCommitF(commits, toRepos, func(ci *pfs.CommitInfo) error {
		result = append(result, ci)
		return nil
	}); err != nil {
		return nil, err
	}
	return result, nil
}

// CommitInfoIterator wraps a stream of commits and makes them easy to iterate.
type CommitInfoIterator interface {
	Next() (*pfs.CommitInfo, error)
	Close()
}

type commitInfoIterator struct {
	stream pfs.API_SubscribeCommitClient
	cancel context.CancelFunc
}

func (c *commitInfoIterator) Next() (*pfs.CommitInfo, error) {
	return c.stream.Recv()
}

func (c *commitInfoIterator) Close() {
	c.cancel()
	// according to this thread it's necessary to drain a server-side stream before closing
	// https://github.com/grpc/grpc-go/issues/188
	for {
		if _, err := c.stream.Recv(); err != nil {
			break
		}
	}
}

// SubscribeCommit is like ListCommit but it keeps listening for commits as
// they come in.
func (c APIClient) SubscribeCommit(repo, branch string, prov *pfs.CommitProvenance, from string, state pfs.CommitState) (CommitInfoIterator, error) {
	ctx, cancel := context.WithCancel(c.Ctx())
	req := &pfs.SubscribeCommitRequest{
		Repo:   NewRepo(repo),
		Branch: branch,
		Prov:   prov,
		State:  state,
	}
	if from != "" {
		req.From = NewCommit(repo, from)
	}
	stream, err := c.PfsAPIClient.SubscribeCommit(ctx, req)
	if err != nil {
		cancel()
		return nil, grpcutil.ScrubGRPC(err)
	}
	return &commitInfoIterator{stream, cancel}, nil
}

// SubscribeCommitF is like ListCommit but it calls a callback function with
// the results rather than returning an iterator.
func (c APIClient) SubscribeCommitF(repo, branch string, prov *pfs.CommitProvenance, from string, state pfs.CommitState, f func(*pfs.CommitInfo) error) error {
	req := &pfs.SubscribeCommitRequest{
		Repo:   NewRepo(repo),
		Branch: branch,
		Prov:   prov,
		State:  state,
	}
	if from != "" {
		req.From = NewCommit(repo, from)
	}
	stream, err := c.PfsAPIClient.SubscribeCommit(c.Ctx(), req)
	if err != nil {
		return grpcutil.ScrubGRPC(err)
	}
	for {
		ci, err := stream.Recv()
		if err != nil {
			return grpcutil.ScrubGRPC(err)
		}
		if err := f(ci); err != nil {
			return grpcutil.ScrubGRPC(err)
		}
	}
}

// ClearCommit clears the state of an open commit.
func (c APIClient) ClearCommit(repo, commit string) (retErr error) {
	defer func() {
		retErr = grpcutil.ScrubGRPC(retErr)
	}()
	_, err := c.PfsAPIClient.ClearCommit(
		c.Ctx(),
		&pfs.ClearCommitRequest{
			Commit: NewCommit(repo, commit),
		},
	)
	return err
}

// PutFileClient manages put file operations.
// TODO: Needs more design work before V2.
type PutFileClient interface {
	// PutFileWriter writes a file to PFS.
	// NOTE: PutFileWriter returns an io.WriteCloser that you must call Close on when
	// you are done writing.
	PutFileWriter(repo, commit, path string) (io.WriteCloser, error)

	// PutFile writes a file to PFS from a reader.
	PutFile(repo, commit, path string, r io.Reader) error

	// PutFileOverwrite is like PutFile but it overwrites the file rather than
	// appending to it. overwriteIndex allows you to specify the index of the
	// object starting from which you'd like to overwrite. If you want to
	// overwrite the entire file, specify an index of 0.
	PutFileOverwrite(repo, commit, path string, r io.Reader) error

	// PutFileSplit writes a file to PFS from a reader.
	// delimiter is used to tell PFS how to break the input into files.
	PutFileSplit(repo, commit, path string, delimiter pfs.Delimiter, targetFileDatums int64, targetFileBytes int64, overwrite bool, r io.Reader) error

	// PutFileURL puts a file using the content found at a URL.
	// The URL is sent to the server which performs the request.
	// recursive allows for recursive scraping of some types URLs. For example on s3:// urls.
	PutFileURL(repoName string, commitID string, path string, url string, recursive bool, overwrite bool) error

	// DeleteFile deletes a file from a Commit.
	// DeleteFile leaves a tombstone in the Commit, assuming the file isn't written
	// to later attempting to get the file from the finished commit will result in
	// not found error.
	// The file will of course remain intact in the Commit's parent.
	DeleteFile(repoName string, commitID string, path string) error

	// Close must be called after you're done using a PutFileClient.
	// Further requests will throw errors.
	Close() error
}

var errV1NotImplemented = errors.Errorf("V1 method not implemented")

type putFileClient struct {
	c APIClient
}

// NewPutFileClient creates a new put file client.
func (c APIClient) NewPutFileClient() (PutFileClient, error) {
	return &putFileClient{c: c}, nil
}

// PutFileWriter returns a write closer for a file.
func (pfc *putFileClient) PutFileWriter(repo, commit, path string) (io.WriteCloser, error) {
	return nil, errV1NotImplemented
}

// PutFile puts a file into PFS.
func (pfc *putFileClient) PutFile(repo, commit, path string, r io.Reader) error {
	return pfc.c.AppendFile(repo, commit, path, r)
}

func (pfc *putFileClient) PutFileOverwrite(repo, commit, path string, r io.Reader) error {
	return pfc.c.AppendFile(repo, commit, path, r, WithOverwrite())
}

func (pfc *putFileClient) PutFileSplit(repo, commit, path string, delimiter pfs.Delimiter, targetFileDatums int64, targetFileBytes int64, overwrite bool, r io.Reader) error {
	var opts []AppendFileOption
	if delimiter != pfs.Delimiter_NONE {
		opts = append(opts, WithSplit(delimiter, targetFileDatums, targetFileBytes))
	}
	if overwrite {
		opts = append(opts, WithOverwrite())
	}
	return pfc.c.AppendFile(repo, commit, path, r, opts...)
}

func (pfc *putFileClient) PutFileURL(repo, commit, path, url string, recursive, overwrite bool) error {
	var opts []AppendFileOption
	if overwrite {
		opts = append(opts, WithOverwrite())
	}
	return pfc.c.AppendFileURL(repo, commit, path, url, recursive, opts...)
}

func (pfc *putFileClient) DeleteFile(repo, commit, path string) error {
	return pfc.c.deleteFile(repo, commit, path)
}

func (pfc *putFileClient) Close() error {
	return nil
}

// PutFileWriter writes a file to PFS.
// NOTE: PutFileWriter returns an io.WriteCloser you must call Close on it when
// you are done writing.
func (c APIClient) PutFileWriter(repoName string, commitID string, path string) (io.WriteCloser, error) {
	pfc, err := c.NewPutFileClient()
	if err != nil {
		return nil, err
	}
	return pfc.PutFileWriter(repoName, commitID, path)
}

// PutFile writes a file to PFS from a reader.
func (c APIClient) PutFile(repo, commit, path string, r io.Reader) error {
	pfc, err := c.NewPutFileClient()
	if err != nil {
		return err
	}
	return pfc.PutFile(repo, commit, path, r)
}

// PutFileOverwrite is like PutFile but it overwrites the file rather than
// appending to it.  overwriteIndex allows you to specify the index of the
// object starting from which you'd like to overwrite.  If you want to
// overwrite the entire file, specify an index of 0.
func (c APIClient) PutFileOverwrite(repoName string, commitID string, path string, reader io.Reader) error {
	pfc, err := c.NewPutFileClient()
	if err != nil {
		return err
	}
	return pfc.PutFileOverwrite(repoName, commitID, path, reader)
}

// PutFileSplit is like PutFile but splits the file based on a delimiter and target datums / bytes per file.
func (c APIClient) PutFileSplit(repoName string, commitID string, path string, delimiter pfs.Delimiter, targetFileDatums int64, targetFileBytes int64, overwrite bool, reader io.Reader) error {
	pfc, err := c.NewPutFileClient()
	if err != nil {
		return err
	}
	return pfc.PutFileSplit(repoName, commitID, path, delimiter, targetFileDatums, targetFileBytes, overwrite, reader)
}

// PutFileURL puts a file using the content found at a URL.
// The URL is sent to the server which performs the request.
// recursive allow for recursive scraping of some types URLs for example on s3:// urls.
func (c APIClient) PutFileURL(repoName string, commitID string, path string, url string, recursive bool, overwrite bool) error {
	pfc, err := c.NewPutFileClient()
	if err != nil {
		return err
	}
	return pfc.PutFileURL(repoName, commitID, path, url, recursive, overwrite)
}

// CopyFile copys a file from one pfs location to another. It can be used on
// directories or regular files.
func (c APIClient) CopyFile(srcRepo, srcCommit, srcPath, dstRepo, dstCommit, dstPath string, overwrite bool) error {
	if _, err := c.PfsAPIClient.CopyFile(c.Ctx(),
		&pfs.CopyFileRequest{
			Src:       NewFile(srcRepo, srcCommit, srcPath),
			Dst:       NewFile(dstRepo, dstCommit, dstPath),
			Overwrite: overwrite,
		}); err != nil {
		return grpcutil.ScrubGRPC(err)
	}
	return nil
}

// GetFile returns the contents of a file at a specific Commit.
// offset specifies a number of bytes that should be skipped in the beginning of the file.
// size limits the total amount of data returned, note you will get fewer bytes
// than size if you pass a value larger than the size of the file.
// If size is set to 0 then all of the data will be returned.
func (c APIClient) GetFile(repo, commit, path string, w io.Writer) error {
	r, err := c.getFile(repo, commit, path)
	if err != nil {
		return err
	}
	return tarutil.Iterate(r, func(f tarutil.File) error {
		return f.Content(w)
	}, true)
}

// GetTarFile gets a tar file from PFS.
func (c APIClient) GetTarFile(repo, commit, path string) (io.Reader, error) {
	return c.getFile(repo, commit, path)
}

func (c APIClient) getFile(repo, commit, path string) (_ io.Reader, retErr error) {
	defer func() {
		retErr = grpcutil.ScrubGRPC(retErr)
	}()
	req := &pfs.GetFileRequest{
		File: NewFile(repo, commit, path),
	}
	client, err := c.PfsAPIClient.GetFile(c.Ctx(), req)
	if err != nil {
		return nil, err
	}
	return grpcutil.NewStreamingBytesReader(client, nil), nil
}

// InspectFile returns info about a specific file.
func (c APIClient) InspectFile(repoName string, commitID string, path string) (*pfs.FileInfo, error) {
	return c.inspectFile(repoName, commitID, path)
}

func (c APIClient) inspectFile(repoName string, commitID string, path string) (*pfs.FileInfo, error) {
	fileInfo, err := c.PfsAPIClient.InspectFile(
		c.Ctx(),
		&pfs.InspectFileRequest{
			File: NewFile(repoName, commitID, path),
		},
	)
	if err != nil {
		return nil, grpcutil.ScrubGRPC(err)
	}
	return fileInfo, nil
}

// ListFile returns info about all files in a Commit under path, calling cb with each FileInfo.
func (c APIClient) ListFile(repo, commit, path string, cb func(fi *pfs.FileInfo) error) (retErr error) {
	defer func() {
		retErr = grpcutil.ScrubGRPC(retErr)
	}()
	fs, err := c.PfsAPIClient.ListFile(
		c.Ctx(),
		&pfs.ListFileRequest{
			File: NewFile(repo, commit, path),
		},
	)
	if err != nil {
		return err
	}
	for {
		fi, err := fs.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if err := cb(fi); err != nil {
			if errors.Is(err, errutil.ErrBreak) {
				return nil
			}
			return err
		}
	}
}

// ListFileAll returns info about all files in a Commit under path.
func (c APIClient) ListFileAll(repo, commit, path string) (_ []*pfs.FileInfo, retErr error) {
	defer func() {
		retErr = grpcutil.ScrubGRPC(retErr)
	}()
	var fis []*pfs.FileInfo
	if err := c.ListFile(repo, commit, path, func(fi *pfs.FileInfo) error {
		fis = append(fis, fi)
		return nil
	}); err != nil {
		return nil, err
	}
	return fis, nil
}

// GlobFile returns files that match a given glob pattern in a given commit,
// calling cb with each FileInfo. The pattern is documented here:
// https://golang.org/pkg/path/filepath/#Match
func (c APIClient) GlobFile(repo, commit, pattern string, cb func(fi *pfs.FileInfo) error) (retErr error) {
	defer func() {
		retErr = grpcutil.ScrubGRPC(retErr)
	}()
	fs, err := c.PfsAPIClient.GlobFile(
		c.Ctx(),
		&pfs.GlobFileRequest{
			Commit:  NewCommit(repo, commit),
			Pattern: pattern,
		},
	)
	if err != nil {
		return err
	}
	for {
		fi, err := fs.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if err := cb(fi); err != nil {
			if errors.Is(err, errutil.ErrBreak) {
				return nil
			}
			return err
		}
	}
}

// GlobFileAll returns files that match a given glob pattern in a given commit.
// The pattern is documented here: https://golang.org/pkg/path/filepath/#Match
func (c APIClient) GlobFileAll(repo, commit, pattern string) (_ []*pfs.FileInfo, retErr error) {
	defer func() {
		retErr = grpcutil.ScrubGRPC(retErr)
	}()
	var fis []*pfs.FileInfo
	if err := c.GlobFile(repo, commit, pattern, func(fi *pfs.FileInfo) error {
		fis = append(fis, fi)
		return nil
	}); err != nil {
		return nil, err
	}
	return fis, nil
}

// DiffFile returns the differences between 2 paths at 2 commits.
// It streams back one file at a time which is either from the new path, or the old path
func (c APIClient) DiffFile(newRepo, newCommit, newPath, oldRepo, oldCommit, oldPath string, shallow bool, cb func(*pfs.FileInfo, *pfs.FileInfo) error) (retErr error) {
	defer func() {
		retErr = grpcutil.ScrubGRPC(retErr)
	}()
	ctx, cancel := context.WithCancel(c.Ctx())
	defer cancel()
	var oldFile *pfs.File
	if oldRepo != "" {
		oldFile = NewFile(oldRepo, oldCommit, oldPath)
	}
	req := &pfs.DiffFileRequest{
		NewFile: NewFile(newRepo, newCommit, newPath),
		OldFile: oldFile,
		Shallow: shallow,
	}
	client, err := c.PfsAPIClient.DiffFile(ctx, req)
	if err != nil {
		return err
	}
	for {
		resp, err := client.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if err := cb(resp.NewFile, resp.OldFile); err != nil {
			return err
		}
	}
}

// DiffFileAll returns the differences between 2 paths at 2 commits.
func (c APIClient) DiffFileAll(newRepo, newCommit, newPath, oldRepo, oldCommit, oldPath string, shallow bool) (_ []*pfs.FileInfo, _ []*pfs.FileInfo, retErr error) {
	defer func() {
		retErr = grpcutil.ScrubGRPC(retErr)
	}()
	var newFis, oldFis []*pfs.FileInfo
	if err := c.DiffFile(newRepo, newCommit, newPath, oldRepo, oldCommit, oldPath, shallow, func(newFi, oldFi *pfs.FileInfo) error {
		if newFi != nil {
			newFis = append(newFis, newFi)
		}
		if oldFi != nil {
			oldFis = append(oldFis, oldFi)
		}
		return nil
	}); err != nil {
		return nil, nil, err
	}
	return newFis, oldFis, nil
}

// WalkFile walks the files under path.
func (c APIClient) WalkFile(repo, commit, path string, cb func(*pfs.FileInfo) error) (retErr error) {
	defer func() {
		retErr = grpcutil.ScrubGRPC(retErr)
	}()
	fs, err := c.PfsAPIClient.WalkFile(
		c.Ctx(),
		&pfs.WalkFileRequest{
			File: NewFile(repo, commit, path),
		})
	if err != nil {
		return err
	}
	for {
		fi, err := fs.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if err := cb(fi); err != nil {
			if errors.Is(err, errutil.ErrBreak) {
				return nil
			}
			return err
		}
	}
}

// DeleteFile deletes a file from a Commit.
// DeleteFile leaves a tombstone in the Commit, assuming the file isn't written
// to later attempting to get the file from the finished commit will result in
// not found error.
// The file will of course remain intact in the Commit's parent.
func (c APIClient) DeleteFile(repoName string, commitID string, path string) error {
	pfc, err := c.NewPutFileClient()
	if err != nil {
		return err
	}
	return pfc.DeleteFile(repoName, commitID, path)
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

// TODO: Delete everything below after 1.12

// PutObjectAsync puts a value into the object store asynchronously.
func (c APIClient) PutObjectAsync(tags []*pfs.Tag) (*PutObjectWriteCloserAsync, error) {
	w, err := c.newPutObjectWriteCloserAsync(tags)
	if err != nil {
		return nil, grpcutil.ScrubGRPC(err)
	}
	return w, nil
}

// PutObject puts a value into the object store and tags it with 0 or more tags.
func (c APIClient) PutObject(_r io.Reader, tags ...string) (object *pfs.Object, _ int64, retErr error) {
	r := grpcutil.ReaderWrapper{_r}
	w, err := c.newPutObjectWriteCloser(tags...)
	if err != nil {
		return nil, 0, grpcutil.ScrubGRPC(err)
	}
	defer func() {
		if err := w.Close(); err != nil && retErr == nil {
			retErr = grpcutil.ScrubGRPC(err)
		}
		if retErr == nil {
			object = w.object
		}
	}()
	buf := grpcutil.GetBuffer()
	defer grpcutil.PutBuffer(buf)
	written, err := io.CopyBuffer(w, r, buf)
	if err != nil {
		return nil, 0, grpcutil.ScrubGRPC(err)
	}
	// return value set by deferred function
	return nil, written, nil
}

// PutObjectSplit is the same as PutObject except that the data is splitted
// into several smaller objects.  This is primarily useful if you'd like to
// be able to resume upload.
func (c APIClient) PutObjectSplit(_r io.Reader) (objects []*pfs.Object, _ int64, retErr error) {
	r := grpcutil.ReaderWrapper{_r}
	w, err := c.newPutObjectSplitWriteCloser()
	if err != nil {
		return nil, 0, grpcutil.ScrubGRPC(err)
	}
	defer func() {
		if err := w.Close(); err != nil && retErr == nil {
			retErr = grpcutil.ScrubGRPC(err)
		}
		if retErr == nil {
			objects = w.objects
		}
	}()
	buf := grpcutil.GetBuffer()
	defer grpcutil.PutBuffer(buf)
	written, err := io.CopyBuffer(w, r, buf)
	if err != nil {
		return nil, 0, grpcutil.ScrubGRPC(err)
	}
	// return value set by deferred function
	return nil, written, nil
}

// CreateObject creates an object with hash, referencing the range
// [lower,upper] in block. The block should already exist.
func (c APIClient) CreateObject(hash, block string, lower, upper uint64) error {
	_, err := c.ObjectAPIClient.CreateObject(c.Ctx(), &pfs.CreateObjectRequest{
		Object:   NewObject(hash),
		BlockRef: NewBlockRef(block, lower, upper),
	})
	return grpcutil.ScrubGRPC(err)
}

// GetObject gets an object out of the object store by hash.
func (c APIClient) GetObject(hash string, writer io.Writer) error {
	getObjectClient, err := c.ObjectAPIClient.GetObject(
		c.Ctx(),
		&pfs.Object{Hash: hash},
	)
	if err != nil {
		return grpcutil.ScrubGRPC(err)
	}
	if err := grpcutil.WriteFromStreamingBytesClient(getObjectClient, writer); err != nil {
		return grpcutil.ScrubGRPC(err)
	}
	return nil
}

// GetObjectReader returns a reader for an object in object store by hash.
func (c APIClient) GetObjectReader(hash string) (io.ReadCloser, error) {
	ctx, cancel := context.WithCancel(c.Ctx())
	getObjectClient, err := c.ObjectAPIClient.GetObject(
		ctx,
		&pfs.Object{Hash: hash},
	)
	if err != nil {
		return nil, grpcutil.ScrubGRPC(err)
	}
	return grpcutil.NewStreamingBytesReader(getObjectClient, cancel), nil
}

// ReadObject gets an object by hash and returns it directly as []byte.
func (c APIClient) ReadObject(hash string) ([]byte, error) {
	var buffer bytes.Buffer
	if err := c.GetObject(hash, &buffer); err != nil {
		return nil, grpcutil.ScrubGRPC(err)
	}
	return buffer.Bytes(), nil
}

// GetObjects gets several objects out of the object store by hash.
func (c APIClient) GetObjects(hashes []string, offset uint64, size uint64, totalSize uint64, writer io.Writer) error {
	var objects []*pfs.Object
	for _, hash := range hashes {
		objects = append(objects, &pfs.Object{Hash: hash})
	}
	getObjectsClient, err := c.ObjectAPIClient.GetObjects(
		c.Ctx(),
		&pfs.GetObjectsRequest{
			Objects:     objects,
			OffsetBytes: offset,
			SizeBytes:   size,
			TotalSize:   totalSize,
		},
	)
	if err != nil {
		return grpcutil.ScrubGRPC(err)
	}
	if err := grpcutil.WriteFromStreamingBytesClient(getObjectsClient, writer); err != nil {
		return grpcutil.ScrubGRPC(err)
	}
	return nil
}

// ReadObjects gets  several objects by hash and returns them directly as []byte.
func (c APIClient) ReadObjects(hashes []string, offset uint64, size uint64) ([]byte, error) {
	var buffer bytes.Buffer
	if err := c.GetObjects(hashes, offset, size, 0, &buffer); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

// TagObject applies a tag to an existing object.
func (c APIClient) TagObject(hash string, tags ...string) error {
	var _tags []*pfs.Tag
	for _, tag := range tags {
		_tags = append(_tags, &pfs.Tag{Name: tag})
	}
	if _, err := c.ObjectAPIClient.TagObject(
		c.Ctx(),
		&pfs.TagObjectRequest{
			Object: &pfs.Object{Hash: hash},
			Tags:   _tags,
		},
	); err != nil {
		return grpcutil.ScrubGRPC(err)
	}
	return nil
}

// ListObject lists objects stored in pfs.
func (c APIClient) ListObject(f func(*pfs.ObjectInfo) error) error {
	listObjectClient, err := c.ObjectAPIClient.ListObjects(c.Ctx(), &pfs.ListObjectsRequest{})
	if err != nil {
		return grpcutil.ScrubGRPC(err)
	}
	for {
		oi, err := listObjectClient.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return grpcutil.ScrubGRPC(err)
		}
		if err := f(oi); err != nil {
			return err
		}
	}
}

// InspectObject returns info about an Object.
func (c APIClient) InspectObject(hash string) (*pfs.ObjectInfo, error) {
	value, err := c.ObjectAPIClient.InspectObject(
		c.Ctx(),
		&pfs.Object{Hash: hash},
	)
	if err != nil {
		return nil, grpcutil.ScrubGRPC(err)
	}
	return value, nil
}

// GetTag gets an object out of the object store by tag.
func (c APIClient) GetTag(tag string, writer io.Writer) error {
	getTagClient, err := c.ObjectAPIClient.GetTag(
		c.Ctx(),
		&pfs.Tag{Name: tag},
	)
	if err != nil {
		return grpcutil.ScrubGRPC(err)
	}
	if err := grpcutil.WriteFromStreamingBytesClient(getTagClient, writer); err != nil {
		return grpcutil.ScrubGRPC(err)
	}
	return nil
}

// GetTagReader returns a reader for an object in object store by tag.
func (c APIClient) GetTagReader(tag string) (io.ReadCloser, error) {
	ctx, cancel := context.WithCancel(c.Ctx())
	getTagClient, err := c.ObjectAPIClient.GetTag(
		ctx,
		&pfs.Tag{Name: tag},
	)
	if err != nil {
		return nil, grpcutil.ScrubGRPC(err)
	}
	return grpcutil.NewStreamingBytesReader(getTagClient, cancel), nil
}

// ReadTag gets an object by tag and returns it directly as []byte.
func (c APIClient) ReadTag(tag string) ([]byte, error) {
	var buffer bytes.Buffer
	if err := c.GetTag(tag, &buffer); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

// ListTag lists tags stored in pfs.
func (c APIClient) ListTag(f func(*pfs.ListTagsResponse) error) error {
	listTagClient, err := c.ObjectAPIClient.ListTags(c.Ctx(), &pfs.ListTagsRequest{IncludeObject: true})
	if err != nil {
		return grpcutil.ScrubGRPC(err)
	}
	for {
		listTagResponse, err := listTagClient.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return grpcutil.ScrubGRPC(err)
		}
		if err := f(listTagResponse); err != nil {
			if errors.Is(err, errutil.ErrBreak) {
				return nil
			}
			return err
		}
	}
}

// ListBlock lists blocks stored in pfs.
func (c APIClient) ListBlock(f func(*pfs.Block) error) error {
	listBlocksClient, err := c.ObjectAPIClient.ListBlock(c.Ctx(), &pfs.ListBlockRequest{})
	if err != nil {
		return err
	}
	for {
		block, err := listBlocksClient.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return grpcutil.ScrubGRPC(err)
		}
		if err := f(block); err != nil {
			if errors.Is(err, errutil.ErrBreak) {
				return nil
			}
			return err
		}
	}
}

// GetBlock gets the content of a block.
func (c APIClient) GetBlock(hash string, w io.Writer) error {
	getBlockClient, err := c.ObjectAPIClient.GetBlock(
		c.Ctx(),
		&pfs.GetBlockRequest{Block: NewBlock(hash)},
	)
	if err != nil {
		return grpcutil.ScrubGRPC(err)
	}
	if err := grpcutil.WriteFromStreamingBytesClient(getBlockClient, w); err != nil {
		return grpcutil.ScrubGRPC(err)
	}
	return nil
}

// PutBlock puts a block.
func (c APIClient) PutBlock(hash string, _r io.Reader) (_ int64, retErr error) {
	r := grpcutil.ReaderWrapper{_r}
	w, err := c.newPutBlockWriteCloser(hash)
	if err != nil {
		return 0, err
	}
	defer func() {
		if err := w.Close(); err != nil && retErr == nil {
			retErr = errors.Wrap(grpcutil.ScrubGRPC(err), "Close")
		}
	}()
	buf := grpcutil.GetBuffer()
	defer grpcutil.PutBuffer(buf)
	written, err := io.CopyBuffer(w, r, buf)
	if err != nil {
		return written, errors.Wrap(grpcutil.ScrubGRPC(err), "CopyBuffer")
	}
	// return value set by deferred function
	return written, nil
}

// Compact forces compaction of objects.
func (c APIClient) Compact() error {
	_, err := c.ObjectAPIClient.Compact(
		c.Ctx(),
		&types.Empty{},
	)
	return err
}

// DirectObjReader returns a reader for the contents of an obj in object
// storage, it reads directly from object storage, bypassing the
// content-addressing layer.
func (c APIClient) DirectObjReader(obj string) (io.ReadCloser, error) {
	getObjClient, err := c.ObjectAPIClient.GetObjDirect(
		c.Ctx(),
		&pfs.GetObjDirectRequest{Obj: obj},
	)
	if err != nil {
		return nil, grpcutil.ScrubGRPC(err)
	}
	return grpcutil.NewStreamingBytesReader(getObjClient, nil), nil
}

// DirectObjWriter returns a writer for an obj in object storage, it writes
// directly to object storage, bypassing the content-addressing layer.
func (c APIClient) DirectObjWriter(obj string) (io.WriteCloser, error) {
	return c.newPutObjWriteCloser(obj)
}

// NewObject creates a pfs.Object.
func NewObject(hash string) *pfs.Object {
	return &pfs.Object{
		Hash: hash,
	}
}

// NewBlock creates a pfs.Block.
func NewBlock(hash string) *pfs.Block {
	return &pfs.Block{
		Hash: hash,
	}
}

// NewBlockRef creates a pfs.BlockRef.
func NewBlockRef(hash string, lower, upper uint64) *pfs.BlockRef {
	return &pfs.BlockRef{
		Block: NewBlock(hash),
		Range: &pfs.ByteRange{Lower: lower, Upper: upper},
	}
}

// NewTag creates a pfs.Tag.
func NewTag(name string) *pfs.Tag {
	return &pfs.Tag{
		Name: name,
	}
}

type putObjectWriteCloser struct {
	request *pfs.PutObjectRequest
	client  pfs.ObjectAPI_PutObjectClient
	object  *pfs.Object
}

func (c APIClient) newPutObjectWriteCloser(tags ...string) (*putObjectWriteCloser, error) {
	client, err := c.ObjectAPIClient.PutObject(c.Ctx())
	if err != nil {
		return nil, grpcutil.ScrubGRPC(err)
	}
	var _tags []*pfs.Tag
	for _, tag := range tags {
		_tags = append(_tags, &pfs.Tag{Name: tag})
	}
	return &putObjectWriteCloser{
		request: &pfs.PutObjectRequest{
			Tags: _tags,
		},
		client: client,
	}, nil
}

func (w *putObjectWriteCloser) Write(p []byte) (int, error) {
	for _, dataSlice := range grpcutil.Chunk(p) {
		w.request.Value = dataSlice
		if err := w.client.Send(w.request); err != nil {
			return 0, grpcutil.ScrubGRPC(err)
		}
		w.request.Tags = nil
	}
	return len(p), nil
}

func (w *putObjectWriteCloser) Close() error {
	var err error
	w.object, err = w.client.CloseAndRecv()
	return grpcutil.ScrubGRPC(err)
}

// PutObjectWriteCloserAsync wraps a put object call in an asynchronous buffered writer.
type PutObjectWriteCloserAsync struct {
	client    pfs.ObjectAPI_PutObjectClient
	request   *pfs.PutObjectRequest
	buf       []byte
	writeChan chan []byte
	errChan   chan error
	object    *pfs.Object
}

func (c APIClient) newPutObjectWriteCloserAsync(tags []*pfs.Tag) (*PutObjectWriteCloserAsync, error) {
	client, err := c.ObjectAPIClient.PutObject(c.Ctx())
	if err != nil {
		return nil, grpcutil.ScrubGRPC(err)
	}
	w := &PutObjectWriteCloserAsync{
		client: client,
		request: &pfs.PutObjectRequest{
			Tags: tags,
		},
		buf:       grpcutil.GetBuffer()[:0],
		writeChan: make(chan []byte, 5),
		errChan:   make(chan error),
	}
	go func() {
		for buf := range w.writeChan {
			w.request.Value = buf
			if err := w.client.Send(w.request); err != nil {
				w.errChan <- err
				break
			}
			w.request.Tags = nil
			grpcutil.PutBuffer(buf[:cap(buf)])
		}
		close(w.errChan)
	}()
	return w, nil
}

// Write performs a write.
func (w *PutObjectWriteCloserAsync) Write(p []byte) (int, error) {
	var written int
	for len(w.buf)+len(p) > cap(w.buf) {
		// Write the bytes that fit into w.buf, then
		// remove those bytes from p.
		i := cap(w.buf) - len(w.buf)
		w.buf = append(w.buf, p[:i]...)
		if err := w.writeBuf(); err != nil {
			return 0, err
		}
		written += i
		p = p[i:]
		w.buf = grpcutil.GetBuffer()[:0]
	}
	w.buf = append(w.buf, p...)
	written += len(p)
	return written, nil
}

// Close closes the writer.
func (w *PutObjectWriteCloserAsync) Close() error {
	if err := w.writeBuf(); err != nil {
		return err
	}
	close(w.writeChan)
	err := <-w.errChan
	if err != nil {
		return grpcutil.ScrubGRPC(err)
	}
	w.object, err = w.client.CloseAndRecv()
	return grpcutil.ScrubGRPC(err)
}

func (w *PutObjectWriteCloserAsync) writeBuf() error {
	select {
	case err := <-w.errChan:
		if err != nil {
			return grpcutil.ScrubGRPC(err)
		}
	case w.writeChan <- w.buf:
	}
	return nil
}

// Object gets the pfs object for this writer.
// This can only be called when the writer is closed (the put object
// call is complete)
func (w *PutObjectWriteCloserAsync) Object() (*pfs.Object, error) {
	select {
	case err := <-w.errChan:
		if err != nil {
			return nil, grpcutil.ScrubGRPC(err)
		}
		return w.object, nil
	default:
		return nil, errors.Errorf("attempting to get object before closing object writer")
	}
}

type putObjectSplitWriteCloser struct {
	request *pfs.PutObjectRequest
	client  pfs.ObjectAPI_PutObjectSplitClient
	objects []*pfs.Object
}

func (c APIClient) newPutObjectSplitWriteCloser() (*putObjectSplitWriteCloser, error) {
	client, err := c.ObjectAPIClient.PutObjectSplit(c.Ctx())
	if err != nil {
		return nil, grpcutil.ScrubGRPC(err)
	}
	return &putObjectSplitWriteCloser{
		request: &pfs.PutObjectRequest{},
		client:  client,
	}, nil
}

func (w *putObjectSplitWriteCloser) Write(p []byte) (int, error) {
	for _, dataSlice := range grpcutil.Chunk(p) {
		w.request.Value = dataSlice
		if err := w.client.Send(w.request); err != nil {
			return 0, grpcutil.ScrubGRPC(err)
		}
	}
	return len(p), nil
}

func (w *putObjectSplitWriteCloser) Close() error {
	objects, err := w.client.CloseAndRecv()
	if err != nil {
		return grpcutil.ScrubGRPC(err)
	}
	w.objects = objects.Objects
	return nil
}

type putBlockWriteCloser struct {
	request *pfs.PutBlockRequest
	client  pfs.ObjectAPI_PutBlockClient
}

func (c APIClient) newPutBlockWriteCloser(hash string) (*putBlockWriteCloser, error) {
	client, err := c.ObjectAPIClient.PutBlock(c.Ctx())
	if err != nil {
		return nil, grpcutil.ScrubGRPC(err)
	}
	return &putBlockWriteCloser{
		request: &pfs.PutBlockRequest{Block: NewBlock(hash)},
		client:  client,
	}, nil
}

func (w *putBlockWriteCloser) Write(p []byte) (int, error) {
	for _, dataSlice := range grpcutil.Chunk(p) {
		w.request.Value = dataSlice
		if err := w.client.Send(w.request); err != nil {
			return 0, grpcutil.ScrubGRPC(err)
		}
		w.request.Block = nil
	}
	return len(p), nil
}

func (w *putBlockWriteCloser) Close() error {
	if w.request.Block != nil {
		// This happens if the block is empty in which case Write was never
		// called, so we need to send an empty request to identify the block.
		if err := w.client.Send(w.request); err != nil {
			return grpcutil.ScrubGRPC(err)
		}
	}
	_, err := w.client.CloseAndRecv()
	return grpcutil.ScrubGRPC(err)
}

type putObjWriteCloser struct {
	request *pfs.PutObjDirectRequest
	client  pfs.ObjectAPI_PutObjDirectClient
}

func (c APIClient) newPutObjWriteCloser(obj string) (*putObjWriteCloser, error) {
	client, err := c.ObjectAPIClient.PutObjDirect(c.Ctx())
	if err != nil {
		return nil, grpcutil.ScrubGRPC(err)
	}
	return &putObjWriteCloser{
		request: &pfs.PutObjDirectRequest{Obj: obj},
		client:  client,
	}, nil
}

func (w *putObjWriteCloser) Write(p []byte) (int, error) {
	for _, dataSlice := range grpcutil.Chunk(p) {
		w.request.Value = dataSlice
		if err := w.client.Send(w.request); err != nil {
			return 0, grpcutil.ScrubGRPC(err)
		}
		w.request.Obj = ""
	}
	return len(p), nil
}

func (w *putObjWriteCloser) Close() error {
	if w.request.Obj != "" {
		// This happens if the block is empty in which case Write was never
		// called, so we need to send an empty request to identify the block.
		if err := w.client.Send(w.request); err != nil {
			return grpcutil.ScrubGRPC(err)
		}
	}
	_, err := w.client.CloseAndRecv()
	return grpcutil.ScrubGRPC(err)
}

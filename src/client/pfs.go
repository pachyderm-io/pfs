package client

import (
	"bytes"
	"context"
	"io"
	"path/filepath"

	"github.com/gogo/protobuf/types"
	"github.com/pachyderm/pachyderm/src/client/pfs"
	"github.com/pachyderm/pachyderm/src/client/pkg/grpcutil"
)

// NewRepo creates a pfs.Repo.
func NewRepo(repoName string) *pfs.Repo {
	return &pfs.Repo{Name: repoName}
}

// NewCommit creates a pfs.Commit.
func NewCommit(repoName string, commitID string) *pfs.Commit {
	return &pfs.Commit{
		Repo: NewRepo(repoName),
		ID:   commitID,
	}
}

// NewFile creates a pfs.File.
func NewFile(repoName string, commitID string, path string) *pfs.File {
	return &pfs.File{
		Commit: NewCommit(repoName, commitID),
		Path:   path,
	}
}

// NewBlock creates a pfs.Block.
func NewBlock(hash string) *pfs.Block {
	return &pfs.Block{
		Hash: hash,
	}
}

// CreateRepo creates a new Repo object in pfs with the given name. Repos are
// the top level data object in pfs and should be used to store data of a
// similar type. For example rather than having a single Repo for an entire
// project you might have seperate Repos for logs, metrics, database dumps etc.
func (c APIClient) CreateRepo(repoName string) error {
	_, err := c.PfsAPIClient.CreateRepo(
		c.ctx(),
		&pfs.CreateRepoRequest{
			Repo: NewRepo(repoName),
		},
	)
	return sanitizeErr(err)
}

// InspectRepo returns info about a specific Repo.
func (c APIClient) InspectRepo(repoName string) (*pfs.RepoInfo, error) {
	repoInfo, err := c.PfsAPIClient.InspectRepo(
		c.ctx(),
		&pfs.InspectRepoRequest{
			Repo: NewRepo(repoName),
		},
	)
	if err != nil {
		return nil, sanitizeErr(err)
	}
	return repoInfo, nil
}

// ListRepo returns info about all Repos.
// provenance specifies a set of provenance repos, only repos which have ALL of
// the specified repos as provenance will be returned unless provenance is nil
// in which case it is ignored.
func (c APIClient) ListRepo(provenance []string) ([]*pfs.RepoInfo, error) {
	request := &pfs.ListRepoRequest{}
	for _, repoName := range provenance {
		request.Provenance = append(request.Provenance, NewRepo(repoName))
	}
	repoInfos, err := c.PfsAPIClient.ListRepo(
		c.ctx(),
		request,
	)
	if err != nil {
		return nil, sanitizeErr(err)
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
	_, err := c.PfsAPIClient.DeleteRepo(
		c.ctx(),
		&pfs.DeleteRepoRequest{
			Repo:  NewRepo(repoName),
			Force: force,
		},
	)
	return err
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
		c.ctx(),
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
		return nil, sanitizeErr(err)
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
		c.ctx(),
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
		return nil, sanitizeErr(err)
	}
	return commit, nil
}

// FinishCommit ends the process of committing data to a Repo and persists the
// Commit. Once a Commit is finished the data becomes immutable and future
// attempts to write to it with PutFile will error.
func (c APIClient) FinishCommit(repoName string, commitID string) error {
	_, err := c.PfsAPIClient.FinishCommit(
		c.ctx(),
		&pfs.FinishCommitRequest{
			Commit: NewCommit(repoName, commitID),
		},
	)
	return sanitizeErr(err)
}

// InspectCommit returns info about a specific Commit.
func (c APIClient) InspectCommit(repoName string, commitID string) (*pfs.CommitInfo, error) {
	commitInfo, err := c.PfsAPIClient.InspectCommit(
		c.ctx(),
		&pfs.InspectCommitRequest{
			Commit: NewCommit(repoName, commitID),
		},
	)
	if err != nil {
		return nil, sanitizeErr(err)
	}
	return commitInfo, nil
}

// ListCommit lists commits.
// If only `repo` is given, all commits in the repo are returned.
// If `to` is given, only the ancestors of `to`, including `to` itself,
// are considered.
// If `from` is given, only the descendents of `from`, including `from`
// itself, are considered.
// `number` determines how many commits are returned.  If `number` is 0,
// all commits that match the aforementioned criteria are returned.
func (c APIClient) ListCommit(repoName string, to string, from string, number uint64) ([]*pfs.CommitInfo, error) {
	req := &pfs.ListCommitRequest{
		Repo:   NewRepo(repoName),
		Number: number,
	}
	if from != "" {
		req.From = NewCommit(repoName, from)
	}
	if to != "" {
		req.To = NewCommit(repoName, to)
	}
	commitInfos, err := c.PfsAPIClient.ListCommit(
		c.ctx(),
		req,
	)
	if err != nil {
		return nil, sanitizeErr(err)
	}
	return commitInfos.CommitInfo, nil
}

// ListCommitByRepo lists all commits in a repo.
func (c APIClient) ListCommitByRepo(repoName string) ([]*pfs.CommitInfo, error) {
	return c.ListCommit(repoName, "", "", 0)
}

// ListBranch lists the active branches on a Repo.
func (c APIClient) ListBranch(repoName string) ([]*pfs.Branch, error) {
	branches, err := c.PfsAPIClient.ListBranch(
		c.ctx(),
		&pfs.ListBranchRequest{
			Repo: NewRepo(repoName),
		},
	)
	if err != nil {
		return nil, sanitizeErr(err)
	}
	return branches.Branches, nil
}

// SetBranch sets a commit and its ancestors as a branch
func (c APIClient) SetBranch(repoName string, commit string, branch string) error {
	_, err := c.PfsAPIClient.SetBranch(
		c.ctx(),
		&pfs.SetBranchRequest{
			Commit: NewCommit(repoName, commit),
			Branch: branch,
		},
	)
	return sanitizeErr(err)
}

// DeleteBranch deletes a branch, but leaves the commits themselves intact.
// In other words, those commits can still be accessed via commit IDs and
// other branches they happen to be on.
func (c APIClient) DeleteBranch(repoName string, branch string) error {
	_, err := c.PfsAPIClient.DeleteBranch(
		c.ctx(),
		&pfs.DeleteBranchRequest{
			Repo:   NewRepo(repoName),
			Branch: branch,
		},
	)
	return sanitizeErr(err)
}

// DeleteCommit deletes a commit.
// Note it is currently not implemented.
func (c APIClient) DeleteCommit(repoName string, commitID string) error {
	_, err := c.PfsAPIClient.DeleteCommit(
		c.ctx(),
		&pfs.DeleteCommitRequest{
			Commit: NewCommit(repoName, commitID),
		},
	)
	return sanitizeErr(err)
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
	ctx, cancel := context.WithCancel(c.ctx())
	stream, err := c.PfsAPIClient.FlushCommit(
		ctx,
		&pfs.FlushCommitRequest{
			Commits: commits,
			ToRepos: toRepos,
		},
	)
	if err != nil {
		cancel()
		return nil, sanitizeErr(err)
	}
	return &commitInfoIterator{stream, cancel}, nil
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
	// this is completely retarded, but according to this thread it's
	// necessary for closing a server-side stream from the client side.
	// https://github.com/grpc/grpc-go/issues/188
	for {
		if _, err := c.stream.Recv(); err != nil {
			break
		}
	}
}

// SubscribeCommit is like ListCommit but it keeps listening for commits as
// they come in.
func (c APIClient) SubscribeCommit(repo string, branch string, from string) (CommitInfoIterator, error) {
	ctx, cancel := context.WithCancel(c.ctx())
	req := &pfs.SubscribeCommitRequest{
		Repo:   NewRepo(repo),
		Branch: branch,
	}
	if from != "" {
		req.From = NewCommit(repo, from)
	}
	stream, err := c.PfsAPIClient.SubscribeCommit(ctx, req)
	if err != nil {
		cancel()
		return nil, sanitizeErr(err)
	}
	return &commitInfoIterator{stream, cancel}, nil
}

// PutObject puts a value into the object store and tags it with 0 or more tags.
func (c APIClient) PutObject(r io.Reader, tags ...string) (object *pfs.Object, _ int64, retErr error) {
	w, err := c.newPutObjectWriteCloser(tags...)
	if err != nil {
		return nil, 0, sanitizeErr(err)
	}
	defer func() {
		if err := w.Close(); err != nil && retErr == nil {
			retErr = sanitizeErr(err)
		}
		if retErr == nil {
			object = w.object
		}
	}()
	buf := grpcutil.GetBuffer()
	defer grpcutil.PutBuffer(buf)
	written, err := io.CopyBuffer(w, r, buf)
	if err != nil {
		return nil, 0, sanitizeErr(err)
	}
	// return value set by deferred function
	return nil, written, nil
}

// GetObject gets an object out of the object store by hash.
func (c APIClient) GetObject(hash string, writer io.Writer) error {
	getObjectClient, err := c.ObjectAPIClient.GetObject(
		c.ctx(),
		&pfs.Object{Hash: hash},
	)
	if err != nil {
		return sanitizeErr(err)
	}
	if err := grpcutil.WriteFromStreamingBytesClient(getObjectClient, writer); err != nil {
		return sanitizeErr(err)
	}
	return nil
}

// ReadObject gets an object by hash and returns it directly as []byte.
func (c APIClient) ReadObject(hash string) ([]byte, error) {
	var buffer bytes.Buffer
	if err := c.GetObject(hash, &buffer); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

// GetObjects gets several objects out of the object store by hash.
func (c APIClient) GetObjects(hashes []string, offset uint64, size uint64, writer io.Writer) error {
	var objects []*pfs.Object
	for _, hash := range hashes {
		objects = append(objects, &pfs.Object{Hash: hash})
	}
	getObjectsClient, err := c.ObjectAPIClient.GetObjects(
		c.ctx(),
		&pfs.GetObjectsRequest{
			Objects:     objects,
			OffsetBytes: offset,
			SizeBytes:   size,
		},
	)
	if err != nil {
		return sanitizeErr(err)
	}
	if err := grpcutil.WriteFromStreamingBytesClient(getObjectsClient, writer); err != nil {
		return sanitizeErr(err)
	}
	return nil
}

// ReadObjects gets  several objects by hash and returns them directly as []byte.
func (c APIClient) ReadObjects(hashes []string, offset uint64, size uint64) ([]byte, error) {
	var buffer bytes.Buffer
	if err := c.GetObjects(hashes, offset, size, &buffer); err != nil {
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
		c.ctx(),
		&pfs.TagObjectRequest{
			Object: &pfs.Object{Hash: hash},
			Tags:   _tags,
		},
	); err != nil {
		return sanitizeErr(err)
	}
	return nil
}

// InspectObject returns info about an Object.
func (c APIClient) InspectObject(hash string) (*pfs.ObjectInfo, error) {
	value, err := c.ObjectAPIClient.InspectObject(
		c.ctx(),
		&pfs.Object{Hash: hash},
	)
	if err != nil {
		return nil, sanitizeErr(err)
	}
	return value, nil
}

// GetTag gets an object out of the object store by tag.
func (c APIClient) GetTag(tag string, writer io.Writer) error {
	getTagClient, err := c.ObjectAPIClient.GetTag(
		c.ctx(),
		&pfs.Tag{Name: tag},
	)
	if err != nil {
		return sanitizeErr(err)
	}
	if err := grpcutil.WriteFromStreamingBytesClient(getTagClient, writer); err != nil {
		return sanitizeErr(err)
	}
	return nil
}

// ReadTag gets an object by tag and returns it directly as []byte.
func (c APIClient) ReadTag(tag string) ([]byte, error) {
	var buffer bytes.Buffer
	if err := c.GetTag(tag, &buffer); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

// Compact forces compaction of objects.
func (c APIClient) Compact() error {
	_, err := c.ObjectAPIClient.Compact(
		c.ctx(),
		&types.Empty{},
	)
	return err
}

// PutFileWriter writes a file to PFS.
// NOTE: PutFileWriter returns an io.WriteCloser you must call Close on it when
// you are done writing.
func (c APIClient) PutFileWriter(repoName string, commitID string, path string) (io.WriteCloser, error) {
	return c.newPutFileWriteCloser(repoName, commitID, path, pfs.Delimiter_NONE, 0, 0)
}

// PutFileSplitWriter writes a multiple files to PFS by splitting up the data
// that is written to it.
// NOTE: PutFileSplitWriter returns an io.WriteCloser you must call Close on it when
// you are done writing.
func (c APIClient) PutFileSplitWriter(repoName string, commitID string, path string,
	delimiter pfs.Delimiter, targetFileDatums int64, targetFileBytes int64) (io.WriteCloser, error) {
	return c.newPutFileWriteCloser(repoName, commitID, path, delimiter, targetFileDatums, targetFileBytes)
}

// PutFile writes a file to PFS from a reader.
func (c APIClient) PutFile(repoName string, commitID string, path string, reader io.Reader) (_ int, retErr error) {
	if c.streamSemaphore != nil {
		c.streamSemaphore <- struct{}{}
		defer func() { <-c.streamSemaphore }()
	}
	return c.PutFileSplit(repoName, commitID, path, pfs.Delimiter_NONE, 0, 0, reader)
}

//PutFileSplit writes a file to PFS from a reader
// delimiter is used to tell PFS how to break the input into blocks
func (c APIClient) PutFileSplit(repoName string, commitID string, path string, delimiter pfs.Delimiter, targetFileDatums int64, targetFileBytes int64, reader io.Reader) (_ int, retErr error) {
	writer, err := c.PutFileSplitWriter(repoName, commitID, path, delimiter, targetFileDatums, targetFileBytes)
	if err != nil {
		return 0, sanitizeErr(err)
	}
	defer func() {
		if err := writer.Close(); err != nil && retErr == nil {
			retErr = err
		}
	}()
	written, err := io.Copy(writer, reader)
	return int(written), err
}

// PutFileURL puts a file using the content found at a URL.
// The URL is sent to the server which performs the request.
// recursive allow for recursive scraping of some types URLs for example on s3:// urls.
func (c APIClient) PutFileURL(repoName string, commitID string, path string, url string, recursive bool) (retErr error) {
	putFileClient, err := c.PfsAPIClient.PutFile(c.ctx())
	if err != nil {
		return sanitizeErr(err)
	}
	defer func() {
		if _, err := putFileClient.CloseAndRecv(); err != nil && retErr == nil {
			retErr = sanitizeErr(err)
		}
	}()
	if err := putFileClient.Send(&pfs.PutFileRequest{
		File:      NewFile(repoName, commitID, path),
		Url:       url,
		Recursive: recursive,
	}); err != nil {
		return sanitizeErr(err)
	}
	return nil
}

// GetFile returns the contents of a file at a specific Commit.
// offset specifies a number of bytes that should be skipped in the beginning of the file.
// size limits the total amount of data returned, note you will get fewer bytes
// than size if you pass a value larger than the size of the file.
// If size is set to 0 then all of the data will be returned.
func (c APIClient) GetFile(repoName string, commitID string, path string, offset int64, size int64, writer io.Writer) error {
	if c.streamSemaphore != nil {
		c.streamSemaphore <- struct{}{}
		defer func() { <-c.streamSemaphore }()
	}
	apiGetFileClient, err := c.getFile(repoName, commitID, path, offset, size)
	if err != nil {
		return sanitizeErr(err)
	}
	if err := grpcutil.WriteFromStreamingBytesClient(apiGetFileClient, writer); err != nil {
		return sanitizeErr(err)
	}
	return nil
}

// GetFileReader returns a reader for the contents of a file at a specific Commit.
// offset specifies a number of bytes that should be skipped in the beginning of the file.
// size limits the total amount of data returned, note you will get fewer bytes
// than size if you pass a value larger than the size of the file.
// If size is set to 0 then all of the data will be returned.
func (c APIClient) GetFileReader(repoName string, commitID string, path string, offset int64, size int64) (io.Reader, error) {
	apiGetFileClient, err := c.getFile(repoName, commitID, path, offset, size)
	if err != nil {
		return nil, sanitizeErr(err)
	}
	return grpcutil.NewStreamingBytesReader(apiGetFileClient), nil
}

func (c APIClient) getFile(repoName string, commitID string, path string, offset int64,
	size int64) (pfs.API_GetFileClient, error) {
	return c.PfsAPIClient.GetFile(
		c.ctx(),
		&pfs.GetFileRequest{
			File:        NewFile(repoName, commitID, path),
			OffsetBytes: offset,
			SizeBytes:   size,
		},
	)
}

// InspectFile returns info about a specific file.
func (c APIClient) InspectFile(repoName string, commitID string, path string) (*pfs.FileInfo, error) {
	return c.inspectFile(repoName, commitID, path)
}

func (c APIClient) inspectFile(repoName string, commitID string, path string) (*pfs.FileInfo, error) {
	fileInfo, err := c.PfsAPIClient.InspectFile(
		c.ctx(),
		&pfs.InspectFileRequest{
			File: NewFile(repoName, commitID, path),
		},
	)
	if err != nil {
		return nil, sanitizeErr(err)
	}
	return fileInfo, nil
}

// ListFile returns info about all files in a Commit.
func (c APIClient) ListFile(repoName string, commitID string, path string) ([]*pfs.FileInfo, error) {
	fileInfos, err := c.PfsAPIClient.ListFile(
		c.ctx(),
		&pfs.ListFileRequest{
			File: NewFile(repoName, commitID, path),
		},
	)
	if err != nil {
		return nil, sanitizeErr(err)
	}
	return fileInfos.FileInfo, nil
}

// GlobFile returns files that match a given glob pattern in a given commit.
// The pattern is documented here:
// https://golang.org/pkg/path/filepath/#Match
func (c APIClient) GlobFile(repoName string, commitID string, pattern string) ([]*pfs.FileInfo, error) {
	fileInfos, err := c.PfsAPIClient.GlobFile(
		c.ctx(),
		&pfs.GlobFileRequest{
			Commit:  NewCommit(repoName, commitID),
			Pattern: pattern,
		},
	)
	if err != nil {
		return nil, sanitizeErr(err)
	}
	return fileInfos.FileInfo, nil
}

// DiffFile returns the difference between 2 paths, old path may be omitted in
// which case the parent of the new path will be used. DiffFile return 2 values
// (unless it returns an error) the first value is files present under new
// path, the second is files present under old path, files which are under both
// paths and have identical content are omitted.
func (c APIClient) DiffFile(newRepoName, newCommitID, newPath, oldRepoName,
	oldCommitID, oldPath string) ([]*pfs.FileInfo, []*pfs.FileInfo, error) {
	var oldFile *pfs.File
	if oldRepoName != "" {
		oldFile = NewFile(oldRepoName, oldCommitID, oldPath)
	}
	resp, err := c.PfsAPIClient.DiffFile(
		c.ctx(),
		&pfs.DiffFileRequest{
			NewFile: NewFile(newRepoName, newCommitID, newPath),
			OldFile: oldFile,
		},
	)
	if err != nil {
		return nil, nil, sanitizeErr(err)
	}
	return resp.NewFiles, resp.OldFiles, nil
}

// WalkFn is the type of the function called for each file in Walk.
// Returning a non-nil error from WalkFn will result in Walk aborting and
// returning said error.
type WalkFn func(*pfs.FileInfo) error

// Walk walks the pfs filesystem rooted at path. walkFn will be called for each
// file found under path, this includes both regular files and directories.
func (c APIClient) Walk(repoName string, commitID string, path string, walkFn WalkFn) error {
	fileInfo, err := c.InspectFile(repoName, commitID, path)
	if err != nil {
		return err
	}
	if err := walkFn(fileInfo); err != nil {
		return err
	}
	for _, childPath := range fileInfo.Children {
		if err := c.Walk(repoName, commitID, filepath.Join(path, childPath), walkFn); err != nil {
			return err
		}
	}
	return nil
}

// DeleteFile deletes a file from a Commit.
// DeleteFile leaves a tombstone in the Commit, assuming the file isn't written
// to later attempting to get the file from the finished commit will result in
// not found error.
// The file will of course remain intact in the Commit's parent.
func (c APIClient) DeleteFile(repoName string, commitID string, path string) error {
	_, err := c.PfsAPIClient.DeleteFile(
		c.ctx(),
		&pfs.DeleteFileRequest{
			File: NewFile(repoName, commitID, path),
		},
	)
	return err
}

func (c APIClient) Fsck(dryRun bool) (*pfs.FsckResponse, error) {
	return c.PfsAPIClient.Fsck(
		c.ctx(),
		&pfs.FsckRequest{
			DryRun: dryRun,
		},
	)
}

type putFileWriteCloser struct {
	request       *pfs.PutFileRequest
	putFileClient pfs.API_PutFileClient
	sent          bool
}

func (c APIClient) newPutFileWriteCloser(repoName string, commitID string, path string, delimiter pfs.Delimiter, targetFileDatums int64, targetFileBytes int64) (*putFileWriteCloser, error) {
	putFileClient, err := c.PfsAPIClient.PutFile(c.ctx())
	if err != nil {
		return nil, err
	}
	return &putFileWriteCloser{
		request: &pfs.PutFileRequest{
			File:             NewFile(repoName, commitID, path),
			Delimiter:        delimiter,
			TargetFileDatums: targetFileDatums,
			TargetFileBytes:  targetFileBytes,
		},
		putFileClient: putFileClient,
	}, nil
}

func (w *putFileWriteCloser) Write(p []byte) (int, error) {
	bytesWritten := 0
	for {
		// Buffer the write so that we don't exceed the grpc
		// MaxMsgSize. This value includes the whole payload
		// including headers, so we're conservative and halve it
		ceil := bytesWritten + grpcutil.MaxMsgSize/2
		if ceil > len(p) {
			ceil = len(p)
		}
		actualP := p[bytesWritten:ceil]
		if len(actualP) == 0 {
			break
		}
		w.request.Value = actualP
		if err := w.putFileClient.Send(w.request); err != nil {
			return 0, sanitizeErr(err)
		}
		w.sent = true
		w.request.Value = nil
		// File is only needed on the first request
		w.request.File = nil
		bytesWritten += len(actualP)
	}
	return bytesWritten, nil
}

func (w *putFileWriteCloser) Close() error {
	// we always send at least one request, otherwise it's impossible to create
	// an empty file
	if !w.sent {
		if err := w.putFileClient.Send(w.request); err != nil {
			return err
		}
	}
	_, err := w.putFileClient.CloseAndRecv()
	return sanitizeErr(err)
}

type putObjectWriteCloser struct {
	request         *pfs.PutObjectRequest
	putObjectClient pfs.ObjectAPI_PutObjectClient
	object          *pfs.Object
}

func (c APIClient) newPutObjectWriteCloser(tags ...string) (*putObjectWriteCloser, error) {
	putObjectClient, err := c.ObjectAPIClient.PutObject(c.ctx())
	if err != nil {
		return nil, sanitizeErr(err)
	}
	var _tags []*pfs.Tag
	for _, tag := range tags {
		_tags = append(_tags, &pfs.Tag{Name: tag})
	}
	return &putObjectWriteCloser{
		request: &pfs.PutObjectRequest{
			Tags: _tags,
		},
		putObjectClient: putObjectClient,
	}, nil
}

func (w *putObjectWriteCloser) Write(p []byte) (int, error) {
	bytesWritten := 0
	for {
		// Buffer the write so that we don't exceed the grpc
		// MaxMsgSize. This value includes the whole payload
		// including headers, so we're conservative and halve it
		ceil := bytesWritten + grpcutil.MaxMsgSize/2
		if ceil > len(p) {
			ceil = len(p)
		}
		actualP := p[bytesWritten:ceil]
		if len(actualP) == 0 {
			break
		}
		w.request.Value = actualP
		if err := w.putObjectClient.Send(w.request); err != nil {
			return 0, sanitizeErr(err)
		}
		w.request.Value = nil
		bytesWritten += len(actualP)
	}
	return bytesWritten, nil
}

func (w *putObjectWriteCloser) Close() error {
	var err error
	w.object, err = w.putObjectClient.CloseAndRecv()
	return sanitizeErr(err)
}

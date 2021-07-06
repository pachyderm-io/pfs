package client

import (
	"archive/tar"
	"context"
	"io"
	"io/ioutil"
	"time"

	"github.com/gogo/protobuf/types"
	"github.com/pachyderm/pachyderm/v2/src/internal/errors"
	"github.com/pachyderm/pachyderm/v2/src/internal/errutil"
	"github.com/pachyderm/pachyderm/v2/src/internal/grpcutil"
	"github.com/pachyderm/pachyderm/v2/src/internal/storage/renew"
	"github.com/pachyderm/pachyderm/v2/src/pfs"
)

// PutFile puts a file into PFS from a reader.
func (c APIClient) PutFile(commit *pfs.Commit, path string, r io.Reader, opts ...PutFileOption) error {
	return c.WithModifyFileClient(commit, func(mf ModifyFile) error {
		return mf.PutFile(path, r, opts...)
	})
}

// PutFileTAR puts a set of files into PFS from a tar stream.
func (c APIClient) PutFileTAR(commit *pfs.Commit, r io.Reader, opts ...PutFileOption) error {
	return c.WithModifyFileClient(commit, func(mf ModifyFile) error {
		return mf.PutFileTAR(r, opts...)
	})
}

// PutFileURL puts a file into PFS using the content found at a URL.
// The URL is sent to the server which performs the request.
// recursive allow for recursive scraping of some types of URLs for example on s3:// urls.
func (c APIClient) PutFileURL(commit *pfs.Commit, path, url string, recursive bool, opts ...PutFileOption) error {
	return c.WithModifyFileClient(commit, func(mf ModifyFile) error {
		return mf.PutFileURL(path, url, recursive, opts...)
	})
}

// DeleteFile deletes a file from PFS.
func (c APIClient) DeleteFile(commit *pfs.Commit, path string, opts ...DeleteFileOption) error {
	return c.WithModifyFileClient(commit, func(mf ModifyFile) error {
		return mf.DeleteFile(path, opts...)
	})
}

// CopyFile copies a file from one PFS location to another.
// It can be used on directories or regular files.
func (c APIClient) CopyFile(dstCommit *pfs.Commit, dstPath string, srcCommit *pfs.Commit, srcPath string, opts ...CopyFileOption) error {
	return c.WithModifyFileClient(dstCommit, func(mf ModifyFile) error {
		return mf.CopyFile(dstPath, srcCommit.NewFile(srcPath), opts...)
	})
}

// ModifyFile is used for performing a stream of file modifications.
// The modifications are not persisted until the ModifyFileClient is closed.
// ModifyFileClient is not thread safe. Multiple ModifyFileClients
// should be used for concurrent modifications.
type ModifyFile interface {
	// PutFile puts a file into PFS from a reader.
	PutFile(path string, r io.Reader, opts ...PutFileOption) error
	// PutFileTAR puts a set of files into PFS from a tar stream.
	PutFileTAR(r io.Reader, opts ...PutFileOption) error
	// PutFileURL puts a file into PFS using the content found at a URL.
	// recursive allows for recursive scraping of some types of URLs.
	PutFileURL(path, url string, recursive bool, opts ...PutFileOption) error
	// DeleteFile deletes a file from PFS.
	DeleteFile(path string, opts ...DeleteFileOption) error
	// CopyFile copies a file from src to dst.
	CopyFile(dst string, src *pfs.File, opts ...CopyFileOption) error
}

// WithModifyFileClient creates a new ModifyFileClient that is scoped to the passed in callback.
// TODO: Context should be a parameter, not stored in the pach client.
func (c APIClient) WithModifyFileClient(commit *pfs.Commit, cb func(ModifyFile) error) (retErr error) {
	cancelCtx, cancel := context.WithCancel(c.Ctx())
	defer cancel()
	mfc, err := c.WithCtx(cancelCtx).NewModifyFileClient(commit)
	if err != nil {
		return err
	}
	defer func() {
		if retErr == nil {
			retErr = mfc.Close()
		}
	}()
	return cb(mfc)
}

// NewModifyFileClient creates a new ModifyFileClient.
func (c APIClient) NewModifyFileClient(commit *pfs.Commit) (_ *ModifyFileClient, retErr error) {
	defer func() {
		retErr = grpcutil.ScrubGRPC(retErr)
	}()
	client, err := c.PfsAPIClient.ModifyFile(c.Ctx())
	if err != nil {
		return nil, err
	}
	if err := client.Send(&pfs.ModifyFileRequest{
		Body: &pfs.ModifyFileRequest_SetCommit{SetCommit: commit},
	}); err != nil {
		return nil, err
	}
	return &ModifyFileClient{
		client: client,
		modifyFileCore: modifyFileCore{
			client: client,
		},
	}, nil
}

type ModifyFileClient struct {
	client pfs.API_ModifyFileClient
	modifyFileCore
}

type modifyFileCore struct {
	client interface {
		Send(*pfs.ModifyFileRequest) error
	}
	err error
}

func (mfc *modifyFileCore) PutFile(path string, r io.Reader, opts ...PutFileOption) error {
	config := &putFileConfig{}
	for _, opt := range opts {
		opt(config)
	}
	return mfc.maybeError(func() error {
		if !config.append {
			if err := mfc.sendDeleteFile(&pfs.DeleteFile{
				Path: path,
				Tag:  config.tag,
			}); err != nil {
				return err
			}
		}
		emptyFile := true
		if _, err := grpcutil.ChunkReader(r, func(data []byte) error {
			emptyFile = false
			return mfc.sendPutFile(&pfs.AddFile{
				Path: path,
				Tag:  config.tag,
				Source: &pfs.AddFile_Raw{
					Raw: &types.BytesValue{Value: data},
				},
			})
		}); err != nil {
			return err
		}
		if emptyFile {
			return mfc.sendPutFile(&pfs.AddFile{
				Path: path,
				Tag:  config.tag,
			})
		}
		return nil
	})
}

func (mfc *modifyFileCore) maybeError(f func() error) (retErr error) {
	if mfc.err != nil {
		return mfc.err
	}
	defer func() {
		retErr = grpcutil.ScrubGRPC(retErr)
		if retErr != nil {
			mfc.err = retErr
		}
	}()
	return f()
}

func (mfc *modifyFileCore) sendPutFile(req *pfs.AddFile) error {
	return mfc.client.Send(&pfs.ModifyFileRequest{
		Body: &pfs.ModifyFileRequest_AddFile{
			AddFile: req,
		},
	})
}

func (mfc *modifyFileCore) PutFileTAR(r io.Reader, opts ...PutFileOption) error {
	config := &putFileConfig{}
	for _, opt := range opts {
		opt(config)
	}
	return mfc.maybeError(func() error {
		tr := tar.NewReader(r)
		for hdr, err := tr.Next(); err != io.EOF; hdr, err = tr.Next() {
			if err != nil {
				return err
			}
			if hdr.Typeflag == tar.TypeDir {
				continue
			}
			p := hdr.Name
			if !config.append {
				if err := mfc.sendDeleteFile(&pfs.DeleteFile{
					Path: p,
					Tag:  config.tag,
				}); err != nil {
					return err
				}
			}
			if hdr.Size == 0 {
				if err := mfc.sendPutFile(&pfs.AddFile{
					Path: p,
					Tag:  config.tag,
				}); err != nil {
					return err
				}
			} else {
				if _, err := grpcutil.ChunkReader(tr, func(data []byte) error {
					return mfc.sendPutFile(&pfs.AddFile{
						Path: p,
						Tag:  config.tag,
						Source: &pfs.AddFile_Raw{
							Raw: &types.BytesValue{Value: data},
						},
					})
				}); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func (mfc *modifyFileCore) PutFileURL(path, url string, recursive bool, opts ...PutFileOption) error {
	config := &putFileConfig{}
	for _, opt := range opts {
		opt(config)
	}
	return mfc.maybeError(func() error {
		if !config.append {
			if err := mfc.sendDeleteFile(&pfs.DeleteFile{
				Path: path,
				Tag:  config.tag,
			}); err != nil {
				return err
			}
		}
		pf := &pfs.AddFile{
			Path: path,
			Tag:  config.tag,
			Source: &pfs.AddFile_Url{
				Url: &pfs.AddFile_URLSource{
					URL:       url,
					Recursive: recursive,
				},
			},
		}
		return mfc.sendPutFile(pf)
	})
}

func (mfc *modifyFileCore) DeleteFile(path string, opts ...DeleteFileOption) error {
	return mfc.maybeError(func() error {
		df := &pfs.DeleteFile{Path: path}
		for _, opt := range opts {
			opt(df)
		}
		return mfc.sendDeleteFile(df)
	})
}

func (mfc *modifyFileCore) sendDeleteFile(req *pfs.DeleteFile) error {
	return mfc.client.Send(&pfs.ModifyFileRequest{
		Body: &pfs.ModifyFileRequest_DeleteFile{
			DeleteFile: req,
		},
	})
}

func (mfc *modifyFileCore) CopyFile(dst string, src *pfs.File, opts ...CopyFileOption) error {
	return mfc.maybeError(func() error {
		cf := &pfs.CopyFile{
			Dst: dst,
			Src: src,
		}
		for _, opt := range opts {
			opt(cf)
		}
		return mfc.sendCopyFile(cf)
	})
}

func (mfc *modifyFileCore) sendCopyFile(req *pfs.CopyFile) error {
	return mfc.client.Send(&pfs.ModifyFileRequest{
		Body: &pfs.ModifyFileRequest_CopyFile{
			CopyFile: req,
		},
	})
}

// Close closes the ModifyFileClient.
func (mfc *ModifyFileClient) Close() error {
	return mfc.maybeError(func() error {
		_, err := mfc.client.CloseAndRecv()
		return err
	})
}

// FileSetsRepoName is the repo name used to access filesets as virtual commits.
const FileSetsRepoName = "__filesets__"

// DefaultTTL is the default time-to-live for a temporary fileset.
const DefaultTTL = 10 * time.Minute

// WithRenewer provides a scoped fileset renewer.
func (c APIClient) WithRenewer(cb func(context.Context, *renew.StringSet) error) error {
	rf := func(ctx context.Context, p string, ttl time.Duration) error {
		return c.WithCtx(ctx).RenewFileSet(p, ttl)
	}
	return renew.WithStringSet(c.Ctx(), DefaultTTL, rf, cb)
}

// WithCreateFileSetClient provides a scoped fileset client.
func (c APIClient) WithCreateFileSetClient(cb func(ModifyFile) error) (resp *pfs.CreateFileSetResponse, retErr error) {
	cancelCtx, cancel := context.WithCancel(c.Ctx())
	defer cancel()
	ctfsc, err := c.WithCtx(cancelCtx).NewCreateFileSetClient()
	if err != nil {
		return nil, err
	}
	defer func() {
		if retErr == nil {
			resp, retErr = ctfsc.Close()
		}
	}()
	return nil, cb(ctfsc)
}

// CreateFileSetClient is used to create a temporary fileset.
type CreateFileSetClient struct {
	client pfs.API_CreateFileSetClient
	modifyFileCore
}

// NewCreateFileSetClient returns a CreateFileSetClient instance backed by this client
func (c APIClient) NewCreateFileSetClient() (_ *CreateFileSetClient, retErr error) {
	defer func() {
		retErr = grpcutil.ScrubGRPC(retErr)
	}()
	client, err := c.PfsAPIClient.CreateFileSet(c.Ctx())
	if err != nil {
		return nil, err
	}
	return &CreateFileSetClient{
		client: client,
		modifyFileCore: modifyFileCore{
			client: client,
		},
	}, nil
}

// Close closes the CreateFileSetClient.
func (ctfsc *CreateFileSetClient) Close() (*pfs.CreateFileSetResponse, error) {
	var ret *pfs.CreateFileSetResponse
	if err := ctfsc.maybeError(func() error {
		resp, err := ctfsc.client.CloseAndRecv()
		if err != nil {
			return err
		}
		ret = resp
		return nil
	}); err != nil {
		return nil, err
	}
	return ret, nil
}

// AddFileSet adds a fileset to a commit.
func (c APIClient) AddFileSet(repo, branch, commit, ID string) (retErr error) {
	defer func() {
		retErr = grpcutil.ScrubGRPC(retErr)
	}()
	_, err := c.PfsAPIClient.AddFileSet(
		c.Ctx(),
		&pfs.AddFileSetRequest{
			Commit:    NewCommit(repo, branch, commit),
			FileSetId: ID,
		},
	)
	return err
}

// RenewFileSet renews a fileset.
func (c APIClient) RenewFileSet(ID string, ttl time.Duration) (retErr error) {
	defer func() {
		retErr = grpcutil.ScrubGRPC(retErr)
	}()
	_, err := c.PfsAPIClient.RenewFileSet(
		c.Ctx(),
		&pfs.RenewFileSetRequest{
			FileSetId:  ID,
			TtlSeconds: int64(ttl.Seconds()),
		},
	)
	return err
}

// GetFile returns the contents of a file at a specific Commit.
// offset specifies a number of bytes that should be skipped in the beginning of the file.
// size limits the total amount of data returned, note you will get fewer bytes
// than size if you pass a value larger than the size of the file.
// If size is set to 0 then all of the data will be returned.
func (c APIClient) GetFile(commit *pfs.Commit, path string, w io.Writer) (retErr error) {
	ctx, cf := context.WithCancel(c.Ctx())
	defer cf()
	gfc, err := c.PfsAPIClient.GetFile(ctx, &pfs.GetFileRequest{
		File: &pfs.File{
			Commit: commit,
			Path:   path,
		},
	})
	if err != nil {
		return err
	}
	for m, err := gfc.Recv(); err != io.EOF; m, err = gfc.Recv() {
		if err != nil {
			return err
		}
		if _, err := w.Write(m.Value); err != nil {
			return err
		}
	}
	return nil
}

// GetFileTAR gets a tar file from PFS.
func (c APIClient) GetFileTAR(commit *pfs.Commit, path string) (io.ReadCloser, error) {
	return c.getFileTar(commit, path)
}

func (c APIClient) getFileTar(commit *pfs.Commit, path string) (_ io.ReadCloser, retErr error) {
	defer func() {
		retErr = grpcutil.ScrubGRPC(retErr)
	}()
	req := &pfs.GetFileRequest{
		File: commit.NewFile(path),
	}
	ctx, cf := context.WithCancel(c.Ctx())
	client, err := c.PfsAPIClient.GetFileTAR(ctx, req)
	if err != nil {
		cf()
		return nil, err
	}
	return grpcutil.NewStreamingBytesReader(client, cf), nil
}

// GetFileReader gets a reader for the specified path
// TODO: This should probably be an io.ReadCloser so we can close the rpc if the full file isn't read.
func (c APIClient) GetFileReader(commit *pfs.Commit, path string) (io.Reader, error) {
	r, err := c.getFileTar(commit, path)
	if err != nil {
		return nil, err
	}
	tr := tar.NewReader(r)
	if _, err := tr.Next(); err != nil {
		return nil, err
	}
	return tr, nil
}

// GetFileReadSeeker returns a reader for the contents of a file at a specific
// Commit that permits Seeking to different points in the file.
func (c APIClient) GetFileReadSeeker(commit *pfs.Commit, path string) (io.ReadSeeker, error) {
	fi, err := c.InspectFile(commit, path)
	if err != nil {
		return nil, err
	}
	r, err := c.GetFileReader(commit, path)
	if err != nil {
		return nil, err
	}
	return &getFileReadSeeker{
		Reader: r,
		c:      c,
		file:   commit.NewFile(path),
		offset: 0,
		size:   int64(fi.Details.SizeBytes),
	}, nil
}

type getFileReadSeeker struct {
	io.Reader
	c            APIClient
	file         *pfs.File
	offset, size int64
}

func (gfrs *getFileReadSeeker) Seek(offset int64, whence int) (int64, error) {
	getFileReader := func(offset int64) (io.Reader, error) {
		r, err := gfrs.c.GetFileReader(gfrs.file.Commit, gfrs.file.Path)
		if err != nil {
			return nil, err
		}
		// TODO: Replace with file range request when implemented in PFS.
		if _, err := io.CopyN(ioutil.Discard, r, offset); err != nil {
			return nil, err
		}
		return r, nil
	}
	switch whence {
	case io.SeekStart:
		r, err := getFileReader(offset)
		if err != nil {
			return gfrs.offset, err
		}
		gfrs.offset = offset
		gfrs.Reader = r
	case io.SeekCurrent:
		r, err := getFileReader(gfrs.offset + offset)
		if err != nil {
			return gfrs.offset, err
		}
		gfrs.offset += offset
		gfrs.Reader = r
	case io.SeekEnd:
		r, err := getFileReader(gfrs.size - offset)
		if err != nil {
			return gfrs.offset, err
		}
		gfrs.offset = gfrs.size - offset
		gfrs.Reader = r
	}
	return gfrs.offset, nil
}

// GetFileURL gets the file at the specified URL
func (c APIClient) GetFileURL(commit *pfs.Commit, path, URL string) (retErr error) {
	defer func() {
		retErr = grpcutil.ScrubGRPC(retErr)
	}()
	req := &pfs.GetFileRequest{
		File: commit.NewFile(path),
		URL:  URL,
	}
	client, err := c.PfsAPIClient.GetFileTAR(c.Ctx(), req)
	if err != nil {
		return err
	}
	_, err = io.Copy(ioutil.Discard, grpcutil.NewStreamingBytesReader(client, nil))
	return err
}

// InspectFile returns metadata about the specified file
func (c APIClient) InspectFile(commit *pfs.Commit, path string) (_ *pfs.FileInfo, retErr error) {
	defer func() {
		retErr = grpcutil.ScrubGRPC(retErr)
	}()
	fi, err := c.PfsAPIClient.InspectFile(
		c.Ctx(),
		&pfs.InspectFileRequest{
			File: commit.NewFile(path),
		},
	)
	return fi, err
}

// ListFile returns info about all files in a Commit under path, calling cb with each FileInfo.
func (c APIClient) ListFile(commit *pfs.Commit, path string, cb func(fi *pfs.FileInfo) error) (retErr error) {
	defer func() {
		retErr = grpcutil.ScrubGRPC(retErr)
	}()
	client, err := c.PfsAPIClient.ListFile(
		c.Ctx(),
		&pfs.ListFileRequest{
			File: commit.NewFile(path),
		},
	)
	if err != nil {
		return err
	}
	for {
		fi, err := client.Recv()
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
func (c APIClient) ListFileAll(commit *pfs.Commit, path string) (_ []*pfs.FileInfo, retErr error) {
	defer func() {
		retErr = grpcutil.ScrubGRPC(retErr)
	}()
	var fis []*pfs.FileInfo
	if err := c.ListFile(commit, path, func(fi *pfs.FileInfo) error {
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
func (c APIClient) GlobFile(commit *pfs.Commit, pattern string, cb func(fi *pfs.FileInfo) error) (retErr error) {
	defer func() {
		retErr = grpcutil.ScrubGRPC(retErr)
	}()
	client, err := c.PfsAPIClient.GlobFile(
		c.Ctx(),
		&pfs.GlobFileRequest{
			Commit:  commit,
			Pattern: pattern,
		},
	)
	if err != nil {
		return err
	}
	for {
		fi, err := client.Recv()
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
func (c APIClient) GlobFileAll(commit *pfs.Commit, pattern string) (_ []*pfs.FileInfo, retErr error) {
	defer func() {
		retErr = grpcutil.ScrubGRPC(retErr)
	}()
	var fis []*pfs.FileInfo
	if err := c.GlobFile(commit, pattern, func(fi *pfs.FileInfo) error {
		fis = append(fis, fi)
		return nil
	}); err != nil {
		return nil, err
	}
	return fis, nil
}

// DiffFile returns the differences between 2 paths at 2 commits.
// It streams back one file at a time which is either from the new path, or the old path
func (c APIClient) DiffFile(newCommit *pfs.Commit, newPath string, oldCommit *pfs.Commit, oldPath string, shallow bool, cb func(*pfs.FileInfo, *pfs.FileInfo) error) (retErr error) {
	defer func() {
		retErr = grpcutil.ScrubGRPC(retErr)
	}()
	ctx, cancel := context.WithCancel(c.Ctx())
	defer cancel()
	var oldFile *pfs.File
	if oldCommit != nil {
		oldFile = oldCommit.NewFile(oldPath)
	}
	req := &pfs.DiffFileRequest{
		NewFile: newCommit.NewFile(newPath),
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
func (c APIClient) DiffFileAll(newCommit *pfs.Commit, newPath string, oldCommit *pfs.Commit, oldPath string, shallow bool) (_ []*pfs.FileInfo, _ []*pfs.FileInfo, retErr error) {
	defer func() {
		retErr = grpcutil.ScrubGRPC(retErr)
	}()
	var newFis, oldFis []*pfs.FileInfo
	if err := c.DiffFile(newCommit, newPath, oldCommit, oldPath, shallow, func(newFi, oldFi *pfs.FileInfo) error {
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
func (c APIClient) WalkFile(commit *pfs.Commit, path string, cb func(*pfs.FileInfo) error) (retErr error) {
	client, err := c.PfsAPIClient.WalkFile(
		c.Ctx(),
		&pfs.WalkFileRequest{
			File: commit.NewFile(path),
		})
	if err != nil {
		return err
	}
	for {
		fi, err := client.Recv()
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

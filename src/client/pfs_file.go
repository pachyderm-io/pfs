package client

import (
	"context"
	"io"
	"time"

	"github.com/pachyderm/pachyderm/v2/src/internal/errors"
	"github.com/pachyderm/pachyderm/v2/src/internal/grpcutil"
	"github.com/pachyderm/pachyderm/v2/src/internal/storage/renew"
	"github.com/pachyderm/pachyderm/v2/src/pfs"
)

// AppendFile appends a file.
func (c APIClient) AppendFile(repo, commit, path string, r io.Reader, opts ...AppendFileOption) error {
	mfc, err := c.NewModifyFileClient(repo, commit)
	if err != nil {
		return err
	}
	if err := mfc.AppendFile(path, r, opts...); err != nil {
		return err
	}
	return mfc.Close()
}

// AppendFileTar appends a set of files from a tar stream.
func (c APIClient) AppendFileTar(repo, commit string, r io.Reader, opts ...AppendFileOption) error {
	mfc, err := c.NewModifyFileClient(repo, commit)
	if err != nil {
		return err
	}
	if err := mfc.AppendFileTar(r, opts...); err != nil {
		return err
	}
	return mfc.Close()
}

// AppendFileURL appends a file from a URL.
func (c APIClient) AppendFileURL(repo, commit, path, url string, recursive bool, opts ...AppendFileOption) error {
	mfc, err := c.NewModifyFileClient(repo, commit)
	if err != nil {
		return err
	}
	if err := mfc.AppendFileURL(path, url, recursive, opts...); err != nil {
		return err
	}
	return mfc.Close()
}

// DeleteFile deletes a set of files.
// The optional tag field indicates specific tags in the files to delete.
func (c APIClient) deleteFile(repo, commit, path string, tag ...string) error {
	mfc, err := c.NewModifyFileClient(repo, commit)
	if err != nil {
		return err
	}
	if err := mfc.DeleteFile(path, tag...); err != nil {
		return err
	}
	return mfc.Close()
}

// ModifyFileClient is used for performing a stream of file modifications.
// The modifications are not persisted until the ModifyFileClient is closed.
// ModifyFileClient is not thread safe. Multiple ModifyFileClients
// should be used for concurrent modifications.
type ModifyFileClient struct {
	client pfs.API_ModifyFileClient
	modifyFileCore
}

// WithModifyFileClient creates a new ModifyFileClient that is scoped to the passed in callback.
func (c APIClient) WithModifyFileClient(repo, commit string, cb func(*ModifyFileClient) error) (retErr error) {
	mfc, err := c.NewModifyFileClient(repo, commit)
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
func (c APIClient) NewModifyFileClient(repo, commit string) (_ *ModifyFileClient, retErr error) {
	defer func() {
		retErr = grpcutil.ScrubGRPC(retErr)
	}()
	client, err := c.PfsAPIClient.ModifyFile(c.Ctx())
	if err != nil {
		return nil, err
	}
	if err := client.Send(&pfs.ModifyFileRequest{
		Commit: NewCommit(repo, commit),
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

// Close closes the ModifyFileClient.
func (mfc *ModifyFileClient) Close() error {
	return mfc.maybeError(func() error {
		_, err := mfc.client.CloseAndRecv()
		return err
	})
}

type modifyFileCore struct {
	client interface {
		Send(*pfs.ModifyFileRequest) error
	}
	err error
}

// AppendFile appends a file.
func (mfc *modifyFileCore) AppendFile(path string, r io.Reader, opts ...AppendFileOption) error {
	return mfc.maybeError(func() error {
		af := &pfs.AppendFile{
			Source: &pfs.AppendFile_RawFileSource{
				RawFileSource: &pfs.RawFileSource{
					Path: path,
				},
			},
		}
		for _, opt := range opts {
			opt(af)
		}
		if err := mfc.sendAppendFile(af); err != nil {
			return err
		}
		if _, err := grpcutil.ChunkReader(r, func(data []byte) error {
			return mfc.sendAppendFile(&pfs.AppendFile{
				Source: &pfs.AppendFile_RawFileSource{
					RawFileSource: &pfs.RawFileSource{
						Data: data,
					},
				},
			})
		}); err != nil {
			return err
		}
		return mfc.sendAppendFile(&pfs.AppendFile{
			Source: &pfs.AppendFile_RawFileSource{
				RawFileSource: &pfs.RawFileSource{
					EOF: true,
				},
			},
		})
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

func (mfc *modifyFileCore) sendAppendFile(req *pfs.AppendFile) error {
	return mfc.client.Send(&pfs.ModifyFileRequest{
		Modification: &pfs.ModifyFileRequest_AppendFile{
			AppendFile: req,
		},
	})
}

// AppendFileTar appends a set of files from a tar stream.
func (mfc *modifyFileCore) AppendFileTar(r io.Reader, opts ...AppendFileOption) error {
	return mfc.maybeError(func() error {
		af := &pfs.AppendFile{
			Source: &pfs.AppendFile_TarFileSource{
				TarFileSource: &pfs.TarFileSource{},
			},
		}
		for _, opt := range opts {
			opt(af)
		}
		if err := mfc.sendAppendFile(af); err != nil {
			return err
		}
		_, err := grpcutil.ChunkReader(r, func(data []byte) error {
			return mfc.sendAppendFile(&pfs.AppendFile{
				Source: &pfs.AppendFile_TarFileSource{
					TarFileSource: &pfs.TarFileSource{
						Data: data,
					},
				},
			})
		})
		return err
	})
}

func (mfc *modifyFileCore) AppendFileURL(path, url string, recursive bool, opts ...AppendFileOption) error {
	return mfc.maybeError(func() error {
		af := &pfs.AppendFile{
			Source: &pfs.AppendFile_UrlFileSource{
				UrlFileSource: &pfs.URLFileSource{
					Path:      path,
					URL:       url,
					Recursive: recursive,
				},
			},
		}
		for _, opt := range opts {
			opt(af)
		}
		return mfc.sendAppendFile(af)
	})
}

// DeleteFile deletes a set of files.
// The optional tag field indicates specific tags in the files to delete.
func (mfc *modifyFileCore) DeleteFile(path string, tag ...string) error {
	return mfc.maybeError(func() error {
		req := &pfs.DeleteFile{File: path}
		if len(tag) > 0 {
			if len(tag) > 1 {
				return errors.Errorf("DeleteFile called with %v tags, expected 0 or 1", len(tag))
			}
			req.Tag = tag[0]
		}
		return mfc.sendDeleteFile(req)
	})
}

func (mfc *modifyFileCore) sendDeleteFile(req *pfs.DeleteFile) error {
	return mfc.client.Send(&pfs.ModifyFileRequest{
		Modification: &pfs.ModifyFileRequest_DeleteFile{
			DeleteFile: req,
		},
	})
}

// TmpRepoName is a reserved repo name used for namespacing temporary filesets
const TmpRepoName = "__tmp__"

// TmpFileSetCommit creates a commit which can be used to access the temporary fileset fileSetID
func (c APIClient) TmpFileSetCommit(fileSetID string) *pfs.Commit {
	return &pfs.Commit{
		ID:   fileSetID,
		Repo: &pfs.Repo{Name: TmpRepoName},
	}
}

// DefaultTTL is the default time-to-live for a temporary fileset.
const DefaultTTL = 10 * time.Minute

// WithRenewer provides a scoped fileset renewer.
func (c APIClient) WithRenewer(cb func(context.Context, *renew.StringSet) error) error {
	rf := func(ctx context.Context, p string, ttl time.Duration) error {
		return c.WithCtx(ctx).RenewFileSet(p, ttl)
	}
	return renew.WithStringSet(c.Ctx(), DefaultTTL, rf, cb)
}

// WithCreateFilesetClient provides a scoped fileset client.
func (c APIClient) WithCreateFilesetClient(cb func(*CreateFilesetClient) error) (resp *pfs.CreateFilesetResponse, retErr error) {
	ctfsc, err := c.NewCreateFilesetClient()
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

// CreateFilesetClient is used to create a temporary fileset.
type CreateFilesetClient struct {
	client pfs.API_CreateFilesetClient
	modifyFileCore
}

// NewCreateFilesetClient returns a CreateFilesetClient instance backed by this client
func (c APIClient) NewCreateFilesetClient() (_ *CreateFilesetClient, retErr error) {
	defer func() {
		retErr = grpcutil.ScrubGRPC(retErr)
	}()
	client, err := c.PfsAPIClient.CreateFileset(c.Ctx())
	if err != nil {
		return nil, err
	}
	return &CreateFilesetClient{
		client: client,
		modifyFileCore: modifyFileCore{
			client: client,
		},
	}, nil
}

// Close closes the CreateFilesetClient.
func (ctfsc *CreateFilesetClient) Close() (*pfs.CreateFilesetResponse, error) {
	var ret *pfs.CreateFilesetResponse
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

// RenewFileSet renews a fileset.
func (c APIClient) RenewFileSet(ID string, ttl time.Duration) (retErr error) {
	defer func() {
		retErr = grpcutil.ScrubGRPC(retErr)
	}()
	_, err := c.PfsAPIClient.RenewFileset(
		c.Ctx(),
		&pfs.RenewFilesetRequest{
			FilesetId:  ID,
			TtlSeconds: int64(ttl.Seconds()),
		},
	)
	return err
}

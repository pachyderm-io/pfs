package server

import (
	"io"
	"path"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pachyderm/pachyderm/v2/src/auth"
	"github.com/pachyderm/pachyderm/v2/src/client"
	"github.com/pachyderm/pachyderm/v2/src/internal/errors"
	"github.com/pachyderm/pachyderm/v2/src/internal/storage/fileset"
	"github.com/pachyderm/pachyderm/v2/src/internal/storage/fileset/index"
	"github.com/pachyderm/pachyderm/v2/src/internal/storage/renew"
	txnenv "github.com/pachyderm/pachyderm/v2/src/internal/transactionenv"
	"github.com/pachyderm/pachyderm/v2/src/internal/uuid"
	"github.com/pachyderm/pachyderm/v2/src/pfs"
	authserver "github.com/pachyderm/pachyderm/v2/src/server/auth/server"
	pfsserver "github.com/pachyderm/pachyderm/v2/src/server/pfs"
	"golang.org/x/net/context"
)

func (d *driver) modifyFile(pachClient *client.APIClient, commit *pfs.Commit, cb func(*fileset.UnorderedWriter) error) error {
	ctx := pachClient.Ctx()
	repo := commit.Repo.Name
	var branch string
	if !uuid.IsUUIDWithoutDashes(commit.ID) {
		branch = commit.ID
	}
	commitInfo, err := d.inspectCommit(pachClient, commit, pfs.CommitState_STARTED)
	if err != nil {
		if (!isNotFoundErr(err) && !isNoHeadErr(err)) || branch == "" {
			return err
		}
		return d.oneOffModifyFile(ctx, repo, branch, cb)
	}
	if commitInfo.Finished != nil {
		if branch == "" {
			return pfsserver.ErrCommitFinished{commitInfo.Commit}
		}
		return d.oneOffModifyFile(ctx, repo, branch, cb)
	}
	return d.withCommitWriter(ctx, commitInfo.Commit, cb)
}

// TODO: Cleanup after failure?
func (d *driver) oneOffModifyFile(ctx context.Context, repo, branch string, cb func(*fileset.UnorderedWriter) error) error {
	return d.txnEnv.WithWriteContext(ctx, func(txnCtx *txnenv.TransactionContext) (retErr error) {
		commit, err := d.startCommit(txnCtx, "", client.NewCommit(repo, ""), branch, nil, "")
		if err != nil {
			return err
		}
		defer func() {
			if retErr == nil {
				retErr = d.finishCommit(txnCtx, commit, "")
			}
		}()
		return d.withCommitWriter(txnCtx.ClientContext, commit, cb)
	})
}

// withCommitWriter calls cb with an unordered writer. All data written to cb is added to the commit, or an error is returned.
func (d *driver) withCommitWriter(ctx context.Context, commit *pfs.Commit, cb func(*fileset.UnorderedWriter) error) (retErr error) {
	n := d.getSubFileset()
	subFileSetStr := fileset.SubFileSetStr(n)
	subFileSetPath := path.Join(commit.Repo.Name, commit.ID, subFileSetStr)
	return d.storage.WithRenewer(ctx, defaultTTL, func(ctx context.Context, renewer *renew.StringSet) error {
		id, err := d.withTmpUnorderedWriter(ctx, renewer, false, cb)
		if err != nil {
			return err
		}
		tmpPath := path.Join(tmpRepo, id)
		return d.storage.Copy(ctx, tmpPath, subFileSetPath, 0)
	})
}

func (d *driver) getSubFileset() int64 {
	// TODO subFileSet will need to be incremented through postgres or etcd.
	nonce := atomic.AddUint64(&d.nonce, 1)
	millis := time.Now().UnixNano() / 1e6 // nano -> milli
	return millis + int64(nonce%1e6)
}

func (d *driver) withTmpUnorderedWriter(ctx context.Context, renewer *renew.StringSet, compact bool, cb func(*fileset.UnorderedWriter) error) (string, error) {
	id := uuid.NewWithoutDashes()
	inputPath := path.Join(tmpRepo, id)
	opts := []fileset.UnorderedWriterOption{fileset.WithRenewal(defaultTTL, renewer)}
	defaultTag := fileset.SubFileSetStr(d.getSubFileset())
	uw, err := d.storage.NewUnorderedWriter(ctx, inputPath, defaultTag, opts...)
	if err != nil {
		return "", err
	}
	if err := cb(uw); err != nil {
		return "", err
	}
	if err := uw.Close(); err != nil {
		return "", err
	}
	if compact {
		outputPath := path.Join(tmpRepo, id, fileset.Compacted)
		_, err := d.storage.Compact(ctx, outputPath, []string{inputPath}, defaultTTL)
		if err != nil {
			return "", err
		}
		renewer.Add(outputPath)
	}
	return id, nil
}

func (d *driver) withWriter(pachClient *client.APIClient, commit *pfs.Commit, cb func(string, *fileset.Writer) error) (retErr error) {
	ctx := pachClient.Ctx()
	commitInfo, err := d.inspectCommit(pachClient, commit, pfs.CommitState_STARTED)
	if err != nil {
		return err
	}
	if commitInfo.Finished != nil {
		return pfsserver.ErrCommitFinished{commitInfo.Commit}
	}
	commit = commitInfo.Commit
	n := d.getSubFileset()
	subFileSetStr := fileset.SubFileSetStr(n)
	subFileSetPath := path.Join(commit.Repo.Name, commit.ID, subFileSetStr)
	fsw := d.storage.NewWriter(ctx, subFileSetPath)
	if err := cb(subFileSetStr, fsw); err != nil {
		return err
	}
	return fsw.Close()
}

func (d *driver) copyFile(pachClient *client.APIClient, src *pfs.File, dst *pfs.File, overwrite bool) (retErr error) {
	ctx := pachClient.Ctx()
	srcCommitInfo, err := d.inspectCommit(pachClient, src.Commit, pfs.CommitState_STARTED)
	if err != nil {
		return err
	}
	if srcCommitInfo.Finished == nil {
		return pfsserver.ErrCommitNotFinished{srcCommitInfo.Commit}
	}
	srcCommit := srcCommitInfo.Commit
	dstCommitInfo, err := d.inspectCommit(pachClient, dst.Commit, pfs.CommitState_STARTED)
	if err != nil {
		return err
	}
	if dstCommitInfo.Finished != nil {
		return pfsserver.ErrCommitFinished{dstCommitInfo.Commit}
	}
	dstCommit := dstCommitInfo.Commit
	if overwrite {
		// TODO: after delete merging is sorted out add overwrite support
		return errors.New("overwrite not yet supported")
	}
	srcPath := cleanPath(src.Path)
	dstPath := cleanPath(dst.Path)
	pathTransform := func(x string) string {
		relPath, err := filepath.Rel(srcPath, x)
		if err != nil {
			panic("cannot apply path transform")
		}
		return path.Join(dstPath, relPath)
	}
	fs, err := d.storage.Open(ctx, []string{compactedCommitPath(srcCommit)}, index.WithPrefix(srcPath))
	if err != nil {
		return err
	}
	fs = fileset.NewIndexFilter(fs, func(idx *index.Index) bool {
		return idx.Path == srcPath || strings.HasPrefix(idx.Path, srcPath+"/")
	})
	fs = fileset.NewIndexMapper(fs, func(idx *index.Index) *index.Index {
		idx.Path = pathTransform(idx.Path)
		return idx
	})
	return d.withWriter(pachClient, dstCommit, func(tag string, dst *fileset.Writer) error {
		return fs.Iterate(ctx, func(f fileset.File) error {
			return dst.Append(f.Index().Path, func(fw *fileset.FileWriter) error {
				fw.Append(tag)
				return f.Content(fw)
			})
		})
	})
}

func (d *driver) getFile(pachClient *client.APIClient, commit *pfs.Commit, glob string, w io.Writer) error {
	ctx := pachClient.Ctx()
	commitInfo, err := d.inspectCommit(pachClient, commit, pfs.CommitState_STARTED)
	if err != nil {
		return err
	}
	if commitInfo.Finished == nil {
		return pfsserver.ErrCommitNotFinished{commitInfo.Commit}
	}
	commit = commitInfo.Commit
	indexOpt, mf, err := parseGlob(glob)
	if err != nil {
		return err
	}
	fs, err := d.storage.Open(ctx, []string{compactedCommitPath(commit)}, indexOpt)
	if err != nil {
		return err
	}
	fs = fileset.NewDirInserter(fs)
	var dir string
	filter := fileset.NewIndexFilter(fs, func(idx *index.Index) bool {
		if dir != "" && strings.HasPrefix(idx.Path, dir) {
			return true
		}
		match := mf(idx.Path)
		if match && fileset.IsDir(idx.Path) {
			dir = idx.Path
		}
		return match
	})
	// TODO: remove absolute paths on the way out?
	// nonAbsolute := &fileset.HeaderMapper{
	// 	R: filter,
	// 	F: func(th *tar.Header) *tar.Header {
	// 		th.Name = "." + th.Name
	// 		return th
	// 	},
	// }
	return fileset.WriteTarStream(ctx, w, filter)
}

func (d *driver) inspectFile(pachClient *client.APIClient, file *pfs.File) (*pfs.FileInfo, error) {
	ctx := pachClient.Ctx()
	commitInfo, err := d.inspectCommit(pachClient, file.Commit, pfs.CommitState_STARTED)
	if err != nil {
		return nil, err
	}
	if commitInfo.Finished == nil {
		return nil, pfsserver.ErrCommitNotFinished{commitInfo.Commit}
	}
	commit := commitInfo.Commit
	p := cleanPath(file.Path)
	if p == "/" {
		p = ""
	}
	fs, err := d.storage.Open(ctx, []string{compactedCommitPath(commit)}, index.WithPrefix(p))
	if err != nil {
		return nil, err
	}
	fs = d.storage.NewIndexResolver(fs)
	fs = fileset.NewDirInserter(fs)
	fs = fileset.NewIndexFilter(fs, func(idx *index.Index) bool {
		return idx.Path == p || strings.HasPrefix(idx.Path, p+"/")
	})
	s := NewSource(commit, fs, true)
	var ret *pfs.FileInfo
	s = NewErrOnEmpty(s, &pfsserver.ErrFileNotFound{File: file})
	if err := s.Iterate(ctx, func(fi *pfs.FileInfo, f fileset.File) error {
		p2 := fi.File.Path
		if p2 == p || p2 == p+"/" {
			ret = fi
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return ret, nil
}

func (d *driver) listFile(pachClient *client.APIClient, file *pfs.File, full bool, cb func(*pfs.FileInfo) error) error {
	if _, err := d.inspectCommit(pachClient, file.Commit, pfs.CommitState_FINISHED); err != nil {
		return err
	}
	ctx := pachClient.Ctx()
	commitInfo, err := d.inspectCommit(pachClient, file.Commit, pfs.CommitState_STARTED)
	if err != nil {
		return err
	}
	if commitInfo.Finished == nil {
		return pfsserver.ErrCommitNotFinished{commitInfo.Commit}
	}
	commit := commitInfo.Commit
	name := cleanPath(file.Path)
	fs, err := d.storage.Open(ctx, []string{compactedCommitPath(commit)}, index.WithPrefix(name))
	if err != nil {
		return err
	}
	fs = d.storage.NewIndexResolver(fs)
	fs = fileset.NewDirInserter(fs)
	fs = fileset.NewIndexFilter(fs, func(idx *index.Index) bool {
		// Check for directory match (don't return directory in list)
		if idx.Path == fileset.Clean(name, true) {
			return false
		}
		// Check for file match.
		if idx.Path == name {
			return true
		}
		// Check for sub directory / file match.
		return strings.HasPrefix(idx.Path, fileset.Clean(name, true))
	})
	s := NewSource(commit, fs, true)
	return s.Iterate(ctx, func(fi *pfs.FileInfo, _ fileset.File) error {
		if pathIsChild(name, cleanPath(fi.File.Path)) {
			return cb(fi)
		}
		return nil
	})
}

func (d *driver) walkFile(pachClient *client.APIClient, file *pfs.File, cb func(*pfs.FileInfo) error) (retErr error) {
	ctx := pachClient.Ctx()
	commitInfo, err := d.inspectCommit(pachClient, file.Commit, pfs.CommitState_STARTED)
	if err != nil {
		return err
	}
	if commitInfo.Finished == nil {
		return pfsserver.ErrCommitNotFinished{commitInfo.Commit}
	}
	commit := commitInfo.Commit
	p := cleanPath(file.Path)
	if p == "/" {
		p = ""
	}
	fs, err := d.storage.Open(ctx, []string{compactedCommitPath(commit)}, index.WithPrefix(p))
	if err != nil {
		return err
	}
	fs = fileset.NewDirInserter(fs)
	fs = fileset.NewIndexFilter(fs, func(idx *index.Index) bool {
		return idx.Path == p || strings.HasPrefix(idx.Path, p+"/")
	})
	s := NewSource(commit, fs, false)
	s = NewErrOnEmpty(s, &pfsserver.ErrFileNotFound{File: file})
	return s.Iterate(ctx, func(fi *pfs.FileInfo, f fileset.File) error {
		return cb(fi)
	})
}

func (d *driver) globFile(pachClient *client.APIClient, commit *pfs.Commit, glob string, cb func(*pfs.FileInfo) error) error {
	ctx := pachClient.Ctx()
	commitInfo, err := d.inspectCommit(pachClient, commit, pfs.CommitState_STARTED)
	if err != nil {
		return err
	}
	if commitInfo.Finished == nil {
		return pfsserver.ErrCommitNotFinished{commitInfo.Commit}
	}
	commit = commitInfo.Commit
	indexOpt, mf, err := parseGlob(glob)
	if err != nil {
		return err
	}
	fs, err := d.storage.Open(ctx, []string{compactedCommitPath(commit)}, indexOpt)
	if err != nil {
		return err
	}
	fs = d.storage.NewIndexResolver(fs)
	fs = fileset.NewDirInserter(fs)
	s := NewSource(commit, fs, true)
	return s.Iterate(ctx, func(fi *pfs.FileInfo, f fileset.File) error {
		if !mf(fi.File.Path) {
			return nil
		}
		return cb(fi)
	})
}

func (d *driver) diffFile(pachClient *client.APIClient, oldFile, newFile *pfs.File, cb func(oldFi, newFi *pfs.FileInfo) error) error {
	// TODO: move validation to the Validating API Server
	// Validation
	if newFile == nil {
		return errors.New("file cannot be nil")
	}
	if newFile.Commit == nil {
		return errors.New("file commit cannot be nil")
	}
	if newFile.Commit.Repo == nil {
		return errors.New("file commit repo cannot be nil")
	}
	// Do READER authorization check for both newFile and oldFile
	if oldFile != nil && oldFile.Commit != nil {
		if err := authserver.CheckIsAuthorized(pachClient, oldFile.Commit.Repo, auth.Scope_READER); err != nil {
			return err
		}
	}
	if newFile != nil && newFile.Commit != nil {
		if err := authserver.CheckIsAuthorized(pachClient, newFile.Commit.Repo, auth.Scope_READER); err != nil {
			return err
		}
	}
	newCommitInfo, err := d.inspectCommit(pachClient, newFile.Commit, pfs.CommitState_STARTED)
	if err != nil {
		return err
	}
	if oldFile == nil {
		oldFile = &pfs.File{
			Commit: newCommitInfo.ParentCommit,
			Path:   newFile.Path,
		}
	}
	ctx := pachClient.Ctx()
	oldCommit := oldFile.Commit
	newCommit := newFile.Commit
	oldName := cleanPath(oldFile.Path)
	if oldName == "/" {
		oldName = ""
	}
	newName := cleanPath(newFile.Path)
	if newName == "/" {
		newName = ""
	}
	var old Source = emptySource{}
	if oldCommit != nil {
		fs, err := d.storage.Open(ctx, []string{compactedCommitPath(oldCommit)}, index.WithPrefix(oldName))
		if err != nil {
			return err
		}
		fs = d.storage.NewIndexResolver(fs)
		fs = fileset.NewDirInserter(fs)
		fs = fileset.NewIndexFilter(fs, func(idx *index.Index) bool {
			return idx.Path == oldName || strings.HasPrefix(idx.Path, oldName+"/")
		})
		old = NewSource(oldCommit, fs, true)
	}
	fs, err := d.storage.Open(ctx, []string{compactedCommitPath(newCommit)}, index.WithPrefix(newName))
	if err != nil {
		return err
	}
	fs = d.storage.NewIndexResolver(fs)
	fs = fileset.NewDirInserter(fs)
	fs = fileset.NewIndexFilter(fs, func(idx *index.Index) bool {
		return idx.Path == newName || strings.HasPrefix(idx.Path, newName+"/")
	})
	new := NewSource(newCommit, fs, true)
	diff := NewDiffer(old, new)
	return diff.Iterate(pachClient.Ctx(), cb)
}

// TODO: We shouldn't be operating on a gRPC server in the driver.
func (d *driver) createFileset(server pfs.API_CreateFilesetServer) (string, error) {
	ctx := server.Context()
	var id string
	if err := d.storage.WithRenewer(ctx, defaultTTL, func(ctx context.Context, renewer *renew.StringSet) error {
		var err error
		id, err = d.withTmpUnorderedWriter(ctx, renewer, true, func(uw *fileset.UnorderedWriter) error {
			for {
				req, err := server.Recv()
				if err != nil {
					if errors.Is(err, io.EOF) {
						return nil
					}
					return err
				}
				// TODO: Refactor this switching logic into one place, this is basically the same as ModifyFile in the API server.
				switch mod := req.Modification.(type) {
				case *pfs.ModifyFileRequest_AppendFile:
					var err error
					switch mod.AppendFile.Source.(type) {
					case *pfs.AppendFile_RawFileSource:
						_, err = appendFileRaw(newAppendFunc(uw, mod.AppendFile), server, mod.AppendFile)
					case *pfs.AppendFile_TarFileSource:
						_, err = appendFileTar(newAppendFunc(uw, mod.AppendFile), server, mod.AppendFile)
					case *pfs.AppendFile_UrlFileSource:
						_, err = appendFileURL(server.Context(), newAppendFunc(uw, mod.AppendFile), mod.AppendFile)
					}
					if err != nil {
						return err
					}
				}
			}
		})
		return err
	}); err != nil {
		return "", err
	}
	return id, nil
}

func (d *driver) renewFileset(ctx context.Context, id string, ttl time.Duration) error {
	if ttl < time.Second {
		return errors.Errorf("ttl (%d) must be at least one second", ttl)
	}
	if ttl > maxTTL {
		return errors.Errorf("ttl (%d) exceeds max ttl (%d)", ttl, maxTTL)
	}
	// check that it is the correct length, to prevent malicious renewing of multiple filesets
	// len(hex(uuid)) == 32
	if len(id) != 32 {
		return errors.Errorf("invalid id (%s)", id)
	}
	p := path.Join(tmpRepo, id)
	_, err := d.storage.SetTTL(ctx, p, ttl)
	return err
}

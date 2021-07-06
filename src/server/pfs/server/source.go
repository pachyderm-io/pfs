package server

import (
	"io"
	"path"
	"strings"

	"github.com/pachyderm/pachyderm/v2/src/internal/errors"
	"github.com/pachyderm/pachyderm/v2/src/internal/storage/fileset"
	"github.com/pachyderm/pachyderm/v2/src/internal/storage/fileset/index"
	"github.com/pachyderm/pachyderm/v2/src/pfs"
	"golang.org/x/net/context"
)

// Source iterates over FileInfos generated from a fileset.FileSet
type Source interface {
	// Iterate calls cb for each File in the underlying fileset.FileSet, with a FileInfo computed
	// during iteration, and the File.
	Iterate(ctx context.Context, cb func(*pfs.FileInfo, fileset.File) error) error
}

type source struct {
	commitInfo *pfs.CommitInfo
	fileSet    fileset.FileSet
	details    bool
}

// NewSource creates a Source which emits FileInfos with the information from commit, and the entries return from fileSet.
func NewSource(commitInfo *pfs.CommitInfo, fs fileset.FileSet, opts ...SourceOption) Source {
	sc := &sourceConfig{}
	for _, opt := range opts {
		opt(sc)
	}
	fs = fileset.NewDirInserter(fs)
	if sc.filter != nil {
		fs = sc.filter(fs)
	}
	return &source{
		commitInfo: commitInfo,
		fileSet:    fs,
		details:    sc.details,
	}
}

// Iterate calls cb for each File in the underlying fileset.FileSet, with a FileInfo computed
// during iteration, and the File.
func (s *source) Iterate(ctx context.Context, cb func(*pfs.FileInfo, fileset.File) error) error {
	ctx, cf := context.WithCancel(ctx)
	defer cf()
	iter := fileset.NewIterator(ctx, s.fileSet)
	cache := make(map[string]*pfs.FileInfo_Details)
	return s.fileSet.Iterate(ctx, func(f fileset.File) error {
		idx := f.Index()
		file := s.commitInfo.Commit.NewFile(idx.Path)
		file.Tag = idx.File.Tag
		fi := &pfs.FileInfo{
			File:      file,
			FileType:  pfs.FileType_FILE,
			Committed: s.commitInfo.Finished,
		}
		if fileset.IsDir(idx.Path) {
			fi.FileType = pfs.FileType_DIR
		}
		if s.details {
			fi.Details = &pfs.FileInfo_Details{}
			cachedDetails, ok, err := s.checkFileDetailsCache(ctx, cache, f)
			if err != nil {
				return err
			}
			if ok {
				fi.Details.SizeBytes = cachedDetails.SizeBytes
				fi.Details.Hash = cachedDetails.Hash
			} else {
				computedDetails, err := s.computeFileDetails(ctx, cache, iter, idx.Path)
				if err != nil {
					return err
				}
				fi.Details.SizeBytes = computedDetails.SizeBytes
				fi.Details.Hash = computedDetails.Hash
			}
		}
		// TODO: Figure out how to remove directory infos from cache when they are no longer needed.
		return cb(fi, f)
	})
}

func (s *source) checkFileDetailsCache(ctx context.Context, cache map[string]*pfs.FileInfo_Details, f fileset.File) (*pfs.FileInfo_Details, bool, error) {
	idx := f.Index()
	// Handle a cached directory file info.
	fi, ok := cache[idx.Path]
	if ok {
		return fi, true, nil
	}
	// Handle a regular file info that has already been iterated through
	// when computing the parent directory file info.
	dir, _ := path.Split(idx.Path)
	_, ok = cache[dir]
	if ok {
		details, err := s.computeRegularFileDetails(ctx, f)
		if err != nil {
			return nil, false, err
		}
		return details, true, nil
	}
	return nil, false, nil
}

func (s *source) computeFileDetails(ctx context.Context, cache map[string]*pfs.FileInfo_Details, iter *fileset.Iterator, target string) (*pfs.FileInfo_Details, error) {
	f, err := iter.Next()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, errors.Errorf("stream is done, can't compute hash for %s", target)
		}
		return nil, err
	}
	idx := f.Index()
	if idx.Path != target {
		return nil, errors.Errorf("stream is wrong place to compute hash for %s", target)
	}
	if !fileset.IsDir(idx.Path) {
		return s.computeRegularFileDetails(ctx, f)
	}
	var size uint64
	h := pfs.NewHash()
	for {
		f2, err := iter.Peek()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		idx2 := f2.Index()
		if !strings.HasPrefix(idx2.Path, target) {
			break
		}
		childDetails, err := s.computeFileDetails(ctx, cache, iter, idx2.Path)
		if err != nil {
			return nil, err
		}
		size += childDetails.SizeBytes
		h.Write(childDetails.Hash)
	}
	details := &pfs.FileInfo_Details{
		SizeBytes: size,
		Hash:      h.Sum(nil),
	}
	cache[target] = details
	return details, nil
}

func (s *source) computeRegularFileDetails(ctx context.Context, f fileset.File) (*pfs.FileInfo_Details, error) {
	details := &pfs.FileInfo_Details{
		SizeBytes: uint64(index.SizeBytes(f.Index())),
	}
	var err error
	details.Hash, err = f.Hash()
	if err != nil {
		return nil, err
	}
	return details, nil
}

type errOnEmpty struct {
	source Source
	err    error
}

// NewErrOnEmpty causes iterate to return a not found error if there are no items to iterate over
func NewErrOnEmpty(s Source, err error) Source {
	return &errOnEmpty{source: s, err: err}
}

// Iterate calls cb for each File in the underlying fileset.FileSet, with a FileInfo computed
// during iteration, and the File.
func (s *errOnEmpty) Iterate(ctx context.Context, cb func(*pfs.FileInfo, fileset.File) error) error {
	empty := true
	if err := s.source.Iterate(ctx, func(fi *pfs.FileInfo, f fileset.File) error {
		empty = false
		return cb(fi, f)
	}); err != nil {
		return err
	}
	if empty {
		return s.err
	}
	return nil
}

type emptySource struct{}

func (emptySource) Iterate(ctx context.Context, cb func(*pfs.FileInfo, fileset.File) error) error {
	return nil
}

// singleFile iterates through the source and returns exactly one file or an error.
// If the source contains a directory, then singleFile errors.
// If the source contains more than one file of any type, then singleFile errors
func singleFile(ctx context.Context, src Source) (*pfs.FileInfo, fileset.File, error) {
	var retFinfo *pfs.FileInfo
	var retFile fileset.File
	err := src.Iterate(ctx, func(finfo *pfs.FileInfo, fsFile fileset.File) error {
		if retFinfo != nil {
			return errors.Errorf("matched multiple files")
		}
		if finfo.FileType != pfs.FileType_FILE {
			return errors.Errorf("cannot get non-regular file. Try GetFileTAR for directories")
		}
		retFinfo = finfo
		retFile = fsFile
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	return retFinfo, retFile, nil
}

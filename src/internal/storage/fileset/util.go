package fileset

import (
	"archive/tar"
	"context"
	"io"
	"path"
	"strings"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/pachyderm/pachyderm/v2/src/internal/errutil"
	"github.com/pachyderm/pachyderm/v2/src/internal/pachhash"
	"github.com/pachyderm/pachyderm/v2/src/internal/storage/chunk"
	"github.com/pachyderm/pachyderm/v2/src/internal/storage/fileset/index"
	"github.com/pachyderm/pachyderm/v2/src/internal/storage/track"
	"github.com/pachyderm/pachyderm/v2/src/internal/tarutil"
)

// NewTestStorage constructs a local storage instance scoped to the lifetime of the test
func NewTestStorage(t testing.TB, db *sqlx.DB, tr track.Tracker) *Storage {
	_, chunks := chunk.NewTestStorage(t, db, tr)
	store := NewTestStore(t, db)
	return NewStorage(store, tr, chunks)
}

// CopyFiles copies files from a file set to a file set writer.
func CopyFiles(ctx context.Context, w *Writer, fs FileSet, deletive ...bool) error {
	if len(deletive) > 0 && deletive[0] {
		if err := fs.Iterate(ctx, func(f File) error {
			idx := f.Index()
			return w.Delete(idx.Path, idx.File.Tag)
		}, deletive...); err != nil {
			return err
		}
	}
	return fs.Iterate(ctx, func(f File) error {
		return w.Copy(f, f.Index().File.Tag)
	})
}

// WriteTarEntry writes an tar entry for f to w
func WriteTarEntry(w io.Writer, f File) error {
	idx := f.Index()
	tw := tar.NewWriter(w)
	if err := tw.WriteHeader(tarutil.NewHeader(idx.Path, index.SizeBytes(idx))); err != nil {
		return err
	}
	if err := f.Content(tw); err != nil {
		return err
	}
	return tw.Flush()
}

// WriteTarStream writes an entire tar stream to w
// It will contain an entry for each File in fs
func WriteTarStream(ctx context.Context, w io.Writer, fs FileSet) error {
	if err := fs.Iterate(ctx, func(f File) error {
		return WriteTarEntry(w, f)
	}); err != nil {
		return err
	}
	return tar.NewWriter(w).Close()
}

// Clean cleans a file path.
func Clean(p string, isDir bool) string {
	p = path.Clean(p)
	if p == "." {
		return "/"
	}
	y := "/" + strings.Trim(p, "/")
	if isDir && !IsDir(y) {
		y += "/"
	}
	return y
}

// IsClean checks if a file path is clean.
func IsClean(x string, isDir bool) bool {
	y := Clean(x, isDir)
	return y == x
}

// IsDir determines if a path is for a directory.
func IsDir(p string) bool {
	return strings.HasSuffix(p, "/")
}

// Iterator provides functionality for imperative iteration over a file set.
type Iterator struct {
	peek     File
	fileChan chan File
	errChan  chan error
}

// NewIterator creates a new iterator.
func NewIterator(ctx context.Context, fs FileSet, deletive ...bool) *Iterator {
	fileChan := make(chan File)
	errChan := make(chan error, 1)
	go func() {
		if err := fs.Iterate(ctx, func(f File) error {
			select {
			case fileChan <- f:
				return nil
			case <-ctx.Done():
				return errutil.ErrBreak
			}
		}, deletive...); err != nil {
			errChan <- err
			return
		}
		close(fileChan)
	}()
	return &Iterator{
		fileChan: fileChan,
		errChan:  errChan,
	}
}

// Peek returns the next file without progressing the iterator.
func (i *Iterator) Peek() (File, error) {
	if i.peek != nil {
		return i.peek, nil
	}
	var err error
	i.peek, err = i.Next()
	return i.peek, err
}

// Next returns the next file and progresses the iterator.
func (i *Iterator) Next() (File, error) {
	if i.peek != nil {
		tmp := i.peek
		i.peek = nil
		return tmp, nil
	}
	select {
	case file, more := <-i.fileChan:
		if !more {
			return nil, io.EOF
		}
		return file, nil
	case err := <-i.errChan:
		return nil, err
	}
}

func hashDataRefs(dataRefs []*chunk.DataRef) ([]byte, error) {
	h := pachhash.New()
	for _, dataRef := range dataRefs {
		_, err := h.Write(dataRef.Hash)
		if err != nil {
			return nil, err
		}
	}
	return h.Sum(nil), nil
}

func SizeFromIndex(idx *index.Index) (size int64) {
	for _, dr := range idx.File.DataRefs {
		size += dr.SizeBytes
	}
	return size
}

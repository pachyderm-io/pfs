package fileset

import (
	"context"

	"github.com/pachyderm/pachyderm/v2/src/internal/errors"
	"github.com/pachyderm/pachyderm/v2/src/internal/storage/fileset/index"
)

// NewIndexResolver creates a file set that resolves index entries.
func (s *Storage) NewIndexResolver(fs FileSet) FileSet {
	return &indexResolver{
		s:  s,
		fs: fs,
	}
}

type indexResolver struct {
	s  *Storage
	fs FileSet
}

func (ir *indexResolver) Iterate(ctx context.Context, cb func(File) error, _ ...bool) error {
	iter := NewIterator(ctx, ir.fs)
	w := ir.s.newWriter(ctx, WithNoUpload(), WithIndexCallback(func(idx *index.Index) error {
		f, err := iter.Next()
		if err != nil {
			return err
		}
		if f.Index().Path != idx.Path {
			return errors.Errorf("index resolver paths out of sync")
		}
		return cb(newFileReader(ctx, ir.s.ChunkStorage(), idx))
	}))
	if err := CopyFiles(ctx, w, ir.fs); err != nil {
		return err
	}
	_, err := w.Close() // don't use the id from this, it will be nil because of WithNoUpload()
	return err
}

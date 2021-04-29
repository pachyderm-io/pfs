package fileset

import (
	"context"
	"io"
	"time"

	"github.com/pachyderm/pachyderm/v2/src/internal/errors"
	"github.com/pachyderm/pachyderm/v2/src/internal/storage/fileset/index"
	"github.com/pachyderm/pachyderm/v2/src/internal/storage/renew"
)

// UnorderedWriter allows writing Files, unordered by path, into multiple ordered filesets.
// This may be a full filesystem or a subfilesystem (e.g. datum / datum set / shard).
type UnorderedWriter struct {
	ctx                        context.Context
	storage                    *Storage
	memAvailable, memThreshold int64
	defaultTag                 string
	buffer                     *Buffer
	subFileSet                 int64
	ttl                        time.Duration
	renewer                    *renew.StringSet
	ids                        []ID
	parentID                   *ID
}

func newUnorderedWriter(ctx context.Context, storage *Storage, memThreshold int64, defaultTag string, opts ...UnorderedWriterOption) (*UnorderedWriter, error) {
	if err := storage.filesetSem.Acquire(ctx, 1); err != nil {
		return nil, err
	}
	uw := &UnorderedWriter{
		ctx:          ctx,
		storage:      storage,
		memAvailable: memThreshold,
		memThreshold: memThreshold,
		defaultTag:   defaultTag,
		buffer:       NewBuffer(),
	}
	for _, opt := range opts {
		opt(uw)
	}
	return uw, nil
}

// Put writes the data exposed by 'r' into the path at 'p'
func (uw *UnorderedWriter) Put(p string, appendFile bool, r io.Reader, customTag ...string) (retErr error) {
	// TODO: Validate
	//if err := ppath.ValidatePath(hdr.Name); err != nil {
	//	return nil, err
	//}
	tag := uw.defaultTag
	if len(customTag) > 0 && customTag[0] != "" {
		tag = customTag[0]
	}
	// TODO: Tag overwrite?
	if !appendFile {
		uw.buffer.Delete(p)
	}
	w := uw.buffer.Add(p, tag)
	for {
		n, err := io.CopyN(w, r, uw.memAvailable)
		uw.memAvailable -= n
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if uw.memAvailable == 0 {
			if err := uw.serialize(); err != nil {
				return err
			}
			w = uw.buffer.Add(p, tag)
		}
	}
}

// serialize will be called whenever the in-memory file set is past the memory threshold.
// A new in-memory file set will be created for the following operations.
func (uw *UnorderedWriter) serialize() error {
	if uw.buffer.Empty() {
		return nil
	}
	return uw.withWriter(func(w *Writer) error {
		var prev string
		var fw *FileWriter
		if err := uw.buffer.WalkAdditive(func(p, tag string, r io.Reader) error {
			if p != prev {
				var err error
				fw, err = w.Add(p)
				if err != nil {
					return err
				}
			}
			prev = p
			fw.Add(tag)
			_, err := io.Copy(fw, r)
			return err
		}); err != nil {
			return err
		}
		prev = ""
		var tags []string
		if err := uw.buffer.WalkDeletive(func(p string, tag ...string) error {
			if prev != "" && p != prev {
				if err := w.Delete(prev, tags...); err != nil {
					return err
				}
				tags = nil
			}
			prev = p
			tags = append(tags, tag...)
			return nil
		}); err != nil {
			return err
		}
		if prev != "" {
			return w.Delete(prev, tags...)
		}
		return nil
	})
}

func (uw *UnorderedWriter) withWriter(cb func(*Writer) error) error {
	// Serialize file set.
	var writerOpts []WriterOption
	if uw.ttl > 0 {
		writerOpts = append(writerOpts, WithTTL(uw.ttl))
	}
	w := uw.storage.newWriter(uw.ctx, writerOpts...)
	if err := cb(w); err != nil {
		return err
	}
	id, err := w.Close()
	if err != nil {
		return err
	}
	uw.ids = append(uw.ids, *id)
	if uw.renewer != nil {
		uw.renewer.Add(id.TrackerID())
	}
	// Reset fileset buffer.
	uw.buffer = NewBuffer()
	uw.memAvailable = uw.memThreshold
	uw.subFileSet++
	return nil
}

// Delete deletes a file from the file set.
func (uw *UnorderedWriter) Delete(p string, tags ...string) error {
	p = Clean(p, IsDir(p))
	if len(tags) > 0 && tags[0] == "" {
		tags = nil
	}
	if IsDir(p) {
		uw.buffer.Delete(p)
		var ids []ID
		if uw.parentID != nil {
			ids = []ID{*uw.parentID}
		}
		fs, err := uw.storage.Open(uw.ctx, append(ids, uw.ids...), index.WithPrefix(p))
		if err != nil {
			return err
		}
		return fs.Iterate(uw.ctx, func(f File) error {
			return uw.Delete(f.Index().Path)
		})
	}
	uw.buffer.Delete(p, tags...)
	return nil
}

// Copy copies all files in 'fs' into the fileset backing 'uw'
func (uw *UnorderedWriter) Copy(ctx context.Context, fs FileSet, appendFile bool, customTag ...string) error {
	if err := uw.serialize(); err != nil {
		return err
	}
	tag := uw.defaultTag
	if len(customTag) > 0 && customTag[0] != "" {
		tag = customTag[0]
	}
	return uw.withWriter(func(w *Writer) error {
		return fs.Iterate(ctx, func(f File) error {
			if !appendFile {
				if err := w.Delete(f.Index().Path, tag); err != nil {
					return err
				}
			}
			fw, err := w.Add(f.Index().Path)
			if err != nil {
				return err
			}
			fw.Add(tag)
			return f.Content(fw)
		})
	})
}

// Close closes the writer.
func (uw *UnorderedWriter) Close() (*ID, error) {
	defer uw.storage.filesetSem.Release(1)
	if err := uw.serialize(); err != nil {
		return nil, err
	}
	return uw.storage.newComposite(uw.ctx, &Composite{
		Layers: idsToHex(uw.ids),
	}, uw.ttl)
}

package fileset

import (
	"bytes"
	"context"
	"io"
	"sort"
	"time"

	"github.com/pachyderm/pachyderm/v2/src/internal/errors"
	"github.com/pachyderm/pachyderm/v2/src/internal/storage/renew"
)

type memFile struct {
	path  string
	parts map[string]*memPart
}

type memPart struct {
	tag string
	buf *bytes.Buffer
}

func (mp *memPart) Write(data []byte) (int, error) {
	return mp.buf.Write(data)
}

type memFileSet struct {
	additive map[string]*memFile
	deletive map[string]*memFile
}

func newMemFileSet() *memFileSet {
	return &memFileSet{
		additive: make(map[string]*memFile),
		deletive: make(map[string]*memFile),
	}
}

func (mfs *memFileSet) appendFile(p string, tag string) io.Writer {
	return mfs.createMemPart(p, tag)
}

func (mfs *memFileSet) createMemPart(p string, tag string) *memPart {
	if _, ok := mfs.additive[p]; !ok {
		mfs.additive[p] = &memFile{
			path:  p,
			parts: make(map[string]*memPart),
		}
	}
	mf := mfs.additive[p]
	if _, ok := mf.parts[tag]; !ok {
		mf.parts[tag] = &memPart{
			tag: tag,
			buf: &bytes.Buffer{},
		}
	}
	return mf.parts[tag]
}

func (mfs *memFileSet) deleteFile(p, tag string) {
	if tag == "" {
		delete(mfs.additive, p)
		mfs.deletive[p] = &memFile{path: p}
		return
	}
	if mf, ok := mfs.additive[p]; ok {
		delete(mf.parts, tag)
	}
	if _, ok := mfs.deletive[p]; !ok {
		mfs.deletive[p] = &memFile{
			path:  p,
			parts: make(map[string]*memPart),
		}
	}
	mf := mfs.deletive[p]
	mf.parts[tag] = &memPart{tag: tag}
}

func (mfs *memFileSet) serialize(w *Writer) error {
	if err := mfs.serializeAdditive(w); err != nil {
		return err
	}
	return mfs.serializeDeletive(w)
}

func (mfs *memFileSet) serializeAdditive(w *Writer) error {
	for _, mf := range sortMemFiles(mfs.additive) {
		if err := w.Append(mf.path, func(fw *FileWriter) error {
			return serializeParts(fw, mf)
		}); err != nil {
			return err
		}
	}
	return nil
}

func serializeParts(fw *FileWriter, mf *memFile) error {
	for _, mp := range sortMemParts(mf.parts) {
		fw.Append(mp.tag)
		if _, err := fw.Write(mp.buf.Bytes()); err != nil {
			return err
		}
	}
	return nil
}

func (mfs *memFileSet) serializeDeletive(w *Writer) error {
	for _, mf := range sortMemFiles(mfs.deletive) {
		var tags []string
		for _, mp := range sortMemParts(mf.parts) {
			tags = append(tags, mp.tag)
		}
		w.Delete(mf.path, tags...)
	}
	return nil
}

func sortMemFiles(mfs map[string]*memFile) []*memFile {
	var result []*memFile
	for _, mf := range mfs {
		result = append(result, mf)
	}
	sort.SliceStable(result, func(i, j int) bool {
		return result[i].path < result[j].path
	})
	return result
}

func sortMemParts(mps map[string]*memPart) []*memPart {
	var result []*memPart
	for _, mp := range mps {
		result = append(result, mp)
	}
	sort.SliceStable(result, func(i, j int) bool {
		return result[i].tag < result[j].tag
	})
	return result
}

// UnorderedWriter allows writing Files, unordered by path, into multiple ordered filesets.
// This may be a full filesystem or a subfilesystem (e.g. datum / datum set / shard).
type UnorderedWriter struct {
	ctx                        context.Context
	storage                    *Storage
	memAvailable, memThreshold int64
	defaultTag                 string
	memFileSet                 *memFileSet
	subFileSet                 int64
	ttl                        time.Duration
	renewer                    *renew.StringSet
	layers                     []ID
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
		memFileSet:   newMemFileSet(),
	}
	for _, opt := range opts {
		opt(uw)
	}
	return uw, nil
}

// Append appends a file to the file set.
func (uw *UnorderedWriter) Append(p string, overwrite bool, r io.Reader, customTag ...string) error {
	// TODO: Validate
	//if err := ppath.ValidatePath(hdr.Name); err != nil {
	//	return nil, err
	//}
	p = Clean(p, false)
	tag := uw.defaultTag
	if len(customTag) > 0 && customTag[0] != "" {
		tag = customTag[0]
	}
	// TODO: Tag overwrite?
	if overwrite {
		uw.memFileSet.deleteFile(p, "")
	}
	w := uw.memFileSet.appendFile(p, tag)
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
			w = uw.memFileSet.appendFile(p, tag)
		}
	}
}

// Delete deletes a file from the file set.
// TODO: Directory deletion needs more invariant checks.
// Right now you have to specify the trailing slash explicitly.
func (uw *UnorderedWriter) Delete(name string, tags ...string) {
	name = Clean(name, IsDir(name))
	var tag string
	if len(tag) > 0 {
		tag = tags[0]
	}
	uw.memFileSet.deleteFile(name, tag)
}

// serialize will be called whenever the in-memory file set is past the memory threshold.
// A new in-memory file set will be created for the following operations.
func (uw *UnorderedWriter) serialize() error {
	// Serialize file set.
	var writerOpts []WriterOption
	if uw.ttl > 0 {
		writerOpts = append(writerOpts, WithTTL(uw.ttl))
	}
	w := uw.storage.newWriter(uw.ctx, writerOpts...)
	if err := uw.memFileSet.serialize(w); err != nil {
		return err
	}
	id, err := w.Close()
	if err != nil {
		return err
	}
	uw.layers = append(uw.layers, *id)
	if uw.renewer != nil {
		uw.renewer.Add(id.TrackerID())
	}
	// Reset in-memory file set.
	uw.memFileSet = newMemFileSet()
	uw.memAvailable = uw.memThreshold
	uw.subFileSet++
	return nil
}

// Close closes the writer.
func (uw *UnorderedWriter) Close() (*ID, error) {
	defer uw.storage.filesetSem.Release(1)
	if err := uw.serialize(); err != nil {
		return nil, err
	}
	return uw.storage.newComposite(uw.ctx, &Composite{
		Layers: idsToHex(uw.layers),
	}, uw.ttl)
}

package fileset

import (
	"context"
	"io"
	"strings"

	"github.com/pachyderm/pachyderm/v2/src/internal/storage/chunk"
	"github.com/pachyderm/pachyderm/v2/src/internal/storage/fileset/index"
	"github.com/pachyderm/pachyderm/v2/src/internal/stream"
)

// MergeReader is an abstraction for reading merged filesets.
// A file's content is ordered based on the lexicographical order of
// the tagged content, so the output file content is produced by
// performing a merge of the tagged content.
type MergeReader struct {
	chunks   *chunk.Storage
	fileSets []FileSet
}

func newMergeReader(chunks *chunk.Storage, fileSets []FileSet) *MergeReader {
	return &MergeReader{
		chunks:   chunks,
		fileSets: fileSets,
	}
}

// Iterate iterates over the files in the merge reader.
func (mr *MergeReader) Iterate(ctx context.Context, cb func(File) error, deletive ...bool) error {
	if len(deletive) > 0 && deletive[0] {
		return mr.iterateDeletive(ctx, cb)
	}
	return mr.iterate(ctx, cb)
}

func (mr *MergeReader) iterate(ctx context.Context, cb func(File) error) error {
	var ss []stream.Stream
	for _, fs := range mr.fileSets {
		ss = append(ss, &fileStream{
			iterator: NewIterator(ctx, fs, true),
			deletive: true,
		})
		ss = append(ss, &fileStream{
			iterator: NewIterator(ctx, fs),
		})
	}
	pq := stream.NewPriorityQueue(ss, compare)
	return pq.Iterate(func(ss []stream.Stream) error {
		var fss []*fileStream
		for _, s := range ss {
			fs := s.(*fileStream)
			if fs.deletive {
				fss = nil
				continue
			}
			fss = append(fss, fs)
		}
		if len(fss) == 0 {
			return nil
		}
		if len(fss) == 1 {
			return cb(newFileReader(ctx, mr.chunks, fss[0].file.Index()))
		}
		var dataRefs []*chunk.DataRef
		for _, fs := range fss {
			idx := fs.file.Index()
			dataRefs = append(dataRefs, idx.File.DataRefs...)
		}
		mergeIdx := fss[0].file.Index()
		mergeIdx.File.DataRefs = dataRefs
		return cb(newMergeFileReader(ctx, mr.chunks, mergeIdx))

	})
}

func (mr *MergeReader) iterateDeletive(ctx context.Context, cb func(File) error) error {
	var ss []stream.Stream
	for _, fs := range mr.fileSets {
		ss = append(ss, &fileStream{
			iterator: NewIterator(ctx, fs, true),
		})
	}
	pq := stream.NewPriorityQueue(ss, compare)
	return pq.Iterate(func(ss []stream.Stream) error {
		fs := ss[0].(*fileStream)
		return cb(newFileReader(ctx, mr.chunks, fs.file.Index()))
	})
}

// MergeFileReader is an abstraction for reading a merged file.
type MergeFileReader struct {
	ctx    context.Context
	chunks *chunk.Storage
	idx    *index.Index
}

func newMergeFileReader(ctx context.Context, chunks *chunk.Storage, idx *index.Index) *MergeFileReader {
	return &MergeFileReader{
		ctx:    ctx,
		chunks: chunks,
		idx:    idx,
	}
}

// Index returns the index for the merged file.
// TODO: Removed clone because it had a significant performance impact for small files.
// May want to revisit.
func (mfr *MergeFileReader) Index() *index.Index {
	return mfr.idx
}

// Content returns the content of the merged file.
func (mfr *MergeFileReader) Content(w io.Writer) error {
	r := mfr.chunks.NewReader(mfr.ctx, mfr.idx.File.DataRefs)
	return r.Get(w)
}

// Hash returns the hash of the file.
func (mfr *MergeFileReader) Hash() ([]byte, error) {
	var resolvedDataRefs []*chunk.DataRef
	cw := mfr.chunks.NewWriter(mfr.ctx, "resolve-writer", func(annotations []*chunk.Annotation) error {
		if annotations[0].NextDataRef != nil {
			resolvedDataRefs = append(resolvedDataRefs, annotations[0].NextDataRef)
		}
		return nil
	}, chunk.WithNoUpload())
	cw.Annotate(&chunk.Annotation{})
	for _, dataRef := range mfr.idx.File.DataRefs {
		if err := cw.Copy(dataRef); err != nil {
			return nil, err
		}
	}
	if err := cw.Close(); err != nil {
		return nil, err
	}
	return hashDataRefs(resolvedDataRefs)
}

type fileStream struct {
	iterator *Iterator
	file     File
	deletive bool
}

func (fs *fileStream) Next() error {
	var err error
	fs.file, err = fs.iterator.Next()
	return err
}

func compare(s1, s2 stream.Stream) int {
	idx1 := s1.(*fileStream).file.Index()
	idx2 := s2.(*fileStream).file.Index()
	if idx1.Path == idx2.Path {
		return strings.Compare(idx1.File.Tag, idx2.File.Tag)
	}
	return strings.Compare(idx1.Path, idx2.Path)
}

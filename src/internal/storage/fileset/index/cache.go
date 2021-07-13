package index

import (
	"bytes"
	"context"
	"io"
	"sort"
	"sync"

	"github.com/hashicorp/golang-lru/simplelru"
	"github.com/pachyderm/pachyderm/v2/src/internal/pbutil"
	"github.com/pachyderm/pachyderm/v2/src/internal/storage/chunk"
)

type Cache interface {
	Get(ctx context.Context, chunkRef *chunk.DataRef, filter *pathFilter, w io.Writer) error
}

type cache struct {
	storage *chunk.Storage
	cache   *simplelru.LRU
	mu      sync.Mutex
}

func NewCache(storage *chunk.Storage, size int) Cache {
	lruCache, err := simplelru.NewLRU(size, nil)
	if err != nil {
		panic(err)
	}
	return &cache{
		storage: storage,
		cache:   lruCache,
	}
}

func (c *cache) Get(ctx context.Context, chunkRef *chunk.DataRef, filter *pathFilter, w io.Writer) error {
	c.mu.Lock()
	v, ok := c.cache.Get(string(chunkRef.Ref.Id))
	c.mu.Unlock()
	if ok {
		return get(v.(*cachedChunk), filter, w)
	}
	cr := c.storage.NewReader(ctx, []*chunk.DataRef{chunkRef})
	buf := &bytes.Buffer{}
	if err := cr.Get(buf); err != nil {
		return err
	}
	cachedChunk, err := c.computeCachedChunk(buf.Bytes())
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.cache.Add(string(chunkRef.Ref.Id), cachedChunk)
	c.mu.Unlock()
	return get(cachedChunk, filter, w)
}

type cachedChunk struct {
	data        []byte
	pathOffsets []*pathOffset
}

type pathOffset struct {
	lower, upper string
	offset       int
}

func (c *cache) computeCachedChunk(data []byte) (*cachedChunk, error) {
	br := bytes.NewReader(data)
	pbr := pbutil.NewReader(br)
	var pathOffsets []*pathOffset
	for {
		pathOffset := &pathOffset{
			offset: len(data) - br.Len(),
		}
		idx := &Index{}
		if err := pbr.Read(idx); err != nil {
			break
		}
		pathOffset.lower = idx.Path
		if idx.Range != nil {
			pathOffset.upper = idx.Range.LastPath
		}
		pathOffsets = append(pathOffsets, pathOffset)
	}
	return &cachedChunk{
		data:        data,
		pathOffsets: pathOffsets,
	}, nil
}

func get(cachedChunk *cachedChunk, filter *pathFilter, w io.Writer) error {
	if len(cachedChunk.pathOffsets) == 0 {
		_, err := w.Write(cachedChunk.data)
		return err
	}
	i := sort.Search(len(cachedChunk.pathOffsets), func(i int) bool {
		return atStart(cachedChunk.pathOffsets[i].lower, filter) || atStart(cachedChunk.pathOffsets[i].upper, filter)
	})
	if i >= len(cachedChunk.pathOffsets) {
		_, err := w.Write(cachedChunk.data[cachedChunk.pathOffsets[i-1].offset:])
		return err
	}
	_, err := w.Write(cachedChunk.data[cachedChunk.pathOffsets[i].offset:])
	return err
}

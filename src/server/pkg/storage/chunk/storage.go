package chunk

import (
	"context"
	"path"

	"github.com/pachyderm/pachyderm/src/client/pkg/errors"
	"github.com/pachyderm/pachyderm/src/server/pkg/obj"
	"github.com/pachyderm/pachyderm/src/server/pkg/storage/gc"
)

const (
	prefix = "chunks"
)

// Storage is the abstraction that manages chunk storage.
type Storage struct {
	objClient obj.Client
	gcClient  gc.Client
}

// NewStorage creates a new Storage.
func NewStorage(objClient obj.Client, opts ...StorageOption) *Storage {
	s := &Storage{
		objClient: objClient,
	}
	s.gcClient = gc.NewMockClient()
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// NewReader creates a new Reader.
func (s *Storage) NewReader(ctx context.Context, dataRefs ...*DataRef) *Reader {
	return newReader(ctx, s.objClient, dataRefs...)
}

// NewWriter creates a new Writer for a stream of bytes to be chunked.
// Chunks are created based on the content, then hashed and deduplicated/uploaded to
// object storage.
func (s *Storage) NewWriter(ctx context.Context, tmpID string, f WriterFunc, opts ...WriterOption) *Writer {
	return newWriter(ctx, s.objClient, s.gcClient, tmpID, f, opts...)
}

// List lists all of the chunks in object storage.
func (s *Storage) List(ctx context.Context, f func(string) error) error {
	var innerErr error
	err := s.objClient.Walk(ctx, prefix, func(x string) error {
		innerErr = f(x)
		return innerErr
	})
	if err != innerErr {
		err = errors.Wrapf(err, "error during walk")
	}
	return err
}

// DeleteAll deletes all of the chunks in object storage.
func (s *Storage) DeleteAll(ctx context.Context) error {
	return s.objClient.Walk(ctx, prefix, func(hash string) error {
		return s.Delete(ctx, hash)
	})
}

// Delete deletes a chunk in object storage.
func (s *Storage) Delete(ctx context.Context, hash string) error {
	err := s.objClient.Delete(ctx, path.Join(prefix, hash))
	return errors.Wrapf(err, "error deleting")
}

// CreateSemanticReference creates a semantic reference to a chunk.
func (s *Storage) CreateSemanticReference(ctx context.Context, name string, chunk *Chunk) error {
	return s.gcClient.CreateReference(ctx, semanticReference(name, chunk.Hash))
}

// DeleteSemanticReference deletes a semantic reference.
func (s *Storage) DeleteSemanticReference(ctx context.Context, name string) error {
	return s.gcClient.DeleteReference(ctx, semanticReference(name, ""))
}

func semanticReference(name, chunk string) *gc.Reference {
	return &gc.Reference{
		Sourcetype: "semantic",
		Source:     name,
		Chunk:      path.Join(prefix, chunk),
	}
}

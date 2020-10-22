package chunk

import (
	"context"
	"crypto/sha512"
	"encoding/hex"

	"github.com/jmoiron/sqlx"
)

type ChunkID []byte

func Hash(data []byte) ChunkID {
	h := sha512.New()
	h.Write(data)
	return h.Sum(nil)[:32]
}

func ChunkIDFromHex(h string) (ChunkID, error) {
	return hex.DecodeString(h)
}

func (id ChunkID) HexString() string {
	return hex.EncodeToString(id)
}

type ChunkMetadata struct {
	Size     int
	PointsTo []ChunkID
}

type MetadataStore interface {
	// SetChunkInfo adds chunk metadata to the tracker
	SetChunkMetadata(ctx context.Context, chunkID ChunkID, md ChunkMetadata) error
	// GetChunkInfo returns info about the chunk if it exists
	GetChunkMetadata(ctx context.Context, chunkID ChunkID) (*ChunkMetadata, error)
	// DeleteChunkInfo removes chunk metadata from the tracker
	DeleteChunkMetadata(ctx context.Context, chunkID ChunkID) error
}

var _ MetadataStore = &PGStore{}

type PGStore struct {
	db *sqlx.DB
}

func (s *PGStore) SetChunkMetadata(ctx context.Context, chunkID ChunkID, md ChunkMetadata) error {
	panic("not implemented")
}

func (s *PGStore) GetChunkMetadata(ctx context.Context, chunkID ChunkID) (*ChunkMetadata, error) {
	panic("not implemented")
}

func (s *PGStore) DeleteChunkMetadata(ctx context.Context, chunkID ChunkID) error {
	panic("not implemented")
}

const schema = `
	CREATE SCHEMA IF NOT EXISTS storage;

	CREATE TABLE storage.chunks (
		int_id BIGSERIAL PRIMARY KEY,
		hash_id BYTEA NOT NULL UNIQUE,
		size INT8 NOT NULL,
		created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
	);	
`
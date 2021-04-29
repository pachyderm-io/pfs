package chunk

import (
	"context"
	"encoding/hex"

	"github.com/jmoiron/sqlx"
	"github.com/pachyderm/pachyderm/v2/src/internal/errors"
	"github.com/pachyderm/pachyderm/v2/src/internal/pachhash"
)

// ID uniquely identifies a chunk. It is the hash of its content
type ID []byte

// Hash produces an ID by hashing data
func Hash(data []byte) ID {
	sum := pachhash.Sum(data)
	return sum[:]
}

// IDFromHex parses a hex string into an ID
func IDFromHex(h string) (ID, error) {
	return hex.DecodeString(h)
}

// HexString hex encodes the ID
func (id ID) HexString() string {
	return hex.EncodeToString(id)
}

// TrackerID returns an ID for use with the tracker.
func (id ID) TrackerID() string {
	return TrackerPrefix + id.HexString()
}

// Metadata holds metadata about a chunk
type Metadata struct {
	Size     int
	PointsTo []ID
}

var (
	// ErrChunkNotExists chunk does not exist
	ErrChunkNotExists = errors.Errorf("chunk does not exist")
)

// Entry is an chunk object mapping
type Entry struct {
	ChunkID   ID     `db:"chunk_id"`
	Gen       uint64 `db:"gen"`
	Uploaded  bool   `db:"uploaded"`
	Tombstone bool   `db:"tombstone"`
}

// SetupPostgresStoreV0 sets up tables in db
func SetupPostgresStoreV0(tx *sqlx.Tx) error {
	_, err := tx.Exec(`
	CREATE TABLE storage.chunk_objects (
		chunk_id BYTEA NOT NULL,
		gen BIGSERIAL NOT NULL,
		uploaded BOOLEAN NOT NULL DEFAULT FALSE,
		tombstone BOOLEAN NOT NULL DEFAULT FALSE,
		size INT8 NOT NULL,
		created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

		PRIMARY KEY(chunk_id, gen)
	);

	CREATE TABLE storage.keys (
		name VARCHAR(128) NOT NULL,
		data BYTEA NOT NULL,

		PRIMARY KEY(name)
	)
	`)
	return err
}

// KeyStore is a store for named secret keys
type KeyStore interface {
	Create(ctx context.Context, name string, data []byte) error
	Get(ctx context.Context, name string) ([]byte, error)
}

type postgresKeyStore struct {
	db *sqlx.DB
}

// NewPostgresKeyStore constructs a Postgres-backed KeyStore
func NewPostgresKeyStore(db *sqlx.DB) KeyStore {
	return &postgresKeyStore{
		db: db,
	}
}

func (s *postgresKeyStore) Create(ctx context.Context, name string, data []byte) error {
	_, err := s.db.ExecContext(ctx, `
	INSERT INTO storage.keys (name, data) VALUES ($1, $2)
	`, name, data)
	return err
}

func (s *postgresKeyStore) Get(ctx context.Context, name string) ([]byte, error) {
	var data []byte
	if err := s.db.GetContext(ctx, &data, `SELECT data FROM storage.keys WHERE name = $1 LIMIT 1`, name); err != nil {
		return nil, err
	}
	return data, nil
}

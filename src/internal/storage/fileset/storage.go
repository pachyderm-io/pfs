package fileset

import (
	"context"
	"math"
	"strings"
	"time"

	units "github.com/docker/go-units"
	"github.com/jmoiron/sqlx"
	"github.com/pachyderm/pachyderm/v2/src/internal/dbutil"
	"github.com/pachyderm/pachyderm/v2/src/internal/errors"
	"github.com/pachyderm/pachyderm/v2/src/internal/storage/chunk"
	"github.com/pachyderm/pachyderm/v2/src/internal/storage/fileset/index"
	"github.com/pachyderm/pachyderm/v2/src/internal/storage/renew"
	"github.com/pachyderm/pachyderm/v2/src/internal/storage/track"
	"golang.org/x/sync/semaphore"
)

const (
	// DefaultMemoryThreshold is the default for the memory threshold that must
	// be met before a file set part is serialized (excluding close).
	DefaultMemoryThreshold = 1024 * units.MB
	// DefaultShardThreshold is the default for the size threshold that must
	// be met before a shard is created by the shard function.
	DefaultShardThreshold = 1024 * units.MB
	// DefaultCompactionFixedDelay is the default fixed delay for compaction.
	// This is expressed as the number of primitive filesets.
	DefaultCompactionFixedDelay = 10
	// DefaultCompactionLevelFactor is the default factor that level sizes increase by in a compacted fileset.
	DefaultCompactionLevelFactor = 10

	// TrackerPrefix is used for creating tracker objects for filesets
	TrackerPrefix = "fileset/"
)

var (
	// ErrNoFileSetFound is returned by the methods on Storage when a fileset does not exist
	ErrNoFileSetFound = errors.Errorf("no fileset found")
)

// Storage is the abstraction that manages fileset storage.
type Storage struct {
	tracker                      track.Tracker
	store                        MetadataStore
	chunks                       *chunk.Storage
	memThreshold, shardThreshold int64
	compactionConfig             *CompactionConfig
	filesetSem                   *semaphore.Weighted
}

// CompactionConfig configures pachd's compaction behavior
type CompactionConfig struct {
	// FixedDelay is the minimum number of primitive FileSets that are allowed to
	// accumulate in a Composite FileSet before the Composite FileSet is
	// compacted.
	FixedDelay int64

	// LevelFactor is minimum ratio that is permitted to occur between each layer
	// of a composite FileSet and its child before compaction is triggered (if
	// size(layer N)/size(layer N+1) < factor, then compaction is triggered).
	LevelFactor int64
}

// NewStorage creates a new Storage.
func NewStorage(mds MetadataStore, tr track.Tracker, chunks *chunk.Storage, opts ...StorageOption) *Storage {
	s := &Storage{
		store:          mds,
		tracker:        tr,
		chunks:         chunks,
		memThreshold:   DefaultMemoryThreshold,
		shardThreshold: DefaultShardThreshold,
		compactionConfig: &CompactionConfig{
			FixedDelay:  DefaultCompactionFixedDelay,
			LevelFactor: DefaultCompactionLevelFactor,
		},
		filesetSem: semaphore.NewWeighted(math.MaxInt64),
	}
	for _, opt := range opts {
		opt(s)
	}
	if s.compactionConfig.LevelFactor < 1 {
		panic("level factor cannot be < 1")
	}
	return s
}

// ChunkStorage returns the underlying chunk storage instance for this storage instance.
func (s *Storage) ChunkStorage() *chunk.Storage {
	return s.chunks
}

// NewUnorderedWriter creates a new unordered file set writer.
func (s *Storage) NewUnorderedWriter(ctx context.Context, defaultTag string, opts ...UnorderedWriterOption) (*UnorderedWriter, error) {
	return newUnorderedWriter(ctx, s, s.memThreshold, defaultTag, opts...)
}

// NewWriter creates a new file set writer.
func (s *Storage) NewWriter(ctx context.Context, opts ...WriterOption) *Writer {
	return s.newWriter(ctx, opts...)
}

func (s *Storage) newWriter(ctx context.Context, opts ...WriterOption) *Writer {
	return newWriter(ctx, s, s.tracker, s.chunks, opts...)
}

// TODO: Expose some notion of read ahead (read a certain number of chunks in parallel).
// this will be necessary to speed up reading large files.
func (s *Storage) newReader(fileSet ID, opts ...index.Option) *Reader {
	return newReader(s.store, s.chunks, fileSet, opts...)
}

// Open opens a file set for reading.
// TODO: It might make sense to have some of the file set transforms as functional options here.
func (s *Storage) Open(ctx context.Context, ids []ID, opts ...index.Option) (FileSet, error) {
	var err error
	ids, err = s.Flatten(ctx, ids)
	if err != nil {
		return nil, err
	}
	var fss []FileSet
	for _, id := range ids {
		fss = append(fss, s.newReader(id, opts...))
	}
	if len(fss) == 0 {
		return emptyFileSet{}, nil
	}
	if len(fss) == 1 {
		return fss[0], nil
	}
	return newMergeReader(s.chunks, fss), nil
}

// Compose produces a composite fileset from the filesets under ids.
// It does not perform a merge or check that the filesets at ids in any way
// other than ensuring that they exist.
func (s *Storage) Compose(ctx context.Context, ids []ID, ttl time.Duration) (*ID, error) {
	c := &Composite{
		Layers: idsToHex(ids),
	}
	return s.newComposite(ctx, c, ttl)
}

// Clone creates a new fileset, identical to the fileset at id, but with the specified ttl.
// The ttl can be ignored by using track.NoTTL
func (s *Storage) Clone(ctx context.Context, id ID, ttl time.Duration) (*ID, error) {
	md, err := s.store.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	switch x := md.Value.(type) {
	case *Metadata_Primitive:
		return s.newPrimitive(ctx, x.Primitive, ttl)
	case *Metadata_Composite:
		return s.newComposite(ctx, x.Composite, ttl)
	default:
		return nil, errors.Errorf("cannot clone type %T", md.Value)
	}
}

// Flatten takes a list of IDs and replaces references to composite FileSets
// with references to all their layers inplace.
// The returned IDs will only contain ids of Primitive FileSets
func (s *Storage) Flatten(ctx context.Context, ids []ID) ([]ID, error) {
	flattened := make([]ID, 0, len(ids))
	for _, id := range ids {
		md, err := s.store.Get(ctx, id)
		if err != nil {
			return nil, err
		}
		switch x := md.Value.(type) {
		case *Metadata_Primitive:
			flattened = append(flattened, id)
		case *Metadata_Composite:
			ids, err := x.Composite.PointsTo()
			if err != nil {
				return nil, err
			}
			ids2, err := s.Flatten(ctx, ids)
			if err != nil {
				return nil, err
			}
			flattened = append(flattened, ids2...)
		default:
			// TODO: should it be?
			return nil, errors.Errorf("Flatten is not defined for empty filesets")
		}
	}
	return flattened, nil
}

func (s *Storage) flattenPrimitives(ctx context.Context, ids []ID) ([]*Primitive, error) {
	ids, err := s.Flatten(ctx, ids)
	if err != nil {
		return nil, err
	}
	return s.getPrimitives(ctx, ids)
}

func (s *Storage) getPrimitives(ctx context.Context, ids []ID) ([]*Primitive, error) {
	var prims []*Primitive
	for _, id := range ids {
		prim, err := s.getPrimitive(ctx, id)
		if err != nil {
			return nil, err
		}
		prims = append(prims, prim)
	}
	return prims, nil
}

// Concat is a special case of Merge, where the filesets each contain paths for distinct ranges.
// The path ranges must be non-overlapping and the ranges must be lexigraphically sorted.
// Concat always returns the ID of a primitive fileset.
func (s *Storage) Concat(ctx context.Context, ids []ID, ttl time.Duration) (*ID, error) {
	fsw := s.NewWriter(ctx, WithTTL(ttl))
	for _, id := range ids {
		fs, err := s.Open(ctx, []ID{id})
		if err != nil {
			return nil, err
		}
		if err := CopyFiles(ctx, fsw, fs, true); err != nil {
			return nil, err
		}
	}
	return fsw.Close()
}

// Drop allows a fileset to be deleted if it is not otherwise referenced.
func (s *Storage) Drop(ctx context.Context, id ID) error {
	_, err := s.SetTTL(ctx, id, track.ExpireNow)
	return err
}

// SetTTL sets the time-to-live for the fileset at id
func (s *Storage) SetTTL(ctx context.Context, id ID, ttl time.Duration) (time.Time, error) {
	oid := id.TrackerID()
	return s.tracker.SetTTLPrefix(ctx, oid, ttl)
}

// SizeOf returns the size of the data in the fileset in bytes
func (s *Storage) SizeOf(ctx context.Context, id ID) (int64, error) {
	prims, err := s.flattenPrimitives(ctx, []ID{id})
	if err != nil {
		return 0, err
	}
	var total int64
	for _, prim := range prims {
		total += prim.SizeBytes
	}
	return total, nil
}

// WithRenewer calls cb with a Renewer, and a context which will be canceled if the renewer is unable to renew a path.
func (s *Storage) WithRenewer(ctx context.Context, ttl time.Duration, cb func(context.Context, *renew.StringSet) error) error {
	rf := func(ctx context.Context, idHexStr string, ttl time.Duration) error {
		id, err := ParseID(idHexStr)
		if err != nil {
			return err
		}
		_, err = s.SetTTL(ctx, *id, ttl)
		return err
	}
	return renew.WithStringSet(ctx, ttl, rf, cb)
}

// GC creates a track.GarbageCollector with a Deleter that can handle deleting filesets and chunks
func (s *Storage) GC(ctx context.Context) error {
	return s.newGC().RunForever(ctx)
}

func (s *Storage) newGC() *track.GarbageCollector {
	const period = 10 * time.Second
	tmpDeleter := track.NewTmpDeleter()
	chunkDeleter := s.chunks.NewDeleter()
	filesetDeleter := &deleter{
		store: s.store,
	}
	mux := track.DeleterMux(func(id string) track.Deleter {
		switch {
		case strings.HasPrefix(id, track.TmpTrackerPrefix):
			return tmpDeleter
		case strings.HasPrefix(id, chunk.TrackerPrefix):
			return chunkDeleter
		case strings.HasPrefix(id, TrackerPrefix):
			return filesetDeleter
		default:
			return nil
		}
	})
	return track.NewGarbageCollector(s.tracker, period, mux)
}

func (s *Storage) exists(ctx context.Context, id ID) (bool, error) {
	_, err := s.store.Get(ctx, id)
	if err != nil {
		if err == ErrFileSetNotExists {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (s *Storage) newPrimitive(ctx context.Context, prim *Primitive, ttl time.Duration) (*ID, error) {
	id := newID()
	md := &Metadata{
		Value: &Metadata_Primitive{
			Primitive: prim,
		},
	}
	var pointsTo []string
	for _, chunkID := range prim.PointsTo() {
		pointsTo = append(pointsTo, chunkID.TrackerID())
	}
	err := dbutil.WithTx(ctx, s.store.DB(), func(tx *sqlx.Tx) error {
		if err := s.store.SetTx(tx, id, md); err != nil {
			return err
		}
		return s.tracker.CreateTx(tx, id.TrackerID(), pointsTo, ttl)
	})
	if err != nil {
		return nil, err
	}
	return &id, nil
}

func (s *Storage) newComposite(ctx context.Context, comp *Composite, ttl time.Duration) (*ID, error) {
	id := newID()
	md := &Metadata{
		Value: &Metadata_Composite{
			Composite: comp,
		},
	}
	ids, err := comp.PointsTo()
	if err != nil {
		return nil, err
	}
	var pointsTo []string
	for _, id := range ids {
		pointsTo = append(pointsTo, id.TrackerID())
	}
	if err := dbutil.WithTx(ctx, s.store.DB(), func(tx *sqlx.Tx) error {
		if err := s.store.SetTx(tx, id, md); err != nil {
			return err
		}
		return s.tracker.CreateTx(tx, id.TrackerID(), pointsTo, ttl)
	}); err != nil {
		return nil, err
	}
	return &id, nil
}

func (s *Storage) getPrimitive(ctx context.Context, id ID) (*Primitive, error) {
	md, err := s.store.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	prim := md.GetPrimitive()
	if prim == nil {
		return nil, errors.Errorf("fileset %v is not primitive", id)
	}
	return prim, nil
}

var _ track.Deleter = &deleter{}

type deleter struct {
	store MetadataStore
}

func (d *deleter) DeleteTx(tx *sqlx.Tx, oid string) error {
	if !strings.HasPrefix(oid, TrackerPrefix) {
		return errors.Errorf("don't know how to delete %v", oid)
	}
	id, err := ParseID(oid[len(TrackerPrefix):])
	if err != nil {
		return err
	}
	return d.store.DeleteTx(tx, *id)
}

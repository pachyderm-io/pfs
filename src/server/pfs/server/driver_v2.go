package server

import (
	"archive/tar"
	"hash"
	"io"
	"log"
	"path"
	"regexp"
	"strconv"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	globlib "github.com/pachyderm/ohmyglob"
	"github.com/pachyderm/pachyderm/src/client"
	"github.com/pachyderm/pachyderm/src/client/auth"
	"github.com/pachyderm/pachyderm/src/client/pfs"
	"github.com/pachyderm/pachyderm/src/client/pkg/errors"
	pfsserver "github.com/pachyderm/pachyderm/src/server/pfs"
	"github.com/pachyderm/pachyderm/src/server/pkg/backoff"
	"github.com/pachyderm/pachyderm/src/server/pkg/hashtree"
	"github.com/pachyderm/pachyderm/src/server/pkg/serviceenv"
	"github.com/pachyderm/pachyderm/src/server/pkg/storage/chunk"
	"github.com/pachyderm/pachyderm/src/server/pkg/storage/fileset"
	"github.com/pachyderm/pachyderm/src/server/pkg/storage/fileset/index"
	"github.com/pachyderm/pachyderm/src/server/pkg/storage/gc"
	txnenv "github.com/pachyderm/pachyderm/src/server/pkg/transactionenv"
	"github.com/pachyderm/pachyderm/src/server/pkg/uuid"
	"github.com/pachyderm/pachyderm/src/server/pkg/work"
	"golang.org/x/net/context"
)

const (
	// tmpPrefix is for temporary storage paths.
	// TODO Temporary prefix cleanup needs some work.
	// Paths should get cleaned up in the background.
	tmpPrefix            = "tmp"
	storageTaskNamespace = "storage"
)

type driverV2 struct {
	*driver
}

// newDriver is used to create a new Driver instance
func newDriverV2(
	env *serviceenv.ServiceEnv,
	txnEnv *txnenv.TransactionEnv,
	etcdPrefix string,
	treeCache *hashtree.Cache,
	storageRoot string,
	memoryRequest int64,
) (*driverV2, error) {
	d1, err := newDriver(env, txnEnv, etcdPrefix, treeCache, storageRoot, memoryRequest)
	if err != nil {
		return nil, err
	}
	d2 := &driverV2{driver: d1}
	objClient, err := NewObjClient(env.Configuration)
	if err != nil {
		return nil, err
	}
	// (bryce) local db for testing.
	db, err := gc.NewLocalDB()
	if err != nil {
		return nil, err
	}
	gcClient, err := gc.NewClient(db)
	if err != nil {
		return nil, err
	}
	chunkStorageOpts := append([]chunk.StorageOption{chunk.WithGarbageCollection(gcClient)}, chunk.ServiceEnvToOptions(env)...)
	d2.storage = fileset.NewStorage(objClient, chunk.NewStorage(objClient, chunkStorageOpts...), fileset.ServiceEnvToOptions(env)...)
	d2.compactionQueue, err = work.NewTaskQueue(context.Background(), d2.etcdClient, d2.prefix, storageTaskNamespace)
	if err != nil {
		return nil, err
	}
	go d2.master(env, objClient, db)
	go d2.compactionWorker()
	return d2, nil
}

func (d *driverV2) finishCommitV2(txnCtx *txnenv.TransactionContext, commit *pfs.Commit, description string) error {
	if err := d.checkIsAuthorizedInTransaction(txnCtx, commit.Repo, auth.Scope_WRITER); err != nil {
		return err
	}
	commitInfo, err := d.resolveCommit(txnCtx.Stm, commit)
	if err != nil {
		return err
	}
	if commitInfo.Finished != nil {
		return pfsserver.ErrCommitFinished{commit}
	}
	if description != "" {
		commitInfo.Description = description
	}
	commitPath := path.Join(commit.Repo.Name, commit.ID)
	// Clean up temporary filesets leftover from failed operations.
	if err := d.storage.Delete(txnCtx.Client.Ctx(), path.Join(tmpPrefix, commitPath)); err != nil {
		return err
	}
	// Run compaction task.
	return d.compactionQueue.RunTaskBlock(txnCtx.Client.Ctx(), func(m *work.Master) error {
		if err := backoff.Retry(func() error {
			if err := d.storage.Delete(context.Background(), path.Join(commitPath, fileset.Diff)); err != nil {
				return err
			}
			return d.storage.Delete(context.Background(), path.Join(commitPath, fileset.Compacted))
		}, backoff.NewExponentialBackOff()); err != nil {
			return err
		}
		// Compact the commit changes into a diff file set.
		if err := d.compact(m, path.Join(commitPath, fileset.Diff), []string{commitPath}); err != nil {
			return err
		}
		// Compact the commit changes (diff file set) into the total changes in the commit's ancestry.
		var compactSpec *fileset.CompactSpec
		if commitInfo.ParentCommit == nil {
			compactSpec, err = d.storage.CompactSpec(m.Ctx(), commitPath)
		} else {
			parentCommitPath := path.Join(commitInfo.ParentCommit.Repo.Name, commitInfo.ParentCommit.ID)
			compactSpec, err = d.storage.CompactSpec(m.Ctx(), commitPath, parentCommitPath)
		}
		if err != nil {
			return err
		}
		if err := d.compact(m, compactSpec.Output, compactSpec.Input); err != nil {
			return err
		}
		// TODO Need to get the size from the compaction process.
		commitInfo.SizeBytes = uint64(0)
		commitInfo.Finished = types.TimestampNow()
		return d.writeFinishedCommit(txnCtx.Stm, commit, commitInfo)
	})
}

// TODO Integrate delete commit.
//func (d *driver) deleteCommitV2(txnCtx *txnenv.TransactionContext, commit *pfs.Commit) error {
//	return d.storage.Delete(txnCtx.Client.Ctx(), path.Join(commit.Repo.Name, commit.ID))
//}

// TODO Need commit validation and handling of branch names.
func (d *driverV2) withFileSet(ctx context.Context, repo, commit string, f func(*fileset.FileSet) error) (retErr error) {
	// TODO subFileSet will need to be incremented through postgres or etcd.
	d.mu.Lock()
	subFileSetStr := fileset.SubFileSetStr(d.subFileSet)
	subFileSetPath := path.Join(repo, commit, subFileSetStr)
	fs, err := d.storage.New(ctx, path.Join(tmpPrefix, subFileSetPath), subFileSetStr)
	if err != nil {
		return err
	}
	d.subFileSet++
	d.mu.Unlock()
	defer func() {
		if err := d.storage.Delete(ctx, path.Join(tmpPrefix, subFileSetPath)); retErr == nil {
			retErr = err
		}
	}()
	if err := f(fs); err != nil {
		return err
	}
	if err := fs.Close(); err != nil {
		return err
	}
	return d.compactionQueue.RunTaskBlock(ctx, func(m *work.Master) error {
		return d.compact(m, subFileSetPath, []string{path.Join(tmpPrefix, subFileSetPath)})
	})
}

// TODO Need to figure out path cleaning.
func (d *driverV2) getTar(ctx context.Context, repo, commit, glob string, w io.Writer) error {
	if err := d.getTarConditional(ctx, repo, commit, glob, func(fr *FileReader) error {
		return fr.Get(w, true)
	}); err != nil {
		return err
	}
	// Close a tar writer to create tar EOF padding.
	return tar.NewWriter(w).Close()
}

func (d *driverV2) listFileV2(pachClient *client.APIClient, file *pfs.File, full bool, history int64, f func(*pfs.FileInfoV2) error) error {
	ctx := pachClient.Ctx()
	if err := validateFile(file); err != nil {
		return err
	}
	if err := d.checkIsAuthorized(pachClient, file.Commit.Repo, auth.Scope_READER); err != nil {
		return err
	}
	return d.getTarConditional(ctx, file.Commit.Repo.Name, file.Commit.ID, file.Path, func(fr *FileReader) error {
		return f(fr.Info())
	})
}

var globRegex = regexp.MustCompile(`[*?[\]{}!()@+^]`)

func globLiteralPrefix(glob string) string {
	idx := globRegex.FindStringIndex(glob)
	if idx == nil {
		return glob
	}
	return glob[:idx[0]]
}

// TODO Need to figure out path cleaning.
func (d *driverV2) getTarConditional(ctx context.Context, repo, commit, glob string, f func(*FileReader) error) error {
	compactedPaths := []string{path.Join(repo, commit, fileset.Compacted)}
	prefix := globLiteralPrefix(glob)
	mr, err := d.storage.NewMergeReader(ctx, compactedPaths, index.WithPrefix(prefix))
	if err != nil {
		return err
	}
	mf, err := matchFunc(glob)
	if err != nil {
		return err
	}
	var fr *FileReader
	nextFileReader := func(idx *index.Index) error {
		fmr, err := mr.Next()
		if err != nil {
			return err
		}
		if !mf(idx.Path) {
			return nil
		}
		fr = newFileReader(client.NewFile(repo, commit, idx.Path), idx, fmr, mr)
		return nil
	}
	if err := d.storage.ResolveIndexes(ctx, compactedPaths, func(idx *index.Index) error {
		if fr == nil {
			return nextFileReader(idx)
		}
		dir := path.Dir(idx.Path)
		if dir == fr.file.Path {
			fr.updateFileInfo(idx)
			return nil
		}
		if err := f(fr); err != nil {
			return err
		}
		if err := fr.drain(); err != nil {
			return err
		}
		fr = nil
		return nextFileReader(idx)

	}, index.WithPrefix(prefix)); err != nil {
		return err
	}
	if fr != nil {
		return f(fr)
	}
	return nil
}

func matchFunc(glob string) (func(string) bool, error) {
	// TODO Need to do a review of the globbing functionality.
	var parentG *globlib.Glob
	parentGlob, baseGlob := path.Split(glob)
	if len(baseGlob) > 0 {
		var err error
		parentG, err = globlib.Compile(parentGlob, '/')
		if err != nil {
			return nil, err
		}
	}
	g, err := globlib.Compile(glob, '/')
	if err != nil {
		return nil, err
	}
	return func(s string) bool {
		return g.Match(s) && (parentG == nil || !parentG.Match(s))
	}, nil
}

// FileReader is a PFS wrapper for a fileset.MergeReader.
// The primary purpose of this abstraction is to convert from index.Index to
// pfs.FileInfo and to convert a set of index hashes to a file hash.
type FileReader struct {
	file      *pfs.File
	idx       *index.Index
	fmr       *fileset.FileMergeReader
	mr        *fileset.MergeReader
	fileCount int
	hash      hash.Hash
}

func newFileReader(file *pfs.File, idx *index.Index, fmr *fileset.FileMergeReader, mr *fileset.MergeReader) *FileReader {
	h := pfs.NewHash()
	for _, dataRef := range idx.DataOp.DataRefs {
		h.Write([]byte(dataRef.Hash))
	}
	return &FileReader{
		file: file,
		idx:  idx,
		fmr:  fmr,
		mr:   mr,
		hash: h,
	}
}

func (fr *FileReader) updateFileInfo(idx *index.Index) {
	fr.fileCount++
	for _, dataRef := range idx.DataOp.DataRefs {
		fr.hash.Write([]byte(dataRef.Hash))
	}
}

// Info returns the info for the file.
func (fr *FileReader) Info() *pfs.FileInfoV2 {
	return &pfs.FileInfoV2{
		File: fr.file,
		Hash: pfs.EncodeHash(fr.hash.Sum(nil)),
	}
}

// Get writes a tar stream that contains the file.
func (fr *FileReader) Get(w io.Writer, noPadding ...bool) error {
	if err := fr.fmr.Get(w); err != nil {
		return err
	}
	for fr.fileCount > 0 {
		fmr, err := fr.mr.Next()
		if err != nil {
			return err
		}
		if err := fmr.Get(w); err != nil {
			return err
		}
		fr.fileCount--
	}
	if len(noPadding) > 0 && noPadding[0] {
		return nil
	}
	// Close a tar writer to create tar EOF padding.
	return tar.NewWriter(w).Close()
}

func (fr *FileReader) drain() error {
	for fr.fileCount > 0 {
		if _, err := fr.mr.Next(); err != nil {
			return err
		}
		fr.fileCount--
	}
	return nil
}

func (d *driverV2) compact(master *work.Master, outputPath string, inputPrefixes []string) error {
	ctx := master.Ctx()
	// resolve prefixes into paths
	inputPaths := []string{}
	for _, inputPrefix := range inputPrefixes {
		if err := d.storage.WalkFileSet(ctx, inputPrefix, func(inputPath string) error {
			inputPaths = append(inputPaths, inputPath)
			return nil
		}); err != nil {
			return err
		}
	}
	return d.compactIter(ctx, compactSpec{
		master:     master,
		inputPaths: inputPaths,
		outputPath: outputPath,
		maxFanIn:   d.env.StorageCompactionMaxFanIn,
	})
}

type compactSpec struct {
	master     *work.Master
	outputPath string
	inputPaths []string
	maxFanIn   int
}

// compactIter is one level of compaction.  It will only perform compaction
// if len(inputPaths) <= params.maxFanIn otherwise it will split inputPaths recursively.
func (d *driverV2) compactIter(ctx context.Context, params compactSpec) (retErr error) {
	if len(params.inputPaths) <= params.maxFanIn {
		return d.shardedCompact(ctx, params.master, params.outputPath, params.inputPaths)
	}
	scratch := path.Join(tmpPrefix, uuid.NewWithoutDashes())
	defer func() {
		if err := d.storage.Delete(ctx, scratch); retErr == nil {
			retErr = err
		}
	}()
	childOutputPaths := []string{}
	childSize := len(params.inputPaths) / params.maxFanIn
	if len(params.inputPaths)%params.maxFanIn != 0 {
		childSize++
	}
	// TODO: use an errgroup to make the recursion concurrecnt.
	// this requires changing the master to allow multiple calls to RunSubtasks
	// don't forget to pass the errgroups childCtx to compactIter instead of ctx.
	for i := 0; i < params.maxFanIn; i++ {
		start := i * childSize
		end := (i + 1) * childSize
		if end > len(params.inputPaths) {
			end = len(params.inputPaths)
		}
		childOutputPath := path.Join(scratch, strconv.Itoa(i))
		childOutputPaths = append(childOutputPaths, childOutputPath)
		if err := d.compactIter(ctx, compactSpec{
			master:     params.master,
			inputPaths: params.inputPaths[start:end],
			outputPath: childOutputPath,
			maxFanIn:   params.maxFanIn,
		}); err != nil {
			return err
		}
	}
	return d.shardedCompact(ctx, params.master, params.outputPath, childOutputPaths)
}

// shardedCompact generates shards for the fileset(s) in inputPaths,
// gives those shards to workers, and waits for them to complete.
// Fan in is bound by len(inputPaths), concatenating shards have
// fan in of one because they are concatenated sequentially.
func (d *driverV2) shardedCompact(ctx context.Context, master *work.Master, outputPath string, inputPaths []string) (retErr error) {
	scratch := path.Join(tmpPrefix, uuid.NewWithoutDashes())
	defer func() {
		if err := d.storage.Delete(ctx, scratch); retErr == nil {
			retErr = err
		}
	}()
	compaction := &pfs.Compaction{InputPrefixes: inputPaths}
	var subtasks []*work.Task
	var shardOutputs []string
	if err := d.storage.Shard(ctx, inputPaths, func(pathRange *index.PathRange) error {
		shardOutputPath := path.Join(scratch, strconv.Itoa(len(subtasks)))
		shard, err := serializeShard(&pfs.Shard{
			Compaction: compaction,
			Range: &pfs.PathRange{
				Lower: pathRange.Lower,
				Upper: pathRange.Upper,
			},
			OutputPath: shardOutputPath,
		})
		if err != nil {
			return err
		}
		subtasks = append(subtasks, &work.Task{Data: shard})
		shardOutputs = append(shardOutputs, shardOutputPath)
		return nil
	}); err != nil {
		return err
	}
	if err := master.RunSubtasks(subtasks, func(_ context.Context, taskInfo *work.TaskInfo) error {
		if taskInfo.State == work.State_FAILURE {
			return errors.Errorf(taskInfo.Reason)
		}
		return nil
	}); err != nil {
		return err
	}
	return d.concatFileSets(ctx, outputPath, shardOutputs)
}

// concatFileSets concatenates the filesets in inputPaths and writes the result to outputPath
// TODO: move this to the fileset package, and error if the entries are not sorted.
func (d *driverV2) concatFileSets(ctx context.Context, outputPath string, inputPaths []string) error {
	fsw := d.storage.NewWriter(ctx, outputPath)
	for _, inputPath := range inputPaths {
		fsr := d.storage.NewReader(ctx, inputPath)
		if err := fsr.Iterate(func(fr *fileset.FileReader) error {
			return fsw.CopyFile(fr)
		}); err != nil {
			return err
		}
	}
	return fsw.Close()
}

func serializeShard(shard *pfs.Shard) (*types.Any, error) {
	serializedShard, err := proto.Marshal(shard)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &types.Any{
		TypeUrl: "/" + proto.MessageName(shard),
		Value:   serializedShard,
	}, nil
}

func deserializeShard(shardAny *types.Any) (*pfs.Shard, error) {
	shard := &pfs.Shard{}
	if err := types.UnmarshalAny(shardAny, shard); err != nil {
		return nil, errors.WithStack(err)
	}
	return shard, nil
}

func (d *driverV2) compactionWorker() {
	ctx := context.Background()
	w := work.NewWorker(d.etcdClient, d.prefix, storageTaskNamespace)
	err := backoff.RetryNotify(func() error {
		return w.Run(ctx, func(ctx context.Context, subtask *work.Task) error {
			shard, err := deserializeShard(subtask.Data)
			if err != nil {
				return err
			}
			pathRange := &index.PathRange{
				Lower: shard.Range.Lower,
				Upper: shard.Range.Upper,
			}
			return d.storage.Compact(ctx, shard.OutputPath, shard.Compaction.InputPrefixes, index.WithRange(pathRange))
		})
	}, backoff.NewInfiniteBackOff(), func(err error, _ time.Duration) error {
		log.Printf("error in compaction worker: %v", err)
		return nil
	})
	// Never ending backoff should prevent us from getting here.
	panic(err)
}

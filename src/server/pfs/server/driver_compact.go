package server

import (
	"path"
	"strconv"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	"github.com/pachyderm/pachyderm/v2/src/internal/backoff"
	"github.com/pachyderm/pachyderm/v2/src/internal/errors"
	"github.com/pachyderm/pachyderm/v2/src/internal/storage/fileset"
	"github.com/pachyderm/pachyderm/v2/src/internal/storage/fileset/index"
	"github.com/pachyderm/pachyderm/v2/src/internal/storage/renew"
	"github.com/pachyderm/pachyderm/v2/src/internal/uuid"
	"github.com/pachyderm/pachyderm/v2/src/internal/work"
	"github.com/pachyderm/pachyderm/v2/src/pfs"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
)

func (d *driver) compactCommit(master *work.Master, commit *pfs.Commit) error {
	ctx := master.Ctx()
	return d.commitStore.UpdateFileset(ctx, commit, func(x fileset.ID) (*fileset.ID, error) {
		if yes, err := d.storage.IsCompacted(ctx, x); err != nil {
			return nil, err
		} else if yes {
			return &x, nil
		}
		return d.storage.Compact(ctx, []fileset.ID{x}, defaultTTL)
	})
}

type compactSpec struct {
	master     *work.Master
	inputPaths []string
	maxFanIn   int
}

type compactResult struct {
	ID fileset.ID
}

// compactIter is one level of compaction.  It will only perform compaction
// if len(inputPaths) <= params.maxFanIn otherwise it will split inputPaths recursively.
func (d *driver) compactIter(ctx context.Context, params compactSpec) (*compactResult, error) {
	if len(params.inputPaths) <= params.maxFanIn {
		return d.shardedCompact(ctx, params.master, params.inputPaths)
	}
	childSize := params.maxFanIn
	for len(params.inputPaths)/childSize > params.maxFanIn {
		childSize *= params.maxFanIn
	}
	// TODO: use an errgroup to make the recursion concurrecnt.
	// this requires changing the master to allow multiple calls to RunSubtasks
	// don't forget to pass the errgroups childCtx to compactIter instead of ctx.
	var res *compactResult
	if err := d.storage.WithRenewer(ctx, defaultTTL, func(ctx context.Context, renewer *renew.StringSet) error {
		var childOutputPaths []string
		for start := 0; start < len(params.inputPaths); start += childSize {
			end := start + childSize
			if end > len(params.inputPaths) {
				end = len(params.inputPaths)
			}
			res, err := d.compactIter(ctx, compactSpec{
				master:     params.master,
				inputPaths: params.inputPaths[start:end],
				maxFanIn:   params.maxFanIn,
			})
			if err != nil {
				return err
			}
			renewer.Add(res.OutputPath)
			childOutputPaths = append(childOutputPaths, res.OutputPath)
		}
		var err error
		res, err = d.shardedCompact(ctx, params.master, childOutputPaths)
		return err
	}); err != nil {
		return nil, err
	}
	return res, nil
}

// shardedCompact generates shards for the fileset(s) in inputPaths,
// gives those shards to workers, and waits for them to complete.
// Fan in is bound by len(inputPaths), concatenating shards have
// fan in of one because they are concatenated sequentially.
func (d *driver) shardedCompact(ctx context.Context, master *work.Master, inputPaths []string) (*compactResult, error) {
	scratch := path.Join(tmpRepo, uuid.NewWithoutDashes())
	compaction := &pfs.Compaction{InputPrefixes: inputPaths}
	var subtasks []*work.Task
	var shardOutputs []string
	fs, err := d.storage.Open(ctx, inputPaths)
	if err != nil {
		return nil, err
	}
	if err := d.storage.Shard(ctx, fs, func(pathRange *index.PathRange) error {
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
		return nil, err
	}
	if len(subtasks) == 1 {
		if err := d.compactShard(ctx, subtasks[0]); err != nil {
			return nil, err
		}
		return &compactResult{
			OutputPath: shardOutputs[0],
		}, nil
	}
	var res *compactResult
	if err := d.storage.WithRenewer(ctx, defaultTTL, func(ctx context.Context, renewer *renew.StringSet) error {
		if err := master.RunSubtasks(subtasks, func(_ context.Context, taskInfo *work.TaskInfo) error {
			if taskInfo.State == work.State_FAILURE {
				return errors.Errorf(taskInfo.Reason)
			}
			shard, err := deserializeShard(taskInfo.Task.Data)
			if err != nil {
				return err
			}
			renewer.Add(shard.OutputPath)
			return nil
		}); err != nil {
			return err
		}
		var err error
		res, err = d.concatFileSets(ctx, shardOutputs)
		return err
	}); err != nil {
		return nil, err
	}
	return res, nil
}

// concatFileSets concatenates the filesets in inputPaths and writes the result to outputPath
// TODO: move this to the fileset package, and error if the entries are not sorted.
func (d *driver) concatFileSets(ctx context.Context, inputPaths []string) (*compactResult, error) {
	fsw := d.storage.NewWriter(ctx, fileset.WithTTL(defaultTTL))
	for _, inputPath := range inputPaths {
		fs, err := d.storage.Open(ctx, []string{inputPath})
		if err != nil {
			return nil, err
		}
		if err := fileset.CopyFiles(ctx, fsw, fs, true); err != nil {
			return nil, err
		}
	}
	id, err := fsw.Close()
	if err != nil {
		return nil, err
	}
	return &compactResult{ID: *id}, nil
}

func (d *driver) compactionWorker() {
	ctx := context.Background()
	w := work.NewWorker(d.etcdClient, d.prefix, storageTaskNamespace)
	err := backoff.RetryNotify(func() error {
		return w.Run(ctx, func(ctx context.Context, subtask *work.Task) error {
			return d.compactShard(ctx, subtask)
		})
	}, backoff.NewInfiniteBackOff(), func(err error, _ time.Duration) error {
		log.Printf("error in compaction worker: %v", err)
		return nil
	})
	// Never ending backoff should prevent us from getting here.
	panic(err)
}

func (d *driver) compactShard(ctx context.Context, subtask *work.Task) error {
	shard, err := deserializeShard(subtask.Data)
	if err != nil {
		return err
	}
	pathRange := &index.PathRange{
		Lower: shard.Range.Lower,
		Upper: shard.Range.Upper,
	}
	_, err = d.storage.Compact(ctx, shard.OutputPath, shard.Compaction.InputPrefixes, defaultTTL, index.WithRange(pathRange))
	return err
}

func serializeShard(shard *pfs.Shard) (*types.Any, error) {
	serializedShard, err := proto.Marshal(shard)
	if err != nil {
		return nil, err
	}
	return &types.Any{
		TypeUrl: "/" + proto.MessageName(shard),
		Value:   serializedShard,
	}, nil
}

func deserializeShard(shardAny *types.Any) (*pfs.Shard, error) {
	shard := &pfs.Shard{}
	if err := types.UnmarshalAny(shardAny, shard); err != nil {
		return nil, err
	}
	return shard, nil
}

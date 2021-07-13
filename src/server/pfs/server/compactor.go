package server

import (
	"time"

	etcd "github.com/coreos/etcd/clientv3"
	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	"github.com/pachyderm/pachyderm/v2/src/internal/backoff"
	"github.com/pachyderm/pachyderm/v2/src/internal/errors"
	"github.com/pachyderm/pachyderm/v2/src/internal/storage/fileset"
	"github.com/pachyderm/pachyderm/v2/src/internal/storage/fileset/index"
	"github.com/pachyderm/pachyderm/v2/src/internal/work"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
)

var _ fileset.Compactor = &compactor{}

type compactor struct {
	storage  *fileset.Storage
	maxFanIn int

	compactionQueue *work.TaskQueue
	worker          *work.Worker
}

func newCompactor(ctx context.Context, storage *fileset.Storage, etcdClient *etcd.Client, etcdPrefix string, maxFanIn int) (*compactor, error) {
	if maxFanIn < 2 {
		panic(maxFanIn)
	}
	compactionQueue, err := work.NewTaskQueue(ctx, etcdClient, etcdPrefix, storageTaskNamespace)
	if err != nil {
		return nil, err
	}
	worker := work.NewWorker(etcdClient, etcdPrefix, storageTaskNamespace)
	c := &compactor{
		storage:         storage,
		maxFanIn:        maxFanIn,
		compactionQueue: compactionQueue,
		worker:          worker,
	}
	go c.compactionWorker(ctx)
	return c, nil
}

func (c *compactor) Compact(ctx context.Context, ids []fileset.ID, ttl time.Duration) (*fileset.ID, error) {
	return c.storage.CompactLevelBased(ctx, ids, defaultTTL, func(ctx context.Context, ids []fileset.ID, ttl time.Duration) (*fileset.ID, error) {
		var id *fileset.ID
		if err := c.compactionQueue.RunTaskBlock(ctx, func(master *work.Master) error {
			workerFunc := func(ctx context.Context, tasks []fileset.CompactionTask) ([]fileset.ID, error) {
				workTasks := make([]*work.Task, len(tasks))
				for i, task := range tasks {
					serInputs := make([]string, len(task.Inputs))
					for i := range task.Inputs {
						serInputs[i] = task.Inputs[i].HexString()
					}
					any, err := serializeCompactionTask(&CompactionTask{
						Index:  int64(i),
						Inputs: serInputs,
						Range: &PathRange{
							Lower: task.PathRange.Lower,
							Upper: task.PathRange.Upper,
						},
					})
					if err != nil {
						return nil, err
					}
					workTasks[i] = &work.Task{Data: any}
				}
				results := make([]fileset.ID, len(tasks))
				if err := master.RunSubtasks(workTasks, func(_ context.Context, taskInfo *work.TaskInfo) error {
					if taskInfo.State == work.State_FAILURE {
						return errors.New(taskInfo.Reason)
					}
					res, err := deserializeCompactionResult(taskInfo.Result)
					if err != nil {
						return err
					}
					id, err := fileset.ParseID(res.Id)
					if err != nil {
						return err
					}
					results[int(res.Index)] = *id
					return nil
				}); err != nil {
					return nil, err
				}
				return results, nil
			}
			dc := fileset.NewDistributedCompactor(c.storage, c.maxFanIn, workerFunc)
			var err error
			id, err = dc.Compact(master.Ctx(), ids, ttl)
			return err
		}); err != nil {
			return nil, err
		}
		return id, nil
	})
}

func (c *compactor) compactionWorker(ctx context.Context) error {
	return backoff.RetryUntilCancel(ctx, func() error {
		return c.worker.Run(ctx, func(ctx context.Context, subtask *work.Task) (*types.Any, error) {
			task, err := deserializeCompactionTask(subtask.Data)
			if err != nil {
				return nil, err
			}
			ids := []fileset.ID{}
			for _, input := range task.Inputs {
				id, err := fileset.ParseID(input)
				if err != nil {
					return nil, err
				}
				ids = append(ids, *id)
			}
			pathRange := &index.PathRange{
				Lower: task.Range.Lower,
				Upper: task.Range.Upper,
			}
			id, err := c.storage.Compact(ctx, ids, defaultTTL, index.WithRange(pathRange))
			if err != nil {
				return nil, err
			}
			return serializeCompactionResult(&CompactionTaskResult{
				Index: task.Index,
				Id:    id.HexString(),
			})
		})
	}, backoff.NewInfiniteBackOff(), func(err error, _ time.Duration) error {
		log.Printf("error in compaction worker: %v", err)
		return nil
	})
}

func serializeCompactionTask(task *CompactionTask) (*types.Any, error) {
	data, err := proto.Marshal(task)
	if err != nil {
		return nil, err
	}
	return &types.Any{
		TypeUrl: "/" + proto.MessageName(task),
		Value:   data,
	}, nil
}

func deserializeCompactionTask(taskAny *types.Any) (*CompactionTask, error) {
	task := &CompactionTask{}
	if err := types.UnmarshalAny(taskAny, task); err != nil {
		return nil, err
	}
	return task, nil
}

func serializeCompactionResult(res *CompactionTaskResult) (*types.Any, error) {
	data, err := proto.Marshal(res)
	if err != nil {
		return nil, err
	}
	return &types.Any{
		TypeUrl: "/" + proto.MessageName(res),
		Value:   data,
	}, nil
}

func deserializeCompactionResult(any *types.Any) (*CompactionTaskResult, error) {
	res := &CompactionTaskResult{}
	if err := types.UnmarshalAny(any, res); err != nil {
		return nil, err
	}
	return res, nil
}

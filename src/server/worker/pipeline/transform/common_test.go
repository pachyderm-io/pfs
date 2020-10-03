package transform

import (
	"context"
	"path/filepath"
	"time"

	etcd "github.com/coreos/etcd/clientv3"
	"github.com/gogo/protobuf/types"

	"github.com/pachyderm/pachyderm/src/client"
	"github.com/pachyderm/pachyderm/src/client/pfs"
	"github.com/pachyderm/pachyderm/src/client/pps"
	col "github.com/pachyderm/pachyderm/src/server/pkg/collection"
	"github.com/pachyderm/pachyderm/src/server/pkg/hashtree"
	"github.com/pachyderm/pachyderm/src/server/pkg/ppsconsts"
	"github.com/pachyderm/pachyderm/src/server/pkg/serviceenv"
	"github.com/pachyderm/pachyderm/src/server/pkg/testpachd"
	"github.com/pachyderm/pachyderm/src/server/pkg/work"
	"github.com/pachyderm/pachyderm/src/server/worker/cache"
	"github.com/pachyderm/pachyderm/src/server/worker/common"
	"github.com/pachyderm/pachyderm/src/server/worker/driver"
	"github.com/pachyderm/pachyderm/src/server/worker/logs"
)

func defaultPipelineInfo() *pps.PipelineInfo {
	name := "testPipeline"
	return &pps.PipelineInfo{
		Pipeline:     client.NewPipeline(name),
		OutputBranch: "master",
		Transform: &pps.Transform{
			Cmd:        []string{"bash", "-c", "cp inputRepo/* out"},
			WorkingDir: client.PPSInputPrefix,
		},
		ParallelismSpec: &pps.ParallelismSpec{
			Constant: 1,
		},
		ResourceRequests: &pps.ResourceSpec{
			Memory: "100M",
			Cpu:    0.5,
		},
		Input: &pps.Input{
			Pfs: &pps.PFSInput{
				Name:   "inputRepo",
				Repo:   "inputRepo",
				Branch: "master",
				Glob:   "/*",
			},
		},
		SpecCommit: client.NewCommit(ppsconsts.SpecRepo, name),
	}
}

type testEnv struct {
	*testpachd.RealEnv
	logger *logs.MockLogger
	driver driver.Driver
}

// testDriver is identical to a real driver except it overloads egress, which is
// tricky to do in a test environment
type testDriver struct {
	inner driver.Driver
}

// Fuck golang
func (td *testDriver) Env() *serviceenv.ServiceEnv {
	return td.inner.Env()
}
func (td *testDriver) Jobs() col.Collection {
	return td.inner.Jobs()
}
func (td *testDriver) Pipelines() col.Collection {
	return td.inner.Pipelines()
}
func (td *testDriver) NewTaskWorker() *work.Worker {
	return td.inner.NewTaskWorker()
}
func (td *testDriver) NewTaskQueue() (*work.TaskQueue, error) {
	return td.inner.NewTaskQueue()
}
func (td *testDriver) PipelineInfo() *pps.PipelineInfo {
	return td.inner.PipelineInfo()
}
func (td *testDriver) Namespace() string {
	return td.inner.Namespace()
}
func (td *testDriver) InputDir() string {
	return td.inner.InputDir()
}
func (td *testDriver) PachClient() *client.APIClient {
	return td.inner.PachClient()
}
func (td *testDriver) ExpectedNumWorkers() (int64, error) {
	return td.inner.ExpectedNumWorkers()
}
func (td *testDriver) NumShards() int64 {
	return td.inner.NumShards()
}
func (td *testDriver) WithContext(ctx context.Context) driver.Driver {
	return &testDriver{td.inner.WithContext(ctx)}
}
func (td *testDriver) WithData(inputs []*common.Input, tree *hashtree.Ordered, logger logs.TaggedLogger, cb func(string, *pps.ProcessStats) error) (*pps.ProcessStats, error) {
	return td.inner.WithData(inputs, tree, logger, cb)
}
func (td *testDriver) WithActiveData(inputs []*common.Input, dir string, cb func() error) error {
	return td.inner.WithActiveData(inputs, dir, cb)
}
func (td *testDriver) UserCodeEnv(job string, commit *pfs.Commit, inputs []*common.Input) []string {
	return td.inner.UserCodeEnv(job, commit, inputs)
}
func (td *testDriver) RunUserCode(logger logs.TaggedLogger, env []string, stats *pps.ProcessStats, d *types.Duration) error {
	return td.inner.RunUserCode(logger, env, stats, d)
}
func (td *testDriver) RunUserErrorHandlingCode(logger logs.TaggedLogger, env []string, stats *pps.ProcessStats, d *types.Duration) error {
	return td.inner.RunUserErrorHandlingCode(logger, env, stats, d)
}
func (td *testDriver) DeleteJob(stm col.STM, ji *pps.EtcdJobInfo) error {
	return td.inner.DeleteJob(stm, ji)
}
func (td *testDriver) UpdateJobState(job string, state pps.JobState, reason string) error {
	return td.inner.UpdateJobState(job, state, reason)
}
func (td *testDriver) UploadOutput(dir string, tag string, logger logs.TaggedLogger, input []*common.Input, stats *pps.ProcessStats, tree *hashtree.Ordered) ([]byte, error) {
	return td.inner.UploadOutput(dir, tag, logger, input, stats, tree)
}
func (td *testDriver) ReportUploadStats(t time.Time, stats *pps.ProcessStats, logger logs.TaggedLogger) {
	td.inner.ReportUploadStats(t, stats, logger)
}
func (td *testDriver) NewSTM(cb func(col.STM) error) (*etcd.TxnResponse, error) {
	return td.inner.NewSTM(cb)
}
func (td *testDriver) ChunkCaches() cache.WorkerCache {
	return td.inner.ChunkCaches()
}
func (td *testDriver) ChunkStatsCaches() cache.WorkerCache {
	return td.inner.ChunkStatsCaches()
}
func (td *testDriver) WithDatumCache(cb func(*hashtree.MergeCache, *hashtree.MergeCache) error) error {
	return td.inner.WithDatumCache(cb)
}

func (td *testDriver) Egress(commit *pfs.Commit, egressURL string) error {
	return nil
}

// withTestEnv provides a test env with etcd and pachd instances and connected
// clients, plus a worker driver for performing worker operations.
func withTestEnv(pipelineInfo *pps.PipelineInfo, cb func(*testEnv) error) error {
	return testpachd.WithRealEnv(func(realEnv *testpachd.RealEnv) error {
		logger := logs.NewMockLogger()
		workerDir := filepath.Join(realEnv.Directory, "worker")
		driver, err := driver.NewDriver(
			realEnv.ServiceEnv,
			pipelineInfo,
			realEnv.PachClient,
			workerDir,
		)
		if err != nil {
			return err
		}

		ctx, cancel := context.WithCancel(realEnv.PachClient.Ctx())
		defer cancel()
		driver = driver.WithContext(ctx)

		env := &testEnv{
			RealEnv: realEnv,
			logger:  logger,
			driver:  &testDriver{driver},
		}

		return cb(env)
	})
}

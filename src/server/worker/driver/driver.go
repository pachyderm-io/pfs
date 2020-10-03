package driver

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unicode/utf8"

	etcd "github.com/coreos/etcd/clientv3"
	"github.com/gogo/protobuf/types"
	"github.com/prometheus/client_golang/prometheus"
	"gopkg.in/go-playground/webhooks.v5/github"
	"gopkg.in/src-d/go-git.v4"
	gitPlumbing "gopkg.in/src-d/go-git.v4/plumbing"

	"github.com/pachyderm/pachyderm/src/client"
	"github.com/pachyderm/pachyderm/src/client/enterprise"
	"github.com/pachyderm/pachyderm/src/client/pfs"
	"github.com/pachyderm/pachyderm/src/client/pkg/errors"
	"github.com/pachyderm/pachyderm/src/client/pkg/grpcutil"
	"github.com/pachyderm/pachyderm/src/client/pps"
	col "github.com/pachyderm/pachyderm/src/server/pkg/collection"
	"github.com/pachyderm/pachyderm/src/server/pkg/errutil"
	"github.com/pachyderm/pachyderm/src/server/pkg/exec"
	"github.com/pachyderm/pachyderm/src/server/pkg/hashtree"
	"github.com/pachyderm/pachyderm/src/server/pkg/obj"
	"github.com/pachyderm/pachyderm/src/server/pkg/ppsconsts"
	"github.com/pachyderm/pachyderm/src/server/pkg/ppsdb"
	"github.com/pachyderm/pachyderm/src/server/pkg/ppsutil"
	"github.com/pachyderm/pachyderm/src/server/pkg/serviceenv"
	filesync "github.com/pachyderm/pachyderm/src/server/pkg/sync"
	"github.com/pachyderm/pachyderm/src/server/pkg/uuid"
	"github.com/pachyderm/pachyderm/src/server/pkg/work"
	"github.com/pachyderm/pachyderm/src/server/worker/cache"
	"github.com/pachyderm/pachyderm/src/server/worker/common"
	"github.com/pachyderm/pachyderm/src/server/worker/logs"
	"github.com/pachyderm/pachyderm/src/server/worker/stats"
)

const (
	// The maximum number of concurrent download/upload operations
	concurrency = 100
)

var (
	errSpecialFile = errors.New("cannot upload special file")
)

func workNamespace(pipelineInfo *pps.PipelineInfo) string {
	return fmt.Sprintf("/pipeline-%s/v%d", pipelineInfo.Pipeline.Name, pipelineInfo.Version)
}

// Driver provides an interface for common functions needed by worker code, and
// captures the relevant objects necessary to provide these functions so that
// users do not need to keep track of as many variables.  In addition, this
// interface can be used to mock out external calls to make unit-testing
// simpler.
type Driver interface {
	Env() *serviceenv.ServiceEnv

	Jobs() col.Collection
	Pipelines() col.Collection

	NewTaskWorker() *work.Worker
	NewTaskQueue() (*work.TaskQueue, error)

	// Returns the PipelineInfo for the pipeline that this worker belongs to
	PipelineInfo() *pps.PipelineInfo

	// Returns the kubernetes namespace that the worker is deployed in
	Namespace() string

	// Returns the path that will contain the input filesets for the job
	InputDir() string

	// Returns the pachd API client for the driver
	PachClient() *client.APIClient

	// Returns the number of workers to be used
	ExpectedNumWorkers() (int64, error)

	// Returns the number of hashtree shards for the pipeline
	NumShards() int64

	// WithContext clones the current driver and applies the context to its
	// pachClient. The pachClient context will be used for other blocking
	// operations as well.
	WithContext(context.Context) Driver

	// WithData prepares the current node the code is running on to run a piece
	// of user code by downloading the specified data, and cleans up afterwards.
	// The temporary scratch directory that the data is stored in will be passed
	// to the given callback.
	WithData([]*common.Input, *hashtree.Ordered, logs.TaggedLogger, func(string, *pps.ProcessStats) error) (*pps.ProcessStats, error)

	// WithActiveData swaps the given scratch directory into the 'active' input
	// directory used when running user code. This also locks a mutex so that no
	// two datums can be active concurrently.
	WithActiveData([]*common.Input, string, func() error) error

	// UserCodeEnv returns the set of environment variables to construct when
	// launching the configured user process.
	UserCodeEnv(string, *pfs.Commit, []*common.Input) []string

	// RunUserCode links a specific scratch space for the active input/output
	// data, then runs the pipeline's configured code. It uses a mutex to enforce
	// that this is not done concurrently, and may block.
	RunUserCode(logs.TaggedLogger, []string, *pps.ProcessStats, *types.Duration) error

	// RunUserErrorHandlingCode runs the pipeline's configured error handling code
	RunUserErrorHandlingCode(logs.TaggedLogger, []string, *pps.ProcessStats, *types.Duration) error

	// TODO: provide a more generic interface for modifying jobs, and
	// some quality-of-life functions for common operations.
	DeleteJob(col.STM, *pps.EtcdJobInfo) error
	UpdateJobState(string, pps.JobState, string) error

	// UploadOutput uploads the stats hashtree and pfs output directory to object
	// storage and returns a buffer of the serialized hashtree
	UploadOutput(string, string, logs.TaggedLogger, []*common.Input, *pps.ProcessStats, *hashtree.Ordered) ([]byte, error)

	// TODO: figure out how to not expose this
	ReportUploadStats(time.Time, *pps.ProcessStats, logs.TaggedLogger)

	// TODO: figure out how to not expose this - currently only used for a few
	// operations in the map spawner
	NewSTM(func(col.STM) error) (*etcd.TxnResponse, error)

	// These caches are used for storing and merging hashtrees from jobs until the
	// job is complete
	ChunkCaches() cache.WorkerCache
	ChunkStatsCaches() cache.WorkerCache

	// WithDatumCache calls the given callback with two hashtree merge caches, one
	// for datums and one for datum stats. The lifetime of these caches will be
	// bound to the callback, and any resources will be cleaned up upon return.
	WithDatumCache(func(*hashtree.MergeCache, *hashtree.MergeCache) error) error

	Egress(commit *pfs.Commit, egressURL string) error
}

type driver struct {
	env             *serviceenv.ServiceEnv
	pipelineInfo    *pps.PipelineInfo
	pachClient      *client.APIClient
	activeDataMutex *sync.Mutex

	jobs col.Collection

	pipelines col.Collection

	numShards int64

	namespace string

	// User and group IDs used for running user code, determined in the constructor
	uid *uint32
	gid *uint32

	// We only export application statistics if enterprise is enabled
	exportStats bool

	// The root directory to use when setting the user code path. This is normally
	// "/", but can be overridden by tests.
	rootDir string

	// The directory to store input data - this is typically static but can be
	// overridden by tests.
	inputDir string

	// These caches are used for storing and merging hashtrees from jobs until the
	// job is complete
	chunkCaches, chunkStatsCaches cache.WorkerCache
}

// NewDriver constructs a Driver object using the given clients and pipeline
// settings.  It makes blocking calls to determine the user/group to use with
// the user code on the current worker node, as well as determining if
// enterprise features are activated (for exporting stats).
func NewDriver(
	env *serviceenv.ServiceEnv,
	pipelineInfo *pps.PipelineInfo,
	pachClient *client.APIClient,
	rootPath string,
) (Driver, error) {

	hashtreePath := env.StorageRoot
	pfsPath := filepath.Join(rootPath, client.PPSInputPrefix)
	chunkCachePath := filepath.Join(hashtreePath, "chunk")
	chunkStatsCachePath := filepath.Join(hashtreePath, "chunkStats")

	// Delete the hashtree path (if it exists) in case it is left over from a previous run
	if err := os.RemoveAll(chunkCachePath); err != nil {
		return nil, errors.EnsureStack(err)
	}
	if err := os.RemoveAll(chunkStatsCachePath); err != nil {
		return nil, errors.EnsureStack(err)
	}

	if err := os.MkdirAll(pfsPath, 0777); err != nil {
		return nil, errors.EnsureStack(err)
	}
	if err := os.MkdirAll(chunkCachePath, 0777); err != nil {
		return nil, errors.EnsureStack(err)
	}
	if err := os.MkdirAll(chunkStatsCachePath, 0777); err != nil {
		return nil, errors.EnsureStack(err)
	}

	numShards, err := ppsutil.GetExpectedNumHashtrees(pipelineInfo.HashtreeSpec)
	if err != nil {
		logs.NewStatlessLogger(pipelineInfo).Logf("error getting number of shards, default to 1 shard: %v", err)
		numShards = 1
	}

	result := &driver{
		env:              env,
		pipelineInfo:     pipelineInfo,
		pachClient:       pachClient,
		activeDataMutex:  &sync.Mutex{},
		jobs:             ppsdb.Jobs(env.GetEtcdClient(), env.EtcdPrefix),
		pipelines:        ppsdb.Pipelines(env.GetEtcdClient(), env.EtcdPrefix),
		numShards:        numShards,
		rootDir:          rootPath,
		inputDir:         pfsPath,
		chunkCaches:      cache.NewWorkerCache(chunkCachePath),
		chunkStatsCaches: cache.NewWorkerCache(chunkStatsCachePath),
	}

	if pipelineInfo.Transform.User != "" {
		user, err := lookupDockerUser(pipelineInfo.Transform.User)
		if err != nil && !os.IsNotExist(err) {
			return nil, errors.EnsureStack(err)
		}
		// If `user` is `nil`, `uid` and `gid` will get set, and we won't
		// customize the user that executes the worker process.
		if user != nil { // user is nil when os.IsNotExist(err) is true in which case we use root
			uid, err := strconv.ParseUint(user.Uid, 10, 32)
			if err != nil {
				return nil, errors.EnsureStack(err)
			}
			uid32 := uint32(uid)
			result.uid = &uid32
			gid, err := strconv.ParseUint(user.Gid, 10, 32)
			if err != nil {
				return nil, errors.EnsureStack(err)
			}
			gid32 := uint32(gid)
			result.gid = &gid32
		}
	}

	if resp, err := pachClient.Enterprise.GetState(context.Background(), &enterprise.GetStateRequest{}); err != nil {
		logs.NewStatlessLogger(pipelineInfo).Logf("failed to get enterprise state with error: %v\n", err)
	} else {
		result.exportStats = resp.State == enterprise.State_ACTIVE
	}

	return result, nil
}

// lookupDockerUser looks up users given the argument to a Dockerfile USER directive.
// According to Docker's docs this directive looks like:
// USER <user>[:<group>] or
// USER <UID>[:<GID>]
func lookupDockerUser(userArg string) (_ *user.User, retErr error) {
	userParts := strings.Split(userArg, ":")
	userOrUID := userParts[0]
	groupOrGID := ""
	if len(userParts) > 1 {
		groupOrGID = userParts[1]
	}
	passwd, err := os.Open("/etc/passwd")
	if err != nil {
		return nil, errors.EnsureStack(err)
	}
	defer func() {
		if err := passwd.Close(); err != nil && retErr == nil {
			retErr = errors.EnsureStack(err)
		}
	}()
	scanner := bufio.NewScanner(passwd)
	for scanner.Scan() {
		parts := strings.Split(scanner.Text(), ":")
		if parts[0] == userOrUID || parts[2] == userOrUID {
			result := &user.User{
				Username: parts[0],
				Uid:      parts[2],
				Gid:      parts[3],
				Name:     parts[4],
				HomeDir:  parts[5],
			}
			if groupOrGID != "" {
				if parts[0] == userOrUID {
					// groupOrGid is a group
					group, err := lookupGroup(groupOrGID)
					if err != nil {
						return nil, errors.EnsureStack(err)
					}
					result.Gid = group.Gid
				} else {
					// groupOrGid is a gid
					result.Gid = groupOrGID
				}
			}
			return result, nil
		}
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	return nil, errors.Errorf("user %s not found", userArg)
}

func lookupGroup(group string) (_ *user.Group, retErr error) {
	groupFile, err := os.Open("/etc/group")
	if err != nil {
		return nil, errors.EnsureStack(err)
	}
	defer func() {
		if err := groupFile.Close(); err != nil && retErr == nil {
			retErr = errors.EnsureStack(err)
		}
	}()
	scanner := bufio.NewScanner(groupFile)
	for scanner.Scan() {
		parts := strings.Split(scanner.Text(), ":")
		if parts[0] == group {
			return &user.Group{
				Gid:  parts[2],
				Name: parts[0],
			}, nil
		}
	}
	return nil, errors.Errorf("group %s not found", group)
}

func (d *driver) WithContext(ctx context.Context) Driver {
	result := &driver{}
	*result = *d
	result.pachClient = result.pachClient.WithCtx(ctx)
	return result
}

func (d *driver) Env() *serviceenv.ServiceEnv {
	return d.env
}

func (d *driver) Jobs() col.Collection {
	return d.jobs
}

func (d *driver) Pipelines() col.Collection {
	return d.pipelines
}

func (d *driver) NewTaskWorker() *work.Worker {
	return work.NewWorker(d.env.GetEtcdClient(), d.env.EtcdPrefix, workNamespace(d.pipelineInfo))
}

func (d *driver) NewTaskQueue() (*work.TaskQueue, error) {
	return work.NewTaskQueue(d.PachClient().Ctx(), d.env.GetEtcdClient(), d.env.EtcdPrefix, workNamespace(d.pipelineInfo))
}

func (d *driver) ExpectedNumWorkers() (int64, error) {
	pipelinePtr := &pps.EtcdPipelineInfo{}
	if err := d.Pipelines().ReadOnly(d.PachClient().Ctx()).Get(d.PipelineInfo().Pipeline.Name, pipelinePtr); err != nil {
		return 0, errors.EnsureStack(err)
	}
	numWorkers := pipelinePtr.Parallelism
	if numWorkers == 0 {
		numWorkers = 1
	}
	return int64(numWorkers), nil
}

func (d *driver) NumShards() int64 {
	return d.numShards
}

func (d *driver) PipelineInfo() *pps.PipelineInfo {
	return d.pipelineInfo
}

func (d *driver) Namespace() string {
	return d.env.Namespace
}

func (d *driver) InputDir() string {
	return d.inputDir
}

func (d *driver) PachClient() *client.APIClient {
	return d.pachClient
}

func (d *driver) ChunkCaches() cache.WorkerCache {
	return d.chunkCaches
}

func (d *driver) ChunkStatsCaches() cache.WorkerCache {
	return d.chunkStatsCaches
}

// This is broken out into its own function because its scope is small and it
// can easily be used by the mock driver for testing purposes.
func withDatumCache(storageRoot string, cb func(*hashtree.MergeCache, *hashtree.MergeCache) error) (retErr error) {
	cacheID := uuid.NewWithoutDashes()

	datumCache, err := hashtree.NewMergeCache(filepath.Join(storageRoot, "datum", cacheID))
	if err != nil {
		return err
	}
	defer func() {
		if err := datumCache.Close(); retErr == nil {
			retErr = err
		}
	}()

	datumStatsCache, err := hashtree.NewMergeCache(filepath.Join(storageRoot, "datumStats", cacheID))
	if err != nil {
		return err
	}
	defer func() {
		if err := datumStatsCache.Close(); retErr == nil {
			retErr = err
		}
	}()

	return cb(datumCache, datumStatsCache)
}

func (d *driver) WithDatumCache(cb func(*hashtree.MergeCache, *hashtree.MergeCache) error) (retErr error) {
	return withDatumCache(d.env.StorageRoot, cb)
}

func (d *driver) NewSTM(cb func(col.STM) error) (*etcd.TxnResponse, error) {
	return col.NewSTM(d.pachClient.Ctx(), d.env.GetEtcdClient(), cb)
}

func (d *driver) WithData(
	inputs []*common.Input,
	inputTree *hashtree.Ordered,
	logger logs.TaggedLogger,
	cb func(string, *pps.ProcessStats) error,
) (retStats *pps.ProcessStats, retErr error) {
	puller := filesync.NewPuller()
	stats := &pps.ProcessStats{}

	// Download input data into a temporary directory
	// This can be interrupted via the pachClient using driver.WithContext
	dir, err := d.downloadData(logger, inputs, puller, stats, inputTree)
	// We run these cleanup functions no matter what, so that if
	// downloadData partially succeeded, we still clean up the resources.
	defer func() {
		if err := os.RemoveAll(dir); err != nil && retErr == nil {
			retErr = errors.EnsureStack(err)
		}
	}()
	// It's important that we run puller.CleanUp before os.RemoveAll,
	// because otherwise puller.Cleanup might try to open pipes that have
	// been deleted.
	defer func() {
		if _, err := puller.CleanUp(); err != nil && retErr == nil {
			retErr = errors.EnsureStack(err)
		}
	}()
	if err != nil {
		return stats, errors.EnsureStack(err)
	}
	if err := os.MkdirAll(d.InputDir(), 0777); err != nil {
		return stats, errors.EnsureStack(err)
	}
	// If the pipeline spec set a custom user to execute the process, make sure
	// the input directory and its content are owned by it
	if d.uid != nil && d.gid != nil {
		filepath.Walk(dir, func(name string, info os.FileInfo, err error) error {
			if err == nil {
				err = os.Chown(name, int(*d.uid), int(*d.gid))
			}
			return errors.EnsureStack(err)
		})
	}

	if err := cb(dir, stats); err != nil {
		return stats, err
	}

	// CleanUp is idempotent so we can call it however many times we want.
	// The reason we are calling it here is that the puller could've
	// encountered an error as it was lazily loading files, in which case
	// the output might be invalid since as far as the user's code is
	// concerned, they might've just seen an empty or partially completed
	// file.
	// TODO: do we really need two puller.CleanUps?
	downSize, err := puller.CleanUp()
	if err != nil {
		logger.Logf("puller encountered an error while cleaning up: %v", err)
		return stats, errors.EnsureStack(err)
	}

	atomic.AddUint64(&stats.DownloadBytes, uint64(downSize))
	d.reportDownloadSizeStats(float64(downSize), logger)
	return stats, nil
}

func (d *driver) downloadData(
	logger logs.TaggedLogger,
	inputs []*common.Input,
	puller *filesync.Puller,
	stats *pps.ProcessStats,
	statsTree *hashtree.Ordered,
) (_ string, retErr error) {
	defer d.reportDownloadTimeStats(time.Now(), stats, logger)
	logger.Logf("starting to download data")
	defer func(start time.Time) {
		if retErr != nil {
			logger.Logf("errored downloading data after %v: %v", time.Since(start), retErr)
		} else {
			logger.Logf("finished downloading data after %v", time.Since(start))
		}
	}(time.Now())

	// The scratch space is where Pachyderm stores downloaded and output data, which is
	// then symlinked into place for the pipeline.
	scratchPath := filepath.Join(d.InputDir(), client.PPSScratchSpace, uuid.NewWithoutDashes())

	// Clean up any files if an error occurs
	defer func() {
		if retErr != nil {
			os.RemoveAll(scratchPath)
		}
	}()

	outPath := filepath.Join(scratchPath, "out")
	// TODO: move this up into spout code
	if d.pipelineInfo.Spout != nil {
		// Spouts need to create a named pipe at /pfs/out
		if err := os.MkdirAll(filepath.Dir(outPath), 0700); err != nil {
			return "", errors.EnsureStack(err)
		}
		// Fifos don't exist on windows (where we may run this code in tests), so
		// this function is defined in a conditional-build file
		if err := createSpoutFifo(outPath); err != nil {
			return "", err
		}
		if d.PipelineInfo().Spout.Marker != "" {
			// check if we have a marker file in the /pfs/out directory
			_, err := d.PachClient().InspectFile(d.PipelineInfo().Pipeline.Name, ppsconsts.SpoutMarkerBranch, d.PipelineInfo().Spout.Marker)
			if err != nil {
				// if this fails because there's no head commit on the marker branch, then we don't want to pull the marker, but it's also not an error
				if !strings.Contains(err.Error(), "no head") {
					// if it fails for some other reason, then fail
					return "", errors.EnsureStack(err)
				}
			} else {
				// the file might be in the spout marker directory, and so we'll try pulling it from there
				if err := puller.Pull(
					d.PachClient(),
					filepath.Join(scratchPath, d.PipelineInfo().Spout.Marker),
					d.PipelineInfo().Pipeline.Name,
					ppsconsts.SpoutMarkerBranch,
					"/"+d.PipelineInfo().Spout.Marker,
					false,
					false,
					concurrency,
					nil,
					"",
				); err != nil {
					// this might fail if the marker branch hasn't been created, so check for that
					if err == nil || !strings.Contains(err.Error(), "branches") || !errutil.IsNotFoundError(err) {
						return "", errors.EnsureStack(err)
					}
					// if it just hasn't been created yet, that's fine and we should just continue as normal
				}
			}
		}
	} else if !d.PipelineInfo().S3Out {
		// Create output directory (typically /pfs/out)
		if err := os.MkdirAll(outPath, 0777); err != nil {
			return "", errors.Wrapf(err, "couldn't create %q", outPath)
		}
	}
	for _, input := range inputs {
		if input.GitURL != "" {
			if err := d.downloadGitData(scratchPath, input); err != nil {
				return "", err
			}
			continue
		}
		if input.S3 {
			continue // don't download any data
		}
		file := input.FileInfo.File
		fullInputPath := filepath.Join(scratchPath, input.Name, file.Path)
		var statsRoot string
		if statsTree != nil {
			statsRoot = filepath.Join(input.Name, file.Path)
			parent, _ := filepath.Split(statsRoot)
			statsTree.MkdirAll(parent)
		}
		if err := puller.Pull(
			d.pachClient,
			fullInputPath,
			file.Commit.Repo.Name,
			file.Commit.ID,
			file.Path,
			input.Lazy,
			input.EmptyFiles,
			concurrency,
			statsTree,
			statsRoot,
		); err != nil {
			return "", errors.EnsureStack(err)
		}
	}
	return scratchPath, nil
}

func (d *driver) downloadGitData(scratchPath string, input *common.Input) error {
	file := input.FileInfo.File

	var rawJSON bytes.Buffer
	err := d.pachClient.GetFile(file.Commit.Repo.Name, file.Commit.ID, file.Path, 0, 0, &rawJSON)
	if err != nil {
		return errors.EnsureStack(err)
	}

	var payload github.PushPayload
	err = json.Unmarshal(rawJSON.Bytes(), &payload)
	if err != nil {
		return errors.EnsureStack(err)
	}

	if payload.Repository.CloneURL == "" {
		return errors.New("git hook payload does not specify the upstream URL")
	} else if payload.Ref == "" {
		return errors.New("git hook payload does not specify the updated ref")
	} else if payload.After == "" {
		return errors.New("git hook payload does not specify the commit SHA")
	}

	// Clone checks out a reference, not a SHA. Github does not support fetching
	// an individual SHA.
	remoteURL := payload.Repository.CloneURL
	gitRepo, err := git.PlainCloneContext(
		d.pachClient.Ctx(),
		filepath.Join(scratchPath, input.Name),
		false,
		&git.CloneOptions{
			URL:           remoteURL,
			SingleBranch:  true,
			ReferenceName: gitPlumbing.ReferenceName(payload.Ref),
		},
	)
	if err != nil {
		return errors.Wrapf(err, "error fetching repo %v with ref %v from URL %v", input.Name, payload.Ref, remoteURL)
	}

	wt, err := gitRepo.Worktree()
	if err != nil {
		return errors.EnsureStack(err)
	}

	sha := payload.After
	err = wt.Checkout(&git.CheckoutOptions{Hash: gitPlumbing.NewHash(sha)})
	if err != nil {
		return errors.Wrapf(err, "error checking out SHA %v for repo %v", sha, input.Name)
	}

	// go-git will silently fail to checkout an invalid SHA and leave the HEAD at
	// the selected ref. Verify that we are now on the correct SHA
	rev, err := gitRepo.ResolveRevision("HEAD")
	if err != nil {
		return errors.Wrapf(err, "failed to inspect HEAD SHA for repo %v", input.Name)
	} else if rev.String() != sha {
		return errors.Errorf("could not find SHA %v for repo %v", sha, input.Name)
	}

	return nil
}

// Run user code and return the combined output of stdout and stderr.
func (d *driver) RunUserCode(
	logger logs.TaggedLogger,
	environ []string,
	procStats *pps.ProcessStats,
	rawDatumTimeout *types.Duration,
) (retErr error) {
	ctx := d.pachClient.Ctx()
	d.reportUserCodeStats(logger)
	defer func(start time.Time) { d.reportDeferredUserCodeStats(retErr, start, procStats, logger) }(time.Now())
	logger.Logf("beginning to run user code")
	defer func(start time.Time) {
		if retErr != nil {
			logger.Logf("errored running user code after %v: %v", time.Since(start), retErr)
		} else {
			logger.Logf("finished running user code after %v", time.Since(start))
		}
	}(time.Now())
	if rawDatumTimeout != nil {
		datumTimeout, err := types.DurationFromProto(rawDatumTimeout)
		if err != nil {
			return errors.EnsureStack(err)
		}
		datumTimeoutCtx, cancel := context.WithTimeout(ctx, datumTimeout)
		defer cancel()
		ctx = datumTimeoutCtx
	}

	if len(d.pipelineInfo.Transform.Cmd) == 0 {
		return errors.New("invalid pipeline transform, no command specified")
	}

	// Run user code
	cmd := exec.CommandContext(ctx, d.pipelineInfo.Transform.Cmd[0], d.pipelineInfo.Transform.Cmd[1:]...)
	if d.pipelineInfo.Transform.Stdin != nil {
		cmd.Stdin = strings.NewReader(strings.Join(d.pipelineInfo.Transform.Stdin, "\n") + "\n")
	}
	cmd.Stdout = logger.WithUserCode()
	cmd.Stderr = logger.WithUserCode()
	cmd.Env = environ
	if d.uid != nil && d.gid != nil {
		cmd.SysProcAttr = makeCmdCredentials(*d.uid, *d.gid)
	}
	cmd.Dir = filepath.Join(d.rootDir, d.pipelineInfo.Transform.WorkingDir)
	err := cmd.Start()
	if err != nil {
		return errors.EnsureStack(err)
	}
	// A context with a deadline will successfully cancel/kill
	// the running process (minus zombies)
	state, err := cmd.Process.Wait()
	if err != nil {
		return errors.EnsureStack(err)
	}
	if common.IsDone(ctx) {
		if err = ctx.Err(); err != nil {
			return errors.EnsureStack(err)
		}
	}

	// Because of this issue: https://github.com/golang/go/issues/18874
	// We forked os/exec so that we can call just the part of cmd.Wait() that
	// happens after blocking on the process. Unfortunately calling
	// cmd.Process.Wait() then cmd.Wait() will produce an error. So instead we
	// close the IO using this helper
	err = cmd.WaitIO(state, err)
	// We ignore broken pipe errors, these occur very occasionally if a user
	// specifies Stdin but their process doesn't actually read everything from
	// Stdin. This is a fairly common thing to do, bash by default ignores
	// broken pipe errors.
	if err != nil && !strings.Contains(err.Error(), "broken pipe") {
		// (if err is an acceptable return code, don't return err)
		exiterr := &exec.ExitError{}
		if errors.As(err, &exiterr) {
			if status, ok := exiterr.Sys().(syscall.WaitStatus); ok {
				for _, returnCode := range d.pipelineInfo.Transform.AcceptReturnCode {
					if int(returnCode) == status.ExitStatus() {
						return nil
					}
				}
			}
		}
		return errors.EnsureStack(err)
	}
	return nil
}

// Run user error code and return the combined output of stdout and stderr.
func (d *driver) RunUserErrorHandlingCode(logger logs.TaggedLogger, environ []string, procStats *pps.ProcessStats, rawDatumTimeout *types.Duration) (retErr error) {
	ctx := d.pachClient.Ctx()
	logger.Logf("beginning to run user error handling code")
	defer func(start time.Time) {
		if retErr != nil {
			logger.Logf("errored running user error handling code after %v: %v", time.Since(start), retErr)
		} else {
			logger.Logf("finished running user error handling code after %v", time.Since(start))
		}
	}(time.Now())

	cmd := exec.CommandContext(ctx, d.pipelineInfo.Transform.ErrCmd[0], d.pipelineInfo.Transform.ErrCmd[1:]...)
	if d.pipelineInfo.Transform.ErrStdin != nil {
		cmd.Stdin = strings.NewReader(strings.Join(d.pipelineInfo.Transform.ErrStdin, "\n") + "\n")
	}
	cmd.Stdout = logger.WithUserCode()
	cmd.Stderr = logger.WithUserCode()
	cmd.Env = environ
	if d.uid != nil && d.gid != nil {
		cmd.SysProcAttr = makeCmdCredentials(*d.uid, *d.gid)
	}
	cmd.Dir = d.pipelineInfo.Transform.WorkingDir
	err := cmd.Start()
	if err != nil {
		return errors.EnsureStack(err)
	}
	// A context w a deadline will successfully cancel/kill
	// the running process (minus zombies)
	state, err := cmd.Process.Wait()
	if err != nil {
		return errors.EnsureStack(err)
	}
	if common.IsDone(ctx) {
		if err = ctx.Err(); err != nil {
			return errors.EnsureStack(err)
		}
	}
	// Because of this issue: https://github.com/golang/go/issues/18874
	// We forked os/exec so that we can call just the part of cmd.Wait() that
	// happens after blocking on the process. Unfortunately calling
	// cmd.Process.Wait() then cmd.Wait() will produce an error. So instead we
	// close the IO using this helper
	err = cmd.WaitIO(state, err)
	// We ignore broken pipe errors, these occur very occasionally if a user
	// specifies Stdin but their process doesn't actually read everything from
	// Stdin. This is a fairly common thing to do, bash by default ignores
	// broken pipe errors.
	if err != nil && !strings.Contains(err.Error(), "broken pipe") {
		// (if err is an acceptable return code, don't return err)
		exiterr := &exec.ExitError{}
		if errors.As(err, &exiterr) {
			if status, ok := exiterr.Sys().(syscall.WaitStatus); ok {
				for _, returnCode := range d.pipelineInfo.Transform.AcceptReturnCode {
					if int(returnCode) == status.ExitStatus() {
						return nil
					}
				}
			}
		}
		return errors.EnsureStack(err)
	}
	return nil
}

func (d *driver) UpdateJobState(jobID string, state pps.JobState, reason string) error {
	_, err := d.NewSTM(func(stm col.STM) error {
		jobPtr := &pps.EtcdJobInfo{}
		if err := d.Jobs().ReadWrite(stm).Get(jobID, jobPtr); err != nil {
			return errors.EnsureStack(err)
		}
		return errors.EnsureStack(ppsutil.UpdateJobState(d.Pipelines().ReadWrite(stm), d.Jobs().ReadWrite(stm), jobPtr, state, reason))
	})
	return errors.EnsureStack(err)
}

// DeleteJob is identical to updateJobState, except that jobPtr points to a job
// that should be deleted rather than marked failed. Jobs may be deleted if
// their output commit is deleted.
func (d *driver) DeleteJob(stm col.STM, jobPtr *pps.EtcdJobInfo) error {
	pipelinePtr := &pps.EtcdPipelineInfo{}
	if err := d.Pipelines().ReadWrite(stm).Update(jobPtr.Pipeline.Name, pipelinePtr, func() error {
		if pipelinePtr.JobCounts == nil {
			pipelinePtr.JobCounts = make(map[int32]int32)
		}
		if pipelinePtr.JobCounts[int32(jobPtr.State)] != 0 {
			pipelinePtr.JobCounts[int32(jobPtr.State)]--
		}
		return nil
	}); err != nil {
		return errors.EnsureStack(err)
	}
	return errors.EnsureStack(d.Jobs().ReadWrite(stm).Delete(jobPtr.Job.ID))
}

func (d *driver) updateCounter(
	stat *prometheus.CounterVec,
	logger logs.TaggedLogger,
	state string,
	cb func(prometheus.Counter),
) {
	labels := []string{d.pipelineInfo.ID, logger.JobID()}
	if state != "" {
		labels = append(labels, state)
	}
	if counter, err := stat.GetMetricWithLabelValues(labels...); err != nil {
		logger.Logf("failed to get counter with labels (%v): %v", labels, err)
	} else {
		cb(counter)
	}
}

func (d *driver) updateHistogram(
	stat *prometheus.HistogramVec,
	logger logs.TaggedLogger,
	state string,
	cb func(prometheus.Observer),
) {
	labels := []string{d.pipelineInfo.ID, logger.JobID()}
	if state != "" {
		labels = append(labels, state)
	}
	if hist, err := stat.GetMetricWithLabelValues(labels...); err != nil {
		logger.Logf("failed to get histogram with labels (%v): %v", labels, err)
	} else {
		cb(hist)
	}
}

func (d *driver) reportUserCodeStats(logger logs.TaggedLogger) {
	if d.exportStats {
		d.updateCounter(stats.DatumCount, logger, "started", func(counter prometheus.Counter) {
			counter.Add(1)
		})
	}
}

func (d *driver) reportDeferredUserCodeStats(
	err error,
	start time.Time,
	procStats *pps.ProcessStats,
	logger logs.TaggedLogger,
) {
	duration := time.Since(start)
	procStats.ProcessTime = types.DurationProto(duration)

	if d.exportStats {
		state := "errored"
		if err == nil {
			state = "finished"
		}

		d.updateCounter(stats.DatumCount, logger, state, func(counter prometheus.Counter) {
			counter.Add(1)
		})
		d.updateHistogram(stats.DatumProcTime, logger, state, func(hist prometheus.Observer) {
			hist.Observe(duration.Seconds())
		})
		d.updateCounter(stats.DatumProcSecondsCount, logger, "", func(counter prometheus.Counter) {
			counter.Add(duration.Seconds())
		})
	}
}

func (d *driver) ReportUploadStats(
	start time.Time,
	procStats *pps.ProcessStats,
	logger logs.TaggedLogger,
) {
	duration := time.Since(start)
	procStats.UploadTime = types.DurationProto(duration)

	if d.exportStats {
		d.updateHistogram(stats.DatumUploadTime, logger, "", func(hist prometheus.Observer) {
			hist.Observe(duration.Seconds())
		})
		d.updateCounter(stats.DatumUploadSecondsCount, logger, "", func(counter prometheus.Counter) {
			counter.Add(duration.Seconds())
		})
		d.updateHistogram(stats.DatumUploadSize, logger, "", func(hist prometheus.Observer) {
			hist.Observe(float64(procStats.UploadBytes))
		})
		d.updateCounter(stats.DatumUploadBytesCount, logger, "", func(counter prometheus.Counter) {
			counter.Add(float64(procStats.UploadBytes))
		})
	}
}

func (d *driver) reportDownloadSizeStats(
	downSize float64,
	logger logs.TaggedLogger,
) {
	if d.exportStats {
		d.updateHistogram(stats.DatumDownloadSize, logger, "", func(hist prometheus.Observer) {
			hist.Observe(downSize)
		})
		d.updateCounter(stats.DatumDownloadBytesCount, logger, "", func(counter prometheus.Counter) {
			counter.Add(downSize)
		})
	}
}

func (d *driver) reportDownloadTimeStats(
	start time.Time,
	procStats *pps.ProcessStats,
	logger logs.TaggedLogger,
) {
	duration := time.Since(start)
	procStats.DownloadTime = types.DurationProto(duration)

	if d.exportStats {

		d.updateHistogram(stats.DatumDownloadTime, logger, "", func(hist prometheus.Observer) {
			hist.Observe(duration.Seconds())
		})
		d.updateCounter(stats.DatumDownloadSecondsCount, logger, "", func(counter prometheus.Counter) {
			counter.Add(duration.Seconds())
		})
	}
}

func (d *driver) unlinkData(inputs []*common.Input) error {
	entries, err := ioutil.ReadDir(d.InputDir())
	if err != nil {
		return errors.EnsureStack(err)
	}
	for _, entry := range entries {
		if entry.Name() == client.PPSScratchSpace {
			continue // don't delete scratch space
		}
		if err := os.RemoveAll(filepath.Join(d.InputDir(), entry.Name())); err != nil {
			return errors.EnsureStack(err)
		}
	}
	return nil
}

func (d *driver) UploadOutput(
	dir string,
	tag string,
	logger logs.TaggedLogger,
	inputs []*common.Input,
	stats *pps.ProcessStats,
	statsTree *hashtree.Ordered,
) (retBuffer []byte, retErr error) {
	defer d.ReportUploadStats(time.Now(), stats, logger)
	logger.Logf("starting to upload output")
	defer func(start time.Time) {
		if retErr != nil {
			logger.Logf("errored uploading output after %v: %v", time.Since(start), retErr)
		} else {
			logger.Logf("finished uploading output after %v", time.Since(start))
		}
	}(time.Now())

	// Set up client for writing file data
	putObjsClient, err := d.pachClient.ObjectAPIClient.PutObjects(d.pachClient.Ctx())
	if err != nil {
		return nil, errors.EnsureStack(err)
	}
	block := &pfs.Block{Hash: uuid.NewWithoutDashes()}
	if err := putObjsClient.Send(&pfs.PutObjectRequest{
		Block: block,
	}); err != nil {
		return nil, errors.EnsureStack(err)
	}
	outputPath := filepath.Join(dir, "out")
	buf := grpcutil.GetBuffer()
	defer grpcutil.PutBuffer(buf)
	var offset uint64
	var tree *hashtree.Ordered

	// Upload all files in output directory
	if err := filepath.Walk(outputPath, func(filePath string, info os.FileInfo, err error) error {
		if err != nil {
			return errors.EnsureStack(err)
		}
		if !utf8.ValidString(filePath) {
			return errors.Errorf("file path is not valid utf-8: %s", filePath)
		}
		if filePath == outputPath {
			tree = hashtree.NewOrdered("/")
			return nil
		}
		relPath, err := filepath.Rel(outputPath, filePath)
		if err != nil {
			return errors.EnsureStack(err)
		}
		// Put directory. Even if the directory is empty, that may be useful to
		// users
		// TODO(msteffen) write a test pipeline that outputs an empty directory and
		// make sure it's preserved
		if info.IsDir() {
			tree.PutDir(relPath)
			if statsTree != nil {
				statsTree.PutDir(relPath)
			}
			return nil
		}
		// Under some circumstances, the user might have copied
		// some pipes from the input directory to the output directory.
		// Reading from these files will result in job blocking.  Thus
		// we preemptively detect if the file is a named pipe.
		if (info.Mode() & os.ModeNamedPipe) > 0 {
			logger.Logf("cannot upload named pipe: %v", relPath)
			return errors.EnsureStack(errSpecialFile)
		}
		// If the output file is a symlink to an input file, we can skip
		// the uploading.
		if (info.Mode() & os.ModeSymlink) > 0 {
			realPath, err := os.Readlink(filePath)
			if err != nil {
				return errors.EnsureStack(err)
			}

			// We can only skip the upload if the real path is
			// under /pfs, meaning that it's a file that already
			// exists in PFS.
			if strings.HasPrefix(realPath, d.InputDir()) {
				if pathWithInput, err := filepath.Rel(dir, realPath); err == nil {
					// The name of the input
					inputName := strings.Split(pathWithInput, string(os.PathSeparator))[0]
					var input *common.Input
					for _, i := range inputs {
						if i.Name == inputName {
							input = i
						}
					}
					// this changes realPath from `/pfs/input/...` to `/scratch/<id>/input/...`
					realPath = filepath.Join(dir, pathWithInput)
					if input != nil {
						return filepath.Walk(realPath, func(filePath string, info os.FileInfo, err error) error {
							if err != nil {
								return errors.EnsureStack(err)
							}
							rel, err := filepath.Rel(realPath, filePath)
							if err != nil {
								return errors.EnsureStack(err)
							}
							subRelPath := filepath.Join(relPath, rel)
							// The path of the input file
							pfsPath, err := filepath.Rel(filepath.Join(dir, input.Name), filePath)
							if err != nil {
								return errors.EnsureStack(err)
							}
							if info.IsDir() {
								tree.PutDir(subRelPath)
								if statsTree != nil {
									statsTree.PutDir(subRelPath)
								}
								return nil
							}
							fc := input.FileInfo.File.Commit
							fileInfo, err := d.pachClient.InspectFile(fc.Repo.Name, fc.ID, pfsPath)
							if err != nil {
								return errors.EnsureStack(err)
							}
							var blockRefs []*pfs.BlockRef
							for _, object := range fileInfo.Objects {
								objectInfo, err := d.pachClient.InspectObject(object.Hash)
								if err != nil {
									return errors.EnsureStack(err)
								}
								blockRefs = append(blockRefs, objectInfo.BlockRef)
							}
							blockRefs = append(blockRefs, fileInfo.BlockRefs...)
							n := &hashtree.FileNodeProto{BlockRefs: blockRefs}
							tree.PutFile(subRelPath, fileInfo.Hash, int64(fileInfo.SizeBytes), n)
							if statsTree != nil {
								statsTree.PutFile(subRelPath, fileInfo.Hash, int64(fileInfo.SizeBytes), n)
							}
							return nil
						})
					}
				}
			}
		}
		// Open local file that is being uploaded
		f, err := os.Open(filePath)
		if err != nil {
			// if the error is that the spout marker file is missing, that's fine, just skip to the next file
			if d.PipelineInfo().Spout != nil {
				if strings.Contains(err.Error(), filepath.Join("out", d.PipelineInfo().Spout.Marker)) {
					return nil
				}
			}
			return errors.Wrapf(err, "os.Open(%s)", filePath)
		}
		defer func() {
			if err := f.Close(); err != nil && retErr == nil {
				retErr = err
			}
		}()
		var size int64
		h := pfs.NewHash()
		r := io.TeeReader(f, h)
		// Write local file to object storage block
		for {
			n, err := r.Read(buf)
			if n == 0 && err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return errors.EnsureStack(err)
			}
			if err := putObjsClient.Send(&pfs.PutObjectRequest{
				Value: buf[:n],
			}); err != nil {
				return errors.EnsureStack(err)
			}
			size += int64(n)
		}
		n := &hashtree.FileNodeProto{
			BlockRefs: []*pfs.BlockRef{
				&pfs.BlockRef{
					Block: block,
					Range: &pfs.ByteRange{
						Lower: offset,
						Upper: offset + uint64(size),
					},
				},
			},
		}
		hash := h.Sum(nil)
		tree.PutFile(relPath, hash, size, n)
		if statsTree != nil {
			statsTree.PutFile(relPath, hash, size, n)
		}
		offset += uint64(size)
		stats.UploadBytes += uint64(size)
		return nil
	}); err != nil {
		return nil, errors.Wrap(err, "error walking output")
	}
	if _, err := putObjsClient.CloseAndRecv(); err != nil && !errors.Is(err, io.EOF) {
		return nil, errors.EnsureStack(err)
	}
	// Serialize datum hashtree
	b := &bytes.Buffer{}
	if err := tree.Serialize(b); err != nil {
		return nil, err
	}
	// Write datum hashtree to object storage
	w, err := d.pachClient.PutObjectAsync([]*pfs.Tag{client.NewTag(tag)})
	if err != nil {
		return nil, errors.EnsureStack(err)
	}
	defer func() {
		if err := w.Close(); err != nil && retErr != nil {
			retErr = errors.EnsureStack(err)
		}
	}()
	if _, err := w.Write(b.Bytes()); err != nil {
		return nil, errors.EnsureStack(err)
	}
	return b.Bytes(), nil
}

func (d *driver) UserCodeEnv(
	jobID string,
	outputCommit *pfs.Commit,
	inputs []*common.Input,
) []string {
	result := os.Environ()

	for _, input := range inputs {
		result = append(result, fmt.Sprintf("%s=%s", input.Name, filepath.Join(d.InputDir(), input.Name, input.FileInfo.File.Path)))
		result = append(result, fmt.Sprintf("%s_COMMIT=%s", input.Name, input.FileInfo.File.Commit.ID))
	}

	if jobID != "" {
		result = append(result, fmt.Sprintf("%s=%s", client.JobIDEnv, jobID))

		if ppsutil.ContainsS3Inputs(d.PipelineInfo().Input) || d.PipelineInfo().S3Out {
			result = append(
				result,
				fmt.Sprintf("S3_ENDPOINT=http://%s.%s:%s",
					ppsutil.SidecarS3GatewayService(jobID),
					d.env.Namespace,
					d.env.S3GatewayPort,
				),
			)
		}
	}

	if outputCommit != nil {
		result = append(result, fmt.Sprintf("%s=%s", client.OutputCommitIDEnv, outputCommit.ID))
	}

	return result
}

func (d *driver) Egress(commit *pfs.Commit, egressURL string) error {
	// copy the pach client (preserving auth info) so we can set a different
	// number of concurrent streams
	pachClient := d.PachClient().WithCtx(d.PachClient().Ctx())
	pachClient.SetMaxConcurrentStreams(100)

	url, err := obj.ParseURL(egressURL)
	if err != nil {
		return err
	}
	objClient, err := obj.NewClientFromURLAndSecret(url, false)
	if err != nil {
		return err
	}
	return filesync.PushObj(pachClient, commit, objClient, url.Object)
}

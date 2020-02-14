package worker

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/user"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/LK4D4/joincontext"
	etcd "github.com/coreos/etcd/clientv3"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/types"
	"golang.org/x/net/context"
	"golang.org/x/sync/errgroup"
	"gopkg.in/go-playground/webhooks.v5/github"
	"gopkg.in/src-d/go-git.v4"
	gitPlumbing "gopkg.in/src-d/go-git.v4/plumbing"

	docker "github.com/fsouza/go-dockerclient"
	"github.com/pachyderm/pachyderm/src/client"
	"github.com/pachyderm/pachyderm/src/client/enterprise"
	"github.com/pachyderm/pachyderm/src/client/limit"
	"github.com/pachyderm/pachyderm/src/client/pfs"
	"github.com/pachyderm/pachyderm/src/client/pkg/grpcutil"
	"github.com/pachyderm/pachyderm/src/client/pkg/pbutil"
	"github.com/pachyderm/pachyderm/src/client/pkg/tracing"
	"github.com/pachyderm/pachyderm/src/client/pkg/tracing/extended"
	"github.com/pachyderm/pachyderm/src/client/pps"
	pfsserver "github.com/pachyderm/pachyderm/src/server/pfs"
	"github.com/pachyderm/pachyderm/src/server/pkg/backoff"
	col "github.com/pachyderm/pachyderm/src/server/pkg/collection"
	"github.com/pachyderm/pachyderm/src/server/pkg/errutil"
	"github.com/pachyderm/pachyderm/src/server/pkg/exec"
	"github.com/pachyderm/pachyderm/src/server/pkg/hashtree"
	"github.com/pachyderm/pachyderm/src/server/pkg/obj"
	"github.com/pachyderm/pachyderm/src/server/pkg/ppsconsts"
	"github.com/pachyderm/pachyderm/src/server/pkg/ppsdb"
	"github.com/pachyderm/pachyderm/src/server/pkg/ppsutil"
	filesync "github.com/pachyderm/pachyderm/src/server/pkg/sync"
	"github.com/pachyderm/pachyderm/src/server/pkg/uuid"
	"github.com/pachyderm/pachyderm/src/server/pkg/watch"
)

const (
	// The maximum number of concurrent download/upload operations
	concurrency = 100
	logBuffer   = 25

	planPrefix        = "/plan"
	chunkPrefix       = "/chunk"
	mergePrefix       = "/merge"
	shardPrefix       = "/shard"
	shardTTL          = 30
	noShard           = int64(-1)
	parentTreeBufSize = 50 * (1 << (10 * 2))
)

type ctxKey int

const (
	shardKey ctxKey = iota
)

var (
	errSpecialFile    = errors.New("cannot upload special file")
	errDatumRecovered = errors.New("the datum errored, and the error was handled successfully")
	statsTagSuffix    = "_stats"
)

// APIServer implements the worker API
type APIServer struct {
	pachClient *client.APIClient
	etcdClient *etcd.Client
	etcdPrefix string

	// Information needed to process input data and upload output
	pipelineInfo *pps.PipelineInfo

	// Information attached to log lines
	logMsgTemplate pps.LogMessage

	// The k8s pod name of this worker
	workerName string

	statusMu sync.Mutex

	// The currently running job ID
	jobID string
	// The currently running data
	data []*Input
	// The time we started the currently running
	started time.Time
	// Func to cancel the currently running datum
	cancel func()
	// Stats about the execution of the job
	stats *pps.ProcessStats
	// queueSize is the number of items enqueued
	queueSize int64

	// The namespace in which pachyderm is deployed
	namespace string
	// The jobs collection
	jobs col.Collection
	// The pipelines collection
	pipelines col.Collection
	// The plans collection
	// Stores chunk layout and merges
	plans col.Collection
	// The shards collection
	// Stores available filesystem shards for a pipeline, workers will claim these
	shards col.Collection

	// Only one datum can be running at a time because they need to be
	// accessing /pfs, runMu enforces this
	runMu sync.Mutex

	// We only export application statistics if enterprise is enabled
	exportStats bool

	uid *uint32
	gid *uint32

	// hashtreeStorage is the where we store on disk hashtrees
	hashtreeStorage string

	// numShards is the number of filesystem shards for the output of this pipeline
	numShards int64
	// claimedShard communicates the context for the shard that was claimed
	claimedShard chan context.Context
	// shardCtx is the context for the shard this worker has claimed
	shardCtx context.Context
	// shard is the shard this worker has claimed
	shard int64
	// chunkCache caches chunk hashtrees during a job and can merge them (chunkStatsCache applies to stats)
	chunkCache, chunkStatsCache *hashtree.MergeCache
	// datumCache caches datum hashtrees during a job and can merge them (datumStatsCache applies to stats)
	datumCache, datumStatsCache *hashtree.MergeCache
	// clients are the worker clients (used for the shuffle step by mergers)
	clients map[string]Client
}

type taggedLogger struct {
	template     pps.LogMessage
	stderrLog    log.Logger
	marshaler    *jsonpb.Marshaler
	buffer       bytes.Buffer
	putObjClient pfs.ObjectAPI_PutObjectClient
	objSize      int64
	msgCh        chan string
	eg           errgroup.Group
}

// DatumID computes the id for a datum, this value is used in ListDatum and
// InspectDatum.
func (a *APIServer) DatumID(data []*Input) string {
	hash := sha256.New()
	for _, d := range data {
		hash.Write([]byte(d.FileInfo.File.Path))
		hash.Write(d.FileInfo.Hash)
	}
	// InputFileID is a single string id for the data from this input, it's used in logs and in
	// the statsTree
	return hex.EncodeToString(hash.Sum(nil))
}

func (a *APIServer) getTaggedLogger(pachClient *client.APIClient, jobID string, data []*Input, enableStats bool) (*taggedLogger, error) {
	result := &taggedLogger{
		template:  a.logMsgTemplate, // Copy struct
		stderrLog: log.Logger{},
		marshaler: &jsonpb.Marshaler{},
		msgCh:     make(chan string, logBuffer),
	}
	result.stderrLog.SetOutput(os.Stderr)
	result.stderrLog.SetFlags(log.LstdFlags | log.Llongfile) // Log file/line

	// Add Job ID to log metadata
	result.template.JobID = jobID

	// Add inputs' details to log metadata, so we can find these logs later
	for _, d := range data {
		result.template.Data = append(result.template.Data, &pps.InputFile{
			Path: d.FileInfo.File.Path,
			Hash: d.FileInfo.Hash,
		})
	}
	// InputFileID is a single string id for the data from this input, it's used in logs and in
	// the statsTree
	result.template.DatumID = a.DatumID(data)
	if enableStats {
		putObjClient, err := pachClient.ObjectAPIClient.PutObject(pachClient.Ctx())
		if err != nil {
			return nil, err
		}
		result.putObjClient = putObjClient
		result.eg.Go(func() error {
			for msg := range result.msgCh {
				for _, chunk := range grpcutil.Chunk([]byte(msg), grpcutil.MaxMsgSize/2) {
					if err := result.putObjClient.Send(&pfs.PutObjectRequest{
						Value: chunk,
					}); err != nil && err != io.EOF {
						return err
					}
				}
				result.objSize += int64(len(msg))
			}
			return nil
		})
	}
	return result, nil
}

// Logf logs the line Sprintf(formatString, args...), but formatted as a json
// message and annotated with all of the metadata stored in 'loginfo'.
//
// Note: this is not thread-safe, as it modifies fields of 'logger.template'
func (logger *taggedLogger) Logf(formatString string, args ...interface{}) {
	logger.template.Message = fmt.Sprintf(formatString, args...)
	if ts, err := types.TimestampProto(time.Now()); err == nil {
		logger.template.Ts = ts
	} else {
		logger.stderrLog.Printf("could not generate logging timestamp: %s\n", err)
		return
	}
	msg, err := logger.marshaler.MarshalToString(&logger.template)
	if err != nil {
		logger.stderrLog.Printf("could not marshal %v for logging: %s\n", &logger.template, err)
		return
	}
	fmt.Println(msg)
	if logger.putObjClient != nil {
		logger.msgCh <- msg + "\n"
	}
}

func (logger *taggedLogger) LogStep(name string, f func() error) (retErr error) {
	logger.Logf("started %v", name)
	defer func() {
		if retErr != nil {
			retErr = fmt.Errorf("errored %v: %v", name, retErr)
		} else {
			logger.Logf("finished %v", name)
		}
	}()
	return f()
}

func (logger *taggedLogger) Write(p []byte) (_ int, retErr error) {
	// never errors
	logger.buffer.Write(p)
	r := bufio.NewReader(&logger.buffer)
	for {
		message, err := r.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				logger.buffer.Write([]byte(message))
				return len(p), nil
			}
			// this shouldn't technically be possible to hit io.EOF should be
			// the only error bufio.Reader can return when using a buffer.
			return 0, fmt.Errorf("error ReadString: %v", err)
		}
		// We don't want to make this call as:
		// logger.Logf(message)
		// because if the message has format characters like %s in it those
		// will result in errors being logged.
		logger.Logf("%s", strings.TrimSuffix(message, "\n"))
	}
}

func (logger *taggedLogger) Close() (*pfs.Object, int64, error) {
	close(logger.msgCh)
	if logger.putObjClient != nil {
		if err := logger.eg.Wait(); err != nil {
			return nil, 0, err
		}
		object, err := logger.putObjClient.CloseAndRecv()
		// we set putObjClient to nil so that future calls to Logf won't send
		// msg down logger.msgCh as we've just closed that channel.
		logger.putObjClient = nil
		return object, logger.objSize, err
	}
	return nil, 0, nil
}

func (logger *taggedLogger) clone() *taggedLogger {
	return &taggedLogger{
		template:     logger.template, // Copy struct
		stderrLog:    log.Logger{},
		marshaler:    &jsonpb.Marshaler{},
		putObjClient: logger.putObjClient,
		msgCh:        logger.msgCh,
	}
}

func (logger *taggedLogger) userLogger() *taggedLogger {
	result := logger.clone()
	result.template.User = true
	return result
}

// NewAPIServer creates an APIServer for a given pipeline
func NewAPIServer(pachClient *client.APIClient, etcdClient *etcd.Client, etcdPrefix string, pipelineInfo *pps.PipelineInfo, workerName string, namespace string, hashtreeStorage string) (*APIServer, error) {
	initPrometheus()

	span, ctx := extended.AddPipelineSpanToAnyTrace(pachClient.Ctx(),
		etcdClient, pipelineInfo.Pipeline.Name, "/worker/Start")
	oldPachClient := pachClient // don't use tracing in apiServer.pachClient
	if span != nil {
		pachClient = pachClient.WithCtx(ctx)
	}
	defer tracing.FinishAnySpan(span)

	server := &APIServer{
		pachClient:   oldPachClient,
		etcdClient:   etcdClient,
		etcdPrefix:   etcdPrefix,
		pipelineInfo: pipelineInfo,
		logMsgTemplate: pps.LogMessage{
			PipelineName: pipelineInfo.Pipeline.Name,
			WorkerID:     os.Getenv(client.PPSPodNameEnv),
		},
		workerName:      workerName,
		namespace:       namespace,
		jobs:            ppsdb.Jobs(etcdClient, etcdPrefix),
		pipelines:       ppsdb.Pipelines(etcdClient, etcdPrefix),
		plans:           col.NewCollection(etcdClient, path.Join(etcdPrefix, planPrefix), nil, &Plan{}, nil, nil),
		shards:          col.NewCollection(etcdClient, path.Join(etcdPrefix, shardPrefix, pipelineInfo.Pipeline.Name), nil, &ShardInfo{}, nil, nil),
		hashtreeStorage: hashtreeStorage,
		claimedShard:    make(chan context.Context, 1),
		shard:           noShard,
		clients:         make(map[string]Client),
	}
	logger, err := server.getTaggedLogger(pachClient, "", nil, false)
	if err != nil {
		return nil, err
	}
	resp, err := pachClient.Enterprise.GetState(context.Background(), &enterprise.GetStateRequest{})
	if err != nil {
		logger.Logf("failed to get enterprise state with error: %v\n", err)
	} else {
		server.exportStats = resp.State == enterprise.State_ACTIVE
	}
	numShards, err := ppsutil.GetExpectedNumHashtrees(pipelineInfo.HashtreeSpec)
	if err != nil {
		logger.Logf("error getting number of shards, default to 1 shard: %v", err)
		numShards = 1
	}
	server.numShards = numShards
	root := filepath.Join(hashtreeStorage, uuid.NewWithoutDashes())
	if err := os.MkdirAll(filepath.Join(root, "chunk", "stats"), 0777); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Join(root, "datum", "stats"), 0777); err != nil {
		return nil, err
	}
	server.chunkCache = hashtree.NewMergeCache(filepath.Join(root, "chunk"))
	server.chunkStatsCache = hashtree.NewMergeCache(filepath.Join(root, "chunk", "stats"))
	server.datumCache = hashtree.NewMergeCache(filepath.Join(root, "datum"))
	server.datumStatsCache = hashtree.NewMergeCache(filepath.Join(root, "datum", "stats"))
	var noDocker bool
	if _, err := os.Stat("/var/run/docker.sock"); err != nil {
		noDocker = true
	}
	if pipelineInfo.Transform.Image != "" && !noDocker {
		docker, err := docker.NewClientFromEnv()
		if err != nil {
			return nil, err
		}
		image, err := docker.InspectImage(pipelineInfo.Transform.Image)
		if err != nil {
			return nil, fmt.Errorf("error inspecting image %s: %+v", pipelineInfo.Transform.Image, err)
		}
		if pipelineInfo.Transform.User == "" {
			pipelineInfo.Transform.User = image.Config.User
		}
		if pipelineInfo.Transform.WorkingDir == "" {
			pipelineInfo.Transform.WorkingDir = image.Config.WorkingDir
		}
		if server.pipelineInfo.Transform.Cmd == nil {
			if len(image.Config.Entrypoint) == 0 {
				ppsutil.FailPipeline(ctx, etcdClient, server.pipelines,
					pipelineInfo.Pipeline.Name,
					"nothing to run: no transform.cmd and no entrypoint")
			}
			server.pipelineInfo.Transform.Cmd = image.Config.Entrypoint
		}
	}
	if pipelineInfo.Transform.User != "" {
		user, err := lookupDockerUser(pipelineInfo.Transform.User)
		if err != nil && !os.IsNotExist(err) {
			return nil, err
		}
		// If `user` is `nil`, `uid` and `gid` will get set, and we won't
		// customize the user that executes the worker process.
		if user != nil { // user is nil when os.IsNotExist(err) is true in which case we use root
			uid, err := strconv.ParseUint(user.Uid, 10, 32)
			if err != nil {
				return nil, err
			}
			uid32 := uint32(uid)
			server.uid = &uid32
			gid, err := strconv.ParseUint(user.Gid, 10, 32)
			if err != nil {
				return nil, err
			}
			gid32 := uint32(gid)
			server.gid = &gid32
		}
	}
	switch {
	case pipelineInfo.Service != nil:
		go server.master("service", server.serviceSpawner)
	case pipelineInfo.Spout != nil:
		go server.master("spout", server.spoutSpawner)
	default:
		go server.master("pipeline", server.jobSpawner)
	}
	go server.worker()
	return server, nil
}

func (a *APIServer) downloadGitData(pachClient *client.APIClient, dir string, input *Input) error {
	file := input.FileInfo.File
	pachydermRepoName := input.Name
	var rawJSON bytes.Buffer
	err := pachClient.GetFile(pachydermRepoName, file.Commit.ID, "commit.json", 0, 0, &rawJSON)
	if err != nil {
		return err
	}
	var payload github.PushPayload
	err = json.Unmarshal(rawJSON.Bytes(), &payload)
	if err != nil {
		return err
	}
	sha := payload.After
	// Clone checks out a reference, not a SHA
	r, err := git.PlainClone(
		filepath.Join(dir, pachydermRepoName),
		false,
		&git.CloneOptions{
			URL:           payload.Repository.CloneURL,
			SingleBranch:  true,
			ReferenceName: gitPlumbing.ReferenceName(payload.Ref),
		},
	)
	if err != nil {
		return fmt.Errorf("error cloning repo %v at SHA %v from URL %v: %v", pachydermRepoName, sha, input.GitURL, err)
	}
	hash := gitPlumbing.NewHash(sha)
	wt, err := r.Worktree()
	if err != nil {
		return err
	}
	err = wt.Checkout(&git.CheckoutOptions{Hash: hash})
	if err != nil {
		return fmt.Errorf("error checking out SHA %v from repo %v: %v", sha, pachydermRepoName, err)
	}
	return nil
}

func (a *APIServer) reportDownloadSizeStats(downSize float64, logger *taggedLogger) {

	if a.exportStats {
		if hist, err := datumDownloadSize.GetMetricWithLabelValues(a.pipelineInfo.ID, a.jobID); err != nil {
			logger.Logf("failed to get histogram w labels: pipeline (%v) job (%v) with error %v", a.pipelineInfo.ID, a.jobID, err)
		} else {
			hist.Observe(downSize)
		}
		if counter, err := datumDownloadBytesCount.GetMetricWithLabelValues(a.pipelineInfo.ID, a.jobID); err != nil {
			logger.Logf("failed to get counter w labels: pipeline (%v) job (%v) with error %v", a.pipelineInfo.ID, a.jobID, err)
		} else {
			counter.Add(downSize)
		}
	}
}

func (a *APIServer) reportDownloadTimeStats(start time.Time, stats *pps.ProcessStats, logger *taggedLogger) {
	duration := time.Since(start)
	stats.DownloadTime = types.DurationProto(duration)
	if a.exportStats {
		if hist, err := datumDownloadTime.GetMetricWithLabelValues(a.pipelineInfo.ID, a.jobID); err != nil {
			logger.Logf("failed to get histogram w labels: pipeline (%v) job (%v) with error %v", a.pipelineInfo.ID, a.jobID, err)
		} else {
			hist.Observe(duration.Seconds())
		}
		if counter, err := datumDownloadSecondsCount.GetMetricWithLabelValues(a.pipelineInfo.ID, a.jobID); err != nil {
			logger.Logf("failed to get counter w labels: pipeline (%v) job (%v) with error %v", a.pipelineInfo.ID, a.jobID, err)
		} else {
			counter.Add(duration.Seconds())
		}
	}
}

func (a *APIServer) downloadData(pachClient *client.APIClient, logger *taggedLogger, inputs []*Input, puller *filesync.Puller, stats *pps.ProcessStats, statsTree *hashtree.Ordered) (_ string, retErr error) {
	defer a.reportDownloadTimeStats(time.Now(), stats, logger)
	logger.Logf("starting to download data")
	defer func(start time.Time) {
		if retErr != nil {
			logger.Logf("errored downloading data after %v: %v", time.Since(start), retErr)
		} else {
			logger.Logf("finished downloading data after %v", time.Since(start))
		}
	}(time.Now())
	dir := filepath.Join(client.PPSInputPrefix, client.PPSScratchSpace, uuid.NewWithoutDashes())
	// Create output directory (currently /pfs/out)
	outPath := filepath.Join(dir, "out")
	if a.pipelineInfo.Spout != nil {
		// Spouts need to create a named pipe at /pfs/out
		if err := os.MkdirAll(filepath.Dir(outPath), 0700); err != nil {
			return "", fmt.Errorf("mkdirall :%v", err)
		}
		if err := createSpoutFifo(outPath); err != nil {
			return "", fmt.Errorf("mkfifo :%v", err)
		}
		if a.pipelineInfo.Spout.Marker != "" {
			// check if we have a marker file in the /pfs/out directory
			_, err := pachClient.InspectFile(a.pipelineInfo.Pipeline.Name, ppsconsts.SpoutMarkerBranch, a.pipelineInfo.Spout.Marker)
			if err != nil {
				// if this fails because there's no head commit on the marker branch, then we don't want to pull the marker, but it's also not an error
				if !strings.Contains(err.Error(), "no head") {
					// if it fails for some other reason, then fail
					return "", err
				}
			} else {
				// the file might be in the spout marker directory, and so we'll try pulling it from there
				if err := puller.Pull(pachClient, filepath.Join(dir, a.pipelineInfo.Spout.Marker), a.pipelineInfo.Pipeline.Name,
					ppsconsts.SpoutMarkerBranch, "/"+a.pipelineInfo.Spout.Marker, false, false, concurrency, nil, ""); err != nil {
					// this might fail if the marker branch hasn't been created, so check for that
					if err == nil || !strings.Contains(err.Error(), "branches") || !errutil.IsNotFoundError(err) {
						return "", err
					}
					// if it just hasn't been created yet, that's fine and we should just continue as normal
				}
			}
		}
	} else {
		if err := os.MkdirAll(outPath, 0777); err != nil {
			return "", err
		}
	}
	for _, input := range inputs {
		if input.GitURL != "" {
			if err := a.downloadGitData(pachClient, dir, input); err != nil {
				return "", err
			}
			continue
		}
		file := input.FileInfo.File
		root := filepath.Join(dir, input.Name, file.Path)
		var statsRoot string
		if statsTree != nil {
			statsRoot = path.Join(input.Name, file.Path)
			parent, _ := path.Split(statsRoot)
			statsTree.MkdirAll(parent)
		}
		if err := puller.Pull(pachClient, root, file.Commit.Repo.Name, file.Commit.ID, file.Path, input.Lazy, input.EmptyFiles, concurrency, statsTree, statsRoot); err != nil {
			return "", err
		}
	}
	return dir, nil
}

func (a *APIServer) linkData(inputs []*Input, dir string) error {
	// Make sure that previously symlinked outputs are removed.
	err := a.unlinkData(inputs)
	if err != nil {
		return err
	}
	for _, input := range inputs {
		src := filepath.Join(dir, input.Name)
		dst := filepath.Join(client.PPSInputPrefix, input.Name)
		if err := os.Symlink(src, dst); err != nil {
			return err
		}
	}

	if a.pipelineInfo.Spout != nil && a.pipelineInfo.Spout.Marker != "" {
		err = os.Symlink(filepath.Join(dir, a.pipelineInfo.Spout.Marker), filepath.Join(client.PPSInputPrefix, a.pipelineInfo.Spout.Marker))
		if err != nil {
			return err
		}
	}

	return os.Symlink(filepath.Join(dir, "out"), filepath.Join(client.PPSInputPrefix, "out"))
}

func (a *APIServer) unlinkData(inputs []*Input) error {
	dirs, err := ioutil.ReadDir(client.PPSInputPrefix)
	if err != nil {
		return fmt.Errorf("ioutil.ReadDir: %v", err)
	}
	for _, d := range dirs {
		if d.Name() == client.PPSScratchSpace {
			continue // don't delete scratch space
		}
		if err := os.RemoveAll(filepath.Join(client.PPSInputPrefix, d.Name())); err != nil {
			return err
		}
	}
	return nil
}

func (a *APIServer) reportUserCodeStats(logger *taggedLogger) {
	if a.exportStats {
		if counter, err := datumCount.GetMetricWithLabelValues(a.pipelineInfo.ID, a.jobID, "started"); err != nil {
			logger.Logf("failed to get histogram w labels: pipeline (%v) job (%v) with error %v", a.pipelineInfo.ID, a.jobID, err)
		} else {
			counter.Add(1)
		}
	}
}

func (a *APIServer) reportDeferredUserCodeStats(err error, start time.Time, stats *pps.ProcessStats, logger *taggedLogger) {
	duration := time.Since(start)
	stats.ProcessTime = types.DurationProto(duration)
	if a.exportStats {
		state := "errored"
		if err == nil {
			state = "finished"
		}
		if counter, err := datumCount.GetMetricWithLabelValues(a.pipelineInfo.ID, a.jobID, state); err != nil {
			logger.Logf("failed to get counter w labels: pipeline (%v) job (%v) state (%v) with error %v", a.pipelineInfo.ID, a.jobID, state, err)
		} else {
			counter.Add(1)
		}
		if hist, err := datumProcTime.GetMetricWithLabelValues(a.pipelineInfo.ID, a.jobID, state); err != nil {
			logger.Logf("failed to get counter w labels: pipeline (%v) job (%v) state (%v) with error %v", a.pipelineInfo.ID, a.jobID, state, err)
		} else {
			hist.Observe(duration.Seconds())
		}
		if counter, err := datumProcSecondsCount.GetMetricWithLabelValues(a.pipelineInfo.ID, a.jobID); err != nil {
			logger.Logf("failed to get counter w labels: pipeline (%v) job (%v) with error %v", a.pipelineInfo.ID, a.jobID, err)
		} else {
			counter.Add(duration.Seconds())
		}
	}
}

// Run user code and return the combined output of stdout and stderr.
func (a *APIServer) runUserCode(ctx context.Context, logger *taggedLogger, environ []string, stats *pps.ProcessStats, rawDatumTimeout *types.Duration) (retErr error) {
	a.reportUserCodeStats(logger)
	defer func(start time.Time) { a.reportDeferredUserCodeStats(retErr, start, stats, logger) }(time.Now())
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
			return err
		}
		datumTimeoutCtx, cancel := context.WithTimeout(ctx, datumTimeout)
		defer cancel()
		ctx = datumTimeoutCtx
	}

	// Run user code
	cmd := exec.CommandContext(ctx, a.pipelineInfo.Transform.Cmd[0], a.pipelineInfo.Transform.Cmd[1:]...)
	if a.pipelineInfo.Transform.Stdin != nil {
		cmd.Stdin = strings.NewReader(strings.Join(a.pipelineInfo.Transform.Stdin, "\n") + "\n")
	}
	cmd.Stdout = logger.userLogger()
	cmd.Stderr = logger.userLogger()
	cmd.Env = environ
	if a.uid != nil && a.gid != nil {
		cmd.SysProcAttr = makeCmdCredentials(*a.uid, *a.gid)
	}
	cmd.Dir = a.pipelineInfo.Transform.WorkingDir
	err := cmd.Start()
	if err != nil {
		return fmt.Errorf("error cmd.Start: %v", err)
	}
	// A context w a deadline will successfully cancel/kill
	// the running process (minus zombies)
	state, err := cmd.Process.Wait()
	if err != nil {
		return fmt.Errorf("error cmd.Wait: %v", err)
	}
	if isDone(ctx) {
		if err = ctx.Err(); err != nil {
			return err
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
		if exiterr, ok := err.(*exec.ExitError); ok {
			if status, ok := exiterr.Sys().(syscall.WaitStatus); ok {
				for _, returnCode := range a.pipelineInfo.Transform.AcceptReturnCode {
					if int(returnCode) == status.ExitStatus() {
						return nil
					}
				}
			}
		}
		return fmt.Errorf("error cmd.WaitIO: %v", err)
	}
	return nil
}

// Run user error code and return the combined output of stdout and stderr.
func (a *APIServer) runUserErrorHandlingCode(ctx context.Context, logger *taggedLogger, environ []string, stats *pps.ProcessStats, rawDatumTimeout *types.Duration) (retErr error) {
	logger.Logf("beginning to run user error handling code")
	defer func(start time.Time) {
		if retErr != nil {
			logger.Logf("errored running user error handling code after %v: %v", time.Since(start), retErr)
		} else {
			logger.Logf("finished running user error handling code after %v", time.Since(start))
		}
	}(time.Now())

	cmd := exec.CommandContext(ctx, a.pipelineInfo.Transform.ErrCmd[0], a.pipelineInfo.Transform.ErrCmd[1:]...)
	if a.pipelineInfo.Transform.ErrStdin != nil {
		cmd.Stdin = strings.NewReader(strings.Join(a.pipelineInfo.Transform.ErrStdin, "\n") + "\n")
	}
	cmd.Stdout = logger.userLogger()
	cmd.Stderr = logger.userLogger()
	cmd.Env = environ
	if a.uid != nil && a.gid != nil {
		cmd.SysProcAttr = makeCmdCredentials(*a.uid, *a.gid)
	}
	cmd.Dir = a.pipelineInfo.Transform.WorkingDir
	err := cmd.Start()
	if err != nil {
		return fmt.Errorf("error cmd.Start: %v", err)
	}
	// A context w a deadline will successfully cancel/kill
	// the running process (minus zombies)
	state, err := cmd.Process.Wait()
	if err != nil {
		return fmt.Errorf("error cmd.Wait: %v", err)
	}
	if isDone(ctx) {
		if err = ctx.Err(); err != nil {
			return err
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
		if exiterr, ok := err.(*exec.ExitError); ok {
			if status, ok := exiterr.Sys().(syscall.WaitStatus); ok {
				for _, returnCode := range a.pipelineInfo.Transform.AcceptReturnCode {
					if int(returnCode) == status.ExitStatus() {
						return nil
					}
				}
			}
		}
		return fmt.Errorf("error cmd.WaitIO: %v", err)
	}
	return nil
}

func (a *APIServer) reportUploadStats(start time.Time, stats *pps.ProcessStats, logger *taggedLogger) {
	duration := time.Since(start)
	stats.UploadTime = types.DurationProto(duration)
	if a.exportStats {
		if hist, err := datumUploadTime.GetMetricWithLabelValues(a.pipelineInfo.ID, a.jobID); err != nil {
			logger.Logf("failed to get histogram w labels: pipeline (%v) job (%v) with error %v", a.pipelineInfo.ID, a.jobID, err)
		} else {
			hist.Observe(duration.Seconds())
		}
		if counter, err := datumUploadSecondsCount.GetMetricWithLabelValues(a.pipelineInfo.ID, a.jobID); err != nil {
			logger.Logf("failed to get counter w labels: pipeline (%v) job (%v) with error %v", a.pipelineInfo.ID, a.jobID, err)
		} else {
			counter.Add(duration.Seconds())
		}
		if hist, err := datumUploadSize.GetMetricWithLabelValues(a.pipelineInfo.ID, a.jobID); err != nil {
			logger.Logf("failed to get histogram w labels: pipeline (%v) job (%v) with error %v", a.pipelineInfo.ID, a.jobID, err)
		} else {
			hist.Observe(float64(stats.UploadBytes))
		}
		if counter, err := datumUploadBytesCount.GetMetricWithLabelValues(a.pipelineInfo.ID, a.jobID); err != nil {
			logger.Logf("failed to get counter w labels: pipeline (%v) job (%v) with error %v", a.pipelineInfo.ID, a.jobID, err)
		} else {
			counter.Add(float64(stats.UploadBytes))
		}
	}
}

func (a *APIServer) uploadOutput(pachClient *client.APIClient, dir string, tag string, logger *taggedLogger, inputs []*Input, stats *pps.ProcessStats, statsTree *hashtree.Ordered, datumIdx int64) (retErr error) {
	defer a.reportUploadStats(time.Now(), stats, logger)
	logger.Logf("starting to upload output")
	defer func(start time.Time) {
		if retErr != nil {
			logger.Logf("errored uploading output after %v: %v", time.Since(start), retErr)
		} else {
			logger.Logf("finished uploading output after %v", time.Since(start))
		}
	}(time.Now())
	// Setup client for writing file data
	putObjsClient, err := pachClient.ObjectAPIClient.PutObjects(pachClient.Ctx())
	if err != nil {
		return err
	}
	block := &pfs.Block{Hash: uuid.NewWithoutDashes()}
	if err := putObjsClient.Send(&pfs.PutObjectRequest{
		Block: block,
	}); err != nil {
		return err
	}
	outputPath := filepath.Join(dir, "out")
	buf := grpcutil.GetBuffer()
	defer grpcutil.PutBuffer(buf)
	var offset uint64
	var tree *hashtree.Ordered
	// Upload all files in output directory
	if err := filepath.Walk(outputPath, func(filePath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !utf8.ValidString(filePath) {
			return fmt.Errorf("file path is not valid utf-8: %s", filePath)
		}
		if filePath == outputPath {
			tree = hashtree.NewOrdered("/")
			return nil
		}
		relPath, err := filepath.Rel(outputPath, filePath)
		if err != nil {
			return err
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
			return errSpecialFile
		}
		// If the output file is a symlink to an input file, we can skip
		// the uploading.
		if (info.Mode() & os.ModeSymlink) > 0 {
			realPath, err := os.Readlink(filePath)
			if err != nil {
				return err
			}
			if strings.HasPrefix(realPath, client.PPSInputPrefix) {
				var pathWithInput string
				var err error
				if strings.HasPrefix(realPath, dir) {
					pathWithInput, err = filepath.Rel(dir, realPath)
				} else {
					pathWithInput, err = filepath.Rel(client.PPSInputPrefix, realPath)
				}
				if err == nil {
					// We can only skip the upload if the real path is
					// under /pfs, meaning that it's a file that already
					// exists in PFS.

					// The name of the input
					inputName := strings.Split(pathWithInput, string(os.PathSeparator))[0]
					var input *Input
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
								return err
							}
							rel, err := filepath.Rel(realPath, filePath)
							if err != nil {
								return err
							}
							subRelPath := filepath.Join(relPath, rel)
							// The path of the input file
							pfsPath, err := filepath.Rel(filepath.Join(dir, input.Name), filePath)
							if err != nil {
								return err
							}
							if info.IsDir() {
								tree.PutDir(subRelPath)
								if statsTree != nil {
									statsTree.PutDir(subRelPath)
								}
								return nil
							}
							fc := input.FileInfo.File.Commit
							fileInfo, err := pachClient.InspectFile(fc.Repo.Name, fc.ID, pfsPath)
							if err != nil {
								return err
							}
							var blockRefs []*pfs.BlockRef
							for _, object := range fileInfo.Objects {
								objectInfo, err := pachClient.InspectObject(object.Hash)
								if err != nil {
									return err
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
			if a.pipelineInfo.Spout != nil {
				if strings.Contains(err.Error(), path.Join("out", a.pipelineInfo.Spout.Marker)) {
					return nil
				}
			}
			return fmt.Errorf("os.Open(%s): %v", filePath, err)
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
				if err == io.EOF {
					break
				}
				return err
			}
			if err := putObjsClient.Send(&pfs.PutObjectRequest{
				Value: buf[:n],
			}); err != nil {
				return err
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
		return fmt.Errorf("error walking output: %v", err)
	}
	if _, err := putObjsClient.CloseAndRecv(); err != nil && err != io.EOF {
		return err
	}
	// Serialize datum hashtree
	b := &bytes.Buffer{}
	if err := tree.Serialize(b); err != nil {
		return err
	}
	// Write datum hashtree to object storage
	w, err := pachClient.PutObjectAsync([]*pfs.Tag{client.NewTag(tag)})
	if err != nil {
		return err
	}
	defer func() {
		if err := w.Close(); err != nil && retErr != nil {
			retErr = err
		}
	}()
	if _, err := w.Write(b.Bytes()); err != nil {
		return err
	}
	// Cache datum hashtree locally
	return a.datumCache.Put(datumIdx, bytes.NewReader(b.Bytes()))
}

// HashDatum computes and returns the hash of datum + pipeline, with a
// pipeline-specific prefix.
func HashDatum(pipelineName string, pipelineSalt string, data []*Input) string {
	hash := sha256.New()
	for _, datum := range data {
		hash.Write([]byte(datum.Name))
		hash.Write([]byte(datum.FileInfo.File.Path))
		hash.Write(datum.FileInfo.Hash)
	}

	hash.Write([]byte(pipelineName))
	hash.Write([]byte(pipelineSalt))

	return client.DatumTagPrefix(pipelineSalt) + hex.EncodeToString(hash.Sum(nil))
}

// HashDatum15 computes and returns the hash of datum + pipeline for version <= 1.5.0, with a
// pipeline-specific prefix.
func HashDatum15(pipelineInfo *pps.PipelineInfo, data []*Input) (string, error) {
	hash := sha256.New()
	for _, datum := range data {
		hash.Write([]byte(datum.Name))
		hash.Write([]byte(datum.FileInfo.File.Path))
		hash.Write(datum.FileInfo.Hash)
	}

	// We set env to nil because if env contains more than one elements,
	// since it's a map, the output of Marshal() can be non-deterministic.
	env := pipelineInfo.Transform.Env
	pipelineInfo.Transform.Env = nil
	defer func() {
		pipelineInfo.Transform.Env = env
	}()
	bytes, err := pipelineInfo.Transform.Marshal()
	if err != nil {
		return "", err
	}
	hash.Write(bytes)
	hash.Write([]byte(pipelineInfo.Pipeline.Name))
	hash.Write([]byte(pipelineInfo.ID))
	hash.Write([]byte(strconv.Itoa(int(pipelineInfo.Version))))

	// Note in 1.5.0 this function was called HashPipelineID, it's now called
	// HashPipelineName but it has the same implementation.
	return client.DatumTagPrefix(pipelineInfo.ID) + hex.EncodeToString(hash.Sum(nil)), nil
}

// Status returns the status of the current worker.
func (a *APIServer) Status(ctx context.Context, _ *types.Empty) (*pps.WorkerStatus, error) {
	a.statusMu.Lock()
	defer a.statusMu.Unlock()
	started, err := types.TimestampProto(a.started)
	if err != nil {
		return nil, err
	}
	result := &pps.WorkerStatus{
		JobID:     a.jobID,
		WorkerID:  a.workerName,
		Started:   started,
		Data:      a.datum(),
		QueueSize: atomic.LoadInt64(&a.queueSize),
	}
	return result, nil
}

// Cancel cancels the currently running datum
func (a *APIServer) Cancel(ctx context.Context, request *CancelRequest) (*CancelResponse, error) {
	a.statusMu.Lock()
	defer a.statusMu.Unlock()
	if request.JobID != a.jobID {
		return &CancelResponse{Success: false}, nil
	}
	if !MatchDatum(request.DataFilters, a.datum()) {
		return &CancelResponse{Success: false}, nil
	}
	a.cancel()
	// clear the status since we're no longer processing this datum
	a.jobID = ""
	a.data = nil
	a.started = time.Time{}
	a.cancel = nil
	return &CancelResponse{Success: true}, nil
}

// GetChunk returns the merged datum hashtrees of a particular chunk (if available)
func (a *APIServer) GetChunk(request *GetChunkRequest, server Worker_GetChunkServer) error {
	filter := hashtree.NewFilter(a.numShards, request.Shard)
	if request.Stats {
		return a.chunkStatsCache.Get(request.Id, grpcutil.NewStreamingBytesWriter(server), filter)
	}
	return a.chunkCache.Get(request.Id, grpcutil.NewStreamingBytesWriter(server), filter)
}

func (a *APIServer) datum() []*pps.InputFile {
	var result []*pps.InputFile
	for _, datum := range a.data {
		result = append(result, &pps.InputFile{
			Path: datum.FileInfo.File.Path,
			Hash: datum.FileInfo.Hash,
		})
	}
	return result
}

func (a *APIServer) userCodeEnv(jobID string, outputCommitID string, data []*Input) []string {
	result := os.Environ()
	for _, input := range data {
		result = append(result, fmt.Sprintf("%s=%s", input.Name, filepath.Join(client.PPSInputPrefix, input.Name, input.FileInfo.File.Path)))
		result = append(result, fmt.Sprintf("%s_COMMIT=%s", input.Name, input.FileInfo.File.Commit.ID))
	}
	result = append(result, fmt.Sprintf("%s=%s", client.JobIDEnv, jobID))
	result = append(result, fmt.Sprintf("%s=%s", client.OutputCommitIDEnv, outputCommitID))
	return result
}

type processResult struct {
	failedDatumID   string
	datumsProcessed int64
	datumsSkipped   int64
	datumsRecovered int64
	datumsFailed    int64
	recoveredDatums *pfs.Object
}

type processFunc func(low, high int64) (*processResult, error)

func (a *APIServer) acquireDatums(ctx context.Context, jobID string, plan *Plan, logger *taggedLogger, process processFunc) error {
	chunks := a.chunks(jobID)
	watcher, err := chunks.ReadOnly(ctx).Watch(watch.WithFilterPut())
	if err != nil {
		return fmt.Errorf("error creating chunk watcher: %v", err)
	}
	var complete bool
	for !complete {
		// We set complete to true and then unset it if we find an incomplete chunk
		complete = true
		// Attempt to claim a chunk
		low, high := int64(0), int64(0)
		for _, high = range plan.Chunks {
			var chunkState ChunkState
			if err := chunks.Claim(ctx, fmt.Sprint(high), &chunkState, func(ctx context.Context) error {
				return a.processChunk(ctx, jobID, low, high, process)
			}); err == col.ErrNotClaimed {
				// Check if a different worker is processing this chunk
				if chunkState.State == State_RUNNING {
					complete = false
				}
			} else if err != nil {
				return err
			}
			low = high
		}
		// Wait for a deletion event (ttl expired) before attempting to claim a chunk again
		select {
		case e := <-watcher.Watch():
			if e.Type == watch.EventError {
				return fmt.Errorf("chunk watch error: %v", e.Err)
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (a *APIServer) processChunk(ctx context.Context, jobID string, low, high int64, process processFunc) error {
	processResult, err := process(low, high)
	if err != nil {
		return err
	}
	if _, err := col.NewSTM(ctx, a.etcdClient, func(stm col.STM) error {
		jobs := a.jobs.ReadWrite(stm)
		jobPtr := &pps.EtcdJobInfo{}
		if err := jobs.Update(jobID, jobPtr, func() error {
			jobPtr.DataProcessed += processResult.datumsProcessed
			jobPtr.DataSkipped += processResult.datumsSkipped
			jobPtr.DataRecovered += processResult.datumsRecovered
			jobPtr.DataFailed += processResult.datumsFailed
			return nil
		}); err != nil {
			return err
		}
		chunks := a.chunks(jobID).ReadWrite(stm)
		if processResult.failedDatumID != "" {
			return chunks.Put(fmt.Sprint(high), &ChunkState{
				State:   State_FAILED,
				DatumID: processResult.failedDatumID,
				Address: os.Getenv(client.PPSWorkerIPEnv),
			})
		}
		return chunks.Put(fmt.Sprint(high), &ChunkState{
			State:           State_COMPLETE,
			Address:         os.Getenv(client.PPSWorkerIPEnv),
			RecoveredDatums: processResult.recoveredDatums,
		})
	}); err != nil {
		return err
	}
	return nil
}

func (a *APIServer) mergeDatums(jobCtx context.Context, pachClient *client.APIClient, jobInfo *pps.JobInfo, jobID string,
	plan *Plan, logger *taggedLogger, df DatumIterator, skip map[string]bool, useParentHashTree bool) (retErr error) {
	for {
		if err := func() error {
			// if this worker is not responsible for a shard, it waits to be assigned one or for the job to finish
			if a.shardCtx == nil {
				select {
				case a.shardCtx = <-a.claimedShard:
					a.shard = a.shardCtx.Value(shardKey).(int64)
				case <-jobCtx.Done():
					return nil
				}
			}
			ctx, _ := joincontext.Join(jobCtx, a.shardCtx)
			objClient, err := obj.NewClientFromSecret(a.hashtreeStorage)
			if err != nil {
				return err
			}
			// collect hashtrees from chunks as they complete
			var failed bool
			if err := logger.LogStep("collecting chunk hashtree(s)", func() error {
				low := int64(0)
				chunks := a.chunks(jobInfo.Job.ID).ReadOnly(ctx)
				for _, high := range plan.Chunks {
					chunkState := &ChunkState{}
					if err := chunks.WatchOneF(fmt.Sprint(high), func(e *watch.Event) error {
						if e.Type == watch.EventError {
							return e.Err
						}
						// unmarshal and check that full key matched
						var key string
						if err := e.Unmarshal(&key, chunkState); err != nil {
							return err
						}
						if key != fmt.Sprint(high) {
							return nil
						}
						switch chunkState.State {
						case State_FAILED:
							failed = true
							fallthrough
						case State_COMPLETE:
							if err := a.getChunk(ctx, high, chunkState.Address, failed); err != nil {
								logger.Logf("error downloading chunk %v from worker at %v (%v), falling back on object storage", high, chunkState.Address, err)
								tags := a.computeTags(df, low, high, skip, useParentHashTree)
								// Download datum hashtrees from object storage if we run into an error getting them from the worker
								if err := a.getChunkFromObjectStorage(ctx, pachClient, objClient, tags, high, failed); err != nil {
									return err
								}
							}
							return errutil.ErrBreak
						}
						return nil
					}); err != nil {
						return err
					}
					low = high
				}
				return nil
			}); err != nil {
				return err
			}
			// get parent hashtree reader if it is being used
			var parentHashtree, parentStatsHashtree io.Reader
			if useParentHashTree {
				var r io.ReadCloser
				if err := logger.LogStep("getting parent hashtree", func() error {
					var err error
					r, err = a.getParentHashTree(ctx, pachClient, objClient, jobInfo.OutputCommit, a.shard)
					return err
				}); err != nil {
					return err
				}
				defer func() {
					if err := r.Close(); err != nil && retErr == nil {
						retErr = err
					}
				}()
				parentHashtree = bufio.NewReaderSize(r, parentTreeBufSize)
				// get parent stats hashtree reader if it is being used
				if a.pipelineInfo.EnableStats {
					var r io.ReadCloser
					if err := logger.LogStep("getting parent stats hashtree", func() error {
						var err error
						r, err = a.getParentHashTree(ctx, pachClient, objClient, jobInfo.StatsCommit, a.shard)
						return err
					}); err != nil {
						return err
					}
					defer func() {
						if err := r.Close(); err != nil && retErr == nil {
							retErr = err
						}
					}()
					parentStatsHashtree = bufio.NewReaderSize(r, parentTreeBufSize)
				}
			}
			// merging output tree(s)
			var tree, statsTree *pfs.Object
			var size, statsSize uint64
			if err := logger.LogStep("merging output", func() error {
				if a.pipelineInfo.EnableStats {
					statsTree, statsSize, err = a.merge(pachClient, objClient, true, parentStatsHashtree)
					if err != nil {
						return err
					}
				}
				if !failed {
					tree, size, err = a.merge(pachClient, objClient, false, parentHashtree)
					if err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				return err
			}
			// mark merge as complete
			_, err = col.NewSTM(ctx, a.etcdClient, func(stm col.STM) error {
				merges := a.merges(jobID).ReadWrite(stm)
				return merges.Put(fmt.Sprint(a.shard), &MergeState{State: State_COMPLETE, Tree: tree, SizeBytes: size, StatsTree: statsTree, StatsSizeBytes: statsSize})
			})
			return err
		}(); err != nil {
			if a.shardCtx.Err() == context.Canceled {
				a.shardCtx = nil
				continue
			}
			return err
		}
		return nil
	}
}

func (a *APIServer) getChunk(ctx context.Context, id int64, address string, failed bool) error {
	// If this worker processed the chunk, then it is already in the chunk cache
	if address == os.Getenv(client.PPSWorkerIPEnv) {
		return nil
	}
	if _, ok := a.clients[address]; !ok {
		client, err := NewClient(address)
		if err != nil {
			return err
		}
		a.clients[address] = client
	}
	client := a.clients[address]
	// Get chunk hashtree and store in chunk cache if the chunk succeeded
	if !failed {
		c, err := client.GetChunk(ctx, &GetChunkRequest{
			Id:    id,
			Shard: a.shard,
		})
		if err != nil {
			return err
		}
		buf := &bytes.Buffer{}
		if err := grpcutil.WriteFromStreamingBytesClient(c, buf); err != nil {
			return err
		}
		if err := a.chunkCache.Put(id, buf); err != nil {
			return err
		}
	}
	// Get chunk stats hashtree and store in chunk stats cache if applicable
	if a.pipelineInfo.EnableStats {
		c, err := client.GetChunk(ctx, &GetChunkRequest{
			Id:    id,
			Shard: a.shard,
			Stats: true,
		})
		if err != nil {
			return err
		}
		buf := &bytes.Buffer{}
		if err := grpcutil.WriteFromStreamingBytesClient(c, buf); err != nil {
			return err
		}
		if err := a.chunkStatsCache.Put(id, buf); err != nil {
			return err
		}
	}
	return nil
}

func (a *APIServer) computeTags(df DatumIterator, low, high int64, skip map[string]bool, useParentHashTree bool) []*pfs.Tag {
	var tags []*pfs.Tag
	for i := low; i < high; i++ {
		files := df.DatumN(int(i))
		datumHash := HashDatum(a.pipelineInfo.Pipeline.Name, a.pipelineInfo.Salt, files)
		// Skip datum if it is in the parent hashtree and the parent hashtree is being used in the merge
		if skip[datumHash] && useParentHashTree {
			continue
		}
		tags = append(tags, client.NewTag(datumHash))
	}
	return tags
}

func (a *APIServer) getChunkFromObjectStorage(ctx context.Context, pachClient *client.APIClient, objClient obj.Client, tags []*pfs.Tag, id int64, failed bool) error {
	// Download, merge, and cache datum hashtrees for a chunk if it succeeded
	if !failed {
		ts, err := a.getHashtrees(ctx, pachClient, objClient, tags, hashtree.NewFilter(a.numShards, a.shard))
		if err != nil {
			return err
		}
		buf := &bytes.Buffer{}
		if err := hashtree.Merge(hashtree.NewWriter(buf), ts); err != nil {
			return err
		}
		if err := a.chunkCache.Put(id, buf); err != nil {
			return err
		}
	}
	// Download, merge, and cache datum stats hashtrees for a chunk if applicable
	if a.pipelineInfo.EnableStats {
		var statsTags []*pfs.Tag
		for _, tag := range tags {
			statsTags = append(statsTags, client.NewTag(tag.Name+statsTagSuffix))
		}
		ts, err := a.getHashtrees(ctx, pachClient, objClient, statsTags, hashtree.NewFilter(a.numShards, a.shard))
		if err != nil {
			return err
		}
		buf := &bytes.Buffer{}
		if err := hashtree.Merge(hashtree.NewWriter(buf), ts); err != nil {
			return err
		}
		if err := a.chunkStatsCache.Put(id, buf); err != nil {
			return err
		}
	}
	return nil
}

func (a *APIServer) merge(pachClient *client.APIClient, objClient obj.Client, stats bool, parent io.Reader) (*pfs.Object, uint64, error) {
	var tree *pfs.Object
	var size uint64
	if err := func() (retErr error) {
		objW, err := pachClient.PutObjectAsync(nil)
		if err != nil {
			return err
		}
		w := hashtree.NewWriter(objW)
		filter := hashtree.NewFilter(a.numShards, a.shard)
		if stats {
			err = a.chunkStatsCache.Merge(w, parent, filter)
		} else {
			err = a.chunkCache.Merge(w, parent, filter)
		}
		size = w.Size()
		if err != nil {
			objW.Close()
			return err
		}
		// Get object hash for hashtree
		if err := objW.Close(); err != nil {
			return err
		}
		tree, err = objW.Object()
		if err != nil {
			return err
		}
		// Get index and write it out
		idx, err := w.Index()
		if err != nil {
			return err
		}
		return writeIndex(pachClient, objClient, tree, idx)
	}(); err != nil {
		return nil, 0, err
	}
	return tree, size, nil
}

func (a *APIServer) getParentCommitInfo(ctx context.Context, pachClient *client.APIClient, commit *pfs.Commit) (*pfs.CommitInfo, error) {
	commitInfo, err := pachClient.PfsAPIClient.InspectCommit(ctx,
		&pfs.InspectCommitRequest{
			Commit: commit,
		})
	if err != nil {
		return nil, err
	}
	for commitInfo.ParentCommit != nil {
		parentCommitInfo, err := pachClient.PfsAPIClient.InspectCommit(ctx,
			&pfs.InspectCommitRequest{
				Commit: commitInfo.ParentCommit,
			})
		if err != nil {
			return nil, err
		}
		// If the parent commit isn't finished, then finish it and continue the traversal.
		// If the parent commit is finished and has output, then return it.
		if parentCommitInfo.Finished == nil {
			if _, err := pachClient.PfsAPIClient.FinishCommit(pachClient.Ctx(), &pfs.FinishCommitRequest{
				Commit: parentCommitInfo.Commit,
				Empty:  true,
			}); err != nil && !pfsserver.IsCommitFinishedErr(err) {
				return nil, err
			}
		} else if parentCommitInfo.Trees != nil {
			return parentCommitInfo, nil
		}
		commitInfo = parentCommitInfo
	}
	return nil, nil
}

func (a *APIServer) getHashtrees(ctx context.Context, pachClient *client.APIClient, objClient obj.Client, tags []*pfs.Tag, filter hashtree.Filter) ([]*hashtree.Reader, error) {
	limiter := limit.New(hashtree.DefaultMergeConcurrency)
	var eg errgroup.Group
	var mu sync.Mutex
	var rs []*hashtree.Reader
	for _, tag := range tags {
		tag := tag
		limiter.Acquire()
		eg.Go(func() (retErr error) {
			defer limiter.Release()
			// Get datum hashtree info
			info, err := pachClient.InspectTag(ctx, tag)
			if err != nil {
				return err
			}
			path, err := obj.BlockPathFromEnv(info.BlockRef.Block)
			if err != nil {
				return err
			}
			// Read the full datum hashtree in memory
			objR, err := objClient.Reader(ctx, path, 0, 0)
			if err != nil {
				return err
			}
			defer func() {
				if err := objR.Close(); err != nil && retErr == nil {
					retErr = err
				}
			}()
			fullTree, err := ioutil.ReadAll(objR)
			if err != nil {
				return err
			}
			// Filter out unecessary keys
			filteredTree := &bytes.Buffer{}
			w := hashtree.NewWriter(filteredTree)
			r := hashtree.NewReader(bytes.NewBuffer(fullTree), filter)
			if err := w.Copy(r); err != nil {
				return err
			}
			// Add it to the list of readers
			mu.Lock()
			defer mu.Unlock()
			rs = append(rs, hashtree.NewReader(filteredTree, nil))
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	return rs, nil
}

func (a *APIServer) getDatumMap(ctx context.Context, pachClient *client.APIClient, object *pfs.Object) (_ map[string]bool, retErr error) {
	if object == nil {
		return nil, nil
	}
	r, err := pachClient.GetObjectReader(object.Hash)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := r.Close(); err != nil && retErr != nil {
			retErr = err
		}
	}()
	pbr := pbutil.NewReader(r)
	datums := make(map[string]bool)
	for {
		k, err := pbr.ReadBytes()
		if err != nil {
			if err == io.EOF {
				return datums, retErr
			}
			return nil, err
		}
		datums[string(k)] = true
	}
	return datums, nil
}

func (a *APIServer) getParentHashTree(ctx context.Context, pachClient *client.APIClient, objClient obj.Client, commit *pfs.Commit, merge int64) (io.ReadCloser, error) {
	parentCommitInfo, err := a.getParentCommitInfo(ctx, pachClient, commit)
	if err != nil {
		return nil, err
	}
	if parentCommitInfo == nil {
		return ioutil.NopCloser(&bytes.Buffer{}), nil
	}
	info, err := pachClient.InspectObject(parentCommitInfo.Trees[merge].Hash)
	if err != nil {
		return nil, err
	}
	path, err := obj.BlockPathFromEnv(info.BlockRef.Block)
	if err != nil {
		return nil, err
	}
	return objClient.Reader(ctx, path, 0, 0)
}

func writeIndex(pachClient *client.APIClient, objClient obj.Client, tree *pfs.Object, idx []byte) (retErr error) {
	info, err := pachClient.InspectObject(tree.Hash)
	if err != nil {
		return err
	}
	path, err := obj.BlockPathFromEnv(info.BlockRef.Block)
	if err != nil {
		return err
	}
	idxW, err := objClient.Writer(pachClient.Ctx(), path+hashtree.IndexPath)
	if err != nil {
		return err
	}
	defer func() {
		if err := idxW.Close(); err != nil && retErr == nil {
			retErr = err
		}
	}()
	_, err = idxW.Write(idx)
	return err
}

func isDone(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

// cancelCtxIfJobFails watches jobID's JobPtr, and if its state is changed to a
// terminal state (KILLED, FAILED, or SUCCESS) cancel the jobCtx so we kill any
// user processes
func (a *APIServer) cancelCtxIfJobFails(jobCtx context.Context, jobCancel func(), jobID string) {
	logger := a.getWorkerLogger() // this worker's formatting logger

	backoff.RetryNotify(func() error {
		// Check if job was cancelled while backoff was sleeping
		if isDone(jobCtx) {
			return nil
		}

		// Start watching for job state changes
		watcher, err := a.jobs.ReadOnly(jobCtx).WatchOne(jobID)
		if err != nil {
			if col.IsErrNotFound(err) {
				jobCancel() // job deleted before we started--cancel the job ctx
				return nil
			}
			return fmt.Errorf("worker: could not create state watcher for job %s, err is %v", jobID, err)
		}

		// If any job events indicate that the job is done, cancel jobCtx
	Outer:
		for {
			select {
			case e := <-watcher.Watch():
				switch e.Type {
				case watch.EventPut:
					var jobID string
					jobPtr := &pps.EtcdJobInfo{}
					if err := e.Unmarshal(&jobID, jobPtr); err != nil {
						return fmt.Errorf("worker: error unmarshalling while watching job state (%v)", err)
					}
					if ppsutil.IsTerminal(jobPtr.State) {
						logger.Logf("job %q put in terminal state %q; cancelling", jobID, jobPtr.State)
						jobCancel() // cancel the job
					}
				case watch.EventDelete:
					logger.Logf("job %q deleted; cancelling", jobID)
					jobCancel() // cancel the job
				case watch.EventError:
					return fmt.Errorf("job state watch error: %v", e.Err)
				}
			case <-jobCtx.Done():
				break Outer
			}
		}
		return nil
	}, backoff.NewInfiniteBackOff(), func(err error, d time.Duration) error {
		if jobCtx.Err() == context.Canceled {
			return err // worker is done, nothing else to do
		}
		logger.Logf("worker: error watching job %s (%v); retrying in %v", jobID, err, d)
		return nil
	})
}

// worker does the following:
//  - claims filesystem shards as they become available
//  - watches for new jobs (jobInfos in the jobs collection)
//  - claims chunks from the chunk layout it finds in the chunks collection
//  - claims those chunks with acquireDatums
//  - processes the chunks with processDatums
//  - merges the chunks with mergeDatums
func (a *APIServer) worker() {
	logger := a.getWorkerLogger() // this worker's formatting logger

	// claim a shard if one is available or becomes available
	go a.claimShard(a.pachClient.Ctx())

	// Process incoming jobs
	backoff.RetryNotify(func() (retErr error) {
		retryCtx, retryCancel := context.WithCancel(a.pachClient.Ctx())
		defer retryCancel()
		watcher, err := a.jobs.ReadOnly(retryCtx).WatchByIndex(ppsdb.JobsPipelineIndex, a.pipelineInfo.Pipeline)
		if err != nil {
			return fmt.Errorf("error creating watch: %v", err)
		}
		defer watcher.Close()
		for e := range watcher.Watch() {
			// Clear chunk caches from previous job
			if err := a.chunkCache.Clear(); err != nil {
				logger.Logf("error clearing chunk cache: %v", err)
			}
			if err := a.chunkStatsCache.Clear(); err != nil {
				logger.Logf("error clearing chunk stats cache: %v", err)
			}
			if e.Type == watch.EventError {
				return fmt.Errorf("worker watch error: %v", e.Err)
			} else if e.Type == watch.EventDelete {
				// Job was deleted, e.g. because input commit was deleted. This is
				// handled by cancelCtxIfJobFails goro, which was spawned when job was
				// created. Nothing to do here
				continue
			}

			// 'e' is a Put event -- new job
			var jobID string
			jobPtr := &pps.EtcdJobInfo{}
			if err := e.Unmarshal(&jobID, jobPtr); err != nil {
				return fmt.Errorf("error unmarshalling: %v", err)
			}
			if ppsutil.IsTerminal(jobPtr.State) {
				// previously-created job has finished, or job was finished during backoff
				// or in the 'watcher' queue
				logger.Logf("skipping job %v as it is already in state %v", jobID, jobPtr.State)
				continue
			}

			// create new ctx for this job, and don't use retryCtx as the
			// parent. Just because another job's etcd write failed doesn't
			// mean this job shouldn't run
			if err := logger.LogStep(fmt.Sprintf("processing job %v", jobID), func() error {
				jobCtx, jobCancel := context.WithCancel(a.pachClient.Ctx())
				defer jobCancel() // cancel the job ctx
				pachClient := a.pachClient.WithCtx(jobCtx)

				//  Watch for any changes to EtcdJobInfo corresponding to jobID; if
				// the EtcdJobInfo is marked 'FAILED', call jobCancel().
				// ('watcher' above can't detect job state changes--it's watching
				// an index and so only emits when jobs are created or deleted).
				go a.cancelCtxIfJobFails(jobCtx, jobCancel, jobID)

				// Inspect the job and make sure it's relevant, as this worker may be old
				jobInfo, err := pachClient.InspectJob(jobID, false)
				if err != nil {
					if col.IsErrNotFound(err) {
						return nil
					}
					return fmt.Errorf("error from InspectJob(%v): %+v", jobID, err)
				}
				if jobInfo.PipelineVersion < a.pipelineInfo.Version {
					logger.Logf("skipping job %v as it uses old pipeline version %d", jobID, jobInfo.PipelineVersion)
					return nil
				}
				if jobInfo.PipelineVersion > a.pipelineInfo.Version {
					return fmt.Errorf("job %s's version (%d) greater than pipeline's "+
						"version (%d), this should automatically resolve when the worker "+
						"is updated", jobID, jobInfo.PipelineVersion, a.pipelineInfo.Version)
				}

				// Read the chunks laid out by the master and create the datum factory
				plan := &Plan{}
				if err := a.plans.ReadOnly(jobCtx).GetBlock(jobInfo.Job.ID, plan); err != nil {
					return fmt.Errorf("error reading job chunks: %v", err)
				}
				var df DatumIterator
				if err := logger.LogStep("creating datum iterator", func() error {
					var err error
					df, err = NewDatumIterator(pachClient, jobInfo.Input)
					return err
				}); err != nil {
					return err
				}

				// Compute the datums to skip
				skip := make(map[string]bool)
				var useParentHashTree bool
				if err := logger.LogStep("computing datums to skip", func() error {
					parentCommitInfo, err := a.getParentCommitInfo(jobCtx, pachClient, jobInfo.OutputCommit)
					if err != nil {
						return err
					}
					if parentCommitInfo != nil {
						var err error
						skip, err = a.getDatumMap(jobCtx, pachClient, parentCommitInfo.Datums)
						if err != nil {
							return err
						}
						var count int
						for i := 0; i < df.Len(); i++ {
							files := df.DatumN(i)
							datumHash := HashDatum(a.pipelineInfo.Pipeline.Name, a.pipelineInfo.Salt, files)
							if skip[datumHash] {
								count++
							}
						}
						if len(skip) == count {
							useParentHashTree = true
						}
					}
					return nil
				}); err != nil {
					return err
				}

				// Get updated job info from master
				jobInfo, err = pachClient.InspectJob(jobID, false)
				if err != nil {
					return err
				}
				eg, ctx := errgroup.WithContext(jobCtx)
				// If a datum fails, acquireDatums updates the relevant lock in
				// etcd, which causes the master to fail the job (which is
				// handled above in the JOB_FAILURE case). There's no need to
				// handle failed datums here, just failed etcd writes.
				eg.Go(func() error {
					return logger.LogStep("claiming/processing chunks", func() error {
						return a.acquireDatums(
							ctx, jobID, plan, logger,
							func(low, high int64) (*processResult, error) {
								processResult, err := a.processDatums(pachClient, logger, jobInfo, df, low, high, skip, useParentHashTree)
								if err != nil {
									return nil, err
								}
								return processResult, nil
							},
						)
					})
				})
				eg.Go(func() error {
					return logger.LogStep("merge worker", func() error {
						return a.mergeDatums(ctx, pachClient, jobInfo, jobID, plan, logger, df, skip, useParentHashTree)
					})
				})
				if err := eg.Wait(); err != nil {
					if jobCtx.Err() == context.Canceled {
						return nil
					}
					return fmt.Errorf("acquire/process/merge datums for job %s exited with err: %v", jobID, err)
				}
				return nil
			}); err != nil {
				return err
			}
		}
		return fmt.Errorf("worker: jobs.WatchByIndex(pipeline = %s) closed unexpectedly", a.pipelineInfo.Pipeline.Name)
	}, backoff.NewInfiniteBackOff(), func(err error, d time.Duration) error {
		logger.Logf("worker: watch closed or error running the worker process: %v; retrying in %v", err, d)
		return nil
	})
}

func (a *APIServer) claimShard(ctx context.Context) {
	watcher, err := a.shards.ReadOnly(ctx).Watch(watch.WithFilterPut())
	if err != nil {
		log.Printf("error creating shard watcher: %v", err)
		return
	}
	for {
		// Attempt to claim a shard
		for shard := int64(0); shard < a.numShards; shard++ {
			var shardInfo ShardInfo
			err := a.shards.Claim(ctx, fmt.Sprint(shard), &shardInfo, func(ctx context.Context) error {
				ctx = context.WithValue(ctx, shardKey, shard)
				a.claimedShard <- ctx
				<-ctx.Done()
				return nil
			})
			if err != nil && err != col.ErrNotClaimed {
				log.Printf("error attempting to claim shard: %v", err)
				return
			}
		}
		// Wait for a deletion event (ttl expired) before attempting to claim a shard again
		select {
		case e := <-watcher.Watch():
			if e.Type == watch.EventError {
				log.Printf("shard watch error: %v", e.Err)
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

// processDatums processes datums from low to high in df, if a datum fails it
// returns the id of the failed datum it also may return a variety of errors
// such as network errors.
func (a *APIServer) processDatums(pachClient *client.APIClient, logger *taggedLogger, jobInfo *pps.JobInfo,
	df DatumIterator, low, high int64, skip map[string]bool, useParentHashTree bool) (result *processResult, retErr error) {
	defer func() {
		if err := a.datumCache.Clear(); err != nil && retErr == nil {
			logger.Logf("error clearing datum cache: %v", err)
		}
		if err := a.datumStatsCache.Clear(); err != nil && retErr == nil {
			logger.Logf("error clearing datum stats cache: %v", err)
		}
	}()
	ctx := pachClient.Ctx()
	objClient, err := obj.NewClientFromSecret(a.hashtreeStorage)
	if err != nil {
		return nil, err
	}
	stats := &pps.ProcessStats{}
	var statsMu sync.Mutex
	result = &processResult{}
	var eg errgroup.Group
	limiter := limit.New(int(a.pipelineInfo.MaxQueueSize))
	var recoveredDatums []string
	var recoverMu sync.Mutex
	for i := low; i < high; i++ {
		datumIdx := i

		limiter.Acquire()
		atomic.AddInt64(&a.queueSize, 1)
		eg.Go(func() (retErr error) {
			defer limiter.Release()
			defer atomic.AddInt64(&a.queueSize, -1)

			data := df.DatumN(int(datumIdx))
			logger, err := a.getTaggedLogger(pachClient, jobInfo.Job.ID, data, a.pipelineInfo.EnableStats)
			if err != nil {
				return err
			}
			// Hash inputs
			tag := HashDatum(a.pipelineInfo.Pipeline.Name, a.pipelineInfo.Salt, data)
			if skip[tag] {
				if !useParentHashTree {
					if err := a.cacheHashtree(pachClient, tag, datumIdx); err != nil {
						return err
					}
				}
				atomic.AddInt64(&result.datumsSkipped, 1)
				logger.Logf("skipping datum")
				return nil
			}
			if _, err := pachClient.InspectTag(ctx, client.NewTag(tag)); err == nil {
				if err := a.cacheHashtree(pachClient, tag, datumIdx); err != nil {
					return err
				}
				atomic.AddInt64(&result.datumsSkipped, 1)
				logger.Logf("skipping datum")
				return nil
			}
			subStats := &pps.ProcessStats{}
			var inputTree, outputTree *hashtree.Ordered
			var statsTree *hashtree.Unordered
			if a.pipelineInfo.EnableStats {
				statsRoot := path.Join("/", logger.template.DatumID)
				inputTree = hashtree.NewOrdered(path.Join(statsRoot, "pfs"))
				outputTree = hashtree.NewOrdered(path.Join(statsRoot, "pfs", "out"))
				statsTree = hashtree.NewUnordered(statsRoot)
				// Write job id to stats tree
				statsTree.PutFile(fmt.Sprintf("job:%s", jobInfo.Job.ID), nil, 0)
				// Write index in datum factory to stats tree
				object, size, err := pachClient.PutObject(strings.NewReader(fmt.Sprint(int(datumIdx))))
				if err != nil {
					return err
				}
				objectInfo, err := pachClient.InspectObject(object.Hash)
				if err != nil {
					return err
				}
				h, err := pfs.DecodeHash(object.Hash)
				if err != nil {
					return err
				}
				statsTree.PutFile("index", h, size, objectInfo.BlockRef)
				defer func() {
					if err := a.writeStats(pachClient, objClient, tag, subStats, logger, inputTree, outputTree, statsTree, datumIdx); err != nil && retErr == nil {
						retErr = err
					}
				}()
			}

			env := a.userCodeEnv(jobInfo.Job.ID, jobInfo.OutputCommit.ID, data)
			var dir string
			var failures int64
			if err := backoff.RetryNotify(func() error {
				if isDone(ctx) {
					return ctx.Err() // timeout or cancelled job--don't run datum
				}
				// Download input data
				puller := filesync.NewPuller()
				// TODO parent tag shouldn't be nil
				var err error
				dir, err = a.downloadData(pachClient, logger, data, puller, subStats, inputTree)
				// We run these cleanup functions no matter what, so that if
				// downloadData partially succeeded, we still clean up the resources.
				defer func() {
					if err := os.RemoveAll(dir); err != nil && retErr == nil {
						retErr = err
					}
				}()
				// It's important that we run puller.CleanUp before os.RemoveAll,
				// because otherwise puller.Cleanup might try tp open pipes that have
				// been deleted.
				defer func() {
					if _, err := puller.CleanUp(); err != nil && retErr == nil {
						retErr = err
					}
				}()
				if err != nil {
					return fmt.Errorf("error downloadData: %v", err)
				}
				a.runMu.Lock()
				defer a.runMu.Unlock()
				// shadow ctx and pachClient for the context of processing this one datum
				ctx, cancel := context.WithCancel(ctx)
				pachClient := pachClient.WithCtx(ctx)
				func() {
					a.statusMu.Lock()
					defer a.statusMu.Unlock()
					a.jobID = jobInfo.Job.ID
					a.data = data
					a.started = time.Now()
					a.cancel = cancel
					a.stats = stats
				}()
				if err := os.MkdirAll(client.PPSInputPrefix, 0777); err != nil {
					return err
				}
				if err := a.linkData(data, dir); err != nil {
					return fmt.Errorf("error linkData: %v", err)
				}
				defer func() {
					if err := a.unlinkData(data); err != nil && retErr == nil {
						retErr = fmt.Errorf("error unlinkData: %v", err)
					}
				}()
				// If the pipeline spec set a custom user to execute the
				// process, make sure `/pfs` and its content are owned by it
				if a.uid != nil && a.gid != nil {
					filepath.Walk("/pfs", func(name string, info os.FileInfo, err error) error {
						if err == nil {
							err = os.Chown(name, int(*a.uid), int(*a.gid))
						}
						return err
					})
				}
				if err := a.runUserCode(ctx, logger, env, subStats, jobInfo.DatumTimeout); err != nil {
					if a.pipelineInfo.Transform.ErrCmd != nil && failures == jobInfo.DatumTries-1 {
						if err = a.runUserErrorHandlingCode(ctx, logger, env, subStats, jobInfo.DatumTimeout); err != nil {
							return fmt.Errorf("error runUserErrorHandlingCode: %v", err)
						}
						return errDatumRecovered
					}
					return fmt.Errorf("error runUserCode: %v", err)
				}
				// CleanUp is idempotent so we can call it however many times we want.
				// The reason we are calling it here is that the puller could've
				// encountered an error as it was lazily loading files, in which case
				// the output might be invalid since as far as the user's code is
				// concerned, they might've just seen an empty or partially completed
				// file.
				downSize, err := puller.CleanUp()
				if err != nil {
					logger.Logf("puller encountered an error while cleaning up: %+v", err)
					return err
				}
				atomic.AddUint64(&subStats.DownloadBytes, uint64(downSize))
				a.reportDownloadSizeStats(float64(downSize), logger)
				return a.uploadOutput(pachClient, dir, tag, logger, data, subStats, outputTree, datumIdx)
			}, &backoff.ZeroBackOff{}, func(err error, d time.Duration) error {
				if isDone(ctx) {
					return ctx.Err() // timeout or cancelled job, err out and don't retry
				}
				failures++
				if failures >= jobInfo.DatumTries {
					logger.Logf("failed to process datum with error: %+v", err)
					if statsTree != nil {
						object, size, err := pachClient.PutObject(strings.NewReader(err.Error()))
						if err != nil {
							logger.stderrLog.Printf("could not put error object: %s\n", err)
						} else {
							objectInfo, err := pachClient.InspectObject(object.Hash)
							if err != nil {
								return err
							}
							h, err := pfs.DecodeHash(object.Hash)
							if err != nil {
								return err
							}
							statsTree.PutFile("failure", h, size, objectInfo.BlockRef)
						}
					}
					return err
				}
				logger.Logf("failed processing datum: %v, retrying in %v", err, d)
				return nil
			}); err == errDatumRecovered {
				// keep track of the recovered datums
				recoverMu.Lock()
				defer recoverMu.Unlock()
				recoveredDatums = append(recoveredDatums, a.DatumID(data))
				atomic.AddInt64(&result.datumsRecovered, 1)
				return nil
			} else if err != nil {
				result.failedDatumID = a.DatumID(data)
				atomic.AddInt64(&result.datumsFailed, 1)
				return nil
			}
			statsMu.Lock()
			defer statsMu.Unlock()
			if err := mergeStats(stats, subStats); err != nil {
				logger.Logf("failed to merge Stats: %v", err)
			}
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}

	// put the list of recovered datums in a pfs.Object, and save it as part of the result
	if len(recoveredDatums) > 0 {
		recoveredDatumsBuf := &bytes.Buffer{}
		pbw := pbutil.NewWriter(recoveredDatumsBuf)
		for _, datumHash := range recoveredDatums {
			if _, err := pbw.WriteBytes([]byte(datumHash)); err != nil {
				return nil, err
			}
		}
		recoveredDatumsObj, _, err := pachClient.PutObject(recoveredDatumsBuf)
		if err != nil {
			return nil, err
		}

		result.recoveredDatums = recoveredDatumsObj
	}

	if _, err := col.NewSTM(ctx, a.etcdClient, func(stm col.STM) error {
		jobs := a.jobs.ReadWrite(stm)
		jobID := jobInfo.Job.ID
		jobPtr := &pps.EtcdJobInfo{}
		if err := jobs.Get(jobID, jobPtr); err != nil {
			return err
		}
		if jobPtr.Stats == nil {
			jobPtr.Stats = &pps.ProcessStats{}
		}
		if err := mergeStats(jobPtr.Stats, stats); err != nil {
			logger.Logf("failed to merge Stats: %v", err)
		}
		return jobs.Put(jobID, jobPtr)
	}); err != nil {
		return nil, err
	}
	result.datumsProcessed = high - low - result.datumsSkipped - result.datumsFailed - result.datumsRecovered
	// Merge datum hashtrees into a chunk hashtree, then cache it.
	if err := a.mergeChunk(logger, high, result); err != nil {
		return nil, err
	}
	return result, nil
}

func (a *APIServer) cacheHashtree(pachClient *client.APIClient, tag string, datumIdx int64) (retErr error) {
	buf := &bytes.Buffer{}
	if err := pachClient.GetTag(tag, buf); err != nil {
		return err
	}
	if err := a.datumCache.Put(datumIdx, buf); err != nil {
		return err
	}
	if a.pipelineInfo.EnableStats {
		buf.Reset()
		if err := pachClient.GetTag(tag+statsTagSuffix, buf); err != nil {
			// We are okay with not finding the stats hashtree.
			// This allows users to enable stats on a pipeline
			// with pre-existing jobs.
			return nil
		}
		return a.datumStatsCache.Put(datumIdx, buf)
	}
	return nil
}

func (a *APIServer) writeStats(pachClient *client.APIClient, objClient obj.Client, tag string, stats *pps.ProcessStats, logger *taggedLogger, inputTree, outputTree *hashtree.Ordered, statsTree *hashtree.Unordered, datumIdx int64) (retErr error) {
	// Store stats and add stats file
	marshaler := &jsonpb.Marshaler{}
	statsString, err := marshaler.MarshalToString(stats)
	if err != nil {
		logger.stderrLog.Printf("could not serialize stats: %s\n", err)
		return err
	}
	object, size, err := pachClient.PutObject(strings.NewReader(statsString))
	if err != nil {
		logger.stderrLog.Printf("could not put stats object: %s\n", err)
		return err
	}
	objectInfo, err := pachClient.InspectObject(object.Hash)
	if err != nil {
		return err
	}
	h, err := pfs.DecodeHash(object.Hash)
	if err != nil {
		return err
	}
	statsTree.PutFile("stats", h, size, objectInfo.BlockRef)
	// Store logs and add logs file
	object, size, err = logger.Close()
	if err != nil {
		return err
	}
	if object != nil {
		objectInfo, err := pachClient.InspectObject(object.Hash)
		if err != nil {
			return err
		}
		h, err := pfs.DecodeHash(object.Hash)
		if err != nil {
			return err
		}
		statsTree.PutFile("logs", h, size, objectInfo.BlockRef)
	}
	// Merge stats trees (input, output, stats) and write out
	inputBuf := &bytes.Buffer{}
	inputTree.Serialize(inputBuf)
	outputBuf := &bytes.Buffer{}
	outputTree.Serialize(outputBuf)
	statsBuf := &bytes.Buffer{}
	statsTree.Ordered().Serialize(statsBuf)
	// Merge datum stats hashtree
	buf := &bytes.Buffer{}
	if err := hashtree.Merge(hashtree.NewWriter(buf), []*hashtree.Reader{
		hashtree.NewReader(inputBuf, nil),
		hashtree.NewReader(outputBuf, nil),
		hashtree.NewReader(statsBuf, nil),
	}); err != nil {
		return err
	}
	// Write datum stats hashtree to object storage
	objW, err := pachClient.PutObjectAsync([]*pfs.Tag{client.NewTag(tag + statsTagSuffix)})
	if err != nil {
		return err
	}
	defer func() {
		if err := objW.Close(); err != nil && retErr == nil {
			retErr = err
		}
	}()
	if _, err := objW.Write(buf.Bytes()); err != nil {
		return err
	}
	// Cache datum stats hashtree locally
	return a.datumStatsCache.Put(datumIdx, bytes.NewReader(buf.Bytes()))
}

// mergeStats merges y into x
func mergeStats(x, y *pps.ProcessStats) error {
	var err error
	if x.DownloadTime, err = plusDuration(x.DownloadTime, y.DownloadTime); err != nil {
		return err
	}
	if x.ProcessTime, err = plusDuration(x.ProcessTime, y.ProcessTime); err != nil {
		return err
	}
	if x.UploadTime, err = plusDuration(x.UploadTime, y.UploadTime); err != nil {
		return err
	}
	x.DownloadBytes += y.DownloadBytes
	x.UploadBytes += y.UploadBytes
	return nil
}

// mergeChunk merges the datum hashtrees into a chunk hashtree and stores it.
func (a *APIServer) mergeChunk(logger *taggedLogger, high int64, result *processResult) (retErr error) {
	logger.Logf("starting to merge chunk")
	defer func(start time.Time) {
		if retErr != nil {
			logger.Logf("errored merging chunk after %v: %v", time.Since(start), retErr)
		} else {
			logger.Logf("finished merging chunk after %v", time.Since(start))
		}
	}(time.Now())
	buf := &bytes.Buffer{}
	if result.datumsFailed <= 0 {
		if err := a.datumCache.Merge(hashtree.NewWriter(buf), nil, nil); err != nil {
			return err
		}
	}
	if err := a.chunkCache.Put(high, buf); err != nil {
		return err
	}
	if a.pipelineInfo.EnableStats {
		buf.Reset()
		if err := a.datumStatsCache.Merge(hashtree.NewWriter(buf), nil, nil); err != nil {
			return err
		}
		return a.chunkStatsCache.Put(high, buf)
	}
	return nil
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
		return nil, err
	}
	defer func() {
		if err := passwd.Close(); err != nil && retErr == nil {
			retErr = err
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
						return nil, err
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
	return nil, fmt.Errorf("user %s not found", userArg)
}

func lookupGroup(group string) (_ *user.Group, retErr error) {
	groupFile, err := os.Open("/etc/group")
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := groupFile.Close(); err != nil && retErr == nil {
			retErr = err
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
	return nil, fmt.Errorf("group %s not found", group)
}

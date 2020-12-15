package driver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/gogo/protobuf/types"
	"github.com/prometheus/client_golang/prometheus"
	prometheus_proto "github.com/prometheus/client_model/go"
	"gopkg.in/go-playground/webhooks.v5/github"

	"github.com/pachyderm/pachyderm/src/client"
	"github.com/pachyderm/pachyderm/src/client/enterprise"
	"github.com/pachyderm/pachyderm/src/client/pfs"
	"github.com/pachyderm/pachyderm/src/client/pkg/errors"
	"github.com/pachyderm/pachyderm/src/client/pkg/require"
	"github.com/pachyderm/pachyderm/src/client/pps"
	"github.com/pachyderm/pachyderm/src/server/pkg/testpachd"
	tu "github.com/pachyderm/pachyderm/src/server/pkg/testutil"
	"github.com/pachyderm/pachyderm/src/server/worker/common"
	"github.com/pachyderm/pachyderm/src/server/worker/logs"
)

var inputRepo = "inputRepo"
var inputGitRepo = "https://github.com/pachyderm/test-artifacts.git"
var inputGitRepoFake = "https://github.com/pachyderm/test-artifacts-fake.git"

func testPipelineInfo() *pps.PipelineInfo {
	return &pps.PipelineInfo{
		Pipeline: client.NewPipeline("testPipeline"),
		Transform: &pps.Transform{
			Cmd: []string{"cp", filepath.Join("pfs", inputRepo, "file"), "pfs/out/file"},
		},
		ParallelismSpec: &pps.ParallelismSpec{
			Constant: 1,
		},
		ResourceRequests: &pps.ResourceSpec{
			Memory: "100M",
			Cpu:    0.5,
		},
		Input: client.NewPFSInput(inputRepo, "/*"),
	}
}

type testEnv struct {
	testpachd.MockEnv
	driver *driver
}

func withTestEnv(cb func(*testEnv)) error {
	return testpachd.WithMockEnv(func(mockEnv *testpachd.MockEnv) (err error) {
		env := &testEnv{MockEnv: *mockEnv}

		// Mock out the enterprise.GetState call that happens during driver construction
		env.MockPachd.Enterprise.GetState.Use(func(context.Context, *enterprise.GetStateRequest) (*enterprise.GetStateResponse, error) {
			return &enterprise.GetStateResponse{State: enterprise.State_NONE}, nil
		})

		var d Driver
		d, err = NewDriver(
			testPipelineInfo(),
			env.PachClient,
			env.EtcdClient,
			tu.UniqueString("driverTest"),
			filepath.Clean(filepath.Join(env.Directory, "hashtrees")),
			filepath.Clean(filepath.Join(env.Directory, "pfs")),
			"namespace",
		)
		if err != nil {
			return err
		}
		d = d.WithContext(env.Context)
		env.driver = d.(*driver)
		env.driver.pipelineInfo.Transform.WorkingDir = "/" // Default Docker wd

		cb(env)

		return nil
	})
}

// collectLogs provides the given callback with a mock TaggedLogger object which
// will be used to collect all the logs and return them. This is pretty naive
// and just splits log statements based on newlines because when running user
// code, it is just used as an io.Writer and doesn't know when one message ends
// and the next begins.
func collectLogs(cb func(logs.TaggedLogger)) []string {
	logger := logs.NewMockLogger()
	buffer := &bytes.Buffer{}
	logger.Writer = buffer
	logger.Job = "job-id"

	cb(logger)

	logStmts := strings.Split(buffer.String(), "\n")
	if len(logStmts) > 0 && logStmts[len(logStmts)-1] == "" {
		return logStmts[0 : len(logStmts)-1]
	}
	return logStmts
}

// requireLogs wraps collectLogs and ensures that certain log statements were
// made. These are specified as regular expressions in the patterns parameter,
// and each pattern must match at least one log line. The patterns are run
// separately against each log line, not against the entire output. If the
// patterns parameter is nil, we require that there are no log statements.
func requireLogs(t *testing.T, patterns []string, cb func(logs.TaggedLogger)) {
	logStmts := collectLogs(cb)

	if patterns == nil {
		require.Equal(t, 0, len(logStmts), "callback should not have logged anything")
	} else {
		for _, pattern := range patterns {
			require.OneOfMatches(t, pattern, logStmts, "callback did not log the expected message")
		}
	}
}

func requireMetric(t *testing.T, metric prometheus.Collector, labels []string, cb func(prometheus_proto.Metric)) {
	reg := prometheus.NewRegistry()
	require.NoError(t, reg.Register(metric))

	stats, err := reg.Gather()
	require.NoError(t, err)

	// Add a placeholder for the state label even if it isn't used
	for len(labels) < 3 {
		labels = append(labels, "")
	}

	// We only have one metric in the registry, so skip over the family level
	for _, family := range stats {
		for _, metric := range family.Metric {
			var pipeline, job, state string
			for _, pair := range metric.Label {
				switch *pair.Name {
				case "pipeline":
					pipeline = *pair.Value
				case "job":
					job = *pair.Value
				case "state":
					state = *pair.Value
				default:
					require.True(t, false, fmt.Sprintf("unexpected metric label: %s", *pair.Name))
				}
			}

			metricLabels := []string{pipeline, job, state}
			if reflect.DeepEqual(labels, metricLabels) {
				cb(*metric)
				return
			}
		}
	}

	require.True(t, false, fmt.Sprintf("no matching metric found for labels: %v", labels))
}

func requireCounter(t *testing.T, counter *prometheus.CounterVec, labels []string, value float64) {
	requireMetric(t, counter, labels, func(m prometheus_proto.Metric) {
		require.NotNil(t, m.Counter)
		require.Equal(t, value, *m.Counter.Value)
	})
}

func requireHistogram(t *testing.T, histogram *prometheus.HistogramVec, labels []string, value uint64) {
	requireMetric(t, histogram, labels, func(m prometheus_proto.Metric) {
		require.NotNil(t, m.Histogram)
		require.Equal(t, value, *m.Histogram.SampleCount)
	})
}

func TestUpdateCounter(t *testing.T) {
	t.Parallel()
	err := withTestEnv(func(env *testEnv) {
		env.driver.pipelineInfo.ID = "foo"

		counterVec := prometheus.NewCounterVec(
			prometheus.CounterOpts{Namespace: "test", Subsystem: "driver", Name: "counter"},
			[]string{"pipeline", "job"},
		)

		counterVecWithState := prometheus.NewCounterVec(
			prometheus.CounterOpts{Namespace: "test", Subsystem: "driver", Name: "counter_with_state"},
			[]string{"pipeline", "job", "state"},
		)

		// Passing a state to the stateless counter should error
		requireLogs(t, []string{"expected 2 label values but got 3"}, func(logger logs.TaggedLogger) {
			env.driver.updateCounter(counterVec, logger, "bar", func(c prometheus.Counter) {
				require.True(t, false, "should have errored")
			})
		})

		// updateCounter should pass a valid counter with the selected tags
		requireLogs(t, nil, func(logger logs.TaggedLogger) {
			env.driver.updateCounter(counterVec, logger, "", func(c prometheus.Counter) {
				c.Add(1)
			})
		})

		// Check that the counter was incremented
		requireCounter(t, counterVec, []string{"foo", "job-id"}, 1)

		// Not passing a state to the stateful counter should error
		requireLogs(t, []string{"expected 3 label values but got 2"}, func(logger logs.TaggedLogger) {
			env.driver.updateCounter(counterVecWithState, logger, "", func(c prometheus.Counter) {
				require.True(t, false, "should have errored")
			})
		})

		// updateCounter should pass a valid counter with the selected tags
		requireLogs(t, nil, func(logger logs.TaggedLogger) {
			env.driver.updateCounter(counterVecWithState, logger, "bar", func(c prometheus.Counter) {
				c.Add(1)
			})
		})

		// Check that the counter was incremented
		requireCounter(t, counterVecWithState, []string{"foo", "job-id", "bar"}, 1)
	})
	require.NoError(t, err)
}

func TestUpdateHistogram(t *testing.T) {
	t.Parallel()
	err := withTestEnv(func(env *testEnv) {
		env.driver.pipelineInfo.ID = "foo"

		histogramVec := prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "test", Subsystem: "driver", Name: "histogram",
				Buckets: prometheus.ExponentialBuckets(1.0, 2.0, 20),
			},
			[]string{"pipeline", "job"},
		)

		histogramVecWithState := prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "test", Subsystem: "driver", Name: "histogram_with_state",
				Buckets: prometheus.ExponentialBuckets(1.0, 2.0, 20),
			},
			[]string{"pipeline", "job", "state"},
		)

		// Passing a state to the stateless histogram should error
		requireLogs(t, []string{"expected 2 label values but got 3"}, func(logger logs.TaggedLogger) {
			env.driver.updateHistogram(histogramVec, logger, "bar", func(h prometheus.Observer) {
				require.True(t, false, "should have errored")
			})
		})

		requireLogs(t, nil, func(logger logs.TaggedLogger) {
			env.driver.updateHistogram(histogramVec, logger, "", func(h prometheus.Observer) {
				h.Observe(0)
			})
		})

		// Check that the counter was incremented
		requireHistogram(t, histogramVec, []string{"foo", "job-id"}, 1)

		// Not passing a state to the stateful histogram should error
		requireLogs(t, []string{"expected 3 label values but got 2"}, func(logger logs.TaggedLogger) {
			env.driver.updateHistogram(histogramVecWithState, logger, "", func(h prometheus.Observer) {
				require.True(t, false, "should have errored")
			})
		})

		requireLogs(t, nil, func(logger logs.TaggedLogger) {
			env.driver.updateHistogram(histogramVecWithState, logger, "bar", func(h prometheus.Observer) {
				h.Observe(0)
			})
		})

		// Check that the counter was incremented
		requireHistogram(t, histogramVecWithState, []string{"foo", "job-id", "bar"}, 1)
	})
	require.NoError(t, err)
}

type inputData struct {
	path     string
	contents string
	regex    string
	found    bool
}

func newInputDataRegex(path string, regex string) *inputData {
	return &inputData{path: filepath.Clean(path), regex: regex}
}

func newInputData(path string, contents string) *inputData {
	return &inputData{path: filepath.Clean(path), contents: contents}
}

func requireEmptyScratch(t *testing.T, inputDir string) {
	entries, err := ioutil.ReadDir(filepath.Join(inputDir, client.PPSScratchSpace))

	if !os.IsNotExist(err) {
		require.ElementsEqual(t, []os.FileInfo{}, entries)
	}
}

func requireContents(t *testing.T, dir string, data []*inputData) {
	checkFile := func(fullPath string, relPath string) {
		for _, checkData := range data {
			if checkData.path == relPath {
				contents, err := ioutil.ReadFile(fullPath)
				require.NoError(t, err)
				if checkData.regex != "" {
					require.Matches(t, checkData.regex, string(contents), "Incorrect contents for input file: %s", relPath)
				} else {
					require.Equal(t, checkData.contents, string(contents), "Incorrect contents for input file: %s", relPath)
				}
				checkData.found = true
				return
			}
		}
		require.True(t, false, "Unexpected input file found: %s", relPath)
	}

	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		require.NoError(t, err)
		if info.Name() == ".git" || info.Name() == client.PPSScratchSpace {
			return filepath.SkipDir
		}
		if !info.IsDir() {
			path = filepath.Clean(path)
			relPath := strings.TrimLeft(strings.TrimPrefix(path, dir), "/\\")
			checkFile(path, relPath)
		}
		return nil
	})
	require.NoError(t, err)

	for _, checkData := range data {
		require.True(t, checkData.found, "Expected input file not found: %s", checkData.path)
	}
}

func TestWithDataEmpty(t *testing.T) {
	t.Parallel()
	err := withTestEnv(func(env *testEnv) {
		requireLogs(t, []string{"finished downloading data"}, func(logger logs.TaggedLogger) {
			_, err := env.driver.WithData(
				[]*common.Input{},
				nil,
				logger,
				func(dir string, stats *pps.ProcessStats) error {
					requireContents(t, dir, []*inputData{})
					return nil
				},
			)
			require.NoError(t, err)
			requireEmptyScratch(t, env.driver.InputDir())
			requireContents(t, env.driver.InputDir(), []*inputData{})
		})
	})
	require.NoError(t, err)
}

func TestWithDataSpout(t *testing.T) {
	t.Parallel()
	err := withTestEnv(func(env *testEnv) {
		env.driver.pipelineInfo.Spout = &pps.Spout{}
		requireLogs(t, []string{"finished downloading data"}, func(logger logs.TaggedLogger) {
			_, err := env.driver.WithData(
				[]*common.Input{},
				nil,
				logger,
				func(dir string, stats *pps.ProcessStats) error {
					// A spout pipeline should have created a 'pfs/out` fifo for the user
					// code to write to
					requireContents(t, dir, []*inputData{newInputData("out", "")})
					return nil
				},
			)
			require.NoError(t, err)
			requireEmptyScratch(t, env.driver.InputDir())
			requireContents(t, env.driver.InputDir(), []*inputData{})
		})
	})
	require.NoError(t, err)
}

// Shitty helper function to create possibly-not-malformed input structures
func newInput(repo string, path string) *common.Input {
	return &common.Input{
		FileInfo: &pfs.FileInfo{
			File: &pfs.File{
				Commit: &pfs.Commit{
					Repo: &pfs.Repo{
						Name: repo,
					},
					ID: "commit-id-string",
				},
				Path: path,
			},
			FileType: pfs.FileType_FILE,
		},
		Name:   repo,
		Branch: "master",
	}
}

func TestWithDataCancel(t *testing.T) {
	t.Parallel()
	err := withTestEnv(func(env *testEnv) {
		requireLogs(t, []string{"errored downloading data", "context canceled"}, func(logger logs.TaggedLogger) {
			ctx, cancel := context.WithCancel(env.Context)
			driver := env.driver.WithContext(ctx)

			// Cancel the context during the download
			env.MockPachd.PFS.WalkFile.Use(func(req *pfs.WalkFileRequest, serv pfs.API_WalkFileServer) error {
				cancel()
				<-serv.Context().Done()
				return errors.Errorf("WalkFile canceled")
			})

			_, err := driver.WithData(
				[]*common.Input{newInput("repo", "input.txt")},
				nil,
				logger,
				func(dir string, stats *pps.ProcessStats) error {
					require.True(t, false, "Should have been canceled before the callback")
					cancel()
					return nil
				},
			)
			require.YesError(t, err, "WithData call should have been canceled")
			requireEmptyScratch(t, env.driver.InputDir())
			requireContents(t, env.driver.InputDir(), []*inputData{})
		})
	})
	require.NoError(t, err)
}

// Check that the driver will download the requested inputs, put them in place
// during WithData, and clean them up after running the inner function.
func TestWithDataDownload(t *testing.T) {
	t.Parallel()
	err := withTestEnv(func(env *testEnv) {
		requireLogs(t, []string{"finished downloading data", "inner function"}, func(logger logs.TaggedLogger) {
			// Mock out the calls that will be used to download the data
			env.MockPachd.PFS.WalkFile.Use(func(req *pfs.WalkFileRequest, serv pfs.API_WalkFileServer) error {
				return serv.Send(&pfs.FileInfo{
					File:     req.File,
					FileType: pfs.FileType_FILE,
				})
			})

			env.MockPachd.PFS.GetFile.Use(func(req *pfs.GetFileRequest, serv pfs.API_GetFileServer) error {
				return serv.Send(&types.BytesValue{Value: []byte(fmt.Sprintf("%s-data", req.File.Commit.Repo.Name))})
			})

			_, err := env.driver.WithData(
				[]*common.Input{newInput("repoA", "input.txt"), newInput("repoB", "input.md")},
				nil,
				logger,
				func(dir string, stats *pps.ProcessStats) error {
					requireContents(t, dir, []*inputData{
						newInputData("repoA/input.txt", "repoA-data"),
						newInputData("repoB/input.md", "repoB-data"),
					})
					logger.Logf("inner function")
					return nil
				},
			)
			require.NoError(t, err)
			requireEmptyScratch(t, env.driver.InputDir())
			requireContents(t, env.driver.InputDir(), []*inputData{})
		})
	})
	require.NoError(t, err)
}

// Create several files and directories inside WithData and verify that they are
// cleaned up after WithData returns.
func TestWithActiveDataCleanup(t *testing.T) {
	t.Parallel()
	err := withTestEnv(func(env *testEnv) {
		create := func(relPath string) {
			fullPath := filepath.Join(env.driver.InputDir(), relPath)
			require.NoError(t, os.MkdirAll(filepath.Dir(fullPath), 0777))
			file, err := os.Create(fullPath)
			require.NoError(t, err)
			require.NoError(t, file.Close())
		}

		requireLogs(t, []string{"finished downloading data", "inner function"}, func(logger logs.TaggedLogger) {
			_, err := env.driver.WithData(
				[]*common.Input{},
				nil,
				logger,
				func(dir string, stats *pps.ProcessStats) error {
					requireContents(t, dir, []*inputData{})
					logger.Logf("inner function")

					expectedContents := []*inputData{
						newInputData("c", ""),
						newInputData("out/1", ""),
						newInputData("out/2/a", ""),
						newInputData("out/2/b", ""),
						newInputData("out/2/3/c", ""),
						newInputData("foo/barbaz", ""),
						newInputData("foo/bar/baz", ""),
						newInputData("floop/blarp/blazj/etc", ""),
					}

					err := env.driver.WithActiveData([]*common.Input{}, dir, func() error {
						for _, x := range expectedContents {
							create(x.path)
						}

						requireContents(t, env.driver.InputDir(), expectedContents)
						return nil
					})
					require.NoError(t, err)
					requireContents(t, dir, expectedContents)
					requireContents(t, env.driver.InputDir(), []*inputData{})
					return nil
				},
			)
			require.NoError(t, err)
			requireEmptyScratch(t, env.driver.InputDir())
			requireContents(t, env.driver.InputDir(), []*inputData{})
		})
	})
	require.NoError(t, err)
}

func newGitInput(repo string, url string) *common.Input {
	return &common.Input{
		FileInfo: &pfs.FileInfo{
			File: &pfs.File{
				Commit: &pfs.Commit{
					Repo: &pfs.Repo{
						Name: repo,
					},
					ID: "commit-id-string",
				},
				Path: "commit.json",
			},
			FileType: pfs.FileType_FILE,
		},
		GitURL: url,
		Name:   repo,
	}
}

func mockGitGetFile(env *testEnv, repo string, ref string, sha string, cb func(*pfs.GetFileRequest)) {
	env.MockPachd.PFS.GetFile.Use(func(req *pfs.GetFileRequest, serv pfs.API_GetFileServer) (retErr error) {
		payload := &github.PushPayload{
			Ref:   ref,
			After: sha,
		}
		payload.Repository.CloneURL = repo
		jsonBytes, err := json.Marshal(payload)
		if err != nil {
			return err
		}

		if cb != nil {
			cb(req)
		}

		return serv.Send(&types.BytesValue{Value: jsonBytes})
	})
}

func TestWithDataGit(t *testing.T) {
	t.Parallel()
	err := withTestEnv(func(env *testEnv) {
		requireLogs(t, []string{"finished downloading data"}, func(logger logs.TaggedLogger) {
			var getFileReq *pfs.GetFileRequest
			mockGitGetFile(env, inputGitRepo, "refs/heads/master", "9047fbfc251e7412ef3300868f743f2c24852539", func(req *pfs.GetFileRequest) {
				getFileReq = req
			})

			_, err := env.driver.WithData(
				[]*common.Input{newGitInput("artifacts", inputGitRepo)},
				nil,
				logger,
				func(dir string, stats *pps.ProcessStats) error {
					requireContents(t, dir, []*inputData{newInputDataRegex("artifacts/readme.md", "Test Artifacts")})
					return nil
				},
			)
			require.NoError(t, err)
			require.NotNil(t, getFileReq)
			require.Equal(t, getFileReq.File, client.NewFile("artifacts", "commit-id-string", "commit.json"))
			requireEmptyScratch(t, env.driver.InputDir())
			requireContents(t, env.driver.InputDir(), []*inputData{})
		})
	})
	require.NoError(t, err)
}

func TestWithDataGitHookError(t *testing.T) {
	t.Parallel()
	err := withTestEnv(func(env *testEnv) {
		requireLogs(t, []string{"errored downloading data"}, func(logger logs.TaggedLogger) {
			mockGitGetFile(env, "", "", "", nil)

			_, err := env.driver.WithData(
				[]*common.Input{newGitInput("artifacts", inputGitRepo)},
				nil,
				logger,
				func(dir string, stats *pps.ProcessStats) error {
					require.True(t, false, "Should have errored before calling WithData callback")
					return nil
				},
			)
			require.YesError(t, err)
			require.Matches(t, "payload does not specify", err.Error())
			requireEmptyScratch(t, env.driver.InputDir())
			requireContents(t, env.driver.InputDir(), []*inputData{})
		})
	})
	require.NoError(t, err)
}

func TestWithDataGitRepoMissing(t *testing.T) {
	t.Parallel()
	err := withTestEnv(func(env *testEnv) {
		requireLogs(t, []string{"errored downloading data"}, func(logger logs.TaggedLogger) {
			mockGitGetFile(env, inputGitRepoFake, "refs/heads/master", "foobar", nil)

			_, err := env.driver.WithData(
				[]*common.Input{newGitInput("artifacts", inputGitRepo)},
				nil,
				logger,
				func(dir string, stats *pps.ProcessStats) error {
					require.True(t, false, "Should have errored before calling WithData callback")
					return nil
				},
			)
			require.YesError(t, err)
			require.Matches(t, "authentication required", err.Error())
			requireEmptyScratch(t, env.driver.InputDir())
			requireContents(t, env.driver.InputDir(), []*inputData{})
		})
	})
	require.NoError(t, err)
}

func TestWithDataGitInvalidSHA(t *testing.T) {
	t.Parallel()
	err := withTestEnv(func(env *testEnv) {
		requireLogs(t, []string{"errored downloading data"}, func(logger logs.TaggedLogger) {
			mockGitGetFile(env, inputGitRepo, "refs/heads/master", "foobar", nil)

			_, err := env.driver.WithData(
				[]*common.Input{newGitInput("artifacts", inputGitRepo)},
				nil,
				logger,
				func(dir string, stats *pps.ProcessStats) error {
					require.True(t, false, "Should have errored before calling WithData callback")
					return nil
				},
			)
			require.YesError(t, err)
			require.Matches(t, "could not find SHA foobar", err.Error())
			requireEmptyScratch(t, env.driver.InputDir())
			requireContents(t, env.driver.InputDir(), []*inputData{})
		})
	})
	require.NoError(t, err)
}

// Test that user code will successfully run and the output will be forwarded to logs
func TestRunUserCode(t *testing.T) {
	t.Parallel()
	logMessage := "this is a user code log message"
	err := withTestEnv(func(env *testEnv) {
		env.driver.pipelineInfo.Transform.Cmd = []string{"echo", logMessage}
		requireLogs(t, []string{logMessage}, func(logger logs.TaggedLogger) {
			err := env.driver.RunUserCode(logger, []string{}, nil, nil)
			require.NoError(t, err)
		})
	})
	require.NoError(t, err)
}

func TestRunUserCodeError(t *testing.T) {
	t.Parallel()
	err := withTestEnv(func(env *testEnv) {
		env.driver.pipelineInfo.Transform.Cmd = []string{"false"}
		requireLogs(t, []string{"exit status 1"}, func(logger logs.TaggedLogger) {
			err := env.driver.RunUserCode(logger, []string{}, nil, nil)
			require.YesError(t, err)
		})
	})
	require.NoError(t, err)
}

func TestRunUserCodeNoCommand(t *testing.T) {
	t.Parallel()
	err := withTestEnv(func(env *testEnv) {
		env.driver.pipelineInfo.Transform.Cmd = []string{}
		requireLogs(t, []string{"no command specified"}, func(logger logs.TaggedLogger) {
			err := env.driver.RunUserCode(logger, []string{}, nil, nil)
			require.YesError(t, err)
		})
	})
	require.NoError(t, err)
}

func TestRunUserCodeTimeout(t *testing.T) {
	t.Parallel()
	err := withTestEnv(func(env *testEnv) {
		env.driver.pipelineInfo.Transform.Cmd = []string{"sleep", "10"}
		timeout := types.DurationProto(10 * time.Millisecond)
		requireLogs(t, []string{"context deadline exceeded"}, func(logger logs.TaggedLogger) {
			err := env.driver.RunUserCode(logger, []string{}, nil, timeout)
			require.YesError(t, err)
			require.Matches(t, "context deadline exceeded", err.Error())
		})
	})
	require.NoError(t, err)
}

func TestRunUserCodeEnv(t *testing.T) {
	t.Parallel()
	err := withTestEnv(func(env *testEnv) {
		env.driver.pipelineInfo.Transform.Cmd = []string{"env"}
		requireLogs(t, []string{"FOO=password", "BAR=hunter2"}, func(logger logs.TaggedLogger) {
			err := env.driver.RunUserCode(logger, []string{"FOO=password", "BAR=hunter2"}, nil, nil)
			require.NoError(t, err)
		})
	})
	require.NoError(t, err)
}

func TestRunUserCodeWithData(t *testing.T) {
	t.Parallel()
	err := withTestEnv(func(env *testEnv) {
		env.driver.pipelineInfo.Transform.Cmd = []string{"bash", "-c", "cat pfs/repoA/input.txt pfs/repoB/input.md > pfs/out/output.txt"}
		requireLogs(t, []string{"finished running user code"}, func(logger logs.TaggedLogger) {
			// Mock out the calls that will be used to download the data
			env.MockPachd.PFS.WalkFile.Use(func(req *pfs.WalkFileRequest, serv pfs.API_WalkFileServer) error {
				return serv.Send(&pfs.FileInfo{
					File:     req.File,
					FileType: pfs.FileType_FILE,
				})
			})

			env.MockPachd.PFS.GetFile.Use(func(req *pfs.GetFileRequest, serv pfs.API_GetFileServer) error {
				return serv.Send(&types.BytesValue{Value: []byte(fmt.Sprintf("%s-data", req.File.Commit.Repo.Name))})
			})

			inputs := []*common.Input{newInput("repoA", "input.txt"), newInput("repoB", "input.md")}
			_, err := env.driver.WithData(
				inputs,
				nil,
				logger,
				func(dir string, stats *pps.ProcessStats) error {
					requireContents(t, dir, []*inputData{
						newInputData("repoA/input.txt", "repoA-data"),
						newInputData("repoB/input.md", "repoB-data"),
					})

					err := env.driver.WithActiveData(inputs, dir, func() error {
						requireContents(t, env.driver.InputDir(), []*inputData{
							newInputData("repoA/input.txt", "repoA-data"),
							newInputData("repoB/input.md", "repoB-data"),
						})

						err := env.driver.RunUserCode(logger, []string{}, nil, nil)
						require.NoError(t, err)

						requireContents(t, env.driver.InputDir(), []*inputData{
							newInputData("repoA/input.txt", "repoA-data"),
							newInputData("repoB/input.md", "repoB-data"),
							newInputData("out/output.txt", "repoA-datarepoB-data"),
						})
						return nil
					})
					require.NoError(t, err)

					requireContents(t, dir, []*inputData{
						newInputData("repoA/input.txt", "repoA-data"),
						newInputData("repoB/input.md", "repoB-data"),
						newInputData("out/output.txt", "repoA-datarepoB-data"),
					})
					return nil
				},
			)
			require.NoError(t, err)
			requireEmptyScratch(t, env.driver.InputDir())
			requireContents(t, env.driver.InputDir(), []*inputData{})
		})
	})
	require.NoError(t, err)
}

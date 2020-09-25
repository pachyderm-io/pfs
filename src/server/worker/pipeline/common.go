package pipeline

import (
	"archive/tar"
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"time"

	"github.com/pachyderm/pachyderm/src/client"
	"github.com/pachyderm/pachyderm/src/client/pfs"
	"github.com/pachyderm/pachyderm/src/client/pkg/errors"
	"github.com/pachyderm/pachyderm/src/client/pps"
	"github.com/pachyderm/pachyderm/src/server/pkg/backoff"
	"github.com/pachyderm/pachyderm/src/server/pkg/errutil"
	"github.com/pachyderm/pachyderm/src/server/pkg/ppsconsts"
	"github.com/pachyderm/pachyderm/src/server/worker/common"
	"github.com/pachyderm/pachyderm/src/server/worker/driver"
	"github.com/pachyderm/pachyderm/src/server/worker/logs"
)

func openAndWait() error {
	// at the end of file, we open the pipe again, since this blocks until something is written to the pipe
	openAndWait, err := os.Open("/pfs/out")
	if err != nil {
		return err
	}
	// and then we immediately close this reader of the pipe, so that the main reader can continue its standard behavior
	err = openAndWait.Close()
	if err != nil {
		return err
	}
	return nil
}

// RunUserCode will run the pipeline's user code until canceled by the context
// - used for services and spouts. Unlike how the transform worker runs user
// code, this does not set environment variables or collect stats.
func RunUserCode(
	driver driver.Driver,
	logger logs.TaggedLogger,
	outputCommit *pfs.Commit,
	inputs []*common.Input,
) error {
	return backoff.RetryUntilCancel(driver.PachClient().Ctx(), func() error {
		// TODO: what about the user error handling code?
		env := driver.UserCodeEnv(logger.JobID(), "", outputCommit, inputs)
		return driver.RunUserCode(logger, env, &pps.ProcessStats{}, nil)
	}, backoff.NewInfiniteBackOff(), func(err error, d time.Duration) error {
		logger.Logf("error in RunUserCode: %+v, retrying in: %+v", err, d)
		return nil
	})
}

// ReceiveSpout is used by both services and spouts if a spout is defined on the
// pipeline. ctx is separate from pachClient because services may call this, and
// they use a cancel function that affects the context but not the pachClient
// (so metadata updates can still be made while unwinding).
func ReceiveSpout(
	ctx context.Context,
	pachClient *client.APIClient,
	pipelineInfo *pps.PipelineInfo,
	logger logs.TaggedLogger,
) (retErr error) {
	// Open a read connection to the /pfs/out named pipe.
	out, err := os.Open("/pfs/out")
	if err != nil {
		return err
	}
	defer func() {
		if err := out.Close(); retErr == nil {
			retErr = err
		}
	}()
	cancelCtx, cancel := context.WithCancel(ctx)
	repo := pipelineInfo.Pipeline.Name
	for {
		if err := withTmpFile("pachyderm_spout_commit", func(f *os.File) error {
			if err := getNextTarStream(f, out); err != nil {
				return err
			}
			return withSpoutCommit(cancelCtx, pachClient, pipelineInfo, logger, f, func(commit *pfs.Commit, tr *tar.Reader) error {
				for {
					hdr, err := tr.Next()
					if err != nil {
						if errors.Is(err, io.EOF) {
							return nil
						}
						return err
					}
					if err := withSpoutFile(cancelCtx, logger, tr, func(r io.Reader) error {
						if pipelineInfo.Spout.Marker != "" && strings.HasPrefix(path.Clean(hdr.Name), pipelineInfo.Spout.Marker) {
							// Check to see if this spout is the latest version of this spout by seeing if its spec commit has any children.
							// TODO: There is a race condition here where the spout could be updated after this check, but before the PutFileOverwrite.
							spec, err := pachClient.InspectCommit(ppsconsts.SpecRepo, pipelineInfo.SpecCommit.ID)
							if err != nil && !errutil.IsNotFoundError(err) {
								return err
							}
							if spec != nil && len(spec.ChildCommits) != 0 {
								cancel()
								return errors.New("outdated spout, now shutting down")
							}
							_, err = pachClient.PutFileOverwrite(repo, ppsconsts.SpoutMarkerBranch, hdr.Name, r, 0)
							if err != nil {
								return err
							}
						} else if pipelineInfo.Spout.Overwrite {
							_, err := pachClient.PutFileOverwrite(repo, commit.ID, hdr.Name, r, 0)
							if err != nil {
								return err
							}
						}
						_, err := pachClient.PutFile(repo, commit.ID, hdr.Name, r)
						return err
					}); err != nil {
						return err
					}
				}
			})
		}); err != nil {
			return err
		}
	}
}

// TODO: Refactor into a file util package.
func withTmpFile(name string, cb func(*os.File) error) (retErr error) {
	if err := os.MkdirAll(os.TempDir(), 0700); err != nil {
		return err
	}
	f, err := ioutil.TempFile(os.TempDir(), name)
	if err != nil {
		return err
	}
	defer func() {
		if err := os.Remove(f.Name()); retErr == nil {
			retErr = err
		}
		if err := f.Close(); retErr == nil {
			retErr = err
		}
	}()
	return cb(f)
}

func getNextTarStream(w io.Writer, r io.Reader) error {
	var hdr *tar.Header
	var err error
	tr := tar.NewReader(newSkipReader(r))
	for {
		hdr, err = tr.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				err = openAndWait()
				if err != nil {
					return err
				}
				tr = tar.NewReader(newSkipReader(r))
				continue
			}
			return err
		}
		break
	}
	tw := tar.NewWriter(w)
	if err := tw.WriteHeader(hdr); err != nil {
		return err
	}
	if _, err := io.Copy(tw, tr); err != nil {
		return err
	}
	if err := copyTar(tw, tr); err != nil {
		return err
	}
	return tw.Close()
}

type skipReader struct {
	buf *bytes.Buffer
	r   io.Reader
}

func newSkipReader(r io.Reader) *skipReader {
	return &skipReader{r: r}
}

func (sr *skipReader) Read(data []byte) (int, error) {
	if sr.buf == nil {
		if err := sr.skipZeroBlocks(); err != nil {
			return 0, err
		}
	}
	bufN, _ := sr.buf.Read(data)
	if bufN == len(data) {
		return bufN, nil
	}
	n, err := sr.r.Read(data[bufN:])
	return bufN + n, err
}

func (sr *skipReader) skipZeroBlocks() error {
	sr.buf = &bytes.Buffer{}
	zeroBlock := make([]byte, 512)
	for {
		_, err := io.CopyN(sr.buf, sr.r, 512)
		if err != nil {
			return err
		}
		if !bytes.Equal(sr.buf.Bytes(), zeroBlock) {
			return nil
		}
		sr.buf.Reset()
	}
}

// TODO: Refactor this into tarutil.
func copyTar(tw *tar.Writer, tr *tar.Reader) error {
	for {
		hdr, err := tr.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		_, err = io.Copy(tw, tr)
		if err != nil {
			return err
		}
	}
}

func withSpoutCommit(ctx context.Context, pachClient *client.APIClient, pipelineInfo *pps.PipelineInfo, logger logs.TaggedLogger, f *os.File, cb func(*pfs.Commit, *tar.Reader) error) error {
	repo := pipelineInfo.Pipeline.Name
	return backoff.RetryUntilCancel(ctx, func() (retErr error) {
		commit, err := pachClient.PfsAPIClient.StartCommit(ctx, &pfs.StartCommitRequest{
			Parent:     client.NewCommit(repo, ""),
			Branch:     pipelineInfo.OutputBranch,
			Provenance: []*pfs.CommitProvenance{client.NewCommitProvenance(ppsconsts.SpecRepo, repo, pipelineInfo.SpecCommit.ID)},
		})
		if err != nil {
			return err
		}
		defer func() {
			if retErr != nil {
				pachClient.DeleteCommit(repo, commit.ID)
				return
			}
			if err := pachClient.FinishCommit(repo, commit.ID); retErr == nil {
				retErr = err
			}
		}()
		_, err = f.Seek(0, 0)
		if err != nil {
			return err
		}
		return cb(commit, tar.NewReader(f))
	}, backoff.NewInfiniteBackOff(), func(err error, d time.Duration) error {
		logger.Logf("error in withSpoutCommit: %+v, retrying in: %+v", err, d)
		return nil
	})
}

func withSpoutFile(ctx context.Context, logger logs.TaggedLogger, r io.Reader, cb func(io.Reader) error) error {
	return withTmpFile("pachyderm_spout_file", func(f *os.File) error {
		_, err := io.Copy(f, r)
		if err != nil {
			return err
		}
		return backoff.RetryUntilCancel(ctx, func() error {
			_, err = f.Seek(0, 0)
			if err != nil {
				return err
			}
			return cb(f)
		}, backoff.NewInfiniteBackOff(), func(err error, d time.Duration) error {
			logger.Logf("error in withSpoutFile: %+v, retrying in: %+v", err, d)
			return nil
		})
	})
}

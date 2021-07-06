package pfssync

import (
	"archive/tar"
	"io"
	"os"
	"path"
	"path/filepath"
	"syscall"

	"github.com/pachyderm/pachyderm/v2/src/client"
	"github.com/pachyderm/pachyderm/v2/src/internal/errors"
	"github.com/pachyderm/pachyderm/v2/src/internal/tarutil"
	"github.com/pachyderm/pachyderm/v2/src/pfs"
	"golang.org/x/sync/errgroup"
)

// Downloader is the standard interface for a PFS downloader.
type Downloader interface {
	// Download a PFS file to a location on the local filesystem.
	Download(storageRoot string, file *pfs.File, opts ...DownloadOption) error
}

type downloader struct {
	pachClient *client.APIClient
	pipes      map[string]struct{}
	eg         *errgroup.Group
	done       bool
}

// WithDownloader provides a scoped environment for a Downloader.
func WithDownloader(pachClient *client.APIClient, cb func(Downloader) error) (retErr error) {
	d := &downloader{
		pachClient: pachClient,
		pipes:      make(map[string]struct{}),
		eg:         &errgroup.Group{},
	}
	defer func() {
		d.done = true
		if err := d.closePipes(); retErr == nil {
			retErr = err
		}
	}()
	return cb(d)
}

func (d *downloader) closePipes() (retErr error) {
	pipes := make(map[string]io.Closer)
	defer func() {
		for path, pipe := range pipes {
			if err := pipe.Close(); retErr == nil {
				retErr = errors.EnsureStack(err)
			}
			if err := os.Remove(path); retErr == nil {
				retErr = errors.EnsureStack(err)
			}
		}
	}()
	// Open all the pipes to unblock the goroutines.
	for path := range d.pipes {
		f, err := os.OpenFile(path, syscall.O_NONBLOCK+os.O_RDONLY, os.ModeNamedPipe)
		if err != nil {
			return errors.EnsureStack(err)
		}
		pipes[path] = f
	}
	return d.eg.Wait()
}

type downloadConfig struct {
	lazy, empty    bool
	headerCallback func(*tar.Header) error
}

// Download a PFS file to a location on the local filesystem.
func (d *downloader) Download(storageRoot string, file *pfs.File, opts ...DownloadOption) error {
	if err := os.MkdirAll(storageRoot, 0700); err != nil {
		return errors.EnsureStack(err)
	}
	dc := &downloadConfig{}
	for _, opt := range opts {
		opt(dc)
	}
	if dc.lazy || dc.empty {
		return d.downloadInfo(storageRoot, file, dc)
	}
	r, err := d.pachClient.GetFileTAR(file.Commit, file.Path)
	if err != nil {
		return err
	}
	if dc.headerCallback != nil {
		return tarutil.Import(storageRoot, r, dc.headerCallback)
	}
	return tarutil.Import(storageRoot, r)
}

func (d *downloader) downloadInfo(storageRoot string, file *pfs.File, config *downloadConfig) (retErr error) {
	return d.pachClient.WalkFile(file.Commit, file.Path, func(fi *pfs.FileInfo) error {
		basePath, err := filepath.Rel(path.Dir(file.Path), fi.File.Path)
		if err != nil {
			return errors.EnsureStack(err)
		}
		fullPath := path.Join(storageRoot, basePath)
		if fi.FileType == pfs.FileType_DIR {
			return errors.EnsureStack(os.MkdirAll(fullPath, 0700))
		}
		if config.lazy {
			return d.makePipe(fullPath, func(w io.Writer) error {
				r, err := d.pachClient.GetFileTAR(file.Commit, fi.File.Path)
				if err != nil {
					return err
				}
				return tarutil.Iterate(r, func(f tarutil.File) error {
					if config.headerCallback != nil {
						hdr, err := f.Header()
						if err != nil {
							return err
						}
						if err := config.headerCallback(hdr); err != nil {
							return err
						}
					}
					return f.Content(w)
				}, true)
			})
		}
		f, err := os.Create(fullPath)
		if err != nil {
			return errors.EnsureStack(err)
		}
		return errors.EnsureStack(f.Close())
	})
}

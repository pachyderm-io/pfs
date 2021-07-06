package tarutil

import (
	"archive/tar"
	"bytes"
	"io"
	"os"
	"path"
	"path/filepath"

	"github.com/pachyderm/pachyderm/v2/src/internal/errors"
)

type File interface {
	Header() (*tar.Header, error)
	Content(io.Writer) error
}

type memFile struct {
	hdr  *tar.Header
	data []byte
}

func NewMemFile(name string, data []byte) File {
	return newMemFile(NewHeader(name, int64(len(data))), data)
}

func newMemFile(hdr *tar.Header, data []byte) File {
	return &memFile{
		hdr:  hdr,
		data: data,
	}
}

func NewHeader(name string, size int64) *tar.Header {
	return &tar.Header{
		Name: name,
		Size: size,
	}
}

func (mf *memFile) Header() (*tar.Header, error) {
	return mf.hdr, nil
}

func (mf *memFile) Content(w io.Writer) error {
	_, err := w.Write(mf.data)
	return err
}

type streamFile struct {
	hdr *tar.Header
	r   io.Reader
}

func NewStreamFile(name string, size int64, r io.Reader) File {
	return newStreamFile(NewHeader(name, size), r)
}

func newStreamFile(hdr *tar.Header, r io.Reader) File {
	return &streamFile{
		hdr: hdr,
		r:   r,
	}
}

func (sf *streamFile) Header() (*tar.Header, error) {
	return sf.hdr, nil
}

func (sf *streamFile) Content(w io.Writer) error {
	_, err := io.Copy(w, sf.r)
	return err
}

func WriteFile(tw *tar.Writer, file File) error {
	hdr, err := file.Header()
	if err != nil {
		return err
	}
	if err := tw.WriteHeader(hdr); err != nil {
		return err
	}
	if err := file.Content(tw); err != nil {
		return err
	}
	return tw.Flush()
}

func WithWriter(w io.Writer, cb func(*tar.Writer) error) (retErr error) {
	tw := tar.NewWriter(w)
	defer func() {
		if retErr == nil {
			retErr = tw.Close()
		}
	}()
	return cb(tw)
}

func Iterate(r io.Reader, cb func(File) error, stream ...bool) error {
	tr := tar.NewReader(r)
	for {
		hdr, err := tr.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if len(stream) > 0 && stream[0] {
			if err := cb(newStreamFile(hdr, tr)); err != nil {
				return err
			}
			continue
		}
		buf := &bytes.Buffer{}
		_, err = io.Copy(buf, tr)
		if err != nil {
			return err
		}
		if err := cb(newMemFile(hdr, buf.Bytes())); err != nil {
			return err
		}
	}
}

func Equal(file1, file2 File, full ...bool) (bool, error) {
	buf1, buf2 := &bytes.Buffer{}, &bytes.Buffer{}
	if len(full) > 0 && full[0] {
		// Check the serialized tar entries.
		tw1, tw2 := tar.NewWriter(buf1), tar.NewWriter(buf2)
		if err := WriteFile(tw1, file1); err != nil {
			return false, err
		}
		if err := WriteFile(tw2, file2); err != nil {
			return false, err
		}
		return bytes.Equal(buf1.Bytes(), buf2.Bytes()), nil
	}
	// Check the header name and content.
	hdr1, err := file1.Header()
	if err != nil {
		return false, err
	}
	hdr2, err := file2.Header()
	if err != nil {
		return false, err
	}
	if hdr1.Name != hdr2.Name {
		return false, nil
	}
	if err := file1.Content(buf1); err != nil {
		return false, err
	}
	if err := file2.Content(buf2); err != nil {
		return false, err
	}
	return bytes.Equal(buf1.Bytes(), buf2.Bytes()), nil
}

func Import(storageRoot string, r io.Reader, cb ...func(*tar.Header) error) error {
	tr := tar.NewReader(r)
	for {
		hdr, err := tr.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if len(cb) > 0 {
			if err := cb[0](hdr); err != nil {
				return err
			}
		}
		// TODO: Use the tar header metadata.
		fullPath := path.Join(storageRoot, hdr.Name)
		if hdr.Typeflag == tar.TypeDir {
			if err := os.MkdirAll(fullPath, 0777); err != nil {
				return err
			}
			continue
		}
		if err := writeFile(fullPath, tr); err != nil {
			return err
		}
	}
}

func writeFile(filePath string, r io.Reader) (retErr error) {
	if err := os.MkdirAll(path.Dir(filePath), 0777); err != nil {
		return err
	}
	f, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer func() {
		if err := f.Close(); retErr == nil {
			retErr = err
		}
	}()
	_, err = io.Copy(f, r)
	return err
}

type exportConfig struct {
	headerCallback func(*tar.Header) error
}

func Export(storageRoot string, w io.Writer, opts ...ExportOption) (retErr error) {
	ec := &exportConfig{}
	for _, opt := range opts {
		opt(ec)
	}
	return WithWriter(w, func(tw *tar.Writer) error {
		return filepath.Walk(storageRoot, func(file string, fi os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if file == storageRoot {
				return nil
			}
			// TODO: Not sure if this is the best way to skip the upload of named pipes.
			// This is needed for uploading the inputs when lazy files is enabled, since the inputs
			// will be closed named pipes.
			if fi.IsDir() || fi.Mode()&os.ModeNamedPipe != 0 || fi.Mode()&os.ModeSymlink != 0 {
				return nil
			}
			// TODO: link name?
			hdr, err := tar.FileInfoHeader(fi, "")
			if err != nil {
				return err
			}
			hdr.Name, err = filepath.Rel(storageRoot, file)
			if err != nil {
				return err
			}
			if ec.headerCallback != nil {
				if err := ec.headerCallback(hdr); err != nil {
					return err
				}
			}
			if err := tw.WriteHeader(hdr); err != nil {
				return err
			}
			f, err := os.Open(file)
			if err != nil {
				return err
			}
			defer func() {
				if err := f.Close(); retErr == nil {
					retErr = err
				}
			}()
			_, err = io.Copy(tw, f)
			return err
		})
	})
}

// Reader converts a set of files to a tar stream.
// TODO: Probably should just go to disk for this.
func NewReader(files []File) (io.Reader, error) {
	buf := &bytes.Buffer{}
	if err := WithWriter(buf, func(tw *tar.Writer) error {
		for _, file := range files {
			hdr, err := file.Header()
			if err != nil {
				return err
			}
			if err := tw.WriteHeader(hdr); err != nil {
				return err
			}
			if err := file.Content(tw); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return buf, nil
}

// ConcatFileContent writes the content of each file in the stream to dst, one after another.
// Headers are not copied.
// `src` must be a tar stream
//
// This is not a terribly useful function but many of the tests are structured around
// the 1.x behavior of concatenating files, and so this is useful for porting those tests.
func ConcatFileContent(dst io.Writer, src io.Reader) error {
	tr := tar.NewReader(src)
	for _, err := tr.Next(); err != io.EOF; _, err = tr.Next() {
		_, err := io.Copy(dst, tr)
		if err != nil {
			return err
		}
	}
	return nil
}

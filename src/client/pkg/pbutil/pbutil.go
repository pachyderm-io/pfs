package pbutil

import (
	"encoding/binary"
	"io"
	"unsafe"

	"github.com/gogo/protobuf/proto"
	"github.com/pachyderm/pachyderm/src/client/pkg/errors"
)

// Reader is io.Reader for proto.Message instead of []byte.
type Reader interface {
	Read(val proto.Message) error
	ReadBytes() ([]byte, error)
}

// Writer is io.Writer for proto.Message instead of []byte.
type Writer interface {
	Write(val proto.Message) (int64, error)
	WriteBytes([]byte) (int64, error)
}

// ReadWriter is io.ReadWriter for proto.Message instead of []byte.
type ReadWriter interface {
	Reader
	Writer
}

type readWriter struct {
	w   io.Writer
	r   io.Reader
	buf []byte
}

func (r *readWriter) ReadBytes() ([]byte, error) {
	var l int64
	if err := binary.Read(r.r, binary.LittleEndian, &l); err != nil {
		return nil, err
	}
	if r.buf == nil || len(r.buf) < int(l) {
		r.buf = make([]byte, l)
	}
	buf := r.buf[0:l]
	if _, err := io.ReadFull(r.r, buf); err != nil {
		if err == io.EOF {
			return nil, io.ErrUnexpectedEOF
		}
		return nil, err
	}
	return buf, nil
}

// Read reads val from r.
func (r *readWriter) Read(val proto.Message) error {
	buf, err := r.ReadBytes()
	if err != nil {
		return err
	}
	return errors.WithStack(proto.Unmarshal(buf, val))
}

func (r *readWriter) WriteBytes(bytes []byte) (int64, error) {
	if err := binary.Write(r.w, binary.LittleEndian, int64(len(bytes))); err != nil {
		return 0, err
	}
	lenByteSize := unsafe.Sizeof(int64(len(bytes)))
	n, err := r.w.Write(bytes)
	return int64(lenByteSize) + int64(n), err
}

// Write writes val to r.
func (r *readWriter) Write(val proto.Message) (int64, error) {
	bytes, err := proto.Marshal(val)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	return r.WriteBytes(bytes)
}

// NewReader returns a new Reader with r as its source.
func NewReader(r io.Reader) Reader {
	return &readWriter{r: r}
}

// NewWriter returns a new Writer with w as its sink.
func NewWriter(w io.Writer) Writer {
	return &readWriter{w: w}
}

// NewReadWriter returns a new ReadWriter with rw as both its source and its sink.
func NewReadWriter(rw io.ReadWriter) ReadWriter {
	return &readWriter{r: rw, w: rw}
}

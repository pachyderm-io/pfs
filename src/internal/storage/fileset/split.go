package fileset

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"path"

	"github.com/pachyderm/pachyderm/v2/src/internal/errors"
	"github.com/pachyderm/pachyderm/v2/src/internal/sql"
)

type SplitType int

const (
	LINE SplitType = iota
	JSON
	SQL
	CSV
)

type splitter interface {
	next() ([]byte, error)
}

func newSplitter(r io.Reader, splitType SplitType) splitter {
	switch splitType {
	case LINE:
		return newLineSplitter(r)
	case JSON:
		return newJSONSplitter(r)
	case SQL:
		return newSQLSplitter(r)
	case CSV:
		return newCSVSplitter(r)
	default:
		panic("unrecognized split type")
	}
}

type lineSplitter struct {
	bufR *bufio.Reader
}

func newLineSplitter(r io.Reader) splitter {
	return &lineSplitter{
		bufR: bufio.NewReader(r),
	}
}

func (ls *lineSplitter) next() ([]byte, error) {
	return ls.bufR.ReadBytes('\n')
}

type jsonSplitter struct {
	decoder *json.Decoder
}

func newJSONSplitter(r io.Reader) splitter {
	return &jsonSplitter{
		decoder: json.NewDecoder(r),
	}
}

func (js *jsonSplitter) next() ([]byte, error) {
	var jsonValue json.RawMessage
	if err := js.decoder.Decode(&jsonValue); err != nil {
		return nil, err
	}
	return jsonValue, nil
}

type sqlSplitter struct {
	sqlR *sql.PGDumpReader
}

func newSQLSplitter(r io.Reader) splitter {
	return &sqlSplitter{
		sqlR: sql.NewPGDumpReader(bufio.NewReader(r)),
	}
}

func (ss *sqlSplitter) next() ([]byte, error) {
	row, err := ss.sqlR.ReadRow()
	if err != nil {
		return nil, err
	}
	return row, nil
}

type csvSplitter struct {
	csvR *csv.Reader
}

func newCSVSplitter(r io.Reader) splitter {
	return &csvSplitter{
		csvR: csv.NewReader(r),
	}
}

func (cs *csvSplitter) next() ([]byte, error) {
	buf := &bytes.Buffer{}
	csvW := csv.NewWriter(buf)
	row, err := cs.csvR.Read()
	if err != nil {
		return nil, err
	}
	if err := csvW.Write(row); err != nil {
		return nil, err
	}
	csvW.Flush()
	if csvW.Error() != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

type splitReader struct {
	splitter              splitter
	nextR                 *bytes.Reader
	datumsLeft, bytesLeft int64
}

func newSplitReader(splitter splitter, targetDatums, targetBytes int64) io.Reader {
	if targetDatums <= 0 {
		targetDatums = math.MaxInt64
	}
	if targetBytes <= 0 {
		targetBytes = math.MaxInt64
	}
	return &splitReader{
		splitter:   splitter,
		nextR:      bytes.NewReader([]byte{}),
		datumsLeft: targetDatums,
		bytesLeft:  targetBytes,
	}
}

func (sr *splitReader) Read(data []byte) (int, error) {
	if sr.nextR.Len() > 0 {
		return sr.nextR.Read(data)
	}
	if sr.datumsLeft == 0 || sr.bytesLeft <= 0 {
		return 0, io.EOF
	}
	next, err := sr.splitter.next()
	if err != nil {
		return 0, err
	}
	sr.datumsLeft--
	sr.bytesLeft -= int64(len(next))
	sr.nextR = bytes.NewReader(next)
	return sr.nextR.Read(data)
}

// TODO: Should the subPaths be padded so that they are lexicographically ordered?
// (i.e. list file order will not match up with put order).
func SplitFile(p string, r io.Reader, splitType SplitType, targetDatums, targetBytes int64, cb func(string, io.Reader) error) error {
	var subPath int64
	bufR := bufio.NewReader(r)
	splitter := newSplitter(bufR, splitType)
	for {
		// Check if we are at the end of the file being split.
		if _, err := bufR.Peek(1); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if err := cb(path.Join(p, fmt.Sprint(subPath)), newSplitReader(splitter, targetDatums, targetBytes)); err != nil {
			return err
		}
		subPath++
	}
}

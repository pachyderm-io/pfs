package client

import (
	"fmt"
	"strconv"
	"time"
	"unsafe"

	json "github.com/json-iterator/go"
	"github.com/modern-go/reflect2"
)

// From: https://github.com/grafana/loki/blob/d9380eaac950c669864c0af60fd99eae281d2438/pkg/loghttp/entry.go
func init() {
	json.RegisterExtension(&jsonExtension{})
}

// ResultType holds the type of the result
type ResultType string

// ResultValue interface mimics the promql.Value interface
// From: https://github.com/grafana/loki/blob/a9d85de4aa5290cf2f8b2dca5d08645bbd0dc66c/pkg/loghttp/query.go
type ResultValue interface {
	Type() ResultType
}

// ResultType values
const (
	ResultTypeStream = "streams"
)

// Type implements the promql.Value interface
// From: https://github.com/grafana/loki/blob/a9d85de4aa5290cf2f8b2dca5d08645bbd0dc66c/pkg/loghttp/query.go
func (Streams) Type() ResultType { return ResultTypeStream }

// Streams is a slice of Stream
// From: https://github.com/grafana/loki/blob/a9d85de4aa5290cf2f8b2dca5d08645bbd0dc66c/pkg/loghttp/query.go
type Streams []Stream

// Stream represents a log stream.  It includes a set of log entries and their labels.
// From:  https://github.com/grafana/loki/blob/a9d85de4aa5290cf2f8b2dca5d08645bbd0dc66c/pkg/loghttp/query.go
type Stream struct {
	Labels  LabelSet `json:"stream"`
	Entries []Entry  `json:"values"`
}

// LabelSet is a key/value pair mapping of labels
// From: https://github.com/grafana/loki/blob/d9380eaac950c669864c0af60fd99eae281d2438/pkg/loghttp/labels.go
type LabelSet map[string]string

// Entry represents a log entry.  It includes a log message and the time it occurred at.
// From: https://github.com/grafana/loki/blob/d9380eaac950c669864c0af60fd99eae281d2438/pkg/loghttp/entry.go
type Entry struct {
	Timestamp time.Time
	Line      string
}

// QueryResponseData represents the http json response to a label query
// From: https://github.com/grafana/loki/blob/a9d85de4aa5290cf2f8b2dca5d08645bbd0dc66c/pkg/loghttp/query.go
type QueryResponseData struct {
	ResultType ResultType  `json:"resultType"`
	Result     ResultValue `json:"result"`
	//Statistics interface{} `json:"stats"`
}

// QueryResponse represents the http json response to a Loki range and instant query
// From: https://github.com/grafana/loki/blob/a9d85de4aa5290cf2f8b2dca5d08645bbd0dc66c/pkg/loghttp/query.go
type QueryResponse struct {
	Status string            `json:"status"`
	Data   QueryResponseData `json:"data"`
}

// UnmarshalJSON implements the json.Unmarshaler interface.
// From: https://github.com/grafana/loki/blob/a9d85de4aa5290cf2f8b2dca5d08645bbd0dc66c/pkg/loghttp/query.go
func (q *QueryResponseData) UnmarshalJSON(data []byte) error {
	unmarshal := struct {
		Type   ResultType      `json:"resultType"`
		Result json.RawMessage `json:"result"`
		//Statistics stats.Result    `json:"stats"`
	}{}

	err := json.Unmarshal(data, &unmarshal)
	if err != nil {
		return err
	}

	var value ResultValue

	// unmarshal results
	switch unmarshal.Type {
	case ResultTypeStream:
		var s Streams
		err = json.Unmarshal(unmarshal.Result, &s)
		value = s
	/*case ResultTypeMatrix:
		var m Matrix
		err = json.Unmarshal(unmarshal.Result, &m)
		value = m
	case ResultTypeVector:
		var v Vector
		err = json.Unmarshal(unmarshal.Result, &v)
		value = v
	case ResultTypeScalar:
		var v Scalar
		err = json.Unmarshal(unmarshal.Result, &v)
		value = v*/
	default:
		return fmt.Errorf("unknown type: %s", unmarshal.Type)
	}

	if err != nil {
		return err
	}

	q.ResultType = unmarshal.Type
	q.Result = value
	//q.Statistics = unmarshal.Statistics

	return nil
}

// From: https://github.com/grafana/loki/blob/d9380eaac950c669864c0af60fd99eae281d2438/pkg/loghttp/entry.go
type jsonExtension struct {
	json.DummyExtension
}

// From: https://github.com/grafana/loki/blob/d9380eaac950c669864c0af60fd99eae281d2438/pkg/loghttp/entry.go
type sliceEntryDecoder struct{}

// From: https://github.com/grafana/loki/blob/d9380eaac950c669864c0af60fd99eae281d2438/pkg/loghttp/entry.go
func (sliceEntryDecoder) Decode(ptr unsafe.Pointer, iter *json.Iterator) {
	fmt.Println("Decode called")
	*((*[]Entry)(ptr)) = (*((*[]Entry)(ptr)))[:0]
	iter.ReadArrayCB(func(iter *json.Iterator) bool {
		i := 0
		var ts time.Time
		var line string
		ok := iter.ReadArrayCB(func(iter *json.Iterator) bool {
			var ok bool
			switch i {
			case 0:
				ts, ok = readTimestamp(iter)
				i++
				return ok
			case 1:
				line = iter.ReadString()
				i++
				if iter.Error != nil {
					return false
				}
				return true
			default:
				iter.ReportError("error reading entry", "array must contains 2 values")
				return false
			}
		})
		if ok {
			*((*[]Entry)(ptr)) = append(*((*[]Entry)(ptr)), Entry{
				Timestamp: ts,
				Line:      line,
			})
			return true
		}
		return false
	})
}

// From: https://github.com/grafana/loki/blob/d9380eaac950c669864c0af60fd99eae281d2438/pkg/loghttp/entry.go
func readTimestamp(iter *json.Iterator) (time.Time, bool) {
	s := iter.ReadString()
	if iter.Error != nil {
		return time.Time{}, false
	}
	t, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		iter.ReportError("error reading entry timestamp", err.Error())
		return time.Time{}, false

	}
	return time.Unix(0, t), true
}

//type entryEncoder struct{}
/*
func (entryEncoder) IsEmpty(ptr unsafe.Pointer) bool {
	// we don't omit-empty with log entries.
	return false
}*/

// From: https://github.com/grafana/loki/blob/d9380eaac950c669864c0af60fd99eae281d2438/pkg/loghttp/entry.go
func (e *jsonExtension) CreateDecoder(typ reflect2.Type) json.ValDecoder {
	if typ == reflect2.TypeOf([]Entry{}) {
		return sliceEntryDecoder{}
	}
	return nil
}

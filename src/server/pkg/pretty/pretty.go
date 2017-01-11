package pretty

import (
	"fmt"
	"strings"
	"time"

	"github.com/docker/go-units"
	"github.com/gogo/protobuf/types"
)

// UnescapeHTML returns s with < and > unescaped.
func UnescapeHTML(s string) string {
	s = strings.Replace(s, "\\u003c", "<", -1)
	s = strings.Replace(s, "\\u003e", ">", -1)
	return s
}

// Ago pretty-prints the amount of time that has passed
// since timestamp as a human-readable string.
func Ago(timestamp *types.Timestamp) string {
	t, _ := types.TimestampFromProto(timestamp)
	return fmt.Sprintf("%s ago", units.HumanDuration(time.Since(t)))
}

// Duration pretty-prints the duration of time between from
// and to as a human-reabable string.
func Duration(from *types.Timestamp, to *types.Timestamp) string {
	tFrom, _ := types.TimestampFromProto(from)
	tTo, _ := types.TimestampFromProto(to)
	return units.HumanDuration(tTo.Sub(tFrom))
}

// Size pretty-prints size amount of bytes as a human readable string.
func Size(size uint64) string {
	return units.BytesSize(float64(size))
}

package lokiutil

import (
	"context"
	"sort"
	"time"

	"github.com/pachyderm/pachyderm/src/client/pkg/errors"
	"github.com/pachyderm/pachyderm/src/server/pkg/errutil"
	loki "github.com/pachyderm/pachyderm/src/server/pkg/lokiutil/client"
)

const (
	// maxLogMessages is the maximum number of log messages the loki server
	// will send us, it will error if this is made higher.
	maxLogMessages = 5000
)

func forEachLine(resp loki.QueryResponse, f func(t time.Time, line string) error) error {
	// sort and display entries
	streams, ok := resp.Data.Result.(loki.Streams)
	if !ok {
		return errors.Errorf("resp.Data.Result must be of type loghttp.Streams to call ForEachStream on it")
	}
	allEntries := make([]streamEntryPair, 0)

	for _, s := range streams {
		for _, e := range s.Entries {
			allEntries = append(allEntries, streamEntryPair{
				entry:  e,
				labels: s.Labels,
			})
		}
	}

	sort.Slice(allEntries, func(i, j int) bool { return allEntries[i].entry.Timestamp.Before(allEntries[j].entry.Timestamp) })

	for _, e := range allEntries {
		if err := f(e.entry.Timestamp, e.entry.Line); err != nil {
			if err == errutil.ErrBreak {
				return nil
			}
			return err
		}
	}
	return nil
}

// QueryRange calls QueryRange on the passed loki.Client and calls f with each logline.
func QueryRange(ctx context.Context, c *loki.Client, queryStr string, from, through time.Time, follow bool, f func(t time.Time, line string) error) error {
	for {
		resp, err := c.QueryRange(ctx, queryStr, maxLogMessages, from, through, "FORWARD", 0, 0, true)
		if err != nil {
			return err
		}
		nMsgs := 0
		if err := forEachLine(*resp, func(t time.Time, line string) error {
			from = t
			nMsgs++
			return f(t, line)
		}); err != nil {
			return err
		}
		if !follow && nMsgs < maxLogMessages {
			return nil
		}
		from = from.Add(time.Nanosecond)
	}
}

type streamEntryPair struct {
	entry  loki.Entry
	labels loki.LabelSet
}

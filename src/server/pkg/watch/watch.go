// Package watch implements better watch semantics on top of etcd.
// See this issue for the reasoning behind the package:
// https://github.com/coreos/etcd/issues/7362
package watch

import (
	"context"

	etcd "github.com/coreos/etcd/clientv3"
	"github.com/gogo/protobuf/proto"
	"github.com/pachyderm/pachyderm/src/server/pkg/pbutil"
)

// EventType is the type of event
type EventType int

const (
	// EventPut happens when an item is added
	EventPut EventType = iota
	// EventDelete happens when an item is removed
	EventDelete
	// EventError happens when an error occurred
	EventError
)

// Event is an event that occurred to an item in etcd.
type Event struct {
	Key       []byte
	Value     []byte
	PrevKey   []byte
	PrevValue []byte
	Type      EventType
	Rev       int64
	Err       error
}

// Unmarshal unmarshals the item in an event into a protobuf message.
func (e *Event) Unmarshal(key *string, val proto.Unmarshaler) error {
	*key = string(e.Key)
	if len(e.Value) > 0 {
		return pbutil.Unmarshal(val, e.Value)
	}
	return nil
}

// UnmarshalPrev unmarshals the prev item in an event into a protobuf
// message.
func (e *Event) UnmarshalPrev(key *string, val proto.Unmarshaler) error {
	*key = string(e.PrevKey)
	if len(e.PrevValue) > 0 {
		return pbutil.Unmarshal(val, e.PrevValue)
	}
	return nil
}

// Watcher ...
type Watcher interface {
	// Watch returns a channel that delivers events
	Watch() <-chan *Event
	// Close this channel when you are done receiving events
	Close()
}

type watcher struct {
	eventCh chan *Event
	done    chan struct{}
}

func (w *watcher) Watch() <-chan *Event {
	return w.eventCh
}

func (w *watcher) Close() {
	close(w.done)
}

// sort the key-value pairs by revision time
type byModRev struct{ etcd.GetResponse }

func (s byModRev) Len() int {
	return len(s.GetResponse.Kvs)
}

func (s byModRev) Swap(i, j int) {
	s.GetResponse.Kvs[i], s.GetResponse.Kvs[j] = s.GetResponse.Kvs[j], s.GetResponse.Kvs[i]
}

func (s byModRev) Less(i, j int) bool {
	return s.GetResponse.Kvs[i].ModRevision < s.GetResponse.Kvs[j].ModRevision
}

// NewWatcher watches a given etcd prefix for events.
func NewWatcher(ctx context.Context, client *etcd.Client, prefix string) (Watcher, error) {
	return newWatcher(ctx, client, prefix, false)
}

// NewWatcherWithPrev is like NewWatcher, except that the returned events
// include the previous version of the values.
func NewWatcherWithPrev(ctx context.Context, client *etcd.Client, prefix string) (Watcher, error) {
	return newWatcher(ctx, client, prefix, true)
}

func newWatcher(ctx context.Context, client *etcd.Client, prefix string, withPrev bool) (Watcher, error) {
	eventCh := make(chan *Event)
	done := make(chan struct{})
	// Firstly we list the collection to get the current items
	// Sort them by ascending order because that's how the items would have
	// been returned if we watched them from the beginning.
	resp, err := client.Get(ctx, prefix, etcd.WithPrefix(), etcd.WithSort(etcd.SortByModRevision, etcd.SortAscend))
	if err != nil {
		return nil, err
	}

	nextRevision := resp.Header.Revision + 1
	etcdWatcher := etcd.NewWatcher(client)
	// Now we issue a watch that uses the revision timestamp returned by the
	// Get request earlier.  That way even if some items are added between
	// when we list the collection and when we start watching the collection,
	// we won't miss any items.
	options := []etcd.OpOption{etcd.WithPrefix(), etcd.WithRev(nextRevision)}
	if withPrev {
		options = append(options, etcd.WithPrevKV())
	}
	rch := etcdWatcher.Watch(ctx, prefix, options...)

	go func() (retErr error) {
		defer func() {
			if retErr != nil {
				select {
				case eventCh <- &Event{
					Err:  retErr,
					Type: EventError,
				}:
				case <-done:
				}
			}
			close(eventCh)
			etcdWatcher.Close()
		}()
		for _, etcdKv := range resp.Kvs {
			eventCh <- &Event{
				Key:   etcdKv.Key,
				Value: etcdKv.Value,
				Type:  EventPut,
				Rev:   etcdKv.ModRevision,
			}
		}
		for {
			var resp etcd.WatchResponse
			var ok bool
			select {
			case resp, ok = <-rch:
			case <-done:
				return nil
			}
			if !ok {
				if err := etcdWatcher.Close(); err != nil {
					return err
				}
				etcdWatcher = etcd.NewWatcher(client)
				rch = etcdWatcher.Watch(ctx, prefix, etcd.WithPrefix(), etcd.WithRev(nextRevision))
				continue
			}
			if err := resp.Err(); err != nil {
				return err
			}
			for _, etcdEv := range resp.Events {
				ev := &Event{
					Key:   etcdEv.Kv.Key,
					Value: etcdEv.Kv.Value,
					Rev:   etcdEv.Kv.ModRevision,
				}
				if etcdEv.PrevKv != nil {
					ev.PrevKey = etcdEv.PrevKv.Key
					ev.PrevValue = etcdEv.PrevKv.Value
				}
				if etcdEv.Type == etcd.EventTypePut {
					ev.Type = EventPut
				} else {
					ev.Type = EventDelete
				}
				select {
				case eventCh <- ev:
				case <-done:
					return nil
				}
			}
			nextRevision = resp.Header.Revision + 1
		}
	}()

	return &watcher{
		eventCh: eventCh,
		done:    done,
	}, nil
}

// MakeWatcher returns a Watcher that uses the given event channel and done
// channel internally to deliver events and signal closure, respectively.
func MakeWatcher(eventCh chan *Event, done chan struct{}) Watcher {
	return &watcher{
		eventCh: eventCh,
		done:    done,
	}
}

// Package watch implements better watch semantics on top of etcd.
// See this issue for the reasoning behind the package:
// https://github.com/coreos/etcd/issues/7362
package watch

import (
	"bytes"
	"context"
	"reflect"

	etcd "github.com/coreos/etcd/clientv3"
	"github.com/gogo/protobuf/proto"
	"github.com/pachyderm/pachyderm/src/client/pkg/errors"
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
	Key      []byte
	Value    []byte
	Type     EventType
	Rev      int64
	Ver      int64
	Err      error
	Template proto.Message
}

// Unmarshal unmarshals the item in an event into a protobuf message.
func (e *Event) Unmarshal(key *string, val proto.Message) error {
	if err := CheckType(e.Template, val); err != nil {
		return err
	}
	*key = string(e.Key)
	return errors.WithStack(proto.Unmarshal(e.Value, val))
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

// NewWatcher watches a given etcd prefix for events.
func NewWatcher(ctx context.Context, client *etcd.Client, trimPrefix, prefix string, template proto.Message, opts ...OpOption) (Watcher, error) {
	eventCh := make(chan *Event)
	done := make(chan struct{})
	// First list the collection to get the current items
	// Sort by mod revision--how the items would have been returned if we watched
	// them from the beginning.
	getOptions := []etcd.OpOption{etcd.WithPrefix(), etcd.WithSort(etcd.SortByModRevision, etcd.SortAscend)}
	for _, opt := range opts {
		if opt.Get != nil {
			getOptions = append(getOptions, etcd.OpOption(opt.Get))
		}
	}
	resp, err := client.Get(ctx, prefix, getOptions...)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	nextRevision := resp.Header.Revision + 1
	watchOptions := []etcd.OpOption{etcd.WithPrefix(), etcd.WithRev(nextRevision)}
	for _, opt := range opts {
		if opt.Watch != nil {
			watchOptions = append(watchOptions, etcd.OpOption(opt.Watch))
		}
	}

	etcdWatcher := etcd.NewWatcher(client)
	// Issue a watch that uses the revision timestamp returned by the
	// Get request earlier.  That way even if some items are added between
	// when we list the collection and when we start watching the collection,
	// we won't miss any items.

	rch := etcdWatcher.Watch(ctx, prefix, watchOptions...)

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
				Key:      bytes.TrimPrefix(etcdKv.Key, []byte(trimPrefix)),
				Value:    etcdKv.Value,
				Type:     EventPut,
				Rev:      etcdKv.ModRevision,
				Ver:      etcdKv.Version,
				Template: template,
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
					return errors.WithStack(err)
				}
				etcdWatcher = etcd.NewWatcher(client)
				// use new "nextRevision"
				options := []etcd.OpOption{etcd.WithPrefix(), etcd.WithRev(nextRevision)}
				for _, opt := range opts {
					if opt.Watch != nil {
						options = append(options, etcd.OpOption(opt.Watch))
					}
				}
				rch = etcdWatcher.Watch(ctx, prefix, options...)
				continue
			}
			if err := resp.Err(); err != nil {
				return errors.WithStack(err)
			}
			for _, etcdEv := range resp.Events {
				ev := &Event{
					Key:      bytes.TrimPrefix(etcdEv.Kv.Key, []byte(trimPrefix)),
					Value:    etcdEv.Kv.Value,
					Rev:      etcdEv.Kv.ModRevision,
					Ver:      etcdEv.Kv.Version,
					Template: template,
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

// CheckType checks to make sure val has the same type as template, unless
// template is nil in which case it always returns nil.
func CheckType(template proto.Message, val interface{}) error {
	if template != nil {
		valType, templateType := reflect.TypeOf(val), reflect.TypeOf(template)
		if valType != templateType {
			return errors.Errorf("invalid type, got: %s, expected: %s", valType, templateType)
		}
	}
	return nil
}

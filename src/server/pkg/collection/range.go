package collection

import (
	"fmt"
	"sort"
	"sync/atomic"

	etcd "github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/pachyderm/pachyderm/src/client/pkg/errutil"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Options are the sort options when iterating through etcd key/values.
// Currently implemented sort targets are CreateRevision and ModRevision.
// The sorting can be done in the calling process by setting SelfSort to true.
type Options struct {
	Target   etcd.SortTarget
	Order    etcd.SortOrder
	SelfSort bool
}

// DefaultOptions are the default sort options when iterating through etcd key/values.
var DefaultOptions = &Options{etcd.SortByCreateRevision, etcd.SortDescend, false}

func listFuncs(opts *Options) (func(*mvccpb.KeyValue) etcd.OpOption, func(kv1 *mvccpb.KeyValue, kv2 *mvccpb.KeyValue) int) {
	var from func(*mvccpb.KeyValue) etcd.OpOption
	var compare func(kv1 *mvccpb.KeyValue, kv2 *mvccpb.KeyValue) int
	switch opts.Target {
	case etcd.SortByCreateRevision:
		switch opts.Order {
		case etcd.SortAscend:
			from = func(fromKey *mvccpb.KeyValue) etcd.OpOption { return etcd.WithMinCreateRev(fromKey.CreateRevision) }
		case etcd.SortDescend:
			from = func(fromKey *mvccpb.KeyValue) etcd.OpOption { return etcd.WithMaxCreateRev(fromKey.CreateRevision) }
		}
		compare = func(kv1 *mvccpb.KeyValue, kv2 *mvccpb.KeyValue) int {
			return int(kv1.CreateRevision - kv2.CreateRevision)
		}
	case etcd.SortByModRevision:
		switch opts.Order {
		case etcd.SortAscend:
			from = func(fromKey *mvccpb.KeyValue) etcd.OpOption { return etcd.WithMinModRev(fromKey.ModRevision) }
		case etcd.SortDescend:
			from = func(fromKey *mvccpb.KeyValue) etcd.OpOption { return etcd.WithMaxModRev(fromKey.ModRevision) }
		}
		compare = func(kv1 *mvccpb.KeyValue, kv2 *mvccpb.KeyValue) int {
			return int(kv1.ModRevision - kv2.ModRevision)
		}
	}
	return from, compare
}

func listRevision(c *readonlyCollection, prefix string, limitPtr *int64, opts *Options, f func(*mvccpb.KeyValue) error) error {
	etcdOpts := []etcd.OpOption{etcd.WithPrefix(), etcd.WithSort(opts.Target, opts.Order)}
	var fromKey *mvccpb.KeyValue
	from, compare := listFuncs(opts)
	for {
		if fromKey != nil {
			etcdOpts = append(etcdOpts, from(fromKey))
		}
		resp, done, err := getWithLimit(c, prefix, limitPtr, etcdOpts)
		if err != nil {
			return err
		}
		kvs := getNewKeys(resp.Kvs, fromKey)
		for _, kv := range kvs {
			if err := f(kv); err != nil {
				if err == errutil.ErrBreak {
					return nil
				}
				return err
			}
		}
		if done {
			return nil
		}
		if compare(resp.Kvs[0], resp.Kvs[len(resp.Kvs)-1]) == 0 {
			return fmt.Errorf("revision contains too many objects to fit in one batch (this is likely a bug)")
		}
		fromKey = kvs[len(kvs)-1]
	}
}

func getNewKeys(respKvs []*mvccpb.KeyValue, fromKey *mvccpb.KeyValue) []*mvccpb.KeyValue {
	if fromKey == nil {
		return respKvs
	}
	for i, kv := range respKvs {
		if string(kv.Key) == string(fromKey.Key) {
			return respKvs[i+1:]
		}
	}
	return nil
}

type kvSort struct {
	kvs     []*mvccpb.KeyValue
	compare func(kv1 *mvccpb.KeyValue, kv2 *mvccpb.KeyValue) int
}

func listSelfSortRevision(c *readonlyCollection, prefix string, limitPtr *int64, opts *Options, f func(*mvccpb.KeyValue) error) error {
	etcdOpts := []etcd.OpOption{etcd.WithFromKey(), etcd.WithRange(endKeyFromPrefix(prefix))}
	fromKey := prefix
	kvs := []*mvccpb.KeyValue{}
	for {
		resp, done, err := getWithLimit(c, fromKey, limitPtr, etcdOpts)
		if err != nil {
			return err
		}
		if fromKey == prefix {
			kvs = append(kvs, resp.Kvs...)
		} else {
			kvs = append(kvs, resp.Kvs[1:]...)
		}
		if done {
			break
		}
		fromKey = string(kvs[len(kvs)-1].Key)
	}
	_, compare := listFuncs(opts)
	sorter := &kvSort{kvs, compare}
	switch opts.Order {
	case etcd.SortAscend:
		sort.Sort(sorter)
	case etcd.SortDescend:
		sort.Sort(sort.Reverse(sorter))
	}
	for _, kv := range kvs {
		if err := f(kv); err != nil {
			if err == errutil.ErrBreak {
				return nil
			}
			return err
		}
	}
	return nil
}

func endKeyFromPrefix(prefix string) string {
	// Lexicographically increment the last character
	return prefix[0:len(prefix)-1] + string(byte(prefix[len(prefix)-1])+1)
}

func (s *kvSort) Len() int {
	return len(s.kvs)
}

func (s *kvSort) Less(i, j int) bool {
	return s.compare(s.kvs[i], s.kvs[j]) < 0
}

func (s *kvSort) Swap(i, j int) {
	t := s.kvs[i]
	s.kvs[i] = s.kvs[j]
	s.kvs[j] = t
}

func getWithLimit(c *readonlyCollection, key string, limitPtr *int64, opts []etcd.OpOption) (*etcd.GetResponse, bool, error) {
	for {
		limit := atomic.LoadInt64(limitPtr)
		resp, err := c.etcdClient.Get(c.ctx, key, append(opts, etcd.WithLimit(limit))...)
		if err != nil {
			if status.Convert(err).Code() == codes.ResourceExhausted && limit > 1 {
				atomic.CompareAndSwapInt64(limitPtr, limit, limit/2)
				continue
			}
			return nil, false, err
		}
		if len(resp.Kvs) < int(limit) {
			return resp, true, nil
		}
		return resp, false, nil
	}
}

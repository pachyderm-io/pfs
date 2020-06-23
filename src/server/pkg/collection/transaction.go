package collection

// Copyright 2016 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// We copy this code from etcd because the etcd implementation of STM does
// not have the DelAll method, which we need.

import (
	"bytes"
	"sort"
	"strings"

	v3 "github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/etcdserver/api/v3rpc/rpctypes"
	"github.com/pachyderm/pachyderm/src/client/pkg/errors"
	"github.com/pachyderm/pachyderm/src/client/pkg/tracing"
	"golang.org/x/net/context"
)

// STM is an interface for software transactional memory.
type STM interface {
	// Get returns the value for a key and inserts the key in the txn's read set.
	// If Get fails, it aborts the transaction with an error, never returning.
	Get(key string) (string, error)
	// Put adds a value for a key to the write set.
	Put(key, val string, ttl int64, ptr uintptr) error
	// Rev returns the revision of a key in the read set.
	Rev(key string) int64
	// Del deletes a key.
	Del(key string)
	// TTL returns the remaining time to live for 'key', or 0 if 'key' has no TTL
	TTL(key string) (int64, error)
	// DelAll deletes all keys with the given prefix
	// Note that the current implementation of DelAll is incomplete.
	// To use DelAll safely, do not issue any Get/Put operations after
	// DelAll is called.
	DelAll(key string)
	Context() context.Context
	// SetSafePutCheck sets the bit pattern to check if a put is safe.
	SetSafePutCheck(key string, ptr uintptr)
	// IsSafePut checks against the bit pattern for a key to see if it is safe to put.
	IsSafePut(key string, ptr uintptr) bool

	// commit attempts to apply the txn's changes to the server.
	commit() *v3.TxnResponse
	reset()
	fetch(key string) *v3.GetResponse
}

// stmError safely passes STM errors through panic to the STM error channel.
type stmError struct{ err error }

// NewSTM intiates a new STM operation. It uses a serializable model.
func NewSTM(ctx context.Context, c *v3.Client, apply func(STM) error) (*v3.TxnResponse, error) {
	return newSTMSerializable(ctx, c, apply, false)
}

// NewDryrunSTM intiates a new STM operation, but the final commit is skipped.
// It uses a serializable model.
func NewDryrunSTM(ctx context.Context, c *v3.Client, apply func(STM) error) error {
	_, err := newSTMSerializable(ctx, c, apply, true)
	return err
}

// newSTMSerializable initiates a new serialized transaction; reads within the
// same transaction attempt to return data from the revision of the first read.
func newSTMSerializable(ctx context.Context, c *v3.Client, apply func(STM) error, dryrun bool) (*v3.TxnResponse, error) {
	s := &stmSerializable{
		stm:      stm{client: c, ctx: ctx},
		prefetch: make(map[string]*v3.GetResponse),
	}
	return runSTM(s, apply, dryrun)
}

type stmResponse struct {
	resp *v3.TxnResponse
	err  error
}

func runSTM(s STM, apply func(STM) error, dryrun bool) (*v3.TxnResponse, error) {
	outc := make(chan stmResponse, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				e, ok := r.(stmError)
				if !ok {
					// client apply panicked
					panic(r)
				}
				outc <- stmResponse{nil, e.err}
			}
		}()
		var out stmResponse
		for {
			s.reset()
			if out.err = apply(s); out.err != nil {
				break
			}
			if dryrun {
				break
			} else if out.resp = s.commit(); out.resp != nil {
				break
			}
		}
		outc <- out
	}()
	r := <-outc
	return r.resp, errors.WithStack(r.err)
}

// stm implements repeatable-read software transactional memory over etcd
type stm struct {
	client *v3.Client
	ctx    context.Context
	// rset holds read key values and revisions
	rset map[string]*v3.GetResponse
	// wset holds overwritten keys and their values
	wset map[string]stmPut
	// deletedPrefixes holds the set of prefixes that have been deleted
	deletedPrefixes []string
	// getOpts are the opts used for gets. Includes revision of first read for
	// stmSerializable
	getOpts []v3.OpOption
	// ttlset is a cache from key to lease TTL. It's similar to rset in that it
	// caches leases that have already been read, but each may contain keys not in
	// the other (ttlset in particular caches the TTL of all keys associated with
	// a lease after reading that lease, even if the other keys haven't been read)
	ttlset map[string]int64
	// newLeases is a map from TTL to lease ID; it caches new leases used for this
	// write. We de-dupe leases by TTL (values written with the same TTL get the
	// same lease) so that kvs in a collection and its indexes all share a lease.
	// It's similar to wset for TTLs.
	newLeases map[int64]v3.LeaseID
}

type stmPut struct {
	val        string
	ttl        int64
	op         v3.Op
	safePutPtr uintptr
}

func (s *stm) Context() context.Context {
	return s.ctx
}

func (s *stm) Get(key string) (string, error) {
	if wv, ok := s.wset[key]; ok {
		return wv.val, nil
	}
	if s.isKeyRangeDeleted(key) {
		return "", ErrNotFound{Key: key}
	}
	return respToValue(key, s.fetch(key))
}

func (s *stm) SetSafePutCheck(key string, ptr uintptr) {
	if wv, ok := s.wset[key]; ok {
		wv.safePutPtr = ptr
		s.wset[key] = wv
	}
}

func (s *stm) IsSafePut(key string, ptr uintptr) bool {
	if _, ok := s.wset[key]; ok && s.wset[key].safePutPtr != 0 && ptr != s.wset[key].safePutPtr {
		return false
	}
	return true
}

func (s *stm) isKeyRangeDeleted(key string) bool {
	for _, prefix := range s.deletedPrefixes {
		if strings.HasPrefix(key, prefix) {
			return true
		}
	}
	return false
}

func (s *stm) Put(key, val string, ttl int64, ptr uintptr) error {
	var options []v3.OpOption
	if ttl > 0 {
		lease, ok := s.newLeases[ttl]
		if !ok {
			span, ctx := tracing.AddSpanToAnyExisting(s.ctx, "/etcd/GrantLease")
			defer tracing.FinishAnySpan(span)
			leaseResp, err := s.client.Grant(ctx, ttl)
			if err != nil {
				return errors.Wrapf(err, "error granting lease")
			}
			lease = leaseResp.ID
			s.newLeases[ttl] = lease
			s.ttlset[key] = ttl // cache key->ttl, in case it's read later
		}
		options = append(options, v3.WithLease(lease))
	}
	s.wset[key] = stmPut{val, ttl, v3.OpPut(key, val, options...), ptr}
	return nil
}

func (s *stm) Del(key string) {
	s.wset[key] = stmPut{"", 0, v3.OpDelete(key), 0}
}

func (s *stm) DelAll(prefix string) {
	// Remove any eclipsed deletes then add the new delete
	isEclipsed := false
	i := 0
	for _, deletedPrefix := range s.deletedPrefixes {
		if strings.HasPrefix(prefix, deletedPrefix) {
			isEclipsed = true
		}
		if !strings.HasPrefix(deletedPrefix, prefix) {
			s.deletedPrefixes[i] = deletedPrefix
			i++
		}
	}
	s.deletedPrefixes = s.deletedPrefixes[:i]

	// If the new DelAll prefix is eclipsed by an already-deleted prefix, don't
	// add it to the set, but still clean up any eclipsed writes.
	if !isEclipsed {
		s.deletedPrefixes = append(s.deletedPrefixes, prefix)
	}

	for k := range s.wset {
		if strings.HasPrefix(k, prefix) {
			delete(s.wset, k)
		}
	}
}

func (s *stm) Rev(key string) int64 {
	if resp := s.fetch(key); resp != nil && len(resp.Kvs) != 0 {
		return resp.Kvs[0].ModRevision
	}
	return 0
}

func (s *stm) commit() *v3.TxnResponse {
	span, ctx := tracing.AddSpanToAnyExisting(s.ctx, "/etcd/Txn")
	defer tracing.FinishAnySpan(span)

	cmps := s.cmps()
	writes := s.writes()
	txnresp, err := s.client.Txn(ctx).If(cmps...).Then(writes...).Commit()
	if err == rpctypes.ErrTooManyOps {
		panic(stmError{
			errors.Errorf(
				"%v (%d comparisons, %d writes: hint: set --max-txn-ops on the "+
					"ETCD cluster to at least the largest of those values)",
				err, len(cmps), len(writes)),
		})
	} else if err != nil {
		panic(stmError{err})
	}
	if txnresp.Succeeded {
		return txnresp
	}
	return nil
}

// cmps guards the txn from updates to read set
func (s *stm) cmps() []v3.Cmp {
	cmps := make([]v3.Cmp, 0, len(s.rset))
	for k, rk := range s.rset {
		cmps = append(cmps, isKeyCurrent(k, rk))
	}
	return cmps
}

func (s *stm) fetch(key string) *v3.GetResponse {
	if resp, ok := s.rset[key]; ok {
		return resp
	}

	span, ctx := tracing.AddSpanToAnyExisting(s.ctx, "/etcd.stm/Get", "key", key)
	defer tracing.FinishAnySpan(span)
	resp, err := s.client.Get(ctx, key, s.getOpts...)
	if err != nil {
		panic(stmError{err})
	}
	s.rset[key] = resp
	return resp
}

// writes is the list of ops for all pending writes
func (s *stm) writes() []v3.Op {
	prefixes := s.deletedPrefixes
	puts := make([]string, 0, len(s.wset))
	for key := range s.wset {
		puts = append(puts, key)
	}
	sort.Strings(puts)
	sort.Strings(s.deletedPrefixes)

	writes := make([]v3.Op, 0, 2*len(s.wset)+len(s.deletedPrefixes))
	i := 0 // index into puts
	j := 0 // index into prefixes
	for i < len(puts) && j < len(prefixes) {
		if puts[i] < prefixes[j] {
			// This is a standalone put, nothing fancy here
			writes = append(writes, s.wset[puts[i]].op)
			i++
		} else {
			// There may be puts within a deleted range, but we can't have two
			// overlapping writes - break up the deleted range into multiple deletes.
			start := prefixes[j]
			for i < len(puts) && strings.HasPrefix(puts[i], prefixes[j]) {
				writes = append(writes, v3.OpDelete(start, v3.WithRange(puts[i])))
				writes = append(writes, s.wset[puts[i]].op)
				start = puts[i] + "\x00"
				i++
			}
			writes = append(writes, v3.OpDelete(start, v3.WithRange(v3.GetPrefixRangeEnd(prefixes[j]))))
			j++
		}
	}
	for i < len(puts) {
		writes = append(writes, s.wset[puts[i]].op)
		i++
	}
	for j < len(prefixes) {
		writes = append(writes, v3.OpDelete(prefixes[j], v3.WithPrefix()))
		j++
	}
	return writes
}

func (s *stm) reset() {
	s.rset = make(map[string]*v3.GetResponse)
	s.wset = make(map[string]stmPut)
	s.deletedPrefixes = []string{}
	s.ttlset = make(map[string]int64)
	s.newLeases = make(map[int64]v3.LeaseID)
}

type stmSerializable struct {
	stm
	prefetch map[string]*v3.GetResponse
}

func (s *stmSerializable) Get(key string) (string, error) {
	if wv, ok := s.wset[key]; ok {
		return wv.val, nil
	}
	if s.isKeyRangeDeleted(key) {
		return "", ErrNotFound{Key: key}
	}
	return respToValue(key, s.fetch(key))
}

func (s *stmSerializable) fetch(key string) *v3.GetResponse {
	firstRead := len(s.rset) == 0
	if resp, ok := s.prefetch[key]; ok {
		delete(s.prefetch, key)
		s.rset[key] = resp
	}
	resp := s.stm.fetch(key)
	if firstRead {
		// txn's base revision is defined by the first read
		s.getOpts = []v3.OpOption{
			v3.WithRev(resp.Header.Revision),
			v3.WithSerializable(),
		}
	}
	return resp
}

func (s *stmSerializable) Rev(key string) int64 {
	s.Get(key)
	return s.stm.Rev(key)
}

func (s *stmSerializable) gets() ([]string, []v3.Op) {
	keys := make([]string, 0, len(s.rset))
	ops := make([]v3.Op, 0, len(s.rset))
	for k := range s.rset {
		keys = append(keys, k)
		ops = append(ops, v3.OpGet(k))
	}
	return keys, ops
}

func (s *stmSerializable) commit() *v3.TxnResponse {
	span, ctx := tracing.AddSpanToAnyExisting(s.ctx, "/etcd/Txn")
	defer tracing.FinishAnySpan(span)
	if span != nil {
		keys := make([]byte, 0, 512)
		for k := range s.wset {
			keys = append(append(keys, ','), k...)
		}
		span.SetTag("updated-keys", string(bytes.TrimLeft(keys, ",")))
	}

	keys, getops := s.gets()
	cmps := s.cmps()
	writes := s.writes()
	txn := s.client.Txn(ctx).If(cmps...).Then(writes...)
	// use Else to prefetch keys in case of conflict to save a round trip
	txnresp, err := txn.Else(getops...).Commit()
	if err == rpctypes.ErrTooManyOps {
		panic(stmError{
			errors.Errorf(
				"%v (%d comparisons, %d writes: hint: set --max-txn-ops on the "+
					"ETCD cluster to at least the largest of those values)",
				err, len(cmps), len(writes)),
		})
	} else if err != nil {
		panic(stmError{err})
	}

	tracing.TagAnySpan(span, "applied-at-revision", txnresp.Header.Revision)
	if txnresp.Succeeded {
		return txnresp
	}
	// load prefetch with Else data
	for i := range keys {
		resp := txnresp.Responses[i].GetResponseRange()
		s.rset[keys[i]] = (*v3.GetResponse)(resp)
	}
	s.prefetch = s.rset
	s.getOpts = nil
	return nil
}

func isKeyCurrent(k string, r *v3.GetResponse) v3.Cmp {
	if len(r.Kvs) != 0 {
		return v3.Compare(v3.ModRevision(k), "=", r.Kvs[0].ModRevision)
	}
	return v3.Compare(v3.ModRevision(k), "=", 0)
}

func respToValue(key string, resp *v3.GetResponse) (string, error) {
	if len(resp.Kvs) == 0 {
		return "", ErrNotFound{Key: key}
	}
	return string(resp.Kvs[0].Value), nil
}

// fetchTTL contains the essential implementation of TTL().
//
// Note that 'iface' should either be the receiver 's' or a containing
// 'stmSerializeable'--the only reason 'iface' is passed as a separate argument
// is because fetchTTL calls iface.fetch(), and the implementation of 'fetch' is
// different for stm and stmSerializeable. Passing the interface ensures the
// correct version of fetch() is called
func (s *stm) fetchTTL(iface STM, key string) (int64, error) {
	// check wset cache
	if wv, ok := s.wset[key]; ok {
		return wv.ttl, nil
	}
	if s.isKeyRangeDeleted(key) {
		return 0, ErrNotFound{Key: key}
	}

	// Read ttl through s.ttlset cache
	if ttl, ok := s.ttlset[key]; ok {
		return ttl, nil
	}

	// Read kv and lease ID, and cache new TTL
	getResp := iface.fetch(key) // call correct implementation of fetch()
	if len(getResp.Kvs) == 0 {
		return 0, ErrNotFound{Key: key}
	}
	leaseID := v3.LeaseID(getResp.Kvs[0].Lease)
	if leaseID == 0 {
		s.ttlset[key] = 0 // 0 is default value, but now 'ok' will be true on check
		return 0, nil
	}
	span, ctx := tracing.AddSpanToAnyExisting(s.ctx, "/etcd.stm/TimeToLive", "key", key)
	defer tracing.FinishAnySpan(span)
	leaseResp, err := s.client.TimeToLive(ctx, leaseID)
	if err != nil {
		panic(stmError{err})
	}
	s.ttlset[key] = leaseResp.TTL
	for _, key := range leaseResp.Keys {
		s.ttlset[string(key)] = leaseResp.TTL
	}
	return leaseResp.TTL, nil
}

func (s *stm) TTL(key string) (int64, error) {
	return s.fetchTTL(s, key)
}

func (s *stmSerializable) TTL(key string) (int64, error) {
	return s.fetchTTL(s, key)
}

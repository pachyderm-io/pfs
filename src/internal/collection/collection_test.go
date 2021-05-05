package collection_test

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pachyderm/pachyderm/v2/src/client"
	col "github.com/pachyderm/pachyderm/v2/src/internal/collection"
	"github.com/pachyderm/pachyderm/v2/src/internal/errors"
	"github.com/pachyderm/pachyderm/v2/src/internal/require"
	"github.com/pachyderm/pachyderm/v2/src/internal/testetcd"
	"github.com/pachyderm/pachyderm/v2/src/internal/uuid"
	"github.com/pachyderm/pachyderm/v2/src/internal/watch"
	"github.com/pachyderm/pachyderm/v2/src/pfs"
	"github.com/pachyderm/pachyderm/v2/src/pps"

	etcd "github.com/coreos/etcd/clientv3"
	"github.com/gogo/protobuf/types"
)

var (
	pipelineIndex *col.Index = &col.Index{
		Field: "Pipeline",
		Multi: false,
	}
	commitMultiIndex *col.Index = &col.Index{
		Field: "Provenance",
		Multi: true,
	}
)

func TestDryrun(t *testing.T) {
	etcdClient := getEtcdClient(t)
	uuidPrefix := uuid.NewWithoutDashes()

	jobs := col.NewCollection(etcdClient, uuidPrefix, nil, &pps.PipelineJobInfo{}, nil, nil)

	job := &pps.PipelineJobInfo{
		Job:      client.NewJob("j1"),
		Pipeline: client.NewPipeline("p1"),
	}
	err := col.NewDryrunSTM(context.Background(), etcdClient, func(stm col.STM) error {
		return jobs.ReadWrite(stm).Put(job.Job.ID, job)
	})
	require.NoError(t, err)

	err = jobs.ReadOnly(context.Background()).Get("j1", job)
	require.True(t, col.IsErrNotFound(err))
}

func TestDelNonexistant(t *testing.T) {
	etcdClient := getEtcdClient(t)
	uuidPrefix := uuid.NewWithoutDashes()

	jobs := col.NewCollection(etcdClient, uuidPrefix, nil, &pps.PipelineJobInfo{}, nil, nil)

	_, err := col.NewSTM(context.Background(), etcdClient, func(stm col.STM) error {
		err := jobs.ReadWrite(stm).Delete("test")
		require.True(t, col.IsErrNotFound(err))
		return err
	})
	require.True(t, col.IsErrNotFound(err))
}

func TestGetAfterDel(t *testing.T) {
	etcdClient := getEtcdClient(t)
	uuidPrefix := uuid.NewWithoutDashes()

	jobs := col.NewCollection(etcdClient, uuidPrefix, nil, &pps.PipelineJobInfo{}, nil, nil)

	j1 := &pps.PipelineJobInfo{
		Job:      client.NewJob("j1"),
		Pipeline: client.NewPipeline("p1"),
	}
	j2 := &pps.PipelineJobInfo{
		Job:      client.NewJob("j2"),
		Pipeline: client.NewPipeline("p1"),
	}
	j3 := &pps.PipelineJobInfo{
		Job:      client.NewJob("j3"),
		Pipeline: client.NewPipeline("p2"),
	}
	_, err := col.NewSTM(context.Background(), etcdClient, func(stm col.STM) error {
		rw := jobs.ReadWrite(stm)
		rw.Put(j1.Job.ID, j1)
		rw.Put(j2.Job.ID, j2)
		rw.Put(j3.Job.ID, j3)
		return nil
	})
	require.NoError(t, err)

	_, err = col.NewSTM(context.Background(), etcdClient, func(stm col.STM) error {
		job := &pps.PipelineJobInfo{}
		rw := jobs.ReadWrite(stm)
		if err := rw.Get(j1.Job.ID, job); err != nil {
			return err
		}

		if err := rw.Get("j4", job); !col.IsErrNotFound(err) {
			return errors.Wrapf(err, "Expected ErrNotFound for key '%s', but got", "j4")
		}

		rw.DeleteAll()

		if err := rw.Get(j1.Job.ID, job); !col.IsErrNotFound(err) {
			return errors.Wrapf(err, "Expected ErrNotFound for key '%s', but got", j1.Job.ID)
		}
		if err := rw.Get(j2.Job.ID, job); !col.IsErrNotFound(err) {
			return errors.Wrapf(err, "Expected ErrNotFound for key '%s', but got", j2.Job.ID)
		}
		return nil
	})
	require.NoError(t, err)

	count, err := jobs.ReadOnly(context.Background()).Count()
	require.NoError(t, err)
	require.Equal(t, int64(0), count)
}

func TestDeletePrefix(t *testing.T) {
	etcdClient := getEtcdClient(t)
	uuidPrefix := uuid.NewWithoutDashes()

	jobs := col.NewCollection(etcdClient, uuidPrefix, nil, &pps.PipelineJobInfo{}, nil, nil)

	j1 := &pps.PipelineJobInfo{
		Job:      client.NewJob("prefix/suffix/job"),
		Pipeline: client.NewPipeline("p"),
	}
	j2 := &pps.PipelineJobInfo{
		Job:      client.NewJob("prefix/suffix/job2"),
		Pipeline: client.NewPipeline("p"),
	}
	j3 := &pps.PipelineJobInfo{
		Job:      client.NewJob("prefix/job3"),
		Pipeline: client.NewPipeline("p"),
	}
	j4 := &pps.PipelineJobInfo{
		Job:      client.NewJob("job4"),
		Pipeline: client.NewPipeline("p"),
	}

	_, err := col.NewSTM(context.Background(), etcdClient, func(stm col.STM) error {
		rw := jobs.ReadWrite(stm)
		rw.Put(j1.Job.ID, j1)
		rw.Put(j2.Job.ID, j2)
		rw.Put(j3.Job.ID, j3)
		rw.Put(j4.Job.ID, j4)
		return nil
	})
	require.NoError(t, err)

	_, err = col.NewSTM(context.Background(), etcdClient, func(stm col.STM) error {
		job := &pps.PipelineJobInfo{}
		rw := jobs.ReadWrite(stm)

		rw.DeleteAllPrefix("prefix/suffix")
		if err := rw.Get(j1.Job.ID, job); !col.IsErrNotFound(err) {
			return errors.Wrapf(err, "Expected ErrNotFound for key '%s', but got", j1.Job.ID)
		}
		if err := rw.Get(j2.Job.ID, job); !col.IsErrNotFound(err) {
			return errors.Wrapf(err, "Expected ErrNotFound for key '%s', but got", j2.Job.ID)
		}
		if err := rw.Get(j3.Job.ID, job); err != nil {
			return err
		}
		if err := rw.Get(j4.Job.ID, job); err != nil {
			return err
		}

		rw.DeleteAllPrefix("prefix")
		if err := rw.Get(j1.Job.ID, job); !col.IsErrNotFound(err) {
			return errors.Wrapf(err, "Expected ErrNotFound for key '%s', but got", j1.Job.ID)
		}
		if err := rw.Get(j2.Job.ID, job); !col.IsErrNotFound(err) {
			return errors.Wrapf(err, "Expected ErrNotFound for key '%s', but got", j2.Job.ID)
		}
		if err := rw.Get(j3.Job.ID, job); !col.IsErrNotFound(err) {
			return errors.Wrapf(err, "Expected ErrNotFound for key '%s', but got", j3.Job.ID)
		}
		if err := rw.Get(j4.Job.ID, job); err != nil {
			return err
		}

		rw.Put(j1.Job.ID, j1)
		if err := rw.Get(j1.Job.ID, job); err != nil {
			return err
		}

		rw.DeleteAllPrefix("prefix/suffix")
		if err := rw.Get(j1.Job.ID, job); !col.IsErrNotFound(err) {
			return errors.Wrapf(err, "Expected ErrNotFound for key '%s', but got", j1.Job.ID)
		}

		rw.Put(j2.Job.ID, j2)
		if err := rw.Get(j2.Job.ID, job); err != nil {
			return err
		}

		return nil
	})
	require.NoError(t, err)

	job := &pps.PipelineJobInfo{}
	ro := jobs.ReadOnly(context.Background())
	require.True(t, col.IsErrNotFound(ro.Get(j1.Job.ID, job)))
	require.NoError(t, ro.Get(j2.Job.ID, job))
	require.Equal(t, j2, job)
	require.True(t, col.IsErrNotFound(ro.Get(j3.Job.ID, job)))
	require.NoError(t, ro.Get(j4.Job.ID, job))
	require.Equal(t, j4, job)
}

func TestIndex(t *testing.T) {
	etcdClient := getEtcdClient(t)
	uuidPrefix := uuid.NewWithoutDashes()

	pipelineJobInfos := col.NewCollection(etcdClient, uuidPrefix, []*col.Index{pipelineIndex}, &pps.PipelineJobInfo{}, nil, nil)

	j1 := &pps.PipelineJobInfo{
		Job:      client.NewJob("j1"),
		Pipeline: client.NewPipeline("p1"),
	}
	j2 := &pps.PipelineJobInfo{
		Job:      client.NewJob("j2"),
		Pipeline: client.NewPipeline("p1"),
	}
	j3 := &pps.PipelineJobInfo{
		Job:      client.NewJob("j3"),
		Pipeline: client.NewPipeline("p2"),
	}
	_, err := col.NewSTM(context.Background(), etcdClient, func(stm col.STM) error {
		rw := pipelineJobInfos.ReadWrite(stm)
		rw.Put(j1.Job.ID, j1)
		rw.Put(j2.Job.ID, j2)
		rw.Put(j3.Job.ID, j3)
		return nil
	})
	require.NoError(t, err)

	ro := pipelineJobInfos.ReadOnly(context.Background())

	job := &pps.PipelineJobInfo{}
	i := 1
	require.NoError(t, ro.GetByIndex(pipelineIndex, j1.Pipeline, job, col.DefaultOptions, func(ID string) error {
		switch i {
		case 1:
			require.Equal(t, j1.Job.ID, ID)
			require.Equal(t, j1, job)
		case 2:
			require.Equal(t, j2.Job.ID, ID)
			require.Equal(t, j2, job)
		case 3:
			t.Fatal("too many jobs")
		}
		i++
		return nil
	}))

	i = 1
	require.NoError(t, ro.GetByIndex(pipelineIndex, j3.Pipeline, job, col.DefaultOptions, func(ID string) error {
		switch i {
		case 1:
			require.Equal(t, j3.Job.ID, ID)
			require.Equal(t, j3, job)
		case 2:
			t.Fatal("too many jobs")
		}
		i++
		return nil
	}))
}

func TestIndexWatch(t *testing.T) {
	etcdClient := getEtcdClient(t)
	uuidPrefix := uuid.NewWithoutDashes()

	pipelineJobInfos := col.NewCollection(etcdClient, uuidPrefix, []*col.Index{pipelineIndex}, &pps.PipelineJobInfo{}, nil, nil)

	j1 := &pps.PipelineJobInfo{
		Job:      client.NewJob("j1"),
		Pipeline: client.NewPipeline("p1"),
	}
	_, err := col.NewSTM(context.Background(), etcdClient, func(stm col.STM) error {
		return pipelineJobInfos.ReadWrite(stm).Put(j1.Job.ID, j1)
	})
	require.NoError(t, err)

	ro := pipelineJobInfos.ReadOnly(context.Background())

	watcher, err := ro.WatchByIndex(pipelineIndex, j1.Pipeline)
	eventCh := watcher.Watch()
	require.NoError(t, err)
	var ID string
	job := new(pps.PipelineJobInfo)
	event := <-eventCh
	require.NoError(t, event.Err)
	require.Equal(t, event.Type, watch.EventPut)
	require.NoError(t, event.Unmarshal(&ID, job))
	require.Equal(t, j1.Job.ID, ID)
	require.Equal(t, j1, job)

	// Now we will put j1 again, unchanged.  We want to make sure
	// that we do not receive an event.
	_, err = col.NewSTM(context.Background(), etcdClient, func(stm col.STM) error {
		return pipelineJobInfos.ReadWrite(stm).Put(j1.Job.ID, j1)
	})
	require.NoError(t, err)

	select {
	case event := <-eventCh:
		t.Fatalf("should not have received an event %v", event)
	case <-time.After(2 * time.Second):
	}

	j2 := &pps.PipelineJobInfo{
		Job:      client.NewJob("j2"),
		Pipeline: client.NewPipeline("p1"),
	}

	_, err = col.NewSTM(context.Background(), etcdClient, func(stm col.STM) error {
		return pipelineJobInfos.ReadWrite(stm).Put(j2.Job.ID, j2)
	})
	require.NoError(t, err)

	event = <-eventCh
	require.NoError(t, event.Err)
	require.Equal(t, event.Type, watch.EventPut)
	require.NoError(t, event.Unmarshal(&ID, job))
	require.Equal(t, j2.Job.ID, ID)
	require.Equal(t, j2, job)

	j1Prime := &pps.PipelineJobInfo{
		Job:      client.NewJob("j1"),
		Pipeline: client.NewPipeline("p3"),
	}
	_, err = col.NewSTM(context.Background(), etcdClient, func(stm col.STM) error {
		return pipelineJobInfos.ReadWrite(stm).Put(j1.Job.ID, j1Prime)
	})
	require.NoError(t, err)

	event = <-eventCh
	require.NoError(t, event.Err)
	require.Equal(t, event.Type, watch.EventDelete)
	require.NoError(t, event.Unmarshal(&ID, job))
	require.Equal(t, j1.Job.ID, ID)

	_, err = col.NewSTM(context.Background(), etcdClient, func(stm col.STM) error {
		return pipelineJobInfos.ReadWrite(stm).Delete(j2.Job.ID)
	})
	require.NoError(t, err)

	event = <-eventCh
	require.NoError(t, event.Err)
	require.Equal(t, event.Type, watch.EventDelete)
	require.NoError(t, event.Unmarshal(&ID, job))
	require.Equal(t, j2.Job.ID, ID)
}

func TestMultiIndex(t *testing.T) {
	etcdClient := getEtcdClient(t)
	uuidPrefix := uuid.NewWithoutDashes()

	cis := col.NewCollection(etcdClient, uuidPrefix, []*col.Index{commitMultiIndex}, &pfs.CommitInfo{}, nil, nil)

	c1 := &pfs.CommitInfo{
		Commit: client.NewCommit("repo", "c1"),
		Provenance: []*pfs.CommitProvenance{
			client.NewCommitProvenance("in", "master", "c1"),
			client.NewCommitProvenance("in", "master", "c2"),
			client.NewCommitProvenance("in", "master", "c3"),
		},
	}
	c2 := &pfs.CommitInfo{
		Commit: client.NewCommit("repo", "c2"),
		Provenance: []*pfs.CommitProvenance{
			client.NewCommitProvenance("in", "master", "c1"),
			client.NewCommitProvenance("in", "master", "c2"),
			client.NewCommitProvenance("in", "master", "c3"),
		},
	}
	_, err := col.NewSTM(context.Background(), etcdClient, func(stm col.STM) error {
		cis := cis.ReadWrite(stm)
		cis.Put(c1.Commit.ID, c1)
		cis.Put(c2.Commit.ID, c2)
		return nil
	})
	require.NoError(t, err)

	cisReadonly := cis.ReadOnly(context.Background())

	// Test that the first key retrieves both r1 and r2
	ci := &pfs.CommitInfo{}
	i := 1
	require.NoError(t, cisReadonly.GetByIndex(commitMultiIndex, client.NewCommit("in", "c1"), ci, col.DefaultOptions, func(ID string) error {
		if i == 1 {
			require.Equal(t, c1.Commit.ID, ID)
			require.Equal(t, c1, ci)
		} else {
			require.Equal(t, c2.Commit.ID, ID)
			require.Equal(t, c2, ci)
		}
		i++
		return nil
	}))

	// Test that the second key retrieves both r1 and r2
	i = 1
	require.NoError(t, cisReadonly.GetByIndex(commitMultiIndex, client.NewCommit("in", "c2"), ci, col.DefaultOptions, func(ID string) error {
		if i == 1 {
			require.Equal(t, c1.Commit.ID, ID)
			require.Equal(t, c1, ci)
		} else {
			require.Equal(t, c2.Commit.ID, ID)
			require.Equal(t, c2, ci)
		}
		i++
		return nil
	}))

	// replace "c3" in the provenance of c1 with "c4"
	c1.Provenance[2].Commit.ID = "c4"
	_, err = col.NewSTM(context.Background(), etcdClient, func(stm col.STM) error {
		return cis.ReadWrite(stm).Put(c1.Commit.ID, c1)
	})
	require.NoError(t, err)

	// Now "c3" only retrieves c2 (indexes are updated)
	require.NoError(t, cisReadonly.GetByIndex(commitMultiIndex, client.NewCommit("in", "c3"), ci, col.DefaultOptions, func(ID string) error {
		require.Equal(t, c2.Commit.ID, ID)
		require.Equal(t, c2, ci)
		return nil
	}))

	// And "C4" only retrieves r1 (indexes are updated)
	require.NoError(t, cisReadonly.GetByIndex(commitMultiIndex, client.NewCommit("in", "c4"), ci, col.DefaultOptions, func(ID string) error {
		require.Equal(t, c1.Commit.ID, ID)
		require.Equal(t, c1, ci)
		return nil
	}))

	// Delete c1 from etcd completely
	_, err = col.NewSTM(context.Background(), etcdClient, func(stm col.STM) error {
		return cis.ReadWrite(stm).Delete(c1.Commit.ID)
	})
	require.NoError(t, err)

	// Now "c1" only retrieves c2
	require.NoError(t, cisReadonly.GetByIndex(commitMultiIndex, client.NewCommit("in", "c1"), ci, col.DefaultOptions, func(ID string) error {
		require.Equal(t, c2.Commit.ID, ID)
		require.Equal(t, c2, ci)
		return nil
	}))
}

func TestBoolIndex(t *testing.T) {
	etcdClient := getEtcdClient(t)
	uuidPrefix := uuid.NewWithoutDashes()
	boolValues := col.NewCollection(etcdClient, uuidPrefix, []*col.Index{{
		Field: "Value",
		Multi: false,
	}}, &types.BoolValue{}, nil, nil)

	r1 := &types.BoolValue{
		Value: true,
	}
	r2 := &types.BoolValue{
		Value: false,
	}
	_, err := col.NewSTM(context.Background(), etcdClient, func(stm col.STM) error {
		boolValues := boolValues.ReadWrite(stm)
		boolValues.Put("true", r1)
		boolValues.Put("false", r2)
		return nil
	})
	require.NoError(t, err)

	// Test that we don't format the index string incorrectly
	resp, err := etcdClient.Get(context.Background(), uuidPrefix, etcd.WithPrefix())
	require.NoError(t, err)
	for _, kv := range resp.Kvs {
		if !bytes.Contains(kv.Key, []byte("__index_")) {
			continue // not an index record
		}
		require.True(t,
			bytes.Contains(kv.Key, []byte("__index_Value/true")) ||
				bytes.Contains(kv.Key, []byte("__index_Value/false")), string(kv.Key))
	}
}

var epsilon = &types.BoolValue{Value: true}

func TestTTL(t *testing.T) {
	etcdClient := getEtcdClient(t)
	uuidPrefix := uuid.NewWithoutDashes()

	clxn := col.NewCollection(etcdClient, uuidPrefix, nil, &types.BoolValue{}, nil, nil)
	const TTL = 5
	_, err := col.NewSTM(context.Background(), etcdClient, func(stm col.STM) error {
		return clxn.ReadWrite(stm).PutTTL("key", epsilon, TTL)
	})
	require.NoError(t, err)

	var actualTTL int64
	_, err = col.NewSTM(context.Background(), etcdClient, func(stm col.STM) error {
		var err error
		actualTTL, err = clxn.ReadWrite(stm).TTL("key")
		return err
	})
	require.NoError(t, err)
	require.True(t, actualTTL > 0 && actualTTL < TTL, "actualTTL was %v", actualTTL)
}

func TestTTLExpire(t *testing.T) {
	etcdClient := getEtcdClient(t)
	uuidPrefix := uuid.NewWithoutDashes()

	clxn := col.NewCollection(etcdClient, uuidPrefix, nil, &types.BoolValue{}, nil, nil)
	const TTL = 5
	_, err := col.NewSTM(context.Background(), etcdClient, func(stm col.STM) error {
		return clxn.ReadWrite(stm).PutTTL("key", epsilon, TTL)
	})
	require.NoError(t, err)

	time.Sleep((TTL + 1) * time.Second)
	value := &types.BoolValue{}
	err = clxn.ReadOnly(context.Background()).Get("key", value)
	require.NotNil(t, err)
	require.Matches(t, "not found", err.Error())
}

func TestTTLExtend(t *testing.T) {
	etcdClient := getEtcdClient(t)
	uuidPrefix := uuid.NewWithoutDashes()

	// Put value with short TLL & check that it was set
	clxn := col.NewCollection(etcdClient, uuidPrefix, nil, &types.BoolValue{}, nil, nil)
	const TTL = 5
	_, err := col.NewSTM(context.Background(), etcdClient, func(stm col.STM) error {
		return clxn.ReadWrite(stm).PutTTL("key", epsilon, TTL)
	})
	require.NoError(t, err)

	var actualTTL int64
	_, err = col.NewSTM(context.Background(), etcdClient, func(stm col.STM) error {
		var err error
		actualTTL, err = clxn.ReadWrite(stm).TTL("key")
		return err
	})
	require.NoError(t, err)
	require.True(t, actualTTL > 0 && actualTTL < TTL, "actualTTL was %v", actualTTL)

	// Put value with new, longer TLL and check that it was set
	const LongerTTL = 15
	_, err = col.NewSTM(context.Background(), etcdClient, func(stm col.STM) error {
		return clxn.ReadWrite(stm).PutTTL("key", epsilon, LongerTTL)
	})
	require.NoError(t, err)

	_, err = col.NewSTM(context.Background(), etcdClient, func(stm col.STM) error {
		var err error
		actualTTL, err = clxn.ReadWrite(stm).TTL("key")
		return err
	})
	require.NoError(t, err)
	require.True(t, actualTTL > TTL && actualTTL < LongerTTL, "actualTTL was %v", actualTTL)
}

func TestIteration(t *testing.T) {
	etcdClient := getEtcdClient(t)
	t.Run("one-val-per-txn", func(t *testing.T) {
		uuidPrefix := uuid.NewWithoutDashes()
		testCol := col.NewCollection(etcdClient, uuidPrefix, nil, &types.Empty{}, nil, nil)
		numVals := 1000
		for i := 0; i < numVals; i++ {
			_, err := col.NewSTM(context.Background(), etcdClient, func(stm col.STM) error {
				return testCol.ReadWrite(stm).Put(fmt.Sprintf("%d", i), &types.Empty{})
			})
			require.NoError(t, err)
		}
		ro := testCol.ReadOnly(context.Background())
		val := &types.Empty{}
		i := numVals - 1
		require.NoError(t, ro.List(val, col.DefaultOptions, func(key string) error {
			require.Equal(t, fmt.Sprintf("%d", i), key)
			i--
			return nil
		}))
	})
	t.Run("many-vals-per-txn", func(t *testing.T) {
		uuidPrefix := uuid.NewWithoutDashes()
		testCol := col.NewCollection(etcdClient, uuidPrefix, nil, &types.Empty{}, nil, nil)
		numBatches := 10
		valsPerBatch := 7
		for i := 0; i < numBatches; i++ {
			_, err := col.NewSTM(context.Background(), etcdClient, func(stm col.STM) error {
				for j := 0; j < valsPerBatch; j++ {
					if err := testCol.ReadWrite(stm).Put(fmt.Sprintf("%d", i*valsPerBatch+j), &types.Empty{}); err != nil {
						return err
					}
				}
				return nil
			})
			require.NoError(t, err)
		}
		vals := make(map[string]bool)
		ro := testCol.ReadOnly(context.Background())
		val := &types.Empty{}
		require.NoError(t, ro.List(val, col.DefaultOptions, func(key string) error {
			require.False(t, vals[key], "saw value %s twice", key)
			vals[key] = true
			return nil
		}))
		require.Equal(t, numBatches*valsPerBatch, len(vals), "didn't receive every value")
	})
	t.Run("large-vals", func(t *testing.T) {
		uuidPrefix := uuid.NewWithoutDashes()
		testCol := col.NewCollection(etcdClient, uuidPrefix, nil, &pfs.Repo{}, nil, nil)
		numVals := 100
		longString := strings.Repeat("foo\n", 1024*256) // 1 MB worth of foo
		for i := 0; i < numVals; i++ {
			_, err := col.NewSTM(context.Background(), etcdClient, func(stm col.STM) error {
				if err := testCol.ReadWrite(stm).Put(fmt.Sprintf("%d", i), &pfs.Repo{Name: longString}); err != nil {
					return err
				}
				return nil
			})
			require.NoError(t, err)
		}
		ro := testCol.ReadOnly(context.Background())
		val := &pfs.Repo{}
		vals := make(map[string]bool)
		valsOrder := []string{}
		require.NoError(t, ro.List(val, col.DefaultOptions, func(key string) error {
			require.False(t, vals[key], "saw value %s twice", key)
			vals[key] = true
			valsOrder = append(valsOrder, key)
			return nil
		}))
		for i, key := range valsOrder {
			require.Equal(t, key, strconv.Itoa(numVals-i-1), "incorrect order returned")
		}
		require.Equal(t, numVals, len(vals), "didn't receive every value")
		vals = make(map[string]bool)
		valsOrder = []string{}
		require.NoError(t, ro.List(val, &col.Options{etcd.SortByCreateRevision, etcd.SortAscend, true}, func(key string) error {
			require.False(t, vals[key], "saw value %s twice", key)
			vals[key] = true
			valsOrder = append(valsOrder, key)
			return nil
		}))
		for i, key := range valsOrder {
			require.Equal(t, key, strconv.Itoa(i), "incorrect order returned")
		}
		require.Equal(t, numVals, len(vals), "didn't receive every value")
	})
}

func getEtcdClient(t *testing.T) *etcd.Client {
	env := testetcd.NewEnv(t)
	return env.EtcdClient
}

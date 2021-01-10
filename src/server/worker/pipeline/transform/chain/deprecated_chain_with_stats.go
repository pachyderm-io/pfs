package chain

import (
	"context"
	"reflect"
	"sync"

	"github.com/pachyderm/pachyderm/src/client/pkg/errors"
	"github.com/pachyderm/pachyderm/src/server/worker/common"
	"github.com/pachyderm/pachyderm/src/server/worker/datum"
)

type deprecatedJobWithStatsDatumIterator struct {
	data JobData
	jc   *deprecatedJobWithStatsChain

	// TODO: lower memory consumption - all these datumsets might result in a
	// really large memory footprint. See if we can do a streaming interface to
	// replace these - will likely require the new storage layer, as additive-only
	// jobs need this stuff the most.
	yielding  DatumSet // Datums that may be yielded as the iterator progresses
	yielded   DatumSet // Datums that have been yielded
	allDatums DatumSet // All datum hashes from the datum iterator

	ancestors []*deprecatedJobWithStatsDatumIterator
	dit       datum.Iterator
	ditIndex  int

	finished     bool
	additiveOnly bool
	done         chan struct{}
}

type deprecatedJobWithStatsChain struct {
	mutex  sync.Mutex
	hasher DatumHasher
	jobs   []*deprecatedJobWithStatsDatumIterator
}

// NewJobChain constructs a JobChain
func NewDeprecatedJobWithStatsChain(hasher DatumHasher, baseDatums DatumSet, baseStatsDatums DatumSet) JobChain {
	jc := &deprecatedJobWithStatsChain{
		hasher: hasher,
	}

	// Insert a dummy job representing the given base datum set
	jdi := &deprecatedJobWithStatsDatumIterator{
		data:      nil,
		jc:        jc,
		allDatums: baseDatums,
		finished:  true,
		done:      make(chan struct{}),
		ditIndex:  -1,
	}
	close(jdi.done)

	jc.jobs = []*deprecatedJobWithStatsDatumIterator{jdi}
	return jc
}

// recalculate is called whenever jdi.yielding is empty (either at init or when
// a blocking ancestor job has finished), to repopulate it.
func (jdi *deprecatedJobWithStatsDatumIterator) recalculate(allAncestors []*deprecatedJobWithStatsDatumIterator) {
	jdi.ancestors = []*deprecatedJobWithStatsDatumIterator{}
	interestingAncestors := map[*deprecatedJobWithStatsDatumIterator]struct{}{}
	for hash, count := range jdi.allDatums {
		if yieldedCount, ok := jdi.yielded[hash]; ok {
			if count-yieldedCount > 0 {
				jdi.yielding[hash] = count - yieldedCount
			}
			continue
		}

		safeToProcess := true
		// interestingAncestors should be _all_ unfinished previous jobs which have
		// _any_ datum overlap with this job
		for _, ancestor := range allAncestors {
			if !ancestor.finished {
				if _, ok := ancestor.allDatums[hash]; ok {
					interestingAncestors[ancestor] = struct{}{}
					safeToProcess = false
				}
			}
		}

		if safeToProcess {
			jdi.yielding[hash] = count
		}
	}

	var parentJob *deprecatedJobWithStatsDatumIterator
	for i := len(allAncestors) - 1; i >= 0; i-- {
		// Skip all failed jobs
		if allAncestors[i].allDatums != nil {
			parentJob = allAncestors[i]
			break
		}
	}

	// If this job is additive-only from the parent job, we should mark it now -
	// loop over parent datums to see if they are all present
	jdi.additiveOnly = false
	for hash, parentCount := range parentJob.allDatums {
		if count, ok := jdi.allDatums[hash]; !ok || count < parentCount {
			jdi.additiveOnly = false
			break
		}
	}

	if jdi.additiveOnly {
		// If this is additive-only, we only need to enqueue new datums (since the parent job)
		for hash, count := range jdi.yielding {
			if parentCount, ok := parentJob.allDatums[hash]; ok {
				if count == parentCount {
					delete(jdi.yielding, hash)
				} else {
					jdi.yielding[hash] = count - parentCount
				}
			}
		}
		// An additive-only job can only progress once its parent job has finished.
		// At that point it will re-evaluate what datums to process in case of a
		// failed job or recovered datums.
		if !parentJob.finished {
			jdi.ancestors = append(jdi.ancestors, parentJob)
		}
	} else {
		for ancestor := range interestingAncestors {
			jdi.ancestors = append(jdi.ancestors, ancestor)
		}
	}
}

func (jc *deprecatedJobWithStatsChain) Start(jd JobData) (JobDatumIterator, error) {
	dit, err := jd.Iterator()
	if err != nil {
		return nil, err
	}

	jdi := &deprecatedJobWithStatsDatumIterator{
		data:      jd,
		jc:        jc,
		yielding:  make(DatumSet),
		yielded:   make(DatumSet),
		allDatums: make(DatumSet),
		ancestors: []*deprecatedJobWithStatsDatumIterator{},
		dit:       dit,
		ditIndex:  -1,
		done:      make(chan struct{}),
	}

	jdi.dit.Reset()
	for jdi.dit.Next() {
		inputs := jdi.dit.Datum()
		hash := jc.hasher.Hash(inputs)
		jdi.allDatums[hash]++
	}
	jdi.dit.Reset()

	jc.mutex.Lock()
	defer jc.mutex.Unlock()

	jdi.recalculate(jc.jobs)

	jc.jobs = append(jc.jobs, jdi)
	return jdi, nil
}

func (jc *deprecatedJobWithStatsChain) indexOf(jd JobData) (int, error) {
	for i, x := range jc.jobs {
		if x.data == jd {
			return i, nil
		}
	}
	return 0, errors.New("job not found in job chain")
}

func (jc *deprecatedJobWithStatsChain) cleanFinishedJobs() {
	for len(jc.jobs) > 1 && jc.jobs[1].finished {
		if jc.jobs[1].allDatums != nil {
			jc.jobs[0].allDatums = jc.jobs[1].allDatums
		}
		jc.jobs = append(jc.jobs[:1], jc.jobs[2:]...)
	}
}

func (jc *deprecatedJobWithStatsChain) Fail(jd JobData) error {
	jc.mutex.Lock()
	defer jc.mutex.Unlock()

	index, err := jc.indexOf(jd)
	if err != nil {
		return err
	}

	jdi := jc.jobs[index]

	if jdi.finished {
		return errors.New("cannot fail a job that is already finished")
	}

	jdi.allDatums = nil
	jdi.finished = true
	close(jdi.done)

	jc.cleanFinishedJobs()

	return nil
}

func (jc *deprecatedJobWithStatsChain) RecoveredDatums(jd JobData, recoveredDatums DatumSet) error {
	jc.mutex.Lock()
	defer jc.mutex.Unlock()

	index, err := jc.indexOf(jd)
	if err != nil {
		return err
	}

	jdi := jc.jobs[index]

	for hash := range recoveredDatums {
		delete(jdi.allDatums, hash)
	}

	return nil
}

func (jc *deprecatedJobWithStatsChain) Succeed(jd JobData) error {
	jc.mutex.Lock()
	defer jc.mutex.Unlock()

	index, err := jc.indexOf(jd)
	if err != nil {
		return err
	}

	jdi := jc.jobs[index]

	if jdi.finished {
		return errors.New("cannot succeed a job that is already finished")
	}

	if len(jdi.yielding) != 0 || len(jdi.ancestors) > 0 {
		return errors.Errorf(
			"cannot succeed a job with items remaining on the iterator: %d datums and %d ancestor jobs",
			len(jdi.yielding), len(jdi.ancestors),
		)
	}

	jdi.finished = true
	jc.cleanFinishedJobs()
	close(jdi.done)
	return nil
}

// TODO: iteration should return a chunk of 'known' new datums before other
// datums (to optimize for distributing processing across workers). This should
// still be true even after resetting the iterator.
func (jdi *deprecatedJobWithStatsDatumIterator) NextBatch(ctx context.Context) (int64, error) {
	for len(jdi.yielding) == 0 {
		if len(jdi.ancestors) == 0 {
			return 0, nil
		}

		// Wait on an ancestor job
		cases := make([]reflect.SelectCase, 0, len(jdi.ancestors)+1)
		for _, x := range jdi.ancestors {
			cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(x.done)})
		}
		cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ctx.Done())})

		// Wait for an ancestor job to finish, then remove it from our dependencies
		selectIndex, _, _ := reflect.Select(cases)
		if selectIndex == len(cases)-1 {
			return 0, ctx.Err()
		}

		if err := func() error {
			jdi.jc.mutex.Lock()
			defer jdi.jc.mutex.Unlock()

			if jdi.finished {
				return errors.New("stopping datum iteration because job failed")
			}

			index, err := jdi.jc.indexOf(jdi.data)
			if err != nil {
				return err
			}

			jdi.recalculate(jdi.jc.jobs[:index])
			return nil
		}(); err != nil {
			return 0, err
		}

		jdi.ditIndex = -1
	}

	batchSize := int64(0)
	for _, count := range jdi.yielding {
		batchSize += count
	}

	return batchSize, nil
}

func (jdi *deprecatedJobWithStatsDatumIterator) NextDatum() ([]*common.Input, int64) {
	jdi.ditIndex++
	for jdi.ditIndex < jdi.dit.Len() {
		inputs := jdi.dit.DatumN(jdi.ditIndex)
		hash := jdi.jc.hasher.Hash(inputs)
		if count, ok := jdi.yielding[hash]; ok {
			if count == 1 {
				delete(jdi.yielding, hash)
			} else {
				jdi.yielding[hash]--
			}
			jdi.yielded[hash]++
			return inputs, int64(jdi.ditIndex)
		}
		jdi.ditIndex++
	}

	return nil, 0
}

func (jdi *deprecatedJobWithStatsDatumIterator) Reset() {
	jdi.ditIndex = -1
	for hash, count := range jdi.yielded {
		delete(jdi.yielded, hash)
		jdi.yielding[hash] += count
	}
}

func (jdi *deprecatedJobWithStatsDatumIterator) MaxLen() int64 {
	return int64(jdi.dit.Len())
}

func (jdi *deprecatedJobWithStatsDatumIterator) DatumSet() DatumSet {
	return jdi.allDatums
}

func (jdi *deprecatedJobWithStatsDatumIterator) AdditiveOnly() bool {
	return jdi.additiveOnly
}

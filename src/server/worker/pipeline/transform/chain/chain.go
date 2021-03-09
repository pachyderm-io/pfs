package chain

import (
	"context"
	"sync"

	"github.com/pachyderm/pachyderm/v2/src/pps"
	"github.com/pachyderm/pachyderm/v2/src/server/worker/datum"
)

// TODO: More documentation.

// JobChain manages a chain of jobs.
type JobChain struct {
	hasher  datum.Hasher
	base    datum.Iterator
	noSkip  bool
	prevJob *JobDatumIterator
}

// NewJobChain creates a new job chain.
// TODO: We should probably pipe a context through here.
func NewJobChain(hasher datum.Hasher, opts ...JobChainOption) *JobChain {
	jc := &JobChain{hasher: hasher}
	for _, opt := range opts {
		opt(jc)
	}
	if jc.base != nil {
		// Insert a dummy job representing the given base datum set
		jdi := &JobDatumIterator{
			jc:        jc,
			dit:       jc.base,
			outputDit: jc.base,
			done:      make(chan struct{}),
		}
		close(jdi.done)
		jc.prevJob = jdi
	}
	return jc
}

// CreateJob creates a job in the job chain.
func (jc *JobChain) CreateJob(ctx context.Context, jobID string, dit, outputDit datum.Iterator) *JobDatumIterator {
	jdi := &JobDatumIterator{
		ctx:       ctx,
		jc:        jc,
		parent:    jc.prevJob,
		jobID:     jobID,
		stats:     &datum.Stats{ProcessStats: &pps.ProcessStats{}},
		dit:       datum.NewJobIterator(dit, jobID, jc.hasher),
		outputDit: outputDit,
		done:      make(chan struct{}),
	}
	jc.prevJob = jdi
	return jdi
}

// JobDatumIterator provides a way to iterate through the datums in a job.
type JobDatumIterator struct {
	ctx            context.Context
	jc             *JobChain
	parent         *JobDatumIterator
	jobID          string
	stats          *datum.Stats
	dit, outputDit datum.Iterator
	finishOnce     sync.Once
	done           chan struct{}
	deleter        func(*datum.Meta) error
}

// SetDeleter sets the deleter callback for the iterator.
// TODO: There should be a way to handle this through callbacks, but this would require some more changes to the registry.
func (jdi *JobDatumIterator) SetDeleter(deleter func(*datum.Meta) error) {
	jdi.deleter = deleter
}

// Iterate iterates through the datums for the job.
func (jdi *JobDatumIterator) Iterate(cb func(*datum.Meta) error) error {
	jdi.stats.Skipped = 0
	if jdi.parent == nil {
		return jdi.dit.Iterate(cb)
	}
	// Generate datum sets for the new datums (datums that do not exist in the parent job).
	// TODO: Logging?
	if err := datum.Merge([]datum.Iterator{jdi.dit, jdi.parent.dit}, func(metas []*datum.Meta) error {
		if len(metas) == 1 {
			if metas[0].JobID != jdi.jobID {
				return nil
			}
			return cb(metas[0])
		}
		if jdi.skippableDatum(metas[0], metas[1]) {
			jdi.stats.Skipped++
			return nil
		}
		return cb(metas[0])
	}); err != nil {
		return err
	}
	select {
	case <-jdi.parent.done:
	case <-jdi.ctx.Done():
		return jdi.ctx.Err()
	}
	// Generate datum sets for the skipped datums that were not processed by the parent (failed, recovered, etc.).
	// Also generate deletion operations appropriately.
	return datum.Merge([]datum.Iterator{jdi.dit, jdi.parent.dit, jdi.parent.outputDit}, func(metas []*datum.Meta) error {
		// Datum only exists in the current job or only exists in the parent job and was not processed.
		if len(metas) == 1 {
			return nil
		}
		if len(metas) == 2 {
			// Datum only exists in the parent job and was processed.
			if metas[0].JobID != jdi.jobID {
				return jdi.deleteDatum(metas[1])
			}
			// Datum exists in both jobs, but was not processed by the parent.
			if jdi.skippableDatum(metas[0], metas[1]) {
				jdi.stats.Skipped--
				return cb(metas[0])
			}
			return nil
		}
		// Check if a skipped datum was not successfully processed by the parent.
		if jdi.skippableDatum(metas[0], metas[1]) {
			if jdi.skippableDatum(metas[1], metas[2]) {
				return nil
			}
			jdi.stats.Skipped--
			if err := cb(metas[0]); err != nil {
				return err
			}
		}
		return jdi.deleteDatum(metas[2])
	})
}

func (jdi *JobDatumIterator) deleteDatum(meta *datum.Meta) error {
	if jdi.deleter == nil {
		return nil
	}
	return jdi.deleter(meta)
}

func (jdi *JobDatumIterator) skippableDatum(meta1, meta2 *datum.Meta) bool {
	// If the hashes are equal and the second datum was processed, then skip it.
	return !jdi.jc.noSkip && meta1.Hash == meta2.Hash && meta2.State == datum.State_PROCESSED
}

// Stats returns the stats for the most recent iteration.
func (jdi *JobDatumIterator) Stats() *datum.Stats {
	return jdi.stats
}

// Finish finishes the job in the job chain.
func (jdi *JobDatumIterator) Finish() {
	jdi.finishOnce.Do(func() {
		close(jdi.done)
	})
}

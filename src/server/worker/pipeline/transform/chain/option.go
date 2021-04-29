package chain

import "github.com/pachyderm/pachyderm/v2/src/server/worker/datum"

// JobChainOption is a functional option modifying a JobChain
type JobChainOption func(*JobChain)

// WithBase sets its JobChain's base DatumIterator. This determines which datums
// are skipped.
func WithBase(di datum.Iterator) JobChainOption {
	return func(jc *JobChain) {
		jc.base = di
	}
}

// WithNoSkip causes its JobChain not to skip any datums, which is used in the
// S3 gateway and when CreatePipelineSpec.ReprocessSpec == "every_job"
func WithNoSkip() JobChainOption {
	return func(jc *JobChain) {
		jc.noSkip = true
	}
}

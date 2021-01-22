package transform

import (
	"context"
	"path/filepath"

	"github.com/gogo/protobuf/types"
	"github.com/pachyderm/pachyderm/src/client"
	"github.com/pachyderm/pachyderm/src/client/pps"
	"github.com/pachyderm/pachyderm/src/server/pkg/uuid"
	"github.com/pachyderm/pachyderm/src/server/pkg/work"
	"github.com/pachyderm/pachyderm/src/server/worker/datum"
	"github.com/pachyderm/pachyderm/src/server/worker/driver"
	"github.com/pachyderm/pachyderm/src/server/worker/logs"
)

// Worker handles a transform pipeline work subtask, then returns.
// TODO:
// datum queuing (probably should be handled by datum package).
// s3 input / gateway stuff (need more information here).
// spouts.
// joins.
// capture datum logs.
// file download features (empty / lazy files). Need to check over the pipe logic.
// git inputs.
// handle custom user set for execution.
// Taking advantage of symlinks during upload?
func Worker(driver driver.Driver, logger logs.TaggedLogger, subtask *work.Task, status *Status) (retErr error) {
	datumSet, err := deserializeDatumSet(subtask.Data)
	if err != nil {
		return err
	}
	return status.withJob(datumSet.JobID, func() error {
		logger = logger.WithJob(datumSet.JobID)
		if err := logger.LogStep("datum task", func() error {
			return handleDatumSet(driver, logger, datumSet, status)
		}); err != nil {
			return err
		}
		subtask.Data, err = serializeDatumSet(datumSet)
		return err
	})
}

// TODO: It would probably be better to write the output to temporary file sets and expose an operation through pfs for adding a temporary fileset to a commit.
func handleDatumSet(driver driver.Driver, logger logs.TaggedLogger, datumSet *DatumSet, status *Status) error {
	pachClient := driver.PachClient()
	storageRoot := filepath.Join(driver.InputDir(), client.PPSScratchSpace, uuid.NewWithoutDashes())
	datumSet.Stats = &datum.Stats{ProcessStats: &pps.ProcessStats{}}
	// Setup file operation client for output meta commit.
	metaCommit := datumSet.MetaCommit
	return pachClient.WithModifyFileClient(metaCommit.Repo.Name, metaCommit.ID, func(mfcMeta *client.ModifyFileClient) error {
		// Setup file operation client for output PFS commit.
		outputCommit := datumSet.OutputCommit
		return pachClient.WithModifyFileClient(outputCommit.Repo.Name, outputCommit.ID, func(mfcPFS *client.ModifyFileClient) error {
			// Setup datum set for processing.
			return datum.WithSet(pachClient, storageRoot, func(s *datum.Set) error {
				di := datum.NewFileSetIterator(pachClient, client.TmpRepoName, datumSet.FileSet)
				// Process each datum in the assigned datum set.
				return di.Iterate(func(meta *datum.Meta) error {
					ctx := pachClient.Ctx()
					inputs := meta.Inputs
					logger = logger.WithData(inputs)
					env := driver.UserCodeEnv(logger.JobID(), outputCommit, inputs)
					var opts []datum.Option
					if driver.PipelineInfo().DatumTimeout != nil {
						timeout, err := types.DurationFromProto(driver.PipelineInfo().DatumTimeout)
						if err != nil {
							return err
						}
						opts = append(opts, datum.WithTimeout(timeout))
					}
					if driver.PipelineInfo().Transform.ErrCmd != nil {
						opts = append(opts, datum.WithRecoveryCallback(func(runCtx context.Context) error {
							return driver.RunUserErrorHandlingCode(runCtx, logger, env)
						}))
					}
					return s.WithDatum(ctx, meta, func(d *datum.Datum) error {
						cancelCtx, cancel := context.WithCancel(ctx)
						defer cancel()
						return status.withDatum(inputs, cancel, func() error {
							return driver.WithActiveData(inputs, d.PFSStorageRoot(), func() error {
								return d.Run(cancelCtx, func(runCtx context.Context) error {
									return driver.RunUserCode(runCtx, logger, env)
								})
							})
						})
					}, opts...)

				})
			}, datum.WithMetaOutput(mfcMeta), datum.WithPFSOutput(mfcPFS), datum.WithStats(datumSet.Stats))
		})
	})
}

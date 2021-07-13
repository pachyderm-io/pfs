package transform

import (
	"context"
	"os"
	"path/filepath"

	"github.com/gogo/protobuf/proto"
	"github.com/pachyderm/pachyderm/v2/src/client"
	"github.com/pachyderm/pachyderm/v2/src/internal/ppsutil"
	"github.com/pachyderm/pachyderm/v2/src/internal/storage/renew"
	"github.com/pachyderm/pachyderm/v2/src/internal/uuid"
	"github.com/pachyderm/pachyderm/v2/src/internal/work"
	"github.com/pachyderm/pachyderm/v2/src/pfs"
	"github.com/pachyderm/pachyderm/v2/src/pps"
	pfsserver "github.com/pachyderm/pachyderm/v2/src/server/pfs"
	"github.com/pachyderm/pachyderm/v2/src/server/worker/datum"
	"github.com/pachyderm/pachyderm/v2/src/server/worker/driver"
	"github.com/pachyderm/pachyderm/v2/src/server/worker/logs"
)

type pendingJob struct {
	driver                     driver.Driver
	logger                     logs.TaggedLogger
	cancel                     context.CancelFunc
	ji                         *pps.JobInfo
	parentDit                  datum.Iterator
	commitInfo, metaCommitInfo *pfs.CommitInfo
	parentMetaCommit           *pfs.Commit
	taskMaster                 *work.Master
	hasher                     datum.Hasher
	noSkip                     bool
}

func (pj *pendingJob) writeJobInfo() error {
	pj.logger.Logf("updating job info, state: %s", pj.ji.State)
	return ppsutil.WriteJobInfo(pj.driver.PachClient(), pj.ji)
}

// TODO: The job info should eventually just have a field with type *datum.Stats
func (pj *pendingJob) saveJobStats(stats *datum.Stats) {
	// TODO: Need to clean up the setup of process stats.
	if pj.ji.Stats == nil {
		pj.ji.Stats = &pps.ProcessStats{}
	}
	datum.MergeProcessStats(pj.ji.Stats, stats.ProcessStats)
	pj.ji.DataProcessed += stats.Processed
	pj.ji.DataSkipped += stats.Skipped
	pj.ji.DataFailed += stats.Failed
	pj.ji.DataRecovered += stats.Recovered
	pj.ji.DataTotal += stats.Processed + stats.Skipped + stats.Failed + stats.Recovered
}

func (pj *pendingJob) load() error {
	pachClient := pj.driver.PachClient()
	var err error
	// Load and clear the output commit.
	pj.commitInfo, err = pachClient.PfsAPIClient.InspectCommit(
		pachClient.Ctx(),
		&pfs.InspectCommitRequest{
			Commit: pj.ji.OutputCommit,
			Wait:   pfs.CommitState_STARTED,
		})
	if err != nil {
		return err
	}
	if _, err := pachClient.PfsAPIClient.ClearCommit(
		pachClient.Ctx(),
		&pfs.ClearCommitRequest{
			Commit: pj.ji.OutputCommit,
		}); err != nil {
		return err
	}
	// Load and clear the meta commit.
	pj.metaCommitInfo, err = pachClient.PfsAPIClient.InspectCommit(
		pachClient.Ctx(),
		&pfs.InspectCommitRequest{
			Commit: ppsutil.MetaCommit(pj.ji.OutputCommit),
			Wait:   pfs.CommitState_STARTED,
		})
	if err != nil {
		return err
	}
	if _, err := pachClient.PfsAPIClient.ClearCommit(
		pachClient.Ctx(),
		&pfs.ClearCommitRequest{
			Commit: ppsutil.MetaCommit(pj.ji.OutputCommit),
		}); err != nil {
		return err
	}
	// Find the most recent successful ancestor commit to use as the
	// base for this job.
	// TODO: This should be an operation supported and exposed by PFS.
	pj.parentMetaCommit = pj.metaCommitInfo.ParentCommit
	for pj.parentMetaCommit != nil {
		ci, err := pachClient.PfsAPIClient.InspectCommit(
			pachClient.Ctx(),
			&pfs.InspectCommitRequest{
				Commit: pj.parentMetaCommit,
				Wait:   pfs.CommitState_STARTED,
			})
		if err != nil {
			return err
		}
		if !ci.Error {
			if ci.Finished != nil {
				pj.parentDit = datum.NewCommitIterator(pachClient, pj.parentMetaCommit)
			} else {
				parentJi, err := pachClient.InspectJob(pj.ji.Job.Pipeline.Name, pj.parentMetaCommit.ID, true)
				if err != nil {
					return err
				}
				dit, err := datum.NewIterator(pachClient, parentJi.Details.Input)
				if err != nil {
					return err
				}
				pj.parentDit = datum.NewJobIterator(dit, parentJi.Job, pj.hasher)
			}
			break
		}
		pj.parentMetaCommit = ci.ParentCommit
	}
	pj.clearJobStats()
	return nil
}

func (pj *pendingJob) clearJobStats() {
	pj.ji.Stats = &pps.ProcessStats{}
	pj.ji.DataProcessed = 0
	pj.ji.DataSkipped = 0
	pj.ji.DataFailed = 0
	pj.ji.DataRecovered = 0
	pj.ji.DataTotal = 0
}

func (pj *pendingJob) withDeleter(pachClient *client.APIClient, cb func(datum.Deleter) error) error {
	// Setup modify file client for meta commit.
	metaCommit := pj.metaCommitInfo.Commit
	return pachClient.WithModifyFileClient(metaCommit, func(mfMeta client.ModifyFile) error {
		// Setup modify file client for output commit.
		outputCommit := pj.commitInfo.Commit
		return pachClient.WithModifyFileClient(outputCommit, func(mfPFS client.ModifyFile) error {
			parentMetaCommit := pj.parentMetaCommit
			metaFileWalker := func(path string) ([]string, error) {
				var files []string
				if err := pachClient.WalkFile(parentMetaCommit, path, func(fi *pfs.FileInfo) error {
					if fi.FileType == pfs.FileType_FILE {
						files = append(files, fi.File.Path)
					}
					return nil
				}); err != nil {
					return nil, err
				}
				return files, nil
			}
			return cb(datum.NewDeleter(metaFileWalker, mfMeta, mfPFS))
		})
	})
}

// The datums that can be processed in parallel are the datums that exist in the current job and do not exist in the parent job.
func (pj *pendingJob) withParallelDatums(pachClient *client.APIClient, cb func(context.Context, datum.Iterator) error) error {
	return pachClient.WithRenewer(func(ctx context.Context, renewer *renew.StringSet) error {
		pachClient = pachClient.WithCtx(ctx)
		// Upload the datums from the current job into the datum file set format.
		dit, err := datum.NewIterator(pachClient, pj.ji.Details.Input)
		if err != nil {
			return err
		}
		dit = datum.NewJobIterator(dit, pj.ji.Job, pj.hasher)
		fileSetID, err := uploadDatumFileSet(pachClient, dit)
		if err != nil {
			return err
		}
		renewer.Add(fileSetID)
		// Set up the datum iterators for merging.
		// If there is no parent, only use the datum iterator for the current job.
		var dits []datum.Iterator
		if pj.parentMetaCommit != nil {
			// Upload the datums from the parent job into the datum file set format.
			parentFileSetID, err := uploadDatumFileSet(pachClient, pj.parentDit)
			if err != nil {
				return err
			}
			renewer.Add(parentFileSetID)
			dits = append(dits, datum.NewFileSetIterator(pachClient, parentFileSetID))
		}
		dits = append(dits, datum.NewFileSetIterator(pachClient, fileSetID))
		// Create the output datum file set for the new datums (datums that do not exist in the parent job).
		outputFileSetID, err := withDatumFileSet(pachClient, func(outputSet *datum.Set) error {
			return datum.Merge(dits, func(metas []*datum.Meta) error {
				if len(metas) > 1 || !proto.Equal(metas[0].Job, pj.ji.Job) {
					return nil
				}
				pj.logger.WithData(metas[0].Inputs).Logf("setting up datum for processing (parallel jobs)")
				return outputSet.UploadMeta(metas[0], datum.WithPrefixIndex())
			})
		})
		if err != nil {
			return err
		}
		renewer.Add(outputFileSetID)
		return cb(ctx, datum.NewFileSetIterator(pachClient, outputFileSetID))
	})
}

// The datums that must be processed serially (with respect to the parent job) are the datums that exist in both the current and parent job.
// A datum is skipped if it exists in both jobs with the same hash and was successfully processed by the parent.
// Deletion operations are created for the datums that need to be removed from the parent job output commits.
func (pj *pendingJob) withSerialDatums(pachClient *client.APIClient, cb func(context.Context, datum.Iterator) error) error {
	// There are no serial datums if no parent exists.
	if pj.parentMetaCommit == nil {
		return nil
	}
	// Wait for the parent job to finish.
	ci, err := pachClient.WaitCommit(pj.parentMetaCommit.Branch.Repo.Name, pj.parentMetaCommit.Branch.Name, pj.parentMetaCommit.ID)
	if err != nil {
		return err
	}
	if ci.Error {
		return pfsserver.ErrCommitError{Commit: ci.Commit}
	}
	return pachClient.WithRenewer(func(ctx context.Context, renewer *renew.StringSet) error {
		pachClient = pachClient.WithCtx(ctx)
		// Upload the datums from the current job into the datum file set format.
		// TODO: Cache this in the parallel step and reuse here.
		dit, err := datum.NewIterator(pachClient, pj.ji.Details.Input)
		if err != nil {
			return err
		}
		dit = datum.NewJobIterator(dit, pj.ji.Job, pj.hasher)
		fileSetID, err := uploadDatumFileSet(pachClient, dit)
		if err != nil {
			return err
		}
		renewer.Add(fileSetID)
		// Setup an iterator using the parent meta commit.
		parentDit := datum.NewCommitIterator(pachClient, pj.parentMetaCommit)
		// Create the output datum file set for the datums that were not processed by the parent (failed, recovered, etc.).
		// Also create deletion operations appropriately.
		fileSetIterator := datum.NewFileSetIterator(pachClient, fileSetID)
		stats := &datum.Stats{ProcessStats: &pps.ProcessStats{}}
		outputFileSetID, err := withDatumFileSet(pachClient, func(s *datum.Set) error {
			return pj.withDeleter(pachClient, func(deleter datum.Deleter) error {
				return datum.Merge([]datum.Iterator{parentDit, fileSetIterator}, func(metas []*datum.Meta) error {
					if len(metas) == 1 {
						// Datum was processed in the parallel step.
						if proto.Equal(metas[0].Job, pj.ji.Job) {
							return nil
						}
						// Datum only exists in the parent job.
						return deleter(metas[0])
					}
					// Check if a skippable datum was successfully processed by the parent.
					if pj.skippableDatum(metas[1], metas[0]) {
						stats.Skipped++
						return nil
					}
					if err := deleter(metas[0]); err != nil {
						return err
					}
					pj.logger.WithData(metas[1].Inputs).Logf("setting up datum for processing (serial jobs)")
					return s.UploadMeta(metas[1], datum.WithPrefixIndex())
				})
			})
		})
		if err != nil {
			return err
		}
		renewer.Add(outputFileSetID)
		pj.saveJobStats(stats)
		if err := pj.writeJobInfo(); err != nil {
			return err
		}
		return cb(ctx, datum.NewFileSetIterator(pachClient, outputFileSetID))
	})
}

func uploadDatumFileSet(pachClient *client.APIClient, dit datum.Iterator) (string, error) {
	return withDatumFileSet(pachClient, func(s *datum.Set) error {
		return dit.Iterate(func(meta *datum.Meta) error {
			return s.UploadMeta(meta)
		})
	})
}

func withDatumFileSet(pachClient *client.APIClient, cb func(*datum.Set) error) (string, error) {
	resp, err := pachClient.WithCreateFileSetClient(func(mf client.ModifyFile) error {
		storageRoot := filepath.Join(os.TempDir(), "pachyderm-skipped-tmp", uuid.NewWithoutDashes())
		return datum.WithSet(nil, storageRoot, cb, datum.WithMetaOutput(mf))
	})
	if err != nil {
		return "", err
	}
	return resp.FileSetId, nil
}

func (pj *pendingJob) skippableDatum(meta1, meta2 *datum.Meta) bool {
	if pj.noSkip {
		return false
	}
	// If the hashes are equal and the second datum was processed, then skip it.
	return meta1.Hash == meta2.Hash && meta2.State == datum.State_PROCESSED
}

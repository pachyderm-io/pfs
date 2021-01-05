package server

import (
	"time"

	units "github.com/docker/go-units"
	"github.com/gogo/protobuf/types"
	"github.com/robfig/cron"

	"github.com/pachyderm/pachyderm/src/client"
	"github.com/pachyderm/pachyderm/src/client/pfs"
	"github.com/pachyderm/pachyderm/src/client/pkg/errors"
	col "github.com/pachyderm/pachyderm/src/server/pkg/collection"
	txnenv "github.com/pachyderm/pachyderm/src/server/pkg/transactionenv"
)

// triggerCommit is called when a commit is finished, it updates branches in
// the repo if they trigger on the change and returns all branches which were
// moved by this call.
func (d *driver) triggerCommit(
	txnCtx *txnenv.TransactionContext,
	commit *pfs.Commit,
) ([]*pfs.Branch, error) {
	repos := d.repos.ReadWrite(txnCtx.Stm)
	branches := d.branches(commit.Repo.Name).ReadWrite(txnCtx.Stm)
	commits := d.commits(commit.Repo.Name).ReadWrite(txnCtx.Stm)
	repoInfo := &pfs.RepoInfo{}
	if err := repos.Get(commit.Repo.Name, repoInfo); err != nil {
		return nil, err
	}
	newHead := &pfs.CommitInfo{}
	if err := commits.Get(commit.ID, newHead); err != nil {
		return nil, err
	}
	// find which branches this commit is the head of
	headBranches := make(map[string]bool)
	if newHead.Branch != nil {
		// If the commit was made as part of a branch, then it _was_ the branch head
		// at some point in time, although it is not guaranteed to still be the
		// branch head. This can happen on a downstream pipeline with triggers - the
		// upstream pipeline may have multiple unfinished commits in its output
		// branch that will be finished one at a time. Without this code, only
		// finishing the _last_ commit would have a chance of triggering.
		headBranches[newHead.Branch.Name] = true
	}
	for _, b := range repoInfo.Branches {
		bi := &pfs.BranchInfo{}
		if err := branches.Get(b.Name, bi); err != nil {
			return nil, err
		}
		if bi.Head != nil && bi.Head.ID == commit.ID {
			headBranches[b.Name] = true
		}
	}
	triggeredBranches := map[string]bool{}
	var result []*pfs.Branch
	var triggerBranch func(branch string) error
	triggerBranch = func(branch string) error {
		if triggeredBranches[branch] {
			return nil
		}
		triggeredBranches[branch] = true
		bi := &pfs.BranchInfo{}
		if err := branches.Get(branch, bi); err != nil {
			return err
		}
		if bi.Trigger != nil {
			if err := triggerBranch(bi.Trigger.Branch); err != nil && !col.IsErrNotFound(err) {
				return err
			}
			if headBranches[bi.Trigger.Branch] {
				var oldHead *pfs.CommitInfo
				if bi.Head != nil {
					oldHead = &pfs.CommitInfo{}
					if err := commits.Get(bi.Head.ID, oldHead); err != nil {
						return err
					}
				}
				triggered, err := d.isTriggered(txnCtx, bi.Trigger, oldHead, newHead)
				if err != nil {
					return err
				}
				if triggered {
					if err := branches.Update(bi.Name, bi, func() error {
						bi.Head = newHead.Commit
						return nil
					}); err != nil {
						return err
					}
					result = append(result, client.NewBranch(commit.Repo.Name, branch))
					headBranches[bi.Branch.Name] = true
				}
			}
		}
		return nil
	}
	for _, b := range repoInfo.Branches {
		if err := triggerBranch(b.Name); err != nil {
			return nil, err
		}
	}
	return result, nil
}

// isTriggered checks to see if a branch should be updated from oldHead to
// newHead based on a trigger.
func (d *driver) isTriggered(txnCtx *txnenv.TransactionContext, t *pfs.Trigger, oldHead, newHead *pfs.CommitInfo) (bool, error) {
	result := t.All
	merge := func(cond bool) {
		if t.All {
			result = result && cond
		} else {
			result = result || cond
		}
	}
	if t.Size_ != "" {
		size, err := units.FromHumanSize(t.Size_)
		if err != nil {
			// Shouldn't be possible to error here since we validate on ingress
			return false, errors.EnsureStack(err)
		}
		var oldSize uint64
		if oldHead != nil {
			oldSize = oldHead.SizeBytes
		}
		merge(int64(newHead.SizeBytes-oldSize) >= size)
	}
	if t.CronSpec != "" {
		// Shouldn't be possible to error here since we validate on ingress
		schedule, err := cron.ParseStandard(t.CronSpec)
		if err != nil {
			// Shouldn't be possible to error here since we validate on ingress
			return false, errors.EnsureStack(err)
		}
		var oldTime, newTime time.Time
		if oldHead != nil && oldHead.Finished != nil {
			oldTime, err = types.TimestampFromProto(oldHead.Finished)
			if err != nil {
				return false, errors.EnsureStack(err)
			}
		}
		if newHead.Finished != nil {
			newTime, err = types.TimestampFromProto(newHead.Finished)
			if err != nil {
				return false, errors.EnsureStack(err)
			}
		}
		merge(schedule.Next(oldTime).Before(newTime))
	}
	if t.Commits != 0 {
		ci := newHead
		var commits int64
		for commits < t.Commits {
			commits++
			if ci.ParentCommit != nil && (oldHead == nil || oldHead.Commit.ID != ci.ParentCommit.ID) {
				var err error
				ci, err = d.inspectCommit(txnCtx.Client, ci.ParentCommit, pfs.CommitState_STARTED)
				if err != nil {
					return false, err
				}
			} else {
				break
			}
		}
		merge(commits == t.Commits)
	}
	return result, nil
}

// validateTrigger returns an error if a trigger is invalid
func (d *driver) validateTrigger(txnCtx *txnenv.TransactionContext, branch *pfs.Branch, trigger *pfs.Trigger) error {
	if trigger == nil {
		return nil
	}
	if trigger.Branch == "" {
		return errors.Errorf("triggers must specify a branch to trigger on")
	}
	if _, err := cron.ParseStandard(trigger.CronSpec); trigger.CronSpec != "" && err != nil {
		return errors.Wrapf(err, "invalid trigger cron spec")
	}
	if _, err := units.FromHumanSize(trigger.Size_); trigger.Size_ != "" && err != nil {
		return errors.Wrapf(err, "invalid trigger size")
	}
	if trigger.Commits < 0 {
		return errors.Errorf("can't trigger on a negative number of commits")
	}
	bis, err := d.listBranch(txnCtx.Client, branch.Repo, false)
	if err != nil {
		return err
	}
	biMaps := make(map[string]*pfs.BranchInfo)
	for _, bi := range bis {
		biMaps[bi.Branch.Name] = bi
	}
	b := trigger.Branch
	for {
		if b == branch.Name {
			return errors.Errorf("triggers cannot form a loop")
		}
		bi, ok := biMaps[b]
		if ok && bi.Trigger != nil {
			b = bi.Trigger.Branch
		} else {
			break
		}
	}
	return nil
}

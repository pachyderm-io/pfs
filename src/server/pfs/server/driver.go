package server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	globlib "github.com/pachyderm/ohmyglob"
	"github.com/pachyderm/pachyderm/src/client"
	"github.com/pachyderm/pachyderm/src/client/auth"
	"github.com/pachyderm/pachyderm/src/client/limit"
	"github.com/pachyderm/pachyderm/src/client/pfs"
	"github.com/pachyderm/pachyderm/src/client/pkg/grpcutil"
	pfsserver "github.com/pachyderm/pachyderm/src/server/pfs"
	"github.com/pachyderm/pachyderm/src/server/pkg/ancestry"
	col "github.com/pachyderm/pachyderm/src/server/pkg/collection"
	"github.com/pachyderm/pachyderm/src/server/pkg/errutil"
	"github.com/pachyderm/pachyderm/src/server/pkg/hashtree"
	"github.com/pachyderm/pachyderm/src/server/pkg/obj"
	"github.com/pachyderm/pachyderm/src/server/pkg/pfsdb"
	"github.com/pachyderm/pachyderm/src/server/pkg/ppsconsts"
	"github.com/pachyderm/pachyderm/src/server/pkg/serviceenv"
	"github.com/pachyderm/pachyderm/src/server/pkg/sql"
	"github.com/pachyderm/pachyderm/src/server/pkg/storage/chunk"
	"github.com/pachyderm/pachyderm/src/server/pkg/storage/fileset"
	txnenv "github.com/pachyderm/pachyderm/src/server/pkg/transactionenv"
	"github.com/pachyderm/pachyderm/src/server/pkg/uuid"
	"github.com/pachyderm/pachyderm/src/server/pkg/watch"
	"github.com/sirupsen/logrus"

	etcd "github.com/coreos/etcd/clientv3"
	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
)

const (
	splitSuffixBase  = 16
	splitSuffixWidth = 64
	splitSuffixFmt   = "%016x"

	// Makes calls to ListRepo and InspectRepo more legible
	includeAuth = true
)

// IsPermissionError returns true if a given error is a permission error.
func IsPermissionError(err error) bool {
	return strings.Contains(err.Error(), "has already finished")
}

func destroyHashtree(tree hashtree.HashTree) {
	if err := tree.Destroy(); err != nil {
		logrus.Infof("failed to destroy hashtree: %v", err)
	}
}

// CommitEvent is an event that contains a CommitInfo or an error
type CommitEvent struct {
	Err   error
	Value *pfs.CommitInfo
}

// CommitStream is a stream of CommitInfos
type CommitStream interface {
	Stream() <-chan CommitEvent
	Close()
}

type collectionFactory func(string) col.Collection

type driver struct {
	// etcdClient and prefix write repo and other metadata to etcd
	etcdClient *etcd.Client
	txnEnv     *txnenv.TransactionEnv
	prefix     string

	// collections
	repos          col.Collection
	putFileRecords col.Collection
	commits        collectionFactory
	branches       collectionFactory
	openCommits    col.Collection

	// a cache for hashtrees
	treeCache *hashtree.Cache

	// storageRoot where we store hashtrees
	storageRoot string

	// memory limiter (useful for limiting operations that could use a lot of memory)
	memoryLimiter *semaphore.Weighted
	// put object limiter (useful for limiting put object requests)
	putObjectLimiter limit.ConcurrencyLimiter

	// New storage layer.
	storage    *fileset.Storage
	subFileSet int64
}

// newDriver is used to create a new Driver instance
func newDriver(
	env *serviceenv.ServiceEnv,
	txnEnv *txnenv.TransactionEnv,
	etcdPrefix string,
	treeCache *hashtree.Cache,
	storageRoot string,
	memoryRequest int64,
) (*driver, error) {
	// Validate arguments
	if treeCache == nil {
		return nil, fmt.Errorf("cannot initialize driver with nil treeCache")
	}
	// Initialize driver
	etcdClient := env.GetEtcdClient()
	d := &driver{
		txnEnv:         txnEnv,
		etcdClient:     etcdClient,
		prefix:         etcdPrefix,
		repos:          pfsdb.Repos(etcdClient, etcdPrefix),
		putFileRecords: pfsdb.PutFileRecords(etcdClient, etcdPrefix),
		commits: func(repo string) col.Collection {
			return pfsdb.Commits(etcdClient, etcdPrefix, repo)
		},
		branches: func(repo string) col.Collection {
			return pfsdb.Branches(etcdClient, etcdPrefix, repo)
		},
		openCommits: pfsdb.OpenCommits(etcdClient, etcdPrefix),
		treeCache:   treeCache,
		storageRoot: storageRoot,
		// Allow up to a third of the requested memory to be used for memory intensive operations
		memoryLimiter:    semaphore.NewWeighted(memoryRequest / 3),
		putObjectLimiter: limit.New(env.StorageUploadConcurrencyLimit),
	}

	// Create spec repo (default repo)
	repo := client.NewRepo(ppsconsts.SpecRepo)
	repoInfo := &pfs.RepoInfo{
		Repo:    repo,
		Created: now(),
	}
	if _, err := col.NewSTM(context.Background(), etcdClient, func(stm col.STM) error {
		repos := d.repos.ReadWrite(stm)
		return repos.Create(repo.Name, repoInfo)
	}); err != nil && !col.IsErrExists(err) {
		return nil, err
	}
	if env.NewStorageLayer {
		// (bryce) local client for testing.
		// need to figure out obj_block_api_server before this
		// can be changed.
		objC, err := obj.NewLocalClient(storageRoot)
		if err != nil {
			return nil, err
		}
		chunkStorage := chunk.NewStorage(objC, chunk.ServiceEnvToOptions(env)...)
		d.storage = fileset.NewStorage(objC, chunkStorage, fileset.ServiceEnvToOptions(env)...)
		go d.compactionWorker()
	}
	return d, nil
}

// checkIsAuthorizedInTransaction is identicalto checkIsAuthorized except that
// it performs reads consistent with the latest state of the STM transaction.
func (d *driver) checkIsAuthorizedInTransaction(txnCtx *txnenv.TransactionContext, r *pfs.Repo, s auth.Scope) error {
	me, err := txnCtx.Client.WhoAmI(txnCtx.ClientContext, &auth.WhoAmIRequest{})
	if auth.IsErrNotActivated(err) {
		return nil
	}

	req := &auth.AuthorizeRequest{Repo: r.Name, Scope: s}
	resp, err := txnCtx.Auth().AuthorizeInTransaction(txnCtx, req)
	if err != nil {
		return fmt.Errorf("error during authorization check for operation on \"%s\": %v",
			r.Name, grpcutil.ScrubGRPC(err))
	}
	if !resp.Authorized {
		return &auth.ErrNotAuthorized{Subject: me.Username, Repo: r.Name, Required: s}
	}
	return nil
}

// checkIsAuthorized returns an error if the current user (in 'pachClient') has
// authorization scope 's' for repo 'r'
func (d *driver) checkIsAuthorized(pachClient *client.APIClient, r *pfs.Repo, s auth.Scope) error {
	ctx := pachClient.Ctx()
	me, err := pachClient.WhoAmI(ctx, &auth.WhoAmIRequest{})
	if auth.IsErrNotActivated(err) {
		return nil
	}

	req := &auth.AuthorizeRequest{Repo: r.Name, Scope: s}
	resp, err := pachClient.AuthAPIClient.Authorize(ctx, req)
	if err != nil {
		return fmt.Errorf("error during authorization check for operation on \"%s\": %v",
			r.Name, grpcutil.ScrubGRPC(err))
	}
	if !resp.Authorized {
		return &auth.ErrNotAuthorized{Subject: me.Username, Repo: r.Name, Required: s}
	}
	return nil
}

func now() *types.Timestamp {
	t, err := types.TimestampProto(time.Now())
	if err != nil {
		return &types.Timestamp{}
	}
	return t
}

func (d *driver) createRepo(txnCtx *txnenv.TransactionContext, repo *pfs.Repo, description string, update bool) error {
	// Validate arguments
	if repo == nil {
		return errors.New("repo cannot be nil")
	}

	// Check that the user is logged in (user doesn't need any access level to
	// create a repo, but they must be authenticated if auth is active)
	whoAmI, err := txnCtx.Client.WhoAmI(txnCtx.ClientContext, &auth.WhoAmIRequest{})
	authIsActivated := !auth.IsErrNotActivated(err)
	if authIsActivated && err != nil {
		return fmt.Errorf("error authenticating (must log in to create a repo): %v",
			grpcutil.ScrubGRPC(err))
	}
	if err := ancestry.ValidateName(repo.Name); err != nil {
		return err
	}

	repos := d.repos.ReadWrite(txnCtx.Stm)

	// check if 'repo' already exists. If so, return that error. Otherwise,
	// proceed with ACL creation (avoids awkward "access denied" error when
	// calling "createRepo" on a repo that already exists)
	var existingRepoInfo pfs.RepoInfo
	err = repos.Get(repo.Name, &existingRepoInfo)
	if err != nil && !col.IsErrNotFound(err) {
		return fmt.Errorf("error checking whether \"%s\" exists: %v",
			repo.Name, err)
	} else if err == nil && !update {
		return fmt.Errorf("cannot create \"%s\" as it already exists", repo.Name)
	}
	created := now()
	if err == nil {
		created = existingRepoInfo.Created
	}

	// Create ACL for new repo
	if authIsActivated {
		// auth is active, and user is logged in. Make user an owner of the new
		// repo (and clear any existing ACL under this name that might have been
		// created by accident)
		_, err := txnCtx.Auth().SetACLInTransaction(txnCtx, &auth.SetACLRequest{
			Repo: repo.Name,
			Entries: []*auth.ACLEntry{{
				Username: whoAmI.Username,
				Scope:    auth.Scope_OWNER,
			}},
		})
		if err != nil {
			return fmt.Errorf("could not create ACL for new repo \"%s\": %v",
				repo.Name, grpcutil.ScrubGRPC(err))
		}
	}

	repoInfo := &pfs.RepoInfo{
		Repo:        repo,
		Created:     created,
		Description: description,
	}
	// Only Put the new repoInfo if something has changed.  This
	// optimization is impactful because pps will frequently update the
	// __spec__ repo to make sure it exists.
	if !proto.Equal(repoInfo, &existingRepoInfo) {
		return repos.Put(repo.Name, repoInfo)
	}
	return nil
}

func (d *driver) inspectRepo(
	txnCtx *txnenv.TransactionContext,
	repo *pfs.Repo,
	includeAuth bool,
) (*pfs.RepoInfo, error) {
	// Validate arguments
	if repo == nil {
		return nil, errors.New("repo cannot be nil")
	}

	result := &pfs.RepoInfo{}
	if err := d.repos.ReadWrite(txnCtx.Stm).Get(repo.Name, result); err != nil {
		return nil, err
	}
	if includeAuth {
		accessLevel, err := d.getAccessLevel(txnCtx.Client, repo)
		if err != nil {
			if auth.IsErrNotActivated(err) {
				return result, nil
			}
			return nil, fmt.Errorf("error getting access level for \"%s\": %v",
				repo.Name, grpcutil.ScrubGRPC(err))
		}
		result.AuthInfo = &pfs.RepoAuthInfo{AccessLevel: accessLevel}
	}
	return result, nil
}

func (d *driver) getAccessLevel(pachClient *client.APIClient, repo *pfs.Repo) (auth.Scope, error) {
	ctx := pachClient.Ctx()
	who, err := pachClient.AuthAPIClient.WhoAmI(ctx, &auth.WhoAmIRequest{})
	if err != nil {
		return auth.Scope_NONE, err
	}
	if who.IsAdmin {
		return auth.Scope_OWNER, nil
	}
	resp, err := pachClient.AuthAPIClient.GetScope(ctx, &auth.GetScopeRequest{Repos: []string{repo.Name}})
	if err != nil {
		return auth.Scope_NONE, err
	}
	if len(resp.Scopes) != 1 {
		return auth.Scope_NONE, fmt.Errorf("too many results from GetScope: %#v", resp)
	}
	return resp.Scopes[0], nil
}

func equalBranches(a, b []*pfs.Branch) bool {
	aMap := make(map[string]bool)
	bMap := make(map[string]bool)
	key := path.Join
	for _, branch := range a {
		aMap[key(branch.Repo.Name, branch.Name)] = true
	}
	for _, branch := range b {
		bMap[key(branch.Repo.Name, branch.Name)] = true
	}
	if len(aMap) != len(bMap) {
		return false
	}

	for k := range aMap {
		if !bMap[k] {
			return false
		}
	}
	return true
}

func equalCommits(a, b []*pfs.Commit) bool {
	aMap := make(map[string]bool)
	bMap := make(map[string]bool)
	key := path.Join
	for _, commit := range a {
		aMap[key(commit.Repo.Name, commit.ID)] = true
	}
	for _, commit := range b {
		bMap[key(commit.Repo.Name, commit.ID)] = true
	}
	if len(aMap) != len(bMap) {
		return false
	}

	for k := range aMap {
		if !bMap[k] {
			return false
		}
	}
	return true
}

// ErrBranchProvenanceTransitivity Branch provenance is not transitively closed.
// This struct contains all the information that was used to demonstrate that this invariant is not being satisfied.
type ErrBranchProvenanceTransitivity struct {
	BranchInfo     *pfs.BranchInfo
	FullProvenance []*pfs.Branch
}

func (e ErrBranchProvenanceTransitivity) Error() string {
	var msg strings.Builder
	msg.WriteString("consistency error: branch provenance was not transitive\n")
	msg.WriteString("on branch " + e.BranchInfo.Name + " in repo " + e.BranchInfo.Branch.Repo.Name + "\n")
	fullMap := make(map[string]*pfs.Branch)
	provMap := make(map[string]*pfs.Branch)
	key := path.Join
	for _, branch := range e.FullProvenance {
		fullMap[key(branch.Repo.Name, branch.Name)] = branch
	}
	provMap[key(e.BranchInfo.Branch.Repo.Name, e.BranchInfo.Name)] = e.BranchInfo.Branch
	for _, branch := range e.BranchInfo.Provenance {
		provMap[key(branch.Repo.Name, branch.Name)] = branch
	}
	msg.WriteString("the following branches are missing from the provenance:\n")
	for k, v := range fullMap {
		if _, ok := provMap[k]; !ok {
			msg.WriteString(v.Name + " in repo " + v.Repo.Name + "\n")
		}
	}
	return msg.String()
}

// ErrBranchInfoNotFound Branch info could not be found. Typically because of an incomplete deletion of a branch.
// This struct contains all the information that was used to demonstrate that this invariant is not being satisfied.
type ErrBranchInfoNotFound struct {
	Branch *pfs.Branch
}

func (e ErrBranchInfoNotFound) Error() string {
	return fmt.Sprintf("consistency error: the branch %v on repo %v could not be found\n", e.Branch.Name, e.Branch.Repo.Name)
}

// ErrCommitInfoNotFound Commit info could not be found. Typically because of an incomplete deletion of a commit.
// This struct contains all the information that was used to demonstrate that this invariant is not being satisfied.
type ErrCommitInfoNotFound struct {
	Location string
	Commit   *pfs.Commit
}

func (e ErrCommitInfoNotFound) Error() string {
	return fmt.Sprintf("consistency error: the commit %v in repo %v could not be found while checking %v",
		e.Commit.ID, e.Commit.Repo.Name, e.Location)
}

// ErrInconsistentCommitProvenance Commit provenance somehow has a branch and commit from different repos.
// This struct contains all the information that was used to demonstrate that this invariant is not being satisfied.
type ErrInconsistentCommitProvenance struct {
	CommitProvenance *pfs.CommitProvenance
}

func (e ErrInconsistentCommitProvenance) Error() string {
	return fmt.Sprintf("consistency error: the commit provenance has repo %v for the branch but repo %v for the commit",
		e.CommitProvenance.Branch.Repo.Name, e.CommitProvenance.Commit.Repo.Name)
}

// ErrHeadProvenanceInconsistentWithBranch The head provenance of a branch does not match the branch's provenance
// This struct contains all the information that was used to demonstrate that this invariant is not being satisfied.
type ErrHeadProvenanceInconsistentWithBranch struct {
	BranchInfo     *pfs.BranchInfo
	ProvBranchInfo *pfs.BranchInfo
	HeadCommitInfo *pfs.CommitInfo
}

func (e ErrHeadProvenanceInconsistentWithBranch) Error() string {
	var msg strings.Builder
	msg.WriteString("consistency error: head provenance is not consistent with branch provenance\n")
	msg.WriteString("on branch " + e.BranchInfo.Name + " in repo " + e.BranchInfo.Branch.Repo.Name + "\n")
	msg.WriteString("which has head commit " + e.HeadCommitInfo.Commit.ID + "\n")
	msg.WriteString("this branch is provenant on the branch " +
		e.ProvBranchInfo.Name + " in repo " + e.ProvBranchInfo.Branch.Repo.Name + "\n")
	msg.WriteString("which has head commit " + e.ProvBranchInfo.Head.ID + "\n")
	msg.WriteString("but this commit is missing from the head commit provenance\n")
	return msg.String()
}

// ErrProvenanceTransitivity Commit provenance is not transitively closed.
// This struct contains all the information that was used to demonstrate that this invariant is not being satisfied.
type ErrProvenanceTransitivity struct {
	CommitInfo     *pfs.CommitInfo
	FullProvenance []*pfs.Commit
}

func (e ErrProvenanceTransitivity) Error() string {
	var msg strings.Builder
	msg.WriteString("consistency error: commit provenance was not transitive\n")
	msg.WriteString("on commit " + e.CommitInfo.Commit.ID + " in repo " + e.CommitInfo.Commit.Repo.Name + "\n")
	fullMap := make(map[string]*pfs.Commit)
	provMap := make(map[string]*pfs.Commit)
	key := path.Join
	for _, prov := range e.FullProvenance {
		fullMap[key(prov.Repo.Name, prov.ID)] = prov
	}
	for _, prov := range e.CommitInfo.Provenance {
		provMap[key(prov.Commit.Repo.Name, prov.Commit.ID)] = prov.Commit
	}
	msg.WriteString("the following commit provenances are missing from the full provenance:\n")
	for k, v := range fullMap {
		if _, ok := provMap[k]; !ok {
			msg.WriteString(v.ID + " in repo " + v.Repo.Name + "\n")
		}
	}
	return msg.String()
}

// ErrNilCommitInSubvenance Commit provenance somehow has a branch and commit from different repos.
// This struct contains all the information that was used to demonstrate that this invariant is not being satisfied.
type ErrNilCommitInSubvenance struct {
	CommitInfo      *pfs.CommitInfo
	SubvenanceRange *pfs.CommitRange
}

func (e ErrNilCommitInSubvenance) Error() string {
	upper := "<nil>"
	if e.SubvenanceRange.Upper != nil {
		upper = e.SubvenanceRange.Upper.ID
	}
	lower := "<nil>"
	if e.SubvenanceRange.Lower != nil {
		lower = e.SubvenanceRange.Lower.ID
	}
	return fmt.Sprintf("consistency error: the commit %v has nil subvenance in the %v - %v range",
		e.CommitInfo.Commit.ID, lower, upper)
}

// ErrSubvenanceOfProvenance The commit was not found in its provenance's subvenance
// This struct contains all the information that was used to demonstrate that this invariant is not being satisfied.
type ErrSubvenanceOfProvenance struct {
	CommitInfo     *pfs.CommitInfo
	ProvCommitInfo *pfs.CommitInfo
}

func (e ErrSubvenanceOfProvenance) Error() string {
	var msg strings.Builder
	msg.WriteString("consistency error: the commit was not in its provenance's subvenance\n")
	msg.WriteString("commit " + e.CommitInfo.Commit.ID + " in repo " + e.CommitInfo.Commit.Repo.Name + "\n")
	msg.WriteString("provenance commit " + e.ProvCommitInfo.Commit.ID + " in repo " + e.ProvCommitInfo.Commit.Repo.Name + "\n")
	return msg.String()
}

// ErrProvenanceOfSubvenance The commit was not found in its subvenance's provenance
// This struct contains all the information that was used to demonstrate that this invariant is not being satisfied.
type ErrProvenanceOfSubvenance struct {
	CommitInfo     *pfs.CommitInfo
	SubvCommitInfo *pfs.CommitInfo
}

func (e ErrProvenanceOfSubvenance) Error() string {
	var msg strings.Builder
	msg.WriteString("consistency error: the commit was not in its subvenance's provenance\n")
	msg.WriteString("commit " + e.CommitInfo.Commit.ID + " in repo " + e.CommitInfo.Commit.Repo.Name + "\n")
	msg.WriteString("subvenance commit " + e.SubvCommitInfo.Commit.ID + " in repo " + e.SubvCommitInfo.Commit.Repo.Name + "\n")
	return msg.String()
}

// fsck verifies that pfs satisfies the following invariants:
// 1. Branch provenance is transitive
// 2. Head commit provenance has heads of branch's branch provenance
// 3. Commit provenance is transitive
// 4. Commit provenance and commit subvenance are dual relations
// If fix is true it will attempt to fix as many of these issues as it can.
func (d *driver) fsck(pachClient *client.APIClient, fix bool, cb func(*pfs.FsckResponse) error) error {
	ctx := pachClient.Ctx()
	repos := d.repos.ReadOnly(ctx)
	key := path.Join

	onError := func(err error) error { return cb(&pfs.FsckResponse{Error: err.Error()}) }
	onFix := func(fix string) error { return cb(&pfs.FsckResponse{Fix: fix}) }

	// collect all the info for the branches and commits in pfs
	branchInfos := make(map[string]*pfs.BranchInfo)
	commitInfos := make(map[string]*pfs.CommitInfo)
	newCommitInfos := make(map[string]*pfs.CommitInfo)
	repoInfo := &pfs.RepoInfo{}
	if err := repos.List(repoInfo, col.DefaultOptions, func(repoName string) error {
		commits := d.commits(repoName).ReadOnly(ctx)
		commitInfo := &pfs.CommitInfo{}
		if err := commits.List(commitInfo, col.DefaultOptions, func(commitID string) error {
			commitInfos[key(repoName, commitID)] = proto.Clone(commitInfo).(*pfs.CommitInfo)
			return nil
		}); err != nil {
			return err
		}
		branches := d.branches(repoName).ReadOnly(ctx)
		branchInfo := &pfs.BranchInfo{}
		return branches.List(branchInfo, col.DefaultOptions, func(branchName string) error {
			branchInfos[key(repoName, branchName)] = proto.Clone(branchInfo).(*pfs.BranchInfo)
			return nil
		})
	}); err != nil {
		return err
	}

	// for each branch
	for _, bi := range branchInfos {
		// we expect the branch's provenance to equal the union of the provenances of the branch's direct provenances
		// i.e. union(branch, branch.Provenance) = union(branch, branch.DirectProvenance, branch.DirectProvenance.Provenance)
		direct := bi.DirectProvenance
		union := []*pfs.Branch{bi.Branch}
		for _, directProvenance := range direct {
			directProvenanceInfo := branchInfos[key(directProvenance.Repo.Name, directProvenance.Name)]
			union = append(union, directProvenance)
			if directProvenanceInfo != nil {
				union = append(union, directProvenanceInfo.Provenance...)
			}
		}

		if !equalBranches(append(bi.Provenance, bi.Branch), union) {
			if err := onError(ErrBranchProvenanceTransitivity{
				BranchInfo:     bi,
				FullProvenance: union,
			}); err != nil {
				return err
			}
		}

		// 	if there is a HEAD commit
		if bi.Head != nil {
			// we expect the branch's provenance to equal the HEAD commit's provenance
			// i.e branch.Provenance contains the branch provBranch and provBranch.Head != nil implies branch.Head.Provenance contains provBranch.Head
			// =>
			for _, provBranch := range bi.Provenance {
				provBranchInfo, ok := branchInfos[key(provBranch.Repo.Name, provBranch.Name)]
				if !ok {
					if err := onError(ErrBranchInfoNotFound{Branch: provBranch}); err != nil {
						return err
					}
					continue
				}
				if provBranchInfo.Head != nil {
					// in this case, the headCommit Provenance should contain provBranch.Head
					headCommitInfo, ok := commitInfos[key(bi.Head.Repo.Name, bi.Head.ID)]
					if !ok {
						if !fix {
							if err := onError(ErrCommitInfoNotFound{
								Location: "head commit provenance (=>)",
								Commit:   bi.Head,
							}); err != nil {
								return err
							}
							continue
						}
						headCommitInfo = &pfs.CommitInfo{
							Commit: bi.Head,
							Origin: &pfs.CommitOrigin{Kind: pfs.OriginKind_FSCK},
						}
						commitInfos[key(bi.Head.Repo.Name, bi.Head.ID)] = headCommitInfo
						newCommitInfos[key(bi.Head.Repo.Name, bi.Head.ID)] = headCommitInfo
						if err := onFix(fmt.Sprintf(
							"creating commit %s@%s which was missing, but referenced by %s@%s",
							bi.Head.Repo.Name, bi.Head.ID,
							bi.Branch.Repo.Name, bi.Branch.Name),
						); err != nil {
							return err
						}
					}
					// If this commit was created on an output branch, then we don't expect it to satisfy this invariant
					// due to the nature of the RunPipeline functionality.
					if headCommitInfo.Origin != nil && headCommitInfo.Origin.Kind == pfs.OriginKind_AUTO && len(headCommitInfo.Provenance) > 0 {
						continue
					}
					contains := false
					for _, headProv := range headCommitInfo.Provenance {
						if provBranchInfo.Head.Repo.Name == headProv.Commit.Repo.Name &&
							provBranchInfo.Branch.Repo.Name == headProv.Branch.Repo.Name &&
							provBranchInfo.Name == headProv.Branch.Name &&
							provBranchInfo.Head.ID == headProv.Commit.ID {
							contains = true
						}
					}
					if !contains {
						if err := onError(ErrHeadProvenanceInconsistentWithBranch{
							BranchInfo:     bi,
							ProvBranchInfo: provBranchInfo,
							HeadCommitInfo: headCommitInfo,
						}); err != nil {
							return err
						}
					}
				}
			}
		}
	}

	// for each commit
	for _, ci := range commitInfos {
		// ensure that the provenance is transitive
		directProvenance := make([]*pfs.Commit, 0, len(ci.Provenance))
		transitiveProvenance := make([]*pfs.Commit, 0, len(ci.Provenance))
		for _, prov := range ci.Provenance {
			// not part of the above invariant, but we want to make sure provenance is self-consistent
			if prov.Commit.Repo.Name != prov.Branch.Repo.Name {
				if err := onError(ErrInconsistentCommitProvenance{CommitProvenance: prov}); err != nil {
					return err
				}
			}
			directProvenance = append(directProvenance, prov.Commit)
			transitiveProvenance = append(transitiveProvenance, prov.Commit)
			provCommitInfo, ok := commitInfos[key(prov.Commit.Repo.Name, prov.Commit.ID)]
			if !ok {
				if !fix {
					if err := onError(ErrCommitInfoNotFound{
						Location: "provenance transitivity",
						Commit:   prov.Commit,
					}); err != nil {
						return err
					}
					continue
				}
				provCommitInfo = &pfs.CommitInfo{
					Commit: prov.Commit,
					Origin: &pfs.CommitOrigin{Kind: pfs.OriginKind_FSCK},
				}
				commitInfos[key(prov.Commit.Repo.Name, prov.Commit.ID)] = provCommitInfo
				newCommitInfos[key(prov.Commit.Repo.Name, prov.Commit.ID)] = provCommitInfo
				if err := onFix(fmt.Sprintf(
					"creating commit %s@%s which was missing, but referenced by %s@%s",
					prov.Commit.Repo.Name, prov.Commit.ID,
					ci.Commit.Repo.Name, ci.Commit.ID),
				); err != nil {
					return err
				}
			}
			for _, provProv := range provCommitInfo.Provenance {
				transitiveProvenance = append(transitiveProvenance, provProv.Commit)
			}
		}
		if !equalCommits(directProvenance, transitiveProvenance) {
			if err := onError(ErrProvenanceTransitivity{
				CommitInfo:     ci,
				FullProvenance: transitiveProvenance,
			}); err != nil {
				return err
			}
		}
	}

	// for each commit
	for _, ci := range commitInfos {
		// we expect that the commit is in the subvenance of another commit iff the other commit is in our commit's provenance
		// i.e. commit.Provenance contains commit C iff C.Subvenance contains commit or C = commit
		// =>
		for _, prov := range ci.Provenance {
			if prov.Commit.ID == ci.Commit.ID {
				continue
			}
			contains := false
			provCommitInfo, ok := commitInfos[key(prov.Commit.Repo.Name, prov.Commit.ID)]
			if !ok {
				if !fix {
					if err := onError(ErrCommitInfoNotFound{
						Location: "provenance for provenance-subvenance duality (=>)",
						Commit:   prov.Commit,
					}); err != nil {
						return err
					}
					continue
				}
				provCommitInfo = &pfs.CommitInfo{
					Commit: prov.Commit,
					Origin: &pfs.CommitOrigin{Kind: pfs.OriginKind_FSCK},
				}
				commitInfos[key(prov.Commit.Repo.Name, prov.Commit.ID)] = provCommitInfo
				newCommitInfos[key(prov.Commit.Repo.Name, prov.Commit.ID)] = provCommitInfo
				if err := onFix(fmt.Sprintf(
					"creating commit %s@%s which was missing, but referenced by %s@%s",
					prov.Commit.Repo.Name, prov.Commit.ID,
					ci.Commit.Repo.Name, ci.Commit.ID),
				); err != nil {
					return err
				}
			}
			for _, subvRange := range provCommitInfo.Subvenance {
				subvCommit := subvRange.Upper
				// loop through the subvenance range
				for {
					if subvCommit == nil {
						if err := onError(ErrNilCommitInSubvenance{
							CommitInfo:      provCommitInfo,
							SubvenanceRange: subvRange,
						}); err != nil {
							return err
						}
						break // can't continue loop now that subvCommit is nil
					}
					subvCommitInfo, ok := commitInfos[key(subvCommit.Repo.Name, subvCommit.ID)]
					if !ok {
						if !fix {
							if err := onError(ErrCommitInfoNotFound{
								Location: "subvenance for provenance-subvenance duality (=>)",
								Commit:   subvCommit,
							}); err != nil {
								return err
							}
							break // can't continue loop if we can't find this commit
						}
						subvCommitInfo = &pfs.CommitInfo{
							Commit: subvCommit,
							Origin: &pfs.CommitOrigin{Kind: pfs.OriginKind_FSCK},
						}
						commitInfos[key(subvCommit.Repo.Name, subvCommit.ID)] = subvCommitInfo
						newCommitInfos[key(subvCommit.Repo.Name, subvCommit.ID)] = subvCommitInfo
						if err := onFix(fmt.Sprintf(
							"creating commit %s@%s which was missing, but referenced by %s@%s",
							subvCommit.Repo.Name, subvCommit.ID,
							ci.Commit.Repo.Name, ci.Commit.ID),
						); err != nil {
							return err
						}
					}
					if ci.Commit.ID == subvCommit.ID {
						contains = true
					}

					if subvCommit.ID == subvRange.Lower.ID {
						break // check at the end of the loop so we fsck 'lower' too (inclusive range)
					}
					subvCommit = subvCommitInfo.ParentCommit
				}
			}
			if !contains {
				if err := onError(ErrSubvenanceOfProvenance{
					CommitInfo:     ci,
					ProvCommitInfo: provCommitInfo,
				}); err != nil {
					return err
				}
			}
		}
		// <=
		for _, subvRange := range ci.Subvenance {
			subvCommit := subvRange.Upper
			// loop through the subvenance range
			for {
				contains := false
				if subvCommit == nil {
					if err := onError(ErrNilCommitInSubvenance{
						CommitInfo:      ci,
						SubvenanceRange: subvRange,
					}); err != nil {
						return err
					}
					break // can't continue loop now that subvCommit is nil
				}
				subvCommitInfo, ok := commitInfos[key(subvCommit.Repo.Name, subvCommit.ID)]
				if !ok {
					if !fix {
						if err := onError(ErrCommitInfoNotFound{
							Location: "subvenance for provenance-subvenance duality (<=)",
							Commit:   subvCommit,
						}); err != nil {
							return err
						}
						break // can't continue loop if we can't find this commit
					}
					subvCommitInfo = &pfs.CommitInfo{
						Commit: subvCommit,
						Origin: &pfs.CommitOrigin{Kind: pfs.OriginKind_FSCK},
					}
					commitInfos[key(subvCommit.Repo.Name, subvCommit.ID)] = subvCommitInfo
					newCommitInfos[key(subvCommit.Repo.Name, subvCommit.ID)] = subvCommitInfo
					if err := onFix(fmt.Sprintf(
						"creating commit %s@%s which was missing, but referenced by %s@%s",
						subvCommit.Repo.Name, subvCommit.ID,
						ci.Commit.Repo.Name, ci.Commit.ID),
					); err != nil {
						return err
					}
				}
				if ci.Commit.ID == subvCommit.ID {
					contains = true
				}
				for _, subvProv := range subvCommitInfo.Provenance {
					if ci.Commit.Repo.Name == subvProv.Commit.Repo.Name &&
						ci.Commit.ID == subvProv.Commit.ID {
						contains = true
					}
				}

				if !contains {
					if err := onError(ErrProvenanceOfSubvenance{
						CommitInfo:     ci,
						SubvCommitInfo: subvCommitInfo,
					}); err != nil {
						return err
					}
				}

				if subvCommit.ID == subvRange.Lower.ID {
					break // check at the end of the loop so we fsck 'lower' too (inclusive range)
				}
				subvCommit = subvCommitInfo.ParentCommit
			}
		}
	}
	if fix {
		_, err := col.NewSTM(ctx, d.etcdClient, func(stm col.STM) error {
			for _, ci := range newCommitInfos {
				// We've observed users getting ErrExists from this create,
				// which doesn't make a lot of sense, but we insulate against
				// it anyways so it doesn't prevent the command from working.
				if err := d.commits(ci.Commit.Repo.Name).ReadWrite(stm).Create(ci.Commit.ID, ci); err != nil && !col.IsErrExists(err) {
					return err
				}
			}
			return nil
		})
		return err
	}
	return nil
}

func (d *driver) listRepo(pachClient *client.APIClient, includeAuth bool) (*pfs.ListRepoResponse, error) {
	ctx := pachClient.Ctx()
	repos := d.repos.ReadOnly(ctx)
	result := &pfs.ListRepoResponse{}
	authSeemsActive := true
	repoInfo := &pfs.RepoInfo{}
	if err := repos.List(repoInfo, col.DefaultOptions, func(repoName string) error {
		if repoName == ppsconsts.SpecRepo {
			return nil
		}
		if includeAuth && authSeemsActive {
			accessLevel, err := d.getAccessLevel(pachClient, repoInfo.Repo)
			if err == nil {
				repoInfo.AuthInfo = &pfs.RepoAuthInfo{AccessLevel: accessLevel}
			} else if auth.IsErrNotActivated(err) {
				authSeemsActive = false
			} else {
				return fmt.Errorf("error getting access level for \"%s\": %v",
					repoName, grpcutil.ScrubGRPC(err))
			}
		}
		result.RepoInfo = append(result.RepoInfo, proto.Clone(repoInfo).(*pfs.RepoInfo))
		return nil
	}); err != nil {
		return nil, err
	}
	return result, nil
}

func (d *driver) deleteRepo(txnCtx *txnenv.TransactionContext, repo *pfs.Repo, force bool) error {
	// Validate arguments
	if repo == nil {
		return errors.New("repo cannot be nil")
	}

	// TODO(msteffen): Fix d.deleteAll() so that it doesn't need to delete and
	// recreate the PPS spec repo, then uncomment this block to prevent users from
	// deleting it and breaking their cluster
	// if repo.Name == ppsconsts.SpecRepo {
	// 	return fmt.Errorf("cannot delete the special PPS repo %s", ppsconsts.SpecRepo)
	// }
	repos := d.repos.ReadWrite(txnCtx.Stm)

	// check if 'repo' is already gone. If so, return that error. Otherwise,
	// proceed with auth check (avoids awkward "access denied" error when calling
	// "deleteRepo" on a repo that's already gone)
	var existingRepoInfo pfs.RepoInfo
	err := repos.Get(repo.Name, &existingRepoInfo)
	if err != nil {
		if !col.IsErrNotFound(err) {
			return fmt.Errorf("error checking whether \"%s\" exists: %v",
				repo.Name, err)
		}
	}

	// Check if the caller is authorized to delete this repo
	if err := d.checkIsAuthorizedInTransaction(txnCtx, repo, auth.Scope_OWNER); err != nil {
		return err
	}

	repoInfo := new(pfs.RepoInfo)
	if err := repos.Get(repo.Name, repoInfo); err != nil {
		if !col.IsErrNotFound(err) {
			return fmt.Errorf("repos.Get: %v", err)
		}
	}

	// make a list of all the commits
	commits := d.commits(repo.Name).ReadOnly(txnCtx.ClientContext)
	commitInfos := make(map[string]*pfs.CommitInfo)
	commitInfo := &pfs.CommitInfo{}
	if err := commits.List(commitInfo, col.DefaultOptions, func(commitID string) error {
		commitInfos[commitID] = proto.Clone(commitInfo).(*pfs.CommitInfo)
		return nil
	}); err != nil {
		return err
	}

	visited := make(map[string]bool) // visitied upstream (provenant) commits
	// and then delete them while making sure that the subvenance of upstream commits gets updated
	for _, ci := range commitInfos {
		// Remove the deleted commit from the upstream commits' subvenance.
		for _, prov := range ci.Provenance {
			// Check if we've fixed prov already (or if it's in this repo and
			// doesn't need to be fixed
			if visited[prov.Commit.ID] || prov.Commit.Repo.Name == repo.Name {
				continue
			}
			// or if the repo has already been deleted
			ri := new(pfs.RepoInfo)
			if err := repos.Get(prov.Commit.Repo.Name, ri); err != nil {
				if !col.IsErrNotFound(err) {
					return fmt.Errorf("repo %v was not found: %v", prov.Commit.Repo.Name, err)
				}
				continue
			}
			visited[prov.Commit.ID] = true

			// fix prov's subvenance
			provCI := &pfs.CommitInfo{}
			if err := d.commits(prov.Commit.Repo.Name).ReadWrite(txnCtx.Stm).Update(prov.Commit.ID, provCI, func() error {
				subvTo := 0 // copy subvFrom to subvTo, excepting subv ranges to delete (so that they're overwritten)
			nextSubvRange:
				for subvFrom, subv := range provCI.Subvenance {
					// Compute path (of commit IDs) connecting subv.Upper to subv.Lower
					cur := subv.Upper.ID
					path := []string{cur}
					for cur != subv.Lower.ID {
						// Get CommitInfo for 'cur' from etcd
						// and traverse parent
						curInfo := &pfs.CommitInfo{}
						if err := d.commits(subv.Lower.Repo.Name).ReadWrite(txnCtx.Stm).Get(cur, curInfo); err != nil {
							return fmt.Errorf("error reading commitInfo for subvenant \"%s/%s\": %v", subv.Lower.Repo.Name, cur, err)
						}
						if curInfo.ParentCommit == nil {
							break
						}
						cur = curInfo.ParentCommit.ID
						path = append(path, cur)
					}

					// move 'subv.Upper' through parents until it points to a non-deleted commit
					for j := range path {
						if subv.Upper.Repo.Name != repo.Name {
							break
						}
						if j+1 >= len(path) {
							// All commits in subvRange are deleted. Remove entire Range
							// from provCI.Subvenance
							continue nextSubvRange
						}
						subv.Upper.ID = path[j+1]
					}

					// move 'subv.Lower' through children until it points to a non-deleted commit
					for j := len(path) - 1; j >= 0; j-- {
						if subv.Lower.Repo.Name != repo.Name {
							break
						}
						// We'll eventually get to a non-deleted commit because the
						// 'upper' block didn't exit
						subv.Lower.ID = path[j-1]
					}
					provCI.Subvenance[subvTo] = provCI.Subvenance[subvFrom]
					subvTo++
				}
				provCI.Subvenance = provCI.Subvenance[:subvTo]
				return nil
			}); err != nil {
				return fmt.Errorf("err fixing subvenance of upstream commit %s/%s: %v", prov.Commit.Repo.Name, prov.Commit.ID, err)
			}
		}
	}

	var branchInfos []*pfs.BranchInfo
	for _, branch := range repoInfo.Branches {
		bi, err := d.inspectBranch(txnCtx, branch)
		if err != nil {
			return fmt.Errorf("error inspecting branch %s: %v", branch, err)
		}
		branchInfos = append(branchInfos, bi)
	}
	// sort ascending provenance
	sort.Slice(branchInfos, func(i, j int) bool { return len(branchInfos[i].Provenance) < len(branchInfos[j].Provenance) })
	for i := range branchInfos {
		// delete branches from most provenance to least, that way if one
		// branch is provenant on another (such as with stats branches) we
		// delete them in the right order.
		branch := branchInfos[len(branchInfos)-1-i].Branch
		if err := d.deleteBranch(txnCtx, branch, force); err != nil {
			return fmt.Errorf("delete branch %s: %v", branch, err)
		}
	}
	// Despite the fact that we already deleted each branch with
	// deleteBranch, we also do branches.DeleteAll(), this insulates us
	// against certain corruption situations where the RepoInfo doesn't
	// exist in etcd but branches do.
	branches := d.branches(repo.Name).ReadWrite(txnCtx.Stm)
	branches.DeleteAll()
	// Similarly with commits
	commitsX := d.commits(repo.Name).ReadWrite(txnCtx.Stm)
	commitsX.DeleteAll()
	if err := repos.Delete(repo.Name); err != nil && !col.IsErrNotFound(err) {
		return fmt.Errorf("repos.Delete: %v", err)
	}

	if _, err = txnCtx.Auth().SetACLInTransaction(txnCtx, &auth.SetACLRequest{
		Repo: repo.Name, // NewACL is unset, so this will clear the acl for 'repo'
	}); err != nil && !auth.IsErrNotActivated(err) {
		return grpcutil.ScrubGRPC(err)
	}
	return nil
}

// ID can be passed in for transactions, which need to ensure the ID doesn't
// change after the commit ID has been reported to a client.
func (d *driver) startCommit(txnCtx *txnenv.TransactionContext, ID string, parent *pfs.Commit, branch string, provenance []*pfs.CommitProvenance, description string) (*pfs.Commit, error) {
	return d.makeCommit(txnCtx, ID, parent, branch, provenance, nil, nil, nil, nil, nil, description, 0)
}

func (d *driver) buildCommit(ctx context.Context, ID string, parent *pfs.Commit,
	branch string, provenance []*pfs.CommitProvenance,
	tree *pfs.Object, trees []*pfs.Object, datums *pfs.Object, sizeBytes uint64) (*pfs.Commit, error) {
	commit := &pfs.Commit{}
	err := d.txnEnv.WithWriteContext(ctx, func(txnCtx *txnenv.TransactionContext) error {
		var err error
		commit, err = d.makeCommit(txnCtx, ID, parent, branch, provenance, tree, trees, datums, nil, nil, "", sizeBytes)
		return err
	})
	return commit, err
}

// make commit makes a new commit in 'branch', with the parent 'parent' and the
// direct provenance 'provenance'. Note that
// - 'parent' must not be nil, but the only required field is 'parent.Repo'.
// - 'parent.ID' may be set to "", in which case the parent commit is inferred
//   from 'parent.Repo' and 'branch'.
// - If both 'parent.ID' and 'branch' are set, 'parent.ID' determines the parent
//   commit, but 'branch' is still moved to point at the new commit
//   to the new commit
// - If neither 'parent.ID' nor 'branch' are set, the new commit will have no
//   parent
// - If only 'parent.ID' is set, and it contains a branch, then the new commit's
//   parent will be the HEAD of that branch, but the branch will not be moved
func (d *driver) makeCommit(
	txnCtx *txnenv.TransactionContext,
	ID string,
	parent *pfs.Commit,
	branch string,
	provenance []*pfs.CommitProvenance,
	treeRef *pfs.Object,
	treesRefs []*pfs.Object,
	datumsRef *pfs.Object,
	recordFiles []string,
	records []*pfs.PutFileRecords,
	description string,
	sizeBytes uint64,
) (*pfs.Commit, error) {
	// Validate arguments:
	if parent == nil {
		return nil, fmt.Errorf("parent cannot be nil")
	}

	// Check that caller is authorized
	if err := d.checkIsAuthorizedInTransaction(txnCtx, parent.Repo, auth.Scope_WRITER); err != nil {
		return nil, err
	}

	// New commit and commitInfo
	newCommit := &pfs.Commit{
		Repo: parent.Repo,
		ID:   ID,
	}
	if newCommit.ID == "" {
		newCommit.ID = uuid.NewWithoutDashes()
	}
	newCommitInfo := &pfs.CommitInfo{
		Commit:      newCommit,
		Origin:      &pfs.CommitOrigin{Kind: pfs.OriginKind_USER},
		Started:     now(),
		Description: description,
	}
	if branch != "" {
		if err := ancestry.ValidateName(branch); err != nil {
			return nil, err
		}
	}

	// create the actual commit in etcd and update the branch + parent/child
	// Clone the parent, as this stm modifies it and might wind up getting
	// run more than once (if there's a conflict.)
	parent = proto.Clone(parent).(*pfs.Commit)
	repos := d.repos.ReadWrite(txnCtx.Stm)
	commits := d.commits(parent.Repo.Name).ReadWrite(txnCtx.Stm)
	branches := d.branches(parent.Repo.Name).ReadWrite(txnCtx.Stm)

	// Check if repo exists
	repoInfo := new(pfs.RepoInfo)
	if err := repos.Get(parent.Repo.Name, repoInfo); err != nil {
		return nil, err
	}

	// create/update 'branch' (if it was set) and set parent.ID (if, in
	// addition, 'parent.ID' was not set)
	key := path.Join
	branchProvMap := make(map[string]bool)
	if branch != "" {
		branchInfo := &pfs.BranchInfo{}
		if err := branches.Upsert(branch, branchInfo, func() error {
			// validate branch
			if parent.ID == "" && branchInfo.Head != nil {
				parent.ID = branchInfo.Head.ID
			}
			// include the branch and its provenance in the branch provenance map
			branchProvMap[key(newCommit.Repo.Name, branch)] = true
			for _, b := range branchInfo.Provenance {
				branchProvMap[key(b.Repo.Name, b.Name)] = true
			}
			if branchInfo.Head != nil {
				headCommitInfo := &pfs.CommitInfo{}
				if err := commits.Get(branchInfo.Head.ID, headCommitInfo); err != nil {
					return err
				}
				for _, prov := range headCommitInfo.Provenance {
					branchProvMap[key(prov.Branch.Repo.Name, prov.Branch.Name)] = true
				}
			}
			// Don't count the __spec__ repo towards the provenance count
			// since spouts will have __spec__ as provenance, but need to accept commits
			provenanceCount := len(branchInfo.Provenance)
			for _, p := range branchInfo.Provenance {
				if p.Repo.Name == ppsconsts.SpecRepo {
					provenanceCount--
					break
				}
			}

			// if the passed in provenance for the commit itself includes a spec
			// commit, (note the difference from the prev condition) then it was
			// created by pps, and so we want to allow it to commit to output branches
			hasSpec := false
			for _, prov := range provenance {
				if prov.Commit.Repo.Name == ppsconsts.SpecRepo {
					hasSpec = true
				}
			}

			if provenanceCount > 0 && treeRef == nil && !hasSpec {
				return fmt.Errorf("cannot start a commit on an output branch")
			}
			// Point 'branch' at the new commit
			branchInfo.Name = branch // set in case 'branch' is new
			branchInfo.Head = newCommit
			branchInfo.Branch = client.NewBranch(newCommit.Repo.Name, branch)
			return nil
		}); err != nil {
			return nil, err
		}
		// Add branch to repo (see "Update repoInfo" below)
		add(&repoInfo.Branches, branchInfo.Branch)
		// and add the branch to the commit info
		newCommitInfo.Branch = branchInfo.Branch
	}

	// 1. Write 'newCommit' to 'openCommits' collection OR
	// 2. Finish 'newCommit' (if treeRef != nil or records != nil); see
	//    "FinishCommit case" above)
	if treeRef != nil || treesRefs != nil || records != nil {
		if records != nil {
			parentTree, err := d.getTreeForCommit(txnCtx, parent)
			if err != nil {
				return nil, err
			}
			tree, err := parentTree.Copy()
			if err != nil {
				return nil, err
			}
			defer destroyHashtree(tree)
			for i, record := range records {
				if err := d.applyWrite(recordFiles[i], record, tree); err != nil {
					return nil, err
				}
			}
			if err := tree.Hash(); err != nil {
				return nil, err
			}
			treeRef, err = hashtree.PutHashTree(txnCtx.Client, tree)
			if err != nil {
				return nil, err
			}
			sizeBytes = uint64(tree.FSSize())
		}

		// now 'treeRef' is guaranteed to be set
		newCommitInfo.Tree = treeRef
		newCommitInfo.Trees = treesRefs
		newCommitInfo.Datums = datumsRef
		newCommitInfo.SizeBytes = sizeBytes
		newCommitInfo.Finished = now()

		// If we're updating the master branch, also update the repo size (see
		// "Update repoInfo" below)
		if branch == "master" {
			repoInfo.SizeBytes = newCommitInfo.SizeBytes
		}
	} else {
		if err := d.openCommits.ReadWrite(txnCtx.Stm).Put(newCommit.ID, newCommit); err != nil {
			return nil, err
		}
	}

	// Update repoInfo (potentially with new branch and new size)
	if err := repos.Put(parent.Repo.Name, repoInfo); err != nil {
		return nil, err
	}

	// Build newCommit's full provenance. B/c commitInfo.Provenance is a
	// transitive closure, there's no need to search the full provenance graph,
	// just take the union of the immediate parents' (in the 'provenance' arg)
	// commitInfo.Provenance
	newCommitProv := make(map[string]*pfs.CommitProvenance)
	for _, prov := range provenance {
		newCommitProv[prov.Commit.ID] = prov
		provCommitInfo, err := d.resolveCommit(txnCtx.Stm, prov.Commit)
		if err != nil {
			return nil, err
		}

		for _, c := range provCommitInfo.Provenance {
			newCommitProv[c.Commit.ID] = c
		}
	}

	// keep track of which branches are represented in the commit provenance
	provenantBranches := make(map[string]bool)
	// Copy newCommitProv into newCommitInfo.Provenance, and update upstream subv
	for _, prov := range newCommitProv {
		// resolve the provenance
		var err error
		prov, err = d.resolveCommitProvenance(txnCtx.Stm, prov)
		if err != nil {
			return nil, err
		}
		// there should only be one representative of each branch in the commit provenance
		if _, ok := provenantBranches[key(prov.Branch.Repo.Name, prov.Branch.Name)]; ok {
			return nil, fmt.Errorf("the commit provenance contains multiple commits from the same branch")
		}
		provenantBranches[key(prov.Branch.Repo.Name, prov.Branch.Name)] = true

		// ensure the commit provenance is consistent with the branch provenance
		if len(branchProvMap) != 0 {
			if prov.Branch.Repo.Name != ppsconsts.SpecRepo && !branchProvMap[key(prov.Branch.Repo.Name, prov.Branch.Name)] {
				return nil, fmt.Errorf("the commit provenance contains a branch which the branch is not provenant on")
			}
		}

		newCommitInfo.Provenance = append(newCommitInfo.Provenance, prov)
		provCommitInfo := &pfs.CommitInfo{}
		if err := d.commits(prov.Commit.Repo.Name).ReadWrite(txnCtx.Stm).Update(prov.Commit.ID, provCommitInfo, func() error {
			appendSubvenance(provCommitInfo, newCommitInfo)
			return nil
		}); err != nil {
			return nil, err
		}
	}

	// Set newCommit.ParentCommit (if 'parent' and/or 'branch' was set) and add
	// newCommit to parent's ChildCommits
	if parent.ID != "" {
		// Resolve parent.ID if it's a branch that isn't 'branch' (which can
		// happen if 'branch' is new and diverges from the existing branch in
		// 'parent.ID')
		parentCommitInfo, err := d.resolveCommit(txnCtx.Stm, parent)
		if err != nil {
			return nil, fmt.Errorf("parent commit not found: %v", err)
		}
		// fail if the parent commit has not been finished
		if parentCommitInfo.Finished == nil {
			return nil, fmt.Errorf("parent commit %s has not been finished", parent.ID)
		}
		if err := commits.Update(parent.ID, parentCommitInfo, func() error {
			newCommitInfo.ParentCommit = parent
			// If we don't know the branch the commit belongs to at this point, assume it is the same as the parent branch
			if newCommitInfo.Branch == nil {
				newCommitInfo.Branch = parentCommitInfo.Branch
			}
			parentCommitInfo.ChildCommits = append(parentCommitInfo.ChildCommits, newCommit)
			return nil
		}); err != nil {
			// Note: error is emitted if parent.ID is a missing/invalid branch OR a
			// missing/invalid commit ID
			return nil, fmt.Errorf("could not resolve parent commit \"%s\": %v", parent.ID, err)
		}
	}

	// Finally, create the commit
	if err := commits.Create(newCommit.ID, newCommitInfo); err != nil {
		return nil, err
	}
	// Defer propagation of the commit until the end of the transaction so we can
	// batch downstream commits together if there are multiple changes.
	if branch != "" {
		if err := txnCtx.PropagateCommit(client.NewBranch(newCommit.Repo.Name, branch), true); err != nil {
			return nil, err
		}
	}

	return newCommit, nil
}

func (d *driver) finishCommit(txnCtx *txnenv.TransactionContext, commit *pfs.Commit, tree *pfs.Object, empty bool, description string) (retErr error) {
	// Validate arguments
	if commit == nil {
		return errors.New("commit cannot be nil")
	}
	if commit.Repo == nil {
		return errors.New("commit repo cannot be nil")
	}

	if err := d.checkIsAuthorizedInTransaction(txnCtx, commit.Repo, auth.Scope_WRITER); err != nil {
		return err
	}
	commitInfo, err := d.resolveCommit(txnCtx.Stm, commit)
	if err != nil {
		return err
	}
	if commitInfo.Finished != nil {
		return pfsserver.ErrCommitFinished{commit}
	}
	if description != "" {
		commitInfo.Description = description
	}

	var parentTree, finishedTree hashtree.HashTree
	if !empty {
		// Retrieve the parent commit's tree (to apply writes from etcd or just
		// compute the size change). If parentCommit.Tree == nil, walk up the branch
		// until we find a successful commit. Otherwise, require that the immediate
		// parent of 'commitInfo' is closed, as we use its contents
		parentCommit := commitInfo.ParentCommit
		for parentCommit != nil {
			parentCommitInfo, err := d.resolveCommit(txnCtx.Stm, parentCommit)
			if err != nil {
				return err
			}
			if parentCommitInfo.Tree != nil {
				break
			}
			parentCommit = parentCommitInfo.ParentCommit
		}
		parentTree, err = d.getTreeForCommit(txnCtx, parentCommit) // result is empty if parentCommit == nil
		if err != nil {
			return err
		}

		defer func() {
			if finishedTree != nil {
				destroyHashtree(finishedTree)
			}
		}()

		if tree == nil {
			var err error
			finishedTree, err = d.getTreeForOpenCommit(txnCtx.Client, &pfs.File{Commit: commit}, parentTree)
			if err != nil {
				return err
			}
			// Put the tree to object storage.
			treeRef, err := hashtree.PutHashTree(txnCtx.Client, finishedTree)
			if err != nil {
				return err
			}
			commitInfo.Tree = treeRef
		} else {
			var err error
			finishedTree, err = hashtree.GetHashTreeObject(txnCtx.Client, d.storageRoot, tree)
			if err != nil {
				return err
			}
			commitInfo.Tree = tree
		}

		commitInfo.SizeBytes = uint64(finishedTree.FSSize())
	}
	commitInfo.Finished = now()
	if err := d.updateProvenanceProgress(txnCtx, !empty, commitInfo); err != nil {
		return err
	}
	return d.writeFinishedCommit(txnCtx.Stm, commit, commitInfo)
}

func (d *driver) finishOutputCommit(txnCtx *txnenv.TransactionContext, commit *pfs.Commit, trees []*pfs.Object, datums *pfs.Object, size uint64) (retErr error) {
	if commit == nil {
		return fmt.Errorf("cannot finish a nil commit")
	}
	if err := d.checkIsAuthorizedInTransaction(txnCtx, commit.Repo, auth.Scope_WRITER); err != nil {
		return err
	}
	commitInfo, err := d.resolveCommit(txnCtx.Stm, commit)
	if err != nil {
		return err
	}
	if commitInfo.Finished != nil {
		return fmt.Errorf("commit %s has already been finished", commit.FullID())
	}
	commitInfo.Trees = trees
	commitInfo.Datums = datums
	commitInfo.SizeBytes = size
	commitInfo.Finished = now()
	if err := d.updateProvenanceProgress(txnCtx, true, commitInfo); err != nil {
		return err
	}
	return d.writeFinishedCommit(txnCtx.Stm, commit, commitInfo)
}

func (d *driver) updateProvenanceProgress(txnCtx *txnenv.TransactionContext, success bool, ci *pfs.CommitInfo) error {
	for _, provC := range ci.Provenance {
		provCi := &pfs.CommitInfo{}
		if err := d.commits(provC.Commit.Repo.Name).ReadWrite(txnCtx.Stm).Update(provC.Commit.ID, provCi, func() error {
			if success {
				provCi.SubvenantCommitsSuccess++
			} else {
				provCi.SubvenantCommitsFailure++
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

// writeFinishedCommit writes these changes to etcd:
// 1) it closes the input commit (i.e., it writes any changes made to it and
//    removes it from the open commits)
// 2) if the commit is the new HEAD of master, it updates the repo size
func (d *driver) writeFinishedCommit(stm col.STM, commit *pfs.Commit, commitInfo *pfs.CommitInfo) error {
	commits := d.commits(commit.Repo.Name).ReadWrite(stm)
	if err := commits.Put(commit.ID, commitInfo); err != nil {
		return err
	}
	if err := d.openCommits.ReadWrite(stm).Delete(commit.ID); err != nil {
		return fmt.Errorf("could not confirm that commit %s is open; this is likely a bug. err: %v", commit.ID, err)
	}
	// update the repo size if this is the head of master
	repos := d.repos.ReadWrite(stm)
	repoInfo := new(pfs.RepoInfo)
	if err := repos.Get(commit.Repo.Name, repoInfo); err != nil {
		return err
	}
	for _, branch := range repoInfo.Branches {
		if branch.Name == "master" {
			branchInfo := &pfs.BranchInfo{}
			if err := d.branches(commit.Repo.Name).ReadWrite(stm).Get(branch.Name, branchInfo); err != nil {
				return err
			}
			// If the head commit of master has been deleted, we could get here if another branch
			// had shared its head commit with master, and then we created a new commit on that branch
			if branchInfo.Head != nil && branchInfo.Head.ID == commit.ID {
				repoInfo.SizeBytes = commitInfo.SizeBytes
				if err := repos.Put(commit.Repo.Name, repoInfo); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// propagateCommits selectively starts commits in or downstream of 'branches' in
// order to restore the invariant that branch provenance matches HEAD commit
// provenance:
//   B.Head is provenant on A.Head <=>
//   branch B is provenant on branch A and A.Head != nil
// The implementation assumes that the invariant already holds for all branches
// upstream of 'branches', but not necessarily for each 'branch' itself. Despite
// the name, 'branches' do not need a HEAD commit to propagate, though one may be
// created.
//
// In other words, propagateCommits scans all branches b_downstream that are
// equal to or downstream of 'branches', and if the HEAD of b_downstream isn't
// provenant on the HEADs of b_downstream's provenance, propagateCommits starts
// a new HEAD commit in b_downstream that is. For example, propagateCommits
// starts downstream output commits (which trigger PPS jobs) when new input
// commits arrive on 'branch', when 'branches's HEAD is deleted, or when
// 'branches' are newly created (i.e. in CreatePipeline).
//
// The isNewCommit flag indicates whether propagateCommits was called during the creation of a new commit.
func (d *driver) propagateCommits(stm col.STM, branches []*pfs.Branch, isNewCommit bool) error {
	key := path.Join
	// subvBIMap = ( ⋃{b.subvenance | b ∈ branches} ) ∪ branches
	subvBIMap := map[string]*pfs.BranchInfo{}
	for _, branch := range branches {
		branchInfo, ok := subvBIMap[key(branch.Repo.Name, branch.Name)]
		if !ok {
			branchInfo = &pfs.BranchInfo{}
			if err := d.branches(branch.Repo.Name).ReadWrite(stm).Get(branch.Name, branchInfo); err != nil {
				return err
			}
			subvBIMap[key(branch.Repo.Name, branch.Name)] = branchInfo
		}
		for _, subvBranch := range branchInfo.Subvenance {
			_, ok := subvBIMap[key(subvBranch.Repo.Name, subvBranch.Name)]
			if !ok {
				subvInfo := &pfs.BranchInfo{}
				if err := d.branches(subvBranch.Repo.Name).ReadWrite(stm).Get(subvBranch.Name, subvInfo); err != nil {
					return err
				}
				subvBIMap[key(subvBranch.Repo.Name, subvBranch.Name)] = subvInfo
			}
		}
	}

	// 'subvBIs' is the collection of downstream branches that may get a new
	// commit. Populate subvBIs and sort it so that upstream branches are
	// processed before their descendants (this guarantees that if branch B is
	// provenant on branch A, we create a new commit in A before creating a new
	// commit in B provenant on the new HEAD of A).
	var subvBIs []*pfs.BranchInfo
	for _, branchData := range subvBIMap {
		subvBIs = append(subvBIs, branchData)
	}
	sort.Slice(subvBIs, func(i, j int) bool {
		return len(subvBIs[i].Provenance) < len(subvBIs[j].Provenance)
	})

	// Iterate through downstream branches and determine which need a new commit.
nextSubvBI:
	for _, subvBI := range subvBIs {
		subvB := subvBI.Branch
		stmCommits := d.commits(subvB.Repo.Name).ReadWrite(stm)
		stmBranches := d.branches(subvB.Repo.Name).ReadWrite(stm)

		// Compute the full provenance of hypothetical new output commit to decide
		// if we need it.
		newCommitProvMap := make(map[string]*pfs.CommitProvenance)
		for _, provOfSubvB := range subvBI.Provenance {
			// get the branch info from the provenance branch
			provOfSubvBI := &pfs.BranchInfo{}
			if err := d.branches(provOfSubvB.Repo.Name).ReadWrite(stm).Get(provOfSubvB.Name, provOfSubvBI); err != nil && !col.IsErrNotFound(err) {
				return fmt.Errorf("could not read branch %s/%s: %v", provOfSubvB.Repo.Name, provOfSubvB.Name, err)
			}
			// if provOfSubvB has no head commit, then it doesn't contribute to newCommit
			if provOfSubvBI.Head == nil {
				continue
			}
			// - Add provOfSubvBI.Head to the new commit's provenance
			// - Since we want the new commit's provenance to be a transitive closure,
			//   we add provOfSubvBI.Head's *provenance* to newCommit's provenance.
			//   - Note: In most cases, every commit in there will be the Head of some
			//     other provOfSubvBI, but not when e.g. deferred downstream
			//     processing, where an upstream branch has no branch provenance but
			//     its head commit has commit provenance.
			// - We need to key on both the commit id and the branch name, so that
			//   branches with a shared commit are both represented in the provenance
			newCommitProvMap[key(provOfSubvBI.Head.ID, provOfSubvB.Name)] = &pfs.CommitProvenance{
				Commit: provOfSubvBI.Head,
				Branch: provOfSubvB,
			}
			provOfSubvBHeadInfo := &pfs.CommitInfo{}
			if err := d.commits(provOfSubvB.Repo.Name).ReadWrite(stm).Get(provOfSubvBI.Head.ID, provOfSubvBHeadInfo); err != nil {
				return err
			}
			for _, provProv := range provOfSubvBHeadInfo.Provenance {
				newProvProv, err := d.resolveCommitProvenance(stm, provProv)
				if err != nil {
					return fmt.Errorf("could not resolve provenant commit %s@%s (%s): %v",
						provProv.Commit.Repo.Name, provProv.Commit.ID, provProv.Branch.Name, err)
				}
				provProv = newProvProv
				newCommitProvMap[key(provProv.Commit.ID, provProv.Branch.Name)] = provProv
			}
		}
		if len(newCommitProvMap) == 0 {
			// no input commits to process; don't create a new output commit
			continue nextSubvBI
		}

		// 'subvB' may already have a HEAD commit, so compute whether the new output
		// commit's provenance would be a subset of the existing HEAD commit's
		// provenance. If so, a new output commit would be a duplicate, so don't
		// create it.
		if subvBI.Head != nil {
			// get the info for subvB's HEAD commit
			subvBHeadInfo := &pfs.CommitInfo{}
			if err := stmCommits.Get(subvBI.Head.ID, subvBHeadInfo); err != nil {
				return pfsserver.ErrCommitNotFound{subvBI.Head}
			}
			provIntersection := make(map[string]struct{})
			for _, p := range subvBHeadInfo.Provenance {
				if _, ok := newCommitProvMap[key(p.Commit.ID, p.Branch.Name)]; ok {
					provIntersection[key(p.Commit.ID, p.Branch.Name)] = struct{}{}
				}
			}
			if len(newCommitProvMap) == len(provIntersection) {
				// newCommit's provenance is subset of existing HEAD's provenance
				continue nextSubvBI
			}
		}

		// If the only branches in the hypothetical output commit's provenance are
		// in the 'spec' repo, creating it would mean creating a confusing "dummy"
		// job with no non-spec input data. If this is the case, don't create a new
		// output commit
		allSpec := true
		for _, p := range newCommitProvMap {
			if p.Branch.Repo.Name != ppsconsts.SpecRepo {
				allSpec = false
				break
			}
		}
		if allSpec {
			// Only input data is PipelineInfo; don't create new output commit
			continue nextSubvBI
		}

		// if a commit was just created and this is the same branch as the one being
		// propagated, we don't need to do anything
		if isNewCommit && len(branches) == 1 &&
			branches[0].Repo.Name == subvB.Repo.Name && branches[0].Name == subvB.Name {
			continue nextSubvBI
		}

		// *All checks passed* start a new output commit in 'subvB'
		newCommit := &pfs.Commit{
			Repo: subvB.Repo,
			ID:   uuid.NewWithoutDashes(),
		}
		newCommitInfo := &pfs.CommitInfo{
			Commit:  newCommit,
			Origin:  &pfs.CommitOrigin{Kind: pfs.OriginKind_AUTO},
			Started: now(),
		}

		// Set 'newCommit's ParentCommit, 'branch.Head's ChildCommits and 'branch.Head'
		newCommitInfo.ParentCommit = subvBI.Head
		if subvBI.Head != nil {
			parentCommitInfo := &pfs.CommitInfo{}
			if err := stmCommits.Update(newCommitInfo.ParentCommit.ID, parentCommitInfo, func() error {
				parentCommitInfo.ChildCommits = append(parentCommitInfo.ChildCommits, newCommit)
				return nil
			}); err != nil {
				return err
			}
		}
		subvBI.Head = newCommit
		newCommitInfo.Branch = subvB
		if err := stmBranches.Put(subvB.Name, subvBI); err != nil {
			return err
		}

		// Set provenance and upstream subvenance (appendSubvenance needs
		// newCommitInfo.ParentCommit to extend the correct subvenance range)
		for _, prov := range newCommitProvMap {
			// set provenance of 'newCommit'
			newCommitInfo.Provenance = append(newCommitInfo.Provenance, prov)

			// update subvenance of 'prov'
			provCommitInfo := &pfs.CommitInfo{}
			if err := d.commits(prov.Commit.Repo.Name).ReadWrite(stm).Update(prov.Commit.ID, provCommitInfo, func() error {
				appendSubvenance(provCommitInfo, newCommitInfo)
				return nil
			}); err != nil {
				return err
			}
		}

		// finally create open 'commit'
		if err := stmCommits.Create(newCommit.ID, newCommitInfo); err != nil {
			return err
		}
		if err := d.openCommits.ReadWrite(stm).Put(newCommit.ID, newCommit); err != nil {
			return err
		}
	}
	return nil
}

// inspectCommit takes a Commit and returns the corresponding CommitInfo.
//
// As a side effect, this function also replaces the ID in the given commit
// with a real commit ID.
func (d *driver) inspectCommit(pachClient *client.APIClient, commit *pfs.Commit, blockState pfs.CommitState) (*pfs.CommitInfo, error) {
	ctx := pachClient.Ctx()
	if commit == nil {
		return nil, fmt.Errorf("cannot inspect nil commit")
	}
	if err := d.checkIsAuthorized(pachClient, commit.Repo, auth.Scope_READER); err != nil {
		return nil, err
	}

	// Check if the commitID is a branch name
	var commitInfo *pfs.CommitInfo
	if err := col.NewDryrunSTM(ctx, d.etcdClient, func(stm col.STM) error {
		var err error
		commitInfo, err = d.resolveCommit(stm, commit)
		return err
	}); err != nil {
		return nil, err
	}

	commits := d.commits(commit.Repo.Name).ReadOnly(ctx)
	if blockState == pfs.CommitState_READY {
		// Wait for each provenant commit to be finished
		for _, p := range commitInfo.Provenance {
			d.inspectCommit(pachClient, p.Commit, pfs.CommitState_FINISHED)
		}
	}
	if blockState == pfs.CommitState_FINISHED {
		// Watch the CommitInfo until the commit has been finished
		if err := func() error {
			commitInfoWatcher, err := commits.WatchOne(commit.ID)
			if err != nil {
				return err
			}
			defer commitInfoWatcher.Close()
			for {
				var commitID string
				_commitInfo := new(pfs.CommitInfo)
				event := <-commitInfoWatcher.Watch()
				switch event.Type {
				case watch.EventError:
					return event.Err
				case watch.EventPut:
					if err := event.Unmarshal(&commitID, _commitInfo); err != nil {
						return fmt.Errorf("unmarshal: %v", err)
					}
				case watch.EventDelete:
					return pfsserver.ErrCommitDeleted{commit}
				}
				if _commitInfo.Finished != nil {
					commitInfo = _commitInfo
					break
				}
			}
			return nil
		}(); err != nil {
			return nil, err
		}
	}
	return commitInfo, nil
}

// resolveCommit contains the essential implementation of inspectCommit: it converts 'commit' (which may
// be a commit ID or branch reference, plus '~' and/or '^') to a repo + commit
// ID. It accepts an STM so that it can be used in a transaction and avoids an
// inconsistent call to d.inspectCommit()
func (d *driver) resolveCommit(stm col.STM, userCommit *pfs.Commit) (*pfs.CommitInfo, error) {
	if userCommit == nil {
		return nil, fmt.Errorf("cannot resolve nil commit")
	}
	if userCommit.ID == "" {
		return nil, fmt.Errorf("cannot resolve commit with no ID or branch")
	}
	commit := proto.Clone(userCommit).(*pfs.Commit) // back up user commit, for error reporting
	// Extract any ancestor tokens from 'commit.ID' (i.e. ~, ^ and .)
	var ancestryLength int
	var err error
	commit.ID, ancestryLength, err = ancestry.Parse(commit.ID)
	if err != nil {
		return nil, err
	}

	// Keep track of the commit branch, in case it isn't set in the commitInfo already
	var commitBranch *pfs.Branch
	// Check if commit.ID is already a commit ID (i.e. a UUID).
	if !uuid.IsUUIDWithoutDashes(commit.ID) {
		branches := d.branches(commit.Repo.Name).ReadWrite(stm)
		branchInfo := &pfs.BranchInfo{}
		// See if we are given a branch
		if err := branches.Get(commit.ID, branchInfo); err != nil {
			return nil, err
		}
		if branchInfo.Head == nil {
			return nil, pfsserver.ErrNoHead{branchInfo.Branch}
		}
		commitBranch = branchInfo.Branch
		commit.ID = branchInfo.Head.ID
	}

	// Traverse commits' parents until you've reached the right ancestor
	commits := d.commits(commit.Repo.Name).ReadWrite(stm)
	commitInfo := &pfs.CommitInfo{}
	if ancestryLength >= 0 {
		for i := 0; i <= ancestryLength; i++ {
			if commit == nil {
				return nil, pfsserver.ErrCommitNotFound{userCommit}
			}
			if err := commits.Get(commit.ID, commitInfo); err != nil {
				if col.IsErrNotFound(err) {
					if i == 0 {
						return nil, pfsserver.ErrCommitNotFound{userCommit}
					}
					return nil, pfsserver.ErrParentCommitNotFound{commit}
				}
				return nil, err
			}
			commit = commitInfo.ParentCommit
		}
	} else {
		cis := make([]pfs.CommitInfo, ancestryLength*-1)
		for i := 0; ; i++ {
			if commit == nil {
				if i >= len(cis) {
					commitInfo = &cis[i%len(cis)]
					break
				}
				return nil, pfsserver.ErrCommitNotFound{userCommit}
			}
			if err := commits.Get(commit.ID, &cis[i%len(cis)]); err != nil {
				if col.IsErrNotFound(err) {
					if i == 0 {
						return nil, pfsserver.ErrCommitNotFound{userCommit}
					}
					return nil, pfsserver.ErrParentCommitNotFound{commit}
				}
			}
			commit = cis[i%len(cis)].ParentCommit
		}
	}
	if commitInfo.Branch == nil {
		commitInfo.Branch = commitBranch
	}
	userCommit.ID = commitInfo.Commit.ID
	return commitInfo, nil
}

func (d *driver) listCommit(pachClient *client.APIClient, repo *pfs.Repo,
	to *pfs.Commit, from *pfs.Commit, number uint64, reverse bool) ([]*pfs.CommitInfo, error) {
	var result []*pfs.CommitInfo
	if err := d.listCommitF(pachClient, repo, to, from, number, reverse, func(ci *pfs.CommitInfo) error {
		result = append(result, ci)
		return nil
	}); err != nil {
		return nil, err
	}
	return result, nil
}

func (d *driver) listCommitF(pachClient *client.APIClient, repo *pfs.Repo,
	to *pfs.Commit, from *pfs.Commit, number uint64, reverse bool, f func(*pfs.CommitInfo) error) error {
	// Validate arguments
	if repo == nil {
		return errors.New("repo cannot be nil")
	}

	ctx := pachClient.Ctx()
	if err := d.checkIsAuthorized(pachClient, repo, auth.Scope_READER); err != nil {
		return err
	}
	if from != nil && from.Repo.Name != repo.Name || to != nil && to.Repo.Name != repo.Name {
		return fmt.Errorf("`from` and `to` commits need to be from repo %s", repo.Name)
	}

	// Make sure that the repo exists
	if repo.Name != "" {
		err := d.txnEnv.WithReadContext(ctx, func(txnCtx *txnenv.TransactionContext) error {
			_, err := d.inspectRepo(txnCtx, repo, !includeAuth)
			return err
		})
		if err != nil {
			return err
		}
	}

	// Make sure that both from and to are valid commits
	if from != nil {
		_, err := d.inspectCommit(pachClient, from, pfs.CommitState_STARTED)
		if err != nil {
			return err
		}
	}
	if to != nil {
		_, err := d.inspectCommit(pachClient, to, pfs.CommitState_STARTED)
		if err != nil {
			if isNoHeadErr(err) {
				return nil
			}
			return err
		}
	}

	// if number is 0, we return all commits that match the criteria
	if number == 0 {
		number = math.MaxUint64
	}
	commits := d.commits(repo.Name).ReadOnly(ctx)
	ci := &pfs.CommitInfo{}

	if from != nil && to == nil {
		return fmt.Errorf("cannot use `from` commit without `to` commit")
	} else if from == nil && to == nil {
		// if neither from and to is given, we list all commits in
		// the repo, sorted by revision timestamp (or reversed if so requested.)
		opts := *col.DefaultOptions // Note we dereference here so as to make a copy
		if reverse {
			opts.Order = etcd.SortAscend
		}
		// we hold onto a revisions worth of cis so that we can sort them by provenance
		var cis []*pfs.CommitInfo
		// sendCis sorts cis and passes them to f
		sendCis := func() error {
			// Sort in reverse provenance order, i.e. commits come before their provenance
			sort.Slice(cis, func(i, j int) bool { return len(cis[i].Provenance) > len(cis[j].Provenance) })
			for i, ci := range cis {
				if number == 0 {
					return errutil.ErrBreak
				}
				number--

				if reverse {
					ci = cis[len(cis)-1-i]
				}
				if err := f(ci); err != nil {
					return err
				}
			}
			cis = nil
			return nil
		}
		lastRev := int64(-1)
		if err := commits.ListRev(ci, &opts, func(commitID string, createRev int64) error {
			if createRev != lastRev {
				if err := sendCis(); err != nil {
					return err
				}
				lastRev = createRev
			}
			cis = append(cis, proto.Clone(ci).(*pfs.CommitInfo))
			return nil
		}); err != nil {
			return err
		}
		// Call sendCis one last time to send whatever's pending in 'cis'
		if err := sendCis(); err != nil {
			return err
		}
	} else {
		if reverse {
			return fmt.Errorf("cannot use 'Reverse' while also using 'From' or 'To'")
		}
		cursor := to
		for number != 0 && cursor != nil && (from == nil || cursor.ID != from.ID) {
			var commitInfo pfs.CommitInfo
			if err := commits.Get(cursor.ID, &commitInfo); err != nil {
				return err
			}
			if err := f(&commitInfo); err != nil {
				if err == errutil.ErrBreak {
					return nil
				}
				return err
			}
			cursor = commitInfo.ParentCommit
			number--
		}
	}
	return nil
}

func (d *driver) subscribeCommit(pachClient *client.APIClient, repo *pfs.Repo, branch string, prov *pfs.CommitProvenance,
	from *pfs.Commit, state pfs.CommitState, f func(*pfs.CommitInfo) error) error {
	// Validate arguments
	if repo == nil {
		return errors.New("repo cannot be nil")
	}
	if from != nil && from.Repo.Name != repo.Name {
		return fmt.Errorf("the `from` commit needs to be from repo %s", repo.Name)
	}

	commits := d.commits(repo.Name).ReadOnly(pachClient.Ctx())
	newCommitWatcher, err := commits.Watch(watch.WithSort(etcd.SortByCreateRevision, etcd.SortAscend))
	if err != nil {
		return err
	}
	defer newCommitWatcher.Close()
	// keep track of the commits that have been sent
	seen := make(map[string]bool)
	for {
		var commitID string
		commitInfo := &pfs.CommitInfo{}
		var event *watch.Event
		var ok bool
		event, ok = <-newCommitWatcher.Watch()
		if !ok {
			return nil
		}
		switch event.Type {
		case watch.EventError:
			return event.Err
		case watch.EventPut:
			if err := event.Unmarshal(&commitID, commitInfo); err != nil {
				return fmt.Errorf("unmarshal: %v", err)
			}
			if commitInfo == nil {
				return fmt.Errorf("commit info is empty for id: %v", commitID)
			}

			// if provenance is provided, ensure that the returned commits have the commit in their provenance
			if prov != nil {
				valid := false
				for _, cProv := range commitInfo.Provenance {
					valid = valid || proto.Equal(cProv, prov)
				}
				if !valid {
					continue
				}
			}

			if commitInfo.Branch != nil {
				// if branch is provided, make sure the commit was created on that branch
				if branch != "" && commitInfo.Branch.Name != branch {
					continue
				}
				// For now, we don't want stats branches to have jobs triggered on them
				// and this is the simplest way to achieve that. Once we have labels,
				// we'll use those instead for a more principled approach.
				if commitInfo.Branch.Name == "stats" {
					continue
				}
			}

			// We don't want to include the `from` commit itself
			if !(seen[commitID] || (from != nil && from.ID == commitID)) {
				commitInfo, err := d.inspectCommit(pachClient, client.NewCommit(repo.Name, commitID), state)
				if err != nil {
					return err
				}
				if err := f(commitInfo); err != nil {
					return err
				}
				seen[commitInfo.Commit.ID] = true
			}
		case watch.EventDelete:
			continue
		}
	}
}

func (d *driver) flushCommit(pachClient *client.APIClient, fromCommits []*pfs.Commit, toRepos []*pfs.Repo, f func(*pfs.CommitInfo) error) error {
	if len(fromCommits) == 0 {
		return fmt.Errorf("fromCommits cannot be empty")
	}

	// First compute intersection of the fromCommits subvenant commits, those
	// are the commits we're interested in. Iterate over all commits and keep a
	// running intersection (in commitsToWatch) of the subvenance of all commits
	// processed so far
	commitsToWatch := make(map[string]*pfs.Commit)
	for i, commit := range fromCommits {
		commitInfo, err := d.inspectCommit(pachClient, commit, pfs.CommitState_STARTED)
		if err != nil {
			return err
		}
		if i == 0 {
			for _, subvCommit := range commitInfo.Subvenance {
				commitsToWatch[commitKey(subvCommit.Upper)] = subvCommit.Upper
			}
		} else {
			newCommitsToWatch := make(map[string]*pfs.Commit)
			for _, subvCommit := range commitInfo.Subvenance {
				if _, ok := commitsToWatch[commitKey(subvCommit.Upper)]; ok {
					newCommitsToWatch[commitKey(subvCommit.Upper)] = subvCommit.Upper
				}
			}
			commitsToWatch = newCommitsToWatch
		}
	}

	// Compute a map of repos we're flushing to.
	toRepoMap := make(map[string]*pfs.Repo)
	for _, toRepo := range toRepos {
		toRepoMap[toRepo.Name] = toRepo
	}

	// Wait for each of the commitsToWatch to be finished.
	for _, commitToWatch := range commitsToWatch {
		if len(toRepoMap) > 0 {
			if _, ok := toRepoMap[commitToWatch.Repo.Name]; !ok {
				continue
			}
		}
		finishedCommitInfo, err := d.inspectCommit(pachClient, commitToWatch, pfs.CommitState_FINISHED)
		if err != nil {
			if _, ok := err.(pfsserver.ErrCommitNotFound); ok {
				continue // just skip this
			} else if auth.IsErrNotAuthorized(err) {
				continue // again, just skip (we can't wait on commits we can't access)
			}
			return err
		}
		if err := f(finishedCommitInfo); err != nil {
			return err
		}
	}
	// Now wait for the root commits to finish. These are not passed to `f`
	// because it's expecting to just get downstream commits.
	for _, commit := range fromCommits {
		_, err := d.inspectCommit(pachClient, commit, pfs.CommitState_FINISHED)
		if err != nil {
			if _, ok := err.(pfsserver.ErrCommitNotFound); ok {
				continue // just skip this
			}
			return err
		}
	}

	return nil
}

func (d *driver) deleteCommit(txnCtx *txnenv.TransactionContext, userCommit *pfs.Commit) error {
	// Validate arguments
	if userCommit == nil {
		return errors.New("commit cannot be nil")
	}
	if userCommit.Repo == nil {
		return errors.New("commit repo cannot be nil")
	}

	if err := d.checkIsAuthorizedInTransaction(txnCtx, userCommit.Repo, auth.Scope_WRITER); err != nil {
		return err
	}
	// Main txn: Delete all downstream commits, and update subvenance of upstream commits
	// TODO update branches inside this txn, by storing a repo's branches in its
	// RepoInfo or its HEAD commit
	deleted := make(map[string]*pfs.CommitInfo) // deleted commits
	affectedRepos := make(map[string]struct{})  // repos containing deleted commits

	// 1) re-read CommitInfo inside txn
	userCommitInfo, err := d.resolveCommit(txnCtx.Stm, userCommit)
	if err != nil {
		return fmt.Errorf("resolveCommit: %v", err)
	}

	// 2) Define helper for deleting commits. 'lower' corresponds to
	// pfs.CommitRange.Lower, and is an ancestor of 'upper'
	deleteCommit := func(lower, upper *pfs.Commit) error {
		// Validate arguments
		if lower.Repo.Name != upper.Repo.Name {
			return fmt.Errorf("cannot delete commit range with mismatched repos \"%s\" and \"%s\"", lower.Repo.Name, upper.Repo.Name)
		}
		affectedRepos[lower.Repo.Name] = struct{}{}
		commits := d.commits(lower.Repo.Name).ReadWrite(txnCtx.Stm)

		// delete commits on path upper -> ... -> lower (traverse ParentCommits)
		commit := upper
		for {
			if commit == nil {
				return fmt.Errorf("encountered nil parent commit in %s/%s...%s", lower.Repo.Name, lower.ID, upper.ID)
			}
			// Store commitInfo in 'deleted' and remove commit from etcd
			commitInfo := &pfs.CommitInfo{}
			if err := commits.Get(commit.ID, commitInfo); err != nil {
				return err
			}
			// If a commit has already been deleted, we don't want to overwrite the existing information, since commitInfo will be nil
			if _, ok := deleted[commit.ID]; !ok {
				deleted[commit.ID] = commitInfo
			}
			if err := commits.Delete(commit.ID); err != nil {
				return err
			}
			if commit.ID == lower.ID {
				break // check after deletion so we delete 'lower' (inclusive range)
			}
			commit = commitInfo.ParentCommit
		}
		return nil
	}

	// 3) Validate the commit (check that it has no provenance) and delete it
	if provenantOnInput(userCommitInfo.Provenance) {
		return fmt.Errorf("cannot delete the commit \"%s/%s\" because it has non-empty provenance", userCommit.Repo.Name, userCommit.ID)
	}
	deleteCommit(userCommitInfo.Commit, userCommitInfo.Commit)

	// 4) Delete all of the downstream commits of 'commit'
	for _, subv := range userCommitInfo.Subvenance {
		deleteCommit(subv.Lower, subv.Upper)
	}

	// 5) Remove the commits in 'deleted' from all remaining upstream commits'
	// subvenance.
	// While 'commit' is required to be an input commit (no provenance),
	// downstream commits from 'commit' may have multiple inputs, and those
	// other inputs must have their subvenance updated
	visited := make(map[string]bool) // visitied upstream (provenant) commits
	for _, deletedInfo := range deleted {
		for _, prov := range deletedInfo.Provenance {
			// Check if we've fixed provCommit already (or if it's deleted and
			// doesn't need to be fixed
			if _, isDeleted := deleted[prov.Commit.ID]; isDeleted || visited[prov.Commit.ID] {
				continue
			}
			visited[prov.Commit.ID] = true

			// fix provCommit's subvenance
			provCI := &pfs.CommitInfo{}
			if err := d.commits(prov.Commit.Repo.Name).ReadWrite(txnCtx.Stm).Update(prov.Commit.ID, provCI, func() error {
				subvTo := 0 // copy subvFrom to subvTo, excepting subv ranges to delete (so that they're overwritten)
			nextSubvRange:
				for subvFrom, subv := range provCI.Subvenance {
					// Compute path (of commit IDs) connecting subv.Upper to subv.Lower
					cur := subv.Upper.ID
					path := []string{cur}
					for cur != subv.Lower.ID {
						// Get CommitInfo for 'cur' (either in 'deleted' or from etcd)
						// and traverse parent
						curInfo, ok := deleted[cur]
						if !ok {
							curInfo = &pfs.CommitInfo{}
							if err := d.commits(subv.Lower.Repo.Name).ReadWrite(txnCtx.Stm).Get(cur, curInfo); err != nil {
								return fmt.Errorf("error reading commitInfo for subvenant \"%s/%s\": %v", subv.Lower.Repo.Name, cur, err)
							}
						}
						if curInfo.ParentCommit == nil {
							break
						}
						cur = curInfo.ParentCommit.ID
						path = append(path, cur)
					}

					// move 'subv.Upper' through parents until it points to a non-deleted commit
					for j := range path {
						if _, ok := deleted[subv.Upper.ID]; !ok {
							break
						}
						if j+1 >= len(path) {
							// All commits in subvRange are deleted. Remove entire Range
							// from provCI.Subvenance
							continue nextSubvRange
						}
						subv.Upper.ID = path[j+1]
					}

					// move 'subv.Lower' through children until it points to a non-deleted commit
					for j := len(path) - 1; j >= 0; j-- {
						if _, ok := deleted[subv.Lower.ID]; !ok {
							break
						}
						// We'll eventually get to a non-deleted commit because the
						// 'upper' block didn't exit
						subv.Lower.ID = path[j-1]
					}
					provCI.Subvenance[subvTo] = provCI.Subvenance[subvFrom]
					subvTo++
				}
				provCI.Subvenance = provCI.Subvenance[:subvTo]
				return nil
			}); err != nil {
				return fmt.Errorf("err fixing subvenance of upstream commit %s/%s: %v", prov.Commit.Repo.Name, prov.Commit.ID, err)
			}
		}
	}

	// 6) Rewrite ParentCommit of deleted commits' children, and
	// ChildCommits of deleted commits' parents
	visited = make(map[string]bool) // visited child/parent commits
	for deletedID, deletedInfo := range deleted {
		if visited[deletedID] {
			continue
		}

		// Traverse downwards until we find the lowest (most ancestral)
		// non-nil, deleted commit
		lowestCommitInfo := deletedInfo
		for {
			if lowestCommitInfo.ParentCommit == nil {
				break // parent is nil
			}
			parentInfo, ok := deleted[lowestCommitInfo.ParentCommit.ID]
			if !ok {
				break // parent is not deleted
			}
			lowestCommitInfo = parentInfo // parent exists and is deleted--go down
		}

		// BFS upwards through graph for all non-deleted children
		var next *pfs.Commit                            // next vertex to search
		queue := []*pfs.Commit{lowestCommitInfo.Commit} // queue of vertices to explore
		liveChildren := make(map[string]struct{})       // live children discovered so far
		for len(queue) > 0 {
			next, queue = queue[0], queue[1:]
			if visited[next.ID] {
				continue
			}
			visited[next.ID] = true
			nextInfo, ok := deleted[next.ID]
			if !ok {
				liveChildren[next.ID] = struct{}{}
				continue
			}
			queue = append(queue, nextInfo.ChildCommits...)
		}

		// Point all non-deleted children at the first valid parent (or nil),
		// and point first non-deleted parent at all non-deleted children
		commits := d.commits(deletedInfo.Commit.Repo.Name).ReadWrite(txnCtx.Stm)
		parent := lowestCommitInfo.ParentCommit
		for child := range liveChildren {
			commitInfo := &pfs.CommitInfo{}
			if err := commits.Update(child, commitInfo, func() error {
				commitInfo.ParentCommit = parent
				return nil
			}); err != nil {
				return fmt.Errorf("err updating child commit %v: %v", lowestCommitInfo.Commit, err)
			}
		}
		if parent != nil {
			commitInfo := &pfs.CommitInfo{}
			if err := commits.Update(parent.ID, commitInfo, func() error {
				// Add existing live commits in commitInfo.ChildCommits to the
				// live children above lowestCommitInfo, then put them all in
				// 'parent'
				for _, child := range commitInfo.ChildCommits {
					if _, ok := deleted[child.ID]; ok {
						continue
					}
					liveChildren[child.ID] = struct{}{}
				}
				commitInfo.ChildCommits = make([]*pfs.Commit, 0, len(liveChildren))
				for child := range liveChildren {
					commitInfo.ChildCommits = append(commitInfo.ChildCommits, client.NewCommit(parent.Repo.Name, child))
				}
				return nil
			}); err != nil {
				return fmt.Errorf("err rewriting children of ancestor commit %v: %v", lowestCommitInfo.Commit, err)
			}
		}
	}

	// 7) Traverse affected repos and rewrite all branches so that no branch
	// points to a deleted commit
	var affectedBranches []*pfs.BranchInfo
	repos := d.repos.ReadWrite(txnCtx.Stm)
	for repo := range affectedRepos {
		repoInfo := &pfs.RepoInfo{}
		if err := repos.Get(repo, repoInfo); err != nil {
			return err
		}
		for _, brokenBranch := range repoInfo.Branches {
			// Traverse HEAD commit until we find a non-deleted parent or nil;
			// rewrite branch
			var branchInfo pfs.BranchInfo
			if err := d.branches(brokenBranch.Repo.Name).ReadWrite(txnCtx.Stm).Update(brokenBranch.Name, &branchInfo, func() error {
				prevHead := branchInfo.Head
				for {
					if branchInfo.Head == nil {
						return nil // no commits left in branch
					}
					headCommitInfo, headIsDeleted := deleted[branchInfo.Head.ID]
					if !headIsDeleted {
						break
					}
					branchInfo.Head = headCommitInfo.ParentCommit
				}
				if prevHead != nil && prevHead.ID != branchInfo.Head.ID {
					affectedBranches = append(affectedBranches, &branchInfo)
				}
				return err
			}); err != nil && !col.IsErrNotFound(err) {
				// If err is NotFound, branch is in downstream provenance but
				// doesn't exist yet--nothing to update
				return fmt.Errorf("error updating branch %v/%v: %v", brokenBranch.Repo.Name, brokenBranch.Name, err)
			}

			// Update repo size if this is the master branch
			if branchInfo.Name == "master" {
				if branchInfo.Head != nil {
					headCommitInfo, err := d.resolveCommit(txnCtx.Stm, branchInfo.Head)
					if err != nil {
						return err
					}
					repoInfo.SizeBytes = headCommitInfo.SizeBytes
				} else {
					// No HEAD commit, set the repo size to 0
					repoInfo.SizeBytes = 0
				}

				if err := repos.Put(repo, repoInfo); err != nil {
					return err
				}
			}
		}
	}

	// 8) propagate the changes to 'branch' and its subvenance. This may start
	// new HEAD commits downstream, if the new branch heads haven't been
	// processed yet
	for _, afBranch := range affectedBranches {
		if err := txnCtx.PropagateCommit(afBranch.Branch, false); err != nil {
			return err
		}
	}

	return nil
}

// resolveCommitProvenance resolves a user 'commit' (which may be a commit ID or
// branch reference) to a commit + branch pair interpreted as commit provenance.
// If a complete commit provenance is passed in it just uses that.
// It accepts an STM so that it can be used in a transaction and avoids an
// inconsistent call to d.inspectCommit()
func (d *driver) resolveCommitProvenance(stm col.STM, userCommitProvenance *pfs.CommitProvenance) (*pfs.CommitProvenance, error) {
	if userCommitProvenance == nil {
		return nil, fmt.Errorf("cannot resolve nil commit provenance")
	}
	// resolve the commit in case the commit is actually a branch name
	userCommitProvInfo, err := d.resolveCommit(stm, userCommitProvenance.Commit)
	if err != nil {
		return nil, err
	}

	if userCommitProvenance.Branch == nil {
		// if the branch isn't specified, default to using the commit's branch
		userCommitProvenance.Branch = userCommitProvInfo.Branch
		// but if the original "commit id" was a branch name, use that as the branch instead
		if userCommitProvInfo.Commit.ID != userCommitProvenance.Commit.ID {
			userCommitProvenance.Branch.Name = userCommitProvenance.Commit.ID
			userCommitProvenance.Commit = userCommitProvInfo.Commit
		}
	}
	return userCommitProvenance, nil
}

// createBranch creates a new branch or updates an existing branch (must be one
// or the other). Most importantly, it sets 'branch.DirectProvenance' to
// 'provenance' and then for all (downstream) branches, restores the invariant:
//   ∀ b . b.Provenance = ∪ b'.Provenance (where b' ∈ b.DirectProvenance)
//
// This invariant is assumed to hold for all branches upstream of 'branch', but not
// for 'branch' itself once 'b.Provenance' has been set.
func (d *driver) createBranch(txnCtx *txnenv.TransactionContext, branch *pfs.Branch, commit *pfs.Commit, provenance []*pfs.Branch) error {
	// Validate arguments
	if branch == nil {
		return errors.New("branch cannot be nil")
	}
	if branch.Repo == nil {
		return errors.New("branch repo cannot be nil")
	}

	var err error
	if err := d.checkIsAuthorizedInTransaction(txnCtx, branch.Repo, auth.Scope_WRITER); err != nil {
		return err
	}
	// Validate request
	if err := ancestry.ValidateName(branch.Name); err != nil {
		return err
	}
	// The request must do exactly one of:
	// 1) updating 'branch's provenance (commit is nil OR commit == branch)
	// 2) re-pointing 'branch' at a new commit
	var ci *pfs.CommitInfo
	if commit != nil {
		// Determine if this is a provenance update
		sameTarget := branch.Repo.Name == commit.Repo.Name && branch.Name == commit.ID
		if !sameTarget && provenance != nil {
			ci, err = d.inspectCommit(txnCtx.Client, commit, pfs.CommitState_STARTED)
			if err != nil {
				return err
			}
			for _, provBranch := range provenance {
				provBranchInfo := &pfs.BranchInfo{}
				if err := d.branches(provBranch.Repo.Name).ReadWrite(txnCtx.Stm).Get(provBranch.Name, provBranchInfo); err != nil {
					// If the branch doesn't exist no need to count it in provenance
					if col.IsErrNotFound(err) {
						continue
					}
					return err
				}
				for _, provC := range ci.Provenance {
					if proto.Equal(provBranch, provC.Branch) && !proto.Equal(provBranchInfo.Head, provC.Commit) {
						return fmt.Errorf("cannot create branch %q with commit %q as head because commit has \"%s/%s\" as provenance but that commit is not the head of branch \"%s/%s\"", branch.Name, commit.ID, provC.Commit.Repo.Name, provC.Commit.ID, provC.Branch.Repo.Name, provC.Branch.Name)
					}
				}
			}
		}
	}

	// if 'commit' is a branch, resolve it
	if commit != nil {
		_, err = d.resolveCommit(txnCtx.Stm, commit) // if 'commit' is a branch, resolve it
		if err != nil {
			// possible that branch exists but has no head commit. This is fine, but
			// branchInfo.Head must also be nil
			if !isNoHeadErr(err) {
				return fmt.Errorf("unable to inspect %s@%s: %v", commit.Repo.Name, commit.ID, err)
			}
			commit = nil
		}
	}

	// Retrieve (and create, if necessary) the current version of this branch
	branches := d.branches(branch.Repo.Name).ReadWrite(txnCtx.Stm)
	branchInfo := &pfs.BranchInfo{}
	if err := branches.Upsert(branch.Name, branchInfo, func() error {
		branchInfo.Name = branch.Name // set in case 'branch' is new
		branchInfo.Branch = branch
		branchInfo.Head = commit
		branchInfo.DirectProvenance = nil
		for _, provBranch := range provenance {
			if provBranch.Repo.Name == branch.Repo.Name && provBranch.Name == branch.Name {
				return fmt.Errorf("branch %s@%s cannot be in its own provenance", branch.Repo.Name, branch.Name)
			}
			add(&branchInfo.DirectProvenance, provBranch)
		}
		return nil
	}); err != nil {
		return err
	}
	repos := d.repos.ReadWrite(txnCtx.Stm)
	repoInfo := &pfs.RepoInfo{}
	if err := repos.Update(branch.Repo.Name, repoInfo, func() error {
		add(&repoInfo.Branches, branch)
		if branch.Name == "master" && commit != nil {
			ci, err := d.inspectCommit(txnCtx.Client, commit, pfs.CommitState_STARTED)
			if err != nil {
				return err
			}
			repoInfo.SizeBytes = ci.SizeBytes
		}
		return nil
	}); err != nil {
		return err
	}

	// Update (or create)
	// 1) 'branch's Provenance
	// 2) the Provenance of all branches in 'branch's Subvenance (in the case of an update), and
	// 3) the Subvenance of all branches in the *old* provenance of 'branch's Subvenance
	toUpdate := []*pfs.BranchInfo{branchInfo}
	for _, subvBranch := range branchInfo.Subvenance {
		subvBranchInfo := &pfs.BranchInfo{}
		if err := d.branches(subvBranch.Repo.Name).ReadWrite(txnCtx.Stm).Get(subvBranch.Name, subvBranchInfo); err != nil {
			return err
		}
		toUpdate = append(toUpdate, subvBranchInfo)
	}
	// Sorting is important here because it sorts topologically. This means
	// that when evaluating element i of `toUpdate` all elements < i will
	// have already been evaluated and thus we can safely use their
	// Provenance field.
	sort.Slice(toUpdate, func(i, j int) bool { return len(toUpdate[i].Provenance) < len(toUpdate[j].Provenance) })
	for _, branchInfo := range toUpdate {
		oldProvenance := branchInfo.Provenance
		branchInfo.Provenance = nil
		// Re-compute Provenance
		for _, provBranch := range branchInfo.DirectProvenance {
			if err := d.addBranchProvenance(branchInfo, provBranch, txnCtx.Stm); err != nil {
				return err
			}
			provBranchInfo := &pfs.BranchInfo{}
			if err := d.branches(provBranch.Repo.Name).ReadWrite(txnCtx.Stm).Get(provBranch.Name, provBranchInfo); err != nil {
				return fmt.Errorf("error getting prov branch: %v", err)
			}
			for _, provBranch := range provBranchInfo.Provenance {
				// add provBranch to branchInfo.Provenance, and branchInfo.Branch to
				// provBranch subvenance
				if err := d.addBranchProvenance(branchInfo, provBranch, txnCtx.Stm); err != nil {
					return err
				}
			}
		}
		// If we have a commit use it to set head of this branch info
		if ci != nil {
			for _, provC := range ci.Provenance {
				if proto.Equal(provC.Branch, branchInfo.Branch) {
					branchInfo.Head = provC.Commit
				}
			}
		}
		if err := d.branches(branchInfo.Branch.Repo.Name).ReadWrite(txnCtx.Stm).Put(branchInfo.Branch.Name, branchInfo); err != nil {
			return err
		}
		// Update Subvenance of 'branchInfo's Provenance (incl. all Subvenance)
		for _, oldProvBranch := range oldProvenance {
			if !has(&branchInfo.Provenance, oldProvBranch) {
				// Provenance was deleted, so we delete ourselves from their subvenance
				oldProvBranchInfo := &pfs.BranchInfo{}
				if err := d.branches(oldProvBranch.Repo.Name).ReadWrite(txnCtx.Stm).Update(oldProvBranch.Name, oldProvBranchInfo, func() error {
					del(&oldProvBranchInfo.Subvenance, branchInfo.Branch)
					return nil
				}); err != nil {
					return err
				}
			}
		}
	}

	// propagate the head commit to 'branch'. This may also modify 'branch', by
	// creating a new HEAD commit if 'branch's provenance was changed and its
	// current HEAD commit has old provenance
	return txnCtx.PropagateCommit(branch, false)
}

func (d *driver) inspectBranch(txnCtx *txnenv.TransactionContext, branch *pfs.Branch) (*pfs.BranchInfo, error) {
	// Validate arguments
	if branch == nil {
		return nil, errors.New("branch cannot be nil")
	}
	if branch.Repo == nil {
		return nil, errors.New("branch repo cannot be nil")
	}

	result := &pfs.BranchInfo{}
	if err := d.branches(branch.Repo.Name).ReadWrite(txnCtx.Stm).Get(branch.Name, result); err != nil {
		return nil, err
	}
	return result, nil
}

func (d *driver) listBranch(pachClient *client.APIClient, repo *pfs.Repo, reverse bool) ([]*pfs.BranchInfo, error) {
	// Validate arguments
	if repo == nil {
		return nil, errors.New("repo cannot be nil")
	}

	if err := d.checkIsAuthorized(pachClient, repo, auth.Scope_READER); err != nil {
		return nil, err
	}

	// Make sure that the repo exists
	if repo.Name != "" {
		err := d.txnEnv.WithReadContext(pachClient.Ctx(), func(txnCtx *txnenv.TransactionContext) error {
			_, err := d.inspectRepo(txnCtx, repo, !includeAuth)
			return err
		})
		if err != nil {
			return nil, err
		}
	}

	var result []*pfs.BranchInfo
	branchInfo := &pfs.BranchInfo{}
	branches := d.branches(repo.Name).ReadOnly(pachClient.Ctx())
	opts := *col.DefaultOptions // Note we dereference here so as to make a copy
	if reverse {
		opts.Order = etcd.SortAscend
	}
	var bis []*pfs.BranchInfo
	sendBis := func() {
		if !reverse {
			sort.Slice(bis, func(i, j int) bool { return len(bis[i].Provenance) < len(bis[j].Provenance) })
		} else {
			sort.Slice(bis, func(i, j int) bool { return len(bis[i].Provenance) > len(bis[j].Provenance) })
		}
		result = append(result, bis...)
		bis = nil
	}
	lastRev := int64(-1)
	if err := branches.ListRev(branchInfo, &opts, func(branch string, createRev int64) error {
		if createRev != lastRev {
			sendBis()
			lastRev = createRev
		}
		bis = append(bis, proto.Clone(branchInfo).(*pfs.BranchInfo))
		return nil
	}); err != nil {
		return nil, err
	}
	sendBis()
	return result, nil
}

func (d *driver) deleteBranch(txnCtx *txnenv.TransactionContext, branch *pfs.Branch, force bool) error {
	// Validate arguments
	if branch == nil {
		return errors.New("branch cannot be nil")
	}
	if branch.Repo == nil {
		return errors.New("branch repo cannot be nil")
	}

	if err := d.checkIsAuthorizedInTransaction(txnCtx, branch.Repo, auth.Scope_WRITER); err != nil {
		return err
	}

	branches := d.branches(branch.Repo.Name).ReadWrite(txnCtx.Stm)
	branchInfo := &pfs.BranchInfo{}
	if err := branches.Get(branch.Name, branchInfo); err != nil {
		if !col.IsErrNotFound(err) {
			return fmt.Errorf("branches.Get: %v", err)
		}
	}
	if branchInfo.Branch != nil {
		if !force {
			if len(branchInfo.Subvenance) > 0 {
				return fmt.Errorf("branch %s has %v as subvenance, deleting it would break those branches", branch.Name, branchInfo.Subvenance)
			}
		}
		if err := branches.Delete(branch.Name); err != nil {
			return fmt.Errorf("branches.Delete: %v", err)
		}
		for _, provBranch := range branchInfo.Provenance {
			provBranchInfo := &pfs.BranchInfo{}
			if err := d.branches(provBranch.Repo.Name).ReadWrite(txnCtx.Stm).Update(provBranch.Name, provBranchInfo, func() error {
				del(&provBranchInfo.Subvenance, branch)
				return nil
			}); err != nil && !isNotFoundErr(err) {
				return fmt.Errorf("error deleting subvenance: %v", err)
			}
		}
	}
	repoInfo := &pfs.RepoInfo{}
	if err := d.repos.ReadWrite(txnCtx.Stm).Update(branch.Repo.Name, repoInfo, func() error {
		del(&repoInfo.Branches, branch)
		return nil
	}); err != nil {
		if !col.IsErrNotFound(err) || !force {
			return err
		}
	}
	return nil
}

// scratchCommitPrefix returns an etcd prefix that's used to temporarily
// store the state of a file in an open commit.  Once the commit is finished,
// the scratch space is removed.
func (d *driver) scratchCommitPrefix(commit *pfs.Commit) string {
	return path.Join(commit.Repo.Name, commit.ID)
}

func (d *driver) checkFilePath(path string) error {
	path = filepath.Clean(path)
	if strings.HasPrefix(path, "../") {
		return fmt.Errorf("path (%s) invalid: traverses above root", path)
	}
	return nil
}

// scratchFilePrefix returns an etcd prefix that's used to temporarily
// store the state of a file in an open commit.  Once the commit is finished,
// the scratch space is removed.
func (d *driver) scratchFilePrefix(file *pfs.File) (string, error) {
	cleanedPath := path.Clean("/" + file.Path)
	if err := d.checkFilePath(cleanedPath); err != nil {
		return "", err
	}
	return path.Join(d.scratchCommitPrefix(file.Commit), cleanedPath), nil
}

func (d *driver) putFiles(pachClient *client.APIClient, s *putFileServer) error {
	var files []*pfs.File
	var putFilePaths []string
	var putFileRecords []*pfs.PutFileRecords
	var mu sync.Mutex
	oneOff, repo, branch, err := d.forEachPutFile(pachClient, s, func(req *pfs.PutFileRequest, r io.Reader) error {
		records, err := d.putFile(pachClient, req.File, req.Delimiter, req.TargetFileDatums,
			req.TargetFileBytes, req.HeaderRecords, req.OverwriteIndex, r)
		if err != nil {
			return err
		}
		mu.Lock()
		defer mu.Unlock()
		files = append(files, req.File)
		putFilePaths = append(putFilePaths, req.File.Path)
		putFileRecords = append(putFileRecords, records)
		return nil
	})
	if err != nil {
		return err
	}

	ctx := pachClient.Ctx()
	if oneOff {
		// oneOff puts only work on branches, so we know branch != "". We pass
		// a commit with no ID, that ID will be filled in with the head of
		// branch (if it exists).
		return d.txnEnv.WithWriteContext(ctx, func(txnCtx *txnenv.TransactionContext) error {
			_, err := d.makeCommit(txnCtx, "", client.NewCommit(repo, ""), branch, nil, nil, nil, nil, putFilePaths, putFileRecords, "", 0)
			return err
		})
	}
	for i, file := range files {
		if err := d.upsertPutFileRecords(pachClient, file, putFileRecords[i]); err != nil {
			return err
		}
	}
	return nil
}

func (d *driver) putFile(pachClient *client.APIClient, file *pfs.File, delimiter pfs.Delimiter,
	targetFileDatums, targetFileBytes, headerRecords int64, overwriteIndex *pfs.OverwriteIndex,
	reader io.Reader) (*pfs.PutFileRecords, error) {
	if err := d.checkIsAuthorized(pachClient, file.Commit.Repo, auth.Scope_WRITER); err != nil {
		return nil, err
	}
	//  validation -- make sure the various putFileSplit options are coherent
	hasPutFileOptions := targetFileBytes != 0 || targetFileDatums != 0 || headerRecords != 0
	if hasPutFileOptions && delimiter == pfs.Delimiter_NONE {
		return nil, fmt.Errorf("cannot set split options--targetFileBytes, targetFileDatums, or headerRecords--with delimiter == NONE, split disabled")
	}
	records := &pfs.PutFileRecords{}
	if overwriteIndex != nil && overwriteIndex.Index == 0 {
		records.Tombstone = true
	}
	if err := d.checkFilePath(file.Path); err != nil {
		return nil, err
	}
	if err := hashtree.ValidatePath(file.Path); err != nil {
		return nil, err
	}

	if delimiter == pfs.Delimiter_NONE {
		d.putObjectLimiter.Acquire()
		defer d.putObjectLimiter.Release()
		objects, size, err := pachClient.PutObjectSplit(reader)
		if err != nil {
			return nil, err
		}

		// Here we use the invariant that every one but the last object
		// should have a size of ChunkSize.
		for i, object := range objects {
			record := &pfs.PutFileRecord{
				ObjectHash: object.Hash,
			}

			if size > pfs.ChunkSize {
				record.SizeBytes = pfs.ChunkSize
			} else {
				record.SizeBytes = size
			}
			size -= pfs.ChunkSize

			// The first record takes care of the overwriting
			if i == 0 && overwriteIndex != nil && overwriteIndex.Index != 0 {
				record.OverwriteIndex = overwriteIndex
			}

			records.Records = append(records.Records, record)
		}
	} else {
		var (
			buffer        = &bytes.Buffer{}
			datumsWritten int64
			bytesWritten  int64
			filesPut      int
			// Note: this code generally distinguishes between nil header/footer (no
			// header) and empty header/footer. To create a header-enabled directory
			// with an empty header, allocate an empty slice & store it here
			header    []byte
			footer    []byte
			EOF       = false
			eg        errgroup.Group
			bufioR    = bufio.NewReader(reader)
			decoder   = json.NewDecoder(bufioR)
			sqlReader = sql.NewPGDumpReader(bufioR)
			csvReader = csv.NewReader(bufioR)
			csvBuffer bytes.Buffer
			csvWriter = csv.NewWriter(&csvBuffer)
			// indexToRecord serves as a de-facto slice of PutFileRecords. We can't
			// use a real slice of PutFileRecords b/c indexToRecord has data appended
			// to it by concurrent processes, and you can't append() to a slice
			// concurrently (append() might allocate a new slice while a goro holds an
			// stale pointer)
			indexToRecord = make(map[int]*pfs.PutFileRecord)
			mu            sync.Mutex
		)
		csvReader.FieldsPerRecord = -1 // ignore unexpected # of fields, for now
		csvReader.ReuseRecord = true   // returned rows are written to buffer immediately
		for !EOF {
			var err error
			var value []byte
			var csvRow []string // only used if delimiter == CSV
			switch delimiter {
			case pfs.Delimiter_JSON:
				var jsonValue json.RawMessage
				err = decoder.Decode(&jsonValue)
				value = jsonValue
			case pfs.Delimiter_LINE:
				value, err = bufioR.ReadBytes('\n')
			case pfs.Delimiter_SQL:
				value, err = sqlReader.ReadRow()
				if err == io.EOF {
					if header == nil {
						header = sqlReader.Header
					} else {
						// header contains SQL records if anything, which should come after
						// the sqlReader header, which creates tables & initializes the DB
						header = append(sqlReader.Header, header...)
					}
					footer = sqlReader.Footer
				}
			case pfs.Delimiter_CSV:
				csvBuffer.Reset()
				if csvRow, err = csvReader.Read(); err == nil {
					if err := csvWriter.Write(csvRow); err != nil {
						return nil, fmt.Errorf("error parsing csv record: %v", err)
					}
					if csvWriter.Flush(); csvWriter.Error() != nil {
						return nil, fmt.Errorf("error copying csv record: %v", csvWriter.Error())
					}
					value = csvBuffer.Bytes()
				}
			default:
				return nil, fmt.Errorf("unrecognized delimiter %s", delimiter.String())
			}
			if err != nil {
				if err == io.EOF {
					EOF = true
				} else {
					return nil, err
				}
			}
			buffer.Write(value)
			bytesWritten += int64(len(value))
			datumsWritten++
			var (
				headerDone         = headerRecords == 0 || header != nil
				headerReady        = !headerDone && datumsWritten >= headerRecords
				hitFileBytesLimit  = headerDone && targetFileBytes != 0 && bytesWritten >= targetFileBytes
				hitFileDatumsLimit = headerDone && targetFileDatums != 0 && datumsWritten >= targetFileDatums
				noLimitsSet        = headerDone && targetFileBytes == 0 && targetFileDatums == 0
			)
			if buffer.Len() != 0 &&
				(headerReady || hitFileBytesLimit || hitFileDatumsLimit || noLimitsSet || EOF) {
				_buffer := buffer
				if !headerDone /* implies headerReady || EOF */ {
					header = _buffer.Bytes() // record header
				} else {
					// put contents
					_bufferLen := int64(_buffer.Len())
					index := filesPut
					filesPut++
					d.memoryLimiter.Acquire(pachClient.Ctx(), _bufferLen)
					d.putObjectLimiter.Acquire()
					eg.Go(func() error {
						defer d.putObjectLimiter.Release()
						defer d.memoryLimiter.Release(_bufferLen)
						object, size, err := pachClient.PutObject(_buffer)
						if err != nil {
							return err
						}
						mu.Lock()
						defer mu.Unlock()
						indexToRecord[index] = &pfs.PutFileRecord{
							SizeBytes:  size,
							ObjectHash: object.Hash,
						}
						return nil
					})
				}
				buffer = &bytes.Buffer{} // can't reset buffer b/c _buffer still in use
				datumsWritten = 0
				bytesWritten = 0
			}
		}
		if err := eg.Wait(); err != nil {
			return nil, err
		}

		records.Split = true
		for i := 0; i < len(indexToRecord); i++ {
			records.Records = append(records.Records, indexToRecord[i])
		}

		// Put 'header' and 'footer' in PutFileRecords
		setHeaderFooter := func(value []byte, hf **pfs.PutFileRecord) {
			// always put empty header, even if 'value' is empty, so
			// that the parent dir is a header/footer dir
			*hf = &pfs.PutFileRecord{}
			if len(value) > 0 {
				d.putObjectLimiter.Acquire()
				eg.Go(func() error {
					defer d.putObjectLimiter.Release()
					object, size, err := pachClient.PutObject(bytes.NewReader(value))
					if err != nil {
						return err
					}
					(*hf).SizeBytes = size
					(*hf).ObjectHash = object.Hash
					return nil
				})
			}
		}
		if header != nil {
			setHeaderFooter(header, &records.Header)
		}
		if footer != nil {
			setHeaderFooter(footer, &records.Footer)
		}
		if err := eg.Wait(); err != nil {
			return nil, err
		}
	}
	return records, nil
}

func appendRecords(pfr *pfs.PutFileRecords, node *hashtree.NodeProto) {
	for i, object := range node.FileNode.Objects {
		// We only have the whole file size in src file, so mark the first object
		// as the size of the whole file and all the rest as size 0; applyWrite
		// will compute the right sum size for the target file
		// TODO(msteffen): this is a bit of a hack--either PutFileRecords should
		// only record the sum size of all PutFileRecord messages as well, or
		// FileNodeProto should record the size of every object
		var size int64
		if i == 0 {
			size = node.SubtreeSize
		}
		pfr.Records = append(pfr.Records, &pfs.PutFileRecord{
			SizeBytes:  size,
			ObjectHash: object.Hash,
		})
	}
	for _, blockRef := range node.FileNode.BlockRefs {
		pfr.Records = append(pfr.Records, &pfs.PutFileRecord{
			BlockRef:  blockRef,
			SizeBytes: int64(blockRef.Range.Upper - blockRef.Range.Lower),
		})
	}
}

// headerDirToPutFileRecords is a helper for copyFile that handles copying
// header/footer directories.
//
// Copy uses essentially the same codepath as putFile--it converts hashtree
// node(s) to PutFileRecords and then uses applyWrite to put the records back
// in the target hashtree. In putFile, the only way to create a headerDir is
// with PutFileSplit (PutFileRecord with Split==true). Rather than split the
// putFile codepath by adding a special case to applyWrite that was valid for
// copyFile but invalid for putFile, we use heaaderDirToPutFileRecords to
// convert a DirectoryNode to a PutFileRecord+Split==true, which applyWrite
// will correctly convert back to a header dir in the target hashtree via the
// regular putFile codepath.
func headerDirToPutFileRecords(tree hashtree.HashTree, path string, node *hashtree.NodeProto) (*pfs.PutFileRecords, error) {
	if tree == nil {
		return nil, fmt.Errorf("called headerDirToPutFileRecords with nil tree (this is likely a bug")
	}
	if node.DirNode == nil || node.DirNode.Shared == nil {
		return nil, fmt.Errorf("headerDirToPutFileRecords only works on header/footer dirs")
	}
	s := node.DirNode.Shared
	pfr := &pfs.PutFileRecords{
		Split: true,
	}
	if s.Header != nil {
		pfr.Header = &pfs.PutFileRecord{
			SizeBytes:  s.HeaderSize,
			ObjectHash: s.Header.Hash,
		}
	}
	if s.Footer != nil {
		pfr.Footer = &pfs.PutFileRecord{
			SizeBytes:  s.FooterSize,
			ObjectHash: s.Footer.Hash,
		}
	}
	if err := tree.List(path, func(child *hashtree.NodeProto) error {
		if child.FileNode == nil {
			return fmt.Errorf("header/footer dir contains child subdirectory, " +
				"which is invalid--header/footer dirs must be created by PutFileSplit")
		}
		appendRecords(pfr, child)
		return nil
	}); err != nil {
		return nil, err
	}
	return pfr, nil // TODO(msteffen) put something real here
}

func (d *driver) copyFile(pachClient *client.APIClient, src *pfs.File, dst *pfs.File, overwrite bool) (retErr error) {
	// Validate arguments
	if src == nil {
		return errors.New("src cannot be nil")
	}
	if src.Commit == nil {
		return errors.New("src commit cannot be nil")
	}
	if src.Commit.Repo == nil {
		return errors.New("src commit repo cannot be nil")
	}
	if dst == nil {
		return errors.New("dst cannot be nil")
	}
	if dst.Commit == nil {
		return errors.New("dst commit cannot be nil")
	}
	if dst.Commit.Repo == nil {
		return errors.New("dst commit repo cannot be nil")
	}

	if err := d.checkIsAuthorized(pachClient, src.Commit.Repo, auth.Scope_READER); err != nil {
		return err
	}
	if err := d.checkIsAuthorized(pachClient, dst.Commit.Repo, auth.Scope_WRITER); err != nil {
		return err
	}
	if err := d.checkFilePath(dst.Path); err != nil {
		return err
	}
	if err := hashtree.ValidatePath(dst.Path); err != nil {
		return err
	}
	branch := ""
	if !uuid.IsUUIDWithoutDashes(dst.Commit.ID) {
		branch = dst.Commit.ID
	}
	var dstIsOpenCommit bool
	if ci, err := d.inspectCommit(pachClient, dst.Commit, pfs.CommitState_STARTED); err != nil {
		if !isNoHeadErr(err) {
			return err
		}
	} else if ci.Finished == nil {
		dstIsOpenCommit = true
	}
	if !dstIsOpenCommit && branch == "" {
		return pfsserver.ErrCommitFinished{dst.Commit}
	}
	var paths []string
	var records []*pfs.PutFileRecords // used if 'dst' is finished (atomic 'put file')
	if overwrite {
		if dstIsOpenCommit {
			if err := d.deleteFile(pachClient, dst); err != nil {
				return err
			}
		} else {
			paths = append(paths, dst.Path)
			records = append(records, &pfs.PutFileRecords{Tombstone: true})
		}
	}
	var eg errgroup.Group
	var srcTree hashtree.HashTree
	cb := func(walkPath string, node *hashtree.NodeProto) error {
		relPath, err := filepath.Rel(src.Path, walkPath)
		if err != nil {
			return fmt.Errorf("error from filepath.Rel (likely a bug): %v", err)
		}
		target := client.NewFile(dst.Commit.Repo.Name, dst.Commit.ID, path.Clean(path.Join(dst.Path, relPath)))
		// Populate 'record' appropriately for this node (or skip it)
		record := &pfs.PutFileRecords{}
		if node.DirNode != nil && node.DirNode.Shared != nil {
			var err error
			record, err = headerDirToPutFileRecords(srcTree, walkPath, node)
			if err != nil {
				return err
			}
		} else if node.FileNode == nil {
			return nil
		} else if node.FileNode.HasHeaderFooter {
			return nil // parent dir will be copied as a PutFileRecord w/ Split==true
		} else {
			appendRecords(record, node)
		}

		// Either upsert 'record' to etcd (if 'dst' is in an open commit) or add it
		// to 'records' to be put at the end
		if dstIsOpenCommit {
			eg.Go(func() error {
				return d.upsertPutFileRecords(pachClient, target, record)
			})
		} else {
			paths = append(paths, target.Path)
			records = append(records, record)
		}
		return nil
	}
	// This is necessary so we can call filepath.Rel
	if !strings.HasPrefix(src.Path, "/") {
		src.Path = "/" + src.Path
	}
	srcCi, err := d.inspectCommit(pachClient, src.Commit, pfs.CommitState_STARTED)
	if err != nil {
		return err
	}
	if !provenantOnInput(srcCi.Provenance) || srcCi.Tree != nil {
		// handle input commits
		srcTree, err = d.getTreeForFile(pachClient, src)
		if err != nil {
			return err
		}
		defer destroyHashtree(srcTree)
		if err := srcTree.Walk(src.Path, cb); err != nil {
			return err
		}
	} else {
		rs, err := d.getTree(pachClient, srcCi, src.Path)
		if err != nil {
			return err
		}
		defer func() {
			for _, r := range rs {
				if err := r.Close(); err != nil && retErr != nil {
					retErr = err
				}
			}
		}()
		if err := hashtree.Walk(rs, src.Path, cb); err != nil {
			return err
		}
	}
	if err := eg.Wait(); err != nil {
		return err
	}
	// dst is finished => all PutFileRecords are in 'records'--put in a new commit
	if !dstIsOpenCommit {
		return d.txnEnv.WithWriteContext(pachClient.Ctx(), func(txnCtx *txnenv.TransactionContext) error {
			_, err = d.makeCommit(txnCtx, "", client.NewCommit(dst.Commit.Repo.Name, ""), branch, nil, nil, nil, nil, paths, records, "", 0)
			return err
		})
	}
	return nil
}

func (d *driver) getTreeForCommit(txnCtx *txnenv.TransactionContext, commit *pfs.Commit) (hashtree.HashTree, error) {
	if commit == nil || commit.ID == "" {
		return d.treeCache.GetOrAdd("nil", func() (hashtree.HashTree, error) {
			return hashtree.NewDBHashTree(d.storageRoot)
		})
	}

	return d.treeCache.GetOrAdd(commit.ID, func() (hashtree.HashTree, error) {
		if _, err := d.resolveCommit(txnCtx.Stm, commit); err != nil {
			return nil, err
		}

		commits := d.commits(commit.Repo.Name).ReadWrite(txnCtx.Stm)
		commitInfo := &pfs.CommitInfo{}
		if err := commits.Get(commit.ID, commitInfo); err != nil {
			return nil, err
		}
		if commitInfo.Finished == nil {
			return nil, fmt.Errorf("cannot read from an open commit")
		}
		treeRef := commitInfo.Tree

		if treeRef == nil {
			return hashtree.NewDBHashTree(d.storageRoot)
		}

		// read the tree from the block store
		h, err := hashtree.GetHashTreeObject(txnCtx.Client, d.storageRoot, treeRef)
		if err != nil {
			return nil, err
		}

		return h, nil
	})
}

func (d *driver) getTree(pachClient *client.APIClient, commitInfo *pfs.CommitInfo, path string) (rs []io.ReadCloser, retErr error) {
	// Determine the hashtree in which the path is located and download the chunk it is in
	idx := hashtree.PathToTree(path, int64(len(commitInfo.Trees)))
	r, err := d.downloadTree(pachClient, commitInfo.Trees[idx], path)
	if err != nil {
		return nil, err
	}
	return []io.ReadCloser{r}, nil
}

func (d *driver) getTrees(pachClient *client.APIClient, commitInfo *pfs.CommitInfo, pattern string) (rs []io.ReadCloser, retErr error) {
	prefix := hashtree.GlobLiteralPrefix(pattern)
	limiter := limit.New(hashtree.DefaultMergeConcurrency)
	var eg errgroup.Group
	var mu sync.Mutex
	// Download each hashtree chunk based on the literal prefix of the pattern
	for _, object := range commitInfo.Trees {
		object := object
		limiter.Acquire()
		eg.Go(func() (retErr error) {
			defer limiter.Release()
			r, err := d.downloadTree(pachClient, object, prefix)
			if err != nil {
				return err
			}
			mu.Lock()
			defer mu.Unlock()
			rs = append(rs, r)
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	return rs, nil
}

func (d *driver) downloadTree(pachClient *client.APIClient, object *pfs.Object, prefix string) (r io.ReadCloser, retErr error) {
	objClient, err := obj.NewClientFromSecret(d.storageRoot)
	if err != nil {
		return nil, err
	}
	info, err := pachClient.InspectObject(object.Hash)
	if err != nil {
		return nil, err
	}
	path, err := obj.BlockPathFromEnv(info.BlockRef.Block)
	if err != nil {
		return nil, err
	}
	offset, size, err := getTreeRange(pachClient.Ctx(), objClient, path, prefix)
	if err != nil {
		return nil, err
	}
	objR, err := objClient.Reader(pachClient.Ctx(), path, offset, size)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := objR.Close(); err != nil && retErr == nil {
			retErr = err
		}
	}()
	name := filepath.Join(d.storageRoot, uuid.NewWithoutDashes())
	f, err := os.Create(name)
	if err != nil {
		return nil, err
	}
	// Mark the file for removal (Linux won't remove it until we close the file)
	if err := os.Remove(name); err != nil {
		return nil, err
	}
	buf := grpcutil.GetBuffer()
	defer grpcutil.PutBuffer(buf)
	if _, err := io.CopyBuffer(f, objR, buf); err != nil {
		return nil, err
	}
	if _, err := f.Seek(0, 0); err != nil {
		return nil, err
	}
	return f, nil
}

func getTreeRange(ctx context.Context, objClient obj.Client, path string, prefix string) (uint64, uint64, error) {
	p := path + hashtree.IndexPath
	r, err := objClient.Reader(ctx, p, 0, 0)
	if err != nil {
		return 0, 0, err
	}
	idx, err := ioutil.ReadAll(r)
	if err != nil {
		return 0, 0, err
	}
	return hashtree.GetRangeFromIndex(bytes.NewBuffer(idx), prefix)
}

// getTreeForFile is like getTreeForCommit except that it can handle open commits.
// It takes a file instead of a commit so that it can apply the changes for
// that path to the tree before it returns it. The returned hash tree is not in
// the treeCache and must be cleaned up by the caller.
func (d *driver) getTreeForFile(pachClient *client.APIClient, file *pfs.File) (hashtree.HashTree, error) {
	ctx := pachClient.Ctx()
	if file.Commit == nil {
		t, err := hashtree.NewDBHashTree(d.storageRoot)
		if err != nil {
			return nil, err
		}
		return t, nil
	}

	var result hashtree.HashTree
	err := d.txnEnv.WithReadContext(ctx, func(txnCtx *txnenv.TransactionContext) error {
		commitInfo, err := d.resolveCommit(txnCtx.Stm, file.Commit)
		if err != nil {
			return err
		}
		if commitInfo.Finished != nil {
			t, err := d.getTreeForCommit(txnCtx, file.Commit)
			if err != nil {
				return err
			}

			result, err = t.Copy()
			return err
		}

		parentTree, err := d.getTreeForCommit(txnCtx, commitInfo.ParentCommit)
		if err != nil {
			return err
		}
		result, err = d.getTreeForOpenCommit(txnCtx.Client, file, parentTree)
		return err
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// getTreeForOpenCommit obtains the resulting hashtree made by applying the
// given file to the hashtree of a parent commit. The returned hash tree is not
// in the treeCache and must be cleaned up by the caller.
func (d *driver) getTreeForOpenCommit(pachClient *client.APIClient, file *pfs.File, parentTree hashtree.HashTree) (result hashtree.HashTree, retErr error) {
	prefix, err := d.scratchFilePrefix(file)
	if err != nil {
		return nil, err
	}
	tree, err := parentTree.Copy()
	if err != nil {
		return nil, err
	}

	defer func() {
		if retErr != nil {
			destroyHashtree(tree)
		}
	}()

	recordsCol := d.putFileRecords.ReadOnly(pachClient.Ctx())
	putFileRecords := &pfs.PutFileRecords{}
	opts := &col.Options{etcd.SortByModRevision, etcd.SortAscend, true}
	err = recordsCol.ListPrefix(prefix, putFileRecords, opts, func(key string) error {
		return d.applyWrite(path.Join(file.Path, key), putFileRecords, tree)
	})
	if err != nil {
		return nil, err
	}
	if err := tree.Hash(); err != nil {
		return nil, err
	}
	return tree, nil
}

// this is a helper function to check if the given provenance has provenance on an input branch
func provenantOnInput(provenance []*pfs.CommitProvenance) bool {
	provenanceCount := len(provenance)
	for _, p := range provenance {
		// in particular, we want to exclude provenance on the spec repo (used e.g. for spouts)
		if p.Commit.Repo.Name == ppsconsts.SpecRepo {
			provenanceCount--
			break
		}
	}
	return provenanceCount > 0
}

func (d *driver) getFile(pachClient *client.APIClient, file *pfs.File, offset int64, size int64) (r io.Reader, retErr error) {
	// Validate arguments
	if file == nil {
		return nil, errors.New("file cannot be nil")
	}
	if file.Commit == nil {
		return nil, errors.New("file commit cannot be nil")
	}
	if file.Commit.Repo == nil {
		return nil, errors.New("file commit repo cannot be nil")
	}

	ctx := pachClient.Ctx()
	if err := d.checkIsAuthorized(pachClient, file.Commit.Repo, auth.Scope_READER); err != nil {
		return nil, err
	}
	commitInfo, err := d.inspectCommit(pachClient, file.Commit, pfs.CommitState_STARTED)
	if err != nil {
		return nil, err
	}
	// Handle commits that use the old hashtree format.
	if !provenantOnInput(commitInfo.Provenance) || commitInfo.Tree != nil {
		tree, err := d.getTreeForFile(pachClient, client.NewFile(file.Commit.Repo.Name, file.Commit.ID, ""))
		if err != nil {
			return nil, err
		}
		defer destroyHashtree(tree)
		var (
			pathsFound int
			objects    []*pfs.Object
			brs        []*pfs.BlockRef
			totalSize  uint64
			footer     *pfs.Object
			prevDir    string
		)
		if err := tree.Glob(file.Path, func(p string, node *hashtree.NodeProto) error {
			pathsFound++
			if node.FileNode == nil {
				return nil
			}

			// add footer + header for next dir. If a user calls e.g.
			// 'GetFile("/*/*")', then the output looks like:
			// [d1 header][d1/1]...[d1/n][d1 footer] [d2 header]...[d2 footer] ...
			parentPath := path.Dir(p)
			if parentPath != prevDir {
				if footer != nil {
					objects = append(objects, footer)
				}
				footer = nil // don't apply footer twice if next dir has no footer
				prevDir = parentPath
				if node.FileNode.HasHeaderFooter {
					// if any child of 'node's parent directory has HasHeaderFooter set,
					// then they all should
					parentNode, err := tree.Get(parentPath)
					if err != nil {
						return fmt.Errorf("file %q has a header, but could not "+
							"retrieve parent node at %q to get header content: %v", p,
							parentPath, err)
					}
					if parentNode.DirNode == nil {
						return fmt.Errorf("parent of %q is not a directory—this is "+
							"likely an internal error", p)
					}
					if parentNode.DirNode.Shared == nil {
						return fmt.Errorf("file %q has a shared header or footer, "+
							"but parent directory does not permit shared data", p)
					}
					if parentNode.DirNode.Shared.Header != nil {
						objects = append(objects, parentNode.DirNode.Shared.Header)
					}
					if parentNode.DirNode.Shared.Footer != nil {
						footer = parentNode.DirNode.Shared.Footer
					}
				}
			}
			objects = append(objects, node.FileNode.Objects...)
			brs = append(brs, node.FileNode.BlockRefs...)
			totalSize += uint64(node.SubtreeSize)
			return nil
		}); err != nil {
			return nil, err
		}
		if footer != nil {
			objects = append(objects, footer) // apply final footer
		}
		if pathsFound == 0 {
			return nil, pfsserver.ErrFileNotFound{file}
		}

		// retrieve the content of all objects in 'objects'
		if len(brs) > 0 {
			getBlocksClient, err := pachClient.ObjectAPIClient.GetBlocks(
				ctx,
				&pfs.GetBlocksRequest{
					BlockRefs:   brs,
					OffsetBytes: uint64(offset),
					SizeBytes:   uint64(size),
					TotalSize:   uint64(totalSize),
				})
			if err != nil {
				return nil, err
			}
			return grpcutil.NewStreamingBytesReader(getBlocksClient, nil), nil
		}
		getObjectsClient, err := pachClient.ObjectAPIClient.GetObjects(
			ctx,
			&pfs.GetObjectsRequest{
				Objects:     objects,
				OffsetBytes: uint64(offset),
				SizeBytes:   uint64(size),
				TotalSize:   uint64(totalSize),
			})
		if err != nil {
			return nil, err
		}
		return grpcutil.NewStreamingBytesReader(getObjectsClient, nil), nil
	}
	// Handle commits that use the newer hashtree format.
	if commitInfo.Finished == nil {
		return nil, pfsserver.ErrOutputCommitNotFinished{commitInfo.Commit}
	}
	if commitInfo.Trees == nil {
		return nil, pfsserver.ErrFileNotFound{file}
	}
	var rs []io.ReadCloser
	// Handles the case when looking for a specific file/directory
	if !hashtree.IsGlob(file.Path) {
		rs, err = d.getTree(pachClient, commitInfo, file.Path)
	} else {
		rs, err = d.getTrees(pachClient, commitInfo, file.Path)
	}
	if err != nil {
		return nil, err
	}
	defer func() {
		for _, r := range rs {
			if err := r.Close(); err != nil && retErr != nil {
				retErr = err
			}
		}
	}()
	blockRefs := []*pfs.BlockRef{}
	var totalSize int64
	var found bool
	if err := hashtree.Glob(rs, file.Path, func(path string, node *hashtree.NodeProto) error {
		if node.FileNode == nil {
			return nil
		}
		blockRefs = append(blockRefs, node.FileNode.BlockRefs...)
		totalSize += node.SubtreeSize
		found = true
		return nil
	}); err != nil {
		return nil, err
	}
	if !found {
		return nil, pfsserver.ErrFileNotFound{file}
	}
	getBlocksClient, err := pachClient.ObjectAPIClient.GetBlocks(
		ctx,
		&pfs.GetBlocksRequest{
			BlockRefs:   blockRefs,
			OffsetBytes: uint64(offset),
			SizeBytes:   uint64(size),
			TotalSize:   uint64(totalSize),
		},
	)
	if err != nil {
		return nil, err
	}
	return grpcutil.NewStreamingBytesReader(getBlocksClient, nil), nil
}

// If full is false, exclude potentially large fields such as `Objects`
// and `Children`
func nodeToFileInfo(ci *pfs.CommitInfo, path string, node *hashtree.NodeProto, full bool) *pfs.FileInfo {
	fileInfo := &pfs.FileInfo{
		File: &pfs.File{
			Commit: ci.Commit,
			Path:   path,
		},
		SizeBytes: uint64(node.SubtreeSize),
		Hash:      node.Hash,
		Committed: ci.Finished,
	}
	if node.FileNode != nil {
		fileInfo.FileType = pfs.FileType_FILE
		if full {
			fileInfo.Objects = node.FileNode.Objects
			fileInfo.BlockRefs = node.FileNode.BlockRefs
		}
	} else if node.DirNode != nil {
		fileInfo.FileType = pfs.FileType_DIR
		if full {
			fileInfo.Children = node.DirNode.Children
		}
	}
	return fileInfo
}

// nodeToFileInfoHeaderFooter is like nodeToFileInfo, but handles the case (which
// currently only occurs in input commits) where files have a header that is
// stored in their parent directory
func nodeToFileInfoHeaderFooter(ci *pfs.CommitInfo, filePath string,
	node *hashtree.NodeProto, tree hashtree.HashTree, full bool) (*pfs.FileInfo, error) {
	if node.FileNode == nil || !node.FileNode.HasHeaderFooter {
		return nodeToFileInfo(ci, filePath, node, full), nil
	}
	node = proto.Clone(node).(*hashtree.NodeProto)
	// validate baseFileInfo for logic below--if input hashtrees start using
	// blockrefs instead of objects, this logic will need to be adjusted
	if node.FileNode.Objects == nil {
		return nil, fmt.Errorf("input commit node uses blockrefs; cannot apply header")
	}

	// 'file' includes header from parent—construct synthetic file info that
	// includes header in list of objects & hash
	parentPath := path.Dir(filePath)
	parentNode, err := tree.Get(parentPath)
	if err != nil {
		return nil, fmt.Errorf("file %q has a header, but could not "+
			"retrieve parent node at %q to get header content: %v", filePath,
			parentPath, err)
	}
	if parentNode.DirNode == nil {
		return nil, fmt.Errorf("parent of %q is not a directory; this is "+
			"likely an internal error", filePath)
	}
	if parentNode.DirNode.Shared == nil {
		return nil, fmt.Errorf("file %q has a shared header or footer, "+
			"but parent directory does not permit shared data", filePath)
	}

	s := parentNode.DirNode.Shared
	var newObjects []*pfs.Object
	if s.Header != nil {
		// cap := len+1 => newObjects is right whether or not we append() a footer
		newL := len(node.FileNode.Objects) + 1
		newObjects = make([]*pfs.Object, newL, newL+1)

		newObjects[0] = s.Header
		copy(newObjects[1:], node.FileNode.Objects)
	} else {
		newObjects = node.FileNode.Objects
	}
	if s.Footer != nil {
		newObjects = append(newObjects, s.Footer)
	}
	node.FileNode.Objects = newObjects
	node.SubtreeSize += s.HeaderSize + s.FooterSize
	node.Hash = hashtree.HashFileNode(node.FileNode)
	return nodeToFileInfo(ci, filePath, node, full), nil
}

func (d *driver) inspectFile(pachClient *client.APIClient, file *pfs.File) (fi *pfs.FileInfo, retErr error) {
	// Validate arguments
	if file == nil {
		return nil, errors.New("file cannot be nil")
	}
	if file.Commit == nil {
		return nil, errors.New("file commit cannot be nil")
	}
	if file.Commit.Repo == nil {
		return nil, errors.New("file commit repo cannot be nil")
	}

	if err := d.checkIsAuthorized(pachClient, file.Commit.Repo, auth.Scope_READER); err != nil {
		return nil, err
	}
	commitInfo, err := d.inspectCommit(pachClient, file.Commit, pfs.CommitState_STARTED)
	if err != nil {
		return nil, err
	}
	// Handle commits that use the old hashtree format.
	if !provenantOnInput(commitInfo.Provenance) || commitInfo.Tree != nil {
		tree, err := d.getTreeForFile(pachClient, file)
		if err != nil {
			return nil, err
		}
		defer destroyHashtree(tree)
		node, err := tree.Get(file.Path)
		if err != nil {
			return nil, pfsserver.ErrFileNotFound{file}
		}
		return nodeToFileInfoHeaderFooter(commitInfo, file.Path, node, tree, true)
	}
	// Handle commits that use the newer hashtree format.
	if commitInfo.Finished == nil {
		return nil, pfsserver.ErrOutputCommitNotFinished{commitInfo.Commit}
	}
	if commitInfo.Trees == nil {
		return nil, pfsserver.ErrFileNotFound{file}
	}
	rs, err := d.getTree(pachClient, commitInfo, file.Path)
	if err != nil {
		return nil, err
	}
	defer func() {
		for _, r := range rs {
			if err := r.Close(); err != nil && retErr != nil {
				retErr = err
			}
		}
	}()
	node, err := hashtree.Get(rs, file.Path)
	if err != nil {
		return nil, pfsserver.ErrFileNotFound{file}
	}
	return nodeToFileInfo(commitInfo, file.Path, node, true), nil
}

func (d *driver) listFile(pachClient *client.APIClient, file *pfs.File, full bool, history int64, f func(*pfs.FileInfo) error) (retErr error) {
	// Validate arguments
	if file == nil {
		return errors.New("file cannot be nil")
	}
	if file.Commit == nil {
		return errors.New("file commit cannot be nil")
	}
	if file.Commit.Repo == nil {
		return errors.New("file commit repo cannot be nil")
	}

	if err := d.checkIsAuthorized(pachClient, file.Commit.Repo, auth.Scope_READER); err != nil {
		return err
	}
	commitInfo, err := d.inspectCommit(pachClient, file.Commit, pfs.CommitState_STARTED)
	if err != nil {
		return err
	}
	g, err := globlib.Compile(file.Path, '/')
	if err != nil {
		// TODO this should be a MalformedGlob error like the hashtree returns
		return err
	}

	// Handle commits that use the old hashtree format.
	if !provenantOnInput(commitInfo.Provenance) || commitInfo.Tree != nil {
		tree, err := d.getTreeForFile(pachClient, client.NewFile(file.Commit.Repo.Name, file.Commit.ID, ""))
		if err != nil {
			return err
		}
		defer destroyHashtree(tree)
		return tree.Glob(file.Path, func(rootPath string, rootNode *hashtree.NodeProto) error {
			if rootNode.DirNode == nil {
				if history != 0 {
					return d.fileHistory(pachClient, client.NewFile(file.Commit.Repo.Name, file.Commit.ID, rootPath), history, f)
				}
				fi, err := nodeToFileInfoHeaderFooter(commitInfo, rootPath, rootNode, tree, full)
				if err != nil {
					return err
				}
				return f(fi)
			}
			return tree.List(rootPath, func(node *hashtree.NodeProto) error {
				path := filepath.Join(rootPath, node.Name)
				if g.Match(path) {
					// Don't return the file now, it will be returned later by Glob
					return nil
				}
				if history != 0 {
					return d.fileHistory(pachClient, client.NewFile(file.Commit.Repo.Name, file.Commit.ID, path), history, f)
				}
				fi, err := nodeToFileInfoHeaderFooter(commitInfo, path, node, tree, full)
				if err != nil {
					return err
				}
				return f(fi)
			})
		})
	}
	// Handle commits that use the newer hashtree format.
	if commitInfo.Finished == nil {
		return pfsserver.ErrOutputCommitNotFinished{commitInfo.Commit}
	}
	if commitInfo.Trees == nil {
		return nil
	}
	rs, err := d.getTrees(pachClient, commitInfo, file.Path)
	if err != nil {
		return err
	}
	defer func() {
		for _, r := range rs {
			if err := r.Close(); err != nil && retErr != nil {
				retErr = err
			}
		}
	}()
	return hashtree.List(rs, file.Path, func(path string, node *hashtree.NodeProto) error {
		if history != 0 {
			return d.fileHistory(pachClient, client.NewFile(file.Commit.Repo.Name, file.Commit.ID, path), history, f)
		}
		return f(nodeToFileInfo(commitInfo, path, node, full))
	})
}

// fileHistory calls f with FileInfos for the file, starting with how it looked
// at the referenced commit and then all past versions that are different.
func (d *driver) fileHistory(pachClient *client.APIClient, file *pfs.File, history int64, f func(*pfs.FileInfo) error) error {
	var fi *pfs.FileInfo
	for {
		_fi, err := d.inspectFile(pachClient, file)
		if err != nil {
			if _, ok := err.(pfsserver.ErrFileNotFound); ok {
				return f(fi)
			}
			return err
		}
		if fi != nil && !bytes.Equal(fi.Hash, _fi.Hash) {
			if err := f(fi); err != nil {
				return err
			}
			if history > 0 {
				history--
				if history == 0 {
					return nil
				}
			}
		}
		fi = _fi
		ci, err := d.inspectCommit(pachClient, file.Commit, pfs.CommitState_STARTED)
		if err != nil {
			return err
		}
		if ci.ParentCommit == nil {
			return f(fi)
		}
		file.Commit = ci.ParentCommit
	}
}

func (d *driver) walkFile(pachClient *client.APIClient, file *pfs.File, f func(*pfs.FileInfo) error) (retErr error) {
	// Validate arguments
	if file == nil {
		return errors.New("file cannot be nil")
	}
	if file.Commit == nil {
		return errors.New("file commit cannot be nil")
	}
	if file.Commit.Repo == nil {
		return errors.New("file commit repo cannot be nil")
	}

	if err := d.checkIsAuthorized(pachClient, file.Commit.Repo, auth.Scope_READER); err != nil {
		return err
	}
	commitInfo, err := d.inspectCommit(pachClient, file.Commit, pfs.CommitState_STARTED)
	if err != nil {
		return err
	}
	// Handle commits that use the old hashtree format.
	if !provenantOnInput(commitInfo.Provenance) || commitInfo.Tree != nil {
		tree, err := d.getTreeForFile(pachClient, client.NewFile(file.Commit.Repo.Name, file.Commit.ID, file.Path))
		if err != nil {
			return err
		}
		defer destroyHashtree(tree)
		return tree.Walk(file.Path, func(path string, node *hashtree.NodeProto) error {
			fi, err := nodeToFileInfoHeaderFooter(commitInfo, path, node, tree, false)
			if err != nil {
				return err
			}
			return f(fi)
		})
	}
	// Handle commits that use the newer hashtree format.
	if commitInfo.Finished == nil {
		return pfsserver.ErrOutputCommitNotFinished{commitInfo.Commit}
	}
	if commitInfo.Trees == nil {
		return nil
	}
	rs, err := d.getTrees(pachClient, commitInfo, file.Path)
	if err != nil {
		return err
	}
	defer func() {
		for _, r := range rs {
			if err := r.Close(); err != nil && retErr != nil {
				retErr = err
			}
		}
	}()
	return hashtree.Walk(rs, file.Path, func(path string, node *hashtree.NodeProto) error {
		return f(nodeToFileInfo(commitInfo, path, node, false))
	})
}

func (d *driver) globFile(pachClient *client.APIClient, commit *pfs.Commit, pattern string, f func(*pfs.FileInfo) error) (retErr error) {
	// Validate arguments
	if commit == nil {
		return errors.New("commit cannot be nil")
	}
	if commit.Repo == nil {
		return errors.New("commit repo cannot be nil")
	}

	if err := d.checkIsAuthorized(pachClient, commit.Repo, auth.Scope_READER); err != nil {
		return err
	}
	commitInfo, err := d.inspectCommit(pachClient, commit, pfs.CommitState_STARTED)
	if err != nil {
		return err
	}
	// Handle commits that use the old hashtree format.
	if !provenantOnInput(commitInfo.Provenance) || commitInfo.Tree != nil {
		tree, err := d.getTreeForFile(pachClient, client.NewFile(commit.Repo.Name, commit.ID, ""))
		if err != nil {
			return err
		}
		defer destroyHashtree(tree)
		globErr := tree.Glob(pattern, func(path string, node *hashtree.NodeProto) error {
			fi, err := nodeToFileInfoHeaderFooter(commitInfo, path, node, tree, false)
			if err != nil {
				return err
			}
			return f(fi)
		})
		if hashtree.Code(globErr) == hashtree.PathNotFound {
			// glob pattern is for a file that doesn't exist, no match
			return nil
		}
		return globErr
	}
	// Handle commits that use the newer hashtree format.
	if commitInfo.Finished == nil {
		return pfsserver.ErrOutputCommitNotFinished{commitInfo.Commit}
	}
	if commitInfo.Trees == nil {
		return nil
	}
	var rs []io.ReadCloser
	// Handles the case when looking for a specific file/directory
	if !hashtree.IsGlob(pattern) {
		rs, err = d.getTree(pachClient, commitInfo, pattern)
	} else {
		rs, err = d.getTrees(pachClient, commitInfo, pattern)
	}
	if err != nil {
		return err
	}
	defer func() {
		for _, r := range rs {
			if err := r.Close(); err != nil && retErr != nil {
				retErr = err
			}
		}
	}()
	return hashtree.Glob(rs, pattern, func(rootPath string, rootNode *hashtree.NodeProto) error {
		return f(nodeToFileInfo(commitInfo, rootPath, rootNode, false))
	})
}

func (d *driver) diffFile(pachClient *client.APIClient, newFile *pfs.File, oldFile *pfs.File, shallow bool) ([]*pfs.FileInfo, []*pfs.FileInfo, error) {
	// Validate arguments
	if newFile == nil {
		return nil, nil, errors.New("file cannot be nil")
	}
	if newFile.Commit == nil {
		return nil, nil, errors.New("file commit cannot be nil")
	}
	if newFile.Commit.Repo == nil {
		return nil, nil, errors.New("file commit repo cannot be nil")
	}

	// Do READER authorization check for both newFile and oldFile
	if oldFile != nil && oldFile.Commit != nil {
		if err := d.checkIsAuthorized(pachClient, oldFile.Commit.Repo, auth.Scope_READER); err != nil {
			return nil, nil, err
		}
	}
	if newFile != nil && newFile.Commit != nil {
		if err := d.checkIsAuthorized(pachClient, newFile.Commit.Repo, auth.Scope_READER); err != nil {
			return nil, nil, err
		}
	}
	newTree, err := d.getTreeForFile(pachClient, newFile)
	if err != nil {
		return nil, nil, err
	}
	defer destroyHashtree(newTree)
	newCommitInfo, err := d.inspectCommit(pachClient, newFile.Commit, pfs.CommitState_STARTED)
	if err != nil {
		return nil, nil, err
	}
	// if oldFile is nil we use the parent of newFile
	if oldFile == nil {
		oldFile = &pfs.File{}
		// ParentCommit may be nil, that's fine because getTreeForCommit
		// handles nil
		oldFile.Commit = newCommitInfo.ParentCommit
		oldFile.Path = newFile.Path
	}
	// `oldCommitInfo` may be nil. While `nodeToFileInfoHeaderFooter` called
	// below expects `oldCommitInfo` to not be nil, it's okay because
	// `newTree.Diff` won't call its callback unless `oldCommitInfo` is not
	// `nil`.
	var oldCommitInfo *pfs.CommitInfo
	if oldFile.Commit != nil {
		oldCommitInfo, err = d.inspectCommit(pachClient, oldFile.Commit, pfs.CommitState_STARTED)
		if err != nil {
			return nil, nil, err
		}
	}
	oldTree, err := d.getTreeForFile(pachClient, oldFile)
	if err != nil {
		return nil, nil, err
	}
	defer destroyHashtree(oldTree)
	var newFileInfos []*pfs.FileInfo
	var oldFileInfos []*pfs.FileInfo
	recursiveDepth := -1
	if shallow {
		recursiveDepth = 1
	}
	if err := newTree.Diff(oldTree, newFile.Path, oldFile.Path, int64(recursiveDepth), func(path string, node *hashtree.NodeProto, isNewFile bool) error {
		if isNewFile {
			fi, err := nodeToFileInfoHeaderFooter(newCommitInfo, path, node, newTree, false)
			if err != nil {
				return err
			}
			newFileInfos = append(newFileInfos, fi)
		} else {
			fi, err := nodeToFileInfoHeaderFooter(oldCommitInfo, path, node, oldTree, false)
			if err != nil {
				return err
			}
			oldFileInfos = append(oldFileInfos, fi)
		}
		return nil
	}); err != nil {
		return nil, nil, err
	}
	return newFileInfos, oldFileInfos, nil
}

func (d *driver) deleteFile(pachClient *client.APIClient, file *pfs.File) error {
	// Validate arguments
	if file == nil {
		return errors.New("file cannot be nil")
	}
	if file.Commit == nil {
		return errors.New("file commit cannot be nil")
	}
	if file.Commit.Repo == nil {
		return errors.New("file commit repo cannot be nil")
	}

	if err := d.checkIsAuthorized(pachClient, file.Commit.Repo, auth.Scope_WRITER); err != nil {
		return err
	}
	branch := ""
	if !uuid.IsUUIDWithoutDashes(file.Commit.ID) {
		branch = file.Commit.ID
	}
	commitInfo, err := d.inspectCommit(pachClient, file.Commit, pfs.CommitState_STARTED)
	if err != nil {
		return err
	}
	if commitInfo.Finished != nil {
		if branch == "" {
			return pfsserver.ErrCommitFinished{file.Commit}
		}
		return d.txnEnv.WithWriteContext(pachClient.Ctx(), func(txnCtx *txnenv.TransactionContext) error {
			_, err := d.makeCommit(txnCtx, "", client.NewCommit(file.Commit.Repo.Name, ""), branch, nil, nil, nil, nil, []string{file.Path}, []*pfs.PutFileRecords{&pfs.PutFileRecords{Tombstone: true}}, "", 0)
			return err
		})
	}

	return d.upsertPutFileRecords(pachClient, file, &pfs.PutFileRecords{Tombstone: true})
}

func (d *driver) deleteAll(txnCtx *txnenv.TransactionContext) error {
	// Note: d.listRepo() doesn't return the 'spec' repo, so it doesn't get
	// deleted here. Instead, PPS is responsible for deleting and re-creating it
	repoInfos, err := d.listRepo(txnCtx.Client, !includeAuth)
	if err != nil {
		return err
	}
	for _, repoInfo := range repoInfos.RepoInfo {
		if err := d.deleteRepo(txnCtx, repoInfo.Repo, true); err != nil && !auth.IsErrNotAuthorized(err) {
			return err
		}
	}
	return nil
}

// Put the tree into the blob store
// Only write the records to etcd if the commit does exist and is open.
// To check that a key exists in etcd, we assert that its CreateRevision
// is greater than zero.
func (d *driver) upsertPutFileRecords(
	pachClient *client.APIClient,
	file *pfs.File,
	newRecords *pfs.PutFileRecords,
) error {
	prefix, err := d.scratchFilePrefix(file)
	if err != nil {
		return err
	}

	ctx := pachClient.Ctx()
	_, err = col.NewSTM(ctx, d.etcdClient, func(stm col.STM) error {
		commitsCol := d.openCommits.ReadWrite(stm)
		var commit pfs.Commit
		err := commitsCol.Get(file.Commit.ID, &commit)
		if err != nil {
			return err
		}
		// Dumb check to make sure the unmarshalled value exists (and matches the current ID)
		// to denote that the current commit is indeed open
		if commit.ID != file.Commit.ID {
			return fmt.Errorf("commit %v is not open", file.Commit.ID)
		}
		recordsCol := d.putFileRecords.ReadWrite(stm)
		var existingRecords pfs.PutFileRecords
		return recordsCol.Upsert(prefix, &existingRecords, func() error {
			if newRecords.Tombstone {
				existingRecords.Tombstone = true
				existingRecords.Records = nil
			}
			existingRecords.Split = newRecords.Split
			existingRecords.Records = append(existingRecords.Records, newRecords.Records...)
			existingRecords.Header = newRecords.Header
			existingRecords.Footer = newRecords.Footer
			return nil
		})
	})
	return err
}

func (d *driver) applyWrite(key string, records *pfs.PutFileRecords, tree hashtree.HashTree) error {
	// a map that keeps track of the sizes of objects
	sizeMap := make(map[string]int64)

	if records.Tombstone {
		if err := tree.DeleteFile(key); err != nil {
			return err
		}
	}
	if !records.Split {
		if len(records.Records) == 0 {
			return nil
		}
		for _, record := range records.Records {
			sizeMap[record.ObjectHash] = record.SizeBytes
			if record.OverwriteIndex != nil {
				// Computing size delta
				delta := record.SizeBytes
				fileNode, err := tree.Get(key)
				if err == nil {
					// If we can't find the file, that's fine.
					for i := record.OverwriteIndex.Index; int(i) < len(fileNode.FileNode.Objects); i++ {
						delta -= sizeMap[fileNode.FileNode.Objects[i].Hash]
					}
				}

				if record.ObjectHash != "" {
					if err := tree.PutFileOverwrite(key, []*pfs.Object{{Hash: record.ObjectHash}}, record.OverwriteIndex, delta); err != nil {
						return err
					}
				} else if record.BlockRef != nil {
					if err := tree.PutFileOverwriteBlockRefs(key, []*pfs.BlockRef{record.BlockRef}, record.OverwriteIndex, delta); err != nil {
						return err
					}
				}
			} else {
				if record.ObjectHash != "" {
					if err := tree.PutFile(key, []*pfs.Object{{Hash: record.ObjectHash}}, record.SizeBytes); err != nil {
						return err
					}
				} else if record.BlockRef != nil {
					if err := tree.PutFileBlockRefs(key, []*pfs.BlockRef{record.BlockRef}, record.SizeBytes); err != nil {
						return err
					}
				}
			}
		}
	} else {
		nodes, err := tree.ListAll(key)
		if err != nil && hashtree.Code(err) != hashtree.PathNotFound {
			return err
		}
		var indexOffset int64
		if len(nodes) > 0 {
			indexOffset, err = strconv.ParseInt(path.Base(nodes[len(nodes)-1].Name), splitSuffixBase, splitSuffixWidth)
			if err != nil {
				return fmt.Errorf("error parsing filename %s as int, this likely means you're "+
					"using split on a directory which contains other data that wasn't put with split",
					path.Base(nodes[len(nodes)-1].Name))
			}
			indexOffset++ // start writing to the file after the last file
		}

		// Upsert parent directory w/ headers if needed
		// (hashtree.PutFileHeaderFooter requires it to already exist)
		if records.Header != nil || records.Footer != nil {
			var headerObj, footerObj *pfs.Object
			var headerSize, footerSize int64
			if records.Header != nil {
				headerObj = client.NewObject(records.Header.ObjectHash)
				headerSize = records.Header.SizeBytes
			}
			if records.Footer != nil {
				footerObj = client.NewObject(records.Footer.ObjectHash)
				footerSize = records.Footer.SizeBytes
			}
			if err := tree.PutDirHeaderFooter(
				key, headerObj, footerObj, headerSize, footerSize); err != nil {
				return err
			}
		}

		// Put individual objects into hashtree
		for i, record := range records.Records {
			if records.Header != nil || records.Footer != nil {
				if err := tree.PutFileHeaderFooter(
					path.Join(key, fmt.Sprintf(splitSuffixFmt, i+int(indexOffset))),
					[]*pfs.Object{{Hash: record.ObjectHash}}, record.SizeBytes); err != nil {
					return err
				}
			} else {
				if record.ObjectHash != "" {
					if err := tree.PutFile(path.Join(key, fmt.Sprintf(splitSuffixFmt, i+int(indexOffset))), []*pfs.Object{{Hash: record.ObjectHash}}, record.SizeBytes); err != nil {
						return err
					}
				} else if record.BlockRef != nil {
					if err := tree.PutFileBlockRefs(path.Join(key, fmt.Sprintf(splitSuffixFmt, i+int(indexOffset))), []*pfs.BlockRef{record.BlockRef}, record.SizeBytes); err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

func isNotFoundErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), "not found")
}

func isNoHeadErr(err error) bool {
	_, ok := err.(pfsserver.ErrNoHead)
	return ok
}

func commitKey(commit *pfs.Commit) string {
	return fmt.Sprintf("%s/%s", commit.Repo.Name, commit.ID)
}

func branchKey(branch *pfs.Branch) string {
	return fmt.Sprintf("%s/%s", branch.Repo.Name, branch.Name)
}

func (d *driver) addBranchProvenance(branchInfo *pfs.BranchInfo, provBranch *pfs.Branch, stm col.STM) error {
	if provBranch.Repo.Name == branchInfo.Branch.Repo.Name && provBranch.Name == branchInfo.Branch.Name {
		return fmt.Errorf("provenance loop, branch %s/%s cannot be provenance for itself", provBranch.Repo.Name, provBranch.Name)
	}
	add(&branchInfo.Provenance, provBranch)
	provBranchInfo := &pfs.BranchInfo{}
	if err := d.branches(provBranch.Repo.Name).ReadWrite(stm).Upsert(provBranch.Name, provBranchInfo, func() error {
		// Set provBranch, we may be creating this branch for the first time
		provBranchInfo.Name = provBranch.Name
		provBranchInfo.Branch = provBranch
		add(&provBranchInfo.Subvenance, branchInfo.Branch)
		return nil
	}); err != nil {
		return err
	}
	repoInfo := &pfs.RepoInfo{}
	return d.repos.ReadWrite(stm).Update(provBranch.Repo.Name, repoInfo, func() error {
		add(&repoInfo.Branches, provBranch)
		return nil
	})
}

func appendSubvenance(commitInfo *pfs.CommitInfo, subvCommitInfo *pfs.CommitInfo) {
	if subvCommitInfo.ParentCommit != nil {
		for _, subvCommitRange := range commitInfo.Subvenance {
			if subvCommitRange.Upper.ID == subvCommitInfo.ParentCommit.ID {
				subvCommitRange.Upper = subvCommitInfo.Commit
				return
			}
		}
	}
	commitInfo.Subvenance = append(commitInfo.Subvenance, &pfs.CommitRange{
		Lower: subvCommitInfo.Commit,
		Upper: subvCommitInfo.Commit,
	})
	commitInfo.SubvenantCommitsTotal++
}

type branchSet []*pfs.Branch

func (b *branchSet) search(branch *pfs.Branch) (int, bool) {
	key := branchKey(branch)
	i := sort.Search(len(*b), func(i int) bool {
		return branchKey((*b)[i]) >= key
	})
	if i == len(*b) {
		return i, false
	}
	return i, branchKey((*b)[i]) == branchKey(branch)
}

func (b *branchSet) add(branch *pfs.Branch) {
	i, ok := b.search(branch)
	if !ok {
		*b = append(*b, nil)
		copy((*b)[i+1:], (*b)[i:])
		(*b)[i] = branch
	}
}

func add(bs *[]*pfs.Branch, branch *pfs.Branch) {
	(*branchSet)(bs).add(branch)
}

func (b *branchSet) del(branch *pfs.Branch) {
	i, ok := b.search(branch)
	if ok {
		copy((*b)[i:], (*b)[i+1:])
		(*b)[len((*b))-1] = nil
		*b = (*b)[:len((*b))-1]
	}
}

func del(bs *[]*pfs.Branch, branch *pfs.Branch) {
	(*branchSet)(bs).del(branch)
}

func (b *branchSet) has(branch *pfs.Branch) bool {
	_, ok := b.search(branch)
	return ok
}

func has(bs *[]*pfs.Branch, branch *pfs.Branch) bool {
	return (*branchSet)(bs).has(branch)
}

type putFileServer struct {
	pfs.API_PutFileServer
	req *pfs.PutFileRequest
}

func newPutFileServer(s pfs.API_PutFileServer) *putFileServer {
	return &putFileServer{API_PutFileServer: s}
}

func (s *putFileServer) Recv() (*pfs.PutFileRequest, error) {
	if s.req != nil {
		req := s.req
		s.req = nil
		return req, nil
	}
	return s.API_PutFileServer.Recv()
}

func (s *putFileServer) Peek() (*pfs.PutFileRequest, error) {
	if s.req != nil {
		return s.req, nil
	}
	req, err := s.Recv()
	if err != nil {
		return nil, err
	}
	s.req = req
	return req, nil
}

func (d *driver) forEachPutFile(pachClient *client.APIClient, server pfs.API_PutFileServer, f func(*pfs.PutFileRequest, io.Reader) error) (oneOff bool, repo string, branch string, err error) {
	limiter := limit.New(client.DefaultMaxConcurrentStreams)
	var pr *io.PipeReader
	var pw *io.PipeWriter
	var req *pfs.PutFileRequest
	var eg errgroup.Group
	var rawCommitID string
	var commitID string

	for req, err = server.Recv(); err == nil; req, err = server.Recv() {
		req := req
		if req.File != nil {
			if req.File.Commit == nil {
				return false, "", "", errors.New("file commit cannot be nil")
			}
			if req.File.Commit.Repo == nil {
				return false, "", "", errors.New("file commit repo cannot be nil")
			}

			// For the first file, dereference the commit ID if needed. For
			// subsequent files, ensure that they're all referencing the same
			// commit ID, and replace their commit objects to all refer to the
			// same thing.
			if rawCommitID == "" {
				commit := req.File.Commit
				repo = commit.Repo.Name
				// The non-dereferenced commit ID. Used to ensure that all
				// subsequent requests use the same value.
				rawCommitID = commit.ID
				// inspectCommit will replace file.Commit.ID with an actual
				// commit ID if it's a branch. So we want to save it first.
				if !uuid.IsUUIDWithoutDashes(commit.ID) {
					branch = commit.ID
				}
				// inspect the commit where we're adding files and figure out
				// if this is a one-off 'put file'.
				// - if 'commit' refers to an open commit                -> not oneOff
				// - otherwise (i.e. branch with closed HEAD or no HEAD) -> yes oneOff
				// Note that if commit is a specific commit ID, it must be
				// open for this call to succeed
				commitInfo, err := d.inspectCommit(pachClient, commit, pfs.CommitState_STARTED)
				if err != nil {
					if (!isNotFoundErr(err) && !isNoHeadErr(err)) || branch == "" {
						return false, "", "", err
					}
					oneOff = true
				}
				if commitInfo != nil && commitInfo.Finished != nil {
					if branch == "" {
						return false, "", "", pfsserver.ErrCommitFinished{commit}
					}
					oneOff = true
				}
				commitID = commit.ID
			} else if req.File.Commit.ID != rawCommitID {
				err = fmt.Errorf("all requests in a put files call must have the same commit ID; expected '%s', got '%s'", rawCommitID, req.File.Commit.ID)
				return false, "", "", err
			} else if req.File.Commit.Repo.Name != repo {
				err = fmt.Errorf("all requests in a put files call must have the same repo name; expected '%s', got '%s'", repo, req.File.Commit.Repo.Name)
				return false, "", "", err
			} else {
				req.File.Commit.ID = commitID
			}

			if req.Url != "" {
				url, err := url.Parse(req.Url)
				if err != nil {
					return false, "", "", err
				}
				switch url.Scheme {
				case "http":
					fallthrough
				case "https":
					limiter.Acquire()
					resp, err := http.Get(req.Url)
					if err != nil {
						return false, "", "", err
					} else if resp.StatusCode >= 400 {
						return false, "", "", fmt.Errorf("error retrieving content from %q: %s", req.Url, resp.Status)
					}
					eg.Go(func() (retErr error) {
						defer limiter.Release()
						defer func() {
							if err := resp.Body.Close(); err != nil && retErr == nil {
								retErr = err
							}
						}()
						return f(req, resp.Body)
					})
				default:
					url, err := obj.ParseURL(req.Url)
					if err != nil {
						err = fmt.Errorf("error parsing url %v: %v", req.Url, err)
						return false, "", "", err
					}
					objClient, err := obj.NewClientFromURLAndSecret(url, false)
					if err != nil {
						return false, "", "", err
					}
					if req.Recursive {
						path := strings.TrimPrefix(url.Object, "/")
						if err := objClient.Walk(server.Context(), path, func(name string) error {
							if strings.HasSuffix(name, "/") {
								// Creating a file with a "/" suffix breaks
								// pfs' directory model, so we don't
								logrus.Warnf("ambiguous key %v, not creating a directory or putting this entry as a file", name)
							}
							req := *req // copy req so we can make changes
							req.File = client.NewFile(req.File.Commit.Repo.Name, req.File.Commit.ID, filepath.Join(req.File.Path, strings.TrimPrefix(name, path)))
							limiter.Acquire()
							r, err := objClient.Reader(server.Context(), name, 0, 0)
							if err != nil {
								return err
							}
							eg.Go(func() (retErr error) {
								defer limiter.Release()
								defer func() {
									if err := r.Close(); err != nil && retErr == nil {
										retErr = err
									}
								}()
								return f(&req, r)
							})
							return nil
						}); err != nil {
							return false, "", "", err
						}
					} else {
						limiter.Acquire()
						r, err := objClient.Reader(server.Context(), url.Object, 0, 0)
						if err != nil {
							return false, "", "", err
						}
						eg.Go(func() (retErr error) {
							defer limiter.Release()
							defer func() {
								if err := r.Close(); err != nil && retErr == nil {
									retErr = err
								}
							}()
							return f(req, r)
						})
					}
				}
				continue
			}
			// Close the previous 'put file' if there is one
			if pw != nil {
				pw.Close() // can't error
			}
			pr, pw = io.Pipe()
			pr := pr
			limiter.Acquire()
			eg.Go(func() error {
				defer limiter.Release()
				if err := f(req, pr); err != nil {
					// needed so the parent goroutine doesn't block
					pr.CloseWithError(err)
					return err
				}
				return nil
			})
		}
		if pw == nil {
			return false, "", "", errors.New("must send a request with a file first")
		}
		if _, err := pw.Write(req.Value); err != nil {
			return false, "", "", err
		}
	}
	if pw != nil {
		// This may pass io.EOF to CloseWithError but that's equivalent to
		// simply calling Close()
		pw.CloseWithError(err) // can't error
	}
	if err != io.EOF {
		return false, "", "", err
	}
	err = eg.Wait()
	return oneOff, repo, branch, err
}

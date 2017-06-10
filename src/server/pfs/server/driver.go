package server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/pachyderm/pachyderm/src/client"
	"github.com/pachyderm/pachyderm/src/client/pfs"
	"github.com/pachyderm/pachyderm/src/client/pkg/grpcutil"
	"github.com/pachyderm/pachyderm/src/client/pkg/uuid"
	pfsserver "github.com/pachyderm/pachyderm/src/server/pfs"
	col "github.com/pachyderm/pachyderm/src/server/pkg/collection"
	"github.com/pachyderm/pachyderm/src/server/pkg/hashtree"

	etcd "github.com/coreos/etcd/clientv3"
	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	"github.com/hashicorp/golang-lru"
	"google.golang.org/grpc"
)

const (
	splitSuffixBase  = 16
	splitSuffixWidth = 64
	splitSuffixFmt   = "%016x"
)

// ValidateRepoName determines if a repo name is valid
func ValidateRepoName(name string) error {
	match, _ := regexp.MatchString("^[a-zA-Z0-9_-]+$", name)

	if !match {
		return fmt.Errorf("repo name (%v) invalid: only alphanumeric characters, underscores, and dashes are allowed", name)
	}

	return nil
}

// ListFileMode specifies how ListFile executes.
type ListFileMode int

const (
	// ListFileNORMAL computes sizes for files but not for directories
	ListFileNORMAL ListFileMode = iota
	// ListFileFAST does not compute sizes for files or directories
	ListFileFAST
	// ListFileRECURSE computes sizes for files and directories
	ListFileRECURSE
)

// IsPermissionError returns true if a given error is a permission error.
func IsPermissionError(err error) bool {
	return strings.Contains(err.Error(), "has already finished")
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
	address      string
	pachConnOnce sync.Once
	pachConn     *grpc.ClientConn
	etcdClient   *etcd.Client
	prefix       string

	// collections
	repos         col.Collection
	repoRefCounts col.Collection
	commits       collectionFactory
	branches      collectionFactory

	// a cache for commit IDs that we know exist
	commitCache *lru.Cache
	// a cache for hashtrees
	treeCache *lru.Cache
}

const (
	tombstone = "delete"
)

// Instead of making the user specify the respective size for each cache,
// we decide internally how to split cache space among different caches.
//
// Each value specifies a percentage of the total cache space to be used.
const (
	commitCachePercentage = 0.05
	treeCachePercentage   = 0.95

	// by default we use 1GB of RAM for cache
	defaultCacheSize = 1024 * 1024
)

// collection prefixes
const (
	reposPrefix         = "/repos"
	repoRefCountsPrefix = "/repoRefCounts"
	commitsPrefix       = "/commits"
	branchesPrefix      = "/branches"
)

var (
	provenanceIndex = col.Index{
		Field: "Provenance",
		Multi: true,
	}
)

// newDriver is used to create a new Driver instance
func newDriver(address string, etcdAddresses []string, etcdPrefix string, cacheBytes int64) (*driver, error) {
	etcdClient, err := etcd.New(etcd.Config{
		Endpoints:   etcdAddresses,
		DialOptions: client.EtcdDialOptions(),
	})
	if err != nil {
		return nil, err
	}

	commitCache, err := lru.New(int(float64(cacheBytes) * commitCachePercentage))
	if err != nil {
		return nil, err
	}
	treeCache, err := lru.New(int(float64(cacheBytes) * treeCachePercentage))
	if err != nil {
		return nil, err
	}

	return &driver{
		address:    address,
		etcdClient: etcdClient,
		prefix:     etcdPrefix,
		repos: col.NewCollection(
			etcdClient,
			path.Join(etcdPrefix, reposPrefix),
			[]col.Index{provenanceIndex},
			&pfs.RepoInfo{},
		),
		repoRefCounts: col.NewCollection(
			etcdClient,
			path.Join(etcdPrefix, repoRefCountsPrefix),
			nil,
			nil,
		),
		commits: func(repo string) col.Collection {
			return col.NewCollection(
				etcdClient,
				path.Join(etcdPrefix, commitsPrefix, repo),
				[]col.Index{provenanceIndex},
				&pfs.CommitInfo{},
			)
		},
		branches: func(repo string) col.Collection {
			return col.NewCollection(
				etcdClient,
				path.Join(etcdPrefix, branchesPrefix, repo),
				nil,
				&pfs.Commit{},
			)
		},
		commitCache: commitCache,
		treeCache:   treeCache,
	}, nil
}

// newLocalDriver creates a driver using an local etcd instance.  This
// function is intended for testing purposes
func newLocalDriver(blockAddress string, etcdPrefix string) (*driver, error) {
	return newDriver(blockAddress, []string{"localhost:32379"}, etcdPrefix, defaultCacheSize)
}

func (d *driver) getObjectClient() (*client.APIClient, error) {
	if d.pachConn == nil {
		var onceErr error
		d.pachConnOnce.Do(func() {
			pachConn, err := grpc.Dial(d.address, client.PachDialOptions()...)
			if err != nil {
				onceErr = err
			}
			d.pachConn = pachConn
		})
		if onceErr != nil {
			return nil, onceErr
		}
	}
	return &client.APIClient{ObjectAPIClient: pfs.NewObjectAPIClient(d.pachConn)}, nil
}

func now() *types.Timestamp {
	t, err := types.TimestampProto(time.Now())
	if err != nil {
		panic(err)
	}
	return t
}

func present(key string) etcd.Cmp {
	return etcd.Compare(etcd.CreateRevision(key), ">", 0)
}

func absent(key string) etcd.Cmp {
	return etcd.Compare(etcd.CreateRevision(key), "=", 0)
}

func (d *driver) createRepo(ctx context.Context, repo *pfs.Repo, provenance []*pfs.Repo, description string) error {
	if err := ValidateRepoName(repo.Name); err != nil {
		return err
	}

	_, err := col.NewSTM(ctx, d.etcdClient, func(stm col.STM) error {
		repos := d.repos.ReadWrite(stm)
		repoRefCounts := d.repoRefCounts.ReadWriteInt(stm)

		// compute the full provenance of this repo
		fullProv := make(map[string]bool)
		for _, prov := range provenance {
			fullProv[prov.Name] = true
			provRepo := new(pfs.RepoInfo)
			if err := repos.Get(prov.Name, provRepo); err != nil {
				return err
			}
			// the provenance of my provenance is my provenance
			for _, prov := range provRepo.Provenance {
				fullProv[prov.Name] = true
			}
		}

		var fullProvRepos []*pfs.Repo
		for prov := range fullProv {
			fullProvRepos = append(fullProvRepos, &pfs.Repo{prov})
			if err := repoRefCounts.Increment(prov); err != nil {
				return err
			}
		}

		if err := repoRefCounts.Create(repo.Name, 0); err != nil {
			return err
		}

		repoInfo := &pfs.RepoInfo{
			Repo:        repo,
			Created:     now(),
			Provenance:  fullProvRepos,
			Description: description,
		}
		return repos.Create(repo.Name, repoInfo)
	})
	return err
}

func (d *driver) inspectRepo(ctx context.Context, repo *pfs.Repo) (*pfs.RepoInfo, error) {
	repoInfo := new(pfs.RepoInfo)
	if err := d.repos.ReadOnly(ctx).Get(repo.Name, repoInfo); err != nil {
		return nil, err
	}
	return repoInfo, nil
}

func (d *driver) listRepo(ctx context.Context, provenance []*pfs.Repo) ([]*pfs.RepoInfo, error) {
	var result []*pfs.RepoInfo
	repos := d.repos.ReadOnly(ctx)
	// Ensure that all provenance repos exist
	for _, prov := range provenance {
		repoInfo := new(pfs.RepoInfo)
		if err := repos.Get(prov.Name, repoInfo); err != nil {
			return nil, err
		}
	}

	iterator, err := repos.List()
	if err != nil {
		return nil, err
	}
nextRepo:
	for {
		repoName, repoInfo := "", new(pfs.RepoInfo)
		ok, err := iterator.Next(&repoName, repoInfo)
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		// A repo needs to have *all* the given repos as provenance
		// in order to be included in the result.
		for _, reqProv := range provenance {
			var matched bool
			for _, prov := range repoInfo.Provenance {
				if reqProv.Name == prov.Name {
					matched = true
				}
			}
			if !matched {
				continue nextRepo
			}
		}
		result = append(result, repoInfo)
	}
	return result, nil
}

func (d *driver) deleteRepo(ctx context.Context, repo *pfs.Repo, force bool) error {
	_, err := col.NewSTM(ctx, d.etcdClient, func(stm col.STM) error {
		repos := d.repos.ReadWrite(stm)
		repoRefCounts := d.repoRefCounts.ReadWriteInt(stm)
		commits := d.commits(repo.Name).ReadWrite(stm)
		branches := d.branches(repo.Name).ReadWrite(stm)

		// Check if this repo is the provenance of some other repos
		if !force {
			refCount, err := repoRefCounts.Get(repo.Name)
			if err != nil {
				return err
			}
			if refCount != 0 {
				return fmt.Errorf("cannot delete the provenance of other repos")
			}
		}

		repoInfo := new(pfs.RepoInfo)
		if err := repos.Get(repo.Name, repoInfo); err != nil {
			return err
		}
		for _, prov := range repoInfo.Provenance {
			if err := repoRefCounts.Decrement(prov.Name); err != nil {
				// Skip NotFound error, because it's possible that the
				// provenance repo has been deleted via --force.
				if _, ok := err.(col.ErrNotFound); !ok {
					return err
				}
			}
		}

		if err := repos.Delete(repo.Name); err != nil {
			return err
		}
		if err := repoRefCounts.Delete(repo.Name); err != nil {
			return err
		}
		commits.DeleteAll()
		branches.DeleteAll()
		return nil
	})
	return err
}

func (d *driver) startCommit(ctx context.Context, parent *pfs.Commit, branch string, provenance []*pfs.Commit) (*pfs.Commit, error) {
	return d.makeCommit(ctx, parent, branch, provenance, nil)
}

func (d *driver) buildCommit(ctx context.Context, parent *pfs.Commit, branch string, provenance []*pfs.Commit, tree *pfs.Object) (*pfs.Commit, error) {
	return d.makeCommit(ctx, parent, branch, provenance, tree)
}

func (d *driver) makeCommit(ctx context.Context, parent *pfs.Commit, branch string, provenance []*pfs.Commit, treeRef *pfs.Object) (*pfs.Commit, error) {
	if parent == nil {
		return nil, fmt.Errorf("parent cannot be nil")
	}
	commit := &pfs.Commit{
		Repo: parent.Repo,
		ID:   uuid.NewWithoutDashes(),
	}
	var commitSize uint64
	if treeRef != nil {
		objClient, err := d.getObjectClient()
		if err != nil {
			return nil, err
		}
		var buf bytes.Buffer
		if err := objClient.GetObject(treeRef.Hash, &buf); err != nil {
			return nil, err
		}
		tree, err := hashtree.Deserialize(buf.Bytes())
		if err != nil {
			return nil, err
		}
		commitSize = uint64(tree.Size())
	}
	if _, err := col.NewSTM(ctx, d.etcdClient, func(stm col.STM) error {
		repos := d.repos.ReadWrite(stm)
		commits := d.commits(parent.Repo.Name).ReadWrite(stm)
		branches := d.branches(parent.Repo.Name).ReadWrite(stm)

		// Check if repo exists
		repoInfo := new(pfs.RepoInfo)
		if err := repos.Get(parent.Repo.Name, repoInfo); err != nil {
			return err
		}

		commitInfo := &pfs.CommitInfo{
			Commit:  commit,
			Started: now(),
		}

		// Use a map to de-dup provenance
		provenanceMap := make(map[string]*pfs.Commit)
		// Build the full provenance; my provenance's provenance is
		// my provenance
		for _, prov := range provenance {
			provCommits := d.commits(prov.Repo.Name).ReadWrite(stm)
			provCommitInfo := new(pfs.CommitInfo)
			if err := provCommits.Get(prov.ID, provCommitInfo); err != nil {
				return err
			}
			for _, c := range provCommitInfo.Provenance {
				provenanceMap[c.ID] = c
			}
		}
		// finally include the given provenance
		for _, c := range provenance {
			provenanceMap[c.ID] = c
		}

		for _, c := range provenanceMap {
			commitInfo.Provenance = append(commitInfo.Provenance, c)
		}

		if branch != "" {
			// If we don't have an explicit parent we use the previous head of
			// branch as the parent, if it exists.
			if parent.ID == "" {
				head := new(pfs.Commit)
				if err := branches.Get(branch, head); err != nil {
					if _, ok := err.(col.ErrNotFound); !ok {
						return err
					}
				} else {
					parent.ID = head.ID
				}
			}
			// Make commit the new head of the branch
			branches.Put(branch, commit)
		}
		if parent.ID != "" {
			parentCommitInfo, err := d.inspectCommit(ctx, parent)
			if err != nil {
				return err
			}
			// fail if the parent commit has not been finished
			if parentCommitInfo.Finished == nil {
				return fmt.Errorf("parent commit %s has not been finished", parent.ID)
			}
			commitInfo.ParentCommit = parent
		}
		if treeRef != nil {
			commitInfo.Tree = treeRef
			commitInfo.SizeBytes = commitSize
			commitInfo.Finished = now()
			repoInfo.SizeBytes += commitSize
			repos.Put(parent.Repo.Name, repoInfo)
		}
		return commits.Create(commit.ID, commitInfo)
	}); err != nil {
		return nil, err
	}

	return commit, nil
}

func (d *driver) finishCommit(ctx context.Context, commit *pfs.Commit) error {
	commitInfo, err := d.inspectCommit(ctx, commit)
	if err != nil {
		return err
	}

	prefix, err := d.scratchCommitPrefix(ctx, commit)
	if err != nil {
		return err
	}

	// Read everything under the scratch space for this commit
	resp, err := d.etcdClient.Get(ctx, prefix, etcd.WithPrefix(), etcd.WithSort(etcd.SortByModRevision, etcd.SortAscend))
	if err != nil {
		return err
	}

	if commitInfo.Finished != nil {
		return fmt.Errorf("commit %s has already been finished", commit.FullID())
	}

	parentTree, err := d.getTreeForCommit(ctx, commitInfo.ParentCommit)
	if err != nil {
		return err
	}
	tree := parentTree.Open()

	for _, kv := range resp.Kvs {
		// fileStr is going to look like "some/path/UUID"
		fileStr := strings.TrimPrefix(string(kv.Key), prefix)
		// the last element of `parts` is going to be UUID
		parts := strings.Split(fileStr, "/")
		// filePath should look like "some/path"
		filePath := strings.Join(parts[:len(parts)-1], "/")

		if string(kv.Value) == tombstone {
			if err := tree.DeleteFile(filePath); err != nil {
				// Deleting a non-existent file in an open commit should
				// be a no-op
				if hashtree.Code(err) != hashtree.PathNotFound {
					return err
				}
			}
		} else {
			records := &PutFileRecords{}
			if err := proto.Unmarshal(kv.Value, records); err != nil {
				return err
			}
			if !records.Split {
				if len(records.Records) != 1 {
					return fmt.Errorf("unexpect %d length PutFileRecord (this is likely a bug)", len(records.Records))
				}
				if err := tree.PutFile(filePath, []*pfs.Object{{Hash: records.Records[0].ObjectHash}}, records.Records[0].SizeBytes); err != nil {
					return err
				}
			} else {
				nodes, err := tree.List(filePath)
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
				for i, record := range records.Records {
					if err := tree.PutFile(path.Join(filePath, fmt.Sprintf(splitSuffixFmt, i+int(indexOffset))), []*pfs.Object{{Hash: record.ObjectHash}}, record.SizeBytes); err != nil {
						return err
					}
				}
			}
		}
	}

	finishedTree, err := tree.Finish()
	if err != nil {
		return err
	}
	// Serialize the tree
	data, err := hashtree.Serialize(finishedTree)
	if err != nil {
		return err
	}

	if len(data) > 0 {
		// Put the tree into the blob store
		objClient, err := d.getObjectClient()
		if err != nil {
			return err
		}

		obj, _, err := objClient.PutObject(bytes.NewReader(data))
		if err != nil {
			return err
		}

		commitInfo.Tree = obj
	}

	commitInfo.SizeBytes = uint64(finishedTree.Size())
	commitInfo.Finished = now()

	_, err = col.NewSTM(ctx, d.etcdClient, func(stm col.STM) error {
		commits := d.commits(commit.Repo.Name).ReadWrite(stm)
		repos := d.repos.ReadWrite(stm)

		commits.Put(commit.ID, commitInfo)
		// update repo size
		repoInfo := new(pfs.RepoInfo)
		if err := repos.Get(commit.Repo.Name, repoInfo); err != nil {
			return err
		}

		// Increment the repo sizes by the sizes of the files that have
		// been added in this commit.
		finishedTree.Diff(parentTree, "", "", func(path string, node *hashtree.NodeProto, new bool) error {
			if node.FileNode != nil && new {
				repoInfo.SizeBytes += uint64(node.SubtreeSize)
			}
			return nil
		})
		repos.Put(commit.Repo.Name, repoInfo)
		return nil
	})
	if err != nil {
		return err
	}

	// Delete the scratch space for this commit
	_, err = d.etcdClient.Delete(ctx, prefix, etcd.WithPrefix())
	return err
}

// inspectCommit takes a Commit and returns the corresponding CommitInfo.
//
// As a side effect, it sets the commit ID to the real commit ID, if the
// original commit ID is actually a branch.
//
// This side effect is used internally by other APIs to resolve branch
// names to real commit IDs.
func (d *driver) inspectCommit(ctx context.Context, commit *pfs.Commit) (*pfs.CommitInfo, error) {
	if commit == nil {
		return nil, fmt.Errorf("cannot inspect nil commit")
	}
	_, err := col.NewSTM(ctx, d.etcdClient, func(stm col.STM) error {
		branches := d.branches(commit.Repo.Name).ReadWrite(stm)

		head := new(pfs.Commit)
		// See if we are given a branch
		if err := branches.Get(commit.ID, head); err != nil {
			if _, ok := err.(col.ErrNotFound); !ok {
				return err
			}
			// If it's not a branch, use it as it is
			return nil
		}
		commit.ID = head.ID
		return nil
	})
	if err != nil {
		return nil, err
	}

	commits := d.commits(commit.Repo.Name).ReadOnly(ctx)
	commitInfo := &pfs.CommitInfo{}
	if err := commits.Get(commit.ID, commitInfo); err != nil {
		return nil, err
	}
	return commitInfo, nil
}

func (d *driver) listCommit(ctx context.Context, repo *pfs.Repo, to *pfs.Commit, from *pfs.Commit, number uint64) ([]*pfs.CommitInfo, error) {
	if from != nil && from.Repo.Name != repo.Name || to != nil && to.Repo.Name != repo.Name {
		return nil, fmt.Errorf("`from` and `to` commits need to be from repo %s", repo.Name)
	}

	// Make sure that the repo exists
	_, err := d.inspectRepo(ctx, repo)
	if err != nil {
		return nil, err
	}

	// Make sure that both from and to are valid commits
	if from != nil {
		if _, err := d.inspectCommit(ctx, from); err != nil {
			return nil, err
		}
	}
	if to != nil {
		if _, err := d.inspectCommit(ctx, to); err != nil {
			return nil, err
		}
	}

	// if number is 0, we return all commits that match the criteria
	if number == 0 {
		number = math.MaxUint64
	}
	var commitInfos []*pfs.CommitInfo
	commits := d.commits(repo.Name).ReadOnly(ctx)

	if from != nil && to == nil {
		return nil, fmt.Errorf("cannot use `from` commit without `to` commit")
	} else if from == nil && to == nil {
		// if neither from and to is given, we list all commits in
		// the repo, sorted by revision timestamp
		iterator, err := commits.List()
		if err != nil {
			return nil, err
		}
		var commitID string
		for number != 0 {
			var commitInfo pfs.CommitInfo
			ok, err := iterator.Next(&commitID, &commitInfo)
			if err != nil {
				return nil, err
			}
			if !ok {
				break
			}
			commitInfos = append(commitInfos, &commitInfo)
			number--
		}
	} else {
		cursor := to
		for number != 0 && cursor != nil && (from == nil || cursor.ID != from.ID) {
			var commitInfo pfs.CommitInfo
			if err := commits.Get(cursor.ID, &commitInfo); err != nil {
				return nil, err
			}
			commitInfos = append(commitInfos, &commitInfo)
			cursor = commitInfo.ParentCommit
			number--
		}
	}
	return commitInfos, nil
}

type commitStream struct {
	stream chan CommitEvent
	done   chan struct{}
}

func (c *commitStream) Stream() <-chan CommitEvent {
	return c.stream
}

func (c *commitStream) Close() {
	close(c.done)
}

func (d *driver) subscribeCommit(ctx context.Context, repo *pfs.Repo, branch string, from *pfs.Commit, f func(commitInfo *pfs.CommitInfo) error) error {
	if from != nil && from.Repo.Name != repo.Name {
		return fmt.Errorf("the `from` commit needs to be from repo %s", repo.Name)
	}

	// We need to watch for new commits before we start listing commits,
	// because otherwise we might miss some commits in between when we
	// finish listing and when we start watching.
	branches := d.branches(repo.Name).ReadOnly(ctx)
	newCommitWatcher, err := branches.WatchOne(branch)
	if err != nil {
		return err
	}
	defer newCommitWatcher.Close()

	// keep track of the commits that have been sent
	seen := make(map[string]bool)

	commitInfos, err := d.listCommit(ctx, repo, &pfs.Commit{
		Repo: repo,
		ID:   branch,
	}, from, 0)
	if err != nil {
		// We skip NotFound error because it's ok if the branch
		// doesn't exist yet, in which case ListCommit returns
		// a NotFound error.
		if !isNotFoundErr(err) {
			return err
		}
	}
	for i := range commitInfos {
		commitInfo := commitInfos[len(commitInfos)-i-1]
		if commitInfo.Finished != nil {
			if err := f(commitInfo); err != nil {
				return err
			}
			seen[commitInfo.Commit.ID] = true
		}
	}

	commits := d.commits(repo.Name).ReadOnly(ctx)
	for {
		event := <-newCommitWatcher.Watch()
		if event.Err != nil {
			return event.Err
		}
		var branchName string
		commit := &pfs.Commit{}
		event.Unmarshal(&branchName, commit)
		if seen[commit.ID] {
			continue
		}
		// Now we watch the commit to see when it's finished
		if err := func() error {
			commitInfoWatcher, err := commits.WatchOne(commit.ID)
			if err != nil {
				return err
			}
			defer commitInfoWatcher.Close()
			for {
				event := <-commitInfoWatcher.Watch()
				if event.Err != nil {
					return event.Err
				}
				var commitID string
				commitInfo := &pfs.CommitInfo{}
				event.Unmarshal(&commitID, commitInfo)
				if commitInfo.Finished != nil {
					seen[commitInfo.Commit.ID] = true
					if err := f(commitInfo); err != nil {
						return err
					}
					return nil
				}
			}
		}(); err != nil {
			return err
		}
	}
}

func (d *driver) deleteCommit(ctx context.Context, commit *pfs.Commit) error {
	commitInfo, err := d.inspectCommit(ctx, commit)
	if err != nil {
		return err
	}

	if commitInfo.Finished != nil {
		return fmt.Errorf("cannot delete finished commit")
	}

	// Delete the scratch space for this commit
	prefix, err := d.scratchCommitPrefix(ctx, commit)
	if err != nil {
		return err
	}
	_, err = d.etcdClient.Delete(ctx, prefix, etcd.WithPrefix())
	if err != nil {
		return err
	}

	// If this commit is the head of a branch, make the commit's parent
	// the head instead.
	branches, err := d.listBranch(ctx, commit.Repo)
	if err != nil {
		return err
	}

	for _, branch := range branches {
		if branch.Head.ID == commitInfo.Commit.ID {
			if commitInfo.ParentCommit != nil {
				if err := d.setBranch(ctx, commitInfo.ParentCommit, branch.Name); err != nil {
					return err
				}
			} else {
				// If this commit doesn't have a parent, delete the branch
				if err := d.deleteBranch(ctx, commit.Repo, branch.Name); err != nil {
					return err
				}
			}
		}
	}

	// Delete the commit itself and subtract the size of the commit
	// from repo size.
	_, err = col.NewSTM(ctx, d.etcdClient, func(stm col.STM) error {
		repos := d.repos.ReadWrite(stm)
		repoInfo := new(pfs.RepoInfo)
		if err := repos.Get(commit.Repo.Name, repoInfo); err != nil {
			return err
		}
		repoInfo.SizeBytes -= commitInfo.SizeBytes
		repos.Put(commit.Repo.Name, repoInfo)

		commits := d.commits(commit.Repo.Name).ReadWrite(stm)
		return commits.Delete(commit.ID)
	})

	return err
}

func (d *driver) listBranch(ctx context.Context, repo *pfs.Repo) ([]*pfs.Branch, error) {
	branches := d.branches(repo.Name).ReadOnly(ctx)
	iterator, err := branches.List()
	if err != nil {
		return nil, err
	}

	var res []*pfs.Branch
	for {
		var branchName string
		head := new(pfs.Commit)
		ok, err := iterator.Next(&branchName, head)
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		res = append(res, &pfs.Branch{
			Name: path.Base(branchName),
			Head: head,
		})
	}
	return res, nil
}

func (d *driver) setBranch(ctx context.Context, commit *pfs.Commit, name string) error {
	if _, err := d.inspectCommit(ctx, commit); err != nil {
		return err
	}
	_, err := col.NewSTM(ctx, d.etcdClient, func(stm col.STM) error {
		commits := d.commits(commit.Repo.Name).ReadWrite(stm)
		branches := d.branches(commit.Repo.Name).ReadWrite(stm)

		// Make sure that the commit exists
		var commitInfo pfs.CommitInfo
		if err := commits.Get(commit.ID, &commitInfo); err != nil {
			return err
		}

		branches.Put(name, commit)
		return nil
	})
	return err
}

func (d *driver) deleteBranch(ctx context.Context, repo *pfs.Repo, name string) error {
	_, err := col.NewSTM(ctx, d.etcdClient, func(stm col.STM) error {
		branches := d.branches(repo.Name).ReadWrite(stm)
		return branches.Delete(name)
	})
	return err
}

// scratchCommitPrefix returns an etcd prefix that's used to temporarily
// store the state of a file in an open commit.  Once the commit is finished,
// the scratch space is removed.
func (d *driver) scratchCommitPrefix(ctx context.Context, commit *pfs.Commit) (string, error) {
	if _, err := d.inspectCommit(ctx, commit); err != nil {
		return "", err
	}
	return path.Join(d.prefix, "scratch", commit.Repo.Name, commit.ID), nil
}

// scratchFilePrefix returns an etcd prefix that's used to temporarily
// store the state of a file in an open commit.  Once the commit is finished,
// the scratch space is removed.
func (d *driver) scratchFilePrefix(ctx context.Context, file *pfs.File) (string, error) {
	return path.Join(d.prefix, "scratch", file.Commit.Repo.Name, file.Commit.ID, file.Path), nil
}

// checkPath checks if a file path is legal
func checkPath(path string) error {
	if strings.Contains(path, "\x00") {
		return fmt.Errorf("filename cannot contain null character: %s", path)
	}
	return nil
}

func (d *driver) commitExists(commitID string) bool {
	_, found := d.commitCache.Get(commitID)
	return found
}

func (d *driver) setCommitExist(commitID string) {
	d.commitCache.Add(commitID, struct{}{})
}

func (d *driver) putFile(ctx context.Context, file *pfs.File, delimiter pfs.Delimiter,
	targetFileDatums int64, targetFileBytes int64, reader io.Reader) error {
	// Cache existing commit IDs so we don't hit the database on every
	// PutFile call.
	records := &PutFileRecords{}
	if !d.commitExists(file.Commit.ID) {
		_, err := d.inspectCommit(ctx, file.Commit)
		if err != nil {
			return err
		}
		d.setCommitExist(file.Commit.ID)
	}

	if err := checkPath(file.Path); err != nil {
		return err
	}
	prefix, err := d.scratchFilePrefix(ctx, file)
	if err != nil {
		return err
	}

	// Put the tree into the blob store
	objClient, err := d.getObjectClient()
	if err != nil {
		return err
	}
	if delimiter == pfs.Delimiter_NONE {
		object, size, err := objClient.PutObject(reader)
		if err != nil {
			return err
		}
		records.Records = append(records.Records, &PutFileRecord{
			SizeBytes:  size,
			ObjectHash: object.Hash,
		})
		marshalledRecords, err := proto.Marshal(records)
		if err != nil {
			return err
		}
		_, err = d.etcdClient.Put(ctx, path.Join(prefix, uuid.NewWithoutDashes()), string(marshalledRecords))
		return err
	}
	buffer := &bytes.Buffer{}
	var datumsWritten int64
	var bytesWritten int64
	var filesPut int
	EOF := false
	var eg errgroup.Group
	decoder := json.NewDecoder(reader)
	bufioR := bufio.NewReader(reader)

	indexToRecord := make(map[int]*PutFileRecord)
	var mu sync.Mutex
	for !EOF {
		var err error
		var value []byte
		switch delimiter {
		case pfs.Delimiter_JSON:
			var jsonValue json.RawMessage
			err = decoder.Decode(&jsonValue)
			value = jsonValue
		case pfs.Delimiter_LINE:
			value, err = bufioR.ReadBytes('\n')
		default:
			return fmt.Errorf("unrecognized delimiter %s", delimiter.String())
		}
		if err != nil {
			if err == io.EOF {
				EOF = true
			} else {
				return err
			}
		}
		buffer.Write(value)
		bytesWritten += int64(len(value))
		datumsWritten++
		if buffer.Len() != 0 &&
			((targetFileBytes != 0 && bytesWritten >= targetFileBytes) ||
				(targetFileDatums != 0 && datumsWritten >= targetFileDatums) ||
				(targetFileBytes == 0 && targetFileDatums == 0) ||
				EOF) {
			_buffer := buffer
			index := filesPut
			eg.Go(func() error {
				object, size, err := objClient.PutObject(_buffer)
				if err != nil {
					return err
				}
				mu.Lock()
				defer mu.Unlock()
				indexToRecord[index] = &PutFileRecord{
					SizeBytes:  size,
					ObjectHash: object.Hash,
				}
				return nil
			})
			datumsWritten = 0
			bytesWritten = 0
			buffer = &bytes.Buffer{}
			filesPut++
		}
	}
	if err := eg.Wait(); err != nil {
		return err
	}

	records.Split = true
	for i := 0; i < len(indexToRecord); i++ {
		records.Records = append(records.Records, indexToRecord[i])
	}
	marshalledRecords, err := proto.Marshal(records)
	if err != nil {
		return err
	}
	_, err = d.etcdClient.Put(ctx, path.Join(prefix, uuid.NewWithoutDashes()), string(marshalledRecords))
	return err
}

func (d *driver) getTreeForCommit(ctx context.Context, commit *pfs.Commit) (hashtree.HashTree, error) {
	if commit == nil {
		t, err := hashtree.NewHashTree().Finish()
		if err != nil {
			return nil, err
		}
		return t, nil
	}

	tree, ok := d.treeCache.Get(commit.ID)
	if ok {
		h, ok := tree.(hashtree.HashTree)
		if ok {
			return h, nil
		}
		return nil, fmt.Errorf("corrupted cache: expected hashtree.Hashtree, found %v", tree)
	}

	if _, err := d.inspectCommit(ctx, commit); err != nil {
		return nil, err
	}

	commits := d.commits(commit.Repo.Name).ReadOnly(ctx)
	commitInfo := &pfs.CommitInfo{}
	if err := commits.Get(commit.ID, commitInfo); err != nil {
		return nil, err
	}
	if commitInfo.Finished == nil {
		return nil, fmt.Errorf("cannot read from an open commit")
	}
	treeRef := commitInfo.Tree

	if treeRef == nil {
		t, err := hashtree.NewHashTree().Finish()
		if err != nil {
			return nil, err
		}
		return t, nil
	}

	// read the tree from the block store
	objClient, err := d.getObjectClient()
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	if err := objClient.GetObject(treeRef.Hash, &buf); err != nil {
		return nil, err
	}

	h, err := hashtree.Deserialize(buf.Bytes())
	if err != nil {
		return nil, err
	}

	d.treeCache.Add(commit.ID, h)

	return h, nil
}

func (d *driver) getFile(ctx context.Context, file *pfs.File, offset int64, size int64) (io.Reader, error) {
	tree, err := d.getTreeForCommit(ctx, file.Commit)
	if err != nil {
		return nil, err
	}

	node, err := tree.Get(file.Path)
	if err != nil {
		return nil, pfsserver.ErrFileNotFound{file}
	}

	if node.FileNode == nil {
		return nil, fmt.Errorf("%s is a directory", file.Path)
	}

	objClient, err := d.getObjectClient()
	if err != nil {
		return nil, err
	}
	getObjectsClient, err := objClient.ObjectAPIClient.GetObjects(ctx, &pfs.GetObjectsRequest{
		Objects:     node.FileNode.Objects,
		OffsetBytes: uint64(offset),
		SizeBytes:   uint64(size),
	})
	if err != nil {
		return nil, err
	}
	return grpcutil.NewStreamingBytesReader(getObjectsClient), nil
}

// If full is false, exclude potentially large fields such as `Objects`
// and `Children`
func nodeToFileInfo(commit *pfs.Commit, path string, node *hashtree.NodeProto, full bool) *pfs.FileInfo {
	fileInfo := &pfs.FileInfo{
		File: &pfs.File{
			Commit: commit,
			Path:   path,
		},
		SizeBytes: uint64(node.SubtreeSize),
		Hash:      node.Hash,
	}
	if node.FileNode != nil {
		fileInfo.FileType = pfs.FileType_FILE
		if full {
			fileInfo.Objects = node.FileNode.Objects
		}
	} else if node.DirNode != nil {
		fileInfo.FileType = pfs.FileType_DIR
		if full {
			fileInfo.Children = node.DirNode.Children
		}
	}
	return fileInfo
}

func (d *driver) inspectFile(ctx context.Context, file *pfs.File) (*pfs.FileInfo, error) {
	tree, err := d.getTreeForCommit(ctx, file.Commit)
	if err != nil {
		return nil, err
	}

	node, err := tree.Get(file.Path)
	if err != nil {
		return nil, pfsserver.ErrFileNotFound{file}
	}

	return nodeToFileInfo(file.Commit, file.Path, node, true), nil
}

func (d *driver) listFile(ctx context.Context, file *pfs.File) ([]*pfs.FileInfo, error) {
	tree, err := d.getTreeForCommit(ctx, file.Commit)
	if err != nil {
		return nil, err
	}

	nodes, err := tree.List(file.Path)
	if err != nil {
		return nil, err
	}

	var fileInfos []*pfs.FileInfo
	for _, node := range nodes {
		fileInfos = append(fileInfos, nodeToFileInfo(file.Commit, path.Join(file.Path, node.Name), node, false))
	}
	return fileInfos, nil
}

func (d *driver) globFile(ctx context.Context, commit *pfs.Commit, pattern string) ([]*pfs.FileInfo, error) {
	tree, err := d.getTreeForCommit(ctx, commit)
	if err != nil {
		return nil, err
	}

	nodes, err := tree.Glob(pattern)
	if err != nil {
		return nil, err
	}

	var fileInfos []*pfs.FileInfo
	for _, node := range nodes {
		fileInfos = append(fileInfos, nodeToFileInfo(commit, node.Name, node, false))
	}
	return fileInfos, nil
}

func (d *driver) diffFile(ctx context.Context, newFile *pfs.File, oldFile *pfs.File) ([]*pfs.FileInfo, []*pfs.FileInfo, error) {
	newTree, err := d.getTreeForCommit(ctx, newFile.Commit)
	if err != nil {
		return nil, nil, err
	}
	// if oldFile is new we use the parent of newFile
	if oldFile == nil {
		oldFile = &pfs.File{}
		newCommitInfo, err := d.inspectCommit(ctx, newFile.Commit)
		if err != nil {
			return nil, nil, err
		}
		// ParentCommit may be nil, that's fine because getTreeForCommit
		// handles nil
		oldFile.Commit = newCommitInfo.ParentCommit
		oldFile.Path = newFile.Path
	}
	oldTree, err := d.getTreeForCommit(ctx, oldFile.Commit)
	if err != nil {
		return nil, nil, err
	}
	var newFileInfos []*pfs.FileInfo
	var oldFileInfos []*pfs.FileInfo
	if err := newTree.Diff(oldTree, newFile.Path, oldFile.Path, func(path string, node *hashtree.NodeProto, new bool) error {
		if new {
			newFileInfos = append(newFileInfos, nodeToFileInfo(newFile.Commit, path, node, false))
		} else {
			oldFileInfos = append(oldFileInfos, nodeToFileInfo(oldFile.Commit, path, node, false))
		}
		return nil
	}); err != nil {
		return nil, nil, err
	}
	return newFileInfos, oldFileInfos, nil
}

func (d *driver) deleteFile(ctx context.Context, file *pfs.File) error {
	commitInfo, err := d.inspectCommit(ctx, file.Commit)
	if err != nil {
		return err
	}

	if commitInfo.Finished != nil {
		return pfsserver.ErrCommitFinished{file.Commit}
	}

	prefix, err := d.scratchFilePrefix(ctx, file)
	if err != nil {
		return err
	}

	_, err = d.etcdClient.Put(ctx, path.Join(prefix, uuid.NewWithoutDashes()), tombstone)
	return err
}

func (d *driver) deleteAll(ctx context.Context) error {
	repoInfos, err := d.listRepo(ctx, nil)
	if err != nil {
		return err
	}
	for _, repoInfo := range repoInfos {
		if err := d.deleteRepo(ctx, repoInfo.Repo, true); err != nil {
			return err
		}
	}
	return nil
}

func isNotFoundErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), "not found")
}

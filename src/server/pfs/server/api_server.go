package server

import (
	"fmt"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	"github.com/pachyderm/pachyderm/src/client"
	"github.com/pachyderm/pachyderm/src/client/pfs"
	"github.com/pachyderm/pachyderm/src/client/pkg/grpcutil"
	"github.com/pachyderm/pachyderm/src/client/pkg/tracing"
	"github.com/pachyderm/pachyderm/src/server/pkg/hashtree"
	"github.com/pachyderm/pachyderm/src/server/pkg/log"
	"github.com/pachyderm/pachyderm/src/server/pkg/serviceenv"
	txnenv "github.com/pachyderm/pachyderm/src/server/pkg/transactionenv"

	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

var (
	grpcErrorf = grpc.Errorf // needed to get passed govet
)

// apiServer implements the public interface of the Pachyderm File System,
// including all RPCs defined in the protobuf spec.  Implementation details
// occur in the 'driver' code, and this layer serves to translate the protobuf
// request structures into normal function calls.
type apiServer struct {
	log.Logger
	driver *driver
	txnEnv *txnenv.TransactionEnv

	// env generates clients for pachyderm's downstream services
	env *serviceenv.ServiceEnv
	// pachClientOnce ensures that _pachClient is only initialized once
	pachClientOnce sync.Once
	// pachClient is a cached Pachd client that connects to Pachyderm's object
	// store API and auth API. Instead of accessing it directly, functions should
	// call a.env.GetPachClient()
	_pachClient *client.APIClient
}

func newAPIServer(
	env *serviceenv.ServiceEnv,
	txnEnv *txnenv.TransactionEnv,
	etcdPrefix string,
	treeCache *hashtree.Cache,
	storageRoot string,
	memoryRequest int64,
) (*apiServer, error) {
	d, err := newDriver(env, txnEnv, etcdPrefix, treeCache, storageRoot, memoryRequest)
	if err != nil {
		return nil, err
	}
	s := &apiServer{
		Logger: log.NewLogger("pfs.API"),
		driver: d,
		env:    env,
		txnEnv: txnEnv,
	}
	go func() { s.env.GetPachClient(context.Background()) }() // Begin dialing connection on startup
	return s, nil
}

// CreateRepoInTransaction is identical to CreateRepo except that it can run
// inside an existing etcd STM transaction.  This is not an RPC.
func (a *apiServer) CreateRepoInTransaction(
	txnCtx *txnenv.TransactionContext,
	request *pfs.CreateRepoRequest,
) error {
	return a.driver.createRepo(txnCtx, request.Repo, request.Description, request.Update)
}

// CreateRepo implements the protobuf pfs.CreateRepo RPC
func (a *apiServer) CreateRepo(ctx context.Context, request *pfs.CreateRepoRequest) (response *types.Empty, retErr error) {
	if err := a.txnEnv.WithTransaction(ctx, func(txn txnenv.Transaction) error {
		return txn.CreateRepo(request)
	}); err != nil {
		return nil, err
	}
	return &types.Empty{}, nil
}

// InspectRepoInTransaction is identical to InspectRepo except that it can run
// inside an existing etcd STM transaction.  This is not an RPC.
func (a *apiServer) InspectRepoInTransaction(
	txnCtx *txnenv.TransactionContext,
	originalRequest *pfs.InspectRepoRequest,
) (*pfs.RepoInfo, error) {
	request := proto.Clone(originalRequest).(*pfs.InspectRepoRequest)
	return a.driver.inspectRepo(txnCtx, request.Repo, true)
}

// InspectRepo implements the protobuf pfs.InspectRepo RPC
func (a *apiServer) InspectRepo(ctx context.Context, request *pfs.InspectRepoRequest) (response *pfs.RepoInfo, retErr error) {
	func() { a.Log(request, nil, nil, 0) }()
	defer func(start time.Time) { a.Log(request, response, retErr, time.Since(start)) }(time.Now())

	var info *pfs.RepoInfo
	err := a.txnEnv.WithReadContext(ctx, func(txnCtx *txnenv.TransactionContext) error {
		var err error
		info, err = a.InspectRepoInTransaction(txnCtx, request)
		return err
	})
	if err != nil {
		return nil, err
	}
	return info, nil
}

// ListRepo implements the protobuf pfs.ListRepo RPC
func (a *apiServer) ListRepo(ctx context.Context, request *pfs.ListRepoRequest) (response *pfs.ListRepoResponse, retErr error) {
	func() { a.Log(request, nil, nil, 0) }()
	defer func(start time.Time) { a.Log(request, response, retErr, time.Since(start)) }(time.Now())

	repoInfos, err := a.driver.listRepo(a.env.GetPachClient(ctx), true)
	return repoInfos, err
}

// DeleteRepoInTransaction is identical to DeleteRepo except that it can run
// inside an existing etcd STM transaction.  This is not an RPC.
func (a *apiServer) DeleteRepoInTransaction(
	txnCtx *txnenv.TransactionContext,
	request *pfs.DeleteRepoRequest,
) error {
	if request.All {
		return a.driver.deleteAll(txnCtx)
	}
	return a.driver.deleteRepo(txnCtx, request.Repo, request.Force)
}

// DeleteRepo implements the protobuf pfs.DeleteRepo RPC
func (a *apiServer) DeleteRepo(ctx context.Context, request *pfs.DeleteRepoRequest) (response *types.Empty, retErr error) {
	func() { a.Log(request, nil, nil, 0) }()
	defer func(start time.Time) { a.Log(request, response, retErr, time.Since(start)) }(time.Now())

	if err := a.txnEnv.WithTransaction(ctx, func(txn txnenv.Transaction) error {
		return txn.DeleteRepo(request)
	}); err != nil {
		return nil, err
	}
	return &types.Empty{}, nil
}

func (a *apiServer) Fsck(ctx context.Context, request *types.Empty) (response *types.Empty, retErr error) {
	func() { a.Log(request, nil, nil, 0) }()
	defer func(start time.Time) { a.Log(request, response, retErr, time.Since(start)) }(time.Now())

	err := a.driver.fsck(a.env.GetPachClient(ctx))
	if err != nil {
		return nil, err
	}
	return &types.Empty{}, nil
}

// StartCommitInTransaction is identical to StartCommit except that it can run
// inside an existing etcd STM transaction.  This is not an RPC.  The target
// commit can be specified but is optional.  This is so that the transaction can
// report the commit ID back to the client before the transaction has finished
// and it can be used in future commands inside the same transaction.
func (a *apiServer) StartCommitInTransaction(
	txnCtx *txnenv.TransactionContext,
	request *pfs.StartCommitRequest,
	commit *pfs.Commit,
) (*pfs.Commit, error) {
	id := ""
	if commit != nil {
		id = commit.ID
	}
	return a.driver.startCommit(txnCtx, id, request.Parent, request.Branch, request.Provenance, request.Description)
}

// StartCommit implements the protobuf pfs.StartCommit RPC
func (a *apiServer) StartCommit(ctx context.Context, request *pfs.StartCommitRequest) (response *pfs.Commit, retErr error) {
	func() { a.Log(request, nil, nil, 0) }()
	defer func(start time.Time) { a.Log(request, response, retErr, time.Since(start)) }(time.Now())

	var err error
	commit := &pfs.Commit{}
	if err = a.txnEnv.WithTransaction(ctx, func(txn txnenv.Transaction) error {
		commit, err = txn.StartCommit(request, nil)
		return err
	}); err != nil {
		return nil, err
	}
	return commit, nil
}

// BuildCommit implements the protobuf pfs.BuildCommit RPC
func (a *apiServer) BuildCommit(ctx context.Context, request *pfs.BuildCommitRequest) (response *pfs.Commit, retErr error) {
	func() { a.Log(request, nil, nil, 0) }()
	defer func(start time.Time) { a.Log(request, response, retErr, time.Since(start)) }(time.Now())

	commit, err := a.driver.buildCommit(ctx, request.ID, request.Parent, request.Branch, request.Provenance, request.Tree)
	if err != nil {
		return nil, err
	}
	return commit, nil
}

// FinishCommitInTransaction is identical to FinishCommit except that it can run
// inside an existing etcd STM transaction.  This is not an RPC.
func (a *apiServer) FinishCommitInTransaction(
	txnCtx *txnenv.TransactionContext,
	request *pfs.FinishCommitRequest,
) error {
	if request.Trees != nil {
		return a.driver.finishOutputCommit(txnCtx, request.Commit, request.Trees, request.Datums, request.SizeBytes)
	}
	return a.driver.finishCommit(txnCtx, request.Commit, request.Tree, request.Empty, request.Description)
}

// FinishCommit implements the protobuf pfs.FinishCommit RPC
func (a *apiServer) FinishCommit(ctx context.Context, request *pfs.FinishCommitRequest) (response *types.Empty, retErr error) {
	func() { a.Log(request, nil, nil, 0) }()
	defer func(start time.Time) { a.Log(request, response, retErr, time.Since(start)) }(time.Now())

	if err := a.txnEnv.WithTransaction(ctx, func(txn txnenv.Transaction) error {
		return txn.FinishCommit(request)
	}); err != nil {
		return nil, err
	}
	return &types.Empty{}, nil
}

// InspectCommit implements the protobuf pfs.InspectCommit RPC
func (a *apiServer) InspectCommit(ctx context.Context, request *pfs.InspectCommitRequest) (response *pfs.CommitInfo, retErr error) {
	func() { a.Log(request, nil, nil, 0) }()
	defer func(start time.Time) { a.Log(request, response, retErr, time.Since(start)) }(time.Now())

	return a.driver.inspectCommit(a.env.GetPachClient(ctx), request.Commit, request.BlockState)
}

// ListCommit implements the protobuf pfs.ListCommit RPC
func (a *apiServer) ListCommit(ctx context.Context, request *pfs.ListCommitRequest) (response *pfs.CommitInfos, retErr error) {
	func() { a.Log(request, nil, nil, 0) }()
	defer func(start time.Time) { a.Log(request, response, retErr, time.Since(start)) }(time.Now())

	commitInfos, err := a.driver.listCommit(a.env.GetPachClient(ctx), request.Repo, request.To, request.From, request.Number)
	if err != nil {
		return nil, err
	}
	return &pfs.CommitInfos{
		CommitInfo: commitInfos,
	}, nil
}

// ListCommitStream implements the protobuf pfs.ListCommitStream RPC
func (a *apiServer) ListCommitStream(req *pfs.ListCommitRequest, respServer pfs.API_ListCommitStreamServer) (retErr error) {
	func() { a.Log(req, nil, nil, 0) }()
	sent := 0
	defer func(start time.Time) {
		a.Log(req, fmt.Sprintf("stream containing %d commits", sent), retErr, time.Since(start))
	}(time.Now())
	return a.driver.listCommitF(a.env.GetPachClient(respServer.Context()), req.Repo, req.To, req.From, req.Number, func(ci *pfs.CommitInfo) error {
		sent++
		return respServer.Send(ci)
	})
}

// CreateBranchInTransaction is identical to CreateBranch except that it can run
// inside an existing etcd STM transaction.  This is not an RPC.
func (a *apiServer) CreateBranchInTransaction(
	txnCtx *txnenv.TransactionContext,
	request *pfs.CreateBranchRequest,
) error {
	return a.driver.createBranch(txnCtx, request.Branch, request.Head, request.Provenance)
}

// CreateBranch implements the protobuf pfs.CreateBranch RPC
func (a *apiServer) CreateBranch(ctx context.Context, request *pfs.CreateBranchRequest) (response *types.Empty, retErr error) {
	func() { a.Log(request, nil, nil, 0) }()
	defer func(start time.Time) { a.Log(request, response, retErr, time.Since(start)) }(time.Now())

	if err := a.txnEnv.WithTransaction(ctx, func(txn txnenv.Transaction) error {
		return txn.CreateBranch(request)
	}); err != nil {
		return nil, err
	}
	return &types.Empty{}, nil
}

// InspectBranch implements the protobuf pfs.InspectBranch RPC
func (a *apiServer) InspectBranch(ctx context.Context, request *pfs.InspectBranchRequest) (response *pfs.BranchInfo, retErr error) {
	func() { a.Log(request, nil, nil, 0) }()
	defer func(start time.Time) { a.Log(request, response, retErr, time.Since(start)) }(time.Now())

	branchInfo := &pfs.BranchInfo{}
	if err := a.txnEnv.WithReadContext(ctx, func(txnCtx *txnenv.TransactionContext) error {
		var err error
		branchInfo, err = a.driver.inspectBranch(txnCtx, request.Branch)
		return err
	}); err != nil {
		return nil, err
	}
	return branchInfo, nil
}

// ListBranch implements the protobuf pfs.ListBranch RPC
func (a *apiServer) ListBranch(ctx context.Context, request *pfs.ListBranchRequest) (response *pfs.BranchInfos, retErr error) {
	func() { a.Log(request, nil, nil, 0) }()
	defer func(start time.Time) { a.Log(request, response, retErr, time.Since(start)) }(time.Now())

	branches, err := a.driver.listBranch(a.env.GetPachClient(ctx), request.Repo)
	if err != nil {
		return nil, err
	}
	return &pfs.BranchInfos{BranchInfo: branches}, nil
}

// DeleteBranchInTransaction is identical to DeleteBranch except that it can run
// inside an existing etcd STM transaction.  This is not an RPC.
func (a *apiServer) DeleteBranchInTransaction(
	txnCtx *txnenv.TransactionContext,
	request *pfs.DeleteBranchRequest,
) error {
	return a.driver.deleteBranch(txnCtx, request.Branch, request.Force)
}

// DeleteBranch implements the protobuf pfs.DeleteBranch RPC
func (a *apiServer) DeleteBranch(ctx context.Context, request *pfs.DeleteBranchRequest) (response *types.Empty, retErr error) {
	func() { a.Log(request, nil, nil, 0) }()
	defer func(start time.Time) { a.Log(request, response, retErr, time.Since(start)) }(time.Now())

	if err := a.txnEnv.WithTransaction(ctx, func(txn txnenv.Transaction) error {
		return txn.DeleteBranch(request)
	}); err != nil {
		return nil, err
	}
	return &types.Empty{}, nil
}

// DeleteCommitInTransaction is identical to DeleteCommit except that it can run
// inside an existing etcd STM transaction.  This is not an RPC.
func (a *apiServer) DeleteCommitInTransaction(
	txnCtx *txnenv.TransactionContext,
	request *pfs.DeleteCommitRequest,
) error {
	return a.driver.deleteCommit(txnCtx, request.Commit)
}

// DeleteCommit implements the protobuf pfs.DeleteCommit RPC
func (a *apiServer) DeleteCommit(ctx context.Context, request *pfs.DeleteCommitRequest) (response *types.Empty, retErr error) {
	func() { a.Log(request, nil, nil, 0) }()
	defer func(start time.Time) { a.Log(request, response, retErr, time.Since(start)) }(time.Now())

	if err := a.txnEnv.WithTransaction(ctx, func(txn txnenv.Transaction) error {
		return txn.DeleteCommit(request)
	}); err != nil {
		return nil, err
	}
	return &types.Empty{}, nil
}

// FlushCommit implements the protobuf pfs.FlushCommit RPC
func (a *apiServer) FlushCommit(request *pfs.FlushCommitRequest, stream pfs.API_FlushCommitServer) (retErr error) {
	func() { a.Log(request, nil, nil, 0) }()
	defer func(start time.Time) { a.Log(request, nil, retErr, time.Since(start)) }(time.Now())

	return a.driver.flushCommit(a.env.GetPachClient(stream.Context()), request.Commits, request.ToRepos, stream.Send)
}

// SubscribeCommit implements the protobuf pfs.SubscribeCommit RPC
func (a *apiServer) SubscribeCommit(request *pfs.SubscribeCommitRequest, stream pfs.API_SubscribeCommitServer) (retErr error) {
	func() { a.Log(request, nil, nil, 0) }()
	defer func(start time.Time) { a.Log(request, nil, retErr, time.Since(start)) }(time.Now())

	return a.driver.subscribeCommit(a.env.GetPachClient(stream.Context()), request.Repo, request.Branch, request.From, request.State, stream.Send)
}

// PutFile implements the protobuf pfs.PutFile RPC
func (a *apiServer) PutFile(putFileServer pfs.API_PutFileServer) (retErr error) {
	s := newPutFileServer(putFileServer)
	r, err := s.Peek()
	if err != nil {
		return err
	}
	request := *r
	request.Value = nil
	func() { a.Log(request, nil, nil, 0) }()
	defer func(start time.Time) { a.Log(request, nil, retErr, time.Since(start)) }(time.Now())
	defer drainFileServer(putFileServer)
	defer func() {
		if err := putFileServer.SendAndClose(&types.Empty{}); err != nil && retErr == nil {
			retErr = err
		}
	}()
	pachClient := a.env.GetPachClient(s.Context())
	return a.driver.putFiles(pachClient, s)
}

// CopyFile implements the protobuf pfs.CopyFile RPC
func (a *apiServer) CopyFile(ctx context.Context, request *pfs.CopyFileRequest) (response *types.Empty, retErr error) {
	func() { a.Log(request, nil, nil, 0) }()
	defer func(start time.Time) { a.Log(request, response, retErr, time.Since(start)) }(time.Now())
	if err := a.driver.copyFile(a.env.GetPachClient(ctx), request.Src, request.Dst, request.Overwrite); err != nil {
		return nil, err
	}
	return &types.Empty{}, nil
}

// GetFile implements the protobuf pfs.GetFile RPC
func (a *apiServer) GetFile(request *pfs.GetFileRequest, apiGetFileServer pfs.API_GetFileServer) (retErr error) {
	func() { a.Log(request, nil, nil, 0) }()
	defer func(start time.Time) {
		tracing.TagAnySpan(apiGetFileServer.Context(), "file", fmt.Sprintf("%s@%s:%s",
			request.File.Commit.Repo.Name, request.File.Commit.ID, request.File.Path),
			"err", retErr)
		a.Log(request, nil, retErr, time.Since(start))
	}(time.Now())

	file, err := a.driver.getFile(a.env.GetPachClient(apiGetFileServer.Context()), request.File, request.OffsetBytes, request.SizeBytes)
	if err != nil {
		return err
	}
	return grpcutil.WriteToStreamingBytesServer(file, apiGetFileServer)
}

// InspectFile implements the protobuf pfs.InspectFile RPC
func (a *apiServer) InspectFile(ctx context.Context, request *pfs.InspectFileRequest) (response *pfs.FileInfo, retErr error) {
	func() { a.Log(request, nil, nil, 0) }()
	defer func(start time.Time) {
		a.Log(request, response, retErr, time.Since(start))
		tracing.TagAnySpan(ctx, "file", fmt.Sprintf("%s@%s:%s",
			request.File.Commit.Repo.Name, request.File.Commit.ID, request.File.Path),
			"err", retErr)
	}(time.Now())

	return a.driver.inspectFile(a.env.GetPachClient(ctx), request.File)
}

// ListFile implements the protobuf pfs.ListFile RPC
func (a *apiServer) ListFile(ctx context.Context, request *pfs.ListFileRequest) (response *pfs.FileInfos, retErr error) {
	func() { a.Log(request, nil, nil, 0) }()
	defer func(start time.Time) {
		if response != nil && len(response.FileInfo) > client.MaxListItemsLog {
			logrus.Infof("Response contains %d objects; logging the first %d", len(response.FileInfo), client.MaxListItemsLog)
			a.Log(request, &pfs.FileInfos{FileInfo: response.FileInfo[:client.MaxListItemsLog]}, retErr, time.Since(start))
		} else {
			a.Log(request, response, retErr, time.Since(start))
		}
	}(time.Now())

	var fileInfos []*pfs.FileInfo
	if err := a.driver.listFile(a.env.GetPachClient(ctx), request.File, request.Full, request.History, func(fi *pfs.FileInfo) error {
		fileInfos = append(fileInfos, fi)
		return nil
	}); err != nil {
		return nil, err
	}
	return &pfs.FileInfos{
		FileInfo: fileInfos,
	}, nil
}

// ListFileStream implements the protobuf pfs.ListFileStream RPC
func (a *apiServer) ListFileStream(request *pfs.ListFileRequest, respServer pfs.API_ListFileStreamServer) (retErr error) {
	func() { a.Log(request, nil, nil, 0) }()
	var sent int
	defer func(start time.Time) {
		a.Log(request, fmt.Sprintf("response stream with %d objects", sent), retErr, time.Since(start))
	}(time.Now())
	return a.driver.listFile(a.env.GetPachClient(respServer.Context()), request.File, request.Full, request.History, func(fi *pfs.FileInfo) error {
		sent++
		return respServer.Send(fi)
	})
}

// WalkFile implements the protobuf pfs.WalkFile RPC
func (a *apiServer) WalkFile(request *pfs.WalkFileRequest, server pfs.API_WalkFileServer) (retErr error) {
	func() { a.Log(request, nil, nil, 0) }()
	var sent int
	defer func(start time.Time) {
		a.Log(request, fmt.Sprintf("response stream with %d objects", sent), retErr, time.Since(start))
	}(time.Now())
	return a.driver.walkFile(a.env.GetPachClient(server.Context()), request.File, func(fi *pfs.FileInfo) error {
		sent++
		return server.Send(fi)
	})
}

// GlobFile implements the protobuf pfs.GlobFile RPC
func (a *apiServer) GlobFile(ctx context.Context, request *pfs.GlobFileRequest) (response *pfs.FileInfos, retErr error) {
	func() { a.Log(request, nil, nil, 0) }()
	defer func(start time.Time) {
		if response != nil && len(response.FileInfo) > client.MaxListItemsLog {
			logrus.Infof("Response contains %d objects; logging the first %d", len(response.FileInfo), client.MaxListItemsLog)
			a.Log(request, &pfs.FileInfos{FileInfo: response.FileInfo[:client.MaxListItemsLog]}, retErr, time.Since(start))
		} else {
			a.Log(request, response, retErr, time.Since(start))
		}
	}(time.Now())

	var fileInfos []*pfs.FileInfo
	if err := a.driver.globFile(a.env.GetPachClient(ctx), request.Commit, request.Pattern, func(fi *pfs.FileInfo) error {
		fileInfos = append(fileInfos, fi)
		return nil
	}); err != nil {
		return nil, err
	}
	return &pfs.FileInfos{
		FileInfo: fileInfos,
	}, nil
}

// GlobFileStream implements the protobuf pfs.GlobFileStream RPC
func (a *apiServer) GlobFileStream(request *pfs.GlobFileRequest, respServer pfs.API_GlobFileStreamServer) (retErr error) {
	func() { a.Log(request, nil, nil, 0) }()
	var sent int
	defer func(start time.Time) {
		a.Log(request, fmt.Sprintf("response stream with %d objects", sent), retErr, time.Since(start))
	}(time.Now())
	return a.driver.globFile(a.env.GetPachClient(respServer.Context()), request.Commit, request.Pattern, func(fi *pfs.FileInfo) error {
		sent++
		return respServer.Send(fi)
	})
}

// DiffFile implements the protobuf pfs.DiffFile RPC
func (a *apiServer) DiffFile(ctx context.Context, request *pfs.DiffFileRequest) (response *pfs.DiffFileResponse, retErr error) {
	func() { a.Log(request, nil, nil, 0) }()
	defer func(start time.Time) {
		if response != nil && (len(response.NewFiles) > client.MaxListItemsLog || len(response.OldFiles) > client.MaxListItemsLog) {
			logrus.Infof("Response contains too many objects; truncating.")
			a.Log(request, &pfs.DiffFileResponse{
				NewFiles: truncateFiles(response.NewFiles),
				OldFiles: truncateFiles(response.OldFiles),
			}, retErr, time.Since(start))
		} else {
			a.Log(request, response, retErr, time.Since(start))
		}
	}(time.Now())
	newFileInfos, oldFileInfos, err := a.driver.diffFile(a.env.GetPachClient(ctx), request.NewFile, request.OldFile, request.Shallow)
	if err != nil {
		return nil, err
	}
	return &pfs.DiffFileResponse{
		NewFiles: newFileInfos,
		OldFiles: oldFileInfos,
	}, nil
}

// DeleteFile implements the protobuf pfs.DeleteFile RPC
func (a *apiServer) DeleteFile(ctx context.Context, request *pfs.DeleteFileRequest) (response *types.Empty, retErr error) {
	func() { a.Log(request, nil, nil, 0) }()
	defer func(start time.Time) { a.Log(request, response, retErr, time.Since(start)) }(time.Now())

	err := a.driver.deleteFile(a.env.GetPachClient(ctx), request.File)
	if err != nil {
		return nil, err
	}
	return &types.Empty{}, nil
}

// DeleteAll implements the protobuf pfs.DeleteAll RPC
func (a *apiServer) DeleteAll(ctx context.Context, request *types.Empty) (response *types.Empty, retErr error) {
	func() { a.Log(request, nil, nil, 0) }()
	defer func(start time.Time) { a.Log(request, response, retErr, time.Since(start)) }(time.Now())

	err := a.txnEnv.WithWriteContext(ctx, func(txnCtx *txnenv.TransactionContext) error {
		return a.driver.deleteAll(txnCtx)
	})
	if err != nil {
		return nil, err
	}
	return &types.Empty{}, nil
}

func drainFileServer(putFileServer interface {
	Recv() (*pfs.PutFileRequest, error)
}) {
	for {
		if _, err := putFileServer.Recv(); err != nil {
			break
		}
	}
}

func truncateFiles(fileInfos []*pfs.FileInfo) []*pfs.FileInfo {
	if len(fileInfos) > client.MaxListItemsLog {
		return fileInfos[:client.MaxListItemsLog]
	}
	return fileInfos
}

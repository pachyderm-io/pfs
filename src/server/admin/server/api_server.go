package server

import (
	"bytes"
	"fmt"
	"io"
	"regexp"
	"sync"
	"time"

	"github.com/golang/snappy"
	"golang.org/x/net/context"

	"github.com/gogo/protobuf/types"
	"github.com/pachyderm/pachyderm/src/client"
	"github.com/pachyderm/pachyderm/src/client/admin"
	"github.com/pachyderm/pachyderm/src/client/pfs"
	"github.com/pachyderm/pachyderm/src/client/pkg/errutil"
	"github.com/pachyderm/pachyderm/src/client/pkg/grpcutil"
	"github.com/pachyderm/pachyderm/src/client/pkg/pbutil"
	"github.com/pachyderm/pachyderm/src/client/pps"
	"github.com/pachyderm/pachyderm/src/server/pkg/ancestry"
	"github.com/pachyderm/pachyderm/src/server/pkg/log"
	"github.com/pachyderm/pachyderm/src/server/pkg/obj"
)

var objHashRE = regexp.MustCompile("[0-9a-f]{128}")

type apiServer struct {
	log.Logger
	address        string
	storageRoot    string // for downloading/converting hashtrees
	pachClient     *client.APIClient
	pachClientOnce sync.Once
	clusterInfo    *admin.ClusterInfo
}

func (a *apiServer) InspectCluster(ctx context.Context, request *types.Empty) (*admin.ClusterInfo, error) {
	return a.clusterInfo, nil
}

type opVersion int8

const (
	undefined opVersion = iota
	v1_7
	v1_8
	v1_9
)

func (v opVersion) String() string {
	switch v {
	case v1_7:
		return "1.7"
	case v1_8:
		return "1.8"
	case v1_9:
		return "1.9"
	}
	return "undefined"
}

func version(op *admin.Op) opVersion {
	switch {
	case op.Op1_7 != nil:
		return v1_7
	case op.Op1_8 != nil:
		return v1_8
	case op.Op1_9 != nil:
		return v1_9
	default:
		return undefined
	}
}

func (a *apiServer) Extract(request *admin.ExtractRequest, extractServer admin.API_ExtractServer) (retErr error) {
	func() { a.Log(request, nil, nil, 0) }()
	defer func(start time.Time) { a.Log(request, nil, retErr, time.Since(start)) }(time.Now())
	ctx := extractServer.Context()
	pachClient := a.getPachClient().WithCtx(ctx)
	writeOp := extractServer.Send
	if request.URL != "" {
		url, err := obj.ParseURL(request.URL)
		if err != nil {
			return fmt.Errorf("error parsing url %v: %v", request.URL, err)
		}
		if url.Object == "" {
			return fmt.Errorf("URL must be <svc>://<bucket>/<object> (no object in %s)", request.URL)
		}
		objClient, err := obj.NewClientFromURLAndSecret(url, false)
		if err != nil {
			return err
		}
		objW, err := objClient.Writer(extractServer.Context(), url.Object)
		if err != nil {
			return err
		}
		defer func() {
			if err := objW.Close(); err != nil && retErr == nil {
				retErr = err
			}
		}()
		snappyW := snappy.NewBufferedWriter(objW)
		defer func() {
			if err := snappyW.Close(); err != nil && retErr == nil {
				retErr = err
			}
		}()
		w := pbutil.NewWriter(snappyW)
		writeOp = func(op *admin.Op) error {
			_, err := w.Write(op)
			return err
		}
	}
	if !request.NoObjects {
		w := extractObjectWriter(writeOp)
		if err := pachClient.ListObject(func(object *pfs.Object) error {
			if err := pachClient.GetObject(object.Hash, w); err != nil {
				return err
			}
			// empty PutObjectRequest to indicate EOF
			return writeOp(&admin.Op{Op1_9: &admin.Op1_9{Object: &pfs.PutObjectRequest{}}})
		}); err != nil {
			return err
		}
		if err := pachClient.ListTag(func(resp *pfs.ListTagsResponse) error {
			return writeOp(&admin.Op{Op1_9: &admin.Op1_9{
				Tag: &pfs.TagObjectRequest{
					Object: resp.Object,
					Tags:   []*pfs.Tag{resp.Tag},
				},
			}})
		}); err != nil {
			return err
		}
	}
	var repos []*pfs.Repo
	if !request.NoRepos {
		ris, err := pachClient.ListRepo()
		if err != nil {
			return err
		}
	repos:
		for _, ri := range ris {
			bis, err := pachClient.ListBranch(ri.Repo.Name)
			if err != nil {
				return err
			}
			for _, bi := range bis {
				if len(bi.Provenance) > 0 {
					continue repos
				}
			}
			if err := writeOp(&admin.Op{Op1_9: &admin.Op1_9{
				Repo: &pfs.CreateRepoRequest{
					Repo:        ri.Repo,
					Description: ri.Description,
				}},
			}); err != nil {
				return err
			}
			repos = append(repos, ri.Repo)
		}
	}
	if !request.NoPipelines {
		pis, err := pachClient.ListPipeline()
		if err != nil {
			return err
		}
		pis = sortPipelineInfos(pis)
		for _, pi := range pis {
			if err := writeOp(&admin.Op{Op1_9: &admin.Op1_9{Pipeline: pipelineInfoToRequest(pi)}}); err != nil {
				return err
			}
		}
	}
	// We send the actual commits last, that way pipelines will have already
	// been created and will recreate output commits for historical outputs.
	for _, repo := range repos {
		cis, err := pachClient.ListCommit(repo.Name, "", "", 0)
		if err != nil {
			return err
		}
		bis, err := pachClient.ListBranch(repo.Name)
		if err != nil {
			return err
		}
		for _, bcr := range buildCommitRequests(cis, bis) {
			if err := writeOp(&admin.Op{Op1_9: &admin.Op1_9{Commit: bcr}}); err != nil {
				return err
			}
		}
		for _, bi := range bis {
			if err := writeOp(&admin.Op{Op1_9: &admin.Op1_9{
				Branch: &pfs.CreateBranchRequest{
					Head:   bi.Head,
					Branch: bi.Branch,
				},
			}}); err != nil {
				return err
			}
		}
	}
	return nil
}

func (a *apiServer) ExtractPipeline(ctx context.Context, request *admin.ExtractPipelineRequest) (response *admin.Op, retErr error) {
	func() { a.Log(request, nil, nil, 0) }()
	defer func(start time.Time) { a.Log(request, response, retErr, time.Since(start)) }(time.Now())
	pachClient := a.getPachClient().WithCtx(ctx)
	pi, err := pachClient.InspectPipeline(request.Pipeline.Name)
	if err != nil {
		return nil, err
	}
	return &admin.Op{Op1_9: &admin.Op1_9{Pipeline: pipelineInfoToRequest(pi)}}, nil
}

func buildCommitRequests(cis []*pfs.CommitInfo, bis []*pfs.BranchInfo) []*pfs.BuildCommitRequest {
	cis = sortCommitInfos(cis)
	result := make([]*pfs.BuildCommitRequest, len(cis))
	commitToBranch := make(map[string]string)
	for _, bi := range bis {
		if bi.Head == nil {
			continue
		}
		if _, ok := commitToBranch[bi.Head.ID]; !ok || bi.Name == "master" {
			commitToBranch[bi.Head.ID] = bi.Name
		}
	}
	for i := range cis {
		ci := cis[len(cis)-i-1]
		branch := commitToBranch[ci.Commit.ID]
		// Even without a parent, ParentCommit is used to indicate which
		// repo to make the commit in.
		if ci.ParentCommit == nil {
			ci.ParentCommit = client.NewCommit(ci.Commit.Repo.Name, "")
		}
		result[len(cis)-i-1] = &pfs.BuildCommitRequest{
			Parent: ci.ParentCommit,
			Tree:   ci.Tree,
			ID:     ci.Commit.ID,
			Branch: branch,
		}
		if _, ok := commitToBranch[ci.ParentCommit.ID]; !ok || branch == "master" {
			commitToBranch[ci.ParentCommit.ID] = branch
		}
	}
	return result
}

func sortCommitInfos(cis []*pfs.CommitInfo) []*pfs.CommitInfo {
	commitMap := make(map[string]*pfs.CommitInfo)
	for _, ci := range cis {
		commitMap[ci.Commit.ID] = ci
	}
	var result []*pfs.CommitInfo
	for _, ci := range cis {
		if commitMap[ci.Commit.ID] == nil {
			continue
		}
		var localResult []*pfs.CommitInfo
		for ci != nil {
			localResult = append(localResult, ci)
			delete(commitMap, ci.Commit.ID)
			if ci.ParentCommit != nil {
				ci = commitMap[ci.ParentCommit.ID]
			} else {
				ci = nil
			}
		}
		for i := range localResult {
			result = append(result, localResult[len(localResult)-i-1])
		}
	}
	return result
}

func sortPipelineInfos(pis []*pps.PipelineInfo) []*pps.PipelineInfo {
	piMap := make(map[string]*pps.PipelineInfo)
	for _, pi := range pis {
		piMap[pi.Pipeline.Name] = pi
	}
	var result []*pps.PipelineInfo
	var add func(string)
	add = func(name string) {
		if pi, ok := piMap[name]; ok {
			pps.VisitInput(pi.Input, func(input *pps.Input) {
				if input.Pfs != nil {
					add(input.Pfs.Repo)
				}
			})
			result = append(result, pi)
			delete(piMap, name)
		}
	}
	for _, pi := range pis {
		add(pi.Pipeline.Name)
	}
	return result
}

func pipelineInfoToRequest(pi *pps.PipelineInfo) *pps.CreatePipelineRequest {
	return &pps.CreatePipelineRequest{
		Pipeline:           pi.Pipeline,
		Transform:          pi.Transform,
		ParallelismSpec:    pi.ParallelismSpec,
		Egress:             pi.Egress,
		OutputBranch:       pi.OutputBranch,
		ScaleDownThreshold: pi.ScaleDownThreshold,
		ResourceRequests:   pi.ResourceRequests,
		ResourceLimits:     pi.ResourceLimits,
		Input:              pi.Input,
		Description:        pi.Description,
		CacheSize:          pi.CacheSize,
		EnableStats:        pi.EnableStats,
		Batch:              pi.Batch,
		MaxQueueSize:       pi.MaxQueueSize,
		Service:            pi.Service,
		ChunkSpec:          pi.ChunkSpec,
		DatumTimeout:       pi.DatumTimeout,
		JobTimeout:         pi.JobTimeout,
	}
}

func (a *apiServer) Restore(restoreServer admin.API_RestoreServer) (retErr error) {
	func() { a.Log(nil, nil, nil, 0) }()
	defer func(start time.Time) { a.Log(nil, nil, retErr, time.Since(start)) }(time.Now())
	pachClient := a.getPachClient().WithCtx(restoreServer.Context())
	defer func() {
		for {
			_, err := restoreServer.Recv()
			if err != nil {
				break
			}
		}
		if err := restoreServer.SendAndClose(&types.Empty{}); err != nil && retErr == nil {
			retErr = err
		}
	}()
	var r pbutil.Reader
	var streamVersion opVersion
	for {
		var op *admin.Op
		if r == nil {
			req, err := restoreServer.Recv()
			if err != nil {
				if err == io.EOF {
					return nil
				}
				return err
			}
			if req.URL != "" {
				url, err := obj.ParseURL(req.URL)
				if err != nil {
					return fmt.Errorf("error parsing url %v: %v", req.URL, err)
				}
				if url.Object == "" {
					return fmt.Errorf("URL must be <svc>://<bucket>/<object> (no object in %s)", req.URL)
				}
				objClient, err := obj.NewClientFromURLAndSecret(url, false)
				if err != nil {
					return err
				}
				objR, err := objClient.Reader(restoreServer.Context(), url.Object, 0, 0)
				if err != nil {
					return err
				}
				snappyR := snappy.NewReader(objR)
				r = pbutil.NewReader(snappyR)
				continue
			} else {
				op = req.Op
			}
		} else {
			op = &admin.Op{}
			if err := r.Read(op); err != nil {
				if err == io.EOF {
					return nil
				}
				return err
			}
		}

		// validate op version
		if streamVersion == undefined {
			streamVersion = version(op)
		} else if streamVersion != version(op) {
			return fmt.Errorf("cannot mix different versions of pachd operation "+
				"within a metadata dumps (found both %s and %s)", version(op), streamVersion)
		}

		// apply op
		if op.Op1_7 != nil {
			if op.Op1_7.Object != nil {
				extractedReader := &extractedObjectReader{
					adminAPIRestoreServer: restoreServer,
					restoreURLReader:      r,
					version:               v1_7,
				}
				extractedReader.buf.Write(op.Op1_7.Object.Value)
				if _, _, err := pachClient.PutObject(extractedReader); err != nil {
					return fmt.Errorf("error putting object: %v", err)
				}
			} else {
				newOp1_8, err := convert1_7Op(pachClient, a.storageRoot, op.Op1_7)
				if err != nil {
					return err
				}
				newOp, err := convert1_8Op(newOp1_8)
				if err != nil {
					return err
				}
				if err := a.applyOp(pachClient, newOp); err != nil {
					return err
				}
			}
		} else if op.Op1_8 != nil {
			if op.Op1_8.Object != nil {
				extractedReader := &extractedObjectReader{
					adminAPIRestoreServer: restoreServer,
					restoreURLReader:      r,
					version:               v1_9,
				}
				extractedReader.buf.Write(op.Op1_8.Object.Value)
				if _, _, err := pachClient.PutObject(extractedReader); err != nil {
					return fmt.Errorf("error putting object: %v", err)
				}
			} else {
				newOp, err := convert1_8Op(op.Op1_8)
				if err != nil {
					return err
				}
				if err := a.applyOp(pachClient, newOp); err != nil {
					return err
				}
			}
		} else if op.Op1_9 != nil {
			if op.Op1_9.Object != nil {
				extractedReader := &extractedObjectReader{
					adminAPIRestoreServer: restoreServer,
					restoreURLReader:      r,
					version:               v1_9,
				}
				extractedReader.buf.Write(op.Op1_9.Object.Value)
				if _, _, err := pachClient.PutObject(extractedReader); err != nil {
					return fmt.Errorf("error putting object: %v", err)
				}
			} else {
				if err := a.applyOp(pachClient, op.Op1_9); err != nil {
					return err
				}
			}
		}
	}
}

func (a *apiServer) applyOp(pachClient *client.APIClient, op *admin.Op1_9) error {
	switch {
	case op.Tag != nil:
		if _, err := pachClient.ObjectAPIClient.TagObject(pachClient.Ctx(), op.Tag); err != nil {
			return fmt.Errorf("error tagging object: %v", grpcutil.ScrubGRPC(err))
		}
	case op.Repo != nil:
		op.Repo.Repo.Name = ancestry.SanitizeName(op.Repo.Repo.Name)
		if _, err := pachClient.PfsAPIClient.CreateRepo(pachClient.Ctx(), op.Repo); err != nil && !errutil.IsAlreadyExistError(err) {
			return fmt.Errorf("error creating repo: %v", grpcutil.ScrubGRPC(err))
		}
	case op.Commit != nil:
		if _, err := pachClient.PfsAPIClient.BuildCommit(pachClient.Ctx(), op.Commit); err != nil && !errutil.IsAlreadyExistError(err) {
			return fmt.Errorf("error creating commit: %v", grpcutil.ScrubGRPC(err))
		}
	case op.Branch != nil:
		if op.Branch.Branch == nil {
			op.Branch.Branch = client.NewBranch(op.Branch.Head.Repo.Name, ancestry.SanitizeName(op.Branch.SBranch))
		}
		if _, err := pachClient.PfsAPIClient.CreateBranch(pachClient.Ctx(), op.Branch); err != nil && !errutil.IsAlreadyExistError(err) {
			return fmt.Errorf("error creating branch: %v", grpcutil.ScrubGRPC(err))
		}
	case op.Pipeline != nil:
		sanitizePipeline(op.Pipeline)
		if _, err := pachClient.PpsAPIClient.CreatePipeline(pachClient.Ctx(), op.Pipeline); err != nil && !errutil.IsAlreadyExistError(err) {
			return fmt.Errorf("error creating pipeline: %v", grpcutil.ScrubGRPC(err))
		}
	}
	return nil
}

func sanitizePipeline(req *pps.CreatePipelineRequest) {
	req.Pipeline.Name = ancestry.SanitizeName(req.Pipeline.Name)
	pps.VisitInput(req.Input, func(input *pps.Input) {
		if input.Pfs != nil {
			if input.Pfs.Branch != "" {
				input.Pfs.Branch = ancestry.SanitizeName(input.Pfs.Branch)
			}
			input.Pfs.Repo = ancestry.SanitizeName(input.Pfs.Repo)
		}
	})
}

func (a *apiServer) getPachClient() *client.APIClient {
	a.pachClientOnce.Do(func() {
		var err error
		a.pachClient, err = client.NewFromAddress(a.address)
		if err != nil {
			panic(fmt.Sprintf("pps failed to initialize pach client: %v", err))
		}
	})
	return a.pachClient
}

type extractObjectWriter func(*admin.Op) error

func (w extractObjectWriter) Write(p []byte) (int, error) {
	chunkSize := grpcutil.MaxMsgSize / 2
	var n int
	for i := 0; i*(chunkSize) < len(p); i++ {
		value := p[i*chunkSize:]
		if len(value) > chunkSize {
			value = value[:chunkSize]
		}
		if err := w(&admin.Op{Op1_9: &admin.Op1_9{Object: &pfs.PutObjectRequest{Value: value}}}); err != nil {
			return n, err
		}
		n += len(value)
	}
	return n, nil
}

type adminAPIRestoreServer admin.API_RestoreServer

type extractedObjectReader struct {
	// One of these two must be set (whether user is restoring over the wire or
	// via URL)
	adminAPIRestoreServer
	restoreURLReader pbutil.Reader

	version opVersion
	buf     bytes.Buffer
	eof     bool
}

func (r *extractedObjectReader) Read(p []byte) (int, error) {
	// Shortcut -- if object is done just return EOF
	if r.eof {
		return 0, io.EOF
	}

	// Read leftover bytes in buffer (from prior Read() call) into 'p'
	n, err := r.buf.Read(p)
	if n == len(p) || err != nil && err != io.EOF {
		return n, err // quit early if done; ignore EOF--just means buf is now empty
	}
	r.buf.Reset() // discard data now in 'p'; ready to refill 'r.buf'
	p = p[n:]     // only want to fill remainder of p

	// refill 'r.buf'
	for len(p) > r.buf.Len() && !r.eof {
		var op *admin.Op
		if r.restoreURLReader == nil {
			request, err := r.Recv()
			if err != nil {
				return 0, grpcutil.ScrubGRPC(err)
			}
			op = request.Op
		} else {
			if op == nil {
				op = &admin.Op{}
			} else {
				*op = admin.Op{} // clear 'op' without making old contents into garbage
			}
			if err := r.restoreURLReader.Read(op); err != nil {
				return 0, fmt.Errorf("unexpected error while restoring object: %v", err)
			}
		}

		// Validate op version
		if r.version != version(op) {
			return 0, fmt.Errorf("cannot mix different versions of pachd operation "+
				"within a metadata dumps (found both %s and %s)", version(op), r.version)
		}

		// extract object bytes
		var value []byte
		if r.version == v1_7 {
			if op.Op1_7.Object == nil {
				return 0, fmt.Errorf("expected an object, but got: %v", op)
			}
			value = op.Op1_7.Object.Value
		} else if r.version == v1_8 {
			if op.Op1_8.Object == nil {
				return 0, fmt.Errorf("expected an object, but got: %v", op)
			}
			value = op.Op1_8.Object.Value
		} else {
			if op.Op1_9.Object == nil {
				return 0, fmt.Errorf("expected an object, but got: %v", op)
			}
			value = op.Op1_9.Object.Value
		}

		if len(value) == 0 {
			r.eof = true
		} else {
			r.buf.Write(value)
		}
	}
	dn, err := r.buf.Read(p)
	return n + dn, err
}

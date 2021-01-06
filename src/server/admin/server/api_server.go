package server

import (
	"bytes"
	"fmt"
	"io"
	"regexp"
	"sync"
	"time"

	"github.com/gogo/protobuf/types"
	"github.com/pachyderm/pachyderm/src/client"
	"github.com/pachyderm/pachyderm/src/client/admin"
	"github.com/pachyderm/pachyderm/src/client/auth"
	"github.com/pachyderm/pachyderm/src/client/enterprise"
	"github.com/pachyderm/pachyderm/src/client/pfs"
	"github.com/pachyderm/pachyderm/src/client/pkg/errors"
	"github.com/pachyderm/pachyderm/src/client/pkg/grpcutil"
	"github.com/pachyderm/pachyderm/src/client/pkg/pbutil"
	"github.com/pachyderm/pachyderm/src/client/pps"
	"github.com/pachyderm/pachyderm/src/server/pkg/ancestry"
	"github.com/pachyderm/pachyderm/src/server/pkg/errutil"
	"github.com/pachyderm/pachyderm/src/server/pkg/log"
	"github.com/pachyderm/pachyderm/src/server/pkg/obj"
	"github.com/pachyderm/pachyderm/src/server/pkg/ppsconsts"
	"github.com/pachyderm/pachyderm/src/server/pkg/ppsutil"

	"github.com/golang/snappy"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
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
	v1_10
	v1_11
	v1_12
)

func (v opVersion) String() string {
	switch v {
	case v1_7:
		return "1.7"
	case v1_8:
		return "1.8"
	case v1_9:
		return "1.9"
	case v1_10:
		return "1.10"
	case v1_11:
		return "1.11"
	case v1_12:
		return "1.12"
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
	case op.Op1_10 != nil:
		return v1_10
	case op.Op1_11 != nil:
		return v1_11
	case op.Op1_12 != nil:
		return v1_12
	default:
		return undefined
	}
}

func (a *apiServer) Extract(request *admin.ExtractRequest, extractServer admin.API_ExtractServer) (retErr error) {
	func() { a.Log(request, nil, nil, 0) }()
	defer func(start time.Time) { a.Log(request, nil, retErr, time.Since(start)) }(time.Now())
	ctx := extractServer.Context()
	pachClient := a.getPachClient().WithCtx(ctx)

	// Check whether the user is an admin at beginning of the call.
	// Many of these APIs also require admin status, but the block API does not.
	whoAmI, err := pachClient.WhoAmI(pachClient.Ctx(), &auth.WhoAmIRequest{})
	if err != nil {
		if auth.IsErrNotActivated(err) {
			request.NoAuth = true
		} else {
			return err
		}
	} else {
		if !whoAmI.IsAdmin {
			return &auth.ErrNotAuthorized{
				Subject: whoAmI.Username,
				AdminOp: "Extract",
			}
		}
	}

	writeOp := extractServer.Send

	if request.URL != "" {
		url, err := obj.ParseURL(request.URL)
		if err != nil {
			return errors.Wrapf(err, "error parsing url %v", request.URL)
		}
		if url.Object == "" {
			return errors.Errorf("URL must be <svc>://<bucket>/<object> (no object in %s)", request.URL)
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

	// If auth is enabled on the cluster and the user has requested to extract auth info, the first op
	// should verify the client doing the restore has provided a valid auth token to use in Activate.
	// This ensures we don't fail to restore half-way through.
	if !request.NoAuth {
		if _, err := pachClient.GetConfiguration(pachClient.Ctx(), &auth.GetConfigurationRequest{}); err != nil {
			return err
		}

		if err := writeOp(&admin.Op{Op1_12: &admin.Op1_12{CheckAuthToken: &admin.CheckAuthToken{}}}); err != nil {
			return err
		}
	}

	if !request.NoObjects {
		if err := pachClient.ListBlock(func(block *pfs.Block) error {
			w := &extractBlockWriter{f: writeOp, block: block}
			if err := pachClient.GetBlock(block.Hash, w); err != nil {
				return err
			}
			return w.Close()
		}); err != nil {
			return err
		}
		if err := pachClient.ListObject(func(oi *pfs.ObjectInfo) error {
			return writeOp(&admin.Op{Op1_12: &admin.Op1_12{CreateObject: &pfs.CreateObjectRequest{
				Object:   oi.Object,
				BlockRef: oi.BlockRef,
			}}})
		}); err != nil {
			return err
		}
		if err := pachClient.ListTag(func(resp *pfs.ListTagsResponse) error {
			return writeOp(&admin.Op{Op1_12: &admin.Op1_12{
				Tag: &pfs.TagObjectRequest{
					Object: resp.Object,
					Tags:   []*pfs.Tag{resp.Tag},
				},
			}})
		}); err != nil {
			return err
		}
	}

	var ris []*pfs.RepoInfo
	if !request.NoRepos {
		ris, err = pachClient.ListRepo()
		if err != nil {
			return err
		}
		ris = append(ris, &pfs.RepoInfo{Repo: &pfs.Repo{Name: ppsconsts.SpecRepo}})
		for i := range ris {
			ri := ris[len(ris)-1-i]
			if err := writeOp(&admin.Op{Op1_12: &admin.Op1_12{
				Repo: &pfs.CreateRepoRequest{
					Repo:        ri.Repo,
					Description: ri.Description,
				}},
			}); err != nil {
				return err
			}
		}
		if err := pachClient.ListCommitF("", "", "", 0, true, func(ci *pfs.CommitInfo) error {
			if ci.ParentCommit == nil {
				ci.ParentCommit = client.NewCommit(ci.Commit.Repo.Name, "")
			}
			// Restore must not create any open commits (which can interfere with
			// restoring other commits), so started and finished are always set
			if ci.Finished == nil {
				logrus.Warnf("Commit %q is not finished, so its data cannot be extracted, and any data it contains will not be restored", ci.Commit.ID)
				ci.Finished = types.TimestampNow()
			}
			return writeOp(&admin.Op{Op1_12: &admin.Op1_12{Commit: &pfs.BuildCommitRequest{
				Origin:     ci.Origin,
				Parent:     ci.ParentCommit,
				Tree:       ci.Tree,
				ID:         ci.Commit.ID,
				Trees:      ci.Trees,
				Datums:     ci.Datums,
				SizeBytes:  ci.SizeBytes,
				Provenance: ci.Provenance,
				Started:    ci.Started,
				Finished:   ci.Finished,
			}}})
		}); err != nil {
			return err
		}
		bis, err := pachClient.PfsAPIClient.ListBranch(pachClient.Ctx(),
			&pfs.ListBranchRequest{
				Repo:    client.NewRepo(""),
				Reverse: true,
			},
		)
		if err != nil {
			return err
		}
		for _, bi := range bis.BranchInfo {
			if err := writeOp(&admin.Op{Op1_12: &admin.Op1_12{
				Branch: &pfs.CreateBranchRequest{
					Head:       bi.Head,
					Branch:     bi.Branch,
					Provenance: bi.DirectProvenance,
				},
			}}); err != nil {
				return err
			}
		}
	}
	if !request.NoPipelines {
		pis, err := pachClient.ListPipeline()
		if err != nil {
			return err
		}
		pis = sortPipelineInfos(pis)
		for _, pi := range pis {
			cPR := ppsutil.PipelineReqFromInfo(pi)
			cPR.SpecCommit = pi.SpecCommit
			if err := writeOp(&admin.Op{Op1_12: &admin.Op1_12{Pipeline: cPR}}); err != nil {
				return err
			}
			if err := pachClient.ListJobF(pi.Pipeline.Name, nil, nil, -1, false, func(ji *pps.JobInfo) error {
				return writeOp(&admin.Op{Op1_12: &admin.Op1_12{Job: &pps.CreateJobRequest{
					Pipeline:      pi.Pipeline,
					OutputCommit:  ji.OutputCommit,
					Restart:       ji.Restart,
					DataProcessed: ji.DataProcessed,
					DataSkipped:   ji.DataSkipped,
					DataTotal:     ji.DataTotal,
					DataFailed:    ji.DataFailed,
					DataRecovered: ji.DataRecovered,
					Stats:         ji.Stats,
					StatsCommit:   ji.StatsCommit,
					State:         ji.State,
					Reason:        ji.Reason,
					Started:       ji.Started,
					Finished:      ji.Finished,
				}}})
			}); err != nil {
				return err
			}
		}
	}
	if !request.NoEnterprise {
		state, err := pachClient.Enterprise.GetState(pachClient.Ctx(), &enterprise.GetStateRequest{})
		if err != nil {
			return err
		}
		// Don't write out expired or missing enterprise keys
		if state.State == enterprise.State_ACTIVE {
			if err := writeOp(&admin.Op{Op1_12: &admin.Op1_12{ActivateEnterprise: &enterprise.ActivateRequest{ActivationCode: state.ActivationCode}}}); err != nil {
				return err
			}
		} else {
			logrus.Warnf("Enterprise license state: %v, not extracting", state.State)
		}
	}

	// Once all the repos and pipelines have been restored, then try to enable auth. Our auth checks block creating a pipeline if the output repo already exists,
	// so we need to wait until everything is restored to turn auth back on.
	if !request.NoAuth {
		config, err := pachClient.GetConfiguration(pachClient.Ctx(), &auth.GetConfigurationRequest{})
		if err != nil {
			return err
		}

		// Activate auth using the token provided by the user. This will make `pach:root` the sole admin
		// and allow the user to authenticate with the token (which they know from the extract process).
		if err := writeOp(&admin.Op{Op1_12: &admin.Op1_12{ActivateAuth: &auth.ActivateRequest{RootToken: ""}}}); err != nil {
			return err
		}

		// Extract all robot tokens. This includes the backup root token created by pachctl, which can be used to recover the cluster if the IDP configuration is wrong.
		robotTokens, err := pachClient.ExtractAuthTokens(pachClient.Ctx(), &auth.ExtractAuthTokensRequest{})
		if err != nil {
			return err
		}

		for _, t := range robotTokens.Tokens {
			if err := writeOp(&admin.Op{Op1_12: &admin.Op1_12{RestoreAuthToken: &auth.RestoreAuthTokenRequest{Token: t}}}); err != nil {
				if !auth.IsErrExpiredToken(err) {
					return err
				}
				logrus.Warnf("Auth token for user %q was not restored, expired", t.TokenInfo.Subject)
			}
		}

		// Extract all the existing cluster role bindings for super and fs admins.
		admins, err := pachClient.GetClusterRoleBindings(pachClient.Ctx(), &auth.GetClusterRoleBindingsRequest{})
		if err != nil {
			return err
		}
		for principal, roles := range admins.Bindings {
			if err := writeOp(&admin.Op{Op1_12: &admin.Op1_12{SetClusterRoleBinding: &auth.ModifyClusterRoleBindingRequest{
				Principal: principal,
				Roles:     roles,
			}}}); err != nil {
				return err
			}
		}

		// Restore the auth configuration.
		config.Configuration.LiveConfigVersion = 0
		if err := writeOp(&admin.Op{Op1_12: &admin.Op1_12{SetAuthConfig: &auth.SetConfigurationRequest{Configuration: config.Configuration}}}); err != nil {
			return err
		}

		// If repos are being extracted, restore the ACLs once the repos have been created.
		if !request.NoRepos {
			for _, ri := range ris {
				acl, err := pachClient.GetACL(pachClient.Ctx(), &auth.GetACLRequest{Repo: ri.Repo.Name})
				if err != nil {
					return err
				}
				if err := writeOp(&admin.Op{Op1_12: &admin.Op1_12{SetAcl: &auth.SetACLRequest{
					Repo:    ri.Repo.Name,
					Entries: acl.Entries,
				}}}); err != nil {
					return err
				}
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
	return &admin.Op{Op1_12: &admin.Op1_12{Pipeline: ppsutil.PipelineReqFromInfo(pi)}}, nil
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

func (a *apiServer) Restore(restoreServer admin.API_RestoreServer) (retErr error) {
	func() { a.Log(nil, nil, nil, 0) }()
	defer func(start time.Time) { a.Log(nil, nil, retErr, time.Since(start)) }(time.Now())
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
	pachClient := a.getPachClient().WithCtx(restoreServer.Context())

	// Check whether the user is an admin at beginning of the call. If the cluster
	// already has auth enabled we shouldn't allow non-admin users to restore at all.
	whoAmI, err := pachClient.WhoAmI(pachClient.Ctx(), &auth.WhoAmIRequest{})
	if err != nil {
		if !auth.IsErrNotActivated(err) {
			return err
		}
	} else {
		if !whoAmI.IsAdmin {
			return &auth.ErrNotAuthorized{
				Subject: whoAmI.Username,
				AdminOp: "Restore",
			}
		}
	}

	// Determine if we're restoring from a URL or not
	r := &restoreCtx{
		a:             a,
		restoreServer: restoreServer,
		// TODO(msteffen): refactor admin apiServer to use serviceenv
		pachClient: pachClient,
	}
	req, err := restoreServer.Recv()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil
		}
		return err
	}
	if req.URL != "" {
		return r.startFromURL(req.URL)
	}
	return r.start(req.Op)
}

// restoreCtx holds the partial results needed to restore a stream of ops to
// Pachyderm. It's designed to be called with the following flow:
// ==========
//   apiServer.Restore()
//           │
// +---------+------------------------------------------------------------------------+
// |         │                   | restoreCtx |                                       |
// |         │                   +------------+                                       |
// |         ↓                                                                        |
// | start/startFromURL // (reads ops from stream in a loop)                          |
// |         ↓                                                                        |
// | validateAndApplyOp ──┬───────────-┬─────────────┬─────────────╮                  |
// |         ↓            ↓            ↓             ↓             ↓                  |
// |     applyOp1_7 → applyOp1_8 → applyOp1_9 → applyOp1_10 → applyOp1_11 → applyOp   |
type restoreCtx struct {
	a *apiServer

	// invariant: pachClient.Ctx() == restoreServer.Context()
	restoreServer admin.API_RestoreServer
	pachClient    *client.APIClient

	r pbutil.Reader // set iff restoring from URL

	// streamVersion specifies the version of all ops in the stream (they must all
	// be the same). streamVersion is set in validateAndApplyOp from first op's
	// version
	streamVersion opVersion
}

func (r *restoreCtx) start(initial *admin.Op) error {
	var op *admin.Op
	for {
		if initial != nil {
			op, initial = initial, nil
		} else {
			req, err := r.restoreServer.Recv()
			if err != nil {
				if errors.Is(err, io.EOF) {
					return nil
				}
				return err
			}
			op = req.Op
		}
		if err := r.validateAndApplyOp(op); err != nil {
			return err
		}
	}
}

func (r *restoreCtx) startFromURL(reqURL string) error {
	// Initialize object client from URL
	url, err := obj.ParseURL(reqURL)
	if err != nil {
		return errors.Wrapf(err, "error parsing url %v", reqURL)
	}
	if url.Object == "" {
		return errors.Errorf("URL must be <svc>://<bucket>/<object> (no object in %s)", reqURL)
	}
	objClient, err := obj.NewClientFromURLAndSecret(url, false)
	if err != nil {
		return err
	}
	objR, err := objClient.Reader(r.pachClient.Ctx(), url.Object, 0, 0)
	if err != nil {
		return err
	}
	snappyR := snappy.NewReader(objR)
	r.r = pbutil.NewReader(snappyR)
	var op admin.Op
	for {
		op.Reset()
		if err := r.r.Read(&op); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if err := r.validateAndApplyOp(&op); err != nil {
			return err
		}
	}
}

// validateAndApplyOp is a helper called by start() and startFromURL(), which
// validates the top-level 'op' and then delegates to the right version of
// 'applyOp':
func (r *restoreCtx) validateAndApplyOp(op *admin.Op) error {
	// validate op version
	opVersion := version(op)
	if r.streamVersion == undefined {
		r.streamVersion = opVersion
	} else if r.streamVersion != opVersion {
		return errors.Errorf("cannot mix different versions of pachd operation "+
			"within a metadata dumps (found both %s and %s)", opVersion, r.streamVersion)
	}
	switch r.streamVersion {
	case v1_7:
		return r.applyOp1_7(op.Op1_7)
	case v1_8:
		return r.applyOp1_8(op.Op1_8)
	case v1_9:
		return r.applyOp1_9(op.Op1_9)
	case v1_10:
		return r.applyOp1_10(op.Op1_10)
	case v1_11:
		return r.applyOp1_11(op.Op1_11)
	case v1_12:
		return r.applyOp1_12(op.Op1_12)
	default:
		return errors.Errorf("unrecognized stream version: %s", r.streamVersion)
	}
}

func (r *restoreCtx) applyOp1_7(op *admin.Op1_7) error {
	if op.Object != nil {
		extractReader := &extractObjectReader{
			adminAPIRestoreServer: r.restoreServer,
			restoreURLReader:      r.r,
			version:               v1_7,
		}
		extractReader.buf.Write(op.Object.Value)
		if _, _, err := r.pachClient.PutObject(extractReader); err != nil {
			return errors.Wrapf(err, "error putting object")
		}
		return nil
	}
	newOp1_8, err := convert1_7Op(r.pachClient, r.a.storageRoot, op)
	if err != nil {
		return err
	}
	return r.applyOp1_8(newOp1_8)
}

func (r *restoreCtx) applyOp1_8(op *admin.Op1_8) error {
	if op.Object != nil {
		extractReader := &extractObjectReader{
			adminAPIRestoreServer: r.restoreServer,
			restoreURLReader:      r.r,
			version:               v1_8,
		}
		extractReader.buf.Write(op.Object.Value)
		if _, _, err := r.pachClient.PutObject(extractReader); err != nil {
			return errors.Wrapf(err, "error putting object")
		}
		return nil
	}
	newOp1_9, err := convert1_8Op(op)
	if err != nil {
		return err
	}
	return r.applyOp1_9(newOp1_9)
}

func (r *restoreCtx) applyOp1_9(op *admin.Op1_9) error {
	switch {
	case op.Object != nil:
		extractReader := &extractObjectReader{
			adminAPIRestoreServer: r.restoreServer,
			restoreURLReader:      r.r,
			version:               v1_9,
		}
		extractReader.buf.Write(op.Object.Value)
		if _, _, err := r.pachClient.PutObject(extractReader); err != nil {
			return errors.Wrapf(err, "error putting object")
		}
		return nil
	case op.Block != nil && len(op.Block.Value) > 0:
		extractReader := &extractBlockReader{
			adminAPIRestoreServer: r.restoreServer,
			restoreURLReader:      r.r,
			version:               v1_9,
		}
		extractReader.buf.Write(op.Block.Value)
		if _, err := r.pachClient.PutBlock(op.Block.Block.Hash, extractReader); err != nil {
			return errors.Wrapf(err, "error putting block")
		}
		return nil
	case op.Block != nil && len(op.Block.Value) == 0:
		// Empty block
		if _, err := r.pachClient.PutBlock(op.Block.Block.Hash, bytes.NewReader(nil)); err != nil {
			return errors.Wrapf(err, "error putting block")
		}
		return nil
	default:
		newOp, err := convert1_9Op(op)
		if err != nil {
			return err
		}
		if err := r.applyOp1_10(newOp); err != nil {
			return err
		}
		return nil
	}
}

func (r *restoreCtx) applyOp1_10(op *admin.Op1_10) error {
	switch {
	case op.Object != nil:
		extractReader := &extractObjectReader{
			adminAPIRestoreServer: r.restoreServer,
			restoreURLReader:      r.r,
			version:               v1_10,
		}
		extractReader.buf.Write(op.Object.Value)
		if _, _, err := r.pachClient.PutObject(extractReader); err != nil {
			return errors.Wrapf(err, "error putting object")
		}
		return nil
	case op.Block != nil && len(op.Block.Value) > 0:
		extractReader := &extractBlockReader{
			adminAPIRestoreServer: r.restoreServer,
			restoreURLReader:      r.r,
			version:               v1_10,
		}
		extractReader.buf.Write(op.Block.Value)
		if _, err := r.pachClient.PutBlock(op.Block.Block.Hash, extractReader); err != nil {
			return errors.Wrapf(err, "error putting block")
		}
		return nil
	case op.Block != nil && len(op.Block.Value) == 0:
		// Empty block
		if _, err := r.pachClient.PutBlock(op.Block.Block.Hash, bytes.NewReader(nil)); err != nil {
			return errors.Wrapf(err, "error putting block")
		}
		return nil
	default:
		newOp, err := convert1_10Op(op)
		if err != nil {
			return err
		}
		if err := r.applyOp1_11(newOp); err != nil {
			return err
		}
		return nil
	}
}

func (r *restoreCtx) applyOp1_11(op *admin.Op1_11) error {
	switch {
	case op.Object != nil:
		extractReader := &extractObjectReader{
			adminAPIRestoreServer: r.restoreServer,
			restoreURLReader:      r.r,
			version:               v1_11,
		}
		extractReader.buf.Write(op.Object.Value)
		if _, _, err := r.pachClient.PutObject(extractReader); err != nil {
			return errors.Wrapf(err, "error putting object")
		}
		return nil
	case op.Block != nil && len(op.Block.Value) > 0:
		extractReader := &extractBlockReader{
			adminAPIRestoreServer: r.restoreServer,
			restoreURLReader:      r.r,
			version:               v1_11,
		}
		extractReader.buf.Write(op.Block.Value)
		if _, err := r.pachClient.PutBlock(op.Block.Block.Hash, extractReader); err != nil {
			return errors.Wrapf(err, "error putting block")
		}
		return nil
	case op.Block != nil && len(op.Block.Value) == 0:
		// Empty block
		if _, err := r.pachClient.PutBlock(op.Block.Block.Hash, bytes.NewReader(nil)); err != nil {
			return errors.Wrapf(err, "error putting block")
		}
		return nil
	default:
		newOp, err := convert1_11Op(op)
		if err != nil {
			return err
		}
		if err := r.applyOp1_12(newOp); err != nil {
			return err
		}
		return nil
	}
}

func (r *restoreCtx) applyOp1_12(op *admin.Op1_12) error {
	switch {
	case op.Object != nil:
		extractReader := &extractObjectReader{
			adminAPIRestoreServer: r.restoreServer,
			restoreURLReader:      r.r,
			version:               v1_12,
		}
		extractReader.buf.Write(op.Object.Value)
		if _, _, err := r.pachClient.PutObject(extractReader); err != nil {
			return errors.Wrapf(err, "error putting object")
		}
		return nil
	case op.Block != nil && len(op.Block.Value) > 0:
		extractReader := &extractBlockReader{
			adminAPIRestoreServer: r.restoreServer,
			restoreURLReader:      r.r,
			version:               v1_12,
		}
		extractReader.buf.Write(op.Block.Value)
		if _, err := r.pachClient.PutBlock(op.Block.Block.Hash, extractReader); err != nil {
			return errors.Wrapf(err, "error putting block")
		}
		return nil
	case op.Block != nil && len(op.Block.Value) == 0:
		// Empty block
		if _, err := r.pachClient.PutBlock(op.Block.Block.Hash, bytes.NewReader(nil)); err != nil {
			return errors.Wrapf(err, "error putting block")
		}
		return nil
	default:
		return r.applyOp(op)
	}
}

func (r *restoreCtx) applyOp(op *admin.Op1_12) error {
	c := r.pachClient
	ctx := r.pachClient.Ctx()
	switch {
	case op.CreateObject != nil:
		if _, err := c.ObjectAPIClient.CreateObject(ctx, op.CreateObject); err != nil {
			return errors.Wrapf(grpcutil.ScrubGRPC(err), "error creating object")
		}
	case op.Tag != nil:
		if _, err := c.ObjectAPIClient.TagObject(ctx, op.Tag); err != nil {
			return errors.Wrapf(grpcutil.ScrubGRPC(err), "error tagging object")
		}
	case op.Repo != nil:
		op.Repo.Repo.Name = ancestry.SanitizeName(op.Repo.Repo.Name)
		if _, err := c.PfsAPIClient.CreateRepo(ctx, op.Repo); err != nil && !errutil.IsAlreadyExistError(err) {
			return errors.Wrapf(grpcutil.ScrubGRPC(err), "error creating repo")
		}
	case op.Commit != nil:
		if op.Commit.Finished == nil {
			// Never allow Restore() to create an unfinished commit. They can only
			// show up in dumps due to issue #4695 and are never there deliberately.
			// Allowing Restore() to create them can cause issues restoring subsequent
			// commits and corrupt the entire cluster.
			op.Commit.Finished = types.TimestampNow()
		}
		if _, err := c.PfsAPIClient.BuildCommit(ctx, op.Commit); err != nil && !errutil.IsAlreadyExistError(err) {
			return errors.Wrapf(grpcutil.ScrubGRPC(err), "error creating commit")
		}
	case op.Branch != nil:
		if op.Branch.Branch == nil {
			op.Branch.Branch = client.NewBranch(op.Branch.Head.Repo.Name, ancestry.SanitizeName(op.Branch.SBranch))
		}
		if _, err := c.PfsAPIClient.CreateBranch(ctx, op.Branch); err != nil && !errutil.IsAlreadyExistError(err) {
			return errors.Wrapf(grpcutil.ScrubGRPC(err), "error creating branch")
		}
	case op.Pipeline != nil:
		sanitizePipeline(op.Pipeline)
		if _, err := c.PpsAPIClient.CreatePipeline(ctx, op.Pipeline); err != nil && !errutil.IsAlreadyExistError(err) {
			return errors.Wrapf(grpcutil.ScrubGRPC(err), "error creating pipeline")
		}
	case op.Job != nil:
		if _, err := c.PpsAPIClient.CreateJob(ctx, op.Job); err != nil && !errutil.IsAlreadyExistError(err) {
			return errors.Wrapf(grpcutil.ScrubGRPC(err), "error creating job")
		}
	case op.SetAcl != nil:
		if _, err := c.SetACL(ctx, op.SetAcl); err != nil {
			return errors.Wrapf(grpcutil.ScrubGRPC(err), "error setting ACL for repo")
		}
	case op.SetClusterRoleBinding != nil:
		if _, err := c.ModifyClusterRoleBinding(ctx, op.SetClusterRoleBinding); err != nil {
			return errors.Wrapf(grpcutil.ScrubGRPC(err), "error setting cluster role bindings")
		}
	case op.SetAuthConfig != nil:
		if _, err := c.SetConfiguration(ctx, op.SetAuthConfig); err != nil {
			return errors.Wrapf(grpcutil.ScrubGRPC(err), "error setting authorization config")
		}
	case op.ActivateAuth != nil:
		// ActivateAuth by setting the root token to the current context's auth token. This guarantees the
		// user will have access to the cluster afterwards.
		authToken, err := auth.GetAuthToken(ctx)
		if err != nil {
			return errors.Wrapf(grpcutil.ScrubGRPC(err), "failed to get auth token from incoming context")
		}

		if _, err := c.AuthAPIClient.Activate(ctx, &auth.ActivateRequest{RootToken: authToken}); err != nil {
			return errors.Wrapf(grpcutil.ScrubGRPC(err), "error activating authentication")
		}
	case op.RestoreAuthToken != nil:
		if _, err := c.RestoreAuthToken(ctx, op.RestoreAuthToken); err != nil {
			return errors.Wrapf(grpcutil.ScrubGRPC(err), "error restoring auth token")
		}
	case op.ActivateEnterprise != nil:
		if _, err := c.Enterprise.Activate(ctx, op.ActivateEnterprise); err != nil {
			return errors.Wrapf(grpcutil.ScrubGRPC(err), "error activating enterprise license")
		}
	case op.CheckAuthToken != nil:
		// CheckAuthToken is inserted at the beginning of an extract that contains an ActivateAuth call
		// The goal is just to check that the calling context includes a non-empty token we can use
		// to activate the cluster.
		if _, err := auth.GetAuthToken(ctx); err != nil {
			return errors.Wrapf(grpcutil.ScrubGRPC(err), "failed to get auth token from incoming context")
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

type adminAPIRestoreServer admin.API_RestoreServer

type extractObjectReader struct {
	// One of these two must be set (whether user is restoring over the wire or
	// via URL)
	adminAPIRestoreServer
	restoreURLReader pbutil.Reader

	version opVersion
	buf     bytes.Buffer
	eof     bool
}

func (r *extractObjectReader) Read(p []byte) (int, error) {
	// Shortcut -- if object is done just return EOF
	if r.eof {
		return 0, io.EOF
	}

	// Read leftover bytes in buffer (from prior Read() call) into 'p'
	n, err := r.buf.Read(p)
	if n == len(p) || err != nil && !errors.Is(err, io.EOF) {
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
				return 0, errors.Wrapf(err, "unexpected error while restoring object")
			}
		}

		// Validate op version
		if r.version != version(op) {
			return 0, errors.Errorf("cannot mix different versions of pachd operation "+
				"within a metadata dumps (found both %s and %s)", version(op), r.version)
		}

		// extract object bytes
		var value []byte
		if r.version == v1_7 {
			if op.Op1_7.Object == nil {
				return 0, errors.Errorf("expected an object, but got: %v", op)
			}
			value = op.Op1_7.Object.Value
		} else if r.version == v1_8 {
			if op.Op1_8.Object == nil {
				return 0, errors.Errorf("expected an object, but got: %v", op)
			}
			value = op.Op1_8.Object.Value
		} else {
			if op.Op1_9.Object == nil {
				return 0, errors.Errorf("expected an object, but got: %v", op)
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

type extractBlockWriter struct {
	f     func(*admin.Op) error
	block *pfs.Block
}

func (w extractBlockWriter) Write(p []byte) (int, error) {
	chunkSize := grpcutil.MaxMsgSize / 2
	var n int
	for i := 0; i*(chunkSize) < len(p); i++ {
		value := p[i*chunkSize:]
		if len(value) > chunkSize {
			value = value[:chunkSize]
		}
		if err := w.f(&admin.Op{Op1_12: &admin.Op1_12{Block: &pfs.PutBlockRequest{Block: w.block, Value: value}}}); err != nil {
			return n, err
		}
		w.block = nil // only need to send block on the first request
		n += len(value)
	}
	return n, nil
}

func (w extractBlockWriter) Close() error {
	return w.f(&admin.Op{Op1_12: &admin.Op1_12{Block: &pfs.PutBlockRequest{Block: w.block}}})
}

type extractBlockReader struct {
	// One of these two must be set (whether user is restoring over the wire or
	// via URL)
	adminAPIRestoreServer
	restoreURLReader pbutil.Reader

	version opVersion
	buf     bytes.Buffer
	eof     bool
}

func (r *extractBlockReader) Read(p []byte) (int, error) {
	// Shortcut -- if object is done just return EOF
	if r.eof {
		return 0, io.EOF
	}

	// Read leftover bytes in buffer (from prior Read() call) into 'p'
	n, err := r.buf.Read(p)
	if n == len(p) || err != nil && !errors.Is(err, io.EOF) {
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
				return 0, errors.Wrapf(err, "unexpected error while restoring object")
			}
		}

		// Validate op version
		if r.version != version(op) {
			return 0, errors.Errorf("cannot mix different versions of pachd operation "+
				"within a metadata dumps (found both %s and %s)", version(op), r.version)
		}

		// extract object bytes
		var value []byte
		if r.version == v1_7 {
			return 0, errors.Errorf("invalid version 1.7 doesn't have extracted blocks")
		} else if r.version == v1_8 {
			return 0, errors.Errorf("invalid version 1.8 doesn't have extracted blocks")
		} else if r.version == v1_9 {
			if op.Op1_9.Block == nil {
				return 0, errors.Errorf("expected a block, but got: %v", op)
			}
			value = op.Op1_9.Block.Value
		} else if r.version == v1_10 {
			if op.Op1_10.Block == nil {
				return 0, errors.Errorf("expected a block, but got: %v", op)
			}
			value = op.Op1_10.Block.Value
		} else if r.version == v1_11 {
			if op.Op1_11.Block == nil {
				return 0, errors.Errorf("expected a block, but got: %v", op)
			}
			value = op.Op1_11.Block.Value
		} else {
			if op.Op1_12.Block == nil {
				return 0, errors.Errorf("expected a block, but got: %v", op)
			}
			value = op.Op1_12.Block.Value
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

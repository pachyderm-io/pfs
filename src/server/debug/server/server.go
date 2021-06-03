package server

import (
	"archive/tar"
	"context"
	"io"
	"math"
	"os"
	"runtime/pprof"
	"strings"
	"time"

	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/types"
	"github.com/pachyderm/pachyderm/src/client"
	"github.com/pachyderm/pachyderm/src/client/auth"
	"github.com/pachyderm/pachyderm/src/client/debug"
	"github.com/pachyderm/pachyderm/src/client/pfs"
	"github.com/pachyderm/pachyderm/src/client/pkg/errors"
	"github.com/pachyderm/pachyderm/src/client/pkg/grpcutil"
	"github.com/pachyderm/pachyderm/src/client/pps"
	"github.com/pachyderm/pachyderm/src/server/pkg/errutil"
	"github.com/pachyderm/pachyderm/src/server/pkg/ppsutil"
	"github.com/pachyderm/pachyderm/src/server/pkg/serviceenv"
	workerserver "github.com/pachyderm/pachyderm/src/server/worker/server"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TODO: Figure out how pipeline versions should come into play with this.
// Right now we just collect the spec and jobs for the current pipeline version.

const (
	defaultDuration = time.Minute
	pachdPrefix     = "pachd"
	pipelinePrefix  = "pipelines"
	podPrefix       = "pods"
)

type debugServer struct {
	env           *serviceenv.ServiceEnv
	name          string
	sidecarClient *client.APIClient
	marshaller    *jsonpb.Marshaler
}

// NewDebugServer creates a new server that serves the debug api over GRPC
func NewDebugServer(env *serviceenv.ServiceEnv, name string, sidecarClient *client.APIClient) debug.DebugServer {
	return &debugServer{
		env:           env,
		name:          name,
		sidecarClient: sidecarClient,
		marshaller:    &jsonpb.Marshaler{Indent: "  "},
	}
}

func (s *debugServer) pachClient(ctx context.Context) (*client.APIClient, error) {
	pachClient := s.env.GetPachClient(ctx)
	ctx = pachClient.Ctx()
	// check if the caller is authorized -- they must be an admin
	if me, err := pachClient.WhoAmI(ctx, &auth.WhoAmIRequest{}); err == nil {
		var isAdmin bool
		for _, s := range me.ClusterRoles.Roles {
			if s == auth.ClusterRole_SUPER {
				isAdmin = true
				break
			}
		}
		if !isAdmin {
			return nil, &auth.ErrNotAuthorized{
				Subject: me.Username,
				AdminOp: "Debug",
			}
		}
	} else if !auth.IsErrNotActivated(err) {
		return nil, errors.Wrapf(err, "error during authorization check")
	}
	return pachClient, nil
}

type collectPipelineFunc func(*tar.Writer, *pps.PipelineInfo, ...string) error
type collectWorkerFunc func(*tar.Writer, *v1.Pod, ...string) error
type redirectFunc func(debug.DebugClient, *debug.Filter) (io.Reader, error)
type collectFunc func(*tar.Writer, ...string) error

func (s *debugServer) handleRedirect(
	pachClient *client.APIClient,
	w io.Writer,
	filter *debug.Filter,
	collectPachd collectFunc,
	collectPipeline collectPipelineFunc,
	collectWorker collectWorkerFunc,
	redirect redirectFunc,
	collect collectFunc,
) error {
	return withDebugWriter(w, func(tw *tar.Writer) error {
		// Handle filter.
		pachdContainerPrefix := join(pachdPrefix, s.name, "pachd")
		if filter != nil {
			switch f := filter.Filter.(type) {
			case *debug.Filter_Pachd:
				return collectPachd(tw, pachdContainerPrefix)
			case *debug.Filter_Pipeline:
				pipelineInfo, err := pachClient.InspectPipeline(f.Pipeline.Name)
				if err != nil {
					return err
				}
				return s.handlePipelineRedirect(tw, pipelineInfo, collectPipeline, collectWorker, redirect)
			case *debug.Filter_Worker:
				if f.Worker.Redirected {
					// Collect the storage container.
					if s.sidecarClient == nil {
						return collect(tw, client.PPSWorkerSidecarContainerName)
					}
					// Collect the user container.
					if err := collect(tw, client.PPSWorkerUserContainerName); err != nil {
						return err
					}
					// Redirect to the storage container.
					r, err := redirect(s.sidecarClient.DebugClient, filter)
					if err != nil {
						return err
					}
					return collectDebugStream(tw, r)

				}
				ctx := context.TODO()
				pod, err := s.env.GetKubeClient().CoreV1().Pods(s.env.Namespace).Get(ctx, f.Worker.Pod, metav1.GetOptions{})
				if err != nil {
					return err
				}
				return s.handleWorkerRedirect(tw, pod, collectWorker, redirect)
			}
		}
		// No filter, collect everything.
		if err := collectPachd(tw, pachdContainerPrefix); err != nil {
			return err
		}
		pipelineInfos, err := pachClient.ListPipeline()
		if err != nil {
			return err
		}
		for _, pipelineInfo := range pipelineInfos {
			if err := s.handlePipelineRedirect(tw, pipelineInfo, collectPipeline, collectWorker, redirect); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *debugServer) handlePipelineRedirect(
	tw *tar.Writer,
	pipelineInfo *pps.PipelineInfo,
	collectPipeline collectPipelineFunc,
	collectWorker collectWorkerFunc,
	redirect redirectFunc,
) (retErr error) {
	prefix := join(pipelinePrefix, pipelineInfo.Pipeline.Name)
	defer func() {
		if retErr != nil {
			retErr = writeErrorFile(tw, retErr, prefix)
		}
	}()
	if collectPipeline != nil {
		if err := collectPipeline(tw, pipelineInfo, prefix); err != nil {
			return err
		}
	}
	return s.forEachWorker(tw, pipelineInfo, func(pod *v1.Pod) error {
		return s.handleWorkerRedirect(tw, pod, collectWorker, redirect, prefix)
	})
}

func (s *debugServer) forEachWorker(tw *tar.Writer, pipelineInfo *pps.PipelineInfo, cb func(*v1.Pod) error) error {
	pods, err := s.getWorkerPods(pipelineInfo)
	if err != nil {
		return err
	}
	if len(pods) == 0 {
		return errors.Errorf("no worker pods found for pipeline %v", pipelineInfo.Pipeline.Name)
	}
	for _, pod := range pods {
		if err := cb(&pod); err != nil {
			return err
		}
	}
	return nil
}

func (s *debugServer) getWorkerPods(pipelineInfo *pps.PipelineInfo) ([]v1.Pod, error) {
	ctx := context.TODO()
	podList, err := s.env.GetKubeClient().CoreV1().Pods(s.env.Namespace).List(ctx,
		metav1.ListOptions{
			TypeMeta: metav1.TypeMeta{
				Kind:       "ListOptions",
				APIVersion: "v1",
			},
			LabelSelector: metav1.FormatLabelSelector(
				metav1.SetAsLabelSelector(
					map[string]string{
						"app": ppsutil.PipelineRcName(pipelineInfo.Pipeline.Name, pipelineInfo.Version),
					},
				),
			),
		},
	)
	if err != nil {
		return nil, err
	}
	return podList.Items, nil
}

func (s *debugServer) handleWorkerRedirect(tw *tar.Writer, pod *v1.Pod, collectWorker collectWorkerFunc, cb redirectFunc, prefix ...string) (retErr error) {
	workerPrefix := join(podPrefix, pod.Name)
	if len(prefix) > 0 {
		workerPrefix = join(prefix[0], workerPrefix)
	}
	defer func() {
		if retErr != nil {
			retErr = writeErrorFile(tw, retErr, workerPrefix)
		}
	}()
	if collectWorker != nil {
		if err := collectWorker(tw, pod, workerPrefix); err != nil {
			return err
		}
	}
	if pod.Status.Phase != v1.PodRunning {
		return errors.Errorf("pod in phase %v, must be in phase %v to collect debug information", pod.Status.Phase, v1.PodRunning)
	}
	c, err := workerserver.NewClient(pod.Status.PodIP)
	if err != nil {
		return err
	}
	r, err := cb(c.DebugClient, &debug.Filter{
		Filter: &debug.Filter_Worker{
			Worker: &debug.Worker{
				Pod:        pod.Name,
				Redirected: true,
			},
		},
	})
	if err != nil {
		return err
	}
	return collectDebugStream(tw, r, workerPrefix)
}

func (s *debugServer) Profile(request *debug.ProfileRequest, server debug.Debug_ProfileServer) error {
	pachClient, err := s.pachClient(server.Context())
	if err != nil {
		return err
	}
	return s.handleRedirect(
		pachClient,
		grpcutil.NewStreamingBytesWriter(server),
		request.Filter,
		collectProfileFunc(request.Profile),
		nil,
		nil,
		redirectProfileFunc(pachClient.Ctx(), request.Profile),
		collectProfileFunc(request.Profile),
	)
}

func collectProfileFunc(profile *debug.Profile) collectFunc {
	return func(tw *tar.Writer, prefix ...string) error {
		return collectProfile(tw, profile, prefix...)
	}
}

func collectProfile(tw *tar.Writer, profile *debug.Profile, prefix ...string) error {
	return collectDebugFile(tw, profile.Name, func(w io.Writer) error {
		return writeProfile(w, profile)
	}, prefix...)
}

func writeProfile(w io.Writer, profile *debug.Profile) error {
	if profile.Name == "cpu" {
		if err := pprof.StartCPUProfile(w); err != nil {
			return err
		}
		duration := defaultDuration
		if profile.Duration != nil {
			var err error
			duration, err = types.DurationFromProto(profile.Duration)
			if err != nil {
				return err
			}
		}
		time.Sleep(duration)
		pprof.StopCPUProfile()
		return nil
	}
	p := pprof.Lookup(profile.Name)
	if p == nil {
		return errors.Errorf("unable to find profile %q", profile.Name)
	}
	return p.WriteTo(w, 2)
}

func redirectProfileFunc(ctx context.Context, profile *debug.Profile) redirectFunc {
	return func(c debug.DebugClient, filter *debug.Filter) (io.Reader, error) {
		profileC, err := c.Profile(ctx, &debug.ProfileRequest{
			Profile: profile,
			Filter:  filter,
		})
		if err != nil {
			return nil, err
		}
		return grpcutil.NewStreamingBytesReader(profileC, nil), nil
	}
}

func (s *debugServer) Binary(request *debug.BinaryRequest, server debug.Debug_BinaryServer) error {
	pachClient, err := s.pachClient(server.Context())
	if err != nil {
		return err
	}
	return s.handleRedirect(
		pachClient,
		grpcutil.NewStreamingBytesWriter(server),
		request.Filter,
		collectBinary,
		nil,
		nil,
		redirectBinaryFunc(pachClient.Ctx()),
		collectBinary,
	)
}

func collectBinary(tw *tar.Writer, prefix ...string) error {
	return collectDebugFile(tw, "binary", func(w io.Writer) (retErr error) {
		f, err := os.Open(os.Args[0])
		if err != nil {
			return err
		}
		defer func() {
			if err := f.Close(); retErr == nil {
				retErr = err
			}
		}()
		_, err = io.Copy(w, f)
		return err
	}, prefix...)
}

func redirectBinaryFunc(ctx context.Context) redirectFunc {
	return func(c debug.DebugClient, filter *debug.Filter) (io.Reader, error) {
		binaryC, err := c.Binary(ctx, &debug.BinaryRequest{Filter: filter})
		if err != nil {
			return nil, err
		}
		return grpcutil.NewStreamingBytesReader(binaryC, nil), nil
	}
}

func (s *debugServer) Dump(request *debug.DumpRequest, server debug.Debug_DumpServer) error {
	if request.Limit == 0 {
		request.Limit = math.MaxInt64
	}
	pachClient, err := s.pachClient(server.Context())
	if err != nil {
		return err
	}
	return s.handleRedirect(
		pachClient,
		grpcutil.NewStreamingBytesWriter(server),
		request.Filter,
		s.collectPachdDumpFunc(pachClient, request.Limit),
		s.collectPipelineDumpFunc(pachClient, request.Limit),
		s.collectWorkerDump,
		redirectDumpFunc(pachClient.Ctx()),
		collectDump,
	)
}

func (s *debugServer) collectPachdDumpFunc(pachClient *client.APIClient, limit int64) collectFunc {
	return func(tw *tar.Writer, prefix ...string) error {
		// Collect input repos.
		if err := s.collectInputRepos(tw, pachClient, limit); err != nil {
			return err
		}
		// Collect the pachd version.
		if err := s.collectPachdVersion(tw, pachClient, prefix...); err != nil {
			return err
		}
		// Collect the pachd container logs.
		if err := s.collectLogs(tw, s.name, "pachd", prefix...); err != nil {
			return err
		}
		// Collect the pachd container dump.
		return collectDump(tw, prefix...)
	}
}

func (s *debugServer) collectInputRepos(tw *tar.Writer, pachClient *client.APIClient, limit int64) error {
	repoInfos, err := pachClient.ListRepo()
	if err != nil {
		return err
	}
	for _, repoInfo := range repoInfos {
		if _, err := pachClient.InspectPipeline(repoInfo.Repo.Name); err != nil {
			// TODO: It would be better for this to be a structured error.
			if strings.Contains(err.Error(), "not found") {
				repoPrefix := join("input-repos", repoInfo.Repo.Name)
				return collectDebugFile(tw, "commits", func(w io.Writer) error {
					return pachClient.ListCommitF(repoInfo.Repo.Name, "", "", uint64(limit), false, func(ci *pfs.CommitInfo) error {
						return s.marshaller.Marshal(w, ci)
					})
				}, repoPrefix)
			}
			return err
		}
	}
	return nil
}

func (s *debugServer) collectPachdVersion(tw *tar.Writer, pachClient *client.APIClient, prefix ...string) error {
	return collectDebugFile(tw, "version", func(w io.Writer) error {
		version, err := pachClient.Version()
		if err != nil {
			return err
		}
		_, err = io.Copy(w, strings.NewReader(version+"\n"))
		return err
	}, prefix...)
}

func (s *debugServer) collectLogs(tw *tar.Writer, pod, container string, prefix ...string) error {
	if err := collectDebugFile(tw, "logs", func(w io.Writer) (retErr error) {
		ctx := context.TODO()
		stream, err := s.env.GetKubeClient().CoreV1().Pods(s.env.Namespace).GetLogs(pod, &v1.PodLogOptions{Container: container}).Stream(ctx)
		if err != nil {
			return err
		}
		defer func() {
			if err := stream.Close(); retErr == nil {
				retErr = err
			}
		}()
		_, err = io.Copy(w, stream)
		return err
	}, prefix...); err != nil {
		return err
	}
	return collectDebugFile(tw, "logs-previous", func(w io.Writer) (retErr error) {
		ctx := context.TODO()
		stream, err := s.env.GetKubeClient().CoreV1().Pods(s.env.Namespace).GetLogs(pod, &v1.PodLogOptions{Container: container, Previous: true}).Stream(ctx)
		if err != nil {
			return err
		}
		defer func() {
			if err := stream.Close(); retErr == nil {
				retErr = err
			}
		}()
		_, err = io.Copy(w, stream)
		return err
	}, prefix...)
}

func collectDump(tw *tar.Writer, prefix ...string) error {
	if err := collectProfile(tw, &debug.Profile{Name: "goroutine"}, prefix...); err != nil {
		return err
	}
	return collectProfile(tw, &debug.Profile{Name: "heap"}, prefix...)
}

func (s *debugServer) collectPipelineDumpFunc(pachClient *client.APIClient, limit int64) collectPipelineFunc {
	return func(tw *tar.Writer, pipelineInfo *pps.PipelineInfo, prefix ...string) error {
		if err := collectDebugFile(tw, "spec", func(w io.Writer) error {
			fullPipelineInfo, err := pachClient.InspectPipeline(pipelineInfo.Pipeline.Name)
			if err != nil {
				return err
			}
			return s.marshaller.Marshal(w, fullPipelineInfo)
		}, prefix...); err != nil {
			return err
		}
		if err := collectDebugFile(tw, "commits", func(w io.Writer) error {
			return pachClient.ListCommitF(pipelineInfo.Pipeline.Name, "", "", uint64(limit), false, func(ci *pfs.CommitInfo) error {
				return s.marshaller.Marshal(w, ci)
			})
		}, prefix...); err != nil {
			return err
		}
		return collectDebugFile(tw, "jobs", func(w io.Writer) error {
			// TODO: The limiting should eventually be a feature of list job.
			var count int64
			return pachClient.ListJobF(pipelineInfo.Pipeline.Name, nil, nil, 0, false, func(ji *pps.JobInfo) error {
				if count >= limit {
					return errutil.ErrBreak
				}
				count++
				return s.marshaller.Marshal(w, ji)
			})
		}, prefix...)
	}
}

func (s *debugServer) collectWorkerDump(tw *tar.Writer, pod *v1.Pod, prefix ...string) error {
	// Collect the worker user and storage container logs.
	userPrefix := client.PPSWorkerUserContainerName
	sidecarPrefix := client.PPSWorkerSidecarContainerName
	if len(prefix) > 0 {
		userPrefix = join(prefix[0], userPrefix)
		sidecarPrefix = join(prefix[0], sidecarPrefix)
	}
	if err := s.collectLogs(tw, pod.Name, client.PPSWorkerUserContainerName, userPrefix); err != nil {
		return err
	}
	return s.collectLogs(tw, pod.Name, client.PPSWorkerSidecarContainerName, sidecarPrefix)
}

func redirectDumpFunc(ctx context.Context) redirectFunc {
	return func(c debug.DebugClient, filter *debug.Filter) (io.Reader, error) {
		dumpC, err := c.Dump(ctx, &debug.DumpRequest{Filter: filter})
		if err != nil {
			return nil, err
		}
		return grpcutil.NewStreamingBytesReader(dumpC, nil), nil
	}
}

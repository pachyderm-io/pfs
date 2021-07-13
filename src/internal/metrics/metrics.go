package metrics

import (
	"fmt"
	"time"

	"github.com/pachyderm/pachyderm/v2/src/enterprise"
	"github.com/pachyderm/pachyderm/v2/src/internal/clientsdk"
	"github.com/pachyderm/pachyderm/v2/src/internal/config"
	"github.com/pachyderm/pachyderm/v2/src/internal/errors"
	"github.com/pachyderm/pachyderm/v2/src/internal/serviceenv"
	"github.com/pachyderm/pachyderm/v2/src/internal/uuid"
	"github.com/pachyderm/pachyderm/v2/src/pfs"
	"github.com/pachyderm/pachyderm/v2/src/pps"
	enterprisemetrics "github.com/pachyderm/pachyderm/v2/src/server/enterprise/metrics"
	"github.com/pachyderm/pachyderm/v2/src/version"

	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc/metadata"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kube "k8s.io/client-go/kubernetes"
)

//Reporter is used to submit user & cluster metrics to segment
type Reporter struct {
	router    *router
	clusterID string
	env       serviceenv.ServiceEnv
}

// NewReporter creates a new reporter and kicks off the loop to report cluster
// metrics
func NewReporter(env serviceenv.ServiceEnv) *Reporter {
	var r *router
	if env.Config().MetricsEndpoint != "" {
		r = newRouter(env.Config().MetricsEndpoint)
	} else {
		r = newRouter()
	}
	reporter := &Reporter{
		router:    r,
		clusterID: env.ClusterID(),
		env:       env,
	}
	go reporter.reportClusterMetrics()
	return reporter
}

//ReportUserAction pushes the action into a queue for reporting,
// and reports the start, finish, and error conditions
func ReportUserAction(ctx context.Context, r *Reporter, action string) func(time.Time, error) {
	if r == nil {
		// This happens when stubbing out metrics for testing, e.g. src/server/pfs/server/server_test.go
		return func(time.Time, error) {}
	}
	// If we report nil, segment sees it, but mixpanel omits the field
	r.reportUserAction(ctx, fmt.Sprintf("%vStarted", action), 1)
	return func(start time.Time, err error) {
		if err == nil {
			r.reportUserAction(ctx, fmt.Sprintf("%vFinished", action), time.Since(start).Seconds())
		} else {
			r.reportUserAction(ctx, fmt.Sprintf("%vErrored", action), err.Error())
		}
	}
}

func getKeyFromMD(md metadata.MD, key string) (string, error) {
	if md[key] != nil && len(md[key]) > 0 {
		return md[key][0], nil
	}
	return "", errors.Errorf("error extracting userid from metadata. userid is empty")
}

func (r *Reporter) reportUserAction(ctx context.Context, action string, value interface{}) {
	md, ok := metadata.FromIncomingContext(ctx)
	if ok {
		// metadata API downcases all the key names
		userID, err := getKeyFromMD(md, "userid")
		if err != nil {
			// The FUSE client will never have a userID, so normal usage will produce a lot of these errors
			return
		}
		prefix, err := getKeyFromMD(md, "prefix")
		if err != nil {
			// metrics errors are non fatal
			return
		}
		r.router.reportUserMetricsToSegment(
			userID,
			prefix,
			action,
			value,
			r.clusterID,
		)
	}
}

// Helper method called by (Start|Finish)ReportAndFlushUserAction. Like those
// functions, it is used by the pachctl binary and runs on users' machines
func reportAndFlushUserAction(action string, value interface{}) func() {
	metricsDone := make(chan struct{})
	go func() {
		defer close(metricsDone)
		cfg, _ := config.Read(false, false)
		if cfg == nil || cfg.UserID == "" || !cfg.V2.Metrics {
			return
		}
		client := newSegmentClient()
		defer client.Close()
		reportUserMetricsToSegment(client, cfg.UserID, "user", action, value, "")
	}()
	return func() {
		select {
		case <-metricsDone:
			return
		case <-time.After(time.Second * 5):
			return
		}
	}
}

// StartReportAndFlushUserAction immediately reports the metric but does
// not block execution. It returns a wait function which waits or times
// out after 5s.
// It is used by the pachctl binary and runs on users' machines
func StartReportAndFlushUserAction(action string, value interface{}) func() {
	return reportAndFlushUserAction(fmt.Sprintf("%vStarted", action), value)
}

// FinishReportAndFlushUserAction immediately reports the metric but does
// not block execution. It returns a wait function which waits or times
// out after 5s.
// It is used by the pachctl binary and runs on users' machines
func FinishReportAndFlushUserAction(action string, err error, start time.Time) func() {
	var wait func()
	if err != nil {
		wait = reportAndFlushUserAction(fmt.Sprintf("%vErrored", action), err)
	} else {
		wait = reportAndFlushUserAction(fmt.Sprintf("%vFinished", action), time.Since(start).Seconds())
	}
	return wait
}

func (r *Reporter) reportClusterMetrics() {
	for {
		time.Sleep(reportingInterval)
		metrics := &Metrics{}
		r.internalMetrics(metrics)
		externalMetrics(r.env.GetKubeClient(), metrics)
		metrics.ClusterID = r.clusterID
		metrics.PodID = uuid.NewWithoutDashes()
		metrics.Version = version.PrettyPrintVersion(version.Version)
		r.router.reportClusterMetricsToSegment(metrics)
	}
}

func externalMetrics(kubeClient *kube.Clientset, metrics *Metrics) error {
	nodeList, err := kubeClient.CoreV1().Nodes().List(metav1.ListOptions{})
	if err != nil {
		return errors.Wrapf(err, "externalMetrics: unable to retrieve node list from k8s")
	}
	metrics.Nodes = int64(len(nodeList.Items))
	return nil
}

func pfsInputMetrics(pfsInput *pps.PFSInput, metrics *Metrics) {
	metrics.InputPfs++
	if pfsInput.Commit != "" {
		metrics.InputCommit++
	}
	if pfsInput.JoinOn != "" {
		metrics.InputJoinOn++
	}
	if pfsInput.OuterJoin {
		metrics.InputOuterJoin++
	}
	if pfsInput.Lazy {
		metrics.InputLazy++
	}
	if pfsInput.EmptyFiles {
		metrics.InputEmptyFiles++
	}
	if pfsInput.S3 {
		metrics.InputS3++
	}
	if pfsInput.Trigger != nil {
		metrics.InputTrigger++
	}
}

func inputMetrics(input *pps.Input, metrics *Metrics) {
	if input.Join != nil {
		metrics.InputJoin++
		for _, item := range input.Join {
			if item.Pfs != nil {
				pfsInputMetrics(item.Pfs, metrics)
			} else {
				inputMetrics(item, metrics)
			}
		}
	}
	if input.Group != nil {
		metrics.InputGroup++
		for _, item := range input.Group {
			if item.Pfs != nil {
				pfsInputMetrics(item.Pfs, metrics)
			} else {
				inputMetrics(item, metrics)
			}
		}
	}
	if input.Cross != nil {
		metrics.InputCross++
		for _, item := range input.Cross {
			if item.Pfs != nil {
				pfsInputMetrics(item.Pfs, metrics)
			} else {
				inputMetrics(item, metrics)
			}
		}
	}
	if input.Union != nil {
		metrics.InputUnion++
		for _, item := range input.Union {
			if item.Pfs != nil {
				pfsInputMetrics(item.Pfs, metrics)
			} else {
				inputMetrics(item, metrics)
			}
		}
	}
	if input.Cron != nil {
		metrics.InputCron++
	}
	if input.Pfs != nil {
		pfsInputMetrics(input.Pfs, metrics)
	}
}

func (r *Reporter) internalMetrics(metrics *Metrics) {
	// We should not return due to an error
	// Activation code
	ctx, cf := context.WithCancel(context.Background())
	defer cf()
	enterpriseState, err := r.env.EnterpriseServer().GetState(ctx, &enterprise.GetStateRequest{})
	if err == nil {
		metrics.ActivationCode = enterpriseState.ActivationCode
	}
	metrics.EnterpriseFailures = enterprisemetrics.GetEnterpriseFailures()

	// Pipeline info
	if err := func() error {
		lpClient, err := r.env.GetPachClient(ctx).PpsAPIClient.ListPipeline(ctx, &pps.ListPipelineRequest{Details: true})
		if err != nil {
			return err
		}
		pipelineInfos, err := clientsdk.ListPipelineInfo(lpClient)
		if err != nil {
			return err
		}
		metrics.Pipelines = int64(len(pipelineInfos)) // Number of pipelines
		for _, pi := range pipelineInfos {
			if pi.JobCounts != nil {
				var cnt int64 = 0
				for _, c := range pi.JobCounts {
					cnt += int64(c)
				}
				if metrics.Jobs < cnt {
					metrics.Jobs = cnt
				}
			}
			if details := pi.Details; details != nil {
				if details.ParallelismSpec != nil {
					if metrics.MaxParallelism < details.ParallelismSpec.Constant {
						metrics.MaxParallelism = details.ParallelismSpec.Constant
					}
					if metrics.MinParallelism > details.ParallelismSpec.Constant {
						metrics.MinParallelism = details.ParallelismSpec.Constant
					}
					metrics.NumParallelism++
				}
				if details.Egress != nil {
					metrics.CfgEgress++
				}
				if details.ResourceRequests != nil {
					if details.ResourceRequests.Cpu != 0 {
						metrics.ResourceCpuReq += details.ResourceRequests.Cpu
						if metrics.ResourceCpuReqMax < details.ResourceRequests.Cpu {
							metrics.ResourceCpuReqMax = details.ResourceRequests.Cpu
						}
					}
					if details.ResourceRequests.Memory != "" {
						metrics.ResourceMemReq += (details.ResourceRequests.Memory + " ")
					}
					if details.ResourceRequests.Gpu != nil {
						metrics.ResourceGpuReq += details.ResourceRequests.Gpu.Number
						if metrics.ResourceGpuReqMax < details.ResourceRequests.Gpu.Number {
							metrics.ResourceGpuReqMax = details.ResourceRequests.Gpu.Number
						}
					}
					if details.ResourceRequests.Disk != "" {
						metrics.ResourceDiskReq += (details.ResourceRequests.Disk + " ")
					}
				}
				if details.ResourceLimits != nil {
					if details.ResourceLimits.Cpu != 0 {
						metrics.ResourceCpuLimit += details.ResourceLimits.Cpu
						if metrics.ResourceCpuLimitMax < details.ResourceLimits.Cpu {
							metrics.ResourceCpuLimitMax = details.ResourceLimits.Cpu
						}
					}
					if details.ResourceLimits.Memory != "" {
						metrics.ResourceMemLimit += (details.ResourceLimits.Memory + " ")
					}
					if details.ResourceLimits.Gpu != nil {
						metrics.ResourceGpuLimit += details.ResourceLimits.Gpu.Number
						if metrics.ResourceGpuLimitMax < details.ResourceLimits.Gpu.Number {
							metrics.ResourceGpuLimitMax = details.ResourceLimits.Gpu.Number
						}
					}
					if details.ResourceLimits.Disk != "" {
						metrics.ResourceDiskLimit += (details.ResourceLimits.Disk + " ")
					}
				}
				if details.Input != nil {
					inputMetrics(details.Input, metrics)
				}
				if details.Service != nil {
					metrics.CfgServices++
				}
				if details.Spout != nil {
					metrics.PpsSpout++
					if details.Spout.Service != nil {
						metrics.PpsSpoutService++
					}
				}
				if details.S3Out {
					metrics.CfgS3Gateway++
				}
				if details.Transform != nil {
					if details.Transform.ErrCmd != nil {
						metrics.CfgErrcmd++
					}
				}
				if details.TFJob != nil {
					metrics.CfgTfjob++
				}
			}
		}
		return nil
	}(); err != nil {
		log.Errorf("Error getting pipeline metrics: %v", err)
	}

	if err := func() error {
		rClient, err := r.env.GetPachClient(ctx).PfsAPIClient.ListRepo(ctx, &pfs.ListRepoRequest{})
		if err != nil {
			return err
		}
		repoInfos, err := clientsdk.ListRepoInfo(rClient)
		if err != nil {
			return err
		}
		var sz, mbranch uint64 = 0, 0
		for _, ri := range repoInfos {
			sz += uint64(ri.SizeBytesUpperBound)
			if mbranch < uint64(len(ri.Branches)) {
				mbranch = uint64(len(ri.Branches))
			}
		}
		metrics.Repos = int64(len(repoInfos))
		metrics.Bytes = sz
		metrics.MaxBranches = mbranch
		return nil
	}(); err != nil {
		log.Errorf("Error getting repos: %v", err)
	}
	//log.Infof("Metrics logged: %v", metrics)
}

package metrics

import (
	"fmt"
	"time"

	"github.com/pachyderm/pachyderm/src/client"
	"github.com/pachyderm/pachyderm/src/client/enterprise"
	"github.com/pachyderm/pachyderm/src/client/pkg/config"
	"github.com/pachyderm/pachyderm/src/client/pkg/errors"
	"github.com/pachyderm/pachyderm/src/client/pps"
	"github.com/pachyderm/pachyderm/src/client/version"
	enterprisemetrics "github.com/pachyderm/pachyderm/src/server/enterprise/metrics"
	"github.com/pachyderm/pachyderm/src/server/pkg/serviceenv"
	"github.com/pachyderm/pachyderm/src/server/pkg/uuid"

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
	env       *serviceenv.ServiceEnv
}

// NewReporter creates a new reporter and kicks off the loop to report cluster
// metrics
func NewReporter(clusterID string, env *serviceenv.ServiceEnv) *Reporter {
	var r *router
	if env.MetricsEndpoint != "" {
		r = newRouter(env.MetricsEndpoint)
	} else {
		r = newRouter()
	}
	reporter := &Reporter{
		router:    r,
		clusterID: clusterID,
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
		internalMetrics(r.env.GetPachClient(context.Background()), metrics)
		externalMetrics(r.env.GetKubeClient(), metrics)
		metrics.ClusterID = r.clusterID
		metrics.PodID = uuid.NewWithoutDashes()
		metrics.Version = version.PrettyPrintVersion(version.Version)
		r.router.reportClusterMetricsToSegment(metrics)
	}
}

func externalMetrics(kubeClient *kube.Clientset, metrics *Metrics) error {
	ctx := context.TODO()
	nodeList, err := kubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
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
	if input.Git != nil {
		metrics.InputGit++
	}
	if input.Pfs != nil {
		pfsInputMetrics(input.Pfs, metrics)
	}
}

func internalMetrics(pachClient *client.APIClient, metrics *Metrics) {

	// We should not return due to an error

	// Activation code
	enterpriseState, err := pachClient.Enterprise.GetState(pachClient.Ctx(), &enterprise.GetStateRequest{})
	if err == nil {
		metrics.ActivationCode = enterpriseState.ActivationCode
	}
	metrics.EnterpriseFailures = enterprisemetrics.GetEnterpriseFailures()

	// Pipeline info
	resp, err := pachClient.PpsAPIClient.ListPipeline(pachClient.Ctx(), &pps.ListPipelineRequest{AllowIncomplete: true})
	if err == nil {
		metrics.Pipelines = int64(len(resp.PipelineInfo)) // Number of pipelines
		for _, pi := range resp.PipelineInfo {
			if pi.ParallelismSpec != nil {
				if metrics.MaxParallelism < pi.ParallelismSpec.Constant {
					metrics.MaxParallelism = pi.ParallelismSpec.Constant
				}
				if metrics.MinParallelism > pi.ParallelismSpec.Constant {
					metrics.MinParallelism = pi.ParallelismSpec.Constant
				}
				metrics.NumParallelism++
			}
			if pi.Egress != nil {
				metrics.CfgEgress++
			}
			if pi.JobCounts != nil {
				var cnt int64 = 0
				for _, c := range pi.JobCounts {
					cnt += int64(c)
				}
				if metrics.Jobs < cnt {
					metrics.Jobs = cnt
				}
			}
			if pi.ResourceRequests != nil {
				if pi.ResourceRequests.Cpu != 0 {
					metrics.ResourceCpuReq += pi.ResourceRequests.Cpu
					if metrics.ResourceCpuReqMax < pi.ResourceRequests.Cpu {
						metrics.ResourceCpuReqMax = pi.ResourceRequests.Cpu
					}
				}
				if pi.ResourceRequests.Memory != "" {
					metrics.ResourceMemReq += (pi.ResourceRequests.Memory + " ")
				}
				if pi.ResourceRequests.Gpu != nil {
					metrics.ResourceGpuReq += pi.ResourceRequests.Gpu.Number
					if metrics.ResourceGpuReqMax < pi.ResourceRequests.Gpu.Number {
						metrics.ResourceGpuReqMax = pi.ResourceRequests.Gpu.Number
					}
				}
				if pi.ResourceRequests.Disk != "" {
					metrics.ResourceDiskReq += (pi.ResourceRequests.Disk + " ")
				}
			}
			if pi.ResourceLimits != nil {
				if pi.ResourceLimits.Cpu != 0 {
					metrics.ResourceCpuLimit += pi.ResourceLimits.Cpu
					if metrics.ResourceCpuLimitMax < pi.ResourceLimits.Cpu {
						metrics.ResourceCpuLimitMax = pi.ResourceLimits.Cpu
					}
				}
				if pi.ResourceLimits.Memory != "" {
					metrics.ResourceMemLimit += (pi.ResourceLimits.Memory + " ")
				}
				if pi.ResourceLimits.Gpu != nil {
					metrics.ResourceGpuLimit += pi.ResourceLimits.Gpu.Number
					if metrics.ResourceGpuLimitMax < pi.ResourceLimits.Gpu.Number {
						metrics.ResourceGpuLimitMax = pi.ResourceLimits.Gpu.Number
					}
				}
				if pi.ResourceLimits.Disk != "" {
					metrics.ResourceDiskLimit += (pi.ResourceLimits.Disk + " ")
				}
			}
			if pi.Input != nil {
				inputMetrics(pi.Input, metrics)
			}
			if pi.EnableStats {
				metrics.CfgStats++
			}
			if pi.Service != nil {
				metrics.CfgServices++
			}
			if pi.Spout != nil {
				metrics.PpsSpout++
				if pi.Spout.Service != nil {
					metrics.PpsSpoutService++
				}
			}
			if pi.Standby {
				metrics.CfgStandby++
			}
			if pi.S3Out {
				metrics.CfgS3Gateway++
			}
			if pi.Transform != nil {
				if pi.Transform.ErrCmd != nil {
					metrics.CfgErrcmd++
				}
				if pi.Transform.Build != nil {
					metrics.PpsBuild++
				}
			}
			if pi.TFJob != nil {
				metrics.CfgTfjob++
			}
		}
	} else {
		log.Errorf("Error getting pipeline metrics: %v", err)
	}

	ris, err := pachClient.ListRepo()
	if err == nil {
		var sz, mbranch uint64 = 0, 0
		for _, ri := range ris {
			if (sz + ri.SizeBytes) < sz {
				sz = 0xFFFFFFFFFFFFFFFF
			} else {
				sz += ri.SizeBytes
			}
			if mbranch < uint64(len(ri.Branches)) {
				mbranch = uint64(len(ri.Branches))
			}
		}
		metrics.Repos = int64(len(ris))
		metrics.Bytes = sz
		metrics.MaxBranches = mbranch
	} else {
		log.Errorf("Error getting repo metrics: %v", err)
	}
	//log.Infof("Metrics logged: %v", metrics)
}

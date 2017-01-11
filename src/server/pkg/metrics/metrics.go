package metrics

import (
	"fmt"
	"time"

	"github.com/pachyderm/pachyderm/src/client/pkg/config"
	"github.com/pachyderm/pachyderm/src/client/pkg/uuid"
	"github.com/pachyderm/pachyderm/src/client/version"
	db "github.com/pachyderm/pachyderm/src/server/pfs/db"

	log "github.com/Sirupsen/logrus"
	"github.com/dancannon/gorethink"
	"github.com/segmentio/analytics-go"
	"golang.org/x/net/context"
	"google.golang.org/grpc/metadata"
	kube "k8s.io/kubernetes/pkg/client/unversioned"
)

//Reporter is used to submit user & cluster metrics to segment
type Reporter struct {
	segmentClient *analytics.Client
	clusterID     string
	kubeClient    *kube.Client
	dbClient      *gorethink.Session
	pfsDbName     string
	ppsDbName     string
}

// NewReporter creates a new reporter and kicks off the loop to report cluster
// metrics
func NewReporter(clusterID string, kubeClient *kube.Client, address string, pfsDbName string, ppsDbName string) *Reporter {

	dbClient, err := db.DbConnect(address)
	if err != nil {
		log.Errorf("error connected to DB when reporting metrics: %v\n", err)
		return nil
	}
	reporter := &Reporter{
		segmentClient: newPersistentClient(),
		clusterID:     clusterID,
		kubeClient:    kubeClient,
		dbClient:      dbClient,
		pfsDbName:     pfsDbName,
		ppsDbName:     ppsDbName,
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
	return "", fmt.Errorf("error extracting userid from metadata. userid is empty")
}

func (r *Reporter) reportUserAction(ctx context.Context, action string, value interface{}) {
	md, ok := metadata.FromContext(ctx)
	if ok {
		// metadata API downcases all the key names
		userID, err := getKeyFromMD(md, "userid")
		if err != nil {
			// The FUSE client will never have a userID, so normal usage will produce a lot of these errors
			return
		}
		prefix, err := getKeyFromMD(md, "prefix")
		if err != nil {
			log.Errorln(err)
			return
		}
		reportUserMetricsToSegment(
			r.segmentClient,
			userID,
			prefix,
			action,
			value,
			r.clusterID,
		)
	} else {
		log.Errorf("Error extracting userid metadata from context: %v\n", ctx)
	}
}

// ReportAndFlushUserAction immediately reports the metric
// It is used in the few places we need to report metrics from the client.
// It handles reporting the start, finish, and error conditions of the action
func ReportAndFlushUserAction(action string) func(time.Time, error) {
	// If we report nil, segment sees it, but mixpanel omits the field
	reportAndFlushUserAction(fmt.Sprintf("%vStarted", action), 1)
	return func(start time.Time, err error) {
		if err == nil {
			reportAndFlushUserAction(fmt.Sprintf("%vFinished", action), time.Since(start).Seconds())
		} else {
			reportAndFlushUserAction(fmt.Sprintf("%vErrored", action), err.Error())
		}
	}
}

func reportAndFlushUserAction(action string, value interface{}) {
	client := newSegmentClient()
	defer client.Close()
	cfg, err := config.Read()
	if err != nil {
		log.Errorf("Error reading userid from ~/.pachyderm/config: %v\n", err)
		// metrics errors are non fatal
		return
	}
	reportUserMetricsToSegment(client, cfg.UserID, "user", action, value, "")
}

func (r *Reporter) dbMetrics(metrics *Metrics) {
	cursor, err := gorethink.Object(
		"Repos",
		gorethink.DB(r.pfsDbName).Table("Repos").Count(),
		"Commits",
		gorethink.DB(r.pfsDbName).Table("Commits").Count(),
		"Diffs",
		gorethink.DB(r.pfsDbName).Table("Diffs").Count(),
		"Jobs",
		gorethink.DB(r.ppsDbName).Table("JobInfos").Count(),
		"Pipelines",
		gorethink.DB(r.ppsDbName).Table("PipelineInfos").Count(),
	).Run(r.dbClient)
	if err != nil {
		log.Errorf("Error Fetching Metrics:%+v", err)
	}
	cursor.One(&metrics)
}

func (r *Reporter) reportClusterMetrics() {
	for {
		time.Sleep(reportingInterval)
		metrics := &Metrics{}
		r.dbMetrics(metrics)
		externalMetrics(r.kubeClient, metrics)
		metrics.ClusterID = r.clusterID
		metrics.PodID = uuid.NewWithoutDashes()
		metrics.Version = version.PrettyPrintVersion(version.Version)
		reportClusterMetricsToSegment(r.segmentClient, metrics)
	}
}

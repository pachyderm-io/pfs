package cmds

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/pachyderm/pachyderm/v2/src/auth"
	"github.com/pachyderm/pachyderm/v2/src/client"
	"github.com/pachyderm/pachyderm/v2/src/identity"
	"github.com/pachyderm/pachyderm/v2/src/internal/cmdutil"
	"github.com/pachyderm/pachyderm/v2/src/internal/config"
	"github.com/pachyderm/pachyderm/v2/src/internal/deploy"
	"github.com/pachyderm/pachyderm/v2/src/internal/deploy/assets"
	"github.com/pachyderm/pachyderm/v2/src/internal/deploy/images"
	"github.com/pachyderm/pachyderm/v2/src/internal/errors"
	"github.com/pachyderm/pachyderm/v2/src/internal/grpcutil"
	_metrics "github.com/pachyderm/pachyderm/v2/src/internal/metrics"
	"github.com/pachyderm/pachyderm/v2/src/internal/obj"
	"github.com/pachyderm/pachyderm/v2/src/internal/serde"
	"github.com/pachyderm/pachyderm/v2/src/version"
	clientcmd "k8s.io/client-go/tools/clientcmd/api/v1"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	awsAccessKeyIDRE = regexp.MustCompile("^[A-Z0-9]{20}$")
	awsSecretRE      = regexp.MustCompile("^[A-Za-z0-9/+=]{40}$")
	awsRegionRE      = regexp.MustCompile("^[a-z]{2}(?:-gov)?-[a-z]+-[0-9]$")
)

const (
	defaultDashImage   = "pachyderm/dash"
	defaultDashVersion = "0.5.57"
	etcdNodePort       = 32379
)

func kubectl(stdin io.Reader, context *config.Context, args ...string) error {
	var environ []string = nil
	if context != nil {
		tmpfile, err := ioutil.TempFile("", "transient-kube-config-*.yaml")
		if err != nil {
			return errors.Wrapf(err, "failed to create transient kube config")
		}
		defer os.Remove(tmpfile.Name())

		config := clientcmd.Config{
			Kind:           "Config",
			APIVersion:     "v1",
			CurrentContext: "pachyderm-active-context",
			Contexts: []clientcmd.NamedContext{
				clientcmd.NamedContext{
					Name: "pachyderm-active-context",
					Context: clientcmd.Context{
						Cluster:   context.ClusterName,
						AuthInfo:  context.AuthInfo,
						Namespace: context.Namespace,
					},
				},
			},
		}

		var buf bytes.Buffer
		if err := encoder("yaml", &buf).Encode(config); err != nil {
			return errors.Wrapf(err, "failed to encode config")
		}

		tmpfile.Write(buf.Bytes())
		tmpfile.Close()

		kubeconfig := os.Getenv("KUBECONFIG")
		if kubeconfig == "" {
			home, err := os.UserHomeDir()
			if err != nil {
				return errors.Wrapf(err, "failed to discover default kube config: could not get user home directory")
			}
			kubeconfig = path.Join(home, ".kube", "config")
			if _, err = os.Stat(kubeconfig); errors.Is(err, os.ErrNotExist) {
				return errors.Wrapf(err, "failed to discover default kube config: %q does not exist", kubeconfig)
			}
		}
		kubeconfig = fmt.Sprintf("%s%c%s", kubeconfig, os.PathListSeparator, tmpfile.Name())

		// note that this will override `KUBECONFIG` (if it is already defined) in
		// the environment; see examples under
		// https://golang.org/pkg/os/exec/#Command
		environ = os.Environ()
		environ = append(environ, fmt.Sprintf("KUBECONFIG=%s", kubeconfig))

		if stdin == nil {
			stdin = os.Stdin
		}
	}

	ioObj := cmdutil.IO{
		Stdin:   stdin,
		Stdout:  os.Stdout,
		Stderr:  os.Stderr,
		Environ: environ,
	}

	args = append([]string{"kubectl"}, args...)
	return cmdutil.RunIO(ioObj, args...)
}

// Generates a random secure token, in hex
func generateSecureToken(length int) string {
	b := make([]byte, length)
	if _, err := rand.Read(b); err != nil {
		return ""
	}
	return hex.EncodeToString(b)
}

// Return the appropriate encoder for the given output format.
func encoder(output string, w io.Writer) serde.Encoder {
	if output == "" {
		output = "json"
	} else {
		output = strings.ToLower(output)
	}
	e, err := serde.GetEncoder(output, w,
		serde.WithIndent(2),
		serde.WithOrigName(true),
	)
	if err != nil {
		cmdutil.ErrorAndExit(err.Error())
	}
	return e
}

func kubectlCreate(dryRun bool, manifest []byte, opts *assets.AssetOpts) error {
	if dryRun {
		_, err := os.Stdout.Write(manifest)
		return err
	}
	// we set --validate=false due to https://github.com/kubernetes/kubernetes/issues/53309
	if err := kubectl(bytes.NewReader(manifest), nil, "apply", "-f", "-", "--validate=false", "--namespace", opts.Namespace); err != nil {
		return err
	}

	fmt.Println("\nPachyderm is launching. Check its status with \"kubectl get all\"")
	if opts.DashOnly || !opts.NoDash {
		fmt.Println("Once launched, access the dashboard by running \"pachctl port-forward\"")
	}
	fmt.Println("")

	return nil
}

// findEquivalentContext searches for a context in the existing config that
// references the same cluster as the context passed in. If no such context
// was found, default values are returned instead.
func findEquivalentContext(cfg *config.Config, to *config.Context) (string, *config.Context) {
	// first check the active context
	activeContextName, activeContext, _ := cfg.ActiveContext(false)
	if activeContextName != "" && to.EqualClusterReference(activeContext) {
		return activeContextName, activeContext
	}

	// failing that, search all contexts (sorted by name to be deterministic)
	contextNames := []string{}
	for contextName := range cfg.V2.Contexts {
		contextNames = append(contextNames, contextName)
	}
	sort.Strings(contextNames)
	for _, contextName := range contextNames {
		existingContext := cfg.V2.Contexts[contextName]

		if to.EqualClusterReference(existingContext) {
			return contextName, existingContext
		}
	}

	return "", nil
}

func contextCreate(namePrefix, namespace, serverCert string, enterpriseServer bool) error {
	kubeConfig, err := config.RawKubeConfig()
	if err != nil {
		return err
	}
	kubeContext := kubeConfig.Contexts[kubeConfig.CurrentContext]

	clusterName := ""
	authInfo := ""
	if kubeContext != nil {
		clusterName = kubeContext.Cluster
		authInfo = kubeContext.AuthInfo
	}

	cfg, err := config.Read(false, false)
	if err != nil {
		return err
	}

	newContext := &config.Context{
		Source:           config.ContextSource_IMPORTED,
		ClusterName:      clusterName,
		AuthInfo:         authInfo,
		Namespace:        namespace,
		ServerCAs:        serverCert,
		EnterpriseServer: enterpriseServer,
	}

	equivalentContextName, equivalentContext := findEquivalentContext(cfg, newContext)
	if equivalentContext != nil {
		cfg.V2.ActiveContext = equivalentContextName
		equivalentContext.Source = newContext.Source
		equivalentContext.ClusterDeploymentID = ""
		equivalentContext.ServerCAs = newContext.ServerCAs
		return cfg.Write()
	}

	// we couldn't find an existing context that is the same as the new one,
	// so we'll have to create it
	newContextName := namePrefix
	if _, ok := cfg.V2.Contexts[newContextName]; ok {
		newContextName = fmt.Sprintf("%s-%s", namePrefix, time.Now().Format("2006-01-02-15-04-05"))
	}

	cfg.V2.Contexts[newContextName] = newContext
	if enterpriseServer {
		cfg.V2.ActiveEnterpriseContext = newContextName
	} else {
		cfg.V2.ActiveContext = newContextName
	}
	return cfg.Write()
}

// containsEmpty is a helper function used for validation (particularly for
// validating that creds arguments aren't empty
func containsEmpty(vals []string) bool {
	for _, val := range vals {
		if val == "" {
			return true
		}
	}
	return false
}

func standardDeployCmds() []*cobra.Command {
	var commands []*cobra.Command
	var opts *assets.AssetOpts

	var dryRun bool
	var outputFormat string
	var namespace string
	var serverCert string
	var dashImage string
	var dashOnly bool
	var etcdCPURequest string
	var etcdMemRequest string
	var etcdNodes int
	var etcdStorageClassName string
	var etcdVolume string
	var postgresCPURequest string
	var postgresMemRequest string
	var postgresStorageClassName string
	var postgresVolume string
	var imagePullSecret string
	var localRoles bool
	var logLevel string
	var noDash bool
	var noExposeDockerSocket bool
	var noGuaranteed bool
	var noRBAC bool
	var pachdCPURequest string
	var pachdNonCacheMemRequest string
	var registry string
	var tlsCertKey string
	var uploadConcurrencyLimit int
	var putFileConcurrencyLimit int
	var clusterDeploymentID string
	var requireCriticalServersOnly bool
	var workerServiceAccountName string
	var enterpriseServer bool
	appendGlobalFlags := func(cmd *cobra.Command) {
		cmd.Flags().IntVar(&etcdNodes, "dynamic-etcd-nodes", 0, "Deploy etcd as a StatefulSet with the given number of pods.  The persistent volumes used by these pods are provisioned dynamically.  Note that StatefulSet is currently a beta kubernetes feature, which might be unavailable in older versions of kubernetes.")
		cmd.Flags().StringVar(&etcdVolume, "static-etcd-volume", "", "Deploy etcd as a ReplicationController with one pod.  The pod uses the given persistent volume.")
		cmd.Flags().StringVar(&etcdStorageClassName, "etcd-storage-class", "", "If set, the name of an existing StorageClass to use for etcd storage. Ignored if --static-etcd-volume is set.")
		cmd.Flags().StringVar(&postgresVolume, "static-postgres-volume", "", "Deploy postgres as a ReplicationController with one pod.  The pod uses the given persistent volume.")
		cmd.Flags().StringVar(&postgresStorageClassName, "postgres-storage-class", "", "If set, the name of an existing StorageClass to use for postgres storage. Ignored if --static-postgres-volume is set.")
		cmd.Flags().BoolVar(&dryRun, "dry-run", false, "Don't actually deploy pachyderm to Kubernetes, instead just print the manifest. Note that a pachyderm context will not be created, unless you also use `--create-context`.")
		cmd.Flags().StringVarP(&outputFormat, "output", "o", "json", "Output format. One of: json|yaml")
		cmd.Flags().StringVar(&logLevel, "log-level", "info", "The level of log messages to print options are, from least to most verbose: \"error\", \"info\", \"debug\".")
		cmd.Flags().BoolVar(&dashOnly, "dashboard-only", false, "Only deploy the Pachyderm UI (experimental), without the rest of pachyderm. This is for launching the UI adjacent to an existing Pachyderm cluster. After deployment, run \"pachctl port-forward\" to connect")
		cmd.Flags().BoolVar(&noDash, "no-dashboard", false, "Don't deploy the Pachyderm UI alongside Pachyderm (experimental).")
		cmd.Flags().StringVar(&registry, "registry", "", "The registry to pull images from.")
		cmd.Flags().StringVar(&imagePullSecret, "image-pull-secret", "", "A secret in Kubernetes that's needed to pull from your private registry.")
		cmd.Flags().StringVar(&dashImage, "dash-image", "", "Image URL for pachyderm dashboard")
		cmd.Flags().BoolVar(&noGuaranteed, "no-guaranteed", false, "Don't use guaranteed QoS for etcd and pachd deployments. Turning this on (turning guaranteed QoS off) can lead to more stable local clusters (such as on Minikube), it should normally be used for production clusters.")
		cmd.Flags().BoolVar(&noRBAC, "no-rbac", false, "Don't deploy RBAC roles for Pachyderm. (for k8s versions prior to 1.8)")
		cmd.Flags().BoolVar(&localRoles, "local-roles", false, "Use namespace-local roles instead of cluster roles. Ignored if --no-rbac is set.")
		cmd.Flags().StringVar(&namespace, "namespace", "", "Kubernetes namespace to deploy Pachyderm to.")
		cmd.Flags().BoolVar(&noExposeDockerSocket, "no-expose-docker-socket", false, "Don't expose the Docker socket to worker containers. This limits the privileges of workers which prevents them from automatically setting the container's working dir and user.")
		cmd.Flags().StringVar(&tlsCertKey, "tls", "", "string of the form \"<cert path>,<key path>\" of the signed TLS certificate and private key that Pachd should use for TLS authentication (enables TLS-encrypted communication with Pachd)")
		cmd.Flags().IntVar(&uploadConcurrencyLimit, "upload-concurrency-limit", assets.DefaultUploadConcurrencyLimit, "The maximum number of concurrent object storage uploads per Pachd instance.")
		cmd.Flags().IntVar(&putFileConcurrencyLimit, "put-file-concurrency-limit", assets.DefaultPutFileConcurrencyLimit, "The maximum number of files to upload or fetch from remote sources (HTTP, blob storage) using PutFile concurrently.")
		cmd.Flags().StringVar(&clusterDeploymentID, "cluster-deployment-id", "", "Set an ID for the cluster deployment. Defaults to a random value.")
		cmd.Flags().BoolVar(&requireCriticalServersOnly, "require-critical-servers-only", assets.DefaultRequireCriticalServersOnly, "Only require the critical Pachd servers to startup and run without errors.")
		cmd.Flags().BoolVar(&enterpriseServer, "enterprise-server", false, "Deploy the Enterprise Server.")
		cmd.Flags().StringVar(&workerServiceAccountName, "worker-service-account", assets.DefaultWorkerServiceAccountName, "The Kubernetes service account for workers to use when creating S3 gateways.")

		// Flags for setting pachd resource requests. These should rarely be set --
		// only if we get the defaults wrong, or users have an unusual access pattern
		//
		// All of these are empty by default, because the actual default values depend
		// on the backend to which we're. The defaults are set in
		// s/s/pkg/deploy/assets/assets.go
		cmd.Flags().StringVar(&pachdCPURequest,
			"pachd-cpu-request", "", "(rarely set) The size of Pachd's CPU "+
				"request, which we give to Kubernetes. Size is in cores (with partial "+
				"cores allowed and encouraged).")
		cmd.Flags().StringVar(&pachdNonCacheMemRequest,
			"pachd-memory-request", "", "(rarely set) The size of Pachd's memory request. "+
				"Size is in bytes, with SI suffixes (M, K, G, Mi, Ki, Gi, etc).")
		cmd.Flags().StringVar(&etcdCPURequest,
			"etcd-cpu-request", "", "(rarely set) The size of etcd's CPU request, "+
				"which we give to Kubernetes. Size is in cores (with partial cores "+
				"allowed and encouraged).")
		cmd.Flags().StringVar(&etcdMemRequest,
			"etcd-memory-request", "", "(rarely set) The size of etcd's memory "+
				"request. Size is in bytes, with SI suffixes (M, K, G, Mi, Ki, Gi, "+
				"etc).")
		cmd.Flags().StringVar(&postgresCPURequest,
			"postgres-cpu-request", "", "(rarely set) The size of postgres's CPU request, "+
				"which we give to Kubernetes. Size is in cores (with partial cores "+
				"allowed and encouraged).")
		cmd.Flags().StringVar(&postgresMemRequest,
			"postgres-memory-request", "", "(rarely set) The size of postgres's memory "+
				"request. Size is in bytes, with SI suffixes (M, K, G, Mi, Ki, Gi, "+
				"etc).")
	}

	var retries int
	var timeout string
	var uploadACL string
	var partSize int64
	var maxUploadParts int
	var disableSSL bool
	var noVerifySSL bool
	var logOptions string
	appendS3Flags := func(cmd *cobra.Command) {
		cmd.Flags().IntVar(&retries, "retries", obj.DefaultRetries, "(rarely set) Set a custom number of retries for object storage requests.")
		cmd.Flags().StringVar(&timeout, "timeout", obj.DefaultTimeout, "(rarely set) Set a custom timeout for object storage requests.")
		cmd.Flags().StringVar(&uploadACL, "upload-acl", obj.DefaultUploadACL, "(rarely set) Set a custom upload ACL for object storage uploads.")
		cmd.Flags().Int64Var(&partSize, "part-size", obj.DefaultPartSize, "(rarely set) Set a custom part size for object storage uploads.")
		cmd.Flags().IntVar(&maxUploadParts, "max-upload-parts", obj.DefaultMaxUploadParts, "(rarely set) Set a custom maximum number of upload parts.")
		cmd.Flags().BoolVar(&disableSSL, "disable-ssl", obj.DefaultDisableSSL, "(rarely set) Disable SSL.")
		cmd.Flags().BoolVar(&noVerifySSL, "no-verify-ssl", obj.DefaultNoVerifySSL, "(rarely set) Skip SSL certificate verification (typically used for enabling self-signed certificates).")
		cmd.Flags().StringVar(&logOptions, "obj-log-options", obj.DefaultAwsLogOptions, "(rarely set) Enable verbose logging in Pachyderm's internal S3 client for debugging. Comma-separated list containing zero or more of: 'Debug', 'Signing', 'HTTPBody', 'RequestRetries', 'RequestErrors', 'EventStreamBody', or 'all' (case-insensitive). See 'AWS SDK for Go' docs for details.")
	}

	var contextName string
	var createContext bool
	appendContextFlags := func(cmd *cobra.Command) {
		cmd.Flags().StringVarP(&contextName, "context", "c", "", "Name of the context to add to the pachyderm config. If unspecified, a context name will automatically be derived.")
		cmd.Flags().BoolVar(&createContext, "create-context", false, "Create a context, even with `--dry-run`.")
	}

	preRunInternal := func(args []string) error {
		cfg, err := config.Read(false, false)
		if err != nil {
			log.Warningf("could not read config to check whether cluster metrics "+
				"will be enabled: %v.\n", err)
		}

		if namespace == "" {
			kubeConfig := config.KubeConfig(nil)
			var err error
			namespace, _, err = kubeConfig.Namespace()
			if err != nil {
				log.Warningf("using namespace \"default\" (couldn't load namespace "+
					"from kubernetes config: %v)\n", err)
				namespace = "default"
			}
		}

		// When deploying the enterprise server don't ever install dash.
		if enterpriseServer {
			noDash = true
		}

		if dashImage == "" {
			dashImage = fmt.Sprintf("%s:%s", defaultDashImage, getCompatibleVersion("dash", "", defaultDashVersion))
		}

		opts = &assets.AssetOpts{
			FeatureFlags: assets.FeatureFlags{},
			EtcdOpts: assets.EtcdOpts{
				Nodes:            etcdNodes,
				Volume:           etcdVolume,
				CPURequest:       etcdCPURequest,
				MemRequest:       etcdMemRequest,
				StorageClassName: etcdStorageClassName,
			},
			PostgresOpts: assets.PostgresOpts{
				Volume:           postgresVolume,
				CPURequest:       postgresCPURequest,
				MemRequest:       postgresMemRequest,
				StorageClassName: postgresStorageClassName,
			},
			StorageOpts: assets.StorageOpts{
				UploadConcurrencyLimit:  uploadConcurrencyLimit,
				PutFileConcurrencyLimit: putFileConcurrencyLimit,
			},
			Version:                    version.PrettyPrintVersion(version.Version),
			LogLevel:                   logLevel,
			Metrics:                    cfg == nil || cfg.V2.Metrics,
			PachdCPURequest:            pachdCPURequest,
			PachdNonCacheMemRequest:    pachdNonCacheMemRequest,
			DashOnly:                   dashOnly,
			NoDash:                     noDash,
			DashImage:                  dashImage,
			Registry:                   registry,
			ImagePullSecret:            imagePullSecret,
			NoGuaranteed:               noGuaranteed,
			NoRBAC:                     noRBAC,
			LocalRoles:                 localRoles,
			Namespace:                  namespace,
			NoExposeDockerSocket:       noExposeDockerSocket,
			ClusterDeploymentID:        clusterDeploymentID,
			RequireCriticalServersOnly: requireCriticalServersOnly,
			WorkerServiceAccountName:   workerServiceAccountName,
			EnterpriseServer:           enterpriseServer,
		}
		if opts.PostgresOpts.Volume == "" {
			opts.PostgresOpts.Nodes = 1
		}
		if tlsCertKey != "" {
			// TODO(msteffen): If either the cert path or the key path contains a
			// comma, this doesn't work
			certKey := strings.Split(tlsCertKey, ",")
			if len(certKey) != 2 {
				return fmt.Errorf("could not split TLS certificate and key correctly; must have two parts but got: %#v", certKey)
			}
			opts.TLS = &assets.TLSOpts{
				ServerCert: certKey[0],
				ServerKey:  certKey[1],
			}

			serverCertBytes, err := ioutil.ReadFile(certKey[0])
			if err != nil {
				return errors.Wrapf(err, "could not read server cert at %q", certKey[0])
			}
			serverCert = base64.StdEncoding.EncodeToString([]byte(serverCertBytes))
		}
		return nil
	}
	preRun := cmdutil.Run(preRunInternal)

	deployPreRun := cmdutil.Run(func(args []string) error {
		if version.IsUnstable() {
			fmt.Fprintf(os.Stderr, "WARNING: The version of Pachyderm you are deploying (%s) is an unstable pre-release build and may not support data migration.\n\n", version.PrettyVersion())

			if ok, err := cmdutil.InteractiveConfirm(); err != nil {
				return err
			} else if !ok {
				return errors.New("deploy aborted")
			}
		}
		return preRunInternal(args)
	})

	var dev bool
	var hostPath string
	deployLocal := &cobra.Command{
		Short:  "Deploy a single-node Pachyderm cluster with local metadata storage.",
		Long:   "Deploy a single-node Pachyderm cluster with local metadata storage.",
		PreRun: deployPreRun,
		Run: cmdutil.RunFixedArgs(0, func(args []string) (retErr error) {
			if !dev {
				start := time.Now()
				startMetricsWait := _metrics.StartReportAndFlushUserAction("Deploy", start)
				defer startMetricsWait()
				defer func() {
					finishMetricsWait := _metrics.FinishReportAndFlushUserAction("Deploy", retErr, start)
					finishMetricsWait()
				}()
			}
			if dev {
				// Use dev build instead of release build
				opts.Version = deploy.DevVersionTag

				// we turn metrics off if this is a dev cluster. The default
				// is set by deploy.PersistentPreRun, below.
				opts.Metrics = false

				// Set the postgres and etcd nodeports explicitly for developers
				if !enterpriseServer {
					opts.PostgresOpts.Port = 32228
					opts.EtcdOpts.Port = etcdNodePort
				}
			}

			// Put the enterprise server backing data in a different path,
			// so a user can deploy a pachd and enterprise server in the same minikube
			// in different namespaces
			if enterpriseServer {
				hostPath = filepath.Join(hostPath, "enterprise")
			}

			var buf bytes.Buffer
			if err := assets.WriteLocalAssets(
				encoder(outputFormat, &buf), opts, hostPath,
			); err != nil {
				return err
			}
			if err := kubectlCreate(dryRun, buf.Bytes(), opts); err != nil {
				return err
			}
			if !dryRun || createContext {
				if contextName == "" {
					contextName = "local"
				}
				if err := contextCreate(contextName, namespace, serverCert, enterpriseServer); err != nil {
					return err
				}
			}
			return nil
		}),
	}
	appendGlobalFlags(deployLocal)
	appendContextFlags(deployLocal)
	deployLocal.Flags().StringVar(&hostPath, "host-path", "/var/pachyderm", "Location on the host machine where PFS metadata will be stored.")
	deployLocal.Flags().BoolVarP(&dev, "dev", "d", false, "Deploy pachd with local version tags, disable metrics, expose Pachyderm's object/block API, and use an insecure authentication mechanism (do not set on any cluster with sensitive data)")
	commands = append(commands, cmdutil.CreateAlias(deployLocal, "deploy local"))

	deployGoogle := &cobra.Command{
		Use:   "{{alias}} <disk-size> [<bucket-name>] [<credentials-file>]",
		Short: "Deploy a Pachyderm cluster running on Google Cloud Platform.",
		Long: `Deploy a Pachyderm cluster running on Google Cloud Platform.
  <disk-size>: Size of Google Compute Engine persistent disks in GB (assumed to all be the same).
  <bucket-name>: A Google Cloud Storage bucket where Pachyderm will store PFS data.
  <credentials-file>: A file containing the private key for the account (downloaded from Google Compute Engine).`,
		PreRun: deployPreRun,
		Run: cmdutil.RunBoundedArgs(1, 3, func(args []string) (retErr error) {
			start := time.Now()
			startMetricsWait := _metrics.StartReportAndFlushUserAction("Deploy", start)
			defer startMetricsWait()
			defer func() {
				finishMetricsWait := _metrics.FinishReportAndFlushUserAction("Deploy", retErr, start)
				finishMetricsWait()
			}()
			volumeSize, err := strconv.Atoi(args[0])
			if err != nil {
				return errors.Errorf("volume size needs to be an integer; instead got %v", args[1])
			}
			var buf bytes.Buffer
			var bucket string
			var cred string

			// The enterprise server doesn't need a GCS bucket, but pachd deployments do
			if len(args) > 1 {
				bucket = strings.TrimPrefix(args[1], "gs://")
			} else if !enterpriseServer {
				return errors.New("bucket-name is required for pachd deployments")
			}

			if len(args) == 3 {
				credBytes, err := ioutil.ReadFile(args[2])
				if err != nil {
					return errors.Wrapf(err, "error reading creds file %s", args[2])
				}
				cred = string(credBytes)
			}
			if err = assets.WriteGoogleAssets(
				encoder(outputFormat, &buf), opts, bucket, cred, volumeSize,
			); err != nil {
				return err
			}
			if err := kubectlCreate(dryRun, buf.Bytes(), opts); err != nil {
				return err
			}
			if !dryRun || createContext {
				if contextName == "" {
					contextName = "gcs"
				}
				if err := contextCreate(contextName, namespace, serverCert, enterpriseServer); err != nil {
					return err
				}
			}
			return nil
		}),
	}
	appendGlobalFlags(deployGoogle)
	appendContextFlags(deployGoogle)
	commands = append(commands, cmdutil.CreateAlias(deployGoogle, "deploy google"))
	commands = append(commands, cmdutil.CreateAlias(deployGoogle, "deploy gcp"))

	var objectStoreBackend string
	var persistentDiskBackend string
	var secure bool
	var isS3V2 bool
	deployCustom := &cobra.Command{
		Use:   "{{alias}} --persistent-disk <persistent disk backend> --object-store <object store backend> <persistent disk args> <object store args>",
		Short: "Deploy a custom Pachyderm cluster configuration",
		Long: `Deploy a custom Pachyderm cluster configuration.
If <object store backend> is \"s3\", then the arguments are:
    <volumes> <size of volumes (in GB)> <bucket> <id> <secret> <endpoint>`,
		PreRun: deployPreRun,
		Run: cmdutil.RunBoundedArgs(4, 7, func(args []string) (retErr error) {
			start := time.Now()
			startMetricsWait := _metrics.StartReportAndFlushUserAction("Deploy", start)
			defer startMetricsWait()
			defer func() {
				finishMetricsWait := _metrics.FinishReportAndFlushUserAction("Deploy", retErr, start)
				finishMetricsWait()
			}()
			// Setup advanced configuration.
			advancedConfig := &obj.AmazonAdvancedConfiguration{
				Retries:        retries,
				Timeout:        timeout,
				UploadACL:      uploadACL,
				PartSize:       partSize,
				MaxUploadParts: maxUploadParts,
				DisableSSL:     disableSSL,
				NoVerifySSL:    noVerifySSL,
				LogOptions:     logOptions,
			}
			if isS3V2 {
				fmt.Printf("DEPRECATED: Support for the S3V2 option is being deprecated. It will be removed in a future version\n\n")
			}
			// Generate manifest and write assets.
			var buf bytes.Buffer
			if err := assets.WriteCustomAssets(
				encoder(outputFormat, &buf), opts, args, objectStoreBackend,
				persistentDiskBackend, secure, isS3V2, advancedConfig,
			); err != nil {
				return err
			}
			if err := kubectlCreate(dryRun, buf.Bytes(), opts); err != nil {
				return err
			}
			if !dryRun || createContext {
				if contextName == "" {
					contextName = "custom"
				}
				if err := contextCreate(contextName, namespace, serverCert, enterpriseServer); err != nil {
					return err
				}
			}
			return nil
		}),
	}
	appendGlobalFlags(deployCustom)
	appendS3Flags(deployCustom)
	appendContextFlags(deployCustom)
	// (bryce) secure should be merged with disableSSL, but it would be a breaking change.
	deployCustom.Flags().BoolVarP(&secure, "secure", "s", false, "Enable secure access to a Minio server.")
	deployCustom.Flags().StringVar(&persistentDiskBackend, "persistent-disk", "aws",
		"(required) Backend providing persistent local volumes to stateful pods. "+
			"One of: aws, google, or azure.")
	deployCustom.Flags().StringVar(&objectStoreBackend, "object-store", "s3",
		"(required) Backend providing an object-storage API to pachyderm. One of: "+
			"s3, gcs, or azure-blob.")
	deployCustom.Flags().BoolVar(&isS3V2, "isS3V2", false, "Enable S3V2 client (DEPRECATED)")
	commands = append(commands, cmdutil.CreateAlias(deployCustom, "deploy custom"))

	var cloudfrontDistribution string
	var creds string
	var iamRole string
	var vault string
	deployAmazon := &cobra.Command{
		Use:   "{{alias}} <region> <disk-size> [<bucket-name>]",
		Short: "Deploy a Pachyderm cluster running on AWS.",
		Long: `Deploy a Pachyderm cluster running on AWS.
  <region>: The AWS region where Pachyderm is being deployed (e.g. us-west-1)
  <disk-size>: Size of EBS volumes, in GB (assumed to all be the same).
  <bucket-name>: An S3 bucket where Pachyderm will store PFS data.`,
		PreRun: deployPreRun,
		Run: cmdutil.RunBoundedArgs(2, 3, func(args []string) (retErr error) {
			start := time.Now()
			startMetricsWait := _metrics.StartReportAndFlushUserAction("Deploy", start)
			defer startMetricsWait()
			defer func() {
				finishMetricsWait := _metrics.FinishReportAndFlushUserAction("Deploy", retErr, start)
				finishMetricsWait()
			}()

			// Require credentials to access S3 for pachd deployments.
			// Enterprise server deployments don't require an S3 bucket, so they don't need credentials.
			if creds == "" && vault == "" && iamRole == "" && !enterpriseServer {
				return errors.Errorf("one of --credentials, --vault, or --iam-role needs to be provided")
			}

			// populate 'amazonCreds' & validate
			var amazonCreds *assets.AmazonCreds
			if creds != "" {
				parts := strings.Split(creds, ",")
				if len(parts) < 2 || len(parts) > 3 || containsEmpty(parts[:2]) {
					return errors.Errorf("incorrect format of --credentials")
				}
				amazonCreds = &assets.AmazonCreds{ID: parts[0], Secret: parts[1]}
				if len(parts) > 2 {
					amazonCreds.Token = parts[2]
				}

				if !awsAccessKeyIDRE.MatchString(amazonCreds.ID) {
					fmt.Fprintf(os.Stderr, "The AWS Access Key seems invalid (does not match %q)\n", awsAccessKeyIDRE)
					if ok, err := cmdutil.InteractiveConfirm(); err != nil {
						return err
					} else if !ok {
						return errors.Errorf("aborted")
					}
				}

				if !awsSecretRE.MatchString(amazonCreds.Secret) {
					fmt.Fprintf(os.Stderr, "The AWS Secret seems invalid (does not match %q)\n", awsSecretRE)
					if ok, err := cmdutil.InteractiveConfirm(); err != nil {
						return err
					} else if !ok {
						return errors.Errorf("aborted")
					}
				}
			}
			if vault != "" {
				if amazonCreds != nil {
					return errors.Errorf("only one of --credentials, --vault, or --iam-role needs to be provided")
				}
				parts := strings.Split(vault, ",")
				if len(parts) != 3 || containsEmpty(parts) {
					return errors.Errorf("incorrect format of --vault")
				}
				amazonCreds = &assets.AmazonCreds{VaultAddress: parts[0], VaultRole: parts[1], VaultToken: parts[2]}
			}
			if iamRole != "" {
				if amazonCreds != nil {
					return errors.Errorf("only one of --credentials, --vault, or --iam-role needs to be provided")
				}
				opts.IAMRole = iamRole
			}
			volumeSize, err := strconv.Atoi(args[1])
			if err != nil {
				return errors.Errorf("volume size needs to be an integer; instead got %v", args[1])
			}
			if strings.TrimSpace(cloudfrontDistribution) != "" {
				log.Warningf("you specified a cloudfront distribution; deploying on " +
					"AWS with cloudfront is currently an experimental feature. No security " +
					"restrictions have been applied to cloudfront, making all data " +
					"public (obscured but not secured)\n")
			}

			var bucket string
			if len(args) == 3 {
				bucket = strings.TrimPrefix(args[2], "s3://")
			} else if !enterpriseServer {
				return errors.New("expected 3 arguments (bucket-name is required for pachd deployments)")
			}

			region := args[0]
			if !awsRegionRE.MatchString(region) {
				fmt.Fprintf(os.Stderr, "The AWS region seems invalid (does not match %q)\n", awsRegionRE)
				if ok, err := cmdutil.InteractiveConfirm(); err != nil {
					return err
				} else if !ok {
					return errors.Errorf("aborted")
				}
			}
			// Setup advanced configuration.
			advancedConfig := &obj.AmazonAdvancedConfiguration{
				Retries:        retries,
				Timeout:        timeout,
				UploadACL:      uploadACL,
				PartSize:       partSize,
				MaxUploadParts: maxUploadParts,
				DisableSSL:     disableSSL,
				NoVerifySSL:    noVerifySSL,
				LogOptions:     logOptions,
			}
			// Generate manifest and write assets.
			var buf bytes.Buffer
			if err = assets.WriteAmazonAssets(
				encoder(outputFormat, &buf), opts, region, bucket, volumeSize,
				amazonCreds, cloudfrontDistribution, advancedConfig,
			); err != nil {
				return err
			}
			if err := kubectlCreate(dryRun, buf.Bytes(), opts); err != nil {
				return err
			}
			if !dryRun || createContext {
				if contextName == "" {
					contextName = "aws"
				}
				if err := contextCreate(contextName, namespace, serverCert, enterpriseServer); err != nil {
					return err
				}
			}
			return nil
		}),
	}
	appendGlobalFlags(deployAmazon)
	appendS3Flags(deployAmazon)
	appendContextFlags(deployAmazon)
	deployAmazon.Flags().StringVar(&cloudfrontDistribution, "cloudfront-distribution", "",
		"Deploying on AWS with cloudfront is currently "+
			"an alpha feature. No security restrictions have been"+
			"applied to cloudfront, making all data public (obscured but not secured)")
	deployAmazon.Flags().StringVar(&creds, "credentials", "", "Use the format \"<id>,<secret>[,<token>]\". You can get a token by running \"aws sts get-session-token\".")
	deployAmazon.Flags().StringVar(&vault, "vault", "", "Use the format \"<address/hostport>,<role>,<token>\".")
	deployAmazon.Flags().StringVar(&iamRole, "iam-role", "", fmt.Sprintf("Use the given IAM role for authorization, as opposed to using static credentials. The given role will be applied as the annotation %s, this used with a Kubernetes IAM role management system such as kube2iam allows you to give pachd credentials in a more secure way.", assets.IAMAnnotation))
	commands = append(commands, cmdutil.CreateAlias(deployAmazon, "deploy amazon"))
	commands = append(commands, cmdutil.CreateAlias(deployAmazon, "deploy aws"))

	deployMicrosoft := &cobra.Command{
		Use:   "{{alias}} <disk-size> [<container> <account-name> <account-key>]",
		Short: "Deploy a Pachyderm cluster running on Microsoft Azure.",
		Long: `Deploy a Pachyderm cluster running on Microsoft Azure.
  <disk-size>: Size of persistent volumes, in GB (assumed to all be the same).
  <container>: An Azure container where Pachyderm will store PFS data.`,
		PreRun: deployPreRun,
		Run: cmdutil.RunBoundedArgs(1, 4, func(args []string) (retErr error) {
			start := time.Now()
			startMetricsWait := _metrics.StartReportAndFlushUserAction("Deploy", start)
			defer startMetricsWait()
			defer func() {
				finishMetricsWait := _metrics.FinishReportAndFlushUserAction("Deploy", retErr, start)
				finishMetricsWait()
			}()

			var container, accountName, accountKey string
			// The enterprise server doesn't need an object store, so these arguments aren't required
			if !enterpriseServer {
				if len(args) != 4 {
					return errors.New("expected 4 arguments (container, account-name and account-key are required for pachd deployments)")
				}

				container = strings.TrimPrefix(args[1], "wasb://")
				accountName, accountKey = args[2], args[3]
			}

			if _, err := base64.StdEncoding.DecodeString(args[3]); err != nil {
				return errors.Errorf("storage-account-key needs to be base64 encoded; instead got '%v'", args[2])
			}
			if opts.EtcdOpts.Volume != "" {
				tempURI, err := url.ParseRequestURI(opts.EtcdOpts.Volume)
				if err != nil {
					return errors.Errorf("volume URI needs to be a well-formed URI; instead got '%v'", opts.EtcdOpts.Volume)
				}
				opts.EtcdOpts.Volume = tempURI.String()
			}
			volumeSize, err := strconv.Atoi(args[0])
			if err != nil {
				return errors.Errorf("volume size needs to be an integer; instead got %v", args[3])
			}
			var buf bytes.Buffer
			if err = assets.WriteMicrosoftAssets(
				encoder(outputFormat, &buf), opts, container, accountName, accountKey, volumeSize,
			); err != nil {
				return err
			}

			if err := kubectlCreate(dryRun, buf.Bytes(), opts); err != nil {
				return err
			}
			if !dryRun || createContext {
				if contextName == "" {
					contextName = "azure"
				}
				if err := contextCreate(contextName, namespace, serverCert, enterpriseServer); err != nil {
					return err
				}
			}
			return nil
		}),
	}
	appendGlobalFlags(deployMicrosoft)
	appendContextFlags(deployMicrosoft)
	commands = append(commands, cmdutil.CreateAlias(deployMicrosoft, "deploy microsoft"))
	commands = append(commands, cmdutil.CreateAlias(deployMicrosoft, "deploy azure"))

	deployStorageSecrets := func(data map[string][]byte) error {
		cfg, err := config.Read(false, false)
		if err != nil {
			return err
		}
		_, activeContext, err := cfg.ActiveContext(true)
		if err != nil {
			return err
		}

		// clean up any empty, but non-nil strings in the data, since those will prevent those fields from getting merged when we do the patch
		for k, v := range data {
			if v != nil && len(v) == 0 {
				delete(data, k)
			}
		}

		var buf bytes.Buffer
		if err = assets.WriteSecret(encoder(outputFormat, &buf), data, opts); err != nil {
			return err
		}
		if dryRun {
			_, err := os.Stdout.Write(buf.Bytes())
			return err
		}

		s := buf.String()
		return kubectl(&buf, activeContext, "patch", "secret", "pachyderm-storage-secret", "-p", s, "--namespace", opts.Namespace, "--type=merge")
	}

	deployStorageAmazon := &cobra.Command{
		Use:    "{{alias}} <region> <access-key-id> <secret-access-key> [<session-token>]",
		Short:  "Deploy credentials for the Amazon S3 storage provider.",
		Long:   "Deploy credentials for the Amazon S3 storage provider, so that Pachyderm can ingress data from and egress data to it.",
		PreRun: preRun,
		Run: cmdutil.RunBoundedArgs(3, 4, func(args []string) error {
			var token string
			if len(args) == 4 {
				token = args[3]
			}
			// Setup advanced configuration.
			advancedConfig := &obj.AmazonAdvancedConfiguration{
				Retries:        retries,
				Timeout:        timeout,
				UploadACL:      uploadACL,
				PartSize:       partSize,
				MaxUploadParts: maxUploadParts,
				DisableSSL:     disableSSL,
				NoVerifySSL:    noVerifySSL,
				LogOptions:     logOptions,
			}
			return deployStorageSecrets(assets.AmazonSecret(args[0], "", args[1], args[2], token, "", "", advancedConfig))
		}),
	}
	appendGlobalFlags(deployStorageAmazon)
	appendS3Flags(deployStorageAmazon)
	commands = append(commands, cmdutil.CreateAlias(deployStorageAmazon, "deploy storage amazon"))

	deployStorageGoogle := &cobra.Command{
		Use:    "{{alias}} <credentials-file>",
		Short:  "Deploy credentials for the Google Cloud storage provider.",
		Long:   "Deploy credentials for the Google Cloud storage provider, so that Pachyderm can ingress data from and egress data to it.",
		PreRun: preRun,
		Run: cmdutil.RunFixedArgs(1, func(args []string) error {
			credBytes, err := ioutil.ReadFile(args[0])
			if err != nil {
				return errors.Wrapf(err, "error reading credentials file %s", args[0])
			}
			return deployStorageSecrets(assets.GoogleSecret("", string(credBytes)))
		}),
	}
	appendGlobalFlags(deployStorageGoogle)
	commands = append(commands, cmdutil.CreateAlias(deployStorageGoogle, "deploy storage google"))

	deployStorageAzure := &cobra.Command{
		Use:    "{{alias}} <account-name> <account-key>",
		Short:  "Deploy credentials for the Azure storage provider.",
		Long:   "Deploy credentials for the Azure storage provider, so that Pachyderm can ingress data from and egress data to it.",
		PreRun: preRun,
		Run: cmdutil.RunFixedArgs(2, func(args []string) error {
			return deployStorageSecrets(assets.MicrosoftSecret("", args[0], args[1]))
		}),
	}
	appendGlobalFlags(deployStorageAzure)
	commands = append(commands, cmdutil.CreateAlias(deployStorageAzure, "deploy storage microsoft"))

	deployStorage := &cobra.Command{
		Short: "Deploy credentials for a particular storage provider.",
		Long:  "Deploy credentials for a particular storage provider, so that Pachyderm can ingress data from and egress data to it.",
	}
	commands = append(commands, cmdutil.CreateAlias(deployStorage, "deploy storage"))

	listImages := &cobra.Command{
		Short:  "Output the list of images in a deployment.",
		Long:   "Output the list of images in a deployment.",
		PreRun: preRun,
		Run: cmdutil.RunFixedArgs(0, func(args []string) error {
			for _, image := range assets.Images(opts) {
				fmt.Println(image)
			}
			return nil
		}),
	}
	appendGlobalFlags(listImages)
	commands = append(commands, cmdutil.CreateAlias(listImages, "deploy list-images"))

	exportImages := &cobra.Command{
		Use:    "{{alias}} <output-file>",
		Short:  "Export a tarball (to stdout) containing all of the images in a deployment.",
		Long:   "Export a tarball (to stdout) containing all of the images in a deployment.",
		PreRun: preRun,
		Run: cmdutil.RunFixedArgs(1, func(args []string) (retErr error) {
			file, err := os.Create(args[0])
			if err != nil {
				return err
			}
			defer func() {
				if err := file.Close(); err != nil && retErr == nil {
					retErr = err
				}
			}()
			return images.Export(opts, file)
		}),
	}
	appendGlobalFlags(exportImages)
	commands = append(commands, cmdutil.CreateAlias(exportImages, "deploy export-images"))

	importImages := &cobra.Command{
		Use:    "{{alias}} <input-file>",
		Short:  "Import a tarball (from stdin) containing all of the images in a deployment and push them to a private registry.",
		Long:   "Import a tarball (from stdin) containing all of the images in a deployment and push them to a private registry.",
		PreRun: preRun,
		Run: cmdutil.RunFixedArgs(1, func(args []string) (retErr error) {
			file, err := os.Open(args[0])
			if err != nil {
				return err
			}
			defer func() {
				if err := file.Close(); err != nil && retErr == nil {
					retErr = err
				}
			}()
			return images.Import(opts, file)
		}),
	}
	appendGlobalFlags(importImages)
	commands = append(commands, cmdutil.CreateAlias(importImages, "deploy import-images"))

	return commands
}

// Cmds returns a list of cobra commands for deploying Pachyderm clusters.
func Cmds() []*cobra.Command {
	commands := standardDeployCmds()
	deployIDE := createDeployIDECmd()
	commands = append(commands, cmdutil.CreateAlias(deployIDE, "deploy ide"))

	deploy := &cobra.Command{
		Short: "Deploy a Pachyderm cluster.",
		Long:  "Deploy a Pachyderm cluster.",
	}
	commands = append(commands, cmdutil.CreateAlias(deploy, "deploy"))

	undeployCmd := makeUndeployCmd()
	commands = append(commands, cmdutil.CreateAlias(undeployCmd, "undeploy"))

	return commands
}

// getCompatibleVersion gets the compatible version of another piece of
// software, or falls back to a default
func getCompatibleVersion(displayName, subpath, defaultValue string) string {
	var relVersion string
	// This is the branch where to look.
	// When a new version needs to be pushed we can just update the
	// compatibility file in pachyderm repo branch. A (re)deploy will pick it
	// up. To make this work we have to point the URL to the branch (not tag)
	// in the repo.
	branch := version.BranchFromVersion(version.Version)
	if version.IsCustomRelease(version.Version) {
		relVersion = version.PrettyPrintVersionNoAdditional(version.Version)
	} else {
		relVersion = version.PrettyPrintVersion(version.Version)
	}

	url := fmt.Sprintf("https://raw.githubusercontent.com/pachyderm/pachyderm/compatibility%s/etc/%s/%s", branch, subpath, relVersion)
	resp, err := http.Get(url)
	if err != nil {
		log.Warningf("error looking up compatible version of %s, falling back to %s: %v", displayName, defaultValue, err)
		return defaultValue
	}

	// Error on non-200; for the requests we're making, 200 is the only OK
	// state
	if resp.StatusCode != 200 {
		log.Warningf("error looking up compatible version of %s, falling back to %s: unexpected return code %d", displayName, defaultValue, resp.StatusCode)
		return defaultValue
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Warningf("error looking up compatible version of %s, falling back to %s: %v", displayName, defaultValue, err)
		return defaultValue
	}

	allVersions := strings.Split(strings.TrimSpace(string(body)), "\n")
	if len(allVersions) < 1 {
		log.Warningf("no compatible version of %s found, falling back to %s", displayName, defaultValue)
		return defaultValue
	}
	latestVersion := strings.TrimSpace(allVersions[len(allVersions)-1])
	return latestVersion
}

// addOIDCClient registers a new OIDC client for the app, and makes it a trusted peer of this app.
// If the client already exists, it will return the existing client secret.
func addOIDCClient(c *client.APIClient, clientID, redirectURI string) (string, string, error) {
	authConfig, err := c.GetConfiguration(c.Ctx(), &auth.GetConfigurationRequest{})
	if err != nil {
		return "", "", errors.Wrapf(grpcutil.ScrubGRPC(err), "could not get auth configuration")
	}
	pachdClient, err := c.GetOIDCClient(c.Ctx(), &identity.GetOIDCClientRequest{Id: authConfig.Configuration.ClientID})
	if err != nil {
		return "", "", errors.Wrapf(grpcutil.ScrubGRPC(err), "could not get the OIDC config for pachd")
	}

	if err := func() error {
		// If the client ID is already a trusted peer, don't add  it again
		for _, peer := range pachdClient.Client.TrustedPeers {
			if peer == clientID {
				return nil
			}
		}

		pachdClient.Client.TrustedPeers = append(pachdClient.Client.TrustedPeers, clientID)
		_, err = c.UpdateOIDCClient(c.Ctx(), &identity.UpdateOIDCClientRequest{Client: pachdClient.Client})
		return err
	}(); err != nil {
		return "", "", errors.Wrapf(grpcutil.ScrubGRPC(err), "could not update OIDC config for pachd")
	}

	existingOIDCClient, err := c.GetOIDCClient(c.Ctx(), &identity.GetOIDCClientRequest{Id: clientID})
	if err == nil {
		if len(existingOIDCClient.Client.RedirectUris) != 1 || existingOIDCClient.Client.RedirectUris[0] != redirectURI {
			return "", "", errors.New("another client is already registered with this ID")
		}
		return authConfig.Configuration.ClientID, existingOIDCClient.Client.Secret, nil
	}

	oidcClient, err := c.CreateOIDCClient(c.Ctx(), &identity.CreateOIDCClientRequest{
		Client: &identity.OIDCClient{
			Id:           clientID,
			Name:         clientID,
			RedirectUris: []string{redirectURI},
		},
	})

	if err != nil {
		return "", "", errors.Wrapf(grpcutil.ScrubGRPC(err), "could not create new OIDC client")
	}
	return authConfig.Configuration.ClientID, oidcClient.Client.Secret, nil
}

package cmds

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/pachyderm/pachyderm/src/client"
	deployclient "github.com/pachyderm/pachyderm/src/client/deploy"

	"github.com/pachyderm/pachyderm/src/client/version"
	"github.com/pachyderm/pachyderm/src/server/pkg/cmdutil"
	"github.com/pachyderm/pachyderm/src/server/pkg/deploy"
	"github.com/pachyderm/pachyderm/src/server/pkg/deploy/assets"
	"github.com/pachyderm/pachyderm/src/server/pkg/deploy/images"
	_metrics "github.com/pachyderm/pachyderm/src/server/pkg/metrics"

	"github.com/ghodss/yaml"
	"github.com/spf13/cobra"
)

var defaultDashImage = "pachyderm/dash:1.8-preview-7"

var awsAccessKeyIDRE = regexp.MustCompile("^[A-Z0-9]{20}$")
var awsSecretRE = regexp.MustCompile("^[A-Za-z0-9/+=]{40}$")
var awsRegionRE = regexp.MustCompile("^[a-z]{2}(?:-gov)?-[a-z]+-[0-9]$")

// BytesEncoder is an Encoder with bytes content.
type BytesEncoder interface {
	assets.Encoder
	// Return the current buffer of the encoder.
	Buffer() *bytes.Buffer
}

// JSON assets.Encoder.
type jsonEncoder struct {
	encoder *json.Encoder
	buffer  *bytes.Buffer
}

func newJSONEncoder() *jsonEncoder {
	buffer := &bytes.Buffer{}
	encoder := json.NewEncoder(buffer)
	encoder.SetIndent("", "\t")
	return &jsonEncoder{encoder, buffer}
}

func (e *jsonEncoder) Encode(item interface{}) error {
	if err := e.encoder.Encode(item); err != nil {
		return err
	}
	_, err := fmt.Fprintf(e.buffer, "\n")
	return err
}

// Return the current bytes content.
func (e *jsonEncoder) Buffer() *bytes.Buffer {
	return e.buffer
}

// YAML assets.Encoder.
type yamlEncoder struct {
	buffer *bytes.Buffer
}

func newYAMLEncoder() *yamlEncoder {
	buffer := &bytes.Buffer{}
	return &yamlEncoder{buffer}
}

func (e *yamlEncoder) Encode(item interface{}) error {
	bytes, err := yaml.Marshal(item)
	if err != nil {
		return err
	}
	_, err = e.buffer.Write(bytes)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(e.buffer, "---\n")
	return err
}

// Return the current bytes content.
func (e *yamlEncoder) Buffer() *bytes.Buffer {
	return e.buffer
}

// Return the appropriate encoder for the given output format.
func getEncoder(outputFormat string) BytesEncoder {
	switch outputFormat {
	case "yaml":
		return newYAMLEncoder()
	case "json":
		return newJSONEncoder()
	default:
		return newJSONEncoder()
	}
}

func kubectlCreate(dryRun bool, manifest BytesEncoder, opts *assets.AssetOpts, metrics bool) error {
	if dryRun {
		_, err := os.Stdout.Write(manifest.Buffer().Bytes())
		return err
	}
	io := cmdutil.IO{
		Stdin:  manifest.Buffer(),
		Stdout: os.Stdout,
		Stderr: os.Stderr,
	}
	// we set --validate=false due to https://github.com/kubernetes/kubernetes/issues/53309
	if err := cmdutil.RunIO(io, "kubectl", "apply", "-f", "-", "--validate=false"); err != nil {
		return err
	}

	fmt.Println("\nPachyderm is launching. Check its status with \"kubectl get all\"")
	if opts.DashOnly || !opts.NoDash {
		fmt.Println("Once launched, access the dashboard by running \"pachctl port-forward\"")
	}
	fmt.Println("")

	return nil
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

// DeployCmd returns a cobra.Command to deploy pachyderm.
func DeployCmd(metrics bool, portForwarding bool) *cobra.Command {
	var pachdShards int
	var hostPath string
	var dev bool
	var dryRun bool
	var outputFormat string
	var secure bool
	var isS3V2 bool
	var etcdNodes int
	var etcdVolume string
	var etcdStorageClassName string
	var pachdCPURequest string
	var pachdNonCacheMemRequest string
	var blockCacheSize string
	var etcdCPURequest string
	var etcdMemRequest string
	var logLevel string
	var persistentDiskBackend string
	var objectStoreBackend string
	var opts *assets.AssetOpts
	var dashOnly bool
	var noDash bool
	var dashImage string
	var registry string
	var imagePullSecret string
	var noGuaranteed bool
	var noRBAC bool
	var localRoles bool
	var namespace string
	var noExposeDockerSocket bool
	var exposeObjectAPI bool
	var tlsCertKey string

	deployLocal := &cobra.Command{
		Use:   "local",
		Short: "Deploy a single-node Pachyderm cluster with local metadata storage.",
		Long:  "Deploy a single-node Pachyderm cluster with local metadata storage.",
		Run: cmdutil.RunFixedArgs(0, func(args []string) (retErr error) {
			if metrics && !dev {
				start := time.Now()
				startMetricsWait := _metrics.StartReportAndFlushUserAction("Deploy", start)
				defer startMetricsWait()
				defer func() {
					finishMetricsWait := _metrics.FinishReportAndFlushUserAction("Deploy", retErr, start)
					finishMetricsWait()
				}()
			}
			manifest := getEncoder(outputFormat)
			if dev {
				// Use dev build instead of release build
				opts.Version = deploy.DevVersionTag

				// we turn metrics off this is a dev cluster. The default is set by
				// deploy.PersistentPreRun, below.
				opts.Metrics = false

				// Disable authentication, for tests
				opts.DisableAuthentication = true

				// Serve the Pachyderm object/block API locally, as this is needed by
				// our tests (and authentication is disabled anyway)
				opts.ExposeObjectAPI = true
			}
			if err := assets.WriteLocalAssets(manifest, opts, hostPath); err != nil {
				return err
			}
			return kubectlCreate(dryRun, manifest, opts, metrics)
		}),
	}
	deployLocal.Flags().StringVar(&hostPath, "host-path", "/var/pachyderm", "Location on the host machine where PFS metadata will be stored.")
	deployLocal.Flags().BoolVarP(&dev, "dev", "d", false, "Deploy pachd with local version tags, disable metrics, expose Pachyderm's object/block API, and use an insecure authentication mechanism (do not set on any cluster with sensitive data)")

	deployGoogle := &cobra.Command{
		Use:   "google <GCS bucket> <size of disk(s) (in GB)> [<service account creds file>]",
		Short: "Deploy a Pachyderm cluster running on GCP.",
		Long: "Deploy a Pachyderm cluster running on GCP.\n" +
			"Arguments are:\n" +
			"  <GCS bucket>: A GCS bucket where Pachyderm will store PFS data.\n" +
			"  <GCE persistent disks>: A comma-separated list of GCE persistent disks, one per etcd node (see --etcd-nodes).\n" +
			"  <size of disks>: Size of GCE persistent disks in GB (assumed to all be the same).\n" +
			"  <service account creds file>: a file contain a private key for a service account (downloaded from GCE).\n",
		Run: cmdutil.RunBoundedArgs(2, 3, func(args []string) (retErr error) {
			if metrics && !dev {
				start := time.Now()
				startMetricsWait := _metrics.StartReportAndFlushUserAction("Deploy", start)
				defer startMetricsWait()
				defer func() {
					finishMetricsWait := _metrics.FinishReportAndFlushUserAction("Deploy", retErr, start)
					finishMetricsWait()
				}()
			}
			volumeSize, err := strconv.Atoi(args[1])
			if err != nil {
				return fmt.Errorf("volume size needs to be an integer; instead got %v", args[1])
			}
			manifest := getEncoder(outputFormat)
			opts.BlockCacheSize = "0G" // GCS is fast so we want to disable the block cache. See issue #1650
			var cred string
			if len(args) == 3 {
				credBytes, err := ioutil.ReadFile(args[2])
				if err != nil {
					return fmt.Errorf("error reading creds file %s: %v", args[2], err)
				}
				cred = string(credBytes)
			}
			if err = assets.WriteGoogleAssets(manifest, opts, args[0], cred, volumeSize); err != nil {
				return err
			}
			return kubectlCreate(dryRun, manifest, opts, metrics)
		}),
	}

	deployCustom := &cobra.Command{
		Use:   "custom --persistent-disk <persistent disk backend> --object-store <object store backend> <persistent disk args> <object store args>",
		Short: "(in progress) Deploy a custom Pachyderm cluster configuration",
		Long: "(in progress) Deploy a custom Pachyderm cluster configuration.\n" +
			"If <object store backend> is \"s3\", then the arguments are:\n" +
			"    <volumes> <size of volumes (in GB)> <bucket> <id> <secret> <endpoint>\n",
		Run: cmdutil.RunBoundedArgs(4, 7, func(args []string) (retErr error) {
			if metrics && !dev {
				start := time.Now()
				startMetricsWait := _metrics.StartReportAndFlushUserAction("Deploy", start)
				defer startMetricsWait()
				defer func() {
					finishMetricsWait := _metrics.FinishReportAndFlushUserAction("Deploy", retErr, start)
					finishMetricsWait()
				}()
			}
			manifest := getEncoder(outputFormat)
			err := assets.WriteCustomAssets(manifest, opts, args, objectStoreBackend, persistentDiskBackend, secure, isS3V2)
			if err != nil {
				return err
			}
			return kubectlCreate(dryRun, manifest, opts, metrics)
		}),
	}
	deployCustom.Flags().BoolVarP(&secure, "secure", "s", false, "Enable secure access to a Minio server.")
	deployCustom.Flags().StringVar(&persistentDiskBackend, "persistent-disk", "aws",
		"(required) Backend providing persistent local volumes to stateful pods. "+
			"One of: aws, google, or azure.")
	deployCustom.Flags().StringVar(&objectStoreBackend, "object-store", "s3",
		"(required) Backend providing an object-storage API to pachyderm. One of: "+
			"s3, gcs, or azure-blob.")
	deployCustom.Flags().BoolVar(&isS3V2, "isS3V2", false, "Enable S3V2 client")

	var creds string
	var vault string
	var iamRole string
	var cloudfrontDistribution string
	deployAmazon := &cobra.Command{
		Use:   "amazon <S3 bucket> <region> <size of volumes (in GB)>",
		Short: "Deploy a Pachyderm cluster running on AWS.",
		Long: "Deploy a Pachyderm cluster running on AWS. Arguments are:\n" +
			"  <S3 bucket>: An S3 bucket where Pachyderm will store PFS data.\n" +
			"\n" +
			"  <region>: The aws region where pachyderm is being deployed (e.g. us-west-1)\n" +
			"  <size of volumes>: Size of EBS volumes, in GB (assumed to all be the same).\n",
		Run: cmdutil.RunFixedArgs(3, func(args []string) (retErr error) {
			if metrics && !dev {
				start := time.Now()
				startMetricsWait := _metrics.StartReportAndFlushUserAction("Deploy", start)
				defer startMetricsWait()
				defer func() {
					finishMetricsWait := _metrics.FinishReportAndFlushUserAction("Deploy", retErr, start)
					finishMetricsWait()
				}()
			}
			if creds == "" && vault == "" && iamRole == "" {
				return fmt.Errorf("One of --credentials, --vault, or --iam-role needs to be provided")
			}

			// populate 'amazonCreds' & validate
			var amazonCreds *assets.AmazonCreds
			s := bufio.NewScanner(os.Stdin)
			if creds != "" {
				parts := strings.Split(creds, ",")
				if len(parts) < 2 || len(parts) > 3 || containsEmpty(parts[:2]) {
					return fmt.Errorf("Incorrect format of --credentials")
				}
				amazonCreds = &assets.AmazonCreds{ID: parts[0], Secret: parts[1]}
				if len(parts) > 2 {
					amazonCreds.Token = parts[2]
				}

				if !awsAccessKeyIDRE.MatchString(amazonCreds.ID) {
					fmt.Printf("The AWS Access Key seems invalid (does not match %q). "+
						"Do you want to continue deploying? [yN]\n", awsAccessKeyIDRE)
					if s.Scan(); s.Text()[0] != 'y' && s.Text()[0] != 'Y' {
						os.Exit(1)
					}
				}

				if !awsSecretRE.MatchString(amazonCreds.Secret) {
					fmt.Printf("The AWS Secret seems invalid (does not match %q). "+
						"Do you want to continue deploying? [yN]\n", awsSecretRE)
					if s.Scan(); s.Text()[0] != 'y' && s.Text()[0] != 'Y' {
						os.Exit(1)
					}
				}
			}
			if vault != "" {
				if amazonCreds != nil {
					return fmt.Errorf("Only one of --credentials, --vault, or --iam-role needs to be provided")
				}
				parts := strings.Split(vault, ",")
				if len(parts) != 3 || containsEmpty(parts) {
					return fmt.Errorf("Incorrect format of --vault")
				}
				amazonCreds = &assets.AmazonCreds{VaultAddress: parts[0], VaultRole: parts[1], VaultToken: parts[2]}
			}
			if iamRole != "" {
				if amazonCreds != nil {
					return fmt.Errorf("Only one of --credentials, --vault, or --iam-role needs to be provided")
				}
				opts.IAMRole = iamRole
			}
			volumeSize, err := strconv.Atoi(args[2])
			if err != nil {
				return fmt.Errorf("volume size needs to be an integer; instead got %v", args[2])
			}
			if strings.TrimSpace(cloudfrontDistribution) != "" {
				fmt.Printf("WARNING: You specified a cloudfront distribution. Deploying on AWS with cloudfront is currently " +
					"an alpha feature. No security restrictions have been applied to cloudfront, making all data public (obscured but not secured)\n")
			}
			bucket, region := strings.TrimPrefix(args[0], "s3://"), args[1]
			if !awsRegionRE.MatchString(region) {
				fmt.Printf("The AWS region seems invalid (does not match %q). "+
					"Do you want to continue deploying? [yN]\n", awsRegionRE)
				if s.Scan(); s.Text()[0] != 'y' && s.Text()[0] != 'Y' {
					os.Exit(1)
				}
			}

			// generate manifest and write assets
			manifest := getEncoder(outputFormat)
			if err = assets.WriteAmazonAssets(manifest, opts, region, bucket, volumeSize, amazonCreds, cloudfrontDistribution); err != nil {
				return err
			}
			return kubectlCreate(dryRun, manifest, opts, metrics)
		}),
	}
	deployAmazon.Flags().StringVar(&cloudfrontDistribution, "cloudfront-distribution", "",
		"Deploying on AWS with cloudfront is currently "+
			"an alpha feature. No security restrictions have been"+
			"applied to cloudfront, making all data public (obscured but not secured)")
	deployAmazon.Flags().StringVar(&creds, "credentials", "", "Use the format \"<id>,<secret>[,<token>]\". You can get a token by running \"aws sts get-session-token\".")
	deployAmazon.Flags().StringVar(&vault, "vault", "", "Use the format \"<address/hostport>,<role>,<token>\".")
	deployAmazon.Flags().StringVar(&iamRole, "iam-role", "", fmt.Sprintf("Use the given IAM role for authorization, as opposed to using static credentials. The given role will be applied as the annotation %s, this used with a Kubernetes IAM role management system such as kube2iam allows you to give pachd credentials in a more secure way.", assets.IAMAnnotation))

	deployMicrosoft := &cobra.Command{
		Use:   "microsoft <container> <storage account name> <storage account key> <size of volumes (in GB)>",
		Short: "Deploy a Pachyderm cluster running on Microsoft Azure.",
		Long: "Deploy a Pachyderm cluster running on Microsoft Azure. Arguments are:\n" +
			"  <container>: An Azure container where Pachyderm will store PFS data.\n" +
			"  <size of volumes>: Size of persistent volumes, in GB (assumed to all be the same).\n",
		Run: cmdutil.RunFixedArgs(4, func(args []string) (retErr error) {
			if metrics && !dev {
				start := time.Now()
				startMetricsWait := _metrics.StartReportAndFlushUserAction("Deploy", start)
				defer startMetricsWait()
				defer func() {
					finishMetricsWait := _metrics.FinishReportAndFlushUserAction("Deploy", retErr, start)
					finishMetricsWait()
				}()
			}
			if _, err := base64.StdEncoding.DecodeString(args[2]); err != nil {
				return fmt.Errorf("storage-account-key needs to be base64 encoded; instead got '%v'", args[2])
			}
			if opts.EtcdVolume != "" {
				tempURI, err := url.ParseRequestURI(opts.EtcdVolume)
				if err != nil {
					return fmt.Errorf("Volume URI needs to be a well-formed URI; instead got '%v'", opts.EtcdVolume)
				}
				opts.EtcdVolume = tempURI.String()
			}
			volumeSize, err := strconv.Atoi(args[3])
			if err != nil {
				return fmt.Errorf("volume size needs to be an integer; instead got %v", args[3])
			}
			manifest := getEncoder(outputFormat)
			if err = assets.WriteMicrosoftAssets(manifest, opts, args[0], args[1], args[2], volumeSize); err != nil {
				return err
			}
			return kubectlCreate(dryRun, manifest, opts, metrics)
		}),
	}

	deployStorage := &cobra.Command{
		Use:   "storage <backend> ...",
		Short: "Deploy credentials for a particular storage provider.",
		Long: `
Deploy credentials for a particular storage provider, so that Pachyderm can
ingress data from and egress data to it.  Currently three backends are
supported: aws, google, and azure.  To see the required arguments for a
particular backend, run "pachctl deploy storage <backend>"`,
		Run: cmdutil.RunBoundedArgs(1, 5, func(args []string) (retErr error) {
			var data map[string][]byte
			switch args[0] {
			case "amazon", "aws":
				// Need at least 4 arguments: backend, bucket, id, secret
				if len(args) < 4 {
					return fmt.Errorf("Usage: pachctl deploy storage amazon <region> <id> <secret> <token>\n\n<token> is optional")
				}
				var token string
				if len(args) == 5 {
					token = args[4]
				}
				data = assets.AmazonSecret(args[1], "", args[2], args[3], token, "")
			case "google":
				if len(args) < 2 {
					return fmt.Errorf("Usage: pachctl deploy storage google <service account creds file>")
				}
				credBytes, err := ioutil.ReadFile(args[1])
				if err != nil {
					return fmt.Errorf("error reading creds file %s: %v", args[2], err)
				}
				data = assets.GoogleSecret("", string(credBytes))
			case "azure":
				// Need 3 arguments: backend, account name, account key
				if len(args) != 3 {
					return fmt.Errorf("Usage: pachctl deploy storage azure <account name> <account key>")
				}
				data = assets.MicrosoftSecret("", args[1], args[2])
			}

			c, err := client.NewOnUserMachine(metrics, portForwarding, "user")
			if err != nil {
				return fmt.Errorf("error constructing pachyderm client: %v", err)
			}
			defer c.Close()

			_, err = c.DeployStorageSecret(context.Background(), &deployclient.DeployStorageSecretRequest{
				Secrets: data,
			})
			if err != nil {
				return fmt.Errorf("error deploying storage secret to pachd: %v", err)
			}
			return nil
		}),
	}

	listImages := &cobra.Command{
		Use:   "list-images",
		Short: "Output the list of images in a deployment.",
		Long:  "Output the list of images in a deployment.",
		Run: cmdutil.RunFixedArgs(0, func(args []string) error {
			for _, image := range assets.Images(opts) {
				fmt.Println(image)
			}
			return nil
		}),
	}

	exportImages := &cobra.Command{
		Use:   "export-images output-file",
		Short: "Export a tarball (to stdout) containing all of the images in a deployment.",
		Long:  "Export a tarball (to stdout) containing all of the images in a deployment.",
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

	importImages := &cobra.Command{
		Use:   "import-images input-file",
		Short: "Import a tarball (from stdin) containing all of the images in a deployment and push them to a private registry.",
		Long:  "Import a tarball (from stdin) containing all of the images in a deployment and push them to a private registry.",
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

	deploy := &cobra.Command{
		Use:   "deploy amazon|google|microsoft|local|custom|storage",
		Short: "Deploy a Pachyderm cluster.",
		Long:  "Deploy a Pachyderm cluster.",
		PersistentPreRun: cmdutil.Run(func([]string) error {
			dashImage = getDefaultOrLatestDashImage(dashImage, dryRun)
			opts = &assets.AssetOpts{
				PachdShards:             uint64(pachdShards),
				Version:                 version.PrettyPrintVersion(version.Version),
				LogLevel:                logLevel,
				Metrics:                 metrics,
				PachdCPURequest:         pachdCPURequest,
				PachdNonCacheMemRequest: pachdNonCacheMemRequest,
				BlockCacheSize:          blockCacheSize,
				EtcdCPURequest:          etcdCPURequest,
				EtcdMemRequest:          etcdMemRequest,
				EtcdNodes:               etcdNodes,
				EtcdVolume:              etcdVolume,
				EtcdStorageClassName:    etcdStorageClassName,
				DashOnly:                dashOnly,
				NoDash:                  noDash,
				DashImage:               dashImage,
				Registry:                registry,
				ImagePullSecret:         imagePullSecret,
				NoGuaranteed:            noGuaranteed,
				NoRBAC:                  noRBAC,
				LocalRoles:              localRoles,
				Namespace:               namespace,
				NoExposeDockerSocket:    noExposeDockerSocket,
				ExposeObjectAPI:         exposeObjectAPI,
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
			}
			return nil
		}),
	}
	deploy.PersistentFlags().IntVar(&pachdShards, "shards", 16, "(rarely set) The maximum number of pachd nodes allowed in the cluster; increasing this number blindly can result in degraded performance.")
	deploy.PersistentFlags().IntVar(&etcdNodes, "dynamic-etcd-nodes", 0, "Deploy etcd as a StatefulSet with the given number of pods.  The persistent volumes used by these pods are provisioned dynamically.  Note that StatefulSet is currently a beta kubernetes feature, which might be unavailable in older versions of kubernetes.")
	deploy.PersistentFlags().StringVar(&etcdVolume, "static-etcd-volume", "", "Deploy etcd as a ReplicationController with one pod.  The pod uses the given persistent volume.")
	deploy.PersistentFlags().StringVar(&etcdStorageClassName, "etcd-storage-class", "", "If set, the name of an existing StorageClass to use for etcd storage. Ignored if --static-etcd-volume is set.")
	deploy.PersistentFlags().BoolVar(&dryRun, "dry-run", false, "Don't actually deploy pachyderm to Kubernetes, instead just print the manifest.")
	deploy.PersistentFlags().StringVarP(&outputFormat, "output", "o", "json", "Output formmat. One of: json|yaml")
	deploy.PersistentFlags().StringVar(&logLevel, "log-level", "info", "The level of log messages to print options are, from least to most verbose: \"error\", \"info\", \"debug\".")
	deploy.PersistentFlags().BoolVar(&dashOnly, "dashboard-only", false, "Only deploy the Pachyderm UI (experimental), without the rest of pachyderm. This is for launching the UI adjacent to an existing Pachyderm cluster. After deployment, run \"pachctl port-forward\" to connect")
	deploy.PersistentFlags().BoolVar(&noDash, "no-dashboard", false, "Don't deploy the Pachyderm UI alongside Pachyderm (experimental).")
	deploy.PersistentFlags().StringVar(&registry, "registry", "", "The registry to pull images from.")
	deploy.PersistentFlags().StringVar(&imagePullSecret, "image-pull-secret", "", "A secret in Kubernetes that's needed to pull from your private registry.")
	deploy.PersistentFlags().StringVar(&dashImage, "dash-image", "", "Image URL for pachyderm dashboard")
	deploy.PersistentFlags().BoolVar(&noGuaranteed, "no-guaranteed", false, "Don't use guaranteed QoS for etcd and pachd deployments. Turning this on (turning guaranteed QoS off) can lead to more stable local clusters (such as a on Minikube), it should normally be used for production clusters.")
	deploy.PersistentFlags().BoolVar(&noRBAC, "no-rbac", false, "Don't deploy RBAC roles for Pachyderm. (for k8s versions prior to 1.8)")
	deploy.PersistentFlags().BoolVar(&localRoles, "local-roles", false, "Use namespace-local roles instead of cluster roles. Ignored if --no-rbac is set.")
	deploy.PersistentFlags().StringVar(&namespace, "namespace", "default", "Kubernetes namespace to deploy Pachyderm to.")
	deploy.PersistentFlags().BoolVar(&noExposeDockerSocket, "no-expose-docker-socket", false, "Don't expose the Docker socket to worker containers. This limits the privileges of workers which prevents them from automatically setting the container's working dir and user.")
	deploy.PersistentFlags().BoolVar(&exposeObjectAPI, "expose-object-api", false, "If set, instruct pachd to serve its object/block API on its public port (not safe with auth enabled, do not set in production).")
	deploy.PersistentFlags().StringVar(&tlsCertKey, "tls", "", "string of the form \"<cert path>,<key path>\" of the signed TLS certificate and private key that Pachd should use for TLS authentication (enables TLS-encrypted communication with Pachd)")

	deploy.AddCommand(
		deployLocal,
		deployAmazon,
		deployGoogle,
		deployMicrosoft,
		deployCustom,
		deployStorage,
		listImages,
		exportImages,
		importImages,
	)

	// Flags for setting pachd resource requests. These should rarely be set --
	// only if we get the defaults wrong, or users have an unusual access pattern
	//
	// All of these are empty by default, because the actual default values depend
	// on the backend to which we're. The defaults are set in
	// s/s/pkg/deploy/assets/assets.go
	deploy.PersistentFlags().StringVar(&pachdCPURequest,
		"pachd-cpu-request", "", "(rarely set) The size of Pachd's CPU "+
			"request, which we give to Kubernetes. Size is in cores (with partial "+
			"cores allowed and encouraged).")
	deploy.PersistentFlags().StringVar(&blockCacheSize, "block-cache-size", "",
		"Size of pachd's in-memory cache for PFS files. Size is specified in "+
			"bytes, with allowed SI suffixes (M, K, G, Mi, Ki, Gi, etc).")
	deploy.PersistentFlags().StringVar(&pachdNonCacheMemRequest,
		"pachd-memory-request", "", "(rarely set) The size of PachD's memory "+
			"request in addition to its block cache (set via --block-cache-size). "+
			"Size is in bytes, with SI suffixes (M, K, G, Mi, Ki, Gi, etc).")
	deploy.PersistentFlags().StringVar(&etcdCPURequest,
		"etcd-cpu-request", "", "(rarely set) The size of etcd's CPU request, "+
			"which we give to Kubernetes. Size is in cores (with partial cores "+
			"allowed and encouraged).")
	deploy.PersistentFlags().StringVar(&etcdMemRequest,
		"etcd-memory-request", "", "(rarely set) The size of etcd's memory "+
			"request. Size is in bytes, with SI suffixes (M, K, G, Mi, Ki, Gi, "+
			"etc).")
	return deploy
}

// Cmds returns a list of cobra commands for deploying Pachyderm clusters.
func Cmds(metrics bool, portForwarding bool) []*cobra.Command {
	deploy := DeployCmd(metrics, portForwarding)
	var all bool
	var namespace string
	undeploy := &cobra.Command{
		Use:   "undeploy",
		Short: "Tear down a deployed Pachyderm cluster.",
		Long:  "Tear down a deployed Pachyderm cluster.",
		Run: cmdutil.RunFixedArgs(0, func(args []string) error {
			if all {
				fmt.Printf(`
By using the --all flag, you are going to delete everything, including the
persistent volumes where metadata is stored.  If your persistent volumes
were dynamically provisioned (i.e. if you used the "--dynamic-etcd-nodes"
flag), the underlying volumes will be removed, making metadata such repos,
commits, pipelines, and jobs unrecoverable. If your persistent volume was
manually provisioned (i.e. if you used the "--static-etcd-volume" flag), the
underlying volume will not be removed.

Are you sure you want to proceed? yN
`)
				r := bufio.NewReader(os.Stdin)
				bytes, err := r.ReadBytes('\n')
				if err != nil {
					return err
				}
				if !(bytes[0] == 'y' || bytes[0] == 'Y') {
					return nil
				}
			}
			io := cmdutil.IO{
				Stdout: os.Stdout,
				Stderr: os.Stderr,
			}
			assets := []string{
				"service",
				"replicationcontroller",
				"deployment",
				"serviceaccount",
				"secret",
				"statefulset",
				"clusterrole",
				"clusterrolebinding",
			}
			if all {
				assets = append(assets, []string{
					"storageclass",
					"persistentvolumeclaim",
					"persistentvolume",
				}...)
			}
			for _, asset := range assets {
				if err := cmdutil.RunIO(io, "kubectl", "delete", asset, "-l", "suite=pachyderm", "--namespace", namespace); err != nil {
					return err
				}
			}
			return nil
		}),
	}
	undeploy.Flags().BoolVarP(&all, "all", "a", false, `
Delete everything, including the persistent volumes where metadata
is stored.  If your persistent volumes were dynamically provisioned (i.e. if
you used the "--dynamic-etcd-nodes" flag), the underlying volumes will be
removed, making metadata such repos, commits, pipelines, and jobs
unrecoverable. If your persistent volume was manually provisioned (i.e. if
you used the "--static-etcd-volume" flag), the underlying volume will not be
removed.`)
	undeploy.Flags().StringVar(&namespace, "namespace", "default", "Kubernetes namespace to undeploy Pachyderm from.")

	var updateDashDryRun bool
	var updateDashOutputFormat string
	updateDash := &cobra.Command{
		Use:   "update-dash",
		Short: "Update and redeploy the Pachyderm Dashboard at the latest compatible version.",
		Long:  "Update and redeploy the Pachyderm Dashboard at the latest compatible version.",
		Run: cmdutil.RunFixedArgs(0, func(args []string) error {
			// Undeploy the dash
			if !updateDashDryRun {
				io := cmdutil.IO{
					Stdout: os.Stdout,
					Stderr: os.Stderr,
				}
				if err := cmdutil.RunIO(io, "kubectl", "delete", "deploy", "-l", "suite=pachyderm,app=dash"); err != nil {
					return err
				}
				if err := cmdutil.RunIO(io, "kubectl", "delete", "svc", "-l", "suite=pachyderm,app=dash"); err != nil {
					return err
				}
			}
			// Redeploy the dash
			manifest := getEncoder(updateDashOutputFormat)
			dashImage := getDefaultOrLatestDashImage("", updateDashDryRun)
			opts := &assets.AssetOpts{
				DashOnly:  true,
				DashImage: dashImage,
			}
			assets.WriteDashboardAssets(manifest, opts)
			return kubectlCreate(updateDashDryRun, manifest, opts, false)
		}),
	}
	updateDash.Flags().BoolVar(&updateDashDryRun, "dry-run", false, "Don't actually deploy Pachyderm Dash to Kubernetes, instead just print the manifest.")
	updateDash.Flags().StringVarP(&updateDashOutputFormat, "output", "o", "json", "Output formmat. One of: json|yaml")

	return []*cobra.Command{deploy, undeploy, updateDash}
}

func getDefaultOrLatestDashImage(dashImage string, dryRun bool) string {
	var err error
	v := version.PrettyPrintVersion(version.Version)
	defer func() {
		if err != nil && !dryRun {
			fmt.Printf("No updated dash image found for pachctl %v: %v Falling back to dash image %v\n", v, err, version.DefaultDashVersion)
		}
	}()
	if dashImage != "" {
		// It has been supplied explicitly by version on the command line
		return dashImage
	}
	dashImage = fmt.Sprintf("pachyderm/dash:%v", version.DefaultDashVersion)
	compatibleDashVersionsURL := fmt.Sprintf("https://raw.githubusercontent.com/pachyderm/pachyderm/master/etc/compatibility/%v", v)
	resp, err := http.Get(compatibleDashVersionsURL)
	if err != nil {
		return dashImage
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return dashImage
	}
	if resp.StatusCode != 200 {
		err = errors.New(string(body))
		return dashImage
	}
	allVersions := strings.Split(strings.TrimSpace(string(body)), "\n")
	if len(allVersions) < 1 {
		return dashImage
	}
	latestVersion := strings.TrimSpace(allVersions[len(allVersions)-1])

	return fmt.Sprintf("pachyderm/dash:%v", latestVersion)
}

package cmds

import (
	"bufio"
	"bytes"
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
	if err := cmdutil.RunIO(io, "kubectl", "apply", "-f", "-", "--validate=false", "--namespace", opts.Namespace); err != nil {
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

// deployCmds returns the set of cobra.Commands used to deploy pachyderm.
func deployCmds(noMetrics *bool, noPortForwarding *bool) []*cobra.Command {
	var commands []*cobra.Command
	var opts *assets.AssetOpts

	var dryRun bool
	var outputFormat string

	var dev bool
	var hostPath string
	deployLocal := &cobra.Command{
		Short: "Deploy a single-node Pachyderm cluster with local metadata storage.",
		Long:  "Deploy a single-node Pachyderm cluster with local metadata storage.",
		Run: cmdutil.RunFixedArgs(0, func(args []string) (retErr error) {
			metrics := !*noMetrics

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
	commands = append(commands, cmdutil.CreateAlias(deployLocal, "deploy local"))

	deployGoogle := &cobra.Command{
		Use:   "{{alias}} <bucket-name> <disk-size> [<credentials-file>]",
		Short: "Deploy a Pachyderm cluster running on Google Cloud Platform.",
		Long: `Deploy a Pachyderm cluster running on Google Cloud Platform.
  <bucket-name>: A Google Cloud Storage bucket where Pachyderm will store PFS data.
  <disk-size>: Size of Google Compute Engine persistent disks in GB (assumed to all be the same).
  <credentials-file>: A file containing the private key for the account (downloaded from Google Compute Engine).`,
		Run: cmdutil.RunBoundedArgs(2, 3, func(args []string) (retErr error) {
			metrics := !*noMetrics

			if metrics {
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
			bucket := strings.TrimPrefix(args[0], "gs://")
			if err = assets.WriteGoogleAssets(manifest, opts, bucket, cred, volumeSize); err != nil {
				return err
			}
			return kubectlCreate(dryRun, manifest, opts, metrics)
		}),
	}
	commands = append(commands, cmdutil.CreateAlias(deployGoogle, "deploy google"))

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
		Run: cmdutil.RunBoundedArgs(4, 7, func(args []string) (retErr error) {
			metrics := !*noMetrics

			if metrics {
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
	commands = append(commands, cmdutil.CreateAlias(deployCustom, "deploy custom"))

	var cloudfrontDistribution string
	var creds string
	var iamRole string
	var vault string
	deployAmazon := &cobra.Command{
		Use:   "{{alias}} <bucket-name> <region> <disk-size>",
		Short: "Deploy a Pachyderm cluster running on AWS.",
		Long: `Deploy a Pachyderm cluster running on AWS.
  <bucket-name>: An S3 bucket where Pachyderm will store PFS data.
  <region>: The AWS region where Pachyderm is being deployed (e.g. us-west-1)
  <disk-size>: Size of EBS volumes, in GB (assumed to all be the same).`,
		Run: cmdutil.RunFixedArgs(3, func(args []string) (retErr error) {
			metrics := !*noMetrics

			if metrics {
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
	commands = append(commands, cmdutil.CreateAlias(deployAmazon, "deploy amazon"))

	deployMicrosoft := &cobra.Command{
		Use:   "{{alias}} <container> <account-name> <account-key> <disk-size>",
		Short: "Deploy a Pachyderm cluster running on Microsoft Azure.",
		Long: `Deploy a Pachyderm cluster running on Microsoft Azure.
  <container>: An Azure container where Pachyderm will store PFS data.
  <disk-size>: Size of persistent volumes, in GB (assumed to all be the same).`,
		Run: cmdutil.RunFixedArgs(4, func(args []string) (retErr error) {
			metrics := !*noMetrics

			if metrics {
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
			container := strings.TrimPrefix(args[0], "wasb://")
			accountName, accountKey := args[1], args[2]
			if err = assets.WriteMicrosoftAssets(manifest, opts, container, accountName, accountKey, volumeSize); err != nil {
				return err
			}
			return kubectlCreate(dryRun, manifest, opts, metrics)
		}),
	}
	commands = append(commands, cmdutil.CreateAlias(deployMicrosoft, "deploy microsoft"))

	deployStorageSecrets := func(data map[string][]byte) error {
		c, err := client.NewOnUserMachine(!*noMetrics, !*noPortForwarding, "user")
		if err != nil {
			return fmt.Errorf("error constructing pachyderm client: %v", err)
		}
		defer c.Close()

		// clean up any empty, but non-nil strings in the data, since those will prevent those fields from getting merged when we do the patch
		for k, v := range data {
			if v != nil && len(v) == 0 {
				delete(data, k)
			}
		}

		manifest := getEncoder(outputFormat)
		err = assets.WriteSecret(manifest, data, opts)
		if err != nil {
			return err
		}
		if dryRun {
			_, err := os.Stdout.Write(manifest.Buffer().Bytes())
			return err
		}

		io := cmdutil.IO{
			Stdin:  manifest.Buffer(),
			Stdout: os.Stdout,
			Stderr: os.Stderr,
		}

		// it can't unmarshal it from stdin in the given format for some reason, so we pass it in directly
		s := string(manifest.Buffer().Bytes())
		return cmdutil.RunIO(io, `kubectl`, "patch", "secret", "pachyderm-storage-secret", "-p", s, "--namespace", opts.Namespace, "--type=merge")
	}

	deployStorageAmazon := &cobra.Command{
		Use:   "{{alias}} <region> <access-key-id> <secret-access-key> [<session-token>]",
		Short: "Deploy credentials for the Amazon S3 storage provider.",
		Long:  "Deploy credentials for the Amazon S3 storage provider, so that Pachyderm can ingress data from and egress data to it.",
		Run: cmdutil.RunBoundedArgs(3, 4, func(args []string) error {
			var token string
			if len(args) == 4 {
				token = args[3]
			}
			return deployStorageSecrets(assets.AmazonSecret(args[0], "", args[1], args[2], token, "", ""))
		}),
	}
	commands = append(commands, cmdutil.CreateAlias(deployStorageAmazon, "deploy storage amazon"))

	deployStorageGoogle := &cobra.Command{
		Use:   "{{alias}} <credentials-file>",
		Short: "Deploy credentials for the Google Cloud storage provider.",
		Long:  "Deploy credentials for the Google Cloud storage provider, so that Pachyderm can ingress data from and egress data to it.",
		Run: cmdutil.RunFixedArgs(1, func(args []string) error {
			credBytes, err := ioutil.ReadFile(args[0])
			if err != nil {
				return fmt.Errorf("error reading credentials file %s: %v", args[0], err)
			}
			return deployStorageSecrets(assets.GoogleSecret("", string(credBytes)))
		}),
	}
	commands = append(commands, cmdutil.CreateAlias(deployStorageGoogle, "deploy storage google"))

	deployStorageAzure := &cobra.Command{
		Use:   "{{alias}} <account-name> <account-key>",
		Short: "Deploy credentials for the Azure storage provider.",
		Long:  "Deploy credentials for the Azure storage provider, so that Pachyderm can ingress data from and egress data to it.",
		Run: cmdutil.RunFixedArgs(2, func(args []string) error {
			return deployStorageSecrets(assets.MicrosoftSecret("", args[0], args[1]))
		}),
	}
	commands = append(commands, cmdutil.CreateAlias(deployStorageAzure, "deploy storage microsoft"))

	deployStorage := &cobra.Command{
		Short: "Deploy credentials for a particular storage provider.",
		Long:  "Deploy credentials for a particular storage provider, so that Pachyderm can ingress data from and egress data to it.",
	}
	commands = append(commands, cmdutil.CreateAlias(deployStorage, "deploy storage"))

	listImages := &cobra.Command{
		Short: "Output the list of images in a deployment.",
		Long:  "Output the list of images in a deployment.",
		Run: cmdutil.RunFixedArgs(0, func(args []string) error {
			for _, image := range assets.Images(opts) {
				fmt.Println(image)
			}
			return nil
		}),
	}
	commands = append(commands, cmdutil.CreateAlias(listImages, "deploy list-images"))

	exportImages := &cobra.Command{
		Use:   "{{alias}} <output-file>",
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
	commands = append(commands, cmdutil.CreateAlias(exportImages, "deploy export-images"))

	importImages := &cobra.Command{
		Use:   "{{alias}} <input-file>",
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
	commands = append(commands, cmdutil.CreateAlias(importImages, "deploy import-images"))

	var blockCacheSize string
	var dashImage string
	var dashOnly bool
	var etcdCPURequest string
	var etcdMemRequest string
	var etcdNodes int
	var etcdStorageClassName string
	var etcdVolume string
	var postgresCPURequest string
	var postgresMemRequest string
	var postgresNodes int
	var postgresStorageClassName string
	var postgresVolume string
	var exposeObjectAPI bool
	var imagePullSecret string
	var localRoles bool
	var logLevel string
	var namespace string
	var newHashTree bool
	var noDash bool
	var noExposeDockerSocket bool
	var noGuaranteed bool
	var noRBAC bool
	var pachdCPURequest string
	var pachdNonCacheMemRequest string
	var pachdShards int
	var registry string
	var tlsCertKey string
	deploy := &cobra.Command{
		Short: "Deploy a Pachyderm cluster.",
		Long:  "Deploy a Pachyderm cluster.",
		PersistentPreRun: cmdutil.Run(func([]string) error {
			dashImage = getDefaultOrLatestDashImage(dashImage, dryRun)
			opts = &assets.AssetOpts{
				FeatureFlags: assets.FeatureFlags{
					NewHashTree: newHashTree,
				},
				PachdShards:              uint64(pachdShards),
				Version:                  version.PrettyPrintVersion(version.Version),
				LogLevel:                 logLevel,
				Metrics:                  !*noMetrics,
				PachdCPURequest:          pachdCPURequest,
				PachdNonCacheMemRequest:  pachdNonCacheMemRequest,
				BlockCacheSize:           blockCacheSize,
				EtcdCPURequest:           etcdCPURequest,
				EtcdMemRequest:           etcdMemRequest,
				EtcdNodes:                etcdNodes,
				EtcdVolume:               etcdVolume,
				EtcdStorageClassName:     etcdStorageClassName,
				PostgresCPURequest:       postgresCPURequest,
				PostgresMemRequest:       postgresMemRequest,
				PostgresNodes:            postgresNodes,
				PostgresVolume:           postgresVolume,
				PostgresStorageClassName: postgresStorageClassName,
				DashOnly:                 dashOnly,
				NoDash:                   noDash,
				DashImage:                dashImage,
				Registry:                 registry,
				ImagePullSecret:          imagePullSecret,
				NoGuaranteed:             noGuaranteed,
				NoRBAC:                   noRBAC,
				LocalRoles:               localRoles,
				Namespace:                namespace,
				NoExposeDockerSocket:     noExposeDockerSocket,
				ExposeObjectAPI:          exposeObjectAPI,
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
	deploy.PersistentFlags().IntVar(&postgresNodes, "dynamic-postgres-nodes", 0, "Deploy postgres as a StatefulSet with the given number of pods.  The persistent volumes used by these pods are provisioned dynamically.  Note that StatefulSet is currently a beta kubernetes feature, which might be unavailable in older versions of kubernetes.")
	deploy.PersistentFlags().StringVar(&postgresVolume, "static-postgres-volume", "", "Deploy postgres as a ReplicationController with one pod.  The pod uses the given persistent volume.")
	deploy.PersistentFlags().StringVar(&postgresStorageClassName, "postgres-storage-class", "", "If set, the name of an existing StorageClass to use for postgres storage. Ignored if --static-postgres-volume is set.")
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
	deploy.PersistentFlags().BoolVar(&newHashTree, "new-hash-tree-flag", false, "(feature flag) Do not set, used for testing")

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
	deploy.PersistentFlags().StringVar(&postgresCPURequest,
		"postgres-cpu-request", "", "(rarely set) The size of postgres's CPU request, "+
			"which we give to Kubernetes. Size is in cores (with partial cores "+
			"allowed and encouraged).")
	deploy.PersistentFlags().StringVar(&postgresMemRequest,
		"postgres-memory-request", "", "(rarely set) The size of postgres's memory "+
			"request. Size is in bytes, with SI suffixes (M, K, G, Mi, Ki, Gi, "+
			"etc).")

	commands = append(commands, cmdutil.CreateAlias(deploy, "deploy"))

	return commands
}

// Cmds returns a list of cobra commands for deploying Pachyderm clusters.
func Cmds(noMetrics *bool, noPortForwarding *bool) []*cobra.Command {
	var commands []*cobra.Command

	commands = append(commands, deployCmds(noMetrics, noPortForwarding)...)

	var all bool
	var namespace string
	undeploy := &cobra.Command{
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
`)
			}
			fmt.Println("Are you sure you want to do this? (y/n):")
			r := bufio.NewReader(os.Stdin)
			bytes, err := r.ReadBytes('\n')
			if err != nil {
				return err
			}
			if bytes[0] == 'y' || bytes[0] == 'Y' {
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
	commands = append(commands, cmdutil.CreateAlias(undeploy, "undeploy"))

	var updateDashDryRun bool
	var updateDashOutputFormat string
	updateDash := &cobra.Command{
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
			opts := &assets.AssetOpts{
				DashOnly:  true,
				DashImage: getDefaultOrLatestDashImage("", updateDashDryRun),
			}
			assets.WriteDashboardAssets(manifest, opts)
			return kubectlCreate(updateDashDryRun, manifest, opts, false)
		}),
	}
	updateDash.Flags().BoolVar(&updateDashDryRun, "dry-run", false, "Don't actually deploy Pachyderm Dash to Kubernetes, instead just print the manifest.")
	updateDash.Flags().StringVarP(&updateDashOutputFormat, "output", "o", "json", "Output formmat. One of: json|yaml")
	commands = append(commands, cmdutil.CreateAlias(updateDash, "update-dash"))

	return commands
}

func getDefaultOrLatestDashImage(dashImage string, dryRun bool) string {
	var err error
	version := version.PrettyPrintVersion(version.Version)
	defer func() {
		if err != nil && !dryRun {
			fmt.Printf("No updated dash image found for pachctl %v: %v Falling back to dash image %v\n", version, err, defaultDashImage)
		}
	}()
	if dashImage != "" {
		// It has been supplied explicitly by version on the command line
		return dashImage
	}
	dashImage = defaultDashImage
	compatibleDashVersionsURL := fmt.Sprintf("https://raw.githubusercontent.com/pachyderm/pachyderm/master/etc/compatibility/%v", version)
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

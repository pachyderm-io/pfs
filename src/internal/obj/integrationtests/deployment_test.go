package integrationtests

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kube "k8s.io/client-go/kubernetes"

	"github.com/pachyderm/pachyderm/v2/src/client"
	"github.com/pachyderm/pachyderm/v2/src/internal/deploy/assets"
	"github.com/pachyderm/pachyderm/v2/src/internal/obj"
	"github.com/pachyderm/pachyderm/v2/src/internal/require"
	"github.com/pachyderm/pachyderm/v2/src/internal/serde"
	tu "github.com/pachyderm/pachyderm/v2/src/internal/testutil"
	"github.com/pachyderm/pachyderm/v2/src/pps"
)

// This test suite works by spinning up separate pachd deployments in a new
// namespace for each configuration. There are several important bits to make
// sure these are parallelizable, that the manifests don't step on each other's
// toes. Once the deployment is up-and-running, we run a simple pipeline test to
// ensure that we can round-trip data to object storage in both the worker and
// in pachd. For testing specific corner-cases, consider modifying the client
// test suite in this same package.

// NOTE: these tests require object storage credentials to be loaded in your
// environment (see util.go for where they are loaded).

// Change this to false to keep kubernetes namespaces around after the test for
// debugging purposes.
const cleanup = true

// Rewrites kubernetes manifest services to auto-allocate external ports and
// reduce cpu resource requests for parallel testing.
type ManifestRewriter struct {
	serde.Encoder
}

func rewriterCallback(innerCb func(map[string]interface{}) error) func(map[string]interface{}) error {
	return func(data map[string]interface{}) error {
		var err error
		if innerCb != nil {
			err = innerCb(data)
		}
		rewriteManifest(data)
		return err
	}
}

func rewriteManifest(data map[string]interface{}) {
	if data["kind"] == "Service" {
		spec := data["spec"].(map[string]interface{})
		if spec["type"] == "NodePort" {
			ports := spec["ports"].([]interface{})
			for _, port := range ports {
				port := port.(map[string]interface{})
				if _, ok := port["nodePort"]; ok {
					port["nodePort"] = 0
				}
			}
		}
	}

	if data["kind"] == "Deployment" {
		if spec, ok := data["spec"]; ok {
			spec := spec.(map[string]interface{})
			if template, ok := spec["template"]; ok {
				template := template.(map[string]interface{})
				if spec, ok := template["spec"]; ok {
					spec := spec.(map[string]interface{})
					if containers, ok := spec["containers"]; ok {
						containers := containers.([]interface{})
						for _, container := range containers {
							container := container.(map[string]interface{})
							if resources, ok := container["resources"]; ok {
								resources := resources.(map[string]interface{})
								if limits, ok := resources["limits"]; ok {
									limits := limits.(map[string]interface{})
									if _, ok := limits["cpu"]; ok {
										limits["cpu"] = "0"
									}
								}
								if requests, ok := resources["requests"]; ok {
									requests := requests.(map[string]interface{})
									if _, ok := requests["cpu"]; ok {
										requests["cpu"] = "0"
									}
								}
							}
						}
					}
				}
			}
		}
	}
}

func (npr *ManifestRewriter) Encode(v interface{}) error {
	return npr.EncodeTransform(v, nil)
}

func (npr *ManifestRewriter) EncodeProto(m proto.Message) error {
	return npr.EncodeProtoTransform(m, nil)
}

func (npr *ManifestRewriter) EncodeTransform(v interface{}, cb func(map[string]interface{}) error) error {
	return npr.Encoder.EncodeTransform(v, rewriterCallback(cb))
}

func (npr *ManifestRewriter) EncodeProtoTransform(m proto.Message, cb func(map[string]interface{}) error) error {
	return npr.Encoder.EncodeProtoTransform(m, rewriterCallback(cb))
}

func getPachClient(t *testing.T, kubeClient *kube.Clientset, namespace string) *client.APIClient {
	// Get the pachd service from kubernetes
	pachd, err := kubeClient.CoreV1().Services(namespace).Get("pachd", metav1.GetOptions{})
	require.NoError(t, err)

	var port int32
	for _, servicePort := range pachd.Spec.Ports {
		if servicePort.Name == "api-grpc-port" {
			port = servicePort.NodePort
		}
	}
	require.NotEqual(t, 0, port)

	// Get the IP address of the nodes (any _should_ work for the service port)
	nodes, err := kubeClient.CoreV1().Nodes().List(metav1.ListOptions{})
	require.NoError(t, err)

	// Minikube 'Hostname' address type didn't work when testing, use InternalIP
	var address string
	for _, addr := range nodes.Items[0].Status.Addresses {
		if addr.Type == "InternalIP" {
			address = addr.Address
		}
	}
	require.NotEqual(t, "", address)

	// Connect to pachd
	tu.WaitForPachdReady(t, namespace)
	client, err := client.NewFromURI(fmt.Sprintf("%s:%d", address, port), client.WithDialTimeout(100*time.Second))

	// Some debugging info in case connecting fails - this will dump the pachd
	// logs in case something went wrong there. In my experience, this has been
	// due to either problems with credentials to object storage (will also fail
	// in client_test.go), or insufficient timeout due to slow CI machines.
	if err != nil {
		fmt.Printf("Failed to connect to pachd: %v\n", err)
		fmt.Printf("Used host:port: %s:%d\n", address, port)
		fmt.Printf("All nodes addresses:\n")
		for i, node := range nodes.Items {
			fmt.Printf(" [%d]: %v\n", i, node.Status.Addresses)
		}
		pods, err := kubeClient.CoreV1().Pods(namespace).List(metav1.ListOptions{
			LabelSelector: "app=pachd",
		})
		if err == nil {
			if len(pods.Items) != 1 {
				fmt.Printf("Got wrong number of pods, expected %d but found %d\n", 1, len(pods.Items))
			} else {
				stream, err := kubeClient.CoreV1().Pods(namespace).GetLogs(
					pods.Items[0].ObjectMeta.Name,
					&v1.PodLogOptions{},
				).Stream()
				if err == nil {
					defer stream.Close()
					fmt.Printf("Pod logs:\n")
					io.Copy(os.Stdout, stream)
				} else {
					fmt.Printf("Failed to get pod logs: %v\n", err)
				}
			}
		} else {
			fmt.Printf("Failed to find pachd pod: %v\n", err)
		}
	}
	require.NoError(t, err)
	return client
}

func makeManifest(t *testing.T, backend assets.Backend, secrets map[string][]byte, opts *assets.AssetOpts) string {
	manifest := &strings.Builder{}
	jsonEncoder, err := serde.GetEncoder("json", manifest, serde.WithIndent(2), serde.WithOrigName(true))
	require.NoError(t, err)

	// Create a wrapper encoder that rewrites the manifest so that we don't get
	// collisions across namespaces and can run these tests in parallel.
	encoder := &ManifestRewriter{Encoder: jsonEncoder}

	// Use a separate hostpath on the kubernetes host for each deployment
	hostPath := fmt.Sprintf("/var/pachyderm-%s", opts.Namespace)
	err = assets.WriteAssets(encoder, opts, backend, assets.LocalBackend, 1, hostPath)
	require.NoError(t, err)

	err = assets.WriteSecret(encoder, secrets, opts)
	require.NoError(t, err)

	return manifest.String()
}

func withManifest(t *testing.T, backend assets.Backend, secrets map[string][]byte, callback func(namespace string, pachClient *client.APIClient)) {
	namespaceName := tu.UniqueString("deployment-test-")
	opts := &assets.AssetOpts{
		StorageOpts: assets.StorageOpts{
			UploadConcurrencyLimit:  assets.DefaultUploadConcurrencyLimit,
			PutFileConcurrencyLimit: assets.DefaultPutFileConcurrencyLimit,
		},
		Version:                    "local",
		LogLevel:                   "info",
		Namespace:                  namespaceName,
		RequireCriticalServersOnly: assets.DefaultRequireCriticalServersOnly,
		WorkerServiceAccountName:   assets.DefaultWorkerServiceAccountName,
		LocalRoles:                 true,
		RunAsRoot:                  true,
	}

	manifest := makeManifest(t, backend, secrets, opts)

	kubeClient := tu.GetKubeClient(t)
	namespace := &v1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespaceName}}
	_, err := kubeClient.CoreV1().Namespaces().Create(namespace)
	require.NoError(t, err)

	if cleanup {
		defer func() {
			err := kubeClient.CoreV1().Namespaces().Delete(namespaceName, nil)
			require.NoError(t, err)
		}()
	}

	cmd := tu.Cmd("kubectl", "apply", "--namespace", namespaceName, "-f", "-")
	cmd.Stdin = strings.NewReader(manifest)
	err = cmd.Run()
	require.NoError(t, err)

	// block until pachd deployment is ready - sometimes postgres isn't ready in time and the pachd pod restarts
	require.NoError(t, tu.Cmd("kubectl", "wait", "--namespace", namespaceName, "--for=condition=available", "deployment/pachd", "--timeout", "2m").Run())

	pachClient := getPachClient(t, kubeClient, namespaceName)
	ctx, cf := context.WithCancel(context.Background())
	defer cf()
	pachClient = pachClient.WithCtx(ctx)
	defer pachClient.Close()
	callback(namespaceName, pachClient)
}

func runDeploymentTest(t *testing.T, pachClient *client.APIClient) {
	// Create an input repo
	dataRepo := "data"
	require.NoError(t, pachClient.CreateRepo(dataRepo))

	// Create a pipeline
	pipelineRepo := tu.UniqueString("pipeline")
	_, err := pachClient.PpsAPIClient.CreatePipeline(context.Background(), &pps.CreatePipelineRequest{
		Pipeline: client.NewPipeline(pipelineRepo),
		Transform: &pps.Transform{
			Image: "",
			Cmd:   []string{"bash"},
			Stdin: []string{
				fmt.Sprintf("cp /pfs/%s/* /pfs/out/", dataRepo),
			},
		},
		ParallelismSpec: &pps.ParallelismSpec{
			Constant: 1,
		},
		Input:                 client.NewPFSInput(dataRepo, "/*"),
		OutputBranch:          "",
		Update:                false,
		ResourceRequests:      &pps.ResourceSpec{Cpu: 0.0},
		ResourceLimits:        &pps.ResourceSpec{Cpu: 0.0},
		SidecarResourceLimits: &pps.ResourceSpec{Cpu: 0.0},
	})
	require.NoError(t, err)

	// Upload some files
	commit1, err := pachClient.StartCommit(dataRepo, "master")
	require.NoError(t, err)
	require.NoError(t, pachClient.PutFile(commit1, "file", strings.NewReader("foo")))
	require.NoError(t, pachClient.FinishCommit(dataRepo, commit1.Branch.Name, commit1.ID))

	// Wait for the output commit
	commitInfos, err := pachClient.WaitCommitSetAll(commit1.ID)
	require.NoError(t, err)
	require.Equal(t, 4, len(commitInfos))

	// Check the pipeline output
	var buf bytes.Buffer
	outputCommit := client.NewCommit(pipelineRepo, "master", commit1.ID)
	require.NoError(t, pachClient.GetFile(outputCommit, "file", &buf))
	require.Equal(t, "foo", buf.String())
}

func TestAmazonDeployment(t *testing.T) {
	t.Parallel()
	advancedConfig := &obj.AmazonAdvancedConfiguration{
		Retries:        obj.DefaultRetries,
		Timeout:        obj.DefaultTimeout,
		UploadACL:      obj.DefaultUploadACL,
		PartSize:       obj.DefaultPartSize,
		MaxUploadParts: obj.DefaultMaxUploadParts,
		DisableSSL:     obj.DefaultDisableSSL,
		NoVerifySSL:    obj.DefaultNoVerifySSL,
		LogOptions:     obj.DefaultAwsLogOptions,
	}

	// Test the Amazon client against S3
	t.Run("AmazonObjectStorage", func(t *testing.T) {
		t.Parallel()
		id, secret, bucket, region := LoadAmazonParameters(t)
		secrets := assets.AmazonSecret(region, bucket, id, secret, "", "", "", advancedConfig)
		withManifest(t, assets.AmazonBackend, secrets, func(namespace string, pachClient *client.APIClient) {
			runDeploymentTest(t, pachClient)
		})
	})

	// Test the Amazon client against ECS
	t.Run("ECSObjectStorage", func(t *testing.T) {
		t.Parallel()
		id, secret, bucket, region, endpoint := LoadECSParameters(t)
		secrets := assets.AmazonSecret(region, bucket, id, secret, "", "", endpoint, advancedConfig)
		withManifest(t, assets.AmazonBackend, secrets, func(namespace string, pachClient *client.APIClient) {
			runDeploymentTest(t, pachClient)
		})
	})

	// Test the Amazon client against GCS
	t.Run("GoogleObjectStorage", func(t *testing.T) {
		t.Skip("Amazon client does not work against GCS currently, see client_test.go")
		t.Parallel()
		id, secret, bucket, region, endpoint := LoadGoogleHMACParameters(t)
		secrets := assets.AmazonSecret(region, bucket, id, secret, "", "", endpoint, advancedConfig)
		withManifest(t, assets.AmazonBackend, secrets, func(namespace string, pachClient *client.APIClient) {
			runDeploymentTest(t, pachClient)
		})
	})
}

func TestMinioDeployment(t *testing.T) {
	t.Parallel()
	minioTests := func(t *testing.T, endpoint string, bucket string, id string, secret string) {
		t.Run("S3v2", func(t *testing.T) {
			t.Skip("Minio client running S3v2 does not handle empty writes properly on S3 and ECS") // (this works for GCS), try upgrading to v7?
			t.Parallel()
			secrets := assets.MinioSecret(bucket, id, secret, endpoint, true, true)
			withManifest(t, assets.MinioBackend, secrets, func(namespace string, pachClient *client.APIClient) {
				runDeploymentTest(t, pachClient)
			})
		})

		t.Run("S3v4", func(t *testing.T) {
			t.Parallel()
			secrets := assets.MinioSecret(bucket, id, secret, endpoint, true, false)
			withManifest(t, assets.MinioBackend, secrets, func(namespace string, pachClient *client.APIClient) {
				runDeploymentTest(t, pachClient)
			})
		})
	}

	// Test the Minio client against S3 using the S3v2 and S3v4 APIs
	t.Run("AmazonObjectStorage", func(t *testing.T) {
		t.Parallel()
		id, secret, bucket, region := LoadAmazonParameters(t)
		endpoint := fmt.Sprintf("s3.%s.amazonaws.com", region) // Note that not all AWS regions support both http/https or both S3v2/S3v4
		minioTests(t, endpoint, bucket, id, secret)
	})

	// Test the Minio client against ECS using the S3v2 and S3v4 APIs
	t.Run("ECSObjectStorage", func(t *testing.T) {
		t.Parallel()
		id, secret, bucket, _, endpoint := LoadECSParameters(t)
		minioTests(t, endpoint, bucket, id, secret)
	})

	// Test the Minio client against GCP using the S3v2 and S3v4 APIs
	t.Run("GoogleObjectStorage", func(t *testing.T) {
		t.Parallel()
		id, secret, bucket, _, endpoint := LoadGoogleHMACParameters(t)
		minioTests(t, endpoint, bucket, id, secret)
	})
}

func TestGoogleDeployment(t *testing.T) {
	t.Parallel()
	bucket, creds := LoadGoogleParameters(t)
	secrets := assets.GoogleSecret(bucket, creds)
	withManifest(t, assets.GoogleBackend, secrets, func(namespace string, pachClient *client.APIClient) {
		runDeploymentTest(t, pachClient)
	})
}

func TestMicrosoftDeployment(t *testing.T) {
	t.Parallel()
	id, secret, container := LoadMicrosoftParameters(t)
	secrets := assets.MicrosoftSecret(container, id, secret)
	withManifest(t, assets.MicrosoftBackend, secrets, func(namespace string, pachClient *client.APIClient) {
		runDeploymentTest(t, pachClient)
	})
}

func TestLocalDeployment(t *testing.T) {
	t.Parallel()
	secrets := assets.LocalSecret()
	withManifest(t, assets.LocalBackend, secrets, func(namespace string, pachClient *client.APIClient) {
		runDeploymentTest(t, pachClient)
	})
}

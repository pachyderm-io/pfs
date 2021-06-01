package assets

import (
	"fmt"
	"io/ioutil"
	"path"
	"strconv"
	"strings"

	"github.com/pachyderm/pachyderm/v2/src/client"
	"github.com/pachyderm/pachyderm/v2/src/internal/errors"
	"github.com/pachyderm/pachyderm/v2/src/internal/obj"
	"github.com/pachyderm/pachyderm/v2/src/internal/serde"
	"github.com/pachyderm/pachyderm/v2/src/internal/tls"
	"github.com/pachyderm/pachyderm/v2/src/internal/uuid"
	"github.com/pachyderm/pachyderm/v2/src/server/pps/server/githook"

	apps "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

const (
	// WorkerServiceAccountEnvVar is the name of the environment variable used to tell pachd
	// what service account to assign to new worker RCs, for the purpose of
	// creating S3 gateway services.
	WorkerServiceAccountEnvVar = "WORKER_SERVICE_ACCOUNT"
	// DefaultWorkerServiceAccountName is the default value to use if WorkerServiceAccountEnvVar is
	// undefined (for compatibility purposes)
	DefaultWorkerServiceAccountName = "pachyderm-worker"
	workerRoleName                  = "pachyderm-worker" // Role given to worker Service Account
	workerRoleBindingName           = "pachyderm-worker" // Binding worker role to Service Account
)

var (
	suite = "pachyderm"

	pachdImage     = "pachyderm/pachd"
	grpcProxyImage = "pachyderm/grpc-proxy:0.4.10"
	dashName       = "dash"
	workerImage    = "pachyderm/worker"
	pauseImage     = "gcr.io/google_containers/pause-amd64:3.0"

	// ServiceAccountName is the name of Pachyderm's service account.
	// It's public because it's needed by pps.APIServer to create the RCs for
	// workers.
	ServiceAccountName = "pachyderm"
	grpcProxyName      = "grpc-proxy"
	pachdName          = "pachd"
	// PrometheusPort hosts the prometheus stats for scraping
	PrometheusPort = 656

	enterpriseServerName = "pach-enterprise"

	// Role & binding names, used for Roles or ClusterRoles and their associated
	// bindings.
	roleName        = "pachyderm"
	roleBindingName = "pachyderm"
	// Policy rules to use for Pachyderm's Role or ClusterRole.
	rolePolicyRules = []rbacv1.PolicyRule{{
		APIGroups: []string{""},
		Verbs:     []string{"get", "list", "watch"},
		Resources: []string{"nodes", "pods", "pods/log", "endpoints"},
	}, {
		APIGroups: []string{""},
		Verbs:     []string{"get", "list", "watch", "create", "update", "delete"},
		Resources: []string{"replicationcontrollers", "services"},
	}, {
		APIGroups: []string{""},
		Verbs:     []string{"get", "list", "watch", "create", "update", "delete", "deletecollection"},
		Resources: []string{"secrets"},
	}}

	// The name of the local volume (mounted kubernetes secret) where pachd
	// should read a TLS cert and private key for authenticating with clients
	tlsVolumeName = "pachd-tls-cert"
	// The name of the kubernetes secret mount in the TLS volume (see
	// tlsVolumeName)
	tlsSecretName = "pachd-tls-cert"

	// IAMAnnotation is the annotation used for the IAM role, this can work
	// with something like kube2iam as an alternative way to provide
	// credentials.
	IAMAnnotation = "iam.amazonaws.com/role"

	// OidcPort is the port where OIDC ID Providers can send auth assertions
	OidcPort = int32(657)
)

// Backend is the type used to enumerate what system provides object storage or
// persistent disks for the cluster (each can be configured separately).
type Backend int

const (
	// LocalBackend is used in development (e.g. minikube) which provides a volume on the host machine
	LocalBackend Backend = iota

	// AmazonBackend uses S3 for object storage
	AmazonBackend

	// GoogleBackend uses GCS for object storage
	GoogleBackend

	// MicrosoftBackend uses Azure blobs for object storage
	MicrosoftBackend

	// MinioBackend uses the Minio client for object storage, but it can point to any S3-compatible API
	MinioBackend

	// S3CustomArgs uses the S3 or Minio clients for object storage with custom endpoint configuration
	S3CustomArgs = 6
)

// TLSOpts indicates the cert and key file that Pachd should use to
// authenticate with clients
type TLSOpts struct {
	ServerCert string
	ServerKey  string
}

// FeatureFlags are flags for experimental features.
type FeatureFlags struct{}

const (
	// UploadConcurrencyLimitEnvVar is the environment variable for the upload concurrency limit.
	UploadConcurrencyLimitEnvVar = "STORAGE_UPLOAD_CONCURRENCY_LIMIT"

	// PutFileConcurrencyLimitEnvVar is the environment variable for the PutFile concurrency limit.
	PutFileConcurrencyLimitEnvVar = "STORAGE_PUT_FILE_CONCURRENCY_LIMIT"
)

const (
	// DefaultUploadConcurrencyLimit is the default maximum number of concurrent object storage uploads.
	// (bryce) this default is set here and in the service env config, need to figure out how to refactor
	// this to be in one place.
	DefaultUploadConcurrencyLimit = 100

	// DefaultPutFileConcurrencyLimit is the default maximum number of concurrent files that can be uploaded over GRPC or downloaded from external sources (ex. HTTP or blob storage).
	DefaultPutFileConcurrencyLimit = 100
)

// StorageOpts are options that are applicable to the storage layer.
type StorageOpts struct {
	UploadConcurrencyLimit  int
	PutFileConcurrencyLimit int
}

const (
	// RequireCriticalServersOnlyEnvVar is the environment variable for requiring critical servers only.
	RequireCriticalServersOnlyEnvVar = "REQUIRE_CRITICAL_SERVERS_ONLY"
)

const (
	// DefaultRequireCriticalServersOnly is the default for requiring critical servers only.
	// (bryce) this default is set here and in the service env config, need to figure out how to refactor
	// this to be in one place.
	DefaultRequireCriticalServersOnly = false
)

// AssetOpts are options that are applicable to all the asset types.
type AssetOpts struct {
	FeatureFlags
	EtcdOpts
	PostgresOpts
	StorageOpts
	Version    string
	LogLevel   string
	Metrics    bool
	Dynamic    bool
	DashOnly   bool
	NoDash     bool
	DashImage  string
	Registry   string
	EtcdPrefix string
	PachdPort  int32
	TracePort  int32
	HTTPPort   int32
	PeerPort   int32

	// NoGuaranteed will not generate assets that have both resource limits and
	// resource requests set which causes kubernetes to give the pods
	// guaranteed QoS. Guaranteed QoS generally leads to more stable clusters
	// but on smaller test clusters such as those run on minikube it doesn't
	// help much and may cause more instability than it prevents.
	NoGuaranteed bool

	// PachdCPURequest is the amount of CPU we request for each pachd node. If
	// empty, assets.go will choose a default size.
	PachdCPURequest string

	// PachdNonCacheMemRequest is the amount of memory we request for each
	// pachd node. If empty, assets.go will choose a default size.
	PachdNonCacheMemRequest string

	// IAM role that the Pachyderm deployment should assume when talking to AWS
	// services (if using kube2iam + metadata service + IAM role to delegate
	// permissions to pachd via its instance).
	// This is in AssetOpts rather than AmazonCreds because it must be passed
	// as an annotation on the pachd pod rather than as a k8s secret
	IAMRole string

	// ImagePullSecret specifies an image pull secret that gets attached to the
	// various deployments so that their images can be pulled from a private
	// registry.
	ImagePullSecret string

	// NoRBAC, if true, will disable creation of RBAC assets.
	NoRBAC bool

	// LocalRoles, if true, uses Role and RoleBinding instead of ClusterRole and
	// ClusterRoleBinding.
	LocalRoles bool

	// Namespace is the kubernetes namespace to deploy to.
	Namespace string

	// NoExposeDockerSocket if true prevents pipelines from accessing the docker socket.
	NoExposeDockerSocket bool

	// If set, the files indictated by 'TLS.ServerCert' and 'TLS.ServerKey' are
	// placed into a Kubernetes secret and used by pachd nodes to authenticate
	// during TLS
	TLS *TLSOpts

	// Sets the cluster deployment ID. If unset, this will be a randomly
	// generated UUID without dashes.
	ClusterDeploymentID string

	// RequireCriticalServersOnly is true when only the critical Pachd servers
	// are required to startup and run without error.
	RequireCriticalServersOnly bool

	// WorkerServiceAccountName is the name of the service account that will be
	// used in the worker pods for creating S3 gateways.
	WorkerServiceAccountName string

	// EnterpriseServer deploys the enterprise server when set.
	EnterpriseServer bool
}

// replicas lets us create a pointer to a non-zero int32 in-line. This is
// helpful because the Replicas field of many specs expectes an *int32
func replicas(r int32) *int32 {
	return &r
}

// fillDefaultResourceRequests sets any of:
//   opts.PachdNonCacheMemRequest
//   opts.PachdCPURequest
//   opts.EtcdCPURequest
//   opts.EtcdMemRequest
// that are unset in 'opts' to the appropriate default ('persistentDiskBackend'
// just used to determine if this is a local deployment, and if so, make the
// resource requests smaller)
func fillDefaultResourceRequests(opts *AssetOpts, persistentDiskBackend Backend) {
	if persistentDiskBackend == LocalBackend {
		// For local deployments, we set the resource requirements and cache sizes
		// low so that pachyderm clusters will fit inside e.g. minikube or travis
		if opts.PachdNonCacheMemRequest == "" {
			opts.PachdNonCacheMemRequest = "512M"
		}
		if opts.PachdCPURequest == "" {
			opts.PachdCPURequest = "0.25"
		}

		if opts.EtcdOpts.MemRequest == "" {
			opts.EtcdOpts.MemRequest = "512M"
		}
		if opts.EtcdOpts.CPURequest == "" {
			opts.EtcdOpts.CPURequest = "0.25"
		}

		if opts.PostgresOpts.MemRequest == "" {
			opts.PostgresOpts.MemRequest = "256M"
		}
		if opts.PostgresOpts.CPURequest == "" {
			opts.PostgresOpts.CPURequest = "0.25"
		}
	} else {
		// For non-local deployments, we set the resource requirements and cache
		// sizes higher, so that the cluster is stable and performant
		if opts.PachdNonCacheMemRequest == "" {
			opts.PachdNonCacheMemRequest = "2G"
		}
		if opts.PachdCPURequest == "" {
			opts.PachdCPURequest = "1"
		}

		if opts.EtcdOpts.MemRequest == "" {
			opts.EtcdOpts.MemRequest = "2G"
		}
		if opts.EtcdOpts.CPURequest == "" {
			opts.EtcdOpts.CPURequest = "1"
		}

		if opts.PostgresOpts.MemRequest == "" {
			opts.PostgresOpts.MemRequest = "2G"
		}
		if opts.PostgresOpts.CPURequest == "" {
			opts.PostgresOpts.CPURequest = "1"
		}
	}
}

// ServiceAccounts returns a kubernetes service account for use with Pachyderm.
func ServiceAccounts(opts *AssetOpts) []*v1.ServiceAccount {
	return []*v1.ServiceAccount{
		// Pachd service account
		{
			TypeMeta: metav1.TypeMeta{
				Kind:       "ServiceAccount",
				APIVersion: "v1",
			},
			ObjectMeta: objectMeta(ServiceAccountName, labels(""), nil, opts.Namespace),
		},

		// worker service account
		{
			TypeMeta: metav1.TypeMeta{
				Kind:       "ServiceAccount",
				APIVersion: "v1",
			},
			ObjectMeta: objectMeta(opts.WorkerServiceAccountName, labels(""), nil, opts.Namespace),
		},
	}
}

// ClusterRole returns a ClusterRole that should be bound to the Pachyderm service account.
func ClusterRole(opts *AssetOpts) *rbacv1.ClusterRole {
	return &rbacv1.ClusterRole{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ClusterRole",
			APIVersion: "rbac.authorization.k8s.io/v1",
		},
		ObjectMeta: objectMeta(roleName, labels(""), nil, opts.Namespace),
		Rules:      rolePolicyRules,
	}
}

// ClusterRoleBinding returns a ClusterRoleBinding that binds Pachyderm's
// ClusterRole to its ServiceAccount.
func ClusterRoleBinding(opts *AssetOpts) *rbacv1.ClusterRoleBinding {
	return &rbacv1.ClusterRoleBinding{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ClusterRoleBinding",
			APIVersion: "rbac.authorization.k8s.io/v1",
		},
		ObjectMeta: objectMeta(roleBindingName, labels(""), nil, opts.Namespace),
		Subjects: []rbacv1.Subject{{
			Kind:      "ServiceAccount",
			Name:      ServiceAccountName,
			Namespace: opts.Namespace,
		}},
		RoleRef: rbacv1.RoleRef{
			Kind: "ClusterRole",
			Name: roleName,
		},
	}
}

// Role returns a Role that should be bound to the Pachyderm service account.
func Role(opts *AssetOpts) *rbacv1.Role {
	return &rbacv1.Role{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Role",
			APIVersion: "rbac.authorization.k8s.io/v1",
		},
		ObjectMeta: objectMeta(roleName, labels(""), nil, opts.Namespace),
		Rules:      rolePolicyRules,
	}
}

// workerRole returns a Role bound to the Pachyderm worker service account
// (used by workers to create an s3 gateway k8s service for each job)
func workerRole(opts *AssetOpts) *rbacv1.Role {
	return &rbacv1.Role{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Role",
			APIVersion: "rbac.authorization.k8s.io/v1",
		},
		ObjectMeta: objectMeta(workerRoleName, labels(""), nil, opts.Namespace),
		Rules: []rbacv1.PolicyRule{{
			APIGroups: []string{""},
			Verbs:     []string{"get", "list", "update", "create", "delete"},
			Resources: []string{"services"},
		}},
	}
}

// RoleBinding returns a RoleBinding that binds Pachyderm's Role to its
// ServiceAccount.
func RoleBinding(opts *AssetOpts) *rbacv1.RoleBinding {
	return &rbacv1.RoleBinding{
		TypeMeta: metav1.TypeMeta{
			Kind:       "RoleBinding",
			APIVersion: "rbac.authorization.k8s.io/v1",
		},
		ObjectMeta: objectMeta(roleBindingName, labels(""), nil, opts.Namespace),
		Subjects: []rbacv1.Subject{{
			Kind:      "ServiceAccount",
			Name:      ServiceAccountName,
			Namespace: opts.Namespace,
		}},
		RoleRef: rbacv1.RoleRef{
			Kind: "Role",
			Name: roleName,
		},
	}
}

// RoleBinding returns a RoleBinding that binds Pachyderm's workerRole to its
// worker service account (assets.WorkerServiceAccountName)
func workerRoleBinding(opts *AssetOpts) *rbacv1.RoleBinding {
	return &rbacv1.RoleBinding{
		TypeMeta: metav1.TypeMeta{
			Kind:       "RoleBinding",
			APIVersion: "rbac.authorization.k8s.io/v1",
		},
		ObjectMeta: objectMeta(workerRoleBindingName, labels(""), nil, opts.Namespace),
		Subjects: []rbacv1.Subject{{
			Kind:      "ServiceAccount",
			Name:      opts.WorkerServiceAccountName,
			Namespace: opts.Namespace,
		}},
		RoleRef: rbacv1.RoleRef{
			Kind: "Role",
			Name: workerRoleName,
		},
	}
}

// GetBackendSecretVolumeAndMount returns a properly configured Volume and
// VolumeMount object given a backend.  The backend needs to be one of the
// constants defined in pfs/server.
func GetBackendSecretVolumeAndMount(backend string) (v1.Volume, v1.VolumeMount) {
	return v1.Volume{
			Name: client.StorageSecretName,
			VolumeSource: v1.VolumeSource{
				Secret: &v1.SecretVolumeSource{
					SecretName: client.StorageSecretName,
				},
			},
		}, v1.VolumeMount{
			Name:      client.StorageSecretName,
			MountPath: "/" + client.StorageSecretName,
		}
}

// GetSecretEnvVars returns the environment variable specs for the storage secret.
func GetSecretEnvVars(storageBackend string) []v1.EnvVar {
	var envVars []v1.EnvVar
	if storageBackend != "" {
		envVars = append(envVars, v1.EnvVar{
			Name:  obj.StorageBackendEnvVar,
			Value: storageBackend,
		})
	}
	trueVal := true
	for _, e := range obj.EnvVarToSecretKey {
		envVars = append(envVars, v1.EnvVar{
			Name: e.Key,
			ValueFrom: &v1.EnvVarSource{
				SecretKeyRef: &v1.SecretKeySelector{
					LocalObjectReference: v1.LocalObjectReference{
						Name: client.StorageSecretName,
					},
					Key:      e.Value,
					Optional: &trueVal,
				},
			},
		})
	}
	return envVars
}

func getStorageEnvVars(opts *AssetOpts) []v1.EnvVar {
	return []v1.EnvVar{
		{Name: UploadConcurrencyLimitEnvVar, Value: strconv.Itoa(opts.StorageOpts.UploadConcurrencyLimit)},
		{Name: PutFileConcurrencyLimitEnvVar, Value: strconv.Itoa(opts.StorageOpts.PutFileConcurrencyLimit)},
	}
}

func versionedPachdImage(opts *AssetOpts) string {
	if opts.Version != "" {
		return fmt.Sprintf("%s:%s", pachdImage, opts.Version)
	}
	return pachdImage
}

func versionedWorkerImage(opts *AssetOpts) string {
	if opts.Version != "" {
		return fmt.Sprintf("%s:%s", workerImage, opts.Version)
	}
	return workerImage
}

func imagePullSecrets(opts *AssetOpts) []v1.LocalObjectReference {
	var result []v1.LocalObjectReference
	if opts.ImagePullSecret != "" {
		result = append(result, v1.LocalObjectReference{Name: opts.ImagePullSecret})
	}
	return result
}

// PachdDeployment returns a pachd k8s Deployment.
func PachdDeployment(opts *AssetOpts, objectStoreBackend Backend, hostPath string) *apps.Deployment {
	// set port defaults
	if opts.PachdPort == 0 {
		opts.PachdPort = 650
	}
	if opts.TracePort == 0 {
		opts.TracePort = 651
	}
	if opts.HTTPPort == 0 {
		opts.HTTPPort = 652
	}
	if opts.PeerPort == 0 {
		opts.PeerPort = 653
	}
	mem := resource.MustParse(opts.PachdNonCacheMemRequest)
	cpu := resource.MustParse(opts.PachdCPURequest)
	image := AddRegistry(opts.Registry, versionedPachdImage(opts))
	volumes := []v1.Volume{
		{
			Name: "pach-disk",
		},
	}
	volumeMounts := []v1.VolumeMount{
		{
			Name:      "pach-disk",
			MountPath: "/pach",
		},
	}

	// Set up storage options
	var backendEnvVar string
	var storageHostPath string
	switch objectStoreBackend {
	case LocalBackend:
		storageHostPath = path.Join(hostPath, "pachd")
		pathType := v1.HostPathDirectoryOrCreate
		volumes[0].HostPath = &v1.HostPathVolumeSource{
			Path: storageHostPath,
			Type: &pathType,
		}
		backendEnvVar = obj.Local
	case MinioBackend:
		backendEnvVar = obj.Minio
	case AmazonBackend:
		backendEnvVar = obj.Amazon
	case GoogleBackend:
		backendEnvVar = obj.Google
	case MicrosoftBackend:
		backendEnvVar = obj.Microsoft
	}
	volume, mount := GetBackendSecretVolumeAndMount(backendEnvVar)
	volumes = append(volumes, volume)
	volumeMounts = append(volumeMounts, mount)
	if opts.TLS != nil {
		volumes = append(volumes, v1.Volume{
			Name: tlsVolumeName,
			VolumeSource: v1.VolumeSource{
				Secret: &v1.SecretVolumeSource{
					SecretName: tlsSecretName,
				},
			},
		})
		volumeMounts = append(volumeMounts, v1.VolumeMount{
			Name:      tlsVolumeName,
			MountPath: tls.VolumePath,
		})
	}
	resourceRequirements := v1.ResourceRequirements{
		Requests: v1.ResourceList{
			v1.ResourceCPU:    cpu,
			v1.ResourceMemory: mem,
		},
	}
	if !opts.NoGuaranteed {
		resourceRequirements.Limits = v1.ResourceList{
			v1.ResourceCPU:    cpu,
			v1.ResourceMemory: mem,
		}
	}
	if opts.ClusterDeploymentID == "" {
		opts.ClusterDeploymentID = uuid.NewWithoutDashes()
	}
	envVars := []v1.EnvVar{
		{Name: "PACH_ROOT", Value: "/pach"},
		{Name: "ETCD_PREFIX", Value: opts.EtcdPrefix},
		{Name: "STORAGE_BACKEND", Value: backendEnvVar},
		{Name: "STORAGE_HOST_PATH", Value: storageHostPath},
		{Name: "WORKER_IMAGE", Value: AddRegistry(opts.Registry, versionedWorkerImage(opts))},
		{Name: "IMAGE_PULL_SECRET", Value: opts.ImagePullSecret},
		{Name: "WORKER_SIDECAR_IMAGE", Value: image},
		{Name: "WORKER_IMAGE_PULL_POLICY", Value: "IfNotPresent"},
		{Name: WorkerServiceAccountEnvVar, Value: opts.WorkerServiceAccountName},
		{Name: "PACHD_VERSION", Value: opts.Version},
		{Name: "METRICS", Value: strconv.FormatBool(opts.Metrics)},
		{Name: "LOG_LEVEL", Value: opts.LogLevel},
		{Name: "IAM_ROLE", Value: opts.IAMRole},
		{Name: "NO_EXPOSE_DOCKER_SOCKET", Value: strconv.FormatBool(opts.NoExposeDockerSocket)},
		{
			Name: "PACH_NAMESPACE",
			ValueFrom: &v1.EnvVarSource{
				FieldRef: &v1.ObjectFieldSelector{
					APIVersion: "v1",
					FieldPath:  "metadata.namespace",
				},
			},
		},
		{
			Name: "PACHD_MEMORY_REQUEST",
			ValueFrom: &v1.EnvVarSource{
				ResourceFieldRef: &v1.ResourceFieldSelector{
					ContainerName: "pachd",
					Resource:      "requests.memory",
				},
			},
		},
		{Name: "CLUSTER_DEPLOYMENT_ID", Value: opts.ClusterDeploymentID},
		{Name: RequireCriticalServersOnlyEnvVar, Value: strconv.FormatBool(opts.RequireCriticalServersOnly)},
		{
			Name: "PACHD_POD_NAME",
			ValueFrom: &v1.EnvVarSource{
				FieldRef: &v1.ObjectFieldSelector{
					APIVersion: "v1",
					FieldPath:  "metadata.name",
				},
			},
		},
		// TODO: Setting the default explicitly to ensure the environment variable is set since we pull directly
		// from it for setting up the worker client. Probably should not be pulling directly from environment variables.
		{
			Name:  client.PPSWorkerPortEnv,
			Value: "80",
		},
		{
			Name:  "POSTGRES_SERVICE_HOST",
			Value: "pg-bouncer." + opts.Namespace,
		},
		{
			Name:  "POSTGRES_DATABASE",
			Value: PostgresDBName,
		},
		{
			Name:  "POSTGRES_USER",
			Value: PostgresUser,
		},
	}
	envVars = append(envVars, GetSecretEnvVars("")...)
	envVars = append(envVars, getStorageEnvVars(opts)...)

	name := pachdName
	command := []string{"/pachd"}
	if opts.EnterpriseServer {
		name = enterpriseServerName
		command = append(command, "--mode=enterprise")
	}

	return &apps.Deployment{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Deployment",
			APIVersion: "apps/v1",
		},
		ObjectMeta: objectMeta(name, labels(name), nil, opts.Namespace),
		Spec: apps.DeploymentSpec{
			Replicas: replicas(1),
			Selector: &metav1.LabelSelector{
				MatchLabels: labels(name),
			},
			Template: v1.PodTemplateSpec{
				ObjectMeta: objectMeta(name, labels(name),
					map[string]string{IAMAnnotation: opts.IAMRole}, opts.Namespace),
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name:    pachdName,
							Image:   image,
							Command: command,
							Env:     envVars,
							Ports: []v1.ContainerPort{
								{
									ContainerPort: opts.PachdPort, // also set in cmd/pachd/main.go
									Protocol:      "TCP",
									Name:          "api-grpc-port",
								},
								{
									ContainerPort: opts.TracePort, // also set in cmd/pachd/main.go
									Name:          "trace-port",
								},
								{
									ContainerPort: opts.HTTPPort, // also set in cmd/pachd/main.go
									Protocol:      "TCP",
									Name:          "api-http-port",
								},
								{
									ContainerPort: opts.PeerPort, // also set in cmd/pachd/main.go
									Protocol:      "TCP",
									Name:          "peer-port",
								},
								{
									ContainerPort: githook.GitHookPort,
									Protocol:      "TCP",
									Name:          "api-git-port",
								},
								{
									ContainerPort: OidcPort,
									Protocol:      "TCP",
									Name:          "oidc-port",
								},
							},
							VolumeMounts:    volumeMounts,
							ImagePullPolicy: "IfNotPresent",
							Resources:       resourceRequirements,
							ReadinessProbe: &v1.Probe{
								Handler: v1.Handler{
									Exec: &v1.ExecAction{
										Command: []string{"/pachd", "--readiness"},
									},
								},
							},
						},
					},
					ServiceAccountName: ServiceAccountName,
					Volumes:            volumes,
					ImagePullSecrets:   imagePullSecrets(opts),
				},
			},
		},
	}
}

// PachdService returns a pachd service.
func PachdService(opts *AssetOpts) *v1.Service {
	prometheusAnnotations := map[string]string{
		"prometheus.io/scrape": "true",
		"prometheus.io/port":   strconv.Itoa(PrometheusPort),
	}
	return &v1.Service{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Service",
			APIVersion: "v1",
		},
		ObjectMeta: objectMeta(pachdName, labels(pachdName), prometheusAnnotations, opts.Namespace),
		Spec: v1.ServiceSpec{
			Type: v1.ServiceTypeNodePort,
			Selector: map[string]string{
				"app": pachdName,
			},
			Ports: []v1.ServicePort{
				// NOTE: do not put any new ports before `api-grpc-port`, as
				// it'll change k8s SERVICE_PORT env var values
				{
					Port:     650, // also set in cmd/pachd/main.go
					Name:     "api-grpc-port",
					NodePort: 30650,
				},
				{
					Port:     651, // also set in cmd/pachd/main.go
					Name:     "trace-port",
					NodePort: 30651,
				},
				{
					Port:     652, // also set in cmd/pachd/main.go
					Name:     "api-http-port",
					NodePort: 30652,
				},
				{
					Port:     OidcPort,
					Name:     "oidc-port",
					NodePort: 30000 + OidcPort,
				},
				{
					Port:     658,
					Name:     "identity-port",
					NodePort: 30658,
				},
				{
					Port:     githook.GitHookPort,
					Name:     "api-git-port",
					NodePort: githook.NodePort(),
				},
				{
					Port:     600, // also set in cmd/pachd/main.go
					Name:     "s3gateway-port",
					NodePort: 30600,
				},
				{
					Port:       656,
					Name:       "prometheus-metrics",
					NodePort:   30656,
					Protocol:   v1.ProtocolTCP,
					TargetPort: intstr.FromInt(656),
				},
			},
		},
	}
}

// PachdPeerService returns an internal pachd service. This service will
// reference the PeerPorr, which does not employ TLS even if cluster TLS is
// enabled. Because of this, the service is a `ClusterIP` type (i.e. not
// exposed outside of the cluster.)
func PachdPeerService(opts *AssetOpts) *v1.Service {
	return &v1.Service{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Service",
			APIVersion: "v1",
		},
		ObjectMeta: objectMeta(fmt.Sprintf("%s-peer", pachdName), labels(pachdName), map[string]string{}, opts.Namespace),
		Spec: v1.ServiceSpec{
			Type: v1.ServiceTypeClusterIP,
			Selector: map[string]string{
				"app": pachdName,
			},
			Ports: []v1.ServicePort{
				{
					Port:       30653,
					Name:       "api-grpc-peer-port",
					TargetPort: intstr.FromInt(653), // also set in cmd/pachd/main.go
				},
			},
		},
	}
}

// GithookService returns a k8s service that exposes a public IP
func GithookService(namespace string) *v1.Service {
	name := "githook"
	return &v1.Service{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Service",
			APIVersion: "v1",
		},
		ObjectMeta: objectMeta(name, labels(name), nil, namespace),
		Spec: v1.ServiceSpec{
			Type: v1.ServiceTypeLoadBalancer,
			Selector: map[string]string{
				"app": pachdName,
			},
			Ports: []v1.ServicePort{
				{
					TargetPort: intstr.FromInt(githook.GitHookPort),
					Name:       "api-git-port",
					Port:       githook.ExternalPort(),
				},
			},
		},
	}
}

// DashDeployment creates a Deployment for the pachyderm dashboard.
func DashDeployment(opts *AssetOpts) *apps.Deployment {
	return &apps.Deployment{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Deployment",
			APIVersion: "apps/v1",
		},
		ObjectMeta: objectMeta(dashName, labels(dashName), nil, opts.Namespace),
		Spec: apps.DeploymentSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: labels(dashName),
			},
			Template: v1.PodTemplateSpec{
				ObjectMeta: objectMeta(dashName, labels(dashName), nil, opts.Namespace),
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name:  dashName,
							Image: AddRegistry(opts.Registry, opts.DashImage),
							Ports: []v1.ContainerPort{
								{
									ContainerPort: 8080,
									Name:          "dash-http",
								},
							},
							ImagePullPolicy: "IfNotPresent",
						},
						{
							Name:  grpcProxyName,
							Image: AddRegistry(opts.Registry, grpcProxyImage),
							Ports: []v1.ContainerPort{
								{
									ContainerPort: 8081,
									Name:          "grpc-proxy-http",
								},
							},
							ImagePullPolicy: "IfNotPresent",
						},
					},
					ImagePullSecrets: imagePullSecrets(opts),
				},
			},
		},
	}
}

// DashService creates a Service for the pachyderm dashboard.
func DashService(opts *AssetOpts) *v1.Service {
	return &v1.Service{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Service",
			APIVersion: "v1",
		},
		ObjectMeta: objectMeta(dashName, labels(dashName), nil, opts.Namespace),
		Spec: v1.ServiceSpec{
			Type:     v1.ServiceTypeNodePort,
			Selector: labels(dashName),
			Ports: []v1.ServicePort{
				{
					Port:     8080,
					Name:     "dash-http",
					NodePort: 30080,
				},
				{
					Port:     8081,
					Name:     "grpc-proxy-http",
					NodePort: 30081,
				},
			},
		},
	}
}

// MinioSecret creates an amazon secret with the following parameters:
//   bucket - S3 bucket name
//   id     - S3 access key id
//   secret - S3 secret access key
//   endpoint  - S3 compatible endpoint
//   secure - set to true for a secure connection.
//   isS3V2 - Set to true if client follows S3V2
func MinioSecret(bucket string, id string, secret string, endpoint string, secure, isS3V2 bool) map[string][]byte {
	secureV := "0"
	if secure {
		secureV = "1"
	}
	s3V2 := "0"
	if isS3V2 {
		s3V2 = "1"
	}
	return map[string][]byte{
		"minio-bucket":    []byte(bucket),
		"minio-id":        []byte(id),
		"minio-secret":    []byte(secret),
		"minio-endpoint":  []byte(endpoint),
		"minio-secure":    []byte(secureV),
		"minio-signature": []byte(s3V2),
	}
}

// WriteSecret writes a JSON-encoded k8s secret to the given writer.
// The secret uses the given map as data.
func WriteSecret(encoder serde.Encoder, data map[string][]byte, opts *AssetOpts) error {
	if opts.DashOnly {
		return nil
	}
	secret := &v1.Secret{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Secret",
			APIVersion: "v1",
		},
		ObjectMeta: objectMeta(client.StorageSecretName, labels(client.StorageSecretName), nil, opts.Namespace),
		Data:       data,
	}
	return encoder.Encode(secret)
}

// LocalSecret creates an empty secret.
func LocalSecret() map[string][]byte {
	return nil
}

// AmazonSecret creates an amazon secret with the following parameters:
//   region         - AWS region
//   bucket         - S3 bucket name
//   id             - AWS access key id
//   secret         - AWS secret access key
//   token          - AWS access token
//   distribution   - cloudfront distribution
//   endpoint       - Custom endpoint (generally used for S3 compatible object stores)
//   advancedConfig - advanced configuration
func AmazonSecret(region, bucket, id, secret, token, distribution, endpoint string, advancedConfig *obj.AmazonAdvancedConfiguration) map[string][]byte {
	s := amazonBasicSecret(region, bucket, distribution, advancedConfig)
	s["amazon-id"] = []byte(id)
	s["amazon-secret"] = []byte(secret)
	s["amazon-token"] = []byte(token)
	s["custom-endpoint"] = []byte(endpoint)
	return s
}

// AmazonVaultSecret creates an amazon secret with the following parameters:
//   region         - AWS region
//   bucket         - S3 bucket name
//   vaultAddress   - address/hostport of vault
//   vaultRole      - pachd's role in vault
//   vaultToken     - pachd's vault token
//   distribution   - cloudfront distribution
//   advancedConfig - advanced configuration
func AmazonVaultSecret(region, bucket, vaultAddress, vaultRole, vaultToken, distribution string, advancedConfig *obj.AmazonAdvancedConfiguration) map[string][]byte {
	s := amazonBasicSecret(region, bucket, distribution, advancedConfig)
	s["amazon-vault-addr"] = []byte(vaultAddress)
	s["amazon-vault-role"] = []byte(vaultRole)
	s["amazon-vault-token"] = []byte(vaultToken)
	return s
}

// AmazonIAMRoleSecret creates an amazon secret with the following parameters:
//   region         - AWS region
//   bucket         - S3 bucket name
//   distribution   - cloudfront distribution
//   advancedConfig - advanced configuration
func AmazonIAMRoleSecret(region, bucket, distribution string, advancedConfig *obj.AmazonAdvancedConfiguration) map[string][]byte {
	return amazonBasicSecret(region, bucket, distribution, advancedConfig)
}

func amazonBasicSecret(region, bucket, distribution string, advancedConfig *obj.AmazonAdvancedConfiguration) map[string][]byte {
	return map[string][]byte{
		"amazon-region":       []byte(region),
		"amazon-bucket":       []byte(bucket),
		"amazon-distribution": []byte(distribution),
		"retries":             []byte(strconv.Itoa(advancedConfig.Retries)),
		"timeout":             []byte(advancedConfig.Timeout),
		"upload-acl":          []byte(advancedConfig.UploadACL),
		"part-size":           []byte(strconv.FormatInt(advancedConfig.PartSize, 10)),
		"max-upload-parts":    []byte(strconv.Itoa(advancedConfig.MaxUploadParts)),
		"disable-ssl":         []byte(strconv.FormatBool(advancedConfig.DisableSSL)),
		"no-verify-ssl":       []byte(strconv.FormatBool(advancedConfig.NoVerifySSL)),
		"log-options":         []byte(advancedConfig.LogOptions),
	}
}

// GoogleSecret creates a google secret with a bucket name.
func GoogleSecret(bucket string, cred string) map[string][]byte {
	return map[string][]byte{
		"google-bucket": []byte(bucket),
		"google-cred":   []byte(cred),
	}
}

// MicrosoftSecret creates a microsoft secret with following parameters:
//   container - Azure blob container
//   id    	   - Azure storage account name
//   secret    - Azure storage account key
func MicrosoftSecret(container string, id string, secret string) map[string][]byte {
	return map[string][]byte{
		"microsoft-container": []byte(container),
		"microsoft-id":        []byte(id),
		"microsoft-secret":    []byte(secret),
	}
}

// WriteDashboardAssets writes the k8s config for deploying the Pachyderm
// dashboard to 'encoder'
func WriteDashboardAssets(encoder serde.Encoder, opts *AssetOpts) error {
	if err := encoder.Encode(DashService(opts)); err != nil {
		return err
	}
	return encoder.Encode(DashDeployment(opts))
}

// WriteAssets writes the assets to encoder.
func WriteAssets(encoder serde.Encoder, opts *AssetOpts, objectStoreBackend Backend,
	persistentDiskBackend Backend, volumeSize int,
	hostPath string) error {
	fillDefaultResourceRequests(opts, persistentDiskBackend)
	if opts.DashOnly {
		if dashErr := WriteDashboardAssets(encoder, opts); dashErr != nil {
			return dashErr
		}
		return nil
	}

	for _, sa := range ServiceAccounts(opts) {
		if err := encoder.Encode(sa); err != nil {
			return err
		}
	}
	// Don't apply the cluster role binding for the enterprise server,
	// because it doesn't interact with the k8s API.
	if !opts.NoRBAC && !opts.EnterpriseServer {
		if opts.LocalRoles {
			if err := encoder.Encode(Role(opts)); err != nil {
				return err
			}
			if err := encoder.Encode(RoleBinding(opts)); err != nil {
				return err
			}
		} else {
			if err := encoder.Encode(ClusterRole(opts)); err != nil {
				return err
			}
			if err := encoder.Encode(ClusterRoleBinding(opts)); err != nil {
				return err
			}
		}
		if err := encoder.Encode(workerRole(opts)); err != nil {
			return err
		}
		if err := encoder.Encode(workerRoleBinding(opts)); err != nil {
			return err
		}
	}

	if err := WriteEtcdAssets(encoder, opts, objectStoreBackend, persistentDiskBackend, volumeSize, hostPath); err != nil {
		return err
	}

	if err := WritePostgresAssets(encoder, opts, objectStoreBackend, persistentDiskBackend, volumeSize, hostPath); err != nil {
		return err
	}

	if err := encoder.Encode(PGBouncerService(opts)); err != nil {
		return err
	}
	if err := encoder.Encode(PGBouncerDeployment(opts)); err != nil {
		return err
	}

	// If we're deploying the enterprise server, use a different service definition with the correct ports.
	if opts.EnterpriseServer {
		if err := encoder.Encode(EnterpriseService(opts)); err != nil {
			return err
		}
	} else {
		if err := encoder.Encode(PachdService(opts)); err != nil {
			return err
		}
	}
	if err := encoder.Encode(PachdPeerService(opts)); err != nil {
		return err
	}
	if err := encoder.Encode(PachdDeployment(opts, objectStoreBackend, hostPath)); err != nil {
		return err
	}
	if !opts.NoDash {
		if err := WriteDashboardAssets(encoder, opts); err != nil {
			return err
		}
	}
	if opts.TLS != nil {
		if err := WriteTLSSecret(encoder, opts); err != nil {
			return err
		}
	}
	return nil
}

// WriteTLSSecret creates a new TLS secret in the kubernetes manifest
// (equivalent to one generate by 'kubectl create secret tls'). This will be
// mounted by the pachd pod and used as its TLS public certificate and private
// key
func WriteTLSSecret(encoder serde.Encoder, opts *AssetOpts) error {
	// Validate arguments
	if opts.DashOnly {
		return nil
	}
	if opts.TLS == nil {
		return errors.Errorf("internal error: WriteTLSSecret called but opts.TLS is nil")
	}
	if opts.TLS.ServerKey == "" {
		return errors.Errorf("internal error: WriteTLSSecret called but opts.TLS.ServerKey is \"\"")
	}
	if opts.TLS.ServerCert == "" {
		return errors.Errorf("internal error: WriteTLSSecret called but opts.TLS.ServerCert is \"\"")
	}

	// Attempt to copy server cert and key files into config (kubernetes client
	// does the base64-encoding)
	certBytes, err := ioutil.ReadFile(opts.TLS.ServerCert)
	if err != nil {
		return errors.Wrapf(err, "could not open server cert at \"%s\"", opts.TLS.ServerCert)
	}
	keyBytes, err := ioutil.ReadFile(opts.TLS.ServerKey)
	if err != nil {
		return errors.Wrapf(err, "could not open server key at \"%s\"", opts.TLS.ServerKey)
	}
	secret := &v1.Secret{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Secret",
			APIVersion: "v1",
		},
		ObjectMeta: objectMeta(tlsSecretName, labels(tlsSecretName), nil, opts.Namespace),
		Data: map[string][]byte{
			tls.CertFile: certBytes,
			tls.KeyFile:  keyBytes,
		},
	}
	return encoder.Encode(secret)
}

// WriteLocalAssets writes assets to a local backend.
func WriteLocalAssets(encoder serde.Encoder, opts *AssetOpts, hostPath string) error {
	if err := WriteAssets(encoder, opts, LocalBackend, LocalBackend, 1 /* = volume size (gb) */, hostPath); err != nil {
		return err
	}
	if secretErr := WriteSecret(encoder, LocalSecret(), opts); secretErr != nil {
		return secretErr
	}
	return nil
}

// WriteCustomAssets writes assets to a custom combination of object-store and persistent disk.
func WriteCustomAssets(encoder serde.Encoder, opts *AssetOpts, args []string, objectStoreBackend string,
	persistentDiskBackend string, secure, isS3V2 bool, advancedConfig *obj.AmazonAdvancedConfiguration) error {
	switch objectStoreBackend {
	case "s3":
		if len(args) != S3CustomArgs {
			return errors.Errorf("expected %d arguments for disk+s3 backend", S3CustomArgs)
		}
		volumeSize, err := strconv.Atoi(args[1])
		if err != nil {
			return errors.Errorf("volume size needs to be an integer; instead got %v", args[1])
		}
		objectStoreBackend := AmazonBackend
		// (bryce) use minio if we need v2 signing enabled.
		if isS3V2 {
			objectStoreBackend = MinioBackend
		}
		switch persistentDiskBackend {
		case "aws":
			if err := WriteAssets(encoder, opts, objectStoreBackend, AmazonBackend, volumeSize, ""); err != nil {
				return err
			}
		case "google":
			if err := WriteAssets(encoder, opts, objectStoreBackend, GoogleBackend, volumeSize, ""); err != nil {
				return err
			}
		case "azure":
			if err := WriteAssets(encoder, opts, objectStoreBackend, MicrosoftBackend, volumeSize, ""); err != nil {
				return err
			}
		default:
			return errors.Errorf("did not recognize the choice of persistent-disk")
		}
		bucket := args[2]
		id := args[3]
		secret := args[4]
		endpoint := args[5]
		if objectStoreBackend == MinioBackend {
			return WriteSecret(encoder, MinioSecret(bucket, id, secret, endpoint, secure, isS3V2), opts)
		}
		// (bryce) hardcode region?
		return WriteSecret(encoder, AmazonSecret("us-east-1", bucket, id, secret, "", "", endpoint, advancedConfig), opts)
	default:
		return errors.Errorf("did not recognize the choice of object-store")
	}
}

// AmazonCreds are options that are applicable specifically to Pachd's
// credentials in an AWS deployment
type AmazonCreds struct {
	// Direct credentials. Only applicable if Pachyderm is given its own permanent
	// AWS credentials
	ID     string // Access Key ID
	Secret string // Secret Access Key
	Token  string // Access token (if using temporary security credentials

	// Vault options (if getting AWS credentials from Vault)
	VaultAddress string // normally addresses come from env, but don't have vault service name
	VaultRole    string
	VaultToken   string
}

// WriteAmazonAssets writes assets to an amazon backend.
func WriteAmazonAssets(encoder serde.Encoder, opts *AssetOpts, region string, bucket string, volumeSize int, creds *AmazonCreds, cloudfrontDistro string, advancedConfig *obj.AmazonAdvancedConfiguration) error {
	if err := WriteAssets(encoder, opts, AmazonBackend, AmazonBackend, volumeSize, ""); err != nil {
		return err
	}
	var secret map[string][]byte
	if creds == nil {
		secret = AmazonIAMRoleSecret(region, bucket, cloudfrontDistro, advancedConfig)
	} else if creds.ID != "" {
		secret = AmazonSecret(region, bucket, creds.ID, creds.Secret, creds.Token, cloudfrontDistro, "", advancedConfig)
	} else if creds.VaultAddress != "" {
		secret = AmazonVaultSecret(region, bucket, creds.VaultAddress, creds.VaultRole, creds.VaultToken, cloudfrontDistro, advancedConfig)
	}
	return WriteSecret(encoder, secret, opts)
}

// WriteGoogleAssets writes assets to a google backend.
func WriteGoogleAssets(encoder serde.Encoder, opts *AssetOpts, bucket string, cred string, volumeSize int) error {
	if err := WriteAssets(encoder, opts, GoogleBackend, GoogleBackend, volumeSize, ""); err != nil {
		return err
	}
	return WriteSecret(encoder, GoogleSecret(bucket, cred), opts)
}

// WriteMicrosoftAssets writes assets to a microsoft backend
func WriteMicrosoftAssets(encoder serde.Encoder, opts *AssetOpts, container string, id string, secret string, volumeSize int) error {
	if err := WriteAssets(encoder, opts, MicrosoftBackend, MicrosoftBackend, volumeSize, ""); err != nil {
		return err
	}
	return WriteSecret(encoder, MicrosoftSecret(container, id, secret), opts)
}

// Images returns a list of all the images that are used by a pachyderm deployment.
func Images(opts *AssetOpts) []string {
	return []string{
		versionedWorkerImage(opts),
		etcdImage,
		postgresImage,
		grpcProxyImage,
		pauseImage,
		versionedPachdImage(opts),
		opts.DashImage,
		pgBouncerImage,
	}
}

// AddRegistry switches the registry that an image is targeting, unless registry is blank
func AddRegistry(registry string, imageName string) string {
	if registry == "" {
		return imageName
	}
	parts := strings.Split(imageName, "/")
	if len(parts) == 3 {
		parts = parts[1:]
	}
	return path.Join(registry, parts[0], parts[1])
}

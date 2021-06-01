package server

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"os"
	"strconv"

	jsonpatch "github.com/evanphx/json-patch"
	client "github.com/pachyderm/pachyderm/v2/src/client"
	"github.com/pachyderm/pachyderm/v2/src/enterprise"
	"github.com/pachyderm/pachyderm/v2/src/internal/config"
	"github.com/pachyderm/pachyderm/v2/src/internal/deploy/assets"
	"github.com/pachyderm/pachyderm/v2/src/internal/errors"
	"github.com/pachyderm/pachyderm/v2/src/internal/errutil"
	"github.com/pachyderm/pachyderm/v2/src/internal/ppsutil"
	"github.com/pachyderm/pachyderm/v2/src/internal/serviceenv"
	"github.com/pachyderm/pachyderm/v2/src/internal/tracing"
	"github.com/pachyderm/pachyderm/v2/src/pps"
	workerstats "github.com/pachyderm/pachyderm/v2/src/server/worker/stats"
	"github.com/pachyderm/pachyderm/v2/src/version"

	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	kube "k8s.io/client-go/kubernetes"
)

const (
	pipelineNameLabel         = "pipelineName"
	pachVersionAnnotation     = "version"
	specCommitAnnotation      = "specCommit"
	hashedAuthTokenAnnotation = "authTokenHash"
)

// Parameters used when creating the kubernetes replication controller in charge
// of a job or pipeline's workers
type workerOptions struct {
	rcName        string // Name of the replication controller managing workers
	specCommit    string // Pipeline spec commit ID (needed for s3 inputs)
	s3GatewayPort int32  // s3 gateway port (if any s3 pipeline inputs)

	userImage             string              // The user's pipeline/job image
	labels                map[string]string   // k8s labels attached to the RC and workers
	annotations           map[string]string   // k8s annotations attached to the RC and workers
	parallelism           int32               // Number of replicas the RC maintains
	cacheSize             string              // Size of cache that sidecar uses
	resourceRequests      *v1.ResourceList    // Resources requested by pipeline/job pods
	resourceLimits        *v1.ResourceList    // Resources requested by pipeline/job pods, applied to the user and init containers
	sidecarResourceLimits *v1.ResourceList    // Resources requested by pipeline/job pods, applied to the sidecar container
	workerEnv             []v1.EnvVar         // Environment vars set in the user container
	volumes               []v1.Volume         // Volumes that we expose to the user container
	volumeMounts          []v1.VolumeMount    // Paths where we mount each volume in 'volumes'
	schedulingSpec        *pps.SchedulingSpec // the SchedulingSpec for the pipeline
	podSpec               string
	podPatch              string

	// Secrets that we mount in the worker container (e.g. for reading/writing to
	// s3)
	imagePullSecrets []v1.LocalObjectReference
	service          *pps.Service
}

// getPachctlSecretVolumeAndMount returns a Volume and
// VolumeMount object configured for the pachctl secret (currently used in spout pipelines).
func getPachctlSecretVolumeAndMount(secret string) (v1.Volume, v1.VolumeMount) {
	return v1.Volume{
			Name: client.PachctlSecretName,
			VolumeSource: v1.VolumeSource{
				Secret: &v1.SecretVolumeSource{
					SecretName: secret,
				},
			},
		}, v1.VolumeMount{
			Name:      client.PachctlSecretName,
			MountPath: "/pachctl",
		}
}

func (a *apiServer) workerPodSpec(options *workerOptions, pipelineInfo *pps.PipelineInfo) (v1.PodSpec, error) {
	pullPolicy := a.workerImagePullPolicy
	if pullPolicy == "" {
		pullPolicy = "IfNotPresent"
	}

	// Set up sidecar env vars
	sidecarEnv := []v1.EnvVar{{
		Name:  "PACH_ROOT",
		Value: a.storageRoot,
	}, {
		Name:  "PACH_NAMESPACE",
		Value: a.namespace,
	}, {
		Name:  "STORAGE_BACKEND",
		Value: a.storageBackend,
	}, {
		Name:  "PORT",
		Value: strconv.FormatUint(uint64(a.port), 10),
	}, {
		Name:  "HTTP_PORT",
		Value: strconv.FormatUint(uint64(a.httpPort), 10),
	}, {
		Name:  "PEER_PORT",
		Value: strconv.FormatUint(uint64(a.peerPort), 10),
	}, {
		Name:  client.PPSSpecCommitEnv,
		Value: options.specCommit,
	}, {
		Name: "PACHD_POD_NAME",
		ValueFrom: &v1.EnvVarSource{
			FieldRef: &v1.ObjectFieldSelector{
				APIVersion: "v1",
				FieldPath:  "metadata.name",
			},
		},
	}, {
		Name:  "GC_PERCENT",
		Value: strconv.FormatInt(int64(a.gcPercent), 10),
	},
	}
	sidecarEnv = append(sidecarEnv, postgresEnvVars(a.env.Config())...)
	sidecarEnv = append(sidecarEnv, assets.GetSecretEnvVars(a.storageBackend)...)
	sidecarEnv = append(sidecarEnv, a.getStorageEnvVars(pipelineInfo)...)

	// Set up worker env vars
	workerEnv := append(options.workerEnv, []v1.EnvVar{
		// Set core pach env vars
		{
			Name:  "PACH_ROOT",
			Value: a.storageRoot,
		},
		{
			Name:  "PACH_NAMESPACE",
			Value: a.namespace,
		},
		{
			Name:  "PACH_IN_WORKER",
			Value: "true",
		},
		// We use Kubernetes' "Downward API" so the workers know their IP
		// addresses, which they will then post on etcd so the job managers
		// can discover the workers.
		{
			Name: client.PPSWorkerIPEnv,
			ValueFrom: &v1.EnvVarSource{
				FieldRef: &v1.ObjectFieldSelector{
					APIVersion: "v1",
					FieldPath:  "status.podIP",
				},
			},
		},
		// Set the PPS env vars
		{
			Name:  client.PPSEtcdPrefixEnv,
			Value: a.etcdPrefix,
		},
		{
			Name: client.PPSPodNameEnv,
			ValueFrom: &v1.EnvVarSource{
				FieldRef: &v1.ObjectFieldSelector{
					APIVersion: "v1",
					FieldPath:  "metadata.name",
				},
			},
		},
		{
			Name:  client.PPSSpecCommitEnv,
			Value: options.specCommit,
		},
		{
			Name:  client.PPSWorkerPortEnv,
			Value: strconv.FormatUint(uint64(a.workerGrpcPort), 10),
		},
		{
			Name:  client.PeerPortEnv,
			Value: strconv.FormatUint(uint64(a.peerPort), 10),
		},
	}...)
	workerEnv = append(workerEnv, postgresEnvVars(a.env.Config())...)
	workerEnv = append(workerEnv, assets.GetSecretEnvVars(a.storageBackend)...)

	// Set S3GatewayPort in the worker (for user code) and sidecar (for serving)
	if options.s3GatewayPort != 0 {
		workerEnv = append(workerEnv, v1.EnvVar{
			Name:  "S3GATEWAY_PORT",
			Value: strconv.FormatUint(uint64(options.s3GatewayPort), 10),
		})
		sidecarEnv = append(sidecarEnv, v1.EnvVar{
			Name:  "S3GATEWAY_PORT",
			Value: strconv.FormatUint(uint64(options.s3GatewayPort), 10),
		})
	}
	// Propagate feature flags to worker and sidecar
	if a.env.Config().DisableCommitProgressCounter {
		sidecarEnv = append(sidecarEnv, v1.EnvVar{Name: "DISABLE_COMMIT_PROGRESS_COUNTER", Value: "true"})
		workerEnv = append(workerEnv, v1.EnvVar{Name: "DISABLE_COMMIT_PROGRESS_COUNTER", Value: "true"})
	}
	if a.env.Config().LokiLogging {
		sidecarEnv = append(sidecarEnv, v1.EnvVar{Name: "LOKI_LOGGING", Value: "true"})
		workerEnv = append(workerEnv, v1.EnvVar{Name: "LOKI_LOGGING", Value: "true"})
	}

	// This only happens in local deployment.  We want the workers to be
	// able to read from/write to the hostpath volume as well.
	storageVolumeName := "pach-disk"
	var sidecarVolumeMounts []v1.VolumeMount
	userVolumeMounts := make([]v1.VolumeMount, len(options.volumeMounts))
	copy(userVolumeMounts, options.volumeMounts)
	if a.storageHostPath != "" {
		options.volumes = append(options.volumes, v1.Volume{
			Name: storageVolumeName,
			VolumeSource: v1.VolumeSource{
				HostPath: &v1.HostPathVolumeSource{
					Path: a.storageHostPath,
				},
			},
		})
		storageMount := v1.VolumeMount{
			Name:      storageVolumeName,
			MountPath: a.storageRoot,
		}
		sidecarVolumeMounts = append(sidecarVolumeMounts, storageMount)
		userVolumeMounts = append(userVolumeMounts, storageMount)
	} else {
		// `pach-dir-volume` is needed for openshift, see:
		// https://github.com/pachyderm/pachyderm/v2/issues/3404
		options.volumes = append(options.volumes, v1.Volume{
			Name: "pach-dir-volume",
			VolumeSource: v1.VolumeSource{
				EmptyDir: &v1.EmptyDirVolumeSource{},
			},
		})
		emptyDirVolumeMount := v1.VolumeMount{
			Name:      "pach-dir-volume",
			MountPath: a.storageRoot,
		}
		sidecarVolumeMounts = append(sidecarVolumeMounts, emptyDirVolumeMount)
		userVolumeMounts = append(userVolumeMounts, emptyDirVolumeMount)
	}
	secretVolume, secretMount := assets.GetBackendSecretVolumeAndMount(a.storageBackend)
	options.volumes = append(options.volumes, secretVolume)
	sidecarVolumeMounts = append(sidecarVolumeMounts, secretMount)
	userVolumeMounts = append(userVolumeMounts, secretMount)

	// mount secret for spouts using pachctl
	if pipelineInfo.Spout != nil {
		pachctlSecretVolume, pachctlSecretMount := getPachctlSecretVolumeAndMount("spout-pachctl-secret-" + pipelineInfo.Pipeline.Name)
		options.volumes = append(options.volumes, pachctlSecretVolume)
		sidecarVolumeMounts = append(sidecarVolumeMounts, pachctlSecretMount)
		userVolumeMounts = append(userVolumeMounts, pachctlSecretMount)
	}

	// Explicitly set CPU requests to zero because some cloud providers set their
	// own defaults which are usually not what we want. Mem request defaults to
	// 64M, but is overridden by the CacheSize setting for the sidecar.
	cpuZeroQuantity := resource.MustParse("0")
	memDefaultQuantity := resource.MustParse("64M")
	memSidecarQuantity := resource.MustParse(options.cacheSize)

	// Get service account name for worker from env or use default
	workerServiceAccountName, ok := os.LookupEnv(assets.WorkerServiceAccountEnvVar)
	if !ok {
		workerServiceAccountName = assets.DefaultWorkerServiceAccountName
	}

	// possibly expose s3 gateway port in the sidecar container
	var sidecarPorts []v1.ContainerPort
	if options.s3GatewayPort != 0 {
		sidecarPorts = append(sidecarPorts, v1.ContainerPort{
			ContainerPort: options.s3GatewayPort,
		})
	}
	if !a.noExposeDockerSocket {
		options.volumes = append(options.volumes, v1.Volume{
			Name: "docker",
			VolumeSource: v1.VolumeSource{
				HostPath: &v1.HostPathVolumeSource{
					Path: "/var/run/docker.sock",
				},
			},
		})
		userVolumeMounts = append(userVolumeMounts, v1.VolumeMount{
			Name:      "docker",
			MountPath: "/var/run/docker.sock",
		})
	}
	zeroVal := int64(0)
	workerImage := a.workerImage
	var securityContext *v1.PodSecurityContext
	if a.workerUsesRoot {
		securityContext = &v1.PodSecurityContext{RunAsUser: &zeroVal}
	}
	resp, err := a.env.GetPachClient(context.Background()).Enterprise.GetState(context.Background(), &enterprise.GetStateRequest{})
	if err != nil {
		return v1.PodSpec{}, err
	}
	if resp.State != enterprise.State_ACTIVE {
		workerImage = assets.AddRegistry("", workerImage)
	}
	podSpec := v1.PodSpec{
		InitContainers: []v1.Container{
			{
				Name:            "init",
				Image:           workerImage,
				Command:         []string{"/app/init"},
				ImagePullPolicy: v1.PullPolicy(pullPolicy),
				VolumeMounts:    options.volumeMounts,
				Resources: v1.ResourceRequirements{
					Requests: v1.ResourceList{
						v1.ResourceCPU:    cpuZeroQuantity,
						v1.ResourceMemory: memDefaultQuantity,
					},
				},
			},
		},
		Containers: []v1.Container{
			{
				Name:            client.PPSWorkerUserContainerName,
				Image:           options.userImage,
				Command:         []string{"/pach-bin/worker"},
				ImagePullPolicy: v1.PullPolicy(pullPolicy),
				Env:             workerEnv,
				Resources: v1.ResourceRequirements{
					Requests: v1.ResourceList{
						v1.ResourceCPU:    cpuZeroQuantity,
						v1.ResourceMemory: memDefaultQuantity,
					},
				},
				VolumeMounts: userVolumeMounts,
			},
			{
				Name:            client.PPSWorkerSidecarContainerName,
				Image:           a.workerSidecarImage,
				Command:         []string{"/pachd", "--mode", "sidecar"},
				ImagePullPolicy: v1.PullPolicy(pullPolicy),
				Env:             sidecarEnv,
				VolumeMounts:    sidecarVolumeMounts,
				Resources: v1.ResourceRequirements{
					Requests: v1.ResourceList{
						v1.ResourceCPU:    cpuZeroQuantity,
						v1.ResourceMemory: memSidecarQuantity,
					},
				},
				Ports: sidecarPorts,
			},
		},
		ServiceAccountName:            workerServiceAccountName,
		RestartPolicy:                 "Always",
		Volumes:                       options.volumes,
		ImagePullSecrets:              options.imagePullSecrets,
		TerminationGracePeriodSeconds: &zeroVal,
		SecurityContext:               securityContext,
	}
	if options.schedulingSpec != nil {
		podSpec.NodeSelector = options.schedulingSpec.NodeSelector
		podSpec.PriorityClassName = options.schedulingSpec.PriorityClassName
	}

	if options.resourceRequests != nil {
		for k, v := range *options.resourceRequests {
			podSpec.Containers[0].Resources.Requests[k] = v
		}
	}

	// Copy over some settings from the user to init container. We don't apply GPU
	// requests because of FUD. The init container shouldn't run concurrently with
	// the user container, so there should be no contention here.
	for _, k := range []v1.ResourceName{v1.ResourceCPU, v1.ResourceMemory, v1.ResourceEphemeralStorage} {
		if val, ok := podSpec.Containers[0].Resources.Requests[k]; ok {
			podSpec.InitContainers[0].Resources.Requests[k] = val
		}
	}

	if options.resourceLimits != nil {
		podSpec.InitContainers[0].Resources.Limits = make(v1.ResourceList)
		podSpec.Containers[0].Resources.Limits = make(v1.ResourceList)
		for k, v := range *options.resourceLimits {
			podSpec.InitContainers[0].Resources.Limits[k] = v
			podSpec.Containers[0].Resources.Limits[k] = v
		}
	}

	if options.sidecarResourceLimits != nil {
		podSpec.Containers[1].Resources.Limits = make(v1.ResourceList)
		for k, v := range *options.sidecarResourceLimits {
			podSpec.Containers[1].Resources.Limits[k] = v
		}
	}

	if options.podSpec != "" || options.podPatch != "" {
		jsonPodSpec, err := json.Marshal(&podSpec)
		if err != nil {
			return v1.PodSpec{}, err
		}

		// the json now contained in jsonPodSpec is the authoritative copy
		// so we should deserialize in into a fresh structure
		podSpec = v1.PodSpec{}

		if options.podSpec != "" {
			jsonPodSpec, err = jsonpatch.MergePatch(jsonPodSpec, []byte(options.podSpec))
			if err != nil {
				return v1.PodSpec{}, err
			}
		}
		if options.podPatch != "" {
			patch, err := jsonpatch.DecodePatch([]byte(options.podPatch))
			if err != nil {
				return v1.PodSpec{}, err
			}
			jsonPodSpec, err = patch.Apply(jsonPodSpec)
			if err != nil {
				return v1.PodSpec{}, err
			}
		}
		if err := json.Unmarshal(jsonPodSpec, &podSpec); err != nil {
			return v1.PodSpec{}, err
		}
	}
	return podSpec, nil
}

func (a *apiServer) getStorageEnvVars(pipelineInfo *pps.PipelineInfo) []v1.EnvVar {
	vars := []v1.EnvVar{
		{Name: assets.UploadConcurrencyLimitEnvVar, Value: strconv.Itoa(a.env.Config().StorageUploadConcurrencyLimit)},
	}
	if pipelineInfo.Spout != nil {
		vars = append(vars, v1.EnvVar{Name: "SPOUT_PIPELINE_NAME", Value: pipelineInfo.Pipeline.Name})
	}
	return vars
}

// We don't want to expose pipeline auth tokens, so we hash it. This will be
// visible to any user with k8s cluster access
// Note: This hash shouldn't be used for authentication in any way. We just use
// this to detect if an auth token has been added/changed
// Note: ptr.AuthToken is a pachyderm-generated UUID, and wouldn't appear in any
// rainbow tables
func hashAuthToken(token string) string {
	h := sha256.Sum256([]byte(token))
	return base64.RawURLEncoding.EncodeToString(h[:])
}

func (a *apiServer) getWorkerOptions(ptr *pps.StoredPipelineInfo, pipelineInfo *pps.PipelineInfo) (*workerOptions, error) {
	pipelineName := pipelineInfo.Pipeline.Name
	pipelineVersion := pipelineInfo.Version
	var resourceRequests *v1.ResourceList
	var resourceLimits *v1.ResourceList
	var sidecarResourceLimits *v1.ResourceList
	if pipelineInfo.ResourceRequests != nil {
		var err error
		resourceRequests, err = ppsutil.GetRequestsResourceListFromPipeline(pipelineInfo)
		if err != nil {
			return nil, errors.Wrapf(err, "could not determine resource request")
		}
	}
	if pipelineInfo.ResourceLimits != nil {
		var err error
		resourceLimits, err = ppsutil.GetLimitsResourceList(pipelineInfo.ResourceLimits)
		if err != nil {
			return nil, errors.Wrapf(err, "could not determine resource limit")
		}
	}
	if pipelineInfo.SidecarResourceLimits != nil {
		var err error
		sidecarResourceLimits, err = ppsutil.GetLimitsResourceList(pipelineInfo.SidecarResourceLimits)
		if err != nil {
			return nil, errors.Wrapf(err, "could not determine sidecar resource limit")
		}
	}

	transform := pipelineInfo.Transform
	rcName := ppsutil.PipelineRcName(pipelineName, pipelineVersion)
	labels := labels(rcName)
	labels[pipelineNameLabel] = pipelineName
	userImage := transform.Image
	if userImage == "" {
		userImage = DefaultUserImage
	}

	workerEnv := []v1.EnvVar{{
		Name:  client.PPSPipelineNameEnv,
		Value: pipelineInfo.Pipeline.Name,
	}}
	for name, value := range transform.Env {
		workerEnv = append(
			workerEnv,
			v1.EnvVar{
				Name:  name,
				Value: value,
			},
		)
	}

	var volumes []v1.Volume
	var volumeMounts []v1.VolumeMount
	for _, secret := range transform.Secrets {
		if secret.MountPath != "" {
			volumes = append(volumes, v1.Volume{
				Name: secret.Name,
				VolumeSource: v1.VolumeSource{
					Secret: &v1.SecretVolumeSource{
						SecretName: secret.Name,
					},
				},
			})
			volumeMounts = append(volumeMounts, v1.VolumeMount{
				Name:      secret.Name,
				MountPath: secret.MountPath,
			})
		}
		if secret.EnvVar != "" {
			workerEnv = append(workerEnv, v1.EnvVar{
				Name: secret.EnvVar,
				ValueFrom: &v1.EnvVarSource{
					SecretKeyRef: &v1.SecretKeySelector{
						LocalObjectReference: v1.LocalObjectReference{
							Name: secret.Name,
						},
						Key: secret.Key,
					},
				},
			})
		}
	}

	volumes = append(volumes, v1.Volume{
		Name: "pach-bin",
		VolumeSource: v1.VolumeSource{
			EmptyDir: &v1.EmptyDirVolumeSource{},
		},
	})
	volumeMounts = append(volumeMounts, v1.VolumeMount{
		Name:      "pach-bin",
		MountPath: "/pach-bin",
	})

	volumes = append(volumes, v1.Volume{
		Name: client.PPSWorkerVolume,
		VolumeSource: v1.VolumeSource{
			EmptyDir: &v1.EmptyDirVolumeSource{},
		},
	})
	volumeMounts = append(volumeMounts, v1.VolumeMount{
		Name:      client.PPSWorkerVolume,
		MountPath: client.PPSInputPrefix,
	})
	var imagePullSecrets []v1.LocalObjectReference
	for _, secret := range transform.ImagePullSecrets {
		imagePullSecrets = append(imagePullSecrets, v1.LocalObjectReference{Name: secret})
	}
	if a.imagePullSecret != "" {
		imagePullSecrets = append(imagePullSecrets, v1.LocalObjectReference{Name: a.imagePullSecret})
	}

	annotations := map[string]string{
		pipelineNameLabel:         pipelineName,
		pachVersionAnnotation:     version.PrettyVersion(),
		specCommitAnnotation:      ptr.SpecCommit.ID,
		hashedAuthTokenAnnotation: hashAuthToken(ptr.AuthToken),
	}
	if a.iamRole != "" {
		annotations["iam.amazonaws.com/role"] = a.iamRole
	}

	// add the user's custom metadata (annotations and labels).
	metadata := pipelineInfo.GetMetadata()
	if metadata != nil {
		for k, v := range metadata.Annotations {
			if annotations[k] == "" {
				annotations[k] = v
			}
		}

		for k, v := range metadata.Labels {
			if labels[k] == "" {
				labels[k] = v
			}
		}
	}

	// A service can be present either directly on the pipeline spec
	// or on the spout field of the spec.
	var service *pps.Service
	if pipelineInfo.Spout != nil && pipelineInfo.Service != nil {
		return nil, errors.New("only one of pipeline.service or pipeline.spout can be set")
	} else if pipelineInfo.Spout != nil && pipelineInfo.Spout.Service != nil {
		service = pipelineInfo.Spout.Service
	} else {
		service = pipelineInfo.Service
	}
	var s3GatewayPort int32
	if ppsutil.ContainsS3Inputs(pipelineInfo.Input) || pipelineInfo.S3Out {
		s3GatewayPort = int32(a.env.Config().S3GatewayPort)
	}

	// Generate options for new RC
	return &workerOptions{
		rcName:                rcName,
		s3GatewayPort:         s3GatewayPort,
		specCommit:            ptr.SpecCommit.ID,
		labels:                labels,
		annotations:           annotations,
		parallelism:           int32(0), // pipelines start w/ 0 workers & are scaled up
		resourceRequests:      resourceRequests,
		resourceLimits:        resourceLimits,
		sidecarResourceLimits: sidecarResourceLimits,
		userImage:             userImage,
		workerEnv:             workerEnv,
		volumes:               volumes,
		volumeMounts:          volumeMounts,
		imagePullSecrets:      imagePullSecrets,
		cacheSize:             pipelineInfo.CacheSize,
		service:               service,
		schedulingSpec:        pipelineInfo.SchedulingSpec,
		podSpec:               pipelineInfo.PodSpec,
		podPatch:              pipelineInfo.PodPatch,
	}, nil
}

func (a *apiServer) createWorkerPachctlSecret(ctx context.Context, ptr *pps.StoredPipelineInfo, pipelineInfo *pps.PipelineInfo) error {
	var cfg config.Config
	err := cfg.InitV2()
	if err != nil {
		return errors.Wrapf(err, "error initializing V2 for config")
	}
	_, context, err := cfg.ActiveContext(true)
	if err != nil {
		return errors.Wrapf(err, "error getting the active context")
	}
	context.SessionToken = ptr.AuthToken
	context.PachdAddress = "localhost:653"

	rawConfig, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return errors.Wrapf(err, "error marshaling the config")
	}
	s := v1.Secret{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Secret",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:   "spout-pachctl-secret-" + pipelineInfo.Pipeline.Name,
			Labels: labels(pipelineInfo.Pipeline.Name),
		},
		Data: map[string][]byte{
			"config.json": rawConfig,
		},
	}
	labels := s.GetLabels()
	labels["pipelineName"] = pipelineInfo.Pipeline.Name
	s.SetLabels(labels)

	// send RPC to k8s to create the secret there
	if _, err := a.env.GetKubeClient().CoreV1().Secrets(a.namespace).Create(&s); err != nil {
		if !isAlreadyExistsErr(err) {
			return err
		}
	}
	return nil
}

// noValidOptions error may be returned by createWorkerSvcAndRc to indicate that
// getWorkerOptions returned an error to it (getWorkerOptions does not return
// noValidOptions). This is a mechanism for createWorkerSvcAndRc to signal to
// its caller not to retry
type noValidOptionsErr struct {
	error
}

func (a *apiServer) createWorkerSvcAndRc(ctx context.Context, ptr *pps.StoredPipelineInfo, pipelineInfo *pps.PipelineInfo) (retErr error) {
	log.Infof("PPS master: upserting workers for %q", pipelineInfo.Pipeline.Name)
	span, ctx := tracing.AddSpanToAnyExisting(ctx, "/pps.Master/CreateWorkerRC", // ctx never used, but we want the right one in scope for future uses
		"pipeline", pipelineInfo.Pipeline.Name)
	defer func() {
		tracing.TagAnySpan(span, "err", retErr)
		tracing.FinishAnySpan(span)
	}()

	// create pachctl secret used in spouts
	if pipelineInfo.Spout != nil {
		if err := a.createWorkerPachctlSecret(ctx, ptr, pipelineInfo); err != nil {
			return err
		}
	}

	options, err := a.getWorkerOptions(ptr, pipelineInfo)
	if err != nil {
		return noValidOptionsErr{err}
	}
	podSpec, err := a.workerPodSpec(options, pipelineInfo)
	if err != nil {
		return err
	}
	rc := &v1.ReplicationController{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ReplicationController",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:        options.rcName,
			Labels:      options.labels,
			Annotations: options.annotations,
		},
		Spec: v1.ReplicationControllerSpec{
			Selector: options.labels,
			Replicas: &options.parallelism,
			Template: &v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Name:        options.rcName,
					Labels:      options.labels,
					Annotations: options.annotations,
				},
				Spec: podSpec,
			},
		},
	}
	if _, err := a.env.GetKubeClient().CoreV1().ReplicationControllers(a.namespace).Create(rc); err != nil {
		if !isAlreadyExistsErr(err) {
			return err
		}
	}
	serviceAnnotations := map[string]string{
		"prometheus.io/scrape": "true",
		"prometheus.io/port":   strconv.Itoa(workerstats.PrometheusPort),
	}

	service := &v1.Service{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Service",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:        options.rcName,
			Labels:      options.labels,
			Annotations: serviceAnnotations,
		},
		Spec: v1.ServiceSpec{
			Selector: options.labels,
			Ports: []v1.ServicePort{
				{
					Port: int32(a.workerGrpcPort),
					Name: "grpc-port",
				},
				{
					Port: workerstats.PrometheusPort,
					Name: "prometheus-metrics",
				},
			},
		},
	}
	if _, err := a.env.GetKubeClient().CoreV1().Services(a.namespace).Create(service); err != nil {
		if !isAlreadyExistsErr(err) {
			return err
		}
	}

	if options.service != nil {
		var servicePort = []v1.ServicePort{
			{
				Port:       options.service.ExternalPort,
				TargetPort: intstr.FromInt(int(options.service.InternalPort)),
				Name:       "user-port",
			},
		}
		var serviceType = v1.ServiceType(options.service.Type)
		if serviceType == v1.ServiceTypeNodePort {
			servicePort[0].NodePort = options.service.ExternalPort
		}
		service := &v1.Service{
			TypeMeta: metav1.TypeMeta{
				Kind:       "Service",
				APIVersion: "v1",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name:        options.rcName + "-user",
				Labels:      options.labels,
				Annotations: options.annotations,
			},
			Spec: v1.ServiceSpec{
				Selector: options.labels,
				Type:     serviceType,
				Ports:    servicePort,
			},
		}
		if _, err := a.env.GetKubeClient().CoreV1().Services(a.namespace).Create(service); err != nil {
			if !isAlreadyExistsErr(err) {
				return err
			}
		}
	}

	var hasGitInput bool
	pps.VisitInput(pipelineInfo.Input, func(input *pps.Input) error {
		if input.Git != nil {
			hasGitInput = true
			return errutil.ErrBreak
		}
		return nil
	})
	if hasGitInput {
		return a.checkOrDeployGithookService()
	}
	return nil
}

func (a *apiServer) checkOrDeployGithookService() error {
	kubeClient := a.env.GetKubeClient()
	_, err := getGithookService(kubeClient, a.namespace)
	if err != nil {
		if errors.As(err, &errGithookServiceNotFound{}) {
			svc := assets.GithookService(a.namespace)
			_, err = kubeClient.CoreV1().Services(a.namespace).Create(svc)
			return err
		}
		return err
	}
	// service already exists
	return nil
}

func getGithookService(kubeClient *kube.Clientset, namespace string) (*v1.Service, error) {
	labels := map[string]string{
		"app":   "githook",
		"suite": suite,
	}
	serviceList, err := kubeClient.CoreV1().Services(namespace).List(metav1.ListOptions{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ListOptions",
			APIVersion: "v1",
		},
		LabelSelector: metav1.FormatLabelSelector(metav1.SetAsLabelSelector(labels)),
	})
	if err != nil {
		return nil, err
	}
	if len(serviceList.Items) != 1 {
		return nil, &errGithookServiceNotFound{
			errors.Errorf("expected 1 githook service but found %v", len(serviceList.Items)),
		}
	}
	return &serviceList.Items[0], nil
}

func postgresEnvVars(config *serviceenv.Configuration) []v1.EnvVar {
	return []v1.EnvVar{
		{
			Name:  "POSTGRES_DATABASE",
			Value: config.PostgresDB,
		}, {
			Name:  "POSTGRES_USER",
			Value: config.PostgresUser,
		}, {
			Name:  "POSTGRES_SERVICE_HOST",
			Value: config.PostgresServiceHost,
		}, {
			Name:  "POSTGRES_SERVICE_PORT",
			Value: strconv.Itoa(config.PostgresServicePort),
		}, {
			Name:  "POSTGRES_SERVICE_SSL",
			Value: config.PostgresServiceSSL,
		},
	}
}

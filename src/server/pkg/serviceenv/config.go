package serviceenv

// Configuration is the generic configuration structure used to access configuration fields.
type Configuration struct {
	*GlobalConfiguration
	*PachdSpecificConfiguration
	*WorkerSpecificConfiguration
}

// GlobalConfiguration contains the global configuration.
type GlobalConfiguration struct {
	FeatureFlags
	EtcdHost      string `env:"ETCD_SERVICE_HOST,required"`
	EtcdPort      string `env:"ETCD_SERVICE_PORT,required"`
	PPSWorkerPort uint16 `env:"PPS_WORKER_GRPC_PORT,default=80"`
	Port          uint16 `env:"PORT,default=650"`
	PProfPort     uint16 `env:"PPROF_PORT,default=651"`
	HTTPPort      uint16 `env:"HTTP_PORT,default=652"`
	PeerPort      uint16 `env:"PEER_PORT,default=653"`
	PPSEtcdPrefix string `env:"PPS_ETCD_PREFIX,default=pachyderm_pps"`
	Namespace     string `env:"NAMESPACE,default=default"`
	StorageRoot   string `env:"PACH_ROOT,default=/pach"`
}

// PachdFullConfiguration contains the full pachd configuration.
type PachdFullConfiguration struct {
	GlobalConfiguration
	PachdSpecificConfiguration
}

// PachdSpecificConfiguration contains the pachd specific configuration.
type PachdSpecificConfiguration struct {
	NumShards             uint64 `env:"NUM_SHARDS,default=32"`
	StorageBackend        string `env:"STORAGE_BACKEND,default="`
	StorageHostPath       string `env:"STORAGE_HOST_PATH,default="`
	EtcdPrefix            string `env:"ETCD_PREFIX,default="`
	PFSEtcdPrefix         string `env:"PFS_ETCD_PREFIX,default=pachyderm_pfs"`
	AuthEtcdPrefix        string `env:"PACHYDERM_AUTH_ETCD_PREFIX,default=pachyderm_auth"`
	EnterpriseEtcdPrefix  string `env:"PACHYDERM_ENTERPRISE_ETCD_PREFIX,default=pachyderm_enterprise"`
	KubeAddress           string `env:"KUBERNETES_PORT_443_TCP_ADDR,required"`
	Metrics               bool   `env:"METRICS,default=true"`
	Init                  bool   `env:"INIT,default=false"`
	BlockCacheBytes       string `env:"BLOCK_CACHE_BYTES,default=1G"`
	PFSCacheSize          string `env:"PFS_CACHE_SIZE,default=0"`
	WorkerImage           string `env:"WORKER_IMAGE,default="`
	WorkerSidecarImage    string `env:"WORKER_SIDECAR_IMAGE,default="`
	WorkerImagePullPolicy string `env:"WORKER_IMAGE_PULL_POLICY,default="`
	LogLevel              string `env:"LOG_LEVEL,default=info"`
	IAMRole               string `env:"IAM_ROLE,default="`
	ImagePullSecret       string `env:"IMAGE_PULL_SECRET,default="`
	NoExposeDockerSocket  bool   `env:"NO_EXPOSE_DOCKER_SOCKET,default=false"`
	ExposeObjectAPI       bool   `env:"EXPOSE_OBJECT_API,default=false"`
	MemoryRequest         string `env:"PACHD_MEMORY_REQUEST,default=1T"`
	WorkerUsesRoot        bool   `env:"WORKER_USES_ROOT,default=true"`
	S3GatewayPort         uint16 `env:"S3GATEWAY_PORT,default=655"`
}

// WorkerFullConfiguration contains the full worker configuration.
type WorkerFullConfiguration struct {
	GlobalConfiguration
	WorkerSpecificConfiguration
}

// WorkerSpecificConfiguration contains the worker specific configuration.
type WorkerSpecificConfiguration struct {
	// Worker gets its own IP here, via the k8s downward API. It then writes that
	// IP back to etcd so that pachd can discover it
	PPSWorkerIP string `env:"PPS_WORKER_IP,required"`
	// The name of the pipeline that this worker belongs to
	PPSPipelineName string `env:"PPS_PIPELINE_NAME,required"`
	// The ID of the commit that contains the pipeline spec.
	PPSSpecCommitID string `env:"PPS_SPEC_COMMIT,required"`
	// The name of this pod
	PodName string `env:"PPS_POD_NAME,required"`
}

// FeatureFlags contains the configuration for feature flags.
type FeatureFlags struct {
	NewHashTree bool `env:"NEW_HASH_TREE,default=false"`
}

// NewConfiguration creates a generic configuration from a specific type of configuration.
func NewConfiguration(config interface{}) *Configuration {
	configuration := &Configuration{}
	switch config.(type) {
	case *GlobalConfiguration:
		configuration.GlobalConfiguration = config.(*GlobalConfiguration)
		return configuration
	case *PachdFullConfiguration:
		configuration.GlobalConfiguration = &config.(*PachdFullConfiguration).GlobalConfiguration
		configuration.PachdSpecificConfiguration = &config.(*PachdFullConfiguration).PachdSpecificConfiguration
		return configuration
	case *WorkerFullConfiguration:
		configuration.GlobalConfiguration = &config.(*WorkerFullConfiguration).GlobalConfiguration
		configuration.WorkerSpecificConfiguration = &config.(*WorkerFullConfiguration).WorkerSpecificConfiguration
		return configuration
	default:
		return nil
	}
}

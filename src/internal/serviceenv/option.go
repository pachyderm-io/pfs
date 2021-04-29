package serviceenv

import (
	"github.com/pachyderm/pachyderm/v2/src/internal/storage/kv"
)

// ChunkMemoryCache returns the in memory cache for chunks, pre-configured to
// the desired size
func (conf *Configuration) ChunkMemoryCache() kv.GetPut {
	size := conf.StorageMemoryCacheSize
	if size < 1 {
		size = 1
	}
	return kv.NewMemCache(size)
}

// ConfigOption is a functional option that modifies this ServiceEnv's config
type ConfigOption = func(*Configuration)

// ApplyOptions applies the functional options 'opts' to the config 'config',
// modifying its values.
func ApplyOptions(config *Configuration, opts ...ConfigOption) {
	for _, opt := range opts {
		opt(config)
	}
}

// ConfigFromOptions is for use in tests where some tests may want to generate a
// config with certain default values overridden via options.
func ConfigFromOptions(opts ...ConfigOption) *Configuration {
	result := &Configuration{
		GlobalConfiguration:         &GlobalConfiguration{},
		PachdSpecificConfiguration:  &PachdSpecificConfiguration{},
		WorkerSpecificConfiguration: &WorkerSpecificConfiguration{},
	}
	ApplyOptions(result, opts...)
	return result
}

// WithPostgresHostPort sets the config's PostgresServiceHost and
// PostgresServicePort values (affecting pachd's DB client endpoint)
func WithPostgresHostPort(host string, port int) ConfigOption {
	return func(config *Configuration) {
		config.PostgresServiceHost = host
		config.PostgresServicePort = port
	}
}

// WithEtcdHostPort sets the config's EtcdHost and EtcdPort values (affecting
// pachd's etcd client endpoint)
func WithEtcdHostPort(host string, port string) ConfigOption {
	return func(config *Configuration) {
		config.EtcdHost = host
		config.EtcdPort = port
	}
}

// WithPachdPeerPort sets the config's PeerPort values (affecting pachd's Pachd
// client endpoint)
func WithPachdPeerPort(port uint16) ConfigOption {
	return func(config *Configuration) {
		config.PeerPort = port
	}
}

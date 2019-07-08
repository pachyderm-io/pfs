module github.com/pachyderm/pachyderm

go 1.12

require (
	cloud.google.com/go v0.40.0
	github.com/Azure/azure-sdk-for-go v5.0.0-beta+incompatible
	github.com/Azure/go-autorest/autorest v0.2.0 // indirect
	github.com/LK4D4/joincontext v0.0.0-20171026170139-1724345da6d5
	github.com/Microsoft/hcsshim v0.8.6 // indirect
	github.com/OneOfOne/xxhash v1.2.5
	github.com/aws/aws-lambda-go v1.11.1
	github.com/aws/aws-sdk-go v1.20.3
	github.com/beevik/etree v1.1.0
	github.com/beorn7/perks v1.0.0 // indirect
	github.com/bmizerany/assert v0.0.0-20160611221934-b7ed37b82869 // indirect
	github.com/chmduquesne/rollinghash v4.0.0+incompatible
	github.com/codahale/hdrhistogram v0.0.0-20161010025455-3a0bb77429bd // indirect
	github.com/containerd/continuity v0.0.0-20190426062206-aaeac12a7ffc // indirect
	github.com/coreos/bbolt v1.3.3
	github.com/coreos/etcd v3.3.13+incompatible
	github.com/coreos/go-etcd v2.0.0+incompatible
	github.com/coreos/go-semver v0.2.0 // indirect
	github.com/coreos/go-systemd v0.0.0-20190618135430-ff7011eec365 // indirect
	github.com/coreos/pkg v0.0.0-20180928190104-399ea9e2e55f // indirect
	github.com/cpuguy83/go-md2man v1.0.10 // indirect
	github.com/crewjam/saml v0.0.0-20190521120225-344d075952c9
	github.com/docker/distribution v2.7.1+incompatible // indirect
	github.com/docker/docker v0.7.3-0.20190621081258-52c16677b22d // indirect
	github.com/docker/go-units v0.4.0
	github.com/docker/libnetwork v0.8.0-dev.2.0.20190604154631-fc5a7d91d54c // indirect
	github.com/docker/spdystream v0.0.0-20160310174837-449fdfce4d96 // indirect
	github.com/elazarl/goproxy v0.0.0-20170405201442-c4fc26588b6e // indirect
	github.com/evanphx/json-patch v4.5.0+incompatible
	github.com/facebookgo/atomicfile v0.0.0-20151019160806-2de1f203e7d5 // indirect
	github.com/facebookgo/pidfile v0.0.0-20150612191647-f242e2999868
	github.com/fatih/camelcase v1.0.0
	github.com/fatih/color v1.7.0
	github.com/fatih/structs v1.1.0 // indirect
	github.com/fsouza/go-dockerclient v1.4.1
	github.com/ghodss/yaml v1.0.0
	github.com/go-ini/ini v1.42.0 // indirect
	github.com/go-test/deep v1.0.1 // indirect
	github.com/gobwas/glob v0.2.3 // indirect
	github.com/gogo/protobuf v1.2.1
	github.com/golang/groupcache v0.0.0-20190129154638-5b532d6fd5ef
	github.com/golang/protobuf v1.3.1
	github.com/golang/snappy v0.0.1
	github.com/google/go-github v17.0.0+incompatible
	github.com/google/go-querystring v1.0.0 // indirect
	github.com/google/gofuzz v1.0.0 // indirect
	github.com/googleapis/gnostic v0.2.0 // indirect
	github.com/gophercloud/gophercloud v0.2.0 // indirect
	github.com/gorilla/mux v1.7.2
	github.com/gorilla/websocket v1.4.0 // indirect
	github.com/gregjones/httpcache v0.0.0-20190611155906-901d90724c79 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.0.0 // indirect
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0 // indirect
	github.com/hanwen/go-fuse v0.0.0-20180522155540-291273cb8ce0
	github.com/hashicorp/go-cleanhttp v0.5.1 // indirect
	github.com/hashicorp/go-hclog v0.8.0 // indirect
	github.com/hashicorp/go-immutable-radix v1.1.0 // indirect
	github.com/hashicorp/go-plugin v1.0.1 // indirect
	github.com/hashicorp/go-retryablehttp v0.5.4 // indirect
	github.com/hashicorp/go-rootcerts v1.0.1 // indirect
	github.com/hashicorp/go-sockaddr v1.0.2 // indirect
	github.com/hashicorp/go-version v1.1.0 // indirect
	github.com/hashicorp/golang-lru v0.5.1
	github.com/hashicorp/hcl v1.0.0 // indirect
	github.com/hashicorp/vault v1.1.3
	github.com/hashicorp/yamux v0.0.0-20181012175058-2f1d1f20f75d // indirect
	github.com/imdario/mergo v0.3.7 // indirect
	github.com/inconshreveable/mousetrap v1.0.0 // indirect
	github.com/jehiah/go-strftime v0.0.0-20171201141054-1d33003b3869 // indirect
	github.com/jonboulle/clockwork v0.1.0 // indirect
	github.com/json-iterator/go v1.1.6 // indirect
	github.com/juju/ansiterm v0.0.0-20180109212912-720a0952cc2a
	github.com/julienschmidt/httprouter v1.2.0
	github.com/kisielk/errcheck v1.2.0 // indirect
	github.com/lunixbochs/vtclean v1.0.0 // indirect
	github.com/mgutz/ansi v0.0.0-20170206155736-9520e82c474b // indirect
	github.com/minio/minio-go v6.0.14+incompatible
	github.com/mitchellh/go-testing-interface v1.0.0 // indirect
	github.com/mitchellh/mapstructure v1.1.2 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.1 // indirect
	github.com/montanaflynn/stats v0.5.0
	github.com/onsi/ginkgo v1.7.0 // indirect
	github.com/onsi/gomega v1.4.3 // indirect
	github.com/opentracing-contrib/go-grpc v0.0.0-20180928155321-4b5a12d3ff02
	github.com/opentracing/opentracing-go v1.1.0
	github.com/pachyderm/glob v0.0.0-20190626005209-0fc448624fad
	github.com/peterbourgon/diskv v2.0.1+incompatible // indirect
	github.com/prometheus/client_golang v0.9.3-0.20190127221311-3c4408c8b829
	github.com/prometheus/client_model v0.0.0-20190129233127-fd36f4220a90 // indirect
	github.com/prometheus/common v0.2.0
	github.com/prometheus/procfs v0.0.2 // indirect
	github.com/robfig/cron v1.2.0
	github.com/russellhaering/goxmldsig v0.0.0-20180430223755-7acd5e4a6ef7 // indirect
	github.com/ryanuber/go-glob v1.0.0 // indirect
	github.com/satori/go.uuid v1.2.0
	github.com/segmentio/analytics-go v0.0.0-20160426181448-2d840d861c32
	github.com/segmentio/backo-go v0.0.0-20160424052352-204274ad699c // indirect
	github.com/segmentio/kafka-go v0.2.4
	github.com/sirupsen/logrus v1.3.0
	github.com/smartystreets/goconvey v0.0.0-20190330032615-68dc04aab96a // indirect
	github.com/soheilhy/cmux v0.1.4 // indirect
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	github.com/spf13/cobra v0.0.0-20160607124346-1238ba19d24b
	github.com/spf13/pflag v1.0.3
	github.com/tmc/grpc-websocket-proxy v0.0.0-20190109142713-0ad062ec5ee5 // indirect
	github.com/uber-go/atomic v1.4.0 // indirect
	github.com/uber/jaeger-client-go v2.16.0+incompatible
	github.com/uber/jaeger-lib v2.0.0+incompatible // indirect
	github.com/vishvananda/netlink v0.0.0-20171020171820-b2de5d10e38e // indirect
	github.com/vishvananda/netns v0.0.0-20180720170159-13995c7128cc // indirect
	github.com/willf/bitset v1.1.10 // indirect
	github.com/willf/bloom v2.0.3+incompatible
	github.com/x-cray/logrus-prefixed-formatter v0.5.2
	github.com/xiang90/probing v0.0.0-20190116061207-43a291ad63a2 // indirect
	github.com/xtgo/uuid v0.0.0-20140804021211-a0b114877d4c // indirect
	go.etcd.io/bbolt v1.3.3 // indirect
	go.uber.org/atomic v1.4.0 // indirect
	go.uber.org/multierr v1.1.0 // indirect
	go.uber.org/zap v1.10.0 // indirect
	golang.org/x/crypto v0.0.0-20190621222207-cc06ce4a13d4
	golang.org/x/net v0.0.0-20190620200207-3b0461eec859
	golang.org/x/oauth2 v0.0.0-20190604053449-0f29369cfe45
	golang.org/x/sync v0.0.0-20190423024810-112230192c58
	golang.org/x/sys v0.0.0-20190626221950-04f50cda93cb // indirect
	golang.org/x/time v0.0.0-20190308202827-9d24e82272b4 // indirect
	golang.org/x/tools v0.0.0-20190627220010-94c5763a7c84 // indirect
	google.golang.org/api v0.6.0
	google.golang.org/grpc v1.21.1
	gopkg.in/go-playground/webhooks.v5 v5.11.0
	gopkg.in/inf.v0 v0.9.1 // indirect
	gopkg.in/ini.v1 v1.42.0 // indirect
	gopkg.in/square/go-jose.v2 v2.3.1 // indirect
	gopkg.in/src-d/go-git.v4 v4.12.0
	k8s.io/api v0.0.0-20181130031204-d04500c8c3dd
	k8s.io/apimachinery v0.0.0-20181128191346-49ce2735e507
	k8s.io/client-go v0.0.0-20181213230135-6924ba6dfc02
	k8s.io/klog v0.3.1 // indirect
	sigs.k8s.io/yaml v1.1.0 // indirect
)

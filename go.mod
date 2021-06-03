module github.com/pachyderm/pachyderm

go 1.15

require (
	cloud.google.com/go/storage v1.6.0
	github.com/Azure/azure-sdk-for-go v36.1.0+incompatible
	github.com/Azure/go-autorest/autorest/to v0.4.0 // indirect
	github.com/HdrHistogram/hdrhistogram-go v1.1.0 // indirect
	github.com/OneOfOne/xxhash v1.2.6
	github.com/aws/aws-lambda-go v1.13.3
	github.com/aws/aws-sdk-go v1.27.0
	github.com/beevik/etree v1.1.0
	github.com/brianvoe/gofakeit v3.18.0+incompatible
	github.com/c-bata/go-prompt v0.2.3
	github.com/cevaris/ordered_map v0.0.0-20190319150403-3adeae072e73
	github.com/chmduquesne/rollinghash v4.0.0+incompatible
	github.com/coreos/bbolt v1.3.3
	github.com/coreos/etcd v3.3.13+incompatible
	github.com/coreos/go-etcd v2.0.0+incompatible
	github.com/coreos/go-oidc v2.2.1+incompatible
	github.com/coreos/pkg v0.0.0-20180928190104-399ea9e2e55f
	github.com/crewjam/saml v0.4.4-0.20201214083806-0dd2422c212e
	github.com/dlclark/regexp2 v1.2.0 // indirect
	github.com/docker/go-units v0.4.0
	github.com/docker/spdystream v0.0.0-20181023171402-6480d4af844c // indirect
	github.com/elazarl/goproxy v0.0.0-20191011121108-aa519ddbe484 // indirect
	github.com/evanphx/json-patch v4.9.0+incompatible
	github.com/fatih/camelcase v1.0.0
	github.com/fatih/color v1.9.0
	github.com/fatih/structs v1.1.0 // indirect
	github.com/fsouza/go-dockerclient v1.4.1
	github.com/go-test/deep v1.0.1 // indirect
	github.com/gofrs/uuid v3.3.0+incompatible // indirect
	github.com/gogo/protobuf v1.3.1
	github.com/golang/groupcache v0.0.0-20200121045136-8c9f03a8e57e
	github.com/golang/protobuf v1.4.3
	github.com/golang/snappy v0.0.1
	github.com/google/go-github v17.0.0+incompatible
	github.com/google/go-querystring v1.1.0 // indirect
	github.com/gorilla/mux v1.7.4
	github.com/hanwen/go-fuse/v2 v2.0.3
	github.com/hashicorp/go-hclog v0.8.0 // indirect
	github.com/hashicorp/go-plugin v1.0.1 // indirect
	github.com/hashicorp/go-retryablehttp v0.5.4 // indirect
	github.com/hashicorp/golang-lru v0.5.3
	github.com/hashicorp/vault v1.1.3
	github.com/hashicorp/yamux v0.0.0-20181012175058-2f1d1f20f75d // indirect
	github.com/itchyny/gojq v0.11.2
	github.com/jehiah/go-strftime v0.0.0-20171201141054-1d33003b3869 // indirect
	github.com/jmoiron/sqlx v1.2.0
	github.com/jonboulle/clockwork v0.2.2 // indirect
	github.com/juju/ansiterm v0.0.0-20180109212912-720a0952cc2a
	github.com/julienschmidt/httprouter v1.3.0
	github.com/lib/pq v1.9.0
	github.com/lunixbochs/vtclean v1.0.0 // indirect
	github.com/mattn/go-isatty v0.0.12
	github.com/mattn/go-sqlite3 v2.0.3+incompatible // indirect
	github.com/mattn/go-tty v0.0.3 // indirect
	github.com/mgutz/ansi v0.0.0-20200706080929-d51e80ef957d // indirect
	github.com/minio/minio-go/v6 v6.0.55
	github.com/onsi/gomega v1.8.1 // indirect
	github.com/opentracing-contrib/go-grpc v0.0.0-20180928155321-4b5a12d3ff02
	github.com/opentracing/opentracing-go v1.1.1-0.20200124165624-2876d2018785
	github.com/pachyderm/ohmyglob v0.0.0-20210308211843-d5b47775fc36
	github.com/pachyderm/s2 v0.0.0-20200609183354-d52f35094520
	github.com/pkg/browser v0.0.0-20180916011732-0a3d74bf9ce4
	github.com/pkg/errors v0.9.1
	github.com/pkg/term v0.0.0-20190109203006-aa71e9d9e942 // indirect
	github.com/pquerna/cachecontrol v0.0.0-20180517163645-1555304b9b35 // indirect
	github.com/prometheus/client_golang v1.7.1
	github.com/prometheus/client_model v0.2.0
	github.com/prometheus/common v0.10.0
	github.com/remyoudompheng/bigfft v0.0.0-20200410134404-eec4a21b6bb0 // indirect
	github.com/robfig/cron v1.2.0
	github.com/ryanuber/go-glob v1.0.0 // indirect
	github.com/satori/go.uuid v1.2.0
	github.com/segmentio/analytics-go v0.0.0-20160426181448-2d840d861c32
	github.com/segmentio/backo-go v0.0.0-20160424052352-204274ad699c // indirect
	github.com/sirupsen/logrus v1.8.1
	github.com/spf13/cobra v1.1.3
	github.com/spf13/pflag v1.0.5
	github.com/stretchr/testify v1.7.0
	github.com/uber/jaeger-client-go v2.20.1+incompatible
	github.com/uber/jaeger-lib v2.4.1+incompatible // indirect
	github.com/vbauerster/mpb/v6 v6.0.2
	github.com/willf/bitset v1.1.10 // indirect
	github.com/willf/bloom v2.0.3+incompatible
	github.com/x-cray/logrus-prefixed-formatter v0.5.2
	github.com/xtgo/uuid v0.0.0-20140804021211-a0b114877d4c // indirect
	go.uber.org/automaxprocs v1.4.0
	golang.org/x/lint v0.0.0-20201208152925-83fdc39ff7b5 // indirect
	golang.org/x/net v0.0.0-20201110031124-69a78807bb2b
	golang.org/x/oauth2 v0.0.0-20200107190931-bf48bf16ab8d
	golang.org/x/sync v0.0.0-20201207232520-09787c993a3a
	golang.org/x/sys v0.0.0-20210124154548-22da62e12c0c
	golang.org/x/term v0.0.0-20201117132131-f5c789dd3221
	google.golang.org/api v0.20.0
	google.golang.org/appengine v1.6.6 // indirect
	google.golang.org/grpc v1.36.1
	gopkg.in/go-playground/webhooks.v5 v5.11.0
	gopkg.in/pachyderm/yaml.v3 v3.0.0-20200130061037-1dd3d7bd0850
	gopkg.in/square/go-jose.v2 v2.5.1 // indirect
	gopkg.in/src-d/go-git.v4 v4.12.0
	helm.sh/helm/v3 v3.5.4
	honnef.co/go/tools v0.1.3 // indirect
	k8s.io/api v0.20.4
	k8s.io/apimachinery v0.20.4
	k8s.io/cli-runtime v0.20.4
	k8s.io/client-go v0.20.4
	modernc.org/mathutil v1.0.0
	sigs.k8s.io/yaml v1.2.0
)

replace github.com/golang/groupcache => github.com/golang/groupcache v0.0.0-20191027212112-611e8accdfc9

// see https://github.com/moby/moby/issues/39302
// For the moment, the windows build requires a fix that has not been tagged with an official release
replace github.com/docker/docker => github.com/docker/docker v1.4.2-0.20191213113251-3452f136aa68

replace github.com/Azure/go-autorest => github.com/Azure/go-autorest v14.2.0+incompatible

//For etcd
replace google.golang.org/grpc => google.golang.org/grpc v1.29.1

//For helm
replace github.com/deislabs/oras => github.com/deislabs/oras v0.11.1

//For Helm
replace github.com/docker/distribution => github.com/docker/distribution v0.0.0-20191216044856-a8371794149d

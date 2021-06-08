#### VARIABLES
# TESTFLAGS: flags for test
# KUBECTLFLAGS: flags for kubectl
# DOCKER_BUILD_FLAGS: flags for 'docker build'
####

include etc/govars.mk

SHELL=/bin/bash -o pipefail
RUN= # used by go tests to decide which tests to run (i.e. passed to -run)
# Don't set the version to the git hash in CI, as it breaks the go build cache.
ifdef CIRCLE_BRANCH
	export VERSION_ADDITIONAL = -CIbuild
	export GC_FLAGS = ""
else
	export VERSION_ADDITIONAL = -$(shell git log --pretty=format:%H | head -n 1)
	export GC_FLAGS = "all=-trimpath=${PWD}"
endif

export CLIENT_ADDITIONAL_VERSION=github.com/pachyderm/pachyderm/v2/src/version.AdditionalVersion=$(VERSION_ADDITIONAL)
export LD_FLAGS=-X $(CLIENT_ADDITIONAL_VERSION)
export DOCKER_BUILD_FLAGS

CLUSTER_NAME ?= pachyderm
CLUSTER_MACHINE_TYPE ?= n1-standard-4
CLUSTER_SIZE ?= 4

MINIKUBE_MEM = 8192 # MB of memory allocated to minikube
MINIKUBE_CPU = 4 # Number of CPUs allocated to minikube

CHLOGFILE = ${PWD}/../changelog.diff
export GOVERSION = $(shell cat etc/compile/GO_VERSION)
GORELSNAP = #--snapshot # uncomment --snapshot if you want to do a dry run.
SKIP = #\# # To skip push to docker and github remove # in front of #
GORELDEBUG = #--debug # uncomment --debug for verbose goreleaser output

# Default upper bound for test timeouts
# You can specify your own, but this is what CI uses
TIMEOUT ?= 3600s

install:
	# GOBIN (default: GOPATH/bin) must be on your PATH to access these binaries:
	go install -ldflags "$(LD_FLAGS)" -gcflags "$(GC_FLAGS)" ./src/server/cmd/pachctl

install-clean:
	@# Need to blow away pachctl binary if its already there
	@rm -f $(PACHCTL)
	@make install

install-doc:
	go install -gcflags "$(GC_FLAGS)" ./src/server/cmd/pachctl-doc

doc-custom: install-doc install-clean
	./etc/build/doc.sh

doc-reference-refresh: install-doc install-clean
	./etc/build/reference_refresh.sh

doc:
	@make VERSION_ADDITIONAL= doc-custom

point-release:
	@./etc/build/make_changelog.sh $(CHLOGFILE)
	@VERSION_ADDITIONAL= ./etc/build/make_release.sh
	@echo "Release completed"

# Run via 'make VERSION_ADDITIONAL=-rc2 release-candidate' to specify a version string
release-candidate:
	@make custom-release

custom-release:
	echo "" > $(CHLOGFILE)
	@VERSION_ADDITIONAL=$(VERSION_ADDITIONAL) ./etc/build/make_release.sh "Custom"
	# Need to check for homebrew updates from release-pachctl-custom

# This is getting called from etc/build/make_release.sh
# Git tag is force pushed. We are assuming if the same build is done again, it is done with intent
release:
	@git tag -f -am "Release tag v$(VERSION)" v$(VERSION)
	$(SKIP) @git push -f origin v$(VERSION)
	@make release-helper
	@make release-pachctl
	@echo "Release $(VERSION) completed"

release-helper: release-docker-images docker-push docker-push-pipeline-build

release-docker-images:
	DOCKER_BUILDKIT=1 goreleaser release -p 1 $(GORELSNAP) $(GORELDEBUG) --skip-publish --rm-dist -f goreleaser/docker.yml
	DOCKER_BUILDKIT=1 goreleaser release -p 1 $(GORELSNAP) $(GORELDEBUG) --skip-publish --rm-dist -f goreleaser/docker-build-pipelines.yml

release-pachctl:
	@goreleaser release -p 1 $(GORELSNAP) $(GORELDEBUG) --release-notes=$(CHLOGFILE) --rm-dist -f goreleaser/pachctl.yml

docker-build:
	DOCKER_BUILDKIT=1 goreleaser release -p 1 --snapshot $(GORELDEBUG) --skip-publish --rm-dist -f goreleaser/docker.yml

docker-build-pipeline-build: install
	VERSION=$$($(PACHCTL) version --client-only) DOCKER_BUILDKIT=1 \
	  goreleaser release -p 1 --snapshot $(GORELDEBUG) --skip-publish --rm-dist -f goreleaser/docker-build-pipelines.yml

docker-build-proto:
	docker build $(DOCKER_BUILD_FLAGS) -t pachyderm_proto etc/proto

docker-build-python-proto:
	docker build $(DOCKER_BUILD_FLAGS) -t pachyderm_python_proto etc/python

docker-build-netcat:
	docker build $(DOCKER_BUILD_FLAGS) -t pachyderm_netcat etc/netcat

docker-build-gpu:
	docker build $(DOCKER_BUILD_FLAGS) -t pachyderm_nvidia_driver_install etc/deploy/gpu
	docker tag pachyderm_nvidia_driver_install pachyderm/nvidia_driver_install

docker-build-kafka:
	docker build -t kafka-demo etc/testing/kafka

docker-build-spout-test:
	docker build -t spout-test etc/testing/spout

docker-push-gpu:
	$(SKIP) docker push pachyderm/nvidia_driver_install

docker-push-gpu-dev:
	docker tag pachyderm/nvidia_driver_install pachyderm/nvidia_driver_install:`git rev-list HEAD --max-count=1`
	$(SKIP) docker push pachyderm/nvidia_driver_install:`git rev-list HEAD --max-count=1`
	echo pushed pachyderm/nvidia_driver_install:`git rev-list HEAD --max-count=1`

docker-gpu: docker-build-gpu docker-push-gpu

docker-gpu-dev: docker-build-gpu docker-push-gpu-dev

docker-build-test-entrypoint:
	docker build $(DOCKER_BUILD_FLAGS) -t pachyderm_entrypoint etc/testing/entrypoint

docker-tag:
	docker tag pachyderm/pachd pachyderm/pachd:$(VERSION)
	docker tag pachyderm/worker pachyderm/worker:$(VERSION)
	docker tag pachyderm/pachctl pachyderm/pachctl:$(VERSION)

docker-push: docker-tag
	$(SKIP) docker push pachyderm/etcd:v3.3.5
	$(SKIP) docker push pachyderm/pachd:$(VERSION)
	$(SKIP) docker push pachyderm/worker:$(VERSION)
	$(SKIP) docker push pachyderm/pachctl:$(VERSION)

docker-push-pipeline-build: install
	$(SKIP) ls etc/pipeline-build | xargs -I {} docker push pachyderm/{}-build:$$($(PACHCTL) version --client-only)

docker-push-pipeline-build-to-minikube: install
	$(SKIP) ls etc/pipeline-build | xargs -I {} etc/kube/push-to-minikube.sh pachyderm/{}-build:$$($(PACHCTL) version --client-only)

check-kubectl:
	@# check that kubectl is installed
	@which kubectl >/dev/null || { \
		echo "error: kubectl not found"; \
		exit 1; \
	}
	@if expr match $(shell kubectl config current-context) gke_pachub > /dev/null; then \
		echo "ERROR: The active kubectl context is pointing to a pachub GKE cluster"; \
		exit 1; \
	fi

check-kubectl-connection:
	kubectl $(KUBECTLFLAGS) get all > /dev/null

launch-kube: check-kubectl
	etc/kube/start-minikube.sh

launch-dev-vm: check-kubectl
	# Making sure minikube isn't still up from a previous run...
	@if minikube ip 2>/dev/null || sudo minikube ip 2>/dev/null; \
	then \
	  echo "minikube is still up. Run 'make clean-launch-kube'"; \
	  exit 1; \
	fi
	etc/kube/start-minikube-vm.sh --cpus=$(MINIKUBE_CPU) --memory=$(MINIKUBE_MEM)

# launch-release-vm is like launch-dev-vm but it doesn't build pachctl locally, and uses the same
# version of pachd associated with the current pachctl (useful if you want to start a VM with a
# point-release version of pachd, instead of whatever's in the current branch)
launch-release-vm:
	# Making sure minikube isn't still up from a previous run...
	@if minikube ip 2>/dev/null || sudo minikube ip 2>/dev/null; \
	then \
	  echo "minikube is still up. Run 'make clean-launch-kube'"; \
	  exit 1; \
	fi
	etc/kube/start-minikube-vm.sh --cpus=$(MINIKUBE_CPU) --memory=$(MINIKUBE_MEM) --tag=v$$($(PACHCTL) version --client-only)

clean-launch-kube:
	@# clean up both of the following cases:
	@# make launch-dev-vm - minikube config is owned by $USER
	@# make launch-kube - minikube config is owned by root
	minikube ip 2>/dev/null && minikube delete || true
	sudo minikube ip 2>/dev/null && sudo minikube delete || true
	killall kubectl || true

launch: install check-kubectl
	$(eval STARTTIME := $(shell date +%s))
	$(PACHCTL) deploy local --dry-run | kubectl $(KUBECTLFLAGS) apply -f -
	# wait for the pachyderm to come up
	kubectl wait --for=condition=ready pod -l app=pachd --timeout=5m
	@echo "pachd launch took $$(($$(date +%s) - $(STARTTIME))) seconds"

launch-dev: check-kubectl check-kubectl-connection install
	$(eval STARTTIME := $(shell date +%s))
	$(PACHCTL) deploy local --no-guaranteed -d --dry-run $(LAUNCH_DEV_ARGS) | kubectl $(KUBECTLFLAGS) apply -f -
	# wait for the pachyderm to come up
	kubectl wait --for=condition=ready pod -l app=pachd --timeout=5m
	@echo "pachd launch took $$(($$(date +%s) - $(STARTTIME))) seconds"

launch-enterprise: check-kubectl check-kubectl-connection install
	$(eval STARTTIME := $(shell date +%s))
	kubectl create namespace enterprise --dry-run=true -o yaml | kubectl apply -f -
	$(PACHCTL) deploy local --no-guaranteed -d --enterprise-server --namespace enterprise  --pachd-memory-request 128M --postgres-memory-request 128M --etcd-memory-request 128M --pachd-cpu-request 100m --postgres-cpu-request 100m --etcd-cpu-request 100m --dry-run $(LAUNCH_DEV_ARGS) | kubectl $(KUBECTLFLAGS) apply -f -
	# wait for the pachyderm to come up
	kubectl wait --for=condition=ready pod -l app=pach-enterprise --namespace enterprise --timeout=5m
	@echo "pachd launch took $$(($$(date +%s) - $(STARTTIME))) seconds"

clean-launch: check-kubectl install
	yes | $(PACHCTL) undeploy

clean-launch-dev: check-kubectl install
	yes | $(PACHCTL) undeploy

full-clean-launch: check-kubectl
	kubectl $(KUBECTLFLAGS) delete --ignore-not-found job -l suite=pachyderm
	kubectl $(KUBECTLFLAGS) delete --ignore-not-found all -l suite=pachyderm
	kubectl $(KUBECTLFLAGS) delete --ignore-not-found serviceaccount -l suite=pachyderm
	kubectl $(KUBECTLFLAGS) delete --ignore-not-found secret -l suite=pachyderm

test-proto-static:
	./etc/proto/test_no_changes.sh || echo "Protos need to be recompiled; run 'DOCKER_BUILD_FLAGS=--no-cache make proto'."

test-deploy-manifests: install
	./etc/testing/deploy-manifests/validate.sh

regenerate-test-deploy-manifests: install
	./etc/testing/deploy-manifests/validate.sh --regenerate

proto: docker-build-proto
	./etc/proto/build.sh

python-proto: docker-build-python-proto
	rm -rf etc/python/src/python_pachyderm
	find src -regex ".*\.proto" | grep -v 'internal' | grep -v 'server' \
	| xargs tar cf - \
	| docker run -i pachyderm_python_proto \
	| tar -C etc/python/src -xf -

# Run all the tests. Note! This is no longer the test entrypoint for travis
test: clean-launch-dev launch-dev lint enterprise-code-checkin-test docker-build test-pfs-server test-cmds test-libs test-auth test-identity test-license test-enterprise test-worker test-admin test-pps

enterprise-code-checkin-test:
	@which ag || { printf "'ag' not found. Run:\n  sudo apt-get install -y silversearcher-ag\n  brew install the_silver_searcher\nto install it\n\n"; exit 1; }
	# Check if our test activation code is anywhere in the repo
	@echo "Files containing test Pachyderm Enterprise activation token:"; \
	if ag --ignore=Makefile -p .gitignore 'RM2o1Qit6YlZhS1RGdXVac'; \
	then \
	  $$( which echo ) -e "\n*** It looks like Pachyderm Engineering's test activation code may be in this repo. Please remove it before committing! ***\n"; \
	  false; \
	fi

test-postgres:
	./etc/testing/start_postgres.sh

test-pfs-server: test-postgres
	./etc/testing/pfs_server.sh $(TIMEOUT) $(TESTFLAGS)

test-pfs-storage: test-postgres
	go test -count=1 ./src/internal/storage/... -timeout $(TIMEOUT) $(TESTFLAGS)
	go test -count=1 ./src/internal/migrations/... $(TESTFLAGS)

test-pps: launch-stats docker-build-spout-test docker-build-test-entrypoint
	@# Use the count flag to disable test caching for this test suite.
	PROM_PORT=$$(kubectl --namespace=monitoring get svc/prometheus -o json | jq -r .spec.ports[0].nodePort) \
	  go test -v -count=1 ./src/server -parallel 1 -timeout $(TIMEOUT) $(RUN) $(TESTFLAGS)

test-cmds:
	go install -v ./src/testing/match
	CGOENABLED=0 go test -v -count=1 ./src/server/cmd/pachctl/cmd
	go test -v -count=1 ./src/internal/deploy/cmds -timeout $(TIMEOUT) $(TESTFLAGS)
	go test -v -count=1 ./src/server/pfs/cmds -timeout $(TIMEOUT) $(TESTFLAGS)
	go test -v -count=1 ./src/server/pps/cmds -timeout $(TIMEOUT) $(TESTFLAGS)
	go test -v -count=1 ./src/server/config -timeout $(TIMEOUT) $(TESTFLAGS)
	@# TODO(msteffen) does this test leave auth active? If so it must run last
	go test -v -count=1 ./src/server/auth/cmds -timeout $(TIMEOUT) $(TESTFLAGS)
	go test -v -count=1 ./src/server/enterprise/cmds -timeout $(TIMEOUT) $(TESTFLAGS)
	go test -v -count=1 ./src/server/identity/cmds -timeout $(TIMEOUT) $(TESTFLAGS)

test-transaction:
	go test -count=1 ./src/server/transaction/server/testing -timeout $(TIMEOUT) $(TESTFLAGS)

test-client:
	go test -count=1 -cover $$(go list ./src/client/...) $(TESTFLAGS)

test-object-clients:
	# The parallelism is lowered here because these tests run several pachd
	# deployments in kubernetes which may contest resources.
	go test -count=1 ./src/internal/obj/integrationtests -timeout $(TIMEOUT) -parallel=2 $(TESTFLAGS)
	go test -count=1 ./src/internal/obj -timeout $(TIMEOUT) $(TESTFLAGS)

test-libs:
	go test -count=1 ./src/internal/grpcutil -timeout $(TIMEOUT) $(TESTFLAGS)
	go test -count=1 ./src/internal/collection -timeout $(TIMEOUT) -vet=off $(TESTFLAGS)
	go test -count=1 ./src/internal/cert -timeout $(TIMEOUT) $(TESTFLAGS)
	go test -count=1 ./src/internal/work -timeout $(TIMEOUT) $(TESTFLAGS)

test-s3gateway-conformance:
	@if [ -z $$CONFORMANCE_SCRIPT_PATH ]; then \
	  echo "Missing environment variable 'CONFORMANCE_SCRIPT_PATH'"; \
	  exit 1; \
	fi
	$(CONFORMANCE_SCRIPT_PATH) --s3tests-config=etc/testing/s3gateway/s3tests.conf --ignore-config=etc/testing/s3gateway/ignore.conf --runs-dir=etc/testing/s3gateway/runs

test-s3gateway-integration:
	@if [ -z $$INTEGRATION_SCRIPT_PATH ]; then \
	  echo "Missing environment variable 'INTEGRATION_SCRIPT_PATH'"; \
	  exit 1; \
	fi
	$(INTEGRATION_SCRIPT_PATH) http://localhost:30600 --access-key=none --secret-key=none

test-s3gateway-unit: test-postgres
	go test -v -count=1 ./src/server/pfs/s3 -timeout $(TIMEOUT) $(TESTFLAGS)

test-fuse:
	CGOENABLED=0 go test -count=1 -cover $$(go list ./src/server/... | grep '/src/server/pfs/fuse') $(TESTFLAGS)

test-local:
	CGOENABLED=0 go test -count=1 -cover -short $$(go list ./src/server/... | grep -v '/src/server/pfs/fuse') -timeout $(TIMEOUT) $(TESTFLAGS)

test-auth:
	go test -v -count=1 ./src/server/auth/server/testing -timeout $(TIMEOUT) $(RUN) $(TESTFLAGS)

test-identity:
	etc/testing/forward-postgres.sh
	go test -v -count=1 ./src/server/identity/server -timeout $(TIMEOUT) $(RUN) $(TESTFLAGS)

test-license:
	go test -v -count=1 ./src/server/license/server -timeout $(TIMEOUT) $(RUN) $(TESTFLAGS)

test-admin:
	go test -v -count=1 ./src/server/admin/server -timeout $(TIMEOUT) $(RUN) $(TESTFLAGS)

test-enterprise:
	go test -v -count=1 ./src/server/enterprise/server -timeout $(TIMEOUT) $(TESTFLAGS)

test-enterprise-integration:
	go install ./src/testing/match
	go test -v -count=1 ./src/server/enterprise/testing -timeout $(TIMEOUT) $(TESTFLAGS)

test-tls:
	./etc/testing/test_tls.sh

test-worker: launch-stats test-worker-helper

test-worker-helper:
	PROM_PORT=$$(kubectl --namespace=monitoring get svc/prometheus -o json | jq -r .spec.ports[0].nodePort) \
	  go test -v -count=1 ./src/server/worker/ -timeout $(TIMEOUT) $(TESTFLAGS)

clean: clean-launch clean-launch-kube

compatibility:
	./etc/build/compatibility.sh

clean-launch-kafka:
	kubectl delete -f etc/kubernetes-kafka -R

launch-kafka:
	kubectl apply -f etc/kubernetes-kafka -R
	kubectl wait --for=condition=ready pod -l app=kafka --timeout=5m
	
clean-launch-stats:
	kubectl delete --filename etc/kubernetes-prometheus -R

launch-stats:
	kubectl apply --filename etc/kubernetes-prometheus -R

clean-launch-monitoring:
	kubectl delete --ignore-not-found -f ./etc/plugin/monitoring

launch-monitoring:
	kubectl create -f ./etc/plugin/monitoring
	@echo "Waiting for services to spin up ..."
	kubectl wait --for=condition=ready pod -l k8s-app=heapster --namespace kube-system --timeout=5m
	kubectl wait --for=condition=ready pod -l k8s-app=influxdb --namespace kube-system --timeout=5m
	kubectl wait --for=condition=ready pod -l k8s-app=grafana --namespace kube-system --timeout=5m
	@echo "All services up. Now port forwarding grafana to localhost:3000"
	kubectl --namespace=kube-system port-forward `kubectl --namespace=kube-system get pods -l k8s-app=grafana -o json | jq '.items[0].metadata.name' -r` 3000:3000 &

clean-launch-logging: check-kubectl check-kubectl-connection
	git submodule update --init
	cd etc/plugin/logging && ./undeploy.sh

launch-logging: check-kubectl check-kubectl-connection
	@# Creates Fluentd / Elasticsearch / Kibana services for logging under --namespace=monitoring
	git submodule update --init
	cd etc/plugin/logging && ./deploy.sh
	kubectl --namespace=monitoring port-forward `kubectl --namespace=monitoring get pods -l k8s-app=kibana-logging -o json | jq '.items[0].metadata.name' -r` 35601:5601 &

launch-loki:
	helm repo remove loki || true
	helm repo add loki https://grafana.github.io/loki/charts
	helm repo update
	helm upgrade --install loki loki/loki-stack
	kubectl wait --for=condition=ready pod -l release=loki --timeout=5m

clean-launch-loki:
	helm uninstall loki

logs: check-kubectl
	kubectl $(KUBECTLFLAGS) get pod -l app=pachd | sed '1d' | cut -f1 -d ' ' | xargs -n 1 -I pod sh -c 'echo pod && kubectl $(KUBECTLFLAGS) logs pod'

follow-logs: check-kubectl
	kubectl $(KUBECTLFLAGS) get pod -l app=pachd | sed '1d' | cut -f1 -d ' ' | xargs -n 1 -I pod sh -c 'echo pod && kubectl $(KUBECTLFLAGS) logs -f pod'

google-cluster-manifest:
	@$(PACHCTL) deploy --dry-run google $(BUCKET_NAME) $(STORAGE_NAME) $(STORAGE_SIZE)

google-cluster:
	gcloud container clusters create $(CLUSTER_NAME) --scopes storage-rw --machine-type $(CLUSTER_MACHINE_TYPE) --num-nodes $(CLUSTER_SIZE)
	gcloud config set container/cluster $(CLUSTER_NAME)
	gcloud container clusters get-credentials $(CLUSTER_NAME)
	gcloud components install kubectl
	-gcloud compute firewall-rules create pachd --allow=tcp:30650
	gsutil mb gs://$(BUCKET_NAME) # for PFS
	gcloud compute disks create --size=$(STORAGE_SIZE)GB $(STORAGE_NAME) # for PPS

clean-google-cluster:
	gcloud container clusters delete $(CLUSTER_NAME)
	gcloud compute firewall-rules delete pachd
	gsutil -m rm -r gs://$(BUCKET_NAME)
	gcloud compute disks delete $(STORAGE_NAME)

amazon-cluster-manifest: install
	@$(PACHCTL) deploy --dry-run amazon $(BUCKET_NAME) $(AWS_ID) $(AWS_KEY) $(AWS_TOKEN) $(AWS_REGION) $(STORAGE_NAME) $(STORAGE_SIZE)

amazon-cluster:
	aws s3api create-bucket --bucket $(BUCKET_NAME) --region $(AWS_REGION)
	aws ec2 create-volume --size $(STORAGE_SIZE) --region $(AWS_REGION) --availability-zone $(AWS_AVAILABILITY_ZONE) --volume-type gp2

amazon-clean-cluster:
	aws s3api delete-bucket --bucket $(BUCKET_NAME) --region $(AWS_REGION)
	aws ec2 detach-volume --force --volume-id $(STORAGE_NAME)
	sleep 20
	aws ec2 delete-volume --volume-id $(STORAGE_NAME)

amazon-clean-launch: clean-launch
	kubectl $(KUBECTLFLAGS) delete --ignore-not-found secrets amazon-secret

amazon-clean:
	@while :; \
        do if echo "The following script will delete your AWS bucket and volume. The action cannot be undone. Do you want to proceed? (Y/n)";read REPLY; then \
        case $$REPLY in Y|y) make amazon-clean-launch;make amazon-clean-cluster;break;; \
	N|n) echo "The amazon clean process has been cancelled by user!";break;; \
	*) echo "input parameter error, please input again ";continue;;esac; \
        fi;done;

microsoft-cluster-manifest:
	@$(PACHCTL) deploy --dry-run microsoft $(CONTAINER_NAME) $(AZURE_STORAGE_NAME) $(AZURE_STORAGE_KEY) $(VHD_URI) $(STORAGE_SIZE)

microsoft-cluster:
	azure group create --name $(AZURE_RESOURCE_GROUP) --location $(AZURE_LOCATION)
	azure storage account create $(AZURE_STORAGE_NAME) --location $(AZURE_LOCATION) --resource-group $(AZURE_RESOURCE_GROUP) --sku-name LRS --kind Storage

clean-microsoft-cluster:
	azure group delete $(AZURE_RESOURCE_GROUP) -q

lint:
	etc/testing/lint.sh

spellcheck:
	@mdspell doc/*.md doc/**/*.md *.md --en-us --ignore-numbers --ignore-acronyms --report --no-suggestions

check-buckets:
	./etc/testing/circle/check_buckets.sh

.PHONY: \
	install \
	install-clean \
	install-doc \
	doc-custom \
	doc \
	point-release \
	release-candidate \
	custom-release \
	release \
	release-helper \
	release-docker-images \
	release-pachctl \
	docker-build \
	docker-build-pipeline-build \
	docker-build-proto \
	docker-build-netcat \
	docker-build-gpu \
	docker-build-kafka \
	docker-build-spout-test \
	docker-push-gpu \
	docker-push-gpu-dev \
	docker-gpu \
	docker-gpu-dev \
	docker-build-test-entrypoint \
	docker-tag \
	docker-push \
	docker-push-pipeline-build \
	check-buckets \
	check-kubectl \
	check-kubectl-connection \
	launch-kube \
	launch-dev-vm \
	launch-release-vm \
	clean-launch-kube \
	launch \
	launch-dev \
	clean-launch \
	clean-launch-dev \
	full-clean-launch \
	test-proto-static \
	test-deploy-manifests \
	regenerate-test-deploy-manifests \
	proto \
	test \
	enterprise-code-checkin-test \
	test-pfs-server \
	test-pfs-storage \
	test-pps \
	test-cmds \
	test-transaction \
	test-client \
	test-libs \
	test-s3gateway-conformance \
	test-s3gateway-integration \
	test-s3gateway-unit \
	test-fuse \
	test-local \
	test-auth \
	test-identity \
	test-admin \
	test-enterprise \
	test-tls \
	test-worker \
	test-worker-helper \
	clean \
	compatibility \
	clean-launch-kafka \
	launch-kafka \
	clean-launch-stats \
	launch-stats \
	clean-launch-monitoring \
	launch-monitoring \
	clean-launch-logging \
	launch-logging \
	launch-loki \
	clean-launch-loki \
	launch-dex \
	clean-launch-dex \
	launch-enterprise \
	logs \
	follow-logs \
	google-cluster-manifest \
	google-cluster \
	clean-google-cluster \
	amazon-cluster-manifest \
	amazon-cluster \
	amazon-clean-cluster \
	amazon-clean-launch \
	amazon-clean \
	microsoft-cluster-manifest \
	microsoft-cluster \
	clean-microsoft-cluster \
	lint \
	spellcheck

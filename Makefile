#### VARIABLES
# RUNARGS: arguments for run
# DOCKER_OPTS: docker-compose options for run, test, launch-*
# TESTPKGS: packages for test, default ./src/...
# TESTFLAGS: flags for test
# VENDOR_ALL: do not ignore some vendors when updating vendor directory
# VENDOR_IGNORE_DIRS: ignore vendor dirs
# KUBECTLFLAGS: flags for kubectl
# DOCKER_BUILD_FLAGS: flags for 'docker build'
####

ifndef TESTPKGS
	TESTPKGS = ./src/...
endif
ifdef VENDOR_ALL
	VENDOR_IGNORE_DIRS =
endif

COMPILE_RUN_ARGS = -d -v /var/run/docker.sock:/var/run/docker.sock --privileged=true
# Label it w the go version we bundle in:
COMPILE_IMAGE = "pachyderm/compile:$(shell cat etc/compile/GO_VERSION)"
export VERSION_ADDITIONAL = -$(shell git log --pretty=format:%H | head -n 1)
LD_FLAGS = -X github.com/pachyderm/pachyderm/src/server/vendor/github.com/pachyderm/pachyderm/src/client/version.AdditionalVersion=$(VERSION_ADDITIONAL)
GC_FLAGS = "all=-trimpath=${PWD}"
export DOCKER_BUILD_FLAGS

CLUSTER_NAME?=pachyderm
CLUSTER_MACHINE_TYPE?=n1-standard-4
CLUSTER_SIZE?=4

BENCH_CLOUD_PROVIDER=aws

MINIKUBE_MEM=8192 # MB of memory allocated to minikube
MINIKUBE_CPU=4 # Number of CPUs allocated to minikube

ETCD_IMAGE=quay.io/coreos/etcd:v3.3.5

ifdef TRAVIS_BUILD_NUMBER
	# Upper bound for travis test timeout
	TIMEOUT = 3600s
else
ifndef TIMEOUT
	# You should be able to specify your own timeout, but by default we'll use the same bound as travis
	TIMEOUT = 3600s
endif
endif

echo-timeout:
	echo $(TIMEOUT)

all: build

version:
	@echo 'package main; import "github.com/pachyderm/pachyderm/src/client/version"; func main() { println(version.PrettyPrintVersion(version.Version)) }' > /tmp/pachyderm_version.go
	go run /tmp/pachyderm_version.go

deps:
	GO15VENDOREXPERIMENT=0 go get -d -v ./src/... ./.

update-deps:
	GO15VENDOREXPERIMENT=0 go get -d -v -u -f ./src/... ./.

test-deps:
	GO15VENDOREXPERIMENT=0 go get -d -v -t ./src/... ./.

update-test-deps:
	GO15VENDOREXPERIMENT=0 go get -d -v -t -u -f ./src/... ./.

build-clean-vendored-client:
	rm -rf src/server/vendor/github.com/pachyderm/pachyderm/src/client

build:
	GO15VENDOREXPERIMENT=1 go build $$(go list ./src/client/... | grep -v '/src/client$$')
	GO15VENDOREXPERIMENT=1 go build $$(go list ./src/server/... | grep -v '/src/server/vendor/' | grep -v '/src/server$$')

pachd:
	go build ./src/server/cmd/pachd

worker:
	go build ./src/server/cmd/worker

install:
	# GOPATH/bin must be on your PATH to access these binaries:
	GO15VENDOREXPERIMENT=1 go install -ldflags "$(LD_FLAGS)" -gcflags "$(GC_FLAGS)" ./src/server/cmd/pachctl

install-mac:
	# Result will be in $GOPATH/bin/darwin_amd64/pachctl (if building on linux)
	GO15VENDOREXPERIMENT=1 GOOS=darwin GOARCH=amd64 go install -ldflags "$(LD_FLAGS)" -gcflags "$(GC_FLAGS)" ./src/server/cmd/pachctl

install-clean:
	@# Need to blow away pachctl binary if its already there
	@rm -f $(GOPATH)/bin/pachctl
	@make install

install-doc:
	GO15VENDOREXPERIMENT=1 go install -gcflags "$(GC_FLAGS)" ./src/server/cmd/pachctl-doc

check-docker-version:
	# The latest docker client requires server api version >= 1.24.
	# However, minikube uses 1.23, so if you're connected to minikube, releases
	# may break
	@ \
		docker_major="$$(docker version -f "{{.Server.APIVersion}}" | cut -d. -f1)"; \
		docker_minor="$$(docker version -f "{{.Server.APIVersion}}" | cut -d. -f2)"; \
		echo "docker version = $${docker_major}.$${docker_minor}, need at least 1.24"; \
		test \( "$${docker_major}" -gt 1 \) -o \( "$${docker_minor}" -ge 24 \)

point-release:
	@make VERSION_ADDITIONAL= release-helper
	@make VERSION_ADDITIONAL= release-pachctl
	@make doc
	@rm VERSION
	@echo "Release completed"

# Run via 'make VERSION_ADDITIONAL=rc2 release-custom' to specify a version string
release-candidate:
	@make release-helper
	@make release-pachctl-custom
	@make doc
	@rm VERSION
	@echo "Release completed"

custom-release: release-helper release-pachctl-custom
	@echo 'For brew install, do:'
	@echo "$$ brew install https://raw.githubusercontent.com/pachyderm/homebrew-tap/$$(cat VERSION)-$$(git log --pretty=format:%H | head -n 1)/pachctl@$$(cat VERSION | cut -f -2 -d\.).rb"
	@echo 'For linux install, do:'
	@echo "$$ curl -o /tmp/pachctl.deb -L https://github.com/pachyderm/pachyderm/releases/download/v$$(cat VERSION)/pachctl_$$(cat VERSION)_amd64.deb && sudo dpkg -i /tmp/pachctl.deb"
	# Workaround for https://github.com/laher/goxc/issues/112
	@git push origin :v$$(cat VERSION)
	@git tag v$$(cat VERSION)
	@git push origin --tags
	@rm VERSION
	@echo "Release completed"

release-pachctl-custom:
	@# Run pachctl release script w deploy branch name
	@VERSION="$$(cat VERSION)" ./etc/build/release_pachctl $$(cat VERSION)

release-pachctl:
	@# Run pachctl release script w deploy branch name
	@VERSION="$(shell cat VERSION)" ./etc/build/release_pachctl

release-helper: check-docker-version release-version release-pachd release-worker

release-version: install-clean
	@./etc/build/release_version

release-pachd:
	@VERSION="$(shell cat VERSION)" ./etc/build/release_pachd

release-worker:
	@VERSION="$(shell cat VERSION)" ./etc/build/release_worker

docker-build-compile:
	docker build $(DOCKER_BUILD_FLAGS) -t pachyderm_compile .

# To bump this, update the etc/compile/GO_VERSION file
publish-compile: docker-build-compile
	docker tag pachyderm_compile $(COMPILE_IMAGE)
	docker push $(COMPILE_IMAGE)

docker-clean-worker:
	docker stop worker_compile || true
	docker rm worker_compile || true

docker-build-worker: docker-clean-worker
	docker run \
		-v $$GOPATH/src/github.com/pachyderm/pachyderm:/go/src/github.com/pachyderm/pachyderm \
		-v $$HOME/.cache/go-build:/root/.cache/go-build \
		--name worker_compile $(COMPILE_RUN_ARGS) $(COMPILE_IMAGE) /go/src/github.com/pachyderm/pachyderm/etc/compile/compile.sh worker "$(LD_FLAGS)"

docker-wait-worker:
	etc/compile/wait.sh worker_compile

docker-clean-pachd:
	docker stop pachd_compile || true
	docker rm pachd_compile || true

docker-build-pachd: docker-clean-pachd
	docker run  \
		-v $$GOPATH/src/github.com/pachyderm/pachyderm:/go/src/github.com/pachyderm/pachyderm \
		-v $$HOME/.cache/go-build:/root/.cache/go-build \
		--name pachd_compile $(COMPILE_RUN_ARGS) $(COMPILE_IMAGE) /go/src/github.com/pachyderm/pachyderm/etc/compile/compile.sh pachd "$(LD_FLAGS)"

docker-clean-test:
	docker stop test_compile || true
	docker rm test_compile || true

docker-build-test: docker-clean-test docker-build-compile
	@# build the pachyderm_test_buildenv container
	etc/compile/compile_test.sh --make-env
	mkdir ./_tmp || true
	@# build pachyderm_test container (don't use COMPILE_RUN_ARGS b/c we don't
	@# want to run this as a daemon container
	docker run \
	  --attach stdout \
	  --attach stderr \
	  --rm \
	  -w /go/src/github.com/pachyderm/pachyderm \
	  -v $$GOPATH/src/github.com/pachyderm/pachyderm:/go/src/github.com/pachyderm/pachyderm \
	  -v $$HOME/.cache/go-build:/root/.cache/go-build \
	  -v /var/run/docker.sock:/var/run/docker.sock \
	  --privileged=true \
	  --name test_compile \
	  pachyderm_test_buildenv sh /go/src/github.com/pachyderm/pachyderm/etc/compile/compile_test.sh
	docker tag pachyderm_test:latest pachyderm/test:`git rev-list HEAD --max-count=1`

docker-push-test:
	docker push pachyderm/test:`git rev-list HEAD --max-count=1`

docker-wait-pachd:
	etc/compile/wait.sh pachd_compile

docker-build-helper:
	@# run these in separate make process so that if
	@# 'enterprise-code-checkin-test' fails, the rest of the build process aborts
	@make docker-build-worker docker-build-pachd docker-wait-worker docker-wait-pachd

docker-build:
	docker pull $(COMPILE_IMAGE)
	make docker-build-helper

docker-build-proto:
	docker build $(DOCKER_BUILD_FLAGS) -t pachyderm_proto etc/proto

docker-build-netcat:
	docker build $(DOCKER_BUILD_FLAGS) -t pachyderm_netcat etc/netcat

docker-build-gpu:
	docker build $(DOCKER_BUILD_FLAGS) -t pachyderm_nvidia_driver_install etc/deploy/gpu
	docker tag pachyderm_nvidia_driver_install pachyderm/nvidia_driver_install

docker-build-kafka:
	docker build -t kafka-demo etc/testing/kafka

docker-push-gpu:
	docker push pachyderm/nvidia_driver_install

docker-push-gpu-dev:
	docker tag pachyderm/nvidia_driver_install pachyderm/nvidia_driver_install:`git rev-list HEAD --max-count=1`
	docker push pachyderm/nvidia_driver_install:`git rev-list HEAD --max-count=1`
	echo pushed pachyderm/nvidia_driver_install:`git rev-list HEAD --max-count=1`

docker-gpu: docker-build-gpu docker-push-gpu

docker-gpu-dev: docker-build-gpu docker-push-gpu-dev

docker-build-test-entrypoint:
	docker build $(DOCKER_BUILD_FLAGS) -t pachyderm_entrypoint etc/testing/entrypoint

check-kubectl:
	@# check that kubectl is installed
	@which kubectl >/dev/null || { \
		echo "error: kubectl not found"; \
		exit 1; \
	}

check-kubectl-connection:
	kubectl $(KUBECTLFLAGS) get all > /dev/null

launch-dev-bench: docker-build docker-build-test install
	@# Put it here so sudo can see it
	rm /usr/local/bin/pachctl || true
	ln -s $(GOPATH)/bin/pachctl /usr/local/bin/pachctl
	make launch-bench

build-bench-images: docker-build docker-build-test

push-bench-images: install-bench tag-images push-images
	# We need the pachyderm_compile image to be up to date
	docker tag pachyderm_test pachyderm/bench:`git rev-list HEAD --max-count=1`
	docker push pachyderm/bench:`git rev-list HEAD --max-count=1`

tag-images: install
	docker tag pachyderm_pachd pachyderm/pachd:`$(GOPATH)/bin/pachctl version --client-only`
	docker tag pachyderm_worker pachyderm/worker:`$(GOPATH)/bin/pachctl version --client-only`

push-images: tag-images
	docker push pachyderm/pachd:`$(GOPATH)/bin/pachctl version --client-only`
	docker push pachyderm/worker:`$(GOPATH)/bin/pachctl version --client-only`

launch-bench:
	@# Make launches each process in its own shell process, so we have to structure
	@# these to run these as one command
	ID=$$( etc/testing/deploy/$(BENCH_CLOUD_PROVIDER).sh --create | tail -n 1); \
	@echo To delete this cluster, run etc/testing/deploy/$(BENCH_CLOUD_PROVIDER).sh --delete=$${ID}; \
	echo etc/testing/deploy/$(BENCH_CLOUD_PROVIDER).sh --delete=$${ID} >./clean_current_bench_cluster.sh; \
	until timeout 10s ./etc/kube/check_ready.sh app=pachd; do sleep 1; done; \
	cat ~/.kube/config;

clean-launch-bench:
	./clean_current_bench_cluster.sh || true

install-bench: install
	@# Since bench is run as sudo, pachctl needs to be under
	@# the secure path
	rm /usr/local/bin/pachctl || true
	[ -f /usr/local/bin/pachctl ] || sudo ln -s $(GOPATH)/bin/pachctl /usr/local/bin/pachctl

launch-dev-test: docker-build-test docker-push-test
	kubectl run pachyderm-test --image=pachyderm/test:`git rev-list HEAD --max-count=1` \
	  --rm \
	  --restart=Never \
	  --attach=true \
	  -- \
	  ./test -test.v

aws-test: tag-images push-images
	ZONE=sa-east-1a etc/testing/deploy/aws.sh --create
	$(MAKE) launch-dev-test
	rm $(HOME)/.pachyderm/config.json
	ZONE=sa-east-1a etc/testing/deploy/aws.sh --delete

run-bench:
	kubectl scale --replicas=4 deploy/pachd
	echo "waiting for pachd to scale up" && sleep 15
	kubectl delete --ignore-not-found po/bench && \
	    kubectl run bench \
	        --image=pachyderm/bench:`git rev-list HEAD --max-count=1` \
	        --image-pull-policy=Always \
	        --restart=Never \
	        --attach=true \
	        -- \
	        PACH_TEST_CLOUD=true ./test -test.v -test.bench=BenchmarkDaily -test.run=`etc/testing/passing_test_regex.sh`

delete-all-launch-bench:
	etc/testing/deploy/$(BENCH_CLOUD_PROVIDER).sh --delete-all

bench: clean-launch-bench build-bench-images push-bench-images launch-bench run-bench clean-launch-bench

launch-kube: check-kubectl
	etc/kube/start-minikube.sh -r

launch-dev-vm: check-kubectl
	@# Make sure the caller sets address to avoid confusion later
	@if [ -z "${PACHD_ADDRESS}" ]; then \
		$$( which echo ) -e "Must set PACHD_ADDRESS\nRun:\nexport PACHD_ADDRESS=192.168.99.100:30650"; \
	  exit 1; \
	fi
	@if [ -n "${PACH_CA_CERTS}" ]; then \
		$$( which echo ) -e "Must unset PACH_CA_CERTS\nRun:\nunset PACH_CA_CERTS"; \
	  exit 1; \
	fi
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
	@# Make sure the caller sets address to avoid confusion later
	@if [ -z "${PACHD_ADDRESS}" ]; then \
		$$( which echo ) -e "Must set PACHD_ADDRESS\nRun:\nexport PACHD_ADDRESS=192.168.99.100:30650"; \
	  exit 1; \
	fi
	@if [ -n "${PACH_CA_CERTS}" ]; then \
		$$( which echo ) -e "Must unset PACH_CA_CERTS\nRun:\nunset PACH_CA_CERTS"; \
	  exit 1; \
	fi
	# Making sure minikube isn't still up from a previous run...
	@if minikube ip 2>/dev/null || sudo minikube ip 2>/dev/null; \
	then \
	  echo "minikube is still up. Run 'make clean-launch-kube'"; \
	  exit 1; \
	fi
	etc/kube/start-minikube-vm.sh --cpus=$(MINIKUBE_CPU) --memory=$(MINIKUBE_MEM) --tag=v$$(pachctl version --client-only)

clean-launch-kube:
	@# clean up both of the following cases:
	@# make launch-dev-vm - minikube config is owned by $USER
	@# make launch-kube - minikube config is owned by root
	minikube ip 2>/dev/null && minikube delete || true
	sudo minikube ip 2>/dev/null && sudo minikube delete || true
	killall kubectl || true

launch: install check-kubectl
	$(eval STARTTIME := $(shell date +%s))
	pachctl deploy local --dry-run | kubectl $(KUBECTLFLAGS) apply -f -
	# wait for the pachyderm to come up
	until timeout 1s ./etc/kube/check_ready.sh app=pachd; do sleep 1; done
	@echo "pachd launch took $$(($$(date +%s) - $(STARTTIME))) seconds"

launch-dev: check-kubectl check-kubectl-connection install
	$(eval STARTTIME := $(shell date +%s))
	pachctl deploy local --no-guaranteed -d --dry-run $(LAUNCH_DEV_ARGS) | kubectl $(KUBECTLFLAGS) apply -f -
	# wait for the pachyderm to come up
	until timeout 1s ./etc/kube/check_ready.sh app=pachd; do sleep 1; done
	@echo "pachd launch took $$(($$(date +%s) - $(STARTTIME))) seconds"

clean-launch: check-kubectl install
	yes | pachctl undeploy

clean-launch-dev: check-kubectl install
	yes | pachctl undeploy

full-clean-launch: check-kubectl
	kubectl $(KUBECTLFLAGS) delete --ignore-not-found job -l suite=pachyderm
	kubectl $(KUBECTLFLAGS) delete --ignore-not-found all -l suite=pachyderm
	kubectl $(KUBECTLFLAGS) delete --ignore-not-found serviceaccount -l suite=pachyderm
	kubectl $(KUBECTLFLAGS) delete --ignore-not-found secret -l suite=pachyderm

launch-test-rethinkdb:
	@# Expose port 8081 so you can connect to the rethink dashboard
	@# (You may need to forward port 8081 if you're running docker machine)
	docker run --name pachyderm-test-rethinkdb -d -p 28015:28015 -p 8081:8080 rethinkdb:2.3.3
	sleep 20  # wait for rethinkdb to start up

clean-launch-test-rethinkdb:
	docker stop pachyderm-test-rethinkdb || true
	docker rm pachyderm-test-rethinkdb || true

clean-pps-storage: check-kubectl
	kubectl $(KUBECTLFLAGS) delete pvc rethink-volume-claim
	kubectl $(KUBECTLFLAGS) delete pv rethink-volume

integration-tests:
	CGOENABLED=0 go test -v ./src/server $(TESTFLAGS) -timeout $(TIMEOUT)

test-proto-static:
	./etc/proto/test_no_changes.sh

proto: docker-build-proto
	find src -regex ".*\.proto" \
	| grep -v vendor \
	| xargs tar cf - \
	| docker run -i pachyderm_proto \
	| tar xf -

# Use this to grab a binary for profiling purposes
pachd-profiling-binary: docker-clean-pachd docker-build-compile
	docker run -i  $(COMPILE_IMAGE) sh etc/compile/compile.sh pachd "$(LD_FLAGS)" PROFILE \
	| tar xf -
	# Binary emitted to ./pachd

pretest:
	go get -v github.com/kisielk/errcheck
	rm -rf src/server/vendor
	go vet -n ./src/... | while read line; do \
		modified=$$(echo $$line | sed "s/ [a-z0-9_/]*\.pb\.gw\.go//g"); \
		$$modified; \
		if [ -n "$$($$modified)" ]; then \
		exit 1; \
		fi; \
		done
	git checkout src/server/vendor
	#errcheck $$(go list ./src/... | grep -v src/cmd/ppsd | grep -v src/pfs$$ | grep -v src/pps$$)

local-test: docker-build launch-dev test-pfs clean-launch-dev

# Run all the tests. Note! This is no longer the test entrypoint for travis
test: clean-launch-dev launch-dev lint enterprise-code-checkin-test docker-build test-pfs-server test-pfs-cmds test-deploy-cmds test-libs test-vault test-auth test-enterprise test-worker test-admin test-pps

enterprise-code-checkin-test:
	@which ag || { printf "'ag' not found. Run:\n  sudo apt-get install -y silversearcher-ag\n  brew install the_silver_searcher\nto install it\n\n"; exit 1; }
	# Check if our test activation code is anywhere in the repo
	@echo "Files containing test Pachyderm Enterprise activation token:"; \
	if ag --ignore=Makefile -p .gitignore 'RM2o1Qit6YlZhS1RGdXVac'; \
	then \
	  $$( which echo ) -e "\n*** It looks like Pachyderm Engineering's test activation code may be in this repo. Please remove it before committing! ***\n"; \
	  false; \
	fi

test-pfs-server:
	./etc/testing/pfs_server.sh $(ETCD_IMAGE) $(TIMEOUT)

test-pfs-cmds:
	@# Unlike test-pfs-server, this target requires a running cluster
	go test ./src/server/pfs/cmds -count 1 -timeout $(TIMEOUT)

test-pfs-storage:
	go test ./src/server/pkg/storage/chunk -count 1 -timeout $(TIMEOUT)
	go test ./src/server/pkg/storage/fileset/index -count 1 -timeout $(TIMEOUT)
	go test ./src/server/pkg/storage/fileset -count 1 -timeout $(TIMEOUT)

test-deploy-cmds:
	go test ./src/server/pkg/deploy/cmds -count 1 -timeout $(TIMEOUT)

test-pps:
	@# Travis uses the helper directly because it needs to specify a
	@# subset of the tests using the run flag
	@make RUN= test-pps-helper

test-pps-helper:
	# Use the count flag to disable test caching for this test suite.
	# PROM_PORT=$$(kubectl --namespace=monitoring get svc/prometheus -o json | jq -r .spec.ports[0].nodePort)
	go test -v ./src/server -parallel 1 -count 1 -timeout $(TIMEOUT) $(RUN) && \
	go test ./src/server/pps/cmds -count 1 -timeout $(TIMEOUT)

test-transaction:
	go test ./src/server/transaction/server -count 1 -timeout $(TIMEOUT)

test-client:
	rm -rf src/client/vendor
	rm -rf src/server/vendor/github.com/pachyderm
	cp -R src/server/vendor src/client/
	GO15VENDOREXPERIMENT=1 go test -cover $$(go list ./src/client/... | grep -v vendor)
	rm -rf src/client/vendor
	git checkout src/server/vendor/github.com/pachyderm

test-libs:
	go test ./src/server/pkg/collection -timeout $(TIMEOUT) -vet=off
	go test ./src/server/pkg/hashtree -timeout $(TIMEOUT)
	go test ./src/server/pkg/cert -timeout $(TIMEOUT)
	go test ./src/server/pkg/localcache -timeout $(TIMEOUT)

test-vault:
	kill $$(cat /tmp/vault.pid) || true
	./src/plugin/vault/etc/start-vault.sh
	./src/plugin/vault/etc/setup-vault.sh
	@# Dont cache these results as they require the pachd cluster
	go test -v -count 1 ./src/plugin/vault -timeout $(TIMEOUT)

./etc/testing/s3gateway/s3-tests:
	cd ./etc/testing/s3gateway && git clone git@github.com:ceph/s3-tests.git
	cd ./etc/testing/s3gateway/s3-tests && ./bootstrap
	cd ./etc/testing/s3gateway/s3-tests && source virtualenv/bin/activate && pip install nose-exclude==0.5.0

test-s3gateway-integration:
	pachctl enterprise activate $$(aws s3 cp s3://pachyderm-engineering/test_enterprise_activation_code.txt -) && echo
	go test -v ./src/server/pfs/s3 -timeout $(TIMEOUT) -count 1 | grep -v 'INFO pfs.'

test-s3gateway-conformance: ./etc/testing/s3gateway/s3-tests install
	./etc/testing/s3gateway/conformance.py

test-fuse:
	CGOENABLED=0 GO15VENDOREXPERIMENT=1 go test -cover $$(go list ./src/server/... | grep -v '/src/server/vendor/' | grep '/src/server/pfs/fuse')

test-local:
	CGOENABLED=0 GO15VENDOREXPERIMENT=1 go test -cover -short $$(go list ./src/server/... | grep -v '/src/server/vendor/' | grep -v '/src/server/pfs/fuse') -timeout $(TIMEOUT)

test-auth:
	yes | pachctl delete all
	@# Dont cache these results as they require the pachd cluster
	go test -v ./src/server/auth/server -count 1 -timeout $(TIMEOUT)
	go test -v ./src/server/auth/cmds -count 1 -timeout $(TIMEOUT)

test-admin:
	@# Dont cache these results as they require the pachd cluster
	go test -v ./src/server/admin/server -count 1 -timeout $(TIMEOUT)

test-enterprise:
	@# Dont cache these results as they require the pachd cluster
	go test -v ./src/server/enterprise/server -count 1 -timeout $(TIMEOUT)

# TODO This is not very robust -- it doesn't work when the PACHD_ADDRESS host isn't an IPv4 address
PACHD_HOST := $(word 1,$(subst :, ,$(PACHD_ADDRESS)))
PACHD_PORT := $(word 2,$(subst :, ,$(PACHD_ADDRESS)))

test-tls:
	# Pachyderm must be running when this target is called
	pachctl version
	# TLS is an enterprise pachyderm feature
	pachctl enterprise activate $$(aws s3 cp s3://pachyderm-engineering/test_enterprise_activation_code.txt -) && echo
	# Generate TLS key and re-deploy pachyderm with TLS enabled
	etc/deploy/gen_pachd_tls.sh --ip=$(PACHD_HOST) --port=$(PACHD_PORT)
	# Restart pachd with new cert
	etc/deploy/restart_with_tls.sh --key=$(PWD)/pachd.key --cert=$(PWD)/pachd.pem
	# If we can run a pipeline, TLS probably works
	@# TODO actually monitor traffic with tcpdump
	PACH_CA_CERTS=${PWD}/pachd.pem go test -v ./src/server -run TestPipelineWithParallelism

test-worker: launch-stats test-worker-helper

test-worker-helper:
	@# Dont cache these results as they require the pachd cluster
	PROM_PORT=$$(kubectl --namespace=monitoring get svc/prometheus -o json | jq -r .spec.ports[0].nodePort) \
	  go test -v ./src/server/worker/ -timeout $(TIMEOUT) -count 1

clean: clean-launch clean-launch-kube

doc-custom: install-doc release-version
	./etc/build/doc

doc:
	@make VERSION_ADDITIONAL= doc-custom

clean-launch-kafka:
	kubectl delete -f etc/kubernetes-kafka -R
	
launch-kafka:
	kubectl apply -f etc/kubernetes-kafka -R
	until timeout 10s ./etc/kube/check_ready.sh app=kafka kafka; do sleep 10; done

clean-launch-stats:
	kubectl delete --filename etc/kubernetes-prometheus -R

launch-stats:
	kubectl apply --filename etc/kubernetes-prometheus -R

clean-launch-monitoring:
	kubectl delete --ignore-not-found -f ./etc/plugin/monitoring

launch-monitoring:
	kubectl create -f ./etc/plugin/monitoring
	@echo "Waiting for services to spin up ..."
	until timeout 5s ./etc/kube/check_ready.sh k8s-app=heapster kube-system; do sleep 5; done
	until timeout 5s ./etc/kube/check_ready.sh k8s-app=influxdb kube-system; do sleep 5; done
	until timeout 5s ./etc/kube/check_ready.sh k8s-app=grafana kube-system; do sleep 5; done
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

grep-data:
	go run examples/grep/generate.go >examples/grep/set1.txt
	go run examples/grep/generate.go >examples/grep/set2.txt

grep-example:
	sh examples/grep/run.sh

logs: check-kubectl
	kubectl $(KUBECTLFLAGS) get pod -l app=pachd | sed '1d' | cut -f1 -d ' ' | xargs -n 1 -I pod sh -c 'echo pod && kubectl $(KUBECTLFLAGS) logs pod'

follow-logs: check-kubectl
	kubectl $(KUBECTLFLAGS) get pod -l app=pachd | sed '1d' | cut -f1 -d ' ' | xargs -n 1 -I pod sh -c 'echo pod && kubectl $(KUBECTLFLAGS) logs -f pod'

kubectl:
	gcloud config set container/cluster $(CLUSTER_NAME)
	gcloud container clusters get-credentials $(CLUSTER_NAME)

google-cluster-manifest:
	@pachctl deploy --rethinkdb-cache-size=5G --dry-run google $(BUCKET_NAME) $(STORAGE_NAME) $(STORAGE_SIZE)

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
	@pachctl deploy --dry-run amazon $(BUCKET_NAME) $(AWS_ID) $(AWS_KEY) $(AWS_TOKEN) $(AWS_REGION) $(STORAGE_NAME) $(STORAGE_SIZE)

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
	kubectl $(KUBECTLFLAGS) delete --ignore-not-found persistentvolumes rethink-volume
	kubectl $(KUBECTLFLAGS) delete --ignore-not-found persistentvolumeclaims rethink-volume-claim

amazon-clean:
	@while :; \
        do if echo "The following script will delete your AWS bucket and volume. The action cannot be undone. Do you want to proceed? (Y/n)";read REPLY; then \
        case $$REPLY in Y|y) make amazon-clean-launch;make amazon-clean-cluster;break;; \
	N|n) echo "The amazon clean process has been cancelled by user!";break;; \
	*) echo "input parameter error, please input again ";continue;;esac; \
        fi;done;

microsoft-cluster-manifest:
	@pachctl deploy --dry-run microsoft $(CONTAINER_NAME) $(AZURE_STORAGE_NAME) $(AZURE_STORAGE_KEY) $(VHD_URI) $(STORAGE_SIZE)

microsoft-cluster:
	azure group create --name $(AZURE_RESOURCE_GROUP) --location $(AZURE_LOCATION)
	azure storage account create $(AZURE_STORAGE_NAME) --location $(AZURE_LOCATION) --resource-group $(AZURE_RESOURCE_GROUP) --sku-name LRS --kind Storage

clean-microsoft-cluster:
	azure group delete $(AZURE_RESOURCE_GROUP) -q

install-go-bindata:
	go get -u github.com/jteeuwen/go-bindata/...

lint:
	etc/testing/lint.sh

spellcheck:
	@mdspell doc/*.md doc/**/*.md *.md --en-us --ignore-numbers --ignore-acronyms --report --no-suggestions

goxc-generate-local:
	@if [ -z $$GITHUB_OAUTH_TOKEN ]; then \
	  echo "Missing token. Please run via: 'make GITHUB_OAUTH_TOKEN=12345 goxc-generate-local'"; \
	  exit 1; \
	fi
	goxc -wlc default publish-github -apikey=$(GITHUB_OAUTH_TOKEN)

goxc-release:
	@if [ -z $$VERSION ]; then \
	  @echo "Missing version. Please run via: 'make VERSION=v1.2.3-4567 VERSION_ADDITIONAL=4567 goxc-release'"; \
	  @exit 1; \
	fi
	sed 's/%%VERSION_ADDITIONAL%%/$(VERSION_ADDITIONAL)/' .goxc.json.template > .goxc.json
	goxc -pv="$(VERSION)" -build-gcflags="$(GC_FLAGS)" -wd=./src/server/cmd/pachctl

goxc-build:
	sed 's/%%VERSION_ADDITIONAL%%/$(VERSION_ADDITIONAL)/' .goxc.json.template > .goxc.json
	goxc -build-gcflags="$(GC_FLAGS)" -tasks=xc -wd=./src/server/cmd/pachctl

.PHONY: all \
	version \
	deps \
	deps-client \
	update-deps \
	test-deps \
	update-test-deps \
	build-clean-vendored-client \
	build \
	install \
	install-clean \
	install-doc \
	homebrew \
	release \
	release-helper \
	release-worker \
	release-manifest \
	release-pachctl-custom \
	release-pachd \
	release-version \
	docker-build \
	docker-build-compile \
	docker-build-worker \
	docker-build-pachd \
	docker-build-proto \
	docker-push-worker \
	docker-push-pachd \
	docker-push \
	launch-kube \
	clean-launch-kube \
	kube-cluster-assets \
	launch \
	launch-dev \
	clean-launch-dev \
	clean-launch \
	full-clean-launch \
	clean-pps-storage \
	integration-tests \
	proto \
	pretest \
	test \
	test-client \
	test-s3gateway-conformance \
	test-s3gateway-integration \
	test-fuse \
	test-local \
	clean \
	doc \
	grep-data \
	grep-example \
	logs \
	kubectl \
	google-cluster-manifest \
	google-cluster \
	clean-google-cluster \
	amazon-cluster-manifest \
	amazon-cluster \
	amazon-clean-cluster \
	amazon-clean-launch \
	amazon-clean \
	install-go-bindata \
	assets \
	lint \
	goxc-generate-local \
	goxc-release \
	goxc-build \
	launch-test-rethinkdb \
	clean-launch-test-rethinkdb

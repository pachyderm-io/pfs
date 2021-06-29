#!/bin/bash

set -ex

source "$(dirname "$0")/env.sh"

VM_IP="$(minikube ip)"
export VM_IP

PACH_PORT="30650"
export PACH_PORT

ENTERPRISE_PORT="31650"
export ENTEPRRISE_PORT

POSTGRES_HOST="$(minikube ip)"
export POSTGRES_HOST

POSTGRES_PORT=32228
export POSTGRES_PORT

TESTFLAGS="-v | stdbuf -i0 tee -a /tmp/results"
export TESTFLAGS

# make launch-kube connects with kubernetes, so it should just be available
minikube status
kubectl version

# any tests that build images will do it directly in minikube's docker registry
eval $(minikube docker-env)

echo "Running test suite based on BUCKET=$BUCKET"

function test_bucket {
    set +x
    package="${1}"
    bucket_num="${2}"
    num_buckets="${3}"
    if (( bucket_num == 0 )); then
        echo "Error: bucket_num should be > 0, but was 0" >/dev/stderr
        exit 1
    fi

    echo "Running bucket $bucket_num of $num_buckets"
    # shellcheck disable=SC2207
    tests=( $(go test -v  "${package}" -list ".*" | grep -v '^ok' | grep -v '^Benchmark') )
    # Add anchors for the regex so we don't run collateral tests
    tests=( "${tests[@]/#/^}" )
    tests=( "${tests[@]/%/\$\$}" )
    total_tests="${#tests[@]}"
    # Determine the offset and length of the sub-array of tests we want to run
    # The last bucket may have a few extra tests, to accommodate rounding
    # errors from bucketing:
    let "bucket_size=total_tests/num_buckets" \
        "start=bucket_size * (bucket_num-1)" \
        "bucket_size+=bucket_num < num_buckets ? 0 : total_tests%num_buckets"
    test_regex="$(IFS=\|; echo "${tests[*]:start:bucket_size}")"
    echo "Running ${bucket_size} tests of ${total_tests} total tests"
    set -x
    go test ${package} -p 1 -run="${test_regex}"
}

# Clean cached test results
go clean -testcache

case "${BUCKET}" in
 MISC)
    make lint
    make check-buckets
    make enterprise-code-checkin-test
    make test-proto-static
    make test-deploy-manifests
    make test-worker
    if [[ "${TRAVIS_SECURE_ENV_VARS:-""}" == "true" ]]; then
        # these tests require secure env vars to run, which aren't available
        # when the PR is coming from an outside contributor - so we just
        # disable them
        make test-tls
    fi
    ;;
  EXAMPLES)
    echo "Running the example test suite"
    ./etc/testing/examples.sh
    ;;
  ENTERPRISE)
    # Launch a stand-alone enterprise server in a separate namespace
    make launch-enterprise
    echo "{\"pachd_address\": \"grpc://${VM_IP}:${ENTERPRISE_PORT}\", \"source\": 2}" | pachctl config set context "enterprise" --overwrite 
    pachctl config set active-enterprise-context enterprise
    make test-enterprise-integration
    ;;
  TESTS?)
    go install -v ./src/testing/match
    make docker-build-kafka
    make launch-stats
    export PROM_PORT=$(kubectl --namespace=monitoring get svc/prometheus -o json | jq -r .spec.ports[0].nodePort) \
    bucket_num="${BUCKET#TESTS}"
    test_bucket "./src/..." "${bucket_num}" "${TEST_BUCKETS}"
    ;;
  *)
    echo "Unknown bucket"
    exit 1
    ;;
esac

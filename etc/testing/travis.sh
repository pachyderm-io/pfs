#!/bin/bash

set -ex

echo "Running test suite based on BUCKET=$BUCKET"

PPS_SUITE=`echo $BUCKET | grep PPS > /dev/null; echo $?`

make install
make docker-build

kind load docker-image pachyderm/worker:local
kind load docker-image pachyderm/pachd:local

#make docker-build-kafka

#kind load docker-image kafka-demo

make docker-build-test-entrypoint

kind load docker-image pachyderm_entrypoint

make launch-dev

go install ./src/testing/match

echo 'something'

if [[ "$BUCKET" == "MISC" ]]; then
    if [[ "$TRAVIS_SECURE_ENV_VARS" == "true" ]]; then
        echo "Running the full misc test suite because secret env vars exist"

        make lint test-pfs-server \
            test-pfs-cmds test-pfs-storage test-deploy-cmds test-libs test-auth \
            test-enterprise test-worker test-admin test-s3gateway-integration \
            test-proto-static test-transaction
    else
        echo "Running the misc test suite with some tests disabled because secret env vars have not been set"

        # Do not run some tests when we don't have access to secret
        # credentials
        make lint test-pfs-server \
            test-pfs-cmds test-pfs-storage test-deploy-cmds test-libs test-admin \
            test-s3gateway-integration
    fi
elif [[ "$BUCKET" == "EXAMPLES" ]]; then
    echo "Running the example test suite"
    ./etc/testing/examples.sh
elif [[ $PPS_SUITE -eq 0 ]]; then
    PART=`echo $BUCKET | grep -Po '\d+'`
    NUM_BUCKETS=`cat etc/build/PPS_BUILD_BUCKET_COUNT`
    echo "Running pps test suite, part $PART of $NUM_BUCKETS"
    LIST=`go test -v  ./src/server/ -list ".*" | grep -v ok | grep -v Benchmark`
    COUNT=`echo $LIST | tr " " "\n" | wc -l`
    BUCKET_SIZE=$(( $COUNT / $NUM_BUCKETS ))
    MIN=$(( $BUCKET_SIZE * $(( $PART - 1 )) ))
    #The last bucket may have a few extra tests, to accommodate rounding errors from bucketing:
    MAX=$COUNT
    if [[ $PART -ne $NUM_BUCKETS ]]; then
        MAX=$(( $MIN + $BUCKET_SIZE ))
    fi

    RUN=""
    INDEX=0

    for test in $LIST; do
        if [[ $INDEX -ge $MIN ]] && [[ $INDEX -lt $MAX ]] ; then
            if [[ "$RUN" == "" ]]; then
                RUN=$test
            else
                RUN="$RUN|$test"
            fi
        fi
        INDEX=$(( $INDEX + 1 ))
    done
    echo "Running $( echo $RUN | tr '|' '\n' | wc -l ) tests of $COUNT total tests"
    make RUN=-run=\"$RUN\" test-pps-helper
else
    echo "Unknown bucket"
    exit 1
fi
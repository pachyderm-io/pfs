#!/bin/bash

set -ex

source "$(dirname "$0")/env.sh"

# Normally `pachctl deploy local` adds a PodSecurityContext to run as root,
# because we can't guarantee the hostpath will be writable by our normal UID (1000).
# We want to run as UID 1000 in CI because that's more reflective of real life,
# so we explicitly create the host path on the host machine and chmod it so we can write to it. 
minikube ssh 'mkdir -p /tmp/pachyderm/pachd && chmod -R 777 /tmp/pachyderm'

helm install pachyderm etc/helm/pachyderm -f etc/testing/circle/helm-values.yaml

kubectl wait --for=condition=ready pod -l app=pachd --timeout=5m

# Wait for loki to be deployed
kubectl wait --for=condition=ready pod -l release=loki --timeout=5m

pachctl config update context "$(pachctl config get active-context)" --pachd-address="$(minikube ip):30650"

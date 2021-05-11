#!/bin/bash

set -euo pipefail

# Set up the shared data directory to point to the virtualbox shared folder
export SHARED_DATA_DIR=/tmp/pach/

# Connect to the k8s API in minikube
export KUBERNETES_PORT_443_TCP_ADDR=$(minikube ip)
export KUBERNETES_PORT=8443

# Mount a bearer token for the default k8s service account,
# so pachd can make authenticated calls
export KUBERNETES_BEARER_TOKEN_FILE=/tmp/pach/kubernetes-default-token
kubectl get -n kube-system -o json secret $(kubectl -n kube-system get secrets  | grep 'default-token' | awk '{print $1}') | jq -r '.data.token' | base64 -d > $KUBERNETES_BEARER_TOKEN_FILE

# Point the serviceenv to the hostports for etcd, postgres in minikube
export ETCD_SERVICE_HOST="$(minikube ip)"
export ETCD_SERVICE_PORT=32379
export POSTGRES_SERVICE_HOST="$(minikube ip)"
export POSTGRES_SERVICE_PORT=32228
export POSTGRES_SERVICE_SSL=disable

# Use the images tagged with `local`.
export WORKER_IMAGE=pachyderm/worker:local
export WORKER_SIDECAR_IMAGE=pachyderm/pachd:local

# Trigger the use of the in-memory pachds rather than trying to
# connect to the current active pach context
export PACH_INMEMORY=true

# Env vars that are required to start services
export STORAGE_BACKEND=LOCAL
export PACHD_POD_NAME=local

$@

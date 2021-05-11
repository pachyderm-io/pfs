#!/bin/bash

set -euo pipefail

# Mount the shared folder in the minikube VM. 
# This has to be done on every restart of the VM, not just when it's initially created
minikube ssh 'mkdir -p /tmp/pach && sudo mount -t vboxsf pach /tmp/pach'

# Build the pachctl CLI
make 

# Set up the environemnt to refer to the minikube VM docker repo
eval $(minikube docker-env)

# Build the worker docker images and tag them with `local`
make docker-build
VERSION=local make docker-tag

# Deploy all the pods except pachd
pachctl deploy local -d --no-dashboard --dry-run | jq 'select(.metadata.name != "pachd")' | kubectl create -f -

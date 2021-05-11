#!/bin/bash -exuo pipefail

# Start the minikube VM in virtualbox
minikube start --kubernetes-version=1.18.3 --memory=4G  --driver=virtualbox

# Stop the VM, attach the shared volume and restart it
minikube stop
VBoxManage sharedfolder add minikube --name pach --hostpath /tmp/pach || true
minikube start

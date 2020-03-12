#!/usr/bin/env bash

# go compiler requires a relative directory or something?
cd $(realpath $(dirname $0)/../..)

VERSION_ADDITIONAL=+$(git log --pretty=format:%H | head -n 1)
LD_FLAGS="-X github.com/pachyderm/pachyderm/src/client/version.AdditionalVersion=${VERSION_ADDITIONAL}"
GC_FLAGS="all=-trimpath=${PWD}"

GO111MODULE=on go install -ldflags "${LD_FLAGS}" -gcflags "${GC_FLAGS}" ./src/server/cmd/pachctl

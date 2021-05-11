#!/bin/bash
# Build the supervisor such that it can be run statically, so that the docker 
# image is as small as possible.
#
# **Compiled binary will be stored in ./_out**

set -ex

# Clear _out, to hold build output
rm -rf ./_out || true
mkdir _out

# Setup build command. The linker flags, along with CGO_ENABLED=0 (set below)
# tell the go compiler to build a fully static binary (see comment at top)
LD_FLAGS="-extldflags -static"
BUILD_PATH=github.com/pachyderm/pachyderm/v2/src/testing/loadtest/obj
BUILD_CMD="
go install -a -ldflags \"${LD_FLAGS}\" ./${BUILD_PATH}/cmd/supervisor && \
mv ../bin/* /out/"

# Run compilation inside golang container, with pachyderm mounted
# Explanation of mounts
# ---------------------
# - _out is where the binaries built in this docker image are written out for
#   us to use:
#     /out <- ./_out
#
# - $HOME/.cache/go-build holds cached docker builds; mounting it makes builds
#   faster:
#     /root/.cache/go-build <- $HOME/.cache/go-build
#
# - Mounting the whole pachyderm repo (including the pachyderm client and this
#   benchmark) makes this benchmark available to be built. Mounting all of it in
#   together (rather than e.g. mounting just this dir and having the pachyderm
#   repo in ./vendor) means that parallel changes can be made to the benchmark,
#   server, and client simultaneously:
#     /go/src/.../pachyderm <- $GOPATH/src/.../pachyerm/
#
PACH_PATH=src/github.com/pachyderm/pachyderm
docker run \
  -w /go/src \
  -e CGO_ENABLED=0 \
  -v "${PWD}/_out:/out" \
  -v "${HOME}/.cache/go-build:/root/.cache/go-build" \
  -v "${GOPATH}/${PACH_PATH}:/go/${PACH_PATH}" \
  golang:1.15.4 /bin/sh -c "${BUILD_CMD}"

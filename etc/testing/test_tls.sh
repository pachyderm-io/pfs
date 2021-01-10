#!/usr/bin/env bash

# -x is set below
set -euo pipefail

TIMEOUT="120s"
if [ "$#" -eq 1 ]; then
  TIMEOUT="${1}"
elif [ "$#" -gt 1 ]; then
  echo "Too many parameters, expected 1, got $#"
  echo "Usage: $0 <TIMEOUT>"
  exit 1
fi

command -v match || {
  here="$(dirname "${0}")"
  go install -v "${here}/../../src/testing/match"
}

address=$(pachctl config get context "$(pachctl config get active-context)" | jq -r .pachd_address)
if [[ "${address}" = "null" ]]; then
  echo "pachd_address must be set on the active context"
  exit 1
fi
hostport=$(echo "$address" | sed -e 's/grpcs:\/\///g' -e 's/grpc:\/\///g')

set -x

# Generate self-signed cert and private key
etc/deploy/gen_pachd_tls.sh "$hostport" ""

# Restart pachyderm with the given certs
etc/deploy/restart_with_tls.sh "$hostport" "${PWD}/pachd.pem" "${PWD}/pachd.key"

set +x # Do not log our activation code when running this script in Travis
echo "$ENT_ACT_CODE" | pachctl enterprise activate && echo
set -x

# Make sure the pachyderm client can connect, write data, and create pipelines
go test -v -count=1 ./src/server -run TestSimplePipeline -timeout "${TIMEOUT}"

# Make sure that config's pachd_address isn't disfigured by pachctl cmds that
# modify the pachctl config (bug fix)
pachctl auth activate
pachctl auth whoami | match 'pach:root' # will fail if pachctl can't connect
echo yes | pachctl auth deactivate

# Undeploy TLS
yes | pachctl undeploy || true
pachctl deploy local -d

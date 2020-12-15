#!/bin/bash
set -euxo pipefail

export TIMEOUT="${1}"
PROM_PORT="$(
  kubectl --namespace=monitoring get svc/prometheus -o json \
    | jq -r .spec.ports[0].nodePort
)"
export PROM_PORT

# grep out PFS server logs, as otherwise the test output is too verbose to
# follow and breaks travis
go test -v -count=1 ./src/server/worker/... -timeout "${TIMEOUT}" \
  | grep -v "$(date +^%FT)"

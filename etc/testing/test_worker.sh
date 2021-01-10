#!/usr/bin/env bash
set -euxo pipefail

TIMEOUT="120s"
if [ "$#" -eq 1 ]; then
  TIMEOUT="${1}"
elif [ "$#" -gt 1 ]; then
  echo "Too many parameters, expected 1, got $#"
  echo "Usage: $0 <TIMEOUT>"
  exit 1
fi

# grep out PFS server logs, as otherwise the test output is too verbose to
# follow and breaks travis
go test -v -count=1 ./src/server/worker/... -timeout "${TIMEOUT}" \
  | grep -v "$(date +^%FT)"

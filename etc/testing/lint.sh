#!/usr/bin/env bash

set -e

GIT_REPO_DIR=$(cd "$( dirname "${BASH_SOURCE[0]}" )" 2>&1 > /dev/null && git rev-parse --show-toplevel)
cd "${GIT_REPO_DIR}"

# Collect untracked args into '\( -o -path abc -o -path def -o ... \) -prune'
# argument to be passed to 'find'
skip_paths=()
for file in $(git status --porcelain | grep '^??' | sed 's/^?? //'); do
  # Convert to relative path, as required by -path
  file="./$(realpath --relative-to="." "${file}")"
  # 1. Always prefix with -o (i.e. don't fencepost; find always has an argument
  #    before skip_paths, so including the initial -o works whether skip_paths
  #    is empty or not
  # 2. strip trailing slash as required by -path
  skip_paths+=( -o -path "${file%/}" )
done

# Update golint once a day, or if it isn't installed
if [ -z "$(find "$(command -v golint)" -mtime -1 2>/dev/null)" ]; then
  go install golang.org/x/lint/golint
fi

find "./src" \
  \( -path "*.pb.go" -o -path "*internal/tar*" "${skip_paths[@]}" \) -prune -o -name '*.go' -print0 \
| xargs -P32 -0 -n 1 golint -set_exit_status

files=$(gofmt -l "${GIT_REPO_DIR}/src" || true)
if [[ -n "${files}" ]]; then
    echo Files not passing gofmt:
    tr ' ' '\n'  <<< "$files"
    exit 1
fi

# Update staticcheck once a day, or if it isn't installed
if [ -z "$(find "$(command -v staticcheck)" -mtime -1 2>/dev/null)" ]; then
  go install honnef.co/go/tools/cmd/staticcheck
fi
staticcheck "${GIT_REPO_DIR}/..."

# shellcheck disable=SC2046
find . \
  \( -path ./etc/plugin "${skip_paths[@]}" \) -prune -o -name "*.sh" -print0 \
| xargs -P16 -0 shellcheck -e SC1091 -e SC2010 -e SC2181 -e SC2004 -e SC2219

#!/bin/bash

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
source "${SCRIPT_DIR}/../../govars.sh"

here="$(dirname "${0}")"
dest_dir="test"
rm -rf "${here:?}/${dest_dir:?}" || true
mkdir -p "${here}/${dest_dir}"

is_regenerate=""
if [[ "${#@}" -eq 1 ]]; then
  if [[ "${1}" == "--regenerate" ]]; then
	  is_regenerate="true"
    dest_dir="golden"
  else
    echo "Unrecognized flag ${1}" >/dev/stderr
    echo "Must be --regenerate" >/dev/stderr
    exit 1
  fi
fi

# Generate a deployment manifest for many different targets using the local
# build of 'pachctl'
# shellcheck disable=SC2034
custom_args=(
--cluster-deployment-id test
--secure
--dynamic-etcd-nodes 3
--etcd-storage-class storage-class
--namespace pachyderm
--no-expose-docker-socket
"--object-store=s3"
  pach-volume       # <volumes>
  50                # <size of volumes (in GB)>
  pach-bucket       # <bucket>
  storage-id        # <id>
  storage-secret    # <secret>
  storage.endpoint  # <endpoint>
)
# shellcheck disable=SC2034
google_args=(
--cluster-deployment-id test
--dynamic-etcd-nodes 3
  50          # <disk-size>
  pach-bucket # <bucket-name>
)
# shellcheck disable=SC2034
amazon_args=(
--cluster-deployment-id test
--dynamic-etcd-nodes 3
--credentials "AWSIDAWSIDAWSIDAWSID,awssecret+awssecret+awssecret+awssecret+"
  us-west-1   # <region>
  50          # <disk-size>
  pach-bucket # <bucket-name>
)
# shellcheck disable=SC2034
microsoft_args=(
--cluster-deployment-id test
--dynamic-etcd-nodes 3
  50                         # <disk-size>
  pach-container             # <container>
  pach-account               # <account-name>
  "cGFjaC1hY2NvdW50LWtleQ==" # <account-key> (base64-encoded "pach-account-key")
)

pach_config="${here}/${dest_dir}/pachconfig"
# Use a test-specific pach config to avoid local settings from changing the
# output
export PACH_CONFIG="${pach_config}"
for platform in custom google amazon microsoft; do
  for fmt in json yaml; do
    output="${here}/${dest_dir}/${platform}-deploy-manifest.${fmt}"
    eval "args=( \"\${${platform}_args[@]}\" )"
    # Generate kubernetes manifest:
    # - strip additional version info so that pachctl builds from the same
    #   version all work
    # - Use an empty pach config so that e.g. metrics don't change the output
    # shellcheck disable=SC2154
    "${PACHCTL}" deploy "${platform}" "${args[@]}" -o "${fmt}" --dry-run \
      | sed 's/\([0-9]\{1,4\}\.[0-9]\{1,4\}\.[0-9]\{1,4\}\)-[0-9a-f]\{40\}/\1/g' >"${output}"
    rm -f "${pach_config}" # remove cfg from next run (or diff dir, or golden/)
    if [[ ! "${is_regenerate}" ]]; then
      # Check manifests with kubeval
      kubeval "${output}"
    fi
  done
done

# XXX: for now, skip diffing because 'ci_build' ends up in the version. Not
# sure how to workaround that.
# https://app.circleci.com/pipelines/github/pachyderm/pachyderm/80/workflows/088e1426-b4ce-4baa-85e2-f9a20cc75151/jobs/2934
if [ ! -z "$CIRCLE_BRANCH" ] && [ -z "$RUN_BAD_TESTS" ]; then
    echo "Skipping because RUN_BAD_TESTS is empty (and we are running in CI)"
    exit 0
fi

if [[ "${is_regenerate}" ]]; then
  exit 0
fi

# Compare manifests to golden files (in addition to kubeval, to see changes
# in storage secrets and such)
#
# TODO(msteffen): if we ever consider removing this because it generates too
# many spurious test failures, then I highly recomment we keep the 'kubeval'
# validation above, as it should accept any valid kubernetes manifest, and
# would've caught at least one serialization bug that completely broke 'pachctl
# deploy' in v1.9.8
echo ""
echo "Diffing 'pachctl deploy' output with known-good golden deploy manifests."
DIFF_CMD="${DIFF_CMD:-diff --unified=6}"
if ! ${DIFF_CMD} "${here}/${dest_dir}" "${here}/golden"; then
  echo ""
  echo "Error: deployment manifest has changed."
  echo "If this test is failing because you have deliberately changed"\
    "'pachctl deploy', you'll need to fix it by replacing the golden"\
    "deployment manifests."
  echo "You can update the golden manifests by running:"
  echo "  make regenerate-test-deploy-manifests"
  exit 1
else
  echo ""
  echo "No differences found"
fi

#!/bin/bash

set -euxo pipefail

# Make sure Pachyderm enterprise and auth are enabled
command -v aws || pip install awscli --upgrade --user

function activate {
    pachctl config update context "$(pachctl config get active-context)" --pachd-address="$(minikube ip):30650"

    if [[ "$(pachctl enterprise get-state)" = "No Pachyderm Enterprise token was found" ]]; then
        # Don't print token to stdout
        # This is very important, or we'd leak it in our CI logs
        set +x
        echo "$ENT_ACT_CODE" | pachctl enterprise activate
        set -x
    fi

    # Activate Pachyderm auth, if needed, and log in
    if ! pachctl auth list-admins ; then
        echo "iamroot" | pachctl auth activate --supply-root-token
    fi
}

function delete_all {
    echo "yes" | pachctl delete all
}

eval "set -- $( getopt -l "activate,delete-all" "--" "${0}" "${@}" )"
while true; do
    case "${1}" in
     --activate)
        activate
        shift
        ;;
     --delete-all)
        delete_all
        shift
        ;;
     --)
        shift
        break
        ;;
     *)
        echo "Unrecognized operation: ${1}"
        echo
        echo "Operation should be \"--activate\" or \"--delete-all\""
        shift
        ;;
    esac
done




#!/bin/bash

set -euxo pipefail
export VAULT_ADDR='http://127.0.0.1:8200'
export PLUGIN_NAME='pachyderm'
export PLUGIN_PATH=$PLUGIN_NAME

# Build it

pachctl version
# Remove the admin I'm going to add to make sure my test has a side effect
pachctl auth modify-admins --remove daffyduck || true

go build -o /tmp/vault-plugins/$PLUGIN_NAME src/plugin/vault/main.go 

echo 'root' | vault login -

# Clean up from last run
vault secrets disable $PLUGIN_PATH 

# Enable the plugin
export SHASUM=$(shasum -a 256 "/tmp/vault-plugins/$PLUGIN_NAME" | cut -d " " -f1)
echo $SHASUM
vault write sys/plugins/catalog/$PLUGIN_NAME sha_256="$SHASUM" command="$PLUGIN_NAME"
vault secrets enable -path=$PLUGIN_PATH -plugin-name=$PLUGIN_NAME plugin

# Test login before admin token is set
vault write $PLUGIN_PATH/login username=tweetybird || true

# Set the admin token vault will use to create user creds
export ADMIN_TOKEN=$(cat ~/.pachyderm/config.json | jq -r .v1.session_token)
echo $ADMIN_TOKEN
vault write $PLUGIN_PATH/config \
    admin_token="${ADMIN_TOKEN}" \
	pachd_address="127.0.0.1:30650"

# Test login (failure/success):
vault write $PLUGIN_PATH/login username=bogusgithubusername || true
vault write $PLUGIN_PATH/login username=daffyduck ttl=125s

# To renew, use the 'token' field
# vault token renew ee9ab3ef-e398-3437-d382-b7aaf02f32e1

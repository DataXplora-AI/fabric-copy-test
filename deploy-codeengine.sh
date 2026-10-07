#!/usr/bin/env bash
# Build and run the test as an IBM Code Engine job.
#
#   ./deploy-codeengine.sh <code-engine-project> [env-file]
#
# The env file (default .env) is split into a secret (AZURE_*) and a configmap (everything else).
# Requires: ibmcloud CLI with the code-engine plugin, logged in and targeting the right region/RG.
set -euo pipefail

PROJECT="${1:?usage: $0 <code-engine-project> [env-file]}"
ENV_FILE="${2:-.env}"
JOB=fabric-copy-test
cd "$(dirname "$0")"

[ -f "$ENV_FILE" ] || { echo "env file $ENV_FILE not found (copy .env.example)" >&2; exit 1; }

tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT
grep -E '^AZURE_[A-Z_]+=.' "$ENV_FILE" > "$tmp/secret.env"
grep -E '^[A-Z_]+=.' "$ENV_FILE" | grep -vE '^AZURE_' > "$tmp/config.env"

ibmcloud ce project select --name "$PROJECT"

upsert() { # kind name file
  # Recreate instead of update: 'update --from-env-file' merges keys, so variables removed or
  # blanked in the env file (e.g. CONNECTOR_*) would otherwise linger in Code Engine.
  if ibmcloud ce "$1" get --name "$2" >/dev/null 2>&1; then
    ibmcloud ce "$1" delete --name "$2" --force
  fi
  if [ "$1" = secret ]; then
    ibmcloud ce secret create --name "$2" --format generic --from-env-file "$3"
  else
    ibmcloud ce configmap create --name "$2" --from-env-file "$3"
  fi
}
upsert secret "$JOB-secret" "$tmp/secret.env"
upsert configmap "$JOB-config" "$tmp/config.env"

job_args=(
  --name "$JOB"
  --build-source .
  --build-strategy dockerfile
  --env-from-secret "$JOB-secret"
  --env-from-configmap "$JOB-config"
  --cpu 1 --memory 4G
  --maxexecutiontime 1800
  --retrylimit 0
  --wait
)
if ibmcloud ce job get --name "$JOB" >/dev/null 2>&1; then
  ibmcloud ce job update "${job_args[@]}"
else
  ibmcloud ce job create "${job_args[@]}"
fi

run="$JOB-$(date +%Y%m%d%H%M%S)"
ibmcloud ce jobrun submit --job "$JOB" --name "$run" --wait --wait-timeout 1800 || true
ibmcloud ce jobrun logs --name "$run"
ibmcloud ce jobrun get --name "$run" | grep -E 'Succeeded|Failed'

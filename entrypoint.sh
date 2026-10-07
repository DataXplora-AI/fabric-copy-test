#!/bin/sh
# Routes Fabric traffic through IBM Satellite Connector endpoints, then runs the test.
#
# For each of OneLake (443), the Warehouse SQL endpoint (1433) and optionally the Fabric API (443):
#   - the Fabric FQDN is pinned to its own loopback address (127.0.0.x) in /etc/hosts
#   - socat listens on 127.0.0.x:<port> and forwards raw TCP to the Satellite Connector
#     endpoint (host:port on the IBM Cloud private network)
#   - the connector agent in AKS forwards to the Fabric private endpoint
#
# TLS stays end-to-end between this container and Fabric: the clients still use the real
# Fabric hostname for SNI, certificate validation and the TDS login packet, which is what
# the Fabric front ends need to route the request.
#
# If CONNECTOR_*_ENDPOINT is unset, that hop is skipped (direct connectivity, e.g. local runs).
set -eu

ONELAKE_HOST="${FABRIC_ONELAKE_HOST:-onelake.dfs.fabric.microsoft.com}"
SQL_HOST="${FABRIC_SQL_HOST:?FABRIC_SQL_HOST is required}"
SQL_PORT="${FABRIC_SQL_PORT:-1433}"
API_HOST="${FABRIC_API_HOST:-api.fabric.microsoft.com}"

tunnel() {
  fqdn="$1"; port="$2"; endpoint="$3"; ip="$4"
  [ -n "$endpoint" ] || { echo "[entrypoint] no connector endpoint for $fqdn, connecting directly"; return 0; }

  if ! printf '%s %s\n' "$ip" "$fqdn" >> /etc/hosts 2>/dev/null; then
    echo "[entrypoint] ERROR: /etc/hosts is not writable; the image must run as root (no USER in Dockerfile)" >&2
    exit 2
  fi

  socat -d "TCP-LISTEN:${port},bind=${ip},reuseaddr,fork" "TCP:${endpoint},connect-timeout=${CONNECT_TIMEOUT_SECONDS:-30}" &

  i=0
  until nc -z "$ip" "$port" 2>/dev/null; do
    i=$((i + 1)); [ "$i" -lt 50 ] || { echo "[entrypoint] socat on :$port did not start" >&2; exit 2; }
    sleep 0.1
  done
  echo "[entrypoint] $fqdn:$port -> $ip:$port -> satellite connector $endpoint"
}

tunnel "$ONELAKE_HOST" 443 "${CONNECTOR_ONELAKE_ENDPOINT:-}" 127.0.0.2
tunnel "$SQL_HOST" "$SQL_PORT" "${CONNECTOR_SQL_ENDPOINT:-}" 127.0.0.3
# Only needed if the Fabric REST API is not reachable over public egress (tenant-level private link).
tunnel "$API_HOST" 443 "${CONNECTOR_API_ENDPOINT:-}" 127.0.0.4

exec python /app/fabric_copy_test.py "$@"

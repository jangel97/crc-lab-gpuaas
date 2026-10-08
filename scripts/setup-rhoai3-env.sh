#!/usr/bin/env bash
#
# One-time OCP upgrade for sno-tenant2: 4.18 → 4.19.
#
# Run this once before running rhoai_3 tests. The RHOAI 3.x operator
# setup (Cert Manager, JobSet, RHOAI, DSC) is handled automatically
# by the test conftest (tests/rhoai_3/conftest.py).
#
# Usage:
#   KUBECONFIG=~/.kube/tenant2 bash scripts/setup-rhoai3-env.sh

set -euo pipefail

export KUBECONFIG="${KUBECONFIG:-$HOME/.kube/tenant2}"
OC="${OC:-oc}"

info()  { echo "==> $*"; }
warn()  { echo "WARNING: $*" >&2; }
fatal() { echo "FATAL: $*" >&2; exit 1; }

$OC version --client >/dev/null 2>&1 || fatal "oc not found on PATH"
$OC get nodes >/dev/null 2>&1 || fatal "Cannot connect to cluster — check KUBECONFIG=$KUBECONFIG"

info "Connected to cluster: $($OC whoami --show-server)"

current_version=$($OC get clusterversion version -o jsonpath='{.status.desired.version}')
info "Current OCP version: $current_version"

major_minor=$(echo "$current_version" | cut -d. -f1-2)

if [[ "$major_minor" == "4.19" ]] || awk "BEGIN{exit !($major_minor >= 4.19)}"; then
    info "OCP already at $current_version (>= 4.19) — nothing to do"
    exit 0
fi

info "Upgrading OCP from $current_version to 4.19..."

current_channel=$($OC get clusterversion version -o jsonpath='{.spec.channel}')
if [[ "$current_channel" != "stable-4.19" ]]; then
    info "Setting upgrade channel to stable-4.19 (was: $current_channel)"
    $OC adm upgrade channel stable-4.19
    sleep 10
fi

upgrade_available=$($OC adm upgrade 2>&1 || true)
if echo "$upgrade_available" | grep -q "No updates available"; then
    warn "No updates available on stable-4.19 channel yet. Check: oc adm upgrade"
    fatal "Cannot proceed without an available upgrade target"
fi

info "Triggering upgrade to latest 4.19..."
$OC adm upgrade --to-latest=true

info "Waiting for upgrade to complete (this can take 30-60 min on SNO)..."

deadline=$((SECONDS + 120))
while [ $SECONDS -lt $deadline ]; do
    progressing=$($OC get clusterversion version \
        -o jsonpath='{.status.conditions[?(@.type=="Progressing")].status}' 2>/dev/null || true)
    if [[ "$progressing" == "True" ]]; then
        info "Upgrade in progress..."
        break
    fi
    sleep 10
done

if ! $OC wait clusterversion/version --for=condition=Available --timeout=3600s; then
    fatal "OCP upgrade did not complete within 60 minutes"
fi

new_version=$($OC get clusterversion version -o jsonpath='{.status.desired.version}')
info "OCP upgraded to $new_version"

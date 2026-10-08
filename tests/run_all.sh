#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

# ---------------------------------------------------------------------------
# Pre-flight: ensure VMs, OpenShift, and VK are fully operational
# ---------------------------------------------------------------------------

echo "=== Pre-flight: checking lab environment ==="
python3 <<'PREFLIGHT'
import sys
import time
import urllib3
from lab_env import LabEnvironment, LabEnvironmentError
from kubernetes import client, config

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def fail(msg):
    print(f"PREFLIGHT FAILED: {msg}", file=sys.stderr)
    sys.exit(1)

def load_core(kubeconfig):
    api_client = config.new_client_from_config(config_file=kubeconfig)
    return client.CoreV1Api(api_client)

try:
    env = LabEnvironment()
except LabEnvironmentError as e:
    fail(f"Lab environment unavailable: {e}")

# Start worker first — tenant's VK depends on worker API being reachable
for vm in ("sno-worker", "sno-tenant"):
    state = env.vm_state(vm)
    if state != "running":
        print(f"  {vm} is {state}, starting...")
        env.start_vm(vm)
        print(f"  {vm} started (API responding, node Ready, stale pods cleaned)")
    else:
        print(f"  {vm} is running")

# Verify VK node is registered and Ready on tenant
print("  Waiting for VK node gpu-worker to be Ready on tenant...")
tenant_core = load_core(env._kubeconfig_path("sno-tenant"))
deadline = time.time() + 120
vk_ready = False
while time.time() < deadline:
    try:
        node = tenant_core.read_node(name="gpu-worker")
        for cond in (node.status.conditions or []):
            if cond.type == "Ready" and cond.status == "True":
                vk_ready = True
                break
        if vk_ready:
            break
    except client.exceptions.ApiException:
        pass
    time.sleep(5)

if not vk_ready:
    fail("VK node gpu-worker not Ready on tenant within 120s. Is the VK deployment running?")
print("  VK node gpu-worker is Ready")

# Verify VK can dispatch — check the config ConfigMap exists with a prefix
try:
    cm = tenant_core.read_namespaced_config_map(
        name="vk-gpu-provider-config-gpu-worker", namespace="kube-system",
    )
    prefix = cm.data.get("worker-namespace-prefix", "")
    if not prefix:
        fail("VK ConfigMap exists but worker-namespace-prefix is empty")
    print(f"  VK namespace prefix: {prefix}")
except client.exceptions.ApiException as e:
    fail(f"VK ConfigMap not found in kube-system: {e.reason}")

# Verify worker cluster is reachable from here (same network as VK)
worker_core = load_core(env._kubeconfig_path("sno-worker"))
try:
    nodes = worker_core.list_node()
    ready_nodes = [
        n.metadata.name for n in nodes.items
        if any(c.type == "Ready" and c.status == "True"
               for c in (n.status.conditions or []))
    ]
    if not ready_nodes:
        fail("Worker cluster has no Ready nodes")
    print(f"  Worker cluster Ready nodes: {', '.join(ready_nodes)}")
except Exception as e:
    fail(f"Cannot reach worker cluster API: {e}")

print("Pre-flight passed. All systems operational.")
PREFLIGHT

# ---------------------------------------------------------------------------
# Test suites
# ---------------------------------------------------------------------------

echo ""
echo "=== VK core tests (single tenant, 14GB) ==="
python3 -m pytest -m vk -v "$@"

echo ""
echo "=== RHOAI tests (single tenant, 28GB) ==="
python3 -m pytest -m rhoai -v "$@"

echo ""
echo "=== Kueue multi-tenant tests (dual tenant, 14GB) ==="
python3 -m pytest -m kueue -v "$@"

echo ""
echo "=== Networking tests (single tenant, 14GB) ==="
python3 -m pytest -m networking -v "$@"

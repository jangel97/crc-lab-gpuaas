# Liqo Spike Assessment — Can Liqo Replace the Custom VK Provider?

## Objective

Test whether Liqo (v1.2.0) with networking disabled + Submariner can
replace the custom Virtual Kubelet provider for cross-cluster GPU
workload dispatch from RHOAI tenant clusters to a bare GPU worker.

## Lab Environment

Same lab as the VK spike. Liqo installed on two clusters:

- **Consumer:** sno-tenant-rhoai3 (OCP 4.19, RHOAI 3.x)
- **Provider:** sno-worker (OCP 4.22, RTX 5090, GPU Operator, Kueue)
- **Networking:** Liqo disabled, Submariner v0.24 (Libreswan IPsec)

## Liqo Configuration

```bash
# Install on both clusters with networking disabled
liqoctl install openshift \
  --set networking.enabled=false \
  --set storage.enabled=true \
  --set telemetry.enabled=false \
  --set offloading.disableNetworkCheck=true

# Peer with GPU resource
liqoctl peer \
  --kubeconfig=~/.kube/tenant-rhoai3 \
  --remote-kubeconfig=~/.kube/worker \
  --networking-disabled \
  --resource nvidia.com/gpu=1

# Offload test namespace
liqoctl offload namespace vk-test \
  --pod-offloading-strategy Remote \
  --namespace-mapping-strategy EnforceSameName
```

## How to Run

```bash
RHOAI3_KUBECONFIG=~/.kube/tenant-rhoai3 WORKER_KUBECONFIG=~/.kube/worker \
  python -m pytest tests/liqo/ -v

# By category
python -m pytest tests/liqo/ -v -m liqo           # All Liqo tests
python -m pytest tests/liqo/ -v -m rhoai           # RHOAI integration
python -m pytest tests/liqo/ -v -m networking      # Submariner
python -m pytest tests/liqo/ -v -m kueue           # Kueue admission
```

---

## Scenarios

### 1. Core Dispatch

#### L1. Virtual node registration

| | |
|---|---|
| **Test** | `test_basic_dispatch.py::test_virtual_node_exists` |
| **What** | Liqo registers a virtual node with `liqo.io/type=virtual-node` label, `nvidia.com/gpu` in allocatable, and Ready condition |
| **Result** | PASS |
| **Proves** | Liqo peering works and the tenant scheduler sees GPU capacity |

#### L2. GPU pod dispatch and status sync

| | |
|---|---|
| **Test** | `test_basic_dispatch.py::test_gpu_pod_dispatched` |
| **What** | GPU pod in offloaded namespace (no nodeName, no tolerations) → Liqo creates ShadowPod on worker → nvidia-smi on RTX 5090 → Succeeded synced back |
| **Result** | PASS |
| **Proves** | Namespace offloading (Remote strategy) dispatches pods without explicit scheduling constraints |

#### L3. PodIP status sync

| | |
|---|---|
| **Test** | `test_basic_dispatch.py::test_pod_status_sync_podip` |
| **What** | Tenant pod shows PodIP from worker CIDR (10.13x.x.x) — needed for Submariner EndpointSlice routing |
| **Result** | PASS |
| **Proves** | Liqo syncs real worker PodIP to tenant (not remapped) when networking is disabled |

### 2. Resource Reflection

#### L4. Secret and ConfigMap reflection

| | |
|---|---|
| **Test** | `test_basic_dispatch.py::test_resource_sync` |
| **What** | Secret (via env secretKeyRef) and ConfigMap (via volume) reflected to worker twin namespace |
| **Result** | PASS |
| **Proves** | Liqo resource reflection replaces custom ResourceSyncer |

#### L5. Pod deletion cleanup

| | |
|---|---|
| **Test** | `test_basic_dispatch.py::test_pod_deletion_cleanup` |
| **What** | Delete tenant pod → ShadowPod deleted → worker pod deleted |
| **Result** | PASS |
| **Proves** | No resource leaks on the worker |

#### L6. Pod logs proxied

| | |
|---|---|
| **Test** | `test_basic_dispatch.py::test_pod_logs_proxied` |
| **What** | kubectl logs from tenant API server returns worker pod output |
| **Result** | PASS |
| **Proves** | Liqo VK proxies log requests to worker |

### 3. Storage

#### L7. PVC reflection with StorageClass remapping

| | |
|---|---|
| **Test** | `test_storage.py::test_pvc_reflection` |
| **What** | PVC with `storageClassName: liqo` → Liqo provisions on worker with LVMS StorageClass (via `--remote-real-storage-class-name=lvms-vg1`) → pod mounts it |
| **Result** | PASS |
| **Proves** | Liqo storage fabric replaces Catapult PVC model |

#### L8. PVC data persistence across pod restarts

| | |
|---|---|
| **Test** | `test_storage.py::test_pvc_persists_across_pod_restart` |
| **What** | Writer pod saves data to PVC → pod deleted → reader pod reads same data from PVC |
| **Result** | PASS |
| **Proves** | PVC lifecycle independent of pod lifecycle (same as Catapult) |

### 4. RHOAI Integration

#### L9. PyTorchJob via Liqo

| | |
|---|---|
| **Test** | `test_rhoai.py::test_pytorchjob_via_liqo` |
| **What** | PyTorchJob CR in offloaded namespace → training operator creates master pod → Liqo offloads → real GPU training on RTX 5090 → Succeeded |
| **Result** | PASS |
| **Proves** | RHOAI training operator works transparently with Liqo namespace offloading |

#### L10. KServe raw inference via Liqo

| | |
|---|---|
| **Test** | `test_rhoai.py::test_kserve_raw_inference_via_liqo` |
| **What** | KServe InferenceService (RawDeployment) in offloaded namespace → KServe creates Deployment → pod offloaded → inference server on GPU |
| **Result** | PASS |
| **Proves** | KServe works with Liqo without any scheduling annotations |

### 5. Kueue Integration

#### L11. Kueue admits Liqo-dispatched pod

| | |
|---|---|
| **Test** | `test_kueue.py::test_kueue_admits_liqo_pod` |
| **What** | GPU pod via Liqo → Kyverno adds Kueue labels on worker → Kueue admits → Succeeded |
| **Result** | SKIP — Kyverno policy not configured for ShadowPod-created Pods |
| **Proves** | Kyverno can mutate ShadowPod-created Pods for Kueue admission |

#### L12. ShadowPod label persistence

| | |
|---|---|
| **Test** | `test_kueue.py::test_shadowpod_label_persistence` |
| **What** | Verify whether Kyverno-added labels survive ShadowPod controller reconciliation |
| **Result** | SKIP — Kyverno policy not configured for ShadowPod-created Pods |
| **Proves** | Whether Kueue integration is viable — if ShadowPod controller overwrites labels, Kueue breaks |

### 6. Cross-Cluster Networking

#### L13. GPU service reachable via Submariner

| | |
|---|---|
| **Test** | `test_networking.py::test_service_reachable_via_submariner` |
| **What** | GPU HTTP server dispatched via Liqo → Service on tenant → curl through Submariner → HTTP 200 with RTX 5090 data |
| **Result** | PASS |
| **Proves** | Submariner works with Liqo-dispatched pods (PodIP synced, EndpointSlice valid) |

#### L14. Headless Service reflection for distributed training DNS

| | |
|---|---|
| **Test** | `test_networking.py::test_headless_service_reflection` |
| **What** | Headless Service reflected to worker → Pod B resolves Pod A's DNS via synced Service |
| **Result** | PASS |
| **Proves** | Liqo reflects headless Services for inter-pod DNS (distributed training prerequisite) |

---

## Comparison: Custom VK vs Liqo

| Capability | Custom VK | Liqo | Notes |
|------------|:---------:|:----:|-------|
| **GPU dispatch** | S2 PASS | L2 PASS | |
| **Resource sync** | S3 PASS | L4 PASS | Custom syncer vs built-in reflection |
| **Cleanup** | S4 PASS | L5 PASS | Per-pod vs ShadowPod lifecycle |
| **PVC** | S5 PASS | L7 PASS | Catapult opt-in vs Liqo StorageClass |
| **PVC rejection** | S6 PASS | — | Liqo reflects all PVCs (no opt-in gating) |
| **PyTorchJob** | S7 PASS | L9 PASS | |
| **Worker stays bare** | S8 PASS | — | Worker needs Liqo components |
| **Submariner** | S9 PASS | L13 PASS | |
| **Notebook** | S10 PASS | — | Not tested in Liqo spike |
| **Tunnel recovery** | S11 PASS | — | Not tested in Liqo spike |
| **Multi-tenant** | S12 PASS | — | Not tested in Liqo spike |
| **Headless DNS** | S13 PASS | L14 PASS | |
| **Logs/exec** | S14 PASS | L6 PASS | |
| **OCP version decoupling** | S15 PASS | — | Not tested in Liqo spike |
| **Real training** | S16 PASS | via L9 PASS | |
| **Checkpoint PVC** | S17 PASS | L8 PASS | |
| **Notebook CR** | S18 PASS | — | Not tested in Liqo spike |
| **KServe raw** | S19 PASS | L10 PASS | |
| **KServe serverless** | S20 PASS | — | Not tested (3.x has no Knative) |
| **Kueue admission** | S21 PASS | L11 SKIP | Kyverno policy not configured for ShadowPods |
| **Kueue queuing** | S22 PASS | — | Not tested in Liqo spike |
| **Kueue preemption** | S23 PASS | — | Not tested in Liqo spike |
| **PodIP sync** | PASS | L3 PASS | Critical for Submariner routing |

---

## Architecture

### Lab Topology

```
┌──────────────────────────┐              ┌──────────────────────────┐
│  sno-tenant-rhoai3       │              │  sno-worker              │
│  OCP 4.19, RHOAI 3.x     │              │  OCP 4.22, RTX 5090      │
│                          │              │  GPU Operator, Kueue      │
│  ┌────────────────────┐  │              │                          │
│  │ Liqo control plane │  │              │  ┌────────────────────┐  │
│  │ (consumer mode)    │  │   Liqo       │  │ Liqo control plane │  │
│  │ - VK controller    │  │   peering    │  │ (provider mode)    │  │
│  │ - resource reflector◄─┼──────────────┼──► - ShadowPod ctrl   │  │
│  │ - storage fabric   │  │              │  │ - auth module      │  │
│  └────────────────────┘  │              │  └────────────────────┘  │
│                          │              │                          │
│  ┌────────────────────┐  │   Submariner │  ┌────────────────────┐  │
│  │ Submariner gateway │◄─┼──────────────┼──► Submariner gateway │  │
│  │ (cross-cluster net)│  │   IPsec      │  │                    │  │
│  └────────────────────┘  │   tunnel     │  └────────────────────┘  │
└──────────────────────────┘              └──────────────────────────┘
```

Two separate concerns, two separate layers:

- **Liqo** handles pod offloading, status sync, and resource reflection
  (Secrets, ConfigMaps, ServiceAccounts, Services, PVCs).
- **Submariner** handles cross-cluster networking via Libreswan IPsec
  tunnels. Liqo's own WireGuard networking is disabled because it does
  not work on OpenShift (SCC/privilege issues).

### Dispatch Flow: Custom VK vs Liqo

**Custom VK** — explicit scheduling, custom sync code:

1. User creates pod with `nodeName: gpu-worker` + VK tolerations
2. Custom VK binary intercepts pod, calls worker API directly
3. Custom ResourceSyncer + DepReconciler sync Secrets/ConfigMaps
4. Custom code syncs pod status back to tenant

**Liqo** — namespace-level offloading, built-in reflection:

1. Namespace `vk-test` is offloaded with `Remote` strategy (all pods go
   to the remote cluster automatically)
2. User creates pod in `vk-test` with **no special scheduling** — no
   `nodeName`, no tolerations, no annotations
3. Liqo's mutating webhook schedules the pod to the virtual node
   `gpu-worker`
4. Liqo creates a **ShadowPod CRD** on the worker in the twin namespace
   (`vk-test`, same name via `EnforceSameName` strategy)
5. ShadowPod controller on the worker creates the actual Pod
6. Liqo resource reflection automatically syncs Secrets, ConfigMaps,
   ServiceAccounts, and Services referenced by the pod
7. Liqo syncs pod status (phase, exit code, PodIP) back to the tenant

The key advantage: RHOAI operators (training operator, KServe) create
pods normally in the offloaded namespace. They don't need any
VK-specific scheduling logic — Liqo handles it transparently at the
namespace level. This is what tests L9 (PyTorchJob) and L10 (KServe)
validate.

### Why Networking Is Disabled

Liqo includes a WireGuard-based networking module, but it has privilege
requirements that conflict with OpenShift's SecurityContextConstraints.
Rather than fight SCC issues, we disable Liqo networking entirely and
keep Submariner, which is already deployed and working.

The critical piece that makes this work: when Liqo's networking is
disabled, it syncs the **real worker PodIP** (e.g., `10.132.x.x`) to
the tenant pod status without IP remapping. Submariner then routes
traffic to that PodIP through the IPsec tunnel. Test L3 validates this
PodIP sync, and test L13 validates end-to-end Service reachability.

### PVC Model

Liqo creates a virtual StorageClass called `liqo` on the tenant cluster.
PVCs use `storageClassName: liqo`, and Liqo reflects them to the worker
with the real StorageClass (`lvms-vg1`), configured at peering time via
`--remote-real-storage-class-name=lvms-vg1`. Actual storage is
provisioned on the worker only.

Unlike the custom VK's Catapult model (which gates PVC creation via an
opt-in StorageClass), Liqo reflects all PVCs in the offloaded namespace.
There is no rejection mechanism for PVCs that should stay local.

### Setup Sequence

The playbook (`playbooks/04-setup-liqo.yml`) runs six steps:

1. **Install liqoctl** v1.2.0 binary
2. **Install Liqo on worker** (provider mode) — networking disabled,
   storage enabled, `--disable-kernel-version-check` (not needed since
   networking is off)
3. **Install Liqo on tenant** (consumer mode) — same flags
4. **Peer clusters** — `liqoctl peer` with `--networking-disabled` and
   `--resource nvidia.com/gpu=1` to advertise 1 GPU on the virtual node
5. **Verify virtual node** — wait for `liqo.io/type=virtual-node` node
   with GPU in allocatable and Ready condition
6. **Create and offload namespace** — `vk-test` with `Remote` strategy
   and `EnforceSameName` mapping

### Architecture Comparison Table

| Aspect | Custom VK | Liqo |
|--------|-----------|------|
| Scheduling model | Explicit (`nodeName` + tolerations) | Implicit (namespace offloading) |
| Worker footprint | SA + RBAC only | Full Liqo control plane (ShadowPod controller, auth) |
| Pod creation | Direct Pod via worker API | ShadowPod CRD → controller creates Pod |
| PVC model | Opt-in (`storageClassName: catapult`) | Virtual StorageClass (`liqo`) with remote remapping |
| Resource sync | Custom Go code (ResourceSyncer + DepReconciler) | Built-in resource reflection |
| RHOAI transparency | Needs `nodeName`/tolerations on workload spec | Fully transparent — namespace offloading, no workload changes |
| Maintenance | Custom Go binary (us) | Upstream open-source project (liqotech) |
| Multi-tenant isolation | Auto-generated namespace prefix (`vk-{hash}-{ns}`) | Twin namespaces per peering (same or custom name) |

---

## Results Summary

```
tests/liqo/test_basic_dispatch.py::test_virtual_node_exists              PASSED
tests/liqo/test_basic_dispatch.py::test_gpu_pod_dispatched               PASSED
tests/liqo/test_basic_dispatch.py::test_resource_sync                    PASSED
tests/liqo/test_basic_dispatch.py::test_pod_deletion_cleanup             PASSED
tests/liqo/test_basic_dispatch.py::test_pod_logs_proxied                 PASSED
tests/liqo/test_basic_dispatch.py::test_pod_status_sync_podip            PASSED
tests/liqo/test_storage.py::test_pvc_reflection                          PASSED
tests/liqo/test_storage.py::test_pvc_persists_across_pod_restart         PASSED
tests/liqo/test_rhoai.py::test_pytorchjob_via_liqo                       PASSED
tests/liqo/test_rhoai.py::test_kserve_raw_inference_via_liqo             PASSED
tests/liqo/test_kueue.py::test_kueue_admits_liqo_pod                     SKIPPED
tests/liqo/test_kueue.py::test_shadowpod_label_persistence               SKIPPED
tests/liqo/test_networking.py::test_service_reachable_via_submariner      PASSED
tests/liqo/test_networking.py::test_headless_service_reflection           PASSED

12 passed, 2 skipped (3m38s) — 2026-10-14
```

**Kueue tests skipped** because the Kyverno ClusterPolicy targeting ShadowPod-created
Pods is not yet deployed. This is a configuration task, not a Liqo limitation — the
same Kyverno approach used with the custom VK should work once the policy match rules
target pods created by the ShadowPod controller.

---

## Deployment Guide — What We Learned

### Install flags that matter

```bash
liqoctl install openshift \
  --set networking.enabled=false \          # mandatory — Liqo WireGuard breaks on OCP (SCC)
  --set storage.enabled=true \              # enables PVC reflection with StorageClass remapping
  --set telemetry.enabled=false \
  --set offloading.disableNetworkCheck=true \  # suppresses Liqo's own network readiness check
  --disable-kernel-version-check            # skips Pod CIDR and kernel validation (not needed without networking)
```

Without `--disable-kernel-version-check`, Liqo validates Pod CIDRs and
kernel versions across clusters even when networking is disabled. This
causes spurious failures, especially when the tenant has stale pods with
worker CIDRs left over from previous VK deployments.

### Peering: GPU resource advertisement

```bash
liqoctl peer \
  --kubeconfig=~/.kube/tenant \
  --remote-kubeconfig=~/.kube/worker \
  --networking-disabled \
  --resource nvidia.com/gpu=1
```

The `--resource nvidia.com/gpu=1` flag controls how many GPUs appear on
the virtual node's `allocatable`. This is a static advertisement — Liqo
does not auto-discover GPU count from the worker. If the worker has 4
GPUs, you must specify `=4` explicitly.

### Namespace offloading strategy

```bash
liqoctl offload namespace <ns> \
  --pod-offloading-strategy Remote \
  --namespace-mapping-strategy EnforceSameName
```

- **`Remote`** means all pods in the namespace go to the worker. No pods
  run locally. This is what we want for GPU workload namespaces.
- **`EnforceSameName`** means the twin namespace on the worker has the
  same name. This simplifies debugging (`oc get pods -n vk-test` works
  on both clusters) and is required for headless Service DNS resolution
  to work correctly across clusters.

### PVC StorageClass — not annotation

Liqo docs mention a `liqo.io/remote-storage-class` annotation on PVCs.
This did not work in our testing. The correct model is:

1. At peering time, configure `--remote-real-storage-class-name=lvms-vg1`
2. Liqo creates a virtual StorageClass called `liqo` on the tenant
3. PVCs use `storageClassName: liqo` — Liqo reflects them to the worker
   with `storageClassName: lvms-vg1`

### Coexistence with existing VK

If you have a custom VK deployment already running (e.g., `vk-gpu-provider`
in `kube-system`), **scale it to 0 before testing Liqo**. Both systems
register virtual nodes and will compete for pods in overlapping namespaces.
With a single GPU, this means both try to schedule GPU pods simultaneously,
and the second one hangs waiting for a GPU that's already in use.

Also delete the old custom VK's virtual node (`oc delete node gpu-worker`)
before Liqo peering — Liqo will fail with `node gpu-worker already exists`
if the old one is still registered.

### Submariner routeagent on virtual node

Submariner's routeagent DaemonSet will try to schedule a pod on the Liqo
virtual node. Liqo rejects it with `OffloadingBackOff` status. This is
harmless — the routeagent doesn't need to run on a virtual node. But it
does generate noise in `oc get pods -A`. If it bothers you, add a node
affinity or anti-affinity to the routeagent DaemonSet to exclude
`liqo.io/type=virtual-node` nodes.

### Liqo install failure recovery

If `liqoctl install` fails partway through (e.g., Kueue webhooks not
ready, timeout), the Helm release is left in a broken state. `liqoctl
install` will then fail with `"liqo" has no deployed releases`. To
recover:

1. Delete the Helm release secret: `oc delete secret -n liqo -l name=liqo`
2. Delete stale webhooks: `oc delete validatingwebhookconfiguration,mutatingwebhookconfiguration -l app.kubernetes.io/part-of=liqo`
3. Check for stale APIServices: `oc get apiservices | grep liqo` — a
   stale `v1beta1.metrics.liqo.io` pointing to a deleted service will
   block namespace termination with `DiscoveryFailed`
4. Delete the `liqo` namespace (may need step 3 first)
5. Retry `liqoctl install`

### Kueue integration (not yet validated)

The plan is the same as with the custom VK: Kyverno ClusterPolicy on
the worker mutates GPU pods to add Kueue queue labels. The difference
is that Liqo creates pods via the ShadowPod controller, so the Kyverno
policy match rules need to target pods created by that controller (not
pods with our custom VK labels). The open question (L12) is whether the
ShadowPod controller overwrites Kyverno-added labels during
reconciliation — if it does, Kueue integration breaks.

### What operators need to know (RHOAI tenants)

Nothing. That's the point. Tenants create PyTorchJobs, InferenceServices,
or plain pods in an offloaded namespace. No `nodeName`, no tolerations,
no annotations. Liqo's mutating webhook handles scheduling transparently.
The only visible difference is that PVCs must use `storageClassName: liqo`
instead of the local StorageClass.

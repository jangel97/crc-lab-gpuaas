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
| **Result** | |
| **Proves** | Liqo peering works and the tenant scheduler sees GPU capacity |

#### L2. GPU pod dispatch and status sync

| | |
|---|---|
| **Test** | `test_basic_dispatch.py::test_gpu_pod_dispatched` |
| **What** | GPU pod in offloaded namespace (no nodeName, no tolerations) → Liqo creates ShadowPod on worker → nvidia-smi on RTX 5090 → Succeeded synced back |
| **Result** | |
| **Proves** | Namespace offloading (Remote strategy) dispatches pods without explicit scheduling constraints |

#### L3. PodIP status sync

| | |
|---|---|
| **Test** | `test_basic_dispatch.py::test_pod_status_sync_podip` |
| **What** | Tenant pod shows PodIP from worker CIDR (10.13x.x.x) — needed for Submariner EndpointSlice routing |
| **Result** | |
| **Proves** | Liqo syncs real worker PodIP to tenant (not remapped) when networking is disabled |

### 2. Resource Reflection

#### L4. Secret and ConfigMap reflection

| | |
|---|---|
| **Test** | `test_basic_dispatch.py::test_resource_sync` |
| **What** | Secret (via env secretKeyRef) and ConfigMap (via volume) reflected to worker twin namespace |
| **Result** | |
| **Proves** | Liqo resource reflection replaces custom ResourceSyncer |

#### L5. Pod deletion cleanup

| | |
|---|---|
| **Test** | `test_basic_dispatch.py::test_pod_deletion_cleanup` |
| **What** | Delete tenant pod → ShadowPod deleted → worker pod deleted |
| **Result** | |
| **Proves** | No resource leaks on the worker |

#### L6. Pod logs proxied

| | |
|---|---|
| **Test** | `test_basic_dispatch.py::test_pod_logs_proxied` |
| **What** | kubectl logs from tenant API server returns worker pod output |
| **Result** | |
| **Proves** | Liqo VK proxies log requests to worker |

### 3. Storage

#### L7. PVC reflection with StorageClass remapping

| | |
|---|---|
| **Test** | `test_storage.py::test_pvc_reflection` |
| **What** | PVC with `liqo.io/remote-storage-class: lvms-vg1` → reflected to worker with LVMS StorageClass → pod mounts it |
| **Result** | |
| **Proves** | Liqo storage fabric replaces Catapult PVC model |

#### L8. PVC data persistence across pod restarts

| | |
|---|---|
| **Test** | `test_storage.py::test_pvc_persists_across_pod_restart` |
| **What** | Writer pod saves data to PVC → pod deleted → reader pod reads same data from PVC |
| **Result** | |
| **Proves** | PVC lifecycle independent of pod lifecycle (same as Catapult) |

### 4. RHOAI Integration

#### L9. PyTorchJob via Liqo

| | |
|---|---|
| **Test** | `test_rhoai.py::test_pytorchjob_via_liqo` |
| **What** | PyTorchJob CR in offloaded namespace → training operator creates master pod → Liqo offloads → real GPU training on RTX 5090 → Succeeded |
| **Result** | |
| **Proves** | RHOAI training operator works transparently with Liqo namespace offloading |

#### L10. KServe raw inference via Liqo

| | |
|---|---|
| **Test** | `test_rhoai.py::test_kserve_raw_inference_via_liqo` |
| **What** | KServe InferenceService (RawDeployment) in offloaded namespace → KServe creates Deployment → pod offloaded → inference server on GPU |
| **Result** | |
| **Proves** | KServe works with Liqo without any scheduling annotations |

### 5. Kueue Integration

#### L11. Kueue admits Liqo-dispatched pod

| | |
|---|---|
| **Test** | `test_kueue.py::test_kueue_admits_liqo_pod` |
| **What** | GPU pod via Liqo → Kyverno adds Kueue labels on worker → Kueue admits → Succeeded |
| **Result** | |
| **Proves** | Kyverno can mutate ShadowPod-created Pods for Kueue admission |

#### L12. ShadowPod label persistence

| | |
|---|---|
| **Test** | `test_kueue.py::test_shadowpod_label_persistence` |
| **What** | Verify whether Kyverno-added labels survive ShadowPod controller reconciliation |
| **Result** | |
| **Proves** | Whether Kueue integration is viable — if ShadowPod controller overwrites labels, Kueue breaks |

### 6. Cross-Cluster Networking

#### L13. GPU service reachable via Submariner

| | |
|---|---|
| **Test** | `test_networking.py::test_service_reachable_via_submariner` |
| **What** | GPU HTTP server dispatched via Liqo → Service on tenant → curl through Submariner → HTTP 200 with RTX 5090 data |
| **Result** | |
| **Proves** | Submariner works with Liqo-dispatched pods (PodIP synced, EndpointSlice valid) |

#### L14. Headless Service reflection for distributed training DNS

| | |
|---|---|
| **Test** | `test_networking.py::test_headless_service_reflection` |
| **What** | Headless Service reflected to worker → Pod B resolves Pod A's DNS via synced Service |
| **Result** | |
| **Proves** | Liqo reflects headless Services for inter-pod DNS (distributed training prerequisite) |

---

## Comparison: Custom VK vs Liqo

| Capability | Custom VK | Liqo | Notes |
|------------|:---------:|:----:|-------|
| **GPU dispatch** | S2 PASS | L2 | |
| **Resource sync** | S3 PASS | L4 | Custom syncer vs built-in reflection |
| **Cleanup** | S4 PASS | L5 | Per-pod vs ShadowPod lifecycle |
| **PVC** | S5 PASS | L7 | Catapult opt-in vs Liqo annotation |
| **PVC rejection** | S6 PASS | — | Liqo reflects all PVCs (no opt-in gating) |
| **PyTorchJob** | S7 PASS | L9 | |
| **Worker stays bare** | S8 PASS | — | Worker needs Liqo components |
| **Submariner** | S9 PASS | L13 | |
| **Notebook** | S10 PASS | — | Not tested in Liqo spike |
| **Tunnel recovery** | S11 PASS | — | Not tested in Liqo spike |
| **Multi-tenant** | S12 PASS | — | Not tested in Liqo spike |
| **Headless DNS** | S13 PASS | L14 | |
| **Logs/exec** | S14 PASS | L6 | |
| **OCP version decoupling** | S15 PASS | — | Not tested in Liqo spike |
| **Real training** | S16 PASS | via L9 | |
| **Checkpoint PVC** | S17 PASS | L8 | |
| **Notebook CR** | S18 PASS | — | Not tested in Liqo spike |
| **KServe raw** | S19 PASS | L10 | |
| **KServe serverless** | S20 PASS | — | Not tested (3.x has no Knative) |
| **Kueue admission** | S21 PASS | L11 | Via Kyverno on both |
| **Kueue queuing** | S22 PASS | — | Not tested in Liqo spike |
| **Kueue preemption** | S23 PASS | — | Not tested in Liqo spike |
| **PodIP sync** | PASS | L3 | Critical for Submariner routing |

---

## Architecture Differences

| Aspect | Custom VK | Liqo |
|--------|-----------|------|
| Scheduling model | Explicit (`nodeName` + tolerations) | Implicit (namespace offloading) |
| Worker footprint | SA + RBAC only | Full Liqo control plane |
| Pod creation | Direct Pod via API | ShadowPod CRD → controller creates Pod |
| PVC model | Opt-in (catapult StorageClass) | Annotation-based remapping |
| Resource sync | Custom code (ResourceSyncer + DepReconciler) | Built-in resource reflection |
| Maintenance | Custom Go binary | Upstream open-source project |

---

## Results Summary

```
tests/liqo/test_basic_dispatch.py::test_virtual_node_exists              ...
tests/liqo/test_basic_dispatch.py::test_gpu_pod_dispatched               ...
tests/liqo/test_basic_dispatch.py::test_resource_sync                    ...
tests/liqo/test_basic_dispatch.py::test_pod_deletion_cleanup             ...
tests/liqo/test_basic_dispatch.py::test_pod_logs_proxied                 ...
tests/liqo/test_basic_dispatch.py::test_pod_status_sync_podip            ...
tests/liqo/test_storage.py::test_pvc_reflection                          ...
tests/liqo/test_storage.py::test_pvc_persists_across_pod_restart         ...
tests/liqo/test_rhoai.py::test_pytorchjob_via_liqo                       ...
tests/liqo/test_rhoai.py::test_kserve_raw_inference_via_liqo             ...
tests/liqo/test_kueue.py::test_kueue_admits_liqo_pod                     ...
tests/liqo/test_kueue.py::test_shadowpod_label_persistence               ...
tests/liqo/test_networking.py::test_service_reachable_via_submariner      ...
tests/liqo/test_networking.py::test_headless_service_reflection           ...
```

*(Fill in results after running tests against live clusters)*

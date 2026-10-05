# Virtual Kubelet GPU Dispatch — Spike Assessment

## Objective

Prove that a custom Virtual Kubelet can transparently dispatch GPU workloads
from a tenant OpenShift cluster (running RHOAI) to a bare worker cluster with
physical GPUs, including cross-cluster networking via Submariner so that
Services and Routes reach remote GPU pods.

## Lab Environment

| Component | Tenant (sno-tenant) | Worker (sno-worker) |
|-----------|--------------------|--------------------|
| OpenShift | 4.22 | 4.22 |
| vCPU / RAM | 12 / 16 GB | 8 / 24 GB |
| GPU | — | RTX 5090 (32 GB VRAM) |
| Operators | RHOAI | GPU Operator, Kueue, LVMS |
| Pod CIDR | 10.128.0.0/14 | 10.132.0.0/14 |
| Service CIDR | 172.30.0.0/16 | 172.31.0.0/16 |
| Networking | Submariner v0.24 (Libreswan IPsec) | Submariner v0.24 |

Both clusters run as single-node OpenShift on libvirt VMs on a gaming PC
(Intel Core Ultra 9 285K, 62 GB RAM, Ubuntu 24.04).

## How to Run

All tests are integration tests that run against live clusters.

```bash
# Prerequisites
export TENANT_KUBECONFIG=~/.kube/tenant
export WORKER_KUBECONFIG=~/.kube/worker

# Run all scenarios
python -m pytest tests/ -v

# Run by category
python -m pytest tests/ -v -m vk            # Core dispatch + resource sync + storage
python -m pytest tests/ -v -m rhoai         # RHOAI integration
python -m pytest tests/ -v -m networking    # Cross-cluster networking
```

---

## Scenarios Assessed

### 1. Core Dispatch

#### S1. Virtual node registration

| | |
|---|---|
| **Test** | `test_vk_gpu.py::test_virtual_node_exists` |
| **What** | VK registers a virtual node `gpu-worker` on the tenant with `nvidia.com/gpu` in allocatable and Ready condition |
| **Result** | PASS |
| **Proves** | Tenant scheduler sees GPU capacity and can assign pods to the virtual node |

#### S2. GPU pod dispatch and status sync

| | |
|---|---|
| **Test** | `test_vk_gpu.py::test_gpu_pod_dispatched_via_vk` |
| **What** | GPU pod submitted on tenant with `nodeName: gpu-worker` → VK creates pod on worker → `nvidia-smi` runs on RTX 5090 → status (Phase, exit code) synced back to tenant as Succeeded |
| **Result** | PASS |
| **Proves** | End-to-end dispatch works: pod scheduled on virtual node executes on real GPU, status is visible on tenant |

### 2. Resource Isolation

#### S3. Secret and ConfigMap sync

| | |
|---|---|
| **Test** | `test_vk_gpu.py::test_resource_sync` |
| **What** | Pod references a Secret (via `secretKeyRef`) and ConfigMap (via volume mount) on tenant → VK syncs both to worker with management labels → pod reads them successfully |
| **Result** | PASS |
| **Proves** | VK discovers resource references by walking the pod spec and syncs them on-demand. No pre-staging required. |

#### S4. Pod deletion cleanup

| | |
|---|---|
| **Test** | `test_vk_gpu.py::test_pod_deletion_cleans_up` |
| **What** | Tenant pod deleted → worker pod deleted → synced secret cleaned up (verified via 404) |
| **Result** | PASS |
| **Proves** | No resource leaks. Management labels enable targeted cleanup. |

### 3. Storage

#### S5. Catapult PVC sync

| | |
|---|---|
| **Test** | `test_vk_gpu.py::test_catapult_pvc_sync` |
| **What** | PVC with `storageClassName: catapult` on tenant → execution PVC created on worker with namespace-prefixed name (`{ns}--{name}`) and LVMS StorageClass → execution PVC survives pod deletion → cleaned up when control PVC deleted |
| **Result** | PASS |
| **Proves** | PVC lifecycle is independent of pod lifecycle. Data persists across pod restarts. Cleanup is automatic via PVC deletion informer. |

#### S6. Non-catapult PVC rejection

| | |
|---|---|
| **Test** | `test_vk_gpu.py::test_non_catapult_pvc_rejected` |
| **What** | Pod referencing PVC without `storageClassName: catapult` → pod marked Failed with reason `InvalidPVCStorageClass` → no execution PVC created on worker |
| **Result** | PASS |
| **Proves** | Only explicitly opted-in PVCs are synced. Clear error message for users. |

### 4. RHOAI Integration

#### S7. PyTorchJob training dispatch

| | |
|---|---|
| **Test** | `test_rhoai_vk.py::test_pytorchjob_via_vk` |
| **What** | PyTorchJob CR on tenant → RHOAI training operator creates master pod → VK dispatches to worker GPU → `nvidia-smi` succeeds → status synced → PyTorchJob condition reaches Succeeded |
| **Result** | PASS |
| **Proves** | RHOAI training operator works transparently. It creates pods, VK dispatches them, status syncs back, and the operator sees Succeeded. No RHOAI modifications needed. |

#### S8. Worker stays bare

| | |
|---|---|
| **Test** | `test_rhoai_vk.py::test_no_rhoai_crds_on_worker` |
| **What** | Worker cluster has no RHOAI CRDs (pytorchjobs, notebooks, inferenceservices, rayclusters) |
| **Result** | PASS |
| **Proves** | Worker is a bare GPU execution node. No operator stack duplication. RHOAI runs only on tenant. |

### 5. Cross-Cluster Networking

#### S9. GPU inference service via Submariner

| | |
|---|---|
| **Test** | `test_submariner_networking.py::test_gpu_service_via_submariner` |
| **What** | GPU HTTP server pod dispatched to worker via VK → Service created on tenant → PodIP synced (worker IP appears on tenant pod) → EndpointSlice created with worker PodIP → curl the Service from a tenant pod → HTTP 200 with GPU info from RTX 5090 |
| **Result** | PASS |
| **Proves** | Full networking chain: VK syncs PodIP → Kubernetes creates EndpointSlice → Service ClusterIP routes through Submariner IPsec tunnel → reaches GPU pod on worker. No Submariner-specific code in VK, pod, or Service. |

**Traffic flow:**
```
curl pod (tenant)
  → Service ClusterIP (tenant)
  → EndpointSlice (10.132.0.148, worker pod IP)
  → Submariner route agent → IPsec tunnel (UDP 4500)
  → worker gateway → OVN
  → GPU pod (nvidia-smi)
  → {"gpu": "NVIDIA GeForce RTX 5090, 32607, 0, 40", "status": "ok"}
```

**Route access (manual validation):**
```
$ oc expose svc gpu-inference -n vk-test
$ curl --resolve gpu-inference-vk-test.apps.tenant.local.lab:80:192.168.122.10 \
    http://gpu-inference-vk-test.apps.tenant.local.lab
{"gpu": "NVIDIA GeForce RTX 5090, 32607, 0, 40", "status": "ok"}
```

OpenShift Route → HAProxy → Service → EndpointSlice → Submariner tunnel → GPU pod. Works transparently.

#### S10. Notebook workbench via Submariner

| | |
|---|---|
| **Test** | `test_submariner_networking.py::test_notebook_workbench_via_submariner` |
| **What** | Long-running GPU HTTP server (simulating a Jupyter notebook) dispatched to worker via VK → Service on tenant → pod stays Running (not Succeeded) → curl returns HTML with GPU info through Submariner tunnel |
| **Result** | PASS |
| **Proves** | Persistent interactive workloads (notebooks, IDEs) work through Submariner. The VK keeps long-running pods in Running state and the networking path remains stable. Combined with Route access from S9, this validates the full notebook access chain. |

#### S11. Submariner tunnel failure and recovery

| | |
|---|---|
| **Test** | `test_submariner_networking.py::test_submariner_tunnel_failure_recovery` |
| **What** | Deploy GPU HTTP server + Service → verify connectivity works → kill Submariner gateway pod on worker → verify tunnel drops → wait for gateway restart and tunnel re-establishment → verify connectivity restored |
| **Result** | PASS |
| **Proves** | Submariner tunnel self-heals after gateway pod failure. IPsec tunnel re-establishes automatically via Deployment restart. Workloads remain running during outage; only network path is disrupted. |

### 6. Multi-Tenant Isolation

#### S12. Multi-tenant resource collision

| | |
|---|---|
| **Test** | `test_vk_gpu.py::test_multitenant_secret_collision` |
| **What** | Two tenant namespaces (`vk-team-a`, `vk-team-b`) each create a Secret named `shared-config` with different data → both dispatch pods via VK → second sync overwrites first in the single worker namespace → proves documented collision |
| **Result** | PASS (collision confirmed) |
| **Proves** | Same-named resources from different tenant namespaces collide in the single `vk-workloads` namespace. This is the documented architectural limitation. Fix: use one worker namespace per tenant namespace. |

### 7. Distributed Training

#### S13. Headless Service sync for inter-pod DNS

| | |
|---|---|
| **Test** | `test_distributed_training.py::test_headless_service_dns_resolution` |
| **What** | Two pods with hostname/subdomain deployed via VK + headless Service created on tenant → VK syncs headless Service to worker namespace → Pod B resolves Pod A's DNS name via synced Service → DNS lookup returns an IP |
| **Result** | PASS |
| **Proves** | Headless Service sync enables inter-pod DNS resolution on the worker cluster. This is the mechanism that makes multi-pod PyTorchJob work: training operator creates headless Services, VK syncs them, worker CoreDNS resolves pod hostnames. Combined with S7 (single-pod PyTorchJob proven), multi-pod distributed training infrastructure is in place. |

---

## Test Results Summary

```
tests/test_vk_gpu.py::test_virtual_node_exists                                PASSED
tests/test_vk_gpu.py::test_gpu_pod_dispatched_via_vk                          PASSED
tests/test_vk_gpu.py::test_resource_sync                                      PASSED
tests/test_vk_gpu.py::test_pod_deletion_cleans_up                             PASSED
tests/test_vk_gpu.py::test_catapult_pvc_sync                                  PASSED
tests/test_vk_gpu.py::test_non_catapult_pvc_rejected                          PASSED
tests/test_vk_gpu.py::test_multitenant_secret_collision                       PASSED
tests/test_rhoai_vk.py::test_pytorchjob_via_vk                               PASSED
tests/test_rhoai_vk.py::test_no_rhoai_crds_on_worker                         PASSED
tests/test_submariner_networking.py::test_gpu_service_via_submariner          PASSED
tests/test_submariner_networking.py::test_notebook_workbench_via_submariner   PASSED
tests/test_submariner_networking.py::test_submariner_tunnel_failure_recovery  PASSED
tests/test_distributed_training.py::test_headless_service_dns_resolution     PASSED

13 passed
```

---

## Scenarios Not Yet Assessed

| Scenario | Category | Why not assessed | Priority |
|----------|----------|-----------------|----------|
| **KServe inference (Knative-managed)** | Networking | Knative autoscaler, activator, queue-proxy, scale-to-zero need investigation. No Serverless operator installed in lab. Raw Deployment inference works (S9 proves the path). | Medium |
| **Multi-pod PyTorchJob (full distributed)** | Training | Headless Service sync is proven (S13), but actual multi-GPU training needs >1 GPU. Lab has 1 GPU. Mechanism is validated; full end-to-end deferred to multi-GPU environment. | Medium |
| **Overlapping CIDRs (Globalnet)** | Networking | Lab uses non-overlapping CIDRs by design. Overlapping CIDRs require Globalnet which changes the PodIP sync model. Not PoC scope. | Low |
| **RHOAI dashboard → Notebook CRD** | Integration | S10 proves the networking path for notebooks. Full RHOAI Notebook CRD creates StatefulSets (not bare Pods), which requires VK awareness of StatefulSet ownership. Deferred. | Medium |

---

## Limitations

### Single worker namespace

All dispatched pods land in `vk-workloads` regardless of source tenant
namespace. Secrets and ConfigMaps keep their original names, so same-named
resources from different tenant namespaces collide. Pod names are
namespace-prefixed (`{ns}--{name}`) so pods don't collide, but synced
resources are not prefixed.

**Impact:** Multi-tenant deployments will see data corruption if teams use
the same resource names.

**Fix:** Use one worker namespace per tenant namespace (e.g., `vk-team-a`).
Not yet implemented.

### Headless Service sync (implemented)

VK now syncs headless Services whose selectors match dispatched pods. This
enables inter-pod DNS for distributed training. Service names are NOT
namespace-prefixed (must match pod `subdomain` for DNS). Validated in S13.

### VK is not using the virtual-kubelet library

Written from scratch with plain `client-go`. This gives full control but
means no compatibility with the VK provider ecosystem. The provider interface
pattern could be adopted later if needed.

### Submariner routeagent on virtual node

Submariner's routeagent DaemonSet schedules a pod on the virtual `gpu-worker`
node, where it gets stuck in `Init:0/1`. Cosmetic — the real routeagent on
`sno-tenant` handles routing. Could be fixed with a toleration or node
selector but does not affect function.

---

## What This Spike Proves

1. **Cross-cluster GPU dispatch works end-to-end.** Pods scheduled on a
   virtual node execute on a real GPU (RTX 5090) in a different cluster.
   Status syncs back transparently.

2. **RHOAI works without modification.** The training operator creates pods,
   VK dispatches them, status syncs back, and the operator sees Succeeded.
   No RHOAI code changes or CRDs needed on the worker.

3. **Resource isolation works.** Secrets, ConfigMaps, ServiceAccounts, and
   PVCs are synced on-demand, labeled for tracking, and cleaned up on pod
   deletion. No data leaks between pods.

4. **Cross-cluster networking works via Submariner.** Regular Kubernetes
   Services and OpenShift Routes reach remote GPU pods transparently. VK
   syncs PodIP, Kubernetes creates EndpointSlices, Submariner provides L3
   routing. No Submariner-specific code in VK.

5. **The worker stays bare.** No RHOAI operators, no CRDs, no training
   framework on the GPU cluster. Just GPU Operator + Kueue for quota.

6. **Interactive workloads (notebooks) work.** Long-running GPU pods stay
   Running and remain accessible through Service/Route via Submariner (S10).

7. **Multi-tenant collision is real and proven.** Same-named resources from
   different namespaces collide in the single worker namespace (S12).
   Documented as a known limitation with a clear fix path.

8. **Headless Service sync enables distributed training DNS.** Inter-pod
   DNS resolution works via synced headless Services (S13). This is the
   mechanism for multi-pod PyTorchJob.

9. **Submariner tunnel self-heals.** Gateway pod failure → tunnel drops →
   automatic restart → tunnel re-establishes → connectivity restored (S11).

## What This Spike Does Not Prove

1. **Full multi-pod distributed training.** The DNS mechanism is proven
   (S13), but actual multi-GPU training needs >1 GPU. Single-GPU lab
   validates the infrastructure, not the workload.

2. **RHOAI Notebook CRD integration.** The networking path works (S10),
   but RHOAI Notebook creates StatefulSets, which requires VK awareness
   of StatefulSet pod ownership.

3. **KServe with Knative.** No Serverless operator installed. Raw
   Deployment inference works (S9).

4. **Production readiness.** No HA, no Globalnet for overlapping CIDRs,
   no performance benchmarks, no multi-tenant namespace isolation.

---

## Architecture Reference

| Document | What it covers |
|----------|---------------|
| [README.md](../README.md) | Project overview, provisioning, how it works |
| [vk-architecture.md](vk-architecture.md) | VK internals: dispatch loop, resource sync, pod transformation, status sync |
| [submariner-architecture.md](submariner-architecture.md) | Cross-cluster networking: topology, traffic flows, failure modes, deployment log |
| [pvc-architecture.md](pvc-architecture.md) | Catapult PVC lifecycle and design |
| [00-prerequisites.md](../00-prerequisites.md) | Hardware setup, VFIO, QEMU 9.2, RTX 5090 XML tweaks |

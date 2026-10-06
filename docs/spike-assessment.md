# Virtual Kubelet GPU Dispatch — Spike Assessment

## Objective

Prove that a custom Virtual Kubelet can transparently dispatch GPU workloads
from tenant OpenShift clusters (running RHOAI) to a bare worker cluster with
physical GPUs. Tenants can run different OCP and RHOAI versions, sharing the
same GPU pool without interference. Cross-cluster networking via Submariner
enables Services and Routes to reach remote GPU pods.

## Lab Environment

```
Gaming PC (Intel Ultra 9 285K, 62 GB RAM, Ubuntu 24.04)
+---- sno-tenant VM ---------+  +---- sno-tenant2 VM --------+  +---- sno-worker VM ---------+
|  12 vCPU, 14 GB RAM        |  |  10 vCPU, 16 GB RAM        |  |  8 vCPU, 24 GB RAM         |
|  OCP 4.22, RHOAI           |  |  OCP 4.18, RHOAI           |  |  OCP 4.22                  |
|  VK → virtual node         |  |  VK → virtual node         |  |  GPU Operator (RTX 5090)   |
|   "gpu-worker" (1 GPU)     |  |   "gpu-worker" (1 GPU)     |  |  Kueue, LVMS               |
+-----------------------------+  +-----------------------------+  +-----------------------------+
         \                                    |                               /
          +------- Submariner v0.24 (Libreswan IPsec) --------+--------------+
```

| Component | Tenant (sno-tenant) | Tenant 2 (sno-tenant2) | Worker (sno-worker) |
|-----------|--------------------|-----------------------|--------------------|
| OpenShift | 4.22 | 4.18 | 4.22 |
| vCPU / RAM | 12 / 14 GB | 10 / 16 GB | 8 / 24 GB |
| GPU | — | — | RTX 5090 (32 GB VRAM) |
| Operators | RHOAI | RHOAI | GPU Operator, Kueue, LVMS |
| Pod CIDR | 10.128.0.0/14 | 10.136.0.0/14 | 10.132.0.0/14 |
| Service CIDR | 172.30.0.0/16 | 172.32.0.0/16 | 172.31.0.0/16 |
| Networking | Submariner v0.24 | Submariner v0.24 | Submariner v0.24 |

All clusters run as single-node OpenShift on libvirt VMs on a gaming PC
(Intel Core Ultra 9 285K, 62 GB RAM, Ubuntu 24.04).

## How to Run

All tests are integration tests that run against live clusters. Each test
creates its own resources, waits for the expected outcome, asserts, and
cleans up.

### Prerequisites

```bash
pip install kubernetes pytest
export TENANT_KUBECONFIG=~/.kube/tenant
export WORKER_KUBECONFIG=~/.kube/worker
```

### Run all scenarios

```bash
python -m pytest tests/ -v
```

### Run by category

```bash
python -m pytest tests/ -v -m vk            # Core dispatch + resource sync + storage + logs
python -m pytest tests/ -v -m rhoai         # RHOAI integration
python -m pytest tests/ -v -m networking    # Cross-cluster networking (Submariner)
```

### Run against second tenant (OCP 4.18)

```bash
TENANT_KUBECONFIG=~/.kube/tenant2 WORKER_KUBECONFIG=~/.kube/worker \
  python -m pytest tests/ -v -m vk
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

**How to run:**
```bash
python -m pytest tests/test_vk_gpu.py::test_virtual_node_exists -v
```

**What to look for:** The test reads the `gpu-worker` node object and checks:
- Label `type=virtual-kubelet` exists
- `nvidia.com/gpu` is in `status.allocatable`
- Node condition `Ready=True`

---

#### S2. GPU pod dispatch and status sync

| | |
|---|---|
| **Test** | `test_vk_gpu.py::test_gpu_pod_dispatched_via_vk` |
| **What** | GPU pod submitted on tenant with `nodeName: gpu-worker` → VK creates pod on worker → `nvidia-smi` runs on RTX 5090 → status (Phase, exit code) synced back to tenant as Succeeded |
| **Result** | PASS |
| **Proves** | End-to-end dispatch works: pod scheduled on virtual node executes on real GPU, status is visible on tenant |

**How to run:**
```bash
python -m pytest tests/test_vk_gpu.py::test_gpu_pod_dispatched_via_vk -v
```

**What to look for:** The test:
1. Creates a pod with `image: nvcr.io/nvidia/cuda:12.8.0-base-ubi9`, `command: nvidia-smi`, requesting 1 GPU
2. Waits for the worker pod (`{namespace}--{name}`) to appear in the worker namespace
3. Waits for worker pod phase to reach `Succeeded` (nvidia-smi exits 0)
4. Verifies the tenant pod also shows `Succeeded` (status synced back by VK informer)
5. Checks container exit code is 0

---

### 2. Resource Isolation

#### S3. Secret and ConfigMap sync

| | |
|---|---|
| **Test** | `test_vk_gpu.py::test_resource_sync` |
| **What** | Pod references a Secret (via `secretKeyRef`) and ConfigMap (via volume mount) on tenant → VK syncs both to worker with management labels → pod reads them successfully |
| **Result** | PASS |
| **Proves** | VK discovers resource references by walking the pod spec and syncs them on-demand. No pre-staging required. |

**How to run:**
```bash
python -m pytest tests/test_vk_gpu.py::test_resource_sync -v
```

**What to look for:** The test:
1. Creates a Secret (`MY_SECRET=hello-from-tenant`) and ConfigMap (`config.txt=value-from-tenant`) on the tenant
2. Creates a pod that reads the secret via `env[].valueFrom.secretKeyRef` and the configmap via a volume mount
3. Verifies both resources appear on the worker with label `app.kubernetes.io/managed-by: vk-gpu-provider`
4. Verifies the pod succeeds (meaning it could read both resources)

---

#### S4. Pod deletion cleanup

| | |
|---|---|
| **Test** | `test_vk_gpu.py::test_pod_deletion_cleans_up` |
| **What** | Tenant pod deleted → worker pod deleted → synced secret cleaned up (verified via 404) |
| **Result** | PASS |
| **Proves** | No resource leaks. Management labels enable targeted cleanup. |

**How to run:**
```bash
python -m pytest tests/test_vk_gpu.py::test_pod_deletion_cleans_up -v
```

**What to look for:** The test:
1. Creates a secret + pod on the tenant, waits for the worker pod to appear
2. Deletes the tenant pod
3. Verifies the worker pod is deleted (404)
4. Verifies the synced secret is also cleaned up (404)

---

### 3. Storage

#### S5. Catapult PVC sync

| | |
|---|---|
| **Test** | `test_vk_gpu.py::test_catapult_pvc_sync` |
| **What** | PVC with `storageClassName: catapult` on tenant → execution PVC created on worker with namespace-prefixed name (`{ns}--{name}`) and LVMS StorageClass → execution PVC survives pod deletion → cleaned up when control PVC deleted |
| **Result** | PASS |
| **Proves** | PVC lifecycle is independent of pod lifecycle. Data persists across pod restarts. Cleanup is automatic via PVC deletion informer. |

**How to run:**
```bash
python -m pytest tests/test_vk_gpu.py::test_catapult_pvc_sync -v
```

**What to look for:** The test:
1. Creates a PVC with `storageClassName: catapult` and a pod that mounts it
2. Verifies execution PVC (`{namespace}--{pvc_name}`) appears on worker with management labels
3. Verifies execution PVC has `ReadWriteOnce` access mode and correct source labels
4. Deletes the tenant pod — verifies execution PVC **survives** (data persists)
5. Deletes the control PVC — verifies execution PVC is cleaned up

---

#### S6. Non-catapult PVC rejection

| | |
|---|---|
| **Test** | `test_vk_gpu.py::test_non_catapult_pvc_rejected` |
| **What** | Pod referencing PVC without `storageClassName: catapult` → pod marked Failed with reason `InvalidPVCStorageClass` → no execution PVC created on worker |
| **Result** | PASS |
| **Proves** | Only explicitly opted-in PVCs are synced. Clear error message for users. |

**How to run:**
```bash
python -m pytest tests/test_vk_gpu.py::test_non_catapult_pvc_rejected -v
```

**What to look for:** The test:
1. Creates a PVC without the `catapult` StorageClass and a pod that mounts it
2. Verifies the tenant pod reaches `Failed` with reason `InvalidPVCStorageClass`
3. Verifies no execution PVC exists on the worker (404)

---

### 4. RHOAI Integration

#### S7. PyTorchJob training dispatch

| | |
|---|---|
| **Test** | `test_rhoai_vk.py::test_pytorchjob_via_vk` |
| **What** | PyTorchJob CR on tenant → RHOAI training operator creates master pod → VK dispatches to worker GPU → `nvidia-smi` succeeds → status synced → PyTorchJob condition reaches Succeeded |
| **Result** | PASS |
| **Proves** | RHOAI training operator works transparently. It creates pods, VK dispatches them, status syncs back, and the operator sees Succeeded. No RHOAI modifications needed. |

**How to run:**
```bash
python -m pytest tests/test_rhoai_vk.py::test_pytorchjob_via_vk -v
```

**What to look for:** The test:
1. Creates a PyTorchJob CR with 1 master replica targeting the virtual node
2. Waits for the training operator to create the master pod (`{job_name}-master-0`)
3. Waits for the worker pod to appear and succeed (nvidia-smi on RTX 5090)
4. Verifies the tenant pod shows `Succeeded`
5. Verifies the PyTorchJob CR has condition `Succeeded=True`

---

#### S8. Worker stays bare

| | |
|---|---|
| **Test** | `test_rhoai_vk.py::test_no_rhoai_crds_on_worker` |
| **What** | Worker cluster has no RHOAI CRDs (pytorchjobs, notebooks, inferenceservices, rayclusters) |
| **Result** | PASS |
| **Proves** | Worker is a bare GPU execution node. No operator stack duplication. RHOAI runs only on tenant. |

**How to run:**
```bash
python -m pytest tests/test_rhoai_vk.py::test_no_rhoai_crds_on_worker -v
```

**What to look for:** The test lists all CRDs on the worker and asserts that none of the RHOAI CRDs
(`pytorchjobs.kubeflow.org`, `notebooks.kubeflow.org`, `inferenceservices.serving.kserve.io`,
`rayclusters.ray.io`) are present.

---

### 5. Cross-Cluster Networking

#### S9. GPU inference service via Submariner

| | |
|---|---|
| **Test** | `test_submariner_networking.py::test_gpu_service_via_submariner` |
| **What** | GPU HTTP server pod dispatched to worker via VK → Service created on tenant → PodIP synced (worker IP appears on tenant pod) → EndpointSlice created with worker PodIP → curl the Service from a tenant pod → HTTP 200 with GPU info from RTX 5090 |
| **Result** | PASS |
| **Proves** | Full networking chain: VK syncs PodIP → Kubernetes creates EndpointSlice → Service ClusterIP routes through Submariner IPsec tunnel → reaches GPU pod on worker. No Submariner-specific code in VK, pod, or Service. |

**How to run:**
```bash
python -m pytest tests/test_submariner_networking.py::test_gpu_service_via_submariner -v
```

**What to look for:** The test:
1. Deploys a Python HTTP server pod on the virtual node (runs nvidia-smi, returns JSON)
2. Creates a Service selecting the pod
3. Waits for the tenant pod to show `Running` with a PodIP from the worker CIDR (10.132-135.x.x)
4. Verifies an EndpointSlice exists with that PodIP and `ready=true`
5. Runs a curl pod on the tenant that hits the Service ClusterIP
6. Asserts HTTP response contains `"status": "ok"` and `RTX 5090`

**Traffic flow:**
```
curl pod (tenant)
  → Service ClusterIP (tenant)
  → EndpointSlice (10.132.x.x, worker pod IP)
  → Submariner route agent → IPsec tunnel (UDP 4500)
  → worker gateway → OVN
  → GPU pod (nvidia-smi)
  → {"gpu": "NVIDIA GeForce RTX 5090, 32607, 0, 40", "status": "ok"}
```

---

#### S10. Notebook workbench via Submariner

| | |
|---|---|
| **Test** | `test_submariner_networking.py::test_notebook_workbench_via_submariner` |
| **What** | Long-running GPU HTTP server (simulating a Jupyter notebook) dispatched to worker via VK → Service on tenant → pod stays Running (not Succeeded) → curl returns HTML with GPU info through Submariner tunnel |
| **Result** | PASS |
| **Proves** | Persistent interactive workloads (notebooks, IDEs) work through Submariner. The VK keeps long-running pods in Running state and the networking path remains stable. Combined with Route access from S9, this validates the full notebook access chain. |

**How to run:**
```bash
python -m pytest tests/test_submariner_networking.py::test_notebook_workbench_via_submariner -v
```

**What to look for:** The test:
1. Deploys a notebook-like HTTP server (port 8888, returns HTML with GPU info)
2. Creates a Service, waits for pod Running with PodIP
3. Sleeps 10s and verifies pod is **still Running** (not completed — interactive workload)
4. Curls the Service, asserts response contains `GPU Notebook` and `RTX 5090`

---

#### S11. Submariner tunnel failure and recovery

| | |
|---|---|
| **Test** | `test_submariner_networking.py::test_submariner_tunnel_failure_recovery` |
| **What** | Deploy GPU HTTP server + Service → verify connectivity works → kill Submariner gateway pod on worker → verify tunnel drops → wait for gateway restart and tunnel re-establishment → verify connectivity restored |
| **Result** | PASS |
| **Proves** | Submariner tunnel self-heals after gateway pod failure. IPsec tunnel re-establishes automatically via Deployment restart. Workloads remain running during outage; only network path is disrupted. |

**How to run:**
```bash
python -m pytest tests/test_submariner_networking.py::test_submariner_tunnel_failure_recovery -v
```

**What to look for:** The test:
1. Deploys GPU HTTP server + Service, verifies connectivity (pre-disruption curl succeeds)
2. Kills the Submariner gateway pod on the worker (`grace_period=0`)
3. Waits 10s, attempts curl (may fail — records result)
4. Waits up to 180s for new gateway pod to reach `Ready=True`
5. Waits 30s for IPsec tunnel to re-establish
6. Verifies connectivity restored (post-recovery curl succeeds with `"status": "ok"`)

---

### 6. Multi-Tenant Isolation

#### S12. Multi-tenant namespace isolation

| | |
|---|---|
| **Test** | `test_vk_gpu.py::test_multitenant_namespace_isolation` |
| **What** | Two tenant namespaces (`vk-team-a`, `vk-team-b`) each create a Secret named `shared-config` with different data → both dispatch pods via VK → each synced to its own per-tenant worker namespace (`{prefix}vk-team-a`, `{prefix}vk-team-b`) → both secrets exist with correct data |
| **Result** | PASS |
| **Proves** | Per-tenant worker namespaces prevent resource collisions. Same-named resources from different tenant namespaces are fully isolated. |

**How to run:**
```bash
python -m pytest tests/test_vk_gpu.py::test_multitenant_namespace_isolation -v
```

**What to look for:** The test:
1. Creates two namespaces (`vk-team-a`, `vk-team-b`) on the tenant
2. Creates a Secret named `shared-config` in each — `team=alpha` in A, `team=bravo` in B
3. Dispatches pods from both namespaces referencing their respective secret
4. Verifies both pods land on the worker in separate namespaces (`{prefix}vk-team-a`, `{prefix}vk-team-b`)
5. Reads both secrets from the worker — asserts alpha in A's namespace, bravo in B's

---

### 7. Distributed Training

#### S13. Headless Service sync for inter-pod DNS

| | |
|---|---|
| **Test** | `test_distributed_training.py::test_headless_service_dns_resolution` |
| **What** | Two pods with hostname/subdomain deployed via VK + headless Service created on tenant → VK syncs headless Service to worker namespace → Pod B resolves Pod A's DNS name via synced Service → DNS lookup returns an IP |
| **Result** | PASS |
| **Proves** | Headless Service sync enables inter-pod DNS resolution on the worker cluster. This is the mechanism that makes multi-pod PyTorchJob work: training operator creates headless Services, VK syncs them, worker CoreDNS resolves pod hostnames. Combined with S7 (single-pod PyTorchJob proven), multi-pod distributed training infrastructure is in place. |

**How to run:**
```bash
python -m pytest tests/test_distributed_training.py::test_headless_service_dns_resolution -v
```

**What to look for:** The test:
1. Creates a headless Service (`clusterIP: None`) on the tenant with selector `app: dns-test`
2. Creates Pod A with `hostname: pod-a` and `subdomain: {svc_name}`, waits for Running
3. Verifies the headless Service was synced to the worker namespace (checks `clusterIP == "None"`)
4. Creates Pod B that runs `getent hosts pod-a.{svc_name}.{worker_namespace}.svc.cluster.local`
5. Pod B succeeds → DNS resolution works; logs contain an IP address

---

### 8. Observability

#### S14. Pod log proxying from worker

| | |
|---|---|
| **Test** | `test_vk_gpu.py::test_pod_logs_proxied_from_worker` |
| **What** | Pod on virtual node echoes a known marker → `kubectl logs` / `oc logs` on tenant API server proxied through VK kubelet API to worker pod → full output and `tail_lines=1` both work correctly |
| **Result** | PASS |
| **Proves** | `kubectl logs` and `oc logs` work transparently against virtual node pods. VK runs a kubelet API server (port 10350) using the node's TLS cert, proxying log requests to the actual worker pod. |

**How to run:**
```bash
# Automated test
python -m pytest tests/test_vk_gpu.py::test_pod_logs_proxied_from_worker -v

# Manual verification with oc/kubectl
oc --kubeconfig=~/.kube/tenant logs <pod-name> -n vk-test -c <container>
oc --kubeconfig=~/.kube/tenant logs <pod-name> -n vk-test -c <container> --tail=1
oc --kubeconfig=~/.kube/tenant logs <pod-name> -n vk-test -c <container> --follow
```

**What to look for:** The test:
1. Creates a pod that echoes `catapult-log-marker-42`, `line2`, `line3`
2. Waits for the pod to succeed
3. Reads full logs via the tenant API — asserts marker and `line3` are present
4. Reads logs with `tail_lines=1` — asserts only `line3` is present, `line2` is not
5. Reads logs directly from the worker pod — asserts marker is present (same content)

**Implementation details:** The VK kubelet API server requires:
- `hostNetwork: true` on the VK Deployment (so the API server binds on the node IP matching the kubelet cert SANs)
- Kubelet serving cert mounted from `/var/lib/kubelet/pki/kubelet-server-current.pem`
- Port 10350 (avoids conflict with real kubelet on 10250)
- Privileged SCC for the VK service account (hostNetwork + hostPath on OpenShift)

---

### 9. Version Decoupling

#### S15. Multi-tenant OCP version decoupling

| | |
|---|---|
| **Test** | All VK tests (`-m vk`) run against both `~/.kube/tenant` (OCP 4.22) and `~/.kube/tenant2` (OCP 4.18) |
| **What** | Two tenant clusters at different OCP versions (4.22 and 4.18), each with their own RHOAI installation, both dispatch GPU workloads to the same worker cluster through independent VK instances. All VK tests pass from both tenants. |
| **Result** | PASS (8/8 VK tests from tenant, 8/8 from tenant2) |
| **Proves** | The AI platform layer (RHOAI) is fully decoupled from the GPU compute layer. Tenants can run different OCP and RHOAI versions independently. The worker cluster is version-agnostic — it runs pods, not operators. |

**How to run:**
```bash
# Run VK tests from tenant (OCP 4.22)
TENANT_KUBECONFIG=~/.kube/tenant WORKER_KUBECONFIG=~/.kube/worker \
  python -m pytest tests/test_vk_gpu.py -v

# Run VK tests from tenant2 (OCP 4.18)
TENANT_KUBECONFIG=~/.kube/tenant2 WORKER_KUBECONFIG=~/.kube/worker \
  python -m pytest tests/test_vk_gpu.py -v

# Verify different RHOAI versions
oc --kubeconfig=~/.kube/tenant get csv -n redhat-ods-operator | grep rhods
oc --kubeconfig=~/.kube/tenant2 get csv -n redhat-ods-operator | grep rhods
```

**What to look for:**
- All 8 VK tests pass from both tenants
- Each VK instance auto-generates a unique worker namespace prefix (stored in ConfigMap `vk-gpu-provider-config` in `kube-system`)
- Worker namespaces from different tenants never collide
- The worker cluster has no RHOAI CRDs — it only runs GPU Operator + Kueue

---

## Test Results Summary

### From tenant (OCP 4.22)

```
tests/test_vk_gpu.py::test_virtual_node_exists                                PASSED
tests/test_vk_gpu.py::test_gpu_pod_dispatched_via_vk                          PASSED
tests/test_vk_gpu.py::test_resource_sync                                      PASSED
tests/test_vk_gpu.py::test_pod_deletion_cleans_up                             PASSED
tests/test_vk_gpu.py::test_catapult_pvc_sync                                  PASSED
tests/test_vk_gpu.py::test_non_catapult_pvc_rejected                          PASSED
tests/test_vk_gpu.py::test_multitenant_namespace_isolation                    PASSED
tests/test_vk_gpu.py::test_pod_logs_proxied_from_worker                       PASSED
tests/test_rhoai_vk.py::test_pytorchjob_via_vk                               PASSED
tests/test_rhoai_vk.py::test_no_rhoai_crds_on_worker                         PASSED
tests/test_submariner_networking.py::test_gpu_service_via_submariner          PASSED
tests/test_submariner_networking.py::test_notebook_workbench_via_submariner   PASSED
tests/test_submariner_networking.py::test_submariner_tunnel_failure_recovery  PASSED
tests/test_distributed_training.py::test_headless_service_dns_resolution     PASSED

14 passed
```

### From tenant2 (OCP 4.18)

```
tests/test_vk_gpu.py::test_virtual_node_exists                                PASSED
tests/test_vk_gpu.py::test_gpu_pod_dispatched_via_vk                          PASSED
tests/test_vk_gpu.py::test_resource_sync                                      PASSED
tests/test_vk_gpu.py::test_pod_deletion_cleans_up                             PASSED
tests/test_vk_gpu.py::test_catapult_pvc_sync                                  PASSED
tests/test_vk_gpu.py::test_non_catapult_pvc_rejected                          PASSED
tests/test_vk_gpu.py::test_multitenant_namespace_isolation                    PASSED
tests/test_vk_gpu.py::test_pod_logs_proxied_from_worker                       PASSED

8 passed
```

---

## Scenarios Not Yet Assessed

| Scenario | Category | Why not assessed | Priority |
|----------|----------|-----------------|----------|
| **KServe inference (Knative-managed)** | Networking | Knative autoscaler, activator, queue-proxy, scale-to-zero need investigation. No Serverless operator installed in lab. Raw Deployment inference works (S9 proves the path). | Medium |
| **Multi-pod PyTorchJob (full distributed)** | Training | Headless Service sync is proven (S13), but actual multi-GPU training needs >1 GPU. Lab has 1 GPU. Mechanism is validated; full end-to-end deferred to multi-GPU environment. | Medium |
| **Overlapping CIDRs (Globalnet)** | Networking | Lab uses non-overlapping CIDRs by design. Overlapping CIDRs require Globalnet which changes the PodIP sync model. Not PoC scope. | Low |
| **RHOAI dashboard → Notebook CRD** | Integration | S10 proves the networking path for notebooks. Full RHOAI Notebook CRD creates StatefulSets (not bare Pods), which requires VK awareness of StatefulSet ownership. Deferred. | Medium |
| **Kueue admission control** | Quota | Worker has Kueue installed but VK does not submit AdmissionChecks or interact with LocalQueue. GPU quota enforcement across tenants not validated. | High |

---

## Limitations

### SecurityContext pass-through

SecurityContext is passed through unchanged. OpenShift SCC mutates
SecurityContext fields before VK sees the pod, and we cannot distinguish
user-set fields from SCC-injected ones (see
[security-context-handling.md](security-context-handling.md)). If an
SCC-generated value (e.g., tenant MCS label) is incompatible with the worker
cluster's security policy, the pod fails visibly on the worker.

### Single GPU quota

The lab has 1 GPU. When both tenants try to dispatch a GPU pod simultaneously,
only one runs — the other waits. Kueue on the worker could manage fair-sharing,
but VK does not interact with Kueue's admission system yet.

### Submariner routeagent on virtual node

Submariner's routeagent DaemonSet schedules a pod on the virtual `gpu-worker`
node. The VK then dispatches it to the worker cluster, where it gets stuck
in `Init:0/1` permanently, retrying a forbidden API call every second.

**What the routeagent does:** The routeagent configures iptables rules and
IP routes on every node for cross-cluster traffic. It runs as a DaemonSet
so every node in the cluster gets the right routing tables. The init
container (`await-node-ready`) waits until the local node is Ready before
the main container starts configuring routes.

**The full failure chain:**

1. The routeagent DaemonSet uses `tolerations: [{operator: Exists}]` and
   no `nodeSelector`, so the DaemonSet controller schedules a pod on the
   virtual node `gpu-worker`.

2. VK's `CreatePod` picks up the pod. The `submariner-operator` namespace
   is not in VK's system namespace skip list (`openshift-*`, `kube-*`,
   `redhat-ods-*`, `default`, `kueue-system`), so VK treats it as a
   regular user pod.

3. VK syncs the `submariner-routeagent` ServiceAccount to the worker
   namespace (`vk-{prefix}submariner-operator`) and creates the pod there.

4. The init container starts on the worker and runs `await-node-ready`,
   which tries to read the local node object (`sno-worker`) via the
   Kubernetes API.

5. The synced ServiceAccount has no ClusterRoleBinding on the worker
   cluster — VK only syncs the SA object, not its RBAC bindings. The
   request fails with:
   ```
   nodes "sno-worker" is forbidden: User
   "system:serviceaccount:vk-{prefix}submariner-operator:submariner-routeagent"
   cannot get resource "nodes" in API group "" at the cluster scope
   ```

6. The init container retries every ~1 second, indefinitely. The pod
   stays in `Init:0/1` on the tenant side and the init container shows
   `Running` (not crashed — it's in a retry loop).

**Impact:** The real routeagent on `sno-tenant` handles all Submariner
routing — cross-cluster networking works fully (validated by S9, S10, S11,
S13). However, the dispatched pod is not truly cosmetic:

- It consumes a pod slot on the worker (though minimal CPU/memory)
- It generates ~1 forbidden API request per second against the worker
  API server
- It appears as an unhealthy pod in both tenant and worker monitoring

**Why it can't be fixed via Submariner configuration:** We investigated
three approaches, all of which fail:

1. **Patch the DaemonSet directly** (add nodeAffinity to exclude
   `type=virtual-kubelet`): The Submariner operator reconciles the
   DaemonSet and reverts the patch within seconds.

2. **Set `nodeSelector` on the Submariner CR**: The CR's `nodeSelector`
   field only applies to the gateway DaemonSet, not the routeagent.

3. **Set `tolerations` on the Submariner CR**: Same — the CR's
   `tolerations` field only applies to the gateway DaemonSet. The
   routeagent always gets the hardcoded `operator: Exists` toleration.

**Applied fix:** VK now skips any pod with a DaemonSet `ownerReference`
in `CreatePod`. This is a general fix — not just for Submariner — since
DaemonSet pods are node-level infrastructure that should never be
dispatched cross-cluster. The routeagent pod still shows `Pending` on
the virtual node (DaemonSet controller keeps creating it, but the virtual
node has no real kubelet to run it), but it is no longer dispatched to
the worker and generates no API traffic.

**Remaining cosmetic issue:** The routeagent pod on the virtual node
shows as `Pending` indefinitely. This is stable and has no functional
impact: no container runs, no CPU/memory is consumed, no API calls are
generated. Submariner routing works fully — the real routeagent on the
tenant SNO node handles all iptables rules and cross-cluster routes (all
networking tests pass: S9, S10, S11, S13). The only consequence is a
visible `Pending` pod in `oc get pods -n submariner-operator` that may
trigger monitoring alerts if pod-health checks are configured.

This cannot be fixed without an upstream Submariner change (e.g., a
`routeAgentNodeSelector` field in the CR) or a mutating admission webhook
that injects a nodeAffinity anti-rule for `type=virtual-kubelet`.

### Kubelet API server requires privileged SCC

The VK Deployment needs `hostNetwork: true` and a `hostPath` volume for the
kubelet TLS cert. On OpenShift, this requires granting the `privileged` SCC
to the VK service account.

---

## What This Spike Proves

1. **Cross-cluster GPU dispatch works end-to-end.** Pods scheduled on a
   virtual node execute on a real GPU (RTX 5090) in a different cluster.
   Status syncs back transparently (S2).

2. **RHOAI works without modification.** The training operator creates pods,
   VK dispatches them, status syncs back, and the operator sees Succeeded.
   No RHOAI code changes or CRDs needed on the worker (S7, S8).

3. **Resource isolation works.** Secrets, ConfigMaps, ServiceAccounts, and
   PVCs are synced on-demand to per-tenant worker namespaces, labeled for
   tracking, and cleaned up on pod deletion. No data leaks or name collisions
   between tenants (S3, S4, S12).

4. **Cross-cluster networking works via Submariner.** Regular Kubernetes
   Services and OpenShift Routes reach remote GPU pods transparently. VK
   syncs PodIP, Kubernetes creates EndpointSlices, Submariner provides L3
   routing. No Submariner-specific code in VK (S9).

5. **The worker stays bare.** No RHOAI operators, no CRDs, no training
   framework on the GPU cluster. Just GPU Operator + Kueue for quota (S8).

6. **Interactive workloads (notebooks) work.** Long-running GPU pods stay
   Running and remain accessible through Service/Route via Submariner (S10).

7. **Multi-tenant isolation works.** Per-tenant worker namespaces prevent
   same-named resources from different tenant namespaces from colliding (S12).

8. **Headless Service sync enables distributed training DNS.** Inter-pod
   DNS resolution works via synced headless Services (S13). This is the
   mechanism for multi-pod PyTorchJob.

9. **Submariner tunnel self-heals.** Gateway pod failure → tunnel drops →
   automatic restart → tunnel re-establishes → connectivity restored (S11).

10. **kubectl logs / oc logs work transparently.** VK runs a kubelet API
    server using the node's TLS cert, proxying log requests to worker
    pods. Supports full logs, tail, and follow (S14).

11. **OCP/RHOAI version decoupling is proven.** Two tenants at different
    OCP versions (4.22 and 4.18) with independent RHOAI installations
    both dispatch GPU workloads to the same worker cluster. All tests
    pass from both tenants. The AI platform layer is fully decoupled
    from the GPU compute layer (S15).

## What This Spike Does Not Prove

1. **Full multi-pod distributed training.** The DNS mechanism is proven
   (S13), but actual multi-GPU training needs >1 GPU. Single-GPU lab
   validates the infrastructure, not the workload.

2. **RHOAI Notebook CRD integration.** The networking path works (S10),
   but RHOAI Notebook creates StatefulSets, which requires VK awareness
   of StatefulSet pod ownership.

3. **KServe with Knative.** No Serverless operator installed. Raw
   Deployment inference works (S9).

4. **Kueue quota enforcement.** Worker has Kueue but VK does not interact
   with it. Fair-sharing between tenants is not validated.

5. **Production readiness.** No HA, no Globalnet for overlapping CIDRs,
   no performance benchmarks, no worker namespace garbage collection.

---

## Architecture Reference

| Document | What it covers |
|----------|---------------|
| [README.md](../README.md) | Project overview, provisioning, how it works |
| [vk-architecture.md](vk-architecture.md) | VK internals: dispatch loop, resource sync, pod transformation, status sync |
| [submariner-architecture.md](submariner-architecture.md) | Cross-cluster networking: topology, traffic flows, failure modes, deployment log |
| [pvc-architecture.md](pvc-architecture.md) | Catapult PVC lifecycle and design |
| [security-context-handling.md](security-context-handling.md) | SecurityContext field classification and SCC ambiguity |
| [00-prerequisites.md](../00-prerequisites.md) | Hardware setup, VFIO, QEMU 9.2, RTX 5090 XML tweaks |

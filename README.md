# crc-lab-gpuaas (Virtual Kubelet spike)

Local lab for GPUaaS spike testing. Two SNO OpenShift 4.22 clusters on a gaming
PC. A custom Virtual Kubelet on the tenant presents a virtual node with GPU
capacity. When pods are scheduled to the virtual node, VK syncs referenced
resources (secrets, configmaps, service accounts) to the worker and creates the
pod there. Kueue on the worker manages GPU quota.

```
Gaming PC (Intel Ultra 9 285K, 62 GB RAM, Ubuntu 24.04)
+----- sno-tenant VM --------+    +----- sno-worker VM --------+
|  12 vCPU, 16 GB RAM        |    |  8 vCPU, 24 GB RAM         |
|                            |    |  GPU Operator (RTX 5090)   |
|  VK Deployment             |    |  Kueue (local quota mgmt)  |
|    registers virtual node  |    |  LVMS (local storage)      |
|    "gpu-worker" (1 GPU)    |    |                             |
|                            |    |  vk-workloads namespace    |
|  scheduler ──> virtual node +-->|    synced secrets/cms/sas   |
|                            |    |    real pods running here   |
|  status synced back <───── ─+<--|                             |
+-----------------------------+    +-----------------------------+
```

## How It Works

1. VK registers a virtual node `gpu-worker` on the tenant with `nvidia.com/gpu: 1`
   in allocatable resources.
2. Users submit pods with `nodeName: gpu-worker` and a toleration for the VK
   taints (NoSchedule + NoExecute):
   ```yaml
   tolerations:
   - key: virtual-kubelet.io/provider
     operator: Exists
   ```
3. VK watches for pods assigned to its node, then:
   - Walks the pod spec to discover referenced secrets, configmaps, and service accounts
   - Syncs those resources to the `vk-workloads` namespace on the worker
   - Transforms the pod spec (strips OpenShift SELinux/SCC mutations, adds Kueue
     queue-name label, clears scheduling fields)
   - Creates the pod on the worker
4. VK watches worker pods and syncs status back to the tenant pod.
5. When a tenant pod is deleted, VK deletes the worker pod and cleans up synced
   resources (using management labels).

## Prerequisites

See [00-prerequisites.md](00-prerequisites.md) for hardware-specific setup
(VFIO, IOMMU, QEMU 9.2 build, libvirt memlock, RTX 5090 XML tweaks).

## Provisioning

```bash
ansible-playbook -i inventory.yml playbooks/01-create-vms.yml
ansible-playbook -i inventory.yml playbooks/02-wait-and-discover.yml
ansible-playbook -i inventory.yml playbooks/03-configure-clusters.yml
ansible-playbook -i inventory.yml playbooks/04-setup-virtual-kubelet.yml
```

After provisioning, kubeconfigs are at `~/.kube/tenant` and `~/.kube/worker`.

## Playbooks

| Playbook | What it does |
|----------|-------------|
| `01-create-vms.yml` | Creates libvirt VMs, generates install-config, starts SNO install |
| `02-wait-and-discover.yml` | Waits for install to complete, discovers API endpoints |
| `03-configure-clusters.yml` | Installs GPU Operator, Kueue, LVMS on worker; RHOAI on tenant |
| `04-setup-virtual-kubelet.yml` | Builds VK image, sets up worker namespace/RBAC/Kueue queues, deploys VK on tenant |
| `teardown.yml` | Destroys VMs and cleans up |

## Resource Sync

VK discovers resource references by walking the pod spec:

| Reference type | Fields scanned |
|---------------|---------------|
| Secrets | `env[].valueFrom.secretKeyRef`, `envFrom[].secretRef`, `volumes[].secret`, `imagePullSecrets` |
| ConfigMaps | `env[].valueFrom.configMapKeyRef`, `envFrom[].configMapRef`, `volumes[].configMap` |
| ServiceAccounts | `serviceAccountName` (plus the SA's image pull secrets) |

Synced resources get management labels (`app.kubernetes.io/managed-by: vk-gpu-provider`,
`vk.gpuaas.io/source-pod`, `vk.gpuaas.io/source-namespace`) for cleanup tracking.

### PVC handling (Catapult storage)

PVCs are handled separately from other resources. Only PVCs with
`storageClassName: catapult` are synced. Non-catapult PVCs cause the pod
to fail with a clear error. See [docs/pvc-architecture.md](docs/pvc-architecture.md)
for the full design.

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: checkpoint
  annotations:
    catapult.redhat.com/remote-storage-class: lvms-vg1  # optional override
spec:
  storageClassName: catapult
  accessModes: [ReadWriteOnce]
  resources:
    requests:
      storage: 10Gi
```

The control PVC stays Pending (no local provisioner). Catapult creates a
namespace-prefixed execution PVC (`{namespace}--{name}`) on the GPU
cluster using the remote StorageClass. Execution PVCs persist across pod
restarts and are deleted when the control PVC is deleted.

## VK Image

The VK provider is a Go binary built from `cmd/vk-gpu-provider/`. It uses
plain `k8s.io/client-go` (no virtual-kubelet library dependency).

```bash
podman build -t quay.io/jmorenas/gpuaas-virtual-kubelet:latest -f cmd/vk-gpu-provider/Dockerfile .
podman push quay.io/jmorenas/gpuaas-virtual-kubelet:latest
```

## Storage

The worker runs LVMS for local storage (`lvms-vg1` StorageClass). The tenant
does not need local storage.

## RHOAI Integration

RHOAI (Red Hat OpenShift AI) is deployed on the tenant cluster only. The worker
cluster stays bare — just GPU Operator + Kueue. Training workloads (PyTorchJob)
submitted on the tenant are dispatched to the worker GPU via VK transparently.

The VK preserves RHOAI training operator labels (`training.kubeflow.org/*`) so
the operator can track pod status. Worker pod names are namespace-prefixed
(`{namespace}--{name}`) to avoid collisions across tenant namespaces.

### Known Gaps

| Workload | Gap | Impact |
|----------|-----|--------|
| Notebooks | Pod on worker, route on tenant points nowhere | No browser access |
| KServe | Inference endpoint unreachable from tenant | No model serving |
| Distributed training | Headless services on tenant, pods on worker | DNS fails for multi-node |

These require cross-cluster networking (Submariner or similar) and are out of
scope for this spike.

## Tests

Integration tests run against live clusters and validate the full dispatch
lifecycle: pod scheduling, resource syncing, GPU execution, status sync,
and cleanup.

```bash
# Run all tests
TENANT_KUBECONFIG=~/.kube/tenant WORKER_KUBECONFIG=~/.kube/worker \
  python -m pytest tests/ -v

# Run only VK core tests
python -m pytest tests/ -v -m vk

# Run only RHOAI tests
python -m pytest tests/ -v -m rhoai
```

### Test Results (2026-10-02)

All 8 tests pass. Runtime: ~40 seconds.

```
tests/test_rhoai_vk.py::test_pytorchjob_via_vk            PASSED
tests/test_rhoai_vk.py::test_no_rhoai_crds_on_worker      PASSED
tests/test_vk_gpu.py::test_virtual_node_exists             PASSED
tests/test_vk_gpu.py::test_gpu_pod_dispatched_via_vk       PASSED
tests/test_vk_gpu.py::test_resource_sync                   PASSED
tests/test_vk_gpu.py::test_pod_deletion_cleans_up          PASSED
tests/test_vk_gpu.py::test_catapult_pvc_sync               PASSED
tests/test_vk_gpu.py::test_non_catapult_pvc_rejected       PASSED
```

### Test Descriptions

| Test | Marker | What it validates |
|------|--------|-------------------|
| `test_virtual_node_exists` | vk | Virtual node `gpu-worker` is registered with `nvidia.com/gpu` in allocatable, Ready condition |
| `test_gpu_pod_dispatched_via_vk` | vk | GPU pod submitted on tenant → dispatched to worker → `nvidia-smi` runs on RTX 5090 → status synced back as Succeeded |
| `test_resource_sync` | vk | Pod referencing a Secret + ConfigMap on tenant → both synced to worker namespace with management labels → pod reads them successfully |
| `test_pod_deletion_cleans_up` | vk | Tenant pod deleted → worker pod and synced secret cleaned up automatically |
| `test_catapult_pvc_sync` | vk | Catapult PVC on tenant → execution PVC created on worker with namespace prefix → survives pod deletion → cleaned up when control PVC deleted |
| `test_non_catapult_pvc_rejected` | vk | Pod referencing non-catapult PVC → rejected with `InvalidPVCStorageClass` → no execution PVC created |
| `test_pytorchjob_via_vk` | rhoai | PyTorchJob CR on tenant → training operator creates master pod → VK dispatches to worker GPU → nvidia-smi succeeds → status synced → PyTorchJob condition Succeeded |
| `test_no_rhoai_crds_on_worker` | rhoai | Worker cluster has no RHOAI CRDs (pytorchjobs, notebooks, inferenceservices, rayclusters) — confirms it stays a bare GPU node |

### What the Tests Prove

1. **Cross-cluster GPU dispatch works end-to-end**: pods scheduled on a virtual
   node execute on a real GPU (RTX 5090) in a different cluster.
2. **Resource isolation**: secrets, configmaps, PVCs, and service accounts are
   synced on-demand and cleaned up on pod deletion. No data leaks between tenants.
3. **Operator compatibility**: RHOAI training operator (PyTorchJob) works
   transparently — it creates pods, VK dispatches them, status syncs back, and
   the operator sees the job as Succeeded. No RHOAI modifications needed.
4. **Worker stays bare**: no RHOAI CRDs or operator workloads on the GPU
   cluster. It only runs GPU Operator + Kueue for quota management.

## Teardown

```bash
ansible-playbook -i inventory.yml playbooks/teardown.yml
```

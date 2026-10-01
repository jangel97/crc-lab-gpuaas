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
|                              |    |  GPU Operator (RTX 5090)   |
|  VK Deployment              |    |  Kueue (local quota mgmt)  |
|    registers virtual node   |    |  LVMS (local storage)      |
|    "gpu-worker" (1 GPU)     |    |                             |
|                              |    |  vk-workloads namespace    |
|  scheduler ──> virtual node +--->|    synced secrets/cms/sas   |
|                              |    |    real pods running here   |
|  status synced back <────────+<--|                             |
+-----------------------------+    +-----------------------------+
```

## How It Works

1. VK registers a virtual node `gpu-worker` on the tenant with `nvidia.com/gpu: 1`
   in allocatable resources.
2. Users submit pods with `nodeName: gpu-worker` (and a toleration for the VK taint).
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
| `03-configure-clusters.yml` | Installs operators (GPU Operator, Kueue, LVMS) on worker |
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

## VK Image

The VK provider is a Go binary built from `cmd/vk-gpu-provider/`. It uses
plain `k8s.io/client-go` (no virtual-kubelet library dependency).

```bash
podman build -t quay.io/jmorenas/vk-gpu-provider:latest -f cmd/vk-gpu-provider/Dockerfile .
podman push quay.io/jmorenas/vk-gpu-provider:latest
```

## Storage

The worker runs LVMS for local storage (`lvms-vg1` StorageClass). The tenant
does not need local storage.

## Tests

Integration tests validate that GPU pods get dispatched from tenant to worker
via the Virtual Kubelet and that resource syncing works.

```bash
cd crc-lab-gpuaas
pip install -e .
TENANT_KUBECONFIG=~/.kube/tenant WORKER_KUBECONFIG=~/.kube/worker \
  python -m pytest tests/ -v -s -m vk
```

## Teardown

```bash
ansible-playbook -i inventory.yml playbooks/teardown.yml
```

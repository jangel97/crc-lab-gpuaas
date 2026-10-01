# crc-lab-gpuaas (MultiKueue spike)

Local lab for GPUaaS spike testing. Two SNO OpenShift 4.22 clusters on a gaming
PC, connected via MultiKueue. The tenant acts as the MultiKueue manager and
submits GPU workloads. The worker has an RTX 5090 and runs them.

```
Gaming PC (Intel Ultra 9 285K, 62 GB RAM, Ubuntu 24.04)
+----- sno-tenant VM --------+    +----- sno-worker VM --------+
|  12 vCPU, 16 GB RAM        |    |  8 vCPU, 24 GB RAM         |
|  Kueue (MultiKueue mgr)    |    |  GPU Operator (RTX 5090)   |
|    AdmissionCheck           |    |  Kueue                     |
|    MultiKueueConfig         |    |  LVMS (local storage)      |
|    MultiKueueCluster -------+--->|  ResourceFlavor + CQ + LQ  |
|    ClusterQueue + AC ref    |    |                             |
|    LocalQueue               |    |                             |
+-----------------------------+    +-----------------------------+
```

Users submit pods on the tenant with a `kueue.x-k8s.io/queue-name` label.
Kueue's MultiKueue controller dispatches the workload to the worker cluster,
where the worker's local Kueue admits it and creates the pod. Status syncs
back to the tenant. No virtual nodes, no ShadowPods, no Liqo.

## Prerequisites

See [00-prerequisites.md](00-prerequisites.md) for hardware-specific setup
(VFIO, IOMMU, QEMU 9.2 build, libvirt memlock, RTX 5090 XML tweaks).

## Provisioning

```bash
ansible-playbook -i inventory.yml playbooks/01-create-vms.yml
ansible-playbook -i inventory.yml playbooks/02-wait-and-discover.yml
ansible-playbook -i inventory.yml playbooks/03-configure-clusters.yml
ansible-playbook -i inventory.yml playbooks/04-setup-multikueue.yml
```

After provisioning, kubeconfigs are at `~/.kube/tenant` and `~/.kube/worker`.

## Playbooks

| Playbook | What it does |
|----------|-------------|
| `01-create-vms.yml` | Creates libvirt VMs, generates install-config, starts SNO install |
| `02-wait-and-discover.yml` | Waits for install to complete, discovers API endpoints |
| `03-configure-clusters.yml` | Installs operators (GPU Operator, Kueue, LVMS) |
| `04-setup-multikueue.yml` | Configures Kueue queues, creates MultiKueue SA on worker, sets up MultiKueue federation on tenant |
| `teardown.yml` | Destroys VMs and cleans up |

## MultiKueue Resource Model

**Both clusters** get a ResourceFlavor (`gpu-flavor`), ClusterQueue (`cluster-queue`),
and LocalQueue (`user-queue` in the test namespace).

**Worker only:** A `multikueue-sa` ServiceAccount with ClusterRole binding gives the
tenant's MultiKueue controller permission to create pods and workloads remotely.

**Tenant only (manager):**
- Secret `worker-kubeconfig` in `kueue-system` (SA-based kubeconfig for worker)
- AdmissionCheck `multikueue` (controller: `kueue.x-k8s.io/multikueue`)
- MultiKueueConfig `multikueue-config` (references `worker-cluster`)
- MultiKueueCluster `worker-cluster` (points to the kubeconfig Secret)
- ClusterQueue patched with `admissionChecks: ["multikueue"]`

## Storage

The worker runs LVMS for local storage (`lvms-vg1` StorageClass). The tenant
does not need local storage for this spike (plain pods only, no PVCs).

## Tests

Integration tests validate that GPU pods get dispatched from tenant to worker
via MultiKueue.

```bash
cd crc-lab-gpuaas
pip install -e .
TENANT_KUBECONFIG=~/.kube/tenant WORKER_KUBECONFIG=~/.kube/worker \
  python -m pytest tests/ -v -s -m multikueue
```

## Teardown

```bash
ansible-playbook -i inventory.yml playbooks/teardown.yml
```

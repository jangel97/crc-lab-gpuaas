# crc-lab-gpuaas

Local lab for GPUaaS spike testing. Two SNO OpenShift 4.22 clusters on a gaming
PC, federated via Liqo v1.2.0. The tenant runs RHOAI and submits GPU workloads.
The worker has an RTX 5090 passed through via VFIO and runs them.

```
Gaming PC (Intel Ultra 9 285K, 62 GB RAM, Ubuntu 24.04)
├── sno-tenant VM        12 vCPU, 16 GB RAM, no GPU
│   ├── RHOAI 2.25       training operator, KServe
│   ├── Liqo             control plane (consumer)
│   └── LVMS             local storage
└── sno-worker VM        8 vCPU, 24 GB RAM, RTX 5090 passthrough
    ├── GPU Operator      nvidia driver, device plugin
    ├── Kueue             quota and scheduling
    └── Liqo              agent (provider)
```

The tenant sees a virtual node `worker` with the GPU. RHOAI users create
PyTorchJobs or InferenceServices on the tenant. Liqo offloads the pods to the
worker as plain ShadowPods. No RHOAI CRDs or operators are installed on the
worker.

## Prerequisites

See [00-prerequisites.md](00-prerequisites.md) for hardware-specific setup
(VFIO, IOMMU, QEMU 9.2 build, libvirt memlock, RTX 5090 XML tweaks).

## Provisioning

```bash
ansible-playbook -i inventory.yml playbooks/01-create-vms.yml
ansible-playbook -i inventory.yml playbooks/02-wait-and-discover.yml
ansible-playbook -i inventory.yml playbooks/03-configure-clusters.yml
ansible-playbook -i inventory.yml playbooks/04-peer-clusters.yml
```

After provisioning, kubeconfigs are at `~/.kube/tenant` and `~/.kube/worker`.

## Playbooks

| Playbook | What it does |
|----------|-------------|
| `01-create-vms.yml` | Creates libvirt VMs, generates install-config, starts SNO install |
| `02-wait-and-discover.yml` | Waits for install to complete, discovers API endpoints |
| `03-configure-clusters.yml` | Installs operators (RHOAI, GPU Operator, Kueue, LVMS, Liqo) |
| `04-peer-clusters.yml` | Peers tenant and worker via Liqo, applies proxy workarounds |
| `teardown.yml` | Destroys VMs and cleans up |

## Liqo workarounds

Liqo v1.2.0 on OpenShift requires five workarounds, all codified in the
playbooks. See the task files for inline comments explaining each one.

| # | Bug | Workaround | File |
|---|-----|-----------|------|
| 1 | SCC template renders invalid labels | Pre-create gateway and fabric SCCs | `install-liqo.yml` |
| 2 | Configuration CRD missing status fields | JSON-patch the CRD after install | `install-liqo.yml` |
| 3 | Proxy init crashes with proxy.enabled=false | Create dummy liqo-proxy Service | `install-liqo.yml` |
| 4 | Proxy URL propagates despite proxy disabled | Delete IP resource, patch ResourceSlice and Identity | `04-peer-clusters.yml` |
| 5 | API server IP remapping disabled but VK requires it | Patch CM arg to enable it | `install-liqo.yml` |

## Tests

Integration tests validate that RHOAI workloads run on the worker GPU without
leaking RHOAI custom resources to the worker cluster.

```bash
cd crc-lab-gpuaas
pip install -e .
TENANT_KUBECONFIG=~/.kube/tenant WORKER_KUBECONFIG=~/.kube/worker \
  python -m pytest tests/ -v -s
```

## Teardown

```bash
ansible-playbook -i inventory.yml playbooks/teardown.yml
```

# GPUaaS Virtual Kubelet Spike

## Can we decouple GPU execution from the RHOAI control plane so multiple tenants share a GPU cluster governed by Kueue?

**Yes.** Virtual Kubelet makes it work. The RHOAI control plane (operators,
CRDs, dashboards) stays on each tenant. Only bare compute pods land on the
shared GPU worker — no RHOAI installed there, just GPU Operator + Kueue.

```mermaid
graph TD
    subgraph tenants["Tenant Clusters (control plane only)"]
        direction LR
        subgraph t1["RHOAI 2.x · OCP 4.22"]
            ops1["Training · KServe\nNotebooks · DSP"]
            vk1["Virtual Kubelet"]
            sm1["Submariner\ngateway + routeagent"]
        end
        subgraph t2["RHOAI 3.x · OCP 4.19"]
            ops2["Training · KServe\nLMEval · Ray"]
            vk2["Virtual Kubelet"]
            sm2["Submariner\ngateway + routeagent"]
        end
    end

    subgraph gpu_cluster["Shared GPU Cluster (no RHOAI)"]
        direction LR
        subgraph infra[" "]
            direction TB
            gpu_op["GPU Operator\nRTX 5090"]
            kueue["Kueue\nquota · preemption"]
            kyverno["Kyverno\npriority labels"]
            sm3["Submariner\ngateway + routeagent"]
        end
        subgraph namespaces["Per-tenant namespaces"]
            direction LR
            ns1["vk-a3f-tenant-ns\nTenant 1 pods"]
            ns2["vk-b7c-tenant-ns\nTenant 2 pods"]
        end
        kueue -.->|admits| ns1
        kueue -.->|admits| ns2
    end

    ops1 -->|creates pods| vk1
    ops2 -->|creates pods| vk2
    vk1 -->|"sync + dispatch"| ns1
    vk2 -->|"sync + dispatch"| ns2
    ns1 -.->|status| vk1
    ns2 -.->|status| vk2
    sm1 <-->|"IPsec tunnel\nServices · Routes · inference"| sm3
    sm2 <-->|"IPsec tunnel\nServices · Routes · inference"| sm3

    style tenants fill:#0f3460,stroke:#16213e,color:#fff
    style t1 fill:#1a3a6b,stroke:#2a5298,color:#fff
    style t2 fill:#1a3a6b,stroke:#2a5298,color:#fff
    style gpu_cluster fill:#1a1a2e,stroke:#e94560,color:#fff
    style infra fill:#2b2d42,stroke:#444,color:#fff
    style namespaces fill:#2b2d42,stroke:#444,color:#fff
    style sm1 fill:#533483,stroke:#7b5ea7,color:#fff
    style sm2 fill:#533483,stroke:#7b5ea7,color:#fff
    style sm3 fill:#533483,stroke:#7b5ea7,color:#fff
```

### What the spike proves

| Claim | How it was validated |
|-------|---------------------|
| **RHOAI version doesn't matter** | RHOAI 2.x (OCP 4.22) and 3.x (OCP 4.19) both dispatch to the same GPU worker. |
| **Tenants run in parallel** | Both suites run simultaneously on the same GPU — no coordination, no awareness of each other. |
| **All major workloads work** | PyTorchJob, KServe, Notebooks, XGBoostJob, TFJob, RayJob, LMEval, DSP — no RHOAI modifications needed. |
| **Kueue enforces quota** | GPU admission, queuing, and cross-tenant preemption all work. Priority is admin-controlled. |
| **Networking works** | Services and inference endpoints reach GPU pods via Submariner. Tunnel self-heals after failure. |
| **Distributed training works** | Headless Service sync enables inter-pod DNS on the worker. Validated for the mechanism; full multi-GPU deferred (lab has 1 GPU). |
| **Worker stays bare** | No RHOAI CRDs or operators on the GPU cluster. |

### What it doesn't prove (yet)

- Full multi-GPU distributed training (mechanism works, need >1 GPU to test end-to-end)
- RBAC sync across clusters (by design — avoids privilege escalation)
- Production HA (single-node clusters in lab)

Full spike assessment with all 22 validated scenarios:
[docs/spike-assessment.md](docs/spike-assessment.md)

---

## VK architecture

The VK provider is a Go binary (`cmd/vk-gpu-provider/`) built on the
`virtual-kubelet/virtual-kubelet` v1.11.0 library. It runs as a Deployment
on each tenant cluster and manages the full pod lifecycle across two clusters.

```mermaid
graph TD
    subgraph tenant["Tenant Cluster"]
        direction TB
        api_t["Tenant API Server"]
        vk_node["Virtual Node\ngpu-worker"]

        subgraph vk_proc["VK Provider Process"]
            direction TB
            lib["VK Library\npod informer · node heartbeat\nlease renewal · work queue"]
            provider["GPUProvider\nCreatePod · DeletePod\nstatus sync · exec proxy"]
            syncer["ResourceSyncer\nsecrets · configmaps · SAs\nheadless services · PVCs"]
            reconciler["DepReconciler\nperiodic resync (2 min)\nevent-driven · GC"]
            kubelet_api["Kubelet API :10350\nkubectl logs · exec · attach"]

            lib -->|"pod assigned\nto virtual node"| provider
            provider --> syncer
            provider --> reconciler
        end

        subgraph t_informers["Tenant Informers"]
            secret_inf["Secret informer"]
            cm_inf["ConfigMap informer"]
            svc_inf["Service informer"]
            pvc_inf["PVC informer"]
        end

        t_informers -->|"resource changed\nenqueue namespace"| reconciler
        pvc_inf -->|"control PVC deleted"| syncer
    end

    subgraph worker["Worker Cluster"]
        direction TB
        api_w["Worker API Server"]
        w_informer["Worker Pod Informer\nlabel: managed-by=vk-gpu-provider"]
        ns["Per-tenant namespaces\n{prefix}{tenant-ns}"]
        gpu["GPU\nRTX 5090"]

        w_informer -->|"status changed"| api_w
    end

    provider -->|"1. ensure namespace\n2. sync resources\n3. create pod"| api_w
    syncer -->|"sync secrets, CMs\nSAs, services, PVCs"| ns
    w_informer -->|"status sync\n(Running, Succeeded, Failed)"| provider
    kubelet_api -->|"log/exec proxy\nvia SPDY"| api_w
    api_w --> gpu

    style tenant fill:#0f3460,stroke:#16213e,color:#fff
    style worker fill:#1a1a2e,stroke:#e94560,color:#fff
    style vk_proc fill:#1a3a6b,stroke:#2a5298,color:#fff
    style t_informers fill:#1a3a6b,stroke:#2a5298,color:#fff
    style ns fill:#2b2d42,stroke:#444,color:#fff
```

### Pod dispatch flow

When a pod is assigned to the virtual node, the provider runs this pipeline:

```mermaid
sequenceDiagram
    participant Op as RHOAI Operator
    participant T as Tenant API
    participant VK as VK Provider
    participant S as ResourceSyncer
    participant R as DepReconciler
    participant W as Worker API
    participant GPU as GPU

    Op->>T: Create pod (nodeName: gpu-worker)
    T->>VK: Pod event (VK library informer)

    Note over VK: Reject DaemonSet pods<br/>Reject system namespaces<br/>Skip if already managed

    VK->>T: Re-read pod from API<br/>(recover original fieldRefs)
    VK->>W: Ensure namespace {prefix}{ns}

    VK->>S: SyncResources(pod)
    S->>T: GET secrets, configmaps, SA
    S->>W: CREATE/UPDATE in worker namespace
    Note over S: Missing resources return nil<br/>(reconciler will retry)

    VK->>S: SyncHeadlessServices(pod)
    S->>T: List services (clusterIP=None)
    S->>W: Sync matching services

    VK->>S: ValidateAndSyncPVCs(pod)
    Note over S: Reject non-catapult PVCs<br/>Create execution PVCs

    Note over VK: transformPod():<br/>clear nodeName, tolerations,<br/>affinity, schedulerName<br/>remap PVC names<br/>strip SA token volumes

    VK->>W: Create pod
    VK->>R: Enqueue namespace

    W->>W: Kyverno adds queue/priority labels
    W->>W: Kueue gates → admits
    W->>GPU: Pod runs

    GPU-->>W: Container exits
    W-->>VK: Worker informer (status sync)
    VK-->>T: Update tenant pod status
    T-->>Op: Operator sees Succeeded
```

### Key internals

**Resource sync is non-blocking.** If a resource doesn't exist yet (e.g.,
cert-manager hasn't issued a TLS secret), the provider creates the worker
pod anyway. The worker kubelet retries volume mounts. Tenant informers
detect the new resource and the reconciler syncs it — the pod starts
without requeuing.

**The reconciler computes desired state from all pods in a namespace,**
not individual pod events. It unions all resource references across pods,
syncs the missing ones, and garbage-collects resources no longer referenced
by any pod. Rate-limited workqueue with exponential backoff. Periodic
resync every 2 minutes guarantees convergence.

**Pod transformation clears all scheduling fields** (nodeName, nodeSelector,
affinity, tolerations, schedulerName, priority) so the worker scheduler
places the pod normally. SecurityContext passes through unchanged — the
worker's SCC/PSA admission re-evaluates it.

**Status sync is informer-driven.** A worker pod informer watches all pods
labeled `managed-by: vk-gpu-provider`. On add/update, the worker pod's
status is copied to the tenant pod via the VK library's `NotifyPods`
callback. On delete (e.g., Kueue preemption), the tenant pod is marked
`Failed` with reason `WorkerPodPreempted`.

**kubectl logs and exec are proxied.** The provider runs a kubelet API
server on port 10350 using the node's TLS cert. `GetContainerLogs` streams
logs from the worker pod. `RunInContainer` uses SPDY to proxy exec sessions
to the worker API server.

**Node capacity is dynamic.** A background goroutine polls worker node
resources every 30s and updates the virtual node's allocatable (CPU,
memory, GPU, pods) only when values change.

For full implementation details, see
[docs/vk-architecture.md](docs/vk-architecture.md).

### PVC handling (Catapult storage)

Cross-cluster storage is opt-in. Users declare a PVC with `storageClassName:
catapult` on the tenant — a no-provisioner class that stays Pending. When a
pod references it, VK creates a real execution PVC on the worker with actual
storage. Non-catapult PVCs are rejected.

```mermaid
graph TD
    subgraph tenant["Tenant Cluster"]
        user["User creates PVC\nstorageClass: catapult"]
        ctrl_pvc["Control PVC\nstatus: Pending\n(no provisioner)"]
        pod_t["Pod references PVC\nnodeName: gpu-worker"]
        user --> ctrl_pvc
        ctrl_pvc --> pod_t
    end

    subgraph vk_sync["VK Provider"]
        validate{"storageClass\n== catapult?"}
        reject["Pod Failed\nInvalidPVCStorageClass"]
        create_exec["Create execution PVC\n{ns}--{name}\nstorageClass: lvms-vg1"]
        remap["transformPod()\nremap claimName\ncheckpoint → ns--checkpoint"]

        validate -->|No| reject
        validate -->|Yes| create_exec
        create_exec --> remap
    end

    subgraph worker["Worker Cluster"]
        exec_pvc["Execution PVC\nstatus: Bound\n(real storage)"]
        pod_w["Worker pod mounts\nexecution PVC"]
        exec_pvc --> pod_w
    end

    pod_t --> validate
    remap --> pod_w

    subgraph lifecycle["PVC Lifecycle"]
        direction LR
        pod_del["Pod deleted →\nexecution PVC persists"]
        pod_restart["Pod restarts →\nsame execution PVC reused"]
        pvc_del["Control PVC deleted →\nPVC informer deletes\nexecution PVC"]
    end

    style tenant fill:#0f3460,stroke:#16213e,color:#fff
    style worker fill:#1a1a2e,stroke:#e94560,color:#fff
    style vk_sync fill:#1a3a6b,stroke:#2a5298,color:#fff
    style lifecycle fill:#2b2d42,stroke:#444,color:#fff
    style reject fill:#e94560,stroke:#1a1a2e,color:#fff
```

Key difference from secrets/configmaps: **PVC lifecycle is independent of pod
lifecycle.** Secrets and configmaps are synced per-pod and garbage-collected
by the reconciler. PVCs persist across pod restarts — a training job can
checkpoint, complete, and a new job can mount the same PVC. Only deleting the
control PVC on the tenant triggers cleanup of the execution PVC (via the PVC
deletion informer).

The remote StorageClass defaults to `lvms-vg1` but can be overridden per-PVC
via the `catapult.redhat.com/remote-storage-class` annotation.

For the full PVC design (failure modes, namespace collision handling, future
proxy PV provisioner), see [docs/pvc-architecture.md](docs/pvc-architecture.md).

## Lab setup

### Prerequisites

See [00-prerequisites.md](00-prerequisites.md) for hardware-specific setup
(VFIO, IOMMU, QEMU 9.2 build, libvirt memlock, RTX 5090 XML tweaks).

### Provisioning

```bash
ansible-playbook -i inventory.yml playbooks/01-create-vms.yml
ansible-playbook -i inventory.yml playbooks/02-wait-and-discover.yml
ansible-playbook -i inventory.yml playbooks/03-configure-clusters.yml
ansible-playbook -i inventory.yml playbooks/04-setup-virtual-kubelet.yml
ansible-playbook -i inventory.yml playbooks/05-setup-submariner.yml
```

After provisioning, kubeconfigs are at `~/.kube/tenant` and `~/.kube/worker`.

### Playbooks

| Playbook | What it does |
|----------|-------------|
| `01-create-vms.yml` | Creates libvirt VMs, generates install-config, starts SNO install |
| `02-wait-and-discover.yml` | Waits for install to complete, discovers API endpoints |
| `03-configure-clusters.yml` | Installs GPU Operator, Kueue, LVMS on worker; RHOAI on tenant |
| `04-setup-virtual-kubelet.yml` | Builds VK image, sets up worker namespace/RBAC/Kueue queues, deploys VK on tenant |
| `05-setup-submariner.yml` | Installs subctl, deploys Submariner broker on GPU cluster, joins both clusters |
| `teardown.yml` | Destroys VMs and cleans up |

### Teardown

```bash
ansible-playbook -i inventory.yml playbooks/teardown.yml
```

## Running tests

```bash
# RHOAI 2.x suite (VK core + RHOAI + networking + Kueue)
TENANT_KUBECONFIG=~/.kube/tenant WORKER_KUBECONFIG=~/.kube/worker \
  python -m pytest tests/ -v

# RHOAI 3.x suite
python -m pytest tests/rhoai_3/e2e/ -v

# By marker
python -m pytest tests/ -v -m vk           # core dispatch
python -m pytest tests/ -v -m rhoai        # RHOAI operator integration
python -m pytest tests/ -v -m networking   # Submariner
python -m pytest tests/ -v -m kueue        # multi-tenant quota
```

## VK provider

Go binary in `cmd/vk-gpu-provider/`, built on `virtual-kubelet/virtual-kubelet` v1.11.0.

```bash
podman build -t quay.io/jmorenas/gpuaas-virtual-kubelet:latest -f cmd/vk-gpu-provider/Dockerfile .
podman push quay.io/jmorenas/gpuaas-virtual-kubelet:latest
```

<details>
<summary>CLI flags</summary>

| Flag | Default | Description |
|------|---------|-------------|
| `--nodename` | `gpu-worker` | Name of the virtual node |
| `--worker-kubeconfig` | *(required)* | Path to worker cluster kubeconfig |
| `--kubeconfig` | *(in-cluster)* | Path to tenant cluster kubeconfig |
| `--gpu-count` | `1` | Number of GPUs to advertise |
| `--worker-namespace-prefix` | *(auto-generated)* | Prefix for per-tenant worker namespaces |
| `--default-remote-storage-class` | `lvms-vg1` | StorageClass for execution PVCs on the GPU cluster |
| `--taint-value` | `catapult` | Value for the `virtual-kubelet.io/provider` taint |
| `--insecure-skip-tls-verify` | `false` | Skip TLS certificate verification for API servers |
| `--kubelet-cert` | *(none)* | PEM file with cert+key for kubelet API TLS (enables `kubectl logs`) |
| `--kubelet-port` | `10350` | Port for the kubelet API server (must differ from real kubelet 10250) |

</details>

<details>
<summary>Resource sync details</summary>

VK discovers resource references by walking the pod spec:

| Reference type | Fields scanned |
|---------------|---------------|
| Secrets | `env[].valueFrom.secretKeyRef`, `envFrom[].secretRef`, `volumes[].secret`, `imagePullSecrets` |
| ConfigMaps | `env[].valueFrom.configMapKeyRef`, `envFrom[].configMapRef`, `volumes[].configMap` |
| ServiceAccounts | `serviceAccountName` (plus the SA's image pull secrets) |
| Headless Services | Services with `clusterIP: None` whose selector matches the pod |
| PVCs | Only `storageClassName: catapult` — see [docs/pvc-architecture.md](docs/pvc-architecture.md) |

Synced resources get management labels (`app.kubernetes.io/managed-by: vk-gpu-provider`,
`vk.gpuaas.io/source-pod`, `vk.gpuaas.io/source-namespace`) for cleanup tracking.

</details>

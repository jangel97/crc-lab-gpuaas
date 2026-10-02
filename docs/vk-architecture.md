# Virtual Kubelet (Catapult) Architecture

Custom Virtual Kubelet that dispatches pods from a tenant cluster to a GPU
worker cluster. Plain `k8s.io/client-go` — no virtual-kubelet library.

## Source Layout

```
cmd/vk-gpu-provider/
├── main.go            # CLI flags, kubeconfig loading, signal handling
├── provider.go        # Node registration, informers, dispatch loop, status sync
├── resourcesync.go    # Secret/ConfigMap/SA/PVC discovery and cross-cluster sync
└── Dockerfile
```

## Startup Sequence

`main.go` parses flags, builds two Kubernetes clients (tenant + worker), and
calls `provider.Run(ctx)`. The provider:

1. **Registers the virtual node** on the tenant cluster (`registerNode`).
   Creates or updates a Node object `gpu-worker` with labels
   (`node.kubernetes.io/gpu: true`), taints (NoSchedule + NoExecute for
   `virtual-kubelet.io/provider`), and a status advertising GPU capacity.

2. **Starts the heartbeat loop** (`heartbeatLoop`). Every 10 seconds: renews
   the node Lease in `kube-node-lease` and refreshes the Node status
   (conditions, capacity). Keeps the node Ready so the scheduler trusts it.

3. **Starts the worker pod informer.** Filtered `ListWatch` on the worker
   namespace for pods with label `app.kubernetes.io/managed-by: vk-gpu-provider`.
   On Add/Update: rebuilds the `managedPods` map (tenant key → worker pod name)
   and syncs worker pod status back to the tenant virtual pod. This informer
   starts first so that on restart the provider recovers its state from existing
   worker pods without re-dispatching.

4. **Starts the control PVC informer.** Watches all PersistentVolumeClaims on
   the tenant. On Delete: if the PVC has `storageClassName: catapult`, deletes
   the corresponding execution PVC on the worker.

5. **Starts the tenant pod informer** (blocking). Filtered `ListWatch` across
   all namespaces for pods assigned to the virtual node
   (`spec.nodeName = gpu-worker`). On Add/Update: dispatch. On Delete: cleanup.

## Dispatch Loop

When a pod is assigned to the virtual node, `handleTenantPod` runs:

```
tenant pod event (Add/Update)
    │
    ├── skip if system namespace (openshift-*, kube-*, redhat-ods-*, default, kueue-system)
    ├── skip if already managed (in managedPods map)
    ├── skip if DeletionTimestamp set → route to deletion handler
    ├── skip if already terminal (Succeeded/Failed)
    │
    ▼
ValidateAndSyncPVCs
    │  For each PVC volume in the pod spec:
    │    1. Get the PVC from tenant
    │    2. Reject if storageClassName != "catapult"
    │    3. Determine remote StorageClass (annotation override or default lvms-vg1)
    │    4. Create execution PVC on worker: {namespace}--{name}
    │    5. Return map: tenant claim name → execution claim name
    │
    ▼
SyncResources
    │  Walk pod spec to discover referenced resources:
    │    Secrets:  env[].valueFrom.secretKeyRef, envFrom[].secretRef,
    │              volumes[].secret, imagePullSecrets
    │    ConfigMaps: env[].valueFrom.configMapKeyRef, envFrom[].configMapRef,
    │                volumes[].configMap
    │    ServiceAccount: spec.serviceAccountName (+ its imagePullSecrets)
    │
    │  For each: GET from tenant → CREATE or UPDATE on worker with labels:
    │    app.kubernetes.io/managed-by: vk-gpu-provider
    │    vk.gpuaas.io/source-namespace: <ns>
    │    vk.gpuaas.io/source-pod: <name>
    │
    ▼
transformPod
    │  Deep-copy the pod spec and rewrite it for the worker cluster:
    │    - Name: {namespace}--{name}
    │    - Namespace: vk-workloads
    │    - Labels: copy user labels (skip openshift.io/*, kueue queue, managed-by)
    │              add: managed-by, source-namespace, source-name, kueue queue
    │    - Annotations: copy user annotations (skip openshift.io/*, kubernetes.io/*, k8s.ovn.org/*)
    │    - Clear: nodeName, nodeSelector, affinity, tolerations, schedulerName, priority
    │    - Remap PVC claim names using the pvcNameMap from step 1
    │    - Strip: kube-api-access volumes, SA token projected volumes
    │    - Strip: SecurityContext on pod and all containers (SCC mutations)
    │    - Filter volume mounts to only reference surviving volumes
    │
    ▼
Create pod on worker
    │  If AlreadyExists: record mapping, skip
    │  On error: set tenant pod to Failed with reason
    │
    ▼
Record in managedPods map: "namespace/name" → worker pod name
```

## Status Sync

The worker pod informer fires on every Add/Update of managed pods.
`syncStatusToTenant` copies:

- Phase, Message, Reason
- Conditions
- ContainerStatuses, InitContainerStatuses
- StartTime
- **PodIP, PodIPs** — the worker pod's real IP, which makes Services/Routes
  on the tenant work via Submariner (see submariner-architecture.md §3.1)

Change detection avoids unnecessary updates: only writes to tenant if Phase,
PodIP, or container Ready/RestartCount changed.

## Deletion and Cleanup

When a tenant pod is deleted (`handleTenantPodDeleted`):

1. Remove entry from `managedPods` map
2. Delete the worker pod
3. `CleanupResources`: list secrets, configmaps, and service accounts on
   worker matching the management labels for this source namespace + pod name,
   delete each one

Execution PVCs are **not** deleted on pod deletion — they persist across pod
restarts. They are only deleted when the control PVC itself is deleted (handled
by the control PVC informer via `CleanupExecutionPVC`).

## Pod Transformation Rules

| Field | Tenant pod | Worker pod |
|-------|-----------|------------|
| Name | `my-pod` | `my-namespace--my-pod` |
| Namespace | user namespace | `vk-workloads` |
| nodeName | `gpu-worker` | *(cleared — let Kueue/scheduler decide)* |
| nodeSelector, affinity, tolerations | set by user/operator | *(cleared)* |
| schedulerName, priority | set by user/operator | *(cleared)* |
| SecurityContext (pod) | SCC-mutated by OpenShift | `{}` (empty) |
| SecurityContext (containers) | SCC-mutated by OpenShift | `nil` |
| ServiceAccountName | user SA | kept (SA synced to worker) |
| kube-api-access volumes | injected by kubelet | *(stripped)* |
| SA token projected volumes | injected by kubelet | *(stripped)* |
| PVC claim names | `checkpoint` | `my-namespace--checkpoint` |
| Labels | user labels | user labels + managed-by + source labels + kueue queue |

## Management Labels

All synced resources carry these labels for tracking and cleanup:

| Label | Purpose |
|-------|---------|
| `app.kubernetes.io/managed-by: vk-gpu-provider` | Identifies VK-managed resources |
| `vk.gpuaas.io/source-namespace` | Tenant namespace the resource came from |
| `vk.gpuaas.io/source-pod` | Tenant pod that triggered the sync |
| `vk.gpuaas.io/source-name` | Tenant pod name (on worker pods, for status sync) |
| `vk.gpuaas.io/source-pvc` | Tenant PVC name (on execution PVCs) |

## Recovery on Restart

If the VK pod restarts, it loses its in-memory `managedPods` map. Recovery:

1. The worker pod informer starts first and lists all managed pods on the
   worker (filtered by `managed-by` label).
2. For each existing worker pod, it reads `source-namespace` and `source-name`
   labels and rebuilds the `managedPods` map.
3. By the time the tenant pod informer starts, the map is populated.
   Existing pods are recognized as already managed → skipped (no re-dispatch).

## Informer Resync

Both informers use a 30-second resync period. The control PVC informer uses
5 minutes (PVC deletion is less time-sensitive). All event handlers are
idempotent — repeated events for the same pod produce no additional API calls
if state hasn't changed.

## Limitations

### Single worker namespace (no multi-tenant isolation)

All dispatched pods land in one namespace (`vk-workloads`) regardless of which
tenant namespace they came from. This has three consequences:

| Issue | Detail |
|-------|--------|
| **Secret/ConfigMap name collisions** | Synced resources keep their original name. If team A and team B both have a secret called `db-creds`, the second sync overwrites the first. Pod names are namespace-prefixed (`{ns}--{name}`) but synced resources are not. |
| **ServiceAccount collisions** | Same problem — two teams with a SA named `trainer` clobber each other. |
| **No network isolation between teams** | All worker pods share a namespace with no NetworkPolicy. Any pod can reach any other pod. |

Tenant-side isolation is fine — each team works in their own namespace with
standard RBAC. The gap is on the worker side.

**Fix (not yet implemented):** use one worker namespace per tenant namespace
(e.g., `vk-team-a`, `vk-team-b`). This gives real RBAC and network isolation
without changing the sync logic. Resource name prefixing alone (like PVCs
already do) would fix collisions but not network isolation.

### No headless Service sync

Distributed training frameworks (PyTorchJob multi-worker) rely on headless
Services for inter-pod DNS discovery. The training operator creates the
headless Service on the tenant, but GPU pods use worker-side CoreDNS which
doesn't know about it. Single-pod training works; multi-pod training does
not yet. See submariner-architecture.md §8.

### No Knative/KServe support

Knative-managed inference (autoscaling, scale-to-zero, queue-proxy) is not
validated. Raw Deployment-based inference works. See
submariner-architecture.md §7.

## CLI Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--nodename` | `gpu-worker` | Virtual node name on tenant |
| `--worker-kubeconfig` | *(required)* | Path to worker cluster kubeconfig |
| `--worker-namespace` | `vk-workloads` | Namespace on worker for dispatched pods |
| `--gpu-count` | `1` | GPUs advertised in node capacity |
| `--kubeconfig` | in-cluster | Tenant cluster kubeconfig (empty = in-cluster) |
| `--default-remote-storage-class` | `lvms-vg1` | Default StorageClass for execution PVCs |

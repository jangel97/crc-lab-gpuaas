# Virtual Kubelet (Catapult) Architecture

Custom Virtual Kubelet that dispatches pods from a tenant cluster to a GPU
worker cluster. Built on the `virtual-kubelet/virtual-kubelet` v1.11.0
library for production-grade pod lifecycle management (work queue with
retry, node heartbeat, lease renewal).

## Source Layout

```
cmd/vk-gpu-provider/
├── main.go            # CLI flags, kubeconfig loading, namespace prefix resolution
├── provider.go        # Node registration, informers, dispatch loop, status sync
├── provider_test.go   # Unit tests for pod transformation (SecurityContext, labels)
├── resourcesync.go    # Secret/ConfigMap/SA/PVC discovery and cross-cluster sync
└── Dockerfile
```

## Startup Sequence

`main.go` parses flags, builds two Kubernetes clients (tenant + worker),
resolves the worker namespace prefix (from `--worker-namespace-prefix` flag
or auto-generated and persisted in ConfigMap `vk-gpu-provider-config` in
`kube-system`), and creates a `nodeutil.Node` via the VK library. The
provider implements `PodLifecycleHandler`, `PodNotifier`, and `NodeProvider`.

1. **Node registration and heartbeat** are handled by the VK library's
   `PodController` and `NodeController`. The library creates or updates the
   Node object, manages the Lease in `kube-node-lease`, and calls
   `ConfigureNode` to set labels (`node.kubernetes.io/gpu: true`), taints
   (NoSchedule + NoExecute for `virtual-kubelet.io/provider`), capacity
   (GPU count, CPU, memory), and the node's InternalIP + kubelet port.

2. **Starts the worker pod informer** (our own, not managed by the library).
   Filtered `ListWatch` across **all namespaces** for pods with label
   `app.kubernetes.io/managed-by: vk-gpu-provider`. On Add/Update: rebuilds
   the `managedPods` map (tenant key → worker pod name) and syncs worker pod
   status back to the tenant virtual pod via `NotifyPods` callback. This
   informer starts first so that on restart the provider recovers its state
   from existing worker pods without re-dispatching.

3. **Starts the control PVC informer.** Watches all PersistentVolumeClaims on
   the tenant. On Delete: if the PVC has `storageClassName: catapult`, deletes
   the corresponding execution PVC on the worker.

4. **Starts the kubelet API server** (if `--kubelet-cert` is provided). An
   HTTPS server on port 10350 (configurable via `--kubelet-port`) that handles
   `kubectl logs`, `kubectl exec`, and `kubectl attach` requests. Uses the
   node's existing kubelet serving cert for TLS. The VK library's
   `AttachProviderRoutes` wires `GetContainerLogs` (and other provider
   methods) to the HTTP handler. Requires `hostNetwork: true` so the server
   binds on the node IP matching the cert SANs.

5. **The VK library starts the tenant pod informer** and its work queue.
   Pods assigned to the virtual node trigger `CreatePod` / `UpdatePod` /
   `DeletePod` on the provider. The library handles retries, rate limiting,
   and concurrent workers (`runtime.NumCPU()`).

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
ensureNamespace
    │  Derive worker namespace: {prefix}{tenant-namespace}
    │  Create namespace on worker if it doesn't exist, with label
    │    app.kubernetes.io/managed-by: vk-gpu-provider
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
SyncHeadlessServices
    │  List Services in the tenant namespace. For each headless Service
    │  (ClusterIP=None) whose selector matches the pod's labels:
    │    CREATE or UPDATE on worker with management labels.
    │  Service name is NOT prefixed — must match pod subdomain for DNS.
    │  Enables inter-pod DNS for distributed training (PyTorchJob).
    │
    ▼
transformPod
    │  Deep-copy the pod spec and rewrite it for the worker cluster:
    │    - Name: {namespace}--{name}
    │    - Namespace: {prefix}{tenant-namespace}
    │    - Labels: copy user labels (skip openshift.io/*, kueue queue, managed-by)
    │              add: managed-by, source-namespace, source-name
    │    - Annotations: copy user annotations (skip openshift.io/*, kubernetes.io/*, k8s.ovn.org/*)
    │    - Clear: nodeName, nodeSelector, affinity, tolerations, schedulerName, priority
    │    - Remap PVC claim names using the pvcNameMap from step 1
    │    - Strip: kube-api-access volumes, SA token projected volumes
    │    - SecurityContext: passed through as-is (see docs/security-context-handling.md)
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

## Headless Service Sync

`SyncHeadlessServices` runs after `SyncResources` in the dispatch path.
It lists all Services in the pod's tenant namespace and syncs any headless
Service (ClusterIP=None) whose selector matches the pod's labels.

```
tenant namespace (vk-test)              worker namespace ({prefix}vk-test)
┌──────────────────────────┐            ┌──────────────────────────┐
│ Headless Service         │            │ Synced Headless Service  │
│   name: job1-worker      │  sync ───> │   name: job1-worker      │
│   selector:              │            │   selector:              │
│     job-name: job1       │            │     job-name: job1       │
│     replica-type: worker │            │     replica-type: worker │
├──────────────────────────┤            ├──────────────────────────┤
│ Pod (virtual)            │            │ Pod (real)               │
│   name: job1-worker-0    │  dispatch  │   name: vk-test--job1-worker-0
│   hostname: job1-worker-0│  ───────>  │   hostname: job1-worker-0│
│   subdomain: job1-worker │            │   subdomain: job1-worker │
│   labels:                │            │   labels:                │
│     job-name: job1       │            │     job-name: job1       │
│     replica-type: worker │            │     replica-type: worker │
└──────────────────────────┘            └──────────────────────────┘
```

**DNS resolution on worker:**
`job1-worker-0.job1-worker.{prefix}vk-test.svc.cluster.local` resolves to
the pod's IP because:
1. The synced headless Service's selector matches the worker pod's labels
2. The worker endpoint controller creates EndpointSlices
3. The pod's `subdomain` matches the Service name
4. CoreDNS on the worker resolves `<hostname>.<subdomain>.<ns>.svc.cluster.local`

**Service name is NOT namespace-prefixed.** Unlike pod names (which get
`{ns}--{name}` prefixing), the headless Service must keep its original name
to match the pod's `subdomain` field. Per-tenant worker namespaces prevent
name collisions across tenant namespaces.

**Non-fatal on error.** If headless Service sync fails (e.g., RBAC, network),
the pod is still dispatched. Inter-pod DNS won't work, but the pod itself
will run.

**RBAC requirements:** The VK service account needs `list` on Services in
tenant namespaces (tenant ClusterRole), and `get/create/update/delete` on
Services in the worker namespace (worker ClusterRole).

## Status Sync

The worker pod informer fires on every Add/Update of managed pods.
`handleWorkerPodEvent` copies the full worker pod status to the cached
tenant pod and calls the `NotifyPods` callback. The VK library then calls
`GetPodStatus` and patches the tenant pod via its own status sync loop.

The cached tenant pod preserves the original tenant pod metadata (labels,
annotations, UID, etc.) while replacing status with the worker pod's status,
which includes:

- Phase, Message, Reason
- Conditions
- ContainerStatuses, InitContainerStatuses
- StartTime
- **PodIP, PodIPs** — the worker pod's real IP, which makes Services/Routes
  on the tenant work via Submariner (see submariner-architecture.md §3.1)

## Deletion and Cleanup

When a tenant pod is deleted (`handleTenantPodDeleted`):

1. Remove entry from `managedPods` map
2. Delete the worker pod
3. `CleanupResources`: list secrets, configmaps, service accounts, and
   headless services on worker matching the management labels for this
   source namespace + pod name, delete each one

Execution PVCs are **not** deleted on pod deletion — they persist across pod
restarts. They are only deleted when the control PVC itself is deleted (handled
by the control PVC informer via `CleanupExecutionPVC`).

## Pod Transformation Rules

| Field | Tenant pod | Worker pod |
|-------|-----------|------------|
| Name | `my-pod` | `my-namespace--my-pod` |
| Namespace | user namespace | `{prefix}{tenant-namespace}` |
| nodeName | `gpu-worker` | *(cleared — let Kueue/scheduler decide)* |
| nodeSelector, affinity, tolerations | set by user/operator | *(cleared)* |
| schedulerName, priority | set by user/operator | *(cleared)* |
| SecurityContext (pod) | SCC-mutated by OpenShift | **passed through as-is** |
| SecurityContext (containers) | SCC-mutated by OpenShift | **passed through as-is** |
| ServiceAccountName | user SA | kept (SA synced to worker) |
| kube-api-access volumes | injected by kubelet | *(stripped)* |
| SA token projected volumes | injected by kubelet | *(stripped)* |
| PVC claim names | `checkpoint` | `my-namespace--checkpoint` |
| Labels | user labels | user labels + managed-by + source labels |

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

## Per-Tenant Worker Namespaces

Each tenant namespace gets its own worker namespace: `{prefix}{tenant-ns}`.
The prefix is either set via `--worker-namespace-prefix` or auto-generated
(8 hex chars, e.g., `vk-27af5d3c-`) and persisted in ConfigMap
`vk-gpu-provider-config` in `kube-system` so it survives restarts.

Worker namespaces are created on-demand by `ensureNamespace` with label
`app.kubernetes.io/managed-by: vk-gpu-provider`. This provides:

- **Resource isolation**: secrets, configmaps, and SAs from different tenants
  never share a namespace — no name collisions.
- **Network isolation potential**: NetworkPolicies can be applied per worker
  namespace to restrict cross-tenant traffic.

## SecurityContext Handling

SecurityContext is passed through unchanged from the tenant pod to the worker
pod. OpenShift SCC mutates SecurityContext fields before the pod reaches VK,
and we cannot distinguish user-set fields from SCC-injected ones. Stripping
any field risks losing user intent silently. If an SCC-generated value is
incompatible with the worker cluster's security policy, the pod fails visibly
(worker SCC/PSA rejects it).

See [security-context-handling.md](security-context-handling.md) for the full
field classification and design rationale.

## Limitations

### Headless Service sync

Headless Services (ClusterIP=None) whose selectors match a dispatched pod
are synced to the worker namespace. This enables inter-pod DNS for
distributed training (PyTorchJob multi-worker). The synced Service name is
NOT namespace-prefixed — it must match the pod's `subdomain` field for DNS
to work: `<hostname>.<subdomain>.<namespace>.svc.cluster.local`.

PyTorchJob compatibility: the training operator creates headless Services
with selector `training.kubeflow.org/job-name`. `transformPod` preserves
these labels and the `hostname`/`subdomain` fields. The worker-side endpoint
controller creates EndpointSlices matching the synced Service's selector.

### No Knative/KServe support

Knative-managed inference (autoscaling, scale-to-zero, queue-proxy) is not
validated. Raw Deployment-based inference works. See
submariner-architecture.md §7.

## Kubelet API Server (Log Proxying)

When `--kubelet-cert` is provided, the VK starts an HTTPS server that
implements the kubelet API endpoints for logs, exec, and attach. This
allows `kubectl logs` and `oc logs` to work transparently against pods
on the virtual node.

```
kubectl logs my-pod -n vk-test
    │
    ▼
API server → GET https://{nodeIP}:10350/containerLogs/vk-test/my-pod/container
    │
    ▼
VK kubelet API server (provider.GetContainerLogs)
    │  Look up worker pod name from managedPods map
    │  Compute worker namespace from prefix + tenant namespace
    │  Forward log options (follow, tail, since, timestamps)
    │
    ▼
worker API server → GET /api/v1/namespaces/{wns}/pods/{wpod}/log
    │
    ▼
Stream response back to kubectl
```

**Requirements:**
- `hostNetwork: true` — the API server must bind on the node IP matching
  the kubelet cert SANs
- Kubelet serving cert at `/var/lib/kubelet/pki/kubelet-server-current.pem`
  (combined cert+key PEM, already trusted by the API server's
  `--kubelet-certificate-authority`)
- Port 10350 (must differ from real kubelet on 10250)
- Privileged SCC for the VK service account (OpenShift)

**Supported operations:** `GetContainerLogs` (full logs, follow, tail,
since, timestamps, limit-bytes). `RunInContainer`, `AttachToContainer`,
and `PortForward` return "not supported".

## CLI Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--nodename` | `gpu-worker` | Virtual node name on tenant |
| `--worker-kubeconfig` | *(required)* | Path to worker cluster kubeconfig |
| `--worker-namespace-prefix` | *(auto-generated)* | Prefix for per-tenant worker namespaces. Auto-generated and persisted in ConfigMap if empty. |
| `--gpu-count` | `1` | GPUs advertised in node capacity |
| `--kubeconfig` | in-cluster | Tenant cluster kubeconfig (empty = in-cluster) |
| `--default-remote-storage-class` | `lvms-vg1` | Default StorageClass for execution PVCs |
| `--taint-value` | `catapult` | Value for the `virtual-kubelet.io/provider` taint |
| `--insecure-skip-tls-verify` | `false` | Skip TLS cert verification for API servers |
| `--kubelet-cert` | *(none)* | PEM file with cert+key for kubelet API TLS (enables `kubectl logs`) |
| `--kubelet-port` | `10350` | Port for kubelet API server (must differ from real kubelet 10250) |

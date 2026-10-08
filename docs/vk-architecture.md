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
├── clientwrap.go      # Workaround: strips unsupported fieldRef env vars for VK library
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
   (NoSchedule + NoExecute for `virtual-kubelet.io/provider`), and the
   node's InternalIP + kubelet port. **Node capacity and allocatable
   resources (CPU, memory, GPU, pods, ephemeral-storage) are fetched
   dynamically from the worker cluster** by aggregating allocatable
   resources across all schedulable worker nodes. `NotifyNodeStatus`
   starts a background goroutine that re-fetches every 30s and pushes
   updates to the tenant only when values change.

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
    ├── reject if DaemonSet-owned (ownerReferences contains Kind=DaemonSet) → ProviderCreateFailed
    ├── reject if system namespace (openshift-*, kube-*, redhat-ods-*, default, kueue-system) → ProviderCreateFailed
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
    │    - Labels: copy user labels (skip openshift.io/*, managed-by)
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
| Labels | user labels | user labels + managed-by + source labels (skip `openshift.io/*`, `pod-security.kubernetes.io/*`, `managed-by`) |

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

## DaemonSet Pod Exclusion

`CreatePod` rejects any pod with a DaemonSet `ownerReference` by returning
`errdefs.InvalidInput`. This causes the VK pod controller to emit a
`ProviderCreateFailed` warning event on the pod and stop tracking it.
The pod displays as `ProviderFailed` in `kubectl get pods` (the status
reason from the event), but the actual pod phase remains `Pending` — the
VK pod controller never transitions it to `Failed`. This means the
DaemonSet controller sees an existing pod for the node and does not
create a replacement, and ClusterOperator health impact is identical to
the previous behavior where rejected pods sat in silent `Pending`.

DaemonSet pods are node-level infrastructure (routeagents, log collectors,
monitoring agents, CNI plugins) that must run on the actual host —
dispatching them cross-cluster breaks them because:

1. They typically need host-level access (iptables, network stack, filesystem)
   that a remote cluster can't provide.
2. Their ServiceAccounts are synced without ClusterRoleBindings, so they lose
   RBAC on the worker.
3. They retry failed operations in tight loops, generating sustained API
   traffic against the worker API server.

Many OpenShift DaemonSets (Istio CNI, Submariner routeagent, OVN, multus,
node-exporter) use `tolerations: [{operator: Exists}]`, which tolerates all
taints including `virtual-kubelet.io/provider`. Taints alone cannot prevent
these pods from being scheduled on the virtual node — the `CreatePod`
rejection is the necessary safety net.

The VK pod controller retries with exponential backoff (1s, 2s, 4s, ...
up to ~16 minutes), so rejected pods are re-attempted infrequently but
indefinitely. Each retry is a single `CreatePod` call that returns
immediately — no worker API traffic, no resources created. The rejected
pods are visible in `kubectl get pods` with a clear `ProviderFailed`
display status and a `ProviderCreateFailed` event explaining the reason
in `kubectl describe pod`.

## SecurityContext Handling

SecurityContext is passed through unchanged from the tenant pod to the worker
pod. OpenShift SCC mutates SecurityContext fields before the pod reaches VK,
and we cannot distinguish user-set fields from SCC-injected ones. Stripping
any field risks losing user intent silently. If an SCC-generated value is
incompatible with the worker cluster's security policy, the pod fails visibly
(worker SCC/PSA rejects it).

See [security-context-handling.md](security-context-handling.md) for the full
field classification and design rationale.

## RHOAI Kueue Compatibility

RHOAI installs Kueue on the tenant cluster by default (`managementState:
Managed` in the DataScienceCluster). Kueue adds a mutating webhook that
intercepts every pod creation. VK is fully compatible — **Kueue does not
gate or mutate VK-bound pods.**

Validated in `test_rhoai_kueue_compat.py` (28GB tenant, RHOAI Kueue Managed):

- No scheduling gates added to VK pods
- No Kueue labels or annotations injected
- GPU and CPU pods dispatched and completed normally

Kueue skips VK pods because they have `nodeName: gpu-worker` set (already
assigned to a node). Since Kueue operates on unscheduled pods, pre-assigned
pods are outside its scope. No LocalQueue or ClusterQueue configuration is
needed on the tenant for VK to work.

**Note:** The tenant SNO requires ~28GB RAM to run RHOAI + Kueue together.
At 14GB, the Kueue controller stays Pending (insufficient memory) and its
webhook blocks all pod creation with 500 errors. The `high_memory_env` test
fixture handles this automatically.

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

### Worker RBAC is cluster-wide

The `vk-remote-sa` service account has a ClusterRoleBinding granting full
CRUD on secrets, configmaps, PVCs, services, and service accounts across
all namespaces on the worker cluster. This means if any non-VK workload
runs on the worker (Kueue, monitoring, GPU Operator), VK's SA can read
and modify its secrets.

**Why it's hard to scope:** Kubernetes RBAC has no namespace wildcards.
Per-tenant worker namespaces are created dynamically (`{prefix}{tenant-ns}`),
so namespaced RoleBindings can't be pre-provisioned. The fix is to have
`ensureNamespace` also create a RoleBinding in each new namespace, binding
`vk-remote-sa` to a namespaced Role. The ClusterRole would then be reduced
to just namespace creation (`get`, `list`, `create` on namespaces) and
node reads.

**Current risk:** Low in the spike (worker cluster is single-purpose GPU
node with no sensitive workloads). In production, this must be scoped.

### KServe / Knative support

KServe raw deployment mode works fully. KServe serverless mode (Knative)
works with Istio sidecar injection disabled (`sidecar.istio.io/inject: "false"`).

**VK library fieldRef workaround (`clientwrap.go`):** The VK library v1.11.0's
`PopulateEnvironmentVariables` does not support `status.podIP`, `status.hostIP`,
or `status.podIPs` fieldRef env vars. Knative's queue-proxy uses `SERVING_POD_IP`
(`status.podIP`) and `HOST_IP` (`status.hostIP`); Istio's sidecar uses
`INSTANCE_IP` (`status.podIP`). The `fieldRefSafeClient` wrapper strips these
unsupported fieldRefs from pods in the VK library's informer List/Watch
responses. The provider's `CreatePod` re-reads the original pod from the API
server, so the worker pod retains the original fieldRefs and the worker kubelet
resolves them normally.

TODO(upstream): contribute `status.podIP`/`hostIP`/`podIPs` support to
`virtual-kubelet/virtual-kubelet` `internal/podutils/env.go` function
`podFieldSelectorRuntimeValue`. Once fixed, delete `clientwrap.go` and
remove the wrapper from `main.go`.

**Istio sidecar** must be disabled on VK-dispatched pods via
`sidecar.istio.io/inject: "false"`. OSSM 2.x uses `policy: disabled` at
the global mesh level, but namespaces that are ServiceMeshMembers get
automatic sidecar injection. KServe serverless requires mesh membership,
so the annotation is required to prevent the `istio-proxy` sidecar from
being injected. Without it, the pod gets stuck in `Pending` on the worker.

**Tested failure mode (`test_istio_sidecar_injection_on_vk_pod`):** When
sidecar injection is enabled on a VK-dispatched pod, the failure is worse
than a sidecar crash — the pod sandbox cannot be created at all:

1. The Istio webhook injects the `istio-proxy` container and adds a
   `k8s.v1.cni.cncf.io/networks: v2-6-istio-cni` annotation.
2. VK copies both to the worker pod (annotations pass through
   `transformPod`).
3. On the worker, Multus reads the annotation and looks for a
   `NetworkAttachmentDefinition` named `v2-6-istio-cni` in the worker
   namespace. It does not exist (the worker has no OSSM).
4. `FailedCreatePodSandBox` — the kubelet retries indefinitely but no
   containers ever start, not even the main one.

This is not a graceful degradation — the pod is permanently stuck in
`ContainerCreating` with an opaque CNI error. The `sidecar.istio.io/inject:
"false"` annotation prevents this entirely by telling the webhook to skip
the pod.

**Why the sidecar would not work even without the CNI issue:**

1. **No control plane.** `istio-proxy` connects to
   `istiod.istio-system.svc` at startup for config, mTLS certificates,
   and routing rules. That service exists only on the tenant cluster.
   From the worker, even with Submariner, the sidecar cannot resolve or
   route to the tenant's `istiod`.
2. **Cert trust.** Istio's CA issues short-lived mTLS certs scoped to
   the tenant mesh. A sidecar on the worker would need cross-cluster
   cert issuance, which is not configured.
3. **Config sync.** Istio pushes VirtualService/DestinationRule config
   to sidecars via xDS. A sidecar on the worker would receive no config
   (it's not registered with the tenant's pilot).

**What is lost:** mTLS between services, Istio telemetry/distributed
tracing, and traffic management (retries, circuit breaking, canary
routing). Inference traffic still works — the curl test proves
tenant → Submariner → worker pod IP:8080 routes correctly without Istio.

**Could it work?** Theoretically, if Submariner exported the `istiod`
service cross-cluster and the mesh trusted the worker's service accounts.
This is untested and likely complex. For cross-cluster Istio, a
multi-cluster mesh (Istio multi-primary or primary-remote) would be the
proper solution, but that is a separate effort from VK.

### Hardcoded namespace references in workloads

Pods run on the worker in a prefixed namespace (`{prefix}{tenant-ns}`), not
the original tenant namespace. The Kubernetes downward API (`fieldRef:
metadata.namespace`) resolves correctly — it reads from the pod's actual
metadata on the worker, so it returns the worker namespace.

However, `transformPod` does not rewrite hardcoded namespace strings inside
container env values, commands, or arguments. A container with
`env: [{name: NS, value: "vk-test"}]` or
`command: ["kubectl", "get", "pods", "-n", "vk-test"]` will reference the
wrong namespace on the worker. Reliably detecting which strings are namespace
references in arbitrary container specs is not feasible without false positives.

**Recommendation:** Workloads should use the downward API for namespace
discovery, which is already a Kubernetes best practice:

```yaml
env:
  - name: POD_NAMESPACE
    valueFrom:
      fieldRef:
        fieldPath: metadata.namespace
```

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
| `--provider-id` | *(--nodename)* | Unique ID for this VK instance. Scopes the `managed-by` label and config ConfigMap to avoid collisions when multiple VK instances share a tenant. Defaults to `--nodename` value. |
| `--worker-kubeconfig` | *(required)* | Path to worker cluster kubeconfig |
| `--worker-namespace-prefix` | *(auto-generated)* | Prefix for per-tenant worker namespaces. Auto-generated and persisted in ConfigMap `vk-gpu-provider-config-{provider-id}` if empty. |
| `--kubeconfig` | in-cluster | Tenant cluster kubeconfig (empty = in-cluster) |
| `--default-remote-storage-class` | `lvms-vg1` | Default StorageClass for execution PVCs |
| `--taint-value` | `catapult` | Value for the `virtual-kubelet.io/provider` taint |
| `--insecure-skip-tls-verify` | `false` | Skip TLS cert verification for API servers |
| `--kubelet-cert` | *(none)* | PEM file with cert+key for kubelet API TLS (enables `kubectl logs`) |
| `--kubelet-port` | `10350` | Port for kubelet API server (must differ from real kubelet 10250) |

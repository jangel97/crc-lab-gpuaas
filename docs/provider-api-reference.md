# Provider API Reference

Complete reference for every interface method and internal function in the
VK GPU provider. The provider is split across five source files:

| File | Responsibility |
|------|----------------|
| `provider.go` | Interface implementations, informers, pod transformation |
| `resourcesync.go` | Cross-cluster resource sync (secrets, configmaps, SAs, PVCs, services) |
| `clientwrap.go` | VK library fieldRef workaround (strips unsupported downward API fields) |
| `main.go` | CLI flags, kubeconfig loading, namespace prefix resolution, node bootstrap |
| `provider_test.go` | Unit tests for pod transformation (SecurityContext pass-through) |

## Interfaces Implemented

```go
var (
    _ node.PodLifecycleHandler = (*GPUProvider)(nil)
    _ node.PodNotifier         = (*GPUProvider)(nil)
    _ node.NodeProvider        = (*GPUProvider)(nil)
)
```

The VK library also calls the provider as a `nodeutil.Provider`, which
adds kubelet API methods (`GetContainerLogs`, `RunInContainer`, etc.).

---

## node.PodLifecycleHandler

Called by the VK library's `PodController` work queue when tenant pods are
assigned to the virtual node. The library handles retries, rate limiting,
and concurrent workers (`runtime.NumCPU()`).

### CreatePod

```go
func (p *GPUProvider) CreatePod(ctx context.Context, pod *corev1.Pod) error
```

Dispatches a tenant pod to the worker cluster. This is the main entry point
for cross-cluster pod lifecycle.

**Guards:**
- Pod is owned by a DaemonSet (checked via `ownerReferences`) → returns
  `errdefs.InvalidInput` error. The VK pod controller emits a
  `ProviderCreateFailed` event and stops tracking the pod.
- Pod is in a system namespace (`openshift-*`, `kube-*`, `redhat-ods-*`,
  `default`, `kueue-system`) → returns `errdefs.InvalidInput` error (same
  behavior as DaemonSet rejection).
- Pod is already tracked in `managedPods` map → returns nil (no-op).

**Steps:**
1. Re-reads the pod from the tenant API server. The VK library's
   `PopulateEnvironmentVariables` runs before `CreatePod` and resolves
   `secretKeyRef`/`configMapKeyRef` to literal values, destroying the
   original references needed for resource sync. The fresh read restores
   them.
2. Derives the worker namespace (`{prefix}{tenant-ns}`) and creates it
   if it does not exist (`ensureNamespace`).
3. Validates and syncs PVCs (`ValidateAndSyncPVCs`). Only
   `storageClassName: catapult` PVCs are allowed. Non-catapult PVCs fail
   the pod with reason `InvalidPVCStorageClass`.
4. Syncs secrets, configmaps, and service accounts discovered in the pod
   spec (`SyncResources`).
5. Syncs headless services whose selectors match the pod
   (`SyncHeadlessServices`). Non-fatal on error.
6. Transforms the pod spec for the worker cluster (`transformPod`).
7. Creates the pod on the worker. If `AlreadyExists`, records the mapping
   silently.

**Error handling:**
- Namespace creation failure → pod set to `Failed/NamespaceCreateFailed`
  via `storeAndNotify`, returns nil (no retry).
- PVC validation failure → pod set to `Failed/InvalidPVCStorageClass`,
  returns nil.
- Resource sync failure → returns error (library retries).
- Worker pod creation failure → returns error (library retries), except
  `AlreadyExists` which is silently accepted.

**Side effects:**
- Creates namespace, secrets, configmaps, SAs, headless services, PVCs,
  and a pod on the worker cluster.
- Updates `managedPods` map: `"namespace/name" → worker pod name`.
- Updates `podCache` map with a deep copy of the tenant pod.

### UpdatePod

```go
func (p *GPUProvider) UpdatePod(ctx context.Context, pod *corev1.Pod) error
```

No-op. Returns nil unconditionally.

The VK library calls this when a tenant pod's spec changes. The provider
does not propagate updates because Kubernetes does not allow mutating most
pod spec fields after creation. Status updates flow in the opposite
direction (worker → tenant) via the worker informer.

### DeletePod

```go
func (p *GPUProvider) DeletePod(ctx context.Context, pod *corev1.Pod) error
```

Cleans up worker resources when a tenant pod is deleted.

**Guards:** Silently returns nil for pods not in `managedPods` (including
previously rejected DaemonSet and system namespace pods).

**Steps:**
1. Removes the pod from `managedPods` and `podCache`.
2. Deletes the worker pod (ignores `NotFound`).
3. Cleans up synced resources: secrets, configmaps, service accounts,
   and headless services matching the management labels for this source
   namespace + pod name.

**What is NOT cleaned up:** Execution PVCs. They persist across pod
restarts. Cleanup happens when the control PVC is deleted (handled by the
PVC informer).

### GetPod

```go
func (p *GPUProvider) GetPod(ctx context.Context, namespace, name string) (*corev1.Pod, error)
```

Returns the cached tenant-view pod (with worker status merged in).

**Returns:** Deep copy from `podCache`, or `errdefs.NotFound` if the pod
is not tracked.

The VK library calls this during its status sync loop. The returned pod
has the original tenant metadata (labels, annotations, UID) with the
worker pod's status (phase, conditions, container statuses, PodIP).

### GetPodStatus

```go
func (p *GPUProvider) GetPodStatus(ctx context.Context, namespace, name string) (*corev1.PodStatus, error)
```

Returns the status portion of the cached tenant-view pod.

**Returns:** Deep copy of `podCache[key].Status`, or `errdefs.NotFound`.

The VK library calls this to patch the tenant pod's status. The returned
status includes the worker pod's real PodIP, which is essential for
Submariner-based cross-cluster networking (Services on the tenant cluster
use this IP in their EndpointSlices).

### GetPods

```go
func (p *GPUProvider) GetPods(ctx context.Context) ([]*corev1.Pod, error)
```

Returns all currently tracked pods.

**Returns:** Slice of deep copies from all entries in `podCache`.

The VK library calls this on startup to reconcile its internal state with
the provider's. Each returned pod is compared against the tenant API
server's state, and any discrepancies trigger `CreatePod` or `DeletePod`.

---

## node.PodNotifier

### NotifyPods

```go
func (p *GPUProvider) NotifyPods(ctx context.Context, cb func(*corev1.Pod))
```

Registers a callback for status change notifications.

The VK library calls this once during startup, passing a callback that
triggers its status sync loop. The provider stores the callback in
`notifyCb` and invokes it from `handleWorkerPodEvent` and
`handleWorkerPodDelete` whenever a worker pod's status changes.

**Thread safety:** The callback reference is stored and read under
`p.mu`. The callback itself is invoked outside the lock.

---

## node.NodeProvider

### Ping

```go
func (p *GPUProvider) Ping(ctx context.Context) error
```

Health check for the virtual node. Called periodically by the VK library's
`NodeController` to determine node readiness.

**Implementation:** Calls `Discovery().ServerVersion()` on the worker
client. Returns nil if the worker API server responds, error otherwise.

When `Ping` returns an error, the VK library sets the node's `Ready`
condition to `False`, which prevents the scheduler from assigning new pods
to the virtual node.

### NotifyNodeStatus

```go
func (p *GPUProvider) NotifyNodeStatus(ctx context.Context, cb func(*corev1.Node))
```

No-op. The provider does not dynamically update node status. All node
configuration is set once in `ConfigureNode`.

The VK library provides this callback so providers can push node status
changes (e.g., capacity changes as GPUs go offline). The GPU provider
could use this to dynamically adjust `nvidia.com/gpu` capacity based on
worker cluster state, but this is not implemented.

### ConfigureNode

```go
func (p *GPUProvider) ConfigureNode(n *corev1.Node)
```

Configures the virtual node spec at startup. Called once from the
`nodeutil.NewNode` factory function (not from the interface — it is
called explicitly in `main.go`).

**Sets:**
- Labels: `kubernetes.io/os: linux`, `kubernetes.io/arch: amd64`,
  `node.kubernetes.io/gpu: true`
- Taints: `virtual-kubelet.io/provider={taintValue}:NoSchedule` and
  `:NoExecute`
- Capacity and Allocatable: 8 CPU, 24Gi memory, 20 pods,
  `nvidia.com/gpu: {gpuCount}`
- Conditions: `Ready=True`, all pressure conditions `False`
- NodeInfo: `linux/amd64`, `v1.31.0-vk`
- DaemonEndpoints: kubelet port (default 10350)
- Addresses: `InternalIP` from `NODE_IP` env var (fallback `127.0.0.1`)

**Design note:** Capacity values are static and do not reflect the actual
worker cluster resources. They are set high enough to accept the expected
workload count. The `nvidia.com/gpu` value is the only meaningful resource
— it controls how many GPU pods the scheduler places on the virtual node.

---

## Kubelet API Methods (nodeutil.Provider)

These methods are called by the VK library's HTTP handler when the
Kubernetes API server proxies kubelet requests (logs, exec, attach) to
the virtual node.

### GetContainerLogs

```go
func (p *GPUProvider) GetContainerLogs(ctx context.Context, namespace, podName, containerName string, opts api.ContainerLogOpts) (io.ReadCloser, error)
```

Proxies `kubectl logs` requests to the worker cluster.

**Steps:**
1. Looks up the worker pod name from `managedPods`.
2. Computes the worker namespace from the prefix + tenant namespace.
3. Maps VK library log options to Kubernetes `PodLogOptions`: container,
   follow, previous, timestamps, tail lines, limit bytes, since seconds,
   since time.
4. Calls `GetLogs().Stream()` on the worker client and returns the
   stream.

**Returns:** `io.ReadCloser` streaming the worker pod's logs, or
`errdefs.NotFound` if the pod is not tracked.

**Supports:** Full logs, `--follow`, `--tail`, `--since`, `--timestamps`,
`--limit-bytes`, `--previous`.

### RunInContainer

```go
func (p *GPUProvider) RunInContainer(ctx context.Context, namespace, podName, containerName string, cmd []string, attach api.AttachIO) error
```

Not implemented. Returns `"not supported"`.

Would handle `kubectl exec`. Implementing this requires SPDY or
WebSocket proxying to the worker kubelet, which is substantially more
complex than log streaming.

### AttachToContainer

```go
func (p *GPUProvider) AttachToContainer(ctx context.Context, namespace, podName, containerName string, attach api.AttachIO) error
```

Not implemented. Returns `"not supported"`.

Would handle `kubectl attach`. Same SPDY/WebSocket requirement as
`RunInContainer`.

### GetStatsSummary

```go
func (p *GPUProvider) GetStatsSummary(ctx context.Context) (*statsv1alpha1.Summary, error)
```

Not implemented. Returns `nil, "not supported"`.

Would provide node and pod resource usage stats for `kubectl top`. Could
be implemented by aggregating `metrics.k8s.io` data from the worker
cluster.

### GetMetricsResource

```go
func (p *GPUProvider) GetMetricsResource(ctx context.Context) ([]*dto.MetricFamily, error)
```

Not implemented. Returns `nil, "not supported"`.

Would provide Prometheus-format metrics for the kubelet metrics endpoint.

### PortForward

```go
func (p *GPUProvider) PortForward(ctx context.Context, namespace, pod string, port int32, stream io.ReadWriteCloser) error
```

Not implemented. Returns `"not supported"`.

Would handle `kubectl port-forward`. Requires bidirectional streaming to
the worker pod, similar to exec/attach.

---

## Worker Informer

The provider runs its own informer (separate from the VK library's tenant
pod informer) to watch worker pods and sync status back.

### startWorkerInformer

```go
func (p *GPUProvider) startWorkerInformer(ctx context.Context) error
```

Creates a filtered `ListWatch` across all namespaces for pods with label
`app.kubernetes.io/managed-by: vk-gpu-provider`. Registers three event
handlers and blocks until the cache is synced.

**Resync period:** 30 seconds.

**Event handlers:**
- `AddFunc` → `handleWorkerPodEvent`: Rebuilds `managedPods` mapping on
  restart recovery. Syncs worker status to tenant pod cache.
- `UpdateFunc` → `handleWorkerPodEvent`: Same as Add.
- `DeleteFunc` → `handleWorkerPodDelete`: Detects external pod deletion
  (e.g., Kueue preemption) and transitions tenant pod to Failed.

**Startup recovery:** On VK restart, `managedPods` is empty. The
informer lists all existing worker pods, each `AddFunc` call rebuilds
the mapping from `source-namespace`/`source-name` labels. By the time
the VK library's tenant pod informer starts, all existing pods are
recognized as already managed and are not re-dispatched.

### handleWorkerPodEvent

```go
func (p *GPUProvider) handleWorkerPodEvent(obj interface{})
```

Called on worker pod Add and Update events. Syncs worker pod status to
the tenant pod view.

**Guards:**
- Ignores non-`*corev1.Pod` objects.
- Ignores pods with `DeletionTimestamp` set (terminating pods are handled
  by `handleWorkerPodDelete` once fully gone).

**Steps:**
1. Extracts `source-namespace` and `source-name` from worker pod labels.
2. If the pod is not in `managedPods`, adds it (recovery path on restart).
3. Retrieves or creates the tenant pod view: uses `podCache` if available,
   otherwise fetches from tenant API server, otherwise creates a minimal
   pod object.
4. Replaces the tenant pod's `Status` with the worker pod's status
   (deep copy). This carries PodIP, phase, conditions, container statuses.
5. Stores the updated tenant pod in `podCache` and invokes `notifyCb` to
   trigger the VK library's status sync.

### handleWorkerPodDelete

```go
func (p *GPUProvider) handleWorkerPodDelete(obj interface{})
```

Called when a worker pod is deleted externally (not by VK's `DeletePod`).
Handles Kueue preemption, manual deletion, or any other external deletion.

**Tombstone handling:** If the informer missed the delete event and only
has a `DeletedFinalStateUnknown` tombstone, extracts the pod from it.

**Steps:**
1. Extracts source labels. Ignores pods without them.
2. Removes the pod from `managedPods`. Checks if it was managed — if not,
   returns silently.
3. Creates a tenant pod view (from cache or minimal object).
4. Sets status to `Failed` with reason `WorkerPodPreempted` and a message
   identifying the deleted worker pod.
5. Stores in `podCache` and invokes `notifyCb`.

**Distinction from DeletePod:** `DeletePod` is called when the *tenant*
pod is deleted (user-initiated). `handleWorkerPodDelete` is called when
the *worker* pod is deleted externally. `DeletePod` cleans up the worker
pod; `handleWorkerPodDelete` reports the external deletion back to the
tenant.

---

## PVC Informer

### startPVCInformer

```go
func (p *GPUProvider) startPVCInformer(ctx context.Context)
```

Watches all PersistentVolumeClaims on the tenant cluster.

**Resync period:** 5 minutes (PVC deletion is less time-sensitive).

**DeleteFunc handler:** When a PVC with `storageClassName: catapult` is
deleted on the tenant, calls `CleanupExecutionPVC` to delete the
corresponding execution PVC (`{namespace}--{name}`) on the worker.

**Tombstone handling:** Same pattern as `handleWorkerPodDelete`.

**Non-blocking:** Does not wait for cache sync. Runs in a background
goroutine.

---

## Internal Helpers

### storeAndNotify

```go
func (p *GPUProvider) storeAndNotify(pod *corev1.Pod, phase corev1.PodPhase, reason, message string)
```

Sets a pod to a terminal state (typically `Failed`) and notifies the VK
library. Used when dispatch fails at a point where retrying would not
help (namespace creation, PVC validation).

**Steps:**
1. Deep-copies the pod.
2. Sets `Status.Phase`, `Status.Reason`, `Status.Message`.
3. Stores in `podCache` and invokes `notifyCb`.

### ensureNamespace

```go
func (p *GPUProvider) ensureNamespace(ctx context.Context, ns string) error
```

Creates a namespace on the worker cluster if it does not exist. Labels it
with `app.kubernetes.io/managed-by: vk-gpu-provider`. Handles
`AlreadyExists` gracefully (returns nil).

### transformPod

```go
func (p *GPUProvider) transformPod(pod *corev1.Pod, workerNS string, pvcNameMap map[string]string) *corev1.Pod
```

Transforms a tenant pod spec into a worker pod spec. See
[Pod Transformation Rules](vk-architecture.md#pod-transformation-rules)
for the field-by-field mapping.

**Key transformations:**
- Name → `{namespace}--{name}` (namespace-prefixed to avoid collisions)
- Namespace → worker namespace
- Labels: copies user labels, skips `openshift.io/*`,
  `pod-security.kubernetes.io/*`, and `managed-by`. Adds management labels.
- Annotations: copies user annotations, skips `openshift.io/*`,
  `kubernetes.io/*`, `k8s.ovn.org/*`.
- Clears: `nodeName`, `nodeSelector`, `affinity`, `tolerations`,
  `schedulerName`, `priority`, `priorityClassName`.
- Remaps PVC claim names using `pvcNameMap`.
- Strips `kube-api-access` volumes and projected SA token volumes.
- Filters volume mounts to only reference surviving volumes.
- SecurityContext: passed through unchanged.

### workerPodName

```go
func workerPodName(namespace, name string) string
```

Returns `{namespace}--{name}`. Prevents name collisions when multiple
tenant namespaces dispatch pods with the same name.

### workerNamespace

```go
func workerNamespace(prefix, tenantNS string) string
```

Returns `{prefix}{tenantNS}`. Each tenant namespace maps to exactly one
worker namespace.

### isSystemNamespace

```go
func isSystemNamespace(ns string) bool
```

Returns true for namespaces that should never be dispatched: `openshift-*`,
`kube-*`, `redhat-ods-*`, `default`, `kueue-system`.

### isDaemonSetPod

```go
func isDaemonSetPod(pod *corev1.Pod) bool
```

Returns true if any `ownerReference` has `Kind: DaemonSet`. See
[DaemonSet Pod Exclusion](vk-architecture.md#daemonset-pod-exclusion)
for rationale.

### filterVolumes

```go
func filterVolumes(volumes []corev1.Volume) []corev1.Volume
```

Removes kube-api-access volumes and projected volumes containing SA
tokens. These are injected by the tenant kubelet and would not function
on the worker cluster (different service account trust domain).

**Removes:**
- Any volume with name prefix `kube-api-access`
- Any `Projected` volume containing a `ServiceAccountToken` source

### filterVolumeMounts

```go
func filterVolumeMounts(mounts []corev1.VolumeMount, volumes []corev1.Volume) []corev1.VolumeMount
```

Removes volume mounts that reference volumes no longer present after
`filterVolumes`. Builds a set of surviving volume names and only keeps
mounts whose `Name` is in the set.

---

## ResourceSyncer (resourcesync.go)

Cross-cluster resource sync engine. Stateless — all state is in the
Kubernetes objects themselves (management labels for tracking).

### SyncResources

```go
func (s *ResourceSyncer) SyncResources(ctx context.Context, pod *corev1.Pod) error
```

Discovers and syncs all secrets, configmaps, and service accounts
referenced by the pod spec to the per-tenant worker namespace.

**Discovery (`discoverReferences`)** walks:
- `env[].valueFrom.secretKeyRef` / `configMapKeyRef`
- `envFrom[].secretRef` / `configMapRef`
- `volumes[].secret` / `configMap`
- `imagePullSecrets`

De-duplicates via map. Service account sync also syncs the SA's own
`imagePullSecrets`.

**Sync semantics (per resource):** GET from tenant → CREATE on worker,
or UPDATE if already exists. Each synced resource gets management labels:
`managed-by`, `source-namespace`, `source-pod`.

**Special cases:**
- `ServiceAccountToken`-type secrets are skipped (they are cluster-bound
  and won't work cross-cluster).
- The `default` service account is not synced.
- Resources not found on tenant are skipped with a warning (not an error).

### SyncHeadlessServices

```go
func (s *ResourceSyncer) SyncHeadlessServices(ctx context.Context, pod *corev1.Pod) error
```

Lists all services in the pod's tenant namespace. For each headless
service (`ClusterIP=None`) whose selector labels are a subset of the
pod's labels, syncs the service to the worker namespace.

Service names are NOT namespace-prefixed — they must match the pod's
`subdomain` field for DNS resolution to work.

### ValidateAndSyncPVCs

```go
func (s *ResourceSyncer) ValidateAndSyncPVCs(ctx context.Context, pod *corev1.Pod) (map[string]string, error)
```

Validates that all PVC volume references use `storageClassName: catapult`
and syncs execution PVCs to the worker.

**Returns:** Map of tenant claim name → execution claim name (e.g.,
`"checkpoint" → "my-namespace--checkpoint"`).

**Rejects:** Any PVC with a non-catapult StorageClass returns an error
(pod will be set to `Failed/InvalidPVCStorageClass`).

**Remote StorageClass:** Determined by the PVC's
`catapult.redhat.com/remote-storage-class` annotation, falling back to
`--default-remote-storage-class` (default `lvms-vg1`).

**Idempotent:** If the execution PVC already exists, skips creation.

### CleanupResources

```go
func (s *ResourceSyncer) CleanupResources(ctx context.Context, sourceNS, podName string) error
```

Deletes all synced resources (secrets, configmaps, service accounts,
headless services) on the worker that carry the management labels for
this source namespace + pod name.

### CleanupExecutionPVC

```go
func (s *ResourceSyncer) CleanupExecutionPVC(ctx context.Context, sourceNS, pvcName string) error
```

Deletes the execution PVC (`{namespace}--{name}`) on the worker. Called
by the PVC informer when the control PVC is deleted on the tenant.

---

## fieldRefSafeClient (clientwrap.go)

Workaround for VK library v1.11.0 not supporting `status.podIP`,
`status.hostIP`, or `status.podIPs` in downward API `fieldRef` env vars.

### Problem

The VK library's `PopulateEnvironmentVariables` runs before `CreatePod`
and calls `podFieldSelectorRuntimeValue` for each `fieldRef` env var.
Unsupported field paths (anything under `status.*` except those listed
below) cause an `"unsupported fieldPath"` error that triggers infinite
requeue. Knative's queue-proxy (`SERVING_POD_IP`, `HOST_IP`) and Istio's
sidecar (`INSTANCE_IP`) use these fieldRefs.

### Supported field paths

```
spec.nodeName, spec.serviceAccountName,
metadata.name, metadata.namespace, metadata.uid,
metadata.annotations, metadata.labels,
metadata.annotations[<key>], metadata.labels[<key>]
```

### How it works

`fieldRefSafeClient` wraps `kubernetes.Interface` and intercepts only the
`CoreV1().Pods(ns).List()` and `CoreV1().Pods(ns).Watch()` calls. For
each pod returned, it strips env vars whose `fieldRef.fieldPath` is not
in the supported set by clearing `ValueFrom` and setting `Value` to `""`.

The provider's `CreatePod` re-reads the original pod from the API server,
so the worker pod retains the original fieldRefs and the worker kubelet
resolves them normally.

### Types

- `fieldRefSafeClient` — wraps `kubernetes.Interface`, overrides `CoreV1()`
- `fieldRefSafeCoreV1` — wraps `CoreV1Interface`, overrides `Pods(ns)`
- `fieldRefSafePods` — wraps `PodInterface`, overrides `List` and `Watch`
- `fieldRefSafeWatch` — proxies watch events, stripping fieldRefs on each
  `*corev1.Pod` event (deep-copied to avoid modifying the informer cache)

---

## main.go

### resolveNamespacePrefix

```go
func resolveNamespacePrefix(ctx context.Context, client kubernetes.Interface) (string, error)
```

Resolves the worker namespace prefix. Checked in order:

1. ConfigMap `vk-gpu-provider-config` in `kube-system` — if it exists and
   has key `worker-namespace-prefix`, uses that value.
2. Otherwise generates `vk-{8 random hex chars}-` and persists it in the
   ConfigMap (creates or updates).

This ensures the prefix survives VK restarts. Each VK instance (one per
tenant cluster) gets its own unique prefix, preventing worker namespace
collisions when multiple tenants share the same GPU cluster.

### buildClient

```go
func buildClient(kubeconfig string, insecure bool) (kubernetes.Interface, error)
```

Builds a Kubernetes client. Tries in order:

1. Explicit kubeconfig path (if provided)
2. In-cluster config (`KUBERNETES_SERVICE_HOST`)
3. `~/.kube/config` fallback

If `insecure` is true, disables TLS verification and clears CA data.

---

## State Model

The provider maintains two in-memory maps under `sync.Mutex`:

| Map | Key | Value | Purpose |
|-----|-----|-------|---------|
| `managedPods` | `"namespace/name"` | worker pod name | Tracks which tenant pods have been dispatched. Used by `DeletePod` to find the worker pod, by `CreatePod` to skip already-dispatched pods, and by `GetContainerLogs` to resolve log requests. |
| `podCache` | `"namespace/name"` | `*corev1.Pod` | Tenant-view pod with worker status. Returned by `GetPod`/`GetPodStatus`/`GetPods`. Updated by `handleWorkerPodEvent` (status sync), `handleWorkerPodDelete` (preemption), and `storeAndNotify` (dispatch failures). |

Both maps are populated on startup by the worker informer's recovery path
and updated throughout the pod lifecycle.

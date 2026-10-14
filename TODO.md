# TODO

## Architecture

- [ ] **Decouple resource syncer into a standalone Go library.**
  `ResourceSyncer` and `DepReconciler` are generic cross-cluster resource
  sync — not coupled to Virtual Kubelet or GPU workloads. Extract into a
  reusable library. The boundary is already clean: both are separate structs
  with explicit interfaces (podCache snapshot callback, Enqueue). Main
  coupling points to break: `discoverReferences` (pod spec walking),
  `managedByValue` (label scoping), catapult-specific PVC logic.

## Upstream contributions

- [x] **Contribute `status.podIP`/`hostIP`/`podIPs` fieldRef support to
  virtual-kubelet.** Fix `internal/podutils/env.go` →
  `podFieldSelectorRuntimeValue`. **Merged:**
  [virtual-kubelet#1451](https://github.com/virtual-kubelet/virtual-kubelet/pull/1451).
  TODO: delete `clientwrap.go` once we bump to a VK release that includes this.

- [ ] **Contribute `tolerations`/`nodeName` fields to LMEvalJob CRD.**
  Currently only `spec.pod.affinity` is exposed. Remove Kyverno workaround
  once available. **Open:**
  [trustyai-service-operator#1005](https://github.com/trustyai-explainability/trustyai-service-operator/pull/1005),
  [docs#85](https://github.com/trustyai-explainability/trustyai-explainability.github.io/pull/85).

- [ ] **Contribute `routeAgentNodeSelector` to Submariner CR.** Allows
  excluding virtual nodes from routeagent DaemonSet without a webhook.
  **Open:**
  [submariner-operator#4301](https://github.com/submariner-io/submariner-operator/pull/4301).

## Production readiness

- [ ] **Scope worker RBAC to per-tenant namespaces.** Extend
  `ensureNamespace` to create a RoleBinding in each new namespace. Reduce
  ClusterRole to namespace creation + node reads.

- [ ] **Handle SecurityContext SCC mismatch.** Strip SCC-injected fields
  before creating worker pod, let worker SCC re-assign. Requires
  distinguishing user-set from SCC-injected fields.

- [ ] **Worker namespace garbage collection.** Periodic reconciler to delete
  worker namespaces with no pods and no recent activity.

- [ ] **Multi-GPU validation.** Test multi-worker PyTorchJob with NCCL/Gloo.
  Test Kueue `borrowWithinCohort` and multi-ClusterQueue fair-sharing.

- [ ] **HA validation.** Test VK with multiple replicas + leader election.
  Test Submariner with multi-node clusters. Validate Kueue during failover.

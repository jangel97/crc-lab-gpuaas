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

- [ ] **Contribute `status.podIP`/`hostIP`/`podIPs` fieldRef support to
  virtual-kubelet.** Fix `internal/podutils/env.go` →
  `podFieldSelectorRuntimeValue`. Once merged, delete `clientwrap.go`.

- [ ] **Contribute `tolerations`/`nodeName` fields to LMEvalJob CRD.**
  Currently only `spec.pod.affinity` is exposed. Remove Kyverno workaround
  once available.

- [ ] **Contribute `routeAgentNodeSelector` to Submariner CR.** Allows
  excluding virtual nodes from routeagent DaemonSet without a webhook.

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

# Catapult PVC Architecture

## Problem

When a pod references a PVC, Catapult must decide what to do with the
volume on the execution (GPU) cluster. The naive approach -- clone the
PVC with the same size and access modes -- is unsafe:

1. A PVC containing training data becomes an empty volume. The pod runs,
   finds no data, and either fails or produces wrong results silently.
2. Every PVC is cloned regardless of user intent. A pod referencing a
   local PostgreSQL PVC gets an empty database on the execution cluster.
3. The cloned PVC is deleted with the pod. Persistent storage that was
   meant to survive across runs is destroyed on pod completion.

## Design

Remote storage is **opt-in**. Users declare intent by using the `catapult`
StorageClass on the control cluster. Catapult creates a corresponding
execution PVC on the GPU cluster with real storage.

```
Control cluster                              Execution cluster (GPU)
+---------------------------------+         +---------------------------------+
| PVC "checkpoint"                |         | PVC "ns-a--checkpoint"          |
|   storageClass: catapult        |         |   storageClass: lvms-vg1        |
|   status: Pending               | Catapult|   status: Bound                 |
|   annotation:                   |-------->|   labels:                       |
|     catapult.redhat.com/        | creates |     managed-by: vk-gpu-provider |
|     remote-storage-class:       |         |     source-ns: ns-a             |
|     lvms-vg1                    |         |     source-pvc: checkpoint      |
|                                 |         |                                 |
| Pod "train-0"                   |         | Pod "ns-a--train-0"             |
|   volumes:                      | offloads|   volumes:                      |
|   - pvc: checkpoint             |-------->|   - pvc: ns-a--checkpoint       |
+---------------------------------+         +---------------------------------+
```

Pods referencing non-catapult PVCs are rejected with a clear error.

## StorageClass

The `catapult` StorageClass uses `kubernetes.io/no-provisioner`, which
means no storage is provisioned on the control cluster. The PVC stays
in Pending state.

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: catapult
provisioner: kubernetes.io/no-provisioner
reclaimPolicy: Retain
volumeBindingMode: WaitForFirstConsumer
```

### Why Pending is acceptable

Catapult IS the kubelet for the virtual node. Unlike a real kubelet, it
does not run volume mount logic -- it creates a worker pod on the
execution cluster. The Kubernetes control plane on the control cluster
does not block pod execution based on PVC binding state when the
"kubelet" (Catapult) controls pod status directly.

The RHOAI training operator does not check PVC binding state. It creates
pods and watches pod status. A Pending PVC does not interfere with
training workload dispatch.

**TODO**: Verify Pending PVC behavior with RHOAI dashboard and notebook
controllers. If Pending state causes issues, a proxy PV approach is
needed (see Future Work).

## Selecting the remote StorageClass

The execution PVC StorageClass is determined by:

1. **Annotation** (per-PVC override):
   ```yaml
   metadata:
     annotations:
       catapult.redhat.com/remote-storage-class: ceph-rbd
   ```

2. **Flag** (cluster-wide default):
   ```
   --default-remote-storage-class=lvms-vg1
   ```

The annotation takes precedence over the flag.

## Namespace collision handling

All execution PVCs are created in a single namespace (`vk-workloads`).
To avoid collisions when different control namespaces have PVCs with
the same name, execution PVCs use a namespace-prefixed name:

```
control:   namespace-a/checkpoint  -->  execution: namespace-a--checkpoint
control:   namespace-b/checkpoint  -->  execution: namespace-b--checkpoint
```

`transformPod` rewrites `ClaimName` in the worker pod spec to match.

## PVC lifecycle

```
                         USER CREATES PVC
                    storageClass: catapult
                              |
                              v
                  +---------------------------+
                  |  Control PVC: Pending     |
                  |  (no provisioner)         |
                  +-------------+-------------+
                                |
                   POD SCHEDULED TO VK NODE
                                |
                                v
              +---------------------------------+
              | Catapult checks PVC class       |
              |   catapult?  -> create exec PVC |
              |   other?     -> FAIL pod        |
              +-----------------+---------------+
                                |
                                v
                  +---------------------------+
                  | Execution PVC: Bound      |
                  | (real storage on GPU node)|
                  +-------------+-------------+
                                |
                   WORKER POD RUNS WITH EXEC PVC
                                |
              +-----------------+-----------------+
              |                                   |
        POD DELETED                         POD RESTARTS
              |                                   |
              v                                   v
    Worker pod cleaned up                Same exec PVC reused
    Execution PVC PERSISTS               (already Bound)
              |
              |
    USER DELETES CONTROL PVC
              |
              v
    Catapult deletes execution PVC
    (via PVC deletion informer)
```

Key difference from secrets/configmaps: PVC lifecycle is independent
of pod lifecycle. Secrets and configmaps are per-pod (created and
deleted with the pod). PVCs persist across pod restarts and are only
deleted when the control PVC is deleted.

| Resource   | Created when    | Deleted when         | Label tracks |
|------------|-----------------|----------------------|--------------|
| Secret     | Pod scheduled   | Pod deleted          | source-pod   |
| ConfigMap  | Pod scheduled   | Pod deleted          | source-pod   |
| PVC        | Pod scheduled   | Control PVC deleted  | source-pvc   |

## Failure modes

| Scenario | Behavior |
|----------|----------|
| Pod references non-catapult PVC | Pod set to Failed, reason `InvalidPVCStorageClass` |
| Pod references mix of catapult and non-catapult PVCs | Pod fails (all PVCs validated before any are synced) |
| Execution PVC provisioning fails (no space) | Worker pod stuck in Pending (PVC unbound), status synced to control |
| GPU cluster unreachable during PVC creation | Pod set to Failed, `InvalidPVCStorageClass` reason, retried on next informer event |
| Control PVC deleted while pod runs | PVC informer triggers cleanup, worker pod loses volume and fails |
| Multiple pods share same catapult PVC | First pod creates execution PVC, subsequent pods find it existing |
| Execution PVC deleted independently | Worker pods using it fail, no automatic re-creation |
| PVC name collision across namespaces | Handled by namespace prefix: `ns-a--pvc` vs `ns-b--pvc` |

## Future work: proxy PV provisioner

If Pending control PVCs cause compatibility issues, a custom external
provisioner can make them show as Bound:

```
StorageClass "catapult"
  provisioner: catapult.redhat.com/catapult

                            +-----------------------------------+
PVC created --------------> | Catapult Provisioner (sidecar)     |
                            |  1. Create execution PVC           |
                            |  2. Wait for execution Bound       |
                            |  3. Create virtual PV:             |
                            |     - hostPath: /dev/null          |
                            |     - nodeAffinity: vk-node        |
                            |  4. Control PVC binds -> Bound     |
                            +-----------------------------------+
```

This provides:
- Control PVC shows Bound (clean UX in `oc get pvc` and dashboards)
- Provisioning errors surfaced as PVC events
- Compatible with any controller that checks PVC state

The virtual PV never needs to be mounted because Catapult intercepts
the pod before any kubelet volume logic runs. The VK node has no real
CSI driver, so the PV is metadata-only.

This is the approach used by Liqo (`storageprovisioner` package). It
requires an external provisioner binary, a proxy CSI driver, and PV
lifecycle management. Out of scope for the spike.

## Reference: how other systems handle this

| System | PVC handling |
|--------|-------------|
| Virtual Kubelet (core) | Ignored by most providers (ACI rejects PVCs) |
| Liqo | Twin PVCs with StorageClass remapping + virtual PV |
| Admiralty | Defers to target cluster scheduler, no PVC sync |
| Tensile-kube | WaitForFirstConsumer only, skips VolumeBindCheck |
| Karmada | Propagates PVC manifests, no data replication |

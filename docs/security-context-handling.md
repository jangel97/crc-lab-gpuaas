# SecurityContext Handling in Catapult

## The Problem

OpenShift's SCC (Security Context Constraints) admission controller mutates
pod SecurityContext fields before the pod is persisted to etcd. By the time
Catapult's informer sees the pod, user-submitted values and SCC-generated
defaults are indistinguishable — the API server does not record which fields
were user-set vs injected.

This document classifies every SecurityContext field by its SCC mutation
behavior and documents what Catapult does with each one.

## Why we can't reconstruct the original

| Potential source | Available? | Why not |
|-----------------|-----------|---------|
| `managedFields` | No | SCC mutations are attributed to the API server's field manager, not a separate "SCC" manager |
| Annotations | No | `openshift.io/scc` records which SCC was selected, but not what it changed |
| Audit logs | Write-only | `requestObject` captures pre-admission spec, but audit logs aren't queryable at runtime |
| Mutating webhook | No | Built-in admission (SCC) runs **before** external webhooks — webhook sees post-mutation pod |

## SCC mutation pattern

For most fields, SCC follows a "fill if nil" pattern:

```go
if container.SecurityContext.RunAsUser == nil {
    uid := strategy.Generate(pod, container)
    container.SecurityContext.RunAsUser = uid
}
```

**If the user explicitly sets a field, SCC will not overwrite it** (exception:
capabilities, which are always merged). However, after admission we cannot
tell whether a present value was user-set or SCC-filled.

## Field Classification

### Category 1: Definitely user-set (SCC only validates, never mutates)

These fields are safe to preserve — if a value is present, the user set it.

| Field | Level | SCC behavior | Catapult action |
|-------|-------|-------------|-----------------|
| `privileged` | Container | Validated against `allowPrivilegedContainer`. Never set by SCC. | **Preserve** |
| `runAsUser` (RunAsAny strategy) | Container | Neither validated nor mutated | **Preserve** |
| `runAsUser` (MustRunAsNonRoot strategy) | Container | Validated non-zero, never generated | **Preserve** |
| `runAsGroup` | Pod + Container | Not managed by SCC (Kubernetes-native field) | **Preserve** |
| `procMount` | Container | Not managed by SCC | **Preserve** |

### Category 2: SCC-generated artifacts (strip)

These fields are almost always SCC-injected. Users rarely set them directly,
and the tenant cluster's values are wrong on the worker cluster.

| Field | Level | SCC behavior | What SCC generates | Catapult action | Risk if preserved |
|-------|-------|-------------|-------------------|-----------------|-------------------|
| `seLinuxOptions` | Pod + Container | MustRunAs: sets MCS label from namespace annotation `openshift.io/sa.scc.mcs` | `{level: "s0:c26,c10"}` | **Strip** | MCS labels are namespace-specific. Wrong labels cause permission denied on worker filesystem. |
| `supplementalGroups` | Pod | MustRunAs: sets `[ranges[0].Min]` from namespace annotation | `[1000620000]` | **Strip** | GIDs from tenant namespace range. Wrong GIDs on worker, may cause volume permission issues. |

**Risk of stripping:** If a user explicitly set `seLinuxOptions` or
`supplementalGroups` (rare), that intent is lost. The worker cluster's
SCC/PSA will apply its own defaults.

### Category 3: Ambiguous (could be user-set or SCC-generated)

These fields are commonly set by both users and SCC. Catapult preserves them
because stripping loses user intent silently, while preserving makes the
conflict visible (worker SCC validates and rejects if the values don't fit).

| Field | Level | SCC behavior | Catapult action | Risk if preserved | Risk if stripped |
|-------|-------|-------------|-----------------|-------------------|-----------------|
| `runAsUser` (MustRunAsRange) | Container | Generates `range.Min` if nil | **Preserve** | Tenant UID (e.g., `1000620000`) may be outside worker namespace's range → SCC rejects pod | User who explicitly set a UID loses it silently |
| `fsGroup` | Pod | MustRunAs: generates `range.Min` if nil | **Preserve** | Tenant GID may conflict with worker volumes | User who set fsGroup for shared volume permissions loses it |
| `runAsNonRoot` | Pod + Container | Set conditionally if nil | **Preserve** | Harmless — boolean is cluster-independent | User loses security intent |
| `allowPrivilegeEscalation` | Container | Defaults from SCC's `DefaultAllowPrivilegeEscalation` if nil | **Preserve** | Harmless — boolean | User loses security hardening intent |
| `readOnlyRootFilesystem` | Container | Set from SCC if nil and SCC requires it | **Preserve** | Harmless — boolean, may even be desired | User loses filesystem protection |
| `seccompProfile` | Pod + Container | Set from first allowed profile if nil | **Preserve** | Usually `runtime/default`, harmless on worker | User loses explicit profile selection |

**Design choice:** For ambiguous fields, Catapult favors **preserving user
intent** over avoiding SCC artifact leakage. If a preserved value is
incompatible with the worker cluster's security policy, the pod will fail
visibly (SCC/PSA rejects it) rather than silently losing the user's
configuration.

### Category 4: Capabilities (always merged by SCC)

Capabilities are the only field where SCC **always mutates**, even if the
user already set values. SCC applies two operations unconditionally:

1. **Add** everything in `defaultAddCapabilities` to `capabilities.add`
2. **Drop** everything in `requiredDropCapabilities` into `capabilities.drop`

This means the pod's capabilities list is a union of user-requested and
SCC-required capabilities. There is no way to separate them.

| Field | Level | Catapult action | Consequence |
|-------|-------|-----------------|-------------|
| `capabilities.add` | Container | **Preserve** | May include SCC's `defaultAddCapabilities` that user didn't request. Worker SCC validates. |
| `capabilities.drop` | Container | **Preserve** | May include SCC's `requiredDropCapabilities`. Dropping extra caps is harmless — only adding unwanted caps is risky. |

**Risk:** If the tenant SCC adds capabilities via `defaultAddCapabilities`
(e.g., `NET_BIND_SERVICE`), those appear in the worker pod's `add` list.
The worker SCC may reject the pod if it doesn't allow those capabilities.

## Summary

```
                        User sets field?
                       /              \
                     Yes               No
                      |                 |
              SCC validates        SCC generates default
                      |                 |
              Value in pod         Value in pod
                      |                 |
                      +--------+--------+
                               |
                    Same result in etcd
                               |
                    Catapult cannot distinguish
```

| Category | Count | Catapult behavior | Rationale |
|----------|-------|-------------------|-----------|
| Definitely user | 3–5 fields | **Pass through** | No ambiguity — safe |
| SCC artifacts | 2 fields | **Pass through** | Can't distinguish from user intent; worker SCC/PSA validates |
| Ambiguous | 6 fields | **Pass through** | User intent > SCC cleanliness; conflicts fail visibly |
| Capabilities | 1 field (2 sub-fields) | **Pass through** | Can't separate user from SCC; dropping extras is harmless |

**Design decision:** Catapult passes the entire SecurityContext through
unchanged. We cannot reliably distinguish user-set fields from SCC-injected
ones, so stripping any field risks silently losing user intent. If an
SCC-generated value (e.g., tenant MCS label in seLinuxOptions) is incompatible
with the worker cluster's security policy, the pod fails visibly — the worker
SCC/PSA rejects it — rather than Catapult silently transforming it.

## Future options

1. **Pre-admission annotation**: Have the submission layer (training operator,
   notebook controller, user tooling) annotate the pod with the original
   SecurityContext before it hits the API server. Catapult reads the
   annotation instead of the mutated spec. Requires client cooperation.

2. **SCC-aware reconstruction**: Read the `openshift.io/scc` annotation, fetch
   the SCC definition and namespace UID annotations, reconstruct what SCC
   would have generated, strip only matching values. Imperfect — cannot
   distinguish user-set values that happen to match SCC defaults.

3. **Worker-side admission policy**: Use Kyverno or a mutating webhook on the
   worker to normalize SecurityContext fields for the worker cluster's
   security policy. Decouples Catapult from SCC details.

# Istio Sidecar Support on VK-Dispatched Pods

Analysis of what it would take to run Istio sidecars on pods dispatched by
the Virtual Kubelet (Catapult) to the GPU worker cluster. The goal is to
understand blockers, requirements, and alternatives while keeping the
worker cluster independent of RHOAI and its specific OSSM version.

## Current State

KServe serverless mode works through VK with Istio sidecar injection
disabled (`sidecar.istio.io/inject: "false"`). Inference traffic reaches
worker pods via direct pod IP routing through Submariner, bypassing Istio
entirely. What is lost: mTLS between services, Istio telemetry/distributed
tracing, and traffic management (retries, circuit breaking, canary routing).

OSSM 2.x (Maistra) uses `policy: disabled` at the global mesh level, but
namespaces that are ServiceMeshMembers get automatic sidecar injection.
KServe serverless requires the namespace to join the mesh (via
ServiceMeshMember), so pods in that namespace receive an `istio-proxy`
sidecar unless explicitly opted out with `sidecar.istio.io/inject: "false"`.

**Confirmed by test:** Removing `sidecar.istio.io/inject: "false"` from the
KServe serverless InferenceService results in the pod getting an
`istio-proxy` container injected. The pod is then stuck in `Pending` on the
worker cluster (containers: `istio-proxy`, `kserve-container`,
`queue-proxy`). The annotation is required, not optional.

## Tested Failure Mode

**Status: CONFIRMED** (test: `test_istio_sidecar_injection_on_vk_pod`)

When sidecar injection is enabled on a VK-dispatched pod:

1. The Istio mutating webhook injects `istio-proxy` container and adds
   annotation `k8s.v1.cni.cncf.io/networks: v2-6-istio-cni`.
2. VK copies both to the worker pod — the CNI annotation passes through
   `transformPod` because `k8s.v1.cni.cncf.io/` is not in the
   annotation skip list (only `kubernetes.io/`, `openshift.io/`,
   `k8s.ovn.org/` are skipped).
3. On the worker, Multus reads the annotation and looks for a
   `NetworkAttachmentDefinition` named `v2-6-istio-cni`. It does not
   exist — the worker has no OSSM.
4. Pod is permanently stuck in `ContainerCreating` with
   `FailedCreatePodSandBox`. No containers ever start — not even the
   main workload container.

This is a CNI-level failure, not a sidecar crash. The pod sandbox cannot
be created at all.

## Blockers Beyond CNI

Even if the CNI issue were resolved (e.g., by stripping the annotation
or installing Istio CNI on the worker), the sidecar would not function.

### 1. Workload Identity

Two distinct authentication mechanisms are involved. Both are broken in the
cross-cluster case, but for different reasons.

#### 1a. JWT Authentication (SA Token) — CONFIRMED BROKEN

**Status: CONFIRMED** (from code: `provider.go` `filterVolumes`, lines
854-875)

The `istio-token` projected volume contains a `ServiceAccountToken` source
with `audience: istio-ca` and `expirationSeconds: 43200`. VK's
`filterVolumes` strips all projected volumes that contain any
`ServiceAccountToken` source — this is how it removes `kube-api-access`
volumes, but it also removes `istio-token`.

Without `istio-token`, `pilot-agent` cannot authenticate to istiod. The
SDS (Secret Discovery Service) call to obtain mTLS certificates requires
a valid JWT with audience `istio-ca`. The sidecar would log
`authentication handshake failed` and never become ready.

**Preserving the volume does not fix this.** Even if `filterVolumes` were
modified to keep `istio-token`, the token would be wrong:

- The worker kubelet manages projected volumes for worker pods. It would
  project a token signed by the **worker** API server's
  service-account-signing-key, for the **worker** ServiceAccount identity
  (`system:serviceaccount:{prefix}{tenant-ns}:{synced-sa}`).
- If istiod runs on the **tenant** cluster, it validates tokens via the
  tenant API server's TokenReview endpoint. A token signed by the worker
  API server would fail validation — different signing keys, different
  API servers. Sharing an Istio root CA does not help here: CA trust
  governs mTLS certificate verification (see 1b), not JWT validation.
- If istiod runs on the **worker** cluster, it would accept the token
  (correct signing key), but the SPIFFE identity derived from the token
  (`/ns/{prefix}{tenant-ns}/sa/{synced-sa}`) would not match the
  tenant-side identity (`/ns/{tenant-ns}/sa/{original-sa}`). This
  identity mismatch would cause authorization policy failures and
  incorrect mTLS certificate subjects.

**Token rotation is also broken.** The projected `istio-token` has
`expirationSeconds: 43200` (12h). On a normal kubelet, the projected
volume manager rotates it before expiry. On the worker, the kubelet
would rotate the token using the worker SA — producing a new token with
the worker identity, not the tenant identity. There is no mechanism to
have the worker kubelet project a token signed by the tenant API server.

#### 1b. mTLS Certificate Trust — NEEDS VALIDATION

istiod's CA issues short-lived mTLS certificates with SPIFFE identities
(`spiffe://{trust-domain}/ns/{namespace}/sa/{sa}`). This is independent
of JWT validation — the JWT authenticates the workload to istiod, and
istiod then issues an mTLS cert for that identity.

For cross-cluster mTLS to work, both clusters must share a root CA so
that sidecars trust each other's certificates. In upstream Istio
multi-cluster, this is done by distributing a shared `cacerts` Secret.
But sharing a root CA alone is insufficient — each sidecar must first
authenticate to istiod via JWT (blocker 1a) to receive a certificate.

**Validation needed:**
- In a primary-remote topology, does the remote cluster's istiod
  accept tokens signed by the remote API server? (Upstream Istio
  supports this via kubeconfig-based remote secret — does OSSM?)
- Can SPIFFE identities be mapped correctly when the namespace name
  differs between clusters (`{tenant-ns}` vs `{prefix}{tenant-ns}`)?

### 2. istiod Connectivity (xDS)

**Status: NEEDS VALIDATION**

`pilot-agent` connects to istiod at startup via the `discoveryAddress`
(default: `istiod.istio-system.svc:15012`). This is how sidecars receive:
- xDS configuration (listeners, routes, clusters, endpoints)
- mTLS certificates via SDS
- Configuration updates (VirtualService, DestinationRule pushes)

**If istiod is on the tenant cluster:** The sidecar on the worker would
need to resolve `istiod.istio-system.svc` to the tenant's istiod. With
Submariner, this could theoretically work if the `istiod` Service were
exported via `ServiceExport` — Submariner's Lighthouse DNS would resolve
it. However:
- The ServiceExport would need to be created in `istio-system` on the
  tenant, which is managed by OSSM/RHOAI.
- gRPC xDS streams are long-lived; Submariner tunnel disruptions would
  break them, and pilot-agent would need to reconnect (it does retry,
  but the disruption window affects traffic).
- OSSM 2.x configures `istiod` to listen only on its mesh scope
  (namespace members). A sidecar in a worker namespace that istiod
  does not know about would be rejected during authentication even if
  network connectivity were established.

**Validation needed:**
- Can Submariner export the `istiod` Service from `istio-system`?
- Does istiod accept connections from pods it does not know about (not
  in any registered namespace)?
- What is the reconnection behavior when the Submariner tunnel drops?

### 3. Certificate Lifecycle

**Status: NEEDS VALIDATION**

Istio mTLS certificates are short-lived (default 24h, configurable). The
sidecar requests new certificates via SDS, authenticating with the SA
token (blocker #1). Certificate lifecycle depends on resolving the JWT
authentication problem first — without a valid token, no certificate is
ever issued.

Even assuming JWT authentication were solved (e.g., via Istio's remote
secret mechanism in primary-remote mode), certificate issuance has its
own issues:

**Identity namespace mismatch:** istiod's CA issues certificates with
SPIFFE identities derived from the pod's namespace and SA. On the worker,
the namespace is `{prefix}{tenant-ns}`, not `{tenant-ns}`. The resulting
SPIFFE URI (`spiffe://{trust-domain}/ns/{prefix}{tenant-ns}/sa/{sa}`)
would not match authorization policies or peer authentication rules
written against tenant namespace names. OSSM 2.x scopes its mesh to
SMMR namespaces — a worker namespace not in the SMMR would be rejected.

**Certificate renewal:** Certificates default to 24h TTL. Renewal
requires a valid SA token at renewal time. Since the token is projected
by the worker kubelet with the worker identity (see blocker 1a), each
renewal would use the mismatched identity. Token rotation and cert
renewal would continue to produce certificates with the wrong SPIFFE
subject.

**Validation needed:**
- In primary-remote mode, does istiod issue certificates for namespaces
  on the remote cluster that are not in the primary's SMMR?
- Can SPIFFE identity be mapped correctly when namespace names differ?
- What happens to active mTLS connections when certificates expire and
  the renewal uses a different identity?

### 4. Service Discovery and Endpoint Visibility

**Status: PARTIALLY CONFIRMED**

VK syncs the worker pod's real PodIP back to the tenant pod's status.
Kubernetes on the tenant creates EndpointSlices from Services that select
the tenant pod, using the synced PodIP. This is how inference traffic
works today (without Istio) — confirmed by tests S9, S10, S20.

For Istio, istiod discovers endpoints by watching EndpointSlices via
the Kubernetes API. On the tenant, these EndpointSlices contain worker
PodIPs (synced by VK). istiod would push these endpoints to sidecars
via EDS (Endpoint Discovery Service). This part of the pipeline would
technically work — istiod does not verify that pods behind an
EndpointSlice are running sidecars or are even on the same cluster.

However, endpoint discovery alone is not sufficient for Istio to function:

**Downstream mTLS would fail.** When a tenant-side sidecar receives an
endpoint IP via EDS and attempts to connect, it initiates an mTLS
handshake. The remote side (worker pod) must present a valid certificate
from the same trust domain. If the worker sidecar never received a
certificate (blocker #1 + #3), the handshake fails and the connection
is refused. istiod would continue pushing the endpoint as healthy —
it does not perform liveness checks on mTLS capability.

**istiod has no visibility into remote pod state.** istiod configures
envoy sidecars based on local cluster state (pods, Services,
VirtualServices, DestinationRules). The worker pod exists only as an IP
in an EndpointSlice — istiod does not know its actual container state,
sidecar health, or certificate status. Health checking relies on envoy's
upstream health checks after connection, not on istiod's endpoint
discovery.

**What is confirmed:**
- PodIP sync from worker to tenant pod status (S2, S9)
- EndpointSlice creation with worker PodIP on tenant (S9)
- L3 routability from tenant to worker PodIPs via Submariner (S9, S10, S20)
- Direct HTTP traffic (bypassing Istio) works end-to-end (S20)

**What needs validation:**
- Does istiod correctly push worker PodIPs to sidecars via EDS, or
  does it filter endpoints that don't correspond to pods it manages?
- When mTLS is enforced (`PeerAuthentication: STRICT`), do connections
  to worker-pod endpoints fail gracefully (timeout, retry) or hang?
- Can Istio's `DestinationRule` `trafficPolicy.tls.mode: DISABLE` be
  used selectively for VK-dispatched services to skip mTLS for specific
  backends while keeping it for local services?

## OSSM 2.x Constraints

OSSM 2.x (Maistra-based, OpenShift Service Mesh) has several constraints
that affect cross-cluster Istio:

### No GA Multi-Cluster Support

OSSM 2.x does not support upstream Istio multi-cluster topologies
(multi-primary or primary-remote). Maistra significantly modifies the
upstream Istio deployment model — it uses `ServiceMeshControlPlane` CRs,
`ServiceMeshMemberRoll` for namespace scoping, and Maistra-specific
webhook configurations.

**OSSM Federation** (GA in OSSM 2.x) is a different model — it connects
two independent meshes by exporting/importing specific services. It does
not extend a single mesh across clusters. Each mesh has its own control
plane, CA, and trust domain. Cross-mesh traffic goes through federation
gateways (ingress/egress), not direct pod-to-pod mTLS.

**OSSM 3.x** (upstream Istio, tech preview as of RHOAI 2.x) is the path
to standard Istio multi-cluster. It supports primary-remote topology
where a single istiod manages sidecars across clusters. However:
- It is not GA in the current RHOAI version
- It would require deploying OSSM 3.x on the worker cluster (istiod
  remote config, east-west gateway)
- This contradicts the goal of keeping the worker cluster bare and
  RHOAI-independent

### Mesh Scoping

OSSM 2.x scopes the mesh to specific namespaces via `ServiceMeshMemberRoll`
(SMMR) or `ServiceMeshMember` (SMM). The sidecar injector webhook only
fires on namespaces that are mesh members. istiod only manages sidecars in
member namespaces.

The worker cluster's namespaces (`{prefix}{tenant-ns}`) are not members of
any mesh — they exist on a different cluster. Registering them in the
tenant's SMMR is not possible because SMMR references namespaces by name
on the same cluster.

### Control Plane Coupling

If Istio infrastructure were installed on the worker cluster, it would
create a dependency on a specific OSSM version. Since different tenants
may run different RHOAI/OSSM versions, the worker would need to be
compatible with all of them — or maintain per-tenant Istio infrastructure,
which defeats the purpose of a shared bare worker.

## Alternatives

### A. No Istio on Worker (Current Approach)

**How it works:** Disable sidecar injection on all VK-dispatched pods.
Inference traffic flows directly to worker pods via Submariner. No mTLS
between services — traffic in the Submariner IPsec tunnel is encrypted
at the tunnel level.

**What works:** Everything except per-service mTLS, Istio telemetry, and
Istio traffic management (retries, circuit breaking, canary routing).

**Tradeoffs:**
- (+) Worker remains completely bare — no Istio dependencies
- (+) No cross-cluster control plane coordination
- (+) Proven and working today (S20)
- (-) No service-level mTLS (tunnel-level encryption only)
- (-) No Istio-based observability (Kiali, Jaeger integration)
- (-) No Istio traffic management features

**Validation status:** Fully validated.

### B. OSSM Federation (Two Independent Meshes)

**How it works:** Install OSSM on the worker with its own SMCP. Configure
OSSM Federation between tenant and worker meshes. Export specific services
(e.g., inference endpoints) via federation gateways. Cross-mesh traffic
goes through ingress/egress gateways, not direct pod-to-pod.

**What it provides:** mTLS within each mesh (tenant and worker
independently). Cross-mesh service discovery and routing via federation
gateways. Each mesh has its own CA and trust domain.

**Tradeoffs:**
- (+) GA in OSSM 2.x — supported path
- (+) Each mesh is independent — no shared control plane
- (+) Avoids the JWT/identity problems of blockers #1-#3 because each
  mesh handles its own authentication internally
- (-) Installs OSSM on the worker — adds operator dependency, breaking
  the "bare worker" principle
- (-) Federation gateways add latency (extra hop per request)
- (-) Federation is service-level, not pod-level — VK-dispatched pods
  would need to be behind worker-side Services exported back to the
  tenant mesh
- (-) Different trust domains mean no direct mTLS between tenant and
  worker pods — cross-mesh traffic is encrypted at the gateway, not
  end-to-end between sidecars
- (-) Worker OSSM version must be maintained independently — multiple
  tenants with different OSSM versions would need compatibility testing
- (-) Operational complexity: two independent SMCP configurations,
  federation export/import rules per service, gateway monitoring,
  certificate management for each mesh separately

**Validation needed:**
- Does federation work when one mesh's pods are VK-dispatched from the
  other? Federation assumes pods in each mesh are managed by that mesh's
  control plane — VK-dispatched pods may not be registered with the
  worker mesh at all.
- How does the federation gateway interact with Submariner routing?
  Both provide cross-cluster connectivity — do they conflict or
  complement? Would Submariner's L3 routing interfere with federation's
  gateway-based routing?
- Can per-tenant worker namespaces (created dynamically by VK) each be
  added to the worker mesh SMMR? This would need to happen at namespace
  creation time in `ensureNamespace`.
- What is the operational cost of maintaining OSSM on the worker across
  RHOAI version upgrades on the tenant?

### C. OSSM 3.x Primary-Remote (Single Mesh, Two Clusters)

**How it works:** Deploy OSSM 3.x (upstream Istio) in primary-remote
mode. The tenant cluster runs istiod (primary). The worker cluster runs
only istio-cni and the remote configuration (east-west gateway,
cacerts secret). Sidecars on both clusters connect to the tenant's istiod.

**What it provides:** True cross-cluster mesh — direct pod-to-pod mTLS,
unified telemetry, traffic management across clusters.

**Tradeoffs:**
- (+) Full Istio feature set across clusters
- (+) Single control plane — simpler operations
- (-) OSSM 3.x is not GA — tech preview only
- (-) Requires Istio infrastructure on the worker (istio-cni, east-west
  gateway, shared root CA)
- (-) Worker depends on tenant's OSSM version — tight coupling
- (-) Multiple tenants with different OSSM versions cannot share a
  worker (each would need its own Istio remote config)
- (-) VK's `filterVolumes` must be modified to preserve `istio-token`
  projected volumes
- (-) VK's `transformPod` may need changes to handle the cross-cluster
  identity mapping

**Validation needed:**
- Can OSSM 3.x primary-remote work with Submariner as the inter-cluster
  network (instead of Istio's own east-west gateway)?
- Does `pilot-agent` on the worker correctly reconnect after Submariner
  tunnel disruptions?
- Does the remote secret mechanism (kubeconfig granting the tenant's
  istiod access to the worker API server's TokenReview) correctly
  resolve the JWT authentication problem (blocker 1a)?
- How does the SPIFFE identity namespace mismatch (`{tenant-ns}` vs
  `{prefix}{tenant-ns}`) affect authorization policies and mTLS
  certificate subjects in primary-remote mode?

### D. Reject Istio-Injected Pods in CreatePod (Defensive)

**How it works:** Add a check in `CreatePod` that detects Istio sidecar
injection (presence of a container named `istio-proxy` or the
`k8s.v1.cni.cncf.io/networks` annotation referencing `istio-cni`) and
returns `errdefs.InvalidInput` with a clear error message explaining
that Istio sidecars are not supported on VK-dispatched pods. This is
the same pattern used for DaemonSet pod rejection — the pod gets a
`ProviderCreateFailed` event visible in `kubectl describe pod`.

**What it provides:** Explicit, visible failure instead of the opaque
`FailedCreatePodSandBox` CNI error on the worker. The user sees
immediately why the pod was rejected and what to do about it (add
`sidecar.istio.io/inject: "false"`).

**Tradeoffs:**
- (+) Fails fast with a clear error — no opaque CNI failure on worker
- (+) No worker-side changes
- (+) Does not silently alter pod specs — user intent is preserved
- (+) Same rejection pattern as DaemonSet pods — consistent behavior
- (-) Introduces Istio-specific detection logic into VK (coupling to
  Istio's injection container name and CNI annotation format)
- (-) Detection heuristic may break across OSSM versions if container
  names or annotation formats change
- (-) VK retries rejected pods with exponential backoff (same as
  DaemonSets) — not harmful but generates periodic events

**Detection approach:** Check for `istio-proxy` in `spec.containers`
(the sidecar container injected by the Istio webhook). The
`sidecar.istio.io/status` annotation is also set by the webhook after
injection and could serve as a secondary signal. Checking the pod spec
directly is more reliable than checking annotations because it confirms
injection actually happened (not just that it was requested).

**Not yet implemented.** Could be added as a future hardening step if
the opaque `FailedCreatePodSandBox` failure proves to be a recurring
user issue.

## VK Code Changes Required per Alternative

| Component | A (No Istio) | B (Federation) | C (Primary-Remote) | D (Reject Injected) |
|-----------|:---:|:---:|:---:|:---:|
| `CreatePod` | none | none | none | detect `istio-proxy` → `InvalidInput` |
| `filterVolumes` | none | none | preserve `istio-token` + identity mapping | none |
| `transformPod` | none | none | namespace/SA identity mapping TBD | none |
| Worker cluster | none | OSSM operator + SMCP + federation config | istio-cni + east-west GW + remote secret | none |
| RHOAI coupling | none | OSSM version dependency | tight OSSM 3.x dependency | none |

## Validation Steps

These steps would confirm or reject the NEEDS VALIDATION assumptions above.
None require code changes — they are manual experiments.

### V1. istiod Reachability via Submariner

Export `istiod` from the tenant's `istio-system` namespace via Submariner
ServiceExport. From a pod on the worker, attempt to connect to
`istiod.istio-system.svc.clusterset.local:15012`. This validates whether
Submariner Lighthouse DNS + tunnel can provide the xDS channel.

### V2. JWT Cross-Cluster Authentication

Two sub-experiments to validate the JWT authentication boundary:

**V2a.** From a pod on the worker, present a projected token (audience
`istio-ca`, signed by the worker API server) to the tenant's istiod SDS
endpoint. Expected result: authentication failure — istiod validates
tokens via the local (tenant) API server's TokenReview, which does not
recognize tokens signed by a different cluster's key. This confirms
that shared root CA alone does not solve authentication.

**V2b.** Configure Istio's remote secret mechanism (a kubeconfig that
lets the tenant's istiod call the worker API server's TokenReview).
Retry V2a. If successful, this validates that primary-remote topology
can bridge the JWT gap — but introduces a tight coupling between the
tenant's istiod and the worker API server.

### V3. OSSM 2.x Federation Proof-of-Concept

Install OSSM 2.x on the worker with a minimal SMCP. Configure federation
between tenant and worker meshes. Export a test service on the worker.
Verify federation gateways route traffic correctly. This validates
option B feasibility independent of VK.

### V4. OSSM 3.x Primary-Remote with Submariner

Install OSSM 3.x (tech preview) on both clusters in primary-remote mode.
Use Submariner instead of Istio's east-west gateway for inter-cluster
connectivity. Deploy a sidecar-injected pod on the worker and verify
xDS, SDS, and mTLS all work. This validates option C.

### V5. Token Identity on Worker

Deploy a test pod via VK with a manually created projected volume
containing a `ServiceAccountToken` source (audience `istio-ca`).
On the worker pod, read the projected token and decode its JWT claims.
Verify:
- The `sub` field contains the worker SA identity, not the tenant SA
- The `iss` field is the worker API server, not the tenant
- The `aud` field is `istio-ca`

This confirms the identity mismatch described in blocker 1a: even if
`filterVolumes` were modified to preserve the volume, the token would
carry the wrong identity. This experiment requires temporarily
modifying `filterVolumes` to skip `istio-token` volumes.

## KServe Scenarios Validated Without Istio

The following KServe scenarios work fully without Istio sidecars:

| Scenario | Test | What works | Istio features not used |
|----------|------|------------|------------------------|
| KServe raw deployment | S19 | InferenceService → Deployment → Pod dispatched to worker GPU → inference server Running | No Istio involvement at all (raw mode bypasses Knative and OSSM) |
| KServe serverless (Knative) | S20 | InferenceService → Knative Service → Revision → Deployment → Pod with queue-proxy → dispatched to worker → Running → HTTP 200 from tenant via Submariner | mTLS, Istio telemetry, VirtualService-based routing, AuthorizationPolicy enforcement |

**What S20 specifically validates:**
- Knative revision/deployment chain creates pods correctly with VK node targeting
- `sidecar.istio.io/inject: "false"` is required — without it, mesh membership triggers automatic sidecar injection and the pod is stuck in `Pending` on the worker
- Knative queue-proxy container works on the worker without Istio
- Tenant pod gets worker PodIP via VK status sync
- Direct HTTP to worker pod IP:8080 succeeds via Submariner (HTTP 200 with RTX 5090 GPU data)
- InferenceService CR reaches operational state

**KServe features that remain untested (require Istio):**
- Istio ingress gateway routing to Knative Services (VirtualService-based)
- mTLS between client pods and inference server
- Istio AuthorizationPolicy enforcement on inference endpoints
- Istio-based request metrics (request count, latency histograms in Kiali)
- Distributed tracing via Istio sidecar span injection (Jaeger)
- Traffic splitting / canary deployment via Istio VirtualService weights

## Recommendation

Option A (no Istio on worker) is the recommended path for the current
phase. It is fully validated, keeps the worker bare, and provides the
core value proposition — GPU workload dispatch — without cross-cluster
control plane complexity.

Option D (reject Istio-injected pods) should be considered as a
hardening step. Currently, if a pod is accidentally injected with an
Istio sidecar (user sets `sidecar.istio.io/inject: "true"` on a
VK-bound pod), it fails with an opaque `FailedCreatePodSandBox` error
on the worker. Explicit rejection in `CreatePod` would fail fast with
a clear error message, consistent with the DaemonSet rejection pattern.

The Istio features lost (mTLS, telemetry, traffic management) can be
partially compensated without Istio on the worker:

- **Encryption:** Submariner's IPsec tunnel encrypts all cross-cluster
  traffic at L3. This is not per-service mTLS, but all traffic between
  clusters is encrypted.
- **Observability:** Application-level metrics (Prometheus), logging
  (via VK log proxying, S14), and custom trace headers can provide
  observability without Istio's sidecar-based telemetry.
- **Traffic management:** Kubernetes-native features (Services,
  NetworkPolicies) and Kueue-based admission control provide workload
  routing and quota management.

If Istio features become a hard requirement in the future, options B
(OSSM Federation) and C (OSSM 3.x primary-remote) each have significant
tradeoffs that need evaluation against our architecture:

- **Option B** is GA but installs OSSM on the worker, adds operational
  complexity (two independent meshes, federation gateways, per-service
  export/import), and its compatibility with VK-dispatched pods is
  unvalidated.
- **Option C** is the architecturally correct solution for a unified
  mesh but depends on OSSM 3.x reaching GA, introduces tight RHOAI
  coupling on the worker, and requires solving the JWT authentication
  problem (blocker #1) via Istio's remote secret mechanism.

Both would break the "bare worker" principle. Neither has been validated
in a VK context. Before pursuing either, run the validation steps above
to confirm or reject the assumptions about JWT authentication (V2),
xDS connectivity (V1), and identity mapping (V5).

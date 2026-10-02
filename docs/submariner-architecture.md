# Submariner + Catapult: Cross-Cluster Networking Architecture

## Context

Catapult dispatches pods from a control cluster (RHOAI) to GPU execution clusters. Batch workloads (PyTorchJob, nvidia-smi) work today because nobody needs to talk to the pod while it runs. Three workload classes require cross-cluster networking:

| Workload | What breaks | Why |
|----------|------------|-----|
| Workbenches (Notebooks) | Route → Service → Endpoint → unreachable | Tenant has no route to worker pod CIDR |
| KServe / vLLM | Inference endpoint unreachable | Same |
| Distributed training | Headless service DNS fails | Worker DNS doesn't know about tenant services |

Rather than building custom tunnel management (WireGuard, iptables, key rotation), Catapult delegates cross-cluster networking to Submariner — a CNCF project that provides L3 connectivity between Kubernetes clusters. Catapult's contract is:

```
Catapult requires cross-cluster workload connectivity
                    |
                    v
         network fabric / provider
                    |
        +-----------+-----------+
        |                       |
   Submariner              cloud networking
   (PoC target)             (possible future)
```

---

## 1. Recommended Submariner Topology

```
                     ┌─────────────────────────────────┐
                     │  Broker (runs on GPU cluster)    │
                     │  CRDs: Endpoint, Cluster         │
                     │  Namespace: submariner-k8s-broker │
                     └────────┬──────────────────────────┘
                              │ control plane only
               ┌──────────────┼──────────────────┐
               │              │                  │
  ┌────────────▼──────┐  ┌────▼────────────┐  ┌──▼──────────────┐
  │ Control cluster   │  │ GPU cluster A   │  │ GPU cluster B   │
  │ (sno-tenant)      │  │ (sno-worker)    │  │ (future)        │
  │                   │  │                 │  │                 │
  │ RHOAI             │  │ GPU Operator    │  │ GPU Operator    │
  │ Catapult VK       │  │ Kueue           │  │ Kueue           │
  │ Gateway Engine    │  │ Gateway Engine  │  │ Gateway Engine  │
  │ Route Agent       │  │ Route Agent     │  │ Route Agent     │
  │ Lighthouse Agent  │  │ Lighthouse Agent│  │ Lighthouse Agent│
  │ Lighthouse DNS    │  │ Lighthouse DNS  │  │ Lighthouse DNS  │
  │                   │  │                 │  │                 │
  │ Pod: 10.128.0.0/14│  │ Pod: 10.132.0.0/14│ Pod: 10.136.0.0/14│
  │ Svc: 172.30.0.0/16│  │ Svc: 172.31.0.0/16│ Svc: 172.32.0.0/16│
  └───────────────────┘  └─────────────────┘  └─────────────────┘
          ▲                       ▲                    ▲
          └───────────────────────┘                    │
              IPsec tunnel (data plane)                │
          └────────────────────────────────────────────┘
              IPsec tunnel (data plane)
```

**Key decisions:**

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Broker location | GPU cluster | GPU cluster is long-lived (tied to physical hardware). Control clusters get rebuilt for different RHOAI versions. If broker goes down, existing tunnels continue working. |
| Cable driver | Libreswan (IPsec) | Default, most tested on OpenShift. Encrypted by default. WireGuard is an alternative. |
| Globalnet | **OFF for PoC** | Our CIDRs don't overlap. Direct pod-to-pod with real IPs. Keeps Catapult Submariner-unaware. See §9 for production implications. |
| Gateway HA | Not possible on SNO | Single node = single gateway. Acceptable for PoC. Production multi-node clusters get active/passive HA. |
| Installation method | `subctl` for PoC | Simplest. RHACM for production. |

### Submariner Internal Addressing (Collision Analysis)

Unlike a custom WireGuard tunnel where you assign tunnel IPs manually, Submariner manages its own internal addressing. All hardcoded ranges use IANA reserved or link-local space that cannot collide with pod/service/RFC 1918 CIDRs:

| CIDR | Purpose | Configurable? | Collision risk |
|------|---------|---------------|----------------|
| `240.0.0.0/8` | Intra-cluster VXLAN VTEP (`vx-submariner`, port 4800) | No — hardcoded Go const | None — IANA Class E reserved |
| `241.0.0.0/8` | Inter-cluster VXLAN VTEP (VXLAN cable driver only) | No — hardcoded Go const | None — IANA Class E reserved |
| `169.254.254.8/29` | OVN upstream link (`ovn-k8s-sub0`) | No — hardcoded Go const | None — link-local (RFC 3927) |
| `242.0.0.0/8` | GlobalCIDR / Globalnet NAT | Yes — `--globalnet-cidr-range` | None at default — IANA reserved |
| `243.0.0.0/8` | ClusterSetIP (service VIPs) | Yes — `--clusterset-ip-cidr-range` | None at default — IANA reserved |
| Node IPs | IPsec / WireGuard tunnel endpoints | N/A — uses existing node IPs | N/A |

CGNAT (`100.64.0.0/10`) is **not needed and not recommended** for Submariner's internal addressing. The defaults use Class E reserved space (`240-243.0.0.0/8`) which is guaranteed collision-free. CGNAT would be less safe — cloud providers sometimes use `100.64.0.0/10` internally (e.g., AWS VPC endpoints, GCP Private Service Connect).

If Globalnet is enabled in production, the default GlobalCIDR (`242.0.0.0/8`) is already safe. Only override it if another system in the environment uses that reserved range (unlikely).

### Submariner on OpenShift: Verified Compatibility

| Requirement | Status |
|-------------|--------|
| OVN-Kubernetes CNI | Supported. Submariner has a dedicated OVN handler (`networkplugin-syncer`) that creates a `submariner_router` in OVN NorthDB and reuses Geneve tunnels. |
| OpenShift 4.22 | Active CI work in `openshift/release` repo. Not GA for Submariner yet. OCP 4.16-4.21 fully supported via RHACM 2.14-2.16. |
| SNO | Validated on OCP 4.18 SNO with OVN-K. No gateway HA (inherent SNO limitation). |
| OVN-K IPsec | **Do NOT enable OVN-K IPsec alongside Submariner** — IPsec tunnels conflict. (Upstream v0.22+ claims support but Red Hat product may lag.) |
| OVN-K local gateway mode | Known fix merged upstream for health-check SNAT issue. Submariner adds a `SUBMARINER-SELF-SNAT` chain automatically. |
| Privileges | Requires cluster-admin + privileged SCC for gateway, routeagent, globalnet, lighthouse-coredns service accounts. |
| Red Hat support | GA via RHACM. Not deprecated. Requires RHACM subscription (OpenShift Platform Plus). |

---

## 2. Traffic Flow Diagrams

### Flow A: Workbench (Route → Service → remote Pod)

```
User browser
    │
    ▼
OpenShift Route (control cluster)
    │ L7 (HAProxy ingress controller on control node)
    ▼
Service "notebook-svc" (control cluster)
    │ Endpoint: 10.132.0.47 (GPU pod real IP)
    ▼
Endpoint Controller (control)
    │ Pod "notebook-0" is on virtual node, PodIP = 10.132.0.47
    │ (Catapult synced PodIP from GPU pod)
    ▼
HAProxy connects to 10.132.0.47
    │
    ▼ (control node host networking)
Route Agent has programmed:
    10.132.0.0/14 → vx-submariner → gateway node
    │
    ▼
Control Gateway Engine
    │ IPsec tunnel (UDP 4500)
    ▼
GPU Gateway Engine
    │ decapsulate
    ▼
OVN-K routes to pod 10.132.0.47
    │
    ▼
Notebook pod (GPU cluster)
```

### Flow B: Distributed training (inter-pod, all on GPU cluster)

```
Training operator (control) creates:
  - PyTorchJob → master pod + worker pod(s)
  - Headless Service "training-svc"

Catapult dispatches all pods to GPU cluster.
Catapult syncs headless Service to GPU cluster.

GPU pod "worker-0" resolves:
  master-0.training-svc.vk-workloads.svc.cluster.local
    │ GPU-side CoreDNS → 10.132.0.50 (master pod IP)
    ▼
  Direct pod-to-pod within GPU cluster (no tunnel needed)
```

### Flow C: KServe inference

```
Client request
    │
    ▼
OpenShift Route / Knative Route (control)
    │ HAProxy / Kourier
    ▼
Service (control) → Endpoint: 10.132.0.60
    │
    ▼ (same tunnel path as Flow A)
    │
Inference pod (GPU cluster, running vLLM on GPU)
```

---

## 3. Catapult Changes Required

### 3.1 Sync PodIP/PodIPs in `syncStatusToTenant`

**File**: `cmd/vk-gpu-provider/provider.go:649-655`

Current status sync copies Phase, Conditions, ContainerStatuses, etc. but **NOT** PodIP/PodIPs. Without PodIP, the endpoint controller on the control cluster never creates endpoints for the virtual pod, so Services can't route to it.

```go
tenantPod.Status.PodIP = workerPod.Status.PodIP
tenantPod.Status.PodIPs = workerPod.Status.PodIPs
```

Also update the change-detection check to trigger sync when PodIP changes (not just Phase):

```go
if tenantPod.Status.Phase == workerPod.Status.Phase &&
   tenantPod.Status.PodIP == workerPod.Status.PodIP &&
   !containerStatusChanged(tenantPod, workerPod) {
    return
}
```

**This is the critical change that makes everything work.** Once PodIP is synced, the standard Kubernetes endpoint controller creates EndpointSlices pointing to the GPU pod's real IP. Submariner provides the L3 route. No Submariner-specific code in Catapult.

**Design constraint**: This code must NOT embed assumptions about globally unique Pod CIDRs. The PodIP sync is a mechanical copy of whatever IP the execution pod reports. Whether that IP is globally unique (non-overlapping CIDRs) or requires translation (Globalnet) is an infrastructure concern, not a Catapult concern. If Globalnet is needed in production, the translation layer belongs outside Catapult — either as a separate adapter or as a future Catapult extension point. The PoC validates the mechanism; the CIDR constraint belongs in deployment configuration and documentation.

### 3.2 Sync headless Services to GPU cluster (deferred)

For distributed training, the training operator creates a headless Service on the control cluster. GPU pods use GPU-side DNS, which doesn't know about control cluster Services. Catapult may need to sync headless Services.

**Do not implement yet.** First create a small multi-pod experiment (Validation §9) and determine exactly what DNS/service behavior the remotely executing pods require. Then decide whether syncing headless Services is the correct abstraction.

### 3.3 What does NOT change

- `transformPod` — no change
- PVC handling — no change
- Resource sync (secrets, configmaps, SAs) — no change
- Pod lifecycle management — no change
- No Submariner CRDs, no ServiceExport/ServiceImport, no Lighthouse API calls

**Catapult remains completely Submariner-unaware.**

---

## 4. Kubernetes Resources That Need Synchronization

| Resource | Already synced | Change needed |
|----------|---------------|---------------|
| Pod (create on GPU cluster) | Yes | No |
| Pod status (GPU → control) | Yes (partial) | **Add PodIP/PodIPs** |
| Secrets | Yes | No |
| ConfigMaps | Yes | No |
| ServiceAccounts | Yes | No |
| PVCs (catapult class) | Yes | No |
| **Headless Services** | **No** | **Deferred — validate need first (§V.9)** |
| ClusterIP Services | No | Not needed — endpoint controller handles it via PodIP |
| Routes | No | Not needed — HAProxy uses Service endpoints |
| ServiceExport/Import | No | Not needed — we use regular Services + PodIP |
| NetworkPolicies | No | Not needed for PoC — see §10 |

---

## 5. How Services/Routes Work

### Regular ClusterIP Service

RHOAI (or a user) creates a Service on the control cluster that selects a pod by label. Catapult dispatches the pod to the GPU cluster. When Catapult syncs PodIP into the virtual pod's status, the standard Kubernetes endpoint controller on the control cluster creates an EndpointSlice pointing to the GPU pod's real IP (e.g., `10.132.0.47`).

Submariner's Route Agent on the control cluster has programmed routing rules for the GPU pod CIDR (`10.132.0.0/14`) through the gateway tunnel. Traffic to `10.132.0.47` exits the OVN overlay on the control cluster, goes through the host networking stack, matches the Submariner route, enters the IPsec tunnel, arrives at the GPU gateway, and is routed to the pod by the GPU cluster's OVN overlay.

**No ServiceExport/ServiceImport needed.** The regular Service works because the endpoint IP is routable.

### OpenShift Route

An OpenShift Route targets a Service. The HAProxy ingress controller resolves the Service to its EndpointSlice IPs and connects directly. Since HAProxy runs on the control node where Submariner's Route Agent has programmed the routes, connections to GPU pod IPs traverse the tunnel transparently.

```
Route → HAProxy (control node) → 10.132.0.47 (via Submariner tunnel) → GPU pod
```

**This works cleanly without any Submariner awareness in the Route, Service, or Catapult.**

### Can a normal Service have an EndpointSlice pointing to a remote IP?

**Yes.** The Kubernetes EndpointSlice controller doesn't validate that endpoint IPs are reachable — it trusts the pod's reported PodIP. As long as the IP is routable at the network layer (Submariner provides this), traffic flows normally. This is how Catapult + Submariner work together without either being aware of the other.

---

## 6. How Workbenches Work

RHOAI Workbenches create: StatefulSet → Pod → Service → Route.

**Flow with Catapult + Submariner:**

1. RHOAI creates StatefulSet with `nodeName: gpu-worker` (or scheduler assigns to virtual node)
2. Catapult intercepts, creates GPU pod on execution cluster
3. GPU pod starts Jupyter server, gets IP `10.132.0.47`
4. Catapult syncs PodIP into virtual pod on control cluster
5. Endpoint controller creates EndpointSlice for the Workbench Service: `10.132.0.47:8888`
6. OpenShift Route → HAProxy → `10.132.0.47:8888` via Submariner tunnel
7. User opens browser → Jupyter notebook loads

**RHOAI does not know the workload is remote.** It sees:
- Pod in Running state with a PodIP (synced by Catapult)
- Service with healthy endpoints
- Route accessible via browser

**Considerations:**
- Jupyter uses WebSockets. OpenShift HAProxy supports WebSockets by default. The tunnel adds latency but doesn't break the protocol.
- File uploads/downloads traverse the tunnel. Latency proportional to file size + tunnel overhead.
- If the tunnel drops, the browser loses connection but the notebook kernel continues running on the GPU cluster. User reconnects when the tunnel recovers.

---

## 7. How KServe Works

KServe / InferenceService creates: Knative Service → Knative Route → pods (via Knative revision).

**This is the most architecturally complex workload.**

### Raw vLLM Deployment (PoC path)

1. Create Deployment with vLLM container + GPU resource request
2. Service selects the pod
3. Route exposes the endpoint
4. Catapult dispatches, syncs PodIP, Route works via tunnel

This path works identically to Workbenches and avoids Knative complexity.

### Knative-managed KServe (future investigation)

**Open questions — do not hide these:**

| Concern | Issue |
|---------|-------|
| Activator | Runs on control cluster, must reach GPU pod to proxy requests during scale-up. Works via Submariner if PodIP is routable. |
| Autoscaling | Reads pod metrics. If metrics scraping expects a local pod, autoscaling may not work with remote pods. |
| Scale-to-zero | Knative scales pods to zero after idle timeout. Activator triggers scale-up. Catapult must handle pod re-creation. |
| Queue-proxy sidecar | Handles readiness and request queuing. Catapult's `transformPod` may strip it. Must preserve if present. |
| Knative Service networking | Kourier/Istio networking layer may have assumptions about pod locality. |
| Readiness/probing | Knative probes the queue-proxy. If the probe path goes through the tunnel, latency may cause false failures. |
| Controllers expecting local execution | Knative revision controller, configuration controller — may not handle virtual node semantics. |

**Recommendation**: Start with raw vLLM Deployment for PoC. Document Knative as the next investigation.

---

## 8. How Distributed Training / Headless Services Work

### The problem

PyTorchJob (or similar) creates:
- Multiple pods (master + workers)
- A headless Service for inter-pod DNS discovery

The training operator creates these on the control cluster. Catapult dispatches all pods to the GPU cluster. GPU pods use GPU-side CoreDNS, which doesn't know about the control cluster's headless Service. DNS resolution fails.

### Potential solution: Catapult syncs headless Services to GPU cluster

**NOT** Lighthouse/ServiceExport. Reasons:

1. All training pods are on the same GPU cluster. Inter-pod traffic doesn't cross the tunnel. Using Lighthouse would add unnecessary cross-cluster DNS indirection for traffic that's local to the GPU cluster.

2. Lighthouse DNS uses `clusterset.local` domain, but training frameworks expect standard `svc.cluster.local` DNS names. Changing DNS names requires training operator changes.

3. Keeping it in Catapult maintains the clean abstraction — Catapult handles all GPU-side resource creation, Submariner handles only L3 routing.

### Do not implement yet

First create a small multi-pod experiment (Validation §9) and determine exactly what DNS/service behavior the remotely executing pods require. Then decide whether syncing headless Services is actually the correct abstraction.

### When would Lighthouse/ServiceExport matter?

If training pods were spread across multiple GPU clusters (e.g., master on cluster A, workers on cluster B), Lighthouse service discovery would provide cross-cluster DNS. But this is a future multi-cluster training scenario, not the PoC target.

---

## 9. Overlapping CIDR Handling

### Current lab (CIDRs do NOT overlap)

| | Control | GPU |
|---|---|---|
| Pod CIDR | `10.128.0.0/14` | `10.132.0.0/14` |
| Service CIDR | `172.30.0.0/16` | `172.31.0.0/16` |

**No Globalnet needed.** Vanilla Submariner provides:
- Direct pod-to-pod connectivity using real IPs
- No NAT overhead
- Catapult copies real PodIP → regular Services/Endpoints work
- Simpler debugging (real IPs end-to-end)

### PoC infrastructure constraint, NOT Catapult architecture constraint

Non-overlapping CIDRs is a **deployment/infrastructure decision** for this PoC. Catapult's runtime code must NOT embed assumptions about globally unique Pod CIDRs. The PodIP sync is a mechanical copy — it doesn't interpret or validate the IP. Whether the IP is globally routable is the network fabric's concern.

### Production concern (CIDRs MAY overlap)

Default OpenShift installations use the same pod CIDR (`10.128.0.0/14`) and service CIDR (`172.30.0.0/16`). If Catapult connects to GPU clusters provisioned with defaults, CIDRs will overlap.

**Critical constraint: Globalnet cannot be enabled after initial Submariner deployment. It's a deploy-time decision.**

### With Globalnet (overlapping CIDRs) — production delta

| Aspect | Impact |
|--------|--------|
| Pod IPs | Real pod IPs are NOT routable cross-cluster. Globalnet assigns GlobalIPs (from `242.0.0.0/8` by default). |
| PodIP sync | Mechanical copy of the real PodIP would produce an unreachable IP. A translation layer between Catapult and the virtual pod status would need to map real IPs to GlobalIPs. |
| Services | ServiceExport/ServiceImport may be needed. Regular Services with endpoints pointing to non-routable IPs won't work. |
| Direct pod-to-pod | Not supported. Must go through Services. |
| Performance | All NAT on the gateway node — potential bottleneck for GPU data transfer. |

### Recommendation

```
PoC:        Non-overlapping CIDRs → No Globalnet → Catapult is Submariner-unaware
                                                    (cleanest proof of concept)

Production: Two paths:
            A) Enforce non-overlapping CIDRs across all clusters
               → Same as PoC, no Globalnet, Catapult stays unaware
               (recommended if org controls cluster provisioning)

            B) Accept overlapping CIDRs → Enable Globalnet
               → Requires a reachable-IP adapter outside current Catapult code
               → Higher complexity, NAT overhead, no direct pod-to-pod
               (production delta requiring further design)
```

---

## 10. Security and Tenant Isolation

### What Submariner provides

- **Encryption**: IPsec by default (Libreswan). WireGuard alternative. All cross-cluster traffic encrypted.
- **Authentication**: Broker access via ServiceAccount + RBAC.
- **Transport security**: IPsec IKE/ESP or WireGuard key exchange.

### What Submariner does NOT provide

- **Namespace-level isolation**: Once clusters are joined, the L3 network is flat. ALL pods in ALL namespaces can reach ALL pods in ALL namespaces across clusters. ServiceExport controls DNS discoverability but NOT network reachability.
- **Cross-cluster NetworkPolicy**: Submariner has no federated NetworkPolicy API. Standard Kubernetes NetworkPolicies apply per-cluster using `ipBlock` rules.
- **Tenant boundary enforcement**: If multiple tenants share the connected clusters, Submariner provides no isolation between them at the network level.

### Catapult's role in isolation

Catapult already provides some isolation:
- All GPU pods run in a single namespace (`vk-workloads`)
- Resources are labeled with source namespace/pod for cleanup
- GPU cluster has no RHOAI CRDs — it's a bare execution target

**For multi-tenant production**, additional measures needed:
1. NetworkPolicies on both clusters restricting cross-namespace traffic
2. Potentially separate worker namespaces per tenant (not currently implemented)
3. Consider whether Skupper (L7, per-service allow lists) is better suited for tenant isolation

---

## 11. Failure Modes

| Scenario | Impact | Recovery |
|----------|--------|----------|
| **Submariner tunnel drops** | All cross-cluster traffic fails. Services/Routes on control cluster return errors. Workbench browser disconnects. | Tunnel auto-reconnects. Existing TCP connections do NOT survive (must reconnect). Workbench notebook kernel on GPU cluster continues running — user reconnects via browser. |
| **GPU gateway node fails (SNO)** | Entire GPU cluster unreachable. No gateway HA on SNO. | Node must recover. For multi-node clusters: active/passive failover in ~15-30s (all TCP breaks). |
| **Broker goes down** | Existing tunnels and routing continue working. New clusters can't join. Service discovery updates pause. | Broker recovers → updates propagate. Data plane unaffected. |
| **GPU cluster fully down** | GPU pods gone. Catapult can't sync status. Control virtual pods stale. | GPU cluster recovers → pods restart → Catapult re-syncs. May need manual cleanup of stale virtual pods. |
| **Training job running when tunnel drops** | If single-pod: training continues on GPU cluster unaffected (no cross-cluster traffic needed). Status sync to control pauses. | Tunnel recovers → status sync resumes. |
| **Tunnel recovers but pod IPs changed** | GPU pods may have gotten new IPs after restart. Control virtual pods have stale PodIPs. Endpoints point to wrong IPs. | Catapult's GPU pod informer picks up new IPs → syncs to control → endpoint controller updates EndpointSlices. Delay depends on informer resync interval (30s). |
| **Long partition (>30min)** | Control pod leases may expire. Node controller may mark virtual node as NotReady. Pods may be evicted. | Catapult heartbeat loop resumes on reconnect. May need to re-register node. |

---

## 12. Multi-Execution-Cluster Design

### Current (single execution cluster)

```
Control cluster ←──tunnel──→ GPU cluster A
   Catapult VK: 1 worker kubeconfig
```

### Future (multiple execution clusters)

```
Control cluster ←──tunnel──→ GPU cluster A (RTX 5090)
                ←──tunnel──→ GPU cluster B (H100)
                ←──tunnel──→ GPU cluster C (MI300X)
```

**Submariner changes**: Each GPU cluster joins the broker on the control cluster. Submariner creates full-mesh tunnels automatically. Pod CIDRs must not overlap (or enable Globalnet on all clusters from day one).

**Catapult changes**:
1. Multiple worker kubeconfigs (one per GPU cluster)
2. Dispatcher logic: which cluster should a pod go to? Criteria: GPU type, available quota (Kueue), latency, cost.
3. Multiple worker namespaces (potentially one per GPU cluster, or shared)
4. Resource sync needs to target the correct cluster
5. PodIP sync works the same — each GPU cluster has a unique pod CIDR routable via Submariner

**Catapult remains Submariner-unaware** in multi-cluster. It just has multiple worker clients. Submariner provides routing to each cluster's pod CIDR.

---

## 13. What Submariner Solves vs. What Catapult Must Still Solve

| Concern | Submariner | Catapult |
|---------|-----------|---------|
| L3 pod CIDR routing | **Solves** | — |
| Encrypted tunnels | **Solves** | — |
| Gateway failover (multi-node) | **Solves** | — |
| NAT traversal (multi-cloud) | **Solves** | — |
| DNS-based service discovery | Provides (Lighthouse) | Not used in PoC — Catapult uses regular Services + PodIP |
| PodIP in virtual pod | — | **Must implement** (sync from GPU pod status) |
| Headless service sync | — | **Deferred — validate need first** |
| Pod lifecycle management | — | Already implemented |
| Resource sync (secrets, CMs, SAs, PVCs) | — | Already implemented |
| Tenant isolation / NetworkPolicy | Not provided | Must add for multi-tenant production |
| GPU scheduling / quota | — | Kueue on GPU cluster |
| Multi-cluster dispatching | — | Must add for multi-cluster |
| Knative/KServe integration | — | Must investigate separately |

---

## 14. Architectural Blockers and Assumptions

### Assumptions (must hold)

1. **Non-overlapping CIDRs for PoC.** Our lab clusters already have distinct pod and service CIDRs. This is a PoC infrastructure constraint, not a Catapult code assumption.

2. **OCP 4.22 Submariner support lands before we need production.** Active CI exists. For the PoC we can use an OCP version that's fully supported (4.18-4.21).

3. **Single gateway on SNO is acceptable.** No HA. If the node goes down, the tunnel is down. For PoC this is fine.

4. **HAProxy WebSocket support.** Required for Workbenches. OpenShift HAProxy supports WebSockets by default. Verified.

5. **OVN-K IPsec is NOT enabled.** If it is, Submariner's IPsec tunnels conflict. Verify before deploying.

### Potential blockers

| Risk | Severity | Mitigation |
|------|----------|------------|
| **Knative autoscaler can't reach worker pods for metrics** | Medium | Start with raw Deployment for inference (skip Knative). Investigate Knative metrics proxying separately. |
| **Knative queue-proxy sidecar stripped by Catapult's transformPod** | Medium | If present, preserve the queue-proxy container. Knative-specific concern — defer. |
| **OVN-K source IP not preserved (OCP 4.18-4.19.4)** | Low | Use OCP 4.19.5+ or 4.20+. Upstream fix exists. |
| **Globalnet required for production** | Medium | If org can't enforce non-overlapping CIDRs, requires a reachable-IP adapter layer. Design it but don't build it for PoC. |
| **Gateway bandwidth bottleneck for large model transfers** | Low | Single gateway handles all cross-cluster traffic. For GPU workloads with large data transfers, gateway node sizing matters. Monitor with `submariner_gateway_rx/tx_bytes`. |
| **Red Hat support requires RHACM subscription** | Info | Submariner is GA via RHACM. Upstream `subctl` works without RHACM for PoC. |

### What makes Submariner suitable

1. **L3 flat network** — exactly what Catapult needs. All pod IPs routable across clusters.
2. **OVN-K integration** — dedicated handler, reuses Geneve tunnels, no second overlay.
3. **Multi-cluster support** — scales to N execution clusters with full-mesh tunnels.
4. **Red Hat GA support** — aligned with OpenShift product lifecycle.
5. **Catapult stays unaware** — no Submariner APIs needed in Catapult code (without Globalnet).

### What could make it unsuitable

1. **Globalnet performance** — if overlapping CIDRs are unavoidable, all traffic through NAT on gateway node. Could bottleneck GPU data transfers.
2. **No namespace-level isolation** — flat L3 network. Multi-tenant needs careful NetworkPolicy.
3. **OCP 4.22 not yet GA for Submariner** — may need to run OCP 4.21 for the PoC or wait.

---

## Summary: Answers to Specific Questions

**Q1: Should Catapult continue copying real PodIP/PodIPs?**
Yes (without Globalnet). The real execution pod IP is directly routable via Submariner's tunnel. Copying it into the virtual pod makes regular Services/Endpoints/Routes work transparently.

**Q2: With Globalnet, what IP?**
The GlobalIP, not the real PodIP. This requires a translation layer (outside current Catapult code) to query `GlobalIngressIP` resources. Avoid by keeping CIDRs non-overlapping.

**Q3: Can a normal Service have EndpointSlice pointing to remote IP?**
Yes. The endpoint controller trusts the pod's PodIP. If the IP is routable (Submariner provides this), traffic flows normally via regular Services.

**Q4: Will Route → Service → remote pod work?**
Yes, cleanly. HAProxy on the control node connects to the endpoint IP. Submariner's Route Agent has programmed routes for the GPU pod CIDR. Traffic goes through the IPsec tunnel transparently.

**Q5: RHOAI Workbenches without RHOAI knowing?**
Yes. RHOAI sees a pod in Running state with a PodIP, a Service with healthy endpoints, and a reachable Route. It doesn't know the pod is remote.

**Q6: KServe/InferenceService?**
Partially. Raw Deployment-based inference works (same as Workbenches). Knative-managed inference (autoscaling, scale-to-zero, queue-proxy) requires additional investigation — start with raw vLLM Deployment for PoC.

**Q7: Distributed training — sync headless Services or use Lighthouse?**
Likely sync headless Services to the GPU cluster. All training pods are on the same GPU cluster — inter-pod traffic stays local. Lighthouse would add unnecessary cross-cluster indirection and requires `clusterset.local` DNS names that training frameworks don't use. But validate the need experimentally first.

**Q8: What objects need synchronization beyond pod status?**
PodIP/PodIPs in status (implement now). Headless Services (validate need first, then implement). Everything else (secrets, CMs, SAs, PVCs) is already synced.

**Q9: Tunnel/cluster disappears during workload?**
Workbench: browser disconnects, notebook kernel continues on GPU cluster, user reconnects when tunnel recovers. Training: single-pod continues unaffected, status sync pauses. All TCP connections break on tunnel loss.

**Q10: Multiple GPU clusters?**
Submariner handles full-mesh tunnels automatically. Catapult needs multiple worker kubeconfigs and dispatch logic. CIDRs must not overlap (or Globalnet on all from day one). Catapult stays Submariner-unaware.

**Q11: Can Submariner remain purely infrastructure?**
**Yes, without Globalnet.** Catapult syncs PodIP, uses regular Services. Submariner provides routing. No Submariner APIs in Catapult code. With Globalnet: requires a reachable-IP adapter — a production delta, not a PoC concern.

---

## V. Experimental Validation Plan

Before implementing Catapult changes, validate every networking assumption independently of Catapult.

### V.1 Establish 3-cluster topology

```
RHOAI A ─────┐
             │
             ├── Submariner ── GPU execution cluster
             │
RHOAI B ─────┘
```

- **RHOAI A**: Control cluster with RHOAI version X (e.g., 2.16)
- **RHOAI B**: Control cluster with RHOAI version Y (e.g., 2.15)
- **GPU cluster**: Execution cluster with GPU Operator + Kueue (no RHOAI)
- All three clusters use non-overlapping Pod CIDRs
- Broker on one of the control clusters (e.g., RHOAI A)
- Submariner deployed via `subctl` with Libreswan (default)
- Lighthouse enabled for service discovery verification

**Steps:**
1. Install `subctl` on the gaming PC
2. `subctl deploy-broker` on RHOAI A
3. `subctl join` from all three clusters to the broker
4. `subctl show all` to verify connections
5. `subctl diagnose all` to check health
6. Record: `subctl show connections`, `subctl show endpoints`, `subctl show gateways`

### V.2 Validate raw cross-cluster Pod connectivity

Before involving Catapult, prove bidirectional pod-to-pod connectivity using native PodIPs.

**Test matrix:**

| From | To | Method |
|------|----|--------|
| pod on RHOAI A | pod on GPU cluster | `curl <gpu-pod-ip>:8080` |
| pod on GPU cluster | pod on RHOAI A | `curl <rhoai-a-pod-ip>:8080` |
| pod on RHOAI B | pod on GPU cluster | `curl <gpu-pod-ip>:8080` |
| pod on GPU cluster | pod on RHOAI B | `curl <rhoai-b-pod-ip>:8080` |

**Steps:**
1. Deploy an nginx pod on each cluster, note its PodIP
2. Deploy a curl/debug pod on each cluster
3. From each debug pod, curl the nginx pods on other clusters by PodIP
4. Verify responses come from the correct cluster

**Record:**
- Routes/interfaces Submariner creates: `ip route show table all | grep -i sub`, `ip link show`
- Verify traffic traverses Submariner: `tcpdump -i <gateway-interface> udp port 4500` during a cross-cluster curl
- OVN state: `ovn-nbctl show | grep submariner`

### V.3 Validate the critical Service assumption manually

**This is the most important experiment.** Prove that a regular Kubernetes Service with an EndpointSlice pointing to a remote PodIP works via Submariner — without Catapult.

**On RHOAI A:**

1. Deploy an HTTP server pod on the GPU cluster. Note its PodIP (e.g., `10.132.0.47`).
2. On RHOAI A, create a Service with no selector:
   ```yaml
   apiVersion: v1
   kind: Service
   metadata:
     name: remote-gpu-svc
   spec:
     ports:
     - port: 80
       targetPort: 8080
   ```
3. Manually create an EndpointSlice pointing to the GPU pod IP:
   ```yaml
   apiVersion: discovery.k8s.io/v1
   kind: EndpointSlice
   metadata:
     name: remote-gpu-svc-manual
     labels:
       kubernetes.io/service-name: remote-gpu-svc
   addressType: IPv4
   endpoints:
   - addresses:
     - "10.132.0.47"
   ports:
   - port: 8080
   ```
4. From a pod on RHOAI A, curl the Service ClusterIP:
   ```
   curl http://remote-gpu-svc.<namespace>.svc.cluster.local
   ```

**Success criteria:**
```
client pod (RHOAI A)
  → ClusterIP (Service)
  → EndpointSlice (10.132.0.47)
  → Submariner tunnel
  → GPU pod
  → HTTP response received
```

5. Repeat from RHOAI B (same GPU pod, different control cluster).

**Record:** Packet path, whether OVN hairpins or goes through host, `conntrack -L` entries.

### V.4 Validate OpenShift Route

Put an OpenShift Route in front of the Service from V.3.

1. Create a Route on RHOAI A targeting `remote-gpu-svc`:
   ```yaml
   apiVersion: route.openshift.io/v1
   kind: Route
   metadata:
     name: remote-gpu-route
   spec:
     to:
       kind: Service
       name: remote-gpu-svc
     port:
       targetPort: 8080
   ```
2. Access the Route URL from outside the cluster (e.g., `curl https://remote-gpu-route-<ns>.apps.<domain>`)

**Success criteria:**
```
external client
  → OpenShift Router (HAProxy)
  → Service (ClusterIP)
  → EndpointSlice (remote PodIP)
  → Submariner tunnel
  → GPU pod
  → HTTP response received
```

**Document:**
- Whether HAProxy connects directly to the endpoint IP (expected: yes, that's how HAProxy works)
- Whether TLS passthrough vs edge termination affects the path
- Latency overhead vs a local-pod Route

3. Repeat from RHOAI B.

### V.5 Implement PodIP synchronization

**Only after V.3 and V.4 succeed.**

Implement in `cmd/vk-gpu-provider/provider.go`:

```go
tenantPod.Status.PodIP = workerPod.Status.PodIP
tenantPod.Status.PodIPs = workerPod.Status.PodIPs
```

Make this reconciliation idempotent — repeated syncs with the same PodIP are no-ops.

**Design constraints:**
- No assumptions about CIDR ranges in Catapult code
- No Submariner imports or API calls
- Mechanical copy of whatever PodIP the execution pod reports

**Then prove** that Kubernetes creates the expected EndpointSlice automatically:

1. Create a Service on the control cluster that selects a label Catapult preserves
2. Dispatch a pod via Catapult to the GPU cluster
3. Wait for Catapult to sync PodIP
4. Verify an EndpointSlice appears with the GPU pod's real IP
5. Curl the Service ClusterIP from within the control cluster
6. Verify traffic reaches the GPU pod

**Add tests** for PodIP sync:
- Unit: `syncStatusToTenant` copies PodIP/PodIPs
- Unit: change detection triggers on PodIP change
- Integration: dispatched pod gets EndpointSlice with correct IP

### V.6 Prove isolation with both control clusters simultaneously

Run workloads from RHOAI A and RHOAI B on the same GPU execution cluster.

**Test cases:**

| Scenario | Expected |
|----------|----------|
| Service A on RHOAI A, pod A on GPU cluster | Service A → only pod A |
| Service B on RHOAI B, pod B on GPU cluster | Service B → only pod B |
| RHOAI A namespace `training` has secret `creds` | GPU cluster: `training--creds` (or namespaced), no collision with RHOAI B's `training/creds` |
| Same pod name `worker-0` from both A and B | GPU cluster: `training-a--worker-0` vs `training-b--worker-0` (namespace prefix) |
| Same PVC name `checkpoint` from both A and B | GPU cluster: `training-a--checkpoint` vs `training-b--checkpoint` |
| Pod from RHOAI A cannot reach service on RHOAI B via GPU cluster | NetworkPolicy or namespace scoping prevents cross-tenant traffic |

### V.7 Workbench

Once Service/Route behavior is proven (V.3, V.4, V.5), test a real RHOAI Workbench.

**Success criteria:** RHOAI creates its normal resources (StatefulSet, Pod, Service, Route) and the existing Route/Service reaches the remotely executed pod without any RHOAI-specific Catapult networking logic.

**Steps:**
1. Create a Workbench in RHOAI A via the dashboard
2. RHOAI creates the pod → Catapult dispatches to GPU cluster
3. Catapult syncs PodIP
4. Open the Workbench URL in a browser
5. Verify Jupyter loads and is functional (create a cell, run code, upload a file)
6. Verify WebSocket connection is stable

**Record:** RHOAI resource creation sequence, any timeouts, browser console errors.

### V.8 Inference

Start with raw vLLM Deployment (not Knative-managed).

**Steps:**
1. Create a Deployment with a vLLM container requesting `nvidia.com/gpu: 1`
2. Create a Service + Route on the control cluster
3. Catapult dispatches, syncs PodIP
4. Send inference requests via the Route
5. Verify GPU-accelerated inference responses

**Do not hide the KServe/Knative issue.** Document explicitly as the next investigation:

| Concern | Status | Action |
|---------|--------|--------|
| Activator | Unknown | Test whether activator can reach remote pod via Submariner |
| Autoscaling | Unknown | Test whether metrics scraping works for remote pods |
| Scale-to-zero | Unknown | Test whether Catapult handles pod re-creation on scale-up |
| Queue-proxy | Unknown | Check if `transformPod` preserves the sidecar |
| Knative Service networking | Unknown | Test Kourier/Istio behavior with remote endpoints |
| Readiness/probing | Unknown | Test probe latency through tunnel |
| Controllers expecting local execution | Unknown | Verify revision/configuration controllers handle virtual node |

### V.9 Headless Services

**Do not implement Service synchronization yet.**

First create a small multi-pod experiment:

1. Deploy 2 pods on the GPU cluster manually (not via Catapult)
2. Create a headless Service on the GPU cluster selecting those pods
3. From pod A, resolve pod B via DNS: `pod-b.<svc>.<ns>.svc.cluster.local`
4. Verify inter-pod connectivity via the headless Service

Then determine:
- What DNS names do training frameworks actually query?
- Does `hostname` / `subdomain` on the pod spec matter?
- Does the PyTorchJob training operator set `hostname` on pods?
- Would the mirror Service on the GPU cluster need EndpointSlice management, or does the GPU-side endpoint controller handle it?

Then decide whether syncing headless Services is the correct abstraction, or if something else (e.g., injecting DNS config) is simpler.

### V.10 Globalnet

Skipping Globalnet is acceptable for this PoC because we deliberately use non-overlapping Pod CIDRs.

**This is a PoC infrastructure constraint, NOT a Catapult architecture constraint.**

Catapult runtime code must NOT contain:
- Hardcoded CIDR ranges
- Assertions about IP uniqueness
- Direct IP comparisons for routing decisions
- Any logic that would break if PodIPs were GlobalIPs instead of real IPs

Document Globalnet / overlapping CIDRs as a **production delta** requiring further design around reachable/global pod identity:

| PoC | Production (overlapping CIDRs) |
|-----|-------------------------------|
| Real PodIP is globally routable | Real PodIP is ambiguous cross-cluster |
| Mechanical PodIP copy works | Needs IP translation layer |
| Regular Services + EndpointSlices work | May need ServiceExport/ServiceImport |
| Catapult is Submariner-unaware | Catapult or an adapter needs GlobalIP awareness |
| No NAT overhead | Gateway NAT is a bottleneck |

---

## 15. Deployment Log (2026-10-02)

### Submariner Version

**v0.24** (release-0.24). Originally deployed v0.19.2 but hit a critical
OVN compatibility bug (see §15.2). Upgraded to v0.24 which has the fix.

### 15.1 Deployment

Fully automated via Ansible:

```bash
ansible-playbook -i inventory.yml playbooks/05-setup-submariner.yml
```

This installs `subctl`, deploys the broker on the GPU cluster, joins both
clusters, and verifies cross-cluster pod connectivity. See
`playbooks/tasks/setup-submariner.yml` for the individual steps.

**Key flags** (set in the playbook):
- `--natt=false`: Both clusters are on the same L2 network (192.168.122.x), no NAT traversal needed
- `--cable-driver libreswan`: IPsec encryption (default, most tested on OpenShift)

### 15.2 OVN Compatibility Bug (v0.19.2 only)

Submariner v0.19.2 writes to the deprecated `nexthop` column (string)
in OVN's `Logical_Router_Policy` table. OCP 4.22 ships OVN 26.03.3
(DB schema 7.18.0) which ignores the deprecated column and only reads
`nexthops` (array).

**Symptom**: IPsec tunnel connected (health check pings work) but
pod-to-pod connectivity fails. OVN trace shows Submariner reroute
policies exist in NB DB but are not compiled into SB DB flows.

**northd log**: `Logical router: ovn_cluster_router, policy uses
deprecated column "nexthop", this column is ignored. Please use
"nexthops" column instead.`

**Upstream fix**: [submariner-io/submariner#4125](https://github.com/submariner-io/submariner/issues/4124),
backported to v0.23.2+ and v0.24.1+. No backport to v0.19.x–v0.22.x.

**Resolution**: Upgraded to Submariner v0.24 which uses `nexthops`
natively.

### 15.3 Verification Results

```
Cluster "tenant"
GATEWAY      CLUSTER   REMOTE IP        NAT   CABLE DRIVER   SUBNETS                        STATUS      RTT avg.
sno-worker   gpu       192.168.122.11   no    libreswan      172.31.0.0/16, 10.132.0.0/14   connected   816µs
```

**Pod-to-pod connectivity** (both directions):
```
Control → GPU (10.132.0.137): HTTP 200 in 0.004s
GPU → Control (10.128.0.75):  HTTP 200 in 0.004s
```

**Service routing through Submariner** (validation step V.3):
```
curl http://podip-test-svc.vk-test.svc.cluster.local → HTTP 200 in 0.006s
```

Flow: Service (tenant) → EndpointSlice (10.132.0.144:80, worker pod IP)
→ OVN reroute policy → IPsec tunnel → worker pod. Regular Kubernetes
Service routing works transparently because the endpoint IP is routable
via Submariner.

**PodIP sync** (validation step V.5):
```
Tenant pod:  10.132.0.144  (synced from worker pod via Catapult)
Worker pod:  10.132.0.144  (real pod IP on sno-worker)
```

### 15.4 GPU Inference Service Test (2026-10-02)

End-to-end validation that a GPU workload on the worker is accessible via
Service and Route on the tenant through Submariner.

**What was deployed:**

A CUDA container running a Python HTTP server that calls `nvidia-smi` on
each request and returns GPU info as JSON. Dispatched via VK to the worker
GPU, accessed from the tenant via Service and Route.

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: gpu-inference-svc
  namespace: vk-test
  labels:
    app: gpu-inference
spec:
  nodeName: gpu-worker
  tolerations:
  - key: virtual-kubelet.io/provider
    operator: Exists
  containers:
  - name: server
    image: nvcr.io/nvidia/cuda:12.8.1-base-ubi9
    command:
    - python3
    - -c
    - |
      import http.server, subprocess, json
      class H(http.server.BaseHTTPRequestHandler):
          def do_GET(self):
              r = subprocess.run(['nvidia-smi', '--query-gpu=name,memory.total,utilization.gpu,temperature.gpu', '--format=csv,noheader,nounits'], capture_output=True, text=True)
              body = json.dumps({'gpu': r.stdout.strip(), 'status': 'ok'})
              self.send_response(200)
              self.send_header('Content-Type', 'application/json')
              self.end_headers()
              self.wfile.write(body.encode())
          def log_message(self, *a): pass
      http.server.HTTPServer(('', 8080), H).serve_forever()
    ports:
    - containerPort: 8080
    resources:
      limits:
        nvidia.com/gpu: "1"
---
apiVersion: v1
kind: Service
metadata:
  name: gpu-inference
  namespace: vk-test
spec:
  selector:
    app: gpu-inference
  ports:
  - port: 80
    targetPort: 8080
```

Then expose via Route: `oc expose svc gpu-inference -n vk-test`

**Results:**

```
# PodIP synced from worker to tenant
Tenant pod:  10.132.0.148  (PodIP synced by Catapult)
Worker pod:  10.132.0.148  (real pod on sno-worker)

# EndpointSlice created automatically by endpoint controller
EndpointSlice: gpu-inference-xjwx5 → 10.132.0.148:8080 (ready: true)

# Service test (from inside cluster)
$ oc run curl-test --rm -i --restart=Never --image=curlimages/curl -- \
    curl -s http://gpu-inference.vk-test.svc.cluster.local
{"gpu": "NVIDIA GeForce RTX 5090, 32607, 0, 40", "status": "ok"}

# Route test (from gaming PC)
$ curl --resolve gpu-inference-vk-test.apps.tenant.local.lab:80:192.168.122.10 \
    http://gpu-inference-vk-test.apps.tenant.local.lab
{"gpu": "NVIDIA GeForce RTX 5090, 32607, 0, 40", "status": "ok"}
```

**Traffic flow:**
```
curl → Service (tenant) → EndpointSlice (10.132.0.148)
  → Submariner route agent → IPsec tunnel (UDP 4500)
  → worker gateway → OVN → GPU pod
  → nvidia-smi on RTX 5090
  → JSON response back through tunnel
```

**What this proves:**
1. GPU inference pod dispatched via VK runs on real GPU
2. PodIP synced back to tenant virtual pod
3. Kubernetes endpoint controller creates EndpointSlice with worker PodIP
4. Service ClusterIP routes through Submariner tunnel transparently
5. OpenShift Route (HAProxy) reaches remote GPU pod via tunnel
6. No Submariner-specific code in the pod, Service, or Route

**Cleanup:**

```bash
oc delete route gpu-inference -n vk-test
oc delete svc gpu-inference -n vk-test
oc delete pod gpu-inference-svc -n vk-test
```

### 15.5 Known Issues

| Issue | Impact | Workaround |
|-------|--------|------------|
| Submariner routeagent DaemonSet schedules pod on virtual `gpu-worker` node | Pod stuck in `Init:0/1` (virtual node can't run DaemonSet pods) | Cosmetic — the real routeagent on `sno-tenant` handles routing. Toleration could be removed but doesn't affect function. |
| `subctl verify` namespace collision on SNO | Test suite creates namespaces that conflict across runs | Manual curl tests confirm connectivity. Not a functional issue. |

### 15.5 Components Deployed

| Component | Tenant (sno-tenant) | Worker (sno-worker) |
|-----------|--------------------|--------------------|
| Gateway Engine | 1 pod (192.168.122.10) | 1 pod (192.168.122.11) |
| Route Agent | 1 pod (+ 1 stuck on virtual node) | 1 pod |
| Lighthouse Agent | 1 pod | 1 pod |
| Lighthouse CoreDNS | 2 pods | 2 pods |
| Metrics Proxy | 1 pod | 1 pod |
| Operator | 1 pod | 1 pod |
| Broker | — | submariner-k8s-broker namespace |

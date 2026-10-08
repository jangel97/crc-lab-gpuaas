# Risks and Known Limitations

## Webhook-Injected Infrastructure Dependencies

**Risk level:** Architectural — inherent to the VK model, no general fix.

Pods are admitted and mutated on the tenant cluster but execute on the
worker cluster. Any mutating admission webhook that injects dependencies
assuming local infrastructure (CNI plugins, control planes, sidecar
containers, network attachments) will break because that infrastructure
does not exist where the pod physically runs.

**Confirmed instance:** OSSM sidecar injection. The Istio webhook injects
an `istio-proxy` container and a `k8s.v1.cni.cncf.io/networks` annotation.
On the worker, Multus looks for the referenced `NetworkAttachmentDefinition`,
fails, and the pod is permanently stuck in `ContainerCreating`. Even if the
CNI issue were bypassed, the sidecar itself would fail — no `istiod`, no
mTLS certificates, no xDS config push.

**Why there is no general fix:**

- **Mirror operators to the worker** — defeats the purpose of keeping the
  worker bare and independent of tenant RHOAI/OSSM versions.
- **Re-admit pods on the worker** — VK does not support this; it would be
  a fundamental change to the virtual-kubelet library and would introduce
  divergence between what the user submitted and what runs.
- **Detect and reject unsatisfiable mutations** — works per-case (e.g.,
  reject pods with Multus network annotations) but is whack-a-mole; each
  new operator requires a new check.

**Mitigation:**

- Keep the worker cluster minimal — no tenant-side operators installed.
- Opt out of injection where possible (e.g., `sidecar.istio.io/inject:
  "false"` on KServe InferenceServices).
- Test new RHOAI features through VK early to catch webhook-injected
  dependencies before they reach users.
- The existing test suite covers PyTorchJob, Notebooks, KServe raw and
  serverless — extend it as new features are added.

**Tested against:** RHOAI 2.25.8 with OSSM 2.x on OCP 4.22 / 4.18.

**RHOAI 3.x context:** RHOAI 3.x removes the OSSM dependency entirely,
and KServe serverless (Knative) is deprecated. This eliminates the
confirmed Istio instance. However, the general risk remains — future
RHOAI features or other products may introduce new mutating webhooks
with the same pattern.

See [istio-cross-cluster-analysis.md](istio-cross-cluster-analysis.md)
for the full Istio investigation and test results.

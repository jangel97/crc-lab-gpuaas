# Risks and Known Limitations

## Webhook-Injected Infrastructure Dependencies

**Architectural limitation — Cross-cluster admission and execution compatibility**

Catapult separates pod admission from execution: pods are admitted and mutated on the tenant cluster but physically execute on a shared GPU worker cluster.

This creates a compatibility boundary. Mutating webhooks may inject dependencies on tenant-local infrastructure that is unavailable on the worker. Such pods may be valid on the tenant cluster but fail during remote execution.

OSSM sidecar injection is the first confirmed instance of this problem.

There is no universal, transparent solution that preserves arbitrary tenant webhook compatibility while keeping the GPU worker independent of tenant-specific operators and infrastructure.

**Mitigation strategy:**
- Maintain an explicit contract defining supported remote workloads and dependencies.
- Keep the GPU worker independent of tenant-specific RHOAI and OSSM installations.
- Disable incompatible injections where possible.
- Detect known unsupported dependencies and fail early with actionable errors.
- Extend compatibility testing as RHOAI evolves.

**Residual risk:** New admission mutations may introduce previously unknown runtime dependencies. Successful tenant-side admission does not guarantee successful remote execution.

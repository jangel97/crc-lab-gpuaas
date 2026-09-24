"""
RHOAI workload isolation across Liqo-peered clusters.

Architecture under test
-----------------------
The GPUaaS model splits responsibility between two clusters:

  tenant cluster (RHOAI)          worker cluster (bare GPU node)
  +--------------------------+    +--------------------------+
  | RHOAI operator           |    | GPU Operator             |
  | Training operator        |    | Kueue                    |
  | KServe                   |    | Liqo agent               |
  | Liqo control plane       |    | (NO RHOAI components)    |
  +--------------------------+    +--------------------------+
          |                                  ^
          |  Liqo virtual node "worker"      |
          +---- pod offloading (ShadowPod) --+

RHOAI users interact only with the tenant. They create PyTorchJobs,
InferenceServices, or Notebooks as usual. Liqo's virtual kubelet picks up
pods targeting the virtual node and reflects them to the worker as plain
ShadowPods. The worker never sees the higher-level RHOAI CRs.

What this test validates
------------------------
1. A PyTorchJob submitted on the tenant succeeds and its pod runs on the
   worker's GPU (nvidia-smi output proves GPU access).
2. No RHOAI CRDs exist on the worker cluster. This confirms that no RHOAI
   operator was installed there. The worker is a generic GPU execution node.
3. No PyTorchJob CR is replicated to the worker. Liqo only reflects pods,
   not arbitrary custom resources.
4. The shadow pod on the worker is owned by a Liqo ShadowPod, not by the
   Kubeflow training operator. This means the worker's control plane has no
   knowledge of PyTorchJob semantics.

Expected results
----------------
- PASS: the worker sees a single pod owned by ShadowPod. The pod ran
  nvidia-smi and printed GPU info. No RHOAI CRDs or CRs exist on the
  worker. The isolation boundary is intact.
- FAIL: any RHOAI CRD appears on the worker, or a PyTorchJob CR is
  replicated, or the pod is owned by a Kubeflow controller instead of
  Liqo. This would mean the worker has RHOAI components or Liqo is
  leaking custom resources across the peering boundary.
"""

import time

import pytest
from kubernetes import client


PYTORCHJOB_GROUP = "kubeflow.org"
PYTORCHJOB_VERSION = "v1"
PYTORCHJOB_PLURAL = "pytorchjobs"

# CRDs that must NOT exist on the worker cluster.
# If any of these are present, the worker is running RHOAI components
# and the isolation boundary is broken.
RHOAI_CRDS = [
    "pytorchjobs.kubeflow.org",
    "notebooks.kubeflow.org",
    "inferenceservices.serving.kserve.io",
    "servingruntimes.serving.kserve.io",
    "rayclusters.ray.io",
    "rayjobs.ray.io",
    "trainings.training.kubeflow.org",
]


@pytest.mark.liqo
def test_pytorchjob_does_not_leak_to_worker(
    tenant_clients, worker_clients, offloaded_namespace
):
    """
    Submit a PyTorchJob on the tenant, wait for it to complete on the
    worker's GPU, then verify that no RHOAI resources leaked to the worker.

    Assertions (6 total):
      1. PyTorchJob reached Succeeded condition on tenant.
      2. Pod was scheduled to the Liqo virtual node ("worker").
      3. Pod logs contain "NVIDIA" (GPU was accessible).
      4. Worker cluster has none of the RHOAI CRDs listed in RHOAI_CRDS.
      5. No PyTorchJob CR exists on the worker in the test namespace.
      6. Shadow pod on worker is owned by ShadowPod, not PyTorchJob.
    """
    tenant_core, tenant_custom = tenant_clients
    worker_core, worker_custom = worker_clients
    ns = offloaded_namespace

    # ── Create a PyTorchJob on the tenant targeting the virtual node ──
    # Uses the small CUDA base image with nvidia-smi (no PyTorch binary needed,
    # avoids Blackwell sm_120 compat issues with older PyTorch builds).
    job_name = "isolation-test"
    pytorchjob = {
        "apiVersion": f"{PYTORCHJOB_GROUP}/{PYTORCHJOB_VERSION}",
        "kind": "PyTorchJob",
        "metadata": {"name": job_name, "namespace": ns},
        "spec": {
            "pytorchReplicaSpecs": {
                "Master": {
                    "replicas": 1,
                    "restartPolicy": "Never",
                    "template": {
                        "spec": {
                            "nodeSelector": {"liqo.io/type": "virtual-node"},
                            "tolerations": [
                                {
                                    "key": "virtual-node.liqo.io/not-allowed",
                                    "operator": "Exists",
                                    "effect": "NoExecute",
                                }
                            ],
                            "containers": [
                                {
                                    "name": "gpu",
                                    "image": "nvcr.io/nvidia/cuda:12.8.0-base-ubi9",
                                    "command": ["nvidia-smi"],
                                    "resources": {
                                        "limits": {"nvidia.com/gpu": "1"},
                                    },
                                }
                            ],
                        }
                    },
                }
            }
        },
    }
    tenant_custom.create_namespaced_custom_object(
        group=PYTORCHJOB_GROUP,
        version=PYTORCHJOB_VERSION,
        namespace=ns,
        plural=PYTORCHJOB_PLURAL,
        body=pytorchjob,
    )

    # ── Wait for the PyTorchJob to finish ──
    # 10 minute timeout accounts for image pull on cold cache.
    deadline = time.time() + 600
    terminal_condition = None
    while time.time() < deadline:
        obj = tenant_custom.get_namespaced_custom_object(
            group=PYTORCHJOB_GROUP,
            version=PYTORCHJOB_VERSION,
            namespace=ns,
            plural=PYTORCHJOB_PLURAL,
            name=job_name,
        )
        for cond in obj.get("status", {}).get("conditions", []):
            if cond["type"] in ("Succeeded", "Failed") and cond["status"] == "True":
                terminal_condition = cond["type"]
                break
        if terminal_condition:
            break
        time.sleep(10)

    # ── Assert: PyTorchJob succeeded on the tenant ──
    assert terminal_condition == "Succeeded", (
        f"PyTorchJob did not succeed on tenant. "
        f"Got condition: {terminal_condition!r}. "
        f"Check pod logs: oc logs {job_name}-master-0 -n {ns}"
    )

    # ── Assert: pod was scheduled to the virtual node ──
    pod = tenant_core.read_namespaced_pod(
        name=f"{job_name}-master-0", namespace=ns
    )
    assert pod.spec.node_name == "worker", (
        f"Pod was scheduled to {pod.spec.node_name!r}, expected 'worker' (virtual node)"
    )

    # ── Assert: pod logs show GPU output ──
    logs = tenant_core.read_namespaced_pod_log(
        name=f"{job_name}-master-0", namespace=ns
    )
    assert "NVIDIA" in logs, (
        f"Pod logs do not contain 'NVIDIA'. GPU may not have been accessible. "
        f"Logs:\n{logs}"
    )

    # ── Assert: worker cluster has no RHOAI CRDs ──
    # This is the core isolation check. The worker is a bare GPU node and
    # must not have any RHOAI operators or CRDs installed.
    worker_api_client = worker_core.api_client
    ext = client.ApiextensionsV1Api(worker_api_client)
    worker_crds = {
        crd.metadata.name for crd in ext.list_custom_resource_definition().items
    }
    leaked_crds = set(RHOAI_CRDS) & worker_crds
    assert not leaked_crds, (
        f"RHOAI CRDs found on worker cluster: {leaked_crds}. "
        f"The worker must be a plain GPU execution node with no RHOAI components."
    )

    # ── Assert: no PyTorchJob CR on the worker ──
    # Even if the CRD somehow existed, no CR should be replicated.
    try:
        worker_jobs = worker_custom.list_namespaced_custom_object(
            group=PYTORCHJOB_GROUP,
            version=PYTORCHJOB_VERSION,
            namespace=ns,
            plural=PYTORCHJOB_PLURAL,
        )
        assert len(worker_jobs.get("items", [])) == 0, (
            f"Found PyTorchJob CRs on worker in namespace {ns}. "
            f"Liqo should not replicate RHOAI custom resources."
        )
    except client.exceptions.ApiException as e:
        # 404 means the CRD doesn't exist on the worker. That's the expected case.
        assert e.status == 404, f"Unexpected API error on worker: {e}"

    # ── Assert: shadow pod on worker is owned by ShadowPod, not PyTorchJob ──
    worker_pods = worker_core.list_namespaced_pod(namespace=ns)
    for wp in worker_pods.items:
        if wp.metadata.name == f"{job_name}-master-0":
            owners = wp.metadata.owner_references or []
            owner_kinds = [o.kind for o in owners]
            assert "ShadowPod" in owner_kinds, (
                f"Worker pod {wp.metadata.name} is not owned by ShadowPod. "
                f"Owner kinds: {owner_kinds}"
            )
            assert "PyTorchJob" not in owner_kinds, (
                f"Worker pod {wp.metadata.name} is owned by PyTorchJob. "
                f"RHOAI ownership must not leak to the worker."
            )
            break
    else:
        pytest.fail(
            f"Shadow pod {job_name}-master-0 not found on worker in namespace {ns}"
        )

    # ── Cleanup ──
    tenant_custom.delete_namespaced_custom_object(
        group=PYTORCHJOB_GROUP,
        version=PYTORCHJOB_VERSION,
        namespace=ns,
        plural=PYTORCHJOB_PLURAL,
        name=job_name,
    )

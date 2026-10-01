"""
MultiKueue GPU workload dispatch across federated clusters.

Architecture under test
-----------------------
  tenant cluster (manager)         worker cluster (GPU node)
  +--------------------------+    +--------------------------+
  | Kueue (MultiKueue mode)  |    | Kueue                    |
  | AdmissionCheck           |    | GPU Operator             |
  | MultiKueueConfig         |    | ResourceFlavor           |
  | MultiKueueCluster        |    | ClusterQueue             |
  | ClusterQueue + AC ref    |    | LocalQueue               |
  | LocalQueue               |    |                          |
  +--------------------------+    +--------------------------+
          |                                  ^
          |  MultiKueue workload dispatch    |
          +----------------------------------+

Users submit pods on the tenant with a kueue.x-k8s.io/queue-name label.
Kueue creates a Workload object, the MultiKueue controller dispatches a
matching workload to the worker cluster, and the worker's Kueue admits it
and creates the pod locally. Status syncs back to the tenant.
"""

import time

import pytest
from kubernetes import client


KUEUE_GROUP = "kueue.x-k8s.io"
KUEUE_VERSION = "v1beta1"
WORKLOAD_PLURAL = "workloads"


@pytest.mark.multikueue
def test_gpu_pod_dispatched_via_multikueue(
    tenant_clients, worker_clients, test_namespace
):
    """
    Submit a GPU pod on the tenant with a Kueue queue-name label.
    Verify it gets dispatched to the worker via MultiKueue and runs
    nvidia-smi successfully.

    Assertions:
      1. Kueue Workload created on tenant within 120s.
      2. Pod on worker reaches Succeeded within 10 minutes.
      3. Worker pod logs contain "NVIDIA" (GPU accessible).
      4. Workload on tenant reaches Finished condition.
    """
    tenant_core, tenant_custom = tenant_clients
    worker_core, _ = worker_clients
    ns = test_namespace

    pod_name = "multikueue-gpu-test"

    # Clean up any leftover pod from a previous run
    try:
        tenant_core.delete_namespaced_pod(name=pod_name, namespace=ns)
        time.sleep(5)
    except client.exceptions.ApiException as e:
        if e.status != 404:
            raise

    pod = client.V1Pod(
        metadata=client.V1ObjectMeta(
            name=pod_name,
            namespace=ns,
            labels={"kueue.x-k8s.io/queue-name": "user-queue"},
        ),
        spec=client.V1PodSpec(
            restart_policy="Never",
            containers=[
                client.V1Container(
                    name="gpu",
                    image="nvcr.io/nvidia/cuda:12.8.0-base-ubi9",
                    command=["nvidia-smi"],
                    resources=client.V1ResourceRequirements(
                        limits={"nvidia.com/gpu": "1"},
                        requests={
                            "nvidia.com/gpu": "1",
                            "cpu": "1",
                            "memory": "1Gi",
                        },
                    ),
                )
            ],
        ),
    )
    tenant_core.create_namespaced_pod(namespace=ns, body=pod)

    # Wait for Kueue to create a Workload object on the tenant
    deadline = time.time() + 120
    workload_name = None
    while time.time() < deadline:
        workloads = tenant_custom.list_namespaced_custom_object(
            group=KUEUE_GROUP,
            version=KUEUE_VERSION,
            namespace=ns,
            plural=WORKLOAD_PLURAL,
        )
        for wl in workloads.get("items", []):
            if pod_name in wl["metadata"]["name"]:
                workload_name = wl["metadata"]["name"]
                break
        if workload_name:
            break
        time.sleep(5)

    assert workload_name is not None, (
        f"No Workload created on tenant for pod {pod_name} within 120s. "
        f"Check Kueue controller logs: "
        f"oc logs -n kueue-system -l control-plane=controller-manager"
    )

    # Wait for the pod to complete on the worker
    deadline = time.time() + 600
    worker_pod_phase = None
    pod_name_on_worker = None
    while time.time() < deadline:
        try:
            worker_pods = worker_core.list_namespaced_pod(namespace=ns)
            for wp in worker_pods.items:
                if wp.status.phase in ("Succeeded", "Failed"):
                    worker_pod_phase = wp.status.phase
                    pod_name_on_worker = wp.metadata.name
                    break
        except client.exceptions.ApiException:
            pass
        if worker_pod_phase:
            break
        time.sleep(10)

    assert worker_pod_phase == "Succeeded", (
        f"GPU pod did not succeed on worker. Phase: {worker_pod_phase!r}. "
        f"Check pod status on worker: "
        f"oc get pods -n {ns} --kubeconfig ~/.kube/worker"
    )

    # Pod logs on worker must show GPU output
    logs = worker_core.read_namespaced_pod_log(
        name=pod_name_on_worker, namespace=ns
    )
    assert "NVIDIA" in logs, (
        f"Worker pod logs do not contain 'NVIDIA'. GPU may not be accessible.\n"
        f"Logs:\n{logs}"
    )

    # Workload on tenant should reach Finished
    deadline = time.time() + 60
    finished = False
    while time.time() < deadline:
        wl = tenant_custom.get_namespaced_custom_object(
            group=KUEUE_GROUP,
            version=KUEUE_VERSION,
            namespace=ns,
            plural=WORKLOAD_PLURAL,
            name=workload_name,
        )
        for cond in wl.get("status", {}).get("conditions", []):
            if cond["type"] == "Finished" and cond.get("status") == "True":
                finished = True
                break
        if finished:
            break
        time.sleep(5)

    assert finished, (
        f"Workload {workload_name} did not reach Finished condition on tenant. "
        f"MultiKueue may not be syncing status back."
    )

    # Cleanup
    try:
        tenant_core.delete_namespaced_pod(name=pod_name, namespace=ns)
    except client.exceptions.ApiException:
        pass


@pytest.mark.multikueue
def test_multikueue_cluster_is_active(tenant_clients):
    """Verify MultiKueueCluster reports Active=True."""
    _, tenant_custom = tenant_clients

    mkc = tenant_custom.get_cluster_custom_object(
        group=KUEUE_GROUP,
        version=KUEUE_VERSION,
        plural="multikueueclusters",
        name="worker-cluster",
    )
    conditions = mkc.get("status", {}).get("conditions", [])
    active_cond = next(
        (c for c in conditions if c["type"] == "Active"), None
    )
    assert active_cond is not None, (
        "MultiKueueCluster 'worker-cluster' has no Active condition. "
        "Check Kueue controller logs on tenant."
    )
    assert active_cond["status"] == "True", (
        f"MultiKueueCluster 'worker-cluster' is not Active. "
        f"Reason: {active_cond.get('reason', 'unknown')}. "
        f"Message: {active_cond.get('message', 'none')}"
    )


@pytest.mark.multikueue
def test_no_rhoai_on_either_cluster(tenant_clients, worker_clients):
    """Confirm neither cluster has RHOAI CRDs installed."""
    rhoai_crds = [
        "pytorchjobs.kubeflow.org",
        "notebooks.kubeflow.org",
        "inferenceservices.serving.kserve.io",
    ]

    for name, (core, _) in [("tenant", tenant_clients), ("worker", worker_clients)]:
        ext = client.ApiextensionsV1Api(core.api_client)
        cluster_crds = {
            crd.metadata.name for crd in ext.list_custom_resource_definition().items
        }
        found = set(rhoai_crds) & cluster_crds
        assert not found, (
            f"RHOAI CRDs found on {name} cluster: {found}. "
            f"This spike should not have RHOAI installed."
        )

"""
RHOAI workload dispatch via Virtual Kubelet.

Tests that RHOAI training operator workloads (PyTorchJob) get dispatched
from the tenant cluster to the worker GPU cluster via the custom VK.
RHOAI runs only on the tenant; the worker is a bare GPU execution node
with no RHOAI CRDs.
"""

import time

import pytest
from kubernetes import client

from conftest import worker_pod_name


VK_NODE_NAME = "gpu-worker"
PYTORCHJOB_GROUP = "kubeflow.org"
PYTORCHJOB_VERSION = "v1"
PYTORCHJOB_PLURAL = "pytorchjobs"

RHOAI_CRDS = [
    "pytorchjobs.kubeflow.org",
    "notebooks.kubeflow.org",
    "inferenceservices.serving.kserve.io",
    "rayclusters.ray.io",
]


@pytest.mark.rhoai
def test_pytorchjob_via_vk(
    tenant_clients, worker_clients, test_namespace, vk_worker_namespace
):
    """
    Submit a single-node PyTorchJob on tenant targeting the VK node.
    Verify the training pod runs on the worker GPU and the PyTorchJob
    reaches Succeeded on tenant.
    """
    tenant_core, tenant_custom = tenant_clients
    worker_core, worker_custom = worker_clients
    ns = test_namespace

    job_name = "vk-gpu-training"
    master_pod_name = f"{job_name}-master-0"
    w_pod_name = worker_pod_name(ns, master_pod_name)

    # Cleanup from previous runs
    try:
        tenant_custom.delete_namespaced_custom_object(
            group=PYTORCHJOB_GROUP,
            version=PYTORCHJOB_VERSION,
            namespace=ns,
            plural=PYTORCHJOB_PLURAL,
            name=job_name,
        )
        time.sleep(5)
    except client.exceptions.ApiException as e:
        if e.status != 404:
            raise

    for core, name, target_ns in [
        (tenant_core, master_pod_name, ns),
        (worker_core, w_pod_name, vk_worker_namespace),
    ]:
        try:
            core.delete_namespaced_pod(name=name, namespace=target_ns)
            time.sleep(3)
        except client.exceptions.ApiException:
            pass

    pytorchjob = {
        "apiVersion": "kubeflow.org/v1",
        "kind": "PyTorchJob",
        "metadata": {"name": job_name, "namespace": ns},
        "spec": {
            "pytorchReplicaSpecs": {
                "Master": {
                    "replicas": 1,
                    "template": {
                        "spec": {
                            "nodeName": VK_NODE_NAME,
                            "restartPolicy": "Never",
                            "tolerations": [
                                {
                                    "key": "virtual-kubelet.io/provider",
                                    "operator": "Exists",
                                    "effect": "NoSchedule",
                                }
                            ],
                            "containers": [
                                {
                                    "name": "pytorch",
                                    "image": "nvcr.io/nvidia/cuda:12.8.0-base-ubi9",
                                    "command": ["nvidia-smi"],
                                    "resources": {
                                        "limits": {"nvidia.com/gpu": "1"},
                                        "requests": {
                                            "nvidia.com/gpu": "1",
                                            "cpu": "1",
                                            "memory": "1Gi",
                                        },
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

    # Wait for training operator to create master pod
    deadline = time.time() + 60
    pod_created = False
    while time.time() < deadline:
        try:
            tenant_core.read_namespaced_pod(name=master_pod_name, namespace=ns)
            pod_created = True
            break
        except client.exceptions.ApiException:
            pass
        time.sleep(3)

    assert pod_created, (
        f"Training operator did not create master pod {master_pod_name} within 60s"
    )

    # Wait for worker pod to appear
    deadline = time.time() + 60
    worker_pod_found = False
    while time.time() < deadline:
        try:
            worker_core.read_namespaced_pod(
                name=w_pod_name, namespace=vk_worker_namespace
            )
            worker_pod_found = True
            break
        except client.exceptions.ApiException:
            pass
        time.sleep(3)

    assert worker_pod_found, (
        f"Worker pod {w_pod_name} did not appear in {vk_worker_namespace} within 60s"
    )

    # Wait for worker pod to complete (long timeout for CUDA image pull)
    deadline = time.time() + 600
    worker_phase = None
    while time.time() < deadline:
        try:
            wp = worker_core.read_namespaced_pod(
                name=w_pod_name, namespace=vk_worker_namespace
            )
            worker_phase = wp.status.phase
            if worker_phase in ("Succeeded", "Failed"):
                break
        except client.exceptions.ApiException:
            pass
        time.sleep(10)

    assert worker_phase == "Succeeded", (
        f"Worker training pod did not succeed. Phase: {worker_phase!r}"
    )

    # Verify tenant pod status synced
    deadline = time.time() + 60
    tenant_phase = None
    while time.time() < deadline:
        tp = tenant_core.read_namespaced_pod(name=master_pod_name, namespace=ns)
        tenant_phase = tp.status.phase
        if tenant_phase == "Succeeded":
            break
        time.sleep(5)

    assert tenant_phase == "Succeeded", (
        f"Tenant pod status not synced. Phase: {tenant_phase!r}"
    )

    # Verify PyTorchJob condition (training operator should see Succeeded)
    deadline = time.time() + 30
    job_succeeded = False
    while time.time() < deadline:
        job = tenant_custom.get_namespaced_custom_object(
            group=PYTORCHJOB_GROUP,
            version=PYTORCHJOB_VERSION,
            namespace=ns,
            plural=PYTORCHJOB_PLURAL,
            name=job_name,
        )
        conditions = job.get("status", {}).get("conditions", [])
        for c in conditions:
            if c.get("type") == "Succeeded" and c.get("status") == "True":
                job_succeeded = True
                break
        if job_succeeded:
            break
        time.sleep(5)

    assert job_succeeded, (
        f"PyTorchJob {job_name} did not reach Succeeded condition"
    )

    # Cleanup
    try:
        tenant_custom.delete_namespaced_custom_object(
            group=PYTORCHJOB_GROUP,
            version=PYTORCHJOB_VERSION,
            namespace=ns,
            plural=PYTORCHJOB_PLURAL,
            name=job_name,
        )
    except client.exceptions.ApiException:
        pass


@pytest.mark.rhoai
def test_no_rhoai_crds_on_worker(worker_clients):
    """Verify the worker cluster has no RHOAI CRDs — it stays bare."""
    _, worker_custom = worker_clients

    api_ext = client.ApiextensionsV1Api(worker_custom.api_client)
    crds = api_ext.list_custom_resource_definition()
    crd_names = {crd.metadata.name for crd in crds.items}

    leaked = [name for name in RHOAI_CRDS if name in crd_names]
    assert not leaked, (
        f"RHOAI CRDs found on worker cluster (should be bare GPU node): {leaked}"
    )

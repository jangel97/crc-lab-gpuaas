"""
Multi-tenant concurrent access tests for RHOAI 3.x via VK.

Both tenants submit PyTorchJobs simultaneously to the same GPU worker.
Tests validate concurrent dispatching and worker namespace isolation.
"""

import time

import pytest
from kubernetes import client

from helpers import worker_pod_name, force_delete_pod, wait_pod_exists


VK_NODE_NAME = "gpu-worker"
VK_TOLERATIONS = [{"key": "virtual-kubelet.io/provider", "operator": "Exists"}]

PYTORCHJOB_GROUP = "kubeflow.org"
PYTORCHJOB_VERSION = "v1"
PYTORCHJOB_PLURAL = "pytorchjobs"


def wait_pod_phase(core_api, name, namespace, phases, timeout=600):
    deadline = time.time() + timeout
    phase = None
    while time.time() < deadline:
        try:
            pod = core_api.read_namespaced_pod(name=name, namespace=namespace)
            phase = pod.status.phase
            if phase in phases:
                return phase
        except client.exceptions.ApiException:
            pass
        time.sleep(5)
    return phase


def _submit_pytorchjob(tenant_custom, ns, job_name, command):
    """Submit a simple PyTorchJob targeting VK."""
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
                            "tolerations": VK_TOLERATIONS,
                            "containers": [
                                {
                                    "name": "pytorch",
                                    "image": "nvcr.io/nvidia/cuda:12.8.0-base-ubi9",
                                    "command": command,
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
        group=PYTORCHJOB_GROUP, version=PYTORCHJOB_VERSION,
        namespace=ns, plural=PYTORCHJOB_PLURAL, body=pytorchjob,
    )


def _delete_pytorchjob(tenant_custom, ns, job_name):
    try:
        tenant_custom.delete_namespaced_custom_object(
            group=PYTORCHJOB_GROUP, version=PYTORCHJOB_VERSION,
            namespace=ns, plural=PYTORCHJOB_PLURAL, name=job_name,
        )
        time.sleep(10)
    except client.exceptions.ApiException as e:
        if e.status != 404:
            raise


@pytest.mark.rhoai3_mt
def test_concurrent_pytorchjobs(
    cleanup,
    rhoai3_tenant_clients, rhoai3_tenant2_clients, rhoai3_worker_clients,
    rhoai3_test_namespace,
    rhoai3_vk_worker_namespace, rhoai3_vk_worker_namespace_t2,
):
    """
    Both tenants submit PyTorchJobs simultaneously. Both should eventually
    succeed (sequentially if only 1 GPU, but both dispatched).
    """
    t1_core, t1_custom = rhoai3_tenant_clients
    t2_core, t2_custom = rhoai3_tenant2_clients
    worker_core, _ = rhoai3_worker_clients
    ns = rhoai3_test_namespace
    w_ns_t1 = rhoai3_vk_worker_namespace
    w_ns_t2 = rhoai3_vk_worker_namespace_t2

    job_t1 = "mt-training-t1"
    job_t2 = "mt-training-t2"
    pod_t1 = f"{job_t1}-master-0"
    pod_t2 = f"{job_t2}-master-0"
    w_pod_t1 = worker_pod_name(ns, pod_t1)
    w_pod_t2 = worker_pod_name(ns, pod_t2)

    # Cleanup
    _delete_pytorchjob(t1_custom, ns, job_t1)
    _delete_pytorchjob(t2_custom, ns, job_t2)
    for core, name, target_ns in [
        (t1_core, pod_t1, ns), (worker_core, w_pod_t1, w_ns_t1),
        (t2_core, pod_t2, ns), (worker_core, w_pod_t2, w_ns_t2),
    ]:
        force_delete_pod(core, name, target_ns)

    # Submit both jobs simultaneously
    _submit_pytorchjob(t1_custom, ns, job_t1, ["nvidia-smi"])
    _submit_pytorchjob(t2_custom, ns, job_t2, ["nvidia-smi"])
    cleanup(lambda: _delete_pytorchjob(t1_custom, ns, job_t1))
    cleanup(lambda: _delete_pytorchjob(t2_custom, ns, job_t2))

    # Both should create master pods
    assert wait_pod_exists(t1_core, pod_t1, ns, timeout=60), (
        f"Tenant1 master pod {pod_t1} not created within 60s"
    )
    assert wait_pod_exists(t2_core, pod_t2, ns, timeout=60), (
        f"Tenant2 master pod {pod_t2} not created within 60s"
    )

    # Both worker pods should appear (in separate namespaces)
    assert wait_pod_exists(worker_core, w_pod_t1, w_ns_t1, timeout=60), (
        f"Tenant1 worker pod not in {w_ns_t1}"
    )
    assert wait_pod_exists(worker_core, w_pod_t2, w_ns_t2, timeout=60), (
        f"Tenant2 worker pod not in {w_ns_t2}"
    )

    # Wait for both to complete (may run sequentially due to 1 GPU)
    phase_t1 = wait_pod_phase(
        worker_core, w_pod_t1, w_ns_t1, ("Succeeded", "Failed"), timeout=600,
    )
    phase_t2 = wait_pod_phase(
        worker_core, w_pod_t2, w_ns_t2, ("Succeeded", "Failed"), timeout=600,
    )
    assert phase_t1 == "Succeeded", (
        f"Tenant1 training pod did not succeed. Phase: {phase_t1!r}"
    )
    assert phase_t2 == "Succeeded", (
        f"Tenant2 training pod did not succeed. Phase: {phase_t2!r}"
    )


@pytest.mark.rhoai3_mt
def test_worker_namespace_isolation(
    rhoai3_tenant_clients, rhoai3_tenant2_clients,
    rhoai3_vk_worker_namespace, rhoai3_vk_worker_namespace_t2,
):
    """
    Each tenant's VK instance uses a unique per-tenant worker namespace.
    Verify the namespaces are different.
    """
    w_ns_t1 = rhoai3_vk_worker_namespace
    w_ns_t2 = rhoai3_vk_worker_namespace_t2

    assert w_ns_t1 != w_ns_t2, (
        f"Both tenants share the same worker namespace {w_ns_t1!r} -- "
        f"namespace isolation is broken"
    )

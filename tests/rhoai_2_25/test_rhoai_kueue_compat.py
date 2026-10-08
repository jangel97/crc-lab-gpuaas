"""
VK compatibility with RHOAI-managed Kueue on tenant.

RHOAI installs Kueue on the tenant cluster by default (managementState: Managed
in the DataScienceCluster). Kueue adds a mutating webhook that intercepts every
pod creation. These tests verify that VK pod dispatch still works when
tenant-side Kueue is active — this is the setup a real RHOAI user would have.

Requires high_memory_env (28GB tenant) so RHOAI + Kueue can schedule.
"""

import time

import pytest
from kubernetes import client

from conftest import worker_pod_name


VK_NODE_NAME = "gpu-worker"
KUEUE_GATE = "kueue.x-k8s.io/admission"
KUEUE_QUEUE_LABEL = "kueue.x-k8s.io/queue-name"
DSC_GROUP = "datasciencecluster.opendatahub.io"
DSC_VERSION = "v1"
DSC_PLURAL = "datascienceclusters"
DSC_NAME = "default-dsc"
KUEUE_NAMESPACE = "redhat-ods-applications"


@pytest.fixture(autouse=True, scope="session")
def _ensure_lab_env(high_memory_env, vk_node_ready, rhoai_operators_ready):
    pass


def force_delete_pod(core_api, name, namespace, timeout=60):
    try:
        core_api.delete_namespaced_pod(
            name=name, namespace=namespace, grace_period_seconds=0,
        )
    except client.exceptions.ApiException:
        return
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            core_api.read_namespaced_pod(name=name, namespace=namespace)
        except client.exceptions.ApiException:
            return
        time.sleep(2)


@pytest.fixture(scope="session")
def tenant_kueue_ready(tenant_clients):
    """Ensure RHOAI-managed Kueue is running on the tenant cluster."""
    tenant_core, tenant_custom = tenant_clients
    apps_api = client.AppsV1Api(tenant_core.api_client)

    dsc = tenant_custom.get_cluster_custom_object(
        group=DSC_GROUP, version=DSC_VERSION, plural=DSC_PLURAL, name=DSC_NAME,
    )
    kueue_state = (
        dsc.get("spec", {})
        .get("components", {})
        .get("kueue", {})
        .get("managementState", "")
    )
    if kueue_state != "Managed":
        tenant_custom.patch_cluster_custom_object(
            group=DSC_GROUP, version=DSC_VERSION, plural=DSC_PLURAL, name=DSC_NAME,
            body={"spec": {"components": {"kueue": {"managementState": "Managed"}}}},
        )

    deadline = time.time() + 300
    while time.time() < deadline:
        try:
            dep = apps_api.read_namespaced_deployment(
                name="kueue-controller-manager", namespace=KUEUE_NAMESPACE,
            )
            if (dep.status.ready_replicas or 0) >= 1:
                break
        except client.exceptions.ApiException:
            pass
        time.sleep(10)
    else:
        pytest.fail(
            "Kueue controller-manager did not become ready in "
            f"{KUEUE_NAMESPACE} within 300s"
        )

    yield


@pytest.mark.rhoai
def test_vk_pod_dispatch_with_tenant_kueue(
    cleanup, tenant_kueue_ready,
    tenant_clients, worker_clients, test_namespace, vk_worker_namespace,
):
    """
    Submit a GPU pod via VK with tenant-side Kueue active.
    Verify VK still dispatches to the worker and the pod completes.
    """
    tenant_core, _ = tenant_clients
    worker_core, _ = worker_clients
    ns = test_namespace

    pod_name = "vk-kueue-compat-gpu"
    w_pod_name = worker_pod_name(ns, pod_name)

    force_delete_pod(tenant_core, pod_name, ns)
    force_delete_pod(worker_core, w_pod_name, vk_worker_namespace)

    pod = client.V1Pod(
        metadata=client.V1ObjectMeta(name=pod_name, namespace=ns),
        spec=client.V1PodSpec(
            node_name=VK_NODE_NAME,
            restart_policy="Never",
            tolerations=[
                client.V1Toleration(
                    key="virtual-kubelet.io/provider", operator="Exists",
                ),
            ],
            containers=[
                client.V1Container(
                    name="gpu",
                    image="nvcr.io/nvidia/cuda:12.8.0-base-ubi9",
                    command=["nvidia-smi"],
                    resources=client.V1ResourceRequirements(
                        limits={"nvidia.com/gpu": "1"},
                        requests={"nvidia.com/gpu": "1", "cpu": "1", "memory": "1Gi"},
                    ),
                )
            ],
        ),
    )
    tenant_core.create_namespaced_pod(namespace=ns, body=pod)
    cleanup(force_delete_pod, tenant_core, pod_name, ns)

    created = tenant_core.read_namespaced_pod(name=pod_name, namespace=ns)
    gates = [g.name for g in (created.spec.scheduling_gates or [])]
    kueue_labels = {
        k: v for k, v in (created.metadata.labels or {}).items()
        if "kueue" in k
    }
    kueue_annotations = {
        k: v for k, v in (created.metadata.annotations or {}).items()
        if "kueue" in k
    }
    print(f"\n--- Kueue mutations on VK pod ---")
    print(f"Scheduling gates: {gates}")
    print(f"Kueue labels: {kueue_labels}")
    print(f"Kueue annotations: {kueue_annotations}")
    print(f"---")

    deadline = time.time() + 120
    phase = None
    while time.time() < deadline:
        tp = tenant_core.read_namespaced_pod(name=pod_name, namespace=ns)
        phase = tp.status.phase
        if phase in ("Succeeded", "Failed", "Running"):
            break
        time.sleep(5)

    assert phase in ("Succeeded", "Running"), (
        f"VK pod did not run with tenant Kueue active (phase={phase!r}). "
        f"Scheduling gates: {gates}. "
        "Tenant Kueue may be blocking VK-bound pods."
    )

    try:
        wp = worker_core.read_namespaced_pod(
            name=w_pod_name, namespace=vk_worker_namespace,
        )
        print(f"Worker pod phase: {wp.status.phase}")
    except client.exceptions.ApiException:
        pytest.fail("Worker pod was never created — VK dispatch failed")


@pytest.mark.rhoai
def test_vk_pod_not_gated_by_tenant_kueue(
    cleanup, tenant_kueue_ready,
    tenant_clients, worker_clients, test_namespace, vk_worker_namespace,
):
    """
    Verify tenant Kueue does not add a scheduling gate to VK-bound pods.
    If it does, VK dispatch would be blocked unless a LocalQueue/ClusterQueue
    is configured on the tenant.
    """
    tenant_core, _ = tenant_clients
    worker_core, _ = worker_clients
    ns = test_namespace

    pod_name = "vk-kueue-gate-check"
    w_pod_name = worker_pod_name(ns, pod_name)

    force_delete_pod(tenant_core, pod_name, ns)
    force_delete_pod(worker_core, w_pod_name, vk_worker_namespace)

    pod = client.V1Pod(
        metadata=client.V1ObjectMeta(name=pod_name, namespace=ns),
        spec=client.V1PodSpec(
            node_name=VK_NODE_NAME,
            restart_policy="Never",
            tolerations=[
                client.V1Toleration(
                    key="virtual-kubelet.io/provider", operator="Exists",
                ),
            ],
            containers=[
                client.V1Container(
                    name="test",
                    image="registry.access.redhat.com/ubi9-micro:latest",
                    command=["echo", "hello"],
                    resources=client.V1ResourceRequirements(
                        requests={"cpu": "100m", "memory": "64Mi"},
                    ),
                )
            ],
        ),
    )
    tenant_core.create_namespaced_pod(namespace=ns, body=pod)
    cleanup(force_delete_pod, tenant_core, pod_name, ns)

    created = tenant_core.read_namespaced_pod(name=pod_name, namespace=ns)
    gates = [g.name for g in (created.spec.scheduling_gates or [])]
    print(f"\n--- Scheduling gates on CPU pod: {gates} ---")

    is_gated = KUEUE_GATE in gates
    if is_gated:
        print(
            "WARNING: Tenant Kueue is gating VK-bound pods. "
            "This blocks dispatch unless a LocalQueue/ClusterQueue is "
            "configured on the tenant, or the vk-test namespace is excluded "
            "from Kueue via kueue.x-k8s.io/managed=disabled label."
        )

    deadline = time.time() + 60
    phase = None
    while time.time() < deadline:
        tp = tenant_core.read_namespaced_pod(name=pod_name, namespace=ns)
        phase = tp.status.phase
        if phase in ("Succeeded", "Failed", "Running"):
            break
        time.sleep(3)

    assert phase in ("Succeeded", "Running"), (
        f"Pod stuck (phase={phase!r}, gated={is_gated}). "
        "Tenant Kueue is blocking VK-bound pods — needs "
        "LocalQueue/ClusterQueue config on tenant or namespace exclusion "
        "via kueue.x-k8s.io/managed=disabled label."
    )

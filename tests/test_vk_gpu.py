"""
Virtual Kubelet GPU workload dispatch across federated clusters.

Architecture under test
-----------------------
  tenant cluster                      worker cluster (GPU node)
  +-----------------------------+    +-----------------------------+
  | VK Deployment               |    | GPU Operator (RTX 5090)     |
  |   registers virtual node    |    | Kueue (local quota mgmt)    |
  |   "gpu-worker"              |    | vk-workloads namespace      |
  |                              |    |   synced secrets/cms/sas    |
  | Scheduler assigns pods to   +--->|   created pods (real)       |
  |   virtual node              |    |                             |
  | VK syncs resources + pod    |    |                             |
  | VK syncs status back        |    |                             |
  +-----------------------------+    +-----------------------------+

Users submit pods on the tenant with nodeName: gpu-worker.
VK syncs referenced resources to the worker, creates the pod in
vk-workloads namespace, and syncs status back to the tenant.
"""

import time

import pytest
from kubernetes import client

from conftest import worker_pod_name


VK_NODE_NAME = "gpu-worker"


def force_delete_pod(core_api, name, namespace, timeout=60):
    """Delete a pod with grace_period=0 and wait for it to disappear."""
    try:
        core_api.delete_namespaced_pod(
            name=name,
            namespace=namespace,
            grace_period_seconds=0,
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


@pytest.mark.vk
def test_virtual_node_exists(tenant_clients):
    """Verify virtual node gpu-worker exists with GPU capacity."""
    tenant_core, _ = tenant_clients

    node = tenant_core.read_node(name=VK_NODE_NAME)

    assert node.metadata.labels.get("type") == "virtual-kubelet", (
        f"Node {VK_NODE_NAME} missing type=virtual-kubelet label"
    )

    alloc = node.status.allocatable or {}
    assert "nvidia.com/gpu" in alloc, (
        f"Node {VK_NODE_NAME} has no nvidia.com/gpu in allocatable: {alloc}"
    )

    ready = next(
        (c for c in (node.status.conditions or []) if c.type == "Ready"), None
    )
    assert ready is not None and ready.status == "True", (
        f"Node {VK_NODE_NAME} is not Ready"
    )


@pytest.mark.vk
def test_gpu_pod_dispatched_via_vk(
    tenant_clients, worker_clients, test_namespace, vk_worker_namespace
):
    """
    Submit a GPU pod on the tenant targeting the virtual node.
    Verify it appears on the worker and runs nvidia-smi successfully.
    """
    tenant_core, _ = tenant_clients
    worker_core, _ = worker_clients
    ns = test_namespace

    pod_name = "vk-gpu-test"
    w_pod_name = worker_pod_name(ns, pod_name)

    # Cleanup from previous runs
    force_delete_pod(tenant_core, pod_name, ns)
    force_delete_pod(worker_core, w_pod_name, vk_worker_namespace)

    pod = client.V1Pod(
        metadata=client.V1ObjectMeta(
            name=pod_name,
            namespace=ns,
        ),
        spec=client.V1PodSpec(
            node_name=VK_NODE_NAME,
            restart_policy="Never",
            tolerations=[
                client.V1Toleration(
                    key="virtual-kubelet.io/provider",
                    operator="Exists",
                ),
            ],
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

    # Wait for pod to appear on worker
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
        f"Pod {w_pod_name} did not appear on worker in {vk_worker_namespace} within 60s"
    )

    # Wait for worker pod to complete
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
        f"Worker pod did not succeed. Phase: {worker_phase!r}"
    )

    # Verify tenant pod status synced
    deadline = time.time() + 60
    tenant_phase = None
    while time.time() < deadline:
        tp = tenant_core.read_namespaced_pod(name=pod_name, namespace=ns)
        tenant_phase = tp.status.phase
        if tenant_phase == "Succeeded":
            break
        time.sleep(5)

    assert tenant_phase == "Succeeded", (
        f"Tenant pod status not synced. Phase: {tenant_phase!r}"
    )

    # Verify exit code
    tp = tenant_core.read_namespaced_pod(name=pod_name, namespace=ns)
    if tp.status.container_statuses:
        cstatus = tp.status.container_statuses[0]
        if cstatus.state and cstatus.state.terminated:
            assert cstatus.state.terminated.exit_code == 0, (
                f"nvidia-smi exited with {cstatus.state.terminated.exit_code}"
            )

    # Cleanup
    try:
        tenant_core.delete_namespaced_pod(name=pod_name, namespace=ns)
    except client.exceptions.ApiException:
        pass


@pytest.mark.vk
def test_resource_sync(
    tenant_clients, worker_clients, test_namespace, vk_worker_namespace
):
    """
    Create a Secret and ConfigMap on tenant, submit a pod that references
    both, verify they get synced to the worker.
    """
    tenant_core, _ = tenant_clients
    worker_core, _ = worker_clients
    ns = test_namespace

    secret_name = "vk-test-secret"
    cm_name = "vk-test-config"
    pod_name = "vk-resource-sync-test"
    w_pod_name = worker_pod_name(ns, pod_name)

    # Create Secret on tenant
    try:
        tenant_core.delete_namespaced_secret(name=secret_name, namespace=ns)
    except client.exceptions.ApiException:
        pass
    tenant_core.create_namespaced_secret(
        namespace=ns,
        body=client.V1Secret(
            metadata=client.V1ObjectMeta(name=secret_name, namespace=ns),
            string_data={"MY_SECRET": "hello-from-tenant"},
        ),
    )

    # Create ConfigMap on tenant
    try:
        tenant_core.delete_namespaced_config_map(name=cm_name, namespace=ns)
    except client.exceptions.ApiException:
        pass
    tenant_core.create_namespaced_config_map(
        namespace=ns,
        body=client.V1ConfigMap(
            metadata=client.V1ObjectMeta(name=cm_name, namespace=ns),
            data={"config.txt": "value-from-tenant"},
        ),
    )

    # Cleanup pod from previous runs
    force_delete_pod(tenant_core, pod_name, ns)
    force_delete_pod(worker_core, w_pod_name, vk_worker_namespace)

    pod = client.V1Pod(
        metadata=client.V1ObjectMeta(name=pod_name, namespace=ns),
        spec=client.V1PodSpec(
            node_name=VK_NODE_NAME,
            restart_policy="Never",
            tolerations=[
                client.V1Toleration(
                    key="virtual-kubelet.io/provider",
                    operator="Exists",
                ),
            ],
            containers=[
                client.V1Container(
                    name="test",
                    image="registry.access.redhat.com/ubi9-micro:latest",
                    command=["sh", "-c", "echo $MY_SECRET && cat /config/config.txt"],
                    env=[
                        client.V1EnvVar(
                            name="MY_SECRET",
                            value_from=client.V1EnvVarSource(
                                secret_key_ref=client.V1SecretKeySelector(
                                    name=secret_name, key="MY_SECRET"
                                )
                            ),
                        )
                    ],
                    volume_mounts=[
                        client.V1VolumeMount(
                            name="config-vol",
                            mount_path="/config",
                            read_only=True,
                        )
                    ],
                )
            ],
            volumes=[
                client.V1Volume(
                    name="config-vol",
                    config_map=client.V1ConfigMapVolumeSource(name=cm_name),
                )
            ],
        ),
    )
    tenant_core.create_namespaced_pod(namespace=ns, body=pod)

    # Wait for resources to appear on worker
    deadline = time.time() + 30
    secret_synced = False
    cm_synced = False
    while time.time() < deadline:
        try:
            s = worker_core.read_namespaced_secret(
                name=secret_name, namespace=vk_worker_namespace
            )
            if s.metadata.labels and s.metadata.labels.get(
                "app.kubernetes.io/managed-by"
            ) == "vk-gpu-provider":
                secret_synced = True
        except client.exceptions.ApiException:
            pass

        try:
            c = worker_core.read_namespaced_config_map(
                name=cm_name, namespace=vk_worker_namespace
            )
            if c.metadata.labels and c.metadata.labels.get(
                "app.kubernetes.io/managed-by"
            ) == "vk-gpu-provider":
                cm_synced = True
        except client.exceptions.ApiException:
            pass

        if secret_synced and cm_synced:
            break
        time.sleep(3)

    assert secret_synced, (
        f"Secret {secret_name} not synced to worker namespace {vk_worker_namespace}"
    )
    assert cm_synced, (
        f"ConfigMap {cm_name} not synced to worker namespace {vk_worker_namespace}"
    )

    # Wait for pod to complete
    deadline = time.time() + 120
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
        time.sleep(5)

    assert worker_phase == "Succeeded", (
        f"Resource sync pod did not succeed. Phase: {worker_phase!r}. "
        f"The pod may have failed to read the synced secret/configmap."
    )

    # Cleanup
    try:
        tenant_core.delete_namespaced_pod(name=pod_name, namespace=ns)
    except client.exceptions.ApiException:
        pass
    try:
        tenant_core.delete_namespaced_secret(name=secret_name, namespace=ns)
    except client.exceptions.ApiException:
        pass
    try:
        tenant_core.delete_namespaced_config_map(name=cm_name, namespace=ns)
    except client.exceptions.ApiException:
        pass


@pytest.mark.vk
def test_pod_deletion_cleans_up(
    tenant_clients, worker_clients, test_namespace, vk_worker_namespace
):
    """
    Submit a pod via VK, wait for completion, delete tenant pod,
    verify worker pod and synced resources are cleaned up.
    """
    tenant_core, _ = tenant_clients
    worker_core, _ = worker_clients
    ns = test_namespace

    pod_name = "vk-cleanup-test"
    w_pod_name = worker_pod_name(ns, pod_name)
    secret_name = "vk-cleanup-secret"

    # Create a secret that the pod references
    try:
        tenant_core.delete_namespaced_secret(name=secret_name, namespace=ns)
    except client.exceptions.ApiException:
        pass
    tenant_core.create_namespaced_secret(
        namespace=ns,
        body=client.V1Secret(
            metadata=client.V1ObjectMeta(name=secret_name, namespace=ns),
            string_data={"KEY": "value"},
        ),
    )

    # Cleanup from previous runs
    force_delete_pod(tenant_core, pod_name, ns)
    force_delete_pod(worker_core, w_pod_name, vk_worker_namespace)

    pod = client.V1Pod(
        metadata=client.V1ObjectMeta(name=pod_name, namespace=ns),
        spec=client.V1PodSpec(
            node_name=VK_NODE_NAME,
            restart_policy="Never",
            tolerations=[
                client.V1Toleration(
                    key="virtual-kubelet.io/provider",
                    operator="Exists",
                ),
            ],
            containers=[
                client.V1Container(
                    name="test",
                    image="registry.access.redhat.com/ubi9-micro:latest",
                    command=["echo", "done"],
                    env=[
                        client.V1EnvVar(
                            name="KEY",
                            value_from=client.V1EnvVarSource(
                                secret_key_ref=client.V1SecretKeySelector(
                                    name=secret_name, key="KEY"
                                )
                            ),
                        )
                    ],
                )
            ],
        ),
    )
    tenant_core.create_namespaced_pod(namespace=ns, body=pod)

    # Wait for worker pod to exist
    deadline = time.time() + 60
    while time.time() < deadline:
        try:
            worker_core.read_namespaced_pod(
                name=w_pod_name, namespace=vk_worker_namespace
            )
            break
        except client.exceptions.ApiException:
            pass
        time.sleep(3)

    # Delete tenant pod (should trigger VK cleanup)
    tenant_core.delete_namespaced_pod(name=pod_name, namespace=ns)

    # Wait for worker pod to be deleted
    deadline = time.time() + 30
    worker_pod_gone = False
    while time.time() < deadline:
        try:
            worker_core.read_namespaced_pod(
                name=w_pod_name, namespace=vk_worker_namespace
            )
        except client.exceptions.ApiException as e:
            if e.status == 404:
                worker_pod_gone = True
                break
        time.sleep(3)

    assert worker_pod_gone, (
        f"Worker pod {w_pod_name} not deleted after tenant pod deletion"
    )

    # Check synced secret is cleaned up
    deadline = time.time() + 15
    secret_gone = False
    while time.time() < deadline:
        try:
            worker_core.read_namespaced_secret(
                name=secret_name, namespace=vk_worker_namespace
            )
        except client.exceptions.ApiException as e:
            if e.status == 404:
                secret_gone = True
                break
        time.sleep(3)

    assert secret_gone, (
        f"Synced secret {secret_name} not cleaned up on worker after pod deletion"
    )

    # Cleanup tenant secret
    try:
        tenant_core.delete_namespaced_secret(name=secret_name, namespace=ns)
    except client.exceptions.ApiException:
        pass

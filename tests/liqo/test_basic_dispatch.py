"""
Liqo basic dispatch tests.

Validates that Liqo can dispatch GPU pods from an offloaded namespace
to the worker cluster and sync status back. No nodeName or VK tolerations
needed — the namespace is offloaded with Remote strategy.
"""

import time

import pytest
from kubernetes import client

from helpers import force_delete_pod, wait_pod_exists


def wait_pod_phase(core_api, name, namespace, phases, timeout=300):
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


@pytest.mark.liqo
def test_virtual_node_exists(liqo_tenant_clients, liqo_virtual_node):
    """Liqo registers a virtual node with liqo.io/type=virtual-node label."""
    tenant_core, _ = liqo_tenant_clients
    node = tenant_core.read_node(name=liqo_virtual_node)

    labels = node.metadata.labels or {}
    assert labels.get("liqo.io/type") == "virtual-node"

    allocatable = node.status.allocatable or {}
    assert "nvidia.com/gpu" in allocatable, (
        f"Virtual node {liqo_virtual_node} has no nvidia.com/gpu in allocatable"
    )

    for cond in (node.status.conditions or []):
        if cond.type == "Ready":
            assert cond.status == "True", f"Virtual node not Ready: {cond.message}"
            break
    else:
        pytest.fail("No Ready condition on virtual node")


@pytest.mark.liqo
def test_gpu_pod_dispatched(
    cleanup, liqo_tenant_clients, liqo_worker_clients,
    liqo_test_namespace, liqo_worker_namespace, liqo_virtual_node_ready,
):
    """GPU pod in offloaded namespace dispatched via Liqo to worker RTX 5090."""
    tenant_core, _ = liqo_tenant_clients
    worker_core, _ = liqo_worker_clients
    ns = liqo_test_namespace
    w_ns = liqo_worker_namespace

    pod_name = "liqo-gpu-test"

    force_delete_pod(tenant_core, pod_name, ns)

    pod = client.V1Pod(
        metadata=client.V1ObjectMeta(name=pod_name, namespace=ns),
        spec=client.V1PodSpec(
            restart_policy="Never",
            containers=[
                client.V1Container(
                    name="gpu",
                    image="nvcr.io/nvidia/cuda:12.8.0-base-ubi9",
                    command=["nvidia-smi"],
                    resources=client.V1ResourceRequirements(
                        limits={"nvidia.com/gpu": "1"},
                    ),
                ),
            ],
        ),
    )

    tenant_core.create_namespaced_pod(namespace=ns, body=pod)
    cleanup(force_delete_pod, tenant_core, pod_name, ns)

    deadline = time.time() + 120
    worker_pod = None
    while time.time() < deadline:
        pods = worker_core.list_namespaced_pod(namespace=w_ns)
        for p in pods.items:
            if pod_name in p.metadata.name:
                worker_pod = p.metadata.name
                break
        if worker_pod:
            break
        time.sleep(5)

    assert worker_pod, f"No worker pod matching '{pod_name}' appeared in {w_ns}"
    cleanup(force_delete_pod, worker_core, worker_pod, w_ns)

    phase = wait_pod_phase(worker_core, worker_pod, w_ns, ["Succeeded"], timeout=300)
    assert phase == "Succeeded", f"Worker pod phase: {phase}, expected Succeeded"

    tenant_phase = wait_pod_phase(tenant_core, pod_name, ns, ["Succeeded"], timeout=60)
    assert tenant_phase == "Succeeded", (
        f"Tenant pod phase: {tenant_phase}, expected Succeeded (status sync)"
    )


@pytest.mark.liqo
def test_resource_sync(
    cleanup, liqo_tenant_clients, liqo_worker_clients,
    liqo_test_namespace, liqo_worker_namespace, liqo_virtual_node_ready,
):
    """Secret and ConfigMap reflected to worker by Liqo resource reflection."""
    tenant_core, _ = liqo_tenant_clients
    worker_core, _ = liqo_worker_clients
    ns = liqo_test_namespace
    w_ns = liqo_worker_namespace

    secret_name = "liqo-test-secret"
    cm_name = "liqo-test-cm"
    pod_name = "liqo-resource-test"

    for name in [secret_name]:
        try:
            tenant_core.delete_namespaced_secret(name=name, namespace=ns)
        except client.exceptions.ApiException:
            pass

    for name in [cm_name]:
        try:
            tenant_core.delete_namespaced_config_map(name=name, namespace=ns)
        except client.exceptions.ApiException:
            pass

    force_delete_pod(tenant_core, pod_name, ns)

    tenant_core.create_namespaced_secret(
        namespace=ns,
        body=client.V1Secret(
            metadata=client.V1ObjectMeta(name=secret_name),
            string_data={"MY_SECRET": "hello-from-liqo"},
        ),
    )
    cleanup(
        lambda: tenant_core.delete_namespaced_secret(name=secret_name, namespace=ns),
    )

    tenant_core.create_namespaced_config_map(
        namespace=ns,
        body=client.V1ConfigMap(
            metadata=client.V1ObjectMeta(name=cm_name),
            data={"config.txt": "liqo-config-value"},
        ),
    )
    cleanup(
        lambda: tenant_core.delete_namespaced_config_map(name=cm_name, namespace=ns),
    )

    pod = client.V1Pod(
        metadata=client.V1ObjectMeta(name=pod_name, namespace=ns),
        spec=client.V1PodSpec(
            restart_policy="Never",
            containers=[
                client.V1Container(
                    name="test",
                    image="registry.access.redhat.com/ubi9/ubi-minimal:latest",
                    command=["sh", "-c", "echo $MY_SECRET && cat /config/config.txt"],
                    env=[
                        client.V1EnvVar(
                            name="MY_SECRET",
                            value_from=client.V1EnvVarSource(
                                secret_key_ref=client.V1SecretKeySelector(
                                    name=secret_name, key="MY_SECRET",
                                ),
                            ),
                        ),
                    ],
                    volume_mounts=[
                        client.V1VolumeMount(
                            name="config-vol",
                            mount_path="/config",
                            read_only=True,
                        ),
                    ],
                    resources=client.V1ResourceRequirements(
                        limits={"nvidia.com/gpu": "1"},
                    ),
                ),
            ],
            volumes=[
                client.V1Volume(
                    name="config-vol",
                    config_map=client.V1ConfigMapVolumeSource(name=cm_name),
                ),
            ],
        ),
    )

    tenant_core.create_namespaced_pod(namespace=ns, body=pod)
    cleanup(force_delete_pod, tenant_core, pod_name, ns)

    deadline = time.time() + 60
    secret_found = False
    cm_found = False
    while time.time() < deadline:
        try:
            worker_core.read_namespaced_secret(name=secret_name, namespace=w_ns)
            secret_found = True
        except client.exceptions.ApiException:
            pass
        try:
            worker_core.read_namespaced_config_map(name=cm_name, namespace=w_ns)
            cm_found = True
        except client.exceptions.ApiException:
            pass
        if secret_found and cm_found:
            break
        time.sleep(5)

    assert secret_found, f"Secret {secret_name} not reflected to worker namespace {w_ns}"
    assert cm_found, f"ConfigMap {cm_name} not reflected to worker namespace {w_ns}"

    phase = wait_pod_phase(tenant_core, pod_name, ns, ["Succeeded"], timeout=300)
    assert phase == "Succeeded", f"Pod failed, resources may not have been available: {phase}"


@pytest.mark.liqo
def test_pod_deletion_cleanup(
    cleanup, liqo_tenant_clients, liqo_worker_clients,
    liqo_test_namespace, liqo_worker_namespace, liqo_virtual_node_ready,
):
    """Deleting tenant pod cleans up worker pod (ShadowPod deleted)."""
    tenant_core, _ = liqo_tenant_clients
    worker_core, _ = liqo_worker_clients
    ns = liqo_test_namespace
    w_ns = liqo_worker_namespace

    pod_name = "liqo-cleanup-test"

    force_delete_pod(tenant_core, pod_name, ns)

    pod = client.V1Pod(
        metadata=client.V1ObjectMeta(name=pod_name, namespace=ns),
        spec=client.V1PodSpec(
            restart_policy="Never",
            containers=[
                client.V1Container(
                    name="test",
                    image="registry.access.redhat.com/ubi9/ubi-minimal:latest",
                    command=["sleep", "300"],
                    resources=client.V1ResourceRequirements(
                        limits={"nvidia.com/gpu": "1"},
                    ),
                ),
            ],
        ),
    )

    tenant_core.create_namespaced_pod(namespace=ns, body=pod)

    deadline = time.time() + 120
    worker_pod = None
    while time.time() < deadline:
        pods = worker_core.list_namespaced_pod(namespace=w_ns)
        for p in pods.items:
            if pod_name in p.metadata.name:
                worker_pod = p.metadata.name
                break
        if worker_pod:
            break
        time.sleep(5)

    assert worker_pod, f"No worker pod for {pod_name} in {w_ns}"

    tenant_core.delete_namespaced_pod(name=pod_name, namespace=ns, grace_period_seconds=0)

    deadline = time.time() + 60
    deleted = False
    while time.time() < deadline:
        try:
            worker_core.read_namespaced_pod(name=worker_pod, namespace=w_ns)
        except client.exceptions.ApiException as e:
            if e.status == 404:
                deleted = True
                break
        time.sleep(3)

    assert deleted, f"Worker pod {worker_pod} not deleted after tenant pod deletion"


@pytest.mark.liqo
def test_pod_logs_proxied(
    cleanup, liqo_tenant_clients,
    liqo_test_namespace, liqo_virtual_node_ready,
):
    """kubectl logs proxied through Liqo VK to worker pod."""
    tenant_core, _ = liqo_tenant_clients
    ns = liqo_test_namespace

    pod_name = "liqo-logs-test"

    force_delete_pod(tenant_core, pod_name, ns)

    marker = "liqo-log-marker-42"
    pod = client.V1Pod(
        metadata=client.V1ObjectMeta(name=pod_name, namespace=ns),
        spec=client.V1PodSpec(
            restart_policy="Never",
            containers=[
                client.V1Container(
                    name="test",
                    image="registry.access.redhat.com/ubi9/ubi-minimal:latest",
                    command=["sh", "-c", f"echo {marker} && echo line2 && echo line3"],
                    resources=client.V1ResourceRequirements(
                        limits={"nvidia.com/gpu": "1"},
                    ),
                ),
            ],
        ),
    )

    tenant_core.create_namespaced_pod(namespace=ns, body=pod)
    cleanup(force_delete_pod, tenant_core, pod_name, ns)

    phase = wait_pod_phase(tenant_core, pod_name, ns, ["Succeeded"], timeout=300)
    assert phase == "Succeeded", f"Pod phase: {phase}"

    logs = tenant_core.read_namespaced_pod_log(
        name=pod_name, namespace=ns, container="test",
    )
    assert marker in logs, f"Marker {marker} not in logs: {logs[:200]}"

    tail_logs = tenant_core.read_namespaced_pod_log(
        name=pod_name, namespace=ns, container="test", tail_lines=1,
    )
    assert "line3" in tail_logs, f"tail_lines=1 missing line3: {tail_logs}"
    assert "line2" not in tail_logs, f"tail_lines=1 should not contain line2"


@pytest.mark.liqo
def test_pod_status_sync_podip(
    cleanup, liqo_tenant_clients,
    liqo_test_namespace, liqo_virtual_node_ready,
):
    """Tenant pod PodIP synced from worker — needed for Submariner routing."""
    tenant_core, _ = liqo_tenant_clients
    ns = liqo_test_namespace

    pod_name = "liqo-podip-test"

    force_delete_pod(tenant_core, pod_name, ns)

    pod = client.V1Pod(
        metadata=client.V1ObjectMeta(name=pod_name, namespace=ns),
        spec=client.V1PodSpec(
            restart_policy="Never",
            containers=[
                client.V1Container(
                    name="test",
                    image="registry.access.redhat.com/ubi9/ubi-minimal:latest",
                    command=["sleep", "30"],
                    resources=client.V1ResourceRequirements(
                        limits={"nvidia.com/gpu": "1"},
                    ),
                ),
            ],
        ),
    )

    tenant_core.create_namespaced_pod(namespace=ns, body=pod)
    cleanup(force_delete_pod, tenant_core, pod_name, ns)

    phase = wait_pod_phase(tenant_core, pod_name, ns, ["Running"], timeout=300)
    assert phase == "Running", f"Pod phase: {phase}"

    pod_obj = tenant_core.read_namespaced_pod(name=pod_name, namespace=ns)
    pod_ip = pod_obj.status.pod_ip

    assert pod_ip, "Tenant pod has no PodIP — status sync may not be working"
    assert pod_ip.startswith("10.13"), (
        f"PodIP {pod_ip} is not from worker CIDR (10.13x.x.x) — "
        f"Liqo may be remapping IPs or status sync is broken"
    )

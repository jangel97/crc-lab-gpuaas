"""
Headless Service sync for distributed training.

Tests that VK syncs headless Services from the tenant to the worker
namespace, enabling inter-pod DNS resolution. This is the mechanism
that makes multi-pod PyTorchJob (master + workers) work: the training
operator creates headless Services for replica-type groups, and pods
use hostname + subdomain for DNS-based discovery.

Since the lab has only 1 GPU, we test the mechanism directly with
CPU-only pods: two pods with hostname/subdomain + a headless Service,
and verify DNS resolution between them on the worker cluster.
"""

import random
import string
import time

import pytest
from kubernetes import client

from conftest import worker_pod_name


VK_NODE_NAME = "gpu-worker"


def _suffix():
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=5))


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


@pytest.mark.networking
def test_headless_service_dns_resolution(
    tenant_clients, worker_clients, test_namespace, vk_worker_namespace
):
    """
    Deploy two pods with hostname/subdomain and a headless Service on
    the tenant. VK dispatches pods and syncs the headless Service to the
    worker. Verify that Pod B can resolve Pod A's DNS name via the
    synced headless Service.
    """
    tenant_core, _ = tenant_clients
    worker_core, _ = worker_clients
    ns = test_namespace

    run_id = _suffix()
    pod_a_name = f"dns-server-{run_id}"
    pod_b_name = f"dns-lookup-{run_id}"
    svc_name = f"dns-test-{run_id}"
    w_pod_a = worker_pod_name(ns, pod_a_name)
    w_pod_b = worker_pod_name(ns, pod_b_name)

    # Cleanup from previous runs — delete worker pods first to avoid
    # VK informer race (worker informer re-adds managedPods entry)
    force_delete_pod(worker_core, w_pod_a, vk_worker_namespace)
    force_delete_pod(worker_core, w_pod_b, vk_worker_namespace)
    force_delete_pod(tenant_core, pod_a_name, ns)
    force_delete_pod(tenant_core, pod_b_name, ns)
    try:
        worker_core.delete_namespaced_service(
            name=svc_name, namespace=vk_worker_namespace
        )
    except client.exceptions.ApiException:
        pass
    try:
        tenant_core.delete_namespaced_service(name=svc_name, namespace=ns)
    except client.exceptions.ApiException:
        pass
    time.sleep(10)

    # Create headless Service on tenant (VK will sync it to worker)
    svc = client.V1Service(
        metadata=client.V1ObjectMeta(name=svc_name, namespace=ns),
        spec=client.V1ServiceSpec(
            cluster_ip="None",
            selector={"app": "dns-test"},
            ports=[client.V1ServicePort(port=80, target_port=80)],
        ),
    )
    tenant_core.create_namespaced_service(namespace=ns, body=svc)

    # Pod A: long-running server pod with hostname/subdomain
    pod_a = client.V1Pod(
        metadata=client.V1ObjectMeta(
            name=pod_a_name,
            namespace=ns,
            labels={"app": "dns-test"},
        ),
        spec=client.V1PodSpec(
            node_name=VK_NODE_NAME,
            hostname="pod-a",
            subdomain=svc_name,
            restart_policy="Never",
            tolerations=[
                client.V1Toleration(
                    key="virtual-kubelet.io/provider",
                    operator="Exists",
                ),
            ],
            containers=[
                client.V1Container(
                    name="server",
                    image="registry.access.redhat.com/ubi9-micro:latest",
                    command=["sleep", "300"],
                    resources=client.V1ResourceRequirements(
                        requests={"cpu": "100m", "memory": "64Mi"},
                    ),
                )
            ],
        ),
    )
    tenant_core.create_namespaced_pod(namespace=ns, body=pod_a)

    # Wait for Pod A to be Running
    deadline = time.time() + 120
    pod_a_running = False
    while time.time() < deadline:
        tp = tenant_core.read_namespaced_pod(name=pod_a_name, namespace=ns)
        if tp.status.phase == "Running":
            pod_a_running = True
            break
        if tp.status.phase == "Failed":
            pytest.fail(
                f"Pod A failed: {tp.status.reason} -- {tp.status.message}"
            )
        time.sleep(5)

    assert pod_a_running, "Pod A did not reach Running within 120s"

    # Verify headless Service was synced to worker namespace
    deadline = time.time() + 30
    svc_synced = False
    while time.time() < deadline:
        try:
            w_svc = worker_core.read_namespaced_service(
                name=svc_name, namespace=vk_worker_namespace
            )
            if w_svc.spec.cluster_ip == "None":
                svc_synced = True
                break
        except client.exceptions.ApiException:
            pass
        time.sleep(3)

    assert svc_synced, (
        f"Headless Service {svc_name} was not synced to {vk_worker_namespace}"
    )

    # Pod B: DNS lookup pod — resolves Pod A's hostname on the worker
    dns_target = f"pod-a.{svc_name}.{vk_worker_namespace}.svc.cluster.local"
    pod_b = client.V1Pod(
        metadata=client.V1ObjectMeta(
            name=pod_b_name,
            namespace=ns,
            labels={"app": "dns-test"},
        ),
        spec=client.V1PodSpec(
            node_name=VK_NODE_NAME,
            hostname="pod-b",
            subdomain=svc_name,
            restart_policy="Never",
            tolerations=[
                client.V1Toleration(
                    key="virtual-kubelet.io/provider",
                    operator="Exists",
                ),
            ],
            containers=[
                client.V1Container(
                    name="lookup",
                    image="registry.access.redhat.com/ubi9:latest",
                    command=[
                        "bash", "-c",
                        f"for i in $(seq 1 10); do "
                        f"getent hosts {dns_target} && exit 0; "
                        f"sleep 3; done; exit 1",
                    ],
                    resources=client.V1ResourceRequirements(
                        requests={"cpu": "100m", "memory": "64Mi"},
                    ),
                )
            ],
        ),
    )
    tenant_core.create_namespaced_pod(namespace=ns, body=pod_b)

    # Wait for Pod B to complete
    deadline = time.time() + 120
    pod_b_phase = None
    while time.time() < deadline:
        tp = tenant_core.read_namespaced_pod(name=pod_b_name, namespace=ns)
        pod_b_phase = tp.status.phase
        if pod_b_phase in ("Succeeded", "Failed"):
            break
        time.sleep(5)

    assert pod_b_phase == "Succeeded", (
        f"DNS lookup pod did not succeed (phase={pod_b_phase!r}). "
        "Headless Service DNS resolution may not be working."
    )

    # Read Pod B logs from worker — should contain the resolved IP
    logs = worker_core.read_namespaced_pod_log(
        name=worker_pod_name(ns, pod_b_name), namespace=vk_worker_namespace,
    ).strip()
    assert logs and ("." in logs), (
        f"DNS lookup did not resolve an address. Logs: {logs}"
    )

    # Cleanup
    force_delete_pod(tenant_core, pod_b_name, ns)
    force_delete_pod(tenant_core, pod_a_name, ns)
    try:
        tenant_core.delete_namespaced_service(name=svc_name, namespace=ns)
    except client.exceptions.ApiException:
        pass

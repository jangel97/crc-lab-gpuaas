"""
Liqo + Kueue integration tests.

Tests whether Kyverno on the worker cluster can mutate ShadowPod-created
pods to add Kueue labels, and whether Kueue can admit/gate those pods.

This is the critical unknown: the ShadowPod controller enforces the pod
spec from the ShadowPod CR. If Kyverno mutates the Pod and the ShadowPod
controller reconciles, it may overwrite the mutation.
"""

import time

import pytest
from kubernetes import client

from helpers import force_delete_pod


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
@pytest.mark.kueue
def test_kueue_admits_liqo_pod(
    cleanup, liqo_tenant_clients, liqo_worker_clients,
    liqo_test_namespace, liqo_worker_namespace, liqo_virtual_node_ready,
):
    """GPU pod dispatched via Liqo → Kyverno adds Kueue labels on worker →
    Kueue admits → pod runs → status synced back."""
    tenant_core, _ = liqo_tenant_clients
    worker_core, _ = liqo_worker_clients
    ns = liqo_test_namespace
    w_ns = liqo_worker_namespace

    pod_name = "liqo-kueue-test"

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

    assert worker_pod, f"No worker pod for {pod_name} in {w_ns}"

    w_pod = worker_core.read_namespaced_pod(name=worker_pod, namespace=w_ns)
    labels = w_pod.metadata.labels or {}

    kueue_queue = labels.get("kueue.x-k8s.io/queue-name")
    kueue_priority = labels.get("kueue.x-k8s.io/priority-class")

    if not kueue_queue:
        pytest.skip(
            "Kyverno did not add kueue.x-k8s.io/queue-name label — "
            "Kyverno policy may not be configured for Liqo ShadowPod-created pods, "
            "or ShadowPod controller may have overwritten the mutation"
        )

    gates = w_pod.spec.scheduling_gates or []
    had_gate = any(g.name == "kueue.x-k8s.io/admission" for g in gates)

    phase = wait_pod_phase(tenant_core, pod_name, ns, ["Succeeded", "Failed"], timeout=300)

    assert phase == "Succeeded", (
        f"Pod phase: {phase}. "
        f"Kueue queue: {kueue_queue}, priority: {kueue_priority}, "
        f"had scheduling gate: {had_gate}"
    )


@pytest.mark.liqo
@pytest.mark.kueue
def test_shadowpod_label_persistence(
    cleanup, liqo_tenant_clients, liqo_worker_clients,
    liqo_test_namespace, liqo_worker_namespace, liqo_virtual_node_ready,
):
    """Verify whether Kyverno-added labels persist or get overwritten by
    the ShadowPod controller during reconciliation."""
    tenant_core, _ = liqo_tenant_clients
    worker_core, _ = liqo_worker_clients
    ns = liqo_test_namespace
    w_ns = liqo_worker_namespace

    pod_name = "liqo-label-persist-test"

    force_delete_pod(tenant_core, pod_name, ns)

    pod = client.V1Pod(
        metadata=client.V1ObjectMeta(name=pod_name, namespace=ns),
        spec=client.V1PodSpec(
            restart_policy="Never",
            containers=[
                client.V1Container(
                    name="test",
                    image="registry.access.redhat.com/ubi9/ubi-minimal:latest",
                    command=["sleep", "60"],
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

    assert worker_pod, f"No worker pod for {pod_name} in {w_ns}"

    w_pod = worker_core.read_namespaced_pod(name=worker_pod, namespace=w_ns)
    labels_t0 = dict(w_pod.metadata.labels or {})

    time.sleep(15)

    w_pod = worker_core.read_namespaced_pod(name=worker_pod, namespace=w_ns)
    labels_t1 = dict(w_pod.metadata.labels or {})

    kueue_key = "kueue.x-k8s.io/queue-name"
    t0_has = kueue_key in labels_t0
    t1_has = kueue_key in labels_t1

    if t0_has and not t1_has:
        pytest.fail(
            "ShadowPod controller OVERWROTE Kyverno-added Kueue labels "
            "during reconciliation — Kueue integration is not viable with Liqo"
        )
    elif not t0_has:
        pytest.skip(
            "Kyverno did not add Kueue labels — policy not configured or "
            "not targeting ShadowPod-created pods"
        )

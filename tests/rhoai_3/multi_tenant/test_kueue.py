"""
Kueue quota enforcement and cross-tenant preemption for RHOAI 3.x.

Tests that Kueue on the worker cluster enforces GPU quota and allows
priority-based preemption when two RHOAI 3.x tenants share the same GPU
through VK.

Requires Kueue + Kyverno deployed on the worker cluster (same setup as
tests/kueue/test_kueue_vk.py).
"""

import os
import time

import pytest
import yaml
from kubernetes import client, utils

from helpers import worker_pod_name, force_delete_pod, wait_pod_exists


VK_NODE_NAME = "gpu-worker"
VK_TOLERATIONS = [{"key": "virtual-kubelet.io/provider", "operator": "Exists"}]
CUDA_IMAGE = "nvcr.io/nvidia/cuda:12.8.1-base-ubi9"

KUEUE_GROUP = "kueue.x-k8s.io"
KUEUE_VERSION = "v1beta1"
KUEUE_GATE = "kueue.x-k8s.io/admission"
PRIORITY_LABEL = "kueue.x-k8s.io/priority-class"


# ---------------------------------------------------------------------------
# Helpers (subset from kueue/test_kueue_vk.py, adapted for rhoai3 fixtures)
# ---------------------------------------------------------------------------


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


def wait_pod_admitted(core_api, name, namespace, timeout=120):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            pod = core_api.read_namespaced_pod(name=name, namespace=namespace)
            gates = pod.spec.scheduling_gates or []
            if not any(g.name == KUEUE_GATE for g in gates):
                return True
        except client.exceptions.ApiException:
            pass
        time.sleep(3)
    return False


def wait_pod_deleted(core_api, name, namespace, timeout=120):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            core_api.read_namespaced_pod(name=name, namespace=namespace)
        except client.exceptions.ApiException as e:
            if e.status == 404:
                return True
        time.sleep(3)
    return False


def ensure_namespace(core_api, namespace):
    try:
        core_api.read_namespace(name=namespace)
    except client.exceptions.ApiException as e:
        if e.status == 404:
            core_api.create_namespace(
                body=client.V1Namespace(
                    metadata=client.V1ObjectMeta(name=namespace),
                ),
            )
        else:
            raise


def ensure_namespace_labels(core_api, namespace, priority_class):
    ensure_namespace(core_api, namespace)
    allowed = ",".join([
        "gpuaas-production", "gpuaas-critical",
        "gpuaas-standard", "gpuaas-opportunistic",
    ])
    core_api.patch_namespace(
        name=namespace,
        body={
            "metadata": {
                "labels": {
                    "gpuaas.redhat.com/managed": "true",
                    "kyverno.io/watch": "enabled",
                    "gpuaas.redhat.com/default-priority": priority_class,
                },
                "annotations": {
                    "gpuaas.redhat.com/allowed-priorities": allowed,
                },
            }
        },
    )


def ensure_local_queue(custom_api, namespace):
    try:
        custom_api.get_namespaced_custom_object(
            group=KUEUE_GROUP, version=KUEUE_VERSION,
            namespace=namespace, plural="localqueues", name="default",
        )
    except client.exceptions.ApiException as e:
        if e.status == 404:
            custom_api.create_namespaced_custom_object(
                group=KUEUE_GROUP, version=KUEUE_VERSION,
                namespace=namespace, plural="localqueues",
                body={
                    "apiVersion": f"{KUEUE_GROUP}/{KUEUE_VERSION}",
                    "kind": "LocalQueue",
                    "metadata": {"name": "default", "namespace": namespace},
                    "spec": {"clusterQueue": "cluster-queue"},
                },
            )
        else:
            raise


def cleanup_stale_workloads(custom_api, namespace):
    try:
        wls = custom_api.list_namespaced_custom_object(
            group=KUEUE_GROUP, version=KUEUE_VERSION,
            namespace=namespace, plural="workloads",
        )
        for wl in wls.get("items", []):
            try:
                custom_api.delete_namespaced_custom_object(
                    group=KUEUE_GROUP, version=KUEUE_VERSION,
                    namespace=namespace, plural="workloads",
                    name=wl["metadata"]["name"],
                )
            except client.exceptions.ApiException:
                pass
    except client.exceptions.ApiException:
        pass


def assert_cluster_queue_active(custom_api, name="cluster-queue", timeout=60):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            cq = custom_api.get_cluster_custom_object(
                group=KUEUE_GROUP, version=KUEUE_VERSION,
                plural="clusterqueues", name=name,
            )
            for cond in (cq.get("status", {}).get("conditions") or []):
                if cond.get("type") == "Active" and cond.get("status") == "True":
                    return
        except client.exceptions.ApiException:
            pass
        time.sleep(5)
    raise AssertionError(
        f"ClusterQueue {name} not Active within {timeout}s"
    )


def make_gpu_pod(name, namespace, command, restart_policy="Never"):
    return client.V1Pod(
        metadata=client.V1ObjectMeta(name=name, namespace=namespace),
        spec=client.V1PodSpec(
            node_name=VK_NODE_NAME,
            restart_policy=restart_policy,
            tolerations=[
                client.V1Toleration(
                    key="virtual-kubelet.io/provider", operator="Exists",
                ),
            ],
            containers=[
                client.V1Container(
                    name="gpu",
                    image=CUDA_IMAGE,
                    command=command,
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


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.rhoai3_mt
def test_kueue_quota_enforcement(
    cleanup,
    rhoai3_tenant_clients, rhoai3_worker_clients,
    rhoai3_test_namespace, rhoai3_vk_worker_namespace,
):
    """
    Only 1 GPU available. Submit 2 GPU pods from tenant1 — second should
    be queued by Kueue until the first completes.
    """
    tenant_core, _ = rhoai3_tenant_clients
    worker_core, worker_custom = rhoai3_worker_clients
    ns = rhoai3_test_namespace
    w_ns = rhoai3_vk_worker_namespace

    assert_cluster_queue_active(worker_custom)
    cleanup_stale_workloads(worker_custom, w_ns)
    ensure_namespace_labels(worker_core, w_ns, "gpuaas-standard")
    ensure_local_queue(worker_custom, w_ns)

    pod_a = "rhoai3-quota-a"
    pod_b = "rhoai3-quota-b"
    w_pod_a = worker_pod_name(ns, pod_a)
    w_pod_b = worker_pod_name(ns, pod_b)

    for p, w in [(pod_a, w_pod_a), (pod_b, w_pod_b)]:
        force_delete_pod(tenant_core, p, ns)
        force_delete_pod(worker_core, w, w_ns)
    time.sleep(3)

    # Pod A: holds the GPU
    tenant_core.create_namespaced_pod(
        namespace=ns,
        body=make_gpu_pod(pod_a, ns, ["sleep", "300"]),
    )
    cleanup(force_delete_pod, tenant_core, pod_a, ns)

    assert wait_pod_exists(worker_core, w_pod_a, w_ns, timeout=60)
    assert wait_pod_admitted(worker_core, w_pod_a, w_ns, timeout=120)

    phase_a = wait_pod_phase(worker_core, w_pod_a, w_ns, ("Running",), timeout=300)
    assert phase_a == "Running", f"Pod A not Running. Phase: {phase_a!r}"

    # Pod B: should be queued
    tenant_core.create_namespaced_pod(
        namespace=ns,
        body=make_gpu_pod(pod_b, ns, ["nvidia-smi"]),
    )
    cleanup(force_delete_pod, tenant_core, pod_b, ns)

    assert wait_pod_exists(worker_core, w_pod_b, w_ns, timeout=60)

    time.sleep(5)
    wp_b = worker_core.read_namespaced_pod(w_pod_b, w_ns)
    gates = wp_b.spec.scheduling_gates or []
    has_gate = any(g.name == KUEUE_GATE for g in gates)
    assert has_gate, (
        f"Pod B should have Kueue scheduling gate. "
        f"Gates: {[g.name for g in gates]}."
    )

    # Delete pod A → frees GPU → Kueue admits pod B
    force_delete_pod(tenant_core, pod_a, ns)

    assert wait_pod_admitted(worker_core, w_pod_b, w_ns, timeout=120), (
        "Kueue did not admit pod B after pod A was deleted"
    )

    phase_b = wait_pod_phase(
        worker_core, w_pod_b, w_ns, ("Succeeded", "Failed"), timeout=300,
    )
    assert phase_b == "Succeeded", f"Pod B did not succeed. Phase: {phase_b!r}"


@pytest.mark.rhoai3_mt
def test_kueue_cross_tenant_preemption(
    cleanup,
    rhoai3_tenant_clients, rhoai3_tenant2_clients, rhoai3_worker_clients,
    rhoai3_test_namespace,
    rhoai3_vk_worker_namespace, rhoai3_vk_worker_namespace_t2,
):
    """
    Tenant1 (opportunistic) holds GPU. Tenant2 (production) submits.
    Kueue preempts tenant1's pod, tenant2 runs.
    """
    t1_core, _ = rhoai3_tenant_clients
    t2_core, _ = rhoai3_tenant2_clients
    worker_core, worker_custom = rhoai3_worker_clients
    ns = rhoai3_test_namespace
    w_ns_t1 = rhoai3_vk_worker_namespace
    w_ns_t2 = rhoai3_vk_worker_namespace_t2

    assert_cluster_queue_active(worker_custom)
    cleanup_stale_workloads(worker_custom, w_ns_t1)
    cleanup_stale_workloads(worker_custom, w_ns_t2)

    ensure_namespace_labels(worker_core, w_ns_t1, "gpuaas-opportunistic")
    ensure_namespace_labels(worker_core, w_ns_t2, "gpuaas-production")
    ensure_local_queue(worker_custom, w_ns_t1)
    ensure_local_queue(worker_custom, w_ns_t2)

    t1_pod = "rhoai3-preempt-low"
    t2_pod = "rhoai3-preempt-high"
    w_pod_t1 = worker_pod_name(ns, t1_pod)
    w_pod_t2 = worker_pod_name(ns, t2_pod)

    force_delete_pod(t1_core, t1_pod, ns)
    force_delete_pod(t2_core, t2_pod, ns)
    force_delete_pod(worker_core, w_pod_t1, w_ns_t1)
    force_delete_pod(worker_core, w_pod_t2, w_ns_t2)
    time.sleep(3)

    # Tenant1: low-priority long-running job
    t1_core.create_namespaced_pod(
        namespace=ns,
        body=make_gpu_pod(t1_pod, ns, ["sleep", "600"]),
    )
    cleanup(force_delete_pod, t1_core, t1_pod, ns)
    cleanup(force_delete_pod, worker_core, w_pod_t1, w_ns_t1)

    assert wait_pod_exists(worker_core, w_pod_t1, w_ns_t1, timeout=60)
    assert wait_pod_admitted(worker_core, w_pod_t1, w_ns_t1, timeout=120)

    phase_t1 = wait_pod_phase(
        worker_core, w_pod_t1, w_ns_t1, ("Running",), timeout=300,
    )
    assert phase_t1 == "Running", f"Tenant1 pod not Running. Phase: {phase_t1!r}"

    # Tenant2: high-priority job
    t2_core.create_namespaced_pod(
        namespace=ns,
        body=make_gpu_pod(t2_pod, ns, ["nvidia-smi"]),
    )
    cleanup(force_delete_pod, t2_core, t2_pod, ns)
    cleanup(force_delete_pod, worker_core, w_pod_t2, w_ns_t2)

    assert wait_pod_exists(worker_core, w_pod_t2, w_ns_t2, timeout=60)

    # Kueue should preempt tenant1
    assert wait_pod_deleted(worker_core, w_pod_t1, w_ns_t1, timeout=120), (
        f"Kueue did not preempt tenant1's worker pod within 120s"
    )

    # Tenant2 should be admitted and succeed
    assert wait_pod_admitted(worker_core, w_pod_t2, w_ns_t2, timeout=120)

    phase_t2 = wait_pod_phase(
        worker_core, w_pod_t2, w_ns_t2, ("Succeeded", "Failed"), timeout=300,
    )
    assert phase_t2 == "Succeeded", (
        f"Tenant2 pod did not succeed. Phase: {phase_t2!r}"
    )

    # Tenant1's pod should show Failed on tenant side
    t1_tenant_phase = wait_pod_phase(
        t1_core, t1_pod, ns, ("Failed",), timeout=60,
    )
    assert t1_tenant_phase == "Failed", (
        f"Tenant1 pod should show Failed after preemption. Phase: {t1_tenant_phase!r}"
    )

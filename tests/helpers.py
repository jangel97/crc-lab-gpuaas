"""Shared test utilities and constants."""

import time

from kubernetes import client


VK_TEST_NAMESPACE = "vk-test"
CATAPULT_STORAGE_CLASS = "catapult"


def worker_pod_name(tenant_namespace, pod_name):
    """Compute the namespaced worker pod name that VK creates."""
    return f"{tenant_namespace}--{pod_name}"


def execution_pvc_name(tenant_namespace, pvc_name):
    """Compute the namespace-prefixed execution PVC name."""
    return f"{tenant_namespace}--{pvc_name}"


def worker_namespace_for_prefix(prefix, tenant_namespace):
    """Compute the per-tenant worker namespace given a prefix."""
    return f"{prefix}{tenant_namespace}"


def safe_delete(fn, *args, **kwargs):
    try:
        fn(*args, **kwargs)
    except Exception:
        pass


def wait_pod_exists(core_api, name, namespace, timeout=120):
    """Wait for a pod to exist. Returns True if found."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            core_api.read_namespaced_pod(name=name, namespace=namespace)
            return True
        except client.exceptions.ApiException:
            pass
        time.sleep(3)
    return False


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

"""
Shared fixtures for RHOAI 3.x test suites (e2e and multi_tenant).

Provides:
  - rhoai3_environment: health-check fixture (skips if RHOAI 3.x not ready)
  - rhoai3_tenant_clients / rhoai3_worker_clients / rhoai3_tenant2_clients
  - rhoai3_vk_node_ready: waits for VK virtual node on tenant-rhoai3
  - rhoai3_lab_env: LabEnvironment with worker running
  - rhoai3_test_namespace / rhoai3_vk_worker_namespace
  - cleanup: per-test cleanup callbacks
"""

import os
import time

import pytest
import urllib3
from kubernetes import client, config

from helpers import (
    VK_TEST_NAMESPACE,
    CATAPULT_STORAGE_CLASS,
    safe_delete,
    worker_namespace_for_prefix,
)
from lab_env import LabEnvironment, LabEnvironmentError

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


RHOAI_OPERATOR_NS = "redhat-ods-operator"
RHOAI_APPS_NS = "redhat-ods-applications"
VK_NODE_NAME = "gpu-worker"
VK_PREFIX_CONFIGMAP = "vk-gpu-provider-config-gpu-worker"
VK_PREFIX_CONFIGMAP_NS = "kube-system"


def _load_clients(kubeconfig_path):
    api_client = config.new_client_from_config(config_file=kubeconfig_path)
    return client.CoreV1Api(api_client), client.CustomObjectsApi(api_client)


def _deployment_ready(apps, name, namespace):
    try:
        dep = apps.read_namespaced_deployment(name, namespace)
        ready = dep.status.ready_replicas or 0
        desired = dep.spec.replicas or 1
        return ready >= desired
    except client.exceptions.ApiException:
        return False


# ---------------------------------------------------------------------------
# Environment health check
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def rhoai3_environment():
    """Verify RHOAI 3.x environment is ready. Skips if not provisioned."""
    path = os.environ.get("RHOAI3_KUBECONFIG", os.path.expanduser("~/.kube/tenant-rhoai3"))
    api = config.new_client_from_config(config_file=path)
    custom = client.CustomObjectsApi(api)
    apps = client.AppsV1Api(api)

    try:
        cv = custom.get_cluster_custom_object(
            "config.openshift.io", "v1", "clusterversions", "version",
        )
        ocp_version = cv.get("status", {}).get("desired", {}).get("version", "unknown")
    except client.exceptions.ApiException:
        pytest.skip("Cannot reach cluster -- check RHOAI3_KUBECONFIG")

    major_minor = ".".join(ocp_version.split(".")[:2])
    if major_minor < "4.19":
        pytest.skip(f"OCP {ocp_version} < 4.19 -- RHOAI 3.x requires 4.19+")

    required = [
        ("rhods-operator", RHOAI_OPERATOR_NS),
        ("kubeflow-training-operator", RHOAI_APPS_NS),
        ("cert-manager", "cert-manager"),
        ("jobset-operator", "jobset-system"),
    ]
    missing = [
        f"{name} in {ns}"
        for name, ns in required
        if not _deployment_ready(apps, name, ns)
    ]
    if missing:
        pytest.skip(
            f"RHOAI 3.x not ready -- missing deployments: {', '.join(missing)}. "
            "Run: ansible-playbook playbooks/03-configure-clusters.yml -l tenant-rhoai3"
        )

    print(f"\n==> RHOAI 3.x environment ready (OCP {ocp_version})")
    return api


# ---------------------------------------------------------------------------
# Lab environment
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def rhoai3_lab_env():
    """Lab VM manager. Ensures the worker is running."""
    try:
        env = LabEnvironment()
    except LabEnvironmentError as e:
        pytest.skip(f"Lab environment unavailable: {e}")
    env.ensure_running("sno-worker")
    return env


# ---------------------------------------------------------------------------
# Client fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def rhoai3_tenant_clients():
    path = os.environ.get("RHOAI3_KUBECONFIG", os.path.expanduser("~/.kube/tenant-rhoai3"))
    return _load_clients(path)


@pytest.fixture(scope="session")
def rhoai3_worker_clients():
    path = os.environ.get("WORKER_KUBECONFIG", os.path.expanduser("~/.kube/worker"))
    return _load_clients(path)


@pytest.fixture(scope="session")
def rhoai3_tenant2_clients():
    path = os.environ.get("RHOAI3_TENANT2_KUBECONFIG", os.path.expanduser("~/.kube/tenant2-rhoai3"))
    return _load_clients(path)


# ---------------------------------------------------------------------------
# VK readiness and namespace fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def rhoai3_vk_node_ready(rhoai3_tenant_clients):
    """Wait for the VK virtual node to be registered and Ready."""
    tenant_core, _ = rhoai3_tenant_clients
    deadline = time.time() + 120
    while time.time() < deadline:
        try:
            node = tenant_core.read_node(name=VK_NODE_NAME)
            for cond in (node.status.conditions or []):
                if cond.type == "Ready" and cond.status == "True":
                    return True
        except client.exceptions.ApiException:
            pass
        time.sleep(10)
    pytest.fail(f"VK node {VK_NODE_NAME} not Ready within 120s")


@pytest.fixture(scope="session")
def rhoai3_worker_namespace_prefix(rhoai3_tenant_clients):
    tenant_core, _ = rhoai3_tenant_clients
    cm = tenant_core.read_namespaced_config_map(
        name=VK_PREFIX_CONFIGMAP, namespace=VK_PREFIX_CONFIGMAP_NS,
    )
    prefix = cm.data.get("worker-namespace-prefix", "")
    assert prefix, (
        f"ConfigMap {VK_PREFIX_CONFIGMAP_NS}/{VK_PREFIX_CONFIGMAP} has no "
        f"worker-namespace-prefix key"
    )
    return prefix


@pytest.fixture(scope="session")
def rhoai3_worker_namespace_prefix_t2(rhoai3_tenant2_clients):
    tenant2_core, _ = rhoai3_tenant2_clients
    cm = tenant2_core.read_namespaced_config_map(
        name=VK_PREFIX_CONFIGMAP, namespace=VK_PREFIX_CONFIGMAP_NS,
    )
    prefix = cm.data.get("worker-namespace-prefix", "")
    assert prefix, "tenant2-rhoai3 VK ConfigMap has no worker-namespace-prefix key"
    return prefix


@pytest.fixture(scope="session")
def rhoai3_test_namespace():
    return VK_TEST_NAMESPACE


@pytest.fixture(scope="session")
def rhoai3_vk_worker_namespace(rhoai3_worker_namespace_prefix):
    return worker_namespace_for_prefix(rhoai3_worker_namespace_prefix, VK_TEST_NAMESPACE)


@pytest.fixture(scope="session")
def rhoai3_vk_worker_namespace_t2(rhoai3_worker_namespace_prefix_t2):
    return worker_namespace_for_prefix(rhoai3_worker_namespace_prefix_t2, VK_TEST_NAMESPACE)


# ---------------------------------------------------------------------------
# Per-test cleanup
# ---------------------------------------------------------------------------


@pytest.fixture
def cleanup():
    _callbacks = []

    def register(fn, *args, **kwargs):
        _callbacks.append((fn, args, kwargs))

    yield register

    for fn, args, kwargs in reversed(_callbacks):
        safe_delete(fn, *args, **kwargs)

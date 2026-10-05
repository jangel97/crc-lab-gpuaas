import os

import pytest
import urllib3
from kubernetes import client, config

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

VK_TEST_NAMESPACE = "vk-test"
CATAPULT_STORAGE_CLASS = "catapult"

VK_PREFIX_CONFIGMAP = "vk-gpu-provider-config"
VK_PREFIX_CONFIGMAP_NS = "kube-system"


def worker_pod_name(tenant_namespace, pod_name):
    """Compute the namespaced worker pod name that VK creates."""
    return f"{tenant_namespace}--{pod_name}"


def execution_pvc_name(tenant_namespace, pvc_name):
    """Compute the namespace-prefixed execution PVC name."""
    return f"{tenant_namespace}--{pvc_name}"


def _load_clients(kubeconfig_path):
    """Load a kubeconfig and return (CoreV1Api, CustomObjectsApi)."""
    api_client = config.new_client_from_config(config_file=kubeconfig_path)
    return client.CoreV1Api(api_client), client.CustomObjectsApi(api_client)


@pytest.fixture(scope="session")
def tenant_clients():
    """API clients for the tenant cluster."""
    path = os.environ.get("TENANT_KUBECONFIG", os.path.expanduser("~/.kube/tenant"))
    return _load_clients(path)


@pytest.fixture(scope="session", autouse=True)
def ensure_catapult_storageclass(tenant_clients):
    """Ensure the catapult StorageClass exists on the tenant cluster."""
    tenant_core, _ = tenant_clients
    storage_api = client.StorageV1Api(tenant_core.api_client)
    try:
        storage_api.read_storage_class(name=CATAPULT_STORAGE_CLASS)
    except client.exceptions.ApiException as e:
        if e.status == 404:
            storage_api.create_storage_class(
                body=client.V1StorageClass(
                    metadata=client.V1ObjectMeta(name=CATAPULT_STORAGE_CLASS),
                    provisioner="kubernetes.io/no-provisioner",
                    reclaim_policy="Retain",
                    volume_binding_mode="WaitForFirstConsumer",
                ),
            )
        else:
            raise


@pytest.fixture(scope="session")
def worker_clients():
    """API clients for the worker (GPU) cluster."""
    path = os.environ.get("WORKER_KUBECONFIG", os.path.expanduser("~/.kube/worker"))
    return _load_clients(path)


@pytest.fixture(scope="session")
def worker_namespace_prefix(tenant_clients):
    """Read the auto-generated worker namespace prefix from the VK ConfigMap."""
    tenant_core, _ = tenant_clients
    cm = tenant_core.read_namespaced_config_map(
        name=VK_PREFIX_CONFIGMAP, namespace=VK_PREFIX_CONFIGMAP_NS,
    )
    prefix = cm.data.get("worker-namespace-prefix", "")
    assert prefix, (
        f"ConfigMap {VK_PREFIX_CONFIGMAP_NS}/{VK_PREFIX_CONFIGMAP} has no "
        f"worker-namespace-prefix key — is the VK running?"
    )
    return prefix


def worker_namespace_for_prefix(prefix, tenant_namespace):
    """Compute the per-tenant worker namespace given a prefix."""
    return f"{prefix}{tenant_namespace}"


@pytest.fixture(scope="session")
def test_namespace():
    """Return the pre-provisioned VK test namespace on the tenant."""
    return VK_TEST_NAMESPACE


@pytest.fixture(scope="session")
def vk_worker_namespace(worker_namespace_prefix):
    """Return the per-tenant worker namespace for the default test namespace."""
    return worker_namespace_for_prefix(worker_namespace_prefix, VK_TEST_NAMESPACE)

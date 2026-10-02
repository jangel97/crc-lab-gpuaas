import os

import pytest
import urllib3
from kubernetes import client, config

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

VK_TEST_NAMESPACE = "vk-test"
VK_WORKER_NAMESPACE = "vk-workloads"
CATAPULT_STORAGE_CLASS = "catapult"


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
def test_namespace():
    """Return the pre-provisioned VK test namespace on the tenant."""
    return VK_TEST_NAMESPACE


@pytest.fixture(scope="session")
def vk_worker_namespace():
    """Return the namespace on the worker where VK creates pods."""
    return VK_WORKER_NAMESPACE

import os

import pytest
import urllib3
from kubernetes import client, config

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

VK_TEST_NAMESPACE = "vk-test"
VK_WORKER_NAMESPACE = "vk-workloads"


def _load_clients(kubeconfig_path):
    """Load a kubeconfig and return (CoreV1Api, CustomObjectsApi)."""
    api_client = config.new_client_from_config(config_file=kubeconfig_path)
    return client.CoreV1Api(api_client), client.CustomObjectsApi(api_client)


@pytest.fixture(scope="session")
def tenant_clients():
    """API clients for the tenant cluster."""
    path = os.environ.get("TENANT_KUBECONFIG", os.path.expanduser("~/.kube/tenant"))
    return _load_clients(path)


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

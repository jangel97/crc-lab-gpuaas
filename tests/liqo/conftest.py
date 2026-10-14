"""
Liqo spike test fixtures.

Liqo offloads entire namespaces — pods in an offloaded namespace with
strategy=Remote are automatically scheduled to the virtual node. No
nodeName or VK tolerations needed on the pod spec.

The worker (twin) namespace has the same name as the tenant namespace
when using EnforceSameName mapping strategy.
"""

import os
import time

import pytest
import urllib3
from kubernetes import client, config

from helpers import VK_TEST_NAMESPACE, safe_delete

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

LIQO_TEST_NAMESPACE = VK_TEST_NAMESPACE


def _load_clients(kubeconfig_path):
    api_client = config.new_client_from_config(config_file=kubeconfig_path)
    return client.CoreV1Api(api_client), client.CustomObjectsApi(api_client)


@pytest.fixture
def cleanup():
    _callbacks = []

    def register(fn, *args, **kwargs):
        _callbacks.append((fn, args, kwargs))

    yield register

    for fn, args, kwargs in reversed(_callbacks):
        safe_delete(fn, *args, **kwargs)


@pytest.fixture(scope="session")
def liqo_tenant_clients():
    path = os.environ.get(
        "RHOAI3_KUBECONFIG", os.path.expanduser("~/.kube/tenant-rhoai3"),
    )
    return _load_clients(path)


@pytest.fixture(scope="session")
def liqo_worker_clients():
    path = os.environ.get(
        "WORKER_KUBECONFIG", os.path.expanduser("~/.kube/worker"),
    )
    return _load_clients(path)


@pytest.fixture(scope="session")
def liqo_test_namespace():
    return LIQO_TEST_NAMESPACE


@pytest.fixture(scope="session")
def liqo_worker_namespace():
    """Twin namespace on worker — same name with EnforceSameName strategy."""
    return LIQO_TEST_NAMESPACE


@pytest.fixture(scope="session")
def liqo_virtual_node(liqo_tenant_clients):
    """Discover the Liqo virtual node name on the tenant."""
    tenant_core, _ = liqo_tenant_clients
    deadline = time.time() + 120
    while time.time() < deadline:
        nodes = tenant_core.list_node(label_selector="liqo.io/type=virtual-node")
        if nodes.items:
            return nodes.items[0].metadata.name
        time.sleep(10)
    pytest.fail("No Liqo virtual node found within 120s")


@pytest.fixture(scope="session")
def liqo_virtual_node_ready(liqo_tenant_clients, liqo_virtual_node):
    """Wait for the Liqo virtual node to be Ready."""
    tenant_core, _ = liqo_tenant_clients
    deadline = time.time() + 120
    while time.time() < deadline:
        node = tenant_core.read_node(name=liqo_virtual_node)
        for cond in (node.status.conditions or []):
            if cond.type == "Ready" and cond.status == "True":
                return True
        time.sleep(10)
    pytest.fail(f"Liqo virtual node {liqo_virtual_node} not Ready within 120s")

import os
import time
import uuid

import pytest
import urllib3
from kubernetes import client, config

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


LIQO_OFFLOADING_GROUP = "offloading.liqo.io"
LIQO_OFFLOADING_VERSION = "v1beta1"


def _load_clients(kubeconfig_path):
    """Load a kubeconfig and return (CoreV1Api, CustomObjectsApi)."""
    api_client = config.new_client_from_config(config_file=kubeconfig_path)
    return client.CoreV1Api(api_client), client.CustomObjectsApi(api_client)


@pytest.fixture(scope="session")
def tenant_clients():
    """API clients for the tenant (RHOAI) cluster."""
    path = os.environ.get("TENANT_KUBECONFIG", os.path.expanduser("~/.kube/tenant"))
    return _load_clients(path)


@pytest.fixture(scope="session")
def worker_clients():
    """API clients for the worker (GPU) cluster."""
    path = os.environ.get("WORKER_KUBECONFIG", os.path.expanduser("~/.kube/worker"))
    return _load_clients(path)


@pytest.fixture
def offloaded_namespace(tenant_clients):
    """
    Create a namespace on the tenant with Liqo NamespaceOffloading enabled.

    Waits for offloading phase to reach Ready, then yields the namespace name.
    Deletes the namespace on teardown.
    """
    core, custom = tenant_clients
    ns_name = f"liqo-test-{uuid.uuid4().hex[:8]}"

    # Create namespace
    core.create_namespace(
        body=client.V1Namespace(metadata=client.V1ObjectMeta(name=ns_name))
    )

    # Create NamespaceOffloading resource
    offloading = {
        "apiVersion": f"{LIQO_OFFLOADING_GROUP}/{LIQO_OFFLOADING_VERSION}",
        "kind": "NamespaceOffloading",
        "metadata": {"name": "offloading", "namespace": ns_name},
        "spec": {
            "namespaceMappingStrategy": "EnforceSameName",
            "podOffloadingStrategy": "LocalAndRemote",
            "clusterSelector": {"nodeSelectorTerms": []},
        },
    }
    custom.create_namespaced_custom_object(
        group=LIQO_OFFLOADING_GROUP,
        version=LIQO_OFFLOADING_VERSION,
        namespace=ns_name,
        plural="namespaceoffloadings",
        body=offloading,
    )

    # Wait for offloading to be ready
    deadline = time.time() + 60
    while time.time() < deadline:
        obj = custom.get_namespaced_custom_object(
            group=LIQO_OFFLOADING_GROUP,
            version=LIQO_OFFLOADING_VERSION,
            namespace=ns_name,
            plural="namespaceoffloadings",
            name="offloading",
        )
        phase = obj.get("status", {}).get("offloadingPhase", "")
        if phase == "Ready":
            break
        time.sleep(3)
    else:
        raise TimeoutError(
            f"NamespaceOffloading in {ns_name} did not reach Ready within 60s"
        )

    yield ns_name

    # Cleanup
    try:
        core.delete_namespace(name=ns_name)
    except client.exceptions.ApiException as e:
        if e.status != 404:
            raise

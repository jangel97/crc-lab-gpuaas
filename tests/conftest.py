import os
import time

import pytest
import urllib3
from kubernetes import client, config

from lab_env import LabEnvironment, LabEnvironmentError

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

VK_TEST_NAMESPACE = "vk-test"


def _safe_delete(fn, *args, **kwargs):
    try:
        fn(*args, **kwargs)
    except Exception:
        pass


def _force_delete_pod(core_api, name, namespace, timeout=30):
    import time
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


@pytest.fixture
def cleanup():
    """Collect cleanup callbacks that run after the test, even on failure.

    Usage::

        def test_foo(cleanup, tenant_clients, ...):
            tenant_core, _ = tenant_clients
            tenant_core.create_namespaced_pod(namespace=ns, body=pod)
            cleanup(force_delete_pod, tenant_core, pod_name, ns)
            ...
    """
    _callbacks = []

    def register(fn, *args, **kwargs):
        _callbacks.append((fn, args, kwargs))

    yield register

    for fn, args, kwargs in reversed(_callbacks):
        _safe_delete(fn, *args, **kwargs)
CATAPULT_STORAGE_CLASS = "catapult"

VK_PREFIX_CONFIGMAP = "vk-gpu-provider-config-gpu-worker"
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
def ensure_catapult_storageclass(lab_env, tenant_clients):
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
def tenant2_clients():
    """API clients for the second tenant cluster (Kueue multi-tenant tests)."""
    path = os.environ.get("TENANT2_KUBECONFIG", os.path.expanduser("~/.kube/tenant2"))
    return _load_clients(path)


@pytest.fixture(scope="session")
def worker_namespace_prefix_t2(tenant2_clients):
    """Read the auto-generated worker namespace prefix from tenant2's VK ConfigMap."""
    tenant2_core, _ = tenant2_clients
    cm = tenant2_core.read_namespaced_config_map(
        name=VK_PREFIX_CONFIGMAP, namespace=VK_PREFIX_CONFIGMAP_NS,
    )
    prefix = cm.data.get("worker-namespace-prefix", "")
    assert prefix, (
        f"ConfigMap {VK_PREFIX_CONFIGMAP_NS}/{VK_PREFIX_CONFIGMAP} on tenant2 has no "
        f"worker-namespace-prefix key — is tenant2's VK running?"
    )
    return prefix


@pytest.fixture(scope="session")
def test_namespace():
    """Return the pre-provisioned VK test namespace on the tenant."""
    return VK_TEST_NAMESPACE


@pytest.fixture(scope="session")
def vk_worker_namespace(worker_namespace_prefix):
    """Return the per-tenant worker namespace for the default test namespace."""
    return worker_namespace_for_prefix(worker_namespace_prefix, VK_TEST_NAMESPACE)


@pytest.fixture(scope="session")
def vk_worker_namespace_t2(worker_namespace_prefix_t2):
    """Return tenant2's per-tenant worker namespace."""
    return worker_namespace_for_prefix(worker_namespace_prefix_t2, VK_TEST_NAMESPACE)


@pytest.fixture(scope="session")
def lab_env():
    """Lab VM manager. Starts worker + tenant1 as baseline."""
    try:
        env = LabEnvironment()
    except LabEnvironmentError as e:
        pytest.skip(f"Lab environment unavailable: {e}")
    env.ensure_running("sno-worker")
    env.ensure_running("sno-tenant")
    return env


@pytest.fixture(scope="session")
def single_tenant_env(lab_env):
    """Ensure tenant1 running at 14GB + worker running."""
    if lab_env.vm_memory_gb("sno-tenant") != 14:
        lab_env.set_memory("sno-tenant", 14)
        lab_env.start_vm("sno-tenant")
    else:
        lab_env.ensure_running("sno-tenant")
    lab_env.ensure_running("sno-worker")
    yield lab_env


@pytest.fixture(scope="session")
def high_memory_env(lab_env):
    """Shut down tenant2, resize tenant1 to 28GB, wait for OCP."""
    original_memory = lab_env.vm_memory_gb("sno-tenant")
    lab_env.ensure_shut_off("sno-tenant2")
    if lab_env.vm_memory_gb("sno-tenant") != 28:
        lab_env.set_memory("sno-tenant", 28)
        lab_env.start_vm("sno-tenant")
    else:
        lab_env.ensure_running("sno-tenant")
    lab_env.ensure_running("sno-worker")
    yield lab_env
    if original_memory != 28:
        lab_env.set_memory("sno-tenant", original_memory)
        lab_env.start_vm("sno-tenant")


@pytest.fixture(scope="session")
def dual_tenant_env(lab_env):
    """Ensure both tenants + worker running at default memory."""
    if lab_env.vm_memory_gb("sno-tenant") != 14:
        lab_env.set_memory("sno-tenant", 14)
        lab_env.start_vm("sno-tenant")
    else:
        lab_env.ensure_running("sno-tenant")
    lab_env.ensure_running("sno-tenant2")
    lab_env.ensure_running("sno-worker")
    yield lab_env


# ---------------------------------------------------------------------------
# Cluster readiness helpers
# ---------------------------------------------------------------------------


VK_NODE_NAME = "gpu-worker"
RHOAI_NAMESPACE = "redhat-ods-applications"


def wait_for_deployment_ready(api_client, name, namespace, timeout=300):
    """Wait for a deployment to have all replicas ready."""
    apps_api = client.AppsV1Api(api_client)
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            dep = apps_api.read_namespaced_deployment(name, namespace)
            ready = dep.status.ready_replicas or 0
            desired = dep.spec.replicas or 1
            if ready >= desired:
                return True
        except client.exceptions.ApiException:
            pass
        time.sleep(10)
    return False


def wait_for_webhook_endpoints(api_client, webhook_config_name, timeout=120):
    """Wait until all webhooks in a MutatingWebhookConfiguration have endpoints."""
    adm_api = client.AdmissionregistrationV1Api(api_client)
    core_api = client.CoreV1Api(api_client)
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            cfg = adm_api.read_mutating_webhook_configuration(webhook_config_name)
            all_have_endpoints = True
            for wh in cfg.webhooks:
                if wh.client_config.service:
                    svc_name = wh.client_config.service.name
                    svc_ns = wh.client_config.service.namespace
                    endpoints = core_api.read_namespaced_endpoints(svc_name, svc_ns)
                    has_addresses = any(
                        subset.addresses
                        for subset in (endpoints.subsets or [])
                    )
                    if not has_addresses:
                        all_have_endpoints = False
                        break
            if all_have_endpoints:
                return True
        except client.exceptions.ApiException:
            pass
        time.sleep(10)
    return False


@pytest.fixture(scope="session")
def vk_node_ready(tenant_clients):
    """Wait for the VK virtual node to be registered and Ready."""
    tenant_core, _ = tenant_clients
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
def rhoai_operators_ready(tenant_clients):
    """Wait for RHOAI operator deployments and webhook endpoints to be healthy."""
    tenant_core, _ = tenant_clients
    api_client = tenant_core.api_client

    for dep_name in (
        "kubeflow-training-operator",
        "kueue-controller-manager",
        "kserve-controller-manager",
    ):
        assert wait_for_deployment_ready(
            api_client, dep_name, RHOAI_NAMESPACE, timeout=300,
        ), f"{dep_name} not ready in {RHOAI_NAMESPACE} within 300s"

    for wh_name in (
        "training-operator.kubeflow.org",
        "kueue-mutating-webhook-configuration",
    ):
        try:
            if not wait_for_webhook_endpoints(api_client, wh_name, timeout=120):
                import warnings
                warnings.warn(f"Webhook {wh_name} endpoints not ready after 120s")
        except client.exceptions.ApiException:
            pass

    return True

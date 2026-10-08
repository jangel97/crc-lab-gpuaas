"""
RHOAI 3.x environment setup — runs once per test session.

Ensures Cert Manager, JobSet, and RHOAI 3.x operators are installed
and the DataScienceCluster is ready before any rhoai_3 test runs.
Every step is idempotent.

Prerequisite: OCP 4.19+ (one-time upgrade via scripts/setup-rhoai3-env.sh)
"""

import os
import time

import pytest
from kubernetes import client, config


RHOAI_OPERATOR_NS = "redhat-ods-operator"
RHOAI_APPS_NS = "redhat-ods-applications"
CERT_MANAGER_NS = "cert-manager-operator"
JOBSET_NS = "jobset-system"
MARKETPLACE_NS = "openshift-marketplace"

RHOAI_CHANNEL = "stable-3.5"


def _api_client():
    path = os.environ.get("TENANT2_KUBECONFIG", os.path.expanduser("~/.kube/tenant2"))
    return config.new_client_from_config(config_file=path)


def _ensure_namespace(core, name):
    try:
        core.create_namespace(
            body=client.V1Namespace(metadata=client.V1ObjectMeta(name=name))
        )
    except client.exceptions.ApiException as e:
        if e.status != 409:
            raise


def _ensure_operator_group(custom, namespace, name, target_namespaces=None):
    body = {
        "apiVersion": "operators.coreos.com/v1",
        "kind": "OperatorGroup",
        "metadata": {"name": name, "namespace": namespace},
    }
    if target_namespaces:
        body["spec"] = {"targetNamespaces": target_namespaces}
    try:
        custom.create_namespaced_custom_object(
            group="operators.coreos.com", version="v1",
            namespace=namespace, plural="operatorgroups", body=body,
        )
    except client.exceptions.ApiException as e:
        if e.status != 409:
            raise


def _ensure_subscription(custom, namespace, name, channel, operator_name, source="redhat-operators"):
    body = {
        "apiVersion": "operators.coreos.com/v1alpha1",
        "kind": "Subscription",
        "metadata": {"name": name, "namespace": namespace},
        "spec": {
            "channel": channel,
            "name": operator_name,
            "source": source,
            "sourceNamespace": MARKETPLACE_NS,
        },
    }
    try:
        custom.create_namespaced_custom_object(
            group="operators.coreos.com", version="v1alpha1",
            namespace=namespace, plural="subscriptions", body=body,
        )
    except client.exceptions.ApiException as e:
        if e.status == 409:
            existing = custom.get_namespaced_custom_object(
                group="operators.coreos.com", version="v1alpha1",
                namespace=namespace, plural="subscriptions", name=name,
            )
            if existing.get("spec", {}).get("channel") != channel:
                custom.patch_namespaced_custom_object(
                    group="operators.coreos.com", version="v1alpha1",
                    namespace=namespace, plural="subscriptions", name=name,
                    body={"spec": {"channel": channel}},
                )
        else:
            raise


def _wait_deployment(apps, name, namespace, timeout=600):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            dep = apps.read_namespaced_deployment(name, namespace)
            ready = dep.status.ready_replicas or 0
            desired = dep.spec.replicas or 1
            if ready >= desired:
                return True
        except client.exceptions.ApiException:
            pass
        time.sleep(15)
    return False


def _wait_csv_succeeded(custom, namespace, timeout=600):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            csvs = custom.list_namespaced_custom_object(
                group="operators.coreos.com", version="v1alpha1",
                namespace=namespace, plural="clusterserviceversions",
            )
            for csv in csvs.get("items", []):
                phase = csv.get("status", {}).get("phase", "")
                if phase == "Succeeded":
                    return csv["metadata"]["name"]
        except client.exceptions.ApiException:
            pass
        time.sleep(15)
    return None


def _ensure_catalog_sources_enabled(custom):
    """Re-enable default catalog sources if disabled by the provisioning playbook."""
    try:
        hub = custom.get_cluster_custom_object(
            group="config.openshift.io", version="v1",
            plural="operatorhubs", name="cluster",
        )
        if hub.get("spec", {}).get("disableAllDefaultSources", False):
            custom.patch_cluster_custom_object(
                group="config.openshift.io", version="v1",
                plural="operatorhubs", name="cluster",
                body={"spec": {"disableAllDefaultSources": False}},
            )
            time.sleep(20)
    except client.exceptions.ApiException:
        pass


@pytest.fixture(scope="session", autouse=True)
def rhoai3_environment():
    """Install Cert Manager, JobSet, and RHOAI 3.x operators. Idempotent."""
    api = _api_client()
    core = client.CoreV1Api(api)
    custom = client.CustomObjectsApi(api)
    apps = client.AppsV1Api(api)

    # Verify OCP >= 4.19
    version_api = client.VersionApi(api)
    info = version_api.get_code()
    try:
        cv = custom.get_cluster_custom_object(
            "config.openshift.io", "v1", "clusterversions", "version",
        )
        ocp_version = cv.get("status", {}).get("desired", {}).get("version", "unknown")
    except client.exceptions.ApiException:
        ocp_version = info.git_version

    major_minor = ".".join(ocp_version.split(".")[:2])
    if major_minor < "4.19":
        pytest.skip(
            f"OCP {ocp_version} < 4.19 — run scripts/setup-rhoai3-env.sh first"
        )

    _ensure_catalog_sources_enabled(custom)

    # ── Cert Manager ──
    print("\n==> Installing Cert Manager operator...")
    _ensure_namespace(core, CERT_MANAGER_NS)
    _ensure_operator_group(custom, CERT_MANAGER_NS, "cert-manager-operator",
                           target_namespaces=[CERT_MANAGER_NS])
    _ensure_subscription(custom, CERT_MANAGER_NS, "openshift-cert-manager-operator",
                         "stable-v1", "openshift-cert-manager-operator")

    csv = _wait_csv_succeeded(custom, CERT_MANAGER_NS, timeout=300)
    assert csv, "Cert Manager CSV did not reach Succeeded within 300s"
    print(f"    Cert Manager: {csv}")

    assert _wait_deployment(apps, "cert-manager", "cert-manager", timeout=300), \
        "cert-manager deployment not ready within 300s"

    # ── JobSet ──
    print("\n==> Installing JobSet operator...")
    _ensure_namespace(core, JOBSET_NS)
    _ensure_operator_group(custom, JOBSET_NS, "jobset-operator")
    _ensure_subscription(custom, JOBSET_NS, "jobset-operator",
                         "stable", "jobset-operator")

    csv = _wait_csv_succeeded(custom, JOBSET_NS, timeout=300)
    assert csv, "JobSet CSV did not reach Succeeded within 300s"
    print(f"    JobSet: {csv}")

    assert _wait_deployment(apps, "jobset-controller-manager", JOBSET_NS, timeout=300), \
        "jobset-controller-manager deployment not ready within 300s"

    # ── RHOAI 3.x ──
    print(f"\n==> Installing RHOAI 3.x operator (channel: {RHOAI_CHANNEL})...")
    _ensure_namespace(core, RHOAI_OPERATOR_NS)
    _ensure_operator_group(custom, RHOAI_OPERATOR_NS, "rhods-operator")
    _ensure_subscription(custom, RHOAI_OPERATOR_NS, "rhods-operator",
                         RHOAI_CHANNEL, "rhods-operator")

    csv = _wait_csv_succeeded(custom, RHOAI_OPERATOR_NS, timeout=600)
    assert csv, "RHOAI CSV did not reach Succeeded within 600s"
    print(f"    RHOAI: {csv}")

    assert _wait_deployment(apps, "rhods-operator", RHOAI_OPERATOR_NS, timeout=600), \
        "rhods-operator deployment not ready within 600s"

    # ── DSCInitialization + DataScienceCluster ──
    print("\n==> Creating DSCInitialization and DataScienceCluster...")

    dsci = {
        "apiVersion": "dscinitialization.opendatahub.io/v1",
        "kind": "DSCInitialization",
        "metadata": {"name": "default-dsci"},
        "spec": {"applicationsNamespace": RHOAI_APPS_NS},
    }
    try:
        custom.create_cluster_custom_object(
            group="dscinitialization.opendatahub.io", version="v1",
            plural="dscinitializations", body=dsci,
        )
    except client.exceptions.ApiException as e:
        if e.status != 409:
            raise

    dsc = {
        "apiVersion": "datasciencecluster.opendatahub.io/v1",
        "kind": "DataScienceCluster",
        "metadata": {"name": "default-dsc"},
        "spec": {
            "components": {
                "trainingoperator": {"managementState": "Managed"},
                "ray": {"managementState": "Managed"},
                "kserve": {"managementState": "Managed"},
                "dashboard": {"managementState": "Managed"},
                "workbenches": {"managementState": "Managed"},
            },
        },
    }
    try:
        custom.create_cluster_custom_object(
            group="datasciencecluster.opendatahub.io", version="v1",
            plural="datascienceclusters", body=dsc,
        )
    except client.exceptions.ApiException as e:
        if e.status != 409:
            raise

    # Wait for key RHOAI deployments
    print("\n==> Waiting for RHOAI components...")
    for dep_name in ("kubeflow-training-operator", "kserve-controller-manager"):
        ok = _wait_deployment(apps, dep_name, RHOAI_APPS_NS, timeout=600)
        status = "ready" if ok else "NOT READY"
        print(f"    {dep_name}: {status}")
        assert ok, f"{dep_name} not ready in {RHOAI_APPS_NS} within 600s"

    print("\n==> RHOAI 3.x environment ready")
    return api

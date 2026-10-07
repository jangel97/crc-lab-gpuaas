"""
Kueue admission control and cross-tenant preemption via Virtual Kubelet.

Tests that Kueue enforces GPU quota, priority-based admission, and preemption
when multiple tenants share the same GPU cluster through VK. VK is completely
Kueue-unaware — all admission logic lives on the worker via Kueue + Kyverno.

Flow:
  1. Tenant submits pod (nodeName=gpu-worker) → VK dispatches to worker
  2. Kyverno mutate-queue-name adds kueue.x-k8s.io/queue-name=default
  3. Kyverno mutate-priority adds kueue.x-k8s.io/priority-class from namespace label
  4. Kueue webhook adds scheduling gate → pod Pending until admitted
  5. Kueue admits (or preempts lower-priority pods) → gate removed → pod runs

Setup (automated by the kueue_setup fixture):
  - Installs Kueue and Kyverno on the worker cluster if not present
  - Deploys WorkloadPriorityClasses, Kyverno mutation policies, RBAC
  - Creates ClusterQueue with preemption policy
  - Patches Kueue webhook for reinvocation (so it sees Kyverno-added labels)

Lab setup for multi-tenant tests:
  The dual_tenant_env fixture automatically starts both tenant VMs.
  Prerequisites (one-time manual setup):
  1. Free memory by removing KServe/Serverless/ServiceMesh from tenants:
       oc patch datasciencecluster default-dsc --type=merge \\
         -p '{"spec":{"components":{"kserve":{"serving":{"managementState":"Removed"}}}}}'
       oc delete subscription serverless-operator -n openshift-serverless --ignore-not-found
       oc delete subscription servicemeshoperator -n openshift-operators --ignore-not-found
  2. Deploy VK on tenant2 (same deployment, points at same worker):
       oc apply -f deploy/vk-deployment.yaml   # on tenant2
"""

import os
import tempfile
import time
import pytest
import yaml
from kubernetes import client, utils

from conftest import worker_pod_name


VK_NODE_NAME = "gpu-worker"
VK_TOLERATIONS = [{"key": "virtual-kubelet.io/provider", "operator": "Exists"}]


@pytest.fixture(autouse=True, scope="session")
def _ensure_lab_env(dual_tenant_env):
    pass
CUDA_IMAGE = "nvcr.io/nvidia/cuda:12.8.1-base-ubi9"

KUEUE_GROUP = "kueue.x-k8s.io"
KUEUE_VERSION = "v1beta1"
QUEUE_NAME_LABEL = "kueue.x-k8s.io/queue-name"
PRIORITY_LABEL = "kueue.x-k8s.io/priority-class"
KUEUE_GATE = "kueue.x-k8s.io/admission"

KUEUE_MANIFEST_URL = os.environ.get(
    "KUEUE_MANIFEST_URL",
    "https://github.com/kubernetes-sigs/kueue/releases/latest/download/manifests.yaml",
)
KYVERNO_MANIFEST_URL = os.environ.get(
    "KYVERNO_MANIFEST_URL",
    "https://github.com/kyverno/kyverno/releases/latest/download/install.yaml",
)

POLICY_DIR = os.path.join(
    os.path.dirname(__file__), "..", "..", "kueue-gpu-policy", "deployment", "policies",
)

KUEUE_DEPLOY_DIR = os.path.join(os.path.dirname(__file__), "..", "deploy")


# ---------------------------------------------------------------------------
# Setup helpers
# ---------------------------------------------------------------------------


def apply_manifest_url(api_client, url, description="manifest"):
    """Download a YAML manifest from URL and create all resources."""
    import urllib.request

    print(f"  Downloading {description} from {url}...")
    try:
        with urllib.request.urlopen(url, timeout=60) as resp:
            content = resp.read().decode("utf-8")
    except Exception as e:
        pytest.skip(f"Cannot download {description}: {e}")

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".yaml", delete=False,
    ) as f:
        f.write(content)
        tmp_path = f.name

    try:
        utils.create_from_yaml(api_client, tmp_path, verbose=False)
        print(f"  {description} applied successfully")
    except utils.FailToCreateError as e:
        non_conflict = [
            exc for exc in e.api_exceptions if exc.status != 409
        ]
        if non_conflict:
            print(f"  {description}: {len(e.api_exceptions)} errors, "
                  f"{len(non_conflict)} non-conflict")
            raise non_conflict[0]
        print(f"  {description} already installed (all resources exist)")
    finally:
        os.unlink(tmp_path)


def apply_local_yaml(api_client, file_path, description="resource"):
    """Apply a local YAML file, ignoring AlreadyExists."""
    try:
        utils.create_from_yaml(api_client, file_path, verbose=False)
        print(f"  {description} applied")
    except utils.FailToCreateError as e:
        non_conflict = [
            exc for exc in e.api_exceptions if exc.status != 409
        ]
        if non_conflict:
            raise non_conflict[0]
        print(f"  {description} already exists")


def apply_yaml_dict(custom_api, body, description="resource"):
    """Create a single resource from a dict, ignoring AlreadyExists."""
    api_version = body.get("apiVersion", "")
    kind = body.get("kind", "")
    name = body.get("metadata", {}).get("name", "")
    namespace = body.get("metadata", {}).get("namespace")

    parts = api_version.split("/")
    if len(parts) == 2:
        group, version = parts
    else:
        group, version = "", parts[0]

    plural_map = {
        "WorkloadPriorityClass": "workloadpriorityclasses",
        "ClusterQueue": "clusterqueues",
        "ResourceFlavor": "resourceflavors",
        "LocalQueue": "localqueues",
        "MutatingPolicy": "mutatingpolicies",
        "ClusterRole": "clusterroles",
        "ClusterRoleBinding": "clusterrolebindings",
    }
    plural = plural_map.get(kind)
    if not plural:
        print(f"  Skipping unknown kind {kind}")
        return

    try:
        if namespace:
            custom_api.create_namespaced_custom_object(
                group=group, version=version,
                namespace=namespace, plural=plural, body=body,
            )
        else:
            custom_api.create_cluster_custom_object(
                group=group, version=version, plural=plural, body=body,
            )
        print(f"  Created {kind}/{name}")
    except client.exceptions.ApiException as e:
        if e.status == 409:
            spec = body.get("spec")
            if spec and kind in ("ClusterQueue", "ResourceFlavor"):
                if namespace:
                    custom_api.patch_namespaced_custom_object(
                        group=group, version=version,
                        namespace=namespace, plural=plural,
                        name=name, body={"spec": spec},
                    )
                else:
                    custom_api.patch_cluster_custom_object(
                        group=group, version=version,
                        plural=plural, name=name, body={"spec": spec},
                    )
                print(f"  Updated {kind}/{name}")
            else:
                print(f"  {kind}/{name} already exists")
        else:
            raise


def wait_deployment_ready(apps_api, name, namespace, timeout=300):
    """Wait for a deployment to have all replicas ready."""
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


def crd_exists(api_client, crd_name):
    """Check if a CRD is registered on the cluster."""
    api_ext = client.ApiextensionsV1Api(api_client)
    try:
        api_ext.read_custom_resource_definition(crd_name)
        return True
    except client.exceptions.ApiException:
        return False


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def kueue_setup(worker_clients):
    """
    Install Kueue + Kyverno on worker if missing, deploy policies,
    create ClusterQueue with preemption, and patch Kueue webhook.
    """
    worker_core, worker_custom = worker_clients
    api_client = worker_core.api_client
    apps_api = client.AppsV1Api(api_client)

    # --- Step 1: Install Kueue ---
    if not crd_exists(api_client, "clusterqueues.kueue.x-k8s.io"):
        print("Installing Kueue on worker cluster...")
        apply_manifest_url(api_client, KUEUE_MANIFEST_URL, "Kueue")

        assert wait_deployment_ready(
            apps_api, "kueue-controller-manager", "kueue-system", timeout=300,
        ), "Kueue controller did not become ready within 5 min"
        print("  Kueue controller is ready")
        time.sleep(5)
    else:
        print("Kueue already installed on worker")

    # --- Step 2: Install Kyverno ---
    kyverno_just_installed = False
    if not crd_exists(api_client, "mutatingpolicies.policies.kyverno.io"):
        print("Installing Kyverno on worker cluster...")
        apply_manifest_url(api_client, KYVERNO_MANIFEST_URL, "Kyverno")
        kyverno_just_installed = True

    # Grant privileged SCC to Kyverno service accounts. On OpenShift,
    # restricted-v2 rejects Kyverno pods (UID 65534 + seccomp annotations).
    # The anyuid SCC doesn't allow seccomp, so privileged is needed.
    # We patch the SCC users list directly (RBAC-based ClusterRoleBinding
    # doesn't work reliably on all OpenShift versions).
    kyverno_sas = [
        f"system:serviceaccount:kyverno:{sa}"
        for sa in (
            "kyverno-admission-controller",
            "kyverno-background-controller",
            "kyverno-cleanup-controller",
            "kyverno-reports-controller",
        )
    ]
    needs_restart = False
    try:
        scc = worker_custom.get_cluster_custom_object(
            group="security.openshift.io", version="v1",
            plural="securitycontextconstraints", name="privileged",
        )
        existing_users = scc.get("users", []) or []
        missing = [sa for sa in kyverno_sas if sa not in existing_users]
        if missing:
            print(f"  Adding {len(missing)} Kyverno SAs to privileged SCC...")
            worker_custom.patch_cluster_custom_object(
                group="security.openshift.io", version="v1",
                plural="securitycontextconstraints", name="privileged",
                body={"users": existing_users + missing},
            )
            needs_restart = True
        else:
            print("  Kyverno SAs already in privileged SCC")
    except client.exceptions.ApiException as e:
        if e.status == 404:
            print("  privileged SCC not found — not an OpenShift cluster, skipping")
        else:
            raise

    if needs_restart or kyverno_just_installed:
        print("  Restarting Kyverno deployments...")
        for dep_name in (
            "kyverno-admission-controller",
            "kyverno-background-controller",
            "kyverno-cleanup-controller",
            "kyverno-reports-controller",
        ):
            try:
                apps_api.patch_namespaced_deployment_scale(
                    dep_name, "kyverno", {"spec": {"replicas": 0}},
                )
            except client.exceptions.ApiException:
                pass
        time.sleep(5)
        for dep_name in (
            "kyverno-admission-controller",
            "kyverno-background-controller",
            "kyverno-cleanup-controller",
            "kyverno-reports-controller",
        ):
            try:
                apps_api.patch_namespaced_deployment_scale(
                    dep_name, "kyverno", {"spec": {"replicas": 1}},
                )
            except client.exceptions.ApiException:
                pass

    if kyverno_just_installed or not wait_deployment_ready(
        apps_api, "kyverno-admission-controller", "kyverno", timeout=10,
    ):
        assert wait_deployment_ready(
            apps_api, "kyverno-admission-controller", "kyverno", timeout=300,
        ), "Kyverno admission controller did not become ready within 5 min"
    print("  Kyverno controller is ready")
    time.sleep(5)

    # --- Step 3: Apply WorkloadPriorityClasses ---
    print("Applying WorkloadPriorityClasses...")
    wpc_file = os.path.join(POLICY_DIR, "workload-priority-classes.yaml")
    if os.path.exists(wpc_file):
        with open(wpc_file) as f:
            for doc in yaml.safe_load_all(f):
                if doc:
                    apply_yaml_dict(worker_custom, doc, "WorkloadPriorityClass")
    else:
        pytest.skip(
            f"WorkloadPriorityClasses file not found: {wpc_file}. "
            f"Clone kueue-gpu-policy repo as a sibling directory."
        )

    # --- Step 4: Apply Kyverno RBAC for namespace lookups ---
    print("Applying Kyverno RBAC...")
    rbac_file = os.path.join(POLICY_DIR, "rbac.yaml")
    if os.path.exists(rbac_file):
        apply_local_yaml(api_client, rbac_file, "Kyverno RBAC")

    # --- Step 5: Apply Kyverno mutation policies ---
    print("Applying Kyverno mutation policies...")
    for policy_name in ("mutate-queue-name.yaml", "mutate-priority.yaml"):
        policy_file = os.path.join(POLICY_DIR, policy_name)
        if os.path.exists(policy_file):
            with open(policy_file) as f:
                doc = yaml.safe_load(f)
                if doc:
                    apply_yaml_dict(worker_custom, doc, policy_name)
        else:
            pytest.skip(f"Kyverno policy not found: {policy_file}")

    # --- Step 6: Apply ClusterQueue with preemption ---
    print("Applying ClusterQueue with preemption...")
    cq_file = os.path.join(KUEUE_DEPLOY_DIR, "worker-kueue-multi-tenant.yml")
    if os.path.exists(cq_file):
        with open(cq_file) as f:
            for doc in yaml.safe_load_all(f):
                if doc:
                    apply_yaml_dict(worker_custom, doc)
    else:
        pytest.skip(f"ClusterQueue manifest not found: {cq_file}")

    # --- Step 7: Ensure Kueue webhook exists and has reinvocation ---
    print("Checking Kueue webhook...")
    adm_api = client.AdmissionregistrationV1Api(api_client)
    try:
        webhook = adm_api.read_mutating_webhook_configuration(
            "kueue-mutating-webhook-configuration",
        )
    except client.exceptions.ApiException as e:
        if e.status == 404:
            print("  Kueue webhook missing — re-applying from manifest...")
            import urllib.request
            with urllib.request.urlopen(KUEUE_MANIFEST_URL, timeout=60) as resp:
                content = resp.read().decode("utf-8")
            webhook_kinds = ("MutatingWebhookConfiguration",
                             "ValidatingWebhookConfiguration")
            for doc in yaml.safe_load_all(content):
                if doc and doc.get("kind") in webhook_kinds:
                    apply_yaml_dict(worker_custom, doc, doc["kind"])
            webhook = adm_api.read_mutating_webhook_configuration(
                "kueue-mutating-webhook-configuration",
            )
        else:
            raise

    needs_patch = any(
        wh.reinvocation_policy != "IfNeeded" for wh in webhook.webhooks
    )
    if needs_patch:
        for wh in webhook.webhooks:
            wh.reinvocation_policy = "IfNeeded"
        adm_api.replace_mutating_webhook_configuration(
            "kueue-mutating-webhook-configuration", webhook,
        )
        print("  Patched Kueue webhook reinvocationPolicy=IfNeeded")
    else:
        print("  Kueue webhook already has reinvocationPolicy=IfNeeded")

    print("Kueue setup complete.")
    return True


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------


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


def wait_pod_exists(core_api, name, namespace, timeout=120):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            core_api.read_namespaced_pod(name=name, namespace=namespace)
            return True
        except client.exceptions.ApiException:
            pass
        time.sleep(3)
    return False


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


def wait_pod_admitted(core_api, name, namespace, timeout=120):
    """Wait until Kueue removes the scheduling gate (pod is admitted)."""
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


def ensure_namespace_labels(core_api, namespace, priority_class):
    """Patch worker namespace with labels required by Kyverno policies."""
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
    """Create LocalQueue named 'default' pointing to cluster-queue."""
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


def make_gpu_pod(name, namespace, command, restart_policy="Never"):
    """Create a pod spec targeting the VK node with 1 GPU."""
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


@pytest.mark.kueue
def test_kueue_admits_gpu_workload(
    cleanup,
    kueue_setup,
    tenant_clients, worker_clients, test_namespace, vk_worker_namespace,
):
    """
    Submit a GPU pod from tenant → VK dispatches to worker → Kyverno labels
    it → Kueue gates then admits → pod runs → VK syncs status back.

    Proves the full Kyverno + Kueue admission pipeline works with VK.
    """
    tenant_core, _ = tenant_clients
    worker_core, worker_custom = worker_clients
    ns = test_namespace
    w_ns = vk_worker_namespace

    # Setup: label worker namespace + create LocalQueue
    ensure_namespace_labels(worker_core, w_ns, "gpuaas-standard")
    ensure_local_queue(worker_custom, w_ns)

    pod_name = "kueue-admit-test"
    w_pod = worker_pod_name(ns, pod_name)

    # Cleanup
    force_delete_pod(tenant_core, pod_name, ns)
    force_delete_pod(worker_core, w_pod, w_ns)

    # Create tenant pod — nvidia-smi, runs and exits
    tenant_core.create_namespaced_pod(
        namespace=ns,
        body=make_gpu_pod(pod_name, ns, ["nvidia-smi"]),
    )
    cleanup(force_delete_pod, tenant_core, pod_name, ns)

    # Wait for worker pod
    assert wait_pod_exists(worker_core, w_pod, w_ns, timeout=60), (
        f"Worker pod {w_pod} did not appear in {w_ns} within 60s"
    )

    # Verify Kyverno added Kueue labels
    wp = worker_core.read_namespaced_pod(w_pod, w_ns)
    labels = wp.metadata.labels or {}
    assert labels.get(QUEUE_NAME_LABEL) == "default", (
        f"Kyverno did not add queue-name label. Labels: {labels}. "
        f"Check mutate-queue-name policy and namespace labels on {w_ns}."
    )
    assert labels.get(PRIORITY_LABEL) == "gpuaas-standard", (
        f"Kyverno did not add priority label. Labels: {labels}. "
        f"Check mutate-priority policy and namespace label "
        f"gpuaas.redhat.com/default-priority on {w_ns}."
    )

    # Wait for Kueue to admit (remove scheduling gate)
    assert wait_pod_admitted(worker_core, w_pod, w_ns, timeout=120), (
        f"Kueue did not admit pod {w_pod} within 120s. "
        f"Check Kueue webhook reinvocationPolicy and ClusterQueue quota."
    )

    # Wait for completion
    worker_phase = wait_pod_phase(
        worker_core, w_pod, w_ns, ("Succeeded", "Failed"), timeout=300,
    )
    assert worker_phase == "Succeeded", (
        f"GPU pod did not succeed. Phase: {worker_phase!r}"
    )

    # Verify tenant status synced
    tenant_phase = wait_pod_phase(
        tenant_core, pod_name, ns, ("Succeeded",), timeout=60,
    )
    assert tenant_phase == "Succeeded", (
        f"Tenant pod status not synced. Phase: {tenant_phase!r}"
    )


@pytest.mark.kueue
def test_kueue_queues_when_full(
    cleanup,
    kueue_setup,
    tenant_clients, worker_clients, test_namespace, vk_worker_namespace,
):
    """
    Submit two GPU pods when only 1 GPU is available. Verify the second
    pod is queued (scheduling gate) until the first completes.

    Proves Kueue quota enforcement works with VK-dispatched pods.
    """
    tenant_core, _ = tenant_clients
    worker_core, worker_custom = worker_clients
    ns = test_namespace
    w_ns = vk_worker_namespace

    ensure_namespace_labels(worker_core, w_ns, "gpuaas-standard")
    ensure_local_queue(worker_custom, w_ns)

    pod_a = "kueue-queue-a"
    pod_b = "kueue-queue-b"
    w_pod_a = worker_pod_name(ns, pod_a)
    w_pod_b = worker_pod_name(ns, pod_b)

    # Cleanup
    for p, w in [(pod_a, w_pod_a), (pod_b, w_pod_b)]:
        force_delete_pod(tenant_core, p, ns)
        force_delete_pod(worker_core, w, w_ns)
    time.sleep(3)

    # Pod A: long-running, holds the GPU
    tenant_core.create_namespaced_pod(
        namespace=ns,
        body=make_gpu_pod(pod_a, ns, ["sleep", "300"]),
    )
    cleanup(force_delete_pod, tenant_core, pod_a, ns)

    assert wait_pod_exists(worker_core, w_pod_a, w_ns, timeout=60), (
        f"Worker pod A {w_pod_a} did not appear within 60s"
    )
    assert wait_pod_admitted(worker_core, w_pod_a, w_ns, timeout=120), (
        f"Kueue did not admit pod A within 120s"
    )

    phase_a = wait_pod_phase(
        worker_core, w_pod_a, w_ns, ("Running",), timeout=300,
    )
    assert phase_a == "Running", (
        f"Pod A not Running. Phase: {phase_a!r}"
    )

    # Pod B: should be queued (GPU quota exhausted)
    tenant_core.create_namespaced_pod(
        namespace=ns,
        body=make_gpu_pod(pod_b, ns, ["nvidia-smi"]),
    )
    cleanup(force_delete_pod, tenant_core, pod_b, ns)

    assert wait_pod_exists(worker_core, w_pod_b, w_ns, timeout=60), (
        f"Worker pod B {w_pod_b} did not appear within 60s"
    )

    # Verify pod B has scheduling gate (queued by Kueue)
    time.sleep(5)
    wp_b = worker_core.read_namespaced_pod(w_pod_b, w_ns)
    gates = wp_b.spec.scheduling_gates or []
    has_gate = any(g.name == KUEUE_GATE for g in gates)
    assert has_gate, (
        f"Pod B should have Kueue scheduling gate (GPU quota exhausted). "
        f"Gates: {[g.name for g in gates]}. "
        f"Pod B phase: {wp_b.status.phase}."
    )

    # Verify tenant pod B is Pending (VK syncs gated status)
    deadline = time.time() + 30
    tenant_b_phase = None
    while time.time() < deadline:
        tp = tenant_core.read_namespaced_pod(pod_b, ns)
        tenant_b_phase = tp.status.phase
        if tenant_b_phase == "Pending":
            break
        time.sleep(3)
    assert tenant_b_phase == "Pending", (
        f"Tenant pod B should show Pending (queued). Phase: {tenant_b_phase!r}"
    )

    # Delete pod A → frees GPU → Kueue should admit pod B
    force_delete_pod(tenant_core, pod_a, ns)

    assert wait_pod_admitted(worker_core, w_pod_b, w_ns, timeout=120), (
        f"Kueue did not admit pod B after pod A was deleted"
    )

    worker_phase = wait_pod_phase(
        worker_core, w_pod_b, w_ns, ("Succeeded", "Failed"), timeout=300,
    )
    assert worker_phase == "Succeeded", (
        f"Pod B did not succeed after admission. Phase: {worker_phase!r}"
    )

    tenant_phase = wait_pod_phase(
        tenant_core, pod_b, ns, ("Succeeded",), timeout=60,
    )
    assert tenant_phase == "Succeeded", (
        f"Tenant pod B status not synced. Phase: {tenant_phase!r}"
    )


@pytest.mark.kueue
def test_kueue_preemption_cross_tenant(
    cleanup,
    kueue_setup,
    tenant_clients, tenant2_clients, worker_clients,
    test_namespace, vk_worker_namespace, vk_worker_namespace_t2,
):
    """
    Cross-tenant preemption: tenant1 (opportunistic priority) holds the GPU,
    tenant2 (production priority) submits → Kueue preempts tenant1.

    Proves:
      - Cross-tenant priority preemption through shared ClusterQueue
      - Priority is namespace-scoped (admin labels, not tenant-controlled)
      - Kueue preemption deletes worker pods
      - VK detects the deletion and transitions tenant1's pod to Failed
    """
    t1_core, _ = tenant_clients
    t2_core, _ = tenant2_clients
    worker_core, worker_custom = worker_clients
    ns = test_namespace
    w_ns_t1 = vk_worker_namespace
    w_ns_t2 = vk_worker_namespace_t2

    # Setup: label tenant1's worker ns as low priority, tenant2's as high
    ensure_namespace_labels(worker_core, w_ns_t1, "gpuaas-opportunistic")
    ensure_namespace_labels(worker_core, w_ns_t2, "gpuaas-production")
    ensure_local_queue(worker_custom, w_ns_t1)
    ensure_local_queue(worker_custom, w_ns_t2)

    t1_pod = "kueue-preempt-low"
    t2_pod = "kueue-preempt-high"
    w_pod_t1 = worker_pod_name(ns, t1_pod)
    w_pod_t2 = worker_pod_name(ns, t2_pod)

    # Cleanup
    force_delete_pod(t1_core, t1_pod, ns)
    force_delete_pod(t2_core, t2_pod, ns)
    force_delete_pod(worker_core, w_pod_t1, w_ns_t1)
    force_delete_pod(worker_core, w_pod_t2, w_ns_t2)
    time.sleep(3)

    # Step 1: Tenant1 submits low-priority GPU pod (long-running)
    t1_core.create_namespaced_pod(
        namespace=ns,
        body=make_gpu_pod(t1_pod, ns, ["sleep", "600"]),
    )
    cleanup(force_delete_pod, t1_core, t1_pod, ns)
    cleanup(force_delete_pod, worker_core, w_pod_t1, w_ns_t1)

    assert wait_pod_exists(worker_core, w_pod_t1, w_ns_t1, timeout=60), (
        f"Tenant1 worker pod {w_pod_t1} did not appear in {w_ns_t1}"
    )
    assert wait_pod_admitted(worker_core, w_pod_t1, w_ns_t1, timeout=120), (
        f"Kueue did not admit tenant1's pod within 120s"
    )

    phase_t1 = wait_pod_phase(
        worker_core, w_pod_t1, w_ns_t1, ("Running",), timeout=300,
    )
    assert phase_t1 == "Running", (
        f"Tenant1 pod not Running. Phase: {phase_t1!r}"
    )

    # Verify tenant1 pod shows Running on tenant cluster
    t1_tenant_phase = wait_pod_phase(
        t1_core, t1_pod, ns, ("Running",), timeout=60,
    )
    assert t1_tenant_phase == "Running", (
        f"Tenant1 pod status not synced to Running. Phase: {t1_tenant_phase!r}"
    )

    # Verify priority labels
    wp = worker_core.read_namespaced_pod(w_pod_t1, w_ns_t1)
    assert wp.metadata.labels.get(PRIORITY_LABEL) == "gpuaas-opportunistic", (
        f"Tenant1 worker pod has wrong priority label: "
        f"{wp.metadata.labels.get(PRIORITY_LABEL)}"
    )

    # Step 2: Tenant2 submits high-priority GPU pod
    t2_core.create_namespaced_pod(
        namespace=ns,
        body=make_gpu_pod(t2_pod, ns, ["nvidia-smi"]),
    )
    cleanup(force_delete_pod, t2_core, t2_pod, ns)
    cleanup(force_delete_pod, worker_core, w_pod_t2, w_ns_t2)

    assert wait_pod_exists(worker_core, w_pod_t2, w_ns_t2, timeout=60), (
        f"Tenant2 worker pod {w_pod_t2} did not appear in {w_ns_t2}"
    )

    # Verify tenant2's priority
    wp2 = worker_core.read_namespaced_pod(w_pod_t2, w_ns_t2)
    assert wp2.metadata.labels.get(PRIORITY_LABEL) == "gpuaas-production", (
        f"Tenant2 worker pod has wrong priority label: "
        f"{wp2.metadata.labels.get(PRIORITY_LABEL)}"
    )

    # Step 3: Kueue should preempt tenant1's pod (delete it)
    assert wait_pod_deleted(worker_core, w_pod_t1, w_ns_t1, timeout=120), (
        f"Kueue did not preempt (delete) tenant1's worker pod {w_pod_t1} "
        f"within 120s. Check ClusterQueue preemption policy "
        f"(withinClusterQueue: LowerPriority)."
    )

    # Step 4: Tenant2's pod should be admitted and run
    assert wait_pod_admitted(worker_core, w_pod_t2, w_ns_t2, timeout=120), (
        f"Kueue did not admit tenant2's pod after preempting tenant1"
    )

    phase_t2 = wait_pod_phase(
        worker_core, w_pod_t2, w_ns_t2, ("Succeeded", "Failed"), timeout=300,
    )
    assert phase_t2 == "Succeeded", (
        f"Tenant2 GPU pod did not succeed. Phase: {phase_t2!r}"
    )

    # Step 5: Verify tenant1 pod transitions to Failed after preemption.
    # VK's worker informer DeleteFunc detects the deletion and updates the
    # tenant pod status to Failed/WorkerPodPreempted.
    t1_tenant_phase_after = wait_pod_phase(
        t1_core, t1_pod, ns, ("Failed",), timeout=60,
    )
    assert t1_tenant_phase_after == "Failed", (
        f"Tenant1 pod should show Failed after preemption. "
        f"Phase: {t1_tenant_phase_after!r}"
    )

    # Verify tenant2 status synced
    t2_tenant_phase = wait_pod_phase(
        t2_core, t2_pod, ns, ("Succeeded",), timeout=60,
    )
    assert t2_tenant_phase == "Succeeded", (
        f"Tenant2 pod status not synced. Phase: {t2_tenant_phase!r}"
    )

"""
Virtual Kubelet GPU workload dispatch across federated clusters.

Architecture under test
-----------------------
  tenant cluster                      worker cluster (GPU node)
  +-----------------------------+    +-----------------------------+
  | VK Deployment               |    | GPU Operator (RTX 5090)     |
  |   registers virtual node    |    | Kueue (local quota mgmt)    |
  |   "gpu-worker"              |    | vk-workloads namespace      |
  |                              |    |   synced secrets/cms/sas    |
  | Scheduler assigns pods to   +--->|   created pods (real)       |
  |   virtual node              |    |                             |
  | VK syncs resources + pod    |    |                             |
  | VK syncs status back        |    |                             |
  +-----------------------------+    +-----------------------------+

Users submit pods on the tenant with nodeName: gpu-worker.
VK syncs referenced resources to the worker, creates the pod in
vk-workloads namespace, and syncs status back to the tenant.
"""

import time

import pytest
from kubernetes import client

from conftest import worker_pod_name, worker_namespace_for_prefix, execution_pvc_name, CATAPULT_STORAGE_CLASS


VK_NODE_NAME = "gpu-worker"


def force_delete_pod(core_api, name, namespace, timeout=60):
    """Delete a pod with grace_period=0 and wait for it to disappear."""
    try:
        core_api.delete_namespaced_pod(
            name=name,
            namespace=namespace,
            grace_period_seconds=0,
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


@pytest.mark.vk
def test_virtual_node_exists(tenant_clients):
    """Verify virtual node gpu-worker exists with GPU capacity."""
    tenant_core, _ = tenant_clients

    node = tenant_core.read_node(name=VK_NODE_NAME)

    assert node.metadata.labels.get("type") == "virtual-kubelet", (
        f"Node {VK_NODE_NAME} missing type=virtual-kubelet label"
    )

    alloc = node.status.allocatable or {}
    assert "nvidia.com/gpu" in alloc, (
        f"Node {VK_NODE_NAME} has no nvidia.com/gpu in allocatable: {alloc}"
    )

    ready = next(
        (c for c in (node.status.conditions or []) if c.type == "Ready"), None
    )
    assert ready is not None and ready.status == "True", (
        f"Node {VK_NODE_NAME} is not Ready"
    )


@pytest.mark.vk
def test_gpu_pod_dispatched_via_vk(
    tenant_clients, worker_clients, test_namespace, vk_worker_namespace
):
    """
    Submit a GPU pod on the tenant targeting the virtual node.
    Verify it appears on the worker and runs nvidia-smi successfully.
    """
    tenant_core, _ = tenant_clients
    worker_core, _ = worker_clients
    ns = test_namespace

    pod_name = "vk-gpu-test"
    w_pod_name = worker_pod_name(ns, pod_name)

    # Cleanup from previous runs
    force_delete_pod(tenant_core, pod_name, ns)
    force_delete_pod(worker_core, w_pod_name, vk_worker_namespace)

    pod = client.V1Pod(
        metadata=client.V1ObjectMeta(
            name=pod_name,
            namespace=ns,
        ),
        spec=client.V1PodSpec(
            node_name=VK_NODE_NAME,
            restart_policy="Never",
            tolerations=[
                client.V1Toleration(
                    key="virtual-kubelet.io/provider",
                    operator="Exists",
                ),
            ],
            containers=[
                client.V1Container(
                    name="gpu",
                    image="nvcr.io/nvidia/cuda:12.8.0-base-ubi9",
                    command=["nvidia-smi"],
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
    tenant_core.create_namespaced_pod(namespace=ns, body=pod)

    # Wait for pod to appear on worker
    deadline = time.time() + 60
    worker_pod_found = False
    while time.time() < deadline:
        try:
            worker_core.read_namespaced_pod(
                name=w_pod_name, namespace=vk_worker_namespace
            )
            worker_pod_found = True
            break
        except client.exceptions.ApiException:
            pass
        time.sleep(3)

    assert worker_pod_found, (
        f"Pod {w_pod_name} did not appear on worker in {vk_worker_namespace} within 60s"
    )

    # Wait for worker pod to complete
    deadline = time.time() + 600
    worker_phase = None
    while time.time() < deadline:
        try:
            wp = worker_core.read_namespaced_pod(
                name=w_pod_name, namespace=vk_worker_namespace
            )
            worker_phase = wp.status.phase
            if worker_phase in ("Succeeded", "Failed"):
                break
        except client.exceptions.ApiException:
            pass
        time.sleep(10)

    assert worker_phase == "Succeeded", (
        f"Worker pod did not succeed. Phase: {worker_phase!r}"
    )

    # Verify tenant pod status synced
    deadline = time.time() + 60
    tenant_phase = None
    while time.time() < deadline:
        tp = tenant_core.read_namespaced_pod(name=pod_name, namespace=ns)
        tenant_phase = tp.status.phase
        if tenant_phase == "Succeeded":
            break
        time.sleep(5)

    assert tenant_phase == "Succeeded", (
        f"Tenant pod status not synced. Phase: {tenant_phase!r}"
    )

    # Verify exit code
    tp = tenant_core.read_namespaced_pod(name=pod_name, namespace=ns)
    if tp.status.container_statuses:
        cstatus = tp.status.container_statuses[0]
        if cstatus.state and cstatus.state.terminated:
            assert cstatus.state.terminated.exit_code == 0, (
                f"nvidia-smi exited with {cstatus.state.terminated.exit_code}"
            )

    # Cleanup
    try:
        tenant_core.delete_namespaced_pod(name=pod_name, namespace=ns)
    except client.exceptions.ApiException:
        pass


@pytest.mark.vk
def test_resource_sync(
    tenant_clients, worker_clients, test_namespace, vk_worker_namespace
):
    """
    Create a Secret and ConfigMap on tenant, submit a pod that references
    both, verify they get synced to the worker.
    """
    tenant_core, _ = tenant_clients
    worker_core, _ = worker_clients
    ns = test_namespace

    secret_name = "vk-test-secret"
    cm_name = "vk-test-config"
    pod_name = "vk-resource-sync-test"
    w_pod_name = worker_pod_name(ns, pod_name)

    # Create Secret on tenant
    try:
        tenant_core.delete_namespaced_secret(name=secret_name, namespace=ns)
    except client.exceptions.ApiException:
        pass
    tenant_core.create_namespaced_secret(
        namespace=ns,
        body=client.V1Secret(
            metadata=client.V1ObjectMeta(name=secret_name, namespace=ns),
            string_data={"MY_SECRET": "hello-from-tenant"},
        ),
    )

    # Create ConfigMap on tenant
    try:
        tenant_core.delete_namespaced_config_map(name=cm_name, namespace=ns)
    except client.exceptions.ApiException:
        pass
    tenant_core.create_namespaced_config_map(
        namespace=ns,
        body=client.V1ConfigMap(
            metadata=client.V1ObjectMeta(name=cm_name, namespace=ns),
            data={"config.txt": "value-from-tenant"},
        ),
    )

    # Cleanup pod from previous runs
    force_delete_pod(tenant_core, pod_name, ns)
    force_delete_pod(worker_core, w_pod_name, vk_worker_namespace)

    pod = client.V1Pod(
        metadata=client.V1ObjectMeta(name=pod_name, namespace=ns),
        spec=client.V1PodSpec(
            node_name=VK_NODE_NAME,
            restart_policy="Never",
            tolerations=[
                client.V1Toleration(
                    key="virtual-kubelet.io/provider",
                    operator="Exists",
                ),
            ],
            containers=[
                client.V1Container(
                    name="test",
                    image="registry.access.redhat.com/ubi9-micro:latest",
                    command=["sh", "-c", "echo $MY_SECRET && cat /config/config.txt"],
                    env=[
                        client.V1EnvVar(
                            name="MY_SECRET",
                            value_from=client.V1EnvVarSource(
                                secret_key_ref=client.V1SecretKeySelector(
                                    name=secret_name, key="MY_SECRET"
                                )
                            ),
                        )
                    ],
                    volume_mounts=[
                        client.V1VolumeMount(
                            name="config-vol",
                            mount_path="/config",
                            read_only=True,
                        )
                    ],
                )
            ],
            volumes=[
                client.V1Volume(
                    name="config-vol",
                    config_map=client.V1ConfigMapVolumeSource(name=cm_name),
                )
            ],
        ),
    )
    tenant_core.create_namespaced_pod(namespace=ns, body=pod)

    # Wait for resources to appear on worker
    deadline = time.time() + 30
    secret_synced = False
    cm_synced = False
    while time.time() < deadline:
        try:
            s = worker_core.read_namespaced_secret(
                name=secret_name, namespace=vk_worker_namespace
            )
            if s.metadata.labels and s.metadata.labels.get(
                "app.kubernetes.io/managed-by"
            ) == "vk-gpu-provider":
                secret_synced = True
        except client.exceptions.ApiException:
            pass

        try:
            c = worker_core.read_namespaced_config_map(
                name=cm_name, namespace=vk_worker_namespace
            )
            if c.metadata.labels and c.metadata.labels.get(
                "app.kubernetes.io/managed-by"
            ) == "vk-gpu-provider":
                cm_synced = True
        except client.exceptions.ApiException:
            pass

        if secret_synced and cm_synced:
            break
        time.sleep(3)

    assert secret_synced, (
        f"Secret {secret_name} not synced to worker namespace {vk_worker_namespace}"
    )
    assert cm_synced, (
        f"ConfigMap {cm_name} not synced to worker namespace {vk_worker_namespace}"
    )

    # Wait for pod to complete
    deadline = time.time() + 120
    worker_phase = None
    while time.time() < deadline:
        try:
            wp = worker_core.read_namespaced_pod(
                name=w_pod_name, namespace=vk_worker_namespace
            )
            worker_phase = wp.status.phase
            if worker_phase in ("Succeeded", "Failed"):
                break
        except client.exceptions.ApiException:
            pass
        time.sleep(5)

    assert worker_phase == "Succeeded", (
        f"Resource sync pod did not succeed. Phase: {worker_phase!r}. "
        f"The pod may have failed to read the synced secret/configmap."
    )

    # Cleanup
    try:
        tenant_core.delete_namespaced_pod(name=pod_name, namespace=ns)
    except client.exceptions.ApiException:
        pass
    try:
        tenant_core.delete_namespaced_secret(name=secret_name, namespace=ns)
    except client.exceptions.ApiException:
        pass
    try:
        tenant_core.delete_namespaced_config_map(name=cm_name, namespace=ns)
    except client.exceptions.ApiException:
        pass


@pytest.mark.vk
def test_pod_deletion_cleans_up(
    tenant_clients, worker_clients, test_namespace, vk_worker_namespace
):
    """
    Submit a pod via VK, wait for completion, delete tenant pod,
    verify worker pod and synced resources are cleaned up.
    """
    tenant_core, _ = tenant_clients
    worker_core, _ = worker_clients
    ns = test_namespace

    pod_name = "vk-cleanup-test"
    w_pod_name = worker_pod_name(ns, pod_name)
    secret_name = "vk-cleanup-secret"

    # Create a secret that the pod references
    try:
        tenant_core.delete_namespaced_secret(name=secret_name, namespace=ns)
    except client.exceptions.ApiException:
        pass
    tenant_core.create_namespaced_secret(
        namespace=ns,
        body=client.V1Secret(
            metadata=client.V1ObjectMeta(name=secret_name, namespace=ns),
            string_data={"KEY": "value"},
        ),
    )

    # Cleanup from previous runs
    force_delete_pod(tenant_core, pod_name, ns)
    force_delete_pod(worker_core, w_pod_name, vk_worker_namespace)

    pod = client.V1Pod(
        metadata=client.V1ObjectMeta(name=pod_name, namespace=ns),
        spec=client.V1PodSpec(
            node_name=VK_NODE_NAME,
            restart_policy="Never",
            tolerations=[
                client.V1Toleration(
                    key="virtual-kubelet.io/provider",
                    operator="Exists",
                ),
            ],
            containers=[
                client.V1Container(
                    name="test",
                    image="registry.access.redhat.com/ubi9-micro:latest",
                    command=["echo", "done"],
                    env=[
                        client.V1EnvVar(
                            name="KEY",
                            value_from=client.V1EnvVarSource(
                                secret_key_ref=client.V1SecretKeySelector(
                                    name=secret_name, key="KEY"
                                )
                            ),
                        )
                    ],
                )
            ],
        ),
    )
    tenant_core.create_namespaced_pod(namespace=ns, body=pod)

    # Wait for worker pod to exist
    deadline = time.time() + 60
    while time.time() < deadline:
        try:
            worker_core.read_namespaced_pod(
                name=w_pod_name, namespace=vk_worker_namespace
            )
            break
        except client.exceptions.ApiException:
            pass
        time.sleep(3)

    # Delete tenant pod (should trigger VK cleanup)
    tenant_core.delete_namespaced_pod(name=pod_name, namespace=ns)

    # Wait for worker pod to be deleted
    deadline = time.time() + 30
    worker_pod_gone = False
    while time.time() < deadline:
        try:
            worker_core.read_namespaced_pod(
                name=w_pod_name, namespace=vk_worker_namespace
            )
        except client.exceptions.ApiException as e:
            if e.status == 404:
                worker_pod_gone = True
                break
        time.sleep(3)

    assert worker_pod_gone, (
        f"Worker pod {w_pod_name} not deleted after tenant pod deletion"
    )

    # Check synced secret is cleaned up
    deadline = time.time() + 15
    secret_gone = False
    while time.time() < deadline:
        try:
            worker_core.read_namespaced_secret(
                name=secret_name, namespace=vk_worker_namespace
            )
        except client.exceptions.ApiException as e:
            if e.status == 404:
                secret_gone = True
                break
        time.sleep(3)

    assert secret_gone, (
        f"Synced secret {secret_name} not cleaned up on worker after pod deletion"
    )

    # Cleanup tenant secret
    try:
        tenant_core.delete_namespaced_secret(name=secret_name, namespace=ns)
    except client.exceptions.ApiException:
        pass


@pytest.mark.vk
def test_catapult_pvc_sync(
    tenant_clients, worker_clients, test_namespace, vk_worker_namespace
):
    """
    Create a PVC with storageClass=catapult on tenant, submit a pod that
    mounts it. Verify Catapult creates a namespace-prefixed execution PVC
    on the worker with management labels. Verify execution PVC survives
    pod deletion but is cleaned up when the control PVC is deleted.
    """
    tenant_core, _ = tenant_clients
    worker_core, _ = worker_clients
    ns = test_namespace

    pvc_name = "vk-test-pvc"
    exec_pvc = execution_pvc_name(ns, pvc_name)
    pod_name = "vk-pvc-sync-test"
    w_pod_name = worker_pod_name(ns, pod_name)

    # Cleanup from previous runs
    force_delete_pod(tenant_core, pod_name, ns)
    force_delete_pod(worker_core, w_pod_name, vk_worker_namespace)
    for name, target_ns, core in [
        (pvc_name, ns, tenant_core),
        (exec_pvc, vk_worker_namespace, worker_core),
    ]:
        try:
            core.delete_namespaced_persistent_volume_claim(
                name=name, namespace=target_ns
            )
        except client.exceptions.ApiException:
            pass
    time.sleep(3)

    # Create catapult PVC on tenant
    tenant_core.create_namespaced_persistent_volume_claim(
        namespace=ns,
        body=client.V1PersistentVolumeClaim(
            metadata=client.V1ObjectMeta(name=pvc_name, namespace=ns),
            spec=client.V1PersistentVolumeClaimSpec(
                access_modes=["ReadWriteOnce"],
                storage_class_name=CATAPULT_STORAGE_CLASS,
                resources=client.V1VolumeResourceRequirements(
                    requests={"storage": "1Gi"},
                ),
            ),
        ),
    )

    # Create pod that mounts the PVC
    pod = client.V1Pod(
        metadata=client.V1ObjectMeta(name=pod_name, namespace=ns),
        spec=client.V1PodSpec(
            node_name=VK_NODE_NAME,
            restart_policy="Never",
            tolerations=[
                client.V1Toleration(
                    key="virtual-kubelet.io/provider",
                    operator="Exists",
                ),
            ],
            containers=[
                client.V1Container(
                    name="test",
                    image="registry.access.redhat.com/ubi9-micro:latest",
                    command=["echo", "pvc-mounted"],
                    volume_mounts=[
                        client.V1VolumeMount(
                            name="data",
                            mount_path="/data",
                        )
                    ],
                )
            ],
            volumes=[
                client.V1Volume(
                    name="data",
                    persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                        claim_name=pvc_name,
                    ),
                )
            ],
        ),
    )
    tenant_core.create_namespaced_pod(namespace=ns, body=pod)

    # Wait for execution PVC to appear on worker (namespace-prefixed name)
    deadline = time.time() + 30
    pvc_synced = False
    while time.time() < deadline:
        try:
            wpvc = worker_core.read_namespaced_persistent_volume_claim(
                name=exec_pvc, namespace=vk_worker_namespace
            )
            labels = wpvc.metadata.labels or {}
            if labels.get("app.kubernetes.io/managed-by") == "vk-gpu-provider":
                pvc_synced = True
                break
        except client.exceptions.ApiException:
            pass
        time.sleep(3)

    assert pvc_synced, (
        f"Execution PVC {exec_pvc} not created in {vk_worker_namespace}"
    )

    # Verify execution PVC spec
    wpvc = worker_core.read_namespaced_persistent_volume_claim(
        name=exec_pvc, namespace=vk_worker_namespace
    )
    assert "ReadWriteOnce" in wpvc.spec.access_modes
    assert wpvc.metadata.labels.get("vk.gpuaas.io/source-pvc") == pvc_name
    assert wpvc.metadata.labels.get("vk.gpuaas.io/source-namespace") == ns

    # Delete tenant pod — execution PVC must survive
    force_delete_pod(tenant_core, pod_name, ns)
    time.sleep(5)

    try:
        worker_core.read_namespaced_persistent_volume_claim(
            name=exec_pvc, namespace=vk_worker_namespace
        )
    except client.exceptions.ApiException:
        pytest.fail(
            f"Execution PVC {exec_pvc} was deleted with the pod — "
            "PVC lifecycle must be independent of pod lifecycle"
        )

    # Delete control PVC — should trigger execution PVC cleanup
    tenant_core.delete_namespaced_persistent_volume_claim(
        name=pvc_name, namespace=ns
    )

    deadline = time.time() + 30
    pvc_gone = False
    while time.time() < deadline:
        try:
            worker_core.read_namespaced_persistent_volume_claim(
                name=exec_pvc, namespace=vk_worker_namespace
            )
        except client.exceptions.ApiException as e:
            if e.status == 404:
                pvc_gone = True
                break
        time.sleep(3)

    assert pvc_gone, (
        f"Execution PVC {exec_pvc} not cleaned up after control PVC deletion"
    )


@pytest.mark.vk
def test_non_catapult_pvc_rejected(
    tenant_clients, worker_clients, test_namespace, vk_worker_namespace
):
    """
    Submit a pod referencing a PVC without storageClass=catapult.
    Verify the pod is rejected with a clear error, and no execution
    PVC is created on the worker.
    """
    tenant_core, _ = tenant_clients
    worker_core, _ = worker_clients
    ns = test_namespace

    pvc_name = "vk-non-catapult-pvc"
    pod_name = "vk-non-catapult-test"
    exec_pvc = execution_pvc_name(ns, pvc_name)
    w_pod_name = worker_pod_name(ns, pod_name)

    # Cleanup from previous runs
    force_delete_pod(tenant_core, pod_name, ns)
    force_delete_pod(worker_core, w_pod_name, vk_worker_namespace)
    for name, target_ns, core in [
        (pvc_name, ns, tenant_core),
        (exec_pvc, vk_worker_namespace, worker_core),
    ]:
        try:
            core.delete_namespaced_persistent_volume_claim(
                name=name, namespace=target_ns
            )
        except client.exceptions.ApiException:
            pass
    time.sleep(3)

    # Create PVC without catapult storageClass (uses cluster default)
    tenant_core.create_namespaced_persistent_volume_claim(
        namespace=ns,
        body=client.V1PersistentVolumeClaim(
            metadata=client.V1ObjectMeta(name=pvc_name, namespace=ns),
            spec=client.V1PersistentVolumeClaimSpec(
                access_modes=["ReadWriteOnce"],
                resources=client.V1VolumeResourceRequirements(
                    requests={"storage": "1Gi"},
                ),
            ),
        ),
    )

    # Create pod that mounts the non-catapult PVC
    pod = client.V1Pod(
        metadata=client.V1ObjectMeta(name=pod_name, namespace=ns),
        spec=client.V1PodSpec(
            node_name=VK_NODE_NAME,
            restart_policy="Never",
            tolerations=[
                client.V1Toleration(
                    key="virtual-kubelet.io/provider",
                    operator="Exists",
                ),
            ],
            containers=[
                client.V1Container(
                    name="test",
                    image="registry.access.redhat.com/ubi9-micro:latest",
                    command=["echo", "should-not-run"],
                    volume_mounts=[
                        client.V1VolumeMount(
                            name="data",
                            mount_path="/data",
                        )
                    ],
                )
            ],
            volumes=[
                client.V1Volume(
                    name="data",
                    persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                        claim_name=pvc_name,
                    ),
                )
            ],
        ),
    )
    tenant_core.create_namespaced_pod(namespace=ns, body=pod)

    # Wait for pod to be marked Failed with InvalidPVCStorageClass
    deadline = time.time() + 30
    pod_failed = False
    reason = ""
    while time.time() < deadline:
        tp = tenant_core.read_namespaced_pod(name=pod_name, namespace=ns)
        if tp.status.phase == "Failed":
            reason = tp.status.reason or ""
            pod_failed = True
            break
        time.sleep(3)

    assert pod_failed, (
        f"Pod {pod_name} was not rejected — expected Failed phase"
    )
    assert "InvalidPVCStorageClass" in reason, (
        f"Pod rejection reason should be InvalidPVCStorageClass, got: {reason!r}"
    )

    # Verify no execution PVC was created on worker
    try:
        worker_core.read_namespaced_persistent_volume_claim(
            name=exec_pvc, namespace=vk_worker_namespace
        )
        pytest.fail(
            f"Execution PVC {exec_pvc} should not exist for non-catapult PVC"
        )
    except client.exceptions.ApiException as e:
        assert e.status == 404

    # Cleanup
    try:
        tenant_core.delete_namespaced_pod(
            name=pod_name, namespace=ns, grace_period_seconds=0
        )
    except client.exceptions.ApiException:
        pass
    try:
        tenant_core.delete_namespaced_persistent_volume_claim(
            name=pvc_name, namespace=ns
        )
    except client.exceptions.ApiException:
        pass


@pytest.mark.vk
def test_multitenant_namespace_isolation(
    tenant_clients, worker_clients, worker_namespace_prefix,
):
    """
    Prove that per-tenant worker namespaces isolate resources.

    Two tenant namespaces each create a Secret named 'shared-config'
    with different data. VK dispatches pods from each, syncing secrets
    to separate worker namespaces. Both secrets coexist with correct
    data — no collision.
    """
    tenant_core, _ = tenant_clients
    worker_core, _ = worker_clients

    ns_a = "vk-team-a"
    ns_b = "vk-team-b"
    wns_a = worker_namespace_for_prefix(worker_namespace_prefix, ns_a)
    wns_b = worker_namespace_for_prefix(worker_namespace_prefix, ns_b)
    secret_name = "shared-config"
    pod_a_name = "isolation-test-a"
    pod_b_name = "isolation-test-b"
    w_pod_a = worker_pod_name(ns_a, pod_a_name)
    w_pod_b = worker_pod_name(ns_b, pod_b_name)

    # Create test namespaces on tenant
    for ns in [ns_a, ns_b]:
        try:
            tenant_core.create_namespace(
                body=client.V1Namespace(
                    metadata=client.V1ObjectMeta(name=ns)
                )
            )
        except client.exceptions.ApiException as e:
            if e.status != 409:
                raise

    # Cleanup from previous runs
    for core, name, target_ns in [
        (worker_core, w_pod_a, wns_a),
        (worker_core, w_pod_b, wns_b),
        (tenant_core, pod_a_name, ns_a),
        (tenant_core, pod_b_name, ns_b),
    ]:
        force_delete_pod(core, name, target_ns)

    for ns in [ns_a, ns_b]:
        try:
            tenant_core.delete_namespaced_secret(
                name=secret_name, namespace=ns
            )
        except client.exceptions.ApiException:
            pass
    for wns in [wns_a, wns_b]:
        try:
            worker_core.delete_namespaced_secret(
                name=secret_name, namespace=wns
            )
        except client.exceptions.ApiException:
            pass
    time.sleep(3)

    # Create same-named secrets with different data in each namespace
    tenant_core.create_namespaced_secret(
        namespace=ns_a,
        body=client.V1Secret(
            metadata=client.V1ObjectMeta(name=secret_name, namespace=ns_a),
            string_data={"team": "alpha"},
        ),
    )
    tenant_core.create_namespaced_secret(
        namespace=ns_b,
        body=client.V1Secret(
            metadata=client.V1ObjectMeta(name=secret_name, namespace=ns_b),
            string_data={"team": "bravo"},
        ),
    )

    # Dispatch pods from both namespaces — both read the same secret name
    for pod_name, ns in [(pod_a_name, ns_a), (pod_b_name, ns_b)]:
        pod = client.V1Pod(
            metadata=client.V1ObjectMeta(name=pod_name, namespace=ns),
            spec=client.V1PodSpec(
                node_name=VK_NODE_NAME,
                restart_policy="Never",
                tolerations=[
                    client.V1Toleration(
                        key="virtual-kubelet.io/provider",
                        operator="Exists",
                    ),
                ],
                containers=[
                    client.V1Container(
                        name="test",
                        image="registry.access.redhat.com/ubi9-micro:latest",
                        command=["sh", "-c", "echo $TEAM_VALUE"],
                        env=[
                            client.V1EnvVar(
                                name="TEAM_VALUE",
                                value_from=client.V1EnvVarSource(
                                    secret_key_ref=client.V1SecretKeySelector(
                                        name=secret_name, key="team"
                                    )
                                ),
                            )
                        ],
                    )
                ],
            ),
        )
        tenant_core.create_namespaced_pod(namespace=ns, body=pod)
        time.sleep(2)

    # Wait for both worker pods to exist in their respective namespaces
    deadline = time.time() + 90
    while time.time() < deadline:
        try:
            worker_core.read_namespaced_pod(name=w_pod_a, namespace=wns_a)
            worker_core.read_namespaced_pod(name=w_pod_b, namespace=wns_b)
            break
        except client.exceptions.ApiException:
            pass
        time.sleep(3)

    # Read secrets from separate worker namespaces — both must exist
    import base64

    deadline = time.time() + 30
    secret_a = secret_b = None
    while time.time() < deadline:
        try:
            if secret_a is None:
                secret_a = worker_core.read_namespaced_secret(
                    name=secret_name, namespace=wns_a
                )
            if secret_b is None:
                secret_b = worker_core.read_namespaced_secret(
                    name=secret_name, namespace=wns_b
                )
            if secret_a and secret_b:
                break
        except client.exceptions.ApiException:
            pass
        time.sleep(3)

    assert secret_a is not None, f"Secret not found in {wns_a}"
    assert secret_b is not None, f"Secret not found in {wns_b}"

    val_a = base64.b64decode(secret_a.data["team"]).decode()
    val_b = base64.b64decode(secret_b.data["team"]).decode()

    assert val_a == "alpha", f"Expected 'alpha' in {wns_a}, got {val_a!r}"
    assert val_b == "bravo", f"Expected 'bravo' in {wns_b}, got {val_b!r}"

    # Cleanup
    for core, name, target_ns in [
        (tenant_core, pod_a_name, ns_a),
        (tenant_core, pod_b_name, ns_b),
    ]:
        try:
            core.delete_namespaced_pod(
                name=name, namespace=target_ns, grace_period_seconds=0
            )
        except client.exceptions.ApiException:
            pass

    for ns in [ns_a, ns_b]:
        try:
            tenant_core.delete_namespaced_secret(
                name=secret_name, namespace=ns
            )
        except client.exceptions.ApiException:
            pass

    time.sleep(5)
    for ns in [ns_a, ns_b]:
        try:
            tenant_core.delete_namespace(name=ns)
        except client.exceptions.ApiException:
            pass


@pytest.mark.vk
def test_pod_logs_proxied_from_worker(
    tenant_clients, worker_clients, test_namespace, vk_worker_namespace
):
    """
    Submit a pod on the virtual node that prints a known message.
    Read logs via the tenant API (kubectl logs path) and verify the
    VK proxies them from the worker pod.
    """
    tenant_core, _ = tenant_clients
    worker_core, _ = worker_clients
    ns = test_namespace

    pod_name = "vk-log-test"
    w_pod_name = worker_pod_name(ns, pod_name)
    marker = "catapult-log-marker-42"

    force_delete_pod(tenant_core, pod_name, ns)
    force_delete_pod(worker_core, w_pod_name, vk_worker_namespace)

    pod = client.V1Pod(
        metadata=client.V1ObjectMeta(name=pod_name, namespace=ns),
        spec=client.V1PodSpec(
            node_name=VK_NODE_NAME,
            restart_policy="Never",
            tolerations=[
                client.V1Toleration(
                    key="virtual-kubelet.io/provider",
                    operator="Exists",
                ),
            ],
            containers=[
                client.V1Container(
                    name="echo",
                    image="docker.io/library/busybox:latest",
                    command=["sh", "-c", f"echo {marker}; echo line2; echo line3"],
                )
            ],
        ),
    )
    tenant_core.create_namespaced_pod(namespace=ns, body=pod)

    # Wait for pod to succeed
    deadline = time.time() + 120
    phase = None
    while time.time() < deadline:
        tp = tenant_core.read_namespaced_pod(name=pod_name, namespace=ns)
        phase = tp.status.phase
        if phase in ("Succeeded", "Failed"):
            break
        time.sleep(5)

    assert phase == "Succeeded", f"Pod did not succeed. Phase: {phase!r}"

    # Read logs from tenant — VK should proxy to worker
    logs = tenant_core.read_namespaced_pod_log(
        name=pod_name, namespace=ns, container="echo"
    )
    assert marker in logs, (
        f"Expected marker {marker!r} in tenant-side logs, got: {logs!r}"
    )
    assert "line3" in logs, f"Expected all lines in logs, got: {logs!r}"

    # Read logs with tail_lines option
    tail_logs = tenant_core.read_namespaced_pod_log(
        name=pod_name, namespace=ns, container="echo", tail_lines=1
    )
    lines = tail_logs.strip().splitlines()
    assert len(lines) == 1, f"Expected 1 line with tail_lines=1, got {len(lines)}: {lines}"
    assert lines[0] == "line3", f"Expected last line 'line3', got {lines[0]!r}"

    # Verify the same logs are on the worker directly
    worker_logs = worker_core.read_namespaced_pod_log(
        name=w_pod_name, namespace=vk_worker_namespace, container="echo"
    )
    assert logs == worker_logs, "Tenant logs should match worker logs exactly"

    # Cleanup
    force_delete_pod(tenant_core, pod_name, ns)

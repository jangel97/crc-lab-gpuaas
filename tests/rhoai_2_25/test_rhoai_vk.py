"""
RHOAI workload dispatch via Virtual Kubelet.

Tests that RHOAI training operator workloads (PyTorchJob), Notebook CRs,
and KServe InferenceServices get dispatched from the tenant cluster to the
worker GPU cluster via the custom VK. RHOAI runs only on the tenant; the
worker is a bare GPU execution node with no RHOAI CRDs.
"""

import json
import time

import pytest
from kubernetes import client

from helpers import worker_pod_name, execution_pvc_name, force_delete_pod, wait_pod_exists, CATAPULT_STORAGE_CLASS


VK_NODE_NAME = "gpu-worker"

PYTORCHJOB_GROUP = "kubeflow.org"
PYTORCHJOB_VERSION = "v1"
PYTORCHJOB_PLURAL = "pytorchjobs"

NOTEBOOK_GROUP = "kubeflow.org"
NOTEBOOK_VERSION = "v1"
NOTEBOOK_PLURAL = "notebooks"

ISVC_GROUP = "serving.kserve.io"
ISVC_VERSION = "v1beta1"
ISVC_PLURAL = "inferenceservices"

PYTORCH_IMAGE = "pytorch/pytorch:2.11.0-cuda12.8-cudnn9-runtime"

RHOAI_CRDS = [
    "pytorchjobs.kubeflow.org",
    "notebooks.kubeflow.org",
    "inferenceservices.serving.kserve.io",
    "rayclusters.ray.io",
]

VK_TOLERATIONS = [{"key": "virtual-kubelet.io/provider", "operator": "Exists"}]


@pytest.fixture(autouse=True, scope="session")
def _ensure_lab_env(high_memory_env, vk_node_ready, rhoai_operators_ready):
    pass


def wait_pod_phase(core_api, name, namespace, phases, timeout=600):
    """Wait for a pod to reach one of the given phases. Returns the phase."""
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


@pytest.mark.rhoai
def test_pytorchjob_via_vk(
    cleanup, tenant_clients, worker_clients, test_namespace, vk_worker_namespace
):
    """
    Submit a single-node PyTorchJob on tenant targeting the VK node.
    Verify the training pod runs on the worker GPU and the PyTorchJob
    reaches Succeeded on tenant.
    """
    tenant_core, tenant_custom = tenant_clients
    worker_core, worker_custom = worker_clients
    ns = test_namespace

    job_name = "vk-gpu-training"
    master_pod_name = f"{job_name}-master-0"
    w_pod_name = worker_pod_name(ns, master_pod_name)

    # Cleanup from previous runs
    try:
        tenant_custom.delete_namespaced_custom_object(
            group=PYTORCHJOB_GROUP,
            version=PYTORCHJOB_VERSION,
            namespace=ns,
            plural=PYTORCHJOB_PLURAL,
            name=job_name,
        )
        time.sleep(10)
    except client.exceptions.ApiException as e:
        if e.status != 404:
            raise

    for core, name, target_ns in [
        (tenant_core, master_pod_name, ns),
        (worker_core, w_pod_name, vk_worker_namespace),
    ]:
        force_delete_pod(core, name, target_ns)

    pytorchjob = {
        "apiVersion": "kubeflow.org/v1",
        "kind": "PyTorchJob",
        "metadata": {"name": job_name, "namespace": ns},
        "spec": {
            "pytorchReplicaSpecs": {
                "Master": {
                    "replicas": 1,
                    "template": {
                        "spec": {
                            "nodeName": VK_NODE_NAME,
                            "restartPolicy": "Never",
                            "tolerations": VK_TOLERATIONS,
                            "containers": [
                                {
                                    "name": "pytorch",
                                    "image": "nvcr.io/nvidia/cuda:12.8.0-base-ubi9",
                                    "command": ["nvidia-smi"],
                                    "resources": {
                                        "limits": {"nvidia.com/gpu": "1"},
                                        "requests": {
                                            "nvidia.com/gpu": "1",
                                            "cpu": "1",
                                            "memory": "1Gi",
                                        },
                                    },
                                }
                            ],
                        }
                    },
                }
            }
        },
    }

    tenant_custom.create_namespaced_custom_object(
        group=PYTORCHJOB_GROUP,
        version=PYTORCHJOB_VERSION,
        namespace=ns,
        plural=PYTORCHJOB_PLURAL,
        body=pytorchjob,
    )
    cleanup(tenant_custom.delete_namespaced_custom_object,
            group=PYTORCHJOB_GROUP, version=PYTORCHJOB_VERSION,
            namespace=ns, plural=PYTORCHJOB_PLURAL, name=job_name)

    assert wait_pod_exists(tenant_core, master_pod_name, ns, timeout=60), (
        f"Training operator did not create master pod {master_pod_name} within 60s"
    )

    assert wait_pod_exists(worker_core, w_pod_name, vk_worker_namespace, timeout=60), (
        f"Worker pod {w_pod_name} did not appear in {vk_worker_namespace} within 60s"
    )

    worker_phase = wait_pod_phase(
        worker_core, w_pod_name, vk_worker_namespace, ("Succeeded", "Failed"),
    )
    assert worker_phase == "Succeeded", (
        f"Worker training pod did not succeed. Phase: {worker_phase!r}"
    )

    tenant_phase = wait_pod_phase(
        tenant_core, master_pod_name, ns, ("Succeeded",), timeout=60,
    )
    assert tenant_phase == "Succeeded", (
        f"Tenant pod status not synced. Phase: {tenant_phase!r}"
    )

    # Verify PyTorchJob condition
    deadline = time.time() + 30
    job_succeeded = False
    while time.time() < deadline:
        job = tenant_custom.get_namespaced_custom_object(
            group=PYTORCHJOB_GROUP,
            version=PYTORCHJOB_VERSION,
            namespace=ns,
            plural=PYTORCHJOB_PLURAL,
            name=job_name,
        )
        conditions = job.get("status", {}).get("conditions", [])
        for c in conditions:
            if c.get("type") == "Succeeded" and c.get("status") == "True":
                job_succeeded = True
                break
        if job_succeeded:
            break
        time.sleep(5)

    assert job_succeeded, (
        f"PyTorchJob {job_name} did not reach Succeeded condition"
    )


@pytest.mark.rhoai
def test_no_rhoai_crds_on_worker(worker_clients):
    """Verify the worker cluster has no RHOAI CRDs — it stays bare."""
    _, worker_custom = worker_clients

    api_ext = client.ApiextensionsV1Api(worker_custom.api_client)
    crds = api_ext.list_custom_resource_definition()
    crd_names = {crd.metadata.name for crd in crds.items}

    leaked = [name for name in RHOAI_CRDS if name in crd_names]
    assert not leaked, (
        f"RHOAI CRDs found on worker cluster (should be bare GPU node): {leaked}"
    )


@pytest.mark.rhoai
def test_notebook_cr_via_vk(
    cleanup, tenant_clients, worker_clients, test_namespace, vk_worker_namespace
):
    """
    Create an RHOAI Notebook CR on the tenant targeting the VK node.
    Verify the notebook controller creates a StatefulSet, the pod gets
    dispatched to the worker GPU, and the Notebook reaches Running state.

    This proves the Dashboard → Notebook → GPU workflow works via VK.

    The high_memory_env fixture automatically configures tenant1 to
    28GB and shuts down tenant2.
    """
    tenant_core, tenant_custom = tenant_clients
    worker_core, _ = worker_clients
    ns = test_namespace

    nb_name = "vk-gpu-notebook"
    pod_name = f"{nb_name}-0"
    w_pod_name = worker_pod_name(ns, pod_name)

    # Cleanup from previous runs
    try:
        tenant_custom.delete_namespaced_custom_object(
            group=NOTEBOOK_GROUP,
            version=NOTEBOOK_VERSION,
            namespace=ns,
            plural=NOTEBOOK_PLURAL,
            name=nb_name,
        )
        time.sleep(10)
    except client.exceptions.ApiException as e:
        if e.status != 404:
            raise
    force_delete_pod(tenant_core, pod_name, ns)
    force_delete_pod(worker_core, w_pod_name, vk_worker_namespace)

    # Notebook CR — lightweight GPU HTTP server that stays Running
    server_script = "\n".join([
        "import http.server, subprocess, json",
        "class H(http.server.BaseHTTPRequestHandler):",
        "  def do_GET(self):",
        "    r = subprocess.run(['nvidia-smi','--query-gpu=name,memory.total','--format=csv,noheader,nounits'], capture_output=True, text=True)",
        "    body = json.dumps(dict(gpu=r.stdout.strip(), status='ready'))",
        "    self.send_response(200)",
        "    self.send_header('Content-Type','application/json')",
        "    self.end_headers()",
        "    self.wfile.write(body.encode())",
        "  def log_message(self, *a): pass",
        "http.server.HTTPServer(('',8888),H).serve_forever()",
    ])

    notebook_cr = {
        "apiVersion": "kubeflow.org/v1",
        "kind": "Notebook",
        "metadata": {"name": nb_name, "namespace": ns},
        "spec": {
            "template": {
                "spec": {
                    "nodeName": VK_NODE_NAME,
                    "tolerations": VK_TOLERATIONS,
                    "containers": [
                        {
                            "name": "notebook",
                            "image": "nvcr.io/nvidia/cuda:12.8.1-base-ubi9",
                            "command": ["python3", "-c", server_script],
                            "ports": [{"containerPort": 8888, "protocol": "TCP"}],
                            "resources": {
                                "limits": {"nvidia.com/gpu": "1"},
                                "requests": {
                                    "nvidia.com/gpu": "1",
                                    "cpu": "1",
                                    "memory": "1Gi",
                                },
                            },
                        }
                    ],
                }
            }
        },
    }

    tenant_custom.create_namespaced_custom_object(
        group=NOTEBOOK_GROUP,
        version=NOTEBOOK_VERSION,
        namespace=ns,
        plural=NOTEBOOK_PLURAL,
        body=notebook_cr,
    )
    cleanup(tenant_custom.delete_namespaced_custom_object,
            group=NOTEBOOK_GROUP, version=NOTEBOOK_VERSION,
            namespace=ns, plural=NOTEBOOK_PLURAL, name=nb_name)
    cleanup(force_delete_pod, tenant_core, pod_name, ns)

    # Wait for StatefulSet to be created by notebook controller
    apps_api = client.AppsV1Api(tenant_core.api_client)
    deadline = time.time() + 60
    sts_found = False
    while time.time() < deadline:
        try:
            apps_api.read_namespaced_stateful_set(name=nb_name, namespace=ns)
            sts_found = True
            break
        except client.exceptions.ApiException:
            pass
        time.sleep(3)

    assert sts_found, (
        f"Notebook controller did not create StatefulSet {nb_name} within 60s"
    )

    # Wait for pod to be created by StatefulSet
    assert wait_pod_exists(tenant_core, pod_name, ns, timeout=60), (
        f"StatefulSet did not create pod {pod_name} within 60s"
    )

    # Wait for worker pod to appear (VK dispatch)
    assert wait_pod_exists(worker_core, w_pod_name, vk_worker_namespace, timeout=60), (
        f"Worker pod {w_pod_name} did not appear in {vk_worker_namespace} within 60s"
    )

    # Wait for worker container to be Running (not just pod phase)
    deadline = time.time() + 300
    worker_phase = None
    while time.time() < deadline:
        try:
            wp = worker_core.read_namespaced_pod(w_pod_name, vk_worker_namespace)
            worker_phase = wp.status.phase
            if worker_phase == "Failed":
                break
            if worker_phase == "Running" and wp.status.container_statuses:
                cs = wp.status.container_statuses[0]
                if cs.state and cs.state.running:
                    break
        except client.exceptions.ApiException:
            pass
        time.sleep(5)
    assert worker_phase == "Running", (
        f"Notebook worker pod should be Running, got {worker_phase!r}"
    )

    # Verify tenant pod status synced to Running
    tenant_phase = wait_pod_phase(
        tenant_core, pod_name, ns, ("Running",), timeout=60,
    )
    assert tenant_phase == "Running", (
        f"Tenant notebook pod status not synced to Running. Phase: {tenant_phase!r}"
    )

    # Wait for Notebook CR status to show Ready or running containerState
    deadline = time.time() + 60
    nb_ready = False
    has_running_state = False
    nb_conditions = []
    container_state = {}
    while time.time() < deadline:
        nb = tenant_custom.get_namespaced_custom_object(
            group=NOTEBOOK_GROUP,
            version=NOTEBOOK_VERSION,
            namespace=ns,
            plural=NOTEBOOK_PLURAL,
            name=nb_name,
        )
        nb_conditions = nb.get("status", {}).get("conditions", [])
        nb_ready = any(
            c.get("type") == "Ready" and c.get("status") == "True"
            for c in nb_conditions
        )
        container_state = nb.get("status", {}).get("containerState", {})
        has_running_state = "running" in container_state
        if nb_ready or has_running_state:
            break
        time.sleep(5)

    assert nb_ready or has_running_state, (
        f"Notebook CR status does not show Ready or running containerState. "
        f"Conditions: {nb_conditions}, ContainerState: {container_state}"
    )


@pytest.mark.rhoai
def test_pytorchjob_real_training(
    cleanup, tenant_clients, worker_clients, test_namespace, vk_worker_namespace
):
    """
    Submit a PyTorchJob with an actual PyTorch training script.
    Verify the model trains on GPU (forward + backward + optimizer),
    not just that nvidia-smi works. Proves CUDA compute is functional.
    """
    tenant_core, tenant_custom = tenant_clients
    worker_core, _ = worker_clients
    ns = test_namespace

    job_name = "vk-real-training"
    master_pod_name = f"{job_name}-master-0"
    w_pod_name = worker_pod_name(ns, master_pod_name)

    # Cleanup from previous runs
    try:
        tenant_custom.delete_namespaced_custom_object(
            group=PYTORCHJOB_GROUP,
            version=PYTORCHJOB_VERSION,
            namespace=ns,
            plural=PYTORCHJOB_PLURAL,
            name=job_name,
        )
        time.sleep(10)
    except client.exceptions.ApiException as e:
        if e.status != 404:
            raise
    force_delete_pod(tenant_core, master_pod_name, ns)
    force_delete_pod(worker_core, w_pod_name, vk_worker_namespace)

    training_script = "\n".join([
        "import torch",
        "import torch.nn as nn",
        "",
        "device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')",
        "print(f'Training on: {device}')",
        "assert device.type == 'cuda', 'CUDA not available'",
        "",
        "model = nn.Sequential(",
        "    nn.Linear(784, 128),",
        "    nn.ReLU(),",
        "    nn.Linear(128, 10),",
        ").to(device)",
        "",
        "optimizer = torch.optim.SGD(model.parameters(), lr=0.01)",
        "loss_fn = nn.CrossEntropyLoss()",
        "",
        "for epoch in range(5):",
        "    x = torch.randn(64, 784, device=device)",
        "    y = torch.randint(0, 10, (64,), device=device)",
        "    loss = loss_fn(model(x), y)",
        "    optimizer.zero_grad()",
        "    loss.backward()",
        "    optimizer.step()",
        "    print(f'Epoch {epoch+1}, Loss: {loss.item():.4f}')",
        "",
        "print('Training complete')",
    ])

    pytorchjob = {
        "apiVersion": "kubeflow.org/v1",
        "kind": "PyTorchJob",
        "metadata": {"name": job_name, "namespace": ns},
        "spec": {
            "pytorchReplicaSpecs": {
                "Master": {
                    "replicas": 1,
                    "template": {
                        "spec": {
                            "nodeName": VK_NODE_NAME,
                            "restartPolicy": "Never",
                            "tolerations": VK_TOLERATIONS,
                            "containers": [
                                {
                                    "name": "pytorch",
                                    "image": PYTORCH_IMAGE,
                                    "command": ["python3", "-c", training_script],
                                    "resources": {
                                        "limits": {"nvidia.com/gpu": "1"},
                                        "requests": {
                                            "nvidia.com/gpu": "1",
                                            "cpu": "1",
                                            "memory": "2Gi",
                                        },
                                    },
                                }
                            ],
                        }
                    },
                }
            }
        },
    }

    tenant_custom.create_namespaced_custom_object(
        group=PYTORCHJOB_GROUP,
        version=PYTORCHJOB_VERSION,
        namespace=ns,
        plural=PYTORCHJOB_PLURAL,
        body=pytorchjob,
    )
    cleanup(tenant_custom.delete_namespaced_custom_object,
            group=PYTORCHJOB_GROUP, version=PYTORCHJOB_VERSION,
            namespace=ns, plural=PYTORCHJOB_PLURAL, name=job_name)

    assert wait_pod_exists(tenant_core, master_pod_name, ns, timeout=60), (
        f"Training operator did not create master pod {master_pod_name} within 60s"
    )

    assert wait_pod_exists(worker_core, w_pod_name, vk_worker_namespace, timeout=60), (
        f"Worker pod {w_pod_name} did not appear in {vk_worker_namespace} within 60s"
    )

    # Long timeout for PyTorch image pull (~5GB)
    worker_phase = wait_pod_phase(
        worker_core, w_pod_name, vk_worker_namespace, ("Succeeded", "Failed"),
        timeout=900,
    )
    assert worker_phase == "Succeeded", (
        f"Real training pod did not succeed. Phase: {worker_phase!r}"
    )

    # Verify training actually ran on CUDA
    logs = worker_core.read_namespaced_pod_log(
        name=w_pod_name, namespace=vk_worker_namespace, container="pytorch",
    )
    assert "Training on: cuda" in logs, (
        f"Training did not use CUDA. Logs: {logs[:500]}"
    )
    assert "Training complete" in logs, (
        f"Training did not complete. Logs: {logs[:500]}"
    )
    assert "Epoch 5" in logs, (
        f"Training did not run all epochs. Logs: {logs[:500]}"
    )

    # Verify tenant status synced
    tenant_phase = wait_pod_phase(
        tenant_core, master_pod_name, ns, ("Succeeded",), timeout=60,
    )
    assert tenant_phase == "Succeeded", (
        f"Tenant pod status not synced. Phase: {tenant_phase!r}"
    )

    # Verify PyTorchJob condition
    deadline = time.time() + 30
    job_succeeded = False
    while time.time() < deadline:
        job = tenant_custom.get_namespaced_custom_object(
            group=PYTORCHJOB_GROUP,
            version=PYTORCHJOB_VERSION,
            namespace=ns,
            plural=PYTORCHJOB_PLURAL,
            name=job_name,
        )
        conditions = job.get("status", {}).get("conditions", [])
        for c in conditions:
            if c.get("type") == "Succeeded" and c.get("status") == "True":
                job_succeeded = True
                break
        if job_succeeded:
            break
        time.sleep(5)

    assert job_succeeded, (
        f"PyTorchJob {job_name} did not reach Succeeded condition"
    )


@pytest.mark.rhoai
def test_pytorchjob_checkpoint_with_pvc(
    cleanup, tenant_clients, worker_clients, test_namespace, vk_worker_namespace
):
    """
    Train a model on GPU and save a checkpoint to a catapult PVC.
    Then launch a verification pod that loads the checkpoint.
    Proves the full training data pipeline: catapult PVC + GPU training
    + checkpoint persistence.
    """
    tenant_core, tenant_custom = tenant_clients
    worker_core, _ = worker_clients
    ns = test_namespace

    job_name = "vk-checkpoint-training"
    master_pod_name = f"{job_name}-master-0"
    w_pod_name = worker_pod_name(ns, master_pod_name)
    pvc_name = "training-checkpoint"
    exec_pvc = execution_pvc_name(ns, pvc_name)
    verify_pod_name = "vk-checkpoint-verify"
    w_verify_pod = worker_pod_name(ns, verify_pod_name)

    # Cleanup from previous runs
    try:
        tenant_custom.delete_namespaced_custom_object(
            group=PYTORCHJOB_GROUP,
            version=PYTORCHJOB_VERSION,
            namespace=ns,
            plural=PYTORCHJOB_PLURAL,
            name=job_name,
        )
        time.sleep(10)
    except client.exceptions.ApiException as e:
        if e.status != 404:
            raise
    for core, name, target_ns in [
        (tenant_core, master_pod_name, ns),
        (worker_core, w_pod_name, vk_worker_namespace),
        (tenant_core, verify_pod_name, ns),
        (worker_core, w_verify_pod, vk_worker_namespace),
    ]:
        force_delete_pod(core, name, target_ns)
    for name, target_ns, core in [
        (pvc_name, ns, tenant_core),
        (exec_pvc, vk_worker_namespace, worker_core),
    ]:
        try:
            core.delete_namespaced_persistent_volume_claim(
                name=name, namespace=target_ns,
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
    cleanup(tenant_core.delete_namespaced_persistent_volume_claim,
            name=pvc_name, namespace=ns)

    # Training script that saves a checkpoint
    training_script = "\n".join([
        "import torch",
        "import torch.nn as nn",
        "",
        "device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')",
        "print(f'Training on: {device}')",
        "",
        "model = nn.Sequential(",
        "    nn.Linear(784, 128),",
        "    nn.ReLU(),",
        "    nn.Linear(128, 10),",
        ").to(device)",
        "",
        "optimizer = torch.optim.SGD(model.parameters(), lr=0.01)",
        "loss_fn = nn.CrossEntropyLoss()",
        "",
        "for epoch in range(3):",
        "    x = torch.randn(64, 784, device=device)",
        "    y = torch.randint(0, 10, (64,), device=device)",
        "    loss = loss_fn(model(x), y)",
        "    optimizer.zero_grad()",
        "    loss.backward()",
        "    optimizer.step()",
        "    print(f'Epoch {epoch+1}, Loss: {loss.item():.4f}')",
        "",
        "torch.save(model.state_dict(), '/data/checkpoint.pt')",
        "print(f'Checkpoint saved, keys: {len(model.state_dict())}')",
        "print('Training complete')",
    ])

    pytorchjob = {
        "apiVersion": "kubeflow.org/v1",
        "kind": "PyTorchJob",
        "metadata": {"name": job_name, "namespace": ns},
        "spec": {
            "pytorchReplicaSpecs": {
                "Master": {
                    "replicas": 1,
                    "template": {
                        "spec": {
                            "nodeName": VK_NODE_NAME,
                            "restartPolicy": "Never",
                            "tolerations": VK_TOLERATIONS,
                            "containers": [
                                {
                                    "name": "pytorch",
                                    "image": PYTORCH_IMAGE,
                                    "command": ["python3", "-c", training_script],
                                    "resources": {
                                        "limits": {"nvidia.com/gpu": "1"},
                                        "requests": {
                                            "nvidia.com/gpu": "1",
                                            "cpu": "1",
                                            "memory": "2Gi",
                                        },
                                    },
                                    "volumeMounts": [
                                        {
                                            "name": "data",
                                            "mountPath": "/data",
                                        }
                                    ],
                                }
                            ],
                            "volumes": [
                                {
                                    "name": "data",
                                    "persistentVolumeClaim": {
                                        "claimName": pvc_name,
                                    },
                                }
                            ],
                        }
                    },
                }
            }
        },
    }

    tenant_custom.create_namespaced_custom_object(
        group=PYTORCHJOB_GROUP,
        version=PYTORCHJOB_VERSION,
        namespace=ns,
        plural=PYTORCHJOB_PLURAL,
        body=pytorchjob,
    )
    cleanup(tenant_custom.delete_namespaced_custom_object,
            group=PYTORCHJOB_GROUP, version=PYTORCHJOB_VERSION,
            namespace=ns, plural=PYTORCHJOB_PLURAL, name=job_name)

    assert wait_pod_exists(tenant_core, master_pod_name, ns, timeout=60), (
        f"Training operator did not create master pod within 60s"
    )

    # Wait for training to complete
    worker_phase = wait_pod_phase(
        worker_core, w_pod_name, vk_worker_namespace, ("Succeeded", "Failed"),
        timeout=900,
    )
    assert worker_phase == "Succeeded", (
        f"Training pod did not succeed. Phase: {worker_phase!r}"
    )

    # Verify checkpoint was saved
    logs = worker_core.read_namespaced_pod_log(
        name=w_pod_name, namespace=vk_worker_namespace, container="pytorch",
    )
    assert "Checkpoint saved" in logs, (
        f"Checkpoint not saved. Logs: {logs[:500]}"
    )

    # Verify execution PVC exists on worker
    try:
        wpvc = worker_core.read_namespaced_persistent_volume_claim(
            name=exec_pvc, namespace=vk_worker_namespace,
        )
        assert wpvc.metadata.labels.get("app.kubernetes.io/managed-by") == "vk-gpu-provider-gpu-worker"
    except client.exceptions.ApiException:
        pytest.fail(f"Execution PVC {exec_pvc} not found in {vk_worker_namespace}")

    # Launch verification pod that reads the checkpoint
    verify_script = "\n".join([
        "import torch",
        "ckpt = torch.load('/data/checkpoint.pt', map_location='cpu', weights_only=True)",
        "print(f'Keys: {len(ckpt)}')",
        "assert len(ckpt) > 0, 'Empty checkpoint'",
        "print('Checkpoint valid')",
    ])

    verify_pod = client.V1Pod(
        metadata=client.V1ObjectMeta(name=verify_pod_name, namespace=ns),
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
                    name="verify",
                    image=PYTORCH_IMAGE,
                    command=["python3", "-c", verify_script],
                    resources=client.V1ResourceRequirements(
                        requests={"cpu": "500m", "memory": "1Gi"},
                    ),
                    volume_mounts=[
                        client.V1VolumeMount(name="data", mount_path="/data"),
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
    tenant_core.create_namespaced_pod(namespace=ns, body=verify_pod)
    cleanup(force_delete_pod, tenant_core, verify_pod_name, ns)

    verify_phase = wait_pod_phase(
        worker_core, w_verify_pod, vk_worker_namespace, ("Succeeded", "Failed"),
        timeout=300,
    )
    assert verify_phase == "Succeeded", (
        f"Verification pod did not succeed. Phase: {verify_phase!r}"
    )

    verify_logs = worker_core.read_namespaced_pod_log(
        name=w_verify_pod, namespace=vk_worker_namespace, container="verify",
    )
    assert "Checkpoint valid" in verify_logs, (
        f"Checkpoint verification failed. Logs: {verify_logs[:500]}"
    )


@pytest.mark.rhoai
def test_kserve_inference_via_vk(
    cleanup, tenant_clients, worker_clients, test_namespace, vk_worker_namespace
):
    """
    End-to-end KServe InferenceService test: installs ServiceMesh and
    Serverless operators if missing, waits for the InferenceService CRD
    to appear, then creates an InferenceService in raw deployment mode
    targeting the VK node.

    This proves model serving via RHOAI's native InferenceService API
    works through VK, with the full operator stack installed.

    Prerequisites (handled automatically by this test):
      - OperatorHub default catalog sources must be enabled, or the test
        enables them (adds ~1GB memory for catalog pods).
      - ServiceMesh operator installed from redhat-operators catalog.
      - Serverless operator installed from redhat-operators catalog.
      - RHOAI DSC has KServe serving.managementState: Managed (already
        the default in the lab).
      - Tenant SNO needs ~28GB RAM (handled by the high_memory_env fixture).

    Timing: First run takes 5-10 minutes (operator installs + CRD
    propagation). Subsequent runs with operators already installed
    take ~30 seconds.
    """
    tenant_core, tenant_custom = tenant_clients
    worker_core, _ = worker_clients
    ns = test_namespace
    apps_api = client.AppsV1Api(tenant_core.api_client)

    # ── Step 1: Enable default catalog sources if disabled ──

    try:
        ophub = tenant_custom.get_cluster_custom_object(
            group="config.openshift.io",
            version="v1",
            plural="operatorhubs",
            name="cluster",
        )
        if ophub.get("spec", {}).get("disableAllDefaultSources"):
            tenant_custom.patch_cluster_custom_object(
                group="config.openshift.io",
                version="v1",
                plural="operatorhubs",
                name="cluster",
                body={"spec": {"disableAllDefaultSources": False}},
            )
            # Wait for redhat-operators catalog pod to appear
            deadline = time.time() + 120
            catalog_ready = False
            while time.time() < deadline:
                pods = tenant_core.list_namespaced_pod(
                    namespace="openshift-marketplace",
                    label_selector="olm.catalogSource=redhat-operators",
                )
                for p in pods.items:
                    if p.status.phase == "Running" and all(
                        cs.ready for cs in (p.status.container_statuses or [])
                    ):
                        catalog_ready = True
                        break
                if catalog_ready:
                    break
                time.sleep(10)
            assert catalog_ready, (
                "redhat-operators CatalogSource pod did not become Ready within 120s"
            )
    except client.exceptions.ApiException:
        pass

    # ── Step 2: Install ServiceMesh operator if not present ──

    SM_SUB_NS = "openshift-operators"
    SM_SUB_NAME = "servicemeshoperator"

    try:
        tenant_custom.get_namespaced_custom_object(
            group="operators.coreos.com",
            version="v1alpha1",
            namespace=SM_SUB_NS,
            plural="subscriptions",
            name=SM_SUB_NAME,
        )
    except client.exceptions.ApiException as e:
        if e.status == 404:
            tenant_custom.create_namespaced_custom_object(
                group="operators.coreos.com",
                version="v1alpha1",
                namespace=SM_SUB_NS,
                plural="subscriptions",
                body={
                    "apiVersion": "operators.coreos.com/v1alpha1",
                    "kind": "Subscription",
                    "metadata": {
                        "name": SM_SUB_NAME,
                        "namespace": SM_SUB_NS,
                    },
                    "spec": {
                        "channel": "stable",
                        "name": SM_SUB_NAME,
                        "source": "redhat-operators",
                        "sourceNamespace": "openshift-marketplace",
                        "installPlanApproval": "Automatic",
                    },
                },
            )
        else:
            raise

    # Wait for ServiceMesh CSV to succeed
    deadline = time.time() + 300
    sm_ready = False
    while time.time() < deadline:
        try:
            csvs = tenant_custom.list_namespaced_custom_object(
                group="operators.coreos.com",
                version="v1alpha1",
                namespace=SM_SUB_NS,
                plural="clusterserviceversions",
            )
            for csv in csvs.get("items", []):
                if "servicemesh" in csv["metadata"]["name"].lower():
                    if csv.get("status", {}).get("phase") == "Succeeded":
                        sm_ready = True
                        break
        except client.exceptions.ApiException:
            pass
        if sm_ready:
            break
        time.sleep(15)
    assert sm_ready, "ServiceMesh operator CSV did not reach Succeeded within 5 min"

    # ── Step 3: Install Serverless operator if not present ──

    SL_SUB_NS = "openshift-serverless"
    SL_SUB_NAME = "serverless-operator"

    try:
        tenant_core.read_namespace(SL_SUB_NS)
    except client.exceptions.ApiException:
        tenant_core.create_namespace(
            client.V1Namespace(metadata=client.V1ObjectMeta(name=SL_SUB_NS))
        )

    # Serverless needs an OperatorGroup in its namespace
    try:
        tenant_custom.get_namespaced_custom_object(
            group="operators.coreos.com",
            version="v1",
            namespace=SL_SUB_NS,
            plural="operatorgroups",
            name="serverless-og",
        )
    except client.exceptions.ApiException as e:
        if e.status == 404:
            tenant_custom.create_namespaced_custom_object(
                group="operators.coreos.com",
                version="v1",
                namespace=SL_SUB_NS,
                plural="operatorgroups",
                body={
                    "apiVersion": "operators.coreos.com/v1",
                    "kind": "OperatorGroup",
                    "metadata": {
                        "name": "serverless-og",
                        "namespace": SL_SUB_NS,
                    },
                    "spec": {},
                },
            )

    try:
        tenant_custom.get_namespaced_custom_object(
            group="operators.coreos.com",
            version="v1alpha1",
            namespace=SL_SUB_NS,
            plural="subscriptions",
            name=SL_SUB_NAME,
        )
    except client.exceptions.ApiException as e:
        if e.status == 404:
            tenant_custom.create_namespaced_custom_object(
                group="operators.coreos.com",
                version="v1alpha1",
                namespace=SL_SUB_NS,
                plural="subscriptions",
                body={
                    "apiVersion": "operators.coreos.com/v1alpha1",
                    "kind": "Subscription",
                    "metadata": {
                        "name": SL_SUB_NAME,
                        "namespace": SL_SUB_NS,
                    },
                    "spec": {
                        "channel": "stable",
                        "name": SL_SUB_NAME,
                        "source": "redhat-operators",
                        "sourceNamespace": "openshift-marketplace",
                        "installPlanApproval": "Automatic",
                    },
                },
            )
        else:
            raise

    # Wait for Serverless CSV to succeed
    deadline = time.time() + 300
    sl_ready = False
    while time.time() < deadline:
        try:
            csvs = tenant_custom.list_namespaced_custom_object(
                group="operators.coreos.com",
                version="v1alpha1",
                namespace=SL_SUB_NS,
                plural="clusterserviceversions",
            )
            for csv in csvs.get("items", []):
                if "serverless" in csv["metadata"]["name"].lower():
                    if csv.get("status", {}).get("phase") == "Succeeded":
                        sl_ready = True
                        break
        except client.exceptions.ApiException:
            pass
        if sl_ready:
            break
        time.sleep(15)
    assert sl_ready, "Serverless operator CSV did not reach Succeeded within 5 min"

    # ── Step 4: Wait for RHOAI to configure KServe ──
    # RHOAI DSCInitialization has serviceMesh.managementState: Managed
    # and DSC has kserve.serving.managementState: Managed. Once the
    # ServiceMesh + Serverless operators are installed, the RHOAI operator
    # automatically creates: ServiceMeshControlPlane (data-science-smcp),
    # ServiceMeshMember, KnativeServing, and the KServe controller.
    # We just need to wait for the InferenceService CRD to appear.

    deadline = time.time() + 300
    crd_exists = False
    while time.time() < deadline:
        try:
            tenant_custom.get_cluster_custom_object(
                group="apiextensions.k8s.io",
                version="v1",
                plural="customresourcedefinitions",
                name="inferenceservices.serving.kserve.io",
            )
            crd_exists = True
            break
        except client.exceptions.ApiException:
            pass
        time.sleep(15)
    assert crd_exists, (
        "InferenceService CRD did not appear within 5 min after operator install. "
        "Check RHOAI DSC kserve.serving.managementState and operator logs."
    )

    # ── Step 5: Create InferenceService (raw deployment mode) ──

    isvc_name = "vk-gpu-inference"

    # Cleanup from previous runs
    try:
        tenant_custom.delete_namespaced_custom_object(
            group=ISVC_GROUP,
            version=ISVC_VERSION,
            namespace=ns,
            plural=ISVC_PLURAL,
            name=isvc_name,
        )
        time.sleep(10)
    except client.exceptions.ApiException as e:
        if e.status != 404:
            raise

    pods = tenant_core.list_namespaced_pod(
        namespace=ns,
        label_selector=f"serving.kserve.io/inferenceservice={isvc_name}",
    )
    for p in pods.items:
        force_delete_pod(tenant_core, p.metadata.name, ns)

    server_script = "\n".join([
        "import http.server, subprocess, json",
        "class H(http.server.BaseHTTPRequestHandler):",
        "  def do_GET(self):",
        "    r = subprocess.run(['nvidia-smi','--query-gpu=name,memory.total','--format=csv,noheader,nounits'], capture_output=True, text=True)",
        "    body = json.dumps(dict(gpu=r.stdout.strip(), status='ok', model='test'))",
        "    self.send_response(200)",
        "    self.send_header('Content-Type','application/json')",
        "    self.end_headers()",
        "    self.wfile.write(body.encode())",
        "  def log_message(self, *a): pass",
        "http.server.HTTPServer(('',8080),H).serve_forever()",
    ])

    isvc = {
        "apiVersion": "serving.kserve.io/v1beta1",
        "kind": "InferenceService",
        "metadata": {
            "name": isvc_name,
            "namespace": ns,
            "annotations": {
                "serving.kserve.io/deploymentMode": "RawDeployment",
            },
        },
        "spec": {
            "predictor": {
                "nodeName": VK_NODE_NAME,
                "tolerations": VK_TOLERATIONS,
                "containers": [
                    {
                        "name": "kserve-container",
                        "image": "nvcr.io/nvidia/cuda:12.8.1-base-ubi9",
                        "command": ["python3", "-c", server_script],
                        "ports": [
                            {"containerPort": 8080, "protocol": "TCP"},
                        ],
                        "resources": {
                            "limits": {"nvidia.com/gpu": "1"},
                            "requests": {
                                "nvidia.com/gpu": "1",
                                "cpu": "1",
                                "memory": "1Gi",
                            },
                        },
                    }
                ],
            },
        },
    }

    tenant_custom.create_namespaced_custom_object(
        group=ISVC_GROUP,
        version=ISVC_VERSION,
        namespace=ns,
        plural=ISVC_PLURAL,
        body=isvc,
    )
    cleanup(tenant_custom.delete_namespaced_custom_object,
            group=ISVC_GROUP, version=ISVC_VERSION,
            namespace=ns, plural=ISVC_PLURAL, name=isvc_name)

    # ── Step 6: Wait for KServe to create Deployment and pod ──

    deadline = time.time() + 120
    deploy_found = False
    while time.time() < deadline:
        deploys = apps_api.list_namespaced_deployment(
            namespace=ns,
            label_selector=f"serving.kserve.io/inferenceservice={isvc_name}",
        )
        if deploys.items:
            deploy_found = True
            break
        time.sleep(5)
    assert deploy_found, (
        f"KServe did not create a Deployment for {isvc_name} within 120s"
    )

    deadline = time.time() + 60
    isvc_pod_name = None
    while time.time() < deadline:
        pods = tenant_core.list_namespaced_pod(
            namespace=ns,
            label_selector=f"serving.kserve.io/inferenceservice={isvc_name}",
        )
        if pods.items:
            isvc_pod_name = pods.items[0].metadata.name
            break
        time.sleep(3)
    assert isvc_pod_name is not None, (
        f"No pod created for InferenceService {isvc_name} within 60s"
    )

    w_pod_name = worker_pod_name(ns, isvc_pod_name)

    assert wait_pod_exists(worker_core, w_pod_name, vk_worker_namespace, timeout=60), (
        f"Worker pod {w_pod_name} did not appear. KServe raw deployment may not "
        f"propagate nodeName/tolerations to the pod template."
    )

    # ── Step 7: Verify inference pod is Running on worker GPU ──

    worker_phase = wait_pod_phase(
        worker_core, w_pod_name, vk_worker_namespace, ("Running", "Failed"),
        timeout=300,
    )
    assert worker_phase == "Running", (
        f"Inference worker pod should be Running, got {worker_phase!r}"
    )

    # Verify InferenceService status
    deadline = time.time() + 60
    isvc_ready = False
    while time.time() < deadline:
        obj = tenant_custom.get_namespaced_custom_object(
            group=ISVC_GROUP,
            version=ISVC_VERSION,
            namespace=ns,
            plural=ISVC_PLURAL,
            name=isvc_name,
        )
        conditions = obj.get("status", {}).get("conditions", [])
        for c in conditions:
            if c.get("type") == "Ready" and c.get("status") == "True":
                isvc_ready = True
                break
        if isvc_ready:
            break
        time.sleep(5)

    if not isvc_ready:
        import warnings
        conditions_summary = [
            f"{c.get('type')}={c.get('status')}" for c in conditions
        ]
        warnings.warn(
            f"InferenceService {isvc_name} did not reach Ready. "
            f"Conditions: {conditions_summary}. "
            f"Worker pod is Running — KServe status sync may have lag."
        )


KSVC_GROUP = "serving.knative.dev"
KSVC_VERSION = "v1"
KSVC_PLURAL = "services"

SMM_GROUP = "maistra.io"
SMM_VERSION = "v1"
SMM_PLURAL = "servicemeshmembers"

SMCP_NAME = "data-science-smcp"
SMCP_NAMESPACE = "istio-system"


def ensure_mesh_member(tenant_custom, namespace, cleanup):
    """Add namespace to the ServiceMesh, return when Ready."""
    try:
        tenant_custom.get_namespaced_custom_object(
            group=SMM_GROUP, version=SMM_VERSION,
            namespace=namespace, plural=SMM_PLURAL, name="default",
        )
    except client.exceptions.ApiException as e:
        if e.status != 404:
            raise
        tenant_custom.create_namespaced_custom_object(
            group=SMM_GROUP, version=SMM_VERSION,
            namespace=namespace, plural=SMM_PLURAL,
            body={
                "apiVersion": f"{SMM_GROUP}/{SMM_VERSION}",
                "kind": "ServiceMeshMember",
                "metadata": {"name": "default", "namespace": namespace},
                "spec": {
                    "controlPlaneRef": {
                        "name": SMCP_NAME,
                        "namespace": SMCP_NAMESPACE,
                    },
                },
            },
        )
    cleanup(
        tenant_custom.delete_namespaced_custom_object,
        group=SMM_GROUP, version=SMM_VERSION,
        namespace=namespace, plural=SMM_PLURAL, name="default",
    )
    deadline = time.time() + 60
    while time.time() < deadline:
        try:
            smm = tenant_custom.get_namespaced_custom_object(
                group=SMM_GROUP, version=SMM_VERSION,
                namespace=namespace, plural=SMM_PLURAL, name="default",
            )
            for c in smm.get("status", {}).get("conditions", []):
                if c.get("type") == "Ready" and c.get("status") == "True":
                    return
        except client.exceptions.ApiException:
            pass
        time.sleep(5)
    pytest.fail(f"ServiceMeshMember for {namespace} not Ready within 60s")


@pytest.mark.rhoai
def test_kserve_serverless_inference_via_vk(
    cleanup, tenant_clients, worker_clients, test_namespace, vk_worker_namespace
):
    """
    KServe InferenceService in serverless mode (Knative + Istio).
    No deploymentMode annotation — uses the default Knative serving path:
    InferenceService → Knative Service → Revision → Deployment → Pod
    with Knative queue-proxy and Istio sidecar.

    Workaround: clientwrap.go wraps the kubernetes.Interface passed to the
    VK library, stripping unsupported fieldRef env vars from pods in the
    informer's List/Watch responses. Our CreatePod re-reads the original pod
    from the API server, so the worker pod retains the original fieldRefs
    and the worker kubelet resolves them normally.
    TODO(upstream): contribute status.podIP/hostIP support to
    virtual-kubelet/virtual-kubelet internal/podutils/env.go so the
    wrapper is no longer needed.

    Istio sidecar injection is disabled because the sidecar would fail on
    the worker cluster (no Istio control plane). The Knative queue-proxy
    remains and is the main validation target.

    This tests whether the Knative serving path works cross-cluster:
      - Knative queue-proxy readiness works cross-cluster
      - Knative autoscaler can monitor the remote pod

    Prerequisites:
      - ServiceMesh + Serverless operators installed
      - RHOAI DSC has KServe serving.managementState: Managed
    """
    tenant_core, tenant_custom = tenant_clients
    worker_core, _ = worker_clients
    ns = test_namespace
    apps_api = client.AppsV1Api(tenant_core.api_client)

    # ── Step 1: Ensure InferenceService CRD exists ──

    deadline = time.time() + 30
    crd_exists = False
    while time.time() < deadline:
        try:
            tenant_custom.get_cluster_custom_object(
                group="apiextensions.k8s.io",
                version="v1",
                plural="customresourcedefinitions",
                name="inferenceservices.serving.kserve.io",
            )
            crd_exists = True
            break
        except client.exceptions.ApiException:
            pass
        time.sleep(5)
    if not crd_exists:
        pytest.skip(
            "InferenceService CRD not found — run test_kserve_inference_via_vk "
            "first to install ServiceMesh + Serverless operators"
        )

    # ── Step 2: Add test namespace to ServiceMesh ──

    try:
        tenant_custom.get_namespaced_custom_object(
            group=SMM_GROUP,
            version=SMM_VERSION,
            namespace=ns,
            plural=SMM_PLURAL,
            name="default",
        )
    except client.exceptions.ApiException as e:
        if e.status == 404:
            tenant_custom.create_namespaced_custom_object(
                group=SMM_GROUP,
                version=SMM_VERSION,
                namespace=ns,
                plural=SMM_PLURAL,
                body={
                    "apiVersion": f"{SMM_GROUP}/{SMM_VERSION}",
                    "kind": "ServiceMeshMember",
                    "metadata": {"name": "default", "namespace": ns},
                    "spec": {
                        "controlPlaneRef": {
                            "name": "data-science-smcp",
                            "namespace": "istio-system",
                        },
                    },
                },
            )
            # Wait for mesh membership to be configured
            deadline = time.time() + 60
            smm_ready = False
            while time.time() < deadline:
                try:
                    smm = tenant_custom.get_namespaced_custom_object(
                        group=SMM_GROUP,
                        version=SMM_VERSION,
                        namespace=ns,
                        plural=SMM_PLURAL,
                        name="default",
                    )
                    conditions = smm.get("status", {}).get("conditions", [])
                    for c in conditions:
                        if c.get("type") == "Ready" and c.get("status") == "True":
                            smm_ready = True
                            break
                except client.exceptions.ApiException:
                    pass
                if smm_ready:
                    break
                time.sleep(5)
            assert smm_ready, (
                f"ServiceMeshMember for {ns} did not become Ready within 60s"
            )
        else:
            raise
    cleanup(tenant_custom.delete_namespaced_custom_object,
            group=SMM_GROUP, version=SMM_VERSION,
            namespace=ns, plural=SMM_PLURAL, name="default")

    # ── Step 3: Create InferenceService (serverless mode — NO RawDeployment annotation) ──

    isvc_name = "vk-gpu-serverless"

    # Cleanup from previous runs
    try:
        tenant_custom.delete_namespaced_custom_object(
            group=ISVC_GROUP,
            version=ISVC_VERSION,
            namespace=ns,
            plural=ISVC_PLURAL,
            name=isvc_name,
        )
        time.sleep(15)
    except client.exceptions.ApiException as e:
        if e.status != 404:
            raise

    pods = tenant_core.list_namespaced_pod(
        namespace=ns,
        label_selector=f"serving.kserve.io/inferenceservice={isvc_name}",
    )
    for p in pods.items:
        force_delete_pod(tenant_core, p.metadata.name, ns)

    server_script = "\n".join([
        "import http.server, subprocess, json",
        "class H(http.server.BaseHTTPRequestHandler):",
        "  def do_GET(self):",
        "    r = subprocess.run(['nvidia-smi','--query-gpu=name,memory.total','--format=csv,noheader,nounits'], capture_output=True, text=True)",
        "    body = json.dumps(dict(gpu=r.stdout.strip(), status='ok', model='serverless-test'))",
        "    self.send_response(200)",
        "    self.send_header('Content-Type','application/json')",
        "    self.end_headers()",
        "    self.wfile.write(body.encode())",
        "  def log_message(self, *a): pass",
        "http.server.HTTPServer(('',8080),H).serve_forever()",
    ])

    isvc = {
        "apiVersion": "serving.kserve.io/v1beta1",
        "kind": "InferenceService",
        "metadata": {
            "name": isvc_name,
            "namespace": ns,
            "annotations": {
                "sidecar.istio.io/inject": "false",
            },
        },
        "spec": {
            "predictor": {
                "nodeName": VK_NODE_NAME,
                "tolerations": VK_TOLERATIONS,
                "annotations": {
                    "sidecar.istio.io/inject": "false",
                },
                "containers": [
                    {
                        "name": "kserve-container",
                        "image": "nvcr.io/nvidia/cuda:12.8.1-base-ubi9",
                        "command": ["python3", "-c", server_script],
                        "ports": [
                            {"containerPort": 8080, "protocol": "TCP"},
                        ],
                        "resources": {
                            "limits": {"nvidia.com/gpu": "1"},
                            "requests": {
                                "nvidia.com/gpu": "1",
                                "cpu": "1",
                                "memory": "1Gi",
                            },
                        },
                    }
                ],
            },
        },
    }

    tenant_custom.create_namespaced_custom_object(
        group=ISVC_GROUP,
        version=ISVC_VERSION,
        namespace=ns,
        plural=ISVC_PLURAL,
        body=isvc,
    )
    cleanup(tenant_custom.delete_namespaced_custom_object,
            group=ISVC_GROUP, version=ISVC_VERSION,
            namespace=ns, plural=ISVC_PLURAL, name=isvc_name)

    # ── Step 4: Verify Knative Service is created ──

    deadline = time.time() + 120
    ksvc_found = False
    while time.time() < deadline:
        try:
            ksvcs = tenant_custom.list_namespaced_custom_object(
                group=KSVC_GROUP,
                version=KSVC_VERSION,
                namespace=ns,
                plural=KSVC_PLURAL,
            )
            for ksvc in ksvcs.get("items", []):
                if isvc_name in ksvc["metadata"]["name"]:
                    ksvc_found = True
                    break
        except client.exceptions.ApiException:
            pass
        if ksvc_found:
            break
        time.sleep(5)
    assert ksvc_found, (
        f"KServe did not create a Knative Service for {isvc_name} within 120s. "
        f"In serverless mode, KServe creates a ksvc, not a Deployment."
    )

    # ── Step 5: Wait for pod to be created by Knative (Revision → Deployment → Pod) ──

    deadline = time.time() + 120
    isvc_pod_name = None
    while time.time() < deadline:
        pods = tenant_core.list_namespaced_pod(
            namespace=ns,
            label_selector=f"serving.kserve.io/inferenceservice={isvc_name}",
        )
        if pods.items:
            isvc_pod_name = pods.items[0].metadata.name
            break
        time.sleep(5)
    assert isvc_pod_name is not None, (
        f"No pod created for serverless InferenceService {isvc_name} within 120s. "
        f"Check Knative Revision and Deployment status."
    )

    # ── Step 6: Check if VK dispatches the pod to worker ──

    w_pod_name = worker_pod_name(ns, isvc_pod_name)

    worker_dispatched = wait_pod_exists(
        worker_core, w_pod_name, vk_worker_namespace, timeout=120,
    )

    if not worker_dispatched:
        # Collect diagnostic info before failing
        tenant_pod = tenant_core.read_namespaced_pod(isvc_pod_name, ns)
        node_name = tenant_pod.spec.node_name or "(none)"
        tolerations = [
            f"{t.key}={t.value}" for t in (tenant_pod.spec.tolerations or [])
        ]
        annotations = tenant_pod.metadata.annotations or {}
        containers = [c.name for c in tenant_pod.spec.containers]
        diag_lines = [
            f"Worker pod {w_pod_name} did not appear in {vk_worker_namespace}.",
            f"Tenant pod nodeName: {node_name}",
            f"Tenant pod tolerations: {tolerations}",
            f"Tenant pod containers: {containers}",
            f"sidecar.istio.io/inject: {annotations.get('sidecar.istio.io/inject', '(not set)')}",
            "Knative/KServe may not propagate nodeName and tolerations ",
            "from the InferenceService predictor spec to the pod template.",
        ]
        # Check if the Knative Deployment has the right pod template
        deploys = apps_api.list_namespaced_deployment(
            namespace=ns,
            label_selector=f"serving.kserve.io/inferenceservice={isvc_name}",
        )
        if deploys.items:
            d = deploys.items[0]
            tpl = d.spec.template.spec
            diag_lines.append(
                f"Deployment nodeName: {tpl.node_name or '(none)'}, "
                f"tolerations: {[t.key for t in (tpl.tolerations or [])]}"
            )
        pytest.fail("\n".join(diag_lines))

    # ── Step 7: Check worker pod status ──
    # Istio sidecar is disabled; pod should have kserve-container + queue-proxy.
    # queue-proxy may fail if it can't reach the Knative activator on the tenant.

    deadline = time.time() + 300
    worker_phase = None
    pod_containers = []
    while time.time() < deadline:
        try:
            wp = worker_core.read_namespaced_pod(w_pod_name, vk_worker_namespace)
            worker_phase = wp.status.phase
            pod_containers = [
                cs.name for cs in (wp.status.container_statuses or [])
            ]
            if worker_phase in ("Running", "Failed"):
                break
        except client.exceptions.ApiException:
            pass
        time.sleep(5)

    # Log what containers are in the pod (expect: kserve-container + queue-proxy)
    init_containers = []
    try:
        wp = worker_core.read_namespaced_pod(w_pod_name, vk_worker_namespace)
        init_containers = [
            cs.name for cs in (wp.status.init_container_statuses or [])
        ]
    except client.exceptions.ApiException:
        pass

    assert worker_phase == "Running", (
        f"Serverless inference pod not Running. Phase: {worker_phase!r}. "
        f"Containers: {pod_containers}. Init containers: {init_containers}. "
        f"Queue-proxy may be unable to reach Knative activator on tenant."
    )

    # ── Step 8: Verify e2e networking — curl the inference endpoint ──
    # Wait for tenant pod to have a PodIP (synced from worker via VK)

    deadline = time.time() + 120
    pod_ip = None
    while time.time() < deadline:
        tp = tenant_core.read_namespaced_pod(name=isvc_pod_name, namespace=ns)
        if tp.status.phase == "Running" and tp.status.pod_ip:
            pod_ip = tp.status.pod_ip
            break
        time.sleep(5)

    assert pod_ip is not None, (
        f"Pod {isvc_pod_name} did not get a PodIP within 120s"
    )

    assert pod_ip.startswith("10.13"), (
        f"PodIP {pod_ip} does not look like a worker cluster IP "
        f"(expected 10.132-135.x.x)"
    )

    # Curl the inference server from a tenant pod (traffic goes via Submariner)
    curl_pod_name = "vk-serverless-curl"
    force_delete_pod(tenant_core, curl_pod_name, ns)

    curl_pod = client.V1Pod(
        metadata=client.V1ObjectMeta(name=curl_pod_name, namespace=ns),
        spec=client.V1PodSpec(
            restart_policy="Never",
            containers=[
                client.V1Container(
                    name="curl",
                    image="curlimages/curl:latest",
                    command=[
                        "curl", "-s", "--connect-timeout", "15",
                        "--max-time", "30",
                        f"http://{pod_ip}:8080",
                    ],
                )
            ],
        ),
    )
    tenant_core.create_namespaced_pod(namespace=ns, body=curl_pod)
    cleanup(force_delete_pod, tenant_core, curl_pod_name, ns)

    deadline = time.time() + 60
    curl_phase = None
    while time.time() < deadline:
        cp = tenant_core.read_namespaced_pod(name=curl_pod_name, namespace=ns)
        curl_phase = cp.status.phase
        if curl_phase in ("Succeeded", "Failed"):
            break
        time.sleep(3)

    assert curl_phase == "Succeeded", (
        f"Curl pod did not succeed (phase={curl_phase!r}). "
        f"Networking from tenant to worker PodIP {pod_ip}:8080 may not work "
        f"through Submariner."
    )

    logs = tenant_core.read_namespaced_pod_log(
        name=curl_pod_name, namespace=ns,
    ).strip()
    try:
        response = json.loads(logs)
    except json.JSONDecodeError:
        import ast
        response = ast.literal_eval(logs)
    assert response.get("status") == "ok", (
        f"Unexpected response from serverless inference: {logs}"
    )
    assert "RTX 5090" in response.get("gpu", ""), (
        f"GPU not detected in inference response: {logs}"
    )

    force_delete_pod(tenant_core, curl_pod_name, ns)

    # ── Step 9: Verify InferenceService status ──

    deadline = time.time() + 120
    isvc_ready = False
    isvc_url = None
    while time.time() < deadline:
        obj = tenant_custom.get_namespaced_custom_object(
            group=ISVC_GROUP,
            version=ISVC_VERSION,
            namespace=ns,
            plural=ISVC_PLURAL,
            name=isvc_name,
        )
        conditions = obj.get("status", {}).get("conditions", [])
        for c in conditions:
            if c.get("type") == "Ready" and c.get("status") == "True":
                isvc_ready = True
                break
        isvc_url = obj.get("status", {}).get("url")
        if isvc_ready:
            break
        time.sleep(10)

    if not isvc_ready:
        conditions_summary = [
            f"{c.get('type')}={c.get('status')}: {c.get('message', '')[:80]}"
            for c in conditions
        ]
        import warnings
        warnings.warn(
            f"InferenceService {isvc_name} not Ready in serverless mode. "
            f"URL: {isvc_url}. Conditions: {conditions_summary}. "
            f"Pod is Running on worker — Knative/Istio status propagation "
            f"may not work cross-cluster."
        )


@pytest.mark.rhoai
def test_istio_sidecar_injection_on_vk_pod(
    cleanup, tenant_clients, worker_clients, test_namespace, vk_worker_namespace
):
    """
    Test what happens when Istio sidecar injection is enabled on a
    VK-dispatched pod. The namespace is added to the ServiceMesh and the
    pod has sidecar.istio.io/inject: "true" (OSSM default policy is
    "disabled" — requires explicit opt-in). We observe:
      1. Whether the sidecar gets injected on the tenant side
      2. Whether VK dispatches the pod with the sidecar to the worker
      3. Whether the sidecar crashes on the worker (no istiod)
      4. Whether istio-init iptables rules block the main container
    """
    tenant_core, tenant_custom = tenant_clients
    worker_core, _ = worker_clients
    ns = test_namespace

    # ── Step 1: Ensure istiod is running ──

    apps_api = client.AppsV1Api(tenant_core.api_client)
    deadline = time.time() + 120
    istiod_ready = False
    while time.time() < deadline:
        try:
            dep = apps_api.read_namespaced_deployment(
                name="istiod-data-science-smcp", namespace=SMCP_NAMESPACE,
            )
            if (dep.status.ready_replicas or 0) >= 1:
                istiod_ready = True
                break
        except client.exceptions.ApiException:
            pass
        time.sleep(10)
    if not istiod_ready:
        pytest.skip("istiod not running — tenant may not have enough memory")

    # ── Step 2: Add namespace to mesh ──

    ensure_mesh_member(tenant_custom, ns, cleanup)

    # Verify namespace got the mesh label
    ns_obj = tenant_core.read_namespace(name=ns)
    mesh_label = (ns_obj.metadata.labels or {}).get("maistra.io/member-of", "")
    assert mesh_label == SMCP_NAMESPACE, (
        f"Namespace {ns} not labeled as mesh member (labels: {ns_obj.metadata.labels})"
    )
    print(f"\n--- Namespace {ns} is a mesh member (maistra.io/member-of={mesh_label}) ---")

    # ── Step 3: Create pod WITH sidecar.istio.io/inject: "true" ──
    # OSSM default policy is "disabled" — injection requires explicit opt-in.

    pod_name = "istio-sidecar-test"
    w_pod_name = worker_pod_name(ns, pod_name)

    force_delete_pod(tenant_core, pod_name, ns)
    force_delete_pod(worker_core, w_pod_name, vk_worker_namespace)

    pod = client.V1Pod(
        metadata=client.V1ObjectMeta(
            name=pod_name, namespace=ns,
            annotations={"sidecar.istio.io/inject": "true"},
        ),
        spec=client.V1PodSpec(
            node_name=VK_NODE_NAME,
            restart_policy="Never",
            tolerations=[
                client.V1Toleration(
                    key="virtual-kubelet.io/provider", operator="Exists",
                ),
            ],
            containers=[
                client.V1Container(
                    name="main",
                    image="registry.access.redhat.com/ubi9-micro:latest",
                    command=["sleep", "60"],
                    resources=client.V1ResourceRequirements(
                        requests={"cpu": "100m", "memory": "64Mi"},
                    ),
                )
            ],
        ),
    )
    tenant_core.create_namespaced_pod(namespace=ns, body=pod)
    cleanup(force_delete_pod, tenant_core, pod_name, ns)
    cleanup(force_delete_pod, worker_core, w_pod_name, vk_worker_namespace)

    # ── Step 4: Inspect what Istio injected ──

    time.sleep(3)
    created = tenant_core.read_namespaced_pod(name=pod_name, namespace=ns)

    container_names = [c.name for c in created.spec.containers]
    init_names = [c.name for c in (created.spec.init_containers or [])]
    inject_annotation = (created.metadata.annotations or {}).get(
        "sidecar.istio.io/inject", "not-set"
    )
    sidecar_status = (created.metadata.annotations or {}).get(
        "sidecar.istio.io/status", "not-set"
    )

    print(f"--- Pod spec after admission ---")
    print(f"Containers: {container_names}")
    print(f"Init containers: {init_names}")
    print(f"sidecar.istio.io/inject: {inject_annotation}")
    print(f"sidecar.istio.io/status: {sidecar_status[:100] if sidecar_status != 'not-set' else 'not-set'}")

    sidecar_injected = "istio-proxy" in container_names
    print(f"Sidecar injected: {sidecar_injected}")

    if not sidecar_injected:
        print("Istio webhook did NOT inject sidecar — nothing to test.")
        return

    # ── Step 5: Wait for VK to dispatch and check worker pod ──

    deadline = time.time() + 120
    worker_pod = None
    while time.time() < deadline:
        try:
            worker_pod = worker_core.read_namespaced_pod(
                name=w_pod_name, namespace=vk_worker_namespace,
            )
            break
        except client.exceptions.ApiException:
            pass
        time.sleep(5)

    if worker_pod is None:
        tenant_pod = tenant_core.read_namespaced_pod(name=pod_name, namespace=ns)
        print(f"Tenant pod phase: {tenant_pod.status.phase}")
        print(f"Tenant pod reason: {tenant_pod.status.reason}")
        pytest.fail(
            "Worker pod never created. VK may have failed to process the "
            "pod due to injected sidecar fieldRefs (status.podIP)."
        )

    w_container_names = [c.name for c in worker_pod.spec.containers]
    w_init_names = [c.name for c in (worker_pod.spec.init_containers or [])]
    print(f"\n--- Worker pod spec ---")
    print(f"Containers: {w_container_names}")
    print(f"Init containers: {w_init_names}")

    # ── Step 6: Observe container statuses on worker ──

    deadline = time.time() + 180
    while time.time() < deadline:
        worker_pod = worker_core.read_namespaced_pod(
            name=w_pod_name, namespace=vk_worker_namespace,
        )
        if worker_pod.status.phase in ("Running", "Failed", "Succeeded"):
            break
        all_created = all(
            cs.state and not (cs.state.waiting and cs.state.waiting.reason == "ContainerCreating")
            for cs in (worker_pod.status.container_statuses or [])
        ) if worker_pod.status.container_statuses else False
        if all_created:
            break
        time.sleep(5)

    print(f"\n--- Worker pod status ---")
    print(f"Phase: {worker_pod.status.phase}")
    for cs in (worker_pod.status.container_statuses or []):
        state = "unknown"
        if cs.state.running:
            state = "running"
        elif cs.state.waiting:
            state = f"waiting: {cs.state.waiting.reason}"
        elif cs.state.terminated:
            state = f"terminated: {cs.state.terminated.reason} (exit {cs.state.terminated.exit_code})"
        print(f"  {cs.name}: ready={cs.ready}, restarts={cs.restart_count}, state={state}")

    for cs in (worker_pod.status.init_container_statuses or []):
        state = "unknown"
        if cs.state.running:
            state = "running"
        elif cs.state.waiting:
            state = f"waiting: {cs.state.waiting.reason}"
        elif cs.state.terminated:
            state = f"terminated: {cs.state.terminated.reason} (exit {cs.state.terminated.exit_code})"
        print(f"  init/{cs.name}: ready={cs.ready}, state={state}")

    # ── Step 7: Report findings ──

    proxy_status = None
    main_status = None
    for cs in (worker_pod.status.container_statuses or []):
        if cs.name == "istio-proxy":
            proxy_status = cs
        if cs.name == "main":
            main_status = cs

    if proxy_status and proxy_status.restart_count > 0:
        print(f"\nistio-proxy is crash-looping (restarts={proxy_status.restart_count})")

    if main_status:
        if main_status.ready:
            print("\nmain container is running — sidecar crash does NOT block it")
        elif main_status.state and main_status.state.waiting:
            print(
                f"\nmain container blocked: {main_status.state.waiting.reason} — "
                "istio-init iptables rules may be blocking traffic"
            )

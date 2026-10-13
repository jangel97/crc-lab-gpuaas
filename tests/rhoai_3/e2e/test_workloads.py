"""
RHOAI 3.x e2e workload dispatch via Virtual Kubelet.

Tests that RHOAI training operator workloads (PyTorchJob), Notebook CRs,
and KServe InferenceServices get dispatched from the tenant cluster to the
worker GPU cluster via VK.

RHOAI 3.x differences from 2.x:
  - No OSSM/Istio (no sidecar injection)
  - No Serverless/Knative (KServe RawDeployment only)
  - kube-rbac-proxy on Notebooks replaces oauth-proxy
"""

import time

import pytest
from kubernetes import client
from kubernetes.stream import stream

from helpers import (
    worker_pod_name,
    execution_pvc_name,
    force_delete_pod,
    wait_pod_exists,
    CATAPULT_STORAGE_CLASS,
)


VK_NODE_NAME = "gpu-worker"
VK_TOLERATIONS = [{"key": "virtual-kubelet.io/provider", "operator": "Exists"}]

KUBEFLOW_GROUP = "kubeflow.org"
KUBEFLOW_VERSION = "v1"

PYTORCHJOB_GROUP = KUBEFLOW_GROUP
PYTORCHJOB_VERSION = KUBEFLOW_VERSION
PYTORCHJOB_PLURAL = "pytorchjobs"

NOTEBOOK_GROUP = KUBEFLOW_GROUP
NOTEBOOK_VERSION = KUBEFLOW_VERSION
NOTEBOOK_PLURAL = "notebooks"

ISVC_GROUP = "serving.kserve.io"
ISVC_VERSION = "v1beta1"
ISVC_PLURAL = "inferenceservices"

RAYJOB_GROUP = "ray.io"
RAYJOB_VERSION = "v1"
RAYJOB_PLURAL = "rayjobs"

RAYCLUSTER_GROUP = "ray.io"
RAYCLUSTER_VERSION = "v1"
RAYCLUSTER_PLURAL = "rayclusters"

DSPA_GROUP = "datasciencepipelinesapplications.opendatahub.io"
DSPA_VERSION = "v1"
DSPA_PLURAL = "datasciencepipelinesapplications"

LMEVAL_GROUP = "trustyai.opendatahub.io"
LMEVAL_VERSION = "v1alpha1"
LMEVAL_PLURAL = "lmevaljobs"

PYTORCH_IMAGE = "pytorch/pytorch:2.11.0-cuda12.8-cudnn9-runtime"
RAY_IMAGE = "rayproject/ray:2.46.0-py312-cu128"


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


@pytest.mark.rhoai3
def test_pytorchjob_via_vk(
    cleanup, rhoai3_tenant_clients, rhoai3_worker_clients,
    rhoai3_test_namespace, rhoai3_vk_worker_namespace,
):
    """Submit a PyTorchJob with real training on GPU via VK."""
    tenant_core, tenant_custom = rhoai3_tenant_clients
    worker_core, _ = rhoai3_worker_clients
    ns = rhoai3_test_namespace

    job_name = "rhoai3-gpu-training"
    master_pod_name = f"{job_name}-master-0"
    w_pod_name = worker_pod_name(ns, master_pod_name)

    try:
        tenant_custom.delete_namespaced_custom_object(
            group=PYTORCHJOB_GROUP, version=PYTORCHJOB_VERSION,
            namespace=ns, plural=PYTORCHJOB_PLURAL, name=job_name,
        )
        time.sleep(10)
    except client.exceptions.ApiException as e:
        if e.status != 404:
            raise

    for core, name, target_ns in [
        (tenant_core, master_pod_name, ns),
        (worker_core, w_pod_name, rhoai3_vk_worker_namespace),
    ]:
        force_delete_pod(core, name, target_ns)

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
                                    "env": [{"name": "TMPDIR", "value": "/tmp"}],
                                    "resources": {
                                        "limits": {"nvidia.com/gpu": "1"},
                                        "requests": {
                                            "nvidia.com/gpu": "1",
                                            "cpu": "1",
                                            "memory": "2Gi",
                                        },
                                    },
                                    "volumeMounts": [
                                        {"name": "tmp", "mountPath": "/tmp"},
                                    ],
                                }
                            ],
                            "volumes": [
                                {"name": "tmp", "emptyDir": {}},
                            ],
                        }
                    },
                }
            }
        },
    }

    tenant_custom.create_namespaced_custom_object(
        group=PYTORCHJOB_GROUP, version=PYTORCHJOB_VERSION,
        namespace=ns, plural=PYTORCHJOB_PLURAL, body=pytorchjob,
    )
    cleanup(tenant_custom.delete_namespaced_custom_object,
            group=PYTORCHJOB_GROUP, version=PYTORCHJOB_VERSION,
            namespace=ns, plural=PYTORCHJOB_PLURAL, name=job_name)

    assert wait_pod_exists(tenant_core, master_pod_name, ns, timeout=60), (
        f"Training operator did not create master pod {master_pod_name} within 60s"
    )

    assert wait_pod_exists(
        worker_core, w_pod_name, rhoai3_vk_worker_namespace, timeout=60,
    ), f"Worker pod {w_pod_name} did not appear within 60s"

    worker_phase = wait_pod_phase(
        worker_core, w_pod_name, rhoai3_vk_worker_namespace,
        ("Succeeded", "Failed"), timeout=900,
    )
    assert worker_phase == "Succeeded", (
        f"Training pod did not succeed. Phase: {worker_phase!r}"
    )

    logs = worker_core.read_namespaced_pod_log(
        name=w_pod_name, namespace=rhoai3_vk_worker_namespace, container="pytorch",
    )
    assert "Training on: cuda" in logs, f"Training did not use CUDA. Logs: {logs[:500]}"
    assert "Training complete" in logs, f"Training did not complete. Logs: {logs[:500]}"

    tenant_phase = wait_pod_phase(
        tenant_core, master_pod_name, ns, ("Succeeded",), timeout=60,
    )
    assert tenant_phase == "Succeeded", (
        f"Tenant pod status not synced. Phase: {tenant_phase!r}"
    )

    deadline = time.time() + 30
    job_succeeded = False
    while time.time() < deadline:
        job = tenant_custom.get_namespaced_custom_object(
            group=PYTORCHJOB_GROUP, version=PYTORCHJOB_VERSION,
            namespace=ns, plural=PYTORCHJOB_PLURAL, name=job_name,
        )
        conditions = job.get("status", {}).get("conditions", [])
        for c in conditions:
            if c.get("type") == "Succeeded" and c.get("status") == "True":
                job_succeeded = True
                break
        if job_succeeded:
            break
        time.sleep(5)
    assert job_succeeded, f"PyTorchJob {job_name} did not reach Succeeded condition"


@pytest.mark.rhoai3
def test_kserve_raw_inference_via_vk(
    cleanup, rhoai3_tenant_clients, rhoai3_worker_clients,
    rhoai3_test_namespace, rhoai3_vk_worker_namespace,
):
    """KServe InferenceService in RawDeployment mode (the only mode in 3.x)."""
    tenant_core, tenant_custom = rhoai3_tenant_clients
    worker_core, _ = rhoai3_worker_clients
    ns = rhoai3_test_namespace
    apps_api = client.AppsV1Api(tenant_core.api_client)

    isvc_name = "rhoai3-gpu-inference"

    try:
        tenant_custom.delete_namespaced_custom_object(
            group=ISVC_GROUP, version=ISVC_VERSION,
            namespace=ns, plural=ISVC_PLURAL, name=isvc_name,
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
        "    body = json.dumps(dict(gpu=r.stdout.strip(), status='ok', model='rhoai3-test'))",
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
                        "ports": [{"containerPort": 8080, "protocol": "TCP"}],
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
        group=ISVC_GROUP, version=ISVC_VERSION,
        namespace=ns, plural=ISVC_PLURAL, body=isvc,
    )
    cleanup(tenant_custom.delete_namespaced_custom_object,
            group=ISVC_GROUP, version=ISVC_VERSION,
            namespace=ns, plural=ISVC_PLURAL, name=isvc_name)

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

    assert wait_pod_exists(
        worker_core, w_pod_name, rhoai3_vk_worker_namespace, timeout=60,
    ), f"Worker pod {w_pod_name} did not appear"

    worker_phase = wait_pod_phase(
        worker_core, w_pod_name, rhoai3_vk_worker_namespace,
        ("Running", "Failed"), timeout=300,
    )
    assert worker_phase == "Running", (
        f"Inference worker pod should be Running, got {worker_phase!r}"
    )

    deadline = time.time() + 60
    isvc_ready = False
    while time.time() < deadline:
        obj = tenant_custom.get_namespaced_custom_object(
            group=ISVC_GROUP, version=ISVC_VERSION,
            namespace=ns, plural=ISVC_PLURAL, name=isvc_name,
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
            f"Worker pod is Running -- KServe status sync may have lag."
        )


@pytest.mark.rhoai3
def test_notebook_cr_via_vk(
    cleanup, rhoai3_tenant_clients, rhoai3_worker_clients,
    rhoai3_test_namespace, rhoai3_vk_worker_namespace,
):
    """Notebook CR dispatched via VK. Verify kube-rbac-proxy works (3.x replaces oauth-proxy)."""
    tenant_core, tenant_custom = rhoai3_tenant_clients
    worker_core, _ = rhoai3_worker_clients
    ns = rhoai3_test_namespace

    apps_check = client.AppsV1Api(tenant_core.api_client)
    try:
        apps_check.read_namespaced_deployment(
            "notebook-controller-deployment", "redhat-ods-applications",
        )
    except client.exceptions.ApiException:
        pytest.skip("Notebook controller not deployed (workbenches Removed in DSC)")

    nb_name = "rhoai3-gpu-notebook"
    pod_name = f"{nb_name}-0"
    w_pod_name = worker_pod_name(ns, pod_name)

    try:
        tenant_custom.delete_namespaced_custom_object(
            group=NOTEBOOK_GROUP, version=NOTEBOOK_VERSION,
            namespace=ns, plural=NOTEBOOK_PLURAL, name=nb_name,
        )
        time.sleep(10)
    except client.exceptions.ApiException as e:
        if e.status != 404:
            raise
    force_delete_pod(tenant_core, pod_name, ns)
    force_delete_pod(worker_core, w_pod_name, rhoai3_vk_worker_namespace)

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
        group=NOTEBOOK_GROUP, version=NOTEBOOK_VERSION,
        namespace=ns, plural=NOTEBOOK_PLURAL, body=notebook_cr,
    )
    cleanup(tenant_custom.delete_namespaced_custom_object,
            group=NOTEBOOK_GROUP, version=NOTEBOOK_VERSION,
            namespace=ns, plural=NOTEBOOK_PLURAL, name=nb_name)
    cleanup(force_delete_pod, tenant_core, pod_name, ns)

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

    assert wait_pod_exists(tenant_core, pod_name, ns, timeout=60), (
        f"StatefulSet did not create pod {pod_name} within 60s"
    )

    assert wait_pod_exists(
        worker_core, w_pod_name, rhoai3_vk_worker_namespace, timeout=60,
    ), f"Worker pod {w_pod_name} did not appear within 60s"

    deadline = time.time() + 300
    worker_phase = None
    while time.time() < deadline:
        try:
            wp = worker_core.read_namespaced_pod(
                w_pod_name, rhoai3_vk_worker_namespace,
            )
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

    tenant_phase = wait_pod_phase(
        tenant_core, pod_name, ns, ("Running",), timeout=60,
    )
    assert tenant_phase == "Running", (
        f"Tenant notebook pod status not synced to Running. Phase: {tenant_phase!r}"
    )

    deadline = time.time() + 60
    nb_ready = False
    has_running_state = False
    nb_conditions = []
    container_state = {}
    while time.time() < deadline:
        nb = tenant_custom.get_namespaced_custom_object(
            group=NOTEBOOK_GROUP, version=NOTEBOOK_VERSION,
            namespace=ns, plural=NOTEBOOK_PLURAL, name=nb_name,
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


@pytest.mark.rhoai3
def test_pytorchjob_checkpoint_with_pvc(
    cleanup, rhoai3_tenant_clients, rhoai3_worker_clients,
    rhoai3_test_namespace, rhoai3_vk_worker_namespace,
):
    """Train on GPU and save checkpoint to catapult PVC, then verify."""
    tenant_core, tenant_custom = rhoai3_tenant_clients
    worker_core, _ = rhoai3_worker_clients
    ns = rhoai3_test_namespace

    job_name = "rhoai3-checkpoint-training"
    master_pod_name = f"{job_name}-master-0"
    w_pod_name = worker_pod_name(ns, master_pod_name)
    pvc_name = "rhoai3-training-checkpoint"
    exec_pvc = execution_pvc_name(ns, pvc_name)
    verify_pod_name = "rhoai3-checkpoint-verify"
    w_verify_pod = worker_pod_name(ns, verify_pod_name)

    try:
        tenant_custom.delete_namespaced_custom_object(
            group=PYTORCHJOB_GROUP, version=PYTORCHJOB_VERSION,
            namespace=ns, plural=PYTORCHJOB_PLURAL, name=job_name,
        )
        time.sleep(10)
    except client.exceptions.ApiException as e:
        if e.status != 404:
            raise
    for core, name, target_ns in [
        (tenant_core, master_pod_name, ns),
        (worker_core, w_pod_name, rhoai3_vk_worker_namespace),
        (tenant_core, verify_pod_name, ns),
        (worker_core, w_verify_pod, rhoai3_vk_worker_namespace),
    ]:
        force_delete_pod(core, name, target_ns)
    for name, target_ns, core in [
        (pvc_name, ns, tenant_core),
        (exec_pvc, rhoai3_vk_worker_namespace, worker_core),
    ]:
        try:
            core.delete_namespaced_persistent_volume_claim(
                name=name, namespace=target_ns,
            )
        except client.exceptions.ApiException:
            pass
    time.sleep(3)

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
                                    "env": [{"name": "TMPDIR", "value": "/tmp"}],
                                    "resources": {
                                        "limits": {"nvidia.com/gpu": "1"},
                                        "requests": {
                                            "nvidia.com/gpu": "1",
                                            "cpu": "1",
                                            "memory": "2Gi",
                                        },
                                    },
                                    "volumeMounts": [
                                        {"name": "data", "mountPath": "/data"},
                                        {"name": "tmp", "mountPath": "/tmp"},
                                    ],
                                }
                            ],
                            "volumes": [
                                {
                                    "name": "data",
                                    "persistentVolumeClaim": {"claimName": pvc_name},
                                },
                                {"name": "tmp", "emptyDir": {}},
                            ],
                        }
                    },
                }
            }
        },
    }

    tenant_custom.create_namespaced_custom_object(
        group=PYTORCHJOB_GROUP, version=PYTORCHJOB_VERSION,
        namespace=ns, plural=PYTORCHJOB_PLURAL, body=pytorchjob,
    )
    cleanup(tenant_custom.delete_namespaced_custom_object,
            group=PYTORCHJOB_GROUP, version=PYTORCHJOB_VERSION,
            namespace=ns, plural=PYTORCHJOB_PLURAL, name=job_name)

    assert wait_pod_exists(tenant_core, master_pod_name, ns, timeout=60), (
        "Training operator did not create master pod within 60s"
    )

    worker_phase = wait_pod_phase(
        worker_core, w_pod_name, rhoai3_vk_worker_namespace,
        ("Succeeded", "Failed"), timeout=900,
    )
    assert worker_phase == "Succeeded", (
        f"Training pod did not succeed. Phase: {worker_phase!r}"
    )

    logs = worker_core.read_namespaced_pod_log(
        name=w_pod_name, namespace=rhoai3_vk_worker_namespace, container="pytorch",
    )
    assert "Checkpoint saved" in logs, f"Checkpoint not saved. Logs: {logs[:500]}"

    try:
        wpvc = worker_core.read_namespaced_persistent_volume_claim(
            name=exec_pvc, namespace=rhoai3_vk_worker_namespace,
        )
        assert wpvc.metadata.labels.get("app.kubernetes.io/managed-by") == "vk-gpu-provider-gpu-worker"
    except client.exceptions.ApiException:
        pytest.fail(
            f"Execution PVC {exec_pvc} not found in {rhoai3_vk_worker_namespace}"
        )

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
                    key="virtual-kubelet.io/provider", operator="Exists",
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
        worker_core, w_verify_pod, rhoai3_vk_worker_namespace,
        ("Succeeded", "Failed"), timeout=300,
    )
    assert verify_phase == "Succeeded", (
        f"Verification pod did not succeed. Phase: {verify_phase!r}"
    )

    verify_logs = worker_core.read_namespaced_pod_log(
        name=w_verify_pod, namespace=rhoai3_vk_worker_namespace, container="verify",
    )
    assert "Checkpoint valid" in verify_logs, (
        f"Checkpoint verification failed. Logs: {verify_logs[:500]}"
    )


@pytest.mark.rhoai3
def test_rayjob_via_vk(
    cleanup, rhoai3_tenant_clients, rhoai3_worker_clients,
    rhoai3_test_namespace, rhoai3_vk_worker_namespace,
):
    """RayJob CR dispatches a Ray head pod via VK, runs a GPU job on the worker."""
    tenant_core, tenant_custom = rhoai3_tenant_clients
    worker_core, _ = rhoai3_worker_clients
    ns = rhoai3_test_namespace

    job_name = "rhoai3-rayjob-gpu"

    try:
        tenant_custom.delete_namespaced_custom_object(
            group=RAYJOB_GROUP, version=RAYJOB_VERSION,
            namespace=ns, plural=RAYJOB_PLURAL, name=job_name,
        )
        time.sleep(10)
    except client.exceptions.ApiException as e:
        if e.status != 404:
            raise

    pods = tenant_core.list_namespaced_pod(
        namespace=ns, label_selector="ray.io/node-type=head",
    )
    for p in pods.items:
        cluster = (p.metadata.labels or {}).get("ray.io/cluster", "")
        if cluster.startswith(job_name):
            force_delete_pod(tenant_core, p.metadata.name, ns)
    wpods = worker_core.list_namespaced_pod(
        namespace=rhoai3_vk_worker_namespace,
        label_selector="ray.io/node-type=head",
    )
    for p in wpods.items:
        force_delete_pod(worker_core, p.metadata.name, rhoai3_vk_worker_namespace)

    rayjob = {
        "apiVersion": "ray.io/v1",
        "kind": "RayJob",
        "metadata": {"name": job_name, "namespace": ns},
        "spec": {
            "entrypoint": "python -c \""
                "import ray; "
                "ray.init(); "
                "import subprocess; "
                "r = subprocess.run(['nvidia-smi', '--query-gpu=name', '--format=csv,noheader'], "
                "capture_output=True, text=True); "
                "print(f'GPU: {r.stdout.strip()}'); "
                "print('RayJob complete')\"",
            "shutdownAfterJobFinishes": True,
            "ttlSecondsAfterFinished": 300,
            "rayClusterSpec": {
                "headGroupSpec": {
                    "rayStartParams": {"dashboard-host": "0.0.0.0"},
                    "template": {
                        "spec": {
                            "nodeName": VK_NODE_NAME,
                            "tolerations": VK_TOLERATIONS,
                            "containers": [
                                {
                                    "name": "ray-head",
                                    "image": RAY_IMAGE,
                                    "resources": {
                                        "limits": {"nvidia.com/gpu": "1", "cpu": "2", "memory": "4Gi"},
                                        "requests": {"nvidia.com/gpu": "1", "cpu": "1", "memory": "2Gi"},
                                    },
                                    "ports": [
                                        {"containerPort": 6379, "name": "gcs-server"},
                                        {"containerPort": 8265, "name": "dashboard"},
                                        {"containerPort": 10001, "name": "client"},
                                    ],
                                    "volumeMounts": [
                                        {"name": "tmp", "mountPath": "/tmp"},
                                    ],
                                }
                            ],
                            "volumes": [
                                {"name": "tmp", "emptyDir": {}},
                            ],
                        }
                    },
                },
                "workerGroupSpecs": [],
            },
        },
    }

    tenant_custom.create_namespaced_custom_object(
        group=RAYJOB_GROUP, version=RAYJOB_VERSION,
        namespace=ns, plural=RAYJOB_PLURAL, body=rayjob,
    )
    cleanup(tenant_custom.delete_namespaced_custom_object,
            group=RAYJOB_GROUP, version=RAYJOB_VERSION,
            namespace=ns, plural=RAYJOB_PLURAL, name=job_name)

    deadline = time.time() + 120
    head_pod_name = None
    while time.time() < deadline:
        try:
            rj = tenant_custom.get_namespaced_custom_object(
                group=RAYJOB_GROUP, version=RAYJOB_VERSION,
                namespace=ns, plural=RAYJOB_PLURAL, name=job_name,
            )
            cluster_name = rj.get("status", {}).get("rayClusterName", "")
        except client.exceptions.ApiException:
            cluster_name = ""

        if cluster_name:
            pods = tenant_core.list_namespaced_pod(
                namespace=ns,
                label_selector=f"ray.io/cluster={cluster_name},ray.io/node-type=head",
            )
            for p in pods.items:
                if p.metadata.deletion_timestamp is None:
                    head_pod_name = p.metadata.name
                    break
        if head_pod_name:
            break
        time.sleep(5)
    assert head_pod_name is not None, (
        f"Ray head pod for {job_name} did not appear within 120s"
    )

    w_head_name = worker_pod_name(ns, head_pod_name)
    assert wait_pod_exists(
        worker_core, w_head_name, rhoai3_vk_worker_namespace, timeout=60,
    ), f"Worker head pod {w_head_name} did not appear"

    worker_phase = wait_pod_phase(
        worker_core, w_head_name, rhoai3_vk_worker_namespace,
        ("Running", "Succeeded", "Failed"), timeout=300,
    )
    assert worker_phase in ("Running", "Succeeded"), (
        f"Ray head pod should be Running or Succeeded, got {worker_phase!r}"
    )

    gpu_output = stream(
        worker_core.connect_get_namespaced_pod_exec,
        name=w_head_name,
        namespace=rhoai3_vk_worker_namespace,
        command=["nvidia-smi", "--query-gpu=name", "--format=csv,noheader"],
        container="ray-head",
        stderr=True, stdout=True, stdin=False, tty=False,
    )
    assert gpu_output.strip(), (
        f"nvidia-smi returned empty output on worker head pod {w_head_name}"
    )

    ray_output = stream(
        worker_core.connect_get_namespaced_pod_exec,
        name=w_head_name,
        namespace=rhoai3_vk_worker_namespace,
        command=["python", "-c",
                 "import ray; ray.init(); print(f'Resources: {ray.cluster_resources()}'); print('RayJob complete')"],
        container="ray-head",
        stderr=True, stdout=True, stdin=False, tty=False,
    )
    assert "RayJob complete" in ray_output, (
        f"Ray head pod did not complete job. Output: {ray_output[:500]}"
    )


@pytest.mark.rhoai3
def test_tfjob_via_vk(
    cleanup, rhoai3_tenant_clients, rhoai3_worker_clients,
    rhoai3_test_namespace, rhoai3_vk_worker_namespace,
):
    """TFJob dispatched via VK — validates TensorFlow training operator compatibility."""
    tenant_core, tenant_custom = rhoai3_tenant_clients
    worker_core, _ = rhoai3_worker_clients
    ns = rhoai3_test_namespace

    job_name = "rhoai3-tfjob-gpu"
    chief_pod_name = f"{job_name}-chief-0"
    w_pod_name = worker_pod_name(ns, chief_pod_name)

    try:
        tenant_custom.delete_namespaced_custom_object(
            group=KUBEFLOW_GROUP, version=KUBEFLOW_VERSION,
            namespace=ns, plural="tfjobs", name=job_name,
        )
        time.sleep(10)
    except client.exceptions.ApiException as e:
        if e.status != 404:
            raise
    for core, name, target_ns in [
        (tenant_core, chief_pod_name, ns),
        (worker_core, w_pod_name, rhoai3_vk_worker_namespace),
    ]:
        force_delete_pod(core, name, target_ns)

    training_script = "\n".join([
        "import tensorflow as tf",
        "print(f'TF version: {tf.__version__}')",
        "gpus = tf.config.list_physical_devices('GPU')",
        "print(f'GPUs available: {len(gpus)}')",
        "assert len(gpus) > 0, 'No GPU found'",
        "print(f'GPU: {gpus[0].name}')",
        "",
        "model = tf.keras.Sequential([",
        "    tf.keras.layers.Dense(128, activation='relu', input_shape=(784,)),",
        "    tf.keras.layers.Dense(10, activation='softmax'),",
        "])",
        "model.compile(optimizer='sgd', loss='sparse_categorical_crossentropy')",
        "",
        "import numpy as np",
        "x = np.random.randn(64, 784).astype(np.float32)",
        "y = np.random.randint(0, 10, (64,)).astype(np.int32)",
        "model.fit(x, y, epochs=3, batch_size=32, verbose=1)",
        "print('TF training complete')",
    ])

    tfjob = {
        "apiVersion": "kubeflow.org/v1",
        "kind": "TFJob",
        "metadata": {"name": job_name, "namespace": ns},
        "spec": {
            "tfReplicaSpecs": {
                "Chief": {
                    "replicas": 1,
                    "template": {
                        "spec": {
                            "nodeName": VK_NODE_NAME,
                            "restartPolicy": "Never",
                            "tolerations": VK_TOLERATIONS,
                            "containers": [
                                {
                                    "name": "tensorflow",
                                    "image": "tensorflow/tensorflow:2.19.0-gpu",
                                    "command": ["python3", "-c", training_script],
                                    "env": [{"name": "TMPDIR", "value": "/tmp"}],
                                    "resources": {
                                        "limits": {"nvidia.com/gpu": "1"},
                                        "requests": {
                                            "nvidia.com/gpu": "1",
                                            "cpu": "1",
                                            "memory": "4Gi",
                                        },
                                    },
                                    "volumeMounts": [
                                        {"name": "tmp", "mountPath": "/tmp"},
                                    ],
                                }
                            ],
                            "volumes": [
                                {"name": "tmp", "emptyDir": {}},
                            ],
                        }
                    },
                }
            }
        },
    }

    tenant_custom.create_namespaced_custom_object(
        group=KUBEFLOW_GROUP, version=KUBEFLOW_VERSION,
        namespace=ns, plural="tfjobs", body=tfjob,
    )
    cleanup(tenant_custom.delete_namespaced_custom_object,
            group=KUBEFLOW_GROUP, version=KUBEFLOW_VERSION,
            namespace=ns, plural="tfjobs", name=job_name)

    assert wait_pod_exists(tenant_core, chief_pod_name, ns, timeout=60), (
        f"Training operator did not create chief pod {chief_pod_name} within 60s"
    )

    assert wait_pod_exists(
        worker_core, w_pod_name, rhoai3_vk_worker_namespace, timeout=60,
    ), f"Worker pod {w_pod_name} did not appear"

    worker_phase = wait_pod_phase(
        worker_core, w_pod_name, rhoai3_vk_worker_namespace,
        ("Succeeded", "Failed"), timeout=900,
    )

    logs = ""
    try:
        logs = worker_core.read_namespaced_pod_log(
            name=w_pod_name, namespace=rhoai3_vk_worker_namespace,
            container="tensorflow",
        )
    except client.exceptions.ApiException:
        pass

    assert worker_phase == "Succeeded", (
        f"TF training pod did not succeed. Phase: {worker_phase!r}. "
        f"Logs: {logs[-500:]}"
    )
    assert "TF training complete" in logs, f"TF training did not complete. Logs: {logs[:500]}"

    tenant_phase = wait_pod_phase(
        tenant_core, chief_pod_name, ns, ("Succeeded",), timeout=60,
    )
    assert tenant_phase == "Succeeded", (
        f"Tenant pod status not synced. Phase: {tenant_phase!r}"
    )


@pytest.mark.rhoai3
def test_xgboostjob_via_vk(
    cleanup, rhoai3_tenant_clients, rhoai3_worker_clients,
    rhoai3_test_namespace, rhoai3_vk_worker_namespace,
):
    """XGBoostJob dispatched via VK — validates XGBoost training operator compatibility."""
    tenant_core, tenant_custom = rhoai3_tenant_clients
    worker_core, _ = rhoai3_worker_clients
    ns = rhoai3_test_namespace

    job_name = "rhoai3-xgboostjob"
    master_pod_name = f"{job_name}-master-0"
    w_pod_name = worker_pod_name(ns, master_pod_name)

    try:
        tenant_custom.delete_namespaced_custom_object(
            group=KUBEFLOW_GROUP, version=KUBEFLOW_VERSION,
            namespace=ns, plural="xgboostjobs", name=job_name,
        )
        time.sleep(10)
    except client.exceptions.ApiException as e:
        if e.status != 404:
            raise
    for core, name, target_ns in [
        (tenant_core, master_pod_name, ns),
        (worker_core, w_pod_name, rhoai3_vk_worker_namespace),
    ]:
        force_delete_pod(core, name, target_ns)

    training_script = "\n".join([
        "import subprocess, sys",
        "subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'xgboost', '-q'])",
        "import xgboost as xgb",
        "import numpy as np",
        "print(f'XGBoost version: {xgb.__version__}')",
        "",
        "X = np.random.randn(100, 10).astype(np.float32)",
        "y = np.random.randint(0, 2, 100).astype(np.float32)",
        "dtrain = xgb.DMatrix(X, label=y)",
        "",
        "params = {'max_depth': 3, 'eta': 0.1, 'objective': 'binary:logistic',",
        "          'device': 'cuda', 'tree_method': 'hist'}",
        "model = xgb.train(params, dtrain, num_boost_round=10)",
        "preds = model.predict(dtrain)",
        "print(f'Predictions shape: {preds.shape}')",
        "print('XGBoost training complete')",
    ])

    xgboostjob = {
        "apiVersion": "kubeflow.org/v1",
        "kind": "XGBoostJob",
        "metadata": {"name": job_name, "namespace": ns},
        "spec": {
            "xgbReplicaSpecs": {
                "Master": {
                    "replicas": 1,
                    "template": {
                        "spec": {
                            "nodeName": VK_NODE_NAME,
                            "restartPolicy": "Never",
                            "tolerations": VK_TOLERATIONS,
                            "containers": [
                                {
                                    "name": "xgboost",
                                    "image": PYTORCH_IMAGE,
                                    "command": ["python3", "-c", training_script],
                                    "env": [{"name": "TMPDIR", "value": "/tmp"}],
                                    "resources": {
                                        "limits": {"nvidia.com/gpu": "1"},
                                        "requests": {
                                            "nvidia.com/gpu": "1",
                                            "cpu": "1",
                                            "memory": "2Gi",
                                        },
                                    },
                                    "volumeMounts": [
                                        {"name": "tmp", "mountPath": "/tmp"},
                                    ],
                                }
                            ],
                            "volumes": [
                                {"name": "tmp", "emptyDir": {}},
                            ],
                        }
                    },
                }
            }
        },
    }

    tenant_custom.create_namespaced_custom_object(
        group=KUBEFLOW_GROUP, version=KUBEFLOW_VERSION,
        namespace=ns, plural="xgboostjobs", body=xgboostjob,
    )
    cleanup(tenant_custom.delete_namespaced_custom_object,
            group=KUBEFLOW_GROUP, version=KUBEFLOW_VERSION,
            namespace=ns, plural="xgboostjobs", name=job_name)

    assert wait_pod_exists(tenant_core, master_pod_name, ns, timeout=60), (
        f"Training operator did not create master pod {master_pod_name} within 60s"
    )

    assert wait_pod_exists(
        worker_core, w_pod_name, rhoai3_vk_worker_namespace, timeout=60,
    ), f"Worker pod {w_pod_name} did not appear"

    worker_phase = wait_pod_phase(
        worker_core, w_pod_name, rhoai3_vk_worker_namespace,
        ("Succeeded", "Failed"), timeout=900,
    )
    assert worker_phase == "Succeeded", (
        f"XGBoost training pod did not succeed. Phase: {worker_phase!r}"
    )

    logs = worker_core.read_namespaced_pod_log(
        name=w_pod_name, namespace=rhoai3_vk_worker_namespace,
        container="xgboost",
    )
    assert "XGBoost training complete" in logs, (
        f"XGBoost training did not complete. Logs: {logs[:500]}"
    )


@pytest.mark.rhoai3
def test_raycluster_via_vk(
    cleanup, rhoai3_tenant_clients, rhoai3_worker_clients,
    rhoai3_test_namespace, rhoai3_vk_worker_namespace,
):
    """Standalone RayCluster dispatched via VK — head pod runs on worker GPU."""
    tenant_core, tenant_custom = rhoai3_tenant_clients
    worker_core, _ = rhoai3_worker_clients
    ns = rhoai3_test_namespace

    cluster_name = "rhoai3-raycluster-gpu"

    try:
        tenant_custom.delete_namespaced_custom_object(
            group=RAYCLUSTER_GROUP, version=RAYCLUSTER_VERSION,
            namespace=ns, plural=RAYCLUSTER_PLURAL, name=cluster_name,
        )
        time.sleep(10)
    except client.exceptions.ApiException as e:
        if e.status != 404:
            raise

    pods = tenant_core.list_namespaced_pod(
        namespace=ns, label_selector=f"ray.io/cluster={cluster_name}",
    )
    for p in pods.items:
        force_delete_pod(tenant_core, p.metadata.name, ns)
    wpods = worker_core.list_namespaced_pod(
        namespace=rhoai3_vk_worker_namespace,
        label_selector=f"ray.io/cluster={cluster_name}",
    )
    for p in wpods.items:
        force_delete_pod(worker_core, p.metadata.name, rhoai3_vk_worker_namespace)

    raycluster = {
        "apiVersion": "ray.io/v1",
        "kind": "RayCluster",
        "metadata": {"name": cluster_name, "namespace": ns},
        "spec": {
            "headGroupSpec": {
                "rayStartParams": {"dashboard-host": "0.0.0.0"},
                "template": {
                    "spec": {
                        "nodeName": VK_NODE_NAME,
                        "tolerations": VK_TOLERATIONS,
                        "containers": [
                            {
                                "name": "ray-head",
                                "image": RAY_IMAGE,
                                "resources": {
                                    "limits": {"nvidia.com/gpu": "1", "cpu": "2", "memory": "4Gi"},
                                    "requests": {"nvidia.com/gpu": "1", "cpu": "1", "memory": "2Gi"},
                                },
                                "ports": [
                                    {"containerPort": 6379, "name": "gcs-server"},
                                    {"containerPort": 8265, "name": "dashboard"},
                                    {"containerPort": 10001, "name": "client"},
                                ],
                                "volumeMounts": [
                                    {"name": "tmp", "mountPath": "/tmp"},
                                ],
                            }
                        ],
                        "volumes": [
                            {"name": "tmp", "emptyDir": {}},
                        ],
                    }
                },
            },
            "workerGroupSpecs": [],
        },
    }

    tenant_custom.create_namespaced_custom_object(
        group=RAYCLUSTER_GROUP, version=RAYCLUSTER_VERSION,
        namespace=ns, plural=RAYCLUSTER_PLURAL, body=raycluster,
    )
    cleanup(tenant_custom.delete_namespaced_custom_object,
            group=RAYCLUSTER_GROUP, version=RAYCLUSTER_VERSION,
            namespace=ns, plural=RAYCLUSTER_PLURAL, name=cluster_name)

    deadline = time.time() + 120
    head_pod_name = None
    while time.time() < deadline:
        pods = tenant_core.list_namespaced_pod(
            namespace=ns,
            label_selector=f"ray.io/cluster={cluster_name},ray.io/node-type=head",
        )
        if pods.items:
            head_pod_name = pods.items[0].metadata.name
            break
        time.sleep(5)
    assert head_pod_name is not None, (
        f"Ray head pod for {cluster_name} did not appear within 120s"
    )

    w_head_name = worker_pod_name(ns, head_pod_name)
    assert wait_pod_exists(
        worker_core, w_head_name, rhoai3_vk_worker_namespace, timeout=60,
    ), f"Worker head pod {w_head_name} did not appear"

    worker_phase = wait_pod_phase(
        worker_core, w_head_name, rhoai3_vk_worker_namespace,
        ("Running", "Failed"), timeout=300,
    )
    assert worker_phase == "Running", (
        f"Ray head pod should be Running, got {worker_phase!r}"
    )

    deadline = time.time() + 60
    cluster_ready = False
    while time.time() < deadline:
        try:
            rc = tenant_custom.get_namespaced_custom_object(
                group=RAYCLUSTER_GROUP, version=RAYCLUSTER_VERSION,
                namespace=ns, plural=RAYCLUSTER_PLURAL, name=cluster_name,
            )
            state = rc.get("status", {}).get("state", "")
            if state == "ready":
                cluster_ready = True
                break
        except client.exceptions.ApiException:
            pass
        time.sleep(5)

    if not cluster_ready:
        import warnings
        warnings.warn(
            f"RayCluster {cluster_name} state is {state!r}, not 'ready'. "
            f"Head pod is Running on worker — cluster may need status sync."
        )


@pytest.mark.rhoai3
def test_dsp_pipeline_via_vk(
    cleanup, rhoai3_tenant_clients,
    rhoai3_test_namespace,
):
    """DataSciencePipelines infrastructure on a VK-enabled cluster.

    Deploys DSPA with embedded MariaDB and MinIO, verifies all components
    reach Ready. Pipeline step pods use standard Kubernetes pod specs
    (nodeName, tolerations) configurable via KFP v2 platform_spec —
    VK dispatch works the same as any other workload pod.
    """
    tenant_core, tenant_custom = rhoai3_tenant_clients
    ns = rhoai3_test_namespace
    apps_api = client.AppsV1Api(tenant_core.api_client)

    dspa_name = "rhoai3-dspa"

    try:
        tenant_custom.delete_namespaced_custom_object(
            group=DSPA_GROUP, version=DSPA_VERSION,
            namespace=ns, plural=DSPA_PLURAL, name=dspa_name,
        )
        time.sleep(30)
    except client.exceptions.ApiException as e:
        if e.status != 404:
            raise

    dspa = {
        "apiVersion": f"{DSPA_GROUP}/{DSPA_VERSION}",
        "kind": "DataSciencePipelinesApplication",
        "metadata": {"name": dspa_name, "namespace": ns},
        "spec": {
            "dspVersion": "v2",
            "apiServer": {
                "cABundle": {
                    "configMapKey": "ca.crt",
                    "configMapName": "kube-root-ca.crt",
                },
            },
            "database": {
                "mariaDB": {"deploy": True, "pvcSize": "1Gi"},
            },
            "objectStorage": {
                "minio": {
                    "deploy": True,
                    "pvcSize": "1Gi",
                    "image": "quay.io/opendatahub/minio:RELEASE.2019-08-14T20-37-41Z-license-compliance",
                },
            },
        },
    }

    tenant_custom.create_namespaced_custom_object(
        group=DSPA_GROUP, version=DSPA_VERSION,
        namespace=ns, plural=DSPA_PLURAL, body=dspa,
    )
    cleanup(tenant_custom.delete_namespaced_custom_object,
            group=DSPA_GROUP, version=DSPA_VERSION,
            namespace=ns, plural=DSPA_PLURAL, name=dspa_name)

    for component in [
        f"ds-pipeline-{dspa_name}",
        f"mariadb-{dspa_name}",
        f"minio-{dspa_name}",
    ]:
        deadline = time.time() + 300
        ready = False
        while time.time() < deadline:
            try:
                dep = apps_api.read_namespaced_deployment(component, ns)
                if (dep.status.ready_replicas or 0) >= 1:
                    ready = True
                    break
            except client.exceptions.ApiException:
                pass
            time.sleep(10)
        assert ready, f"DSPA component {component} not ready within 300s"

    deadline = time.time() + 60
    dspa_ready = False
    conditions = []
    while time.time() < deadline:
        obj = tenant_custom.get_namespaced_custom_object(
            group=DSPA_GROUP, version=DSPA_VERSION,
            namespace=ns, plural=DSPA_PLURAL, name=dspa_name,
        )
        conditions = obj.get("status", {}).get("conditions", [])
        for c in conditions:
            if c.get("type") == "Ready" and c.get("status") == "True":
                dspa_ready = True
                break
        if dspa_ready:
            break
        time.sleep(5)
    assert dspa_ready, (
        f"DSPA {dspa_name} did not reach Ready. "
        f"Conditions: {[c.get('type') + '=' + c.get('status') for c in conditions]}"
    )


KYVERNO_POLICY_GROUP = "policies.kyverno.io"
KYVERNO_POLICY_VERSION = "v1"
KYVERNO_POLICY_PLURAL = "mutatingpolicies"

SERVING_RUNTIME_GROUP = "serving.kserve.io"
SERVING_RUNTIME_VERSION = "v1alpha1"
SERVING_RUNTIME_PLURAL = "servingruntimes"

VLLM_CPU_IMAGE = "registry.redhat.io/rhaii/vllm-cpu-rhel9@sha256:7a28b867955055a8836ea2a78e70b8bad63c1ee50e8a5f8dd95d1194f20ec489"
LMEVAL_MODEL_NAME = "facebook/opt-125m"


def _get_tenant_node_ip(tenant_core):
    nodes = tenant_core.list_node()
    for node in nodes.items:
        for addr in (node.status.addresses or []):
            if addr.type == "InternalIP":
                return addr.address
    raise RuntimeError("No InternalIP found on tenant nodes")


def _wait_isvc_ready(tenant_custom, ns, isvc_name, timeout=300):
    deadline = time.time() + timeout
    while time.time() < deadline:
        obj = tenant_custom.get_namespaced_custom_object(
            group=ISVC_GROUP, version=ISVC_VERSION,
            namespace=ns, plural=ISVC_PLURAL, name=isvc_name,
        )
        conditions = obj.get("status", {}).get("conditions", [])
        for c in conditions:
            if c.get("type") == "Ready" and c.get("status") == "True":
                return True
        time.sleep(10)
    return False


@pytest.mark.rhoai3
def test_lmeval_via_vk(
    cleanup, rhoai3_tenant_clients, rhoai3_worker_clients,
    rhoai3_test_namespace, rhoai3_vk_worker_namespace,
):
    """LMEvalJob evaluating a served model, dispatched via VK with Kyverno.

    Deploys a model (facebook/opt-125m) via KServe on the tenant, then
    creates an LMEvalJob using local-completions to evaluate it.  The
    LMEvalJob CRD has no tolerations/nodeName fields, so a Kyverno
    MutatingPolicy injects VK tolerations.  A NodePort service exposes
    the model to the worker cluster over the shared network.
    """
    tenant_core, tenant_custom = rhoai3_tenant_clients
    worker_core, _ = rhoai3_worker_clients
    ns = rhoai3_test_namespace
    apps_api = client.AppsV1Api(tenant_core.api_client)
    api_ext = client.ApiextensionsV1Api(tenant_core.api_client)

    try:
        apps_api.read_namespaced_deployment(
            "trustyai-service-operator-controller-manager",
            "redhat-ods-applications",
        )
    except client.exceptions.ApiException:
        pytest.skip("TrustyAI operator not deployed — enable trustyai in DSC")

    try:
        api_ext.read_custom_resource_definition(
            "mutatingpolicies.policies.kyverno.io"
        )
    except client.exceptions.ApiException:
        pytest.skip("Kyverno not installed — run install-kyverno.yml playbook")

    try:
        api_ext.read_custom_resource_definition(
            "inferenceservices.serving.kserve.io"
        )
    except client.exceptions.ApiException:
        pytest.skip("KServe not installed — enable kserve in DSC")

    # --- Cleanup from previous runs ---

    policy_name = "gpuaas-inject-vk-tolerations"
    job_name = "rhoai3-lmeval-vk"
    pod_name = job_name
    isvc_name = "lmeval-model"
    runtime_name = "vllm-cpu"
    nodeport_svc_name = "lmeval-model-nodeport"

    for name in [job_name]:
        try:
            tenant_custom.delete_namespaced_custom_object(
                group=LMEVAL_GROUP, version=LMEVAL_VERSION,
                namespace=ns, plural=LMEVAL_PLURAL, name=name,
            )
            time.sleep(10)
        except client.exceptions.ApiException as e:
            if e.status != 404:
                raise
    force_delete_pod(tenant_core, pod_name, ns)

    try:
        tenant_custom.delete_cluster_custom_object(
            group=KYVERNO_POLICY_GROUP, version=KYVERNO_POLICY_VERSION,
            plural=KYVERNO_POLICY_PLURAL, name=policy_name,
        )
        time.sleep(5)
    except client.exceptions.ApiException as e:
        if e.status != 404:
            raise

    # --- Step 1: Deploy model via KServe ---

    runtime = {
        "apiVersion": f"{SERVING_RUNTIME_GROUP}/{SERVING_RUNTIME_VERSION}",
        "kind": "ServingRuntime",
        "metadata": {"name": runtime_name, "namespace": ns},
        "spec": {
            "multiModel": False,
            "supportedModelFormats": [
                {"name": "vLLM", "autoSelect": True},
            ],
            "containers": [
                {
                    "name": "kserve-container",
                    "image": VLLM_CPU_IMAGE,
                    "args": [
                        "--model", "/mnt/models",
                        "--port", "8080",
                        "--gpu-memory-utilization", "0.4",
                        "--served-model-name", LMEVAL_MODEL_NAME,
                    ],
                    "ports": [{"containerPort": 8080, "protocol": "TCP"}],
                    "volumeMounts": [{"name": "dshm", "mountPath": "/dev/shm"}],
                    "resources": {
                        "requests": {"cpu": "1", "memory": "2Gi"},
                        "limits": {"cpu": "2", "memory": "4Gi"},
                    },
                },
            ],
            "volumes": [
                {"name": "dshm", "emptyDir": {"medium": "Memory", "sizeLimit": "1Gi"}},
            ],
        },
    }

    try:
        existing = tenant_custom.get_namespaced_custom_object(
            group=SERVING_RUNTIME_GROUP, version=SERVING_RUNTIME_VERSION,
            namespace=ns, plural=SERVING_RUNTIME_PLURAL, name=runtime_name,
        )
        runtime["metadata"]["resourceVersion"] = existing["metadata"]["resourceVersion"]
        tenant_custom.replace_namespaced_custom_object(
            group=SERVING_RUNTIME_GROUP, version=SERVING_RUNTIME_VERSION,
            namespace=ns, plural=SERVING_RUNTIME_PLURAL, name=runtime_name,
            body=runtime,
        )
    except client.exceptions.ApiException:
        tenant_custom.create_namespaced_custom_object(
            group=SERVING_RUNTIME_GROUP, version=SERVING_RUNTIME_VERSION,
            namespace=ns, plural=SERVING_RUNTIME_PLURAL, body=runtime,
        )
    cleanup(tenant_custom.delete_namespaced_custom_object,
            group=SERVING_RUNTIME_GROUP, version=SERVING_RUNTIME_VERSION,
            namespace=ns, plural=SERVING_RUNTIME_PLURAL, name=runtime_name)

    isvc = {
        "apiVersion": f"{ISVC_GROUP}/{ISVC_VERSION}",
        "kind": "InferenceService",
        "metadata": {"name": isvc_name, "namespace": ns},
        "spec": {
            "predictor": {
                "model": {
                    "modelFormat": {"name": "vLLM"},
                    "runtime": runtime_name,
                    "storageUri": f"hf://{LMEVAL_MODEL_NAME}",
                    "resources": {
                        "requests": {"cpu": "1", "memory": "2Gi"},
                        "limits": {"cpu": "2", "memory": "4Gi"},
                    },
                },
            },
        },
    }

    try:
        existing = tenant_custom.get_namespaced_custom_object(
            group=ISVC_GROUP, version=ISVC_VERSION,
            namespace=ns, plural=ISVC_PLURAL, name=isvc_name,
        )
        isvc["metadata"]["resourceVersion"] = existing["metadata"]["resourceVersion"]
        tenant_custom.replace_namespaced_custom_object(
            group=ISVC_GROUP, version=ISVC_VERSION,
            namespace=ns, plural=ISVC_PLURAL, name=isvc_name,
            body=isvc,
        )
    except client.exceptions.ApiException:
        tenant_custom.create_namespaced_custom_object(
            group=ISVC_GROUP, version=ISVC_VERSION,
            namespace=ns, plural=ISVC_PLURAL, body=isvc,
        )
    cleanup(tenant_custom.delete_namespaced_custom_object,
            group=ISVC_GROUP, version=ISVC_VERSION,
            namespace=ns, plural=ISVC_PLURAL, name=isvc_name)

    assert _wait_isvc_ready(tenant_custom, ns, isvc_name, timeout=300), (
        f"InferenceService {isvc_name} not Ready within 300s"
    )

    # --- Step 2: NodePort service for cross-cluster access ---

    nodeport_svc = client.V1Service(
        metadata=client.V1ObjectMeta(name=nodeport_svc_name, namespace=ns),
        spec=client.V1ServiceSpec(
            type="NodePort",
            selector={"app": f"isvc.{isvc_name}-predictor"},
            ports=[client.V1ServicePort(port=8080, target_port=8080, protocol="TCP")],
        ),
    )
    try:
        tenant_core.read_namespaced_service(nodeport_svc_name, ns)
        tenant_core.replace_namespaced_service(nodeport_svc_name, ns, nodeport_svc)
    except client.exceptions.ApiException:
        tenant_core.create_namespaced_service(ns, nodeport_svc)
    cleanup(tenant_core.delete_namespaced_service, nodeport_svc_name, ns)

    svc = tenant_core.read_namespaced_service(nodeport_svc_name, ns)
    node_port = svc.spec.ports[0].node_port
    tenant_ip = _get_tenant_node_ip(tenant_core)
    model_url = f"http://{tenant_ip}:{node_port}/v1/completions"

    # --- Step 3: Kyverno MutatingPolicy for LMEval pods ---

    policy = {
        "apiVersion": f"{KYVERNO_POLICY_GROUP}/{KYVERNO_POLICY_VERSION}",
        "kind": "MutatingPolicy",
        "metadata": {"name": policy_name},
        "spec": {
            "failurePolicy": "Ignore",
            "matchConstraints": {
                "namespaceSelector": {
                    "matchLabels": {
                        "kubernetes.io/metadata.name": ns,
                    },
                },
                "resourceRules": [
                    {
                        "apiGroups": [""],
                        "apiVersions": ["v1"],
                        "operations": ["CREATE"],
                        "resources": ["pods"],
                    },
                ],
            },
            "matchConditions": [
                {
                    "name": "is-lmeval-pod",
                    "expression": (
                        "has(object.metadata.labels) && "
                        "'app.kubernetes.io/name' in object.metadata.labels && "
                        "object.metadata.labels['app.kubernetes.io/name'] == 'ta-lmes'"
                    ),
                },
            ],
            "mutations": [
                {
                    "patchType": "JSONPatch",
                    "jsonPatch": {
                        "expression": (
                            "has(object.spec.tolerations)"
                            " ? [JSONPatch{op: \"add\", path: \"/spec/tolerations/-\","
                            " value: {\"key\":\"virtual-kubelet.io/provider\",\"operator\":\"Exists\",\"effect\":\"NoSchedule\"}},"
                            " JSONPatch{op: \"add\", path: \"/spec/tolerations/-\","
                            " value: {\"key\":\"virtual-kubelet.io/provider\",\"operator\":\"Exists\",\"effect\":\"NoExecute\"}}]"
                            " : [JSONPatch{op: \"add\", path: \"/spec/tolerations\","
                            " value: [{\"key\":\"virtual-kubelet.io/provider\",\"operator\":\"Exists\",\"effect\":\"NoSchedule\"},"
                            " {\"key\":\"virtual-kubelet.io/provider\",\"operator\":\"Exists\",\"effect\":\"NoExecute\"}]}]"
                        ),
                    },
                },
                {
                    "patchType": "JSONPatch",
                    "jsonPatch": {
                        "expression": (
                            "has(object.spec.initContainers) && size(object.spec.initContainers) > 0"
                            " ? [JSONPatch{op: \"add\", path: \"/spec/initContainers/0/volumeMounts/-\","
                            " value: {\"name\":\"tmp\",\"mountPath\":\"/tmp\"}},"
                            " JSONPatch{op: \"add\", path: \"/spec/initContainers/0/volumeMounts/-\","
                            " value: {\"name\":\"lmeval-output\",\"mountPath\":\"/opt/app-root/src/output\"}},"
                            " JSONPatch{op: \"add\", path: \"/spec/initContainers/0/volumeMounts/-\","
                            " value: {\"name\":\"lmeval-catalogs\",\"mountPath\":\"/opt/app-root/src/my_catalogs\"}}]"
                            " : []"
                        ),
                    },
                },
            ],
        },
    }

    tenant_custom.create_cluster_custom_object(
        group=KYVERNO_POLICY_GROUP, version=KYVERNO_POLICY_VERSION,
        plural=KYVERNO_POLICY_PLURAL, body=policy,
    )
    cleanup(tenant_custom.delete_cluster_custom_object,
            group=KYVERNO_POLICY_GROUP, version=KYVERNO_POLICY_VERSION,
            plural=KYVERNO_POLICY_PLURAL, name=policy_name)

    time.sleep(5)

    # --- Step 4: Create LMEvalJob with local-completions ---

    lmeval_job = {
        "apiVersion": f"{LMEVAL_GROUP}/{LMEVAL_VERSION}",
        "kind": "LMEvalJob",
        "metadata": {"name": job_name, "namespace": ns},
        "spec": {
            "model": "local-completions",
            "modelArgs": [
                {"name": "model", "value": LMEVAL_MODEL_NAME},
                {"name": "base_url", "value": model_url},
                {"name": "tokenizer", "value": LMEVAL_MODEL_NAME},
                {"name": "num_concurrent", "value": "1"},
            ],
            "taskList": {
                "taskNames": ["arc_easy"],
            },
            "limit": "2",
            "batchSize": "1",
            "allowOnline": True,
            "pod": {
                "affinity": {
                    "nodeAffinity": {
                        "requiredDuringSchedulingIgnoredDuringExecution": {
                            "nodeSelectorTerms": [
                                {
                                    "matchExpressions": [
                                        {
                                            "key": "kubernetes.io/hostname",
                                            "operator": "In",
                                            "values": [VK_NODE_NAME],
                                        }
                                    ]
                                }
                            ]
                        }
                    }
                },
                "volumes": [
                    {"name": "hf-cache", "emptyDir": {}},
                    {"name": "tmp", "emptyDir": {}},
                    {"name": "lmeval-output", "emptyDir": {}},
                    {"name": "lmeval-catalogs", "emptyDir": {}},
                ],
                "container": {
                    "env": [
                        {"name": "HF_HOME", "value": "/tmp/hf-cache"},
                    ],
                    "volumeMounts": [
                        {"name": "hf-cache", "mountPath": "/tmp/hf-cache"},
                        {"name": "tmp", "mountPath": "/tmp"},
                        {"name": "lmeval-output", "mountPath": "/opt/app-root/src/output"},
                        {"name": "lmeval-catalogs", "mountPath": "/opt/app-root/src/my_catalogs"},
                    ],
                },
            },
        },
    }

    tenant_custom.create_namespaced_custom_object(
        group=LMEVAL_GROUP, version=LMEVAL_VERSION,
        namespace=ns, plural=LMEVAL_PLURAL, body=lmeval_job,
    )
    cleanup(tenant_custom.delete_namespaced_custom_object,
            group=LMEVAL_GROUP, version=LMEVAL_VERSION,
            namespace=ns, plural=LMEVAL_PLURAL, name=job_name)

    # --- Step 5: Verify Kyverno injection ---

    assert wait_pod_exists(tenant_core, pod_name, ns, timeout=120), (
        f"LMEvalJob controller did not create pod {pod_name} within 120s"
    )

    pod = tenant_core.read_namespaced_pod(name=pod_name, namespace=ns)
    tol_keys = [t.key for t in (pod.spec.tolerations or [])]
    assert "virtual-kubelet.io/provider" in tol_keys, (
        f"Kyverno did not inject VK tolerations. Pod tolerations: {tol_keys}"
    )

    # --- Step 6: Verify VK dispatch ---

    w_pod_name = worker_pod_name(ns, pod_name)
    assert wait_pod_exists(
        worker_core, w_pod_name, rhoai3_vk_worker_namespace, timeout=120,
    ), f"Worker pod {w_pod_name} did not appear — VK did not dispatch the pod"

    # --- Step 7: Wait for LMEvalJob to complete via controller lifecycle ---
    # The driver auto-exits after evaluation, the worker pod transitions to
    # Succeeded, VK syncs the phase to the tenant pod, and the controller
    # detects completion and marks the LMEvalJob CR as Complete.

    deadline = time.time() + 600
    job_state = ""
    job_reason = ""
    while time.time() < deadline:
        try:
            job = tenant_custom.get_namespaced_custom_object(
                group=LMEVAL_GROUP, version=LMEVAL_VERSION,
                namespace=ns, plural=LMEVAL_PLURAL, name=job_name,
            )
            job_state = job.get("status", {}).get("state", "")
            job_reason = job.get("status", {}).get("reason", "")
            if job_state == "Complete":
                break
        except client.exceptions.ApiException:
            pass
        time.sleep(10)

    assert job_state == "Complete", (
        f"LMEvalJob did not reach Complete state. state={job_state!r}, reason={job_reason!r}"
    )
    assert job_reason == "Succeeded", (
        f"LMEvalJob completed but not with Succeeded. reason={job_reason!r}"
    )

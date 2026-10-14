"""
Liqo RHOAI integration tests.

Tests that RHOAI operators (training operator, KServe) create pods in an
offloaded namespace and Liqo transparently dispatches them to the GPU worker.

Key difference from custom VK tests: no nodeName or VK tolerations on the
workload spec. The namespace is offloaded with Remote strategy — Liqo's
webhook injects the scheduling constraints automatically.
"""

import time

import pytest
from kubernetes import client

from helpers import force_delete_pod


KUBEFLOW_GROUP = "kubeflow.org"
KUBEFLOW_VERSION = "v1"
PYTORCHJOB_PLURAL = "pytorchjobs"

ISVC_GROUP = "serving.kserve.io"
ISVC_VERSION = "v1beta1"
ISVC_PLURAL = "inferenceservices"

PYTORCH_IMAGE = "pytorch/pytorch:2.11.0-cuda12.8-cudnn9-runtime"


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


@pytest.mark.liqo
@pytest.mark.rhoai
def test_pytorchjob_via_liqo(
    cleanup, liqo_tenant_clients, liqo_worker_clients,
    liqo_test_namespace, liqo_worker_namespace, liqo_virtual_node_ready,
):
    """PyTorchJob in offloaded namespace — training operator creates pod,
    Liqo offloads to worker GPU, status syncs back, job succeeds."""
    tenant_core, tenant_custom = liqo_tenant_clients
    ns = liqo_test_namespace

    job_name = "liqo-pytorch-test"
    master_pod = f"{job_name}-master-0"

    try:
        tenant_custom.delete_namespaced_custom_object(
            group=KUBEFLOW_GROUP, version=KUBEFLOW_VERSION,
            namespace=ns, plural=PYTORCHJOB_PLURAL, name=job_name,
        )
        time.sleep(10)
    except client.exceptions.ApiException as e:
        if e.status != 404:
            raise

    force_delete_pod(tenant_core, master_pod, ns)

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
        "criterion = nn.CrossEntropyLoss()",
        "",
        "for epoch in range(3):",
        "    x = torch.randn(32, 784, device=device)",
        "    y = torch.randint(0, 10, (32,), device=device)",
        "    out = model(x)",
        "    loss = criterion(out, y)",
        "    optimizer.zero_grad()",
        "    loss.backward()",
        "    optimizer.step()",
        "    print(f'Epoch {epoch+1}, loss={loss.item():.4f}')",
        "",
        "print('Training complete')",
    ])

    pytorchjob = {
        "apiVersion": f"{KUBEFLOW_GROUP}/{KUBEFLOW_VERSION}",
        "kind": "PyTorchJob",
        "metadata": {"name": job_name, "namespace": ns},
        "spec": {
            "pytorchReplicaSpecs": {
                "Master": {
                    "replicas": 1,
                    "template": {
                        "spec": {
                            "restartPolicy": "Never",
                            "containers": [
                                {
                                    "name": "pytorch",
                                    "image": PYTORCH_IMAGE,
                                    "command": ["python", "-c", training_script],
                                    "resources": {
                                        "limits": {"nvidia.com/gpu": "1"},
                                    },
                                    "volumeMounts": [
                                        {"name": "tmp", "mountPath": "/tmp"},
                                    ],
                                },
                            ],
                            "volumes": [
                                {"name": "tmp", "emptyDir": {}},
                            ],
                        },
                    },
                },
            },
        },
    }

    tenant_custom.create_namespaced_custom_object(
        group=KUBEFLOW_GROUP, version=KUBEFLOW_VERSION,
        namespace=ns, plural=PYTORCHJOB_PLURAL, body=pytorchjob,
    )
    cleanup(
        lambda: tenant_custom.delete_namespaced_custom_object(
            group=KUBEFLOW_GROUP, version=KUBEFLOW_VERSION,
            namespace=ns, plural=PYTORCHJOB_PLURAL, name=job_name,
        ),
    )

    deadline = time.time() + 120
    pod_exists = False
    while time.time() < deadline:
        try:
            tenant_core.read_namespaced_pod(name=master_pod, namespace=ns)
            pod_exists = True
            break
        except client.exceptions.ApiException:
            pass
        time.sleep(5)

    assert pod_exists, f"Training operator did not create {master_pod} in {ns}"

    phase = wait_pod_phase(tenant_core, master_pod, ns, ["Succeeded", "Failed"], timeout=600)
    assert phase == "Succeeded", f"PyTorchJob master pod phase: {phase}"

    logs = tenant_core.read_namespaced_pod_log(
        name=master_pod, namespace=ns, container="pytorch",
    )
    assert "Training on: cuda" in logs, f"Training did not use CUDA: {logs[:300]}"
    assert "Training complete" in logs, f"Training incomplete: {logs[:300]}"

    job = tenant_custom.get_namespaced_custom_object(
        group=KUBEFLOW_GROUP, version=KUBEFLOW_VERSION,
        namespace=ns, plural=PYTORCHJOB_PLURAL, name=job_name,
    )
    conditions = job.get("status", {}).get("conditions", [])
    succeeded = any(
        c.get("type") == "Succeeded" and c.get("status") == "True"
        for c in conditions
    )
    assert succeeded, f"PyTorchJob not Succeeded: {conditions}"


@pytest.mark.liqo
@pytest.mark.rhoai
def test_kserve_raw_inference_via_liqo(
    cleanup, liqo_tenant_clients,
    liqo_test_namespace, liqo_virtual_node_ready,
):
    """KServe InferenceService (RawDeployment) in offloaded namespace — Liqo
    offloads the predictor pod to the GPU worker."""
    tenant_core, tenant_custom = liqo_tenant_clients
    ns = liqo_test_namespace

    try:
        tenant_custom.list_namespaced_custom_object(
            group=ISVC_GROUP, version=ISVC_VERSION,
            namespace=ns, plural=ISVC_PLURAL,
        )
    except client.exceptions.ApiException as e:
        if e.status == 404:
            pytest.skip("InferenceService CRD not available — KServe not installed")
        raise

    isvc_name = "liqo-kserve-test"

    try:
        tenant_custom.delete_namespaced_custom_object(
            group=ISVC_GROUP, version=ISVC_VERSION,
            namespace=ns, plural=ISVC_PLURAL, name=isvc_name,
        )
        time.sleep(10)
    except client.exceptions.ApiException as e:
        if e.status != 404:
            raise

    gpu_server_script = "\n".join([
        "import http.server, json, subprocess",
        "class H(http.server.BaseHTTPRequestHandler):",
        "  def do_GET(self):",
        "    r = subprocess.run(['nvidia-smi', '--query-gpu=name,memory.total,memory.used,temperature.gpu', '--format=csv,noheader,nounits'], capture_output=True, text=True)",
        "    self.send_response(200)",
        "    self.send_header('Content-Type', 'application/json')",
        "    self.end_headers()",
        "    self.wfile.write(json.dumps({'gpu': r.stdout.strip(), 'status': 'ok'}).encode())",
        "  def log_message(self, *a): pass",
        "http.server.HTTPServer(('0.0.0.0', 8080), H).serve_forever()",
    ])

    isvc = {
        "apiVersion": f"{ISVC_GROUP}/{ISVC_VERSION}",
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
                "containers": [
                    {
                        "name": "kserve-container",
                        "image": "nvcr.io/nvidia/cuda:12.8.0-runtime-ubi9",
                        "command": ["python3", "-c", gpu_server_script],
                        "ports": [{"containerPort": 8080, "protocol": "TCP"}],
                        "resources": {
                            "limits": {"nvidia.com/gpu": "1"},
                        },
                    },
                ],
            },
        },
    }

    tenant_custom.create_namespaced_custom_object(
        group=ISVC_GROUP, version=ISVC_VERSION,
        namespace=ns, plural=ISVC_PLURAL, body=isvc,
    )
    cleanup(
        lambda: tenant_custom.delete_namespaced_custom_object(
            group=ISVC_GROUP, version=ISVC_VERSION,
            namespace=ns, plural=ISVC_PLURAL, name=isvc_name,
        ),
    )

    deadline = time.time() + 120
    predictor_pod = None
    while time.time() < deadline:
        pods = tenant_core.list_namespaced_pod(
            namespace=ns,
            label_selector=f"serving.kserve.io/inferenceservice={isvc_name}",
        )
        for p in pods.items:
            predictor_pod = p.metadata.name
            break
        if predictor_pod:
            break
        time.sleep(5)

    assert predictor_pod, f"KServe did not create a predictor pod for {isvc_name}"

    phase = wait_pod_phase(tenant_core, predictor_pod, ns, ["Running"], timeout=300)
    assert phase == "Running", f"Predictor pod phase: {phase}, expected Running"

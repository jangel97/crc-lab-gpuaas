"""
Cross-cluster networking via Submariner.

Tests that a GPU workload dispatched to the worker cluster is accessible
via a regular Kubernetes Service on the tenant cluster. Traffic flows:

  Service (tenant) → EndpointSlice (worker PodIP) → Submariner tunnel → GPU pod

Submariner provides L3 routing between clusters. The VK syncs the worker
pod's real PodIP into the tenant virtual pod status. The standard Kubernetes
endpoint controller creates EndpointSlices pointing to that IP. No
Submariner-specific code in the VK, pod, or Service.
"""

import json
import time

import pytest
from kubernetes import client

from conftest import worker_pod_name


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


@pytest.mark.networking
def test_gpu_service_via_submariner(
    tenant_clients, worker_clients, test_namespace, vk_worker_namespace
):
    """
    Deploy a GPU HTTP server on the worker via VK, create a Service on
    the tenant, and verify the Service routes through Submariner to the
    GPU pod.
    """
    tenant_core, _ = tenant_clients
    worker_core, _ = worker_clients
    ns = test_namespace

    pod_name = "vk-inference-test"
    svc_name = "vk-inference-svc"
    curl_pod_name = "vk-inference-curl"
    w_pod_name = worker_pod_name(ns, pod_name)

    # Cleanup from previous runs
    force_delete_pod(tenant_core, pod_name, ns)
    force_delete_pod(tenant_core, curl_pod_name, ns)
    force_delete_pod(worker_core, w_pod_name, vk_worker_namespace)
    try:
        tenant_core.delete_namespaced_service(name=svc_name, namespace=ns)
    except client.exceptions.ApiException:
        pass
    time.sleep(3)

    # Deploy GPU HTTP server pod on tenant (VK dispatches to worker)
    server_script = "\n".join([
        "import http.server, subprocess, json",
        "class H(http.server.BaseHTTPRequestHandler):",
        "  def do_GET(self):",
        "    r = subprocess.run(['nvidia-smi','--query-gpu=name,memory.total','--format=csv,noheader,nounits'], capture_output=True, text=True)",
        "    body = json.dumps(dict(gpu=r.stdout.strip(), status='ok'))",
        "    self.send_response(200)",
        "    self.send_header('Content-Type','application/json')",
        "    self.end_headers()",
        "    self.wfile.write(body.encode())",
        "  def log_message(self, *a): pass",
        "http.server.HTTPServer(('',8080),H).serve_forever()",
    ])
    pod = client.V1Pod(
        metadata=client.V1ObjectMeta(
            name=pod_name,
            namespace=ns,
            labels={"app": "vk-inference-test"},
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
                    name="server",
                    image="nvcr.io/nvidia/cuda:12.8.1-base-ubi9",
                    command=["python3", "-c", server_script],
                    ports=[client.V1ContainerPort(container_port=8080)],
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

    # Create Service selecting the pod
    svc = client.V1Service(
        metadata=client.V1ObjectMeta(name=svc_name, namespace=ns),
        spec=client.V1ServiceSpec(
            selector={"app": "vk-inference-test"},
            ports=[
                client.V1ServicePort(port=80, target_port=8080),
            ],
        ),
    )
    tenant_core.create_namespaced_service(namespace=ns, body=svc)

    # Wait for pod to be Running with a PodIP (synced from worker)
    deadline = time.time() + 120
    pod_ip = None
    while time.time() < deadline:
        tp = tenant_core.read_namespaced_pod(name=pod_name, namespace=ns)
        if tp.status.phase == "Running" and tp.status.pod_ip:
            pod_ip = tp.status.pod_ip
            break
        if tp.status.phase == "Failed":
            pytest.fail(
                f"Pod {pod_name} failed: {tp.status.reason} — {tp.status.message}"
            )
        time.sleep(5)

    assert pod_ip is not None, (
        f"Pod {pod_name} did not reach Running with a PodIP within 120s"
    )

    # Verify PodIP is from the worker cluster CIDR (10.132.0.0/14)
    assert pod_ip.startswith("10.13"), (
        f"PodIP {pod_ip} does not look like a worker cluster IP (expected 10.132-135.x.x)"
    )

    # Verify EndpointSlice exists with the worker PodIP
    discovery = client.DiscoveryV1Api(tenant_core.api_client)
    deadline = time.time() + 30
    endpoint_found = False
    while time.time() < deadline:
        slices = discovery.list_namespaced_endpoint_slice(
            namespace=ns,
            label_selector=f"kubernetes.io/service-name={svc_name}",
        )
        for es in slices.items:
            for ep in es.endpoints or []:
                if pod_ip in (ep.addresses or []):
                    if ep.conditions and ep.conditions.ready:
                        endpoint_found = True
                        break
            if endpoint_found:
                break
        if endpoint_found:
            break
        time.sleep(3)

    assert endpoint_found, (
        f"EndpointSlice for {svc_name} does not contain worker PodIP {pod_ip}"
    )

    # Curl the Service from a tenant pod (traffic goes through Submariner)
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
                        f"http://{svc_name}.{ns}.svc.cluster.local",
                    ],
                )
            ],
        ),
    )
    tenant_core.create_namespaced_pod(namespace=ns, body=curl_pod)

    # Wait for curl pod to complete
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
        "Service may not be routing through Submariner."
    )

    # Read curl pod logs — should contain GPU info
    logs = tenant_core.read_namespaced_pod_log(
        name=curl_pod_name, namespace=ns,
    ).strip()
    try:
        response = json.loads(logs)
    except json.JSONDecodeError:
        import ast
        response = ast.literal_eval(logs)
    assert response.get("status") == "ok", (
        f"Unexpected response from GPU inference service: {logs}"
    )
    assert "RTX 5090" in response.get("gpu", ""), (
        f"GPU response does not mention RTX 5090: {response}"
    )

    # Cleanup
    force_delete_pod(tenant_core, curl_pod_name, ns)
    try:
        tenant_core.delete_namespaced_service(name=svc_name, namespace=ns)
    except client.exceptions.ApiException:
        pass
    try:
        tenant_core.delete_namespaced_pod(
            name=pod_name, namespace=ns, grace_period_seconds=0,
        )
    except client.exceptions.ApiException:
        pass

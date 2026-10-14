"""
Liqo + Submariner networking tests.

Validates that Services on the tenant cluster can reach GPU pods on the
worker cluster via Submariner when Liqo's networking module is disabled.

The key question: does Liqo sync the worker PodIP to the tenant pod
status, enabling Submariner to route traffic?
"""

import time

import pytest
from kubernetes import client

from helpers import force_delete_pod


def wait_pod_phase(core_api, name, namespace, phases, timeout=300):
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


def _curl_service(tenant_core, ns, service_name, port, timeout=60):
    """Run a curl pod on the tenant and return (success, response_body)."""
    curl_pod_name = "liqo-curl-test"
    force_delete_pod(tenant_core, curl_pod_name, ns)

    url = f"http://{service_name}.{ns}.svc.cluster.local:{port}"
    curl_pod = client.V1Pod(
        metadata=client.V1ObjectMeta(name=curl_pod_name, namespace=ns),
        spec=client.V1PodSpec(
            restart_policy="Never",
            containers=[
                client.V1Container(
                    name="curl",
                    image="registry.access.redhat.com/ubi9/ubi-minimal:latest",
                    command=[
                        "curl", "-s", "-m", "10", "--retry", "3",
                        "--retry-delay", "5", url,
                    ],
                ),
            ],
        ),
    )

    tenant_core.create_namespaced_pod(namespace=ns, body=curl_pod)

    phase = wait_pod_phase(tenant_core, curl_pod_name, ns, ["Succeeded", "Failed"], timeout=timeout)

    logs = ""
    if phase in ("Succeeded", "Failed"):
        try:
            logs = tenant_core.read_namespaced_pod_log(
                name=curl_pod_name, namespace=ns, container="curl",
            )
        except client.exceptions.ApiException:
            pass

    force_delete_pod(tenant_core, curl_pod_name, ns)

    return phase == "Succeeded", logs


@pytest.mark.liqo
@pytest.mark.networking
def test_service_reachable_via_submariner(
    cleanup, liqo_tenant_clients, liqo_worker_clients,
    liqo_test_namespace, liqo_worker_namespace, liqo_virtual_node_ready,
):
    """GPU HTTP server pod dispatched via Liqo → Service on tenant →
    curl through Submariner → HTTP 200 with GPU info."""
    tenant_core, _ = liqo_tenant_clients
    ns = liqo_test_namespace

    pod_name = "liqo-gpu-server"
    svc_name = "liqo-gpu-svc"

    force_delete_pod(tenant_core, pod_name, ns)
    try:
        tenant_core.delete_namespaced_service(name=svc_name, namespace=ns)
    except client.exceptions.ApiException:
        pass

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

    pod = client.V1Pod(
        metadata=client.V1ObjectMeta(
            name=pod_name, namespace=ns,
            labels={"app": "liqo-gpu-server"},
        ),
        spec=client.V1PodSpec(
            restart_policy="Never",
            containers=[
                client.V1Container(
                    name="server",
                    image="nvcr.io/nvidia/cuda:12.8.0-runtime-ubi9",
                    command=["python3", "-c", gpu_server_script],
                    ports=[client.V1ContainerPort(container_port=8080)],
                    resources=client.V1ResourceRequirements(
                        limits={"nvidia.com/gpu": "1"},
                    ),
                ),
            ],
        ),
    )

    tenant_core.create_namespaced_pod(namespace=ns, body=pod)
    cleanup(force_delete_pod, tenant_core, pod_name, ns)

    svc = client.V1Service(
        metadata=client.V1ObjectMeta(name=svc_name, namespace=ns),
        spec=client.V1ServiceSpec(
            selector={"app": "liqo-gpu-server"},
            ports=[client.V1ServicePort(port=8080, target_port=8080)],
        ),
    )
    tenant_core.create_namespaced_service(namespace=ns, body=svc)
    cleanup(lambda: tenant_core.delete_namespaced_service(name=svc_name, namespace=ns))

    phase = wait_pod_phase(tenant_core, pod_name, ns, ["Running"], timeout=300)
    assert phase == "Running", f"GPU server pod phase: {phase}"

    pod_obj = tenant_core.read_namespaced_pod(name=pod_name, namespace=ns)
    pod_ip = pod_obj.status.pod_ip
    assert pod_ip, "No PodIP on tenant pod — Liqo status sync may not be working"

    discovery_api = client.DiscoveryV1Api(tenant_core.api_client)
    deadline = time.time() + 60
    endpoint_ready = False
    while time.time() < deadline:
        slices = discovery_api.list_namespaced_endpoint_slice(
            namespace=ns,
            label_selector=f"kubernetes.io/service-name={svc_name}",
        )
        for s in slices.items:
            for ep in (s.endpoints or []):
                if pod_ip in (ep.addresses or []):
                    if ep.conditions and ep.conditions.ready:
                        endpoint_ready = True
                        break
        if endpoint_ready:
            break
        time.sleep(5)

    assert endpoint_ready, (
        f"EndpointSlice for {svc_name} does not have PodIP {pod_ip} with ready=true"
    )

    time.sleep(5)
    success, body = _curl_service(tenant_core, ns, svc_name, 8080, timeout=120)

    assert success, f"curl to {svc_name} failed. Response: {body[:200]}"
    assert "status" in body and "ok" in body, (
        f"Response missing status:ok — {body[:200]}"
    )
    assert "5090" in body, (
        f"Response missing GPU info — {body[:200]}"
    )


@pytest.mark.liqo
@pytest.mark.networking
def test_headless_service_reflection(
    cleanup, liqo_tenant_clients, liqo_worker_clients,
    liqo_test_namespace, liqo_worker_namespace, liqo_virtual_node_ready,
):
    """Headless Service reflected to worker for inter-pod DNS resolution."""
    tenant_core, _ = liqo_tenant_clients
    worker_core, _ = liqo_worker_clients
    ns = liqo_test_namespace
    w_ns = liqo_worker_namespace

    svc_name = "liqo-dns-test"
    pod_a_name = "liqo-dns-pod-a"
    pod_b_name = "liqo-dns-pod-b"

    for name in [pod_a_name, pod_b_name]:
        force_delete_pod(tenant_core, name, ns)
    try:
        tenant_core.delete_namespaced_service(name=svc_name, namespace=ns)
    except client.exceptions.ApiException:
        pass

    svc = client.V1Service(
        metadata=client.V1ObjectMeta(name=svc_name, namespace=ns),
        spec=client.V1ServiceSpec(
            cluster_ip="None",
            selector={"app": "liqo-dns-test"},
            ports=[client.V1ServicePort(port=80, target_port=80)],
        ),
    )
    tenant_core.create_namespaced_service(namespace=ns, body=svc)
    cleanup(lambda: tenant_core.delete_namespaced_service(name=svc_name, namespace=ns))

    pod_a = client.V1Pod(
        metadata=client.V1ObjectMeta(
            name=pod_a_name, namespace=ns,
            labels={"app": "liqo-dns-test"},
        ),
        spec=client.V1PodSpec(
            hostname="pod-a",
            subdomain=svc_name,
            restart_policy="Never",
            containers=[
                client.V1Container(
                    name="test",
                    image="registry.access.redhat.com/ubi9/ubi-minimal:latest",
                    command=["sleep", "120"],
                ),
            ],
        ),
    )

    tenant_core.create_namespaced_pod(namespace=ns, body=pod_a)
    cleanup(force_delete_pod, tenant_core, pod_a_name, ns)

    phase = wait_pod_phase(tenant_core, pod_a_name, ns, ["Running"], timeout=300)
    assert phase == "Running", f"Pod A phase: {phase}"

    deadline = time.time() + 60
    svc_reflected = False
    while time.time() < deadline:
        try:
            w_svc = worker_core.read_namespaced_service(name=svc_name, namespace=w_ns)
            if w_svc.spec.cluster_ip in (None, "None"):
                svc_reflected = True
                break
        except client.exceptions.ApiException:
            pass
        time.sleep(5)

    assert svc_reflected, (
        f"Headless Service {svc_name} not reflected to worker namespace {w_ns}"
    )

    dns_target = f"pod-a.{svc_name}.{ns}.svc.cluster.local"
    pod_b = client.V1Pod(
        metadata=client.V1ObjectMeta(
            name=pod_b_name, namespace=ns,
            labels={"app": "liqo-dns-test"},
        ),
        spec=client.V1PodSpec(
            restart_policy="Never",
            containers=[
                client.V1Container(
                    name="test",
                    image="registry.access.redhat.com/ubi9/ubi-minimal:latest",
                    command=["getent", "hosts", dns_target],
                ),
            ],
        ),
    )

    tenant_core.create_namespaced_pod(namespace=ns, body=pod_b)
    cleanup(force_delete_pod, tenant_core, pod_b_name, ns)

    phase = wait_pod_phase(tenant_core, pod_b_name, ns, ["Succeeded", "Failed"], timeout=300)

    if phase == "Failed":
        try:
            logs = tenant_core.read_namespaced_pod_log(
                name=pod_b_name, namespace=ns, container="test",
            )
        except client.exceptions.ApiException:
            logs = "(logs unavailable — pod may have been cleaned up on worker)"
        pytest.fail(
            f"DNS resolution failed for {dns_target}. "
            f"Headless Service reflection may not support inter-pod DNS. "
            f"Logs: {logs[:300]}"
        )

    assert phase == "Succeeded", f"Pod B phase: {phase}"

    logs = tenant_core.read_namespaced_pod_log(
        name=pod_b_name, namespace=ns, container="test",
    )
    assert logs.strip(), f"Empty DNS response for {dns_target}"

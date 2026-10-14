"""
RHOAI 3.x Submariner networking tests.

Validates that RHOAI workloads (KServe, Notebook) dispatched to the worker
GPU cluster are reachable from the tenant cluster via Submariner. Traffic:

  Service (tenant) → EndpointSlice (worker PodIP) → Submariner tunnel → GPU pod

Also validates tunnel failure recovery (gateway pod kill → auto-restart).
"""

import ast
import json
import time

import pytest
from kubernetes import client

from helpers import worker_pod_name, force_delete_pod, wait_pod_exists


def _parse_response(text):
    """Parse server response that may be JSON or Python dict repr."""
    try:
        return json.loads(text)
    except (json.JSONDecodeError, ValueError):
        pass
    try:
        return ast.literal_eval(text)
    except (ValueError, SyntaxError):
        pass
    return None


VK_NODE_NAME = "gpu-worker"
VK_TOLERATIONS = [{"key": "virtual-kubelet.io/provider", "operator": "Exists"}]

ISVC_GROUP = "serving.kserve.io"
ISVC_VERSION = "v1beta1"
ISVC_PLURAL = "inferenceservices"

NOTEBOOK_GROUP = "kubeflow.org"
NOTEBOOK_VERSION = "v1"
NOTEBOOK_PLURAL = "notebooks"


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


def _curl_service(core_api, namespace, curl_pod_name, svc_name,
                  port=80, connect_timeout=15, retries=3, retry_delay=10):
    """Curl a ClusterIP Service from a disposable pod.

    Uses /bin/sh (not bash — ubi-minimal doesn't ship bash) and writes the
    curl output to /tmp/out so we can always read it regardless of exit code.
    Retries handle DNS propagation / endpoint readiness lag.
    """
    url = f"http://{svc_name}.{namespace}.svc.cluster.local:{port}"
    curl_cmd = (
        f"i=0; while [ $i -lt {retries} ]; do "
        f"  i=$((i+1)); "
        f"  curl -s --connect-timeout {connect_timeout} {url} && exit 0; "
        f"  sleep {retry_delay}; "
        f"done; exit 1"
    )

    force_delete_pod(core_api, curl_pod_name, namespace)
    time.sleep(2)

    curl_pod = client.V1Pod(
        metadata=client.V1ObjectMeta(name=curl_pod_name, namespace=namespace),
        spec=client.V1PodSpec(
            restart_policy="Never",
            containers=[
                client.V1Container(
                    name="curl",
                    image="registry.access.redhat.com/ubi9/ubi-minimal:latest",
                    command=["/bin/sh", "-c", curl_cmd],
                )
            ],
        ),
    )
    core_api.create_namespaced_pod(namespace=namespace, body=curl_pod)

    total_timeout = (connect_timeout + retry_delay) * retries + 30
    deadline = time.time() + total_timeout
    phase = None
    while time.time() < deadline:
        cp = core_api.read_namespaced_pod(name=curl_pod_name, namespace=namespace)
        phase = cp.status.phase
        if phase in ("Succeeded", "Failed"):
            return phase
        time.sleep(3)
    return phase


# ---------------------------------------------------------------------------
# Test 1: KServe inference reachable via Submariner
# ---------------------------------------------------------------------------


@pytest.mark.rhoai3
def test_kserve_inference_reachable_via_submariner(
    cleanup, rhoai3_tenant_clients, rhoai3_worker_clients,
    rhoai3_test_namespace, rhoai3_vk_worker_namespace,
):
    """KServe RawDeployment inference endpoint reachable from tenant via Submariner."""
    tenant_core, tenant_custom = rhoai3_tenant_clients
    worker_core, _ = rhoai3_worker_clients
    ns = rhoai3_test_namespace
    apps_api = client.AppsV1Api(tenant_core.api_client)
    api_ext = client.ApiextensionsV1Api(tenant_core.api_client)

    try:
        api_ext.read_custom_resource_definition(
            "inferenceservices.serving.kserve.io"
        )
    except client.exceptions.ApiException:
        pytest.skip("KServe not installed — enable kserve in DSC")

    isvc_name = "rhoai3-net-inference"
    curl_pod_name = "rhoai3-net-inference-curl"

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
    force_delete_pod(tenant_core, curl_pod_name, ns)

    server_script = "\n".join([
        "import http.server, subprocess, json",
        "class H(http.server.BaseHTTPRequestHandler):",
        "  def do_GET(self):",
        "    r = subprocess.run(['nvidia-smi','--query-gpu=name,memory.total','--format=csv,noheader,nounits'], capture_output=True, text=True)",
        "    body = json.dumps(dict(gpu=r.stdout.strip(), status='ok', model='rhoai3-net-test'))",
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

    # Wait for KServe to create the Deployment
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

    # Wait for the pod
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

    # Wait for tenant pod to have PodIP and containers ready (synced from worker)
    deadline = time.time() + 120
    pod_ip = None
    while time.time() < deadline:
        tp = tenant_core.read_namespaced_pod(name=isvc_pod_name, namespace=ns)
        if tp.status.phase == "Running" and tp.status.pod_ip:
            containers_ready = all(
                cs.ready for cs in (tp.status.container_statuses or [])
            )
            if containers_ready:
                pod_ip = tp.status.pod_ip
                break
        time.sleep(5)

    assert pod_ip is not None, (
        f"Pod {isvc_pod_name} did not reach Running with ready containers within 120s"
    )
    assert pod_ip.startswith("10.13"), (
        f"PodIP {pod_ip} does not look like a worker cluster IP (expected 10.13x.x.x)"
    )

    # Verify EndpointSlice via KServe predictor Service
    predictor_svc = f"{isvc_name}-predictor"
    discovery = client.DiscoveryV1Api(tenant_core.api_client)
    deadline = time.time() + 60
    endpoint_found = False
    while time.time() < deadline:
        slices = discovery.list_namespaced_endpoint_slice(
            namespace=ns,
            label_selector=f"kubernetes.io/service-name={predictor_svc}",
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
        f"EndpointSlice for {predictor_svc} does not contain ready endpoint "
        f"for worker PodIP {pod_ip} within 60s"
    )

    # Allow DNS to propagate the ready endpoint (CoreDNS caches negative results)
    time.sleep(10)

    # KServe creates a headless Service (clusterIP=None), so DNS resolves to
    # the pod IP directly — must use the container port, not the Service port.
    curl_result = _curl_service(
        tenant_core, ns, curl_pod_name, predictor_svc, port=8080,
    )
    cleanup(force_delete_pod, tenant_core, curl_pod_name, ns)

    if curl_result != "Succeeded":
        try:
            curl_logs = tenant_core.read_namespaced_pod_log(
                name=curl_pod_name, namespace=ns,
            )
        except Exception:
            curl_logs = "<no logs>"
        try:
            curl_pod_obj = tenant_core.read_namespaced_pod(
                name=curl_pod_name, namespace=ns,
            )
            exit_code = None
            for cs in (curl_pod_obj.status.container_statuses or []):
                if cs.state and cs.state.terminated:
                    exit_code = cs.state.terminated.exit_code
        except Exception:
            exit_code = "unknown"
        pytest.fail(
            f"Curl pod failed (phase={curl_result!r}, exit_code={exit_code}). "
            f"Logs: {curl_logs}"
        )

    logs = tenant_core.read_namespaced_pod_log(
        name=curl_pod_name, namespace=ns,
    ).strip()
    response = _parse_response(logs)
    assert response is not None, (
        f"Curl response is not parseable: {logs}"
    )

    assert response.get("status") == "ok", (
        f"Unexpected response from inference service: {logs}"
    )
    assert "RTX 5090" in response.get("gpu", ""), (
        f"GPU response does not mention RTX 5090: {response}"
    )


# ---------------------------------------------------------------------------
# Test 2: Notebook reachable via Submariner
# ---------------------------------------------------------------------------


@pytest.mark.rhoai3
def test_notebook_reachable_via_submariner(
    cleanup, rhoai3_tenant_clients, rhoai3_worker_clients,
    rhoai3_test_namespace, rhoai3_vk_worker_namespace,
):
    """Notebook CR on worker GPU reachable from tenant via Submariner."""
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

    nb_name = "rhoai3-net-notebook"
    pod_name = f"{nb_name}-0"
    w_pod_name = worker_pod_name(ns, pod_name)
    svc_name = "rhoai3-net-notebook-svc"
    curl_pod_name = "rhoai3-net-notebook-curl"

    # Cleanup from previous runs
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
    force_delete_pod(tenant_core, curl_pod_name, ns)
    try:
        tenant_core.delete_namespaced_service(name=svc_name, namespace=ns)
    except client.exceptions.ApiException:
        pass
    time.sleep(3)

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
        "metadata": {
            "name": nb_name,
            "namespace": ns,
            "labels": {"app": nb_name},
        },
        "spec": {
            "template": {
                "metadata": {
                    "labels": {"app": nb_name},
                },
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

    # Wait for StatefulSet
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
    ), f"Worker pod {w_pod_name} did not appear"

    # Wait for worker pod Running with container running
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

    # Wait for tenant pod PodIP
    deadline = time.time() + 60
    pod_ip = None
    while time.time() < deadline:
        tp = tenant_core.read_namespaced_pod(name=pod_name, namespace=ns)
        if tp.status.phase == "Running" and tp.status.pod_ip:
            pod_ip = tp.status.pod_ip
            break
        time.sleep(5)

    assert pod_ip is not None, (
        f"Notebook pod {pod_name} did not get PodIP synced within 60s"
    )
    assert pod_ip.startswith("10.13"), (
        f"PodIP {pod_ip} does not look like a worker cluster IP"
    )

    # Create Service selecting the notebook pod
    svc = client.V1Service(
        metadata=client.V1ObjectMeta(name=svc_name, namespace=ns),
        spec=client.V1ServiceSpec(
            selector={"app": nb_name},
            ports=[client.V1ServicePort(port=80, target_port=8888)],
        ),
    )
    tenant_core.create_namespaced_service(namespace=ns, body=svc)
    cleanup(tenant_core.delete_namespaced_service, name=svc_name, namespace=ns)

    # Verify pod stays Running (interactive workload)
    time.sleep(10)
    tp = tenant_core.read_namespaced_pod(name=pod_name, namespace=ns)
    assert tp.status.phase == "Running", (
        f"Notebook pod should stay Running, got {tp.status.phase}"
    )

    # Curl the Service
    curl_result = _curl_service(tenant_core, ns, curl_pod_name, svc_name)
    cleanup(force_delete_pod, tenant_core, curl_pod_name, ns)

    assert curl_result == "Succeeded", (
        f"Curl pod did not succeed (phase={curl_result!r}). "
        "Notebook not reachable via Submariner."
    )

    logs = tenant_core.read_namespaced_pod_log(
        name=curl_pod_name, namespace=ns,
    ).strip()
    response = _parse_response(logs)
    assert response is not None, (
        f"Curl response is not parseable: {logs}"
    )

    assert response.get("status") == "ready", (
        f"Unexpected response from notebook service: {logs}"
    )
    assert "RTX 5090" in response.get("gpu", ""), (
        f"GPU response does not mention RTX 5090: {response}"
    )


# ---------------------------------------------------------------------------
# Test 3: Submariner tunnel failure recovery
# ---------------------------------------------------------------------------


@pytest.mark.rhoai3
def test_submariner_tunnel_recovery_rhoai3(
    cleanup, rhoai3_tenant_clients, rhoai3_worker_clients,
    rhoai3_test_namespace, rhoai3_vk_worker_namespace,
):
    """Submariner tunnel self-heals after gateway pod failure (RHOAI 3.x tenant)."""
    tenant_core, _ = rhoai3_tenant_clients
    worker_core, _ = rhoai3_worker_clients
    ns = rhoai3_test_namespace

    pod_name = "rhoai3-tunnel-test"
    svc_name = "rhoai3-tunnel-svc"
    curl_prefix = "rhoai3-tunnel-curl"
    w_pod_name = worker_pod_name(ns, pod_name)

    # Cleanup from previous runs
    force_delete_pod(tenant_core, pod_name, ns)
    for suffix in ["pre", "during", "post"]:
        force_delete_pod(tenant_core, f"{curl_prefix}-{suffix}", ns)
    force_delete_pod(worker_core, w_pod_name, rhoai3_vk_worker_namespace)
    try:
        tenant_core.delete_namespaced_service(name=svc_name, namespace=ns)
    except client.exceptions.ApiException:
        pass
    time.sleep(3)

    # Deploy GPU HTTP server
    server_script = "\n".join([
        "import http.server, json",
        "class H(http.server.BaseHTTPRequestHandler):",
        "  def do_GET(self):",
        "    self.send_response(200)",
        "    self.send_header('Content-Type','application/json')",
        "    self.end_headers()",
        "    self.wfile.write(json.dumps(dict(status='ok')).encode())",
        "  def log_message(self, *a): pass",
        "http.server.HTTPServer(('',8080),H).serve_forever()",
    ])
    pod = client.V1Pod(
        metadata=client.V1ObjectMeta(
            name=pod_name,
            namespace=ns,
            labels={"app": "rhoai3-tunnel-test"},
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
    cleanup(force_delete_pod, tenant_core, pod_name, ns)

    svc = client.V1Service(
        metadata=client.V1ObjectMeta(name=svc_name, namespace=ns),
        spec=client.V1ServiceSpec(
            selector={"app": "rhoai3-tunnel-test"},
            ports=[client.V1ServicePort(port=80, target_port=8080)],
        ),
    )
    tenant_core.create_namespaced_service(namespace=ns, body=svc)
    cleanup(tenant_core.delete_namespaced_service, name=svc_name, namespace=ns)

    # Wait for pod Running
    deadline = time.time() + 120
    while time.time() < deadline:
        tp = tenant_core.read_namespaced_pod(name=pod_name, namespace=ns)
        if tp.status.phase == "Running" and tp.status.pod_ip:
            break
        if tp.status.phase == "Failed":
            pytest.fail(f"Pod failed: {tp.status.reason}")
        time.sleep(5)
    else:
        pytest.fail("Pod did not reach Running within 120s")

    # Step 1: Verify connectivity before disruption
    pre_result = _curl_service(
        tenant_core, ns, f"{curl_prefix}-pre", svc_name,
    )
    cleanup(force_delete_pod, tenant_core, f"{curl_prefix}-pre", ns)
    assert pre_result == "Succeeded", (
        f"Pre-disruption curl failed (phase={pre_result}). "
        "Cannot test tunnel recovery without baseline connectivity."
    )

    # Step 2: Kill the Submariner gateway pod on the worker cluster
    submariner_ns = "submariner-operator"
    gw_pods = worker_core.list_namespaced_pod(
        namespace=submariner_ns,
        label_selector="app=submariner-gateway",
    )
    assert len(gw_pods.items) > 0, "No Submariner gateway pod found on worker"

    gw_pod_name = gw_pods.items[0].metadata.name
    worker_core.delete_namespaced_pod(
        name=gw_pod_name,
        namespace=submariner_ns,
        grace_period_seconds=0,
    )

    # Step 3: Wait for tunnel to drop, try curl
    time.sleep(10)
    during_result = _curl_service(
        tenant_core, ns, f"{curl_prefix}-during", svc_name,
        connect_timeout=5,
    )
    cleanup(force_delete_pod, tenant_core, f"{curl_prefix}-during", ns)

    # Step 4: Wait for gateway pod recovery
    deadline = time.time() + 180
    gateway_ready = False
    while time.time() < deadline:
        gw_pods = worker_core.list_namespaced_pod(
            namespace=submariner_ns,
            label_selector="app=submariner-gateway",
        )
        for gw in gw_pods.items:
            if gw.metadata.name == gw_pod_name:
                continue
            if gw.status.phase == "Running":
                conditions = gw.status.conditions or []
                for c in conditions:
                    if c.type == "Ready" and c.status == "True":
                        gateway_ready = True
                        break
            if gateway_ready:
                break
        if gateway_ready:
            break
        time.sleep(5)

    assert gateway_ready, "Submariner gateway did not recover within 180s"

    # Give tunnel time to re-establish IPsec
    time.sleep(30)

    # Step 5: Verify connectivity restored
    post_result = _curl_service(
        tenant_core, ns, f"{curl_prefix}-post", svc_name,
    )
    cleanup(force_delete_pod, tenant_core, f"{curl_prefix}-post", ns)
    assert post_result == "Succeeded", (
        f"Post-recovery curl failed (phase={post_result}). "
        "Submariner tunnel did not recover."
    )

    post_logs = tenant_core.read_namespaced_pod_log(
        name=f"{curl_prefix}-post", namespace=ns,
    ).strip()
    assert '"status": "ok"' in post_logs or "'status': 'ok'" in post_logs, (
        f"Post-recovery response unexpected: {post_logs}"
    )

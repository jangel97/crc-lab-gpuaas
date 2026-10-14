"""
RHOAI 3.x smoke tests — verify operator installation and architecture.

Moved from tests/rhoai_3/test_rhoai3_vk.py.
"""

import os

import pytest
from kubernetes import client
from kubernetes import config as k8s_config


RHOAI_APPS_NS = "redhat-ods-applications"

RHOAI_CRDS = [
    "pytorchjobs.kubeflow.org",
    "notebooks.kubeflow.org",
    "inferenceservices.serving.kserve.io",
    "rayclusters.ray.io",
]


@pytest.mark.rhoai3
def test_no_rhoai_crds_on_worker(rhoai3_environment):
    """RHOAI CRDs should only exist on the tenant, not the GPU worker."""
    worker_cfg = os.environ.get("WORKER_KUBECONFIG", os.path.expanduser("~/.kube/worker"))
    worker_api = k8s_config.new_client_from_config(config_file=worker_cfg)
    ext = client.ApiextensionsV1Api(worker_api)

    worker_crds = {c.metadata.name for c in ext.list_custom_resource_definition().items}
    for crd in RHOAI_CRDS:
        assert crd not in worker_crds, f"CRD {crd} should NOT be on worker cluster"


@pytest.mark.rhoai3
def test_rhoai3_components_running(rhoai3_environment):
    """Verify key RHOAI 3.x deployments are available."""
    api = rhoai3_environment
    apps = client.AppsV1Api(api)

    expected = [
        "kubeflow-training-operator",
        "kserve-controller-manager",
    ]
    for dep_name in expected:
        dep = apps.read_namespaced_deployment(dep_name, RHOAI_APPS_NS)
        ready = dep.status.ready_replicas or 0
        desired = dep.spec.replicas or 1
        assert ready >= desired, f"{dep_name} not ready: {ready}/{desired}"


@pytest.mark.rhoai3
def test_no_ossm_on_rhoai3(rhoai3_environment):
    """RHOAI 3.x should not have OSSM/Istio."""
    api = rhoai3_environment
    ext = client.ApiextensionsV1Api(api)

    crds = {c.metadata.name for c in ext.list_custom_resource_definition().items}
    assert "servicemeshcontrolplanes.maistra.io" not in crds, \
        "OSSM CRD found -- RHOAI 3.x should not install Service Mesh"

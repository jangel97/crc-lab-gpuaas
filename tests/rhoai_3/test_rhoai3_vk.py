"""
RHOAI 3.x workload dispatch via Virtual Kubelet.

RHOAI 3.x introduces significant architectural changes from 2.x:
  - OSSM (Istio) removed — Gateway API replaces Service Mesh v2
  - KServe Serverless (Knative) retired — RawDeployment only
  - ModelMesh removed — single-model serving only
  - oauth-proxy replaced by kube-rbac-proxy on workbenches
  - New prereqs: Cert Manager operator, JobSet operator
  - Minimum OCP 4.19.9

Tests validated against: RHOAI 3.x on OCP 4.x (TODO: fill in versions)

Migration from rhoai_2_25/:
  - test_pytorchjob_via_vk → carried forward (training operator unchanged)
  - test_notebook_cr_via_vk → carried forward (verify kube-rbac-proxy)
  - test_kserve_inference_via_vk → carried forward (already RawDeployment)
  - test_kserve_serverless_inference_via_vk → REMOVED (Serverless retired)
  - test_istio_sidecar_injection_on_vk_pod → REMOVED (no OSSM)
  - test_no_rhoai_crds_on_worker → carried forward
  - NEW: Gateway API routing to VK-dispatched model server
  - NEW: kube-rbac-proxy on Notebook workbenches via VK
"""

import time

import pytest
from kubernetes import client

from helpers import worker_pod_name, execution_pvc_name, force_delete_pod, wait_pod_exists, CATAPULT_STORAGE_CLASS


VK_NODE_NAME = "gpu-worker"
VK_TOLERATIONS = [{"key": "virtual-kubelet.io/provider", "operator": "Exists"}]

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


@pytest.fixture(autouse=True, scope="session")
def _ensure_lab_env(rhoai3_environment):
    """Depends on rhoai3_environment (from local conftest) which installs operators on tenant2."""
    pass


# ---------------------------------------------------------------------------
# TODO: Implement these tests once RHOAI 3.x is installed on a tenant cluster.
#
# Priority order:
#   1. test_no_rhoai_crds_on_worker — sanity check
#   2. test_pytorchjob_via_vk — training is the primary use case
#   3. test_kserve_raw_inference_via_vk — KServe RawDeployment (only mode in 3.x)
#   4. test_notebook_cr_via_vk — verify kube-rbac-proxy works through VK
#   5. test_gateway_api_routing_to_vk_pod — new: Gateway API replaces OSSM routes
#
# Key questions to answer during validation:
#   - Does Gateway API inject anything into pods (like OSSM did)?
#   - Does kube-rbac-proxy on Notebooks require tenant-side infrastructure?
#   - Does Cert Manager add any annotations/sidecars to dispatched pods?
#   - Does JobSet operator interact with VK-dispatched pods?
# ---------------------------------------------------------------------------

"""
Liqo PVC reflection tests.

Liqo's storage fabric reflects PVCs to the remote cluster with
StorageClass remapping via the liqo.io/remote-storage-class annotation.
"""

import time

import pytest
from kubernetes import client

from helpers import force_delete_pod


LIQO_STORAGE_CLASS = "liqo"
LVMS_STORAGE_CLASS = "lvms-vg1"


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


@pytest.mark.liqo
def test_pvc_reflection(
    cleanup, liqo_tenant_clients, liqo_worker_clients,
    liqo_test_namespace, liqo_worker_namespace, liqo_virtual_node_ready,
):
    """PVC with liqo.io/remote-storage-class reflected to worker with real storage."""
    tenant_core, _ = liqo_tenant_clients
    worker_core, _ = liqo_worker_clients
    ns = liqo_test_namespace
    w_ns = liqo_worker_namespace

    pvc_name = "liqo-test-pvc"
    pod_name = "liqo-pvc-test"

    force_delete_pod(tenant_core, pod_name, ns)
    try:
        tenant_core.delete_namespaced_persistent_volume_claim(name=pvc_name, namespace=ns)
        time.sleep(5)
    except client.exceptions.ApiException:
        pass

    pvc = client.V1PersistentVolumeClaim(
        metadata=client.V1ObjectMeta(name=pvc_name, namespace=ns),
        spec=client.V1PersistentVolumeClaimSpec(
            access_modes=["ReadWriteOnce"],
            storage_class_name=LIQO_STORAGE_CLASS,
            resources=client.V1VolumeResourceRequirements(
                requests={"storage": "1Gi"},
            ),
        ),
    )

    tenant_core.create_namespaced_persistent_volume_claim(namespace=ns, body=pvc)
    cleanup(
        lambda: tenant_core.delete_namespaced_persistent_volume_claim(
            name=pvc_name, namespace=ns,
        ),
    )

    pod = client.V1Pod(
        metadata=client.V1ObjectMeta(name=pod_name, namespace=ns),
        spec=client.V1PodSpec(
            restart_policy="Never",
            containers=[
                client.V1Container(
                    name="test",
                    image="registry.access.redhat.com/ubi9/ubi-minimal:latest",
                    command=[
                        "sh", "-c",
                        "echo 'liqo-pvc-works' > /data/test.txt && cat /data/test.txt",
                    ],
                    volume_mounts=[
                        client.V1VolumeMount(
                            name="data", mount_path="/data",
                        ),
                    ],
                    resources=client.V1ResourceRequirements(
                        limits={"nvidia.com/gpu": "1"},
                    ),
                ),
            ],
            volumes=[
                client.V1Volume(
                    name="data",
                    persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                        claim_name=pvc_name,
                    ),
                ),
            ],
        ),
    )

    tenant_core.create_namespaced_pod(namespace=ns, body=pod)
    cleanup(force_delete_pod, tenant_core, pod_name, ns)

    deadline = time.time() + 60
    worker_pvc_found = False
    worker_pvc_name = None
    while time.time() < deadline:
        pvcs = worker_core.list_namespaced_persistent_volume_claim(namespace=w_ns)
        for p in pvcs.items:
            if pvc_name in p.metadata.name:
                worker_pvc_found = True
                worker_pvc_name = p.metadata.name
                break
        if worker_pvc_found:
            break
        time.sleep(5)

    assert worker_pvc_found, f"PVC not reflected to worker namespace {w_ns}"

    worker_pvc = worker_core.read_namespaced_persistent_volume_claim(
        name=worker_pvc_name, namespace=w_ns,
    )
    assert worker_pvc.spec.storage_class_name == LVMS_STORAGE_CLASS, (
        f"Worker PVC StorageClass: {worker_pvc.spec.storage_class_name}, "
        f"expected {LVMS_STORAGE_CLASS}"
    )

    phase = wait_pod_phase(tenant_core, pod_name, ns, ["Succeeded"], timeout=300)
    assert phase == "Succeeded", f"Pod phase: {phase}"


@pytest.mark.liqo
def test_pvc_persists_across_pod_restart(
    cleanup, liqo_tenant_clients, liqo_worker_clients,
    liqo_test_namespace, liqo_worker_namespace, liqo_virtual_node_ready,
):
    """Data written to PVC survives pod deletion — new pod reads same data."""
    tenant_core, _ = liqo_tenant_clients
    ns = liqo_test_namespace

    pvc_name = "liqo-persist-pvc"
    writer_name = "liqo-pvc-writer"
    reader_name = "liqo-pvc-reader"

    for name in [writer_name, reader_name]:
        force_delete_pod(tenant_core, name, ns)
    try:
        tenant_core.delete_namespaced_persistent_volume_claim(name=pvc_name, namespace=ns)
        time.sleep(5)
    except client.exceptions.ApiException:
        pass

    pvc = client.V1PersistentVolumeClaim(
        metadata=client.V1ObjectMeta(name=pvc_name, namespace=ns),
        spec=client.V1PersistentVolumeClaimSpec(
            access_modes=["ReadWriteOnce"],
            storage_class_name=LIQO_STORAGE_CLASS,
            resources=client.V1VolumeResourceRequirements(
                requests={"storage": "1Gi"},
            ),
        ),
    )
    tenant_core.create_namespaced_persistent_volume_claim(namespace=ns, body=pvc)
    cleanup(
        lambda: tenant_core.delete_namespaced_persistent_volume_claim(
            name=pvc_name, namespace=ns,
        ),
    )

    def _make_pod(name, command):
        return client.V1Pod(
            metadata=client.V1ObjectMeta(name=name, namespace=ns),
            spec=client.V1PodSpec(
                restart_policy="Never",
                containers=[
                    client.V1Container(
                        name="test",
                        image="registry.access.redhat.com/ubi9/ubi-minimal:latest",
                        command=["sh", "-c", command],
                        volume_mounts=[
                            client.V1VolumeMount(name="data", mount_path="/data"),
                        ],
                        resources=client.V1ResourceRequirements(
                            limits={"nvidia.com/gpu": "1"},
                        ),
                    ),
                ],
                volumes=[
                    client.V1Volume(
                        name="data",
                        persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                            claim_name=pvc_name,
                        ),
                    ),
                ],
            ),
        )

    tenant_core.create_namespaced_pod(
        namespace=ns, body=_make_pod(writer_name, "echo persist-check > /data/marker.txt"),
    )
    cleanup(force_delete_pod, tenant_core, writer_name, ns)

    phase = wait_pod_phase(tenant_core, writer_name, ns, ["Succeeded"], timeout=300)
    assert phase == "Succeeded", f"Writer pod phase: {phase}"

    force_delete_pod(tenant_core, writer_name, ns)
    time.sleep(5)

    tenant_core.create_namespaced_pod(
        namespace=ns, body=_make_pod(reader_name, "cat /data/marker.txt"),
    )
    cleanup(force_delete_pod, tenant_core, reader_name, ns)

    phase = wait_pod_phase(tenant_core, reader_name, ns, ["Succeeded"], timeout=300)
    assert phase == "Succeeded", f"Reader pod phase: {phase}"

    logs = tenant_core.read_namespaced_pod_log(
        name=reader_name, namespace=ns, container="test",
    )
    assert "persist-check" in logs, f"Data not persisted: {logs}"

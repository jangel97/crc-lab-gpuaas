"""
Lab environment management for VK integration tests.

Manages libvirt VMs via virsh subprocess calls. Requires passwordless sudo
for virsh commands (configure NOPASSWD in sudoers).

VM inventory:
  sno-tenant:  14GB default, tenant cluster
  sno-tenant2: 16GB default, second tenant for multi-tenant tests
  sno-worker:  24GB, GPU worker cluster (never touched by this module)
"""

import os
import re
import subprocess
import time


VM_INVENTORY = {
    "sno-tenant": {
        "default_memory_gb": 14,
        "kubeconfig_env": "TENANT_KUBECONFIG",
        "kubeconfig_default": "~/.kube/tenant",
    },
    "sno-tenant2": {
        "default_memory_gb": 16,
        "kubeconfig_env": "TENANT2_KUBECONFIG",
        "kubeconfig_default": "~/.kube/tenant2",
    },
    "sno-worker": {
        "default_memory_gb": 24,
        "kubeconfig_env": "WORKER_KUBECONFIG",
        "kubeconfig_default": "~/.kube/worker",
    },
}


class LabEnvironmentError(Exception):
    pass


def _pod_is_stale(pod):
    """True if the pod is a leftover from before a VM restart."""
    if pod.status.reason in ("NodeShutdown", "NodeNotReady"):
        return True
    for cs in (pod.status.container_statuses or []):
        if cs.state and cs.state.terminated:
            if cs.state.terminated.reason == "ContainerStatusUnknown":
                return True
    for cs in (pod.status.init_container_statuses or []):
        if cs.state and cs.state.terminated:
            if cs.state.terminated.reason == "ContainerStatusUnknown":
                return True
    return False


class LabEnvironment:
    """Manage libvirt VMs for test lab environments via virsh."""

    def __init__(self):
        self._verify_virsh_available()

    def _verify_virsh_available(self):
        try:
            result = subprocess.run(
                ["sudo", "-n", "virsh", "version"],
                capture_output=True, text=True, timeout=10,
            )
        except FileNotFoundError:
            raise LabEnvironmentError("virsh not found on PATH")
        except subprocess.TimeoutExpired:
            raise LabEnvironmentError("sudo virsh version timed out")
        if result.returncode != 0:
            raise LabEnvironmentError(
                "Passwordless sudo virsh not available. "
                "Configure NOPASSWD in sudoers for virsh."
            )

    def _run_virsh(self, *args):
        result = subprocess.run(
            ["sudo", "virsh"] + list(args),
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode != 0:
            raise LabEnvironmentError(
                f"virsh {' '.join(args)} failed: {result.stderr.strip()}"
            )
        return result

    def vm_state(self, vm_name):
        """Return VM state: 'running', 'shut off', 'paused', etc."""
        return self._run_virsh("domstate", vm_name).stdout.strip()

    def vm_memory_gb(self, vm_name):
        """Parse 'Max memory' from virsh dominfo, return GB (integer)."""
        info = self._run_virsh("dominfo", vm_name).stdout
        match = re.search(r"Max memory:\s+(\d+)\s+KiB", info)
        if not match:
            raise LabEnvironmentError(
                f"Could not parse Max memory from virsh dominfo {vm_name}"
            )
        kib = int(match.group(1))
        return kib // (1024 * 1024)

    def _kubeconfig_path(self, vm_name):
        info = VM_INVENTORY[vm_name]
        return os.environ.get(
            info["kubeconfig_env"],
            os.path.expanduser(info["kubeconfig_default"]),
        )

    def wait_for_node_ready(self, kubeconfig, timeout=120):
        """Wait until at least one node reports Ready condition."""
        from kubernetes import client, config

        deadline = time.time() + timeout
        last_err = None
        while time.time() < deadline:
            try:
                api_client = config.new_client_from_config(
                    config_file=kubeconfig,
                )
                core = client.CoreV1Api(api_client)
                nodes = core.list_node()
                for node in nodes.items:
                    for cond in (node.status.conditions or []):
                        if cond.type == "Ready" and cond.status == "True":
                            return
            except Exception as e:
                last_err = e
            time.sleep(10)
        raise LabEnvironmentError(
            f"No node Ready at {kubeconfig} after {timeout}s: {last_err}"
        )

    def cleanup_stale_pods(self, kubeconfig):
        """Delete pods stuck after a VM restart (ContainerStatusUnknown, NodeShutdown)."""
        from kubernetes import client, config

        try:
            api_client = config.new_client_from_config(config_file=kubeconfig)
            core = client.CoreV1Api(api_client)
            pods = core.list_pod_for_all_namespaces()
        except Exception:
            return

        for pod in pods.items:
            if _pod_is_stale(pod):
                try:
                    core.delete_namespaced_pod(
                        name=pod.metadata.name,
                        namespace=pod.metadata.namespace,
                        grace_period_seconds=0,
                    )
                except client.exceptions.ApiException:
                    pass

    def wait_for_ocp(self, kubeconfig, timeout=300):
        """Poll Kubernetes API until it responds."""
        from kubernetes import client, config

        deadline = time.time() + timeout
        last_err = None
        while time.time() < deadline:
            try:
                api_client = config.new_client_from_config(
                    config_file=kubeconfig,
                )
                client.VersionApi(api_client).get_code()
                return
            except Exception as e:
                last_err = e
            time.sleep(10)
        raise LabEnvironmentError(
            f"OCP API at {kubeconfig} not ready after {timeout}s: {last_err}"
        )

    def start_vm(self, vm_name, timeout=300):
        """Start a VM and wait for OCP API + node Ready + stale pod cleanup."""
        state = self.vm_state(vm_name)
        if state == "running":
            return
        if state == "paused":
            self._run_virsh("resume", vm_name)
        else:
            self._run_virsh("start", vm_name)
        kubeconfig = self._kubeconfig_path(vm_name)
        self.wait_for_ocp(kubeconfig, timeout)
        self.wait_for_node_ready(kubeconfig, timeout=120)
        self.cleanup_stale_pods(kubeconfig)

    def shutdown_vm(self, vm_name, timeout=180):
        """Gracefully shut down a VM and wait for it to stop.

        Falls back to virsh destroy if graceful shutdown times out.
        """
        state = self.vm_state(vm_name)
        if state == "shut off":
            return
        self._run_virsh("shutdown", vm_name)
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.vm_state(vm_name) == "shut off":
                return
            time.sleep(5)
        self._run_virsh("destroy", vm_name)

    def set_memory(self, vm_name, gb):
        """Set persistent memory config. Shuts down the VM if running."""
        self.shutdown_vm(vm_name)
        mem = f"{gb}G"
        self._run_virsh("setmaxmem", vm_name, mem, "--config")
        self._run_virsh("setmem", vm_name, mem, "--config")

    def ensure_running(self, vm_name, timeout=300):
        """Start VM if not already running."""
        if self.vm_state(vm_name) != "running":
            self.start_vm(vm_name, timeout)

    def ensure_shut_off(self, vm_name, timeout=120):
        """Shut down VM if not already off."""
        if self.vm_state(vm_name) != "shut off":
            self.shutdown_vm(vm_name, timeout)

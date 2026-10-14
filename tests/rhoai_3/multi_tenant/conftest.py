"""
Multi-tenant suite conftest: two tenants + worker with VK.
"""

import pytest

from lab_env import LabEnvironmentError


@pytest.fixture(autouse=True, scope="session")
def rhoai3_mt_env(rhoai3_lab_env, rhoai3_environment):
    """Ensure both tenants and worker are running for multi-tenant tests."""
    env = rhoai3_lab_env

    for vm in ("sno-tenant-rhoai3", "sno-tenant2-rhoai3"):
        try:
            env.ensure_running(vm)
        except LabEnvironmentError:
            pytest.skip(f"VM {vm} not available")

    env.ensure_running("sno-worker")
    yield env

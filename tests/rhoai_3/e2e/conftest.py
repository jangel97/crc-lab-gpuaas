"""
E2E suite conftest: single tenant + worker with VK.
"""

import pytest

from lab_env import LabEnvironmentError


@pytest.fixture(autouse=True, scope="session")
def rhoai3_e2e_env(rhoai3_lab_env, rhoai3_environment):
    """Ensure tenant and worker are running for e2e tests."""
    env = rhoai3_lab_env

    try:
        env.ensure_running("sno-tenant-rhoai3")
    except LabEnvironmentError:
        pytest.skip("VM sno-tenant-rhoai3 not available")

    env.ensure_running("sno-worker")
    yield env

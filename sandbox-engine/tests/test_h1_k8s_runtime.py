import os
import yaml
import pytest
from unittest.mock import AsyncMock, patch

from sandbox_engine.config import EngineSettings, _allow_insecure_k8s_runtime
from sandbox_engine.drivers.k8s_driver import KubernetesDriver
from sandbox_engine.models import SandboxSpec


def test_k8s_manifest_runtime_class_set_to_gvisor():
    """Assert that the shipped k8s manifest specifies SANDBOX_K8S_RUNTIME_CLASS: gvisor."""
    manifest_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../k8s/engine-deployment.yaml"))
    assert os.path.exists(manifest_path), f"Manifest not found at {manifest_path}"

    with open(manifest_path, "r", encoding="utf-8") as f:
        docs = list(yaml.safe_load_all(f))

    deployment = next(d for d in docs if d and d.get("kind") == "Deployment")
    container = deployment["spec"]["template"]["spec"]["containers"][0]
    env_vars = {e["name"]: e.get("value") for e in container.get("env", []) if "value" in e}

    assert env_vars.get("SANDBOX_DRIVER") == "k8s"
    assert env_vars.get("SANDBOX_K8S_RUNTIME_CLASS") == "gvisor", (
        f"Expected SANDBOX_K8S_RUNTIME_CLASS to be 'gvisor', found: {env_vars.get('SANDBOX_K8S_RUNTIME_CLASS')!r}"
    )


def test_production_rejects_empty_or_runc_runtime_class():
    """Assert validate_production_settings() rejects empty, runc, and none runtime class for k8s driver."""
    base_prod = dict(
        environment="production",
        api_key="a" * 32,
        encryption_key="b" * 32,
        postgres_url="postgresql://sandbox:sandbox@localhost:5432/sandbox",
        tenant_keys_json='{"tenant": "' + ("a" * 32) + '"}',
        driver="k8s",
    )

    # Valid gvisor passes
    s_valid = EngineSettings(**base_prod, k8s_runtime_class="gvisor")
    s_valid.validate_production_settings()

    # Empty runtime class rejected
    s_empty = EngineSettings(**base_prod, k8s_runtime_class="")
    with pytest.raises(ValueError, match="requires a secure runtime class"):
        s_empty.validate_production_settings()

    # runc runtime class rejected
    s_runc = EngineSettings(**base_prod, k8s_runtime_class="runc")
    with pytest.raises(ValueError, match="requires a secure runtime class"):
        s_runc.validate_production_settings()

    # none runtime class rejected
    s_none = EngineSettings(**base_prod, k8s_runtime_class="none")
    with pytest.raises(ValueError, match="requires a secure runtime class"):
        s_none.validate_production_settings()


def test_production_allows_insecure_k8s_override():
    """Assert explicit insecure override allows empty or runc runtime class in production."""
    base_prod = dict(
        environment="production",
        api_key="a" * 32,
        encryption_key="b" * 32,
        postgres_url="postgresql://sandbox:sandbox@localhost:5432/sandbox",
        tenant_keys_json='{"tenant": "' + ("a" * 32) + '"}',
        driver="k8s",
        k8s_runtime_class="",
    )

    # Via setting flag
    s_override = EngineSettings(**base_prod, allow_insecure_k8s_runtime=True)
    s_override.validate_production_settings()

    # Via env var ALLOW_INSECURE_K8S_RUNTIME=1
    os.environ["ALLOW_INSECURE_K8S_RUNTIME"] = "1"
    try:
        assert _allow_insecure_k8s_runtime()
        s_env = EngineSettings(**base_prod)
        s_env.validate_production_settings()
    finally:
        os.environ.pop("ALLOW_INSECURE_K8S_RUNTIME", None)


@pytest.mark.asyncio
async def test_k8s_driver_sets_runtime_class_name():
    """Assert KubernetesDriver attaches runtime_class_name='gvisor' to PodSpec."""
    from sandbox_engine.config import settings

    driver = KubernetesDriver()
    driver._initialized = True
    driver._core_api = AsyncMock()

    created_pod = None

    async def mock_create_namespaced_pod(namespace, body):
        nonlocal created_pod
        created_pod = body
        # Return mock pod status
        mock_res = AsyncMock()
        mock_res.metadata.name = body.metadata.name
        mock_res.status.phase = "Running"
        mock_res.status.pod_ip = "10.244.1.99"
        return mock_res

    mock_pod = AsyncMock()
    mock_pod.status.pod_ip = "10.244.1.99"
    mock_cond = AsyncMock()
    mock_cond.type = "Ready"
    mock_cond.status = "True"
    mock_pod.status.conditions = [mock_cond]
    mock_pod.status.container_statuses = []

    driver._core_api.create_namespaced_pod = mock_create_namespaced_pod
    driver._core_api.read_namespaced_pod = AsyncMock(return_value=mock_pod)

    spec = SandboxSpec(spec_id="test-spec-h1", image="sandbox-base:latest")

    with patch.object(settings, "k8s_runtime_class", "gvisor"), \
         patch.object(settings, "environment", "test"):
        await driver.create_sandbox(spec)

    assert created_pod is not None
    assert created_pod.spec.runtime_class_name == "gvisor"


@pytest.mark.asyncio
async def test_k8s_driver_rejects_empty_runtime_in_prod_without_override():
    """Assert KubernetesDriver.create_sandbox rejects empty runtime class in production without override."""
    from sandbox_engine.config import settings

    driver = KubernetesDriver()
    driver._initialized = True
    driver._core_api = AsyncMock()

    spec = SandboxSpec(spec_id="test-spec-h1-fail", image="sandbox-base:latest")

    with patch.object(settings, "k8s_runtime_class", ""), \
         patch.object(settings, "environment", "production"), \
         patch.object(settings, "allow_insecure_k8s_runtime", False):
        with pytest.raises(ValueError, match="requires a secure runtime class"):
            await driver.create_sandbox(spec)


def test_validating_admission_policy_manifest():
    """Assert k8s/validating-admission-policy.yaml requires runtimeClassName: gvisor for sandboxes."""
    policy_path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "../../k8s/validating-admission-policy.yaml")
    )
    assert os.path.exists(policy_path), f"Policy file not found at {policy_path}"

    with open(policy_path, "r", encoding="utf-8") as f:
        docs = list(yaml.safe_load_all(f))

    policy = next(d for d in docs if d and d.get("kind") == "ValidatingAdmissionPolicy")
    binding = next(d for d in docs if d and d.get("kind") == "ValidatingAdmissionPolicyBinding")

    # Assert validation expression checks runtimeClassName == 'gvisor'
    validations = policy["spec"]["validations"]
    assert any("gvisor" in v.get("expression", "") for v in validations)

    # Assert binding targets sandboxes namespace
    assert binding["spec"]["policyName"] == policy["metadata"]["name"]
    ns_expr = binding["spec"]["matchResources"]["namespaceSelector"]["matchExpressions"][0]
    assert "sandboxes" in ns_expr["values"]

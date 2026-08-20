from pathlib import Path

import pytest
from pydantic import ValidationError

from chattocodex.orchestrator_backend.setup.runtime_setup_service import (
    RuntimeSetupService,
)
from chattocodex.shared.contracts.setup_schemas import SetupRequest
from chattocodex.shared.security.owner_token_service import OwnerTokenService


def _workspace_with_project(tmp_path: Path) -> Path:
    workspace_root = tmp_path / "workspace"
    project_root = workspace_root / "sample"
    project_root.mkdir(parents=True)
    (project_root / "pyproject.toml").write_text(
        "[project]\nname = 'sample'\n",
        encoding="utf-8",
    )
    return workspace_root


def test_setup_persists_remote_configuration_and_returns_owner_token_once(
    tmp_path: Path,
) -> None:
    workspace_root = _workspace_with_project(tmp_path)
    state_root = tmp_path / "state"
    service = RuntimeSetupService(state_root=state_root)
    request = SetupRequest(
        workspace_root=workspace_root,
        public_url="https://agent.example.test",
        oauth_enabled=True,
    )

    first = service.configure(request=request)
    assert first.owner_token_created is True
    assert first.owner_token is not None
    assert first.owner_token.startswith("cto_owner_")
    assert first.connector_url == "https://agent.example.test/mcp"
    assert first.project_count == 1
    assert OwnerTokenService(state_root=state_root).verify(candidate=first.owner_token)

    second = service.configure(request=request)
    assert second.owner_token_created is False
    assert second.owner_token is None

    doctor = service.doctor()
    assert doctor.ok is True
    assert doctor.configured is True
    assert doctor.oauth_enabled is True
    assert doctor.owner_token_configured is True
    assert doctor.project_count == 1
    assert doctor.connector_url == "https://agent.example.test/mcp"


def test_setup_validation_requires_https_for_nonlocal_oauth(tmp_path: Path) -> None:
    workspace_root = _workspace_with_project(tmp_path)

    with pytest.raises(ValidationError):
        SetupRequest(
            workspace_root=workspace_root,
            public_url="http://agent.example.test",
            oauth_enabled=True,
        )

    local_request = SetupRequest(
        workspace_root=workspace_root,
        public_url="http://127.0.0.1:8765",
        oauth_enabled=True,
    )
    assert local_request.oauth_enabled is True


def test_doctor_reports_unconfigured_runtime(tmp_path: Path) -> None:
    doctor = RuntimeSetupService(state_root=tmp_path / "state").doctor()
    assert doctor.ok is False
    assert doctor.configured is False
    assert doctor.issues == ("Runtime is not configured. Run `chattocodex setup`.",)

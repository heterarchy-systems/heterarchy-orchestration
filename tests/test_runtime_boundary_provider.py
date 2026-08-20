from pathlib import Path

from starlette.testclient import TestClient

from chattocodex.orchestrator_backend.control_panel.infrastructure.cloudflare_process_supervisor import (
    CloudflareProcessSupervisor,
)
from chattocodex.orchestrator_backend.control_panel.interface.routes.control_panel_routes import (
    create_control_panel_app,
)
from chattocodex.orchestrator_backend.control_panel.application.control_panel_service import (
    ControlPanelService,
)
from chattocodex.orchestrator_backend.control_panel.infrastructure.mcp_process_supervisor import (
    McpProcessSupervisor,
)
from chattocodex.orchestrator_backend.control_panel.composition.runtime_boundary_provider import (
    WorkspaceRuntimeBoundaryProvider,
)
from chattocodex.shared.settings.runtime_config_repository import (
    RuntimeConfigRepository,
)
from chattocodex.orchestrator_backend.persistence.wake.wake_target_repository import (
    WakeTargetRepository,
)
from chattocodex.shared.pathing.workspace_execution_layout import (
    WorkspaceExecutionLayout,
)
from chattocodex.shared.contracts.auth_schemas import RuntimeConfigRecord


def _workspace(tmp_path: Path, name: str) -> Path:
    workspace = tmp_path / name
    project = workspace / "sample"
    project.mkdir(parents=True)
    (project / "pyproject.toml").write_text(
        "[project]\nname='sample'\n", encoding="utf-8"
    )
    return workspace


def _save_runtime_config(
    *, repository: RuntimeConfigRepository, workspace: Path
) -> None:
    repository.save(
        record=RuntimeConfigRecord(
            workspace_root=str(workspace.resolve()),
            public_url=None,
            oauth_enabled=False,
        )
    )


def _app(*, state_root: Path, repository: RuntimeConfigRepository):
    return create_control_panel_app(
        service=ControlPanelService(state_root=state_root),
        runtime_boundary_provider=WorkspaceRuntimeBoundaryProvider(
            runtime_config_repository=repository
        ),
    )


def test_runtime_routes_fail_closed_before_runtime_config_exists(
    tmp_path: Path,
) -> None:
    state_root = tmp_path / "control"
    repository = RuntimeConfigRepository(state_root=state_root)

    with TestClient(_app(state_root=state_root, repository=repository)) as client:
        response = client.get("/api/wake/projects")

    assert response.status_code == 409
    assert response.json()["error_code"] == "project_binding_configuration"
    assert not (state_root / "execution-bootstrap").exists()


def test_runtime_routes_share_mcp_workspace_execution_root_after_setup(
    tmp_path: Path,
    monkeypatch,
) -> None:
    state_root = tmp_path / "control"
    workspace = _workspace(tmp_path, "workspace-a")
    repository = RuntimeConfigRepository(state_root=state_root)
    execution_root = WorkspaceExecutionLayout.state_root(workspace_root=workspace)

    async def fake_start_quick(
        self: CloudflareProcessSupervisor,
        *,
        local_port: int,
    ) -> str:
        assert local_port == 8765
        self._public_url = "https://runtime-root-test.trycloudflare.com"
        return self._public_url

    async def fake_start_mcp(self: McpProcessSupervisor, *, port: int) -> None:
        assert port == 8765

    async def fake_ensure_port_available(
        self: McpProcessSupervisor,
        *,
        port: int,
    ) -> None:
        assert port == 8765

    monkeypatch.setattr(CloudflareProcessSupervisor, "start_quick", fake_start_quick)
    monkeypatch.setattr(
        McpProcessSupervisor,
        "ensure_port_available",
        fake_ensure_port_available,
    )
    monkeypatch.setattr(McpProcessSupervisor, "start", fake_start_mcp)

    with TestClient(_app(state_root=state_root, repository=repository)) as client:
        saved = client.post(
            "/api/preferences",
            json={
                "workspace_root": str(workspace),
                "remote_mode": "quick_tunnel",
                "custom_origin": None,
                "mcp_port": 8765,
            },
        )
        assert saved.status_code == 200
        assert not repository.exists()
        started = client.post("/api/start")
        assert started.status_code == 200
        assert repository.read().workspace_root == str(workspace.resolve())

        project_id = client.get("/api/wake/projects").json()["projects"][0][
            "project_id"
        ]
        wake_target_id = "wtgt_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        response = client.post(
            "/api/wake/targets/register",
            json={"wake_target_id": wake_target_id, "project_id": project_id},
        )

    assert response.status_code == 200
    assert (
        WakeTargetRepository(state_root=execution_root)
        .read(wake_target_id=wake_target_id)
        .project_id
        == project_id
    )
    assert not (state_root / "execution-bootstrap").exists()


def test_runtime_routes_switch_to_new_workspace_execution_root(tmp_path: Path) -> None:
    state_root = tmp_path / "control"
    first_workspace = _workspace(tmp_path, "workspace-a")
    second_workspace = _workspace(tmp_path, "workspace-b")
    repository = RuntimeConfigRepository(state_root=state_root)
    _save_runtime_config(repository=repository, workspace=first_workspace)
    app = _app(state_root=state_root, repository=repository)

    with TestClient(app) as client:
        first_project_id = client.get("/api/wake/projects").json()["projects"][0][
            "project_id"
        ]
        first_target_id = "wtgt_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        assert (
            client.post(
                "/api/wake/targets/register",
                json={
                    "wake_target_id": first_target_id,
                    "project_id": first_project_id,
                },
            ).status_code
            == 200
        )

        _save_runtime_config(repository=repository, workspace=second_workspace)
        second_project_id = client.get("/api/wake/projects").json()["projects"][0][
            "project_id"
        ]
        second_target_id = "wtgt_cccccccccccccccccccccccccccccccc"
        assert (
            client.post(
                "/api/wake/targets/register",
                json={
                    "wake_target_id": second_target_id,
                    "project_id": second_project_id,
                },
            ).status_code
            == 200
        )

    first_root = WorkspaceExecutionLayout.state_root(workspace_root=first_workspace)
    second_root = WorkspaceExecutionLayout.state_root(workspace_root=second_workspace)
    assert WakeTargetRepository(state_root=first_root).exists(
        wake_target_id=first_target_id
    )
    assert not WakeTargetRepository(state_root=first_root).exists(
        wake_target_id=second_target_id
    )
    assert WakeTargetRepository(state_root=second_root).exists(
        wake_target_id=second_target_id
    )

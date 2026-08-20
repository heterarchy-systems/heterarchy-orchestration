from pathlib import Path

from chattocodex.orchestrator_backend.control_panel.domain.control_panel_dto import (
    RemoteMode,
)
from chattocodex.orchestrator_backend.control_panel.infrastructure.control_panel_preferences_repository import (
    ControlPanelPreferencesRepository,
)
from chattocodex.orchestrator_backend.control_panel.application.control_panel_service import (
    ControlPanelService,
)
from chattocodex.shared.contracts.control_panel.control_panel_schemas import (
    ControlPanelPreferencesRequest,
)


def _workspace(tmp_path: Path) -> Path:
    workspace = tmp_path / "workspace"
    project = workspace / "sample"
    project.mkdir(parents=True)
    (project / "pyproject.toml").write_text(
        "[project]\nname='sample'\n", encoding="utf-8"
    )
    return workspace


def test_control_panel_saves_quick_tunnel_preferences(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    service = ControlPanelService(state_root=tmp_path / "state")

    import asyncio

    snapshot = asyncio.run(
        service.save_preferences(
            request=ControlPanelPreferencesRequest(
                workspace_root=str(workspace),
                remote_mode=RemoteMode.QUICK_TUNNEL,
                custom_origin=None,
                mcp_port=8765,
            )
        )
    )

    assert snapshot.configured is True
    assert snapshot.project_count == 1
    saved = ControlPanelPreferencesRepository(state_root=tmp_path / "state").read()
    assert saved.workspace_root == workspace.resolve()
    assert saved.remote_mode is RemoteMode.QUICK_TUNNEL
    assert saved.custom_origin is None


def test_control_panel_normalizes_custom_domain(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    service = ControlPanelService(state_root=tmp_path / "state")

    import asyncio

    asyncio.run(
        service.save_preferences(
            request=ControlPanelPreferencesRequest(
                workspace_root=str(workspace),
                remote_mode=RemoteMode.CUSTOM_DOMAIN,
                custom_origin="mcp.example.com/",
                mcp_port=9000,
            )
        )
    )

    saved = ControlPanelPreferencesRepository(state_root=tmp_path / "state").read()
    assert saved.custom_origin == "https://mcp.example.com"
    assert saved.mcp_port == 9000

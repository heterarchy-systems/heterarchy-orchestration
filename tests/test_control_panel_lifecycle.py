import asyncio
import threading
from pathlib import Path

import pytest

from chattocodex.orchestrator_backend.control_panel.infrastructure.cloudflare_process_supervisor import (
    CloudflareProcessSupervisor,
)
from chattocodex.orchestrator_backend.control_panel.domain.control_panel_dto import (
    RemoteMode,
)
from chattocodex.orchestrator_backend.control_panel.domain.control_panel_exceptions import (
    RuntimeLifecycleError,
)
from chattocodex.orchestrator_backend.control_panel.application.control_panel_service import (
    ControlPanelService,
)
from chattocodex.orchestrator_backend.control_panel.infrastructure.mcp_process_supervisor import (
    McpProcessSupervisor,
)
from chattocodex.shared.settings.runtime_config_repository import (
    RuntimeConfigRepository,
)
from chattocodex.shared.contracts.control_panel.control_panel_schemas import (
    ControlPanelPreferencesRequest,
)


@pytest.fixture(autouse=True)
def _stub_mcp_port_preflight(monkeypatch) -> None:
    async def fake_ensure_port_available(
        self: McpProcessSupervisor,
        *,
        port: int,
    ) -> None:
        del self, port

    monkeypatch.setattr(
        McpProcessSupervisor,
        "ensure_port_available",
        fake_ensure_port_available,
    )


def _workspace(tmp_path: Path) -> Path:
    workspace = tmp_path / "workspace"
    project = workspace / "sample"
    project.mkdir(parents=True)
    (project / "pyproject.toml").write_text(
        "[project]\nname='sample'\n",
        encoding="utf-8",
    )
    return workspace


def test_quick_start_configures_oauth_from_issued_tunnel_and_hides_stale_url(
    tmp_path: Path,
    monkeypatch,
) -> None:
    state_root = tmp_path / "state"
    workspace = _workspace(tmp_path)
    service = ControlPanelService(state_root=state_root)
    quick_origin = "https://calm-river-42.trycloudflare.com"

    async def fake_start_quick(
        self: CloudflareProcessSupervisor,
        *,
        local_port: int,
    ) -> str:
        assert local_port == 8765
        self._public_url = quick_origin
        return quick_origin

    async def fake_start_mcp(self: McpProcessSupervisor, *, port: int) -> None:
        assert port == 8765

    monkeypatch.setattr(CloudflareProcessSupervisor, "start_quick", fake_start_quick)
    monkeypatch.setattr(McpProcessSupervisor, "start", fake_start_mcp)

    async def scenario() -> None:
        await service.save_preferences(
            request=ControlPanelPreferencesRequest(
                workspace_root=str(workspace),
                remote_mode=RemoteMode.QUICK_TUNNEL,
                custom_origin=None,
                mcp_port=8765,
            )
        )
        started = await service.start()
        assert started.public_url == quick_origin
        assert started.connector_url == f"{quick_origin}/mcp"
        assert started.visible_owner_token is not None

        runtime = RuntimeConfigRepository(state_root=state_root).read()
        assert runtime.public_url == quick_origin
        assert runtime.oauth_enabled is True

        stopped = await service.stop()
        assert stopped.public_url is None
        assert stopped.connector_url is None

    asyncio.run(scenario())


def test_start_recovers_missing_mcp_without_rotating_running_quick_tunnel(
    tmp_path: Path,
    monkeypatch,
) -> None:
    state_root = tmp_path / "state"
    workspace = _workspace(tmp_path)
    service = ControlPanelService(state_root=state_root)
    quick_origin = "https://stable-dogfood.trycloudflare.com"
    state = {
        "tunnel_running": False,
        "mcp_running": False,
        "tunnel_starts": 0,
        "mcp_starts": 0,
    }

    def fake_tunnel_running(self: CloudflareProcessSupervisor) -> bool:
        del self
        return bool(state["tunnel_running"])

    def fake_mcp_running(self: McpProcessSupervisor) -> bool:
        del self
        return bool(state["mcp_running"])

    async def fake_start_quick(
        self: CloudflareProcessSupervisor,
        *,
        local_port: int,
    ) -> str:
        assert local_port == 8765
        state["tunnel_starts"] += 1
        state["tunnel_running"] = True
        self._public_url = quick_origin
        return quick_origin

    async def fake_start_mcp(self: McpProcessSupervisor, *, port: int) -> None:
        del self
        assert port == 8765
        state["mcp_starts"] += 1
        state["mcp_running"] = True

    monkeypatch.setattr(CloudflareProcessSupervisor, "is_running", fake_tunnel_running)
    monkeypatch.setattr(McpProcessSupervisor, "is_running", fake_mcp_running)
    monkeypatch.setattr(CloudflareProcessSupervisor, "start_quick", fake_start_quick)
    monkeypatch.setattr(McpProcessSupervisor, "start", fake_start_mcp)

    async def scenario() -> None:
        await service.save_preferences(
            request=ControlPanelPreferencesRequest(
                workspace_root=str(workspace),
                remote_mode=RemoteMode.QUICK_TUNNEL,
                custom_origin=None,
                mcp_port=8765,
            )
        )
        first = await service.start()
        assert first.public_url == quick_origin
        assert state["tunnel_starts"] == 1
        assert state["mcp_starts"] == 1

        state["mcp_running"] = False
        recovered = await service.start()

        assert recovered.public_url == quick_origin
        assert recovered.connector_url == f"{quick_origin}/mcp"
        assert state["tunnel_starts"] == 1
        assert state["mcp_starts"] == 2

    asyncio.run(scenario())


def test_save_preferences_finishes_before_racing_start_reads_them(
    tmp_path: Path,
    monkeypatch,
) -> None:
    state_root = tmp_path / "state"
    workspace = _workspace(tmp_path)
    service = ControlPanelService(state_root=state_root)
    save_entered = threading.Event()
    allow_save = threading.Event()
    start_entered = asyncio.Event()
    started_ports: list[int] = []

    original_save = service._preferences_repository.save

    def blocking_save(*, preferences) -> None:
        save_entered.set()
        assert allow_save.wait(timeout=2)
        original_save(preferences=preferences)

    async def fake_start_quick(
        self: CloudflareProcessSupervisor,
        *,
        local_port: int,
    ) -> str:
        start_entered.set()
        self._public_url = "https://linearizable.trycloudflare.com"
        return self._public_url

    async def fake_start_mcp(self: McpProcessSupervisor, *, port: int) -> None:
        del self
        started_ports.append(port)

    monkeypatch.setattr(service._preferences_repository, "save", blocking_save)
    monkeypatch.setattr(CloudflareProcessSupervisor, "start_quick", fake_start_quick)
    monkeypatch.setattr(McpProcessSupervisor, "start", fake_start_mcp)

    async def scenario() -> None:
        save_task = asyncio.create_task(
            service.save_preferences(
                request=ControlPanelPreferencesRequest(
                    workspace_root=str(workspace),
                    remote_mode=RemoteMode.QUICK_TUNNEL,
                    custom_origin=None,
                    mcp_port=9000,
                )
            )
        )
        assert await asyncio.to_thread(save_entered.wait, 2)

        start_task = asyncio.create_task(service.start())
        await asyncio.sleep(0)
        assert not start_entered.is_set()

        allow_save.set()
        await save_task
        await start_task

        assert started_ports == [9000]

    asyncio.run(scenario())


def test_save_preferences_is_rejected_after_racing_start_enters_running(
    tmp_path: Path,
    monkeypatch,
) -> None:
    state_root = tmp_path / "state"
    workspace = _workspace(tmp_path)
    service = ControlPanelService(state_root=state_root)
    start_entered = asyncio.Event()
    allow_start = asyncio.Event()
    state = {"tunnel_running": False, "mcp_running": False}

    def fake_tunnel_running(self: CloudflareProcessSupervisor) -> bool:
        del self
        return state["tunnel_running"]

    def fake_mcp_running(self: McpProcessSupervisor) -> bool:
        del self
        return state["mcp_running"]

    async def fake_start_quick(
        self: CloudflareProcessSupervisor,
        *,
        local_port: int,
    ) -> str:
        assert local_port == 8765
        state["tunnel_running"] = True
        start_entered.set()
        await allow_start.wait()
        self._public_url = "https://running.trycloudflare.com"
        return self._public_url

    async def fake_start_mcp(self: McpProcessSupervisor, *, port: int) -> None:
        del self
        assert port == 8765
        state["mcp_running"] = True

    monkeypatch.setattr(CloudflareProcessSupervisor, "is_running", fake_tunnel_running)
    monkeypatch.setattr(McpProcessSupervisor, "is_running", fake_mcp_running)
    monkeypatch.setattr(CloudflareProcessSupervisor, "start_quick", fake_start_quick)
    monkeypatch.setattr(McpProcessSupervisor, "start", fake_start_mcp)

    async def scenario() -> None:
        await service.save_preferences(
            request=ControlPanelPreferencesRequest(
                workspace_root=str(workspace),
                remote_mode=RemoteMode.QUICK_TUNNEL,
                custom_origin=None,
                mcp_port=8765,
            )
        )

        start_task = asyncio.create_task(service.start())
        await start_entered.wait()
        save_task = asyncio.create_task(
            service.save_preferences(
                request=ControlPanelPreferencesRequest(
                    workspace_root=str(workspace),
                    remote_mode=RemoteMode.QUICK_TUNNEL,
                    custom_origin=None,
                    mcp_port=9000,
                )
            )
        )
        await asyncio.sleep(0)
        assert not save_task.done()

        allow_start.set()
        await start_task
        with pytest.raises(
            RuntimeLifecycleError,
            match="Stop MCP and the tunnel before changing saved settings",
        ):
            await save_task

        saved = service._preferences_repository.read()
        assert saved.mcp_port == 8765

    asyncio.run(scenario())

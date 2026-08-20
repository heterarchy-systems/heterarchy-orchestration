import asyncio
import socket
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
from chattocodex.shared.contracts.control_panel.control_panel_schemas import (
    ControlPanelPreferencesRequest,
)
from chattocodex.shared.contracts.setup_schemas import SetupRequest
from chattocodex.orchestrator_backend.setup.runtime_setup_service import (
    RuntimeSetupService,
)


def _occupied_listener() -> socket.socket:
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen()
    return listener


def _workspace(tmp_path: Path) -> Path:
    workspace = tmp_path / "workspace"
    project = workspace / "sample"
    project.mkdir(parents=True)
    (project / "pyproject.toml").write_text(
        "[project]\nname='sample'\n",
        encoding="utf-8",
    )
    return workspace


def test_start_rejects_an_already_occupied_port(tmp_path: Path) -> None:
    listener = _occupied_listener()
    port = listener.getsockname()[1]
    supervisor = McpProcessSupervisor(state_root=tmp_path / "state")

    async def scenario() -> None:
        try:
            with pytest.raises(RuntimeLifecycleError, match="already in use"):
                await supervisor.start(port=port)
            assert supervisor.snapshot().running is False
        finally:
            await supervisor.stop()
            listener.close()

    asyncio.run(scenario())


def test_quick_start_checks_mcp_port_before_starting_tunnel(
    tmp_path: Path,
    monkeypatch,
) -> None:
    listener = _occupied_listener()
    port = listener.getsockname()[1]
    state_root = tmp_path / "state"
    workspace = _workspace(tmp_path)
    service = ControlPanelService(state_root=state_root)
    tunnel_started = False

    async def fail_if_tunnel_starts(
        self: CloudflareProcessSupervisor,
        *,
        local_port: int,
    ) -> str:
        del self, local_port
        nonlocal tunnel_started
        tunnel_started = True
        raise AssertionError("Quick Tunnel started before MCP port preflight.")

    monkeypatch.setattr(
        CloudflareProcessSupervisor,
        "start_quick",
        fail_if_tunnel_starts,
    )

    async def scenario() -> None:
        try:
            await service.save_preferences(
                request=ControlPanelPreferencesRequest(
                    workspace_root=str(workspace),
                    remote_mode=RemoteMode.QUICK_TUNNEL,
                    custom_origin=None,
                    mcp_port=port,
                )
            )
            with pytest.raises(RuntimeLifecycleError, match="already in use"):
                await service.start()
            assert tunnel_started is False
        finally:
            await service.shutdown()
            listener.close()

    asyncio.run(scenario())


def test_start_does_not_accept_an_unowned_listener_after_preflight_race(
    tmp_path: Path,
    monkeypatch,
) -> None:
    listener = _occupied_listener()
    port = listener.getsockname()[1]
    state_root = tmp_path / "state"
    workspace = _workspace(tmp_path)
    RuntimeSetupService(state_root=state_root).configure(
        request=SetupRequest(
            workspace_root=workspace,
            public_url=f"http://127.0.0.1:{port}",
            oauth_enabled=True,
        )
    )
    supervisor = McpProcessSupervisor(state_root=state_root)

    async def simulate_port_becoming_occupied_after_preflight(
        self: McpProcessSupervisor,
        *,
        port: int,
    ) -> None:
        del self, port

    monkeypatch.setattr(
        McpProcessSupervisor,
        "ensure_port_available",
        simulate_port_becoming_occupied_after_preflight,
        raising=False,
    )

    async def scenario() -> None:
        try:
            with pytest.raises(
                RuntimeLifecycleError,
                match="exited during startup",
            ):
                await supervisor.start(port=port)
            assert supervisor.snapshot().running is False
        finally:
            await supervisor.stop()
            listener.close()

    asyncio.run(scenario())

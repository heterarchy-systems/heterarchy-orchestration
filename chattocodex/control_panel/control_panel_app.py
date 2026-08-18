import threading
import webbrowser
from pathlib import Path

import uvicorn
from agent_broker import OperationsClientConfig

from chattocodex.broker_client.broker_operations_gateway import (
    AgentBrokerOperationsGateway,
)
from chattocodex.control_panel.control_panel_routes import create_control_panel_app
from chattocodex.control_panel.control_panel_service import ControlPanelService
from chattocodex.control_panel.runtime_boundary_provider import (
    WorkspaceRuntimeBoundaryProvider,
)
from chattocodex.persistence.runtime_config_repository import RuntimeConfigRepository
from chattocodex.runtime_settings import RuntimeSettings

_DEFAULT_HOST = "127.0.0.1"
_DEFAULT_PORT = 8790


def serve_control_panel(
    *,
    state_root: Path,
    host: str = _DEFAULT_HOST,
    port: int = _DEFAULT_PORT,
    open_browser: bool = True,
) -> None:
    """Run the loopback Control Panel and its local runtime boundaries.

    Args:
        state_root: Global durable Control Panel state root.
        host: Host to bind; only loopback hosts are accepted.
        port: Local Control Panel port.
        open_browser: Whether to open the local Control Panel automatically.

    Raises:
        ValueError: If ``host`` is not a loopback host.
    """
    if host not in {"127.0.0.1", "localhost", "::1"}:
        raise ValueError("Control Panel must bind to a loopback host.")
    service = ControlPanelService(state_root=state_root)
    runtime_config_repository = RuntimeConfigRepository(state_root=state_root)
    runtime_boundary_provider = WorkspaceRuntimeBoundaryProvider(
        runtime_config_repository=runtime_config_repository,
    )
    settings = RuntimeSettings()
    application = create_control_panel_app(
        service=service,
        runtime_boundary_provider=runtime_boundary_provider,
        broker_operations=AgentBrokerOperationsGateway(
            config=OperationsClientConfig(
                host=settings.broker_host,
                port=settings.broker_operations_port,
                timeout_seconds=settings.broker_timeout_seconds,
            )
        ),
    )
    url = f"http://{host}:{port}/"
    if open_browser:
        threading.Timer(0.7, webbrowser.open, args=(url,)).start()
    uvicorn.run(
        application,
        host=host,
        port=port,
        access_log=False,
        log_level="warning",
    )

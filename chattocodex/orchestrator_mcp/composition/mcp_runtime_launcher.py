from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Protocol

from mcp.server.mcpserver import MCPServer

from chattocodex.orchestrator_backend.composition.runtime_bootstrap import (
    BackendResourceContextFactory,
    build_orchestrator_backend_runtime,
)
from chattocodex.orchestrator_backend.composition.runtime_services import (
    OrchestratorBackendServices,
)
from chattocodex.orchestrator_mcp.auth.oauth_dto import OAuthServerOptions
from chattocodex.orchestrator_mcp.composition.mcp_lifespan import McpLifespan
from chattocodex.orchestrator_mcp.transport.mcp_transport_security import (
    build_mcp_transport_security,
)
from chattocodex.shared.contracts.broker.broker_gateway_contract import (
    WorkBrokerGateway,
)
from chattocodex.shared.security.runtime_access_service import OwnerTokenService
from chattocodex.shared.settings.runtime_config_repository import (
    RuntimeConfigRepository,
)
from chattocodex.shared.settings.runtime_settings import RuntimeSettings


class McpServerFactory(Protocol):
    """Describe the MCP transport factory that consumes composed backend services."""

    def __call__(
        self,
        state_root: Path,
        backend_services: OrchestratorBackendServices,
        mcp_path: str,
        oauth_options: OAuthServerOptions | None,
        lifespan: McpLifespan | None,
    ) -> MCPServer[None]:
        """Create one MCP server around an already composed backend.

        Args:
            state_root: Process-local state root used by MCP-owned transport state.
            backend_services: Transport-neutral backend application service graph.
            mcp_path: Streamable HTTP MCP route path.
            oauth_options: Optional owner OAuth configuration.
            lifespan: Optional MCP lifespan callback owning backend resources.

        Returns:
            Configured MCP server instance.
        """
        ...


def _build_mcp_lifespan(
    resource_context_factory: BackendResourceContextFactory | None,
) -> McpLifespan | None:
    """Adapt a transport-neutral backend lifecycle to the MCP lifespan protocol.

    Args:
        resource_context_factory: Optional backend async resource context factory.

    Returns:
        MCP lifespan callback, or None when the backend owns no remote resources.
    """
    if resource_context_factory is None:
        return None

    @asynccontextmanager
    async def lifespan(server: MCPServer[None]) -> AsyncIterator[None]:
        """Enter backend resources for one MCP server lifetime.

        Args:
            server: MCP server instance supplied by the framework.

        Returns:
            None. The context yields after backend resources initialize.
        """
        del server
        async with resource_context_factory():
            yield

    return lifespan


def run_configured_mcp(
    settings: RuntimeSettings,
    server_factory: McpServerFactory,
    work_broker_gateway: WorkBrokerGateway | None = None,
) -> None:
    """Load MCP-owned configuration and run a separately composed backend runtime.

    Args:
        settings: Process settings resolved from environment variables.
        server_factory: Factory binding backend services to MCP tools and transport.
        work_broker_gateway: Optional Broker port override used by tests or embedding.

    Returns:
        None.

    Raises:
        RuntimeError: If configured OAuth or required backend state is invalid.
    """
    config = RuntimeConfigRepository(state_root=settings.state_root).read()
    oauth_options: OAuthServerOptions | None = None
    if config.oauth_enabled:
        if config.public_url is None:
            raise RuntimeError("OAuth is enabled but public_url is not configured.")
        owner_tokens = OwnerTokenService(state_root=settings.state_root)
        if not owner_tokens.is_configured():
            raise RuntimeError("OAuth is enabled but Owner Token is not configured.")
        oauth_options = OAuthServerOptions(
            public_url=config.public_url,
            allowed_redirect_hosts=settings.oauth_allowed_redirect_hosts,
            access_token_ttl_seconds=settings.oauth_access_token_ttl_seconds,
            refresh_token_ttl_seconds=settings.oauth_refresh_token_ttl_seconds,
        )

    workspace_root = Path(config.workspace_root).expanduser().resolve()
    backend_runtime = build_orchestrator_backend_runtime(
        settings=settings,
        workspace_root=workspace_root,
        work_broker_gateway=work_broker_gateway,
    )
    server = server_factory(
        state_root=settings.state_root,
        backend_services=backend_runtime.services,
        mcp_path=settings.mcp_path,
        oauth_options=oauth_options,
        lifespan=_build_mcp_lifespan(backend_runtime.resource_context_factory),
    )
    if config.public_url is None:
        server.run(
            "streamable-http",
            host=settings.host,
            port=settings.port,
            streamable_http_path=settings.mcp_path,
            json_response=True,
            stateless_http=False,
        )
        return

    server.run(
        "streamable-http",
        host=settings.host,
        port=settings.port,
        streamable_http_path=settings.mcp_path,
        json_response=True,
        stateless_http=False,
        transport_security=build_mcp_transport_security(public_url=config.public_url),
    )

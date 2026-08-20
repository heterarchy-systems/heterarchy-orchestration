from __future__ import annotations

from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from functools import partial
from pathlib import Path

from agent_broker import BrokerClientConfig

from chattocodex.orchestrator_backend.broker_client.broker_cluster_config import (
    build_broker_cluster_config,
)
from chattocodex.orchestrator_backend.broker_client.external_broker_gateway import (
    AgentBrokerGateway,
)
from chattocodex.orchestrator_backend.composition.backend_container import (
    BackendContainer,
    backend_resources,
    create_backend_container,
)
from chattocodex.orchestrator_backend.composition.runtime_services import (
    OrchestratorBackendServices,
    build_orchestrator_backend_services,
)
from chattocodex.orchestrator_backend.database.database_config import DatabaseConfig
from chattocodex.orchestrator_backend.organization.group_folder_service import (
    GroupFolderService,
)
from chattocodex.shared.contracts.broker.broker_gateway_contract import (
    WorkBrokerGateway,
)
from chattocodex.shared.pathing.workspace_execution_layout import (
    WorkspaceExecutionLayout,
)
from chattocodex.shared.settings.runtime_settings import RuntimeSettings

BackendResourceContextFactory = Callable[[], AbstractAsyncContextManager[None]]


@dataclass(frozen=True, slots=True)
class OrchestratorBackendRuntime:
    """Expose a fully composed backend and its optional resource lifecycle."""

    services: OrchestratorBackendServices
    resource_context_factory: BackendResourceContextFactory | None


def build_orchestrator_backend_runtime(
    settings: RuntimeSettings,
    workspace_root: Path,
    work_broker_gateway: WorkBrokerGateway | None = None,
) -> OrchestratorBackendRuntime:
    """Compose Broker, PostgreSQL, and application services behind one backend boundary.

    Args:
        settings: Validated process settings used by backend infrastructure adapters.
        workspace_root: Configured root containing orchestration projects.
        work_broker_gateway: Optional Broker port override used by tests or embedding.

    Returns:
        Fully composed backend services plus an optional asynchronous resource lifecycle.

    Raises:
        RuntimeError: If PostgreSQL is required but not configured or DI composition is invalid.
    """
    database_config = DatabaseConfig()
    group_folder_service, resource_context_factory = _build_database_backend(
        database_config=database_config
    )
    broker_gateway = work_broker_gateway or AgentBrokerGateway(
        config=BrokerClientConfig(
            host=settings.broker_host,
            port=settings.broker_port,
            timeout_seconds=settings.broker_timeout_seconds,
        ),
        lease_duration_ms=settings.broker_lease_duration_ms,
        cluster_config=build_broker_cluster_config(settings=settings),
    )
    services = build_orchestrator_backend_services(
        state_root=settings.state_root,
        workspace_root=workspace_root,
        work_broker_gateway=broker_gateway,
        execution_state_root=WorkspaceExecutionLayout.state_root(
            workspace_root=workspace_root
        ),
        group_folder_service=group_folder_service,
    )
    return OrchestratorBackendRuntime(
        services=services,
        resource_context_factory=resource_context_factory,
    )


def _build_database_backend(
    database_config: DatabaseConfig,
) -> tuple[GroupFolderService | None, BackendResourceContextFactory | None]:
    """Compose optional PostgreSQL services and their transport-neutral lifecycle.

    Args:
        database_config: Validated PostgreSQL backend settings.

    Returns:
        Optional group-folder service and resource context factory.

    Raises:
        RuntimeError: If PostgreSQL is required but not configured or DI returns an invalid service.
    """
    if not database_config.enabled:
        if database_config.required:
            raise RuntimeError(
                "PostgreSQL backend is required but CHATTOCODEX_DATABASE_URL is not set."
            )
        return None, None

    container = create_backend_container(database_config=database_config)
    group_folder_service = container.group_folder_service()
    if not isinstance(group_folder_service, GroupFolderService):
        raise RuntimeError(
            "Dependency Injector returned an asynchronous GroupFolder service; "
            "resource providers must remain behind the unit-of-work boundary."
        )
    return group_folder_service, _resource_context_factory(container)


def _resource_context_factory(
    container: BackendContainer,
) -> BackendResourceContextFactory:
    """Bind one backend DI container to a no-argument async context factory.

    Args:
        container: Configured backend dependency-injector container.

    Returns:
        Callable that opens and closes the container's backend resources.
    """
    return partial(backend_resources, container)

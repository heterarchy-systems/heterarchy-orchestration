from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from inspect import isawaitable

from dependency_injector import containers, providers

from chattocodex.orchestrator_backend.database.async_postgres_migration_service import (
    AsyncPostgresMigrationService,
)
from chattocodex.orchestrator_backend.database.database_config import DatabaseConfig
from chattocodex.orchestrator_backend.database.sqlalchemy_runtime import (
    init_sqlalchemy_resources,
)
from chattocodex.orchestrator_backend.organization.group_folder_service import (
    GroupFolderService,
)
from chattocodex.orchestrator_backend.organization.group_folder_sqlalchemy_repository import (
    SqlAlchemyGroupFolderUnitOfWork,
)


class BackendContainer(containers.DeclarativeContainer):
    """Compose PostgreSQL-backed orchestration backend services."""

    config = providers.Configuration(strict=True)

    migration_service = providers.Factory(
        AsyncPostgresMigrationService,
        database_url=config.database_url,
        connect_timeout_seconds=config.pool_timeout_seconds.as_float(),
    )

    sqlalchemy_resources = providers.Resource(
        init_sqlalchemy_resources,
        database_url=config.database_url,
        pool_size=config.pool_size.as_int(),
        max_overflow=config.max_overflow.as_int(),
        pool_timeout_seconds=config.pool_timeout_seconds.as_float(),
    )

    group_folder_unit_of_work = providers.Factory(
        SqlAlchemyGroupFolderUnitOfWork,
        resources_provider=sqlalchemy_resources.provider,
    )

    group_folder_service = providers.Factory(
        GroupFolderService,
        unit_of_work_factory=group_folder_unit_of_work.provider,
    )


def create_backend_container(database_config: DatabaseConfig) -> BackendContainer:
    """Create one configured backend DI container without opening resources.

    Args:
        database_config: Validated PostgreSQL backend configuration.

    Returns:
        Configured dependency-injector container for backend services.

    Raises:
        ValueError: If PostgreSQL has no configured connection URL.
    """
    if database_config.url is None:
        raise ValueError(
            "CHATTOCODEX_DATABASE_URL is required for PostgreSQL-backed backend tools"
        )
    container = BackendContainer()
    container.config.from_dict(
        {
            "database_url": database_config.url,
            "pool_size": database_config.pool_size,
            "max_overflow": database_config.max_overflow,
            "pool_timeout_seconds": database_config.pool_timeout_seconds,
        }
    )
    return container


@asynccontextmanager
async def backend_resources(container: BackendContainer) -> AsyncIterator[None]:
    """Initialize and release backend resources independently of any transport.

    Args:
        container: Configured backend dependency-injector container.

    Returns:
        None. The async context yields only after migrations and resources initialize.

    Raises:
        RuntimeError: If a configured backend resource cannot initialize or shut down.
    """
    migration_service = container.migration_service()
    await migration_service.migrate()
    try:
        initialization = container.init_resources()
        if isawaitable(initialization):
            await initialization
        yield
    finally:
        cleanup = container.shutdown_resources()
        if isawaitable(cleanup):
            await cleanup

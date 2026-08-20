from __future__ import annotations

from chattocodex.orchestrator_backend.database.database_config import DatabaseConfig
from chattocodex.orchestrator_backend.database.database_exceptions import (
    DatabaseConnectionError,
)
from chattocodex.orchestrator_backend.database.operations_read_model_repository import (
    OperationsReadModelRepository,
)
from chattocodex.shared.contracts.control_panel.control_panel_operations_schemas import (
    ControlPanelOperationsResponse,
    DatabaseOperationsState,
    DatabaseOperationsView,
)


class ControlPanelDatabaseBridge:
    """Project runtime state through one reusable asynchronous PostgreSQL repository."""

    def __init__(self, config: DatabaseConfig) -> None:
        """Create the optional async read-model repository once per Control Panel service.

        Args:
            config: Validated PostgreSQL connection and pool configuration.

        Returns:
            None.
        """
        self._repository: OperationsReadModelRepository | None = None
        if config.enabled:
            assert config.url is not None
            self._repository = OperationsReadModelRepository(
                database_url=config.url,
                pool_size=config.pool_size,
                max_overflow=config.max_overflow,
                pool_timeout_seconds=config.pool_timeout_seconds,
            )

    async def project_or_fallback(
        self,
        snapshot: ControlPanelOperationsResponse,
    ) -> ControlPanelOperationsResponse:
        """Project a snapshot asynchronously or preserve file-backed state on DB failure.

        Args:
            snapshot: File-backed operations snapshot that remains runtime authority.

        Returns:
            PostgreSQL-reloaded snapshot when available, otherwise the original snapshot
            annotated with database availability state.
        """
        repository = self._repository
        if repository is None:
            return snapshot
        try:
            return await repository.project_and_reload(snapshot)
        except DatabaseConnectionError:
            return snapshot.model_copy(
                update={
                    "database": DatabaseOperationsView(
                        enabled=True,
                        state=DatabaseOperationsState.UNAVAILABLE,
                        projected_at=None,
                        message=(
                            "PostgreSQL read model is unavailable. Worker execution remains "
                            "file-backed and continues independently."
                        ),
                    )
                }
            )

    async def close(self) -> None:
        """Release the optional repository-owned SQLAlchemy connection pool.

        Args:
            None.

        Returns:
            None after any allocated async engine is disposed.
        """
        if self._repository is not None:
            await self._repository.close()

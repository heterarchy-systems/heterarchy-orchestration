from __future__ import annotations

from chattocodex.database.database_config import DatabaseConfig
from chattocodex.database.database_exceptions import (
    DatabaseConnectionError,
    DatabaseDriverUnavailableError,
)
from chattocodex.database.operations_read_model_repository import (
    OperationsReadModelRepository,
)
from chattocodex.database.postgres_driver import load_postgres_connect
from chattocodex.schemas.control_panel_operations_schemas import (
    ControlPanelOperationsResponse,
    DatabaseOperationsView,
)


class ControlPanelDatabaseBridge:
    """Project runtime state into PostgreSQL without moving runtime authority there."""

    def __init__(self, *, config: DatabaseConfig) -> None:
        self._config = config

    def project_or_fallback(
        self,
        *,
        snapshot: ControlPanelOperationsResponse,
    ) -> ControlPanelOperationsResponse:
        if not self._config.enabled:
            return snapshot
        assert self._config.url is not None
        try:
            connect = load_postgres_connect()
            repository = OperationsReadModelRepository(
                database_url=self._config.url,
                connect=connect,
            )
            return repository.project_and_reload(snapshot=snapshot)
        except DatabaseDriverUnavailableError:
            return snapshot.model_copy(
                update={
                    "database": DatabaseOperationsView(
                        enabled=True,
                        state="driver_missing",
                        projected_at=None,
                        message=(
                            "PostgreSQL is configured but the psycopg driver is not installed. "
                            "Worker execution remains file-backed."
                        ),
                    )
                }
            )
        except DatabaseConnectionError:
            return snapshot.model_copy(
                update={
                    "database": DatabaseOperationsView(
                        enabled=True,
                        state="unavailable",
                        projected_at=None,
                        message=(
                            "PostgreSQL read model is unavailable. Worker execution remains "
                            "file-backed and continues independently."
                        ),
                    )
                }
            )

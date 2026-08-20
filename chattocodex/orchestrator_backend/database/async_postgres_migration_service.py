from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import Connection, MetaData, func, insert, inspect, select
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from chattocodex.orchestrator_backend.database.database_exceptions import (
    DatabaseConnectionError,
)
from chattocodex.orchestrator_backend.database.operations_read_model_models import (
    SchemaMigrationRecord,
)
from chattocodex.orchestrator_backend.database.orm_base import OrmBase
from chattocodex.orchestrator_backend.database.sqlalchemy_runtime import (
    normalize_async_postgres_url,
)
from chattocodex.orchestrator_backend.organization.group_folder_model import (
    GroupFolderModel,
)

_CURRENT_SCHEMA_VERSION = "0005_sqlalchemy_canonical_schema"
_MIGRATION_LOCK_ID = 721503673250208493
_LEGACY_READ_MODEL_TABLES = frozenset(
    {
        "operations_consumer_groups",
        "operations_broker_cluster",
        "operations_broker_nodes",
        "operations_group_coordinators",
    }
)


class AsyncPostgresMigrationService:
    """Reconcile PostgreSQL to the canonical SQLAlchemy-defined schema."""

    def __init__(
        self,
        database_url: str,
        connect_timeout_seconds: float = 3.0,
    ) -> None:
        """Configure one short-lived asynchronous schema reconciliation service.

        Args:
            database_url: PostgreSQL connection URL accepted by the application.
            connect_timeout_seconds: Maximum seconds for the migration connection.

        Raises:
            ValueError: If the connection timeout is not positive. Returns: None."""
        if connect_timeout_seconds <= 0:
            raise ValueError("connect_timeout_seconds must be positive")
        self._database_url = normalize_async_postgres_url(database_url)
        self._connect_timeout_seconds = connect_timeout_seconds

    async def migrate(self) -> tuple[str, ...]:
        """Create or reconcile the ORM schema inside one serialized transaction.

        Returns:
            The newly recorded canonical schema version, or an empty tuple when the
            database was already at the current version.

        Raises:
            DatabaseConnectionError: If PostgreSQL schema reconciliation fails. Args: None."""
        engine = create_async_engine(
            self._database_url,
            poolclass=NullPool,
            connect_args={"timeout": self._connect_timeout_seconds},
        )
        try:
            async with engine.begin() as connection:
                applied = await connection.run_sync(_reconcile_schema)
        except Exception as error:
            raise DatabaseConnectionError(
                "PostgreSQL migration failed; runtime file state was not modified."
            ) from error
        finally:
            await engine.dispose()
        return (_CURRENT_SCHEMA_VERSION,) if applied else ()


def _reconcile_schema(connection: Connection) -> bool:
    """Reconcile derived legacy tables and create all registered ORM tables.

    Args:
        connection: Synchronous SQLAlchemy connection supplied by ``run_sync``.

    Returns:
        ``True`` when the canonical schema version was newly recorded.
    """
    connection.execute(select(func.pg_advisory_xact_lock(_MIGRATION_LOCK_ID)))
    table_names = frozenset(inspect(connection).get_table_names())
    if table_names & _LEGACY_READ_MODEL_TABLES:
        _drop_legacy_operations_projection(connection, table_names=table_names)

    # Importing GroupFolderModel above deliberately registers the domain model on
    # the shared declarative metadata before create_all() executes.
    if GroupFolderModel.metadata is not OrmBase.metadata:
        raise RuntimeError("GroupFolder ORM metadata is not registered on OrmBase")
    OrmBase.metadata.create_all(connection)

    existing = connection.scalar(
        select(SchemaMigrationRecord.version).where(
            SchemaMigrationRecord.version == _CURRENT_SCHEMA_VERSION
        )
    )
    if existing is not None:
        return False
    connection.execute(
        insert(SchemaMigrationRecord).values(
            version=_CURRENT_SCHEMA_VERSION,
            applied_at=datetime.now(UTC),
        )
    )
    return True


def _drop_legacy_operations_projection(
    connection: Connection,
    table_names: frozenset[str],
) -> None:
    """Drop only rebuildable legacy operations read-model tables.

    Args:
        connection: Active migration transaction connection.
        table_names: Current PostgreSQL table-name snapshot.

    Returns:
        None. The reflected legacy projection tables are removed in dependency order.
    """
    operation_table_names = tuple(
        sorted(name for name in table_names if name.startswith("operations_"))
    )
    if not operation_table_names:
        return
    legacy_metadata = MetaData()
    legacy_metadata.reflect(bind=connection, only=operation_table_names)
    legacy_metadata.drop_all(bind=connection)

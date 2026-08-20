from __future__ import annotations

import asyncio
from datetime import UTC, datetime

from pydantic_core import to_json
from sqlalchemy import delete, literal, select
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from chattocodex.orchestrator_backend.database.async_postgres_migration_service import (
    AsyncPostgresMigrationService,
)
from chattocodex.orchestrator_backend.database.database_exceptions import (
    DatabaseConnectionError,
)
from chattocodex.orchestrator_backend.database.operations_read_model_models import (
    OperationsBrokerConsumerGroupModel,
    OperationsBrokerNamespaceModel,
    OperationsBrokerStateModel,
    OperationsProjectionModel,
    OperationsProjectModel,
    OperationsRunModel,
    OperationsRuntimeGroupModel,
    OperationsWorkerModel,
)
from chattocodex.orchestrator_backend.database.sqlalchemy_runtime import (
    normalize_async_postgres_url,
)
from chattocodex.shared.contracts.control_panel.control_panel_operations_schemas import (
    ControlPanelOperationsResponse,
    DatabaseOperationsState,
    DatabaseOperationsView,
)

_DEFAULT_RUNTIME_GROUP_KEY = "project_default"


class OperationsReadModelRepository:
    """Project runtime snapshots into PostgreSQL through SQLAlchemy asyncio APIs."""

    def __init__(
        self,
        database_url: str,
        pool_size: int = 4,
        max_overflow: int = 4,
        pool_timeout_seconds: float = 5.0,
    ) -> None:
        """Create a reusable asynchronous operations read-model repository.

        Args:
            database_url: PostgreSQL URL for the optional management read model.
            pool_size: Persistent SQLAlchemy connection-pool size.
            max_overflow: Temporary connections allowed above ``pool_size``.
            pool_timeout_seconds: Maximum seconds to wait for a pooled connection.

        Raises:
            ValueError: If pool limits are invalid. Returns: None."""
        if pool_size <= 0:
            raise ValueError("pool_size must be positive")
        if max_overflow < 0:
            raise ValueError("max_overflow must not be negative")
        if pool_timeout_seconds <= 0:
            raise ValueError("pool_timeout_seconds must be positive")
        normalized_url = normalize_async_postgres_url(database_url)
        self._engine: AsyncEngine = create_async_engine(
            normalized_url,
            pool_pre_ping=True,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_timeout=pool_timeout_seconds,
        )
        self._sessions = async_sessionmaker(
            bind=self._engine,
            class_=AsyncSession,
            expire_on_commit=False,
            autoflush=False,
        )
        self._migration_service = AsyncPostgresMigrationService(
            database_url=normalized_url,
            connect_timeout_seconds=pool_timeout_seconds,
        )
        self._migration_lock = asyncio.Lock()
        self._migrated = False

    async def project_and_reload(
        self,
        snapshot: ControlPanelOperationsResponse,
    ) -> ControlPanelOperationsResponse:
        """Persist a complete projection and read the canonical payload back.

        Args:
            snapshot: File-backed Control Panel snapshot to project.

        Returns:
            Strictly validated snapshot loaded from PostgreSQL JSONB state.

        Raises:
            DatabaseConnectionError: If migration, persistence, or reload fails.
        """
        await self._ensure_migrated()
        projected_at = datetime.now(UTC)
        ready_snapshot = snapshot.model_copy(
            update={
                "database": DatabaseOperationsView(
                    enabled=True,
                    state=DatabaseOperationsState.READY,
                    projected_at=projected_at,
                    message=None,
                )
            }
        )
        payload = ready_snapshot.model_dump(mode="json")
        try:
            async with self._sessions() as session:
                async with session.begin():
                    await self._replace_normalized_rows(
                        session,
                        ready_snapshot,
                        projected_at,
                    )
                    projection = await session.get(OperationsProjectionModel, 1)
                    if projection is None:
                        session.add(
                            OperationsProjectionModel(
                                singleton_id=1,
                                schema_version=1,
                                payload=payload,
                                projected_at=projected_at,
                            )
                        )
                    else:
                        projection.schema_version = 1
                        projection.payload = payload
                        projection.projected_at = projected_at
                stored_payload = await session.scalar(
                    select(OperationsProjectionModel.payload).where(
                        OperationsProjectionModel.singleton_id == 1
                    )
                )
        except Exception as error:
            raise DatabaseConnectionError(
                "PostgreSQL operations projection failed; runtime file state was not modified."
            ) from error
        if stored_payload is None:
            raise DatabaseConnectionError(
                "PostgreSQL operations projection did not return the persisted snapshot."
            )
        return ControlPanelOperationsResponse.model_validate_json(
            to_json(stored_payload)
        )

    async def healthcheck(self) -> None:
        """Verify that PostgreSQL can execute a minimal SQLAlchemy expression.

        Returns:
            None when the database returns the expected scalar value.

        Raises:
            DatabaseConnectionError: If the connection or scalar result is invalid. Args: None."""
        try:
            async with self._engine.connect() as connection:
                value = await connection.scalar(select(literal(1)))
            if not isinstance(value, int) or isinstance(value, bool) or value != 1:
                raise DatabaseConnectionError(
                    "PostgreSQL health check returned no result."
                )
        except DatabaseConnectionError:
            raise
        except Exception as error:
            raise DatabaseConnectionError("PostgreSQL health check failed.") from error

    async def close(self) -> None:
        """Dispose the repository-owned async SQLAlchemy connection pool.

        Returns:
            None after all pooled connections are released. Args: None."""
        await self._engine.dispose()

    async def _ensure_migrated(self) -> None:
        """Run schema reconciliation once for this repository lifetime.

        Returns:
            None after the canonical ORM schema is available. Args: None."""
        if self._migrated:
            return
        async with self._migration_lock:
            if self._migrated:
                return
            await self._migration_service.migrate()
            self._migrated = True

    @staticmethod
    async def _replace_normalized_rows(
        session: AsyncSession,
        snapshot: ControlPanelOperationsResponse,
        projected_at: datetime,
    ) -> None:
        """Replace normalized management rows inside the caller transaction.

        Args:
            session: Active SQLAlchemy session transaction.
            snapshot: Strict operations projection to normalize.
            projected_at: Shared projection timestamp for every generated row.

        Returns:
            None after pending ORM changes have been staged in the session.
        """
        for model in (
            OperationsBrokerConsumerGroupModel,
            OperationsBrokerNamespaceModel,
            OperationsBrokerStateModel,
            OperationsWorkerModel,
            OperationsRuntimeGroupModel,
            OperationsProjectModel,
            OperationsRunModel,
        ):
            await session.execute(delete(model))

        project_rows: list[OperationsProjectModel] = []
        runtime_group_rows: list[OperationsRuntimeGroupModel] = []
        worker_rows: list[OperationsWorkerModel] = []
        for project in snapshot.projects:
            binding = project.binding
            project_rows.append(
                OperationsProjectModel(
                    project_id=project.project_id,
                    name=project.name,
                    root=project.root,
                    provider=binding.provider if binding is not None else None,
                    provider_project_ref=binding.project_ref
                    if binding is not None
                    else None,
                    provider_project_label=binding.project_label
                    if binding is not None
                    else None,
                    provider_project_path=binding.project_path
                    if binding is not None
                    else None,
                    connector_ready=binding.connector_ready
                    if binding is not None
                    else False,
                    binding_revision=binding.revision if binding is not None else None,
                    desired_capacity=project.desired_capacity,
                    spawn_needed=project.spawn_needed,
                    provisioning_pending=project.provisioning_pending,
                    projected_at=projected_at,
                )
            )
            runtime_group = project.worker_runtime
            runtime_group_rows.append(
                OperationsRuntimeGroupModel(
                    project_id=project.project_id,
                    runtime_group_key=_DEFAULT_RUNTIME_GROUP_KEY,
                    initialized=runtime_group.initialized,
                    topology_generation=runtime_group.topology_generation,
                    revision=runtime_group.revision,
                    idle=runtime_group.idle,
                    waking=runtime_group.waking,
                    claiming=runtime_group.claiming,
                    busy=runtime_group.busy,
                    projected_at=projected_at,
                )
            )
            worker_rows.extend(
                OperationsWorkerModel(
                    wake_target_id=worker.wake_target_id,
                    project_id=project.project_id,
                    runtime_group_key=_DEFAULT_RUNTIME_GROUP_KEY,
                    status=worker.status.value,
                    wake_revision=worker.wake_revision,
                    worker_updated_at=worker.updated_at,
                    projected_at=projected_at,
                )
                for worker in runtime_group.workers
            )
        session.add_all(project_rows)
        session.add_all(runtime_group_rows)
        session.add_all(worker_rows)

        broker = snapshot.broker
        if broker is not None:
            session.add(
                OperationsBrokerStateModel(
                    singleton_id=1,
                    term=broker.term,
                    revision=broker.revision,
                    queued_count=broker.queued_count,
                    ready_count=broker.ready_count,
                    claimed_count=broker.claimed_count,
                    completed_count=broker.completed_count,
                    failed_count=broker.failed_count,
                    idle_slot_count=broker.idle_slot_count,
                    busy_slot_count=broker.busy_slot_count,
                    projected_at=projected_at,
                )
            )
            session.add_all(
                OperationsBrokerNamespaceModel(
                    project_id=namespace.project_id,
                    queued_count=namespace.queued_count,
                    ready_count=namespace.ready_count,
                    claimed_count=namespace.claimed_count,
                    completed_count=namespace.completed_count,
                    failed_count=namespace.failed_count,
                    idle_slot_count=namespace.idle_slot_count,
                    busy_slot_count=namespace.busy_slot_count,
                    projected_at=projected_at,
                )
                for namespace in broker.namespaces
            )
            session.add_all(
                OperationsBrokerConsumerGroupModel(
                    group_id=group.group_id,
                    namespace_id=group.namespace_id,
                    generation=group.generation,
                    group_revision=group.group_revision,
                    consumer_count=group.consumer_count,
                    projected_at=projected_at,
                )
                for group in broker.consumer_groups
            )

        session.add_all(
            OperationsRunModel(
                run_id=run.run_id,
                project_id=run.project_id,
                project_name=run.project_name,
                goal=run.goal,
                status=run.status.value,
                requested_parallelism=run.requested_parallelism,
                run_updated_at=run.updated_at,
                projected_at=projected_at,
            )
            for run in snapshot.recent_runs
        )

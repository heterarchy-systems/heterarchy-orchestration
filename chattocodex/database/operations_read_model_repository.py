from __future__ import annotations

import json
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from chattocodex.database.database_exceptions import DatabaseConnectionError
from chattocodex.database.database_migration_service import DatabaseMigrationService
from chattocodex.schemas.control_panel_operations_schemas import (
    ControlPanelOperationsResponse,
    DatabaseOperationsView,
)

_DEFAULT_RUNTIME_GROUP_KEY = "project_default"


class OperationsReadModelRepository:
    """Project the file-backed runtime snapshot into a PostgreSQL management read model."""

    def __init__(
        self,
        *,
        database_url: str,
        connect: Callable[..., Any],
    ) -> None:
        self._database_url = database_url
        self._connect = connect
        self._migrations = DatabaseMigrationService(
            database_url=database_url,
            connect=connect,
        )

    def project_and_reload(
        self,
        *,
        snapshot: ControlPanelOperationsResponse,
    ) -> ControlPanelOperationsResponse:
        """Write a complete read-model projection, then read it back from PostgreSQL."""

        self._migrations.migrate()
        projected_at = datetime.now(UTC)
        ready_snapshot = snapshot.model_copy(
            update={
                "database": DatabaseOperationsView(
                    enabled=True,
                    state="ready",
                    projected_at=projected_at,
                    message=None,
                )
            }
        )
        payload = json.dumps(
            ready_snapshot.model_dump(mode="json"),
            ensure_ascii=False,
            separators=(",", ":"),
        )
        try:
            with self._connect(self._database_url, connect_timeout=3) as connection:
                with connection.cursor() as cursor:
                    self._replace_normalized_rows(
                        cursor=cursor,
                        snapshot=ready_snapshot,
                        projected_at=projected_at,
                    )
                    cursor.execute(
                        """
                        INSERT INTO operations_projection(
                            singleton_id, schema_version, payload, projected_at
                        ) VALUES (1, 1, %s::jsonb, %s)
                        ON CONFLICT (singleton_id) DO UPDATE SET
                            schema_version = EXCLUDED.schema_version,
                            payload = EXCLUDED.payload,
                            projected_at = EXCLUDED.projected_at
                        """,
                        (payload, projected_at),
                    )
                    cursor.execute(
                        "SELECT payload FROM operations_projection WHERE singleton_id = 1"
                    )
                    row = cursor.fetchone()
                connection.commit()
        except Exception as error:
            raise DatabaseConnectionError(
                "PostgreSQL operations projection failed; runtime file state was not modified."
            ) from error
        if row is None:
            raise DatabaseConnectionError(
                "PostgreSQL operations projection did not return the persisted snapshot."
            )
        stored_payload = row[0]
        if isinstance(stored_payload, str):
            return ControlPanelOperationsResponse.model_validate_json(stored_payload)
        return ControlPanelOperationsResponse.model_validate(stored_payload)

    def healthcheck(self) -> None:
        try:
            with (
                self._connect(self._database_url, connect_timeout=3) as connection,
                connection.cursor() as cursor,
            ):
                cursor.execute("SELECT 1")
                row = cursor.fetchone()
            if row is None or int(row[0]) != 1:
                raise DatabaseConnectionError(
                    "PostgreSQL health check returned no result."
                )
        except DatabaseConnectionError:
            raise
        except Exception as error:
            raise DatabaseConnectionError("PostgreSQL health check failed.") from error

    @staticmethod
    def _replace_normalized_rows(
        *,
        cursor: Any,
        snapshot: ControlPanelOperationsResponse,
        projected_at: datetime,
    ) -> None:
        cursor.execute("DELETE FROM operations_broker_consumer_groups")
        cursor.execute("DELETE FROM operations_broker_namespaces")
        cursor.execute("DELETE FROM operations_broker_state")
        cursor.execute("DELETE FROM operations_workers")
        cursor.execute("DELETE FROM operations_runtime_groups")
        cursor.execute("DELETE FROM operations_projects")
        cursor.execute("DELETE FROM operations_runs")

        for project in snapshot.projects:
            binding = project.binding
            cursor.execute(
                """
                INSERT INTO operations_projects(
                    project_id, name, root, provider, provider_project_ref,
                    provider_project_label, provider_project_path, connector_ready,
                    binding_revision, desired_capacity, spawn_needed,
                    provisioning_pending, projected_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    project.project_id,
                    project.name,
                    project.root,
                    binding.provider if binding is not None else None,
                    binding.project_ref if binding is not None else None,
                    binding.project_label if binding is not None else None,
                    binding.project_path if binding is not None else None,
                    binding.connector_ready if binding is not None else False,
                    binding.revision if binding is not None else None,
                    project.desired_capacity,
                    project.spawn_needed,
                    project.provisioning_pending,
                    projected_at,
                ),
            )
            runtime_group = project.worker_runtime
            cursor.execute(
                """
                INSERT INTO operations_runtime_groups(
                    project_id, runtime_group_key, initialized, topology_generation, revision,
                    idle, waking, claiming, busy, projected_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    project.project_id,
                    _DEFAULT_RUNTIME_GROUP_KEY,
                    runtime_group.initialized,
                    runtime_group.topology_generation,
                    runtime_group.revision,
                    runtime_group.idle,
                    runtime_group.waking,
                    runtime_group.claiming,
                    runtime_group.busy,
                    projected_at,
                ),
            )
            for worker in runtime_group.workers:
                cursor.execute(
                    """
                    INSERT INTO operations_workers(
                        wake_target_id, project_id, runtime_group_key, status, wake_revision,
                        worker_updated_at, projected_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        worker.wake_target_id,
                        project.project_id,
                        _DEFAULT_RUNTIME_GROUP_KEY,
                        worker.status.value,
                        worker.wake_revision,
                        worker.updated_at,
                        projected_at,
                    ),
                )

        broker = snapshot.broker
        if broker is not None:
            cursor.execute(
                """
                INSERT INTO operations_broker_state(
                    singleton_id, term, revision, queued_count, ready_count,
                    claimed_count, completed_count,
                    failed_count, idle_slot_count, busy_slot_count, projected_at
                ) VALUES (1, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    broker.term,
                    broker.revision,
                    broker.queued_count,
                    broker.ready_count,
                    broker.claimed_count,
                    broker.completed_count,
                    broker.failed_count,
                    broker.idle_slot_count,
                    broker.busy_slot_count,
                    projected_at,
                ),
            )
            for namespace in broker.namespaces:
                cursor.execute(
                    """
                    INSERT INTO operations_broker_namespaces(
                        project_id, queued_count, ready_count, claimed_count,
                        completed_count, failed_count, idle_slot_count,
                        busy_slot_count, projected_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        namespace.project_id,
                        namespace.queued_count,
                        namespace.ready_count,
                        namespace.claimed_count,
                        namespace.completed_count,
                        namespace.failed_count,
                        namespace.idle_slot_count,
                        namespace.busy_slot_count,
                        projected_at,
                    ),
                )
            for group in broker.consumer_groups:
                cursor.execute(
                    """
                    INSERT INTO operations_broker_consumer_groups(
                        group_id, namespace_id, generation, group_revision,
                        consumer_count, projected_at
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (
                        group.group_id,
                        group.namespace_id,
                        group.generation,
                        group.group_revision,
                        group.consumer_count,
                        projected_at,
                    ),
                )

        for run in snapshot.recent_runs:
            cursor.execute(
                """
                INSERT INTO operations_runs(
                    run_id, project_id, project_name, goal, status,
                    requested_parallelism, run_updated_at, projected_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    run.run_id,
                    run.project_id,
                    run.project_name,
                    run.goal,
                    run.status.value,
                    run.requested_parallelism,
                    run.updated_at,
                    projected_at,
                ),
            )

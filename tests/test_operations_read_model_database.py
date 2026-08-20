from __future__ import annotations

import asyncio
from inspect import iscoroutinefunction

import pytest

from chattocodex.orchestrator_backend.control_panel.infrastructure import (
    control_panel_database_bridge,
)
from chattocodex.orchestrator_backend.control_panel.infrastructure.control_panel_database_bridge import (
    ControlPanelDatabaseBridge,
)
from chattocodex.orchestrator_backend.database.database_config import DatabaseConfig
from chattocodex.orchestrator_backend.database.database_exceptions import (
    DatabaseConnectionError,
)
from chattocodex.orchestrator_backend.database.operations_read_model_models import (
    OperationsBrokerStateModel,
    OperationsProjectionModel,
    OperationsProjectModel,
    OperationsRunModel,
    OperationsRuntimeGroupModel,
    OperationsWorkerModel,
)
from chattocodex.orchestrator_backend.database.operations_read_model_repository import (
    OperationsReadModelRepository,
)
from chattocodex.shared.contracts.broker.broker_management_schemas import (
    BrokerOperationsView,
)
from chattocodex.shared.contracts.control_panel.control_panel_operations_schemas import (
    ControlPanelOperationsResponse,
    DatabaseOperationsState,
    DatabaseOperationsView,
    OperationsTotalsView,
    WorkerBrowserOperationsView,
)


def _snapshot() -> ControlPanelOperationsResponse:
    """Build the minimal strict operations snapshot used by database boundary tests.

    Args:
        None.

    Returns:
        File-backed operations snapshot with an explicitly disabled database view.
    """
    return ControlPanelOperationsResponse(
        browser=WorkerBrowserOperationsView(
            running=False,
            mode=None,
            profile_initialized=False,
        ),
        database=DatabaseOperationsView(
            enabled=False,
            state=DatabaseOperationsState.DISABLED,
            projected_at=None,
            message="PostgreSQL read model is not configured.",
        ),
        broker=BrokerOperationsView(
            term=3,
            revision=9,
            namespaces=(),
            consumer_groups=(),
            queued_count=0,
            ready_count=0,
            claimed_count=0,
            completed_count=0,
            failed_count=0,
            idle_slot_count=0,
            busy_slot_count=0,
        ),
        totals=OperationsTotalsView(
            project_count=0,
            runtime_group_count=0,
            worker_count=0,
            idle=0,
            waking=0,
            claiming=0,
            busy=0,
            provisioning_pending=0,
            active_runs=0,
        ),
        projects=(),
        executions=(),
        recent_runs=(),
    )


def test_database_config_is_opt_in_and_does_not_expose_default_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify PostgreSQL remains opt-in and environment-backed.

    Args:
        monkeypatch: Pytest environment mutation helper.

    Returns:
        None.
    """
    for key in (
        "CHATTOCODEX_DATABASE_URL",
        "CHATTOCODEX_DATABASE_REQUIRED",
        "CHATTOCODEX_DATABASE_POOL_SIZE",
        "CHATTOCODEX_DATABASE_MAX_OVERFLOW",
        "CHATTOCODEX_DATABASE_POOL_TIMEOUT_SECONDS",
    ):
        monkeypatch.delenv(key, raising=False)

    disabled = DatabaseConfig()
    assert disabled.enabled is False
    assert disabled.url is None
    assert disabled.required is False

    monkeypatch.setenv(
        "CHATTOCODEX_DATABASE_URL",
        "postgresql://example.invalid/chattocodex",
    )
    monkeypatch.setenv("CHATTOCODEX_DATABASE_REQUIRED", "true")

    enabled = DatabaseConfig()
    assert enabled.enabled is True
    assert enabled.required is True


def test_database_bridge_keeps_file_snapshot_when_database_is_disabled() -> None:
    """Verify a disabled database adds no async I/O or snapshot copying.

    Args:
        None.

    Returns:
        None.
    """
    snapshot = _snapshot()
    bridge = ControlPanelDatabaseBridge(config=DatabaseConfig(url=None, required=False))
    result = asyncio.run(bridge.project_or_fallback(snapshot))

    assert result is snapshot
    assert result.database.state == DatabaseOperationsState.DISABLED


def test_database_bridge_reuses_async_repository_and_falls_back_on_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify one async repository instance is reused and safely disposed.

    Args:
        monkeypatch: Pytest module replacement helper.

    Returns:
        None.
    """

    class FakeOperationsReadModelRepository:
        """Record async repository lifecycle without opening PostgreSQL connections."""

        instances = 0
        calls = 0
        closes = 0

        def __init__(
            self,
            database_url: str,
            pool_size: int,
            max_overflow: int,
            pool_timeout_seconds: float,
        ) -> None:
            """Record construction parameters for one fake repository.

            Args:
                database_url: Configured database URL.
                pool_size: Configured persistent pool size.
                max_overflow: Configured overflow allowance.
                pool_timeout_seconds: Configured pool wait timeout.

            Returns:
                None.
            """
            assert database_url.startswith("postgresql://")
            assert pool_size > 0
            assert max_overflow >= 0
            assert pool_timeout_seconds > 0
            type(self).instances += 1

        async def project_and_reload(
            self,
            snapshot: ControlPanelOperationsResponse,
        ) -> ControlPanelOperationsResponse:
            """Simulate an unavailable async PostgreSQL projection.

            Args:
                snapshot: Snapshot supplied by the bridge.

            Returns:
                Never returns because the simulated database is unavailable.

            Raises:
                DatabaseConnectionError: Always, to exercise fallback behavior.
            """
            del snapshot
            type(self).calls += 1
            raise DatabaseConnectionError("unavailable")

        async def close(self) -> None:
            """Record async repository disposal.

            Args:
                None.

            Returns:
                None.
            """
            type(self).closes += 1

    monkeypatch.setattr(
        control_panel_database_bridge,
        "OperationsReadModelRepository",
        FakeOperationsReadModelRepository,
    )
    bridge = ControlPanelDatabaseBridge(
        config=DatabaseConfig(
            url="postgresql://example.invalid/chattocodex",
            required=False,
        )
    )

    first = asyncio.run(bridge.project_or_fallback(_snapshot()))
    second = asyncio.run(bridge.project_or_fallback(_snapshot()))
    asyncio.run(bridge.close())

    assert FakeOperationsReadModelRepository.instances == 1
    assert FakeOperationsReadModelRepository.calls == 2
    assert FakeOperationsReadModelRepository.closes == 1
    assert first.database.enabled is True
    assert first.database.state == DatabaseOperationsState.UNAVAILABLE
    assert second.database.state == DatabaseOperationsState.UNAVAILABLE


def test_operations_read_model_repository_is_async_sqlalchemy_only() -> None:
    """Guard the read model against regression to synchronous PostgreSQL adapters.

    Args:
        None.

    Returns:
        None.
    """
    assert iscoroutinefunction(OperationsReadModelRepository.project_and_reload)
    assert iscoroutinefunction(OperationsReadModelRepository.healthcheck)
    assert iscoroutinefunction(OperationsReadModelRepository.close)


def test_operations_read_model_orm_contains_only_current_tables() -> None:
    """Verify canonical ORM metadata represents the post-legacy read model.

    Args:
        None.

    Returns:
        None.
    """
    current_tables = {
        OperationsProjectionModel.__tablename__,
        OperationsProjectModel.__tablename__,
        OperationsRuntimeGroupModel.__tablename__,
        OperationsWorkerModel.__tablename__,
        OperationsRunModel.__tablename__,
        OperationsBrokerStateModel.__tablename__,
    }

    assert "operations_projection" in current_tables
    assert "operations_projects" in current_tables
    assert "operations_runtime_groups" in current_tables
    assert "operations_workers" in current_tables
    assert "operations_runs" in current_tables
    assert "operations_broker_state" in current_tables
    assert "operations_consumer_groups" not in current_tables
    assert "operations_broker_nodes" not in current_tables
    assert "operations_group_coordinators" not in current_tables

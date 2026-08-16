import json
from datetime import datetime

import pytest

from chattocodex.broker.broker_management_dto import (
    BrokerClusterTopology,
    BrokerNodeRole,
    BrokerNodeStatus,
)
from chattocodex.control_panel.control_panel_database_bridge import (
    ControlPanelDatabaseBridge,
)
from chattocodex.database.database_config import DatabaseConfig
from chattocodex.database.database_exceptions import DatabaseDriverUnavailableError
from chattocodex.database.operations_read_model_repository import (
    OperationsReadModelRepository,
)
from chattocodex.schemas.broker_management_schemas import (
    BrokerClusterOperationsView,
    BrokerNodeOperationsView,
)
from chattocodex.schemas.control_panel_operations_schemas import (
    ControlPanelOperationsResponse,
    DatabaseOperationsView,
    OperationsTotalsView,
    WorkerBrowserOperationsView,
)


def _snapshot() -> ControlPanelOperationsResponse:
    return ControlPanelOperationsResponse(
        browser=WorkerBrowserOperationsView(
            running=False,
            mode=None,
            profile_initialized=False,
        ),
        database=DatabaseOperationsView(
            enabled=False,
            state="disabled",
            projected_at=None,
            message="PostgreSQL read model is not configured.",
        ),
        broker=BrokerClusterOperationsView(
            cluster_id="local-broker-cluster",
            topology=BrokerClusterTopology.EMBEDDED_SINGLE_NODE,
            controller_node_id="broker-0",
            nodes=(
                BrokerNodeOperationsView(
                    node_id="broker-0",
                    roles=(BrokerNodeRole.BROKER, BrokerNodeRole.CONTROLLER),
                    status=BrokerNodeStatus.ONLINE,
                ),
            ),
            namespaces=(),
            group_coordinators=(),
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
            consumer_group_count=0,
            worker_count=0,
            idle=0,
            waking=0,
            claiming=0,
            busy=0,
            provisioning_pending=0,
            active_runs=0,
        ),
        projects=(),
        recent_runs=(),
    )


def test_database_config_is_opt_in_and_does_not_expose_default_credentials() -> None:
    disabled = DatabaseConfig.from_environment(environ={})
    assert disabled.enabled is False
    assert disabled.url is None
    assert disabled.required is False

    enabled = DatabaseConfig.from_environment(
        environ={
            "CHATTOCODEX_DATABASE_URL": "postgresql://example.invalid/chattocodex",
            "CHATTOCODEX_DATABASE_REQUIRED": "true",
        }
    )
    assert enabled.enabled is True
    assert enabled.required is True


def test_database_bridge_keeps_file_snapshot_when_database_is_disabled() -> None:
    snapshot = _snapshot()
    result = ControlPanelDatabaseBridge(
        config=DatabaseConfig(url=None, required=False)
    ).project_or_fallback(snapshot=snapshot)

    assert result is snapshot
    assert result.database.state == "disabled"


def test_database_bridge_reports_missing_driver_without_breaking_runtime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def missing_driver():
        raise DatabaseDriverUnavailableError("missing")

    monkeypatch.setattr(
        "chattocodex.control_panel.control_panel_database_bridge.load_postgres_connect",
        missing_driver,
    )
    result = ControlPanelDatabaseBridge(
        config=DatabaseConfig(
            url="postgresql://example.invalid/chattocodex",
            required=False,
        )
    ).project_or_fallback(snapshot=_snapshot())

    assert result.database.enabled is True
    assert result.database.state == "driver_missing"
    assert result.totals.worker_count == 0


class _FakeCursor:
    def __init__(self) -> None:
        self.queries: list[str] = []
        self.payload: str | None = None
        self._last_query = ""

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, query, params=None) -> None:
        text = str(query)
        self.queries.append(text)
        self._last_query = text
        if "INSERT INTO operations_projection" in text and params:
            self.payload = str(params[0])

    def fetchall(self):
        if "chattocodex_schema_migrations" in self._last_query:
            return []
        return []

    def fetchone(self):
        if "SELECT payload FROM operations_projection" in self._last_query:
            assert self.payload is not None
            return (json.loads(self.payload),)
        if self._last_query.strip() == "SELECT 1":
            return (1,)
        return None


class _FakeConnection:
    def __init__(self, cursor: _FakeCursor) -> None:
        self._cursor = cursor
        self.committed = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def cursor(self):
        return self._cursor

    def commit(self) -> None:
        self.committed = True


def test_postgres_read_model_projects_and_reads_back_snapshot() -> None:
    cursor = _FakeCursor()
    connection = _FakeConnection(cursor)

    def connect(_url: str, **_kwargs):
        return connection

    repository = OperationsReadModelRepository(
        database_url="postgresql://example.invalid/chattocodex",
        connect=connect,
    )
    result = repository.project_and_reload(snapshot=_snapshot())

    assert result.database.state == "ready"
    assert isinstance(result.database.projected_at, datetime)
    assert result.database.projected_at.tzinfo is not None
    assert connection.committed is True
    assert any("operations_projection" in query for query in cursor.queries)
    assert any("operations_projects" in query for query in cursor.queries)
    assert any("operations_consumer_groups" in query for query in cursor.queries)
    assert any("operations_runs" in query for query in cursor.queries)
    assert any("operations_broker_cluster" in query for query in cursor.queries)
    assert any("operations_broker_nodes" in query for query in cursor.queries)


def test_postgres_read_model_healthcheck_uses_select_one() -> None:
    cursor = _FakeCursor()
    connection = _FakeConnection(cursor)

    repository = OperationsReadModelRepository(
        database_url="postgresql://example.invalid/chattocodex",
        connect=lambda _url, **_kwargs: connection,
    )
    repository.healthcheck()

    assert "SELECT 1" in cursor.queries

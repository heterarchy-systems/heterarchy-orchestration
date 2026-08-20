from __future__ import annotations

import asyncio
from collections.abc import Callable
from types import TracebackType

import pytest
from sqlalchemy import Connection

from chattocodex.orchestrator_backend.database import async_postgres_migration_service
from chattocodex.orchestrator_backend.database.async_postgres_migration_service import (
    AsyncPostgresMigrationService,
)
from chattocodex.orchestrator_backend.database.database_exceptions import (
    DatabaseConnectionError,
)


class _FakeAsyncConnection:
    """Record SQLAlchemy run_sync migration callbacks without opening PostgreSQL."""

    def __init__(self, applied: bool) -> None:
        """Configure the fake callback result.

        Args:
            applied: Value returned by the simulated synchronous migration callback.

        Returns:
            None.
        """
        self._applied = applied
        self.callback_names: list[str] = []

    async def run_sync(self, callback: Callable[[Connection], bool]) -> bool:
        """Record the callback selected by the migration service.

        Args:
            callback: SQLAlchemy synchronous migration callback.

        Returns:
            Configured migration application result.
        """
        self.callback_names.append(callback.__name__)
        return self._applied


class _FakeBeginContext:
    """Provide the minimal async context-manager shape used by AsyncEngine.begin()."""

    def __init__(self, connection: _FakeAsyncConnection) -> None:
        """Store the fake migration connection.

        Args:
            connection: Fake asynchronous SQLAlchemy connection.

        Returns:
            None.
        """
        self._connection = connection

    async def __aenter__(self) -> _FakeAsyncConnection:
        """Enter the fake migration transaction.

        Args:
            None.

        Returns:
            Fake asynchronous migration connection.
        """
        return self._connection

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        """Exit the fake migration transaction without suppressing failures.

        Args:
            exc_type: Optional exception type raised in the transaction.
            exc_value: Optional exception instance raised in the transaction.
            traceback: Optional exception traceback.

        Returns:
            False so exceptions remain visible to the service.
        """
        del exc_type, exc_value, traceback
        return False


class _FakeAsyncEngine:
    """Expose begin/dispose behavior required by the migration service."""

    def __init__(self, applied: bool) -> None:
        """Create one fake migration engine.

        Args:
            applied: Result returned by the fake run_sync callback.

        Returns:
            None.
        """
        self.connection = _FakeAsyncConnection(applied=applied)
        self.disposed = False

    def begin(self) -> _FakeBeginContext:
        """Open the fake async migration transaction.

        Args:
            None.

        Returns:
            Async context manager wrapping the fake connection.
        """
        return _FakeBeginContext(self.connection)

    async def dispose(self) -> None:
        """Record deterministic migration-engine disposal.

        Args:
            None.

        Returns:
            None.
        """
        self.disposed = True


def test_async_migration_uses_python_schema_reconciliation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify migration executes the Python ORM reconciler instead of SQL files.

    Args:
        monkeypatch: Pytest replacement helper.

    Returns:
        None.
    """
    engine = _FakeAsyncEngine(applied=True)
    observed_urls: list[str] = []
    observed_connect_args: list[dict[str, float]] = []

    def fake_create_async_engine(
        database_url: str,
        *,
        poolclass: type[object],
        connect_args: dict[str, float],
    ) -> _FakeAsyncEngine:
        """Capture SQLAlchemy engine settings used by migration.

        Args:
            database_url: Normalized asynchronous PostgreSQL URL.
            poolclass: SQLAlchemy pool class selected by migration.
            connect_args: Driver connection arguments.

        Returns:
            Reusable fake async engine.
        """
        del poolclass
        observed_urls.append(database_url)
        observed_connect_args.append(connect_args)
        return engine

    monkeypatch.setattr(
        async_postgres_migration_service,
        "create_async_engine",
        fake_create_async_engine,
    )
    service = AsyncPostgresMigrationService(
        database_url="postgresql://user:password@localhost/db",
        connect_timeout_seconds=2.5,
    )

    applied = asyncio.run(service.migrate())

    assert len(applied) == 1
    assert engine.connection.callback_names == ["_reconcile_schema"]
    assert engine.disposed is True
    assert observed_urls == ["postgresql+asyncpg://user:password@localhost/db"]
    assert observed_connect_args == [{"timeout": 2.5}]


def test_async_migration_returns_empty_when_schema_version_is_current(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify an already-current ORM schema produces no new migration record.

    Args:
        monkeypatch: Pytest replacement helper.

    Returns:
        None.
    """
    engine = _FakeAsyncEngine(applied=False)
    monkeypatch.setattr(
        async_postgres_migration_service,
        "create_async_engine",
        lambda *args, **kwargs: engine,
    )

    result = asyncio.run(
        AsyncPostgresMigrationService(
            database_url="postgresql://user:password@localhost/db"
        ).migrate()
    )

    assert result == ()
    assert engine.disposed is True


def test_async_migration_maps_engine_failure_and_disposes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify migration errors become backend database errors and release resources.

    Args:
        monkeypatch: Pytest replacement helper.

    Returns:
        None.
    """

    class FailingEngine(_FakeAsyncEngine):
        """Raise while entering the migration transaction."""

        def begin(self) -> _FakeBeginContext:
            """Fail before a migration connection is yielded.

            Args:
                None.

            Returns:
                Never returns because the simulated database is unavailable.

            Raises:
                OSError: Always, to exercise migration error mapping.
            """
            raise OSError("database unavailable")

    engine = FailingEngine(applied=False)
    monkeypatch.setattr(
        async_postgres_migration_service,
        "create_async_engine",
        lambda *args, **kwargs: engine,
    )
    service = AsyncPostgresMigrationService(
        database_url="postgresql://user:password@localhost/db"
    )

    with pytest.raises(DatabaseConnectionError, match="migration failed"):
        asyncio.run(service.migrate())

    assert engine.disposed is True

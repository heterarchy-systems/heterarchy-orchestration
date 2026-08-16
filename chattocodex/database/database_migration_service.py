from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from chattocodex.database.database_exceptions import DatabaseConnectionError

_MIGRATION_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS chattocodex_schema_migrations (
    version TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL
)
"""


class DatabaseMigrationService:
    """Apply packaged SQL migrations transactionally in filename order."""

    def __init__(
        self,
        *,
        database_url: str,
        connect: Callable[..., Any],
        migrations_root: Path | None = None,
    ) -> None:
        self._database_url = database_url
        self._connect = connect
        self._migrations_root = migrations_root or (
            Path(__file__).resolve().parent / "migrations"
        )

    def migrate(self) -> tuple[str, ...]:
        applied: list[str] = []
        try:
            with self._connect(self._database_url, connect_timeout=3) as connection:
                with connection.cursor() as cursor:
                    cursor.execute(_MIGRATION_TABLE_SQL)
                    cursor.execute(
                        "SELECT version FROM chattocodex_schema_migrations ORDER BY version"
                    )
                    current = {str(row[0]) for row in cursor.fetchall()}
                    for path in sorted(self._migrations_root.glob("*.sql")):
                        version = path.stem
                        if version in current:
                            continue
                        sql = path.read_text(encoding="utf-8")
                        cursor.execute(sql)
                        cursor.execute(
                            "INSERT INTO chattocodex_schema_migrations(version, applied_at) "
                            "VALUES (%s, %s)",
                            (version, datetime.now(UTC)),
                        )
                        applied.append(version)
                connection.commit()
        except Exception as error:
            raise DatabaseConnectionError(
                "PostgreSQL migration failed; runtime file state was not modified."
            ) from error
        return tuple(applied)

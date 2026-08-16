from __future__ import annotations

import importlib
from collections.abc import Callable
from typing import Any

from chattocodex.database.database_exceptions import (
    DatabaseDriverUnavailableError,
)

PostgresConnect = Callable[..., Any]


def load_postgres_connect() -> PostgresConnect:
    """Load psycopg lazily so the file-backed runtime remains usable without DB extras."""

    try:
        module = importlib.import_module("psycopg")
    except ModuleNotFoundError as error:
        raise DatabaseDriverUnavailableError(
            "PostgreSQL support requires the optional psycopg driver."
        ) from error
    connect = getattr(module, "connect", None)
    if not callable(connect):
        raise DatabaseDriverUnavailableError(
            "The installed psycopg package does not expose a usable connect function."
        )
    return connect

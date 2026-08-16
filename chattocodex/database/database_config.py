from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass

_DATABASE_URL_ENV = "CHATTOCODEX_DATABASE_URL"
_DATABASE_REQUIRED_ENV = "CHATTOCODEX_DATABASE_REQUIRED"
_TRUE_VALUES = frozenset({"1", "true", "yes", "on"})


@dataclass(frozen=True, slots=True, kw_only=True)
class DatabaseConfig:
    """Process-local PostgreSQL configuration without exposing credentials to the UI."""

    url: str | None
    required: bool

    @property
    def enabled(self) -> bool:
        return self.url is not None

    @classmethod
    def from_environment(
        cls,
        *,
        environ: Mapping[str, str] | None = None,
    ) -> DatabaseConfig:
        values = os.environ if environ is None else environ
        raw_url = values.get(_DATABASE_URL_ENV, "").strip()
        raw_required = values.get(_DATABASE_REQUIRED_ENV, "").strip().lower()
        return cls(
            url=raw_url or None,
            required=raw_required in _TRUE_VALUES,
        )

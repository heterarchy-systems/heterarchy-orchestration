from __future__ import annotations

from typing import Annotated

from pydantic import StringConstraints
from pydantic_settings import BaseSettings, SettingsConfigDict

from chattocodex.shared.contracts.schema_base import described_field

DatabaseUrl = Annotated[
    str,
    StringConstraints(
        strict=True,
        strip_whitespace=True,
        min_length=1,
        max_length=4096,
    ),
]


class DatabaseConfig(BaseSettings):
    """Load process-local PostgreSQL settings through pydantic-settings."""

    model_config = SettingsConfigDict(
        env_prefix="CHATTOCODEX_DATABASE_",
        env_ignore_empty=True,
        extra="ignore",
        frozen=True,
        strict=True,
        validate_default=True,
    )

    url: Annotated[
        DatabaseUrl | None,
        described_field(
            description=(
                "Optional PostgreSQL connection URL. Credentials remain process-local and are "
                "never exposed through Control Panel response models."
            )
        ),
    ] = None
    required: Annotated[
        bool,
        described_field(
            description="Whether startup must fail when PostgreSQL is not configured or unavailable."
        ),
    ] = False
    pool_size: Annotated[
        int,
        described_field(
            description="Maximum number of persistent SQLAlchemy pool connections.",
            ge=1,
            le=256,
        ),
    ] = 10
    max_overflow: Annotated[
        int,
        described_field(
            description="Maximum number of temporary SQLAlchemy pool overflow connections.",
            ge=0,
            le=256,
        ),
    ] = 10
    pool_timeout_seconds: Annotated[
        float,
        described_field(
            description="Maximum seconds to wait for a pooled PostgreSQL connection.",
            gt=0.0,
            le=60.0,
        ),
    ] = 5.0

    @property
    def enabled(self) -> bool:
        """Return whether the PostgreSQL backend has an explicit connection URL. Args: None. Returns: bool."""

        return self.url is not None

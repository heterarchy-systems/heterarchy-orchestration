from pathlib import Path
from typing import Annotated

from pydantic import StringConstraints, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from chattocodex.shared.contracts.schema_base import described_field

Host = Annotated[
    str,
    StringConstraints(strict=True, strip_whitespace=True, min_length=1, max_length=255),
]
McpPath = Annotated[
    str,
    StringConstraints(strict=True, strip_whitespace=True, min_length=1, max_length=255),
]


class RuntimeSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="CHATTOCODEX_",
        env_ignore_empty=True,
        extra="ignore",
        frozen=True,
        strict=True,
        validate_default=True,
    )

    state_root: Path = Path.home() / ".chattocodex"
    workspace_root: Path = Path(".")
    chrome_binary: Path | None = None
    host: Host = "127.0.0.1"
    port: Annotated[
        int,
        described_field(
            description="TCP port used by the server endpoint for this runtime settings.",
            ge=1,
            le=65535,
        ),
    ] = 8765
    broker_host: Host = "127.0.0.1"
    broker_port: Annotated[
        int,
        described_field(
            description="TCP port used by the broker endpoint for this runtime settings.",
            ge=1,
            le=65535,
        ),
    ] = 8811
    broker_operations_port: Annotated[
        int,
        described_field(
            description="TCP port used by the broker operations endpoint for this runtime settings.",
            ge=1,
            le=65535,
        ),
    ] = 8812
    broker_cluster_auto_routing: bool = True
    broker_cluster_broker_ports: tuple[int, int, int] = (8811, 8812, 8813)
    broker_cluster_operations_ports: tuple[int, int, int] = (9811, 9812, 9813)
    broker_timeout_seconds: Annotated[
        float,
        described_field(
            description="Broker timeout seconds for this runtime settings.",
            gt=0.0,
            le=60.0,
        ),
    ] = 5.0
    broker_lease_duration_ms: Annotated[
        int,
        described_field(
            description="Broker lease duration duration in milliseconds for this runtime settings.",
            ge=10000,
            le=600000,
        ),
    ] = 90000
    mcp_path: McpPath = "/mcp"
    oauth_access_token_ttl_seconds: Annotated[
        int,
        described_field(
            description="Oauth access token ttl seconds for this runtime settings.",
            ge=60,
        ),
    ] = 3600
    oauth_refresh_token_ttl_seconds: Annotated[
        int,
        described_field(
            description="Oauth refresh token ttl seconds for this runtime settings.",
            ge=3600,
        ),
    ] = 2592000
    oauth_allowed_redirect_hosts: tuple[Host, ...] = (
        "chatgpt.com",
        "chat.openai.com",
        "localhost",
        "127.0.0.1",
    )

    @field_validator("broker_cluster_broker_ports", "broker_cluster_operations_ports")
    @classmethod
    def require_unique_cluster_ports(
        cls, value: tuple[int, int, int]
    ) -> tuple[int, int, int]:
        """Require unique cluster ports. Args: value. Returns: tuple[int, int, int]."""
        if len(set(value)) != 3:
            raise ValueError("Broker cluster ports must be unique")
        if any(port < 1 or port > 65_535 for port in value):
            raise ValueError("Broker cluster ports must be in 1..=65535")
        return value

    @field_validator("mcp_path")
    @classmethod
    def require_absolute_mcp_path(cls, value: str) -> str:
        """Require absolute mcp path. Args: value. Returns: str."""
        if not value.startswith("/"):
            raise ValueError("mcp_path must start with '/'.")
        return value

from pathlib import Path
from typing import Annotated

from pydantic import Field, StringConstraints, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

Host = Annotated[
    str, StringConstraints(strip_whitespace=True, min_length=1, max_length=255)
]
McpPath = Annotated[
    str, StringConstraints(strip_whitespace=True, min_length=1, max_length=255)
]


class RuntimeSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="CHATTOCODEX_",
        extra="ignore",
        frozen=True,
        validate_default=True,
    )

    state_root: Path = Path.home() / ".chattocodex"
    workspace_root: Path = Path(".")
    host: Host = "127.0.0.1"
    port: int = Field(default=8765, ge=1, le=65_535)
    mcp_path: McpPath = "/mcp"
    oauth_access_token_ttl_seconds: int = Field(default=3600, ge=60)
    oauth_refresh_token_ttl_seconds: int = Field(default=2_592_000, ge=3600)
    oauth_allowed_redirect_hosts: tuple[str, ...] = (
        "chatgpt.com",
        "chat.openai.com",
        "localhost",
        "127.0.0.1",
    )

    @field_validator("mcp_path")
    @classmethod
    def require_absolute_mcp_path(cls, value: str) -> str:
        if not value.startswith("/"):
            raise ValueError("mcp_path must start with '/'.")
        return value

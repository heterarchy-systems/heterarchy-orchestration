from pathlib import Path
from typing import Literal, Self
from urllib.parse import urlparse

from pydantic import (
    AnyHttpUrl,
    BaseModel,
    ConfigDict,
    DirectoryPath,
    field_validator,
    model_validator,
)


class SetupBoundaryModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        validate_default=True,
    )


class SetupRequest(SetupBoundaryModel):
    workspace_root: DirectoryPath
    public_url: AnyHttpUrl | None
    oauth_enabled: bool = True

    @field_validator("public_url")
    @classmethod
    def require_origin_url(cls, value: AnyHttpUrl | None) -> AnyHttpUrl | None:
        if value is None:
            return None
        parsed = urlparse(str(value))
        if parsed.query or parsed.fragment:
            raise ValueError(
                "public_url must not contain query or fragment components."
            )
        if parsed.path not in {"", "/"}:
            raise ValueError("public_url must be an origin without a path.")
        return value

    @model_validator(mode="after")
    def require_remote_oauth_origin(self) -> Self:
        if self.oauth_enabled and self.public_url is None:
            raise ValueError("OAuth-enabled setup requires public_url.")
        if self.oauth_enabled and self.public_url is not None:
            parsed = urlparse(str(self.public_url))
            is_localhost = parsed.hostname in {"localhost", "127.0.0.1", "::1"}
            if parsed.scheme != "https" and not is_localhost:
                raise ValueError("Remote OAuth public_url must use https.")
        return self


class SetupOutput(SetupBoundaryModel):
    ok: Literal[True] = True
    state_root: Path
    workspace_root: Path
    public_url: str | None
    connector_url: str | None
    project_count: int
    owner_token_created: bool
    owner_token: str | None


class DoctorOutput(SetupBoundaryModel):
    ok: bool
    state_root: Path
    configured: bool
    workspace_root: Path | None
    public_url: str | None
    connector_url: str | None
    oauth_enabled: bool
    owner_token_configured: bool
    project_count: int
    issues: tuple[str, ...]

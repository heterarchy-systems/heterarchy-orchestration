from pathlib import Path
from typing import Annotated, Literal, Self
from urllib.parse import urlparse

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, model_validator

from chattocodex.control_panel.control_panel_dto import RemoteMode

WorkspacePathText = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=4096),
]
OriginText = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=2048),
]
OwnerTokenText = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=20, max_length=1024),
]
LogLine = Annotated[str, StringConstraints(max_length=4000)]


class ControlPanelBoundaryModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        validate_default=True,
    )


class ControlPanelPreferencesRequest(ControlPanelBoundaryModel):
    workspace_root: WorkspacePathText
    remote_mode: RemoteMode
    custom_origin: OriginText | None = None
    mcp_port: int = Field(default=8765, ge=1024, le=65_535)

    @model_validator(mode="after")
    def validate_remote_mode(self) -> Self:
        if self.remote_mode is RemoteMode.CUSTOM_DOMAIN and self.custom_origin is None:
            raise ValueError("custom_origin is required for custom_domain mode")
        if (
            self.remote_mode is RemoteMode.QUICK_TUNNEL
            and self.custom_origin is not None
        ):
            raise ValueError("custom_origin must be omitted for quick_tunnel mode")
        return self


class ControlPanelPreferencesRecord(ControlPanelBoundaryModel):
    schema_version: Literal[1] = 1
    workspace_root: WorkspacePathText
    remote_mode: RemoteMode
    custom_origin: OriginText | None
    mcp_port: int = Field(ge=1024, le=65_535)


class ProcessStatusView(ControlPanelBoundaryModel):
    running: bool
    pid: int | None
    log_lines: tuple[LogLine, ...]


class ControlPanelPreferencesView(ControlPanelBoundaryModel):
    workspace_root: WorkspacePathText
    remote_mode: RemoteMode
    custom_origin: OriginText | None
    mcp_port: int


class ControlPanelStatusResponse(ControlPanelBoundaryModel):
    ok: Literal[True] = True
    configured: bool
    preferences: ControlPanelPreferencesView | None
    project_count: int = Field(ge=0)
    owner_token_configured: bool
    visible_owner_token: OwnerTokenText | None
    public_url: str | None
    connector_url: str | None
    mcp_process: ProcessStatusView
    tunnel_process: ProcessStatusView


class FolderPickerResponse(ControlPanelBoundaryModel):
    ok: Literal[True] = True
    selected_path: WorkspacePathText | None


class ControlPanelErrorResponse(ControlPanelBoundaryModel):
    ok: Literal[False] = False
    error_code: str
    message: str
    recoverable: bool


class OwnerTokenResponse(ControlPanelBoundaryModel):
    ok: Literal[True] = True
    owner_token: OwnerTokenText


def normalize_custom_origin(*, value: str) -> str:
    candidate = value.strip().rstrip("/")
    if "://" not in candidate:
        candidate = f"https://{candidate}"
    parsed = urlparse(candidate)
    if parsed.scheme != "https" or parsed.hostname is None:
        raise ValueError("Custom domain must be a valid HTTPS origin.")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("Custom domain must not include user credentials.")
    if parsed.path not in {"", "/"} or parsed.params or parsed.query or parsed.fragment:
        raise ValueError(
            "Custom domain must be an origin without path, query, or fragment."
        )
    if parsed.port is not None and parsed.port != 443:
        raise ValueError("Custom domain only supports the standard HTTPS port 443.")
    return f"https://{parsed.hostname}"


def normalize_workspace_path(*, value: str) -> Path:
    return Path(value).expanduser().resolve()

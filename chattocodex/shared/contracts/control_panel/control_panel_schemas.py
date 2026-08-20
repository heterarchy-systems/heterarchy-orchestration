from pathlib import Path
from typing import Annotated, Literal, Self
from urllib.parse import urlparse

from pydantic import model_validator

from chattocodex.shared.contracts.schema_base import StrictSchemaModel, described_field
from chattocodex.shared.contracts.types.control_plane.control_panel_types import (
    LogLine,
    OriginText,
    OwnerTokenText,
    WorkspacePathText,
)
from chattocodex.shared.element_collect.control_panel_enums import RemoteMode


class ControlPanelBoundaryModel(StrictSchemaModel):
    pass


class ControlPanelPreferencesRequest(ControlPanelBoundaryModel):
    workspace_root: WorkspacePathText
    remote_mode: RemoteMode
    custom_origin: OriginText | None = None
    mcp_port: Annotated[
        int,
        described_field(
            description="TCP port used by the MCP endpoint for this control panel preferences request.",
            ge=1024,
            le=65535,
        ),
    ] = 8765

    @model_validator(mode="after")
    def validate_remote_mode(self) -> Self:
        """Validate remote mode. Args: None. Returns: Self."""
        remote_mode = RemoteMode(self.remote_mode)
        if remote_mode is RemoteMode.CUSTOM_DOMAIN and self.custom_origin is None:
            raise ValueError("custom_origin is required for custom_domain mode")
        if remote_mode is RemoteMode.QUICK_TUNNEL and self.custom_origin is not None:
            raise ValueError("custom_origin must be omitted for quick_tunnel mode")
        return self


class ControlPanelPreferencesRecord(ControlPanelBoundaryModel):
    schema_version: Literal[1] = 1
    workspace_root: WorkspacePathText
    remote_mode: RemoteMode
    custom_origin: OriginText | None
    mcp_port: Annotated[
        int,
        described_field(
            description="TCP port used by the MCP endpoint for this control panel preferences record.",
            ge=1024,
            le=65535,
        ),
    ]


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
    project_count: Annotated[
        int,
        described_field(
            description="Number of project entries represented by this control panel status response.",
            ge=0,
        ),
    ]
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


def normalize_custom_origin(value: str) -> str:
    """Normalize custom origin. Args: value. Returns: str."""
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


def normalize_workspace_path(value: str) -> Path:
    """Normalize workspace path. Args: value. Returns: Path."""
    return Path(value).expanduser().resolve()

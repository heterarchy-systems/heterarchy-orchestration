from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path


class RemoteMode(StrEnum):
    QUICK_TUNNEL = "quick_tunnel"
    CUSTOM_DOMAIN = "custom_domain"


@dataclass(frozen=True, slots=True, kw_only=True)
class ControlPanelPreferences:
    workspace_root: Path
    remote_mode: RemoteMode
    custom_origin: str | None
    mcp_port: int


@dataclass(frozen=True, slots=True, kw_only=True)
class ProcessSnapshot:
    running: bool
    pid: int | None
    log_lines: tuple[str, ...]


@dataclass(frozen=True, slots=True, kw_only=True)
class ControlPanelSnapshot:
    configured: bool
    preferences: ControlPanelPreferences | None
    project_count: int
    owner_token_configured: bool
    visible_owner_token: str | None
    public_url: str | None
    connector_url: str | None
    mcp_process: ProcessSnapshot
    tunnel_process: ProcessSnapshot

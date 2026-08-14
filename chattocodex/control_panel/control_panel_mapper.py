from chattocodex.control_panel.control_panel_dto import (
    ControlPanelPreferences,
    ControlPanelSnapshot,
    ProcessSnapshot,
)
from chattocodex.schemas.control_panel_schemas import (
    ControlPanelPreferencesView,
    ControlPanelStatusResponse,
    ProcessStatusView,
)


def build_control_panel_status(
    *, snapshot: ControlPanelSnapshot
) -> ControlPanelStatusResponse:
    return ControlPanelStatusResponse(
        configured=snapshot.configured,
        preferences=(
            _build_preferences_view(preferences=snapshot.preferences)
            if snapshot.preferences is not None
            else None
        ),
        project_count=snapshot.project_count,
        owner_token_configured=snapshot.owner_token_configured,
        visible_owner_token=snapshot.visible_owner_token,
        public_url=snapshot.public_url,
        connector_url=snapshot.connector_url,
        mcp_process=_build_process_view(snapshot=snapshot.mcp_process),
        tunnel_process=_build_process_view(snapshot=snapshot.tunnel_process),
    )


def _build_preferences_view(
    *, preferences: ControlPanelPreferences
) -> ControlPanelPreferencesView:
    return ControlPanelPreferencesView(
        workspace_root=str(preferences.workspace_root),
        remote_mode=preferences.remote_mode,
        custom_origin=preferences.custom_origin,
        mcp_port=preferences.mcp_port,
    )


def _build_process_view(*, snapshot: ProcessSnapshot) -> ProcessStatusView:
    return ProcessStatusView(
        running=snapshot.running,
        pid=snapshot.pid,
        log_lines=snapshot.log_lines,
    )

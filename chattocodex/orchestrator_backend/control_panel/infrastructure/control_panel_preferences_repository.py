from pathlib import Path

from chattocodex.orchestrator_backend.control_panel.domain.control_panel_dto import (
    ControlPanelPreferences,
    RemoteMode,
)
from chattocodex.shared.contracts.control_panel.control_panel_schemas import (
    ControlPanelPreferencesRecord,
)
from chattocodex.shared.storage.atomic_json_file import (
    read_json_model,
    write_json_model,
)


class ControlPanelPreferencesRepository:
    def __init__(self, state_root: Path) -> None:
        """Initialize this object. Args: state_root. Returns: None."""
        self._path = state_root.expanduser().resolve() / "control-panel.json"

    def exists(self) -> bool:
        """Exists. Args: None. Returns: bool."""
        return self._path.is_file()

    def read(self) -> ControlPanelPreferences:
        """Read. Args: None. Returns: ControlPanelPreferences."""
        record = read_json_model(
            path=self._path,
            record_type=ControlPanelPreferencesRecord,
        )
        return ControlPanelPreferences(
            workspace_root=Path(record.workspace_root),
            remote_mode=RemoteMode(record.remote_mode),
            custom_origin=record.custom_origin,
            mcp_port=record.mcp_port,
        )

    def save(self, preferences: ControlPanelPreferences) -> ControlPanelPreferences:
        """Save. Args: preferences. Returns: ControlPanelPreferences."""
        record = ControlPanelPreferencesRecord(
            workspace_root=str(preferences.workspace_root),
            remote_mode=preferences.remote_mode,
            custom_origin=preferences.custom_origin,
            mcp_port=preferences.mcp_port,
        )
        write_json_model(path=self._path, record=record)
        return self.read()

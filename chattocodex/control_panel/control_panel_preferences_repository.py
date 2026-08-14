from pathlib import Path

from chattocodex.control_panel.control_panel_dto import ControlPanelPreferences
from chattocodex.persistence.atomic_json_file import read_json_model, write_json_model
from chattocodex.schemas.control_panel_schemas import ControlPanelPreferencesRecord


class ControlPanelPreferencesRepository:
    def __init__(self, *, state_root: Path) -> None:
        self._path = state_root.expanduser().resolve() / "control-panel.json"

    def exists(self) -> bool:
        return self._path.is_file()

    def read(self) -> ControlPanelPreferences:
        record = read_json_model(
            path=self._path,
            record_type=ControlPanelPreferencesRecord,
        )
        return ControlPanelPreferences(
            workspace_root=Path(record.workspace_root),
            remote_mode=record.remote_mode,
            custom_origin=record.custom_origin,
            mcp_port=record.mcp_port,
        )

    def save(self, *, preferences: ControlPanelPreferences) -> ControlPanelPreferences:
        record = ControlPanelPreferencesRecord(
            workspace_root=str(preferences.workspace_root),
            remote_mode=preferences.remote_mode,
            custom_origin=preferences.custom_origin,
            mcp_port=preferences.mcp_port,
        )
        write_json_model(path=self._path, record=record)
        return self.read()

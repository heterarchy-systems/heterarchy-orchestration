from pathlib import Path

from chattocodex.shared.contracts.auth_schemas import RuntimeConfigRecord
from chattocodex.shared.storage.atomic_json_file import (
    read_json_model,
    write_json_model,
)


class RuntimeConfigRepository:
    def __init__(self, state_root: Path) -> None:
        """Initialize this object. Args: state_root. Returns: None."""
        self._path = state_root / "config.json"

    def save(self, record: RuntimeConfigRecord) -> RuntimeConfigRecord:
        """Save. Args: record. Returns: RuntimeConfigRecord."""
        write_json_model(path=self._path, record=record)
        return read_json_model(path=self._path, record_type=RuntimeConfigRecord)

    def read(self) -> RuntimeConfigRecord:
        """Read. Args: None. Returns: RuntimeConfigRecord."""
        return read_json_model(path=self._path, record_type=RuntimeConfigRecord)

    def exists(self) -> bool:
        """Exists. Args: None. Returns: bool."""
        return self._path.is_file()

from pathlib import Path

from chattocodex.persistence.atomic_json_file import read_json_model, write_json_model
from chattocodex.schemas.auth_schemas import RuntimeConfigRecord


class RuntimeConfigRepository:
    def __init__(self, *, state_root: Path) -> None:
        self._path = state_root / "config.json"

    def save(self, *, record: RuntimeConfigRecord) -> RuntimeConfigRecord:
        write_json_model(path=self._path, record=record)
        return read_json_model(path=self._path, record_type=RuntimeConfigRecord)

    def read(self) -> RuntimeConfigRecord:
        return read_json_model(path=self._path, record_type=RuntimeConfigRecord)

    def exists(self) -> bool:
        return self._path.is_file()

from pathlib import Path

import pytest

import chattocodex.shared.storage.atomic_json_file as atomic_json_file
from chattocodex.shared.contracts.auth_schemas import RuntimeConfigRecord
from chattocodex.shared.storage.atomic_json_file import (
    AtomicJsonFileNotFoundError,
    delete_json_file,
    read_json_model,
)


def test_delete_json_file_flushes_parent_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "state" / "record.json"
    path.parent.mkdir()
    path.write_text("{}", encoding="utf-8")
    flushed: list[Path] = []
    monkeypatch.setattr(
        atomic_json_file,
        "_fsync_directory",
        lambda *, path: flushed.append(path),
    )

    assert delete_json_file(path=path, missing_ok=False)
    assert not path.exists()
    assert flushed == [path.parent]


def test_delete_json_file_has_explicit_missing_semantics(tmp_path: Path) -> None:
    path = tmp_path / "missing.json"

    assert not delete_json_file(path=path, missing_ok=True)
    with pytest.raises(AtomicJsonFileNotFoundError):
        delete_json_file(path=path, missing_ok=False)


def test_read_json_model_has_typed_missing_semantics(tmp_path: Path) -> None:
    with pytest.raises(AtomicJsonFileNotFoundError):
        read_json_model(
            path=tmp_path / "missing.json",
            record_type=RuntimeConfigRecord,
        )

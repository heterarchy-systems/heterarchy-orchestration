from pathlib import Path

import pytest

from chattocodex.persistence import atomic_json_file
from chattocodex.persistence.atomic_json_file import (
    AtomicJsonFileError,
    delete_json_file,
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
    with pytest.raises(AtomicJsonFileError):
        delete_json_file(path=path, missing_ok=False)

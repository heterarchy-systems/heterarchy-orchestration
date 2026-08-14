import os
from pathlib import Path
from uuid import uuid4

from pydantic import BaseModel, ValidationError


class AtomicJsonFileError(RuntimeError):
    pass


def write_json_model(*, path: Path, record: BaseModel) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = record.model_dump_json(exclude_none=False).encode("utf-8")
    temporary_path = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    file_descriptor = os.open(
        temporary_path,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL,
        0o600,
    )
    try:
        with os.fdopen(file_descriptor, "wb") as file_handle:
            file_handle.write(payload)
            file_handle.flush()
            os.fsync(file_handle.fileno())
        os.replace(temporary_path, path)
        _fsync_directory(path=path.parent)
    except OSError as error:
        raise AtomicJsonFileError(f"Failed to atomically write {path}.") from error
    finally:
        temporary_path.unlink(missing_ok=True)


def read_json_model[RecordT: BaseModel](
    *, path: Path, record_type: type[RecordT]
) -> RecordT:
    try:
        payload = path.read_bytes()
    except OSError as error:
        raise AtomicJsonFileError(f"Failed to read {path}.") from error
    try:
        return record_type.model_validate_json(payload)
    except ValidationError as error:
        raise AtomicJsonFileError(f"Invalid persisted JSON in {path}.") from error


def delete_json_file(*, path: Path, missing_ok: bool) -> bool:
    """Delete one durable JSON file and flush its directory entry.

    Args:
        path: Exact durable JSON path owned by the calling repository.
        missing_ok: Whether an already-absent record is an idempotent success.

    Returns:
        True when a file was removed; otherwise False for an allowed absence.

    Raises:
        AtomicJsonFileError: If deletion or directory synchronization fails.
    """
    try:
        path.unlink()
    except FileNotFoundError as error:
        if missing_ok:
            return False
        raise AtomicJsonFileError(
            f"Durable JSON file {path} does not exist."
        ) from error
    except OSError as error:
        raise AtomicJsonFileError(
            f"Failed to delete durable JSON file {path}."
        ) from error
    try:
        _fsync_directory(path=path.parent)
    except OSError as error:
        raise AtomicJsonFileError(
            f"Failed to flush deletion of durable JSON file {path}."
        ) from error
    return True


def _fsync_directory(*, path: Path) -> None:
    directory_flag = getattr(os, "O_DIRECTORY", 0)
    directory_descriptor = os.open(path, os.O_RDONLY | directory_flag)
    try:
        os.fsync(directory_descriptor)
    finally:
        os.close(directory_descriptor)

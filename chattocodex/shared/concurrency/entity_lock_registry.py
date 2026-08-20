import fcntl
import hashlib
import os
import stat
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from threading import Lock


class EntityLockNamespaceError(RuntimeError):
    """Raised when a configured cross-process lock namespace is no longer safe."""


@dataclass
class _LockEntry:
    lock: Lock
    users: int = 0


class EntityLockRegistry:
    """Own entity locks within a runtime and, optionally, across processes."""

    def __init__(self, lock_directory: Path | None = None) -> None:
        """Initialize this object. Args: lock_directory. Returns: None."""
        self._registry_lock = Lock()
        self._entity_locks: dict[str, _LockEntry] = {}
        self._lock_directory: Path | None = None
        self._namespace_descriptor: int | None = None
        self._namespace_identity: tuple[int, int] | None = None
        if lock_directory is not None:
            self._initialize_namespace(lock_directory=lock_directory)

    @property
    def active_entry_count(self) -> int:
        """Return the current number of retained in-process lock entries. Args: None. Returns: int."""

        with self._registry_lock:
            return len(self._entity_locks)

    @contextmanager
    def hold(self, entity_key: str) -> Iterator[None]:
        """Hold. Args: entity_key. Returns: Iterator[None]."""
        entry = self._retain_lock(entity_key=entity_key)
        entry.lock.acquire()
        lock_file_descriptor: int | None = None
        try:
            if self._lock_directory is not None:
                lock_file_descriptor = self._acquire_file_lock(entity_key=entity_key)
            yield
        finally:
            try:
                if lock_file_descriptor is not None:
                    try:
                        fcntl.flock(lock_file_descriptor, fcntl.LOCK_UN)
                    finally:
                        os.close(lock_file_descriptor)
            finally:
                entry.lock.release()
                self._release_lock(entity_key=entity_key, entry=entry)

    def _retain_lock(self, entity_key: str) -> _LockEntry:
        """Retain lock. Args: entity_key. Returns: _LockEntry."""
        with self._registry_lock:
            entry = self._entity_locks.get(entity_key)
            if entry is None:
                entry = _LockEntry(lock=Lock())
                self._entity_locks[entity_key] = entry
            entry.users += 1
            return entry

    def _release_lock(self, entity_key: str, entry: _LockEntry) -> None:
        """Release lock. Args: entity_key, entry. Returns: None."""
        with self._registry_lock:
            entry.users -= 1
            if entry.users == 0 and self._entity_locks.get(entity_key) is entry:
                self._entity_locks.pop(entity_key, None)

    def _initialize_namespace(self, lock_directory: Path) -> None:
        """Initialize namespace. Args: lock_directory. Returns: None."""
        expanded = lock_directory.expanduser()
        directory = expanded.parent.resolve() / expanded.name
        directory.parent.mkdir(parents=True, exist_ok=True)
        guardian_path = directory.parent / f".{directory.name}.guardian"
        guardian_descriptor = _open_guardian(path=guardian_path)
        namespace_descriptor: int | None = None
        try:
            fcntl.flock(guardian_descriptor, fcntl.LOCK_EX)
            expected_identity = _read_guardian_identity(
                descriptor=guardian_descriptor,
                path=guardian_path,
            )
            if expected_identity is None:
                try:
                    directory.mkdir(mode=0o700, exist_ok=True)
                except OSError as error:
                    raise EntityLockNamespaceError(
                        f"Lock namespace could not be created: {directory}."
                    ) from error
            namespace_descriptor = _open_namespace(path=directory)
            actual_identity = _directory_identity(descriptor=namespace_descriptor)
            if expected_identity is not None and actual_identity != expected_identity:
                raise EntityLockNamespaceError(
                    f"Lock namespace identity was replaced: {directory}."
                )
            _require_current_namespace(
                path=directory,
                expected_identity=actual_identity,
            )
            if expected_identity is None:
                _write_guardian_identity(
                    descriptor=guardian_descriptor,
                    identity=actual_identity,
                )
            _require_current_namespace(
                path=directory,
                expected_identity=actual_identity,
            )
        except BaseException:
            if namespace_descriptor is not None:
                os.close(namespace_descriptor)
            raise
        finally:
            try:
                fcntl.flock(guardian_descriptor, fcntl.LOCK_UN)
            finally:
                os.close(guardian_descriptor)

        self._lock_directory = directory
        self._namespace_descriptor = namespace_descriptor
        self._namespace_identity = actual_identity

    def _acquire_file_lock(self, entity_key: str) -> int:
        """Acquire file lock. Args: entity_key. Returns: int."""
        if (
            self._lock_directory is None
            or self._namespace_descriptor is None
            or self._namespace_identity is None
        ):
            raise RuntimeError("Cross-process locking is not configured.")
        self._verify_namespace()
        digest = hashlib.sha256(entity_key.encode("utf-8")).hexdigest()
        lock_path = self._lock_directory / f"{digest}.lock"
        flags = os.O_CREAT | os.O_RDWR | getattr(os, "O_CLOEXEC", 0)
        flags |= getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(lock_path, flags, 0o600)
        except OSError as error:
            raise EntityLockNamespaceError(
                f"Lock file could not be opened in the configured namespace: {lock_path}."
            ) from error
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            self._verify_namespace()
        except BaseException:
            os.close(descriptor)
            raise
        return descriptor

    def _verify_namespace(self) -> None:
        """Verify namespace. Args: None. Returns: None."""
        if (
            self._lock_directory is None
            or self._namespace_descriptor is None
            or self._namespace_identity is None
        ):
            raise RuntimeError("Cross-process locking is not configured.")
        held_identity = _directory_identity(descriptor=self._namespace_descriptor)
        if held_identity != self._namespace_identity:
            raise EntityLockNamespaceError(
                f"Held lock namespace identity changed: {self._lock_directory}."
            )
        _require_current_namespace(
            path=self._lock_directory,
            expected_identity=self._namespace_identity,
        )


def _open_guardian(path: Path) -> int:
    """Open guardian. Args: path. Returns: int."""
    flags = os.O_CREAT | os.O_RDWR | getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    try:
        return os.open(path, flags, 0o600)
    except OSError as error:
        raise EntityLockNamespaceError(
            f"Lock namespace guardian could not be opened: {path}."
        ) from error


def _open_namespace(path: Path) -> int:
    """Open namespace. Args: path. Returns: int."""
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise EntityLockNamespaceError(
            f"Lock namespace is missing or invalid: {path}."
        ) from error
    if not stat.S_ISDIR(os.fstat(descriptor).st_mode):
        os.close(descriptor)
        raise EntityLockNamespaceError(f"Lock namespace is not a directory: {path}.")
    return descriptor


def _directory_identity(descriptor: int) -> tuple[int, int]:
    """Directory identity. Args: descriptor. Returns: tuple[int, int]."""
    details = os.fstat(descriptor)
    return details.st_dev, details.st_ino


def _require_current_namespace(
    path: Path,
    expected_identity: tuple[int, int],
) -> None:
    """Require current namespace. Args: path, expected_identity. Returns: None."""
    try:
        details = path.stat(follow_symlinks=False)
    except OSError as error:
        raise EntityLockNamespaceError(f"Lock namespace is missing: {path}.") from error
    current_identity = details.st_dev, details.st_ino
    if not stat.S_ISDIR(details.st_mode) or current_identity != expected_identity:
        raise EntityLockNamespaceError(f"Lock namespace identity was replaced: {path}.")


def _read_guardian_identity(
    descriptor: int,
    path: Path,
) -> tuple[int, int] | None:
    """Read guardian identity. Args: descriptor, path. Returns: tuple[int, int] | None."""
    os.lseek(descriptor, 0, os.SEEK_SET)
    payload = os.read(descriptor, 256)
    if not payload:
        return None
    try:
        device_text, inode_text = payload.decode("ascii").strip().split(":", 1)
        return int(device_text), int(inode_text)
    except (UnicodeDecodeError, ValueError) as error:
        raise EntityLockNamespaceError(
            f"Lock namespace guardian is invalid: {path}."
        ) from error


def _write_guardian_identity(
    descriptor: int,
    identity: tuple[int, int],
) -> None:
    """Write guardian identity. Args: descriptor, identity. Returns: None."""
    payload = f"{identity[0]}:{identity[1]}\n".encode("ascii")
    os.lseek(descriptor, 0, os.SEEK_SET)
    os.ftruncate(descriptor, 0)
    os.write(descriptor, payload)
    os.fsync(descriptor)

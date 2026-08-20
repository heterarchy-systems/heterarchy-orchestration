import re
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

from chattocodex.shared.concurrency.entity_lock_registry import (
    EntityLockNamespaceError,
    EntityLockRegistry,
)

_CHILD_LOCK_SCRIPT = """
import sys
from pathlib import Path

from chattocodex.shared.concurrency.entity_lock_registry import EntityLockRegistry

lock_directory = Path(sys.argv[1])
attempted_path = Path(sys.argv[2])
acquired_path = Path(sys.argv[3])
registry = EntityLockRegistry(lock_directory=lock_directory)
attempted_path.touch()
with registry.hold(entity_key="run:shared"):
    acquired_path.touch()
"""


def _wait_for_path(path: Path, *, timeout: float = 2) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists():
            return True
        time.sleep(0.01)
    return path.exists()


def test_two_registries_serialize_the_same_entity_key(tmp_path: Path) -> None:
    lock_directory = tmp_path / "locks"
    first = EntityLockRegistry(lock_directory=lock_directory)
    second = EntityLockRegistry(lock_directory=lock_directory)
    attempted = threading.Event()
    acquired = threading.Event()

    def hold_from_second_registry() -> None:
        attempted.set()
        with second.hold(entity_key="run:shared"):
            acquired.set()

    with first.hold(entity_key="run:shared"):
        contender = threading.Thread(target=hold_from_second_registry)
        contender.start()
        assert attempted.wait(timeout=1)
        assert not acquired.wait(timeout=0.1)

    assert acquired.wait(timeout=1)
    contender.join(timeout=1)
    assert not contender.is_alive()


def test_file_lock_serializes_the_same_key_across_processes(tmp_path: Path) -> None:
    lock_directory = tmp_path / "locks"
    attempted_path = tmp_path / "attempted"
    acquired_path = tmp_path / "acquired"
    registry = EntityLockRegistry(lock_directory=lock_directory)
    child: subprocess.Popen[str] | None = None
    try:
        with registry.hold(entity_key="run:shared"):
            child = subprocess.Popen(
                (
                    sys.executable,
                    "-c",
                    _CHILD_LOCK_SCRIPT,
                    str(lock_directory),
                    str(attempted_path),
                    str(acquired_path),
                ),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            assert _wait_for_path(attempted_path)
            time.sleep(0.1)
            assert not acquired_path.exists()
            assert child.poll() is None

        assert _wait_for_path(acquired_path)
        stdout, stderr = child.communicate(timeout=2)
        assert child.returncode == 0, (stdout, stderr)
        assert registry.active_entry_count == 0
    finally:
        if child is not None and child.poll() is None:
            child.terminate()
            child.wait(timeout=2)


def test_two_registries_allow_different_entity_keys_concurrently(
    tmp_path: Path,
) -> None:
    lock_directory = tmp_path / "locks"
    first = EntityLockRegistry(lock_directory=lock_directory)
    second = EntityLockRegistry(lock_directory=lock_directory)
    acquired = threading.Event()

    def hold_different_key() -> None:
        with second.hold(entity_key="run:second"):
            acquired.set()

    with first.hold(entity_key="run:first"):
        contender = threading.Thread(target=hold_different_key)
        contender.start()
        assert acquired.wait(timeout=1)

    contender.join(timeout=1)
    assert not contender.is_alive()


def test_registry_prunes_entry_after_last_waiter_releases() -> None:
    registry = EntityLockRegistry()
    attempted = threading.Event()
    acquired = threading.Event()

    def wait_for_entity() -> None:
        attempted.set()
        with registry.hold(entity_key="worker:shared"):
            acquired.set()

    with registry.hold(entity_key="worker:shared"):
        contender = threading.Thread(target=wait_for_entity)
        contender.start()
        assert attempted.wait(timeout=1)
        assert registry.active_entry_count == 1
        assert not acquired.wait(timeout=0.1)

    assert acquired.wait(timeout=1)
    contender.join(timeout=1)
    assert not contender.is_alive()
    assert registry.active_entry_count == 0


def test_file_lock_name_does_not_expose_the_entity_key(tmp_path: Path) -> None:
    lock_directory = tmp_path / "locks"
    registry = EntityLockRegistry(lock_directory=lock_directory)

    with registry.hold(entity_key="../../project/with spaces:\N{SNOWMAN}"):
        pass

    lock_files = list(lock_directory.iterdir())
    assert len(lock_files) == 1
    assert re.fullmatch(r"[0-9a-f]{64}\.lock", lock_files[0].name)


def test_registry_fails_closed_when_lock_namespace_disappears(
    tmp_path: Path,
) -> None:
    lock_directory = tmp_path / "locks"
    registry = EntityLockRegistry(lock_directory=lock_directory)
    lock_directory.rmdir()

    with (
        pytest.raises(EntityLockNamespaceError, match="namespace"),
        registry.hold(entity_key="worker-provisioning:capacity"),
    ):
        pass

    assert not lock_directory.exists()
    assert registry.active_entry_count == 0


def test_new_registry_cannot_split_from_locked_deleted_namespace(
    tmp_path: Path,
) -> None:
    lock_directory = tmp_path / "locks"
    first = EntityLockRegistry(lock_directory=lock_directory)

    with first.hold(entity_key="run:shared"):
        for lock_file in lock_directory.iterdir():
            lock_file.unlink()
        lock_directory.rmdir()

        with pytest.raises(EntityLockNamespaceError, match="namespace"):
            EntityLockRegistry(lock_directory=lock_directory)

    assert not lock_directory.exists()

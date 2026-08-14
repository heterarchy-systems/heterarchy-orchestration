import stat
from pathlib import Path

from chattocodex.persistence.runtime_lock_layout import RuntimeLockLayout


def test_runtime_lock_directory_is_stable_and_outside_execution_state(
    tmp_path: Path,
) -> None:
    state_root = tmp_path / "workspace" / ".chattocodex"
    lock_base = tmp_path / "user-runtime"

    first = RuntimeLockLayout.lock_directory(
        state_root=state_root,
        namespace="execution",
        base_directory=lock_base,
    )
    second = RuntimeLockLayout.lock_directory(
        state_root=state_root / ".." / ".chattocodex",
        namespace="execution",
        base_directory=lock_base,
    )

    assert first == second
    assert not first.is_relative_to(state_root)
    assert state_root.parent.name not in first.name
    assert stat.S_IMODE(lock_base.stat().st_mode) == 0o700


def test_runtime_lock_namespaces_do_not_share_directories(tmp_path: Path) -> None:
    state_root = tmp_path / "state"
    lock_base = tmp_path / "user-runtime"

    execution = RuntimeLockLayout.lock_directory(
        state_root=state_root,
        namespace="execution",
        base_directory=lock_base,
    )
    authentication = RuntimeLockLayout.lock_directory(
        state_root=state_root,
        namespace="authentication",
        base_directory=lock_base,
    )

    assert execution != authentication
    assert len(execution.name) == 64
    assert len(authentication.name) == 64

import os
import subprocess
from pathlib import Path

import pytest

from chattocodex.wake.wake_exceptions import WakeConfigurationError
from chattocodex.wake.worker_browser_runtime_dto import WorkerBrowserRuntimeMode
from chattocodex.wake.worker_browser_runtime_service import WorkerBrowserRuntimeService


def _runtime_fixture(tmp_path: Path) -> tuple[WorkerBrowserRuntimeService, Path, Path]:
    chrome_binary = tmp_path / "chrome-for-testing"
    chrome_binary.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    chrome_binary.chmod(0o700)
    extension_root = tmp_path / "extension"
    extension_root.mkdir()
    (extension_root / "manifest.json").write_text("{}\n", encoding="utf-8")
    service = WorkerBrowserRuntimeService(
        state_root=tmp_path / "state",
        extension_root=extension_root,
    )
    return service, chrome_binary, extension_root


def test_worker_browser_launch_spec_is_headless_and_uses_persistent_profile(
    tmp_path: Path,
) -> None:
    service, chrome_binary, extension_root = _runtime_fixture(tmp_path)

    spec = service.build_launch_spec(chrome_binary=chrome_binary)

    assert spec.mode is WorkerBrowserRuntimeMode.HEADLESS
    assert spec.chrome_binary == chrome_binary.resolve()
    assert spec.profile_root == tmp_path / "state" / "worker_browser_profile"
    assert spec.extension_root == extension_root.resolve()
    assert "--headless=new" in spec.argv
    assert f"--user-data-dir={spec.profile_root}" in spec.argv
    assert f"--disable-extensions-except={extension_root.resolve()}" in spec.argv
    assert f"--load-extension={extension_root.resolve()}" in spec.argv
    assert spec.argv[-1] == "about:blank"


def test_visible_bootstrap_reuses_profile_without_headless_flag(tmp_path: Path) -> None:
    service, chrome_binary, _extension_root = _runtime_fixture(tmp_path)
    profile_root = tmp_path / "persistent-profile"

    spec = service.build_launch_spec(
        chrome_binary=chrome_binary,
        profile_root=profile_root,
        mode=WorkerBrowserRuntimeMode.VISIBLE_BOOTSTRAP,
    )

    assert spec.profile_root == profile_root
    assert "--headless=new" not in spec.argv
    assert f"--user-data-dir={profile_root}" in spec.argv


def test_worker_browser_binary_can_come_from_environment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    service, chrome_binary, _extension_root = _runtime_fixture(tmp_path)
    monkeypatch.setenv("CHATTOCODEX_CHROME_BINARY", str(chrome_binary))

    spec = service.build_launch_spec()

    assert spec.chrome_binary == chrome_binary.resolve()


def test_run_foreground_prepares_private_profile_and_uses_argv(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    service, chrome_binary, _extension_root = _runtime_fixture(tmp_path)
    observed: list[tuple[str, ...]] = []

    def fake_run(
        argv: tuple[str, ...], *, check: bool
    ) -> subprocess.CompletedProcess[str]:
        assert check is False
        observed.append(argv)
        return subprocess.CompletedProcess(argv, 0)

    monkeypatch.setattr(subprocess, "run", fake_run)

    exit_code = service.run_foreground(chrome_binary=chrome_binary)

    profile_root = tmp_path / "state" / "worker_browser_profile"
    assert exit_code == 0
    assert observed and "--headless=new" in observed[0]
    assert profile_root.is_dir()
    assert os.stat(profile_root).st_mode & 0o777 == 0o700


def test_single_runtime_lock_rejects_second_runtime(tmp_path: Path) -> None:
    service, chrome_binary, _extension_root = _runtime_fixture(tmp_path)

    with (
        service._single_runtime_admission(),
        pytest.raises(WakeConfigurationError, match="already running"),
    ):
        service.run_foreground(chrome_binary=chrome_binary)

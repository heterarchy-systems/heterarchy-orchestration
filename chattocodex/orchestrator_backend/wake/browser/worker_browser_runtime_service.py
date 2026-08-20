from __future__ import annotations

import fcntl
import os
import subprocess
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from chattocodex.orchestrator_backend.wake.browser.worker_browser_runtime_dto import (
    WorkerBrowserLaunchSpec,
    WorkerBrowserRuntimeMode,
)
from chattocodex.orchestrator_backend.wake.wake_exceptions import WakeConfigurationError

_DEFAULT_PROFILE_DIRECTORY = "worker_browser_profile"
_RUNTIME_LOCK_NAME = "worker-browser-runtime.lock"
_COMMON_CHROME_BINARIES = (
    Path(
        "/Applications/Google Chrome for Testing.app/Contents/MacOS/Google Chrome for Testing"
    ),
    Path("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"),
    Path("/usr/bin/google-chrome"),
    Path("/usr/bin/google-chrome-stable"),
    Path("/usr/bin/chromium"),
    Path("/usr/bin/chromium-browser"),
)


class WorkerBrowserRuntimeService:
    """Launch one reusable Chrome runtime that only delivers admitted Worker wakes.

    The browser owns no Task authority. Existing extension registrations retain the
    Project conversation identity, while the Broker and wake services remain the
    only authorities for assignment and wake revision.
    """

    def __init__(
        self,
        state_root: Path,
        extension_root: Path | None = None,
    ) -> None:
        """Initialize this object. Args: state_root, extension_root. Returns: None."""
        self._state_root = state_root.expanduser().resolve()
        self._extension_root = (
            extension_root.expanduser().resolve()
            if extension_root is not None
            else (Path(__file__).resolve().parent / "chrome_extension")
        )

    def build_launch_spec(
        self,
        chrome_binary: Path | None = None,
        profile_root: Path | None = None,
        mode: WorkerBrowserRuntimeMode = WorkerBrowserRuntimeMode.HEADLESS,
    ) -> WorkerBrowserLaunchSpec:
        """Build a safe launch command for the single persistent browser runtime. Args: chrome_binary, profile_root, mode. Returns: WorkerBrowserLaunchSpec."""

        resolved_binary = self._resolve_chrome_binary(chrome_binary=chrome_binary)
        resolved_profile = self._resolve_profile_root(profile_root=profile_root)
        extension_root = self._require_extension_root()

        argv = [
            str(resolved_binary),
            f"--user-data-dir={resolved_profile}",
            f"--disable-extensions-except={extension_root}",
            f"--load-extension={extension_root}",
            "--no-first-run",
            "--no-default-browser-check",
        ]
        if mode is WorkerBrowserRuntimeMode.HEADLESS:
            argv.append("--headless=new")
        argv.append("about:blank")
        return WorkerBrowserLaunchSpec(
            chrome_binary=resolved_binary,
            profile_root=resolved_profile,
            extension_root=extension_root,
            mode=mode,
            argv=tuple(argv),
        )

    def run_foreground(
        self,
        chrome_binary: Path | None = None,
        profile_root: Path | None = None,
        mode: WorkerBrowserRuntimeMode = WorkerBrowserRuntimeMode.HEADLESS,
    ) -> int:
        """Run exactly one Worker browser process until Chrome exits. Args: chrome_binary, profile_root, mode. Returns: int."""

        spec = self.build_launch_spec(
            chrome_binary=chrome_binary,
            profile_root=profile_root,
            mode=mode,
        )
        with self._single_runtime_admission():
            self._prepare_profile_root(profile_root=spec.profile_root)
            try:
                completed = subprocess.run(spec.argv, check=False)
            except OSError as error:
                raise WakeConfigurationError(
                    message=f"Worker browser runtime could not start: {error}."
                ) from error
        return completed.returncode

    def _resolve_chrome_binary(self, chrome_binary: Path | None) -> Path:
        """Resolve chrome binary. Args: chrome_binary. Returns: Path."""
        candidates: tuple[Path, ...]
        if chrome_binary is not None:
            candidates = (chrome_binary.expanduser(),)
        else:
            candidates = _COMMON_CHROME_BINARIES
        for candidate in candidates:
            try:
                resolved = candidate.resolve(strict=True)
            except OSError:
                continue
            if resolved.is_file() and os.access(resolved, os.X_OK):
                return resolved
        raise WakeConfigurationError(
            message=(
                "Chrome executable was not found. Pass --chrome-binary or configure "
                "CHATTOCODEX_CHROME_BINARY through RuntimeSettings."
            )
        )

    def _resolve_profile_root(self, profile_root: Path | None) -> Path:
        """Resolve profile root. Args: profile_root. Returns: Path."""
        selected = (
            profile_root.expanduser()
            if profile_root is not None
            else self._state_root / _DEFAULT_PROFILE_DIRECTORY
        )
        return selected.parent.resolve() / selected.name

    def _require_extension_root(self) -> Path:
        """Require extension root. Args: None. Returns: Path."""
        manifest_path = self._extension_root / "manifest.json"
        if not self._extension_root.is_dir() or not manifest_path.is_file():
            raise WakeConfigurationError(
                message=f"Worker wake extension is unavailable: {self._extension_root}."
            )
        return self._extension_root

    def _prepare_profile_root(self, profile_root: Path) -> None:
        """Prepare profile root. Args: profile_root. Returns: None."""
        profile_root.parent.mkdir(parents=True, exist_ok=True)
        if profile_root.exists() and profile_root.is_symlink():
            raise WakeConfigurationError(
                message=f"Worker browser profile cannot be a symlink: {profile_root}."
            )
        profile_root.mkdir(mode=0o700, exist_ok=True)
        profile_root.chmod(0o700)

    @contextmanager
    def _single_runtime_admission(self) -> Iterator[None]:
        """Single runtime admission. Args: None. Returns: Iterator[None]."""
        lock_root = self._state_root / "locks"
        lock_root.mkdir(parents=True, exist_ok=True)
        lock_path = lock_root / _RUNTIME_LOCK_NAME
        flags = os.O_CREAT | os.O_RDWR | getattr(os, "O_CLOEXEC", 0)
        flags |= getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(lock_path, flags, 0o600)
        except OSError as error:
            raise WakeConfigurationError(
                message=f"Worker browser runtime lock could not be opened: {lock_path}."
            ) from error
        try:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as error:
                raise WakeConfigurationError(
                    message="A Worker browser runtime is already running."
                ) from error
            yield
        finally:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
            finally:
                os.close(descriptor)

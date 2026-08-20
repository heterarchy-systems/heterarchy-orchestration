import asyncio
import sys
from contextlib import suppress
from pathlib import Path

from chattocodex.orchestrator_backend.control_panel.domain.control_panel_exceptions import (
    RuntimeLifecycleError,
)
from chattocodex.orchestrator_backend.wake.browser.worker_browser_runtime_dto import (
    WorkerBrowserRuntimeMode,
)

_STOP_TIMEOUT_SECONDS = 5.0
_PROFILE_DIRECTORY = "worker_browser_profile"


class WorkerBrowserProcessSupervisor:
    """Own the Control Panel-managed persistent Worker browser process."""

    def __init__(self, state_root: Path) -> None:
        """Initialize this object. Args: state_root. Returns: None."""
        self._state_root = state_root.expanduser().resolve()
        self._process: asyncio.subprocess.Process | None = None
        self._mode: WorkerBrowserRuntimeMode | None = None

    async def start(self, mode: WorkerBrowserRuntimeMode) -> None:
        """Start. Args: mode. Returns: None."""
        if self.is_running():
            if self._mode is mode:
                return
            raise RuntimeLifecycleError(
                message="Stop the current Worker browser before changing browser mode."
            )
        command = [
            sys.executable,
            "-m",
            "chattocodex.cli",
            "--state-root",
            str(self._state_root),
            "worker-browser",
        ]
        if mode is WorkerBrowserRuntimeMode.VISIBLE_BOOTSTRAP:
            command.append("--visible-bootstrap")
        try:
            process = await asyncio.create_subprocess_exec(
                *command,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
        except OSError as error:
            raise RuntimeLifecycleError(
                message="Worker browser could not be started."
            ) from error
        self._process = process
        self._mode = mode
        await asyncio.sleep(0.1)
        if process.returncode is not None:
            self._process = None
            self._mode = None
            raise RuntimeLifecycleError(
                message=(
                    "Worker browser exited during startup. Check Chrome availability "
                    "and whether another Worker browser runtime already owns the profile."
                )
            )

    async def stop(self) -> None:
        """Stop. Args: None. Returns: None."""
        process = self._process
        if process is None:
            return
        if process.returncode is None:
            process.terminate()
            try:
                await asyncio.wait_for(process.wait(), timeout=_STOP_TIMEOUT_SECONDS)
            except TimeoutError:
                process.kill()
                await process.wait()
        self._process = None
        self._mode = None

    def is_running(self) -> bool:
        """Is running. Args: None. Returns: bool."""
        return self._process is not None and self._process.returncode is None

    def mode(self) -> WorkerBrowserRuntimeMode | None:
        """Mode. Args: None. Returns: WorkerBrowserRuntimeMode | None."""
        if not self.is_running():
            return None
        return self._mode

    def pid(self) -> int | None:
        """Pid. Args: None. Returns: int | None."""
        if not self.is_running() or self._process is None:
            return None
        return self._process.pid

    def profile_root(self) -> Path:
        """Profile root. Args: None. Returns: Path."""
        return self._state_root / _PROFILE_DIRECTORY

    def profile_initialized(self) -> bool:
        """Profile initialized. Args: None. Returns: bool."""
        profile_root = self.profile_root()
        if not profile_root.is_dir():
            return False
        try:
            return next(profile_root.iterdir(), None) is not None
        except OSError:
            return False

    async def close(self) -> None:
        """Close. Args: None. Returns: None."""
        with suppress(RuntimeLifecycleError):
            await self.stop()

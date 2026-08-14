import asyncio
import os
import socket
import sys
from collections import deque
from contextlib import suppress
from pathlib import Path

from chattocodex.control_panel.control_panel_dto import ProcessSnapshot
from chattocodex.control_panel.control_panel_exceptions import RuntimeLifecycleError

_LOG_LIMIT = 120
_STOP_TIMEOUT_SECONDS = 5.0
_START_TIMEOUT_SECONDS = 10.0
_UVICORN_READY_LOG_MARKER = "Uvicorn running on "


class McpProcessSupervisor:
    def __init__(self, *, state_root: Path, host: str = "127.0.0.1") -> None:
        self._state_root = state_root.expanduser().resolve()
        self._host = host
        self._process: asyncio.subprocess.Process | None = None
        self._drain_task: asyncio.Task[None] | None = None
        self._logs: deque[str] = deque(maxlen=_LOG_LIMIT)

    async def start(self, *, port: int) -> None:
        if self.is_running():
            return
        await self.ensure_port_available(port=port)
        environment = os.environ.copy()
        environment["CHATTOCODEX_STATE_ROOT"] = str(self._state_root)
        environment["CHATTOCODEX_HOST"] = self._host
        environment["CHATTOCODEX_PORT"] = str(port)
        environment["CHATTOCODEX_COMPANION_BRIDGE_ENABLED"] = "true"
        self._logs.clear()
        self._process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "chattocodex.mcp_server",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            env=environment,
        )
        if self._process.stdout is None:
            await self.stop()
            raise RuntimeLifecycleError(
                message="MCP process output stream is unavailable."
            )
        ready = asyncio.Event()
        self._drain_task = asyncio.create_task(
            self._drain_output(self._process.stdout, ready=ready)
        )
        try:
            await asyncio.wait_for(
                self._wait_until_ready(ready=ready),
                timeout=_START_TIMEOUT_SECONDS,
            )
        except TimeoutError as error:
            await self.stop()
            logs = self._log_summary()
            raise RuntimeLifecycleError(
                message=f"MCP server did not become ready on port {port}. {logs}"
            ) from error
        if self._process.returncode is not None:
            await self.stop()
            logs = self._log_summary()
            raise RuntimeLifecycleError(
                message=f"MCP server exited during startup. {logs}"
            )

    async def ensure_port_available(self, *, port: int) -> None:
        try:
            await asyncio.to_thread(
                _verify_bind_available,
                host=self._host,
                port=port,
            )
        except OSError as error:
            raise RuntimeLifecycleError(
                message=f"MCP port {port} is already in use."
            ) from error

    async def stop(self) -> None:
        process = self._process
        if process is None:
            return
        if process.returncode is None:
            process.terminate()
            try:
                await asyncio.wait_for(
                    process.wait(),
                    timeout=_STOP_TIMEOUT_SECONDS,
                )
            except TimeoutError:
                process.kill()
                await process.wait()
        if self._drain_task is not None:
            await _finish_drain_task(task=self._drain_task)
        self._process = None
        self._drain_task = None

    def is_running(self) -> bool:
        return self._process is not None and self._process.returncode is None

    def snapshot(self) -> ProcessSnapshot:
        process = self._process
        return ProcessSnapshot(
            running=self.is_running(),
            pid=process.pid
            if process is not None and process.returncode is None
            else None,
            log_lines=tuple(self._logs),
        )

    async def _wait_until_ready(self, *, ready: asyncio.Event) -> None:
        while not ready.is_set():
            if self._process is None or self._process.returncode is not None:
                return
            await asyncio.sleep(0.01)

    async def _drain_output(
        self,
        stream: asyncio.StreamReader,
        *,
        ready: asyncio.Event,
    ) -> None:
        while True:
            line = await stream.readline()
            if not line:
                return
            safe_line = _safe_log_line(line=line)
            self._logs.append(safe_line)
            if _UVICORN_READY_LOG_MARKER in safe_line:
                ready.set()

    def _log_summary(self) -> str:
        if not self._logs:
            return "No MCP logs were captured."
        return f"Last log: {self._logs[-1]}"


def _safe_log_line(*, line: bytes) -> str:
    return line.decode("utf-8", errors="replace").strip()[:4000]


def _verify_bind_available(*, host: str, port: int) -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind((host, port))


async def _finish_drain_task(*, task: asyncio.Task[None]) -> None:
    try:
        await asyncio.wait_for(task, timeout=0.5)
    except TimeoutError:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task

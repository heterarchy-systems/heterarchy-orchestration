import asyncio
import re
import shutil
from collections import deque
from contextlib import suppress
from urllib.parse import urlparse

from chattocodex.control_panel.control_panel_dto import ProcessSnapshot
from chattocodex.control_panel.control_panel_exceptions import TunnelLifecycleError
from chattocodex.remote.cloudflare_tunnel import build_cloudflare_tunnel_command

_LOG_LIMIT = 120
_STOP_TIMEOUT_SECONDS = 5.0
_QUICK_URL_TIMEOUT_SECONDS = 20.0
_QUICK_URL_PATTERN = re.compile(
    r"https://[a-z0-9-]+\.trycloudflare\.com", re.IGNORECASE
)


class CloudflareProcessSupervisor:
    def __init__(self, *, local_host: str = "127.0.0.1") -> None:
        self._local_host = local_host
        self._process: asyncio.subprocess.Process | None = None
        self._drain_task: asyncio.Task[None] | None = None
        self._logs: deque[str] = deque(maxlen=_LOG_LIMIT)
        self._public_url: str | None = None

    async def start_quick(self, *, local_port: int) -> str:
        await self._require_stopped()
        cloudflared_path = _require_cloudflared_path()
        command = (
            cloudflared_path,
            "tunnel",
            "--url",
            f"http://{self._local_host}:{local_port}",
            "--no-autoupdate",
        )
        self._process = await self._spawn(command=command)
        if self._process.stdout is None:
            await self.stop()
            raise TunnelLifecycleError(
                message="Cloudflare output stream is unavailable."
            )
        try:
            public_url = await asyncio.wait_for(
                self._read_until_quick_url(stream=self._process.stdout),
                timeout=_QUICK_URL_TIMEOUT_SECONDS,
            )
        except TimeoutError as error:
            logs = self._log_summary()
            await self.stop()
            raise TunnelLifecycleError(
                message=f"Quick Tunnel URL was not issued in time. {logs}"
            ) from error
        self._public_url = public_url
        self._drain_task = asyncio.create_task(self._drain_output(self._process.stdout))
        return public_url

    async def start_custom(self, *, public_origin: str, local_port: int) -> None:
        await self._require_stopped()
        hostname = urlparse(public_origin).hostname
        if hostname is None:
            raise TunnelLifecycleError(message="Custom public origin has no hostname.")
        cloudflared_path = _require_cloudflared_path()
        command = build_cloudflare_tunnel_command(
            cloudflared_path=cloudflared_path,
            hostname=hostname,
            local_host=self._local_host,
            local_port=local_port,
        )
        self._process = await self._spawn(command=command)
        if self._process.stdout is None:
            await self.stop()
            raise TunnelLifecycleError(
                message="Cloudflare output stream is unavailable."
            )
        self._public_url = public_origin
        self._drain_task = asyncio.create_task(self._drain_output(self._process.stdout))
        await asyncio.sleep(0.75)
        if self._process.returncode is not None:
            logs = self._log_summary()
            await self.stop()
            raise TunnelLifecycleError(
                message=f"Custom-domain tunnel exited during startup. {logs}"
            )

    async def stop(self) -> None:
        process = self._process
        if process is None:
            self._public_url = None
            return
        if process.returncode is None:
            process.terminate()
            try:
                await asyncio.wait_for(process.wait(), timeout=_STOP_TIMEOUT_SECONDS)
            except TimeoutError:
                process.kill()
                await process.wait()
        if self._drain_task is not None:
            await _finish_drain_task(task=self._drain_task)
        self._process = None
        self._drain_task = None
        self._public_url = None

    def is_running(self) -> bool:
        return self._process is not None and self._process.returncode is None

    def public_url(self) -> str | None:
        return self._public_url

    def snapshot(self) -> ProcessSnapshot:
        process = self._process
        return ProcessSnapshot(
            running=self.is_running(),
            pid=process.pid
            if process is not None and process.returncode is None
            else None,
            log_lines=tuple(self._logs),
        )

    async def _require_stopped(self) -> None:
        if self.is_running():
            raise TunnelLifecycleError(message="Cloudflare tunnel is already running.")
        if self._process is not None:
            await self.stop()
        self._logs.clear()

    async def _spawn(self, *, command: tuple[str, ...]) -> asyncio.subprocess.Process:
        return await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )

    async def _read_until_quick_url(self, *, stream: asyncio.StreamReader) -> str:
        while True:
            if self._process is None:
                raise TunnelLifecycleError(message="Cloudflare process disappeared.")
            line = await stream.readline()
            if not line:
                logs = self._log_summary()
                raise TunnelLifecycleError(
                    message=f"Quick Tunnel exited before issuing a public URL. {logs}"
                )
            text = _safe_log_line(line=line)
            self._logs.append(text)
            match = _QUICK_URL_PATTERN.search(text)
            if match is not None:
                return match.group(0).rstrip("/")

    async def _drain_output(self, stream: asyncio.StreamReader) -> None:
        while True:
            line = await stream.readline()
            if not line:
                return
            self._logs.append(_safe_log_line(line=line))

    def _log_summary(self) -> str:
        if not self._logs:
            return "No Cloudflare logs were captured."
        return f"Last log: {self._logs[-1]}"


def _require_cloudflared_path() -> str:
    cloudflared_path = shutil.which("cloudflared")
    if cloudflared_path is None:
        raise TunnelLifecycleError(
            message="cloudflared is not installed or is not available on PATH."
        )
    return cloudflared_path


def _safe_log_line(*, line: bytes) -> str:
    return line.decode("utf-8", errors="replace").strip()[:4000]


async def _finish_drain_task(*, task: asyncio.Task[None]) -> None:
    try:
        await asyncio.wait_for(task, timeout=0.5)
    except TimeoutError:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task

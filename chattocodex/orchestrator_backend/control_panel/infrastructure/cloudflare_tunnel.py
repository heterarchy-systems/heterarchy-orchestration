import os
import shutil
from pathlib import Path
from urllib.parse import urlparse

from chattocodex.shared.settings.runtime_config_repository import (
    RuntimeConfigRepository,
)


class CloudflareTunnelError(RuntimeError):
    pass


def build_cloudflare_tunnel_command(
    cloudflared_path: str,
    hostname: str,
    local_host: str,
    local_port: int,
) -> tuple[str, ...]:
    """Build cloudflare tunnel command. Args: cloudflared_path, hostname, local_host, local_port. Returns: tuple[str, ...]."""
    if not hostname or hostname in {"localhost", "127.0.0.1", "::1"}:
        raise CloudflareTunnelError("A non-local configured hostname is required.")
    local_url = f"http://{local_host}:{local_port}"
    return (
        cloudflared_path,
        "tunnel",
        "--hostname",
        hostname,
        "--url",
        local_url,
        "--no-autoupdate",
    )


def launch_configured_cloudflare_tunnel(
    state_root: Path,
    local_host: str,
    local_port: int,
) -> None:
    """Launch configured cloudflare tunnel. Args: state_root, local_host, local_port. Returns: None."""
    config_repository = RuntimeConfigRepository(state_root=state_root)
    if not config_repository.exists():
        raise CloudflareTunnelError(
            "Runtime is not configured. Run `chattocodex setup` first."
        )
    config = config_repository.read()
    if config.public_url is None:
        raise CloudflareTunnelError("Configured runtime has no public_url.")
    hostname = urlparse(config.public_url).hostname
    if hostname is None:
        raise CloudflareTunnelError("Configured public_url has no hostname.")
    cloudflared_path = shutil.which("cloudflared")
    if cloudflared_path is None:
        raise CloudflareTunnelError(
            "cloudflared is not installed or is not available on PATH."
        )
    command = build_cloudflare_tunnel_command(
        cloudflared_path=cloudflared_path,
        hostname=hostname,
        local_host=local_host,
        local_port=local_port,
    )
    os.execv(cloudflared_path, command)

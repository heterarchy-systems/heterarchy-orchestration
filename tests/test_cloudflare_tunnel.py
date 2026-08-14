import pytest

from chattocodex.remote.cloudflare_tunnel import (
    CloudflareTunnelError,
    build_cloudflare_tunnel_command,
)


def test_cloudflare_tunnel_command_uses_configured_hostname_and_local_port() -> None:
    command = build_cloudflare_tunnel_command(
        cloudflared_path="/usr/local/bin/cloudflared",
        hostname="multi.example.test",
        local_host="127.0.0.1",
        local_port=8765,
    )

    assert command == (
        "/usr/local/bin/cloudflared",
        "tunnel",
        "--hostname",
        "multi.example.test",
        "--url",
        "http://127.0.0.1:8765",
        "--no-autoupdate",
    )


def test_cloudflare_tunnel_rejects_local_hostname() -> None:
    with pytest.raises(CloudflareTunnelError):
        build_cloudflare_tunnel_command(
            cloudflared_path="cloudflared",
            hostname="localhost",
            local_host="127.0.0.1",
            local_port=8765,
        )

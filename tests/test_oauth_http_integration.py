import asyncio
import base64
import hashlib
import os
import socket
import subprocess
import sys
import time
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import httpx2
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client

from chattocodex.schemas.setup_schemas import SetupRequest
from chattocodex.setup.runtime_setup_service import RuntimeSetupService


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.bind(("127.0.0.1", 0))
        return int(server_socket.getsockname()[1])


def _wait_for_port(*, port: int, timeout_seconds: float = 10.0) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
            client_socket.settimeout(0.1)
            if client_socket.connect_ex(("127.0.0.1", port)) == 0:
                return
        time.sleep(0.05)
    raise TimeoutError(f"MCP server did not open port {port} within {timeout_seconds}s")


async def _exercise_oauth_and_mcp(*, base_url: str, owner_token: str) -> None:
    resource_url = f"{base_url}/mcp"
    redirect_uri = "http://127.0.0.1:45678/callback"
    verifier = "v" * 64
    challenge = (
        base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest())
        .rstrip(b"=")
        .decode()
    )

    async with httpx2.AsyncClient(follow_redirects=False) as client:
        metadata = await client.get(
            f"{base_url}/.well-known/oauth-protected-resource/mcp"
        )
        assert metadata.status_code == 200
        assert metadata.json()["resource"] == resource_url

        unauthenticated = await client.post(
            resource_url,
            json={"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}},
        )
        assert unauthenticated.status_code == 401

        registration = await client.post(
            f"{base_url}/register",
            json={
                "redirect_uris": [redirect_uri],
                "token_endpoint_auth_method": "none",
                "grant_types": ["authorization_code", "refresh_token"],
                "response_types": ["code"],
                "client_name": "integration-test",
                "scope": "chattocodex offline_access",
            },
        )
        assert registration.status_code == 201
        client_id = str(registration.json()["client_id"])

        authorization = await client.get(
            f"{base_url}/authorize",
            params={
                "response_type": "code",
                "client_id": client_id,
                "redirect_uri": redirect_uri,
                "scope": "chattocodex offline_access",
                "state": "integration-state",
                "code_challenge": challenge,
                "code_challenge_method": "S256",
                "resource": resource_url,
            },
        )
        assert authorization.status_code == 302
        approval_url = authorization.headers["location"]
        pending_id = parse_qs(urlparse(approval_url).query)["pending_id"][0]

        approval = await client.post(
            f"{base_url}/oauth/approve",
            data={"pending_id": pending_id, "owner_token": owner_token},
        )
        assert approval.status_code == 302
        callback_url = approval.headers["location"]
        callback_query = parse_qs(urlparse(callback_url).query)
        assert callback_query["state"] == ["integration-state"]
        authorization_code = callback_query["code"][0]

        token_response = await client.post(
            f"{base_url}/token",
            data={
                "grant_type": "authorization_code",
                "client_id": client_id,
                "code": authorization_code,
                "redirect_uri": redirect_uri,
                "code_verifier": verifier,
                "resource": resource_url,
            },
        )
        assert token_response.status_code == 200
        token_payload = token_response.json()
        access_token = str(token_payload["access_token"])
        refresh_token = str(token_payload["refresh_token"])

    async with (
        httpx2.AsyncClient(
            headers={"Authorization": f"Bearer {access_token}"}
        ) as authenticated_client,
        streamable_http_client(
            resource_url,
            http_client=authenticated_client,
        ) as (read_stream, write_stream),
        ClientSession(read_stream, write_stream) as session,
    ):
        await session.initialize()
        tools = await session.list_tools()
        assert "project_list" in {tool.name for tool in tools.tools}
        projects = await session.call_tool(
            "arbitrary-display-name.project_list",
            {},
        )
        assert projects.is_error is False
        assert projects.structured_content is not None
        assert projects.structured_content["result"]["projects"][0]["name"] == "demo"

    async with httpx2.AsyncClient() as client:
        refresh_response = await client.post(
            f"{base_url}/token",
            data={
                "grant_type": "refresh_token",
                "client_id": client_id,
                "refresh_token": refresh_token,
            },
        )
        assert refresh_response.status_code == 200
        assert refresh_response.json()["refresh_token"] != refresh_token


def test_oauth_http_flow_authenticates_real_streamable_mcp(tmp_path: Path) -> None:
    state_root = tmp_path / "state"
    workspace_root = tmp_path / "workspace"
    project_root = workspace_root / "demo"
    project_root.mkdir(parents=True)
    (project_root / "pyproject.toml").write_text(
        "[project]\nname = 'demo'\n",
        encoding="utf-8",
    )
    port = _find_free_port()
    base_url = f"http://127.0.0.1:{port}"
    setup = RuntimeSetupService(state_root=state_root).configure(
        request=SetupRequest(
            workspace_root=workspace_root,
            public_url=base_url,
            oauth_enabled=True,
        )
    )
    assert setup.owner_token is not None

    environment = os.environ.copy()
    environment["CHATTOCODEX_STATE_ROOT"] = str(state_root)
    environment["CHATTOCODEX_PORT"] = str(port)
    process = subprocess.Popen(
        [sys.executable, "-m", "chattocodex.mcp_server"],
        env=environment,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        _wait_for_port(port=port)
        asyncio.run(
            _exercise_oauth_and_mcp(
                base_url=base_url,
                owner_token=setup.owner_token,
            )
        )
    finally:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)

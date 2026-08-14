import base64
import hashlib
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from starlette.testclient import TestClient

from chattocodex.auth.oauth_dto import OAuthServerOptions
from chattocodex.auth.owner_token_service import OwnerTokenService
from chattocodex.mcp_server import create_mcp_server
from chattocodex.remote.mcp_transport_security import build_mcp_transport_security

_PUBLIC_ORIGIN = "https://stands-confidence-void-walter.trycloudflare.com"
_INITIALIZE_REQUEST = {
    "jsonrpc": "2.0",
    "id": 1,
    "method": "initialize",
    "params": {
        "protocolVersion": "2025-03-26",
        "capabilities": {},
        "clientInfo": {"name": "host-security-test", "version": "1.0"},
    },
}


def _build_app(*, tmp_path: Path):
    workspace_root = tmp_path / "workspace"
    workspace_root.mkdir()
    server = create_mcp_server(
        state_root=tmp_path / "state",
        workspace_root=workspace_root,
    )
    return server.streamable_http_app(
        host="127.0.0.1",
        streamable_http_path="/mcp",
        json_response=True,
        stateless_http=False,
        transport_security=build_mcp_transport_security(public_url=_PUBLIC_ORIGIN),
    )


def test_public_quick_tunnel_host_is_allowed_by_real_mcp_transport(
    tmp_path: Path,
) -> None:
    app = _build_app(tmp_path=tmp_path)

    with TestClient(app) as client:
        response = client.post(
            "/mcp",
            json=_INITIALIZE_REQUEST,
            headers={
                "host": "stands-confidence-void-walter.trycloudflare.com",
                "accept": "application/json, text/event-stream",
            },
        )

    assert response.status_code == 200


def test_unknown_remote_host_is_still_rejected(tmp_path: Path) -> None:
    app = _build_app(tmp_path=tmp_path)

    with TestClient(app) as client:
        response = client.post(
            "/mcp",
            json=_INITIALIZE_REQUEST,
            headers={
                "host": "attacker.example",
                "accept": "application/json, text/event-stream",
            },
        )

    assert response.status_code == 421
    assert response.text == "Invalid Host header"


def test_openid_discovery_alias_matches_oauth_metadata(tmp_path: Path) -> None:
    state_root = tmp_path / "state"
    workspace_root = tmp_path / "workspace"
    workspace_root.mkdir()
    OwnerTokenService(state_root=state_root).generate()
    server = create_mcp_server(
        state_root=state_root,
        workspace_root=workspace_root,
        oauth_options=OAuthServerOptions(
            public_url=_PUBLIC_ORIGIN,
            allowed_redirect_hosts=("chatgpt.com",),
            access_token_ttl_seconds=3600,
            refresh_token_ttl_seconds=3600,
        ),
    )
    app = server.streamable_http_app(
        host="127.0.0.1",
        streamable_http_path="/mcp",
        json_response=True,
        stateless_http=False,
        transport_security=build_mcp_transport_security(public_url=_PUBLIC_ORIGIN),
    )
    headers = {"host": "stands-confidence-void-walter.trycloudflare.com"}

    with TestClient(app) as client:
        oauth = client.get("/.well-known/oauth-authorization-server", headers=headers)
        openid = client.get("/.well-known/openid-configuration", headers=headers)

    assert oauth.status_code == 200
    assert openid.status_code == 200
    assert openid.json() == oauth.json()
    assert openid.headers["cache-control"] == "no-store"


def test_quick_tunnel_host_completes_oauth_then_authenticated_mcp(
    tmp_path: Path,
) -> None:
    state_root = tmp_path / "state"
    workspace_root = tmp_path / "workspace"
    workspace_root.mkdir()
    owner_token = OwnerTokenService(state_root=state_root).generate()
    server = create_mcp_server(
        state_root=state_root,
        workspace_root=workspace_root,
        oauth_options=OAuthServerOptions(
            public_url=_PUBLIC_ORIGIN,
            allowed_redirect_hosts=("chatgpt.com",),
            access_token_ttl_seconds=3600,
            refresh_token_ttl_seconds=3600,
        ),
    )
    app = server.streamable_http_app(
        host="127.0.0.1",
        streamable_http_path="/mcp",
        json_response=True,
        stateless_http=False,
        transport_security=build_mcp_transport_security(public_url=_PUBLIC_ORIGIN),
    )
    verifier = "v" * 64
    challenge = (
        base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest())
        .rstrip(b"=")
        .decode()
    )
    redirect_uri = "https://chatgpt.com/connector/oauth/test"
    resource_url = f"{_PUBLIC_ORIGIN}/mcp"

    with TestClient(app, base_url=_PUBLIC_ORIGIN) as client:
        registration = client.post(
            "/register",
            json={
                "redirect_uris": [redirect_uri],
                "token_endpoint_auth_method": "none",
                "grant_types": ["authorization_code", "refresh_token"],
                "response_types": ["code"],
                "client_name": "chatgpt-host-regression",
                "scope": "chattocodex offline_access",
            },
        )
        assert registration.status_code == 201
        client_id = str(registration.json()["client_id"])

        authorization = client.get(
            "/authorize",
            params={
                "response_type": "code",
                "client_id": client_id,
                "redirect_uri": redirect_uri,
                "scope": "chattocodex offline_access",
                "state": "chatgpt-regression",
                "code_challenge": challenge,
                "code_challenge_method": "S256",
                "resource": resource_url,
            },
            follow_redirects=False,
        )
        assert authorization.status_code == 302
        pending_id = parse_qs(urlparse(authorization.headers["location"]).query)[
            "pending_id"
        ][0]

        approval = client.post(
            "/oauth/approve",
            data={"pending_id": pending_id, "owner_token": owner_token},
            follow_redirects=False,
        )
        assert approval.status_code == 302
        authorization_code = parse_qs(urlparse(approval.headers["location"]).query)[
            "code"
        ][0]

        token = client.post(
            "/token",
            data={
                "grant_type": "authorization_code",
                "client_id": client_id,
                "code": authorization_code,
                "redirect_uri": redirect_uri,
                "code_verifier": verifier,
                "resource": resource_url,
            },
        )
        assert token.status_code == 200
        access_token = str(token.json()["access_token"])

        authenticated = client.post(
            "/mcp",
            json=_INITIALIZE_REQUEST,
            headers={
                "authorization": f"Bearer {access_token}",
                "accept": "application/json, text/event-stream",
            },
        )

    assert authenticated.status_code == 200

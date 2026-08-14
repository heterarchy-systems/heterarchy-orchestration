from mcp.server.mcpserver import MCPServer
from mcp.shared.auth import OAuthMetadata
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

_MCP_SCOPE = "chattocodex"
_OFFLINE_ACCESS_SCOPE = "offline_access"
_TOKEN_AUTH_METHODS = (
    "client_secret_post",
    "client_secret_basic",
)


def register_oauth_compatibility_routes(
    *,
    server: MCPServer[None],
    public_url: str,
) -> None:
    metadata = _build_oauth_metadata(public_url=public_url)

    @server.custom_route(
        "/.well-known/openid-configuration",
        methods=["GET"],
        include_in_schema=False,
    )
    async def openid_configuration(_request: Request) -> Response:
        return JSONResponse(
            metadata.model_dump(mode="json", exclude_none=True),
            headers={
                "Access-Control-Allow-Origin": "*",
                "Cache-Control": "no-store",
                "X-Content-Type-Options": "nosniff",
            },
        )


def _build_oauth_metadata(*, public_url: str) -> OAuthMetadata:
    origin = public_url.rstrip("/")
    return OAuthMetadata(
        issuer=origin,
        authorization_endpoint=f"{origin}/authorize",
        token_endpoint=f"{origin}/token",
        registration_endpoint=f"{origin}/register",
        scopes_supported=[_MCP_SCOPE, _OFFLINE_ACCESS_SCOPE],
        response_types_supported=["code"],
        grant_types_supported=["authorization_code", "refresh_token"],
        token_endpoint_auth_methods_supported=list(_TOKEN_AUTH_METHODS),
        revocation_endpoint=f"{origin}/revoke",
        revocation_endpoint_auth_methods_supported=list(_TOKEN_AUTH_METHODS),
        code_challenge_methods_supported=["S256"],
    )

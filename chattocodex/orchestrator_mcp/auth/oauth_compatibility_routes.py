from fastapi import Request, Response
from mcp.server.mcpserver import MCPServer
from mcp.shared.auth import OAuthMetadata

from chattocodex.orchestrator_mcp.auth.oauth_enums import (
    OAuthCodeChallengeMethod,
    OAuthGrantType,
    OAuthResponseType,
    OAuthScope,
    TokenEndpointAuthMethod,
)


def register_oauth_compatibility_routes(
    server: MCPServer[None],
    public_url: str,
) -> None:
    """Register oauth compatibility routes. Args: server, public_url. Returns: None."""
    metadata = _build_oauth_metadata(public_url=public_url)

    @server.custom_route(
        "/.well-known/openid-configuration",
        methods=["GET"],
        include_in_schema=False,
    )
    async def openid_configuration(_request: Request) -> Response:
        """Openid configuration. Args: _request. Returns: Response."""
        return Response(
            content=metadata.model_dump_json(exclude_none=True),
            media_type="application/json",
            headers={
                "Access-Control-Allow-Origin": "*",
                "Cache-Control": "no-store",
                "X-Content-Type-Options": "nosniff",
            },
        )


def _build_oauth_metadata(public_url: str) -> OAuthMetadata:
    """Build oauth metadata. Args: public_url. Returns: OAuthMetadata."""
    origin = public_url.rstrip("/")
    return OAuthMetadata(
        issuer=origin,
        authorization_endpoint=f"{origin}/authorize",
        token_endpoint=f"{origin}/token",
        registration_endpoint=f"{origin}/register",
        scopes_supported=[item.value for item in OAuthScope],
        response_types_supported=[item.value for item in OAuthResponseType],
        grant_types_supported=[item.value for item in OAuthGrantType],
        token_endpoint_auth_methods_supported=[
            item.value for item in TokenEndpointAuthMethod
        ],
        revocation_endpoint=f"{origin}/revoke",
        revocation_endpoint_auth_methods_supported=[
            item.value for item in TokenEndpointAuthMethod
        ],
        code_challenge_methods_supported=[
            item.value for item in OAuthCodeChallengeMethod
        ],
    )

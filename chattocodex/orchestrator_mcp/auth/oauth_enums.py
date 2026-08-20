from enum import StrEnum


class OAuthScope(StrEnum):
    """OAuth scopes advertised by the MCP authorization compatibility endpoint."""

    MCP = "chattocodex"
    OFFLINE_ACCESS = "offline_access"


class TokenEndpointAuthMethod(StrEnum):
    """Client authentication methods advertised by OAuth metadata."""

    CLIENT_SECRET_POST = "client_secret_post"
    CLIENT_SECRET_BASIC = "client_secret_basic"


class OAuthResponseType(StrEnum):
    """OAuth authorization response types supported by this MCP server."""

    CODE = "code"


class OAuthGrantType(StrEnum):
    """OAuth grant types supported by this MCP server."""

    AUTHORIZATION_CODE = "authorization_code"
    REFRESH_TOKEN = "refresh_token"


class OAuthCodeChallengeMethod(StrEnum):
    """PKCE code challenge methods supported by this MCP server."""

    S256 = "S256"

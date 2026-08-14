from dataclasses import dataclass
from pathlib import Path

from mcp.server.auth.settings import (
    AuthSettings,
    ClientRegistrationOptions,
    RevocationOptions,
)

from chattocodex.auth.oauth_dto import OAuthServerOptions
from chattocodex.auth.owner_token_service import OwnerTokenService
from chattocodex.auth.single_user_oauth_provider import SingleUserOAuthProvider

_MCP_SCOPE = "chattocodex"
_OFFLINE_ACCESS_SCOPE = "offline_access"


@dataclass(frozen=True, slots=True, kw_only=True)
class OAuthServerConfiguration:
    provider: SingleUserOAuthProvider
    auth_settings: AuthSettings


def build_oauth_server_configuration(
    *,
    state_root: Path,
    options: OAuthServerOptions,
    mcp_path: str,
) -> OAuthServerConfiguration:
    normalized_public_url = options.public_url.rstrip("/")
    resource_url = f"{normalized_public_url}{mcp_path}"
    if not OwnerTokenService(state_root=state_root).is_configured():
        raise ValueError(
            "OAuth is enabled but Owner Token is not configured. Run `chattocodex setup` first."
        )

    provider = SingleUserOAuthProvider(
        state_root=state_root,
        public_url=normalized_public_url,
        resource_url=resource_url,
        allowed_redirect_hosts=options.allowed_redirect_hosts,
        access_token_ttl_seconds=options.access_token_ttl_seconds,
        refresh_token_ttl_seconds=options.refresh_token_ttl_seconds,
    )
    auth_settings = AuthSettings(
        issuer_url=normalized_public_url,
        resource_server_url=resource_url,
        client_registration_options=ClientRegistrationOptions(
            enabled=True,
            valid_scopes=[_MCP_SCOPE, _OFFLINE_ACCESS_SCOPE],
            default_scopes=[_MCP_SCOPE, _OFFLINE_ACCESS_SCOPE],
        ),
        revocation_options=RevocationOptions(enabled=True),
        required_scopes=[_MCP_SCOPE],
    )
    return OAuthServerConfiguration(
        provider=provider,
        auth_settings=auth_settings,
    )

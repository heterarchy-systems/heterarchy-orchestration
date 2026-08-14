from dataclasses import dataclass


@dataclass(frozen=True, slots=True, kw_only=True)
class OAuthServerOptions:
    public_url: str
    allowed_redirect_hosts: tuple[str, ...]
    access_token_ttl_seconds: int
    refresh_token_ttl_seconds: int

from urllib.parse import urlparse

from mcp.server.transport_security import TransportSecuritySettings

_TRUSTED_CHATGPT_ORIGINS = (
    "https://chatgpt.com",
    "https://chat.openai.com",
)
_LOOPBACK_HOSTS = (
    "127.0.0.1:*",
    "localhost:*",
    "[::1]:*",
)
_LOOPBACK_ORIGINS = (
    "http://127.0.0.1:*",
    "http://localhost:*",
    "http://[::1]:*",
)


def build_mcp_transport_security(*, public_url: str) -> TransportSecuritySettings:
    parsed = urlparse(public_url)
    hostname = parsed.hostname
    if hostname is None:
        raise ValueError("Configured public_url must include a hostname.")

    public_origin = f"{parsed.scheme}://{parsed.netloc}"
    return TransportSecuritySettings(
        enable_dns_rebinding_protection=True,
        allowed_hosts=[
            hostname,
            f"{hostname}:*",
            *_LOOPBACK_HOSTS,
        ],
        allowed_origins=[
            public_origin,
            *_TRUSTED_CHATGPT_ORIGINS,
            *_LOOPBACK_ORIGINS,
        ],
    )

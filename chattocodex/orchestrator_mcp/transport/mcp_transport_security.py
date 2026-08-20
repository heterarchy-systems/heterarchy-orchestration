from urllib.parse import urlparse

from mcp.server.transport_security import TransportSecuritySettings

from chattocodex.orchestrator_mcp.transport.mcp_transport_enums import (
    LoopbackHostPattern,
    LoopbackOriginPattern,
    TrustedChatGptOrigin,
)


def build_mcp_transport_security(public_url: str) -> TransportSecuritySettings:
    """Build mcp transport security. Args: public_url. Returns: TransportSecuritySettings."""
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
            *(item.value for item in LoopbackHostPattern),
        ],
        allowed_origins=[
            public_origin,
            *(item.value for item in TrustedChatGptOrigin),
            *(item.value for item in LoopbackOriginPattern),
        ],
    )

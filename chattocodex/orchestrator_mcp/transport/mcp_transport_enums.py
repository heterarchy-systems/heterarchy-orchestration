from enum import StrEnum


class TrustedChatGptOrigin(StrEnum):
    """Trusted browser origins accepted by the remote MCP transport."""

    CHATGPT = "https://chatgpt.com"
    LEGACY_CHATGPT = "https://chat.openai.com"


class LoopbackHostPattern(StrEnum):
    """Loopback Host patterns accepted by MCP DNS-rebinding protection."""

    IPV4 = "127.0.0.1:*"
    LOCALHOST = "localhost:*"
    IPV6 = "[::1]:*"


class LoopbackOriginPattern(StrEnum):
    """Loopback browser Origin patterns accepted by the MCP transport."""

    IPV4 = "http://127.0.0.1:*"
    LOCALHOST = "http://localhost:*"
    IPV6 = "http://[::1]:*"

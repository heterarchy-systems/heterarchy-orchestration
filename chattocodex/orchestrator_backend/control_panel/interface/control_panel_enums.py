from enum import StrEnum


class LoopbackHost(StrEnum):
    """Loopback hostnames accepted by the Control Panel ASGI boundary."""

    IPV4 = "127.0.0.1"
    LOCALHOST = "localhost"
    IPV6 = "::1"


class StateChangingHttpMethod(StrEnum):
    """HTTP methods that require same-origin validation at the Control Panel boundary."""

    POST = "POST"
    PUT = "PUT"
    PATCH = "PATCH"
    DELETE = "DELETE"

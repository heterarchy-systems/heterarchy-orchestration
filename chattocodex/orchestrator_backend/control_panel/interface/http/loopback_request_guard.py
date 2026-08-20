"""ASGI guard for the loopback-only Control Panel boundary."""

import re
from collections.abc import Awaitable, Callable
from urllib.parse import urlsplit

from fastapi import Response
from starlette.types import Receive, Scope, Send

from chattocodex.orchestrator_backend.control_panel.interface.control_panel_enums import (
    LoopbackHost,
    StateChangingHttpMethod,
)
from chattocodex.shared.contracts.control_panel.control_panel_schemas import (
    ControlPanelErrorResponse,
)

_LOOPBACK_HOST_VALUES = frozenset(item.value for item in LoopbackHost)
_STATE_CHANGING_METHOD_VALUES = frozenset(
    item.value for item in StateChangingHttpMethod
)
_CHROME_EXTENSION_ID = re.compile(r"[a-p]{32}")


class LoopbackRequestGuard:
    """Reject DNS-rebinding hosts and cross-origin state-changing requests."""

    def __init__(self, app: Callable[[Scope, Receive, Send], Awaitable[None]]) -> None:
        """Initialize this object. Args: app. Returns: None."""
        self._app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Invoke this callable. Args: scope, receive, send. Returns: None."""
        if scope["type"] != "http":
            await self._app(scope, receive, send)
            return

        headers = _headers(scope=scope)
        if not _host_is_allowed(scope=scope, host_header=headers.get("host")):
            await _denied_response(
                status_code=400,
                error_code="invalid_host",
                message="Control Panel requests must use a loopback Host header.",
            )(scope, receive, send)
            return

        origin = headers.get("origin")
        if (
            scope.get("method") in _STATE_CHANGING_METHOD_VALUES
            and origin is not None
            and not _origin_is_allowed(origin=origin)
        ):
            await _denied_response(
                status_code=403,
                error_code="cross_origin_request_denied",
                message="Cross-origin Control Panel mutations are not allowed.",
            )(scope, receive, send)
            return

        await self._app(scope, receive, send)


def _headers(scope: Scope) -> dict[str, str]:
    """Headers. Args: scope. Returns: dict[str, str]."""
    return {
        key.decode("latin-1").lower(): value.decode("latin-1")
        for key, value in scope.get("headers", ())
    }


def _host_is_allowed(scope: Scope, host_header: str | None) -> bool:
    """Host is allowed. Args: scope, host_header. Returns: bool."""
    hostname = _hostname(value=host_header)
    if hostname in _LOOPBACK_HOST_VALUES:
        return True
    server = scope.get("server")
    return bool(server and server[0] == "testserver" and hostname == "testserver")


def _origin_is_allowed(origin: str) -> bool:
    """Origin is allowed. Args: origin. Returns: bool."""
    try:
        parsed = urlsplit(origin)
    except ValueError:
        return False
    if parsed.username is not None or parsed.password is not None:
        return False
    if parsed.path not in {"", "/"} or parsed.query or parsed.fragment:
        return False
    if parsed.scheme == "http" and parsed.hostname in _LOOPBACK_HOST_VALUES:
        return True
    return bool(
        parsed.scheme == "chrome-extension"
        and parsed.port is None
        and parsed.hostname is not None
        and _CHROME_EXTENSION_ID.fullmatch(parsed.hostname)
    )


def _hostname(value: str | None) -> str | None:
    """Hostname. Args: value. Returns: str | None."""
    if value is None:
        return None
    try:
        return urlsplit(f"//{value}").hostname
    except ValueError:
        return None


def _denied_response(status_code: int, error_code: str, message: str) -> Response:
    """Denied response. Args: status_code, error_code, message. Returns: Response."""
    payload = ControlPanelErrorResponse(
        error_code=error_code,
        message=message,
        recoverable=False,
    )
    return Response(
        content=payload.model_dump_json(),
        status_code=status_code,
        media_type="application/json",
        headers={"Cache-Control": "no-store", "X-Content-Type-Options": "nosniff"},
    )

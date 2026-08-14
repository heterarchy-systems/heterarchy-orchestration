"""ASGI guard for the loopback-only Control Panel boundary."""

import re
from collections.abc import Awaitable, Callable
from urllib.parse import urlsplit

from starlette.responses import JSONResponse
from starlette.types import Receive, Scope, Send

_LOOPBACK_HOSTS = frozenset({"127.0.0.1", "localhost", "::1"})
_STATE_CHANGING_METHODS = frozenset({"POST", "PUT", "PATCH", "DELETE"})
_CHROME_EXTENSION_ID = re.compile(r"[a-p]{32}")


class LoopbackRequestGuard:
    """Reject DNS-rebinding hosts and cross-origin state-changing requests."""

    def __init__(self, app: Callable[[Scope, Receive, Send], Awaitable[None]]) -> None:
        self._app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
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
            scope.get("method") in _STATE_CHANGING_METHODS
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


def _headers(*, scope: Scope) -> dict[str, str]:
    return {
        key.decode("latin-1").lower(): value.decode("latin-1")
        for key, value in scope.get("headers", ())
    }


def _host_is_allowed(*, scope: Scope, host_header: str | None) -> bool:
    hostname = _hostname(value=host_header)
    if hostname in _LOOPBACK_HOSTS:
        return True
    server = scope.get("server")
    return bool(server and server[0] == "testserver" and hostname == "testserver")


def _origin_is_allowed(*, origin: str) -> bool:
    try:
        parsed = urlsplit(origin)
    except ValueError:
        return False
    if parsed.username is not None or parsed.password is not None:
        return False
    if parsed.path not in {"", "/"} or parsed.query or parsed.fragment:
        return False
    if parsed.scheme == "http" and parsed.hostname in _LOOPBACK_HOSTS:
        return True
    return bool(
        parsed.scheme == "chrome-extension"
        and parsed.port is None
        and parsed.hostname is not None
        and _CHROME_EXTENSION_ID.fullmatch(parsed.hostname)
    )


def _hostname(*, value: str | None) -> str | None:
    if value is None:
        return None
    try:
        return urlsplit(f"//{value}").hostname
    except ValueError:
        return None


def _denied_response(
    *, status_code: int, error_code: str, message: str
) -> JSONResponse:
    return JSONResponse(
        {
            "ok": False,
            "error_code": error_code,
            "message": message,
            "recoverable": False,
        },
        status_code=status_code,
        headers={"Cache-Control": "no-store", "X-Content-Type-Options": "nosniff"},
    )

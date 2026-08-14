import html

from mcp.server.mcpserver import MCPServer
from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse, RedirectResponse, Response

from chattocodex.auth.single_user_oauth_provider import SingleUserOAuthProvider


def register_oauth_owner_routes(
    *,
    server: MCPServer[None],
    provider: SingleUserOAuthProvider,
) -> None:
    @server.custom_route("/healthz", methods=["GET"], include_in_schema=False)
    async def healthz(_request: Request) -> Response:
        return JSONResponse({"ok": True, "name": "chattocodex"})

    @server.custom_route("/oauth/approve", methods=["GET"], include_in_schema=False)
    async def approval_form(request: Request) -> Response:
        pending_id = request.query_params.get("pending_id", "")
        if not pending_id:
            return HTMLResponse(
                "Missing pending authorization request.", status_code=400
            )
        safe_pending_id = html.escape(pending_id, quote=True)
        return HTMLResponse(
            _approval_html(pending_id=safe_pending_id),
            headers={"Cache-Control": "no-store", "X-Content-Type-Options": "nosniff"},
        )

    @server.custom_route("/oauth/approve", methods=["POST"], include_in_schema=False)
    async def approval_submit(request: Request) -> Response:
        form = await request.form()
        pending_id = str(form.get("pending_id", ""))
        owner_token = str(form.get("owner_token", ""))
        redirect_uri = await provider.approve_owner_authorization(
            pending_id=pending_id,
            owner_token=owner_token,
        )
        if redirect_uri is None:
            return HTMLResponse(
                "Authorization denied or expired.",
                status_code=403,
                headers={"Cache-Control": "no-store"},
            )
        return RedirectResponse(
            redirect_uri,
            status_code=302,
            headers={"Cache-Control": "no-store"},
        )


def _approval_html(*, pending_id: str) -> str:
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>ChatToCodex Authorization</title>
</head>
<body>
  <main>
    <h1>Authorize ChatToCodex</h1>
    <p>Enter the local Owner Token to allow this ChatGPT connection.</p>
    <form method="post" action="/oauth/approve">
      <input type="hidden" name="pending_id" value="{pending_id}">
      <label for="owner_token">Owner Token</label>
      <input id="owner_token" name="owner_token" type="password" autocomplete="current-password" required>
      <button type="submit">Authorize</button>
    </form>
  </main>
</body>
</html>"""

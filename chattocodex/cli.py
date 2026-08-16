import argparse
from pathlib import Path

from pydantic import ValidationError

from chattocodex.auth.owner_token_service import OwnerTokenService
from chattocodex.control_panel.control_panel_app import serve_control_panel
from chattocodex.mcp_server import serve_configured
from chattocodex.remote.cloudflare_tunnel import (
    CloudflareTunnelError,
    launch_configured_cloudflare_tunnel,
)
from chattocodex.runtime_settings import RuntimeSettings
from chattocodex.schemas.setup_schemas import SetupRequest
from chattocodex.setup.runtime_setup_service import RuntimeSetupService
from chattocodex.wake.worker_browser_runtime_dto import WorkerBrowserRuntimeMode
from chattocodex.wake.worker_browser_runtime_service import WorkerBrowserRuntimeService


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="chattocodex",
        description="Configure, diagnose, and run the ChatToCodex MCP server.",
    )
    parser.add_argument(
        "--state-root",
        type=Path,
        default=None,
        help="Runtime state directory. Defaults to ~/.chattocodex.",
    )
    subcommands = parser.add_subparsers(dest="command", required=True)

    setup_parser = subcommands.add_parser(
        "setup",
        help="Configure workspace root, public URL, and OAuth.",
    )
    setup_parser.add_argument("--workspace-root", type=Path, required=True)
    setup_parser.add_argument("--public-url")
    setup_parser.add_argument(
        "--no-oauth",
        action="store_true",
        help="Run without OAuth. Intended for local integration tests only.",
    )

    subcommands.add_parser("doctor", help="Validate saved runtime configuration.")
    serve_parser = subcommands.add_parser(
        "serve", help="Start the Streamable HTTP MCP server."
    )
    serve_parser.add_argument("--host", default=None)
    serve_parser.add_argument("--port", type=int, default=None)

    app_parser = subcommands.add_parser(
        "app",
        help="Open the local ChatToCodex settings Control Panel.",
    )
    app_parser.add_argument("--host", default="127.0.0.1")
    app_parser.add_argument("--port", type=int, default=8790)
    app_parser.add_argument("--no-open", action="store_true")

    worker_browser_parser = subcommands.add_parser(
        "worker-browser",
        help="Run the persistent Project Worker Chat browser transport.",
    )
    worker_browser_parser.add_argument("--chrome-binary", type=Path, default=None)
    worker_browser_parser.add_argument("--profile-root", type=Path, default=None)
    worker_browser_parser.add_argument(
        "--visible-bootstrap",
        action="store_true",
        help=(
            "Open the persistent Worker profile visibly for one-time ChatGPT login "
            "or provider setup. Normal Worker dispatch should run headless."
        ),
    )

    tunnel_parser = subcommands.add_parser(
        "tunnel",
        help="Run cloudflared for the hostname saved by setup.",
    )
    tunnel_parser.add_argument("--host", default=None)
    tunnel_parser.add_argument("--port", type=int, default=None)

    owner_token_parser = subcommands.add_parser(
        "owner-token",
        help="Manage the local Owner Token used to approve OAuth connections.",
    )
    owner_token_commands = owner_token_parser.add_subparsers(
        dest="owner_token_command",
        required=True,
    )
    owner_token_commands.add_parser(
        "rotate",
        help="Generate and print a new Owner Token once.",
    )
    return parser


def _run_setup(*, args: argparse.Namespace, settings: RuntimeSettings) -> int:
    try:
        request = SetupRequest(
            workspace_root=args.workspace_root,
            public_url=args.public_url,
            oauth_enabled=not args.no_oauth,
        )
        result = RuntimeSetupService(state_root=settings.state_root).configure(
            request=request
        )
    except (ValidationError, ValueError) as error:
        print(f"setup failed: {error}")
        return 2
    print(result.model_dump_json(indent=2))
    if result.owner_token_created:
        print("Store the Owner Token securely. It is only returned at creation time.")
    return 0


def _run_doctor(*, settings: RuntimeSettings) -> int:
    result = RuntimeSetupService(state_root=settings.state_root).doctor()
    print(result.model_dump_json(indent=2))
    return 0 if result.ok else 1


def _rotate_owner_token(*, settings: RuntimeSettings) -> int:
    owner_token = OwnerTokenService(state_root=settings.state_root).generate()
    print(owner_token)
    print(
        "Store the new Owner Token securely. The previous Owner Token is invalid now."
    )
    return 0


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    base_settings = RuntimeSettings()
    state_root = (
        args.state_root.expanduser().resolve()
        if args.state_root is not None
        else base_settings.state_root.expanduser().resolve()
    )
    if args.command == "app":
        serve_control_panel(
            state_root=state_root,
            host=args.host,
            port=args.port,
            open_browser=not args.no_open,
        )
        return
    if args.command == "worker-browser":
        mode = (
            WorkerBrowserRuntimeMode.VISIBLE_BOOTSTRAP
            if args.visible_bootstrap
            else WorkerBrowserRuntimeMode.HEADLESS
        )
        exit_code = WorkerBrowserRuntimeService(state_root=state_root).run_foreground(
            chrome_binary=args.chrome_binary,
            profile_root=args.profile_root,
            mode=mode,
        )
        raise SystemExit(exit_code)

    host = getattr(args, "host", None) or base_settings.host
    port = getattr(args, "port", None) or base_settings.port
    settings = RuntimeSettings(
        state_root=state_root,
        workspace_root=base_settings.workspace_root,
        host=host,
        port=port,
        mcp_path=base_settings.mcp_path,
        oauth_access_token_ttl_seconds=base_settings.oauth_access_token_ttl_seconds,
        oauth_refresh_token_ttl_seconds=base_settings.oauth_refresh_token_ttl_seconds,
        oauth_allowed_redirect_hosts=base_settings.oauth_allowed_redirect_hosts,
    )

    if args.command == "setup":
        raise SystemExit(_run_setup(args=args, settings=settings))
    if args.command == "doctor":
        raise SystemExit(_run_doctor(settings=settings))
    if args.command == "serve":
        serve_configured(settings=settings)
        return
    if args.command == "tunnel":
        try:
            launch_configured_cloudflare_tunnel(
                state_root=settings.state_root,
                local_host=settings.host,
                local_port=settings.port,
            )
        except CloudflareTunnelError as error:
            print(f"tunnel failed: {error}")
            raise SystemExit(2) from error
        return
    if args.command == "owner-token" and args.owner_token_command == "rotate":
        raise SystemExit(_rotate_owner_token(settings=settings))
    parser.error("Unsupported command.")


if __name__ == "__main__":
    main()

from chattocodex.orchestrator_mcp.composition.mcp_server import serve_configured
from chattocodex.shared.settings.runtime_settings import RuntimeSettings


def main() -> None:
    serve_configured(settings=RuntimeSettings())


if __name__ == "__main__":
    main()

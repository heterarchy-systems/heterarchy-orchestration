from chattocodex.orchestrator_mcp.composition.mcp_server import serve_configured
from chattocodex.shared.settings.runtime_settings import RuntimeSettings
from tests.broker_support import FakeWorkBrokerGateway


def main() -> None:
    serve_configured(
        settings=RuntimeSettings(),
        work_broker_gateway=FakeWorkBrokerGateway(),
    )


if __name__ == "__main__":
    main()

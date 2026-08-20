from ipaddress import ip_address

from agent_broker import StaticClusterConfig, StaticClusterNode

from chattocodex.shared.settings.runtime_settings import RuntimeSettings


def build_broker_cluster_config(
    settings: RuntimeSettings,
) -> StaticClusterConfig | None:
    """Build broker cluster config. Args: settings. Returns: StaticClusterConfig | None."""
    if not settings.broker_cluster_auto_routing:
        return None
    try:
        address = ip_address(settings.broker_host)
    except ValueError:
        return None
    if not address.is_loopback:
        return None
    host = str(address)
    broker_ports = settings.broker_cluster_broker_ports
    operations_ports = settings.broker_cluster_operations_ports
    return StaticClusterConfig(
        nodes=(
            StaticClusterNode(
                node_id=1,
                host=host,
                broker_port=broker_ports[0],
                operations_port=operations_ports[0],
            ),
            StaticClusterNode(
                node_id=2,
                host=host,
                broker_port=broker_ports[1],
                operations_port=operations_ports[1],
            ),
            StaticClusterNode(
                node_id=3,
                host=host,
                broker_port=broker_ports[2],
                operations_port=operations_ports[2],
            ),
        ),
        timeout_seconds=settings.broker_timeout_seconds,
    )

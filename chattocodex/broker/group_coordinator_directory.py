from hashlib import sha256

from chattocodex.broker.broker_management_dto import (
    BrokerNodeRole,
    BrokerNodeState,
    BrokerNodeStatus,
)


class GroupCoordinatorDirectory:
    """Resolve one stable coordinator broker for a Consumer Group identity."""

    def __init__(self, *, nodes: tuple[BrokerNodeState, ...]) -> None:
        self._nodes = nodes

    def coordinator_node_id(self, *, group_id: str) -> str:
        candidates = tuple(
            sorted(
                (
                    node
                    for node in self._nodes
                    if node.status is BrokerNodeStatus.ONLINE
                    and BrokerNodeRole.BROKER in node.roles
                ),
                key=lambda node: node.node_id,
            )
        )
        if not candidates:
            raise ValueError(
                "At least one online broker node is required for coordination."
            )
        digest = sha256(group_id.encode("utf-8")).digest()
        index = int.from_bytes(digest[:8], byteorder="big") % len(candidates)
        return candidates[index].node_id


def default_consumer_group_id(*, project_id: str) -> str:
    """Return the stable logical id for today's one-group-per-project topology."""

    return f"{project_id}:default"

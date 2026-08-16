from dataclasses import dataclass
from enum import StrEnum


class BrokerNodeRole(StrEnum):
    BROKER = "broker"
    CONTROLLER = "controller"


class BrokerNodeStatus(StrEnum):
    ONLINE = "online"
    OFFLINE = "offline"


class BrokerClusterTopology(StrEnum):
    EMBEDDED_SINGLE_NODE = "embedded_single_node"
    MULTI_NODE = "multi_node"


@dataclass(frozen=True, slots=True, kw_only=True)
class BrokerNodeState:
    node_id: str
    roles: tuple[BrokerNodeRole, ...]
    status: BrokerNodeStatus


@dataclass(frozen=True, slots=True, kw_only=True)
class GroupCoordinatorAssignment:
    group_id: str
    project_id: str
    coordinator_node_id: str
    generation: int
    member_count: int


@dataclass(frozen=True, slots=True, kw_only=True)
class BrokerNamespaceState:
    project_id: str
    queued_count: int
    ready_count: int
    claimed_count: int
    completed_count: int
    failed_count: int
    idle_slot_count: int
    busy_slot_count: int
    consumer_group_count: int


@dataclass(frozen=True, slots=True, kw_only=True)
class BrokerClusterSnapshot:
    cluster_id: str
    topology: BrokerClusterTopology
    controller_node_id: str
    nodes: tuple[BrokerNodeState, ...]
    namespaces: tuple[BrokerNamespaceState, ...]
    group_coordinators: tuple[GroupCoordinatorAssignment, ...]
    queued_count: int
    ready_count: int
    claimed_count: int
    completed_count: int
    failed_count: int
    idle_slot_count: int
    busy_slot_count: int

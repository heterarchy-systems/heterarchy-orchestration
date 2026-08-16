from pydantic import Field

from chattocodex.broker.broker_management_dto import (
    BrokerClusterTopology,
    BrokerNodeRole,
    BrokerNodeStatus,
)
from chattocodex.schemas.control_panel_schemas import ControlPanelBoundaryModel


class BrokerNodeOperationsView(ControlPanelBoundaryModel):
    node_id: str
    roles: tuple[BrokerNodeRole, ...]
    status: BrokerNodeStatus


class GroupCoordinatorOperationsView(ControlPanelBoundaryModel):
    group_id: str
    project_id: str
    coordinator_node_id: str
    generation: int = Field(ge=0)
    member_count: int = Field(ge=0)


class BrokerNamespaceOperationsView(ControlPanelBoundaryModel):
    project_id: str
    queued_count: int = Field(ge=0)
    ready_count: int = Field(ge=0)
    claimed_count: int = Field(ge=0)
    completed_count: int = Field(ge=0)
    failed_count: int = Field(ge=0)
    idle_slot_count: int = Field(ge=0)
    busy_slot_count: int = Field(ge=0)
    consumer_group_count: int = Field(ge=0)


class BrokerClusterOperationsView(ControlPanelBoundaryModel):
    cluster_id: str
    topology: BrokerClusterTopology
    controller_node_id: str
    nodes: tuple[BrokerNodeOperationsView, ...]
    namespaces: tuple[BrokerNamespaceOperationsView, ...]
    group_coordinators: tuple[GroupCoordinatorOperationsView, ...]
    queued_count: int = Field(ge=0)
    ready_count: int = Field(ge=0)
    claimed_count: int = Field(ge=0)
    completed_count: int = Field(ge=0)
    failed_count: int = Field(ge=0)
    idle_slot_count: int = Field(ge=0)
    busy_slot_count: int = Field(ge=0)

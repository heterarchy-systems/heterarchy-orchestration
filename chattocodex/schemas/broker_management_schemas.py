from pydantic import Field

from chattocodex.schemas.control_panel_schemas import ControlPanelBoundaryModel


class BrokerConsumerGroupOperationsView(ControlPanelBoundaryModel):
    group_id: str
    namespace_id: str
    generation: int = Field(ge=1)
    group_revision: int = Field(ge=1)
    consumer_count: int = Field(ge=0)


class BrokerNamespaceOperationsView(ControlPanelBoundaryModel):
    project_id: str
    queued_count: int = Field(ge=0)
    ready_count: int = Field(ge=0)
    claimed_count: int = Field(ge=0)
    completed_count: int = Field(ge=0)
    failed_count: int = Field(ge=0)
    idle_slot_count: int = Field(ge=0)
    busy_slot_count: int = Field(ge=0)


class BrokerOperationsView(ControlPanelBoundaryModel):
    term: int = Field(ge=1)
    revision: int = Field(ge=0)
    namespaces: tuple[BrokerNamespaceOperationsView, ...]
    consumer_groups: tuple[BrokerConsumerGroupOperationsView, ...]
    queued_count: int = Field(ge=0)
    ready_count: int = Field(ge=0)
    claimed_count: int = Field(ge=0)
    completed_count: int = Field(ge=0)
    failed_count: int = Field(ge=0)
    idle_slot_count: int = Field(ge=0)
    busy_slot_count: int = Field(ge=0)

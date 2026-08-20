from typing import Annotated

from chattocodex.shared.contracts.control_panel.control_panel_schemas import (
    ControlPanelBoundaryModel,
)
from chattocodex.shared.contracts.schema_base import described_field


class BrokerConsumerGroupOperationsView(ControlPanelBoundaryModel):
    group_id: str
    namespace_id: str
    generation: Annotated[
        int,
        described_field(
            description="Generation for this broker consumer group operations view.",
            ge=1,
        ),
    ]
    group_revision: Annotated[
        int,
        described_field(
            description="Group revision for this broker consumer group operations view.",
            ge=1,
        ),
    ]
    consumer_count: Annotated[
        int,
        described_field(
            description="Number of consumer entries represented by this broker consumer group operations view.",
            ge=0,
        ),
    ]


class BrokerNamespaceOperationsView(ControlPanelBoundaryModel):
    project_id: str
    queued_count: Annotated[
        int,
        described_field(
            description="Number of queued entries represented by this broker namespace operations view.",
            ge=0,
        ),
    ]
    ready_count: Annotated[
        int,
        described_field(
            description="Number of ready entries represented by this broker namespace operations view.",
            ge=0,
        ),
    ]
    claimed_count: Annotated[
        int,
        described_field(
            description="Number of claimed entries represented by this broker namespace operations view.",
            ge=0,
        ),
    ]
    completed_count: Annotated[
        int,
        described_field(
            description="Number of completed entries represented by this broker namespace operations view.",
            ge=0,
        ),
    ]
    failed_count: Annotated[
        int,
        described_field(
            description="Number of failed entries represented by this broker namespace operations view.",
            ge=0,
        ),
    ]
    idle_slot_count: Annotated[
        int,
        described_field(
            description="Number of idle slot entries represented by this broker namespace operations view.",
            ge=0,
        ),
    ]
    busy_slot_count: Annotated[
        int,
        described_field(
            description="Number of busy slot entries represented by this broker namespace operations view.",
            ge=0,
        ),
    ]


class BrokerOperationsView(ControlPanelBoundaryModel):
    term: Annotated[
        int, described_field(description="Term for this broker operations view.", ge=1)
    ]
    revision: Annotated[
        int,
        described_field(
            description="Monotonic revision for this broker operations view.", ge=0
        ),
    ]
    namespaces: tuple[BrokerNamespaceOperationsView, ...]
    consumer_groups: tuple[BrokerConsumerGroupOperationsView, ...]
    queued_count: Annotated[
        int,
        described_field(
            description="Number of queued entries represented by this broker operations view.",
            ge=0,
        ),
    ]
    ready_count: Annotated[
        int,
        described_field(
            description="Number of ready entries represented by this broker operations view.",
            ge=0,
        ),
    ]
    claimed_count: Annotated[
        int,
        described_field(
            description="Number of claimed entries represented by this broker operations view.",
            ge=0,
        ),
    ]
    completed_count: Annotated[
        int,
        described_field(
            description="Number of completed entries represented by this broker operations view.",
            ge=0,
        ),
    ]
    failed_count: Annotated[
        int,
        described_field(
            description="Number of failed entries represented by this broker operations view.",
            ge=0,
        ),
    ]
    idle_slot_count: Annotated[
        int,
        described_field(
            description="Number of idle slot entries represented by this broker operations view.",
            ge=0,
        ),
    ]
    busy_slot_count: Annotated[
        int,
        described_field(
            description="Number of busy slot entries represented by this broker operations view.",
            ge=0,
        ),
    ]

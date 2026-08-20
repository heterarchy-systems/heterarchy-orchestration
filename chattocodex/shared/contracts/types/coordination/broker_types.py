"""Reusable Pydantic v2 constrained types for Broker boundaries."""

from typing import Annotated

from pydantic import StringConstraints

from chattocodex.shared.contracts.schema_base import described_field

BrokerTaskId = Annotated[
    str,
    StringConstraints(strict=True, pattern=r"^btask_[0-9a-f]{24}$"),
]
WorkerSlotId = Annotated[
    str,
    StringConstraints(strict=True, pattern=r"^slot_[0-9a-f]{24}$"),
]
BrokerConsumerId = Annotated[
    str,
    StringConstraints(strict=True, pattern=r"^consumer_[0-9a-f]{24}$"),
]
WorkerSlotHandle = Annotated[
    str,
    StringConstraints(strict=True, pattern=r"^slh_[A-Za-z0-9_-]{32,128}$"),
]
SlotLabel = Annotated[
    str,
    StringConstraints(
        strict=True,
        strip_whitespace=True,
        min_length=1,
        max_length=120,
    ),
]
WaitSeconds = Annotated[
    float,
    described_field(
        description="Maximum wait duration in seconds for a Broker work claim.",
        ge=0,
        le=30,
    ),
]

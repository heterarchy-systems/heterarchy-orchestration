"""Reusable Pydantic v2 constrained types for orchestration boundaries."""

from typing import Annotated

from pydantic import StringConstraints

from chattocodex.shared.contracts.schema_base import described_field

RunId = Annotated[str, StringConstraints(strict=True, pattern=r"^run_[0-9a-f]{24}$")]
WorkerId = Annotated[
    str,
    StringConstraints(strict=True, pattern=r"^worker_[0-9a-f]{24}$"),
]
TaskId = Annotated[str, StringConstraints(strict=True, pattern=r"^task_[0-9a-f]{24}$")]
RunHandle = Annotated[
    str,
    StringConstraints(strict=True, pattern=r"^rnh_[A-Za-z0-9_-]{32,128}$"),
]
WorkerHandle = Annotated[
    str,
    StringConstraints(strict=True, pattern=r"^wkh_[A-Za-z0-9_-]{32,128}$"),
]
CapabilityDigest = Annotated[
    str,
    StringConstraints(strict=True, pattern=r"^[0-9a-f]{64}$"),
]
Revision = Annotated[
    int,
    described_field(
        description="Non-negative revision used for optimistic concurrency checks.",
        ge=0,
    ),
]
GoalText = Annotated[
    str,
    StringConstraints(
        strict=True,
        strip_whitespace=True,
        min_length=1,
        max_length=10_000,
    ),
]
ObjectiveText = Annotated[
    str,
    StringConstraints(
        strict=True,
        strip_whitespace=True,
        min_length=1,
        max_length=10_000,
    ),
]
SummaryText = Annotated[
    str,
    StringConstraints(
        strict=True,
        strip_whitespace=True,
        min_length=1,
        max_length=20_000,
    ),
]
ResultText = Annotated[
    str,
    StringConstraints(
        strict=True,
        strip_whitespace=True,
        min_length=1,
        max_length=50_000,
    ),
]
RoleText = Annotated[
    str,
    StringConstraints(
        strict=True,
        strip_whitespace=True,
        min_length=1,
        max_length=120,
    ),
]

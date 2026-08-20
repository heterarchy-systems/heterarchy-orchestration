"""Reusable Pydantic v2 constrained types for Worker report boundaries."""

from typing import Annotated

from pydantic import StringConstraints

from chattocodex.shared.contracts.schema_base import described_field

ReportTitle = Annotated[
    str,
    StringConstraints(strict=True, strip_whitespace=True, min_length=1, max_length=200),
]
ReportSummary = Annotated[
    str,
    StringConstraints(
        strict=True,
        strip_whitespace=True,
        min_length=1,
        max_length=4_000,
    ),
]
ReportContent = Annotated[
    str,
    StringConstraints(
        strict=True,
        strip_whitespace=True,
        min_length=1,
        max_length=100_000,
    ),
]
ReportReference = Annotated[
    str,
    StringConstraints(
        strict=True,
        strip_whitespace=True,
        min_length=1,
        max_length=4_096,
    ),
]
ReportReferences = Annotated[
    tuple[ReportReference, ...],
    described_field(
        description="References retained by a worker report.",
        max_length=32,
    ),
]

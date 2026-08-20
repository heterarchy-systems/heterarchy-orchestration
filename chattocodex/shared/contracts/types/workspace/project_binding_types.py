"""Reusable Pydantic v2 constrained types for project-binding boundaries."""

from typing import Annotated

from pydantic import StringConstraints

from chattocodex.shared.contracts.schema_base import described_field

ProjectBindingRevision = Annotated[
    int,
    described_field(
        description="Non-negative project binding revision used for optimistic concurrency checks.",
        ge=0,
    ),
]
ConnectorEpoch = Annotated[
    int,
    described_field(
        description="Non-negative connector epoch used to fence stale connector state.",
        ge=0,
    ),
]
AdapterProvider = Annotated[
    str,
    StringConstraints(strict=True, strip_whitespace=True, min_length=1, max_length=120),
]
AdapterProjectReference = Annotated[
    str,
    StringConstraints(strict=True, strip_whitespace=True, min_length=1, max_length=512),
]
AdapterProjectLabel = Annotated[
    str,
    StringConstraints(strict=True, strip_whitespace=True, min_length=1, max_length=255),
]
AdapterProjectPath = Annotated[
    str,
    StringConstraints(
        strict=True,
        strip_whitespace=True,
        min_length=1,
        max_length=4096,
    ),
]
AdapterAppReference = Annotated[
    str,
    StringConstraints(
        strict=True,
        strip_whitespace=True,
        min_length=1,
        max_length=512,
        pattern=r"^plugin_[A-Za-z0-9_-]+$",
    ),
]
AdapterAppLabel = Annotated[
    str,
    StringConstraints(strict=True, strip_whitespace=True, min_length=1, max_length=255),
]

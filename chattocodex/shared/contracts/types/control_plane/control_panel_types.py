"""Reusable Pydantic v2 constrained types for Control Panel boundaries."""

from typing import Annotated

from pydantic import StringConstraints

WorkspacePathText = Annotated[
    str,
    StringConstraints(
        strict=True,
        strip_whitespace=True,
        min_length=1,
        max_length=4096,
    ),
]
OriginText = Annotated[
    str,
    StringConstraints(
        strict=True,
        strip_whitespace=True,
        min_length=1,
        max_length=2048,
    ),
]
OwnerTokenText = Annotated[
    str,
    StringConstraints(
        strict=True,
        strip_whitespace=True,
        min_length=20,
        max_length=1024,
    ),
]
LogLine = Annotated[
    str,
    StringConstraints(strict=True, max_length=4000),
]

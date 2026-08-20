from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field, RootModel
from pydantic.fields import FieldInfo


class StrictSchemaModel(BaseModel):
    """Provide the backend-wide default for named-field schemas."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        use_enum_values=True,
        validate_default=True,
    )


class StrictRootSchemaModel[RootValueT](RootModel[RootValueT]):
    """Provide the backend-wide default for root-value schemas."""

    model_config = ConfigDict(
        frozen=True,
        strict=True,
        use_enum_values=True,
        validate_default=True,
    )


def described_field(
    description: str,
    ge: int | float | None = None,
    gt: int | float | None = None,
    le: int | float | None = None,
    min_length: int | None = None,
    max_length: int | None = None,
) -> FieldInfo:
    """Create Pydantic field metadata with a mandatory human-readable description. Args: description, ge, gt, le, min_length, max_length. Returns: FieldInfo."""

    normalized_description = description.strip()
    if not normalized_description:
        raise ValueError("Pydantic field description must not be blank")
    return Field(
        description=normalized_description,
        ge=ge,
        gt=gt,
        le=le,
        min_length=min_length,
        max_length=max_length,
    )

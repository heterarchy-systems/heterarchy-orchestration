"""Pydantic v2 base contract for explicit optional query filters."""

from chattocodex.shared.contracts.schema_base import StrictSchemaModel


class FilterSchemaModel(StrictSchemaModel):
    """Centralize nullable query criteria instead of parallel optional parameters."""

    def active_field_names(self) -> tuple[str, ...]:
        """Return filter field names whose values are currently active.

        Returns:
            Field names with non-None values in declaration order. Args: None."""
        return tuple(
            name for name in type(self).model_fields if getattr(self, name) is not None
        )

"""Internal immutable values for local project bindings."""

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


@dataclass(frozen=True, slots=True, kw_only=True)
class ProjectBindingAdapterMetadataState:
    """Validated adapter metadata kept separate from canonical project identity."""

    provider: str | None
    project_ref: str | None
    project_label: str | None
    project_path: str | None
    app_ref: str | None
    app_label: str | None
    observed_at: datetime | None


@dataclass(frozen=True, slots=True, kw_only=True)
class ProjectBindingState:
    """Validated durable state for one local project binding."""

    schema_version: int
    project_id: str
    project_name: str
    project_root: Path
    adapter_metadata: ProjectBindingAdapterMetadataState | None
    connector_epoch: int
    revision: int
    created_at: datetime
    updated_at: datetime

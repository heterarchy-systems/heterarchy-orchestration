"""Mappings between project binding boundary records and internal values."""

from pathlib import Path

from chattocodex.orchestrator_backend.project_runtime.project_binding_dto import (
    ProjectBindingAdapterMetadataState,
    ProjectBindingState,
)
from chattocodex.shared.contracts.project.project_binding_schemas import (
    ProjectBindingAdapterMetadata,
    ProjectBindingRecord,
    ProjectBindingView,
)


def metadata_state_from_schema(
    metadata: ProjectBindingAdapterMetadata | None,
) -> ProjectBindingAdapterMetadataState | None:
    """Convert optional boundary metadata to an immutable internal value.

    Args:
        metadata: Validated adapter metadata from a request or record.

    Returns:
        An internal metadata value, or ``None`` when no observation exists.
    """
    if metadata is None:
        return None
    return ProjectBindingAdapterMetadataState(
        provider=metadata.provider,
        project_ref=metadata.project_ref,
        project_label=metadata.project_label,
        project_path=metadata.project_path,
        app_ref=metadata.app_ref,
        app_label=metadata.app_label,
        observed_at=metadata.observed_at,
    )


def metadata_schema_from_state(
    metadata: ProjectBindingAdapterMetadataState | None,
) -> ProjectBindingAdapterMetadata | None:
    """Convert internal metadata to a validated boundary value.

    Args:
        metadata: Internal adapter observation value.

    Returns:
        A boundary metadata value, or ``None`` when no observation exists.
    """
    if metadata is None:
        return None
    return ProjectBindingAdapterMetadata(
        provider=metadata.provider,
        project_ref=metadata.project_ref,
        project_label=metadata.project_label,
        project_path=metadata.project_path,
        app_ref=metadata.app_ref,
        app_label=metadata.app_label,
        observed_at=metadata.observed_at,
    )


def state_from_record(record: ProjectBindingRecord) -> ProjectBindingState:
    """Convert a durable Pydantic record to an internal binding state.

    Args:
        record: Validated durable binding record.

    Returns:
        The immutable internal binding state.
    """
    return ProjectBindingState(
        schema_version=record.schema_version,
        project_id=record.project_id,
        project_name=record.project_name,
        project_root=Path(record.project_root),
        adapter_metadata=metadata_state_from_schema(record.adapter_metadata),
        connector_epoch=record.connector_epoch,
        revision=record.revision,
        created_at=record.created_at,
        updated_at=record.updated_at,
    )


def record_from_state(state: ProjectBindingState) -> ProjectBindingRecord:
    """Convert internal binding state to a durable Pydantic record.

    Args:
        state: Immutable internal binding state.

    Returns:
        A validated record suitable for atomic persistence.
    """
    return ProjectBindingRecord(
        schema_version=1,
        project_id=state.project_id,
        project_name=state.project_name,
        project_root=str(state.project_root),
        adapter_metadata=metadata_schema_from_state(state.adapter_metadata),
        connector_epoch=state.connector_epoch,
        revision=state.revision,
        created_at=state.created_at,
        updated_at=state.updated_at,
    )


def view_from_state(state: ProjectBindingState) -> ProjectBindingView:
    """Convert internal binding state to the Control Panel response view.

    Args:
        state: Immutable internal binding state.

    Returns:
        An opaque-safe response view containing no task or assignment data.
    """
    return ProjectBindingView(
        project_id=state.project_id,
        project_name=state.project_name,
        project_root=str(state.project_root),
        adapter_metadata=metadata_schema_from_state(state.adapter_metadata),
        connector_epoch=state.connector_epoch,
        revision=state.revision,
        created_at=state.created_at,
        updated_at=state.updated_at,
    )

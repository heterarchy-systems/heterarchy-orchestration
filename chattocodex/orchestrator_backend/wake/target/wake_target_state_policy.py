from datetime import datetime

from chattocodex.orchestrator_backend.wake.wake_dto import (
    WakeTargetState,
    WakeTargetStatus,
)


def transition_wake_target(
    target: WakeTargetState,
    status: WakeTargetStatus,
    bound_slot_id: str | None,
    wake_revision: int,
    now: datetime,
) -> WakeTargetState:
    """Build the next immutable Wake target state revision. Args: target, status, bound_slot_id, wake_revision, now. Returns: WakeTargetState."""

    return WakeTargetState(
        schema_version=target.schema_version,
        wake_target_id=target.wake_target_id,
        project_id=target.project_id,
        registered_connector_epoch=target.registered_connector_epoch,
        status=status,
        bound_slot_id=bound_slot_id,
        wake_revision=wake_revision,
        revision=target.revision + 1,
        created_at=target.created_at,
        updated_at=now,
    )

from chattocodex.orchestrator_backend.wake.wake_dto import WakeTargetState
from chattocodex.shared.contracts.wake.wake_schemas import WakeTargetView


def build_wake_target_view(state: WakeTargetState) -> WakeTargetView:
    """Build wake target view. Args: state. Returns: WakeTargetView."""
    return WakeTargetView(
        wake_target_id=state.wake_target_id,
        project_id=state.project_id,
        registered_connector_epoch=state.registered_connector_epoch,
        status=state.status,
        wake_revision=state.wake_revision,
        revision=state.revision,
        updated_at=state.updated_at,
    )

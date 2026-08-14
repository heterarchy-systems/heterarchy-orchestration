from chattocodex.schemas.wake_schemas import WakeTargetView
from chattocodex.wake.wake_dto import WakeTargetState


def build_wake_target_view(*, state: WakeTargetState) -> WakeTargetView:
    return WakeTargetView(
        wake_target_id=state.wake_target_id,
        project_id=state.project_id,
        registered_connector_epoch=state.registered_connector_epoch,
        status=state.status,
        wake_revision=state.wake_revision,
        revision=state.revision,
        updated_at=state.updated_at,
    )

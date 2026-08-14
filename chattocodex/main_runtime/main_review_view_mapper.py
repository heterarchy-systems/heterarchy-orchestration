from chattocodex.main_runtime.main_review_target_state import MainReviewTargetState
from chattocodex.schemas.main_runtime_schemas import MainReviewTargetView


def build_main_review_target_view(
    *, state: MainReviewTargetState
) -> MainReviewTargetView:
    return MainReviewTargetView(
        main_target_id=state.main_target_id,
        project_id=state.project_id,
        status=state.status,
        wake_revision=state.wake_revision,
        revision=state.revision,
        updated_at=state.updated_at,
    )

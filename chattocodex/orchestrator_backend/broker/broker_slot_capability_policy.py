from chattocodex.orchestrator_backend.broker.broker_dto import WorkerSlotState
from chattocodex.orchestrator_backend.orchestration.orchestration_exceptions import (
    CapabilityDeniedError,
)
from chattocodex.orchestrator_backend.persistence.broker.broker_repository import (
    WorkerSlotRepository,
)
from chattocodex.orchestrator_backend.run_control.run_control_identity import (
    capability_matches,
)


def require_slot_capability(
    slot_repository: WorkerSlotRepository,
    slot_id: str,
    slot_handle: str,
) -> WorkerSlotState:
    """Return one Worker Slot only when its opaque capability digest matches. Args: slot_repository, slot_id, slot_handle. Returns: WorkerSlotState."""

    slot = slot_repository.read(slot_id=slot_id)
    if not capability_matches(
        handle=slot_handle,
        expected_digest=slot.slot_handle_digest,
    ):
        raise CapabilityDeniedError(
            operation="worker_slot_capability",
            resource_id=slot_id,
        )
    return slot

from chattocodex.orchestrator_backend.broker.broker_dto import BrokerRunSnapshot
from chattocodex.orchestrator_backend.broker.broker_view_mapper import (
    build_broker_run_status_success,
)
from chattocodex.orchestrator_backend.facade.facade_schemas import (
    DelegationStatusSuccess,
)


def build_delegation_status_success(
    snapshot: BrokerRunSnapshot,
) -> DelegationStatusSuccess:
    """Map one Broker snapshot to the facade status contract without re-coercion. Args: snapshot. Returns: DelegationStatusSuccess."""

    broker_view = build_broker_run_status_success(snapshot=snapshot)
    return DelegationStatusSuccess(
        run=broker_view.run,
        broker_tasks=broker_view.broker_tasks,
        slots=broker_view.slots,
        ready_count=broker_view.ready_count,
        queued_count=broker_view.queued_count,
        claimed_count=broker_view.claimed_count,
        completed_count=broker_view.completed_count,
        failed_count=broker_view.failed_count,
        idle_slot_count=broker_view.idle_slot_count,
        busy_slot_count=broker_view.busy_slot_count,
        is_complete=broker_view.is_complete,
    )

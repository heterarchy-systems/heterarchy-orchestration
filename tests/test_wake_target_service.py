import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from chattocodex.shared.concurrency.entity_lock_registry import EntityLockRegistry
from chattocodex.orchestrator_backend.orchestration.orchestration_exceptions import (
    ResourceNotFoundError,
)
from chattocodex.orchestrator_backend.persistence.wake.wake_delivery_lease_store import (
    WakeDeliveryLeaseStore,
)
from chattocodex.orchestrator_backend.persistence.wake.wake_presence_repository import (
    WakePresenceRepository,
)
from chattocodex.orchestrator_backend.persistence.wake.wake_target_repository import (
    WakeTargetRepository,
)
from chattocodex.orchestrator_backend.persistence.worker.worker_runtime_presence_store import (
    WorkerRuntimePresenceStore,
)
from chattocodex.orchestrator_backend.wake.wake_dto import WakeTargetStatus
from chattocodex.orchestrator_backend.wake.wake_exceptions import (
    WakeTargetConflictError,
)
from chattocodex.orchestrator_backend.wake.presence.wake_presence_service import (
    WakePresenceService,
)
from chattocodex.orchestrator_backend.wake.target.wake_target_service import (
    WakeTargetService,
)
from chattocodex.orchestrator_backend.worker_runtime.lease.wake_delivery_lease_tracker import (
    WakeDeliveryLeaseTracker,
)
from chattocodex.orchestrator_backend.worker_runtime.presence.worker_runtime_presence_tracker import (
    WorkerRuntimePresenceTracker,
)

_PROJECT_A = "prj_project-a_11111111"
_PROJECT_B = "prj_project-b_22222222"
_SLOT_A = "slot_aaaaaaaaaaaaaaaaaaaaaaaa"
_SLOT_B = "slot_bbbbbbbbbbbbbbbbbbbbbbbb"


class _Clock:
    def __init__(self) -> None:
        self._now = datetime(2026, 8, 11, 0, 0, tzinfo=UTC)

    def __call__(self) -> datetime:
        return self._now

    def advance(self, *, seconds: int) -> None:
        self._now += timedelta(seconds=seconds)


def _service(tmp_path: Path) -> tuple[WakeTargetService, _Clock]:
    clock = _Clock()
    presence_service = WakePresenceService(
        repository=WakePresenceRepository(state_root=tmp_path / "state"),
        now_provider=clock,
    )
    service = WakeTargetService(
        repository=WakeTargetRepository(state_root=tmp_path / "state"),
        presence_service=presence_service,
        runtime_presence_tracker=_runtime_tracker(tmp_path=tmp_path, clock=clock),
        delivery_lease_tracker=WakeDeliveryLeaseTracker(
            store=WakeDeliveryLeaseStore(state_root=tmp_path / "state"),
            now_provider=clock,
        ),
        lock_registry=EntityLockRegistry(),
        now_provider=clock,
    )
    return service, clock


def _runtime_tracker(*, tmp_path: Path, clock: _Clock) -> WorkerRuntimePresenceTracker:
    return WorkerRuntimePresenceTracker(
        store=WorkerRuntimePresenceStore(state_root=tmp_path / "state"),
        now_provider=clock,
    )


def test_register_is_idempotent_for_same_project_and_rejects_cross_project_reuse(
    tmp_path: Path,
) -> None:
    service, _clock = _service(tmp_path)
    target_id = "wtgt_11111111111111111111111111111111"

    first = service.register_target(wake_target_id=target_id, project_id=_PROJECT_A)
    repeated = service.register_target(wake_target_id=target_id, project_id=_PROJECT_A)

    assert repeated == first
    with pytest.raises(WakeTargetConflictError):
        service.register_target(wake_target_id=target_id, project_id=_PROJECT_B)


def test_pre_epoch_persisted_target_migrates_to_zero_and_is_fenced_after_rebind(
    tmp_path: Path,
) -> None:
    state_root = tmp_path / "state"
    target_id = "wtgt_11111111111111111111111111111110"
    target_path = state_root / "wake" / "targets" / f"{target_id}.json"
    target_path.parent.mkdir(parents=True)
    target_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "wake_target_id": target_id,
                "project_id": _PROJECT_A,
                "status": "idle",
                "bound_slot_id": None,
                "wake_revision": 0,
                "revision": 0,
                "created_at": "2026-08-11T00:00:00Z",
                "updated_at": "2026-08-11T00:00:00Z",
            }
        ),
        encoding="utf-8",
    )
    service, _clock = _service(tmp_path)
    current_epoch = [0]
    service.attach_connector_epoch_resolver(lambda _project_id: current_epoch[0])
    WakePresenceService(
        repository=WakePresenceRepository(state_root=state_root)
    ).heartbeat(wake_target_id=target_id)

    migrated = service.get_target(wake_target_id=target_id)
    assert migrated.registered_connector_epoch == 0
    assert service.is_connector_compatible(target=migrated) is True

    service.sync_ready_capacity(project_id=_PROJECT_A, ready_count=1)
    persisted = json.loads(target_path.read_text(encoding="utf-8"))
    assert persisted["registered_connector_epoch"] == 0

    current_epoch[0] = 1
    rebound = service.get_target(wake_target_id=target_id)
    assert service.is_connector_compatible(target=rebound) is False
    with pytest.raises(WakeTargetConflictError, match="different connector epoch"):
        service.reserve_wake_claim(
            wake_target_id=target_id,
            wake_revision=rebound.wake_revision,
        )


def test_unregister_provisional_target_deletes_only_exact_pristine_identity(
    tmp_path: Path,
) -> None:
    service, _clock = _service(tmp_path)
    exact_target_id = "wtgt_11111111111111111111111111111112"
    changed_target_id = "wtgt_11111111111111111111111111111113"
    exact = service.register_target(
        wake_target_id=exact_target_id, project_id=_PROJECT_A
    )
    changed = service.register_target(
        wake_target_id=changed_target_id, project_id=_PROJECT_A
    )
    service.sync_ready_capacity(project_id=_PROJECT_A, ready_count=1)

    assert service.unregister_provisional_target_if_matches(target=exact) is False
    assert (
        service.get_target(wake_target_id=exact_target_id).status
        is WakeTargetStatus.WAKING
    )
    assert service.unregister_provisional_target_if_matches(target=changed) is True
    with pytest.raises(ResourceNotFoundError):
        service.get_target(wake_target_id=changed_target_id)


def test_sync_ready_capacity_wakes_only_needed_idle_targets_and_can_shrink(
    tmp_path: Path,
) -> None:
    service, _clock = _service(tmp_path)
    target_ids = tuple(f"wtgt_{index:032x}" for index in range(1, 4))
    for target_id in target_ids:
        service.register_target(wake_target_id=target_id, project_id=_PROJECT_A)

    awakened = service.sync_ready_capacity(project_id=_PROJECT_A, ready_count=2)

    assert tuple(target.wake_target_id for target in awakened) == target_ids[:2]
    assert all(target.status is WakeTargetStatus.WAKING for target in awakened)
    assert all(target.wake_revision == 1 for target in awakened)
    assert (
        service.get_target(wake_target_id=target_ids[2]).status is WakeTargetStatus.IDLE
    )

    service.sync_ready_capacity(project_id=_PROJECT_A, ready_count=1)

    assert (
        service.get_target(wake_target_id=target_ids[0]).status
        is WakeTargetStatus.WAKING
    )
    assert (
        service.get_target(wake_target_id=target_ids[1]).status is WakeTargetStatus.IDLE
    )


def test_bind_and_release_keep_physical_target_bound_to_reusable_slot(
    tmp_path: Path,
) -> None:
    service, _clock = _service(tmp_path)
    target_id = "wtgt_22222222222222222222222222222222"
    service.register_target(wake_target_id=target_id, project_id=_PROJECT_A)
    service.sync_ready_capacity(project_id=_PROJECT_A, ready_count=1)

    busy = service.bind_target(
        wake_target_id=target_id,
        project_id=_PROJECT_A,
        slot_id=_SLOT_A,
        has_assignment=True,
    )
    assert busy.status is WakeTargetStatus.BUSY
    assert busy.bound_slot_id == _SLOT_A

    with pytest.raises(WakeTargetConflictError):
        service.bind_target(
            wake_target_id=target_id,
            project_id=_PROJECT_A,
            slot_id=_SLOT_B,
            has_assignment=True,
        )

    idle = service.release_slot(slot_id=_SLOT_A)
    assert idle is not None
    assert idle.status is WakeTargetStatus.IDLE
    assert idle.bound_slot_id == _SLOT_A

    refreshed = service.refresh_bound_slot(
        slot_id=_SLOT_A,
        project_id=_PROJECT_A,
        has_assignment=True,
    )
    assert refreshed is not None
    assert refreshed.status is WakeTargetStatus.BUSY


@pytest.mark.parametrize("status", (WakeTargetStatus.IDLE, WakeTargetStatus.WAKING))
def test_retire_unclaimed_target_rejects_bound_target_without_changing_state(
    tmp_path: Path,
    status: WakeTargetStatus,
) -> None:
    service, _clock = _service(tmp_path)
    target_id = "wtgt_22222222222222222222222222222223"
    service.register_target(wake_target_id=target_id, project_id=_PROJECT_A)
    service.bind_target(
        wake_target_id=target_id,
        project_id=_PROJECT_A,
        slot_id=_SLOT_A,
        has_assignment=False,
    )
    if status is WakeTargetStatus.WAKING:
        service.sync_ready_capacity(project_id=_PROJECT_A, ready_count=1)
    before = service.get_target(wake_target_id=target_id)
    assert before.status is status
    presence_repository = WakePresenceRepository(state_root=tmp_path / "state")
    presence_before = presence_repository.read(wake_target_id=target_id)
    lease_store = WakeDeliveryLeaseStore(state_root=tmp_path / "state")
    lease_before = (
        lease_store.read(wake_target_id=target_id)
        if lease_store.exists(wake_target_id=target_id)
        else None
    )
    before_delete_calls = []

    with pytest.raises(WakeTargetConflictError, match="bound to a reusable Slot"):
        service.retire_unclaimed_target(
            wake_target_id=target_id,
            expected_revision=before.revision,
            before_delete=before_delete_calls.append,
        )

    assert service.get_target(wake_target_id=target_id) == before
    assert presence_repository.read(wake_target_id=target_id) == presence_before
    if lease_before is None:
        assert not lease_store.exists(wake_target_id=target_id)
    else:
        assert lease_store.read(wake_target_id=target_id) == lease_before
    assert before_delete_calls == []


def test_sync_ready_capacity_ignores_stale_registered_targets(tmp_path: Path) -> None:
    service, clock = _service(tmp_path)
    stale_target = "wtgt_00000000000000000000000000000011"
    active_target = "wtgt_00000000000000000000000000000012"
    service.register_target(wake_target_id=stale_target, project_id=_PROJECT_A)
    clock.advance(seconds=31)
    service.register_target(wake_target_id=active_target, project_id=_PROJECT_A)

    awakened = service.sync_ready_capacity(project_id=_PROJECT_A, ready_count=1)

    assert tuple(target.wake_target_id for target in awakened) == (active_target,)
    assert (
        service.get_target(wake_target_id=stale_target).status is WakeTargetStatus.IDLE
    )


def test_dormant_runtime_target_can_be_woken_without_open_chat_tab(
    tmp_path: Path,
) -> None:
    service, clock = _service(tmp_path)
    target_id = "wtgt_00000000000000000000000000000061"
    service.register_target(wake_target_id=target_id, project_id=_PROJECT_A)
    clock.advance(seconds=31)
    _runtime_tracker(tmp_path=tmp_path, clock=clock).heartbeat(wake_target_id=target_id)

    awakened = service.sync_ready_capacity(project_id=_PROJECT_A, ready_count=1)

    assert tuple(target.wake_target_id for target in awakened) == (target_id,)
    assert awakened[0].status is WakeTargetStatus.WAKING


def test_active_chat_target_is_preferred_over_older_dormant_target(
    tmp_path: Path,
) -> None:
    service, clock = _service(tmp_path)
    dormant_target = "wtgt_00000000000000000000000000000071"
    active_target = "wtgt_00000000000000000000000000000072"
    service.register_target(wake_target_id=dormant_target, project_id=_PROJECT_A)
    clock.advance(seconds=31)
    _runtime_tracker(tmp_path=tmp_path, clock=clock).heartbeat(
        wake_target_id=dormant_target
    )
    service.register_target(wake_target_id=active_target, project_id=_PROJECT_A)

    awakened = service.sync_ready_capacity(project_id=_PROJECT_A, ready_count=1)

    assert tuple(target.wake_target_id for target in awakened) == (active_target,)
    assert (
        service.get_target(wake_target_id=dormant_target).status
        is WakeTargetStatus.IDLE
    )


def test_expired_runtime_presence_does_not_reserve_ready_capacity(
    tmp_path: Path,
) -> None:
    service, clock = _service(tmp_path)
    target_id = "wtgt_00000000000000000000000000000081"
    service.register_target(wake_target_id=target_id, project_id=_PROJECT_A)
    clock.advance(seconds=31)
    _runtime_tracker(tmp_path=tmp_path, clock=clock).heartbeat(wake_target_id=target_id)
    clock.advance(seconds=76)

    awakened = service.sync_ready_capacity(project_id=_PROJECT_A, ready_count=1)

    assert awakened == ()
    assert service.get_target(wake_target_id=target_id).status is WakeTargetStatus.IDLE


def test_expired_wake_delivery_fails_over_to_another_consumer(tmp_path: Path) -> None:
    service, clock = _service(tmp_path)
    first = f"wtgt_{10:032x}"
    second = f"wtgt_{11:032x}"
    runtime = _runtime_tracker(tmp_path=tmp_path, clock=clock)
    for target_id in (first, second):
        service.register_target(wake_target_id=target_id, project_id=_PROJECT_A)
    clock.advance(seconds=31)
    runtime.heartbeat(wake_target_id=first)
    runtime.heartbeat(wake_target_id=second)

    initial = service.sync_ready_capacity(project_id=_PROJECT_A, ready_count=1)
    assert tuple(target.wake_target_id for target in initial) == (first,)
    assert initial[0].wake_revision == 1

    clock.advance(seconds=91)
    runtime.heartbeat(wake_target_id=first)
    runtime.heartbeat(wake_target_id=second)
    recovered = service.sync_ready_capacity(project_id=_PROJECT_A, ready_count=1)

    assert service.get_target(wake_target_id=first).status is WakeTargetStatus.IDLE
    assert service.get_target(wake_target_id=second).status is WakeTargetStatus.WAKING
    assert any(target.wake_target_id == second for target in recovered)
    with pytest.raises(WakeTargetConflictError):
        service.validate_wake_claim(wake_target_id=first, wake_revision=1)
    assert (
        service.validate_wake_claim(wake_target_id=second, wake_revision=1).status
        is WakeTargetStatus.WAKING
    )


def test_single_consumer_retries_after_delivery_backoff(tmp_path: Path) -> None:
    service, clock = _service(tmp_path)
    target_id = f"wtgt_{12:032x}"
    runtime = _runtime_tracker(tmp_path=tmp_path, clock=clock)
    service.register_target(wake_target_id=target_id, project_id=_PROJECT_A)
    clock.advance(seconds=31)
    runtime.heartbeat(wake_target_id=target_id)
    service.sync_ready_capacity(project_id=_PROJECT_A, ready_count=1)

    clock.advance(seconds=91)
    runtime.heartbeat(wake_target_id=target_id)
    expired = service.sync_ready_capacity(project_id=_PROJECT_A, ready_count=1)
    assert all(target.status is not WakeTargetStatus.WAKING for target in expired)
    assert service.get_target(wake_target_id=target_id).status is WakeTargetStatus.IDLE

    clock.advance(seconds=31)
    runtime.heartbeat(wake_target_id=target_id)
    retried = service.sync_ready_capacity(project_id=_PROJECT_A, ready_count=1)
    assert tuple(target.wake_target_id for target in retried) == (target_id,)
    assert retried[0].wake_revision == 2


def test_wake_claim_requires_current_delivery_revision(tmp_path: Path) -> None:
    service, _clock = _service(tmp_path)
    target_id = f"wtgt_{13:032x}"
    service.register_target(wake_target_id=target_id, project_id=_PROJECT_A)
    awakened = service.sync_ready_capacity(project_id=_PROJECT_A, ready_count=1)
    assert awakened[0].wake_revision == 1

    with pytest.raises(WakeTargetConflictError):
        service.validate_wake_claim(wake_target_id=target_id, wake_revision=2)
    assert (
        service.validate_wake_claim(
            wake_target_id=target_id,
            wake_revision=1,
        ).wake_revision
        == 1
    )

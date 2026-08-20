from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from chattocodex.shared.concurrency.entity_lock_registry import EntityLockRegistry
from chattocodex.orchestrator_backend.orchestration.orchestration_exceptions import (
    CapabilityDeniedError,
)
from chattocodex.orchestrator_backend.persistence.worker.worker_wake_capability_repository import (
    WorkerWakeCapabilityRepository,
)
from chattocodex.orchestrator_backend.wake.capability.worker_wake_capability_dto import (
    WorkerWakeCapabilityStatus,
)
from chattocodex.orchestrator_backend.wake.capability.worker_wake_capability_service import (
    WorkerWakeCapabilityService,
)


def _service(
    *,
    tmp_path: Path,
    now: list[datetime],
    epoch: list[int],
    ttl: timedelta = timedelta(minutes=30),
) -> WorkerWakeCapabilityService:
    service = WorkerWakeCapabilityService(
        repository=WorkerWakeCapabilityRepository(state_root=tmp_path / "state"),
        lock_registry=EntityLockRegistry(lock_directory=tmp_path / "locks"),
        now_provider=lambda: now[0],
        capability_ttl=ttl,
    )
    service.attach_connector_epoch_resolver(lambda _project_id: epoch[0])
    return service


def test_worker_wake_capability_is_one_shot_and_binds_slot(tmp_path: Path) -> None:
    now = [datetime(2026, 8, 15, tzinfo=UTC)]
    epoch = [7]
    service = _service(tmp_path=tmp_path, now=now, epoch=epoch)

    issued = service.issue(
        project_id="prj_demo_1234abcd",
        run_id="run_000000000000000000000001",
    )
    assert issued.status is WorkerWakeCapabilityStatus.ISSUED
    assert issued.registered_connector_epoch == 7

    reserved = service.reserve(
        wake_target_id=issued.wake_target_id,
        wake_revision=issued.wake_revision,
    )
    assert reserved.status is WorkerWakeCapabilityStatus.CLAIMING

    consumed = service.commit(
        wake_target_id=issued.wake_target_id,
        wake_revision=issued.wake_revision,
        slot_id="slot_000000000000000000000001",
    )
    assert consumed.status is WorkerWakeCapabilityStatus.CONSUMED
    assert consumed.bound_slot_id == "slot_000000000000000000000001"
    assert service.owns_slot(slot_id="slot_000000000000000000000001") is True

    with pytest.raises(CapabilityDeniedError):
        service.reserve(
            wake_target_id=issued.wake_target_id,
            wake_revision=issued.wake_revision,
        )


def test_worker_wake_capability_rejects_expiry_and_connector_epoch_change(
    tmp_path: Path,
) -> None:
    now = [datetime(2026, 8, 15, tzinfo=UTC)]
    epoch = [3]
    service = _service(
        tmp_path=tmp_path,
        now=now,
        epoch=epoch,
        ttl=timedelta(seconds=5),
    )

    expired = service.issue(
        project_id="prj_demo_1234abcd",
        run_id="run_000000000000000000000002",
    )
    now[0] += timedelta(seconds=6)
    with pytest.raises(CapabilityDeniedError):
        service.reserve(
            wake_target_id=expired.wake_target_id,
            wake_revision=expired.wake_revision,
        )

    now[0] = datetime(2026, 8, 15, 0, 1, tzinfo=UTC)
    epoch[0] = 3
    stale_epoch = service.issue(
        project_id="prj_demo_1234abcd",
        run_id="run_000000000000000000000003",
    )
    epoch[0] = 4
    with pytest.raises(CapabilityDeniedError):
        service.reserve(
            wake_target_id=stale_epoch.wake_target_id,
            wake_revision=stale_epoch.wake_revision,
        )

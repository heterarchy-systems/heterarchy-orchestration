from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Barrier, Event
from typing import cast

import pytest

from chattocodex.consumer_group.capacity_decision import CapacityDecision
from chattocodex.consumer_group.consumer_group_state import ConsumerGroupState
from chattocodex.orchestration.entity_lock_registry import EntityLockRegistry
from chattocodex.orchestration.orchestration_exceptions import (
    OrchestrationError,
    RevisionConflictError,
)
from chattocodex.persistence.project_binding_repository import (
    ProjectBindingRepository,
)
from chattocodex.persistence.worker_provisioning_repository import (
    WorkerProvisioningRepository,
)
from chattocodex.project_runtime.project_binding_service import ProjectBindingService
from chattocodex.provisioning.provisioning_dto import ProvisioningRequestStatus
from chattocodex.provisioning.provisioning_exceptions import (
    ProvisioningConflictError,
)
from chattocodex.provisioning.worker_provisioning_service import (
    WorkerProvisioningService,
)
from chattocodex.schemas.project_binding_schemas import ProjectBindingAdapterMetadata
from chattocodex.wake.wake_target_service import WakeTargetService
from chattocodex.workspace.workspace_project_service import WorkspaceProjectService


class _MembershipCoordinator:
    def __init__(self, *, wake_target_id: str) -> None:
        self.wake_target_id = wake_target_id
        self.membership_calls = 0
        self.project_calls = 0

    def reconcile_membership(self, *, project_id: str) -> ConsumerGroupState:
        self.membership_calls += 1
        now = datetime.now(UTC)
        return ConsumerGroupState(
            schema_version=1,
            project_id=project_id,
            generation=1,
            member_ids=(self.wake_target_id,),
            revision=0,
            created_at=now,
            updated_at=now,
        )

    def reconcile_project(self, *, project_id: str) -> object:
        self.project_calls += 1
        return project_id


def _decision(
    *,
    project_id: str,
    spawn_needed: int,
    pending: int = 0,
    global_pending: int = 0,
    global_headroom: int = 4,
    capacity_deficit: int = 4,
) -> CapacityDecision:
    return CapacityDecision(
        project_id=project_id,
        requested_parallelism=4,
        ready_task_count=4,
        project_max_workers=4,
        global_max_workers=4,
        global_headroom=global_headroom,
        recoverable_consumer_count=0,
        busy_consumer_count=0,
        global_busy_consumer_count=0,
        waking_consumer_count=0,
        wake_target_count=0,
        pending_provisioning_count=pending,
        global_pending_provisioning_count=global_pending,
        desired_capacity=4,
        capacity_deficit=capacity_deficit,
        spawn_needed=spawn_needed,
    )


def _bound_provisioning_service(
    *, tmp_path: Path, current: list[datetime]
) -> tuple[
    WorkerProvisioningService,
    WorkerProvisioningRepository,
    ProjectBindingService,
    str,
]:
    workspace_root = tmp_path / "workspace"
    project_root = workspace_root / "alpha"
    project_root.mkdir(parents=True)
    (project_root / "pyproject.toml").write_text(
        "[project]\nname = 'alpha'\n", encoding="utf-8"
    )
    workspace_service = WorkspaceProjectService(workspace_root=workspace_root)
    project = workspace_service.list_projects()[0]
    lock_registry = EntityLockRegistry(lock_directory=tmp_path / "locks")
    binding_service = ProjectBindingService(
        repository=ProjectBindingRepository(state_root=tmp_path / "binding-state"),
        workspace_project_service=workspace_service,
        lock_registry=lock_registry,
        now_provider=lambda: current[0],
    )
    binding = binding_service.upsert_binding(
        project_id=project.project_id,
        metadata=ProjectBindingAdapterMetadata(
            provider="chatgpt",
            project_ref="project-alpha",
            project_path="/g/project-alpha",
            app_ref="plugin_alpha",
            app_label="ChatOrchestration",
        ),
    )
    repository = WorkerProvisioningRepository(
        state_root=tmp_path / "provisioning-state"
    )
    service = WorkerProvisioningService(
        repository=repository,
        project_binding_service=binding_service,
        lock_registry=lock_registry,
        now_provider=lambda: current[0],
    )
    service.upsert_demand(
        project_id=project.project_id,
        desired_capacity=1,
        spawn_needed=1,
        binding_revision=binding.revision,
    )
    return service, repository, binding_service, project.project_id


def test_capacity_decision_replay_is_idempotent(tmp_path) -> None:
    service = WorkerProvisioningService(
        repository=WorkerProvisioningRepository(state_root=tmp_path)
    )
    decision = _decision(project_id="prj_a_12345678", spawn_needed=1)

    service.on_capacity_decision(decision)
    service.on_capacity_decision(decision)

    assert service.pending_provisioning_count("prj_a_12345678") == 1
    assert len(service.list_status()[1]) == 1


def test_two_independent_services_share_one_global_reservation(tmp_path) -> None:
    services = tuple(
        WorkerProvisioningService(
            repository=WorkerProvisioningRepository(state_root=tmp_path)
        )
        for _ in range(2)
    )
    decisions = (
        _decision(project_id="prj_a_12345678", spawn_needed=1, global_headroom=1),
        _decision(project_id="prj_b_12345678", spawn_needed=1, global_headroom=1),
    )

    with ThreadPoolExecutor(max_workers=2) as pool:
        tuple(
            pool.map(
                lambda item: item[0].on_capacity_decision(item[1]),
                zip(services, decisions, strict=True),
            )
        )

    assert services[0].pending_provisioning_count() == 1


def test_global_admission_is_one_across_independent_services(tmp_path) -> None:
    services = tuple(
        WorkerProvisioningService(
            repository=WorkerProvisioningRepository(state_root=tmp_path),
            minimum_admission_interval=timedelta(0),
        )
        for _ in range(2)
    )
    services[0].on_capacity_decision(
        _decision(project_id="prj_a_12345678", spawn_needed=2)
    )

    with ThreadPoolExecutor(max_workers=2) as pool:
        admitted = tuple(pool.map(lambda service: service.admit_next(), services))

    assert sum(item is not None for item in admitted) == 1
    assert len({item.request_id for item in admitted if item is not None}) == 1


def test_registration_is_persistent_and_duplicate_failure_is_strict(tmp_path) -> None:
    repository = WorkerProvisioningRepository(state_root=tmp_path)
    service = WorkerProvisioningService(
        repository=repository, minimum_admission_interval=timedelta(0)
    )
    service.on_capacity_decision(_decision(project_id="prj_a_12345678", spawn_needed=1))
    admitted = service.admit_next()
    assert admitted is not None
    registered = service.register_callback(
        request_id=admitted.request_id,
        conversation_id="conversation-a",
        wake_target_id=admitted.wake_target_id or "",
        admission_revision=admitted.admission_revision or 0,
    )

    restarted = WorkerProvisioningService(repository=repository)
    assert restarted.pending_provisioning_count("prj_a_12345678") == 0
    assert restarted.list_status()[1][0].conversation_id == "conversation-a"
    with pytest.raises(ProvisioningConflictError):
        restarted.fail_callback(
            request_id=registered.request_id,
            admission_revision=registered.admission_revision or 0,
            reason="late_failure",
        )


def test_runtime_ownership_retirement_is_exact_idempotent_and_demand_neutral(
    tmp_path,
) -> None:
    repository = WorkerProvisioningRepository(state_root=tmp_path)
    service = WorkerProvisioningService(
        repository=repository,
        minimum_admission_interval=timedelta(0),
    )
    project_id = "prj_a_12345678"
    service.upsert_demand(
        project_id=project_id,
        desired_capacity=2,
        spawn_needed=2,
    )
    first_admitted = service.admit_next()
    assert first_admitted is not None
    first_active = service.register_callback(
        request_id=first_admitted.request_id,
        conversation_id="conversation-retired",
        wake_target_id=first_admitted.wake_target_id or "",
        admission_revision=first_admitted.admission_revision or 0,
    )
    second_admitted = service.admit_next()
    assert second_admitted is not None

    demand_before = repository.read_state().demands
    admission_before = repository.read_state().admission
    restarted = WorkerProvisioningService(repository=repository)
    with ThreadPoolExecutor(max_workers=2) as pool:
        retired, retired_again = tuple(
            pool.map(
                lambda instance: instance.retire_runtime_ownership(
                    wake_target_id=first_active.wake_target_id or ""
                ),
                (service, restarted),
            )
        )

    assert retired is not None
    assert retired.status is ProvisioningRequestStatus.CANCELLED
    assert retired.failure_reason == "runtime_ownership_lost"
    assert retired_again == retired
    state = repository.read_state()
    request_by_id = {request.request_id: request for request in state.requests}
    assert state.demands == demand_before
    assert state.admission == admission_before
    assert (
        request_by_id[second_admitted.request_id].status
        is ProvisioningRequestStatus.OPENING_PROJECT
    )
    assert service.retire_runtime_ownership(wake_target_id="wake_missing") is None
    assert repository.read_state() == state


def test_expired_admission_gets_a_fresh_revision(tmp_path) -> None:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    current = [now]
    service = WorkerProvisioningService(
        repository=WorkerProvisioningRepository(state_root=tmp_path),
        now_provider=lambda: current[0],
        minimum_admission_interval=timedelta(0),
        admission_lease=timedelta(seconds=1),
    )
    service.on_capacity_decision(_decision(project_id="prj_a_12345678", spawn_needed=1))
    first = service.admit_next()
    assert first is not None
    current[0] += timedelta(seconds=2)

    second = service.admit_next()

    assert second is not None
    assert second.request_id == first.request_id
    assert second.admission_revision != first.admission_revision


def test_two_workers_are_admitted_strictly_sequentially_after_cooldown(
    tmp_path,
) -> None:
    current = [datetime(2026, 1, 1, tzinfo=UTC)]
    service = WorkerProvisioningService(
        repository=WorkerProvisioningRepository(state_root=tmp_path),
        now_provider=lambda: current[0],
    )
    service.on_capacity_decision(_decision(project_id="prj_a_12345678", spawn_needed=2))
    first = service.admit_next()
    assert first is not None
    service.register_callback(
        request_id=first.request_id,
        conversation_id="conversation-1",
        wake_target_id=first.wake_target_id or "",
        admission_revision=first.admission_revision or 0,
    )

    assert service.admit_next() is None
    current[0] += timedelta(seconds=12)
    second = service.admit_next()
    assert second is not None
    assert second.request_id != first.request_id


def test_rate_limit_backoff_advances_30_60_120_and_stays_capped(
    tmp_path,
) -> None:
    current = [datetime(2026, 1, 1, tzinfo=UTC)]
    service = WorkerProvisioningService(
        repository=WorkerProvisioningRepository(state_root=tmp_path),
        now_provider=lambda: current[0],
        minimum_admission_interval=timedelta(0),
    )
    service.on_capacity_decision(_decision(project_id="prj_a_12345678", spawn_needed=1))

    request_id: str | None = None
    previous_admission_revision: int | None = None
    for expected_seconds in (30, 60, 120, 120):
        admitted = service.admit_next()
        assert admitted is not None
        request_id = request_id or admitted.request_id
        assert admitted.request_id == request_id
        assert admitted.admission_revision != previous_admission_revision
        previous_admission_revision = admitted.admission_revision

        _, retry_after_ms = service.fail_callback(
            request_id=admitted.request_id,
            admission_revision=admitted.admission_revision or 0,
            reason="provider_rate_limited",
            rate_limited=True,
        )

        assert retry_after_ms == expected_seconds * 1000
        current[0] += timedelta(seconds=expected_seconds - 1)
        assert service.admit_next() is None
        current[0] += timedelta(seconds=1)


def test_cross_project_runtime_open_cooldown_is_deferred_then_succeeds(
    tmp_path,
) -> None:
    current = [datetime(2026, 1, 1, tzinfo=UTC)]
    repository = WorkerProvisioningRepository(state_root=tmp_path)
    service = WorkerProvisioningService(
        repository=repository,
        now_provider=lambda: current[0],
    )
    service.on_capacity_decision(
        _decision(project_id="prj_project-b_12345678", spawn_needed=1)
    )
    admitted = service.admit_next()
    assert admitted is not None

    deferred, retry_after_ms = service.fail_callback(
        request_id=admitted.request_id,
        admission_revision=admitted.admission_revision or 0,
        reason="browser_open_cooldown",
    )

    assert deferred.status is ProvisioningRequestStatus.DEFERRED
    assert retry_after_ms == 12_000
    assert service.admit_next() is None

    current[0] += timedelta(seconds=12)
    restarted = WorkerProvisioningService(
        repository=repository,
        now_provider=lambda: current[0],
        minimum_admission_interval=timedelta(seconds=12),
    )
    retried = restarted.admit_next()
    assert retried is not None
    assert retried.request_id == admitted.request_id
    registered = restarted.register_callback(
        request_id=retried.request_id,
        conversation_id="conversation-project-b",
        wake_target_id=retried.wake_target_id or "",
        admission_revision=retried.admission_revision or 0,
    )
    assert registered.status is ProvisioningRequestStatus.ACTIVE


def test_pending_worker_retirement_defers_consumed_admission_for_normal_retry(
    tmp_path,
) -> None:
    current = [datetime(2026, 1, 1, tzinfo=UTC)]
    repository = WorkerProvisioningRepository(state_root=tmp_path)
    service = WorkerProvisioningService(
        repository=repository,
        now_provider=lambda: current[0],
    )
    service.on_capacity_decision(
        _decision(project_id="prj_retirement_12345678", spawn_needed=1)
    )
    admitted = service.admit_next()
    assert admitted is not None

    deferred, retry_after_ms = service.fail_callback(
        request_id=admitted.request_id,
        admission_revision=admitted.admission_revision or 0,
        reason="worker_retirement_pending",
    )

    assert deferred.status is ProvisioningRequestStatus.DEFERRED
    assert deferred.failure_reason == "worker_retirement_pending"
    assert retry_after_ms == 12_000
    assert service.pending_provisioning_count("prj_retirement_12345678") == 1
    assert service.admit_next() is None

    current[0] += timedelta(seconds=12)
    restarted = WorkerProvisioningService(
        repository=repository,
        now_provider=lambda: current[0],
        minimum_admission_interval=timedelta(seconds=12),
    )
    retried = restarted.admit_next()
    assert retried is not None
    assert retried.request_id == admitted.request_id
    assert retried.admission_revision != admitted.admission_revision


def test_failure_classification_and_exact_duplicate_are_durable(tmp_path) -> None:
    current = [datetime(2026, 1, 1, tzinfo=UTC)]
    service = WorkerProvisioningService(
        repository=WorkerProvisioningRepository(state_root=tmp_path),
        now_provider=lambda: current[0],
    )
    service.on_capacity_decision(_decision(project_id="prj_a_12345678", spawn_needed=1))
    transient = service.admit_next()
    assert transient is not None
    failed, retry = service.fail_callback(
        request_id=transient.request_id,
        admission_revision=transient.admission_revision or 0,
        reason="browser_open_failed",
    )
    duplicate, duplicate_retry = service.fail_callback(
        request_id=transient.request_id,
        admission_revision=transient.admission_revision or 0,
        reason="browser_open_failed",
    )
    assert duplicate == failed
    assert duplicate_retry == retry
    assert service.pending_provisioning_count("prj_a_12345678") == 1

    current[0] += timedelta(seconds=12)
    service.on_capacity_decision(
        _decision(
            project_id="prj_a_12345678",
            spawn_needed=0,
            pending=1,
            global_pending=1,
            capacity_deficit=1,
        )
    )
    assert service.admit_next() is None

    restarted = WorkerProvisioningService(
        repository=WorkerProvisioningRepository(state_root=tmp_path),
        now_provider=lambda: current[0],
        minimum_admission_interval=timedelta(0),
    )
    assert restarted.pending_provisioning_count("prj_a_12345678") == 1
    assert restarted.admit_next() is None
    requests = restarted.list_status()[1]
    assert len(requests) == 1
    assert requests[0].status == "operator_blocked"


def test_unsupported_identity_remains_visible_and_operator_blocked(tmp_path) -> None:
    service = WorkerProvisioningService(
        repository=WorkerProvisioningRepository(state_root=tmp_path),
        minimum_admission_interval=timedelta(0),
    )
    service.on_capacity_decision(_decision(project_id="prj_a_12345678", spawn_needed=1))
    admitted = service.admit_next()
    assert admitted is not None

    failed, retry_after_ms = service.fail_callback(
        request_id=admitted.request_id,
        admission_revision=admitted.admission_revision or 0,
        reason="unsupported_project_identity",
    )

    assert failed.status is ProvisioningRequestStatus.OPERATOR_BLOCKED
    assert retry_after_ms is None
    assert service.pending_provisioning_count("prj_a_12345678") == 1
    assert service.admit_next() is None


def test_operator_retry_preserves_global_cooldown_and_resets_failure_fields(
    tmp_path: Path,
) -> None:
    current = [datetime(2026, 8, 13, tzinfo=UTC)]
    service, repository, _binding_service, _project_id = _bound_provisioning_service(
        tmp_path=tmp_path, current=current
    )
    admitted = service.admit_next()
    assert admitted is not None
    blocked, _retry_after_ms = service.fail_callback(
        request_id=admitted.request_id,
        admission_revision=admitted.admission_revision or 0,
        reason="app_menu_unavailable",
    )
    admission_before_retry = repository.read_admission()

    queued = service.retry_operator_blocked_request(
        request_id=blocked.request_id,
        expected_revision=blocked.revision,
    )

    assert queued.status is ProvisioningRequestStatus.WAITING_ADMISSION
    assert queued.admission_revision is None
    assert queued.failure_reason is None
    assert queued.wake_target_id == blocked.wake_target_id
    assert repository.read_admission() == admission_before_retry
    assert service.admit_next() is None

    current[0] += timedelta(seconds=12)
    retried = service.admit_next()
    assert retried is not None
    assert retried.request_id == blocked.request_id
    assert retried.admission_revision != blocked.admission_revision


def test_operator_retry_is_request_revision_cas_under_concurrency(
    tmp_path: Path,
) -> None:
    current = [datetime(2026, 8, 13, tzinfo=UTC)]
    service, _repository, _binding_service, _project_id = _bound_provisioning_service(
        tmp_path=tmp_path, current=current
    )
    admitted = service.admit_next()
    assert admitted is not None
    blocked, _retry_after_ms = service.fail_callback(
        request_id=admitted.request_id,
        admission_revision=admitted.admission_revision or 0,
        reason="app_menu_unavailable",
    )

    def retry_once() -> ProvisioningRequestStatus | type[RevisionConflictError]:
        try:
            return service.retry_operator_blocked_request(
                request_id=blocked.request_id,
                expected_revision=blocked.revision,
            ).status
        except RevisionConflictError as error:
            return type(error)

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = tuple(pool.map(lambda _index: retry_once(), range(2)))

    assert results.count(ProvisioningRequestStatus.WAITING_ADMISSION) == 1
    assert results.count(RevisionConflictError) == 1


def test_operator_retry_rejects_stale_project_binding(tmp_path: Path) -> None:
    current = [datetime(2026, 8, 13, tzinfo=UTC)]
    service, repository, binding_service, project_id = _bound_provisioning_service(
        tmp_path=tmp_path, current=current
    )
    admitted = service.admit_next()
    assert admitted is not None
    blocked, _retry_after_ms = service.fail_callback(
        request_id=admitted.request_id,
        admission_revision=admitted.admission_revision or 0,
        reason="app_menu_unavailable",
    )
    binding = binding_service.get_binding(project_id=project_id)
    binding_service.upsert_binding(
        project_id=project_id,
        expected_revision=binding.revision,
        metadata=ProjectBindingAdapterMetadata(
            provider="chatgpt",
            project_ref="project-alpha",
            project_path="/g/project-alpha",
            app_ref="plugin_beta",
            app_label="AnotherApp",
        ),
    )

    with pytest.raises(RevisionConflictError):
        service.retry_operator_blocked_request(
            request_id=blocked.request_id,
            expected_revision=blocked.revision,
        )

    persisted = repository.read_request(request_id=blocked.request_id)
    assert persisted.status is ProvisioningRequestStatus.OPERATOR_BLOCKED
    assert persisted.failure_reason == "app_menu_unavailable"


def test_stale_operator_block_does_not_block_current_binding_admission(
    tmp_path: Path,
) -> None:
    current = [datetime(2026, 8, 13, tzinfo=UTC)]
    service, repository, binding_service, project_id = _bound_provisioning_service(
        tmp_path=tmp_path, current=current
    )
    admitted = service.admit_next()
    assert admitted is not None
    blocked, _retry_after_ms = service.fail_callback(
        request_id=admitted.request_id,
        admission_revision=admitted.admission_revision or 0,
        reason="bootstrap_unavailable",
    )
    binding = binding_service.get_binding(project_id=project_id)
    updated_binding = binding_service.upsert_binding(
        project_id=project_id,
        expected_revision=binding.revision,
        metadata=ProjectBindingAdapterMetadata(
            provider="chatgpt",
            project_ref="project-alpha",
            project_path="/g/project-alpha",
            app_ref="plugin_beta",
            app_label="ChatOrchestration",
        ),
    )
    service.upsert_demand(
        project_id=project_id,
        desired_capacity=2,
        spawn_needed=2,
        binding_revision=updated_binding.revision,
    )
    current[0] += timedelta(seconds=12)

    retried = service.admit_next()

    assert retried is not None
    assert retried.request_id != blocked.request_id
    assert retried.binding_revision == updated_binding.revision
    assert retried.adapter_metadata == updated_binding.adapter_metadata
    persisted_block = repository.read_request(request_id=blocked.request_id)
    assert persisted_block.status is ProvisioningRequestStatus.OPERATOR_BLOCKED


def test_operator_retry_rejects_non_blocked_request(tmp_path: Path) -> None:
    current = [datetime(2026, 8, 13, tzinfo=UTC)]
    service, _repository, _binding_service, _project_id = _bound_provisioning_service(
        tmp_path=tmp_path, current=current
    )
    admitted = service.admit_next()
    assert admitted is not None

    with pytest.raises(ProvisioningConflictError):
        service.retry_operator_blocked_request(
            request_id=admitted.request_id,
            expected_revision=admitted.revision,
        )


def test_admission_holds_project_binding_lock_through_opening_commit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    current = [datetime(2026, 8, 13, tzinfo=UTC)]
    update_entered = Event()
    release_update = Event()
    binding_update_finished = Event()
    service, repository, binding_service, project_id = _bound_provisioning_service(
        tmp_path=tmp_path, current=current
    )
    original_update = repository.update

    def paused_update(*, updater):
        update_entered.set()
        assert release_update.wait(timeout=2)
        return original_update(updater=updater)

    monkeypatch.setattr(repository, "update", paused_update)

    def update_binding() -> None:
        binding = binding_service.get_binding(project_id=project_id)
        binding_service.upsert_binding(
            project_id=project_id,
            expected_revision=binding.revision,
            metadata=ProjectBindingAdapterMetadata(
                provider="chatgpt",
                project_ref="project-alpha",
                project_path="/g/project-alpha",
                app_ref="plugin_beta",
                app_label="RenamedOrchestration",
            ),
        )
        binding_update_finished.set()

    with ThreadPoolExecutor(max_workers=2) as pool:
        admission = pool.submit(service.admit_next)
        assert update_entered.wait(timeout=2)
        binding_update = pool.submit(update_binding)
        assert not binding_update_finished.wait(timeout=0.1)
        release_update.set()

        admitted = admission.result(timeout=2)
        binding_update.result(timeout=2)

    assert admitted is not None
    assert admitted.status is ProvisioningRequestStatus.OPENING_PROJECT
    assert binding_update_finished.is_set()


def test_same_main_observation_does_not_stale_in_flight_registration(
    tmp_path: Path,
) -> None:
    workspace_root = tmp_path / "workspace"
    project_root = workspace_root / "alpha"
    project_root.mkdir(parents=True)
    (project_root / "pyproject.toml").write_text(
        "[project]\nname = 'alpha'\n", encoding="utf-8"
    )
    workspace_service = WorkspaceProjectService(workspace_root=workspace_root)
    project = workspace_service.list_projects()[0]
    binding_service = ProjectBindingService(
        repository=ProjectBindingRepository(state_root=tmp_path / "binding-state"),
        workspace_project_service=workspace_service,
    )
    first_observed_at = datetime(2026, 8, 12, tzinfo=UTC)
    metadata = {
        "provider": "chatgpt",
        "project_ref": "gpt-project-alpha",
        "project_label": "Alpha",
        "project_path": "/g/gpt-project-alpha",
    }
    binding = binding_service.upsert_binding(
        project_id=project.project_id,
        metadata=ProjectBindingAdapterMetadata(
            **metadata,
            observed_at=first_observed_at,
        ),
    )
    service = WorkerProvisioningService(
        repository=WorkerProvisioningRepository(state_root=tmp_path / "provisioning"),
        project_binding_service=binding_service,
        minimum_admission_interval=timedelta(0),
    )
    service.upsert_demand(
        project_id=project.project_id,
        desired_capacity=1,
        spawn_needed=1,
        binding_revision=binding.revision,
    )
    admitted = service.admit_next()
    assert admitted is not None

    reregistered = binding_service.upsert_binding(
        project_id=project.project_id,
        expected_revision=binding.revision,
        metadata=ProjectBindingAdapterMetadata(
            **metadata,
            observed_at=first_observed_at + timedelta(minutes=1),
        ),
    )
    registered = service.register_callback(
        request_id=admitted.request_id,
        conversation_id="conversation-alpha",
        wake_target_id=admitted.wake_target_id or "",
        admission_revision=admitted.admission_revision or 0,
    )

    assert reregistered.revision == binding.revision
    assert registered.status is ProvisioningRequestStatus.ACTIVE


def test_registration_binding_change_during_side_effect_cancels_exact_request(
    tmp_path: Path,
) -> None:
    current = [datetime(2026, 8, 13, tzinfo=UTC)]
    side_effect_started = Event()
    release_side_effect = Event()

    cleaned_targets: list[object] = []

    class PausingWakeTargetService:
        def register_target(
            self,
            *,
            wake_target_id: str,
            project_id: str,
            registered_connector_epoch: int | None = None,
        ) -> object:
            side_effect_started.set()
            assert release_side_effect.wait(timeout=2)
            return (wake_target_id, project_id)

        def unregister_provisional_target_if_matches(self, *, target: object) -> bool:
            cleaned_targets.append(target)
            return True

    service, repository, binding_service, project_id = _bound_provisioning_service(
        tmp_path=tmp_path, current=current
    )
    service._wake_target_service = cast(WakeTargetService, PausingWakeTargetService())
    admitted = service.admit_next()
    assert admitted is not None

    with ThreadPoolExecutor(max_workers=2) as pool:
        registration = pool.submit(
            service.register_callback,
            request_id=admitted.request_id,
            conversation_id="conversation-alpha",
            wake_target_id=admitted.wake_target_id or "",
            admission_revision=admitted.admission_revision or 0,
        )
        assert side_effect_started.wait(timeout=2)
        binding = binding_service.get_binding(project_id=project_id)
        changed = binding_service.upsert_binding(
            project_id=project_id,
            expected_revision=binding.revision,
            metadata=ProjectBindingAdapterMetadata(
                provider="chatgpt",
                project_ref="project-alpha",
                project_path="/g/project-alpha",
                app_ref="plugin_beta",
                app_label="RenamedOrchestration",
            ),
        )
        release_side_effect.set()

        with pytest.raises(ProvisioningConflictError):
            registration.result(timeout=2)

    persisted = repository.read_request(request_id=admitted.request_id)
    assert changed.revision > (admitted.binding_revision or 0)
    assert persisted.status is ProvisioningRequestStatus.CANCELLED
    assert persisted.failure_reason == "project_binding_changed_during_registration"
    assert repository.read_admission().in_flight_request_id is None
    assert service.pending_provisioning_count(project_id) == 1
    assert cleaned_targets == [(admitted.wake_target_id, project_id)]


def test_registration_missing_binding_after_side_effect_cancels_exact_request(
    tmp_path: Path,
) -> None:
    current = [datetime(2026, 8, 13, tzinfo=UTC)]
    side_effect_started = Event()
    release_side_effect = Event()

    cleaned_targets: list[object] = []

    class PausingWakeTargetService:
        def register_target(
            self,
            *,
            wake_target_id: str,
            project_id: str,
            registered_connector_epoch: int | None = None,
        ) -> object:
            side_effect_started.set()
            assert release_side_effect.wait(timeout=2)
            return (wake_target_id, project_id)

        def unregister_provisional_target_if_matches(self, *, target: object) -> bool:
            cleaned_targets.append(target)
            return True

    service, repository, _binding_service, project_id = _bound_provisioning_service(
        tmp_path=tmp_path, current=current
    )
    service._wake_target_service = cast(WakeTargetService, PausingWakeTargetService())
    admitted = service.admit_next()
    assert admitted is not None

    with ThreadPoolExecutor(max_workers=2) as pool:
        registration = pool.submit(
            service.register_callback,
            request_id=admitted.request_id,
            conversation_id="conversation-alpha",
            wake_target_id=admitted.wake_target_id or "",
            admission_revision=admitted.admission_revision or 0,
        )
        assert side_effect_started.wait(timeout=2)
        binding_path = (
            tmp_path / "binding-state" / "project_bindings" / f"{project_id}.json"
        )
        binding_path.unlink()
        release_side_effect.set()

        with pytest.raises(ProvisioningConflictError):
            registration.result(timeout=2)

    persisted = repository.read_request(request_id=admitted.request_id)
    assert persisted.status is ProvisioningRequestStatus.CANCELLED
    assert persisted.failure_reason == "project_binding_changed_during_registration"
    assert repository.read_admission().in_flight_request_id is None
    assert service.pending_provisioning_count(project_id) == 1
    assert cleaned_targets == [(admitted.wake_target_id, project_id)]


def test_membership_failure_compensates_target_before_registration_retry(
    tmp_path: Path,
) -> None:
    membership_started = Event()
    release_membership = Event()

    class TrackingWakeTargetService:
        def __init__(self) -> None:
            self.targets: set[object] = set()

        def register_target(
            self,
            *,
            wake_target_id: str,
            project_id: str,
            registered_connector_epoch: int | None = None,
        ) -> object:
            target = (wake_target_id, project_id, registered_connector_epoch)
            if target in self.targets:
                raise AssertionError("a failed provisional target poisoned retry")
            self.targets.add(target)
            return target

        def unregister_provisional_target_if_matches(self, *, target: object) -> bool:
            if target not in self.targets:
                return False
            self.targets.remove(target)
            return True

    class RacingMembershipCoordinator:
        def __init__(self, *, target_id: str) -> None:
            self.target_id = target_id
            self.accept = False
            self.calls = 0

        def reconcile_membership(self, *, project_id: str) -> ConsumerGroupState:
            self.calls += 1
            if self.calls == 1:
                membership_started.set()
                assert release_membership.wait(timeout=2)
            now = datetime.now(UTC)
            return ConsumerGroupState(
                schema_version=1,
                project_id=project_id,
                generation=self.calls,
                member_ids=(self.target_id,) if self.accept else (),
                revision=self.calls - 1,
                created_at=now,
                updated_at=now,
            )

        def reconcile_project(self, *, project_id: str) -> object:
            return project_id

    current = [datetime(2026, 8, 13, tzinfo=UTC)]
    service, repository, _binding_service, _project_id = _bound_provisioning_service(
        tmp_path=tmp_path, current=current
    )
    wake_service = TrackingWakeTargetService()
    service._wake_target_service = cast(WakeTargetService, wake_service)
    admitted = service.admit_next()
    assert admitted is not None
    coordinator = RacingMembershipCoordinator(target_id=admitted.wake_target_id or "")
    service.attach_consumer_group_coordinator(coordinator)

    with ThreadPoolExecutor(max_workers=1) as pool:
        registration = pool.submit(
            service.register_callback,
            request_id=admitted.request_id,
            conversation_id="conversation-failed",
            wake_target_id=admitted.wake_target_id or "",
            admission_revision=admitted.admission_revision or 0,
        )
        assert membership_started.wait(timeout=2)
        assert len(wake_service.targets) == 1
        release_membership.set()
        with pytest.raises(ProvisioningConflictError):
            registration.result(timeout=2)

    failed = repository.read_request(request_id=admitted.request_id)
    assert failed.status is ProvisioningRequestStatus.CANCELLED
    assert failed.failure_reason == "registration_membership_confirmation_failed"
    assert repository.read_admission().in_flight_request_id is None
    assert wake_service.targets == set()

    current[0] += timedelta(seconds=12)
    retry = service.admit_next()
    assert retry is not None
    coordinator.target_id = retry.wake_target_id or ""
    coordinator.accept = True
    registered = service.register_callback(
        request_id=retry.request_id,
        conversation_id="conversation-retry",
        wake_target_id=retry.wake_target_id or "",
        admission_revision=retry.admission_revision or 0,
    )

    assert registered.status is ProvisioningRequestStatus.ACTIVE
    assert len(wake_service.targets) == 1


def test_admission_requires_complete_consistent_chatgpt_project_binding(
    tmp_path: Path,
) -> None:
    workspace_root = tmp_path / "workspace"
    for name in ("complete", "ref-only", "mismatch"):
        project_root = workspace_root / name
        project_root.mkdir(parents=True)
        (project_root / "pyproject.toml").write_text(
            f"[project]\nname = '{name}'\n", encoding="utf-8"
        )
    workspace_service = WorkspaceProjectService(workspace_root=workspace_root)
    projects = {project.name: project for project in workspace_service.list_projects()}
    binding_service = ProjectBindingService(
        repository=ProjectBindingRepository(state_root=tmp_path / "binding-state"),
        workspace_project_service=workspace_service,
    )
    metadata_by_name = {
        "complete": ProjectBindingAdapterMetadata(
            provider="chatgpt",
            project_ref="project-complete",
            project_path="/g/project-complete",
        ),
        "ref-only": ProjectBindingAdapterMetadata(
            provider="other-provider",
            project_ref="project-ref-only",
        ),
        "mismatch": ProjectBindingAdapterMetadata(
            provider="other-provider",
            project_ref="project-mismatch",
            project_path="/g/different-project",
        ),
    }
    for name, metadata in metadata_by_name.items():
        binding_service.upsert_binding(
            project_id=projects[name].project_id,
            metadata=metadata,
        )

    def admission_for(name: str):
        service = WorkerProvisioningService(
            repository=WorkerProvisioningRepository(
                state_root=tmp_path / f"provisioning-{name}"
            ),
            project_binding_service=binding_service,
            minimum_admission_interval=timedelta(0),
        )
        service.upsert_demand(
            project_id=projects[name].project_id,
            desired_capacity=1,
            spawn_needed=1,
        )
        return service.admit_next()

    assert admission_for("complete") is not None
    assert admission_for("ref-only") is None
    assert admission_for("mismatch") is None


def test_exact_duplicate_registration_revalidates_membership_without_capacity_reconcile(
    tmp_path,
) -> None:
    service = WorkerProvisioningService(
        repository=WorkerProvisioningRepository(state_root=tmp_path),
        minimum_admission_interval=timedelta(0),
    )
    service.on_capacity_decision(_decision(project_id="prj_a_12345678", spawn_needed=1))
    admitted = service.admit_next()
    assert admitted is not None
    coordinator = _MembershipCoordinator(wake_target_id=admitted.wake_target_id or "")
    service.attach_consumer_group_coordinator(coordinator)

    first = service.register_callback(
        request_id=admitted.request_id,
        conversation_id="conversation-1",
        wake_target_id=admitted.wake_target_id or "",
        admission_revision=admitted.admission_revision or 0,
    )
    duplicate = service.register_callback(
        request_id=admitted.request_id,
        conversation_id="conversation-1",
        wake_target_id=admitted.wake_target_id or "",
        admission_revision=admitted.admission_revision or 0,
    )

    assert duplicate == first
    assert coordinator.membership_calls == 2
    assert coordinator.project_calls == 1


def test_no_demand_cancels_in_flight_and_late_callback_is_rejected(tmp_path) -> None:
    service = WorkerProvisioningService(
        repository=WorkerProvisioningRepository(state_root=tmp_path),
        minimum_admission_interval=timedelta(0),
    )
    service.upsert_demand(
        project_id="prj_a_12345678", desired_capacity=1, spawn_needed=1
    )
    admitted = service.admit_next()
    assert admitted is not None

    service.upsert_demand(
        project_id="prj_a_12345678", desired_capacity=0, spawn_needed=0
    )

    assert service.admit_next() is None
    assert service.list_status()[1][0].status == "cancelled"
    with pytest.raises(OrchestrationError):
        service.register_callback(
            request_id=admitted.request_id,
            conversation_id="late",
            wake_target_id=admitted.wake_target_id or "",
            admission_revision=admitted.admission_revision or 0,
        )


def test_reuse_decision_creates_no_new_request_for_second_run(tmp_path) -> None:
    service = WorkerProvisioningService(
        repository=WorkerProvisioningRepository(state_root=tmp_path),
        minimum_admission_interval=timedelta(0),
    )
    service.on_capacity_decision(_decision(project_id="prj_a_12345678", spawn_needed=1))
    admitted = service.admit_next()
    assert admitted is not None
    service.register_callback(
        request_id=admitted.request_id,
        conversation_id="conversation-1",
        wake_target_id=admitted.wake_target_id or "",
        admission_revision=admitted.admission_revision or 0,
    )

    service.on_capacity_decision(
        _decision(
            project_id="prj_a_12345678",
            spawn_needed=0,
            global_headroom=3,
        )
    )

    assert service.admit_next() is None
    assert len(service.list_status()[1]) == 1


def test_registration_membership_callback_can_publish_capacity_without_deadlock(
    tmp_path,
) -> None:
    service = WorkerProvisioningService(
        repository=WorkerProvisioningRepository(state_root=tmp_path),
        minimum_admission_interval=timedelta(0),
    )
    service.on_capacity_decision(_decision(project_id="prj_a_12345678", spawn_needed=1))
    admitted = service.admit_next()
    assert admitted is not None

    class ReentrantCoordinator(_MembershipCoordinator):
        def reconcile_membership(self, *, project_id: str) -> ConsumerGroupState:
            service.on_capacity_decision(
                _decision(
                    project_id=project_id,
                    spawn_needed=0,
                    pending=1,
                    global_pending=1,
                )
            )
            return super().reconcile_membership(project_id=project_id)

    service.attach_consumer_group_coordinator(
        ReentrantCoordinator(wake_target_id=admitted.wake_target_id or "")
    )
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(
            service.register_callback,
            request_id=admitted.request_id,
            conversation_id="conversation-1",
            wake_target_id=admitted.wake_target_id or "",
            admission_revision=admitted.admission_revision or 0,
        )

        assert future.result(timeout=2).status.value == "active"


def test_concurrent_registration_and_capacity_reconciliation_do_not_duplicate(
    tmp_path,
) -> None:
    rendezvous = Barrier(2)
    capacity_done = Event()

    class BlockingWakeTargetService:
        def __init__(self) -> None:
            self.calls = 0
            self.target_ids: list[str] = []

        def register_target(
            self,
            *,
            wake_target_id: str,
            project_id: str,
            registered_connector_epoch: int | None = None,
        ) -> object:
            self.calls += 1
            self.target_ids.append(wake_target_id)
            rendezvous.wait(timeout=1)
            assert capacity_done.wait(timeout=1)
            return project_id

    wake_service = BlockingWakeTargetService()
    repository = WorkerProvisioningRepository(state_root=tmp_path)
    service = WorkerProvisioningService(
        repository=repository,
        wake_target_service=cast(WakeTargetService, wake_service),
        minimum_admission_interval=timedelta(0),
    )
    service.on_capacity_decision(_decision(project_id="prj_a_12345678", spawn_needed=1))
    admitted = service.admit_next()
    assert admitted is not None
    coordinator = _MembershipCoordinator(wake_target_id=admitted.wake_target_id or "")
    service.attach_consumer_group_coordinator(coordinator)

    def reconcile_capacity() -> None:
        try:
            rendezvous.wait(timeout=1)
            service.on_capacity_decision(
                _decision(
                    project_id="prj_a_12345678",
                    spawn_needed=0,
                    pending=1,
                    global_pending=1,
                    capacity_deficit=0,
                )
            )
        finally:
            capacity_done.set()

    with ThreadPoolExecutor(max_workers=2) as pool:
        registration = pool.submit(
            service.register_callback,
            request_id=admitted.request_id,
            conversation_id="conversation-1",
            wake_target_id=admitted.wake_target_id or "",
            admission_revision=admitted.admission_revision or 0,
        )
        reconciliation = pool.submit(reconcile_capacity)

        reconciliation.result(timeout=2)
        registered = registration.result(timeout=2)

    state = repository.read_state()
    assert registered.status is ProvisioningRequestStatus.ACTIVE
    assert service.pending_provisioning_count("prj_a_12345678") == 0
    assert len(state.requests) == 1
    assert state.requests[0].status is ProvisioningRequestStatus.ACTIVE
    assert wake_service.calls == 1
    assert wake_service.target_ids == [admitted.wake_target_id]
    assert coordinator.membership_calls == 1
    assert coordinator.project_calls == 1


def test_terminal_request_history_is_bounded_without_pruning_live_records(
    tmp_path,
) -> None:
    repository = WorkerProvisioningRepository(state_root=tmp_path)
    service = WorkerProvisioningService(
        repository=repository,
        minimum_admission_interval=timedelta(0),
        terminal_request_retention=8,
    )
    service.upsert_demand(
        project_id="prj_a_12345678", desired_capacity=1, spawn_needed=1
    )
    admitted_active = service.admit_next()
    assert admitted_active is not None
    active = service.register_callback(
        request_id=admitted_active.request_id,
        conversation_id="conversation-active",
        wake_target_id=admitted_active.wake_target_id or "",
        admission_revision=admitted_active.admission_revision or 0,
    )

    for _index in range(20):
        service.upsert_demand(
            project_id="prj_a_12345678", desired_capacity=1, spawn_needed=1
        )
        admitted = service.admit_next()
        assert admitted is not None
        service.cancel_request(
            request_id=admitted.request_id,
            admission_revision=admitted.admission_revision or 0,
        )

    service.upsert_demand(
        project_id="prj_a_12345678", desired_capacity=1, spawn_needed=1
    )

    state = repository.read_state()
    terminal = [
        request
        for request in state.requests
        if request.status
        in {ProvisioningRequestStatus.FAILED, ProvisioningRequestStatus.CANCELLED}
    ]
    request_by_id = {request.request_id: request for request in state.requests}
    assert len(terminal) <= 8
    assert request_by_id[active.request_id].status is ProvisioningRequestStatus.ACTIVE
    assert any(
        request.status is ProvisioningRequestStatus.WAITING_ADMISSION
        for request in state.requests
    )
    assert len(state.requests) <= 10

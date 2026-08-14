import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from chattocodex.broker.broker_service import BrokerService
from chattocodex.facade.facade_service import ChatFacadeService
from chattocodex.git.git_command_runner import GitCommandRunner
from chattocodex.main_runtime.main_review_resume_coordinator import (
    MainReviewResumeCoordinator,
)
from chattocodex.main_runtime.main_review_delivery_lease_tracker import (
    MainReviewDeliveryLeaseTracker,
)
from chattocodex.main_runtime.main_review_chat_presence_tracker import (
    MainReviewChatPresenceTracker,
)
from chattocodex.main_runtime.main_review_target_coordinator import (
    MainReviewTargetCoordinator,
)
from chattocodex.main_runtime.main_review_target_state import MainReviewTargetStatus
from chattocodex.orchestration.entity_lock_registry import EntityLockRegistry
from chattocodex.orchestration.orchestration_dto import TaskStatus
from chattocodex.orchestration.orchestration_exceptions import (
    CapabilityDeniedError,
    InvalidTransitionError,
)
from chattocodex.orchestration.run_orchestration_service import RunOrchestrationService
from chattocodex.orchestration.worker_execution_service import WorkerExecutionService
from chattocodex.persistence.broker_repository import (
    BrokerTaskRepository,
    WorkerSlotRepository,
)
from chattocodex.persistence.main_review_target_store import MainReviewTargetStore
from chattocodex.persistence.main_review_delivery_lease_store import (
    MainReviewDeliveryLeaseStore,
)
from chattocodex.persistence.main_review_chat_presence_store import (
    MainReviewChatPresenceStore,
)
from chattocodex.persistence.orchestration_state_repository import (
    RunStateRepository,
    TaskStateRepository,
    WorkerStateRepository,
)
from chattocodex.persistence.run_control_state_store import RunControlStateStore
from chattocodex.persistence.wake_presence_repository import WakePresenceRepository
from chattocodex.persistence.wake_target_repository import WakeTargetRepository
from chattocodex.run_control.run_control_coordinator import RunControlCoordinator
from chattocodex.run_control.run_control_state import (
    WorkflowDecision,
    WorkflowStatus,
    WorkflowTaskSpec,
)
from chattocodex.wake.wake_presence_service import WakePresenceService
from chattocodex.wake.wake_dto import WakeTargetStatus
from chattocodex.wake.wake_target_service import WakeTargetService
from chattocodex.worker_runtime.broker_ready_capacity_projection import (
    BrokerReadyCapacityProjection,
)
from chattocodex.workspace.workspace_project_service import WorkspaceProjectService


class _Clock:
    def __init__(self) -> None:
        self._now = datetime(2026, 8, 11, 0, 0, tzinfo=UTC)

    def __call__(self) -> datetime:
        return self._now

    def advance(self, *, seconds: int) -> None:
        self._now += timedelta(seconds=seconds)


class _Runtime:
    def __init__(self, *, tmp_path: Path, clock=None) -> None:
        self.state_root = tmp_path / "state"
        self.workspace_root = tmp_path / "workspace"
        project_root = self.workspace_root / "sample-project"
        project_root.mkdir(parents=True)
        (project_root / "pyproject.toml").write_text(
            "[project]\nname='sample-project'\nversion='0.1.0'\n",
            encoding="utf-8",
        )
        for args in (
            ("init",),
            ("config", "user.email", "test@example.com"),
            ("config", "user.name", "Test User"),
            ("add", "."),
            ("commit", "-m", "initial"),
        ):
            subprocess.run(
                ("git", "-C", str(project_root), *args),
                check=True,
                capture_output=True,
                text=True,
                timeout=10,
            )

        workspace = WorkspaceProjectService(workspace_root=self.workspace_root)
        self.project_id = workspace.list_projects()[0].project_id
        locks = EntityLockRegistry()
        self.run_repository = RunStateRepository(state_root=self.state_root)
        self.worker_repository = WorkerStateRepository(state_root=self.state_root)
        self.task_repository = TaskStateRepository(state_root=self.state_root)
        self.broker_task_repository = BrokerTaskRepository(state_root=self.state_root)
        self.slot_repository = WorkerSlotRepository(state_root=self.state_root)
        run_service = RunOrchestrationService(
            run_repository=self.run_repository,
            workspace_project_service=workspace,
            command_runner=GitCommandRunner(),
        )
        self.worker_service = WorkerExecutionService(
            run_repository=self.run_repository,
            worker_repository=self.worker_repository,
            task_repository=self.task_repository,
            lock_registry=locks,
        )
        self.broker_service = BrokerService(
            run_repository=self.run_repository,
            worker_repository=self.worker_repository,
            task_repository=self.task_repository,
            broker_task_repository=self.broker_task_repository,
            slot_repository=self.slot_repository,
            workspace_project_service=workspace,
            lock_registry=locks,
        )
        self.ready_capacity_projection = BrokerReadyCapacityProjection(
            broker_task_repository=self.broker_task_repository,
            run_repository=self.run_repository,
        )
        wake_presence_service = WakePresenceService(
            repository=WakePresenceRepository(state_root=self.state_root)
        )
        self.wake_service = WakeTargetService(
            repository=WakeTargetRepository(state_root=self.state_root),
            presence_service=wake_presence_service,
            lock_registry=locks,
        )
        self.run_control_state_store = RunControlStateStore(state_root=self.state_root)
        self.run_control_coordinator = RunControlCoordinator(
            repository=self.run_control_state_store,
            run_service=run_service,
            broker_service=self.broker_service,
            wake_target_service=self.wake_service,
            workspace_project_service=workspace,
            lock_registry=locks,
        )
        self.main_review_target_coordinator = MainReviewTargetCoordinator(
            target_store=MainReviewTargetStore(state_root=self.state_root),
            run_control_state_store=self.run_control_state_store,
            delivery_lease_tracker=MainReviewDeliveryLeaseTracker(
                store=MainReviewDeliveryLeaseStore(state_root=self.state_root),
                now_provider=clock,
            ),
            chat_presence_tracker=MainReviewChatPresenceTracker(
                store=MainReviewChatPresenceStore(state_root=self.state_root),
                now_provider=clock,
            ),
            lock_registry=locks,
            now_provider=clock,
        )
        self.main_review_resume_coordinator = MainReviewResumeCoordinator(
            target_coordinator=self.main_review_target_coordinator,
            run_control_coordinator=self.run_control_coordinator,
        )
        self.facade = ChatFacadeService(
            run_service=run_service,
            broker_service=self.broker_service,
            wake_target_service=self.wake_service,
            workspace_project_service=workspace,
            run_control_coordinator=self.run_control_coordinator,
        )

    def register_wake_target(self, index: int) -> str:
        target_id = f"wtgt_{index:032x}"
        self.wake_service.register_target(
            wake_target_id=target_id,
            project_id=self.project_id,
        )
        return target_id

    def complete_one(self, *, wake_target_id: str):
        self.wake_service.sync_ready_capacity(
            project_id=self.project_id,
            ready_count=self.ready_capacity_projection.ready_count(
                project_id=self.project_id
            ),
        )
        target = self.wake_service.get_target(wake_target_id=wake_target_id)
        assert target.status is WakeTargetStatus.WAKING
        work = self.facade.work(
            project_id=self.project_id,
            slot_id=None,
            slot_handle=None,
            label=None,
            wake_target_id=wake_target_id,
            wake_revision=target.wake_revision,
        )
        assert work.assignment is not None
        assignment = work.assignment
        self.worker_service.update_task(
            run_id=assignment.run.run_id,
            worker_id=assignment.worker.worker_id,
            worker_handle=assignment.worker_handle,
            task_id=assignment.task.task_id,
            expected_revision=assignment.task.revision,
            status=TaskStatus.COMPLETED,
            progress_summary="done",
            result="done",
        )
        self.facade.work_done(slot_id=work.slot.slot_id, slot_handle=work.slot_handle)
        return work


def _parallel_tasks() -> tuple[WorkflowTaskSpec, ...]:
    return (
        WorkflowTaskSpec(
            key="implementation",
            objective="Implement the change.",
            role="implementer",
            depends_on=(),
        ),
        WorkflowTaskSpec(
            key="tests",
            objective="Add regression tests.",
            role="tester",
            depends_on=(),
        ),
    )


def test_workflow_waits_for_main_review_after_last_worker_finishes(
    tmp_path: Path,
) -> None:
    runtime = _Runtime(tmp_path=tmp_path)
    created = runtime.run_control_coordinator.start_workflow(
        goal="Improve project",
        tasks=_parallel_tasks(),
        review_instructions="Review results and tests.",
        max_rework_rounds=2,
        project_id=runtime.project_id,
    )
    assert created.state.requested_parallelism == 2
    assert (
        runtime.run_repository.read(
            run_id=created.state.current_round.run_id
        ).requested_parallelism
        == 2
    )
    target_a = runtime.register_wake_target(1)
    target_b = runtime.register_wake_target(2)

    runtime.complete_one(wake_target_id=target_a)
    mid = runtime.run_control_coordinator.get_status(
        workflow_id=created.state.workflow_id,
        workflow_handle=created.workflow_handle,
    )
    assert mid.workflow.status is WorkflowStatus.RUNNING
    assert mid.needs_main_review is False

    runtime.complete_one(wake_target_id=target_b)
    final = runtime.run_control_coordinator.get_status(
        workflow_id=created.state.workflow_id,
        workflow_handle=created.workflow_handle,
    )
    assert final.workflow.status is WorkflowStatus.WAITING_MAIN_REVIEW
    assert final.needs_main_review is True
    assert final.current_run.completed_count == 2


def test_main_can_approve_completed_round(tmp_path: Path) -> None:
    runtime = _Runtime(tmp_path=tmp_path)
    created = runtime.run_control_coordinator.start_workflow(
        goal="Review project",
        tasks=(
            WorkflowTaskSpec(
                key="review",
                objective="Review.",
                role="reviewer",
                depends_on=(),
            ),
        ),
        review_instructions="Verify the report.",
        max_rework_rounds=1,
        project_id=runtime.project_id,
    )
    runtime.complete_one(wake_target_id=runtime.register_wake_target(10))

    reviewed = runtime.run_control_coordinator.review_workflow(
        workflow_id=created.state.workflow_id,
        workflow_handle=created.workflow_handle,
        decision=WorkflowDecision.APPROVE,
        review_summary="All acceptance criteria are satisfied.",
        rework_tasks=(),
    )
    assert reviewed.snapshot.workflow.status is WorkflowStatus.COMPLETED
    assert reviewed.new_round_started is False


def test_rework_starts_new_execution_round_then_returns_to_review_gate(
    tmp_path: Path,
) -> None:
    runtime = _Runtime(tmp_path=tmp_path)
    created = runtime.run_control_coordinator.start_workflow(
        goal="Improve persistence",
        tasks=(
            WorkflowTaskSpec(
                key="review",
                objective="Review persistence.",
                role="reviewer",
                depends_on=(),
            ),
        ),
        review_instructions="Require persistence regression coverage.",
        max_rework_rounds=2,
        project_id=runtime.project_id,
    )
    first = runtime.complete_one(wake_target_id=runtime.register_wake_target(20))
    assert first.assignment is not None
    first_run_id = first.assignment.run.run_id

    reworked = runtime.run_control_coordinator.review_workflow(
        workflow_id=created.state.workflow_id,
        workflow_handle=created.workflow_handle,
        decision=WorkflowDecision.REWORK,
        review_summary="Delete failure path is missing coverage.",
        rework_tasks=(
            WorkflowTaskSpec(
                key="delete-regression",
                objective="Add delete storage failure regression coverage.",
                role="test-fixer",
                depends_on=(),
            ),
        ),
    )
    assert reworked.new_round_started is True
    assert reworked.snapshot.workflow.status is WorkflowStatus.RUNNING
    assert reworked.snapshot.workflow.rework_rounds == 1
    assert len(reworked.snapshot.workflow.rounds) == 2
    assert reworked.snapshot.workflow.current_round.run_id != first_run_id

    second = runtime.facade.work(
        project_id=runtime.project_id,
        slot_id=first.slot.slot_id,
        slot_handle=first.slot_handle,
        label=None,
    )
    assert second.assignment is not None
    assignment = second.assignment
    runtime.worker_service.update_task(
        run_id=assignment.run.run_id,
        worker_id=assignment.worker.worker_id,
        worker_handle=assignment.worker_handle,
        task_id=assignment.task.task_id,
        expected_revision=assignment.task.revision,
        status=TaskStatus.COMPLETED,
        progress_summary="fixed",
        result="fixed",
    )
    runtime.facade.work_done(
        slot_id=second.slot.slot_id, slot_handle=second.slot_handle
    )

    status = runtime.run_control_coordinator.get_status(
        workflow_id=created.state.workflow_id,
        workflow_handle=created.workflow_handle,
    )
    assert status.workflow.status is WorkflowStatus.WAITING_MAIN_REVIEW
    assert status.current_run.completed_count == 1


def test_rework_round_limit_is_enforced(tmp_path: Path) -> None:
    runtime = _Runtime(tmp_path=tmp_path)
    created = runtime.run_control_coordinator.start_workflow(
        goal="Bounded workflow",
        tasks=(WorkflowTaskSpec(key="a", objective="A", role=None, depends_on=()),),
        review_instructions="Review.",
        max_rework_rounds=0,
        project_id=runtime.project_id,
    )
    runtime.complete_one(wake_target_id=runtime.register_wake_target(30))
    with pytest.raises(InvalidTransitionError, match="maximum rework"):
        runtime.run_control_coordinator.review_workflow(
            workflow_id=created.state.workflow_id,
            workflow_handle=created.workflow_handle,
            decision=WorkflowDecision.REWORK,
            review_summary="Needs more work.",
            rework_tasks=(
                WorkflowTaskSpec(key="b", objective="B", role=None, depends_on=()),
            ),
        )


def test_workflow_capability_is_required(tmp_path: Path) -> None:
    runtime = _Runtime(tmp_path=tmp_path)
    created = runtime.run_control_coordinator.start_workflow(
        goal="Protected workflow",
        tasks=(WorkflowTaskSpec(key="a", objective="A", role=None, depends_on=()),),
        review_instructions="Review.",
        max_rework_rounds=1,
        project_id=runtime.project_id,
    )
    with pytest.raises(CapabilityDeniedError):
        runtime.run_control_coordinator.get_status(
            workflow_id=created.state.workflow_id,
            workflow_handle="wfh_wrong_capability_value_that_is_long_enough",
        )


def test_only_one_main_review_target_claims_a_waiting_workflow(tmp_path: Path) -> None:
    runtime = _Runtime(tmp_path=tmp_path)
    runtime.run_control_coordinator.start_workflow(
        goal="Review once",
        tasks=(WorkflowTaskSpec(key="a", objective="A", role=None, depends_on=()),),
        review_instructions="Verify the result.",
        max_rework_rounds=1,
        project_id=runtime.project_id,
    )
    runtime.complete_one(wake_target_id=runtime.register_wake_target(40))
    first_id = f"mtgt_{1:032x}"
    second_id = f"mtgt_{2:032x}"
    runtime.main_review_target_coordinator.register_target(
        main_target_id=first_id,
        project_id=runtime.project_id,
    )
    runtime.main_review_target_coordinator.register_target(
        main_target_id=second_id,
        project_id=runtime.project_id,
    )

    first = runtime.main_review_target_coordinator.poll_target(main_target_id=first_id)
    second = runtime.main_review_target_coordinator.poll_target(
        main_target_id=second_id
    )

    assert first.status is MainReviewTargetStatus.WAKING
    assert second.status is MainReviewTargetStatus.IDLE


def test_main_auto_resume_rotates_review_capability_and_releases_after_approval(
    tmp_path: Path,
) -> None:
    runtime = _Runtime(tmp_path=tmp_path)
    created = runtime.run_control_coordinator.start_workflow(
        goal="Protected Main review",
        tasks=(WorkflowTaskSpec(key="a", objective="A", role=None, depends_on=()),),
        review_instructions="Verify the result.",
        max_rework_rounds=1,
        project_id=runtime.project_id,
    )
    runtime.complete_one(wake_target_id=runtime.register_wake_target(41))
    main_target_id = f"mtgt_{3:032x}"
    runtime.main_review_target_coordinator.register_target(
        main_target_id=main_target_id,
        project_id=runtime.project_id,
    )
    waking = runtime.main_review_target_coordinator.poll_target(
        main_target_id=main_target_id
    )
    assert waking.status is MainReviewTargetStatus.WAKING

    resumed = runtime.main_review_resume_coordinator.resume_review(
        main_target_id=main_target_id,
        main_wake_revision=waking.wake_revision,
    )
    assert resumed.target.status is MainReviewTargetStatus.ACTIVE
    assert resumed.workflow_handle != created.workflow_handle
    with pytest.raises(CapabilityDeniedError):
        runtime.run_control_coordinator.get_status(
            workflow_id=created.state.workflow_id,
            workflow_handle=created.workflow_handle,
        )

    reviewed = runtime.run_control_coordinator.review_workflow(
        workflow_id=created.state.workflow_id,
        workflow_handle=resumed.workflow_handle,
        decision=WorkflowDecision.APPROVE,
        review_summary="Approved by Main.",
        rework_tasks=(),
    )
    assert reviewed.snapshot.workflow.status is WorkflowStatus.COMPLETED
    released = runtime.main_review_target_coordinator.poll_target(
        main_target_id=main_target_id
    )
    assert released.status is MainReviewTargetStatus.IDLE


def test_main_review_target_wakes_again_after_rework_round(tmp_path: Path) -> None:
    runtime = _Runtime(tmp_path=tmp_path)
    created = runtime.run_control_coordinator.start_workflow(
        goal="Review with rework",
        tasks=(WorkflowTaskSpec(key="a", objective="A", role=None, depends_on=()),),
        review_instructions="Rework once if needed.",
        max_rework_rounds=1,
        project_id=runtime.project_id,
    )
    first_work = runtime.complete_one(wake_target_id=runtime.register_wake_target(42))
    main_target_id = f"mtgt_{4:032x}"
    runtime.main_review_target_coordinator.register_target(
        main_target_id=main_target_id,
        project_id=runtime.project_id,
    )
    waking = runtime.main_review_target_coordinator.poll_target(
        main_target_id=main_target_id
    )
    resumed = runtime.main_review_resume_coordinator.resume_review(
        main_target_id=main_target_id,
        main_wake_revision=waking.wake_revision,
    )
    runtime.run_control_coordinator.review_workflow(
        workflow_id=created.state.workflow_id,
        workflow_handle=resumed.workflow_handle,
        decision=WorkflowDecision.REWORK,
        review_summary="One correction is required.",
        rework_tasks=(
            WorkflowTaskSpec(
                key="fix", objective="Fix it", role="fixer", depends_on=()
            ),
        ),
    )
    idle = runtime.main_review_target_coordinator.poll_target(
        main_target_id=main_target_id
    )
    assert idle.status is MainReviewTargetStatus.IDLE

    second_work = runtime.facade.work(
        project_id=runtime.project_id,
        slot_id=first_work.slot.slot_id,
        slot_handle=first_work.slot_handle,
        label=None,
    )
    assert second_work.assignment is not None
    assignment = second_work.assignment
    runtime.worker_service.update_task(
        run_id=assignment.run.run_id,
        worker_id=assignment.worker.worker_id,
        worker_handle=assignment.worker_handle,
        task_id=assignment.task.task_id,
        expected_revision=assignment.task.revision,
        status=TaskStatus.COMPLETED,
        progress_summary="fixed",
        result="fixed",
    )
    runtime.facade.work_done(
        slot_id=second_work.slot.slot_id,
        slot_handle=second_work.slot_handle,
    )

    waking_again = runtime.main_review_target_coordinator.poll_target(
        main_target_id=main_target_id
    )
    assert waking_again.status is MainReviewTargetStatus.WAKING
    assert waking_again.wake_revision == 2


def test_expired_main_review_wake_fails_over_to_another_main_target(
    tmp_path: Path,
) -> None:
    clock = _Clock()
    runtime = _Runtime(tmp_path=tmp_path, clock=clock)
    runtime.run_control_coordinator.start_workflow(
        goal="Main review failover",
        tasks=(WorkflowTaskSpec(key="a", objective="A", role=None, depends_on=()),),
        review_instructions="Review after failover.",
        max_rework_rounds=1,
        project_id=runtime.project_id,
    )
    runtime.complete_one(wake_target_id=runtime.register_wake_target(50))
    first_id = f"mtgt_{10:032x}"
    second_id = f"mtgt_{11:032x}"
    runtime.main_review_target_coordinator.register_target(
        main_target_id=first_id,
        project_id=runtime.project_id,
    )
    runtime.main_review_target_coordinator.register_target(
        main_target_id=second_id,
        project_id=runtime.project_id,
    )

    first = runtime.main_review_target_coordinator.poll_target(main_target_id=first_id)
    assert first.status is MainReviewTargetStatus.WAKING
    assert first.wake_revision == 1

    clock.advance(seconds=91)
    second = runtime.main_review_target_coordinator.poll_target(
        main_target_id=second_id
    )
    assert second.status is MainReviewTargetStatus.WAKING
    assert second.pending_workflow_id == first.pending_workflow_id
    assert (
        runtime.main_review_target_coordinator.poll_target(
            main_target_id=first_id
        ).status
        is MainReviewTargetStatus.IDLE
    )
    with pytest.raises(InvalidTransitionError, match="not waiting|stale"):
        runtime.main_review_target_coordinator.activate_target(
            main_target_id=first_id,
            main_wake_revision=first.wake_revision,
        )


def test_single_main_target_retries_after_review_delivery_backoff(
    tmp_path: Path,
) -> None:
    clock = _Clock()
    runtime = _Runtime(tmp_path=tmp_path, clock=clock)
    runtime.run_control_coordinator.start_workflow(
        goal="Single Main retry",
        tasks=(WorkflowTaskSpec(key="a", objective="A", role=None, depends_on=()),),
        review_instructions="Review after retry.",
        max_rework_rounds=1,
        project_id=runtime.project_id,
    )
    runtime.complete_one(wake_target_id=runtime.register_wake_target(51))
    main_target_id = f"mtgt_{12:032x}"
    runtime.main_review_target_coordinator.register_target(
        main_target_id=main_target_id,
        project_id=runtime.project_id,
    )
    first = runtime.main_review_target_coordinator.poll_target(
        main_target_id=main_target_id
    )
    assert first.wake_revision == 1

    clock.advance(seconds=91)
    expired = runtime.main_review_target_coordinator.poll_target(
        main_target_id=main_target_id
    )
    assert expired.status is MainReviewTargetStatus.IDLE
    clock.advance(seconds=29)
    cooling = runtime.main_review_target_coordinator.poll_target(
        main_target_id=main_target_id
    )
    assert cooling.status is MainReviewTargetStatus.IDLE
    clock.advance(seconds=2)
    retried = runtime.main_review_target_coordinator.poll_target(
        main_target_id=main_target_id
    )
    assert retried.status is MainReviewTargetStatus.WAKING
    assert retried.wake_revision == 2


def test_main_review_resume_rejects_stale_wake_revision(tmp_path: Path) -> None:
    runtime = _Runtime(tmp_path=tmp_path)
    runtime.run_control_coordinator.start_workflow(
        goal="Reject stale Main wake",
        tasks=(WorkflowTaskSpec(key="a", objective="A", role=None, depends_on=()),),
        review_instructions="Review safely.",
        max_rework_rounds=1,
        project_id=runtime.project_id,
    )
    runtime.complete_one(wake_target_id=runtime.register_wake_target(52))
    main_target_id = f"mtgt_{13:032x}"
    runtime.main_review_target_coordinator.register_target(
        main_target_id=main_target_id,
        project_id=runtime.project_id,
    )
    waking = runtime.main_review_target_coordinator.poll_target(
        main_target_id=main_target_id
    )

    with pytest.raises(InvalidTransitionError, match="stale"):
        runtime.main_review_resume_coordinator.resume_review(
            main_target_id=main_target_id,
            main_wake_revision=waking.wake_revision + 1,
        )
    resumed = runtime.main_review_resume_coordinator.resume_review(
        main_target_id=main_target_id,
        main_wake_revision=waking.wake_revision,
    )
    assert resumed.target.status is MainReviewTargetStatus.ACTIVE


def test_missing_active_main_chat_releases_review_to_another_target(
    tmp_path: Path,
) -> None:
    clock = _Clock()
    runtime = _Runtime(tmp_path=tmp_path, clock=clock)
    runtime.run_control_coordinator.start_workflow(
        goal="Recover dead active Main",
        tasks=(WorkflowTaskSpec(key="a", objective="A", role=None, depends_on=()),),
        review_instructions="Review after active recovery.",
        max_rework_rounds=1,
        project_id=runtime.project_id,
    )
    runtime.complete_one(wake_target_id=runtime.register_wake_target(53))
    first_id = f"mtgt_{14:032x}"
    second_id = f"mtgt_{15:032x}"
    for target_id in (first_id, second_id):
        runtime.main_review_target_coordinator.register_target(
            main_target_id=target_id,
            project_id=runtime.project_id,
        )
    waking = runtime.main_review_target_coordinator.poll_target(main_target_id=first_id)
    active = runtime.main_review_target_coordinator.activate_target(
        main_target_id=first_id,
        main_wake_revision=waking.wake_revision,
    )
    assert active.status is MainReviewTargetStatus.ACTIVE

    clock.advance(seconds=121)
    replacement = runtime.main_review_target_coordinator.poll_target(
        main_target_id=second_id
    )

    assert replacement.status is MainReviewTargetStatus.WAKING
    assert replacement.pending_workflow_id == active.pending_workflow_id
    assert (
        runtime.main_review_target_coordinator.poll_target(
            main_target_id=first_id
        ).status
        is MainReviewTargetStatus.IDLE
    )


def test_active_main_chat_heartbeat_preserves_review_claim(tmp_path: Path) -> None:
    clock = _Clock()
    runtime = _Runtime(tmp_path=tmp_path, clock=clock)
    runtime.run_control_coordinator.start_workflow(
        goal="Keep active Main alive",
        tasks=(WorkflowTaskSpec(key="a", objective="A", role=None, depends_on=()),),
        review_instructions="Long review with heartbeat.",
        max_rework_rounds=1,
        project_id=runtime.project_id,
    )
    runtime.complete_one(wake_target_id=runtime.register_wake_target(54))
    first_id = f"mtgt_{16:032x}"
    second_id = f"mtgt_{17:032x}"
    for target_id in (first_id, second_id):
        runtime.main_review_target_coordinator.register_target(
            main_target_id=target_id,
            project_id=runtime.project_id,
        )
    waking = runtime.main_review_target_coordinator.poll_target(main_target_id=first_id)
    runtime.main_review_target_coordinator.activate_target(
        main_target_id=first_id,
        main_wake_revision=waking.wake_revision,
    )

    clock.advance(seconds=100)
    runtime.main_review_target_coordinator.heartbeat_chat(main_target_id=first_id)
    clock.advance(seconds=100)
    second = runtime.main_review_target_coordinator.poll_target(
        main_target_id=second_id
    )

    assert second.status is MainReviewTargetStatus.IDLE
    assert (
        runtime.main_review_target_coordinator.poll_target(
            main_target_id=first_id
        ).status
        is MainReviewTargetStatus.ACTIVE
    )

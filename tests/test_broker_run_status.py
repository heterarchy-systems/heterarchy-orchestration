import subprocess
from pathlib import Path

from chattocodex.broker.broker_service import BrokerService
from chattocodex.git.git_command_runner import GitCommandRunner
from chattocodex.orchestration.entity_lock_registry import EntityLockRegistry
from chattocodex.orchestration.orchestration_dto import TaskStatus
from chattocodex.orchestration.run_orchestration_service import RunOrchestrationService
from chattocodex.orchestration.worker_execution_service import WorkerExecutionService
from chattocodex.persistence.broker_repository import (
    BrokerTaskRepository,
    WorkerSlotRepository,
)
from chattocodex.persistence.orchestration_state_repository import (
    RunStateRepository,
    TaskStateRepository,
    WorkerStateRepository,
)
from chattocodex.workspace.workspace_project_service import WorkspaceProjectService


def test_broker_run_status_tracks_ready_dag_and_consumer_group(tmp_path: Path) -> None:
    state_root = tmp_path / "state"
    workspace_root = tmp_path / "workspace"
    project_root = workspace_root / "sample-project"
    project_root.mkdir(parents=True)
    (project_root / "pyproject.toml").write_text(
        "[project]\nname = 'sample-project'\nversion = '0.1.0'\n"
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

    workspace_service = WorkspaceProjectService(workspace_root=workspace_root)
    project_id = workspace_service.list_projects()[0].project_id
    run_repository = RunStateRepository(state_root=state_root)
    worker_repository = WorkerStateRepository(state_root=state_root)
    task_repository = TaskStateRepository(state_root=state_root)
    locks = EntityLockRegistry()
    run_service = RunOrchestrationService(
        run_repository=run_repository,
        workspace_project_service=workspace_service,
        command_runner=GitCommandRunner(),
    )
    worker_service = WorkerExecutionService(
        run_repository=run_repository,
        worker_repository=worker_repository,
        task_repository=task_repository,
        lock_registry=locks,
    )
    broker_service = BrokerService(
        run_repository=run_repository,
        worker_repository=worker_repository,
        task_repository=task_repository,
        broker_task_repository=BrokerTaskRepository(state_root=state_root),
        slot_repository=WorkerSlotRepository(state_root=state_root),
        workspace_project_service=workspace_service,
        lock_registry=locks,
    )

    run = run_service.create_run(goal="Broker status test", project_id=project_id)
    slot = broker_service.register_slot(project_id=project_id, label="generic-consumer")
    first = broker_service.create_broker_task(
        run_id=run.state.run_id,
        run_handle=run.run_handle,
        objective="Collect evidence.",
        role="research",
        depends_on=(),
    )
    broker_service.create_broker_task(
        run_id=run.state.run_id,
        run_handle=run.run_handle,
        objective="Synthesize evidence.",
        role="architect",
        depends_on=(first.broker_task_id,),
    )

    initial = broker_service.get_run_status(run_id=run.state.run_id)
    assert initial.queued_count == 2
    assert initial.ready_count == 1
    assert initial.claimed_count == 0
    assert initial.completed_count == 0
    assert initial.idle_slot_count == 1
    assert initial.busy_slot_count == 0
    assert initial.is_complete is False

    assignment = broker_service.claim_task(
        slot_id=slot.state.slot_id,
        slot_handle=slot.slot_handle,
    )
    assert assignment is not None
    claimed = broker_service.get_run_status(run_id=run.state.run_id)
    assert claimed.ready_count == 0
    assert claimed.queued_count == 1
    assert claimed.claimed_count == 1
    assert claimed.idle_slot_count == 0
    assert claimed.busy_slot_count == 1

    worker_service.update_task(
        run_id=assignment.run.run_id,
        worker_id=assignment.worker.worker_id,
        worker_handle=assignment.worker_handle,
        task_id=assignment.task.task_id,
        expected_revision=assignment.task.revision,
        status=TaskStatus.COMPLETED,
        progress_summary="Research completed.",
        result="research-result",
    )
    broker_service.finish_assignment(
        slot_id=slot.state.slot_id,
        slot_handle=slot.slot_handle,
    )
    after_first = broker_service.get_run_status(run_id=run.state.run_id)
    assert after_first.ready_count == 1
    assert after_first.completed_count == 1
    assert after_first.idle_slot_count == 1
    assert after_first.is_complete is False

    second = broker_service.claim_task(
        slot_id=slot.state.slot_id,
        slot_handle=slot.slot_handle,
    )
    assert second is not None
    worker_service.update_task(
        run_id=second.run.run_id,
        worker_id=second.worker.worker_id,
        worker_handle=second.worker_handle,
        task_id=second.task.task_id,
        expected_revision=second.task.revision,
        status=TaskStatus.COMPLETED,
        progress_summary="Architecture completed.",
        result="architecture-result",
    )
    broker_service.finish_assignment(
        slot_id=slot.state.slot_id,
        slot_handle=slot.slot_handle,
    )
    final = broker_service.get_run_status(run_id=run.state.run_id)
    assert final.ready_count == 0
    assert final.queued_count == 0
    assert final.claimed_count == 0
    assert final.completed_count == 2
    assert final.failed_count == 0
    assert final.is_complete is True

from __future__ import annotations

import os
import socket
import subprocess
import time
from pathlib import Path

import pytest
from agent_broker import BrokerClientConfig, OperationsClientConfig

from chattocodex.orchestrator_backend.broker.broker_dto import (
    BrokerTaskStatus,
    WorkerSlotStatus,
)
from chattocodex.orchestrator_backend.broker.broker_service import BrokerService
from chattocodex.orchestrator_backend.broker_client.broker_operations_gateway import (
    AgentBrokerOperationsGateway,
)
from chattocodex.orchestrator_backend.broker_client.external_broker_gateway import (
    AgentBrokerGateway,
)
from chattocodex.orchestrator_backend.version_control.git_command_runner import (
    GitCommandRunner,
)
from chattocodex.shared.concurrency.entity_lock_registry import EntityLockRegistry
from chattocodex.orchestrator_backend.orchestration.orchestration_dto import TaskStatus
from chattocodex.orchestrator_backend.orchestration.run_orchestration_service import (
    RunOrchestrationService,
)
from chattocodex.orchestrator_backend.orchestration.worker_execution_service import (
    WorkerExecutionService,
)
from chattocodex.orchestrator_backend.persistence.broker.broker_repository import (
    BrokerTaskRepository,
    WorkerSlotRepository,
)
from chattocodex.orchestrator_backend.persistence.orchestration.orchestration_state_repository import (
    RunStateRepository,
    TaskStateRepository,
    WorkerStateRepository,
)
from chattocodex.orchestrator_backend.workspace.workspace_project_service import (
    WorkspaceProjectService,
)


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


def _wait_for_port(*, port: int, process: subprocess.Popen[str]) -> None:
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        if process.poll() is not None:
            stderr = process.stderr.read() if process.stderr is not None else ""
            raise AssertionError(f"Broker exited before readiness: {stderr}")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                return
        except OSError:
            time.sleep(0.02)
    raise AssertionError(f"Broker did not listen on port {port}")


def _broker_binary() -> Path:
    configured = os.environ.get("HETERARCHY_BROKER_BINARY")
    if configured:
        binary = Path(configured).expanduser().resolve()
    else:
        binary = (
            Path(__file__).resolve().parents[2]
            / "heterarchy-broker"
            / "target"
            / "debug"
            / "agentbrokerd"
        )
    if not binary.is_file():
        pytest.skip(
            "real Broker E2E requires HETERARCHY_BROKER_BINARY or a sibling "
            "heterarchy-broker/target/debug/agentbrokerd"
        )
    return binary


def _initialize_project(*, workspace_root: Path) -> str:
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
    return (
        WorkspaceProjectService(workspace_root=workspace_root)
        .list_projects()[0]
        .project_id
    )


def test_real_broker_two_consumer_assignment_lifecycle(tmp_path: Path) -> None:
    binary = _broker_binary()
    broker_port = _free_port()
    operations_port = _free_port()
    broker_state = tmp_path / "broker-state.json"
    process = subprocess.Popen(
        (
            str(binary),
            "serve",
            "--host",
            "127.0.0.1",
            "--port",
            str(broker_port),
            "--operations-port",
            str(operations_port),
            "--state-path",
            str(broker_state),
        ),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        _wait_for_port(port=broker_port, process=process)
        _wait_for_port(port=operations_port, process=process)

        state_root = tmp_path / "orchestration-state"
        workspace_root = tmp_path / "workspace"
        project_id = _initialize_project(workspace_root=workspace_root)
        workspace_service = WorkspaceProjectService(workspace_root=workspace_root)
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
        gateway = AgentBrokerGateway(
            config=BrokerClientConfig(
                host="127.0.0.1",
                port=broker_port,
                timeout_seconds=2.0,
            ),
            lease_duration_ms=30_000,
        )
        broker_service = BrokerService(
            work_broker_gateway=gateway,
            run_repository=run_repository,
            worker_repository=worker_repository,
            task_repository=task_repository,
            broker_task_repository=BrokerTaskRepository(state_root=state_root),
            slot_repository=WorkerSlotRepository(state_root=state_root),
            workspace_project_service=workspace_service,
            lock_registry=locks,
        )
        operations = AgentBrokerOperationsGateway(
            config=OperationsClientConfig(
                host="127.0.0.1",
                port=operations_port,
                timeout_seconds=2.0,
            )
        )

        run = run_service.create_run(
            goal="real Broker two-consumer E2E",
            project_id=project_id,
            requested_parallelism=2,
        )
        slot_a = broker_service.register_slot(project_id=project_id, label="worker-a")
        slot_b = broker_service.register_slot(project_id=project_id, label="worker-b")
        task_a = broker_service.create_broker_task(
            run_id=run.state.run_id,
            run_handle=run.run_handle,
            objective="return worker-a",
            role="worker-a",
            depends_on=(),
        )
        task_b = broker_service.create_broker_task(
            run_id=run.state.run_id,
            run_handle=run.run_handle,
            objective="return worker-b",
            role="worker-b",
            depends_on=(),
        )

        assignment_a = broker_service.claim_task_for_run(
            slot_id=slot_a.state.slot_id,
            slot_handle=slot_a.slot_handle,
            run_id=run.state.run_id,
        )
        assignment_b = broker_service.claim_task_for_run(
            slot_id=slot_b.state.slot_id,
            slot_handle=slot_b.slot_handle,
            run_id=run.state.run_id,
        )
        assert assignment_a is not None
        assert assignment_b is not None
        assert assignment_a.broker_task.broker_task_id == task_a.broker_task_id
        assert assignment_b.broker_task.broker_task_id == task_b.broker_task_id
        assert (
            assignment_a.slot.broker_consumer_id != assignment_b.slot.broker_consumer_id
        )

        directory = operations.group_directory()
        group = next(
            group
            for group in directory.groups
            if group.group_id == f"workers_{run.state.run_id}"
        )
        assert group.consumer_count == 2

        broker_service.renew_assignment_lease(slot_id=assignment_b.slot.slot_id)

        for assignment in (assignment_a, assignment_b):
            worker_service.update_task(
                run_id=assignment.run.run_id,
                worker_id=assignment.worker.worker_id,
                worker_handle=assignment.worker_handle,
                task_id=assignment.task.task_id,
                expected_revision=assignment.task.revision,
                status=TaskStatus.COMPLETED,
                progress_summary="real Broker E2E completed",
                result=f"done:{assignment.broker_task.role}",
            )
            finished = broker_service.finish_assignment(
                slot_id=assignment.slot.slot_id,
                slot_handle=slot_a.slot_handle
                if assignment.slot.slot_id == slot_a.state.slot_id
                else slot_b.slot_handle,
            )
            assert finished.broker_task.status is BrokerTaskStatus.COMPLETED
            assert finished.slot.status is WorkerSlotStatus.IDLE

        snapshot = broker_service.get_run_status(run_id=run.state.run_id)
        assert snapshot.completed_count == 2
        assert snapshot.is_complete
    finally:
        process.terminate()
        try:
            process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=3)
        if process.stderr is not None:
            process.stderr.close()

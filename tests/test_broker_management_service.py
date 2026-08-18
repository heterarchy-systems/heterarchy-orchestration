from datetime import UTC, datetime
from pathlib import Path

from chattocodex.broker.broker_dto import (
    BrokerTaskState,
    BrokerTaskStatus,
    WorkerSlotState,
    WorkerSlotStatus,
)
from chattocodex.broker.broker_management_service import BrokerManagementService
from chattocodex.broker_client.broker_operations_gateway import (
    BrokerConsumerGroupDirectory,
    BrokerConsumerGroupView,
)
from chattocodex.orchestration.orchestration_dto import RunState, RunStatus
from chattocodex.persistence.broker_repository import (
    BrokerTaskRepository,
    WorkerSlotRepository,
)
from chattocodex.persistence.orchestration_state_repository import RunStateRepository


class _FakeBrokerOperationsGateway:
    def __init__(self, *, directory: BrokerConsumerGroupDirectory) -> None:
        self._directory = directory

    def group_directory(self) -> BrokerConsumerGroupDirectory:
        return self._directory


def test_broker_management_combines_authoritative_groups_with_local_runtime_projection(
    tmp_path: Path,
) -> None:
    now = datetime.now(UTC)
    execution_root = tmp_path / "execution"
    runs = RunStateRepository(state_root=execution_root)
    tasks = BrokerTaskRepository(state_root=execution_root)
    slots = WorkerSlotRepository(state_root=execution_root)
    project_a = "prj_frontend_1234abcd"
    project_b = "prj_backend_1234abcd"
    run_a = _run(
        now=now,
        run_id="run_111111111111111111111111",
        project_id=project_a,
        name="frontend",
        root=tmp_path / "frontend",
    )
    run_b = _run(
        now=now,
        run_id="run_222222222222222222222222",
        project_id=project_b,
        name="backend",
        root=tmp_path / "backend",
    )
    runs.create(state=run_a)
    runs.create(state=run_b)
    dependency_id = "btask_111111111111111111111111"
    tasks.create(
        state=_broker_task(
            now=now,
            task_id=dependency_id,
            run_id=run_a.run_id,
            status=BrokerTaskStatus.COMPLETED,
        )
    )
    tasks.create(
        state=_broker_task(
            now=now,
            task_id="btask_222222222222222222222222",
            run_id=run_a.run_id,
            status=BrokerTaskStatus.QUEUED,
            depends_on=(dependency_id,),
        )
    )
    tasks.create(
        state=_broker_task(
            now=now,
            task_id="btask_333333333333333333333333",
            run_id=run_b.run_id,
            status=BrokerTaskStatus.CLAIMED,
            assigned_slot_id="slot_222222222222222222222222",
        )
    )
    slots.create(
        state=_slot(
            now=now,
            slot_id="slot_111111111111111111111111",
            project_id=project_a,
            status=WorkerSlotStatus.IDLE,
        )
    )
    slots.create(
        state=_slot(
            now=now,
            slot_id="slot_222222222222222222222222",
            project_id=project_b,
            status=WorkerSlotStatus.BUSY,
        )
    )
    broker_operations = _FakeBrokerOperationsGateway(
        directory=BrokerConsumerGroupDirectory(
            term=6,
            revision=42,
            groups=(
                BrokerConsumerGroupView(
                    group_id="frontend-company",
                    namespace_id=project_a,
                    generation=4,
                    group_revision=19,
                    consumer_count=2,
                ),
                BrokerConsumerGroupView(
                    group_id="backend-company",
                    namespace_id=project_b,
                    generation=7,
                    group_revision=31,
                    consumer_count=3,
                ),
            ),
        )
    )

    snapshot = BrokerManagementService(
        broker_task_repository=tasks,
        slot_repository=slots,
        run_repository=runs,
        broker_operations=broker_operations,
    ).snapshot(project_ids=(project_a, project_b))

    assert snapshot.term == 6
    assert snapshot.revision == 42
    assert {item.group_id for item in snapshot.consumer_groups} == {
        "frontend-company",
        "backend-company",
    }
    assert {item.consumer_count for item in snapshot.consumer_groups} == {2, 3}
    namespace_a = next(
        item for item in snapshot.namespaces if item.project_id == project_a
    )
    namespace_b = next(
        item for item in snapshot.namespaces if item.project_id == project_b
    )
    assert namespace_a.ready_count == 1
    assert namespace_a.completed_count == 1
    assert namespace_a.idle_slot_count == 1
    assert namespace_b.claimed_count == 1
    assert namespace_b.busy_slot_count == 1
    assert snapshot.ready_count == 1
    assert snapshot.claimed_count == 1


def _run(
    *,
    now: datetime,
    run_id: str,
    project_id: str,
    name: str,
    root: Path,
) -> RunState:
    root.mkdir()
    return RunState(
        schema_version=1,
        run_id=run_id,
        goal="test",
        project_id=project_id,
        project_name=name,
        project_root=str(root),
        run_handle_digest="a" * 64,
        status=RunStatus.ACTIVE,
        revision=0,
        created_at=now,
        updated_at=now,
        requested_parallelism=2,
    )


def _broker_task(
    *,
    now: datetime,
    task_id: str,
    run_id: str,
    status: BrokerTaskStatus,
    depends_on: tuple[str, ...] = (),
    assigned_slot_id: str | None = None,
) -> BrokerTaskState:
    claimed = status is BrokerTaskStatus.CLAIMED
    return BrokerTaskState(
        schema_version=1,
        broker_task_id=task_id,
        run_id=run_id,
        objective="test task",
        role=None,
        depends_on=depends_on,
        status=status,
        assigned_slot_id=assigned_slot_id,
        assigned_worker_id="worker_111111111111111111111111" if claimed else None,
        assigned_task_id="task_111111111111111111111111" if claimed else None,
        revision=0,
        created_at=now,
        updated_at=now,
    )


def _slot(
    *,
    now: datetime,
    slot_id: str,
    project_id: str,
    status: WorkerSlotStatus,
) -> WorkerSlotState:
    busy = status is WorkerSlotStatus.BUSY
    return WorkerSlotState(
        schema_version=1,
        slot_id=slot_id,
        project_id=project_id,
        broker_consumer_id=f"consumer_{slot_id.removeprefix('slot_')}",
        slot_handle_digest="b" * 64,
        label=None,
        status=status,
        current_broker_task_id=("btask_333333333333333333333333" if busy else None),
        current_run_id="run_222222222222222222222222" if busy else None,
        current_worker_id="worker_111111111111111111111111" if busy else None,
        current_task_id="task_111111111111111111111111" if busy else None,
        revision=0,
        created_at=now,
        updated_at=now,
    )

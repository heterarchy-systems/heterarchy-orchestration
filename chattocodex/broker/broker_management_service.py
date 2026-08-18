from chattocodex.broker.broker_dto import BrokerTaskStatus, WorkerSlotStatus
from chattocodex.broker.broker_management_dto import (
    BrokerConsumerGroupState,
    BrokerManagementSnapshot,
    BrokerNamespaceState,
)
from chattocodex.broker_client.broker_operations_gateway import BrokerOperationsGateway
from chattocodex.orchestration.orchestration_dto import RunStatus
from chattocodex.persistence.broker_repository import (
    BrokerTaskRepository,
    WorkerSlotRepository,
)
from chattocodex.persistence.orchestration_state_repository import RunStateRepository


class BrokerManagementService:
    """Combine Broker-authoritative Group state with local orchestration projections."""

    def __init__(
        self,
        *,
        broker_task_repository: BrokerTaskRepository,
        slot_repository: WorkerSlotRepository,
        run_repository: RunStateRepository,
        broker_operations: BrokerOperationsGateway,
    ) -> None:
        self._broker_tasks = broker_task_repository
        self._slots = slot_repository
        self._runs = run_repository
        self._broker_operations = broker_operations

    def snapshot(self, *, project_ids: tuple[str, ...]) -> BrokerManagementSnapshot:
        directory = self._broker_operations.group_directory()
        tasks = self._broker_tasks.list_all()
        slots = self._slots.list_all()
        task_by_id = {task.broker_task_id: task for task in tasks}
        run_by_id = {run.run_id: run for run in self._runs.list_all()}
        namespace_ids = set(project_ids)
        namespace_ids.update(slot.project_id for slot in slots)
        namespace_ids.update(
            run_by_id[task.run_id].project_id
            for task in tasks
            if task.run_id in run_by_id
        )

        namespaces: list[BrokerNamespaceState] = []
        for project_id in sorted(namespace_ids):
            project_tasks = tuple(
                task
                for task in tasks
                if (run := run_by_id.get(task.run_id)) is not None
                and run.project_id == project_id
            )
            project_slots = tuple(
                slot for slot in slots if slot.project_id == project_id
            )
            ready_count = sum(
                self._is_ready(task=task, task_by_id=task_by_id, run_by_id=run_by_id)
                for task in project_tasks
            )
            namespaces.append(
                BrokerNamespaceState(
                    project_id=project_id,
                    queued_count=_count_task_status(
                        tasks=project_tasks, status=BrokerTaskStatus.QUEUED
                    ),
                    ready_count=ready_count,
                    claimed_count=_count_task_status(
                        tasks=project_tasks, status=BrokerTaskStatus.CLAIMED
                    ),
                    completed_count=_count_task_status(
                        tasks=project_tasks, status=BrokerTaskStatus.COMPLETED
                    ),
                    failed_count=_count_task_status(
                        tasks=project_tasks, status=BrokerTaskStatus.FAILED
                    ),
                    idle_slot_count=_count_slot_status(
                        slots=project_slots, status=WorkerSlotStatus.IDLE
                    ),
                    busy_slot_count=_count_slot_status(
                        slots=project_slots, status=WorkerSlotStatus.BUSY
                    ),
                )
            )

        consumer_groups = tuple(
            BrokerConsumerGroupState(
                group_id=group.group_id,
                namespace_id=group.namespace_id,
                generation=group.generation,
                group_revision=group.group_revision,
                consumer_count=group.consumer_count,
            )
            for group in directory.groups
        )
        return BrokerManagementSnapshot(
            term=directory.term,
            revision=directory.revision,
            namespaces=tuple(namespaces),
            consumer_groups=consumer_groups,
            queued_count=sum(namespace.queued_count for namespace in namespaces),
            ready_count=sum(namespace.ready_count for namespace in namespaces),
            claimed_count=sum(namespace.claimed_count for namespace in namespaces),
            completed_count=sum(namespace.completed_count for namespace in namespaces),
            failed_count=sum(namespace.failed_count for namespace in namespaces),
            idle_slot_count=sum(namespace.idle_slot_count for namespace in namespaces),
            busy_slot_count=sum(namespace.busy_slot_count for namespace in namespaces),
        )

    @staticmethod
    def _is_ready(*, task, task_by_id, run_by_id) -> bool:
        if task.status is not BrokerTaskStatus.QUEUED:
            return False
        run = run_by_id.get(task.run_id)
        if run is None or run.status is not RunStatus.ACTIVE:
            return False
        return all(
            dependency_id in task_by_id
            and task_by_id[dependency_id].status is BrokerTaskStatus.COMPLETED
            for dependency_id in task.depends_on
        )


def _count_task_status(*, tasks, status: BrokerTaskStatus) -> int:
    return sum(task.status is status for task in tasks)


def _count_slot_status(*, slots, status: WorkerSlotStatus) -> int:
    return sum(slot.status is status for slot in slots)

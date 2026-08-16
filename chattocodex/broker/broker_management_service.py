from chattocodex.broker.broker_dto import BrokerTaskStatus, WorkerSlotStatus
from chattocodex.broker.broker_management_dto import (
    BrokerClusterSnapshot,
    BrokerClusterTopology,
    BrokerNamespaceState,
    BrokerNodeRole,
    BrokerNodeState,
    BrokerNodeStatus,
    GroupCoordinatorAssignment,
)
from chattocodex.broker.group_coordinator_directory import (
    GroupCoordinatorDirectory,
    default_consumer_group_id,
)
from chattocodex.orchestration.orchestration_dto import RunStatus
from chattocodex.persistence.broker_repository import (
    BrokerTaskRepository,
    WorkerSlotRepository,
)
from chattocodex.persistence.consumer_group_state_store import ConsumerGroupStateStore
from chattocodex.persistence.orchestration_state_repository import RunStateRepository

_DEFAULT_CLUSTER_ID = "local-broker-cluster"
_DEFAULT_NODE_ID = "broker-0"


class BrokerManagementService:
    """Build a provider-independent management view over the shared work broker."""

    def __init__(
        self,
        *,
        broker_task_repository: BrokerTaskRepository,
        slot_repository: WorkerSlotRepository,
        run_repository: RunStateRepository,
        consumer_group_store: ConsumerGroupStateStore,
        nodes: tuple[BrokerNodeState, ...] | None = None,
        cluster_id: str = _DEFAULT_CLUSTER_ID,
    ) -> None:
        self._broker_tasks = broker_task_repository
        self._slots = slot_repository
        self._runs = run_repository
        self._consumer_groups = consumer_group_store
        self._nodes = nodes or (
            BrokerNodeState(
                node_id=_DEFAULT_NODE_ID,
                roles=(BrokerNodeRole.BROKER, BrokerNodeRole.CONTROLLER),
                status=BrokerNodeStatus.ONLINE,
            ),
        )
        self._cluster_id = cluster_id
        self._coordinators = GroupCoordinatorDirectory(nodes=self._nodes)

    def snapshot(self, *, project_ids: tuple[str, ...]) -> BrokerClusterSnapshot:
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
        coordinators: list[GroupCoordinatorAssignment] = []
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
            group_count = 0
            if self._consumer_groups.exists(project_id=project_id):
                group = self._consumer_groups.read(project_id=project_id)
                group_id = default_consumer_group_id(project_id=project_id)
                coordinators.append(
                    GroupCoordinatorAssignment(
                        group_id=group_id,
                        project_id=project_id,
                        coordinator_node_id=self._coordinators.coordinator_node_id(
                            group_id=group_id
                        ),
                        generation=group.generation,
                        member_count=len(group.member_ids),
                    )
                )
                group_count = 1
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
                    consumer_group_count=group_count,
                )
            )

        controller_node_id = self._controller_node_id()
        topology = (
            BrokerClusterTopology.EMBEDDED_SINGLE_NODE
            if len(self._nodes) == 1
            else BrokerClusterTopology.MULTI_NODE
        )
        return BrokerClusterSnapshot(
            cluster_id=self._cluster_id,
            topology=topology,
            controller_node_id=controller_node_id,
            nodes=self._nodes,
            namespaces=tuple(namespaces),
            group_coordinators=tuple(coordinators),
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

    def _controller_node_id(self) -> str:
        controller_nodes = tuple(
            node
            for node in self._nodes
            if node.status is BrokerNodeStatus.ONLINE
            and BrokerNodeRole.CONTROLLER in node.roles
        )
        if not controller_nodes:
            raise ValueError(
                "Broker cluster requires at least one online controller node."
            )
        return sorted(controller_nodes, key=lambda node: node.node_id)[0].node_id


def _count_task_status(*, tasks, status: BrokerTaskStatus) -> int:
    return sum(task.status is status for task in tasks)


def _count_slot_status(*, slots, status: WorkerSlotStatus) -> int:
    return sum(slot.status is status for slot in slots)

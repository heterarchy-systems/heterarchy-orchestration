from dataclasses import dataclass

from chattocodex.orchestrator_backend.broker.broker_dto import BrokerTaskStatus
from chattocodex.orchestrator_backend.orchestration.orchestration_dto import RunStatus
from chattocodex.orchestrator_backend.persistence.broker.broker_repository import (
    BrokerTaskRepository,
)
from chattocodex.orchestrator_backend.persistence.orchestration.orchestration_state_repository import (
    RunStateRepository,
)


@dataclass(frozen=True, slots=True, kw_only=True)
class BrokerReadyCapacity:
    """READY and engaged task demand projected from the Broker.

    Args:
        ready_task_count: Number of active, dependency-satisfied queued tasks.
        requested_parallelism: Sum of persisted requested parallelism for active
            Runs that currently contribute READY or CLAIMED tasks.
        engaged_task_count: Per-Run bounded demand, calculated as the sum of
            ``min(requested_parallelism, READY + CLAIMED)`` for active Runs.
    """

    ready_task_count: int
    requested_parallelism: int
    engaged_task_count: int


class BrokerReadyCapacityProjection:
    """Read-only projection of Broker READY work for Consumer capacity decisions."""

    def __init__(
        self,
        broker_task_repository: BrokerTaskRepository,
        run_repository: RunStateRepository,
    ) -> None:
        """Initialize this object. Args: broker_task_repository, run_repository. Returns: None."""
        self._broker_task_repository = broker_task_repository
        self._run_repository = run_repository

    def ready_count(self, project_id: str) -> int:
        """Return the current READY task count for one active project.

        Args:
            project_id: Canonical project identity to scope the projection.

        Returns:
            The number of queued tasks whose Run is active and dependencies are
            all complete.
        """
        return self.project_capacity(project_id=project_id).ready_task_count

    def project_capacity(self, project_id: str) -> BrokerReadyCapacity:
        """Return the READY-demand inputs for a project capacity calculation.

        Args:
            project_id: Canonical project identity to scope the projection.

        Returns:
            READY task count, requested parallelism, and per-Run bounded engaged
            demand for all active Runs that contribute READY or CLAIMED work.
        """
        tasks = self._broker_task_repository.list_all()
        by_id = {task.broker_task_id: task for task in tasks}
        run_cache = {}
        ready_by_run: dict[str, int] = {}
        claimed_by_run: dict[str, int] = {}
        for task in tasks:
            if task.status not in {
                BrokerTaskStatus.QUEUED,
                BrokerTaskStatus.CLAIMED,
            }:
                continue
            run = run_cache.get(task.run_id)
            if run is None:
                run = self._run_repository.read(run_id=task.run_id)
                run_cache[task.run_id] = run
            if run.project_id != project_id or run.status is not RunStatus.ACTIVE:
                continue
            if task.status is BrokerTaskStatus.CLAIMED:
                claimed_by_run[run.run_id] = claimed_by_run.get(run.run_id, 0) + 1
                continue
            if all(
                by_id[dependency_id].status is BrokerTaskStatus.COMPLETED
                for dependency_id in task.depends_on
            ):
                ready_by_run[run.run_id] = ready_by_run.get(run.run_id, 0) + 1
        contributing_run_ids = ready_by_run.keys() | claimed_by_run.keys()
        requested_parallelism = sum(
            run_cache[run_id].requested_parallelism for run_id in contributing_run_ids
        )
        engaged_task_count = sum(
            min(
                run_cache[run_id].requested_parallelism,
                ready_by_run.get(run_id, 0) + claimed_by_run.get(run_id, 0),
            )
            for run_id in contributing_run_ids
        )
        return BrokerReadyCapacity(
            ready_task_count=sum(ready_by_run.values()),
            requested_parallelism=requested_parallelism,
            engaged_task_count=engaged_task_count,
        )

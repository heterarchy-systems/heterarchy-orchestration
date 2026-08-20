from collections import deque
from typing import Protocol

from chattocodex.orchestrator_backend.orchestration.orchestration_exceptions import (
    InvalidTransitionError,
)


class LogicalTaskSpec(Protocol):
    """Minimal logical-task shape required for DAG validation and planning."""

    @property
    def key(self) -> str:
        """Key. Args: None. Returns: str."""
        ...

    @property
    def depends_on(self) -> tuple[str, ...]:
        """Depends on. Args: None. Returns: tuple[str, ...]."""
        ...


def validate_and_order_logical_tasks[TaskT: LogicalTaskSpec](
    tasks: tuple[TaskT, ...],
    operation: str,
    empty_message: str,
) -> tuple[TaskT, ...]:
    """Validate a logical task DAG and return stable topological input order. Args: tasks, operation, empty_message. Returns: tuple[TaskT, ...]."""

    if not tasks:
        raise InvalidTransitionError(
            operation=operation,
            resource_id="tasks",
            message=empty_message,
        )

    by_key: dict[str, TaskT] = {}
    input_index: dict[str, int] = {}
    for index, task in enumerate(tasks):
        if task.key in by_key:
            raise InvalidTransitionError(
                operation=operation,
                resource_id=task.key,
                message=f"Logical task key {task.key!r} is duplicated.",
            )
        by_key[task.key] = task
        input_index[task.key] = index

    indegree = {task.key: 0 for task in tasks}
    dependents: dict[str, list[str]] = {task.key: [] for task in tasks}
    for task in tasks:
        seen_dependencies: set[str] = set()
        for dependency in task.depends_on:
            if dependency == task.key:
                raise InvalidTransitionError(
                    operation=operation,
                    resource_id=task.key,
                    message="A logical task cannot depend on itself.",
                )
            if dependency not in by_key:
                raise InvalidTransitionError(
                    operation=operation,
                    resource_id=dependency,
                    message=f"Unknown logical task dependency {dependency!r}.",
                )
            if dependency in seen_dependencies:
                continue
            seen_dependencies.add(dependency)
            indegree[task.key] += 1
            dependents[dependency].append(task.key)

    ready = deque(
        sorted(
            (key for key, degree in indegree.items() if degree == 0),
            key=input_index.__getitem__,
        )
    )
    ordered_keys: list[str] = []
    while ready:
        key = ready.popleft()
        ordered_keys.append(key)
        newly_ready: list[str] = []
        for dependent in dependents[key]:
            indegree[dependent] -= 1
            if indegree[dependent] == 0:
                newly_ready.append(dependent)
        for dependent in sorted(newly_ready, key=input_index.__getitem__):
            ready.append(dependent)

    if len(ordered_keys) != len(tasks):
        raise InvalidTransitionError(
            operation=operation,
            resource_id="tasks",
            message="Logical task dependencies contain a cycle.",
        )
    return tuple(by_key[key] for key in ordered_keys)


def resolve_requested_parallelism[TaskT: LogicalTaskSpec](
    requested_parallelism: int | None,
    tasks: tuple[TaskT, ...],
) -> int:
    """Resolve explicit parallelism or default to the number of logical DAG roots. Args: requested_parallelism, tasks. Returns: int."""

    if requested_parallelism is not None:
        if requested_parallelism < 1:
            raise ValueError("requested_parallelism must be at least one")
        return requested_parallelism
    return max(1, sum(not task.depends_on for task in tasks))

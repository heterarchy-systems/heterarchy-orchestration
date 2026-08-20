from chattocodex.orchestrator_backend.worker_runtime.capacity.capacity_decision import (
    CapacityDecision,
    CapacityLimits,
)


def build_capacity_decision(
    project_id: str,
    requested_parallelism: int,
    ready_task_count: int,
    engaged_task_count: int,
    capacity_limits: CapacityLimits,
    recoverable_consumer_count: int,
    busy_consumer_count: int,
    waking_consumer_count: int,
    global_recoverable_count: int,
    global_busy_consumer_count: int,
    global_waking_consumer_count: int,
    project_pending_provisioning_count: int,
    global_pending_provisioning_count: int,
) -> CapacityDecision:
    """Calculate immutable Worker capacity policy from already-observed counts. Args: project_id, requested_parallelism, ready_task_count, engaged_task_count, capacity_limits, recoverable_consumer_count, busy_consumer_count, waking_consumer_count, global_recoverable_count, global_busy_consumer_count, global_waking_consumer_count, project_pending_provisioning_count, global_pending_provisioning_count. Returns: CapacityDecision."""

    other_project_waking_count = global_waking_consumer_count - waking_consumer_count
    desired_capacity = min(
        engaged_task_count,
        capacity_limits.project_max_workers,
        capacity_limits.global_max_workers,
    )
    capacity_deficit = max(0, desired_capacity - recoverable_consumer_count)
    wake_target_count = min(
        max(0, desired_capacity - busy_consumer_count),
        max(0, capacity_limits.project_max_workers - busy_consumer_count),
        max(
            0,
            capacity_limits.global_max_workers
            - global_busy_consumer_count
            - other_project_waking_count,
        ),
    )
    global_headroom = max(
        0,
        capacity_limits.global_max_workers
        - global_recoverable_count
        - global_pending_provisioning_count,
    )
    spawn_needed = min(
        max(0, capacity_deficit - project_pending_provisioning_count),
        global_headroom,
    )
    return CapacityDecision(
        project_id=project_id,
        requested_parallelism=requested_parallelism,
        ready_task_count=ready_task_count,
        project_max_workers=capacity_limits.project_max_workers,
        global_max_workers=capacity_limits.global_max_workers,
        global_headroom=global_headroom,
        recoverable_consumer_count=recoverable_consumer_count,
        busy_consumer_count=busy_consumer_count,
        global_busy_consumer_count=global_busy_consumer_count,
        waking_consumer_count=waking_consumer_count,
        wake_target_count=wake_target_count,
        pending_provisioning_count=project_pending_provisioning_count,
        global_pending_provisioning_count=global_pending_provisioning_count,
        desired_capacity=desired_capacity,
        capacity_deficit=capacity_deficit,
        spawn_needed=spawn_needed,
    )

"""Immutable Consumer Group capacity projections and configuration."""

from dataclasses import dataclass


@dataclass(frozen=True, slots=True, kw_only=True)
class CapacityLimits:
    """Project and global upper bounds for recoverable Worker capacity.

    Args:
        project_max_workers: Maximum Consumer capacity for one project.
        global_max_workers: Maximum Consumer capacity across all projects.

    Raises:
        ValueError: If either configured limit is less than one.
    """

    project_max_workers: int = 32
    global_max_workers: int = 32

    def __post_init__(self) -> None:
        """Validate that both capacity limits can admit at least one Worker.

        Raises:
            ValueError: If a capacity limit is less than one.
        """
        if self.project_max_workers < 1:
            raise ValueError("project_max_workers must be at least one")
        if self.global_max_workers < 1:
            raise ValueError("global_max_workers must be at least one")


@dataclass(frozen=True, slots=True, kw_only=True)
class CapacityDecision:
    """Scheduler-owned, immutable outcome of one project capacity reconciliation.

    The decision deliberately describes only logical demand and reusable Consumer
    capacity. A provisioning adapter may reserve ``spawn_needed`` but must not
    infer task, role, or browser semantics from this object.

    Args:
        project_id: Canonical project receiving the capacity decision.
        requested_parallelism: Aggregate persisted parallelism requested by active
            Runs that currently have READY or CLAIMED work.
        ready_task_count: Number of Broker-ready tasks for the project.
        project_max_workers: Project capacity ceiling used for this decision.
        global_max_workers: Global capacity ceiling used for this decision.
        global_headroom: Capacity still available for new Consumers after current
            recoverable Consumers and pending provisioning reservations.
        recoverable_consumer_count: ACTIVE or recoverable DORMANT Consumers in the
            project. BUSY Consumers remain included so they cannot be stolen.
        busy_consumer_count: Consumers with a sticky in-flight assignment. They
            consume total engaged capacity but are never reclaimed by scheduling.
        global_busy_consumer_count: Sticky BUSY consumers across all projects.
            This prevents a cross-project wake from exceeding global engagement.
        waking_consumer_count: Recoverable Consumers with an issued wake or an
            in-progress claim reservation.
        wake_target_count: Maximum project WAKING reservations permitted after
            global BUSY consumers and recoverable WAKING reservations in other
            projects consume their share of global capacity.
        pending_provisioning_count: Durable provisioning reservations already made
            for the project.
        global_pending_provisioning_count: Durable provisioning reservations across
            all projects at the time this decision was calculated.
        desired_capacity: Bounded capacity target before reuse/pending deductions.
        capacity_deficit: Deficit after reusable Consumers but before reservations.
        spawn_needed: Remaining provisionable deficit after pending reservations and
            global headroom are applied.
    """

    project_id: str
    requested_parallelism: int
    ready_task_count: int
    project_max_workers: int
    global_max_workers: int
    global_headroom: int
    recoverable_consumer_count: int
    busy_consumer_count: int
    global_busy_consumer_count: int
    waking_consumer_count: int
    wake_target_count: int
    pending_provisioning_count: int
    global_pending_provisioning_count: int
    desired_capacity: int
    capacity_deficit: int
    spawn_needed: int

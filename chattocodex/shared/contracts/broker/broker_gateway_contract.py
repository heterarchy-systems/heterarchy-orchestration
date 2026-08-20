from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True, slots=True)
class BrokerGroupLeaseContext:
    """Describe the Broker term and consumer-group generation for one assignment."""

    term: int
    generation: int


@dataclass(frozen=True, slots=True)
class BrokerClaim:
    """Describe one fenced Broker task claim returned to orchestration."""

    task_id: str
    objective: str
    term: int
    generation: int
    lease_id: str
    lease_epoch: int
    lease_expires_at_ms: int


@dataclass(frozen=True, slots=True)
class BrokerRenewal:
    """Describe one renewed Broker assignment lease fence."""

    term: int
    generation: int
    lease_id: str
    lease_epoch: int
    lease_expires_at_ms: int


class WorkBrokerGateway(Protocol):
    """Define the transport-neutral Broker operations required by orchestration."""

    def publish_task(
        self,
        project_id: str,
        run_id: str,
        task_id: str,
        objective: str,
    ) -> None:
        """Publish one logical task to the external Broker.

        Args:
            project_id: Stable project namespace identifier.
            run_id: Stable orchestration run identifier.
            task_id: Stable logical task identifier.
            objective: Human-readable task objective.

        Returns:
            None.
        """
        ...

    def claim_task(
        self,
        project_id: str,
        run_id: str,
        member_id: str,
    ) -> BrokerClaim | None:
        """Claim the next available task for one Broker member.

        Args:
            project_id: Stable project namespace identifier.
            run_id: Stable orchestration run identifier.
            member_id: Broker consumer/member identifier.

        Returns:
            Claimed task fence, or None when no task is currently claimable.
        """
        ...

    def renew_task(
        self,
        project_id: str,
        run_id: str,
        member_id: str,
        task_id: str,
        term: int,
        generation: int,
        lease_id: str,
        lease_epoch: int,
    ) -> BrokerRenewal:
        """Renew one currently fenced Broker task lease.

        Args:
            project_id: Stable project namespace identifier.
            run_id: Stable orchestration run identifier.
            member_id: Broker consumer/member identifier.
            task_id: Claimed task identifier.
            term: Broker leadership term expected by the caller.
            generation: Consumer-group generation expected by the caller.
            lease_id: Stable lease identifier.
            lease_epoch: Current lease epoch expected by the caller.

        Returns:
            Renewed lease fence.
        """
        ...

    def complete_task(
        self,
        project_id: str,
        run_id: str,
        member_id: str,
        task_id: str,
        term: int,
        generation: int,
        lease_id: str,
        lease_epoch: int,
        result: str,
    ) -> None:
        """Complete one currently fenced Broker task assignment.

        Args:
            project_id: Stable project namespace identifier.
            run_id: Stable orchestration run identifier.
            member_id: Broker consumer/member identifier.
            task_id: Claimed task identifier.
            term: Broker leadership term expected by the caller.
            generation: Consumer-group generation expected by the caller.
            lease_id: Stable lease identifier.
            lease_epoch: Current lease epoch expected by the caller.
            result: Worker result payload persisted by the Broker.

        Returns:
            None.
        """
        ...

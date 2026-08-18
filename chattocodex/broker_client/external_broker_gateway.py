from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol
from uuid import uuid4

from agent_broker import BrokerClientConfig, StandaloneBrokerClient


@dataclass(frozen=True, slots=True, kw_only=True)
class BrokerGroupLeaseContext:
    term: int
    generation: int


@dataclass(frozen=True, slots=True, kw_only=True)
class BrokerClaim:
    task_id: str
    objective: str
    term: int
    generation: int
    lease_id: str
    lease_epoch: int
    lease_expires_at_ms: int


@dataclass(frozen=True, slots=True, kw_only=True)
class BrokerRenewal:
    term: int
    generation: int
    lease_id: str
    lease_epoch: int
    lease_expires_at_ms: int


class WorkBrokerGateway(Protocol):
    def publish_task(
        self,
        *,
        project_id: str,
        run_id: str,
        task_id: str,
        objective: str,
    ) -> None: ...

    def claim_task(
        self, *, project_id: str, run_id: str, member_id: str
    ) -> BrokerClaim | None: ...

    def renew_task(
        self,
        *,
        project_id: str,
        run_id: str,
        member_id: str,
        task_id: str,
        term: int,
        generation: int,
        lease_id: str,
        lease_epoch: int,
    ) -> BrokerRenewal: ...

    def complete_task(
        self,
        *,
        project_id: str,
        run_id: str,
        member_id: str,
        task_id: str,
        term: int,
        generation: int,
        lease_id: str,
        lease_epoch: int,
        result: str,
    ) -> None: ...


class AgentBrokerGateway:
    """Orchestration adapter for the external Rust Broker Python SDK.

    Run/DAG/Review/Wake state stays in heterarchy-orchestration. This gateway owns
    only the translation to Broker namespace, Consumer Group, task, and lease
    operations. A fresh SDK client is used per logical call so concurrent MCP
    requests never share a mutable transport connection.
    """

    def __init__(
        self,
        *,
        config: BrokerClientConfig,
        lease_duration_ms: int,
        client_factory: Callable[[BrokerClientConfig], StandaloneBrokerClient]
        | None = None,
    ) -> None:
        if lease_duration_ms <= 0:
            raise ValueError("lease_duration_ms must be positive")
        self._config = config
        self._lease_duration_ms = lease_duration_ms
        self._client_factory = client_factory or StandaloneBrokerClient

    def publish_task(
        self,
        *,
        project_id: str,
        run_id: str,
        task_id: str,
        objective: str,
    ) -> None:
        with self._client_factory(self._config) as client:
            namespace_id = self._namespace_id(project_id=project_id, run_id=run_id)
            client.ensure_namespace(namespace_id=namespace_id)
            client.publish_task(
                namespace_id=namespace_id,
                task_id=task_id,
                objective=objective,
            )

    def claim_task(
        self, *, project_id: str, run_id: str, member_id: str
    ) -> BrokerClaim | None:
        with self._client_factory(self._config) as client:
            group = self._ensure_member(
                client=client,
                project_id=project_id,
                run_id=run_id,
                member_id=member_id,
            )
            lease_id = f"lease_{uuid4().hex}"
            claimed = client.claim_task(
                group_id=self._group_id(run_id=run_id),
                member_id=member_id,
                expected_term=group.term,
                expected_generation=group.generation,
                lease_id=lease_id,
                lease_duration_ms=self._lease_duration_ms,
            )
        if claimed.task_id is None:
            return None
        if (
            claimed.objective is None
            or claimed.lease_id is None
            or claimed.lease_epoch is None
            or claimed.lease_expires_at_ms is None
        ):
            raise RuntimeError("Broker returned an incomplete claimed-task lease")
        return BrokerClaim(
            task_id=claimed.task_id,
            objective=claimed.objective,
            term=claimed.term,
            generation=claimed.generation,
            lease_id=claimed.lease_id,
            lease_epoch=claimed.lease_epoch,
            lease_expires_at_ms=claimed.lease_expires_at_ms,
        )

    def renew_task(
        self,
        *,
        project_id: str,
        run_id: str,
        member_id: str,
        task_id: str,
        term: int,
        generation: int,
        lease_id: str,
        lease_epoch: int,
    ) -> BrokerRenewal:
        with self._client_factory(self._config) as client:
            renewed = client.renew_task(
                task_id=task_id,
                group_id=self._group_id(run_id=run_id),
                member_id=member_id,
                expected_term=term,
                expected_generation=generation,
                expected_lease_epoch=lease_epoch,
                lease_id=lease_id,
                lease_duration_ms=self._lease_duration_ms,
            )
        return BrokerRenewal(
            term=renewed.term,
            generation=renewed.generation,
            lease_id=renewed.lease_id,
            lease_epoch=renewed.lease_epoch,
            lease_expires_at_ms=renewed.lease_expires_at_ms,
        )

    def complete_task(
        self,
        *,
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
        with self._client_factory(self._config) as client:
            client.complete_task(
                task_id=task_id,
                group_id=self._group_id(run_id=run_id),
                member_id=member_id,
                expected_term=term,
                expected_generation=generation,
                expected_lease_epoch=lease_epoch,
                lease_id=lease_id,
                result=result,
            )

    def _ensure_member(
        self,
        *,
        client: StandaloneBrokerClient,
        project_id: str,
        run_id: str,
        member_id: str,
    ) -> BrokerGroupLeaseContext:
        namespace_id = self._namespace_id(project_id=project_id, run_id=run_id)
        group_id = self._group_id(run_id=run_id)
        client.ensure_namespace(namespace_id=namespace_id)
        client.ensure_group(
            namespace_id=namespace_id,
            group_id=group_id,
        )
        joined = client.join_group(
            group_id=group_id,
            member_id=member_id,
            capabilities=("worker",),
        )
        return BrokerGroupLeaseContext(term=joined.term, generation=joined.generation)

    @staticmethod
    def _namespace_id(*, project_id: str, run_id: str) -> str:
        return f"{project_id}_{run_id}"

    @staticmethod
    def _group_id(*, run_id: str) -> str:
        return f"workers_{run_id}"

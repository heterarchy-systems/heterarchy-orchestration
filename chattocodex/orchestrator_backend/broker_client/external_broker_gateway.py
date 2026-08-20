from __future__ import annotations

from collections.abc import Callable
from threading import Event, Lock
from typing import TypeVar
from uuid import uuid4

from agent_broker import (
    BrokerClientConfig,
    BrokerError,
    BrokerErrorCode,
    ClusterBrokerClient,
    StandaloneBrokerClient,
    StaticClusterConfig,
    StaticClusterNode,
    TaskClaimResult,
    TaskLeaseRenewedResult,
    TransportError,
)

from chattocodex.shared.contracts.broker.broker_gateway_contract import (
    BrokerClaim,
    BrokerGroupLeaseContext,
    BrokerRenewal,
)

_T = TypeVar("_T")
_ClientCall = Callable[[StandaloneBrokerClient], _T]


class AgentBrokerGateway:
    """Translate orchestration work ownership into external Broker operations.

    Direct standalone routing remains the fast path. If a configured cluster node
    reports that it is not leader, the gateway switches to Broker operations
    readiness discovery and routes each later logical mutation to the current
    verified leader. Every routed retry reuses the same task/Consumer/lease fence
    supplied by the caller so leader changes do not manufacture a second logical
    ownership transition.
    """

    def __init__(
        self,
        config: BrokerClientConfig,
        lease_duration_ms: int,
        client_factory: Callable[[BrokerClientConfig], StandaloneBrokerClient]
        | None = None,
        cluster_config: StaticClusterConfig | None = None,
        cluster_client_factory: Callable[[StaticClusterConfig], ClusterBrokerClient]
        | None = None,
    ) -> None:
        """Initialize this object. Args: config, lease_duration_ms, client_factory, cluster_config, cluster_client_factory. Returns: None."""
        if lease_duration_ms <= 0:
            raise ValueError("lease_duration_ms must be positive")
        self._config = config
        self._lease_duration_ms = lease_duration_ms
        self._client_factory = client_factory or StandaloneBrokerClient
        self._cluster_config = cluster_config
        self._cluster_client_factory = cluster_client_factory or ClusterBrokerClient
        self._cluster_detected = Event()
        self._leader_hint_lock = Lock()
        self._leader_hint: int | None = None

    def publish_task(
        self,
        project_id: str,
        run_id: str,
        task_id: str,
        objective: str,
    ) -> None:
        """Publish task. Args: project_id, run_id, task_id, objective. Returns: None."""
        namespace_id = self._namespace_id(project_id=project_id, run_id=run_id)

        def publish(client: StandaloneBrokerClient) -> None:
            """Publish. Args: client. Returns: None."""
            client.ensure_namespace(namespace_id=namespace_id)
            client.publish_task(
                namespace_id=namespace_id,
                task_id=task_id,
                objective=objective,
            )

        self._call(publish)

    def claim_task(
        self, project_id: str, run_id: str, member_id: str
    ) -> BrokerClaim | None:
        """Claim task. Args: project_id, run_id, member_id. Returns: BrokerClaim | None."""
        lease_id = f"lease_{uuid4().hex}"

        def claim(client: StandaloneBrokerClient) -> TaskClaimResult:
            """Claim. Args: client. Returns: TaskClaimResult."""
            group = self._ensure_member(
                client=client,
                project_id=project_id,
                run_id=run_id,
                member_id=member_id,
            )
            return client.claim_task(
                group_id=self._group_id(run_id=run_id),
                member_id=member_id,
                expected_term=group.term,
                expected_generation=group.generation,
                lease_id=lease_id,
                lease_duration_ms=self._lease_duration_ms,
            )

        claimed = self._call(claim)
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
        project_id: str,
        run_id: str,
        member_id: str,
        task_id: str,
        term: int,
        generation: int,
        lease_id: str,
        lease_epoch: int,
    ) -> BrokerRenewal:
        """Renew task. Args: project_id, run_id, member_id, task_id, term, generation, lease_id, lease_epoch. Returns: BrokerRenewal."""
        del project_id

        def renew(client: StandaloneBrokerClient) -> TaskLeaseRenewedResult:
            """Renew. Args: client. Returns: TaskLeaseRenewedResult."""
            return client.renew_task(
                task_id=task_id,
                group_id=self._group_id(run_id=run_id),
                member_id=member_id,
                expected_term=term,
                expected_generation=generation,
                expected_lease_epoch=lease_epoch,
                lease_id=lease_id,
                lease_duration_ms=self._lease_duration_ms,
            )

        renewed = self._call(renew)
        return BrokerRenewal(
            term=renewed.term,
            generation=renewed.generation,
            lease_id=renewed.lease_id,
            lease_epoch=renewed.lease_epoch,
            lease_expires_at_ms=renewed.lease_expires_at_ms,
        )

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
        """Complete task. Args: project_id, run_id, member_id, task_id, term, generation, lease_id, lease_epoch, result. Returns: None."""
        del project_id

        def complete(client: StandaloneBrokerClient) -> None:
            """Complete. Args: client. Returns: None."""
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

        self._call(complete)

    def _call(self, operation: _ClientCall[_T]) -> _T:
        """Call. Args: operation. Returns: _T."""
        if self._cluster_detected.is_set():
            return self._call_cluster(operation)
        try:
            return self._call_direct(config=self._config, operation=operation)
        except BrokerError as error:
            if self._cluster_config is None or not _is_not_leader(error):
                raise
            self._set_leader_hint(_leader_hint(error))
            self._cluster_detected.set()
        return self._call_cluster(operation)

    def _call_cluster(self, operation: _ClientCall[_T]) -> _T:
        """Call cluster. Args: operation. Returns: _T."""
        if self._cluster_config is None:
            raise RuntimeError(
                "cluster routing was selected without cluster configuration"
            )
        for attempt in range(1, self._cluster_config.retry.max_attempts + 1):
            leader = self._hinted_leader()
            if leader is None:
                leader = self._cluster_client_factory(
                    self._cluster_config
                ).discover_write_leader()
                self._set_leader_hint(leader.node_id)
            leader_config = BrokerClientConfig(
                host=leader.host,
                port=leader.broker_port,
                timeout_seconds=self._config.timeout_seconds,
                max_response_frame_bytes=self._config.max_response_frame_bytes,
            )
            try:
                return self._call_direct(config=leader_config, operation=operation)
            except BrokerError as error:
                if (
                    not _is_not_leader(error)
                    or attempt == self._cluster_config.retry.max_attempts
                ):
                    raise
                self._set_leader_hint(_leader_hint(error))
            except TransportError:
                self._set_leader_hint(None)
                if attempt == self._cluster_config.retry.max_attempts:
                    raise
        raise RuntimeError("cluster Broker routing exhausted without a terminal result")

    def _call_direct(
        self,
        config: BrokerClientConfig,
        operation: _ClientCall[_T],
    ) -> _T:
        """Call direct. Args: config, operation. Returns: _T."""
        with self._client_factory(config) as client:
            return operation(client)

    def _hinted_leader(self) -> StaticClusterNode | None:
        """Hinted leader. Args: None. Returns: StaticClusterNode | None."""
        if self._cluster_config is None:
            return None
        with self._leader_hint_lock:
            leader_id = self._leader_hint
        if leader_id is None:
            return None
        return next(
            (node for node in self._cluster_config.nodes if node.node_id == leader_id),
            None,
        )

    def _set_leader_hint(self, leader_id: int | None) -> None:
        """Set leader hint. Args: leader_id. Returns: None."""
        with self._leader_hint_lock:
            self._leader_hint = leader_id

    def _ensure_member(
        self,
        client: StandaloneBrokerClient,
        project_id: str,
        run_id: str,
        member_id: str,
    ) -> BrokerGroupLeaseContext:
        """Ensure member. Args: client, project_id, run_id, member_id. Returns: BrokerGroupLeaseContext."""
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
    def _namespace_id(project_id: str, run_id: str) -> str:
        """Namespace id. Args: project_id, run_id. Returns: str."""
        return f"{project_id}_{run_id}"

    @staticmethod
    def _group_id(run_id: str) -> str:
        """Group id. Args: run_id. Returns: str."""
        return f"workers_{run_id}"


def _is_not_leader(error: BrokerError) -> bool:
    """Is not leader. Args: error. Returns: bool."""
    return (
        error.code is BrokerErrorCode.TRANSPORT_ERROR
        and error.message.startswith("cluster node ")
        and " is not leader" in error.message
    )


def _leader_hint(error: BrokerError) -> int | None:
    """Leader hint. Args: error. Returns: int | None."""
    marker = "current leader is "
    if not _is_not_leader(error) or marker not in error.message:
        return None
    candidate = error.message.rsplit(marker, maxsplit=1)[1]
    return int(candidate) if candidate.isdigit() else None

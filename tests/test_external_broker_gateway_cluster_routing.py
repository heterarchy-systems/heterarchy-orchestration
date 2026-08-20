from __future__ import annotations

from collections.abc import Callable
from contextlib import AbstractContextManager
from typing import cast

from agent_broker import (
    BrokerClientConfig,
    BrokerError,
    BrokerErrorCode,
    StaticClusterConfig,
    StaticClusterNode,
)

from chattocodex.orchestrator_backend.broker_client.external_broker_gateway import (
    AgentBrokerGateway,
)


class _FakeStandaloneClient(AbstractContextManager["_FakeStandaloneClient"]):
    def __init__(
        self,
        *,
        port: int,
        on_ensure_namespace: Callable[[int], None],
        published_ports: list[int],
    ) -> None:
        self._port = port
        self._on_ensure_namespace = on_ensure_namespace
        self._published_ports = published_ports

    def __exit__(self, *_args: object) -> None:
        return None

    def ensure_namespace(self, *, namespace_id: str) -> None:
        assert namespace_id
        self._on_ensure_namespace(self._port)

    def publish_task(self, *, namespace_id: str, task_id: str, objective: str) -> None:
        assert namespace_id
        assert task_id
        assert objective
        self._published_ports.append(self._port)


class _FakeClusterClient:
    def __init__(self, leaders: list[StaticClusterNode]) -> None:
        self._leaders = leaders
        self.discovery_count = 0

    def discover_write_leader(self) -> StaticClusterNode:
        index = min(self.discovery_count, len(self._leaders) - 1)
        self.discovery_count += 1
        return self._leaders[index]


def _cluster_config() -> StaticClusterConfig:
    return StaticClusterConfig(
        nodes=(
            StaticClusterNode(1, 8811, 9811),
            StaticClusterNode(2, 8812, 9812),
            StaticClusterNode(3, 8813, 9813),
        )
    )


def _not_leader(*, node_id: int, leader_id: int) -> BrokerError:
    return BrokerError(
        BrokerErrorCode.TRANSPORT_ERROR,
        f"cluster node {node_id} is not leader; current leader is {leader_id}",
    )


def test_direct_standalone_success_does_not_probe_cluster() -> None:
    published_ports: list[int] = []
    cluster = _FakeClusterClient([StaticClusterNode(3, 8813, 9813)])

    def client_factory(config: BrokerClientConfig):
        return _FakeStandaloneClient(
            port=config.port,
            on_ensure_namespace=lambda _port: None,
            published_ports=published_ports,
        )

    gateway = AgentBrokerGateway(
        config=BrokerClientConfig(port=8811),
        lease_duration_ms=30_000,
        client_factory=cast(object, client_factory),
        cluster_config=_cluster_config(),
        cluster_client_factory=cast(object, lambda _config: cluster),
    )
    gateway.publish_task(
        project_id="project-a",
        run_id="run-a",
        task_id="task-a",
        objective="standalone",
    )

    assert published_ports == [8811]
    assert cluster.discovery_count == 0


def test_follower_response_switches_gateway_to_discovered_leader() -> None:
    published_ports: list[int] = []
    cluster = _FakeClusterClient([StaticClusterNode(3, 8813, 9813)])

    def on_ensure_namespace(port: int) -> None:
        if port == 8811:
            raise _not_leader(node_id=1, leader_id=3)

    def client_factory(config: BrokerClientConfig):
        return _FakeStandaloneClient(
            port=config.port,
            on_ensure_namespace=on_ensure_namespace,
            published_ports=published_ports,
        )

    gateway = AgentBrokerGateway(
        config=BrokerClientConfig(port=8811),
        lease_duration_ms=30_000,
        client_factory=cast(object, client_factory),
        cluster_config=_cluster_config(),
        cluster_client_factory=cast(object, lambda _config: cluster),
    )
    gateway.publish_task(
        project_id="project-a",
        run_id="run-a",
        task_id="task-a",
        objective="cluster",
    )

    assert published_ports == [8813]
    assert cluster.discovery_count == 0


def test_cluster_mode_rediscovers_after_leader_change() -> None:
    published_ports: list[int] = []
    cluster = _FakeClusterClient(
        [
            StaticClusterNode(2, 8812, 9812),
            StaticClusterNode(2, 8812, 9812),
            StaticClusterNode(3, 8813, 9813),
        ]
    )
    node2_has_lost_leadership = False

    def on_ensure_namespace(port: int) -> None:
        nonlocal node2_has_lost_leadership
        if port == 8811:
            raise _not_leader(node_id=1, leader_id=2)
        if port == 8812 and node2_has_lost_leadership:
            raise _not_leader(node_id=2, leader_id=3)

    def client_factory(config: BrokerClientConfig):
        return _FakeStandaloneClient(
            port=config.port,
            on_ensure_namespace=on_ensure_namespace,
            published_ports=published_ports,
        )

    gateway = AgentBrokerGateway(
        config=BrokerClientConfig(port=8811),
        lease_duration_ms=30_000,
        client_factory=cast(object, client_factory),
        cluster_config=_cluster_config(),
        cluster_client_factory=cast(object, lambda _config: cluster),
    )
    gateway.publish_task(
        project_id="project-a",
        run_id="run-a",
        task_id="task-a",
        objective="before failover",
    )
    node2_has_lost_leadership = True
    gateway.publish_task(
        project_id="project-a",
        run_id="run-a",
        task_id="task-b",
        objective="after failover",
    )

    assert published_ports == [8812, 8813]
    assert cluster.discovery_count == 0


def test_unknown_leader_falls_back_to_operations_discovery() -> None:
    published_ports: list[int] = []
    cluster = _FakeClusterClient([StaticClusterNode(3, 8813, 9813)])

    def on_ensure_namespace(port: int) -> None:
        if port == 8811:
            raise BrokerError(
                BrokerErrorCode.TRANSPORT_ERROR,
                "cluster node 1 is not leader and no current leader is known",
            )

    def client_factory(config: BrokerClientConfig):
        return _FakeStandaloneClient(
            port=config.port,
            on_ensure_namespace=on_ensure_namespace,
            published_ports=published_ports,
        )

    gateway = AgentBrokerGateway(
        config=BrokerClientConfig(port=8811),
        lease_duration_ms=30_000,
        client_factory=cast(object, client_factory),
        cluster_config=_cluster_config(),
        cluster_client_factory=cast(object, lambda _config: cluster),
    )
    gateway.publish_task(
        project_id="project-a",
        run_id="run-a",
        task_id="task-a",
        objective="discover leader",
    )

    assert published_ports == [8813]
    assert cluster.discovery_count == 1

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol

from agent_broker import BrokerOperationsClient, OperationsClientConfig


@dataclass(frozen=True, slots=True, kw_only=True)
class BrokerConsumerGroupView:
    group_id: str
    namespace_id: str
    generation: int
    group_revision: int
    consumer_count: int


@dataclass(frozen=True, slots=True, kw_only=True)
class BrokerConsumerGroupDirectory:
    term: int
    revision: int
    groups: tuple[BrokerConsumerGroupView, ...]


class BrokerOperationsGateway(Protocol):
    def group_directory(self) -> BrokerConsumerGroupDirectory: ...


class AgentBrokerOperationsGateway:
    """Read-only orchestration adapter for Broker-authoritative group state."""

    def __init__(
        self,
        *,
        config: OperationsClientConfig,
        client_factory: Callable[[OperationsClientConfig], BrokerOperationsClient]
        | None = None,
    ) -> None:
        self._config = config
        self._client_factory = client_factory or BrokerOperationsClient

    def group_directory(self) -> BrokerConsumerGroupDirectory:
        client = self._client_factory(self._config)
        groups: list[BrokerConsumerGroupView] = []
        after_group_id: str | None = None
        term: int | None = None
        revision: int | None = None
        while True:
            page = client.list_groups(limit=8, after_group_id=after_group_id)
            if term is None:
                term = page.broker_term
                revision = page.broker_revision
            elif page.broker_term != term or page.broker_revision != revision:
                raise RuntimeError(
                    "Broker group-directory pagination changed term or revision between pages"
                )
            groups.extend(
                BrokerConsumerGroupView(
                    group_id=group.group_id,
                    namespace_id=group.namespace_id,
                    generation=group.generation,
                    group_revision=group.group_revision,
                    consumer_count=group.consumer_count,
                )
                for group in page.groups
            )
            after_group_id = page.next_after_group_id
            if after_group_id is None:
                break
        if term is None or revision is None:
            raise RuntimeError(
                "Broker group-directory response was empty without metadata"
            )
        return BrokerConsumerGroupDirectory(
            term=term,
            revision=revision,
            groups=tuple(groups),
        )

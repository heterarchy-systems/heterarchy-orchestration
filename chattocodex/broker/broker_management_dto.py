from dataclasses import dataclass


@dataclass(frozen=True, slots=True, kw_only=True)
class BrokerConsumerGroupState:
    group_id: str
    namespace_id: str
    generation: int
    group_revision: int
    consumer_count: int


@dataclass(frozen=True, slots=True, kw_only=True)
class BrokerNamespaceState:
    project_id: str
    queued_count: int
    ready_count: int
    claimed_count: int
    completed_count: int
    failed_count: int
    idle_slot_count: int
    busy_slot_count: int


@dataclass(frozen=True, slots=True, kw_only=True)
class BrokerManagementSnapshot:
    term: int
    revision: int
    namespaces: tuple[BrokerNamespaceState, ...]
    consumer_groups: tuple[BrokerConsumerGroupState, ...]
    queued_count: int
    ready_count: int
    claimed_count: int
    completed_count: int
    failed_count: int
    idle_slot_count: int
    busy_slot_count: int

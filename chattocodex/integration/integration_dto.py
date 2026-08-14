from dataclasses import dataclass


@dataclass(frozen=True, slots=True, kw_only=True)
class IntegrationConflict:
    relative_path: str
    worker_ids: tuple[str, ...]
    commit_shas: tuple[str, ...]


@dataclass(frozen=True, slots=True, kw_only=True)
class IntegrationPreview:
    run_id: str
    base_commit: str | None
    result_count: int
    ready_to_integrate: bool
    conflicts: tuple[IntegrationConflict, ...]


@dataclass(frozen=True, slots=True, kw_only=True)
class IntegrationExecution:
    integration_commit: str
    changed_paths: tuple[str, ...]


@dataclass(frozen=True, slots=True, kw_only=True)
class IntegrationApplyResult:
    run_id: str
    base_commit: str
    integration_commit: str
    result_count: int
    changed_paths: tuple[str, ...]

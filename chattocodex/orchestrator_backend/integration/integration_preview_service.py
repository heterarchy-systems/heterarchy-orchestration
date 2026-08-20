from chattocodex.orchestrator_backend.integration.integration_dto import (
    IntegrationConflict,
    IntegrationPreview,
)
from chattocodex.orchestrator_backend.orchestration.orchestration_exceptions import (
    InvalidTransitionError,
)
from chattocodex.orchestrator_backend.persistence.worker.worker_result_repository import (
    WorkerResultRepository,
)


class IntegrationPreviewService:
    def __init__(self, result_repository: WorkerResultRepository) -> None:
        """Initialize this object. Args: result_repository. Returns: None."""
        self._result_repository = result_repository

    def preview(self, run_id: str) -> IntegrationPreview:
        """Preview. Args: run_id. Returns: IntegrationPreview."""
        results = self._result_repository.list_for_run(run_id=run_id)
        if not results:
            return IntegrationPreview(
                run_id=run_id,
                base_commit=None,
                result_count=0,
                ready_to_integrate=False,
                conflicts=(),
            )

        base_commits = {result.base_commit for result in results}
        if len(base_commits) != 1:
            raise InvalidTransitionError(
                operation="integration_preview",
                resource_id=run_id,
                message="Worker result commits do not share one Run base commit.",
            )
        base_commit = next(iter(base_commits))

        path_owners: dict[str, list[tuple[str, str]]] = {}
        for result in results:
            for relative_path in result.changed_paths:
                path_owners.setdefault(relative_path, []).append(
                    (result.worker_id, result.commit_sha)
                )

        conflicts: list[IntegrationConflict] = []
        for relative_path, owners in sorted(path_owners.items()):
            if len(owners) < 2:
                continue
            ordered_owners = sorted(owners)
            conflicts.append(
                IntegrationConflict(
                    relative_path=relative_path,
                    worker_ids=tuple(
                        worker_id for worker_id, _commit in ordered_owners
                    ),
                    commit_shas=tuple(commit for _worker_id, commit in ordered_owners),
                )
            )

        return IntegrationPreview(
            run_id=run_id,
            base_commit=base_commit,
            result_count=len(results),
            ready_to_integrate=not conflicts,
            conflicts=tuple(conflicts),
        )

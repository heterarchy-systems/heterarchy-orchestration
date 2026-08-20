from datetime import UTC, datetime
from pathlib import Path

import pytest

from chattocodex.orchestrator_backend.version_control.git_result_dto import (
    WorkerResultCommit,
)
from chattocodex.orchestrator_backend.integration.integration_preview_service import (
    IntegrationPreviewService,
)
from chattocodex.orchestrator_backend.orchestration.orchestration_exceptions import (
    InvalidTransitionError,
)
from chattocodex.orchestrator_backend.persistence.worker.worker_result_repository import (
    WorkerResultRepository,
)


def _save_result(
    *,
    repository: WorkerResultRepository,
    run_id: str,
    worker_id: str,
    base_commit: str,
    commit_sha: str,
    changed_paths: tuple[str, ...],
) -> None:
    repository.save(
        result=WorkerResultCommit(
            run_id=run_id,
            worker_id=worker_id,
            base_commit=base_commit,
            commit_sha=commit_sha,
            changed_paths=changed_paths,
            summary=f"{worker_id} result",
            created_at=datetime.now(UTC),
        )
    )


def test_integration_preview_detects_only_overlapping_paths(tmp_path: Path) -> None:
    repository = WorkerResultRepository(state_root=tmp_path)
    run_id = "run_1234567890abcdef12345678"
    base_commit = "a" * 40
    _save_result(
        repository=repository,
        run_id=run_id,
        worker_id="worker_111111111111111111111111",
        base_commit=base_commit,
        commit_sha="b" * 40,
        changed_paths=("alpha.py", "shared.py"),
    )
    _save_result(
        repository=repository,
        run_id=run_id,
        worker_id="worker_222222222222222222222222",
        base_commit=base_commit,
        commit_sha="c" * 40,
        changed_paths=("beta.py", "shared.py"),
    )
    _save_result(
        repository=repository,
        run_id=run_id,
        worker_id="worker_333333333333333333333333",
        base_commit=base_commit,
        commit_sha="d" * 40,
        changed_paths=("gamma.py",),
    )

    preview = IntegrationPreviewService(result_repository=repository).preview(
        run_id=run_id
    )

    assert preview.base_commit == base_commit
    assert preview.result_count == 3
    assert preview.ready_to_integrate is False
    assert len(preview.conflicts) == 1
    conflict = preview.conflicts[0]
    assert conflict.relative_path == "shared.py"
    assert conflict.worker_ids == (
        "worker_111111111111111111111111",
        "worker_222222222222222222222222",
    )
    assert conflict.commit_shas == ("b" * 40, "c" * 40)


def test_integration_preview_is_ready_when_paths_do_not_overlap(tmp_path: Path) -> None:
    repository = WorkerResultRepository(state_root=tmp_path)
    run_id = "run_abcdefabcdefabcdefabcdef"
    base_commit = "a" * 40
    _save_result(
        repository=repository,
        run_id=run_id,
        worker_id="worker_111111111111111111111111",
        base_commit=base_commit,
        commit_sha="b" * 40,
        changed_paths=("alpha.py",),
    )
    _save_result(
        repository=repository,
        run_id=run_id,
        worker_id="worker_222222222222222222222222",
        base_commit=base_commit,
        commit_sha="c" * 40,
        changed_paths=("beta.py",),
    )

    preview = IntegrationPreviewService(result_repository=repository).preview(
        run_id=run_id
    )

    assert preview.result_count == 2
    assert preview.ready_to_integrate is True
    assert preview.conflicts == ()


def test_integration_preview_rejects_result_base_mismatch(tmp_path: Path) -> None:
    repository = WorkerResultRepository(state_root=tmp_path)
    run_id = "run_fedcbafedcbafedcbafedcba"
    _save_result(
        repository=repository,
        run_id=run_id,
        worker_id="worker_111111111111111111111111",
        base_commit="a" * 40,
        commit_sha="b" * 40,
        changed_paths=("alpha.py",),
    )
    _save_result(
        repository=repository,
        run_id=run_id,
        worker_id="worker_222222222222222222222222",
        base_commit="e" * 40,
        commit_sha="c" * 40,
        changed_paths=("beta.py",),
    )

    with pytest.raises(InvalidTransitionError, match="base commit"):
        IntegrationPreviewService(result_repository=repository).preview(run_id=run_id)

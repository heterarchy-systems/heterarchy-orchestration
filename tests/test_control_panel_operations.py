from datetime import UTC, datetime
from pathlib import Path

from chattocodex.orchestrator_backend.broker_client.broker_operations_gateway import (
    BrokerConsumerGroupDirectory,
    BrokerConsumerGroupView,
)
from chattocodex.orchestrator_backend.control_panel.application.control_panel_operations_service import (
    ControlPanelOperationsService,
)
from chattocodex.orchestrator_backend.orchestration.orchestration_dto import (
    RunState,
    RunStatus,
)
from chattocodex.orchestrator_backend.persistence.orchestration.orchestration_state_repository import (
    RunStateRepository,
)
from chattocodex.orchestrator_backend.persistence.wake.wake_target_repository import (
    WakeTargetRepository,
)
from chattocodex.orchestrator_backend.persistence.worker.worker_runtime_group_state_store import (
    WorkerRuntimeGroupStateStore,
)
from chattocodex.orchestrator_backend.wake.wake_dto import (
    WakeTargetState,
    WakeTargetStatus,
)
from chattocodex.orchestrator_backend.wake.browser.worker_browser_runtime_dto import (
    WorkerBrowserRuntimeMode,
)
from chattocodex.orchestrator_backend.worker_runtime.worker_runtime_group_state import (
    WorkerRuntimeGroupState,
)
from chattocodex.orchestrator_backend.workspace.workspace_project_dto import (
    WorkspaceProject,
)


class _BrokerOperations:
    def __init__(self, *, namespace_id: str) -> None:
        self._directory = BrokerConsumerGroupDirectory(
            term=5,
            revision=12,
            groups=(
                BrokerConsumerGroupView(
                    group_id="sample-company",
                    namespace_id=namespace_id,
                    generation=7,
                    group_revision=11,
                    consumer_count=3,
                ),
            ),
        )

    def group_directory(self) -> BrokerConsumerGroupDirectory:
        return self._directory


def test_operations_snapshot_projects_consumer_group_workers_and_runs(
    tmp_path: Path,
) -> None:
    execution_root = tmp_path / "execution"
    project_root = tmp_path / "sample"
    project_root.mkdir()
    project_id = "prj_sample_1234abcd"
    wake_target_id = "wtgt_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    now = datetime.now(UTC)

    WakeTargetRepository(state_root=execution_root).create(
        state=WakeTargetState(
            schema_version=1,
            wake_target_id=wake_target_id,
            project_id=project_id,
            registered_connector_epoch=0,
            status=WakeTargetStatus.IDLE,
            bound_slot_id=None,
            wake_revision=3,
            revision=4,
            created_at=now,
            updated_at=now,
        )
    )
    WorkerRuntimeGroupStateStore(state_root=execution_root).write(
        state=WorkerRuntimeGroupState(
            schema_version=1,
            project_id=project_id,
            topology_generation=2,
            wake_target_ids=(wake_target_id,),
            revision=5,
            created_at=now,
            updated_at=now,
        )
    )
    RunStateRepository(state_root=execution_root).create(
        state=RunState(
            schema_version=1,
            run_id="run_111111111111111111111111",
            goal="Inspect README and report the result",
            project_id=project_id,
            project_name="sample",
            project_root=str(project_root),
            run_handle_digest="a" * 64,
            status=RunStatus.ACTIVE,
            revision=1,
            created_at=now,
            updated_at=now,
            requested_parallelism=2,
        )
    )

    snapshot = ControlPanelOperationsService(
        execution_root=execution_root,
        broker_operations=_BrokerOperations(namespace_id=project_id),
    ).snapshot(
        projects=(
            WorkspaceProject(
                project_id=project_id,
                name="sample",
                root=project_root,
            ),
        ),
        bindings=(),
        browser_running=True,
        browser_mode=WorkerBrowserRuntimeMode.HEADLESS,
        browser_profile_initialized=True,
    )

    assert snapshot.browser.running is True
    assert snapshot.browser.mode == WorkerBrowserRuntimeMode.HEADLESS
    assert snapshot.broker is not None
    assert snapshot.broker.term == 5
    assert snapshot.broker.revision == 12
    assert snapshot.broker.namespaces[0].project_id == project_id
    assert snapshot.broker.consumer_groups[0].namespace_id == project_id
    assert snapshot.broker.consumer_groups[0].generation == 7
    assert snapshot.broker.consumer_groups[0].consumer_count == 3
    assert snapshot.totals.project_count == 1
    assert snapshot.totals.runtime_group_count == 1
    assert snapshot.totals.worker_count == 1
    assert snapshot.totals.idle == 1
    assert snapshot.totals.active_runs == 1
    assert snapshot.projects[0].worker_runtime.topology_generation == 2
    assert snapshot.projects[0].worker_runtime.workers[0].wake_revision == 3
    assert snapshot.recent_runs[0].requested_parallelism == 2

from datetime import UTC, datetime
from pathlib import Path

from chattocodex.consumer_group.consumer_group_state import ConsumerGroupState
from chattocodex.control_panel.control_panel_operations_service import (
    ControlPanelOperationsService,
)
from chattocodex.orchestration.orchestration_dto import RunState, RunStatus
from chattocodex.persistence.consumer_group_state_store import ConsumerGroupStateStore
from chattocodex.persistence.orchestration_state_repository import RunStateRepository
from chattocodex.persistence.wake_target_repository import WakeTargetRepository
from chattocodex.wake.wake_dto import WakeTargetState, WakeTargetStatus
from chattocodex.wake.worker_browser_runtime_dto import WorkerBrowserRuntimeMode
from chattocodex.workspace.workspace_project_dto import WorkspaceProject


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
    ConsumerGroupStateStore(state_root=execution_root).write(
        state=ConsumerGroupState(
            schema_version=1,
            project_id=project_id,
            generation=2,
            member_ids=(wake_target_id,),
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

    snapshot = ControlPanelOperationsService(execution_root=execution_root).snapshot(
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
    assert snapshot.browser.mode is WorkerBrowserRuntimeMode.HEADLESS
    assert snapshot.broker is not None
    assert snapshot.broker.cluster_id == "local-broker-cluster"
    assert snapshot.broker.controller_node_id == "broker-0"
    assert snapshot.broker.namespaces[0].project_id == project_id
    assert snapshot.broker.namespaces[0].consumer_group_count == 1
    assert snapshot.broker.group_coordinators[0].project_id == project_id
    assert snapshot.broker.group_coordinators[0].generation == 2
    assert snapshot.broker.group_coordinators[0].member_count == 1
    assert snapshot.totals.project_count == 1
    assert snapshot.totals.consumer_group_count == 1
    assert snapshot.totals.worker_count == 1
    assert snapshot.totals.idle == 1
    assert snapshot.totals.active_runs == 1
    assert snapshot.projects[0].consumer_group.generation == 2
    assert snapshot.projects[0].consumer_group.workers[0].wake_revision == 3
    assert snapshot.recent_runs[0].requested_parallelism == 2

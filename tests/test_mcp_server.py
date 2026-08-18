import asyncio
import hashlib
import subprocess
from pathlib import Path

import pytest
from mcp.server.mcpserver.exceptions import ToolError

from chattocodex.mcp_server import create_mcp_server
from chattocodex.persistence.project_binding_repository import (
    ProjectBindingRepository,
)
from chattocodex.persistence.worker_provisioning_repository import (
    WorkerProvisioningRepository,
)
from chattocodex.project_runtime.project_binding_service import (
    ProjectBindingService,
)
from chattocodex.schemas.project_binding_schemas import (
    ProjectBindingAdapterMetadata,
)
from chattocodex.workspace.workspace_project_service import WorkspaceProjectService
from tests.broker_support import FakeWorkBrokerGateway

_EXPECTED_TOOLS = {
    "project_list",
    "project_tree",
    "project_file_read",
    "project_text_search",
    "worker_workspace_prepare",
    "worker_file_create",
    "worker_file_replace",
    "worker_result_commit",
    "run_results",
    "worker_report_submit",
    "run_reports",
    "integration_preview",
    "integration_apply",
    "delegate_work",
    "prepare_scheduled_work",
    "delegation_status",
    "orchestrate_work",
    "workflow_status",
    "review_workflow",
    "resume_main_review",
    "work",
    "work_done",
    "worker_context",
    "task_update",
}


def _create_workspace(tmp_path: Path) -> Path:
    workspace_root = tmp_path / "workspace"
    project_root = workspace_root / "sample-project"
    project_root.mkdir(parents=True)
    (project_root / "pyproject.toml").write_text("[project]\nname = 'sample-project'\n")
    for args in (
        ("init",),
        ("config", "user.email", "test@example.com"),
        ("config", "user.name", "Test User"),
        ("add", "."),
        ("commit", "-m", "initial"),
    ):
        subprocess.run(
            ("git", "-C", str(project_root), *args),
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        )
    return workspace_root


def test_mcp_server_exposes_expected_tools(tmp_path: Path) -> None:
    server = create_mcp_server(
        work_broker_gateway=FakeWorkBrokerGateway(),
        state_root=tmp_path / "state",
        workspace_root=_create_workspace(tmp_path),
    )

    async def read_tools() -> set[str]:
        tools = await server.list_tools()
        return {tool.name for tool in tools}

    assert asyncio.run(read_tools()) == _EXPECTED_TOOLS


def test_mcp_server_accepts_connector_qualified_tool_names(tmp_path: Path) -> None:
    server = create_mcp_server(
        work_broker_gateway=FakeWorkBrokerGateway(),
        state_root=tmp_path / "state",
        workspace_root=_create_workspace(tmp_path),
    )

    async def list_projects():
        return await server.call_tool("arbitrary-connector.project_list", {})

    result = asyncio.run(list_projects())

    assert result.is_error is False
    assert result.structured_content is not None
    projects = result.structured_content["result"]["projects"]
    assert [project["name"] for project in projects] == ["sample-project"]


def test_mcp_server_rejects_qualified_unknown_tool_names(tmp_path: Path) -> None:
    server = create_mcp_server(
        work_broker_gateway=FakeWorkBrokerGateway(),
        state_root=tmp_path / "state",
        workspace_root=_create_workspace(tmp_path),
    )

    async def call_unknown_tool():
        return await server.call_tool("arbitrary-connector.not_a_tool", {})

    with pytest.raises(ToolError, match="Unknown tool"):
        asyncio.run(call_unknown_tool())


@pytest.mark.parametrize("name", (".project_list", "a..project_list"))
def test_mcp_server_rejects_malformed_qualified_tool_names(
    tmp_path: Path,
    name: str,
) -> None:
    server = create_mcp_server(
        work_broker_gateway=FakeWorkBrokerGateway(),
        state_root=tmp_path / "state",
        workspace_root=_create_workspace(tmp_path),
    )

    async def call_malformed_tool():
        return await server.call_tool(name, {})

    with pytest.raises(ToolError, match="Unknown tool"):
        asyncio.run(call_malformed_tool())


def test_review_requests_route_to_orchestrated_main_review(tmp_path: Path) -> None:
    server = create_mcp_server(
        work_broker_gateway=FakeWorkBrokerGateway(),
        state_root=tmp_path / "state",
        workspace_root=_create_workspace(tmp_path),
    )

    async def read_descriptions() -> dict[str, str]:
        tools = await server.list_tools()
        return {tool.name: tool.description or "" for tool in tools}

    descriptions = asyncio.run(read_descriptions())
    delegate_description = descriptions["delegate_work"]
    orchestrate_description = descriptions["orchestrate_work"]

    assert "fire-and-report" in delegate_description
    assert "use orchestrate_work instead" in delegate_description
    assert "두 명이 검토해줘" in orchestrate_description
    assert "Main Review Gate" in orchestrate_description


def test_delegate_work_rejects_non_git_project_before_state_is_written(
    tmp_path: Path,
) -> None:
    state_root = tmp_path / "state"
    workspace_root = tmp_path / "workspace"
    project_root = workspace_root / "sample-project"
    project_root.mkdir(parents=True)
    (project_root / "pyproject.toml").write_text("[project]\nname = 'sample-project'\n")
    server = create_mcp_server(
        work_broker_gateway=FakeWorkBrokerGateway(),
        state_root=state_root,
        workspace_root=workspace_root,
    )
    project_id = (
        "prj_sample-project_"
        + hashlib.sha256(str(project_root.resolve()).encode("utf-8")).hexdigest()[:8]
    )

    async def delegate():
        return await server.call_tool(
            "delegate_work",
            {
                "goal": "fail fast",
                "project_id": project_id,
                "tasks": [
                    {
                        "key": "review",
                        "role": "reviewer",
                        "objective": "Review the project.",
                    }
                ],
            },
        )

    result = asyncio.run(delegate())
    structured = result.structured_content
    assert structured is not None
    payload = structured["result"]
    assert payload["ok"] is False
    assert payload["error_code"] == "git_repository_required"
    assert payload["operation"] == "run_create"
    assert not (state_root / "runs").exists()


def test_mcp_execution_state_can_live_under_configured_workspace(
    tmp_path: Path,
) -> None:
    control_root = tmp_path / "global-control"
    workspace_root = _create_workspace(tmp_path)
    execution_root = workspace_root / ".chattocodex"
    project_id = (
        WorkspaceProjectService(workspace_root=workspace_root)
        .list_projects()[0]
        .project_id
    )
    server = create_mcp_server(
        work_broker_gateway=FakeWorkBrokerGateway(),
        state_root=control_root,
        workspace_root=workspace_root,
        execution_state_root=execution_root,
    )

    async def delegate():
        return await server.call_tool(
            "delegate_work",
            {
                "goal": "workspace-local execution state",
                "project_id": project_id,
                "tasks": [
                    {
                        "key": "review",
                        "role": "reviewer",
                        "objective": "Review the project.",
                    }
                ],
            },
        )

    result = asyncio.run(delegate())
    assert result.structured_content is not None
    assert result.structured_content["result"]["ok"] is True
    assert (execution_root / "runs").is_dir()
    assert (execution_root / "broker" / "tasks").is_dir()
    assert not (control_root / "runs").exists()
    assert not (control_root / "broker").exists()


def test_mcp_ready_reconciliation_persists_bound_project_provisioning_demand(
    tmp_path: Path,
) -> None:
    control_root = tmp_path / "global-control"
    workspace_root = _create_workspace(tmp_path)
    execution_root = workspace_root / ".chattocodex"
    workspace_service = WorkspaceProjectService(workspace_root=workspace_root)
    project = workspace_service.list_projects()[0]
    ProjectBindingService(
        repository=ProjectBindingRepository(state_root=execution_root),
        workspace_project_service=workspace_service,
    ).upsert_binding(
        project_id=project.project_id,
        metadata=ProjectBindingAdapterMetadata(
            provider="chatgpt",
            project_ref="gpt-project-sample",
            project_path="/g/gpt-project-sample",
        ),
    )
    server = create_mcp_server(
        work_broker_gateway=FakeWorkBrokerGateway(),
        state_root=control_root,
        workspace_root=workspace_root,
        execution_state_root=execution_root,
    )

    async def delegate():
        return await server.call_tool(
            "delegate_work",
            {
                "goal": "provision from READY work",
                "project_id": project.project_id,
                "requested_parallelism": 2,
                "tasks": [
                    {
                        "key": "first",
                        "role": "implementer",
                        "objective": "Implement the first independent change.",
                    },
                    {
                        "key": "second",
                        "role": "reviewer",
                        "objective": "Review the second independent change.",
                    },
                ],
            },
        )

    result = asyncio.run(delegate())

    assert result.structured_content is not None
    assert result.structured_content["result"]["ok"] is True
    demand = WorkerProvisioningRepository(state_root=execution_root).read_demand(
        project_id=project.project_id
    )
    assert demand.desired_capacity == 2
    assert demand.spawn_needed == 2

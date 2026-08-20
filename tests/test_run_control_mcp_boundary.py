import asyncio
import subprocess
from pathlib import Path

from chattocodex.orchestrator_mcp.composition.mcp_server import create_mcp_server
from tests.broker_support import FakeWorkBrokerGateway


def _workspace(tmp_path: Path) -> Path:
    workspace_root = tmp_path / "workspace"
    project_root = workspace_root / "sample-project"
    project_root.mkdir(parents=True)
    (project_root / "pyproject.toml").write_text(
        "[project]\nname='sample-project'\nversion='0.1.0'\n",
        encoding="utf-8",
    )
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


def _payload(result: object) -> dict[str, object]:
    structured = result.structured_content
    assert isinstance(structured, dict)
    payload = structured["result"]
    assert isinstance(payload, dict)
    return payload


def test_orchestrate_work_and_status_are_real_mcp_boundaries(tmp_path: Path) -> None:
    server = create_mcp_server(
        work_broker_gateway=FakeWorkBrokerGateway(),
        state_root=tmp_path / "state",
        workspace_root=_workspace(tmp_path),
    )

    async def scenario() -> tuple[dict[str, object], dict[str, object]]:
        started = _payload(
            await server.call_tool(
                "orchestrate_work",
                {
                    "goal": "Parallel review followed by Main review",
                    "tasks": [
                        {
                            "key": "structure",
                            "objective": "Review structure.",
                            "role": "structure-reviewer",
                        },
                        {
                            "key": "tests",
                            "objective": "Review tests.",
                            "role": "test-reviewer",
                        },
                    ],
                    "review_instructions": "Inspect both reports before final approval.",
                    "max_rework_rounds": 2,
                },
            )
        )
        workflow = started["workflow"]
        assert isinstance(workflow, dict)
        status = _payload(
            await server.call_tool(
                "workflow_status",
                {
                    "workflow_id": workflow["workflow_id"],
                    "workflow_handle": started["workflow_handle"],
                },
            )
        )
        return started, status

    started, status = asyncio.run(scenario())
    workflow = started["workflow"]
    assert isinstance(workflow, dict)
    assert workflow["status"] == "running"
    assert status["needs_main_review"] is False
    current_run = status["current_run"]
    assert isinstance(current_run, dict)
    assert current_run["ready_count"] == 2
    assert current_run["queued_count"] == 2

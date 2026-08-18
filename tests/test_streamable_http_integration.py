import asyncio
import os
import socket
import subprocess
import sys
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client

from chattocodex.orchestration.entity_lock_registry import EntityLockRegistry
from chattocodex.persistence.wake_presence_repository import WakePresenceRepository
from chattocodex.persistence.wake_target_repository import WakeTargetRepository
from chattocodex.project_runtime.workspace_execution_layout import (
    WorkspaceExecutionLayout,
)
from chattocodex.schemas.setup_schemas import SetupRequest
from chattocodex.setup.runtime_setup_service import RuntimeSetupService
from chattocodex.wake.wake_presence_service import WakePresenceService
from chattocodex.wake.wake_target_service import WakeTargetService


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.bind(("127.0.0.1", 0))
        return int(server_socket.getsockname()[1])


def _wait_for_port(*, port: int, timeout_seconds: float = 10.0) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
            client_socket.settimeout(0.1)
            if client_socket.connect_ex(("127.0.0.1", port)) == 0:
                return
        time.sleep(0.05)
    raise TimeoutError(f"MCP server did not open port {port} within {timeout_seconds}s")


@contextmanager
def _running_server(
    *,
    state_root: Path,
    workspace_root: Path,
    port: int,
) -> Iterator[None]:
    RuntimeSetupService(state_root=state_root).configure(
        request=SetupRequest(
            workspace_root=workspace_root,
            public_url=None,
            oauth_enabled=False,
        )
    )
    environment = os.environ.copy()
    environment["CHATTOCODEX_STATE_ROOT"] = str(state_root)
    environment["CHATTOCODEX_WORKSPACE_ROOT"] = str(workspace_root)
    environment["CHATTOCODEX_PORT"] = str(port)
    process = subprocess.Popen(
        [sys.executable, "-m", "tests.mcp_test_server"],
        env=environment,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        _wait_for_port(port=port)
        yield
    finally:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


def _result_payload(result: object) -> dict[str, object]:
    structured_content = result.structured_content
    assert isinstance(structured_content, dict)
    payload = structured_content["result"]
    assert isinstance(payload, dict)
    return payload


def _register_active_wake_targets(
    *, state_root: Path, project_id: str, count: int
) -> tuple[str, ...]:
    presence = WakePresenceService(
        repository=WakePresenceRepository(state_root=state_root)
    )
    targets = WakeTargetService(
        repository=WakeTargetRepository(state_root=state_root),
        presence_service=presence,
        lock_registry=EntityLockRegistry(),
    )
    target_ids: list[str] = []
    for number in range(1, count + 1):
        wake_target_id = f"wtgt_{number:032x}"
        targets.register_target(wake_target_id=wake_target_id, project_id=project_id)
        presence.heartbeat(wake_target_id=wake_target_id)
        target_ids.append(wake_target_id)
    return tuple(target_ids)


async def _setup_run(url: str, *, state_root: Path) -> tuple[str, str]:
    async with streamable_http_client(url) as (read_stream, write_stream):
        async with ClientSession(read_stream, write_stream) as session:
            await session.initialize()
            projects = _result_payload(await session.call_tool("project_list", {}))
            projects_view = projects["projects"]
            assert isinstance(projects_view, list)
            project = projects_view[0]
            assert isinstance(project, dict)
            project_id = str(project["project_id"])
            _register_active_wake_targets(
                state_root=state_root,
                project_id=project_id,
                count=3,
            )
            delegated = _result_payload(
                await session.call_tool(
                    "delegate_work",
                    {
                        "goal": "Three generic Chat consumer integration",
                        "project_id": project_id,
                        "tasks": [
                            {
                                "key": f"lane-{index}",
                                "objective": f"Complete independent lane {index}",
                                "role": role,
                            }
                            for index, role in enumerate(
                                ("research", "implementation", "review"), start=1
                            )
                        ],
                    },
                )
            )
            run = delegated["run"]
            assert isinstance(run, dict)
            run_id = str(run["run_id"])
            tasks = delegated["tasks"]
            assert isinstance(tasks, list)
            assert len(tasks) == 3
            blocked_self_claim = _result_payload(
                await session.call_tool(
                    "work",
                    {
                        "project_id": project_id,
                        "label": "main-chat-must-not-self-claim",
                    },
                )
            )
            assert blocked_self_claim["ok"] is False
            assert blocked_self_claim["operation"] == "work"
            return run_id, project_id


async def _worker_flow(
    *,
    url: str,
    state_root: Path,
    run_id: str,
    project_id: str,
    worker_number: int,
) -> str:
    async with streamable_http_client(url) as (read_stream, write_stream):
        async with ClientSession(read_stream, write_stream) as session:
            await session.initialize()
            wake_target_id = f"wtgt_{worker_number:032x}"
            wake_revision = (
                WakeTargetRepository(state_root=state_root)
                .read(wake_target_id=wake_target_id)
                .wake_revision
            )
            claimed = _result_payload(
                await session.call_tool(
                    "work",
                    {
                        "project_id": project_id,
                        "label": f"http-consumer-{worker_number}",
                        "wake_target_id": wake_target_id,
                        "wake_revision": wake_revision,
                        "wait_seconds": 2,
                    },
                )
            )
            slot = claimed["slot"]
            assert isinstance(slot, dict)
            slot_id = str(slot["slot_id"])
            slot_handle = str(claimed["slot_handle"])
            assignment = claimed["assignment"]
            assert isinstance(assignment, dict)
            worker = assignment["worker"]
            task = assignment["task"]
            assert isinstance(worker, dict)
            assert isinstance(task, dict)
            worker_handle = str(assignment["worker_handle"])

            context = _result_payload(
                await session.call_tool(
                    "worker_context",
                    {
                        "run_id": run_id,
                        "worker_id": str(worker["worker_id"]),
                        "worker_handle": worker_handle,
                    },
                )
            )
            assert context["ok"] is True

            if worker_number == 1:
                tree = _result_payload(
                    await session.call_tool(
                        "project_tree",
                        {"run_id": run_id, "max_depth": 1, "max_entries": 20},
                    )
                )
                entries = tree["entries"]
                assert isinstance(entries, list)
                assert any(
                    isinstance(entry, dict)
                    and entry["relative_path"] == "pyproject.toml"
                    for entry in entries
                )
            elif worker_number == 2:
                file_read = _result_payload(
                    await session.call_tool(
                        "project_file_read",
                        {
                            "run_id": run_id,
                            "relative_path": "pyproject.toml",
                            "start_line": 1,
                            "end_line": 10,
                        },
                    )
                )
                assert "integration-project" in str(file_read["content"])
            else:
                search = _result_payload(
                    await session.call_tool(
                        "project_text_search",
                        {
                            "run_id": run_id,
                            "query": "integration-project",
                            "max_results": 10,
                        },
                    )
                )
                matches = search["matches"]
                assert isinstance(matches, list)
                assert any(
                    isinstance(match, dict)
                    and match["relative_path"] == "pyproject.toml"
                    for match in matches
                )

            completed = _result_payload(
                await session.call_tool(
                    "task_update",
                    {
                        "run_id": run_id,
                        "worker_id": str(worker["worker_id"]),
                        "worker_handle": worker_handle,
                        "task_id": str(task["task_id"]),
                        "expected_revision": 0,
                        "status": "completed",
                        "progress_summary": f"consumer {worker_number} completed its lane",
                        "result": f"consumer-{worker_number}-result",
                    },
                )
            )
            completed_task = completed["task"]
            assert isinstance(completed_task, dict)

            released = _result_payload(
                await session.call_tool(
                    "work_done",
                    {"slot_id": slot_id, "slot_handle": slot_handle},
                )
            )
            released_slot = released["slot"]
            assert isinstance(released_slot, dict)
            assert released_slot["status"] == "idle"
            return str(completed_task["task_id"])


async def _read_broker_status(*, url: str, run_id: str) -> dict[str, object]:
    async with streamable_http_client(url) as (read_stream, write_stream):
        async with ClientSession(read_stream, write_stream) as session:
            await session.initialize()
            return _result_payload(
                await session.call_tool("delegation_status", {"run_id": run_id})
            )


def test_three_independent_mcp_sessions_complete_one_broker_run(tmp_path: Path) -> None:
    port = _find_free_port()
    url = f"http://127.0.0.1:{port}/mcp"
    state_root = tmp_path / "state"
    workspace_root = tmp_path / "workspace"
    execution_state_root = WorkspaceExecutionLayout.state_root(
        workspace_root=workspace_root
    )
    project_root = workspace_root / "integration-project"
    project_root.mkdir(parents=True)
    (project_root / "pyproject.toml").write_text(
        "[project]\nname = 'integration-project'\n"
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

    with _running_server(
        state_root=state_root,
        workspace_root=workspace_root,
        port=port,
    ):
        run_id, project_id = asyncio.run(
            _setup_run(url, state_root=execution_state_root)
        )

        async def complete_workers() -> tuple[str, ...]:
            return tuple(
                await asyncio.gather(
                    *(
                        _worker_flow(
                            url=url,
                            state_root=execution_state_root,
                            run_id=run_id,
                            project_id=project_id,
                            worker_number=index,
                        )
                        for index in range(1, 4)
                    )
                )
            )

        completed_task_ids = asyncio.run(complete_workers())
        status = asyncio.run(_read_broker_status(url=url, run_id=run_id))

    assert status["is_complete"] is True
    assert status["completed_count"] == 3
    assert status["claimed_count"] == 0
    assert status["ready_count"] == 0
    assert status["busy_slot_count"] == 0
    assert status["idle_slot_count"] == 3
    assert len(set(completed_task_ids)) == 3

    restarted_port = _find_free_port()
    restarted_url = f"http://127.0.0.1:{restarted_port}/mcp"
    with _running_server(
        state_root=state_root,
        workspace_root=workspace_root,
        port=restarted_port,
    ):
        restored_status = asyncio.run(
            _read_broker_status(url=restarted_url, run_id=run_id)
        )

    assert restored_status["is_complete"] is True
    assert restored_status["completed_count"] == 3

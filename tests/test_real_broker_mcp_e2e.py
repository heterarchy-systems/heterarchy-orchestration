from __future__ import annotations

import asyncio
import os
import socket
import subprocess
import sys
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import pytest
from agent_broker import BrokerOperationsClient, OperationsClientConfig
from mcp import ClientSession, types
from mcp.client.streamable_http import streamable_http_client

from chattocodex.shared.concurrency.entity_lock_registry import EntityLockRegistry
from chattocodex.orchestrator_backend.persistence.wake.wake_presence_repository import (
    WakePresenceRepository,
)
from chattocodex.orchestrator_backend.persistence.wake.wake_target_repository import (
    WakeTargetRepository,
)
from chattocodex.shared.pathing.workspace_execution_layout import (
    WorkspaceExecutionLayout,
)
from chattocodex.shared.contracts.setup_schemas import SetupRequest
from chattocodex.orchestrator_backend.setup.runtime_setup_service import (
    RuntimeSetupService,
)
from chattocodex.orchestrator_backend.wake.presence.wake_presence_service import (
    WakePresenceService,
)
from chattocodex.orchestrator_backend.wake.target.wake_target_service import (
    WakeTargetService,
)


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


def _wait_for_port(*, port: int, process: subprocess.Popen[str]) -> None:
    deadline = time.monotonic() + 10.0
    while time.monotonic() < deadline:
        if process.poll() is not None:
            stderr = process.stderr.read() if process.stderr is not None else ""
            raise AssertionError(f"process exited before readiness: {stderr}")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                return
        except OSError:
            time.sleep(0.05)
    raise AssertionError(f"process did not listen on port {port}")


def _broker_binary() -> Path:
    configured = os.environ.get("HETERARCHY_BROKER_BINARY")
    if configured:
        binary = Path(configured).expanduser().resolve()
    else:
        binary = (
            Path(__file__).resolve().parents[2]
            / "heterarchy-broker"
            / "target"
            / "debug"
            / "agentbrokerd"
        )
    if not binary.is_file():
        pytest.skip(
            "real Broker MCP E2E requires HETERARCHY_BROKER_BINARY or a sibling "
            "heterarchy-broker/target/debug/agentbrokerd"
        )
    return binary


@contextmanager
def _running_broker(
    *, state_path: Path, broker_port: int, operations_port: int
) -> Iterator[None]:
    process = subprocess.Popen(
        (
            str(_broker_binary()),
            "serve",
            "--host",
            "127.0.0.1",
            "--port",
            str(broker_port),
            "--operations-port",
            str(operations_port),
            "--state-path",
            str(state_path),
        ),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        _wait_for_port(port=broker_port, process=process)
        _wait_for_port(port=operations_port, process=process)
        yield
    finally:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
        if process.stderr is not None:
            process.stderr.close()


@contextmanager
def _running_mcp_server(
    *,
    state_root: Path,
    workspace_root: Path,
    mcp_port: int,
    broker_port: int,
    operations_port: int,
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
    environment["CHATTOCODEX_PORT"] = str(mcp_port)
    environment["CHATTOCODEX_BROKER_HOST"] = "127.0.0.1"
    environment["CHATTOCODEX_BROKER_PORT"] = str(broker_port)
    environment["CHATTOCODEX_BROKER_OPERATIONS_PORT"] = str(operations_port)
    process = subprocess.Popen(
        (sys.executable, "-m", "tests.real_broker_mcp_test_server"),
        env=environment,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        _wait_for_port(port=mcp_port, process=process)
        yield
    finally:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
        if process.stderr is not None:
            process.stderr.close()


def _create_workspace(tmp_path: Path) -> Path:
    workspace_root = tmp_path / "workspace"
    project_root = workspace_root / "integration-project"
    project_root.mkdir(parents=True)
    (project_root / "pyproject.toml").write_text(
        "[project]\nname = 'integration-project'\nversion = '0.1.0'\n"
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


def _payload(
    result: types.CallToolResult | types.InputRequiredResult | types.Result,
) -> dict[str, object]:
    if not isinstance(result, types.CallToolResult):
        raise AssertionError(f"expected CallToolResult, got {type(result).__name__}")
    structured = result.structured_content
    assert isinstance(structured, dict)
    payload = structured["result"]
    assert isinstance(payload, dict)
    return payload


def _register_wake_targets(
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
    result: list[str] = []
    for number in range(1, count + 1):
        wake_target_id = f"wtgt_{number:032x}"
        targets.register_target(wake_target_id=wake_target_id, project_id=project_id)
        presence.heartbeat(wake_target_id=wake_target_id)
        result.append(wake_target_id)
    return tuple(result)


async def _create_run(*, url: str, execution_state_root: Path) -> tuple[str, str]:
    async with (
        streamable_http_client(url) as (read_stream, write_stream),
        ClientSession(read_stream, write_stream) as session,
    ):
        await session.initialize()
        projects = _payload(await session.call_tool("project_list", {}))
        project_rows = projects["projects"]
        assert isinstance(project_rows, list)
        project = project_rows[0]
        assert isinstance(project, dict)
        project_id = str(project["project_id"])
        _register_wake_targets(
            state_root=execution_state_root,
            project_id=project_id,
            count=2,
        )
        delegated = _payload(
            await session.call_tool(
                "delegate_work",
                {
                    "goal": "real Broker MCP two-worker E2E",
                    "project_id": project_id,
                    "requested_parallelism": 2,
                    "tasks": [
                        {
                            "key": "lane-a",
                            "objective": "Complete real Broker lane A.",
                            "role": "worker-a",
                        },
                        {
                            "key": "lane-b",
                            "objective": "Complete real Broker lane B.",
                            "role": "worker-b",
                        },
                    ],
                },
            )
        )
        assert delegated["ok"] is True
        run = delegated["run"]
        assert isinstance(run, dict)
        return str(run["run_id"]), project_id


async def _worker_flow(
    *,
    url: str,
    execution_state_root: Path,
    run_id: str,
    project_id: str,
    worker_number: int,
) -> str:
    async with (
        streamable_http_client(url) as (read_stream, write_stream),
        ClientSession(read_stream, write_stream) as session,
    ):
        await session.initialize()
        wake_target_id = f"wtgt_{worker_number:032x}"
        wake_revision = (
            WakeTargetRepository(state_root=execution_state_root)
            .read(wake_target_id=wake_target_id)
            .wake_revision
        )
        claimed = _payload(
            await session.call_tool(
                "work",
                {
                    "project_id": project_id,
                    "label": f"real-broker-consumer-{worker_number}",
                    "wake_target_id": wake_target_id,
                    "wake_revision": wake_revision,
                    "wait_seconds": 2,
                },
            )
        )
        assert claimed["ok"] is True
        slot = claimed["slot"]
        assignment = claimed["assignment"]
        assert isinstance(slot, dict)
        assert isinstance(assignment, dict)
        worker = assignment["worker"]
        task = assignment["task"]
        assert isinstance(worker, dict)
        assert isinstance(task, dict)
        worker_handle = str(assignment["worker_handle"])

        completed = _payload(
            await session.call_tool(
                "task_update",
                {
                    "run_id": run_id,
                    "worker_id": str(worker["worker_id"]),
                    "worker_handle": worker_handle,
                    "task_id": str(task["task_id"]),
                    "expected_revision": int(task["revision"]),
                    "status": "completed",
                    "progress_summary": f"real Broker consumer {worker_number} completed",
                    "result": f"real-broker-consumer-{worker_number}-result",
                },
            )
        )
        assert completed["ok"] is True
        released = _payload(
            await session.call_tool(
                "work_done",
                {
                    "slot_id": str(slot["slot_id"]),
                    "slot_handle": str(claimed["slot_handle"]),
                },
            )
        )
        assert released["ok"] is True
        released_slot = released["slot"]
        assert isinstance(released_slot, dict)
        assert released_slot["status"] == "idle"
        return str(task["task_id"])


async def _run_status(*, url: str, run_id: str) -> dict[str, object]:
    async with (
        streamable_http_client(url) as (read_stream, write_stream),
        ClientSession(read_stream, write_stream) as session,
    ):
        await session.initialize()
        return _payload(
            await session.call_tool("delegation_status", {"run_id": run_id})
        )


def test_real_broker_mcp_transport_completes_two_consumer_run(tmp_path: Path) -> None:
    broker_port = _free_port()
    operations_port = _free_port()
    mcp_port = _free_port()
    workspace_root = _create_workspace(tmp_path)
    state_root = tmp_path / "control-state"
    execution_state_root = WorkspaceExecutionLayout.state_root(
        workspace_root=workspace_root
    )
    url = f"http://127.0.0.1:{mcp_port}/mcp"

    with _running_broker(
        state_path=tmp_path / "broker-state.json",
        broker_port=broker_port,
        operations_port=operations_port,
    ):
        with _running_mcp_server(
            state_root=state_root,
            workspace_root=workspace_root,
            mcp_port=mcp_port,
            broker_port=broker_port,
            operations_port=operations_port,
        ):
            run_id, project_id = asyncio.run(
                _create_run(url=url, execution_state_root=execution_state_root)
            )

            async def complete_workers() -> tuple[str, ...]:
                return tuple(
                    await asyncio.gather(
                        *(
                            _worker_flow(
                                url=url,
                                execution_state_root=execution_state_root,
                                run_id=run_id,
                                project_id=project_id,
                                worker_number=number,
                            )
                            for number in range(1, 3)
                        )
                    )
                )

            completed_task_ids = asyncio.run(complete_workers())
            status = asyncio.run(_run_status(url=url, run_id=run_id))
            assert status["is_complete"] is True
            assert status["completed_count"] == 2
            assert status["claimed_count"] == 0
            assert status["busy_slot_count"] == 0
            assert len(set(completed_task_ids)) == 2

            operations = BrokerOperationsClient(
                OperationsClientConfig(
                    host="127.0.0.1",
                    port=operations_port,
                    timeout_seconds=2.0,
                )
            )
            directory = operations.list_groups()
            group = next(
                group
                for group in directory.groups
                if group.group_id == f"workers_{run_id}"
            )
            assert group.consumer_count == 2

        restarted_mcp_port = _free_port()
        restarted_url = f"http://127.0.0.1:{restarted_mcp_port}/mcp"
        with _running_mcp_server(
            state_root=state_root,
            workspace_root=workspace_root,
            mcp_port=restarted_mcp_port,
            broker_port=broker_port,
            operations_port=operations_port,
        ):
            restored = asyncio.run(_run_status(url=restarted_url, run_id=run_id))
        assert restored["is_complete"] is True
        assert restored["completed_count"] == 2

from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

import asyncpg
import pytest
from mcp import types
from pydantic import BaseModel
from sqlalchemy.engine import URL

from chattocodex.orchestrator_backend.composition.backend_container import (
    backend_resources,
    create_backend_container,
)
from chattocodex.orchestrator_backend.database.database_config import DatabaseConfig
from chattocodex.orchestrator_mcp.composition.mcp_server import create_mcp_server
from chattocodex.shared.contracts.group_folder_schemas import (
    GroupFolderCreateRequest,
    GroupFolderDeleteRequest,
    GroupFolderDeleteSuccess,
    GroupFolderGetRequest,
    GroupFolderGetSuccess,
    GroupFolderMoveRequest,
    GroupFolderMutationSuccess,
    GroupFolderRenameRequest,
    GroupFolderReorderRequest,
    GroupFolderTreeRequest,
    GroupFolderTreeSuccess,
)
from tests.broker_support import FakeWorkBrokerGateway

_RUN_POSTGRES_E2E = os.environ.get("CHATTOCODEX_RUN_POSTGRES_E2E", "") == "1"


@dataclass(frozen=True, slots=True, kw_only=True)
class _LocalPostgresSettings:
    user: str
    password: str
    port: int

    @classmethod
    def from_environment(cls) -> _LocalPostgresSettings:
        return cls(
            user=os.environ.get("CHATTOCODEX_POSTGRES_USER", "chattocodex"),
            password=os.environ.get(
                "CHATTOCODEX_POSTGRES_PASSWORD",
                "chattocodex-local",
            ),
            port=int(os.environ.get("CHATTOCODEX_POSTGRES_PORT", "55432")),
        )

    def url(self, *, database: str, async_sqlalchemy: bool) -> str:
        drivername = "postgresql+asyncpg" if async_sqlalchemy else "postgresql"
        return URL.create(
            drivername=drivername,
            username=self.user,
            password=self.password,
            host="127.0.0.1",
            port=self.port,
            database=database,
        ).render_as_string(hide_password=False)


def _parse_tool_result[ModelT: BaseModel](
    result: types.CallToolResult | types.InputRequiredResult | types.Result,
    *,
    model_type: type[ModelT],
) -> ModelT:
    if not isinstance(result, types.CallToolResult):
        raise AssertionError(f"expected CallToolResult, got {type(result).__name__}")
    assert result.is_error is False
    structured = result.structured_content
    assert structured is not None
    return model_type.model_validate(structured["result"])


async def _create_temporary_database(
    *,
    settings: _LocalPostgresSettings,
    database_name: str,
) -> None:
    connection = await asyncpg.connect(
        dsn=settings.url(database="postgres", async_sqlalchemy=False),
        timeout=5.0,
    )
    try:
        statement = await connection.fetchval(
            "SELECT format('CREATE DATABASE %I', $1)",
            database_name,
        )
        assert isinstance(statement, str)
        await connection.execute(statement)
    finally:
        await connection.close()


async def _drop_temporary_database(
    *,
    settings: _LocalPostgresSettings,
    database_name: str,
) -> None:
    connection = await asyncpg.connect(
        dsn=settings.url(database="postgres", async_sqlalchemy=False),
        timeout=5.0,
    )
    try:
        statement = await connection.fetchval(
            "SELECT format('DROP DATABASE %I WITH (FORCE)', $1)",
            database_name,
        )
        assert isinstance(statement, str)
        await connection.execute(statement)
    finally:
        await connection.close()


@pytest.mark.skipif(
    not _RUN_POSTGRES_E2E,
    reason="set CHATTOCODEX_RUN_POSTGRES_E2E=1 to run local PostgreSQL E2E",
)
def test_postgres_backed_mcp_group_folder_crud_on_fresh_database(
    tmp_path: Path,
) -> None:
    settings = _LocalPostgresSettings.from_environment()
    database_name = f"chattocodex_e2e_{uuid4().hex}"
    project_id = f"prj-postgres-e2e_{uuid4().hex[:8]}"

    async def scenario() -> None:
        await _create_temporary_database(
            settings=settings,
            database_name=database_name,
        )
        try:
            database_url = settings.url(
                database=database_name,
                async_sqlalchemy=True,
            )
            container = create_backend_container(
                database_config=DatabaseConfig(
                    url=database_url,
                    required=True,
                    pool_size=2,
                    max_overflow=1,
                    pool_timeout_seconds=5.0,
                )
            )
            service = container.group_folder_service()
            workspace_root = tmp_path / "workspace"
            workspace_root.mkdir()
            server = create_mcp_server(
                state_root=tmp_path / "state",
                workspace_root=workspace_root,
                work_broker_gateway=FakeWorkBrokerGateway(),
                group_folder_service=service,
            )

            async with backend_resources(container=container):
                root_a = _parse_tool_result(
                    await server.call_tool(
                        "group_folder_create",
                        GroupFolderCreateRequest(
                            project_id=project_id,
                            name="Alpha",
                        ).model_dump(mode="json"),
                    ),
                    model_type=GroupFolderMutationSuccess,
                ).folder
                root_b = _parse_tool_result(
                    await server.call_tool(
                        "group_folder_create",
                        GroupFolderCreateRequest(
                            project_id=project_id,
                            name="Beta",
                        ).model_dump(mode="json"),
                    ),
                    model_type=GroupFolderMutationSuccess,
                ).folder
                child = _parse_tool_result(
                    await server.call_tool(
                        "group_folder_create",
                        GroupFolderCreateRequest(
                            project_id=project_id,
                            name="Child",
                            parent_id=root_a.folder_id,
                        ).model_dump(mode="json"),
                    ),
                    model_type=GroupFolderMutationSuccess,
                ).folder

                loaded_child = _parse_tool_result(
                    await server.call_tool(
                        "group_folder_get",
                        GroupFolderGetRequest(
                            project_id=project_id,
                            folder_id=child.folder_id,
                        ).model_dump(mode="json"),
                    ),
                    model_type=GroupFolderGetSuccess,
                ).folder
                assert loaded_child.parent_id == root_a.folder_id

                renamed = _parse_tool_result(
                    await server.call_tool(
                        "group_folder_rename",
                        GroupFolderRenameRequest(
                            project_id=project_id,
                            folder_id=child.folder_id,
                            name="Child Renamed",
                        ).model_dump(mode="json"),
                    ),
                    model_type=GroupFolderMutationSuccess,
                ).folder
                assert renamed.name == "Child Renamed"

                moved = _parse_tool_result(
                    await server.call_tool(
                        "group_folder_move",
                        GroupFolderMoveRequest(
                            project_id=project_id,
                            folder_id=child.folder_id,
                            parent_id=root_b.folder_id,
                        ).model_dump(mode="json"),
                    ),
                    model_type=GroupFolderMutationSuccess,
                ).folder
                assert moved.parent_id == root_b.folder_id

                reordered = _parse_tool_result(
                    await server.call_tool(
                        "group_folder_reorder",
                        GroupFolderReorderRequest(
                            project_id=project_id,
                            folder_id=root_b.folder_id,
                            order_index=7,
                        ).model_dump(mode="json"),
                    ),
                    model_type=GroupFolderMutationSuccess,
                ).folder
                assert reordered.order_index == 7

                tree = _parse_tool_result(
                    await server.call_tool(
                        "group_folder_tree",
                        GroupFolderTreeRequest(project_id=project_id).model_dump(
                            mode="json"
                        ),
                    ),
                    model_type=GroupFolderTreeSuccess,
                ).tree.root
                roots_by_id = {node.folder.folder_id: node for node in tree}
                assert tuple(
                    node.folder.folder_id
                    for node in roots_by_id[root_b.folder_id].children
                ) == (child.folder_id,)

                for folder_id in (root_a.folder_id, root_b.folder_id):
                    deleted = _parse_tool_result(
                        await server.call_tool(
                            "group_folder_delete",
                            GroupFolderDeleteRequest(
                                project_id=project_id,
                                folder_id=folder_id,
                            ).model_dump(mode="json"),
                        ),
                        model_type=GroupFolderDeleteSuccess,
                    )
                    assert deleted.folder_id == folder_id

                final_tree = _parse_tool_result(
                    await server.call_tool(
                        "group_folder_tree",
                        GroupFolderTreeRequest(project_id=project_id).model_dump(
                            mode="json"
                        ),
                    ),
                    model_type=GroupFolderTreeSuccess,
                ).tree.root
                assert final_tree == ()
        finally:
            await _drop_temporary_database(
                settings=settings,
                database_name=database_name,
            )

    asyncio.run(scenario())

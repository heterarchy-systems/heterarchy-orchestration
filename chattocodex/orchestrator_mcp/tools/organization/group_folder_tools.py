from __future__ import annotations

from uuid import UUID

from mcp.server.mcpserver import MCPServer
from pydantic import ValidationError

from chattocodex.orchestrator_backend.orchestration.orchestration_exceptions import (
    OrchestrationError,
    OrchestrationErrorCode,
)
from chattocodex.orchestrator_backend.orchestration.orchestration_view_mapper import (
    build_tool_error,
    build_validation_error,
)
from chattocodex.orchestrator_backend.organization.group_folder_mapper import (
    build_group_folder_tree,
    build_group_folder_view,
)
from chattocodex.orchestrator_backend.organization.group_folder_service import (
    GroupFolderService,
)
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
from chattocodex.shared.contracts.orchestration_schemas import ToolError


def register_group_folder_tools(
    server: MCPServer[None],
    service: GroupFolderService,
) -> None:
    """Register strict MCP v2 tools for the optional project organization tree. Args: server, service. Returns: None."""

    @server.tool(
        name="group_folder_create",
        description=(
            "Create an optional organization folder inside one HETERARCHY project. "
            "Folders organize persistent Teams but have no Broker ownership semantics."
        ),
        structured_output=True,
    )
    async def group_folder_create(
        project_id: str,
        name: str,
        parent_id: UUID | None = None,
    ) -> GroupFolderMutationSuccess | ToolError:
        """Group folder create. Args: project_id, name, parent_id. Returns: GroupFolderMutationSuccess | ToolError."""
        operation = "group_folder_create"
        try:
            request = GroupFolderCreateRequest(
                project_id=project_id,
                name=name,
                parent_id=parent_id,
            )
            folder = await service.create_folder(
                project_id=request.project_id,
                name=request.name,
                parent_id=request.parent_id,
            )
            return GroupFolderMutationSuccess(
                folder=build_group_folder_view(folder=folder)
            )
        except ValidationError as error:
            return build_validation_error(operation=operation, error=error)
        except ValueError as error:
            return _build_value_error(operation=operation, error=error)
        except OrchestrationError as error:
            return build_tool_error(error=error)

    @server.tool(
        name="group_folder_get",
        description="Read one project-scoped organization folder by its stable identifier.",
        structured_output=True,
    )
    async def group_folder_get(
        project_id: str,
        folder_id: UUID,
    ) -> GroupFolderGetSuccess | ToolError:
        """Group folder get. Args: project_id, folder_id. Returns: GroupFolderGetSuccess | ToolError."""
        operation = "group_folder_get"
        try:
            request = GroupFolderGetRequest(
                project_id=project_id,
                folder_id=folder_id,
            )
            folder = await service.get_folder(
                project_id=request.project_id,
                folder_id=request.folder_id,
            )
            return GroupFolderGetSuccess(folder=build_group_folder_view(folder=folder))
        except ValidationError as error:
            return build_validation_error(operation=operation, error=error)
        except OrchestrationError as error:
            return build_tool_error(error=error)

    @server.tool(
        name="group_folder_tree",
        description=(
            "Return the complete deterministic organization folder tree for one project. "
            "The result is a named Pydantic root model rather than a bare collection."
        ),
        structured_output=True,
    )
    async def group_folder_tree(
        project_id: str,
    ) -> GroupFolderTreeSuccess | ToolError:
        """Group folder tree. Args: project_id. Returns: GroupFolderTreeSuccess | ToolError."""
        operation = "group_folder_tree"
        try:
            request = GroupFolderTreeRequest(project_id=project_id)
            nodes = await service.get_tree(project_id=request.project_id)
            return GroupFolderTreeSuccess(tree=build_group_folder_tree(nodes=nodes))
        except ValidationError as error:
            return build_validation_error(operation=operation, error=error)
        except OrchestrationError as error:
            return build_tool_error(error=error)

    @server.tool(
        name="group_folder_rename",
        description="Rename a project organization folder without changing its stable identity.",
        structured_output=True,
    )
    async def group_folder_rename(
        project_id: str,
        folder_id: UUID,
        name: str,
    ) -> GroupFolderMutationSuccess | ToolError:
        """Group folder rename. Args: project_id, folder_id, name. Returns: GroupFolderMutationSuccess | ToolError."""
        operation = "group_folder_rename"
        try:
            request = GroupFolderRenameRequest(
                project_id=project_id,
                folder_id=folder_id,
                name=name,
            )
            folder = await service.rename_folder(
                project_id=request.project_id,
                folder_id=request.folder_id,
                name=request.name,
            )
            return GroupFolderMutationSuccess(
                folder=build_group_folder_view(folder=folder)
            )
        except ValidationError as error:
            return build_validation_error(operation=operation, error=error)
        except ValueError as error:
            return _build_value_error(operation=operation, error=error)
        except OrchestrationError as error:
            return build_tool_error(error=error)

    @server.tool(
        name="group_folder_move",
        description=(
            "Move a project organization folder to another parent or to project root. "
            "Self and descendant moves fail closed."
        ),
        structured_output=True,
    )
    async def group_folder_move(
        project_id: str,
        folder_id: UUID,
        parent_id: UUID | None = None,
    ) -> GroupFolderMutationSuccess | ToolError:
        """Group folder move. Args: project_id, folder_id, parent_id. Returns: GroupFolderMutationSuccess | ToolError."""
        operation = "group_folder_move"
        try:
            request = GroupFolderMoveRequest(
                project_id=project_id,
                folder_id=folder_id,
                parent_id=parent_id,
            )
            folder = await service.move_folder(
                project_id=request.project_id,
                folder_id=request.folder_id,
                parent_id=request.parent_id,
            )
            return GroupFolderMutationSuccess(
                folder=build_group_folder_view(folder=folder)
            )
        except ValidationError as error:
            return build_validation_error(operation=operation, error=error)
        except OrchestrationError as error:
            return build_tool_error(error=error)

    @server.tool(
        name="group_folder_reorder",
        description="Set one non-negative sibling ordering value for deterministic rendering.",
        structured_output=True,
    )
    async def group_folder_reorder(
        project_id: str,
        folder_id: UUID,
        order_index: int,
    ) -> GroupFolderMutationSuccess | ToolError:
        """Group folder reorder. Args: project_id, folder_id, order_index. Returns: GroupFolderMutationSuccess | ToolError."""
        operation = "group_folder_reorder"
        try:
            request = GroupFolderReorderRequest(
                project_id=project_id,
                folder_id=folder_id,
                order_index=order_index,
            )
            folder = await service.reorder_folder(
                project_id=request.project_id,
                folder_id=request.folder_id,
                order_index=request.order_index,
            )
            return GroupFolderMutationSuccess(
                folder=build_group_folder_view(folder=folder)
            )
        except ValidationError as error:
            return build_validation_error(operation=operation, error=error)
        except OrchestrationError as error:
            return build_tool_error(error=error)

    @server.tool(
        name="group_folder_delete",
        description=(
            "Delete one organization folder and its folder descendants. "
            "Future Team placement records move to root rather than becoming Broker state."
        ),
        structured_output=True,
    )
    async def group_folder_delete(
        project_id: str,
        folder_id: UUID,
    ) -> GroupFolderDeleteSuccess | ToolError:
        """Group folder delete. Args: project_id, folder_id. Returns: GroupFolderDeleteSuccess | ToolError."""
        operation = "group_folder_delete"
        try:
            request = GroupFolderDeleteRequest(
                project_id=project_id,
                folder_id=folder_id,
            )
            await service.delete_folder(
                project_id=request.project_id,
                folder_id=request.folder_id,
            )
            return GroupFolderDeleteSuccess(folder_id=request.folder_id)
        except ValidationError as error:
            return build_validation_error(operation=operation, error=error)
        except OrchestrationError as error:
            return build_tool_error(error=error)


def _build_value_error(operation: str, error: ValueError) -> ToolError:
    """Build value error. Args: operation, error. Returns: ToolError."""
    return ToolError(
        error_code=OrchestrationErrorCode.VALIDATION_ERROR,
        message=str(error),
        operation=operation,
        resource_id=None,
        recoverable=False,
        current_revision=None,
    )

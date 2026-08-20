from __future__ import annotations

from chattocodex.orchestrator_backend.organization.group_folder_dto import (
    GroupFolder,
    GroupFolderTreeNode,
)
from chattocodex.shared.contracts.group_folder_schemas import (
    GroupFolderTree,
    GroupFolderTreeNodeView,
    GroupFolderView,
)


def build_group_folder_view(folder: GroupFolder) -> GroupFolderView:
    """Map one internal GroupFolder DTO to its strict external schema. Args: folder. Returns: GroupFolderView."""

    return GroupFolderView(
        folder_id=folder.folder_id,
        project_id=folder.project_id,
        parent_id=folder.parent_id,
        name=folder.name,
        order_index=folder.order_index,
        created_at=folder.created_at,
        updated_at=folder.updated_at,
    )


def build_group_folder_tree(
    nodes: tuple[GroupFolderTreeNode, ...],
) -> GroupFolderTree:
    """Map an immutable internal tree to the named root response model. Args: nodes. Returns: GroupFolderTree."""

    return GroupFolderTree(root=tuple(_build_tree_node(node=node) for node in nodes))


def _build_tree_node(node: GroupFolderTreeNode) -> GroupFolderTreeNodeView:
    """Build tree node. Args: node. Returns: GroupFolderTreeNodeView."""
    return GroupFolderTreeNodeView(
        folder=build_group_folder_view(folder=node.folder),
        children=tuple(_build_tree_node(node=child) for child in node.children),
    )

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Literal
from uuid import UUID

from pydantic import StringConstraints

from chattocodex.shared.contracts.project.project_schemas import ProjectId
from chattocodex.shared.contracts.schema_base import (
    StrictRootSchemaModel,
    StrictSchemaModel,
    described_field,
)

GroupFolderId = Annotated[
    UUID,
    described_field(
        description="Stable opaque identifier for one project organization folder."
    ),
]
GroupFolderName = Annotated[
    str,
    StringConstraints(strict=True, strip_whitespace=True, min_length=1, max_length=255),
    described_field(
        description="Human-readable organization folder name, unique among siblings."
    ),
]
GroupFolderOrderIndex = Annotated[
    int,
    described_field(
        description="Non-negative sibling ordering value used for deterministic tree rendering.",
        ge=0,
    ),
]


class GroupFolderCreateRequest(StrictSchemaModel):
    project_id: Annotated[
        ProjectId,
        described_field(
            description="Canonical local project that owns the organization folder."
        ),
    ]
    name: Annotated[
        GroupFolderName,
        described_field(description="Organization folder name to create."),
    ]
    parent_id: Annotated[
        GroupFolderId | None,
        described_field(
            description="Optional parent folder; null creates the folder at project root."
        ),
    ] = None


class GroupFolderGetRequest(StrictSchemaModel):
    project_id: Annotated[
        ProjectId,
        described_field(
            description="Canonical local project expected to own the requested folder."
        ),
    ]
    folder_id: Annotated[
        GroupFolderId,
        described_field(description="Organization folder identifier to retrieve."),
    ]


class GroupFolderTreeRequest(StrictSchemaModel):
    project_id: Annotated[
        ProjectId,
        described_field(
            description="Canonical local project whose organization tree is requested."
        ),
    ]


class GroupFolderRenameRequest(GroupFolderGetRequest):
    name: Annotated[
        GroupFolderName,
        described_field(description="Replacement organization folder name."),
    ]


class GroupFolderMoveRequest(GroupFolderGetRequest):
    parent_id: Annotated[
        GroupFolderId | None,
        described_field(
            description="New parent folder; null moves the folder to project root."
        ),
    ] = None


class GroupFolderReorderRequest(GroupFolderGetRequest):
    order_index: Annotated[
        GroupFolderOrderIndex,
        described_field(description="Replacement sibling ordering value."),
    ]


class GroupFolderDeleteRequest(GroupFolderGetRequest):
    pass


class GroupFolderView(StrictSchemaModel):
    folder_id: Annotated[
        GroupFolderId,
        described_field(description="Stable identifier of this organization folder."),
    ]
    project_id: Annotated[
        ProjectId,
        described_field(description="Canonical local project owning this folder."),
    ]
    parent_id: Annotated[
        GroupFolderId | None,
        described_field(
            description="Parent organization folder identifier, or null at project root."
        ),
    ]
    name: Annotated[
        GroupFolderName,
        described_field(description="Current human-readable organization folder name."),
    ]
    order_index: Annotated[
        GroupFolderOrderIndex,
        described_field(description="Current deterministic sibling ordering value."),
    ]
    created_at: Annotated[
        datetime,
        described_field(
            description="Timezone-aware timestamp when the folder was created."
        ),
    ]
    updated_at: Annotated[
        datetime,
        described_field(
            description="Timezone-aware timestamp of the latest folder mutation."
        ),
    ]


class GroupFolderTreeNodeView(StrictSchemaModel):
    folder: Annotated[
        GroupFolderView,
        described_field(description="Folder represented by this tree node."),
    ]
    children: Annotated[
        tuple[GroupFolderTreeNodeView, ...],
        described_field(
            description="Deterministically ordered child organization folders."
        ),
    ]


class GroupFolderTree(StrictRootSchemaModel[tuple[GroupFolderTreeNodeView, ...]]):
    """Named root collection for one project organization tree."""


class GroupFolderMutationSuccess(StrictSchemaModel):
    ok: Annotated[
        Literal[True],
        described_field(description="True when the folder mutation completed."),
    ] = True
    folder: Annotated[
        GroupFolderView,
        described_field(description="Authoritative folder state after the mutation."),
    ]


class GroupFolderGetSuccess(StrictSchemaModel):
    ok: Annotated[
        Literal[True],
        described_field(description="True when the requested folder was found."),
    ] = True
    folder: Annotated[
        GroupFolderView,
        described_field(description="Requested authoritative folder state."),
    ]


class GroupFolderTreeSuccess(StrictSchemaModel):
    ok: Annotated[
        Literal[True],
        described_field(description="True when the project folder tree was loaded."),
    ] = True
    tree: Annotated[
        GroupFolderTree,
        described_field(description="Named root model containing the folder tree."),
    ]


class GroupFolderDeleteSuccess(StrictSchemaModel):
    ok: Annotated[
        Literal[True],
        described_field(description="True when the folder subtree was deleted."),
    ] = True
    folder_id: Annotated[
        GroupFolderId,
        described_field(description="Identifier of the deleted organization folder."),
    ]

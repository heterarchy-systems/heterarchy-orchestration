from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from pydantic import ValidationError

from chattocodex.shared.contracts.group_folder_schemas import (
    GroupFolderCreateRequest,
    GroupFolderTree,
    GroupFolderTreeNodeView,
    GroupFolderView,
)

_PROJECT_ID = "prj_folder-schema_1a2b3c4d"


def test_group_folder_schema_fields_have_descriptions() -> None:
    schema = GroupFolderCreateRequest.model_json_schema()
    properties = schema["properties"]
    assert all(
        property_schema.get("description") for property_schema in properties.values()
    )


def test_group_folder_request_rejects_blank_name_and_extra_fields() -> None:
    with pytest.raises(ValidationError):
        GroupFolderCreateRequest.model_validate(
            {
                "project_id": _PROJECT_ID,
                "name": "   ",
                "parent_id": None,
                "unexpected": True,
            }
        )


def test_group_folder_tree_is_a_named_root_model() -> None:
    now = datetime.now(UTC)
    folder = GroupFolderView(
        folder_id=uuid4(),
        project_id=_PROJECT_ID,
        parent_id=None,
        name="Research",
        order_index=1_000,
        created_at=now,
        updated_at=now,
    )
    tree = GroupFolderTree(root=(GroupFolderTreeNodeView(folder=folder, children=()),))
    assert tree.root[0].folder.name == "Research"
    assert GroupFolderTree.model_json_schema()["title"] == "GroupFolderTree"

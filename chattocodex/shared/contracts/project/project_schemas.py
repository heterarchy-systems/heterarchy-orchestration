from typing import Annotated, Literal

from pydantic import StringConstraints

from chattocodex.shared.contracts.schema_base import StrictSchemaModel

ProjectId = Annotated[
    str,
    StringConstraints(strict=True, pattern=r"^prj_[a-z0-9-]{1,64}_[0-9a-f]{8}$"),
]
ProjectName = Annotated[
    str,
    StringConstraints(strict=True, strip_whitespace=True, min_length=1, max_length=255),
]
ProjectRoot = Annotated[
    str,
    StringConstraints(
        strict=True, strip_whitespace=True, min_length=1, max_length=4096
    ),
]


class ProjectBoundaryModel(StrictSchemaModel):
    pass


class ProjectView(ProjectBoundaryModel):
    project_id: ProjectId
    name: ProjectName
    root: ProjectRoot


class ProjectListSuccess(ProjectBoundaryModel):
    ok: Literal[True] = True
    projects: tuple[ProjectView, ...]

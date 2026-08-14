from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, StringConstraints

ProjectId = Annotated[
    str,
    StringConstraints(pattern=r"^prj_[a-z0-9-]{1,64}_[0-9a-f]{8}$"),
]
ProjectName = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=255),
]
ProjectRoot = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=4096),
]


class ProjectBoundaryModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        validate_default=True,
    )


class ProjectView(ProjectBoundaryModel):
    project_id: ProjectId
    name: ProjectName
    root: ProjectRoot


class ProjectListSuccess(ProjectBoundaryModel):
    ok: Literal[True] = True
    projects: tuple[ProjectView, ...]

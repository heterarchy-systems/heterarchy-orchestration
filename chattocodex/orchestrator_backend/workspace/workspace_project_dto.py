from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True, kw_only=True)
class WorkspaceProject:
    project_id: str
    name: str
    root: Path

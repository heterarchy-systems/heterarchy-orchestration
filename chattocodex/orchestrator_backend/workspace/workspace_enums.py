from enum import StrEnum


class ProjectMarker(StrEnum):
    """Filesystem markers that identify a discoverable workspace project."""

    GIT = ".git"
    PYPROJECT = "pyproject.toml"
    PACKAGE_JSON = "package.json"
    CARGO = "Cargo.toml"
    GO_MODULE = "go.mod"
    REQUIREMENTS = "requirements.txt"

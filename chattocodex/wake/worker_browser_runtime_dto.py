from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path


class WorkerBrowserRuntimeMode(StrEnum):
    """Provider browser mode for the reusable Project Worker pool."""

    HEADLESS = "headless"
    VISIBLE_BOOTSTRAP = "visible_bootstrap"


@dataclass(frozen=True, slots=True)
class WorkerBrowserLaunchSpec:
    """Immutable launch description for one persistent Worker browser runtime."""

    chrome_binary: Path
    profile_root: Path
    extension_root: Path
    mode: WorkerBrowserRuntimeMode
    argv: tuple[str, ...]

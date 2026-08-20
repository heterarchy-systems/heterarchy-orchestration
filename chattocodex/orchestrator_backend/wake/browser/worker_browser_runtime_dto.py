from dataclasses import dataclass
from pathlib import Path

from chattocodex.shared.element_collect.wake_enums import WorkerBrowserRuntimeMode


@dataclass(frozen=True, slots=True)
class WorkerBrowserLaunchSpec:
    """Immutable launch description for one persistent Worker browser runtime."""

    chrome_binary: Path
    profile_root: Path
    extension_root: Path
    mode: WorkerBrowserRuntimeMode
    argv: tuple[str, ...]

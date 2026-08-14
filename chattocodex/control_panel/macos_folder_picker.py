import asyncio
import platform
from pathlib import Path

from chattocodex.control_panel.control_panel_exceptions import FolderPickerError

_PICKER_SCRIPT = (
    'POSIX path of (choose folder with prompt "ChatToCodex 작업 폴더 선택")'
)


async def choose_workspace_folder() -> Path | None:
    if platform.system() != "Darwin":
        raise FolderPickerError(
            message="Native folder selection is currently implemented for macOS only."
        )
    process = await asyncio.create_subprocess_exec(
        "/usr/bin/osascript",
        "-e",
        _PICKER_SCRIPT,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await process.communicate()
    if process.returncode == 0:
        selected = stdout.decode("utf-8", errors="replace").strip()
        return Path(selected).expanduser().resolve() if selected else None
    error_text = stderr.decode("utf-8", errors="replace").strip()
    if "User canceled" in error_text or "-128" in error_text:
        return None
    raise FolderPickerError(
        message="macOS folder picker failed. Enter the workspace path manually and retry."
    )

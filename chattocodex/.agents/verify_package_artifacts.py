from __future__ import annotations

import subprocess
from pathlib import Path
from tempfile import TemporaryDirectory
from zipfile import ZipFile

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONTROL_PANEL_STATIC_ROOT = "chattocodex/orchestrator_backend/control_panel/static/"
REQUIRED_FILES = (
    "chattocodex/orchestrator_backend/control_panel/static/index.html",
    "chattocodex/orchestrator_backend/control_panel/static/app.js",
    "chattocodex/orchestrator_backend/control_panel/static/frontend-source.sha256",
    "chattocodex/orchestrator_backend/wake/chrome_extension/manifest.json",
    "chattocodex/orchestrator_backend/wake/chrome_extension/service_worker.js",
)
REQUIRED_PREFIXES = ("chattocodex/orchestrator_backend/control_panel/static/_next/",)
FORBIDDEN_PREFIXES = ("tests/",)
FORBIDDEN_SEGMENTS = ("/.agents/",)
ALLOWED_CONTROL_PANEL_ROOT_ENTRIES = frozenset(
    {"_next", "app.js", "frontend-source.sha256", "index.html"}
)


def _run(*args: str) -> None:
    subprocess.run(
        args,
        cwd=PROJECT_ROOT,
        check=True,
        timeout=120,
        capture_output=True,
        text=True,
    )


def main() -> int:
    tracked_paths = subprocess.run(
        ("git", "ls-files", "-z", "--", CONTROL_PANEL_STATIC_ROOT),
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    ).stdout.split("\0")
    tracked_root_entries = {
        relative_path.removeprefix(CONTROL_PANEL_STATIC_ROOT).split("/", 1)[0]
        for relative_path in tracked_paths
        if relative_path
        and (PROJECT_ROOT / relative_path).exists()
        and relative_path.removeprefix(CONTROL_PANEL_STATIC_ROOT)
    }
    errors = [
        f"tracked source contains unexpected Control Panel root entry: {entry_name}"
        for entry_name in sorted(
            tracked_root_entries - ALLOWED_CONTROL_PANEL_ROOT_ENTRIES
        )
    ]
    with TemporaryDirectory(prefix="chattocodex-package-check-") as temporary_root:
        output_root = Path(temporary_root)
        _run(
            "uv",
            "build",
            "--sdist",
            "--out-dir",
            temporary_root,
            "--offline",
            "--no-build-logs",
        )
        source_distribution = next(output_root.glob("*.tar.gz"))
        _run(
            "uv",
            "build",
            "--wheel",
            str(source_distribution),
            "--out-dir",
            temporary_root,
            "--offline",
            "--no-build-logs",
        )
        wheel = next(output_root.glob("*.whl"))
        with ZipFile(wheel) as archive:
            names = tuple(archive.namelist())

    errors.extend(
        f"wheel is missing required file: {required_file}"
        for required_file in REQUIRED_FILES
        if required_file not in names
    )
    errors.extend(
        f"wheel is missing required content: {required_prefix}"
        for required_prefix in REQUIRED_PREFIXES
        if not any(name.startswith(required_prefix) for name in names)
    )
    errors.extend(
        f"wheel contains forbidden package content: {forbidden_prefix}"
        for forbidden_prefix in FORBIDDEN_PREFIXES
        if any(name.startswith(forbidden_prefix) for name in names)
    )
    errors.extend(
        f"wheel contains forbidden package content: {forbidden_segment}"
        for forbidden_segment in FORBIDDEN_SEGMENTS
        if any(forbidden_segment in name for name in names)
    )
    control_panel_root_entries = {
        name.removeprefix(CONTROL_PANEL_STATIC_ROOT).split("/", 1)[0]
        for name in names
        if name.startswith(CONTROL_PANEL_STATIC_ROOT)
        and name.removeprefix(CONTROL_PANEL_STATIC_ROOT)
    }
    errors.extend(
        f"wheel contains unexpected Control Panel root entry: {entry_name}"
        for entry_name in sorted(
            control_panel_root_entries - ALLOWED_CONTROL_PANEL_ROOT_ENTRIES
        )
    )

    if errors:
        for error in errors:
            print(error)
        return 1
    print("package artifact verification passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

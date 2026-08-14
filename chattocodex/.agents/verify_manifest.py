#!/usr/bin/env python3
"""Verify the rule harness manifest without third-party dependencies."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

HARNESS_ROOT = Path(__file__).resolve().parent
MANIFEST_PATH = HARNESS_ROOT / "MANIFEST.json"


def main() -> int:
    manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    rules_root = HARNESS_ROOT / manifest["rules_root"]
    expected_paths = {entry["path"] for entry in manifest["files"]}
    actual_paths = {
        path.relative_to(HARNESS_ROOT).as_posix()
        for path in rules_root.rglob("*")
        if path.is_file()
    }
    errors: list[str] = []

    if expected_paths != actual_paths:
        errors.append(
            f"file set mismatch: missing={sorted(expected_paths - actual_paths)}, "
            f"unexpected={sorted(actual_paths - expected_paths)}"
        )

    for entry in manifest["files"]:
        path = HARNESS_ROOT / entry["path"]
        if not path.is_file():
            continue
        content = path.read_bytes()
        digest = hashlib.sha256(content).hexdigest()
        if len(content) != entry["bytes"]:
            errors.append(f"byte count mismatch: {entry['path']}")
        if digest != entry["sha256"]:
            errors.append(f"sha256 mismatch: {entry['path']}")

    if errors:
        for error in errors:
            print(error)
        return 1
    print(f"verified {len(expected_paths)} rule files")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

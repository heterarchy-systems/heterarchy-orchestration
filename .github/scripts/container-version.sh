#!/usr/bin/env bash
set -euo pipefail

version=$(cargo metadata --locked --format-version 1 --no-deps |
    jq --exit-status --raw-output '.packages[] | select(.name == "heterarchy-orchestration-backend") | .version')
if [[ ! "$version" =~ ^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]]; then
    printf 'Backend Cargo version must be numeric MAJOR.MINOR.PATCH.\n' >&2
    exit 2
fi
case "${GITHUB_REF:-refs/heads/main}" in
    refs/heads/main) ;;
    "refs/tags/container-v$version") ;;
    *)
        printf 'Container tag must match container-v%s.\n' "$version" >&2
        exit 2
        ;;
esac
printf 'version=%s\n' "$version"

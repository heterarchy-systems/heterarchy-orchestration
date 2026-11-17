#!/usr/bin/env bash
set -euo pipefail

if (( $# != 3 )); then
    printf 'Usage: %s <ghcr.io/organization/image> <version> <inspect|prepare|published>\n' "$0" >&2
    exit 2
fi

image=$1
version=$2
phase=$3
if [[ ! "$image" =~ ^ghcr\.io/[a-z0-9-]+/[a-z0-9._-]+$ ]]; then
    printf 'Expected a GHCR organization image name.\n' >&2
    exit 2
fi
if [[ ! "$version" =~ ^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]]; then
    printf 'Expected a plain numeric SemVer image version.\n' >&2
    exit 2
fi
case "$phase" in
    inspect|prepare|published) ;;
    *)
        printf 'Unknown GHCR verification phase.\n' >&2
        exit 2
        ;;
esac

package=${image#ghcr.io/}
owner=${package%%/*}
name=${package#*/}
endpoint="orgs/$owner/packages/container/$name"
response=$(mktemp)
error=$(mktemp)
trap 'rm -f "$response" "$error"' EXIT

if ! gh api --include --jq .visibility "$endpoint" > "$response" 2> "$error"; then
    http_status=$(awk 'NR == 1 {print $2}' "$response")
    if [[ "$http_status" == 404 && "$phase" != published ]]; then
        printf 'New GHCR package will use the default private visibility.\n'
        exit 0
    fi
    cat "$error" >&2
    printf 'GHCR package verification failed.\n' >&2
    exit 1
fi

visibility=$(tail -n 1 "$response")
if [[ "$visibility" != private ]]; then
    printf 'Expected private GHCR visibility; received %s.\n' "$visibility" >&2
    exit 1
fi
printf 'GHCR package visibility verified: private.\n'
if [[ "$phase" == inspect ]]; then
    exit 0
fi

version_records=$(gh api --paginate "$endpoint/versions?per_page=100" \
    --jq '.[].metadata.container.tags // [] | join(",")')
version_exists=false
latest_matches_version=false
while IFS= read -r tags; do
    if [[ ",$tags," == *",$version,"* ]]; then
        version_exists=true
        if [[ ",$tags," == *,latest,* ]]; then
            latest_matches_version=true
        fi
    fi
done <<< "$version_records"

if [[ "$phase" == prepare && "$version_exists" == true ]]; then
    printf 'GHCR version %s already exists; refusing to overwrite it.\n' "$version" >&2
    exit 1
fi
if [[ "$phase" == published && "$version_exists" != true ]]; then
    printf 'Published GHCR version %s was not found.\n' "$version" >&2
    exit 1
fi
if [[ "$phase" == published && "$latest_matches_version" != true ]]; then
    printf 'GHCR latest does not refer to the same image version %s.\n' "$version" >&2
    exit 1
fi
printf 'GHCR numeric version and latest tag verified: %s.\n' "$version"

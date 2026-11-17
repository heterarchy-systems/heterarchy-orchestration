#!/usr/bin/env bash
set -euo pipefail

if (( $# != 2 )); then
    printf 'Usage: %s <image-reference> <expected-version>\n' "$0" >&2
    exit 2
fi

image=$1
expected_version=$2
container="orchestration-image-smoke-${GITHUB_RUN_ID:-local}-$$"
initializer="$container-init"
volume="$container-state"
cleanup() {
    docker rm --force "$container" "$initializer" >/dev/null 2>&1 || true
    docker volume rm "$volume" >/dev/null 2>&1 || true
}
trap cleanup EXIT

wait_for_health() {
    for attempt in {1..45}; do
        status=$(docker inspect --format '{{.State.Health.Status}}' "$container")
        if [[ "$status" == healthy ]]; then
            return
        fi
        running=$(docker inspect --format '{{.State.Running}}' "$container")
        if [[ "$running" != true ]]; then
            break
        fi
        sleep 2
    done
    docker logs "$container" >&2
    printf 'Orchestration image did not become healthy.\n' >&2
    exit 1
}

assert_authenticated() {
    result=$(docker exec "$container" /usr/local/bin/heterarchy-orchestration-backend \
        profile list --bearer-file /state/smoke-bearer --addr 127.0.0.1:8798)
    printf '%s' "$result" | jq --exit-status 'type == "array" and length == 0' >/dev/null
    rejected=$(docker exec "$container" curl --silent --output /dev/null \
        --write-out '%{http_code}' http://127.0.0.1:8798/internal/v1/agent-profiles)
    [[ "$rejected" == 401 ]]
}

docker volume create "$volume" >/dev/null
version=$(docker run --rm --entrypoint /usr/local/bin/heterarchy-orchestration-backend "$image" --version)
[[ "$version" == "heterarchy-orchestration-backend $expected_version" ]]
docker run --rm --name "$initializer" --volume "$volume:/state" --read-only \
    --tmpfs /tmp:rw,nosuid,nodev,size=64m,mode=1777 \
    "$image" token issue --initialize-authority --label container-smoke --output /state/smoke-bearer
docker run --detach --name "$container" --network host --volume "$volume:/state" \
    --read-only --tmpfs /tmp:rw,nosuid,nodev,size=64m,mode=1777 \
    --cap-drop ALL --security-opt no-new-privileges:true "$image" >/dev/null
wait_for_health
key_metadata=$(docker exec "$container" stat --format '%a %s %u' \
    '/state/auth/Heterarchy Forge/mcp-token-pepper-v1')
[[ "$key_metadata" == '600 32 10001' ]]
assert_authenticated
docker restart --time 15 "$container" >/dev/null
wait_for_health
assert_authenticated
printf 'Normal Orchestration CLI, private auth, health and persisted-token restart verified.\n'

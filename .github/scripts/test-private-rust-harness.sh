#!/usr/bin/env bash
set -euo pipefail

project="${1:-}"
if [[ "$project" != "forge" && "$project" != "orchestration" ]]; then
  printf 'usage: %s forge|orchestration\n' "$0" >&2
  exit 2
fi

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
helper="$script_dir/private-rust-harness.sh"
test_root="$(mktemp -d "${TMPDIR:-/tmp}/heterarchy-private-harness.XXXXXX")"
trap 'rm -rf -- "$test_root"' EXIT

fail() {
  printf 'private Harness contract test failed: %s\n' "$1" >&2
  exit 1
}

git_init_and_commit() {
  local path="$1"
  git -C "$path" init -q || fail 'fixture git repository could not be initialized'
  git -C "$path" add -A || fail 'fixture files could not be staged'
  git -C "$path" -c user.name='CI Contract Test' -c user.email='ci-contract@example.invalid' \
    commit -qm 'fixture' || fail 'fixture commit could not be created'
}

new_source_repository() {
  local path="$1"
  mkdir -p "$path" || fail 'dependency fixture directory could not be created'
  printf 'synthetic dependency fixture\n' > "$path/README.md"
  git_init_and_commit "$path"
}

new_fixture() {
  local name="$1"
  local mode="${2:-regular}"
  local workspace="$test_root/$name/workspace"
  local repo="$workspace"
  if [[ "$project" == "orchestration" ]]; then
    repo="$workspace/heterarchy-orchestration"
  fi
  local private_source="$test_root/$name/private-source"
  local source="$private_source/projects/$project/rust_dev_harness"

  mkdir -p "$repo" "$source" || fail 'fixture repository could not be created'
  git -C "$repo" init -q || fail 'fixture worktree could not be initialized'
  git -C "$private_source" init -q || fail 'private checkout fixture repository could not be initialized'
  printf 'synthetic Harness fixture\n' > "$source/README.md"
  case "$mode" in
    regular) ;;
    symlink) ln -s README.md "$source/link" || fail 'symlink fixture could not be created' ;;
    special|ignored) ;;
    *) fail 'unknown fixture mode' ;;
  esac
  if [[ "$mode" == "ignored" ]]; then
    printf 'ignored-fixture.bin\n' > "$source/.gitignore"
  fi
  git_init_and_commit "$private_source"
  git -C "$private_source" rev-parse HEAD > "$test_root/$name/harness-revision"

  if [[ "$project" == "orchestration" ]]; then
    new_source_repository "$test_root/$name/source-repositories/forge"
    new_source_repository "$test_root/$name/source-repositories/broker"
    git -C "$test_root/$name/source-repositories/forge" rev-parse HEAD > "$test_root/$name/forge-revision"
    git -C "$test_root/$name/source-repositories/broker" rev-parse HEAD > "$test_root/$name/broker-revision"
  fi
}

fixture_repo() {
  local workspace="$test_root/$1/workspace"
  if [[ "$project" == "orchestration" ]]; then
    printf '%s\n' "$workspace/heterarchy-orchestration"
  else
    printf '%s\n' "$workspace"
  fi
}

checkout_private_input() {
  local repo
  repo="$(fixture_repo "$1")"
  git clone -q "$test_root/$1/private-source" "$repo/.ci-private-harness-input" || \
    fail 'private checkout fixture could not be materialized'
}

run_helper() {
  local name="$1"
  local command="$2"
  local workspace="$test_root/$name/workspace"
  local repo="$workspace"
  local harness_revision
  harness_revision="${TEST_HARNESS_REVISION-$(cat "$test_root/$name/harness-revision")}"
  local harness_repository="${TEST_HARNESS_REPOSITORY-heterarchy-systems/private-rust-harness}"
  local token="${TEST_HARNESS_TOKEN-synthetic-token-do-not-print}"
  local forge_revision="${TEST_FORGE_REVISION-}"
  local broker_revision="${TEST_BROKER_REVISION-}"
  local checkout_path='.ci-private-harness-input'

  if [[ "$project" == "orchestration" ]]; then
    repo="$workspace/heterarchy-orchestration"
    checkout_path="heterarchy-orchestration/.ci-private-harness-input"
    forge_revision="${TEST_FORGE_REVISION-$(cat "$test_root/$name/forge-revision")}"
    broker_revision="${TEST_BROKER_REVISION-$(cat "$test_root/$name/broker-revision")}"
  fi

  (
    cd "$repo"
    HETERARCHY_HARNESS_PROJECT="$project" \
      HETERARCHY_HARNESS_REPOSITORY="$harness_repository" \
      HETERARCHY_HARNESS_REVISION="$harness_revision" \
      HETERARCHY_HARNESS_CHECKOUT_PATH="$checkout_path" \
      HETERARCHY_HARNESS_READ_TOKEN="$token" \
      HETERARCHY_FORGE_REVISION="$forge_revision" \
      HETERARCHY_BROKER_REVISION="$broker_revision" \
      GITHUB_WORKSPACE="$workspace" \
      bash "$helper" "$command"
  )
}

expect_failure() {
  local name="$1"
  local command="$2"
  local expected="$3"
  local output
  if output="$(run_helper "$name" "$command" 2>&1)"; then
    fail "$name unexpectedly succeeded"
  fi
  [[ "$output" != *'synthetic-token-do-not-print'* ]] || fail "$name exposed the configured token"
  if [[ "$output" != *"$expected"* ]]; then
    output="${output//synthetic-token-do-not-print/[REDACTED]}"
    printf 'unexpected helper failure text: %s\n' "$output" >&2
    fail "$name did not report its explicit contract failure"
  fi
}

assert_absent() {
  [[ ! -e "$1" && ! -L "$1" ]] || fail "CI-owned path remained after cleanup: $2"
}

new_fixture normal regular
normal_repo="$(fixture_repo normal)"
run_helper normal validate >/dev/null
checkout_private_input normal
run_helper normal restore >/dev/null
[[ "$(cat "$normal_repo/.agents/rust_dev_harness/README.md")" == 'synthetic Harness fixture' ]] || fail 'restored Harness content differs from fixture'
if [[ "$project" == "orchestration" ]]; then
  git clone -q "$test_root/normal/source-repositories/forge" "$test_root/normal/workspace/heterarchy-forge"
  git clone -q "$test_root/normal/source-repositories/broker" "$test_root/normal/workspace/heterarchy-broker"
  run_helper normal verify-sibling-checkouts >/dev/null
fi
run_helper normal cleanup >/dev/null
assert_absent "$normal_repo/.ci-private-harness-input" normal-input
assert_absent "$normal_repo/.agents/rust_dev_harness" normal-harness
assert_absent "$normal_repo/.ci-private-harness-state" normal-state
assert_absent "$normal_repo/.agents" normal-agent-parent
if [[ "$project" == "orchestration" ]]; then
  assert_absent "$test_root/normal/workspace/heterarchy-forge" normal-forge
  assert_absent "$test_root/normal/workspace/heterarchy-broker" normal-broker
fi
printf 'ok: normal restore and exact-scope cleanup\n'

new_fixture preserve-agents regular
preserve_repo="$(fixture_repo preserve-agents)"
mkdir -p "$preserve_repo/.agents"
printf 'unrelated local Harness data\n' > "$preserve_repo/.agents/keep.txt"
run_helper preserve-agents validate >/dev/null
checkout_private_input preserve-agents
run_helper preserve-agents restore >/dev/null
run_helper preserve-agents cleanup >/dev/null
[[ "$(cat "$preserve_repo/.agents/keep.txt")" == 'unrelated local Harness data' ]] || fail 'preexisting .agents content was modified'
assert_absent "$preserve_repo/.agents/rust_dev_harness" preserved-harness
printf 'ok: unrelated .agents content is preserved\n'

new_fixture missing-token regular
TEST_HARNESS_TOKEN='' expect_failure missing-token validate 'HETERARCHY_HARNESS_READ_TOKEN is required'
assert_absent "$(fixture_repo missing-token)/.ci-private-harness-state" missing-token-state
printf 'ok: missing token fails before checkout\n'

new_fixture missing-repository regular
TEST_HARNESS_REPOSITORY='' expect_failure missing-repository validate 'HETERARCHY_HARNESS_REPOSITORY is required'
assert_absent "$(fixture_repo missing-repository)/.ci-private-harness-state" missing-repository-state
printf 'ok: missing private repository fails before checkout\n'

new_fixture missing-harness-revision regular
TEST_HARNESS_REVISION='' expect_failure missing-harness-revision validate 'HETERARCHY_HARNESS_REVISION must be a full 40-character commit SHA'
assert_absent "$(fixture_repo missing-harness-revision)/.ci-private-harness-state" missing-revision-state
printf 'ok: missing Harness revision fails before checkout\n'

new_fixture malformed-revision regular
TEST_HARNESS_REVISION='1234-not-an-immutable-commit' expect_failure malformed-revision validate 'HETERARCHY_HARNESS_REVISION must be a full 40-character commit SHA'
assert_absent "$(fixture_repo malformed-revision)/.ci-private-harness-state" malformed-state
printf 'ok: malformed Harness revision fails before checkout\n'

new_fixture ref-mismatch regular
TEST_HARNESS_REVISION='0000000000000000000000000000000000000000' run_helper ref-mismatch validate >/dev/null
checkout_private_input ref-mismatch
TEST_HARNESS_REVISION='0000000000000000000000000000000000000000' expect_failure ref-mismatch restore 'checked out private Harness commit does not match configured revision'
run_helper ref-mismatch cleanup >/dev/null
assert_absent "$(fixture_repo ref-mismatch)/.agents/rust_dev_harness" ref-mismatch-harness
printf 'ok: mismatched Harness ref is rejected and cleaned\n'

new_fixture preexisting-input regular
preexisting_repo="$(fixture_repo preexisting-input)"
checkout_private_input preexisting-input
expect_failure preexisting-input validate 'private checkout destination already exists'
[[ -d "$preexisting_repo/.ci-private-harness-input/.git" ]] || fail 'preexisting checkout input was modified'
assert_absent "$preexisting_repo/.ci-private-harness-state" preexisting-input-state
printf 'ok: preexisting private checkout input is preserved\n'

new_fixture dirty-tracked regular
run_helper dirty-tracked validate >/dev/null
checkout_private_input dirty-tracked
dirty_repo="$(fixture_repo dirty-tracked)"
printf 'tampered tracked fixture\n' > "$dirty_repo/.ci-private-harness-input/projects/$project/rust_dev_harness/README.md"
expect_failure dirty-tracked restore 'contains changes, ignored files, or untracked files'
run_helper dirty-tracked cleanup >/dev/null
printf 'ok: modified tracked Harness input is rejected\n'

new_fixture untracked regular
run_helper untracked validate >/dev/null
checkout_private_input untracked
untracked_repo="$(fixture_repo untracked)"
: > "$untracked_repo/.ci-private-harness-input/projects/$project/rust_dev_harness/untracked.bin"
expect_failure untracked restore 'contains changes, ignored files, or untracked files'
run_helper untracked cleanup >/dev/null
printf 'ok: untracked Harness input is rejected\n'

new_fixture ignored-file ignored
run_helper ignored-file validate >/dev/null
checkout_private_input ignored-file
ignored_repo="$(fixture_repo ignored-file)"
: > "$ignored_repo/.ci-private-harness-input/projects/$project/rust_dev_harness/ignored-fixture.bin"
expect_failure ignored-file restore 'contains changes, ignored files, or untracked files'
run_helper ignored-file cleanup >/dev/null
printf 'ok: ignored Harness input is rejected\n'

new_fixture symlink symlink
run_helper symlink validate >/dev/null
checkout_private_input symlink
expect_failure symlink restore 'symlink'
run_helper symlink cleanup >/dev/null
printf 'ok: symlink input is rejected\n'

new_fixture special-file regular
run_helper special-file validate >/dev/null
checkout_private_input special-file
special_repo="$(fixture_repo special-file)"
mkfifo "$special_repo/.ci-private-harness-input/projects/$project/rust_dev_harness/fifo" || fail 'special-file fixture could not be created'
expect_failure special-file restore 'special file'
run_helper special-file cleanup >/dev/null
printf 'ok: special-file input is rejected\n'

new_fixture oversized regular
run_helper oversized validate >/dev/null
oversized_repo="$(fixture_repo oversized)"
checkout_private_input oversized
truncate -s 134217729 "$oversized_repo/.ci-private-harness-input/projects/$project/rust_dev_harness/oversized-sparse.bin"
expect_failure oversized restore 'logical size limit'
run_helper oversized cleanup >/dev/null
printf 'ok: oversized checkout is rejected\n'

new_fixture too-many-entries regular
run_helper too-many-entries validate >/dev/null
entries_repo="$(fixture_repo too-many-entries)"
checkout_private_input too-many-entries
index=1
while [[ "$index" -le 4097 ]]; do
  : > "$entries_repo/.ci-private-harness-input/projects/$project/rust_dev_harness/entry-$index"
  index=$((index + 1))
done
expect_failure too-many-entries restore 'tree exceeds the configured entry limit'
run_helper too-many-entries cleanup >/dev/null
printf 'ok: excessive Harness entry count is rejected\n'

new_fixture existing-destination regular
destination_repo="$(fixture_repo existing-destination)"
mkdir -p "$destination_repo/.agents/rust_dev_harness"
printf 'preserve\n' > "$destination_repo/.agents/rust_dev_harness/keep.txt"
expect_failure existing-destination validate 'Harness destination already exists'
[[ -f "$destination_repo/.agents/rust_dev_harness/keep.txt" ]] || fail 'existing destination was modified'
printf 'ok: preexisting Harness destination is preserved\n'

if [[ "$project" == "orchestration" ]]; then
  new_fixture missing-forge-revision regular
  TEST_FORGE_REVISION='' expect_failure missing-forge-revision validate 'HETERARCHY_FORGE_REVISION is required'
  assert_absent "$(fixture_repo missing-forge-revision)/.ci-private-harness-state" missing-forge-state
  printf 'ok: missing Forge revision fails before checkout\n'

  new_fixture missing-broker-revision regular
  TEST_BROKER_REVISION='' expect_failure missing-broker-revision validate 'HETERARCHY_BROKER_REVISION must be a full 40-character commit SHA'
  assert_absent "$(fixture_repo missing-broker-revision)/.ci-private-harness-state" missing-broker-state
  printf 'ok: missing Broker revision fails before checkout\n'

  new_fixture malformed-broker-revision regular
  TEST_BROKER_REVISION='main' expect_failure malformed-broker-revision validate 'HETERARCHY_BROKER_REVISION must be a full 40-character commit SHA'
  assert_absent "$(fixture_repo malformed-broker-revision)/.ci-private-harness-state" malformed-broker-state
  printf 'ok: floating Broker revision is rejected before checkout\n'

  new_fixture existing-sibling regular
  mkdir -p "$test_root/existing-sibling/workspace/heterarchy-forge"
  printf 'preserve\n' > "$test_root/existing-sibling/workspace/heterarchy-forge/keep.txt"
  expect_failure existing-sibling validate 'sibling checkout destination already exists'
  [[ -f "$test_root/existing-sibling/workspace/heterarchy-forge/keep.txt" ]] || fail 'preexisting Forge sibling was modified'
  printf 'ok: preexisting sibling checkout is preserved\n'

  new_fixture sibling-ref-mismatch regular
  run_helper sibling-ref-mismatch validate >/dev/null
  checkout_private_input sibling-ref-mismatch
  run_helper sibling-ref-mismatch restore >/dev/null
  git clone -q "$test_root/sibling-ref-mismatch/source-repositories/forge" \
    "$test_root/sibling-ref-mismatch/workspace/heterarchy-forge"
  git clone -q "$test_root/sibling-ref-mismatch/source-repositories/broker" \
    "$test_root/sibling-ref-mismatch/workspace/heterarchy-broker"
  git -C "$test_root/sibling-ref-mismatch/workspace/heterarchy-forge" \
    -c user.name='CI Contract Test' -c user.email='ci-contract@example.invalid' \
    commit --allow-empty -qm 'wrong revision'
  expect_failure sibling-ref-mismatch verify-sibling-checkouts 'Forge checkout commit does not match configured revision'
  run_helper sibling-ref-mismatch cleanup >/dev/null
  assert_absent "$test_root/sibling-ref-mismatch/workspace/heterarchy-forge" sibling-mismatch-forge
  assert_absent "$test_root/sibling-ref-mismatch/workspace/heterarchy-broker" sibling-mismatch-broker
  printf 'ok: mismatched sibling checkout is rejected and cleaned\n'

fi

printf 'private Harness CI contract tests passed for %s\n' "$project"

#!/usr/bin/env bash
set -euo pipefail
umask 077

readonly PRIVATE_CHECKOUT_DIR='.ci-private-harness-input'
readonly PRIVATE_STATE_FILE='.ci-private-harness-state'
readonly HARNESS_DESTINATION='.agents/rust_dev_harness'
readonly STATE_MAGIC='heterarchy-private-harness-state-v1'
readonly MAX_CHECKOUT_KIB=131072
readonly MAX_HARNESS_ENTRIES=4096
readonly MAX_HARNESS_BYTES=134217728

fail() {
  printf 'private Harness provisioning: %s\n' "$1" >&2
  exit 1
}

repo_root() {
  local root
  root="$(git rev-parse --show-toplevel 2>/dev/null)" || fail 'the repository root could not be resolved'
  (cd "$root" && pwd -P)
}

read_configuration() {
  CONFIG_PROJECT="${HETERARCHY_HARNESS_PROJECT:-}"
  CONFIG_REPOSITORY="${HETERARCHY_HARNESS_REPOSITORY:-}"
  CONFIG_REVISION="${HETERARCHY_HARNESS_REVISION:-}"
  CONFIG_CHECKOUT_PATH="${HETERARCHY_HARNESS_CHECKOUT_PATH:-}"
  CONFIG_FORGE_REVISION="${HETERARCHY_FORGE_REVISION:-}"
  CONFIG_BROKER_REVISION="${HETERARCHY_BROKER_REVISION:-}"

  case "$CONFIG_PROJECT" in
    forge|orchestration) ;;
    *) fail 'project must be forge or orchestration' ;;
  esac
  if [[ -z "$CONFIG_REPOSITORY" ]]; then
    fail 'HETERARCHY_HARNESS_REPOSITORY is required for full CI'
  fi
  if [[ ! "$CONFIG_REPOSITORY" =~ ^[[:alnum:]_.-]+/[[:alnum:]_.-]+$ ]]; then
    fail 'HETERARCHY_HARNESS_REPOSITORY must be owner/repository'
  fi
  if [[ -z "${HETERARCHY_HARNESS_READ_TOKEN:-}" ]]; then
    fail 'HETERARCHY_HARNESS_READ_TOKEN is required for full private-Harness CI'
  fi
  if [[ ! "$CONFIG_REVISION" =~ ^[[:xdigit:]]{40}$ ]]; then
    fail 'HETERARCHY_HARNESS_REVISION must be a full 40-character commit SHA'
  fi
  CONFIG_REVISION="$(printf '%s' "$CONFIG_REVISION" | tr '[:upper:]' '[:lower:]')"

  if [[ "$CONFIG_PROJECT" == 'forge' ]]; then
    if [[ "$CONFIG_CHECKOUT_PATH" != "$PRIVATE_CHECKOUT_DIR" ]]; then
      fail 'Forge private checkout path is invalid'
    fi
    CONFIG_FORGE_REVISION='-'
    CONFIG_BROKER_REVISION='-'
  else
    if [[ "$CONFIG_CHECKOUT_PATH" != "heterarchy-orchestration/$PRIVATE_CHECKOUT_DIR" ]]; then
      fail 'Orchestration private checkout path is invalid'
    fi
    if [[ ! "$CONFIG_FORGE_REVISION" =~ ^[[:xdigit:]]{40}$ ]]; then
      fail 'HETERARCHY_FORGE_REVISION is required as a full 40-character commit SHA'
    fi
    if [[ ! "$CONFIG_BROKER_REVISION" =~ ^[[:xdigit:]]{40}$ ]]; then
      fail 'HETERARCHY_BROKER_REVISION must be a full 40-character commit SHA'
    fi
    CONFIG_FORGE_REVISION="$(printf '%s' "$CONFIG_FORGE_REVISION" | tr '[:upper:]' '[:lower:]')"
    CONFIG_BROKER_REVISION="$(printf '%s' "$CONFIG_BROKER_REVISION" | tr '[:upper:]' '[:lower:]')"
  fi
}

workspace_root() {
  local root="$1"
  local workspace="${GITHUB_WORKSPACE:-$root}"
  [[ -d "$workspace" && ! -L "$workspace" ]] || fail 'GITHUB_WORKSPACE is missing or not a directory'
  (cd "$workspace" && pwd -P)
}

validate_repository_location() {
  local project="$1"
  local root="$2"
  local workspace="$3"
  case "$project" in
    forge)
      [[ "$root" == "$workspace" ]] || fail 'Forge must be checked out at GITHUB_WORKSPACE'
      ;;
    orchestration)
      [[ "$root" == "$workspace/heterarchy-orchestration" ]] || \
        fail 'Orchestration must be checked out at GITHUB_WORKSPACE/heterarchy-orchestration'
      ;;
    *) fail 'project must be forge or orchestration' ;;
  esac
}

path_exists() {
  [[ -e "$1" || -L "$1" ]]
}

validate_staging_paths() {
  local project="$1"
  local root="$2"
  local workspace="$3"
  local agents="$root/.agents"
  local destination="$root/$HARNESS_DESTINATION"

  if path_exists "$root/$PRIVATE_CHECKOUT_DIR"; then
    fail 'private checkout destination already exists'
  fi
  if path_exists "$root/$PRIVATE_STATE_FILE"; then
    fail 'private Harness state destination already exists'
  fi
  if [[ -L "$agents" ]]; then
    fail '.agents must be a real directory, not a symlink'
  fi
  if path_exists "$agents" && [[ ! -d "$agents" ]]; then
    fail '.agents exists but is not a directory'
  fi
  if path_exists "$destination"; then
    fail 'Harness destination already exists'
  fi

  if [[ "$project" == 'orchestration' ]]; then
    if path_exists "$workspace/heterarchy-forge" || path_exists "$workspace/heterarchy-broker"; then
      fail 'sibling checkout destination already exists'
    fi
  fi
}

validate() {
  read_configuration
  local root
  root="$(repo_root)"
  local workspace
  workspace="$(workspace_root "$root")"
  validate_repository_location "$CONFIG_PROJECT" "$root" "$workspace"
  validate_staging_paths "$CONFIG_PROJECT" "$root" "$workspace"

  local agents_created=0
  if [[ ! -e "$root/.agents" ]]; then
    agents_created=1
  fi
  local state="$root/$PRIVATE_STATE_FILE"
  if ! (
    set -o noclobber
    printf '%s\n%s\n%s\n%s\n%s\n%s\n%s\n' \
      "$STATE_MAGIC" \
      "$CONFIG_PROJECT" \
      "$CONFIG_REPOSITORY" \
      "$CONFIG_REVISION" \
      "$agents_created" \
      "$CONFIG_FORGE_REVISION" \
      "$CONFIG_BROKER_REVISION" > "$state"
  ) 2>/dev/null; then
    fail 'private Harness state could not be claimed exclusively'
  fi
  printf 'private Harness inputs and destinations validated\n'
}

read_state() {
  local root="$1"
  local state="$root/$PRIVATE_STATE_FILE"
  [[ -f "$state" && ! -L "$state" ]] || fail 'CI-owned private Harness state is missing or invalid'
  local extra
  {
    IFS= read -r STATE_RECORD_MAGIC || fail 'private Harness state is incomplete'
    IFS= read -r STATE_PROJECT || fail 'private Harness state is incomplete'
    IFS= read -r STATE_REPOSITORY || fail 'private Harness state is incomplete'
    IFS= read -r STATE_REVISION || fail 'private Harness state is incomplete'
    IFS= read -r STATE_AGENTS_CREATED || fail 'private Harness state is incomplete'
    IFS= read -r STATE_FORGE_REVISION || fail 'private Harness state is incomplete'
    IFS= read -r STATE_BROKER_REVISION || fail 'private Harness state is incomplete'
    if IFS= read -r extra; then
      fail 'private Harness state contains unexpected fields'
    fi
  } < "$state"

  [[ "$STATE_RECORD_MAGIC" == "$STATE_MAGIC" ]] || fail 'private Harness state has an unknown format'
  [[ "$STATE_PROJECT" == 'forge' || "$STATE_PROJECT" == 'orchestration' ]] || fail 'private Harness state has an invalid project'
  [[ "$STATE_REPOSITORY" =~ ^[[:alnum:]_.-]+/[[:alnum:]_.-]+$ ]] || fail 'private Harness state has an invalid repository'
  [[ "$STATE_REVISION" =~ ^[[:xdigit:]]{40}$ ]] || fail 'private Harness state has an invalid revision'
  [[ "$STATE_AGENTS_CREATED" == '0' || "$STATE_AGENTS_CREATED" == '1' ]] || fail 'private Harness state has an invalid ownership marker'
  if [[ "$STATE_PROJECT" == 'orchestration' ]]; then
    [[ "$STATE_FORGE_REVISION" =~ ^[[:xdigit:]]{40}$ ]] || fail 'private Harness state has an invalid Forge revision'
    [[ "$STATE_BROKER_REVISION" =~ ^[[:xdigit:]]{40}$ ]] || fail 'private Harness state has an invalid Broker revision'
  else
    [[ "$STATE_FORGE_REVISION" == '-' && "$STATE_BROKER_REVISION" == '-' ]] || fail 'private Harness state has unexpected sibling revisions'
  fi
}

assert_state_matches_configuration() {
  [[ "$STATE_PROJECT" == "$CONFIG_PROJECT" \
    && "$STATE_REPOSITORY" == "$CONFIG_REPOSITORY" \
    && "$STATE_REVISION" == "$CONFIG_REVISION" \
    && "$STATE_FORGE_REVISION" == "$CONFIG_FORGE_REVISION" \
    && "$STATE_BROKER_REVISION" == "$CONFIG_BROKER_REVISION" ]] || \
    fail 'private Harness configuration changed after staging was claimed'
}

validate_harness_tree() {
  local checkout="$1"
  local source="$2"
  local revision="$3"
  local project="$4"
  local unsafe_entry
  if ! unsafe_entry="$(find -P "$source" \( -type l -o \( ! -type f ! -type d \) \) -print -quit)"; then
    fail 'private Harness tree could not be inspected'
  fi
  if [[ -n "$unsafe_entry" ]]; then
    fail 'private Harness contains a symlink or special file'
  fi

  local raw_entries
  if ! raw_entries="$(find -P "$source" -print | wc -l | tr -d '[:space:]')"; then
    fail 'private Harness entry count could not be measured'
  fi
  [[ "$raw_entries" =~ ^[0-9]+$ ]] || fail 'private Harness entry count is invalid'
  local entries=$((raw_entries - 1))
  if (( entries > MAX_HARNESS_ENTRIES )); then
    fail 'private Harness tree exceeds the configured entry limit'
  fi

  local metadata_file
  metadata_file="$(mktemp "${TMPDIR:-/tmp}/private-rust-harness-tree.XXXXXX")" || \
    fail 'private Harness validation scratch file could not be created'
  trap 'rm -f "$metadata_file"' EXIT
  if ! git -C "$checkout" ls-tree -r -z "$revision" -- "projects/$project/rust_dev_harness" > "$metadata_file"; then
    fail 'private Harness Git tree could not be read'
  fi
  [[ -s "$metadata_file" ]] || fail 'private Harness Git tree is empty'

  local blob_count=0
  local record
  local mode
  local remainder
  local kind
  while IFS= read -r -d '' record; do
    mode="${record%% *}"
    remainder="${record#* }"
    kind="${remainder%% *}"
    if [[ "$kind" != 'blob' || ( "$mode" != '100644' && "$mode" != '100755' ) ]]; then
      fail 'private Harness Git tree contains a symlink, submodule, or non-regular entry'
    fi
    blob_count=$((blob_count + 1))
  done < "$metadata_file"

  if ! find -P "$source" -type f -print0 > "$metadata_file"; then
    fail 'private Harness file list could not be read'
  fi
  local logical_bytes=0
  local file_path
  local file_size
  while IFS= read -r -d '' file_path; do
    case "$(uname -s)" in
      Darwin) file_size="$(stat -f '%z' "$file_path" 2>/dev/null)" || fail 'private Harness file size could not be read' ;;
      Linux) file_size="$(stat -c '%s' "$file_path" 2>/dev/null)" || fail 'private Harness file size could not be read' ;;
      *) fail 'private Harness size checks require macOS or Linux' ;;
    esac
    [[ "$file_size" =~ ^[0-9]+$ ]] || fail 'private Harness file size is invalid'
    if (( file_size > MAX_HARNESS_BYTES - logical_bytes )); then
      fail 'private Harness tree exceeds the configured logical size limit'
    fi
    logical_bytes=$((logical_bytes + file_size))
  done < "$metadata_file"

  local dirty_entries
  if ! dirty_entries="$(GIT_OPTIONAL_LOCKS=0 git -C "$checkout" status --porcelain=v1 --untracked-files=all --ignored=matching -- "projects/$project/rust_dev_harness")"; then
    fail 'private Harness Git status could not be read'
  fi
  if [[ -n "$dirty_entries" ]]; then
    fail 'private Harness subtree contains changes, ignored files, or untracked files'
  fi

  local file_count="$blob_count"
  local physical_file_count
  if ! physical_file_count="$(find -P "$source" -type f -print | wc -l | tr -d '[:space:]')"; then
    fail 'private Harness file count could not be measured'
  fi
  [[ "$physical_file_count" =~ ^[0-9]+$ ]] || fail 'private Harness file count is invalid'
  [[ "$physical_file_count" == "$file_count" ]] || fail 'private Harness checkout does not match its Git tree'

  rm -f "$metadata_file"
  trap - EXIT
}

restore() {
  read_configuration
  local root
  root="$(repo_root)"
  local workspace
  workspace="$(workspace_root "$root")"
  read_state "$root"
  assert_state_matches_configuration
  validate_repository_location "$CONFIG_PROJECT" "$root" "$workspace"

  local checkout="$root/$PRIVATE_CHECKOUT_DIR"
  [[ -d "$checkout" && ! -L "$checkout" ]] || fail 'private Harness checkout is missing or invalid'
  local actual_revision
  actual_revision="$(git -C "$checkout" rev-parse --verify 'HEAD^{commit}' 2>/dev/null)" || \
    fail 'private Harness checkout has no valid commit'
  actual_revision="$(printf '%s' "$actual_revision" | tr '[:upper:]' '[:lower:]')"
  [[ "$actual_revision" == "$CONFIG_REVISION" ]] || fail 'checked out private Harness commit does not match configured revision'

  local checkout_kib
  if ! checkout_kib="$(du -sk "$checkout" | cut -f1)"; then
    fail 'private Harness checkout size could not be measured'
  fi
  [[ "$checkout_kib" =~ ^[0-9]+$ ]] || fail 'private Harness checkout size is invalid'
  if (( checkout_kib > MAX_CHECKOUT_KIB )); then
    fail 'private Harness checkout exceeds the configured size limit'
  fi

  local source="$checkout/projects/$CONFIG_PROJECT/rust_dev_harness"
  [[ -d "$source" && ! -L "$source" ]] || fail 'selected private Harness subtree is missing or invalid'
  validate_harness_tree "$checkout" "$source" "$actual_revision" "$CONFIG_PROJECT"

  local agents="$root/.agents"
  if [[ -L "$agents" ]]; then
    fail '.agents must be a real directory, not a symlink'
  fi
  if [[ -e "$agents" && ! -d "$agents" ]]; then
    fail '.agents exists but is not a directory'
  fi
  local destination="$root/$HARNESS_DESTINATION"
  if path_exists "$destination"; then
    fail 'Harness destination already exists'
  fi
  mkdir -p "$agents" || fail 'Harness parent directory could not be created'
  mkdir -m 700 "$destination" || fail 'Harness destination could not be created exclusively'
  cp -Rp "$source/." "$destination/" || fail 'private Harness subtree could not be restored'
  printf 'pinned private Harness subtree restored\n'
}

verify_sibling_checkouts() {
  read_configuration
  [[ "$CONFIG_PROJECT" == 'orchestration' ]] || fail 'sibling source verification applies only to Orchestration'
  local root
  root="$(repo_root)"
  local workspace
  workspace="$(workspace_root "$root")"
  read_state "$root"
  assert_state_matches_configuration
  validate_repository_location "$CONFIG_PROJECT" "$root" "$workspace"

  local actual
  local forge="$workspace/heterarchy-forge"
  [[ -d "$forge" && ! -L "$forge" ]] || fail 'Forge sibling checkout is missing or invalid'
  actual="$(git -C "$forge" rev-parse --verify 'HEAD^{commit}' 2>/dev/null)" || \
    fail 'Forge sibling checkout has no valid commit'
  actual="$(printf '%s' "$actual" | tr '[:upper:]' '[:lower:]')"
  [[ "$actual" == "$CONFIG_FORGE_REVISION" ]] || fail 'Forge checkout commit does not match configured revision'
  local forge_status
  if ! forge_status="$(GIT_OPTIONAL_LOCKS=0 git -C "$forge" status --porcelain=v1 --untracked-files=all --ignored=matching)"; then
    fail 'Forge sibling checkout status could not be read'
  fi
  [[ -z "$forge_status" ]] || fail 'Forge sibling checkout contains changes, ignored files, or untracked files'

  local broker="$workspace/heterarchy-broker"
  [[ -d "$broker" && ! -L "$broker" ]] || fail 'Broker sibling checkout is missing or invalid'
  actual="$(git -C "$broker" rev-parse --verify 'HEAD^{commit}' 2>/dev/null)" || \
    fail 'Broker sibling checkout has no valid commit'
  actual="$(printf '%s' "$actual" | tr '[:upper:]' '[:lower:]')"
  [[ "$actual" == "$CONFIG_BROKER_REVISION" ]] || fail 'Broker checkout commit does not match configured revision'
  local broker_status
  if ! broker_status="$(GIT_OPTIONAL_LOCKS=0 git -C "$broker" status --porcelain=v1 --untracked-files=all --ignored=matching)"; then
    fail 'Broker sibling checkout status could not be read'
  fi
  [[ -z "$broker_status" ]] || fail 'Broker sibling checkout contains changes, ignored files, or untracked files'
  printf 'Forge and Broker sibling revisions verified\n'
}

cleanup() {
  local root
  if ! root="$(git rev-parse --show-toplevel 2>/dev/null)"; then
    printf 'no repository checkout exists for CI-owned cleanup\n'
    return 0
  fi
  root="$(cd "$root" && pwd -P)"
  local state="$root/$PRIVATE_STATE_FILE"
  if ! path_exists "$state"; then
    printf 'no CI-owned private Harness staging remains\n'
    return 0
  fi
  [[ -f "$state" && ! -L "$state" ]] || fail 'private Harness state is not a regular CI-owned file'
  read_state "$root"
  if [[ -n "${HETERARCHY_HARNESS_PROJECT:-}" && "$HETERARCHY_HARNESS_PROJECT" != "$STATE_PROJECT" ]]; then
    fail 'private Harness cleanup project does not match its ownership state'
  fi
  local workspace
  workspace="$(workspace_root "$root")"
  validate_repository_location "$STATE_PROJECT" "$root" "$workspace"

  local cleanup_failed=0
  local input="$root/$PRIVATE_CHECKOUT_DIR"
  if path_exists "$input"; then
    rm -rf "$input" 2>/dev/null || cleanup_failed=1
  fi

  if [[ "$STATE_PROJECT" == 'orchestration' ]]; then
    rm -rf "$workspace/heterarchy-forge" 2>/dev/null || cleanup_failed=1
    rm -rf "$workspace/heterarchy-broker" 2>/dev/null || cleanup_failed=1
  fi

  local agents="$root/.agents"
  if [[ -L "$agents" ]] || { path_exists "$agents" && [[ ! -d "$agents" ]]; }; then
    cleanup_failed=1
  else
    local destination="$root/$HARNESS_DESTINATION"
    if path_exists "$destination"; then
      rm -rf "$destination" 2>/dev/null || cleanup_failed=1
    fi
  fi

  if [[ "$STATE_AGENTS_CREATED" == '1' && -d "$agents" && ! -L "$agents" ]]; then
    rmdir "$agents" 2>/dev/null || cleanup_failed=1
  fi
  if (( cleanup_failed != 0 )); then
    fail 'one or more CI-owned inputs remain; ownership state retained for cleanup retry'
  fi
  rm -f "$state" || fail 'private Harness cleanup state could not be removed'
  printf 'CI-owned private Harness inputs cleaned\n'
}

case "${1:-}" in
  validate) validate ;;
  restore) restore ;;
  verify-sibling-checkouts) verify_sibling_checkouts ;;
  cleanup) cleanup ;;
  *) fail 'command must be validate, restore, verify-sibling-checkouts, or cleanup' ;;
esac

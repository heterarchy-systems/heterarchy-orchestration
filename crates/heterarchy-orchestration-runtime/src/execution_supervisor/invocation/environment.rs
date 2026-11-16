//! Host environment scrubbing, `PATH` sanitization, and scrubbed child
//! environment assembly for isolated `CLI` worker invocations.

use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};

use heterarchy_orchestration_operations::AssignmentExecutionSnapshot;

use super::path_guard::{is_allowed_tmpdir, is_inside_worktree, is_path_entry_forbidden};
use super::{ALLOWLISTED_HOST_ENV_KEYS, CliExecutionContext, DEFAULT_SAFE_SEARCH_PATH};

pub(super) fn sanitize_ambient_host_env<I>(
    ambient_env: I,
    worktree: &Path,
    canonical_worktree: &Path,
) -> Vec<(OsString, OsString)>
where
    I: IntoIterator<Item = (OsString, OsString)>,
{
    let mut saw_path = false;
    let mut sanitized = Vec::new();
    for (key, value) in ambient_env {
        if key == OsStr::new("PATH") {
            saw_path = true;
            let clean_path = scrub_ambient_search_path(&value, worktree, canonical_worktree)
                .unwrap_or_else(|| OsString::from(DEFAULT_SAFE_SEARCH_PATH));
            sanitized.push((key, clean_path));
            continue;
        }
        sanitized.push((key, value));
    }
    if !saw_path {
        sanitized.push((
            OsString::from("PATH"),
            OsString::from(DEFAULT_SAFE_SEARCH_PATH),
        ));
    }
    sanitized
}

pub(super) fn resolve_effective_search_path(collected_env: &[(OsString, OsString)]) -> OsString {
    collected_env
        .iter()
        .rev()
        .find(|(key, _)| key == OsStr::new("PATH"))
        .map_or_else(
            || OsString::from(DEFAULT_SAFE_SEARCH_PATH),
            |(_, val)| val.clone(),
        )
}

fn scrub_ambient_search_path(
    raw_path: &OsStr,
    worktree: &Path,
    canonical_worktree: &Path,
) -> Option<OsString> {
    let allowed_dirs: Vec<PathBuf> = std::env::split_paths(raw_path)
        .filter(|dir| {
            !dir.as_os_str().is_empty()
                && dir.is_absolute()
                && !is_path_entry_forbidden(dir)
                && !is_inside_worktree(dir, worktree, canonical_worktree)
        })
        .collect();
    if allowed_dirs.is_empty() {
        return None;
    }
    std::env::join_paths(allowed_dirs).ok()
}

pub(super) fn build_scrubbed_environment<I>(
    snapshot: &AssignmentExecutionSnapshot,
    context: &CliExecutionContext,
    host_env: I,
    validated_search_path: OsString,
) -> BTreeMap<String, OsString>
where
    I: IntoIterator<Item = (OsString, OsString)>,
{
    let mut env = BTreeMap::new();
    for (raw_key, value) in host_env {
        let Some(key) = raw_key.to_str() else {
            continue;
        };
        if !ALLOWLISTED_HOST_ENV_KEYS.contains(&key)
            || value.is_empty()
            || value.as_encoded_bytes().contains(&0)
            || key == "PATH"
        {
            continue;
        }
        if key == "TMPDIR" && !is_allowed_tmpdir(Path::new(&value)) {
            continue;
        }
        env.insert(key.to_owned(), value);
    }
    env.insert("PATH".to_owned(), validated_search_path);

    let attempt_key = snapshot.attempt_key();
    env.insert(
        "HETERARCHY_WORKSPACE_ID".to_owned(),
        OsString::from(attempt_key.workspace_id().to_string()),
    );
    env.insert(
        "HETERARCHY_SNAPSHOT_ID".to_owned(),
        OsString::from(snapshot.snapshot_id().to_string()),
    );
    env.insert(
        "HETERARCHY_GOAL_ID".to_owned(),
        OsString::from(attempt_key.goal_id().to_string()),
    );
    env.insert(
        "HETERARCHY_NODE_ID".to_owned(),
        OsString::from(attempt_key.node_id().to_string()),
    );
    env.insert(
        "HETERARCHY_ASSIGNMENT_ID".to_owned(),
        OsString::from(attempt_key.assignment_id().to_string()),
    );
    env.insert(
        "HETERARCHY_ATTEMPT".to_owned(),
        OsString::from(attempt_key.attempt().to_string()),
    );
    if let Some(swarm_run_id) = context.swarm_run_id() {
        env.insert(
            "HETERARCHY_SWARM_RUN_ID".to_owned(),
            OsString::from(swarm_run_id.to_string()),
        );
    }
    if let Some(swarm_member_id) = context.swarm_member_id() {
        env.insert(
            "HETERARCHY_SWARM_MEMBER_ID".to_owned(),
            OsString::from(swarm_member_id.to_string()),
        );
    }
    if let Some(backend_addr) = context.orchestration_backend_addr() {
        env.insert(
            "HETERARCHY_ORCHESTRATION_BACKEND_ADDR".to_owned(),
            OsString::from(backend_addr.to_string()),
        );
    }
    if let Some(bearer_file) = context.bearer_file() {
        // Only the validated path string is forwarded; token bytes are never read here.
        env.insert(
            "HETERARCHY_ORCHESTRATION_BEARER_FILE".to_owned(),
            bearer_file.as_os_str().to_os_string(),
        );
    }
    env
}

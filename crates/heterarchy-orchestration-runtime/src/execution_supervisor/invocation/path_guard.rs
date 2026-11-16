//! Defense-in-depth filesystem, symlink-chain, `PATH`, executable, and
//! bearer-file path validation for local `CLI` worker execution.

use std::ffi::OsStr;
use std::fs;
use std::path::{Component, Path, PathBuf};

use super::ExecutionSupervisorError;

const FORBIDDEN_SSD_COMPONENTS: [&str; 3] = ["Volumes", "256.SSD", "RustBuild"];
const MAX_SYMLINK_HOPS: usize = 40;
pub(super) const MAX_BEARER_FILE_BYTES: u64 = 4_096;

/// Defense-in-depth validation of an execution worktree directory on disk.
///
/// # Errors
/// Returns [`ExecutionSupervisorError::InvalidWorktreePath`] for non-absolute
/// paths, [`ExecutionSupervisorError::ForbiddenSsdBuildWorktree`] when the
/// lexical or canonicalized path resides under `/Volumes/256.SSD/RustBuild`
/// (matched ASCII-case-insensitively across `APFS` variants and symlinks),
/// and [`ExecutionSupervisorError::MissingWorktreeDirectory`] when the path is
/// absent or not a directory.
pub fn validate_worktree_dir(path: &Path) -> Result<(), ExecutionSupervisorError> {
    validate_worktree_dir_with_forbidden(path, &FORBIDDEN_SSD_COMPONENTS)
}

pub(in crate::execution_supervisor) fn validate_worktree_dir_with_forbidden(
    path: &Path,
    forbidden_segments: &[&str],
) -> Result<(), ExecutionSupervisorError> {
    if !path.is_absolute() {
        return Err(ExecutionSupervisorError::InvalidWorktreePath(
            path.to_path_buf(),
        ));
    }
    if path_or_ancestor_reaches_forbidden_root(path, forbidden_segments) {
        return Err(ExecutionSupervisorError::ForbiddenSsdBuildWorktree(
            path.to_path_buf(),
        ));
    }
    let Ok(metadata) = fs::metadata(path) else {
        return Err(ExecutionSupervisorError::MissingWorktreeDirectory(
            path.to_path_buf(),
        ));
    };
    if !metadata.is_dir() {
        return Err(ExecutionSupervisorError::MissingWorktreeDirectory(
            path.to_path_buf(),
        ));
    }
    let canonical = fs::canonicalize(path)?;
    if has_forbidden_root_components(&canonical, forbidden_segments) {
        return Err(ExecutionSupervisorError::ForbiddenSsdBuildWorktree(
            path.to_path_buf(),
        ));
    }
    Ok(())
}

/// Strictly validate a bearer-token file path without reading any token bytes.
///
/// Rejects empty, NUL-containing, relative, `/Volumes/256.SSD/RustBuild`-scoped,
/// missing, symlinked, non-regular, empty (`0`-byte), and oversized (`> 4096`-byte)
/// files using `std::fs::symlink_metadata` and canonicalization only.
pub(super) fn validate_bearer_file(path: &Path) -> Result<(), ExecutionSupervisorError> {
    if path.as_os_str().is_empty()
        || path.as_os_str().as_encoded_bytes().contains(&0)
        || !path.is_absolute()
    {
        return Err(ExecutionSupervisorError::InvalidExecutionContext(
            "bearer_file must be a non-empty NUL-free absolute path",
        ));
    }
    if path_or_ancestor_reaches_forbidden_root(path, &FORBIDDEN_SSD_COMPONENTS) {
        return Err(ExecutionSupervisorError::InvalidExecutionContext(
            "bearer_file cannot reside under /Volumes/256.SSD/RustBuild",
        ));
    }
    let metadata = fs::symlink_metadata(path).map_err(|_| {
        ExecutionSupervisorError::InvalidExecutionContext(
            "bearer_file does not exist or cannot be inspected",
        )
    })?;
    if metadata.file_type().is_symlink() {
        return Err(ExecutionSupervisorError::InvalidExecutionContext(
            "bearer_file cannot be a symlink",
        ));
    }
    if !metadata.is_file() {
        return Err(ExecutionSupervisorError::InvalidExecutionContext(
            "bearer_file must be a regular file",
        ));
    }
    if !(1..=MAX_BEARER_FILE_BYTES).contains(&metadata.len()) {
        return Err(ExecutionSupervisorError::InvalidExecutionContext(
            "bearer_file must contain 1..=4096 bytes",
        ));
    }
    let canonical = fs::canonicalize(path).map_err(|_| {
        ExecutionSupervisorError::InvalidExecutionContext("bearer_file could not be canonicalized")
    })?;
    if is_forbidden_ssd_path(&canonical) {
        return Err(ExecutionSupervisorError::InvalidExecutionContext(
            "bearer_file cannot reside under /Volumes/256.SSD/RustBuild",
        ));
    }
    Ok(())
}

pub(super) fn validate_search_path_entries(
    search_path: &OsStr,
    worktree: &Path,
    canonical_worktree: &Path,
) -> Result<(), ExecutionSupervisorError> {
    if search_path.is_empty() {
        return Err(ExecutionSupervisorError::InvalidExecutablePath(
            PathBuf::new(),
        ));
    }
    for dir in std::env::split_paths(search_path) {
        if is_path_entry_forbidden(&dir) {
            return Err(ExecutionSupervisorError::ForbiddenSsdBuildExecutable(dir));
        }
    }
    for dir in std::env::split_paths(search_path) {
        if dir.as_os_str().is_empty()
            || !dir.is_absolute()
            || is_inside_worktree(&dir, worktree, canonical_worktree)
        {
            return Err(ExecutionSupervisorError::InvalidExecutablePath(dir));
        }
    }
    Ok(())
}

pub(super) fn is_path_entry_forbidden(dir: &Path) -> bool {
    if dir.as_os_str().is_empty() {
        return false;
    }
    if path_or_ancestor_reaches_forbidden_root(dir, &FORBIDDEN_SSD_COMPONENTS) {
        return true;
    }
    if let Ok(canonical_dir) = fs::canonicalize(dir)
        && is_forbidden_ssd_path(&canonical_dir)
    {
        return true;
    }
    if let Some(canonical_ancestor) = dir
        .ancestors()
        .find_map(|ancestor| fs::canonicalize(ancestor).ok())
        && is_forbidden_ssd_path(&canonical_ancestor)
    {
        return true;
    }
    false
}

pub(super) fn resolve_executable_target(
    program: &Path,
    worktree: &Path,
    canonical_worktree: &Path,
    effective_path: &OsStr,
    require_existing: bool,
) -> Result<PathBuf, ExecutionSupervisorError> {
    if program.as_os_str().is_empty()
        || program.as_os_str().as_encoded_bytes().ends_with(b"/")
        || program == Path::new(".")
        || program == Path::new("..")
    {
        return Err(ExecutionSupervisorError::InvalidExecutablePath(
            program.to_path_buf(),
        ));
    }
    if is_forbidden_or_lexically_ssd(program) {
        return Err(ExecutionSupervisorError::ForbiddenSsdBuildExecutable(
            program.to_path_buf(),
        ));
    }
    if program.is_absolute() {
        return resolve_absolute_executable(
            program,
            worktree,
            canonical_worktree,
            require_existing,
        );
    }
    if is_bare_command_name(program) {
        return resolve_bare_executable(
            program,
            worktree,
            canonical_worktree,
            effective_path,
            require_existing,
        );
    }
    // Check `/Volumes/256.SSD/RustBuild` first so relative symlinks targeting
    // the forbidden build root still surface `ForbiddenSsdBuildExecutable`,
    // then reject all relative/worktree-local executable paths.
    let resolved = worktree.join(program);
    validate_concrete_executable_target(&resolved)?;
    if canonical_worktree != worktree {
        validate_concrete_executable_target(&canonical_worktree.join(program))?;
    }
    Err(ExecutionSupervisorError::InvalidExecutablePath(
        program.to_path_buf(),
    ))
}

fn resolve_absolute_executable(
    program: &Path,
    worktree: &Path,
    canonical_worktree: &Path,
    require_existing: bool,
) -> Result<PathBuf, ExecutionSupervisorError> {
    validate_concrete_executable_target(program)?;
    if is_inside_worktree(program, worktree, canonical_worktree) {
        return Err(ExecutionSupervisorError::InvalidExecutablePath(
            program.to_path_buf(),
        ));
    }
    match fs::symlink_metadata(program) {
        Ok(_) => finalize_existing_executable(program, worktree, canonical_worktree),
        Err(err) if require_existing => Err(ExecutionSupervisorError::Io(err)),
        Err(_) => Ok(program.to_path_buf()),
    }
}

fn resolve_bare_executable(
    program: &Path,
    worktree: &Path,
    canonical_worktree: &Path,
    effective_path: &OsStr,
    require_existing: bool,
) -> Result<PathBuf, ExecutionSupervisorError> {
    let mut first_resolved: Option<PathBuf> = None;
    for dir in std::env::split_paths(effective_path) {
        let candidate = dir.join(program);
        validate_concrete_executable_target(&candidate)?;
        if fs::symlink_metadata(&candidate).is_ok() {
            if is_inside_worktree(&candidate, worktree, canonical_worktree) {
                return Err(ExecutionSupervisorError::InvalidExecutablePath(candidate));
            }
            let canonical = finalize_existing_executable(&candidate, worktree, canonical_worktree)?;
            if first_resolved.is_none() {
                first_resolved = Some(canonical);
            }
        }
    }
    if let Some(canonical) = first_resolved {
        return Ok(canonical);
    }
    if require_existing {
        return Err(ExecutionSupervisorError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("CLI executable {} not found in PATH", program.display()),
        )));
    }
    Ok(program.to_path_buf())
}

fn finalize_existing_executable(
    target: &Path,
    worktree: &Path,
    canonical_worktree: &Path,
) -> Result<PathBuf, ExecutionSupervisorError> {
    use std::os::unix::fs::PermissionsExt as _;

    let canonical = fs::canonicalize(target)
        .map_err(|_| ExecutionSupervisorError::ForbiddenSsdBuildExecutable(target.to_path_buf()))?;
    if is_forbidden_ssd_path(&canonical) {
        return Err(ExecutionSupervisorError::ForbiddenSsdBuildExecutable(
            canonical,
        ));
    }
    if is_inside_worktree(&canonical, worktree, canonical_worktree)
        || is_untrusted_mutable_executable_path(&canonical)
    {
        return Err(ExecutionSupervisorError::InvalidExecutablePath(canonical));
    }
    let metadata = fs::symlink_metadata(&canonical)?;
    if !metadata.is_file() || (metadata.permissions().mode() & 0o022) != 0 {
        return Err(ExecutionSupervisorError::InvalidExecutablePath(canonical));
    }
    for ancestor in canonical.ancestors().skip(1) {
        let ancestor_meta = fs::symlink_metadata(ancestor)?;
        if !ancestor_meta.is_dir() || (ancestor_meta.permissions().mode() & 0o022) != 0 {
            return Err(ExecutionSupervisorError::InvalidExecutablePath(canonical));
        }
    }
    Ok(canonical)
}

fn is_untrusted_mutable_executable_path(canonical: &Path) -> bool {
    const UNTRUSTED_TEMP_PREFIXES: [&str; 6] = [
        "/tmp",
        "/private/tmp",
        "/var/tmp",
        "/private/var/tmp",
        "/var/folders",
        "/private/var/folders",
    ];
    if UNTRUSTED_TEMP_PREFIXES
        .iter()
        .any(|prefix| canonical.starts_with(Path::new(prefix)))
    {
        return true;
    }
    let temp_dir = std::env::temp_dir();
    if canonical.starts_with(&temp_dir) {
        return true;
    }
    if let Ok(canonical_temp) = fs::canonicalize(&temp_dir)
        && canonical.starts_with(&canonical_temp)
    {
        return true;
    }
    false
}

pub(super) fn is_inside_worktree(path: &Path, worktree: &Path, canonical_worktree: &Path) -> bool {
    if path.starts_with(worktree) || path.starts_with(canonical_worktree) {
        return true;
    }
    if let Some(normalized) = normalize_lexical_absolute(path)
        && (normalized.starts_with(worktree) || normalized.starts_with(canonical_worktree))
    {
        return true;
    }
    if let Ok(canonical) = fs::canonicalize(path)
        && (canonical.starts_with(worktree) || canonical.starts_with(canonical_worktree))
    {
        return true;
    }
    if let Some(canonical_ancestor) = path
        .ancestors()
        .find_map(|ancestor| fs::canonicalize(ancestor).ok())
        && (canonical_ancestor.starts_with(worktree)
            || canonical_ancestor.starts_with(canonical_worktree))
    {
        return true;
    }
    false
}

fn is_bare_command_name(program: &Path) -> bool {
    if program.is_absolute() || program.as_os_str().as_encoded_bytes().contains(&b'/') {
        return false;
    }
    let mut components = program.components();
    matches!(
        (components.next(), components.next()),
        (Some(Component::Normal(_)), None)
    )
}

fn validate_concrete_executable_target(target: &Path) -> Result<(), ExecutionSupervisorError> {
    if path_or_ancestor_reaches_forbidden_root(target, &FORBIDDEN_SSD_COMPONENTS) {
        return Err(ExecutionSupervisorError::ForbiddenSsdBuildExecutable(
            target.to_path_buf(),
        ));
    }
    if fs::symlink_metadata(target).is_ok() {
        let canonical = fs::canonicalize(target).map_err(|_| {
            ExecutionSupervisorError::ForbiddenSsdBuildExecutable(target.to_path_buf())
        })?;
        if is_forbidden_ssd_path(&canonical) {
            return Err(ExecutionSupervisorError::ForbiddenSsdBuildExecutable(
                canonical,
            ));
        }
    } else if let Some(canonical_ancestor) = target
        .ancestors()
        .find_map(|ancestor| fs::canonicalize(ancestor).ok())
        && is_forbidden_ssd_path(&canonical_ancestor)
    {
        return Err(ExecutionSupervisorError::ForbiddenSsdBuildExecutable(
            target.to_path_buf(),
        ));
    }
    Ok(())
}

pub(super) fn is_allowed_tmpdir(path: &Path) -> bool {
    if !path.is_absolute()
        || is_forbidden_ssd_path(path)
        || path
            .components()
            .any(|component| matches!(component, Component::ParentDir))
    {
        return false;
    }
    if let Some(canonical) = path
        .ancestors()
        .find_map(|ancestor| fs::canonicalize(ancestor).ok())
        && is_forbidden_ssd_path(&canonical)
    {
        return false;
    }
    true
}

fn normalize_lexical_absolute(path: &Path) -> Option<PathBuf> {
    let mut normalized = PathBuf::new();
    let mut saw_root = false;
    for component in path.components() {
        match component {
            Component::RootDir => {
                normalized.push(Path::new("/"));
                saw_root = true;
            }
            Component::CurDir => {}
            Component::ParentDir => {
                if !saw_root {
                    return None;
                }
                let _ = normalized.pop();
                if normalized.as_os_str().is_empty() {
                    normalized.push(Path::new("/"));
                }
            }
            Component::Normal(part) => normalized.push(part),
            Component::Prefix(_) => return None,
        }
    }
    saw_root.then_some(normalized)
}

fn path_or_ancestor_reaches_forbidden_root(path: &Path, forbidden_segments: &[&str]) -> bool {
    is_forbidden_or_lexically_rooted(path, forbidden_segments)
        || path
            .ancestors()
            .any(|ancestor| symlink_chain_reaches_forbidden_root(ancestor, forbidden_segments))
}

fn is_forbidden_or_lexically_ssd(path: &Path) -> bool {
    is_forbidden_or_lexically_rooted(path, &FORBIDDEN_SSD_COMPONENTS)
}

fn is_forbidden_or_lexically_rooted(path: &Path, forbidden_segments: &[&str]) -> bool {
    if has_forbidden_root_components(path, forbidden_segments) {
        return true;
    }
    normalize_lexical_absolute(path)
        .is_some_and(|normalized| has_forbidden_root_components(&normalized, forbidden_segments))
}

fn symlink_chain_reaches_forbidden_root(start: &Path, forbidden_segments: &[&str]) -> bool {
    let mut current = start.to_path_buf();
    for _ in 0..MAX_SYMLINK_HOPS {
        if is_forbidden_or_lexically_rooted(&current, forbidden_segments) {
            return true;
        }
        let Ok(metadata) = fs::symlink_metadata(&current) else {
            return false;
        };
        if !metadata.file_type().is_symlink() {
            return false;
        }
        let Ok(link_target) = fs::read_link(&current) else {
            return true;
        };
        if is_forbidden_or_lexically_rooted(&link_target, forbidden_segments) {
            return true;
        }
        current = if link_target.is_absolute() {
            link_target
        } else {
            current
                .parent()
                .unwrap_or_else(|| Path::new("/"))
                .join(link_target)
        };
    }
    true
}

pub(in crate::execution_supervisor) fn is_forbidden_ssd_path(path: &Path) -> bool {
    has_forbidden_root_components(path, &FORBIDDEN_SSD_COMPONENTS)
}

fn has_forbidden_root_components(path: &Path, forbidden_segments: &[&str]) -> bool {
    if forbidden_segments.is_empty() {
        return false;
    }
    let mut components = path.components();
    if !matches!(components.next(), Some(Component::RootDir)) {
        return false;
    }
    for expected in forbidden_segments {
        let Some(Component::Normal(actual)) = components.next() else {
            return false;
        };
        if !actual.eq_ignore_ascii_case(expected) {
            return false;
        }
    }
    true
}

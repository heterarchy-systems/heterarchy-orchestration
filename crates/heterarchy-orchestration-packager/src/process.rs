use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::io::{self, Read};
use std::os::fd::OwnedFd;
use std::os::unix::net::UnixStream;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::Value;

const PROCESS_CAPTURE_TIMEOUT: Duration = Duration::from_secs(30);
const PROCESS_CAPTURE_POLL_INTERVAL: Duration = Duration::from_millis(10);
const MAX_METADATA_OUTPUT_BYTES: usize = 1_048_576;
const MAX_TOOLCHAIN_BYTES: usize = 16 * 1024;
const CLEANUP_REAP_TIMEOUT: Duration = Duration::from_millis(500);

/// Finite deadline for the release desktop build, which may compile the full
/// native workspace while retaining process-group cleanup on timeout.
pub const RELEASE_BUILD_TIMEOUT: Duration = Duration::from_mins(30);
/// Finite deadline for one inherited-stdio packaging/doctor command.
pub const PACKAGING_COMMAND_TIMEOUT: Duration = Duration::from_mins(5);

/// Resolve Cargo's selected target directory through bounded metadata capture.
pub fn cargo_target_directory(root: &Path) -> Result<PathBuf, String> {
    let metadata = cargo_metadata(root)?;
    let target = metadata
        .get("target_directory")
        .and_then(Value::as_str)
        .map(PathBuf::from)
        .ok_or_else(|| "cargo metadata is missing target_directory".to_owned())?;
    if !target.is_absolute() {
        return Err(format!(
            "cargo metadata returned a non-absolute target directory: {}",
            target.display()
        ));
    }
    Ok(target)
}

/// Build the selected workspace package using the repository's Cargo toolchain.
pub fn cargo_build(
    root: &Path,
    package: &str,
    target_directory: &Path,
    features: &[&str],
) -> Result<(), String> {
    let channel = toolchain_channel(root)?;
    let macos_toolchain_libraries = macos_toolchain_fallback_library_path(root, &channel)?;
    let mut arguments = vec![
        OsString::from("run"),
        OsString::from(channel),
        OsString::from("cargo"),
        OsString::from("build"),
        OsString::from("--locked"),
        OsString::from("--release"),
        OsString::from("--target-dir"),
        target_directory.as_os_str().to_owned(),
        OsString::from("-p"),
        OsString::from(package),
    ];
    if !features.is_empty() {
        arguments.push(OsString::from("--features"));
        arguments.push(OsString::from(features.join(",")));
    }
    let mut environment = vec![(OsStr::new("CARGO_INCREMENTAL"), OsStr::new("0"))];
    if let Some(libraries) = macos_toolchain_libraries.as_deref() {
        environment.push((OsStr::new("DYLD_FALLBACK_LIBRARY_PATH"), libraries));
    }
    run_command_with_env(
        "rustup",
        &arguments,
        root,
        &environment,
        RELEASE_BUILD_TIMEOUT,
    )
}

#[cfg(target_os = "macos")]
fn macos_toolchain_fallback_library_path(
    root: &Path,
    channel: &str,
) -> Result<Option<OsString>, String> {
    let arguments = ["run", channel, "rustc", "--print", "sysroot"];
    let captured = capture_process(Path::new("rustup"), &arguments, root, MAX_TOOLCHAIN_BYTES)?;
    if !captured.status.success() {
        return Err(format!(
            "Rust toolchain sysroot lookup exited with status {}: {}",
            captured.status,
            String::from_utf8_lossy(&captured.stderr)
        ));
    }
    let sysroot = std::str::from_utf8(&captured.stdout)
        .map_err(|error| format!("Rust toolchain sysroot is not UTF-8: {error}"))?
        .trim();
    if sysroot.is_empty() {
        return Err("Rust toolchain sysroot lookup returned an empty path".to_owned());
    }
    let sysroot = PathBuf::from(sysroot);
    if !sysroot.is_absolute() {
        return Err(format!(
            "Rust toolchain sysroot is not absolute: {}",
            sysroot.display()
        ));
    }
    let library_directory = std::fs::canonicalize(sysroot.join("lib"))
        .map_err(|error| format!("canonicalize Rust toolchain library directory: {error}"))?;
    if !library_directory.is_dir() {
        return Err(format!(
            "Rust toolchain library path is not a directory: {}",
            library_directory.display()
        ));
    }
    let mut paths = vec![library_directory];
    if let Some(existing) = std::env::var_os("DYLD_FALLBACK_LIBRARY_PATH") {
        paths.extend(std::env::split_paths(&existing));
    }
    std::env::join_paths(paths)
        .map(Some)
        .map_err(|error| format!("compose Rust toolchain fallback library path: {error}"))
}

#[cfg(not(target_os = "macos"))]
fn macos_toolchain_fallback_library_path(
    _root: &Path,
    _channel: &str,
) -> Result<Option<OsString>, String> {
    Ok(None)
}

/// Run one inherited-stdio command with a finite deadline and process-group
/// cleanup. The child's output remains visible to the caller's terminal.
pub fn run_command(
    program: &str,
    arguments: &[OsString],
    cwd: &Path,
    timeout: Duration,
) -> Result<(), String> {
    run_command_with_env(program, arguments, cwd, &[], timeout)
}

/// Environment-aware form of [`run_command`] used by the release build.
pub fn run_command_with_env(
    program: &str,
    arguments: &[OsString],
    cwd: &Path,
    environment: &[(&OsStr, &OsStr)],
    timeout: Duration,
) -> Result<(), String> {
    if timeout.is_zero() {
        return Err("inherited-stdio command timeout must be positive".to_owned());
    }
    let mut child = spawn_inherited_process(program, arguments, cwd, environment)?;
    let deadline = Instant::now() + timeout;
    loop {
        match child.try_wait() {
            Ok(Some(status)) if status.success() => return Ok(()),
            Ok(Some(status)) => {
                return Err(format!(
                    "{program} {} exited with status {status}",
                    display_arguments(arguments)
                ));
            }
            Ok(None) if Instant::now() < deadline => {
                thread::sleep(PROCESS_CAPTURE_POLL_INTERVAL);
            }
            Ok(None) => {
                let termination = terminate_process_group(&mut child);
                let status = wait_for_inherited_child(&mut child, program, arguments)?;
                if !termination {
                    return Err(format!(
                        "failed to terminate timed-out {program} {} before reaping it with status {status}",
                        display_arguments(arguments)
                    ));
                }
                return Err(format!(
                    "{program} {} exceeded the bounded command timeout",
                    display_arguments(arguments)
                ));
            }
            Err(error) => {
                let termination = terminate_process_group(&mut child);
                let status = wait_for_inherited_child(&mut child, program, arguments)?;
                return Err(if termination {
                    format!(
                        "failed waiting for {program} {}: {error}; process exited with status {status}",
                        display_arguments(arguments)
                    )
                } else {
                    format!(
                        "failed waiting for {program} {}: {error}; process cleanup failed with status {status}",
                        display_arguments(arguments)
                    )
                });
            }
        }
    }
}

fn spawn_inherited_process(
    program: &str,
    arguments: &[OsString],
    cwd: &Path,
    environment: &[(&OsStr, &OsStr)],
) -> Result<Child, String> {
    let mut command = Command::new(program);
    command
        .args(arguments)
        .current_dir(cwd)
        .envs(environment.iter().copied())
        .process_group(0);
    command.spawn().map_err(|error| {
        format!(
            "failed to start {program} {}: {error}",
            display_arguments(arguments)
        )
    })
}

fn display_arguments(arguments: &[OsString]) -> String {
    arguments
        .iter()
        .map(|argument| argument.to_string_lossy())
        .collect::<Vec<_>>()
        .join(" ")
}

fn wait_for_inherited_child(
    child: &mut Child,
    program: &str,
    arguments: &[OsString],
) -> Result<ExitStatus, String> {
    let _kill_attempt = child.kill();
    let deadline = Instant::now() + CLEANUP_REAP_TIMEOUT;
    loop {
        match child.try_wait() {
            Ok(Some(status)) => return Ok(status),
            Ok(None) if Instant::now() < deadline => {
                thread::sleep(PROCESS_CAPTURE_POLL_INTERVAL);
            }
            Ok(None) => {
                return Err(format!(
                    "failed to reap {program} {} within the bounded cleanup deadline",
                    display_arguments(arguments)
                ));
            }
            Err(error) => {
                return Err(format!(
                    "failed to reap {program} {}: {error}",
                    display_arguments(arguments)
                ));
            }
        }
    }
}

fn cargo_metadata(root: &Path) -> Result<Value, String> {
    let channel = toolchain_channel(root)?;
    let arguments = [
        "run".to_owned(),
        channel,
        "cargo".to_owned(),
        "metadata".to_owned(),
        "--locked".to_owned(),
        "--no-deps".to_owned(),
        "--format-version".to_owned(),
        "1".to_owned(),
    ];
    let arguments = arguments.iter().map(String::as_str).collect::<Vec<_>>();
    let captured = capture_process(
        Path::new("rustup"),
        &arguments,
        root,
        MAX_METADATA_OUTPUT_BYTES,
    )?;
    if !captured.status.success() {
        return Err(format!(
            "cargo metadata exited with status {}: {}",
            captured.status,
            String::from_utf8_lossy(&captured.stderr)
        ));
    }
    serde_json::from_slice(&captured.stdout)
        .map_err(|error| format!("invalid cargo metadata JSON: {error}"))
}

fn toolchain_channel(root: &Path) -> Result<String, String> {
    let path = root.join("rust-toolchain.toml");
    let metadata = std::fs::metadata(&path)
        .map_err(|error| format!("read Rust toolchain metadata {}: {error}", path.display()))?;
    if !metadata.is_file() || metadata.len() > MAX_TOOLCHAIN_BYTES as u64 {
        return Err(format!(
            "Rust toolchain file {} is missing or exceeds {MAX_TOOLCHAIN_BYTES} bytes",
            path.display()
        ));
    }
    let mut body = String::new();
    File::open(&path)
        .map_err(|error| format!("open Rust toolchain file {}: {error}", path.display()))?
        .take((MAX_TOOLCHAIN_BYTES + 1) as u64)
        .read_to_string(&mut body)
        .map_err(|error| format!("read Rust toolchain file {}: {error}", path.display()))?;
    if body.len() > MAX_TOOLCHAIN_BYTES {
        return Err(format!(
            "Rust toolchain file {} exceeds {MAX_TOOLCHAIN_BYTES} bytes",
            path.display()
        ));
    }
    let manifest = toml::from_str::<toml::Value>(&body)
        .map_err(|error| format!("invalid Rust toolchain manifest: {error}"))?;
    let channel = manifest
        .get("toolchain")
        .and_then(|value| value.get("channel"))
        .and_then(toml::Value::as_str)
        .ok_or_else(|| "Rust toolchain manifest is missing toolchain.channel".to_owned())?;
    if channel.is_empty()
        || channel.len() > 128
        || channel.bytes().any(|byte| byte.is_ascii_control())
    {
        return Err("Rust toolchain channel is invalid".to_owned());
    }
    Ok(channel.to_owned())
}

#[derive(Debug)]
struct BoundedCapture {
    bytes: Vec<u8>,
    total: usize,
}

#[derive(Debug)]
struct CapturedProcess {
    status: ExitStatus,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

fn capture_process(
    program: &Path,
    arguments: &[&str],
    cwd: &Path,
    maximum: usize,
) -> Result<CapturedProcess, String> {
    capture_process_with_env(program, arguments, cwd, maximum, &[])
}

fn capture_process_with_env(
    program: &Path,
    arguments: &[&str],
    cwd: &Path,
    maximum: usize,
    environment: &[(&OsStr, &OsStr)],
) -> Result<CapturedProcess, String> {
    capture_process_until_with_env(
        program,
        arguments,
        cwd,
        maximum,
        Instant::now() + PROCESS_CAPTURE_TIMEOUT,
        environment,
    )
}

#[cfg(test)]
fn capture_process_until(
    program: &Path,
    arguments: &[&str],
    cwd: &Path,
    maximum: usize,
    deadline: Instant,
) -> Result<CapturedProcess, String> {
    capture_process_until_with_env(program, arguments, cwd, maximum, deadline, &[])
}

fn capture_process_until_with_env(
    program: &Path,
    arguments: &[&str],
    cwd: &Path,
    maximum: usize,
    deadline: Instant,
    environment: &[(&OsStr, &OsStr)],
) -> Result<CapturedProcess, String> {
    let (mut child, mut stdout, mut stderr) =
        spawn_captured_process(program, arguments, cwd, environment)?;
    stdout.set_nonblocking(true).map_err(|error| {
        terminate_then_report(&mut child, program, arguments, error.to_string())
    })?;
    stderr.set_nonblocking(true).map_err(|error| {
        terminate_then_report(&mut child, program, arguments, error.to_string())
    })?;

    let mut stdout_capture = BoundedCapture {
        bytes: Vec::with_capacity(maximum.min(64 * 1024)),
        total: 0,
    };
    let mut stderr_capture = BoundedCapture {
        bytes: Vec::with_capacity(maximum.min(64 * 1024)),
        total: 0,
    };
    let mut stdout_closed = false;
    let mut stderr_closed = false;
    let mut status = None;

    loop {
        if Instant::now() >= deadline {
            return Err(terminate_then_report(
                &mut child,
                program,
                arguments,
                format!("{} exceeded the bounded capture timeout", program.display()),
            ));
        }
        if status.is_none() {
            status = child.try_wait().map_err(|error| {
                terminate_then_report(
                    &mut child,
                    program,
                    arguments,
                    format!("failed to inspect {}: {error}", program.display()),
                )
            })?;
        }
        if !stdout_closed {
            match drain_nonblocking(&mut stdout, &mut stdout_capture, maximum, "stdout")
                .map_err(|error| terminate_then_report(&mut child, program, arguments, error))?
            {
                StreamReadState::Open => {}
                StreamReadState::Closed => stdout_closed = true,
                StreamReadState::TooLarge { actual } => {
                    return Err(terminate_then_report(
                        &mut child,
                        program,
                        arguments,
                        format!(
                            "{} stdout returned {actual} bytes; maximum is {maximum}",
                            program.display()
                        ),
                    ));
                }
            }
        }
        if !stderr_closed {
            match drain_nonblocking(&mut stderr, &mut stderr_capture, maximum, "stderr")
                .map_err(|error| terminate_then_report(&mut child, program, arguments, error))?
            {
                StreamReadState::Open => {}
                StreamReadState::Closed => stderr_closed = true,
                StreamReadState::TooLarge { actual } => {
                    return Err(terminate_then_report(
                        &mut child,
                        program,
                        arguments,
                        format!(
                            "{} stderr returned {actual} bytes; maximum is {maximum}",
                            program.display()
                        ),
                    ));
                }
            }
        }
        if let Some(status) = status.filter(|_| stdout_closed && stderr_closed) {
            return Ok(CapturedProcess {
                status,
                stdout: stdout_capture.bytes,
                stderr: stderr_capture.bytes,
            });
        }
        thread::sleep(PROCESS_CAPTURE_POLL_INTERVAL);
    }
}

enum StreamReadState {
    Open,
    Closed,
    TooLarge { actual: usize },
}

fn drain_nonblocking(
    stream: &mut UnixStream,
    capture: &mut BoundedCapture,
    maximum: usize,
    label: &str,
) -> Result<StreamReadState, String> {
    let mut buffer = [0_u8; 8 * 1024];
    loop {
        let remaining = maximum.saturating_add(1).saturating_sub(capture.total);
        let requested = remaining.min(buffer.len());
        match stream.read(&mut buffer[..requested]) {
            Ok(0) => return Ok(StreamReadState::Closed),
            Ok(read) => {
                capture.total = capture.total.saturating_add(read);
                capture.bytes.extend_from_slice(&buffer[..read]);
                if capture.total > maximum {
                    return Ok(StreamReadState::TooLarge {
                        actual: capture.total,
                    });
                }
            }
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                ) =>
            {
                return Ok(StreamReadState::Open);
            }
            Err(error) => return Err(format!("failed to read {label} capture stream: {error}")),
        }
    }
}

fn spawn_captured_process(
    program: &Path,
    arguments: &[&str],
    cwd: &Path,
    environment: &[(&OsStr, &OsStr)],
) -> Result<(Child, UnixStream, UnixStream), String> {
    let (stdout_reader, stdout_child) =
        UnixStream::pair().map_err(|error| format!("create stdout capture socket: {error}"))?;
    let (stderr_reader, stderr_child) =
        UnixStream::pair().map_err(|error| format!("create stderr capture socket: {error}"))?;
    let mut command = Command::new(program);
    command
        .args(arguments)
        .current_dir(cwd)
        .envs(environment.iter().copied())
        .process_group(0)
        .stdout(Stdio::from(OwnedFd::from(stdout_child)))
        .stderr(Stdio::from(OwnedFd::from(stderr_child)));
    let child = command.spawn().map_err(|error| {
        format!(
            "failed to start {} {arguments:?}: {error}",
            program.display()
        )
    })?;
    Ok((child, stdout_reader, stderr_reader))
}

fn terminate_and_reap(
    child: &mut std::process::Child,
    program: &Path,
    arguments: &[&str],
) -> Result<ExitStatus, String> {
    let termination = terminate_process_group(child);
    let status = wait_for_child_with_deadline(child, program, arguments)?;
    if !termination {
        return Err(format!(
            "failed to terminate {} {arguments:?} before reaping it with status {status}",
            program.display()
        ));
    }
    Ok(status)
}

fn wait_for_child_with_deadline(
    child: &mut Child,
    program: &Path,
    arguments: &[&str],
) -> Result<ExitStatus, String> {
    let _kill_attempt = child.kill();
    let deadline = Instant::now() + CLEANUP_REAP_TIMEOUT;
    loop {
        match child.try_wait() {
            Ok(Some(status)) => return Ok(status),
            Ok(None) if Instant::now() < deadline => {
                thread::sleep(PROCESS_CAPTURE_POLL_INTERVAL);
            }
            Ok(None) => {
                return Err(format!(
                    "failed to reap {} {arguments:?} within the bounded cleanup deadline",
                    program.display()
                ));
            }
            Err(error) => {
                return Err(format!(
                    "failed to reap {} {arguments:?}: {error}",
                    program.display()
                ));
            }
        }
    }
}

fn terminate_process_group(child: &mut Child) -> bool {
    let pid = child.id();
    let _term_sent = run_group_signal(pid, "-TERM");
    let term_deadline = Instant::now() + Duration::from_millis(50);
    while Instant::now() < term_deadline {
        match child.try_wait() {
            Ok(Some(_)) => break,
            Ok(None) => thread::sleep(PROCESS_CAPTURE_POLL_INTERVAL),
            Err(_) => return false,
        }
    }
    let _kill_sent = run_group_signal(pid, "-KILL");
    let group_deadline = Instant::now() + Duration::from_millis(500);
    while Instant::now() < group_deadline {
        if !run_group_signal(pid, "-0") {
            return true;
        }
        thread::sleep(PROCESS_CAPTURE_POLL_INTERVAL);
    }
    let _direct_kill = child.kill();
    !run_group_signal(pid, "-0")
}

fn run_group_signal(pid: u32, signal: &str) -> bool {
    let group = format!("-{pid}");
    let mut command = Command::new("/bin/kill");
    command
        .arg(signal)
        .arg("--")
        .arg(group)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let Ok(mut killer) = command.spawn() else {
        return false;
    };
    let deadline = Instant::now() + Duration::from_millis(100);
    loop {
        match killer.try_wait() {
            Ok(Some(status)) => return status.success(),
            Ok(None) if Instant::now() < deadline => {
                thread::sleep(PROCESS_CAPTURE_POLL_INTERVAL);
            }
            Ok(None) | Err(_) => {
                let _ignored = killer.kill();
                let _ignored =
                    wait_for_child_with_deadline(&mut killer, Path::new("/bin/kill"), &[]);
                return false;
            }
        }
    }
}

fn terminate_then_report(
    child: &mut std::process::Child,
    program: &Path,
    arguments: &[&str],
    primary_error: String,
) -> String {
    match terminate_and_reap(child, program, arguments) {
        Ok(_) => primary_error,
        Err(cleanup_error) => format!("{primary_error}; {cleanup_error}"),
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;
    use std::fs;

    use super::*;

    fn native_process_tests_are_unavailable() -> bool {
        !cfg!(unix)
    }

    #[test]
    fn repository_toolchain_manifest_is_read_as_a_document() -> Result<(), String> {
        let channel = toolchain_channel(&crate::workspace_root()?)?;
        assert!(!channel.is_empty());
        Ok(())
    }

    #[test]
    fn closed_output_does_not_end_the_process_deadline() -> Result<(), String> {
        let started = Instant::now();
        let result = capture_process_until(
            Path::new("/bin/sh"),
            &["-c", "exec >/dev/null 2>/dev/null; sleep 60"],
            Path::new("/"),
            1024,
            started + Duration::from_millis(100),
        );
        let error = result
            .err()
            .ok_or_else(|| "lingering process unexpectedly succeeded".to_owned())?;
        assert!(error.contains("timeout"), "{error}");
        assert!(started.elapsed() < Duration::from_secs(5));
        Ok(())
    }

    #[test]
    fn captured_process_rejects_large_output_at_the_per_stream_cap() {
        if native_process_tests_are_unavailable() {
            return;
        }
        let large = "x".repeat(MAX_METADATA_OUTPUT_BYTES + 1);
        let result = capture_process(
            Path::new("/usr/bin/printf"),
            &["%s", &large],
            Path::new("/"),
            MAX_METADATA_OUTPUT_BYTES,
        );
        assert!(result.is_err());
    }

    #[test]
    fn captured_process_kills_and_reaps_an_endless_oversized_producer() -> Result<(), String> {
        if native_process_tests_are_unavailable() {
            return Ok(());
        }
        let pid_path = std::env::temp_dir().join(format!(
            "heterarchy-orchestration-packager-capture-{}-{}.pid",
            std::process::id(),
            line!()
        ));
        let pid_path_argument = pid_path
            .to_str()
            .ok_or_else(|| "temporary PID path was not valid UTF-8".to_owned())?;
        let script = concat!(
            "printf '%s' \"$$\" > \"$1\"; ",
            "while :; do printf 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'; done"
        );

        let result = capture_process(
            Path::new("/bin/sh"),
            &["-c", script, "capture-test", pid_path_argument],
            Path::new("/"),
            1_024,
        );
        let pid = fs::read_to_string(&pid_path)
            .map_err(|error| format!("failed to read producer PID: {error}"))?;
        fs::remove_file(&pid_path)
            .map_err(|error| format!("failed to remove producer PID file: {error}"))?;

        let Err(error) = result else {
            return Err("endless oversized output unexpectedly succeeded".to_owned());
        };
        assert!(error.contains("stdout returned 1025 bytes; maximum is 1024"));
        let still_exists = Command::new("/bin/kill")
            .args(["-0", pid.trim()])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map_err(|error| format!("failed to inspect producer PID: {error}"))?
            .success();
        assert!(!still_exists, "producer remained alive or unreaped");
        Ok(())
    }

    #[test]
    fn captured_process_times_out_and_kills_pipe_holding_descendants() -> Result<(), String> {
        if native_process_tests_are_unavailable() {
            return Ok(());
        }
        let pid_path = std::env::temp_dir().join(format!(
            "heterarchy-orchestration-packager-descendant-{}-{}.pid",
            std::process::id(),
            line!()
        ));
        let pid_path_argument = pid_path
            .to_str()
            .ok_or_else(|| "temporary descendant PID path was not valid UTF-8".to_owned())?;
        let script = "sleep 30 & child=$!; printf '%s' \"$child\" > \"$1\"; wait";
        let started = Instant::now();
        let result = capture_process_until(
            Path::new("/bin/sh"),
            &["-c", script, "capture-timeout", pid_path_argument],
            Path::new("/"),
            1_024,
            Instant::now() + Duration::from_millis(50),
        );
        let descendant = fs::read_to_string(&pid_path)
            .map_err(|error| format!("failed to read descendant PID: {error}"))?;
        fs::remove_file(&pid_path)
            .map_err(|error| format!("failed to remove descendant PID file: {error}"))?;

        let Err(error) = result else {
            return Err("silent pipe-holding descendant unexpectedly succeeded".to_owned());
        };
        assert!(error.contains("exceeded the bounded capture timeout"));
        assert!(started.elapsed() < Duration::from_secs(2));
        let still_exists = Command::new("/bin/kill")
            .args(["-0", descendant.trim()])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map_err(|error| format!("failed to inspect descendant PID: {error}"))?
            .success();
        assert!(!still_exists, "pipe-holding descendant remained alive");
        Ok(())
    }

    #[test]
    fn inherited_command_reports_nonzero_exit_status() -> Result<(), String> {
        let arguments = vec![OsString::from("-c"), OsString::from("exit 7")];
        let error = run_command(
            "/bin/sh",
            &arguments,
            Path::new("/"),
            Duration::from_secs(1),
        )
        .err()
        .ok_or_else(|| "nonzero inherited command unexpectedly succeeded".to_owned())?;
        assert!(error.contains("status"));
        Ok(())
    }

    #[test]
    fn inherited_command_timeout_reaps_pipe_holding_descendant() -> Result<(), String> {
        let pid_path = std::env::temp_dir().join(format!(
            "heterarchy-orchestration-packager-inherited-{}-{}.pid",
            std::process::id(),
            line!()
        ));
        let pid_path_argument = pid_path
            .to_str()
            .ok_or_else(|| "temporary descendant PID path was not valid UTF-8".to_owned())?;
        let script = "sleep 30 & child=$!; printf '%s' \"$child\" > \"$1\"; wait";
        let arguments = vec![
            OsString::from("-c"),
            OsString::from(script),
            OsString::from("inherited-timeout"),
            OsString::from(pid_path_argument),
        ];
        let result = run_command(
            "/bin/sh",
            &arguments,
            Path::new("/"),
            Duration::from_millis(100),
        );
        let descendant = fs::read_to_string(&pid_path)
            .map_err(|error| format!("failed to read descendant PID: {error}"))?;
        fs::remove_file(&pid_path)
            .map_err(|error| format!("failed to remove descendant PID file: {error}"))?;
        let Err(error) = result else {
            return Err("inherited timeout unexpectedly succeeded".to_owned());
        };
        assert!(error.contains("bounded command timeout"));
        let still_exists = Command::new("/bin/kill")
            .args(["-0", descendant.trim()])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map_err(|error| format!("failed to inspect descendant PID: {error}"))?
            .success();
        assert!(!still_exists, "inherited command descendant remained alive");
        Ok(())
    }
}

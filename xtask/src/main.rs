use std::collections::BTreeSet;
use std::ffi::OsStr;
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::process::{Command, ExitStatus};

const NORMAL_RULES: &[&str] = &[
    "README.md",
    "00-overview.md",
    "01-boundary.md",
    "02-workspace-crate-rules.md",
    "03-deterministic-state-machine-rules.md",
    "04-ffi-interop-boundary-rules.md",
    "05-error-panic-rules.md",
    "06-testing-verification-rules.md",
    "07-performance-memory-rules.md",
    "08-observability-operations-rules.md",
    "09-module-layout-rules.md",
];

const TYPE_RULES: &[&str] = &[
    "README.md",
    "01-domain-newtype-invariant-rules.md",
    "02-enum-state-sum-type-rules.md",
    "03-option-result-outcome-rules.md",
    "04-trait-generic-bound-rules.md",
    "05-ownership-borrow-lifetime-rules.md",
    "06-smart-pointer-interior-mutability-rules.md",
    "07-send-sync-thread-safety-type-rules.md",
    "08-conversion-parsing-cast-rules.md",
    "09-public-api-type-evolution-rules.md",
    "10-unsafe-type-invariant-rules.md",
];

const ASYNC_RULES: &[&str] = &[
    "README.md",
    "01-execution-model-runtime-rules.md",
    "02-task-ownership-structured-concurrency-rules.md",
    "03-cancellation-timeout-select-rules.md",
    "04-bounded-concurrency-backpressure-rules.md",
    "05-shared-state-lock-channel-rules.md",
    "06-blocking-cpu-parallelism-rules.md",
    "07-send-sync-local-task-rules.md",
    "08-async-error-shutdown-rules.md",
    "09-async-testing-verification-rules.md",
    "10-async-observability-performance-rules.md",
];

const REQUIRED_SHARED: &[&str] = &[
    "shared/authority.md",
    "shared/testing.md",
    "shared/agent-execution.md",
    "shared/solution-minimality.md",
];

const REQUIRED_SKILLS: &[&str] = &[
    "skills/rust-engineering/SKILL.md",
    "skills/minimal-engineering/SKILL.md",
];

const CHECK_ARGS: &[&str] = &["check", "--locked", "--workspace", "--all-targets"];
const CLIPPY_ARGS: &[&str] = &["clippy", "--locked", "--workspace", "--all-targets"];
const TEST_ARGS: &[&str] = &["test", "--locked", "--workspace", "--all-targets"];
const DOC_TEST_ARGS: &[&str] = &["test", "--locked", "--workspace", "--doc"];

#[derive(Clone, Debug, Eq, PartialEq)]
struct ToolchainPolicy {
    channel: String,
}

impl ToolchainPolicy {
    fn load(root: &Path) -> Result<Self, String> {
        let path = root.join("rust-toolchain.toml");
        let manifest = read_toml(&path)?;
        let channel = required_str(required_table(&manifest, "toolchain")?, "channel")?;
        Ok(Self {
            channel: channel.to_owned(),
        })
    }

    fn command(&self, program: &str) -> Command {
        let mut command = Command::new("rustup");
        command.args(["run", &self.channel, program]);
        command
    }

    fn cargo_family_command(&self, program: &str) -> Result<Command, String> {
        let rustc = self.tool_path("rustc")?;
        let rustdoc = self.tool_path("rustdoc")?;
        let mut command = self.command(program);
        command.env("RUSTC", rustc).env("RUSTDOC", rustdoc);
        Ok(command)
    }

    fn tool_path(&self, program: &str) -> Result<PathBuf, String> {
        let mut command = Command::new("rustup");
        command.args(["which", "--toolchain", &self.channel, program]);
        let path = output_line(
            &mut command,
            &format!("rustup which --toolchain {} {program}", self.channel),
        )?;
        let path = PathBuf::from(path);
        if !path.is_absolute() {
            return Err(format!(
                "declared toolchain returned a non-absolute {program} path"
            ));
        }
        Ok(path)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PublicPolicy {
    crate_name: String,
    msrv: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct HarnessPolicy {
    workspace_msrv: String,
    public_policy: PublicPolicy,
    supply_chain: bool,
}

impl HarnessPolicy {
    fn load(root: &Path) -> Result<Self, String> {
        let harness = harness_root(root);
        if !harness.is_dir() {
            return Ok(Self {
                workspace_msrv: workspace_msrv(root)?,
                public_policy: PublicPolicy {
                    crate_name: "heterarchy-orchestration-policy".to_owned(),
                    msrv: "1.92".to_owned(),
                },
                supply_chain: false,
            });
        }
        let path = harness.join("HARNESS.toml");
        let config = read_toml(&path)?;
        let toolchain = required_table(&config, "toolchain")?;
        if required_str(toolchain, "policy")? != "msrv" {
            return Err("toolchain.policy must be \"msrv\"".to_owned());
        }
        let workspace_msrv = required_str(toolchain, "msrv")?.to_owned();

        let public = required_table(&config, "public_policy")?;
        let public_policy = PublicPolicy {
            crate_name: required_str(public, "crate")?.to_owned(),
            msrv: required_str(public, "msrv")?.to_owned(),
        };
        if required_str(public, "verification")? != "direct-cargo-with-rust-1.92" {
            return Err(
                "public_policy.verification must be direct-cargo-with-rust-1.92".to_owned(),
            );
        }

        let supply_chain = required_bool(required_table(&config, "verification")?, "supply_chain")?;
        let unsafe_policy = required_table(&config, "unsafe")?;
        if required_str(unsafe_policy, "mode")? != "forbid" {
            return Err("unsafe.mode must be \"forbid\" for this repository".to_owned());
        }
        let approved = required_array(unsafe_policy, "approved_crates")?;
        if !approved.is_empty() {
            return Err("unsafe.approved_crates must be empty when unsafe.mode=forbid".to_owned());
        }

        Ok(Self {
            workspace_msrv,
            public_policy,
            supply_chain,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum XtaskCommand {
    Rules,
    Fmt,
    Check,
    Clippy,
    Test,
    Msrv,
    Ci,
    Deps,
    Doctor,
    Extended,
    Perf,
    Help,
}

fn parse_command<I, S>(args: I) -> Result<XtaskCommand, String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let mut args = args.into_iter();
    let command = args.next().map_or_else(
        || "help".to_owned(),
        |value| value.as_ref().to_string_lossy().into_owned(),
    );
    if let Some(extra) = args.next() {
        return Err(format!(
            "unexpected argument after {command:?}: {}",
            extra.as_ref().to_string_lossy()
        ));
    }
    match command.as_str() {
        "rules" => Ok(XtaskCommand::Rules),
        "fmt" => Ok(XtaskCommand::Fmt),
        "check" => Ok(XtaskCommand::Check),
        "clippy" => Ok(XtaskCommand::Clippy),
        "test" => Ok(XtaskCommand::Test),
        "msrv" => Ok(XtaskCommand::Msrv),
        "ci" => Ok(XtaskCommand::Ci),
        "deps" => Ok(XtaskCommand::Deps),
        "doctor" => Ok(XtaskCommand::Doctor),
        "extended" => Ok(XtaskCommand::Extended),
        "perf" => Ok(XtaskCommand::Perf),
        "help" | "-h" | "--help" => Ok(XtaskCommand::Help),
        other => Err(format!("unknown xtask command: {other}")),
    }
}

fn main() {
    let result = parse_command(std::env::args_os().skip(1)).and_then(run);
    if let Err(error) = result {
        eprintln!("xtask: {error}");
        std::process::exit(1);
    }
}

fn run(command: XtaskCommand) -> Result<(), String> {
    match command {
        XtaskCommand::Rules => rules(),
        XtaskCommand::Fmt => fmt(),
        XtaskCommand::Check => check(),
        XtaskCommand::Clippy => clippy(),
        XtaskCommand::Test => test(),
        XtaskCommand::Msrv => msrv(),
        XtaskCommand::Ci => ci(),
        XtaskCommand::Deps => deps(),
        XtaskCommand::Doctor => doctor(),
        XtaskCommand::Extended => extended(),
        XtaskCommand::Perf => perf(),
        XtaskCommand::Help => {
            help();
            Ok(())
        }
    }
}

fn help() {
    println!("cargo xtask <rules|fmt|check|clippy|test|msrv|ci|deps|doctor|extended|perf>");
}

fn workspace_root() -> Result<PathBuf, String> {
    let runtime_manifest_dir = std::env::var_os("CARGO_MANIFEST_DIR");
    let cwd = std::env::current_dir()
        .map_err(|error| format!("failed to read current working directory: {error}"))?;
    workspace_root_from(runtime_manifest_dir.as_deref(), &cwd)
}

fn workspace_root_from(
    runtime_manifest_dir: Option<&OsStr>,
    cwd: &Path,
) -> Result<PathBuf, String> {
    if let Some(runtime_manifest_dir) = runtime_manifest_dir {
        return find_workspace_root(Path::new(runtime_manifest_dir));
    }
    find_workspace_root(cwd)
}

fn find_workspace_root(start: &Path) -> Result<PathBuf, String> {
    for ancestor in start.ancestors() {
        let path = ancestor.join("Cargo.toml");
        if path.is_file() && read_toml(&path)?.get("workspace").is_some() {
            return ancestor.canonicalize().map_err(|error| {
                format!(
                    "failed to canonicalize Cargo workspace root {}: {error}",
                    ancestor.display()
                )
            });
        }
    }
    Err(format!(
        "could not locate Cargo workspace root from {}",
        start.display()
    ))
}

fn harness_root(root: &Path) -> PathBuf {
    root.join(".agents").join("rust_dev_harness")
}

fn read_toml(path: &Path) -> Result<toml::Value, String> {
    let source = fs::read_to_string(path)
        .map_err(|error| format!("failed to read {}: {error}", path.display()))?;
    toml::from_str(&source).map_err(|error| format!("invalid TOML {}: {error}", path.display()))
}

fn required_table<'a>(
    value: &'a toml::Value,
    name: &str,
) -> Result<&'a toml::value::Table, String> {
    value
        .get(name)
        .and_then(toml::Value::as_table)
        .ok_or_else(|| format!("missing [{name}] table"))
}

fn required_str<'a>(table: &'a toml::value::Table, name: &str) -> Result<&'a str, String> {
    table
        .get(name)
        .and_then(toml::Value::as_str)
        .ok_or_else(|| format!("missing/non-string field {name}"))
}

fn required_bool(table: &toml::value::Table, name: &str) -> Result<bool, String> {
    table
        .get(name)
        .and_then(toml::Value::as_bool)
        .ok_or_else(|| format!("missing/non-boolean field {name}"))
}

fn required_array<'a>(
    table: &'a toml::value::Table,
    name: &str,
) -> Result<&'a Vec<toml::Value>, String> {
    table
        .get(name)
        .and_then(toml::Value::as_array)
        .ok_or_else(|| format!("missing/non-array field {name}"))
}

fn require_file(path: &Path) -> Result<(), String> {
    if path.is_file() {
        Ok(())
    } else {
        Err(format!("missing required file: {}", path.display()))
    }
}

fn verify_group(root: &Path, directory: &str, files: &[&str]) -> Result<(), String> {
    let directory = root.join("rules").join(directory);
    for file in files {
        require_file(&directory.join(file))?;
    }
    Ok(())
}

fn workspace_msrv(root: &Path) -> Result<String, String> {
    let manifest = read_toml(&root.join("Cargo.toml"))?;
    let workspace = required_table(&manifest, "workspace")?;
    let package = workspace
        .get("package")
        .and_then(toml::Value::as_table)
        .ok_or_else(|| "root Cargo.toml must define [workspace.package]".to_owned())?;
    package
        .get("rust-version")
        .and_then(toml::Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| "workspace.package.rust-version is required".to_owned())
}

fn rules() -> Result<(), String> {
    let root = workspace_root()?;
    let harness = harness_root(&root);
    let toolchain = ToolchainPolicy::load(&root)?;
    let policy = HarnessPolicy::load(&root)?;

    require_file(&root.join("AGENTS.md"))?;
    // The `.agents/` harness bundle is local operator material and is never
    // pushed (the Alexandria CI pattern). Fresh checkouts verify the tracked
    // gates only; an operator machine that materialized the bundle gets the
    // full bundle verification on top.
    if harness.is_dir() {
        for required in [
            "PROJECT_PROFILE.md",
            "HARNESS.toml",
            "HARNESS_MANIFEST.toml",
        ] {
            require_file(&harness.join(required))?;
        }
        for required in REQUIRED_SHARED.iter().chain(REQUIRED_SKILLS) {
            require_file(&harness.join(required))?;
        }
        verify_group(&harness, "normal_dev_rules", NORMAL_RULES)?;
        verify_group(&harness, "type_dev_rules", TYPE_RULES)?;
        verify_group(&harness, "async_dev_rules", ASYNC_RULES)?;
        verify_manifest(&harness)?;
        verify_sources_inventory(&harness)?;
    } else {
        println!(
            "harness bundle not materialized; verifying tracked gates only (workspace MSRV {})",
            policy.workspace_msrv
        );
    }
    verify_declared_toolchain_available(&root, &toolchain)?;
    verify_cargo_baseline(&root, &policy)?;
    verify_removed_packages_absent(&root)?;
    verify_workspace_members(&root, &toolchain, &policy)?;
    println!("xtask rules: PASS");
    Ok(())
}

fn verify_manifest(harness: &Path) -> Result<(), String> {
    let manifest = read_toml(&harness.join("HARNESS_MANIFEST.toml"))?;
    if manifest
        .get("schema_version")
        .and_then(toml::Value::as_integer)
        != Some(1)
    {
        return Err("HARNESS_MANIFEST.toml schema_version must be 1".to_owned());
    }
    for (key, expected) in [
        ("family", "engineering"),
        ("package", "Rust_package"),
        ("language", "rust"),
    ] {
        if manifest.get(key).and_then(toml::Value::as_str) != Some(expected) {
            return Err(format!("HARNESS_MANIFEST.toml {key} must be {expected:?}"));
        }
    }
    let identity = required_table(&manifest, "identity")?;
    if required_str(identity, "source")? != "Alexandria" {
        return Err("HARNESS_MANIFEST.toml identity.source must be Alexandria".to_owned());
    }
    let source_path = required_str(identity, "source_path")?;
    if source_path != "Jobs/Harness/Engineering Harness/02 Rust/" {
        return Err(
            "HARNESS_MANIFEST.toml identity.source_path is not the canonical Rust Harness path"
                .to_owned(),
        );
    }

    let contracts = required_table(&manifest, "contracts")?;
    verify_ref(harness, contracts, "project_profile", "PROJECT_PROFILE.md")?;
    verify_ref(harness, contracts, "machine_config", "HARNESS.toml")?;
    verify_ref(harness, contracts, "rules_router", "rules/README.md")?;

    let harness_refs = required_table(&manifest, "harness")?;
    verify_ref(harness, harness_refs, "xtask", "harness/xtask/Cargo.toml")?;
    verify_ref(harness, harness_refs, "sources", "SOURCES.json")?;

    verify_string_array(
        required_table(&manifest, "shared")?,
        "contracts",
        &[
            "authority",
            "testing",
            "agent-execution",
            "solution-minimality",
        ],
    )?;
    verify_string_array(
        required_table(&manifest, "shared")?,
        "skills",
        &["minimal-engineering"],
    )?;
    verify_string_array(
        required_table(&manifest, "activation")?,
        "baseline",
        &["normal", "type"],
    )?;
    verify_string_array(
        required_table(&manifest, "activation")?,
        "optional",
        &["async"],
    )?;
    verify_string_array(
        required_table(&manifest, "skills")?,
        "entries",
        REQUIRED_SKILLS,
    )?;
    for skill in REQUIRED_SKILLS {
        require_file(&harness.join(skill))?;
    }
    verify_string_array(
        required_table(&manifest, "verification")?,
        "entrypoints",
        &["make rules", "make ci", "make doctor", "make extended"],
    )?;
    if required_str(
        required_table(&manifest, "provenance")?,
        "manifest_hash_algorithm",
    )? != "sha256"
    {
        return Err("provenance.manifest_hash_algorithm must be sha256".to_owned());
    }
    Ok(())
}

fn verify_ref(
    harness: &Path,
    table: &toml::value::Table,
    key: &str,
    expected: &str,
) -> Result<(), String> {
    if required_str(table, key)? != expected {
        return Err(format!("manifest {key} reference must be {expected:?}"));
    }
    require_file(&harness.join(expected))
}

fn verify_string_array(
    table: &toml::value::Table,
    key: &str,
    expected: &[&str],
) -> Result<(), String> {
    let actual = required_array(table, key)?
        .iter()
        .map(|value| {
            value
                .as_str()
                .ok_or_else(|| format!("{key} entries must be strings"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    if actual == expected {
        Ok(())
    } else {
        Err(format!(
            "manifest {key} must be {expected:?}, found {actual:?}"
        ))
    }
}

fn verify_sources_inventory(harness: &Path) -> Result<(), String> {
    let path = harness.join("SOURCES.json");
    let bytes =
        fs::read(&path).map_err(|error| format!("failed to read {}: {error}", path.display()))?;
    let inventory: serde_json::Value = serde_json::from_slice(&bytes)
        .map_err(|error| format!("invalid JSON {}: {error}", path.display()))?;
    if inventory
        .get("schema_version")
        .and_then(serde_json::Value::as_u64)
        != Some(1)
    {
        return Err("SOURCES.json schema_version must be 1".to_owned());
    }
    for key in ["retrieved_at", "materialization"] {
        if inventory
            .get(key)
            .and_then(serde_json::Value::as_str)
            .is_none()
        {
            return Err(format!("SOURCES.json {key} must be a string"));
        }
    }
    let sources = inventory
        .get("sources")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| "SOURCES.json sources must be an array".to_owned())?;
    if sources.is_empty() {
        return Err("SOURCES.json sources must not be empty".to_owned());
    }
    let mut seen = BTreeSet::new();
    for source in sources {
        let relative = source
            .get("path")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| "SOURCES.json source.path must be a string".to_owned())?;
        let relative_path = Path::new(relative);
        if relative_path.is_absolute()
            || relative_path.components().any(|component| {
                matches!(
                    component,
                    Component::ParentDir | Component::RootDir | Component::Prefix(_)
                )
            })
        {
            return Err(format!(
                "SOURCES.json source.path is not confined: {relative:?}"
            ));
        }
        if !seen.insert(relative.to_owned()) {
            return Err(format!(
                "SOURCES.json contains duplicate path: {relative:?}"
            ));
        }
        let candidate = harness.join(relative_path);
        require_file(&candidate)?;
        let canonical_harness = harness
            .canonicalize()
            .map_err(|error| format!("failed to canonicalize {}: {error}", harness.display()))?;
        let canonical_candidate = candidate
            .canonicalize()
            .map_err(|error| format!("failed to canonicalize {}: {error}", candidate.display()))?;
        if !canonical_candidate.starts_with(&canonical_harness) {
            return Err(format!(
                "SOURCES.json source.path escapes through a symlink: {relative:?}"
            ));
        }
        for key in ["source_path", "note_id", "modified_at"] {
            if source
                .get(key)
                .and_then(serde_json::Value::as_str)
                .is_none()
            {
                return Err(format!(
                    "SOURCES.json {relative:?} field {key} must be a string"
                ));
            }
        }
        if source
            .get("version")
            .and_then(serde_json::Value::as_u64)
            .is_none()
        {
            return Err(format!(
                "SOURCES.json {relative:?} version must be an integer"
            ));
        }
        for key in ["source_content_hash", "materialized_sha256"] {
            let hash = source
                .get(key)
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| format!("SOURCES.json {relative:?} field {key} must be a string"))?;
            if hash.len() != 64 || !hash.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                return Err(format!(
                    "SOURCES.json {relative:?} field {key} must be a SHA-256 hex string"
                ));
            }
        }
    }
    Ok(())
}

fn lint_level<'a>(table: &'a toml::value::Table, name: &str) -> Option<&'a str> {
    match table.get(name)? {
        toml::Value::String(value) => Some(value),
        toml::Value::Table(value) => value.get("level").and_then(toml::Value::as_str),
        _ => None,
    }
}

fn verify_cargo_baseline(root: &Path, policy: &HarnessPolicy) -> Result<(), String> {
    let manifest = read_toml(&root.join("Cargo.toml"))?;
    verify_cargo_manifest(&manifest, policy)
}

fn verify_cargo_manifest(manifest: &toml::Value, policy: &HarnessPolicy) -> Result<(), String> {
    let workspace = required_table(manifest, "workspace")?;
    if workspace.get("resolver").and_then(toml::Value::as_str) != Some("3") {
        return Err("workspace.resolver must be \"3\"".to_owned());
    }
    let package = workspace
        .get("package")
        .and_then(toml::Value::as_table)
        .ok_or_else(|| "root Cargo.toml must define [workspace.package]".to_owned())?;
    if package.get("edition").and_then(toml::Value::as_str) != Some("2024") {
        return Err("workspace.package.edition must be \"2024\"".to_owned());
    }
    let actual_msrv = package
        .get("rust-version")
        .and_then(toml::Value::as_str)
        .ok_or_else(|| "workspace.package.rust-version is required".to_owned())?;
    if actual_msrv != policy.workspace_msrv {
        return Err(format!(
            "workspace MSRV mismatch: HARNESS.toml={:?}, Cargo.toml={actual_msrv:?}",
            policy.workspace_msrv
        ));
    }

    let lints = workspace
        .get("lints")
        .and_then(toml::Value::as_table)
        .ok_or_else(|| "root Cargo.toml must define [workspace.lints]".to_owned())?;
    let rust = lints
        .get("rust")
        .and_then(toml::Value::as_table)
        .ok_or_else(|| "root Cargo.toml must define [workspace.lints.rust]".to_owned())?;
    for (lint, expected) in [("warnings", "deny"), ("unsafe_code", "forbid")] {
        if lint_level(rust, lint) != Some(expected) {
            return Err(format!("workspace rust lint {lint} must be {expected}"));
        }
    }
    let clippy = lints
        .get("clippy")
        .and_then(toml::Value::as_table)
        .ok_or_else(|| "root Cargo.toml must define [workspace.lints.clippy]".to_owned())?;
    if lint_level(clippy, "all") != Some("deny") {
        return Err("workspace Clippy all group must be deny".to_owned());
    }
    if lint_level(clippy, "pedantic") != Some("warn") {
        return Err("workspace Clippy pedantic group must remain warn".to_owned());
    }
    Ok(())
}

fn verify_removed_packages_absent(root: &Path) -> Result<(), String> {
    let lock = read_toml(&root.join("Cargo.lock"))?;
    let packages = lock
        .get("package")
        .and_then(toml::Value::as_array)
        .ok_or_else(|| "Cargo.lock must contain package records".to_owned())?;
    for package in packages {
        let name = package
            .get("name")
            .and_then(toml::Value::as_str)
            .ok_or_else(|| "Cargo.lock package missing name".to_owned())?;
        if matches!(
            name,
            "heterarchy-orchestration-core" | "heterarchy-orchestration-py" | "pyo3"
        ) {
            return Err(format!(
                "removed architecture package remains in Cargo.lock: {name}"
            ));
        }
    }
    Ok(())
}

fn cargo_metadata(root: &Path, policy: &ToolchainPolicy) -> Result<serde_json::Value, String> {
    let mut command = policy.command("cargo");
    command
        .args(["metadata", "--locked", "--format-version", "1", "--no-deps"])
        .current_dir(root);
    let output = command
        .output()
        .map_err(|error| format!("failed to run cargo metadata: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "cargo metadata failed with status {}",
            output.status
        ));
    }
    serde_json::from_slice(&output.stdout)
        .map_err(|error| format!("cargo metadata returned invalid JSON: {error}"))
}

fn verify_workspace_members(
    root: &Path,
    toolchain: &ToolchainPolicy,
    policy: &HarnessPolicy,
) -> Result<(), String> {
    let metadata = cargo_metadata(root, toolchain)?;
    let packages = metadata
        .get("packages")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| "cargo metadata has no packages array".to_owned())?;
    let mut public_seen = false;
    let mut xtask_count = 0_u8;
    for package in packages {
        let name = package
            .get("name")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| "cargo metadata package missing name".to_owned())?;
        let manifest_path = package
            .get("manifest_path")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| "cargo metadata package missing manifest_path".to_owned())?;
        let manifest = read_toml(Path::new(manifest_path))?;
        if manifest
            .get("lints")
            .and_then(toml::Value::as_table)
            .and_then(|table| table.get("workspace"))
            .and_then(toml::Value::as_bool)
            != Some(true)
        {
            return Err(format!(
                "safe workspace member must use [lints] workspace = true: {manifest_path}"
            ));
        }
        if name == policy.public_policy.crate_name {
            public_seen = true;
            let actual = package
                .get("rust_version")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| format!("public policy crate {name:?} must declare rust-version"))?;
            verify_public_policy_version(name, actual, &policy.public_policy)?;
        }
        if name == "xtask" {
            xtask_count = xtask_count.saturating_add(1);
            let expected = root.join("xtask/Cargo.toml");
            if Path::new(manifest_path) != expected {
                return Err(format!(
                    "xtask must be owned by {}, found {manifest_path}",
                    expected.display()
                ));
            }
        }
    }
    if !public_seen {
        return Err(format!(
            "public policy crate {:?} is not a workspace member",
            policy.public_policy.crate_name
        ));
    }
    if xtask_count != 1 {
        return Err(format!(
            "workspace must contain exactly one xtask package, found {xtask_count}"
        ));
    }
    Ok(())
}

fn verify_public_policy_version(
    name: &str,
    actual: &str,
    policy: &PublicPolicy,
) -> Result<(), String> {
    if name != policy.crate_name {
        return Err(format!(
            "public policy crate mismatch: expected {:?}, found {name:?}",
            policy.crate_name
        ));
    }
    if actual != policy.msrv {
        return Err(format!(
            "public policy MSRV mismatch: HARNESS.toml={:?}, Cargo metadata={actual:?}",
            policy.msrv
        ));
    }
    Ok(())
}

fn tool_version(root: &Path, policy: &ToolchainPolicy, program: &str) -> Result<String, String> {
    let mut command = policy.command(program);
    command.arg("--version").current_dir(root);
    output_line(&mut command, &format!("declared {program} --version"))
}

fn ambient_tool_version(root: &Path, program: &str) -> Result<String, String> {
    let mut command = Command::new(program);
    command.arg("--version").current_dir(root);
    output_line(&mut command, &format!("ambient {program} --version"))
}

fn output_line(command: &mut Command, label: &str) -> Result<String, String> {
    let output = command
        .output()
        .map_err(|error| format!("failed to execute {label}: {error}"))?;
    if !output.status.success() {
        return Err(format!("{label} failed with status {}", output.status));
    }
    String::from_utf8(output.stdout)
        .map(|value| value.trim().to_owned())
        .map_err(|error| format!("{label} returned non-UTF8 output: {error}"))
}

fn verify_declared_toolchain_available(
    root: &Path,
    policy: &ToolchainPolicy,
) -> Result<(), String> {
    let _ = tool_version(root, policy, "rustc")?;
    let _ = tool_version(root, policy, "cargo")?;
    Ok(())
}

fn cargo_command(root: &Path) -> Result<Command, String> {
    ToolchainPolicy::load(root)?.cargo_family_command("cargo")
}

fn status(command: &mut Command, label: &str) -> Result<(), String> {
    let exit = command
        .status()
        .map_err(|error| format!("failed to execute {label}: {error}"))?;
    require_success(exit, label)
}

fn require_success(exit: ExitStatus, label: &str) -> Result<(), String> {
    if exit.success() {
        Ok(())
    } else {
        Err(format!("{label} failed with status {exit}"))
    }
}

fn fmt() -> Result<(), String> {
    let root = workspace_root()?;
    let mut command = ToolchainPolicy::load(&root)?.cargo_family_command("cargo-fmt")?;
    command
        .args(["fmt", "--all", "--", "--check"])
        .current_dir(&root);
    status(&mut command, "cargo fmt")
}

fn check() -> Result<(), String> {
    let root = workspace_root()?;
    let mut command = cargo_command(&root)?;
    command.args(CHECK_ARGS).current_dir(&root);
    status(&mut command, "cargo check")?;

    clippy()
}

fn clippy() -> Result<(), String> {
    let root = workspace_root()?;
    let mut command = ToolchainPolicy::load(&root)?.cargo_family_command("cargo-clippy")?;
    command.args(CLIPPY_ARGS).current_dir(&root);
    status(&mut command, "cargo clippy")
}

fn test() -> Result<(), String> {
    let root = workspace_root()?;
    let mut command = cargo_command(&root)?;
    command.args(TEST_ARGS).current_dir(&root);
    status(&mut command, "cargo test --all-targets")?;

    let mut command = cargo_command(&root)?;
    command.args(DOC_TEST_ARGS).current_dir(&root);
    status(&mut command, "cargo test --doc")
}

fn msrv() -> Result<(), String> {
    let root = workspace_root()?;
    let policy = HarnessPolicy::load(&root)?;
    let channel = format!("{}.0", policy.public_policy.msrv);
    let toolchain = ToolchainPolicy { channel };
    let mut command = toolchain.cargo_family_command("cargo")?;
    command
        .args([
            "test",
            "--locked",
            "-p",
            &policy.public_policy.crate_name,
            "--all-targets",
        ])
        .current_dir(&root);
    status(&mut command, "public policy MSRV cargo test --all-targets")?;

    let mut command = toolchain.cargo_family_command("cargo")?;
    command
        .args([
            "test",
            "--locked",
            "-p",
            &policy.public_policy.crate_name,
            "--doc",
        ])
        .current_dir(&root);
    status(&mut command, "public policy MSRV cargo test --doc")
}

fn ci() -> Result<(), String> {
    rules()?;
    fmt()?;
    check()?;
    test()
}

fn deps() -> Result<(), String> {
    let root = workspace_root()?;
    require_file(&root.join("deny.toml"))?;
    let mut command = cargo_command(&root)?;
    command.args(["deny", "check"]).current_dir(&root);
    status(&mut command, "cargo deny check")
}

fn doctor() -> Result<(), String> {
    let root = workspace_root()?;
    let policy = ToolchainPolicy::load(&root)?;
    println!("workspace: {}", root.display());
    println!("declared toolchain: {}", policy.channel);
    for program in ["rustc", "cargo"] {
        let ambient = ambient_tool_version(&root, program)?;
        let declared = tool_version(&root, &policy, program)?;
        println!("ambient {program}: {ambient}");
        println!("declared {program}: {declared}");
        if declared != ambient {
            println!(
                "WARNING: ambient {program} differs; xtask child commands use the declared toolchain"
            );
        }
    }
    rules()
}

fn extended() -> Result<(), String> {
    ci()?;
    msrv()?;
    let root = workspace_root()?;
    if HarnessPolicy::load(&root)?.supply_chain {
        deps()?;
    }
    let mut command = cargo_command(&root)?;
    command
        .args([
            "--config",
            "build.rustdocflags=[\"-D\",\"warnings\"]",
            "doc",
            "--locked",
            "--workspace",
            "--no-deps",
        ])
        .current_dir(&root);
    status(&mut command, "cargo doc")
}

fn perf() -> Result<(), String> {
    let root = workspace_root()?;
    let policy = ToolchainPolicy::load(&root)?;
    let metadata = cargo_metadata(&root, &policy)?;
    if !has_benchmark_target(&metadata) {
        return Err(
            "perf is not configured: Cargo metadata declares no benchmark target".to_owned(),
        );
    }
    let mut command = policy.command("cargo");
    command
        .args(["bench", "--locked", "--workspace"])
        .current_dir(&root);
    status(&mut command, "cargo bench")
}

fn has_benchmark_target(metadata: &serde_json::Value) -> bool {
    metadata
        .get("packages")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|package| package.get("targets").and_then(serde_json::Value::as_array))
        .flatten()
        .filter_map(|target| target.get("kind").and_then(serde_json::Value::as_array))
        .flatten()
        .any(|kind| kind.as_str() == Some("bench"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);

    struct TempDir(PathBuf);

    impl TempDir {
        fn new(label: &str) -> Result<Self, String> {
            let serial = NEXT_TEMP.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "orchestration-xtask-{label}-{}-{serial}",
                std::process::id()
            ));
            fs::create_dir_all(&path)
                .map_err(|error| format!("failed to create {}: {error}", path.display()))?;
            Ok(Self(path))
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn write(path: &Path, content: &str) -> Result<(), String> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .map_err(|error| format!("failed to create {}: {error}", parent.display()))?;
        }
        fs::write(path, content)
            .map_err(|error| format!("failed to write {}: {error}", path.display()))
    }

    fn command_parts(command: &Command) -> Vec<String> {
        std::iter::once(command.get_program())
            .chain(command.get_args())
            .map(|part| part.to_string_lossy().into_owned())
            .collect()
    }

    #[test]
    fn declared_toolchain_pins_child_argv() {
        let policy = ToolchainPolicy {
            channel: "1.98.0".to_owned(),
        };
        assert_eq!(
            command_parts(&policy.command("cargo")),
            ["rustup", "run", "1.98.0", "cargo"]
        );
    }

    #[test]
    fn strict_command_parser_rejects_unknown_and_extra_arguments() {
        assert_eq!(
            parse_command(std::iter::empty::<&str>()),
            Ok(XtaskCommand::Help)
        );
        assert_eq!(parse_command(["check"]), Ok(XtaskCommand::Check));
        assert_eq!(parse_command(["clippy"]), Ok(XtaskCommand::Clippy));
        assert!(parse_command(["check", "extra"]).is_err());
    }

    #[test]
    fn workspace_discovery_is_independent_of_process_cwd() -> Result<(), String> {
        let temp = TempDir::new("workspace-root")?;
        write(
            &temp.path().join("Cargo.toml"),
            "[workspace]\nresolver = \"3\"\n",
        )?;
        let nested = temp.path().join("xtask/src");
        fs::create_dir_all(&nested)
            .map_err(|error| format!("failed to create {}: {error}", nested.display()))?;
        let expected = temp
            .path()
            .canonicalize()
            .map_err(|error| format!("failed to canonicalize test workspace: {error}"))?;
        assert_eq!(find_workspace_root(&nested)?, expected);
        Ok(())
    }

    #[test]
    fn runtime_manifest_dir_wins_when_a_cached_binary_runs_from_another_copy() -> Result<(), String>
    {
        let first = TempDir::new("cached-binary-first")?;
        let second = TempDir::new("cached-binary-second")?;
        for root in [first.path(), second.path()] {
            write(&root.join("Cargo.toml"), "[workspace]\nresolver = \"3\"\n")?;
        }
        let selected_manifest_dir = second.path().join("xtask");
        fs::create_dir_all(&selected_manifest_dir).map_err(|error| {
            format!(
                "failed to create selected manifest directory {}: {error}",
                selected_manifest_dir.display()
            )
        })?;
        let resolved = workspace_root_from(Some(selected_manifest_dir.as_os_str()), first.path())?;
        let expected = second
            .path()
            .canonicalize()
            .map_err(|error| format!("failed to canonicalize selected workspace: {error}"))?;
        assert_eq!(resolved, expected);
        Ok(())
    }

    #[test]
    fn malformed_and_duplicate_toml_fields_fail() -> Result<(), String> {
        let temp = TempDir::new("invalid-toml")?;
        let path = temp.path().join("Cargo.toml");
        write(&path, "[workspace]\nresolver = \"3\"\nresolver = \"2\"\n")?;
        assert!(read_toml(&path).is_err());
        write(&path, "[workspace\nresolver = \"3\"\n")?;
        assert!(read_toml(&path).is_err());
        Ok(())
    }

    #[test]
    fn wrong_section_and_commented_lints_do_not_satisfy_policy() -> Result<(), String> {
        let policy = HarnessPolicy {
            workspace_msrv: "1.98".to_owned(),
            public_policy: PublicPolicy {
                crate_name: "heterarchy-orchestration-policy".to_owned(),
                msrv: "1.92".to_owned(),
            },
            supply_chain: false,
        };
        let valid: toml::Value = toml::from_str(
            "[workspace]\nresolver=\"3\"\n[workspace.package]\nedition=\"2024\"\nrust-version=\"1.98\"\n[workspace.lints.rust]\nwarnings=\"deny\"\nunsafe_code=\"forbid\"\n[workspace.lints.clippy]\nall=\"deny\"\npedantic=\"warn\"\n",
        )
        .map_err(|error| error.to_string())?;
        verify_cargo_manifest(&valid, &policy)?;

        let wrong_section: toml::Value = toml::from_str(
            "[workspace]\nresolver=\"3\"\n[workspace.package]\nedition=\"2024\"\nrust-version=\"1.98\"\n[workspace.lints.rust]\n# warnings=\"deny\"\nunsafe_code=\"forbid\"\n[package.metadata]\nwarnings=\"deny\"\n[workspace.lints.clippy]\nall=\"deny\"\npedantic=\"warn\"\n",
        )
        .map_err(|error| error.to_string())?;
        let error = verify_cargo_manifest(&wrong_section, &policy)
            .err()
            .ok_or_else(|| "commented and wrong-section warnings unexpectedly passed".to_owned())?;
        assert!(error.contains("warnings"));
        Ok(())
    }

    #[test]
    fn public_policy_msrv_mismatch_is_rejected() {
        let policy = PublicPolicy {
            crate_name: "heterarchy-orchestration-policy".to_owned(),
            msrv: "1.92".to_owned(),
        };
        assert!(
            verify_public_policy_version("heterarchy-orchestration-policy", "1.93", &policy)
                .is_err()
        );
        assert!(verify_public_policy_version("wrong-crate", "1.92", &policy).is_err());
    }

    #[test]
    fn verification_argv_keeps_lock_and_cargo_lint_authority() {
        for args in [CHECK_ARGS, CLIPPY_ARGS, TEST_ARGS, DOC_TEST_ARGS] {
            assert!(args.contains(&"--locked"));
            assert!(!args.contains(&"-D"));
            assert!(!args.contains(&"warnings"));
        }
    }

    #[test]
    fn manifest_reference_rejects_wrong_value_and_missing_asset() -> Result<(), String> {
        let temp = TempDir::new("manifest-ref")?;
        let mut table = toml::value::Table::new();
        table.insert(
            "project_profile".to_owned(),
            toml::Value::String("wrong.md".to_owned()),
        );
        assert!(verify_ref(temp.path(), &table, "project_profile", "PROJECT_PROFILE.md").is_err());
        table.insert(
            "project_profile".to_owned(),
            toml::Value::String("PROJECT_PROFILE.md".to_owned()),
        );
        assert!(verify_ref(temp.path(), &table, "project_profile", "PROJECT_PROFILE.md").is_err());
        write(&temp.path().join("PROJECT_PROFILE.md"), "profile")?;
        verify_ref(temp.path(), &table, "project_profile", "PROJECT_PROFILE.md")
    }

    #[test]
    fn benchmark_detection_requires_real_target_kind() {
        let none = serde_json::json!({"packages":[{"targets":[{"kind":["lib"]}]}]});
        let bench = serde_json::json!({"packages":[{"targets":[{"kind":["bench"]}]}]});
        assert!(!has_benchmark_target(&none));
        assert!(has_benchmark_target(&bench));
    }
}

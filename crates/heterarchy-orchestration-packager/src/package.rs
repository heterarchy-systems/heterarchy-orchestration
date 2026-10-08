//! Local macOS headless Backend packaging.
//!
//! The product payload is the single independently runnable daemon
//! (`heterarchy-orchestration-backend`).
//! There is no GUI application bundle: the archive installs `bin/` and
//! `share/` payloads under a fixed prefix and ships a launchd service
//! template plus an env example for operator-managed lifecycle.

use std::collections::BTreeMap;
use std::env;
use std::ffi::OsString;
use std::fs::{self, File};
use std::io::{self, BufReader, Read};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use sha2::{Digest, Sha256};

use crate::process::{cargo_build, cargo_target_directory};

const BACKEND_PACKAGE: &str = "heterarchy-orchestration-backend";
const BACKEND_MANIFEST: &str = "crates/heterarchy-orchestration-backend/Cargo.toml";
const PRODUCT_NAME: &str = "Heterarchy Orchestration Backend";
const PRODUCT_ARTIFACT_STEM: &str = "Heterarchy-Orchestration-Backend";
const PRODUCT_IDENTIFIER: &str = "systems.heterarchy.orchestration.backend";
const INSTALL_PREFIX: &str = "/usr/local";
const DAEMON_DATA_ROOT: &str = "/var/lib/heterarchy-orchestration";
const LAUNCHD_TEMPLATE: &str = "packaging/macos/backend.launchd.plist.in";
const LAUNCHD_FILE_NAME: &str = "systems.heterarchy.orchestration.backend.plist";
const ENV_EXAMPLE_SOURCE: &str = "packaging/macos/backend.env.example";
const README_SOURCE: &str = "packaging/macos/README.md";
const PREFIX_TOKEN: &str = "@@PREFIX@@";
const DATA_ROOT_TOKEN: &str = "@@DATA_ROOT@@";
const MAX_METADATA_BYTES: u64 = 64 * 1024;
const MAX_WALK_ENTRIES: usize = 4_096;
const MAX_WALK_DEPTH: usize = 16;
/// Private execution-runtime override for every package-generated path.
///
/// This is intentionally an environment contract between the Orchestration
/// launcher and the tracked Orchestration packager. The
/// launcher creates and identity-checks the directory before invoking xtask,
/// while this boundary rejects a missing, relative, or symlinked root before
/// any package cleanup is attempted.
const PACKAGE_OUTPUT_ROOT_ENV: &str = "ORCHESTRATION_PACKAGE_OUTPUT_ROOT";

#[derive(Debug)]
struct PackageSpec {
    version: String,
    architecture: &'static str,
    app_signing_identity: Option<OsString>,
    installer_signing_identity: Option<OsString>,
}

impl PackageSpec {
    fn load(root: &Path) -> Result<Self, String> {
        let manifest = read_bounded_text(&root.join(BACKEND_MANIFEST), MAX_METADATA_BYTES)?;
        let parsed = toml::from_str::<toml::Value>(&manifest)
            .map_err(|error| format!("Backend Cargo.toml is invalid: {error}"))?;
        let version = parsed
            .get("package")
            .and_then(|value| value.get("version"))
            .and_then(toml::Value::as_str)
            .ok_or_else(|| "Backend package version is missing".to_owned())?;
        validate_package_version(version)?;
        validate_product_identifier(PRODUCT_IDENTIFIER)?;
        // An Orchestration package output is a local generated artifact. The
        // sandboxed launch deliberately clears credential-bearing signing
        // environment, and this explicit branch keeps a caller-provided
        // signing identity from changing that policy.
        let generated_output = env::var_os(PACKAGE_OUTPUT_ROOT_ENV).is_some();
        Ok(Self {
            version: version.to_owned(),
            architecture: architecture_label()?,
            app_signing_identity: if generated_output {
                None
            } else {
                signing_identity("ORCHESTRATION_APP_SIGNING_IDENTITY")?
            },
            installer_signing_identity: if generated_output {
                None
            } else {
                signing_identity("ORCHESTRATION_INSTALLER_SIGNING_IDENTITY")?
            },
        })
    }

    fn versioned_package_name(&self) -> String {
        format!(
            "{PRODUCT_ARTIFACT_STEM}-{}-{}.pkg",
            self.version, self.architecture
        )
    }

    fn stable_package_name() -> String {
        format!("{PRODUCT_ARTIFACT_STEM}.pkg")
    }

    fn installer_identifier() -> String {
        format!("{PRODUCT_IDENTIFIER}.installer")
    }
}

#[derive(Debug)]
struct PackageLayout {
    generated_output: bool,
    target_directory: PathBuf,
    build_root: PathBuf,
    staging_root: PathBuf,
    bin: PathBuf,
    share: PathBuf,
    backend_executable: PathBuf,
    package_work_root: PathBuf,
    expanded: PathBuf,
    dist: PathBuf,
    component_package: PathBuf,
    package: PathBuf,
    latest_package: PathBuf,
}

impl PackageLayout {
    fn new(root: &Path, spec: &PackageSpec, target_directory: PathBuf) -> Result<Self, String> {
        let output_root = package_output_root()?;
        let generated_output = output_root.is_some();
        let run_id = format!(
            "{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|error| error.to_string())?
                .as_nanos()
        );
        let (build_root, package_work_root, dist) = match output_root {
            Some(output_root) => (
                output_root.join("build"),
                output_root.join("work"),
                output_root.join("dist"),
            ),
            None => (
                target_directory.join("package").join(&run_id),
                env::temp_dir().join(format!("heterarchy-orchestration-package-{run_id}")),
                root.join("dist").join(&run_id),
            ),
        };
        let staging_root = package_work_root.join("payload");
        let bin = staging_root.join("bin");
        let share = staging_root.join("share").join("heterarchy-orchestration");
        Ok(Self {
            generated_output,
            target_directory,
            backend_executable: bin.join(BACKEND_PACKAGE),
            expanded: package_work_root.join("expanded"),
            package: dist.join(spec.versioned_package_name()),
            latest_package: dist.join(PackageSpec::stable_package_name()),
            component_package: package_work_root.join(format!("{PRODUCT_IDENTIFIER}.pkg")),
            package_work_root,
            staging_root,
            bin,
            share,
            build_root,
            dist,
        })
    }

    fn prepare(&self) -> Result<(), String> {
        // Only paths exclusively created by this invocation may be cleaned up.
        // Refuse existing directories, including symlinks, without deleting them.
        reserve_directory(&self.build_root)?;
        reserve_directory(&self.package_work_root)?;
        reserve_directory(&self.dist)?;
        fs::create_dir_all(&self.bin).map_err(path_error("create payload bin directory"))?;
        fs::create_dir_all(self.share.join("launchd"))
            .map_err(path_error("create payload launchd directory"))?;
        fs::create_dir_all(self.share.join("config"))
            .map_err(path_error("create payload config directory"))?;
        fs::create_dir_all(&self.dist).map_err(path_error("create distribution directory"))?;
        Ok(())
    }
}

pub(super) fn run(root: &Path) -> Result<(), String> {
    if !cfg!(target_os = "macos") {
        return Err("Orchestration packaging is supported only on macOS".to_owned());
    }
    let spec = PackageSpec::load(root)?;
    let target_directory = cargo_target_directory(root)?;
    let layout = PackageLayout::new(root, &spec, target_directory)?;
    verify_required_inputs(root)?;
    cargo_build(root, BACKEND_PACKAGE, &layout.target_directory, &[])?;
    layout.prepare()?;
    assemble_payload(root, &layout)?;
    clear_extended_attributes(&layout.staging_root, &layout.build_root)?;
    sign_payload(&spec, &layout)?;
    verify_payload(&layout)?;
    write_payload_manifest(&layout)?;
    build_product_archive(&spec, &layout)?;
    verify_product_archive(&layout)?;
    fs::copy(&layout.package, &layout.latest_package)
        .map_err(path_error("copy stable package artifact"))?;
    remove_path_if_present(&layout.package_work_root)?;
    remove_path_if_present(&layout.build_root)?;
    if layout.generated_output {
        // The execution output contract retains only the stable artifact.
        // Disposable payload staging and the duplicate versioned archive are
        // removed before the runner records a generated-package manifest.
        remove_path_if_present(&layout.package)?;
    }
    println!("{PRODUCT_NAME} headless local package created:");
    if !layout.generated_output {
        println!("  pkg: {}", layout.package.display());
    }
    println!("  stable pkg: {}", layout.latest_package.display());
    if spec.installer_signing_identity.is_none() {
        println!("  installer signature: unsigned local-development package");
    }
    Ok(())
}

fn verify_required_inputs(root: &Path) -> Result<(), String> {
    for source in [
        root.join(LAUNCHD_TEMPLATE),
        root.join(ENV_EXAMPLE_SOURCE),
        root.join(README_SOURCE),
    ] {
        require_regular_nonempty_file(&source, "headless package source asset")?;
    }
    Ok(())
}

fn package_output_root() -> Result<Option<PathBuf>, String> {
    package_output_root_from(env::var_os(PACKAGE_OUTPUT_ROOT_ENV))
}

fn package_output_root_from(value: Option<OsString>) -> Result<Option<PathBuf>, String> {
    let Some(value) = value else {
        return Ok(None);
    };
    if value.is_empty() {
        return Err(format!("{PACKAGE_OUTPUT_ROOT_ENV} must not be empty"));
    }
    let path = PathBuf::from(value);
    if !path.is_absolute() {
        return Err(format!("{PACKAGE_OUTPUT_ROOT_ENV} must be absolute"));
    }
    let metadata = fs::symlink_metadata(&path)
        .map_err(|error| format!("inspect {PACKAGE_OUTPUT_ROOT_ENV}: {error}"))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(format!(
            "{PACKAGE_OUTPUT_ROOT_ENV} must name an existing non-symlink directory"
        ));
    }
    let canonical = fs::canonicalize(&path)
        .map_err(|error| format!("canonicalize {PACKAGE_OUTPUT_ROOT_ENV}: {error}"))?;
    if canonical != path {
        return Err(format!(
            "{PACKAGE_OUTPUT_ROOT_ENV} must already be canonical"
        ));
    }
    Ok(Some(path))
}

fn assemble_payload(root: &Path, layout: &PackageLayout) -> Result<(), String> {
    let release_binary = layout
        .target_directory
        .clone()
        .join("release")
        .join(BACKEND_PACKAGE);
    require_regular_nonempty_file(&release_binary, "release backend product binary")?;
    fs::copy(&release_binary, &layout.backend_executable)
        .map_err(path_error("copy release product binary into payload"))?;
    make_executable(&layout.backend_executable, "bundled product executable")?;

    let template = read_bounded_text(&root.join(LAUNCHD_TEMPLATE), MAX_METADATA_BYTES)?;
    let rendered = render_launchd_plist(&template)?;
    fs::write(
        layout.share.join("launchd").join(LAUNCHD_FILE_NAME),
        rendered,
    )
    .map_err(path_error("write launchd service template"))?;

    fs::copy(
        root.join(ENV_EXAMPLE_SOURCE),
        layout.share.join("config").join("backend.env.example"),
    )
    .map_err(path_error("copy backend env example"))?;
    fs::copy(root.join(README_SOURCE), layout.share.join("README.md"))
        .map_err(path_error("copy headless README"))?;
    Ok(())
}

fn render_launchd_plist(template: &str) -> Result<String, String> {
    let prefix_occurrences = template.match_indices(PREFIX_TOKEN).count();
    if prefix_occurrences != 1 {
        return Err(format!(
            "launchd template must contain exactly one prefix token, found {prefix_occurrences}"
        ));
    }
    let data_root_occurrences = template.match_indices(DATA_ROOT_TOKEN).count();
    if data_root_occurrences < 2 {
        return Err(format!(
            "launchd template must contain the data-root token at least twice, found {data_root_occurrences}"
        ));
    }
    let rendered = template
        .replace(PREFIX_TOKEN, INSTALL_PREFIX)
        .replace(DATA_ROOT_TOKEN, DAEMON_DATA_ROOT);
    if rendered.contains("@@") {
        return Err("launchd template contains an unresolved token".to_owned());
    }
    Ok(rendered)
}

fn make_executable(path: &Path, label: &'static str) -> Result<(), String> {
    let mut permissions = fs::metadata(path)
        .map_err(path_error("read bundled executable metadata"))?
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions).map_err(path_error(label))
}

fn sign_payload(spec: &PackageSpec, layout: &PackageLayout) -> Result<(), String> {
    sign_macho(
        spec,
        &layout.backend_executable,
        PRODUCT_IDENTIFIER,
        &layout.build_root,
    )
}

fn clear_extended_attributes(path: &Path, current_dir: &Path) -> Result<(), String> {
    run_command(
        "/usr/bin/xattr",
        &["-c".into(), "-r".into(), path.as_os_str().to_owned()],
        current_dir,
    )
}

fn sign_macho(
    spec: &PackageSpec,
    path: &Path,
    identifier: &str,
    current_dir: &Path,
) -> Result<(), String> {
    let identity = spec
        .app_signing_identity
        .clone()
        .unwrap_or_else(|| OsString::from("-"));
    let mut arguments = vec![
        OsString::from("--force"),
        OsString::from("--sign"),
        identity,
        OsString::from("--identifier"),
        OsString::from(identifier),
    ];
    if spec.app_signing_identity.is_some() {
        arguments.extend([
            OsString::from("--options"),
            OsString::from("runtime"),
            OsString::from("--timestamp"),
        ]);
    } else {
        arguments.push(OsString::from("--timestamp=none"));
    }
    arguments.push(path.as_os_str().to_owned());
    run_command("/usr/bin/codesign", &arguments, current_dir)
}

fn verify_payload(layout: &PackageLayout) -> Result<(), String> {
    run_command(
        "/usr/bin/plutil",
        &[
            "-lint".into(),
            layout
                .share
                .join("launchd")
                .join(LAUNCHD_FILE_NAME)
                .into_os_string(),
        ],
        &layout.build_root,
    )?;
    {
        let path = &layout.backend_executable;
        run_command(
            "/usr/bin/codesign",
            &[
                "--verify".into(),
                "--strict".into(),
                "--verbose=2".into(),
                path.as_os_str().to_owned(),
            ],
            &layout.build_root,
        )?;
        run_command(
            "/usr/bin/file",
            &[path.as_os_str().to_owned()],
            &layout.build_root,
        )?;
        run_command(
            "/usr/bin/lipo",
            &["-archs".into(), path.as_os_str().to_owned()],
            &layout.build_root,
        )?;
        run_command(
            "/usr/bin/otool",
            &["-L".into(), path.as_os_str().to_owned()],
            &layout.build_root,
        )?;
    }
    Ok(())
}

/// Hash every staged payload file into a `SHA256SUMS` manifest at the payload
/// root. Paths are staging-relative so the manifest also validates the
/// expanded archive layout.
fn write_payload_manifest(layout: &PackageLayout) -> Result<(), String> {
    let files = collect_payload_files(&layout.staging_root)?;
    let mut manifest = String::new();
    for relative in &files {
        let digest = sha256_file(&layout.staging_root.join(relative))?;
        let hex = digest
            .iter()
            .fold(String::with_capacity(64), |mut hex, byte| {
                use std::fmt::Write as _;
                let _ = write!(hex, "{byte:02x}");
                hex
            });
        manifest.push_str(&hex);
        manifest.push_str("  ");
        manifest.push_str(&relative.to_string_lossy());
        manifest.push('\n');
    }
    fs::write(layout.staging_root.join("SHA256SUMS"), manifest)
        .map_err(path_error("write payload SHA256SUMS manifest"))
}

/// Compare the expanded package payload against the staged manifest: every
/// listed file must exist with an identical digest and no unlisted file may
/// exist inside the expanded payload.
fn verify_expanded_payload(layout: &PackageLayout, expanded_payload: &Path) -> Result<(), String> {
    let manifest_path = layout.staging_root.join("SHA256SUMS");
    let manifest = fs::read_to_string(&manifest_path)
        .map_err(path_error("read staged SHA256SUMS manifest"))?;
    let mut listed: BTreeMap<String, [u8; 32]> = BTreeMap::new();
    for line in manifest.lines() {
        let (hex, relative) = line
            .split_once("  ")
            .ok_or_else(|| "SHA256SUMS manifest line is malformed".to_owned())?;
        if hex.len() != 64 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err("SHA256SUMS manifest digest is malformed".to_owned());
        }
        let mut digest = [0_u8; 32];
        for (index, chunk) in hex.as_bytes().chunks(2).enumerate() {
            let byte = std::str::from_utf8(chunk)
                .ok()
                .and_then(|pair| u8::from_str_radix(pair, 16).ok())
                .ok_or_else(|| "SHA256SUMS manifest digest is not hexadecimal".to_owned())?;
            digest[index] = byte;
        }
        if relative.contains("..") || Path::new(relative).is_absolute() {
            return Err("SHA256SUMS manifest path escapes the payload root".to_owned());
        }
        if listed.insert(relative.to_owned(), digest).is_some() {
            return Err(format!("SHA256SUMS manifest lists {relative:?} twice"));
        }
    }
    let expanded_files = collect_payload_files(expanded_payload)?;
    if expanded_files.len() != listed.len() + 1 {
        // The manifest itself is part of the expanded payload but not of the
        // pre-manifest staging listing, so exactly one extra entry is allowed.
        return Err(format!(
            "expanded payload holds {} files but the manifest lists {} plus itself",
            expanded_files.len(),
            listed.len()
        ));
    }
    for relative in &expanded_files {
        if relative == Path::new("SHA256SUMS") {
            continue;
        }
        let key = relative.to_string_lossy();
        let expected = listed.get(key.as_ref()).ok_or_else(|| {
            format!(
                "expanded payload contains unlisted file {}",
                relative.display()
            )
        })?;
        let actual = sha256_file(&expanded_payload.join(relative))?;
        if actual != *expected {
            return Err(format!(
                "expanded payload file {} differs from the signed payload",
                relative.display()
            ));
        }
    }
    Ok(())
}

/// Bounded listing of every regular file under `root`, returned relative to
/// `root`. Symlinks are rejected outright: the payload must be a plain tree.
fn collect_payload_files(root: &Path) -> Result<Vec<PathBuf>, String> {
    let mut files = Vec::new();
    let mut entries = 0_usize;
    collect_payload_files_inner(root, Path::new(""), 0, &mut entries, &mut files)?;
    files.sort();
    Ok(files)
}

fn collect_payload_files_inner(
    root: &Path,
    relative: &Path,
    depth: usize,
    entries: &mut usize,
    files: &mut Vec<PathBuf>,
) -> Result<(), String> {
    if depth > MAX_WALK_DEPTH {
        return Err(format!(
            "package payload exceeded walk depth {MAX_WALK_DEPTH}"
        ));
    }
    for entry in fs::read_dir(root).map_err(path_error("read package payload directory"))? {
        let entry = entry.map_err(path_error("read package payload entry"))?;
        *entries = entries
            .checked_add(1)
            .ok_or_else(|| "package payload entry count overflowed".to_owned())?;
        if *entries > MAX_WALK_ENTRIES {
            return Err(format!(
                "package payload exceeded {MAX_WALK_ENTRIES} entries"
            ));
        }
        let child_relative = relative.join(entry.file_name());
        let file_type = entry
            .file_type()
            .map_err(path_error("read package payload entry type"))?;
        if file_type.is_symlink() {
            return Err(format!(
                "package payload must not contain symlinks: {}",
                child_relative.display()
            ));
        }
        if file_type.is_dir() {
            collect_payload_files_inner(&entry.path(), &child_relative, depth + 1, entries, files)?;
        } else if file_type.is_file() {
            files.push(child_relative);
        }
    }
    Ok(())
}

fn build_product_archive(spec: &PackageSpec, layout: &PackageLayout) -> Result<(), String> {
    run_command(
        "/usr/bin/pkgbuild",
        &[
            "--root".into(),
            layout.staging_root.clone().into_os_string(),
            "--identifier".into(),
            OsString::from(PRODUCT_IDENTIFIER),
            "--version".into(),
            spec.version.clone().into(),
            "--install-location".into(),
            OsString::from(INSTALL_PREFIX),
            "--ownership".into(),
            "recommended".into(),
            layout.component_package.clone().into_os_string(),
        ],
        &layout.package_work_root,
    )?;
    require_regular_nonempty_file(&layout.component_package, "Installer component package")?;
    let mut arguments = vec![
        OsString::from("--package"),
        layout.component_package.clone().into_os_string(),
        OsString::from("--identifier"),
        OsString::from(PackageSpec::installer_identifier()),
        OsString::from("--version"),
        OsString::from(&spec.version),
    ];
    if let Some(identity) = &spec.installer_signing_identity {
        arguments.extend([
            OsString::from("--sign"),
            identity.clone(),
            OsString::from("--timestamp"),
        ]);
    }
    arguments.push(layout.package.clone().into_os_string());
    run_command(
        "/usr/bin/productbuild",
        &arguments,
        &layout.package_work_root,
    )?;
    require_regular_nonempty_file(&layout.package, "Installer product archive")
}

fn verify_product_archive(layout: &PackageLayout) -> Result<(), String> {
    remove_path_if_present(&layout.expanded)?;
    run_command(
        "/usr/sbin/pkgutil",
        &[
            "--expand-full".into(),
            layout.package.clone().into_os_string(),
            layout.expanded.clone().into_os_string(),
        ],
        &layout.package_work_root,
    )?;
    require_regular_nonempty_file(
        &layout.expanded.join("Distribution"),
        "expanded product Distribution definition",
    )?;
    let payload = find_unique_directory(&layout.expanded, "Payload")?;
    require_regular_nonempty_file(
        &payload.join("bin").join(BACKEND_PACKAGE),
        "expanded backend executable",
    )?;
    require_regular_nonempty_file(
        &payload
            .join("share")
            .join("heterarchy-orchestration")
            .join("launchd")
            .join(LAUNCHD_FILE_NAME),
        "expanded launchd service template",
    )?;
    run_command(
        "/usr/bin/codesign",
        &[
            "--verify".into(),
            "--strict".into(),
            "--verbose=2".into(),
            payload.join("bin").join(BACKEND_PACKAGE).into_os_string(),
        ],
        &layout.build_root,
    )?;
    verify_expanded_payload(layout, &payload)?;
    run_command(
        "/usr/sbin/installer",
        &[
            "-showChoicesXML".into(),
            "-pkg".into(),
            layout.package.clone().into_os_string(),
            "-target".into(),
            OsString::from("/"),
        ],
        &layout.build_root,
    )?;
    Ok(())
}

/// Find exactly one directory with `name` anywhere in the expanded archive
/// within the bounded walk. Ambiguity or absence is a verification failure.
fn find_unique_directory(root: &Path, name: &str) -> Result<PathBuf, String> {
    let mut matches = Vec::new();
    let mut entries = 0_usize;
    find_named_directories(root, name, 0, &mut entries, &mut matches)?;
    match matches.len() {
        1 => Ok(matches.remove(0)),
        count => Err(format!(
            "expanded package contained {count} {name:?} entries, expected exactly one"
        )),
    }
}

fn find_named_directories(
    root: &Path,
    name: &str,
    depth: usize,
    entries: &mut usize,
    matches: &mut Vec<PathBuf>,
) -> Result<(), String> {
    if depth > MAX_WALK_DEPTH {
        return Err(format!(
            "expanded package exceeded walk depth {MAX_WALK_DEPTH}"
        ));
    }
    for entry in fs::read_dir(root).map_err(path_error("read expanded package directory"))? {
        let entry = entry.map_err(path_error("read expanded package entry"))?;
        *entries = entries
            .checked_add(1)
            .ok_or_else(|| "expanded package entry count overflowed".to_owned())?;
        if *entries > MAX_WALK_ENTRIES {
            return Err(format!(
                "expanded package exceeded {MAX_WALK_ENTRIES} entries"
            ));
        }
        let file_type = entry
            .file_type()
            .map_err(path_error("read expanded package entry type"))?;
        if file_type.is_symlink() || !file_type.is_dir() {
            continue;
        }
        if entry.file_name() == name {
            matches.push(entry.path());
        } else {
            find_named_directories(&entry.path(), name, depth + 1, entries, matches)?;
        }
    }
    Ok(())
}

fn validate_package_version(version: &str) -> Result<(), String> {
    if version.is_empty()
        || version.len() > 64
        || !version
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'+'))
    {
        return Err(format!("package version {version:?} is not canonical"));
    }
    Ok(())
}

fn validate_product_identifier(identifier: &str) -> Result<(), String> {
    if identifier.is_empty()
        || identifier.len() > 128
        || identifier.starts_with('.')
        || identifier.ends_with('.')
        || !identifier.contains('.')
        || !identifier.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'-')
        })
    {
        return Err(format!(
            "product identifier {identifier:?} is not package-safe"
        ));
    }
    Ok(())
}

fn architecture_label() -> Result<&'static str, String> {
    match env::consts::ARCH {
        "aarch64" => Ok("arm64"),
        "x86_64" => Ok("x86_64"),
        other => Err(format!("unsupported macOS package architecture {other:?}")),
    }
}

fn signing_identity(name: &str) -> Result<Option<OsString>, String> {
    let Some(value) = env::var_os(name) else {
        return Ok(None);
    };
    if value.is_empty() || value.as_encoded_bytes().len() > 256 {
        return Err(format!("{name} is empty or oversized"));
    }
    if value.as_encoded_bytes().iter().any(u8::is_ascii_control) {
        return Err(format!("{name} contains a control character"));
    }
    Ok(Some(value))
}

fn read_bounded_text(path: &Path, maximum: u64) -> Result<String, String> {
    let metadata = fs::metadata(path).map_err(path_error("read metadata"))?;
    if !metadata.is_file() || metadata.len() > maximum {
        return Err(format!(
            "{} is not a regular file within {maximum} bytes",
            path.display()
        ));
    }
    fs::read_to_string(path).map_err(path_error("read UTF-8 text"))
}

fn sha256_file(path: &Path) -> Result<[u8; 32], String> {
    let file = File::open(path).map_err(path_error("open file for hashing"))?;
    let mut reader = BufReader::new(file);
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; 64 * 1024].into_boxed_slice();
    loop {
        let read = reader
            .read(&mut buffer)
            .map_err(path_error("read file for hashing"))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hasher.finalize().into())
}

fn require_regular_nonempty_file(path: &Path, label: &str) -> Result<(), String> {
    let metadata = fs::symlink_metadata(path).map_err(path_error("read artifact metadata"))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() || metadata.len() == 0 {
        return Err(format!("{label} {} is missing or invalid", path.display()));
    }
    Ok(())
}

fn reserve_directory(path: &Path) -> Result<(), String> {
    let parent = path
        .parent()
        .ok_or_else(|| "package directory has no parent".to_owned())?;
    fs::create_dir_all(parent)
        .map_err(|error| format!("create package parent {}: {error}", parent.display()))?;
    fs::create_dir(path).map_err(|error| {
        format!(
            "reserve new package directory {} (existing paths are preserved): {error}",
            path.display()
        )
    })
}

fn remove_path_if_present(path: &Path) -> Result<(), String> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {
            fs::remove_dir_all(path).map_err(path_error("remove previous package directory"))
        }
        Ok(_) => fs::remove_file(path).map_err(path_error("remove previous package artifact")),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(format!(
            "inspect {} before removal: {error}",
            path.display()
        )),
    }
}

fn run_command(program: &str, arguments: &[OsString], cwd: &Path) -> Result<(), String> {
    crate::process::run_command(
        program,
        arguments,
        cwd,
        crate::process::PACKAGING_COMMAND_TIMEOUT,
    )
}

fn path_error(action: &'static str) -> impl FnOnce(io::Error) -> String {
    move |error| format!("{action}: {error}")
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    static NEXT_EXECUTABLE_FIXTURE: AtomicU64 = AtomicU64::new(1);

    #[test]
    fn existing_output_directory_and_symlink_are_preserved()
    -> Result<(), Box<dyn std::error::Error>> {
        let serial = NEXT_EXECUTABLE_FIXTURE.fetch_add(1, Ordering::Relaxed);
        let root = env::temp_dir().join(format!(
            "orchestration-reserve-{}-{serial}",
            std::process::id()
        ));
        fs::create_dir(&root)?;
        fs::write(root.join("keep"), b"unmanaged data")?;
        assert!(reserve_directory(&root).is_err());
        assert_eq!(fs::read(root.join("keep"))?, b"unmanaged data");
        let link = root.join("link");
        std::os::unix::fs::symlink(&root, &link)?;
        assert!(reserve_directory(&link).is_err());
        assert!(fs::symlink_metadata(&link)?.file_type().is_symlink());
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn launchd_template_renders_with_the_headless_daemon_contract() -> Result<(), String> {
        let root = crate::workspace_root()?;
        let template = read_bounded_text(&root.join(LAUNCHD_TEMPLATE), MAX_METADATA_BYTES)?;
        let rendered = render_launchd_plist(&template)?;
        assert!(rendered.contains(&format!(
            "<string>{INSTALL_PREFIX}/bin/{BACKEND_PACKAGE}</string>"
        )));
        assert!(rendered.contains(&format!("<string>{DAEMON_DATA_ROOT}</string>")));
        assert!(rendered.contains("<string>systems.heterarchy.orchestration.backend</string>"));
        assert!(!rendered.contains("@@"));
        Ok(())
    }

    #[test]
    fn env_example_documents_the_daemon_environment_contract() -> Result<(), String> {
        let root = crate::workspace_root()?;
        let example = read_bounded_text(&root.join(ENV_EXAMPLE_SOURCE), MAX_METADATA_BYTES)?;
        for required in [
            "HETERARCHY_ORCHESTRATION_DATA_DIR=",
            "HETERARCHY_ORCHESTRATION_BACKEND_ADDR=",
            "HETERARCHY_ORCHESTRATION_TOKEN_BOOTSTRAP=",
            "HETERARCHY_ORCHESTRATION_OPERATOR_PRINCIPALS=",
            "HETERARCHY_ORCHESTRATION_FORGE_SOURCE_CONFIG=",
        ] {
            assert!(
                example.contains(required),
                "env example is missing {required}"
            );
        }
        assert!(!example.contains("HETERARCHY_ORCHESTRATION_OAUTH_RESOURCE="));
        Ok(())
    }

    #[test]
    fn version_and_artifact_name_are_bounded_and_arch_specific() -> Result<(), String> {
        validate_package_version("0.1.0")?;
        assert!(validate_package_version("").is_err());
        assert!(validate_package_version("1.0</string>").is_err());
        validate_product_identifier(PRODUCT_IDENTIFIER)?;
        assert!(validate_product_identifier("systems.heterarchy.<bad>").is_err());
        let spec = PackageSpec {
            version: "0.1.0".to_owned(),
            architecture: "arm64",
            app_signing_identity: None,
            installer_signing_identity: None,
        };
        assert_eq!(
            spec.versioned_package_name(),
            "Heterarchy-Orchestration-Backend-0.1.0-arm64.pkg"
        );
        assert_eq!(
            PackageSpec::installer_identifier(),
            "systems.heterarchy.orchestration.backend.installer"
        );
        Ok(())
    }

    #[test]
    fn package_identity_comes_from_the_backend_manifest() -> Result<(), String> {
        let spec = PackageSpec::load(&crate::workspace_root()?)?;
        assert_eq!(spec.version, "0.1.0");
        Ok(())
    }

    #[test]
    fn generated_output_root_is_absolute_canonical_and_not_a_symlink()
    -> Result<(), Box<dyn std::error::Error>> {
        let serial = NEXT_EXECUTABLE_FIXTURE.fetch_add(1, Ordering::Relaxed);
        let root = env::temp_dir().join(format!(
            "heterarchy-orchestration-package-output-{}-{serial}",
            std::process::id()
        ));
        fs::create_dir_all(&root)?;
        let canonical = fs::canonicalize(&root)?;
        assert_eq!(
            package_output_root_from(Some(canonical.clone().into_os_string()))?,
            Some(canonical.clone())
        );
        assert!(package_output_root_from(Some(OsString::from("relative-output"))).is_err());
        let link = root
            .parent()
            .ok_or("temporary root has no parent")?
            .join(format!(
                "heterarchy-orchestration-package-output-link-{}-{serial}",
                std::process::id()
            ));
        std::os::unix::fs::symlink(&canonical, &link)?;
        assert!(package_output_root_from(Some(link.clone().into_os_string())).is_err());
        fs::remove_file(link)?;
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn payload_manifest_covers_the_staged_tree_and_rejects_drift()
    -> Result<(), Box<dyn std::error::Error>> {
        let serial = NEXT_EXECUTABLE_FIXTURE.fetch_add(1, Ordering::Relaxed);
        let root = env::temp_dir().join(format!(
            "orchestration-manifest-{}-{serial}",
            std::process::id()
        ));
        let staging = root.join("payload");
        let bin = staging.join("bin");
        let share = staging.join("share").join("heterarchy-orchestration");
        fs::create_dir_all(&bin)?;
        fs::create_dir_all(&share)?;
        fs::write(bin.join("daemon"), b"daemon fixture")?;
        fs::write(share.join("README.md"), b"readme fixture")?;
        let layout = PackageLayout {
            generated_output: false,
            target_directory: root.clone(),
            build_root: root.join("build"),
            staging_root: staging.clone(),
            bin: bin.clone(),
            share: share.clone(),
            backend_executable: bin.join("daemon"),
            package_work_root: root.join("work"),
            expanded: root.join("expanded"),
            dist: root.join("dist"),
            component_package: root.join("component.pkg"),
            package: root.join("product.pkg"),
            latest_package: root.join("stable.pkg"),
        };
        write_payload_manifest(&layout)?;
        let expanded = root.join("expanded-payload");
        fs::create_dir_all(expanded.join("bin"))?;
        fs::create_dir_all(expanded.join("share").join("heterarchy-orchestration"))?;
        fs::copy(bin.join("daemon"), expanded.join("bin").join("daemon"))?;
        fs::copy(
            share.join("README.md"),
            expanded
                .join("share")
                .join("heterarchy-orchestration")
                .join("README.md"),
        )?;
        fs::copy(staging.join("SHA256SUMS"), expanded.join("SHA256SUMS"))?;
        verify_expanded_payload(&layout, &expanded)?;
        fs::write(expanded.join("bin").join("daemon"), b"tampered")?;
        assert!(verify_expanded_payload(&layout, &expanded).is_err());
        fs::write(bin.join("daemon"), b"daemon fixture")?;
        fs::copy(bin.join("daemon"), expanded.join("bin").join("daemon"))?;
        fs::write(expanded.join("unlisted"), b"extra")?;
        assert!(verify_expanded_payload(&layout, &expanded).is_err());
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn payload_walk_rejects_symlinks() -> Result<(), Box<dyn std::error::Error>> {
        let serial = NEXT_EXECUTABLE_FIXTURE.fetch_add(1, Ordering::Relaxed);
        let root = env::temp_dir().join(format!(
            "orchestration-payload-link-{}-{serial}",
            std::process::id()
        ));
        let bin = root.join("bin");
        fs::create_dir_all(&bin)?;
        std::os::unix::fs::symlink("/etc/hosts", bin.join("escape"))?;
        assert!(collect_payload_files(&root).is_err());
        fs::remove_dir_all(root)?;
        Ok(())
    }
}

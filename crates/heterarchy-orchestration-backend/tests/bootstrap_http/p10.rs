//! P10 genuine Rust-CI verification: a separate opt-in path.
//! The host capability is always the product's validated paired SSD storage,
//! never a synthetic Rustup tree, mutable source receipt or test fake.
use super::*;
use forge_control::{FileActivityScope, ProjectCommandId, ProjectCommandListRequest};
use forge_domain::{ProjectCommandClass, ProjectCommandEvidence};
use forge_execution::{
    RustCiDiscoveryInput, RustCiHostArchitecture, RustCiStorageSelection, RustCiToolchainCapability,
};

pub(super) fn prepare_host_capability(
    boot_instance: RuntimeInstanceId,
    root: &Path,
) -> TestResult<RustCiToolchainCapability> {
    let home =
        std::fs::canonicalize(std::env::var_os("HOME").ok_or("P10 missing real host HOME")?)?;
    let arch = RustCiHostArchitecture::current().ok_or("P10 unsupported Rust-CI architecture")?;
    let input = RustCiDiscoveryInput::new(home, root.join("product-cache"), arch, boot_instance)?;
    let prepared = RustCiToolchainCapability::prepare_host_storage(&input)?;
    assert!(
        matches!(prepared.selection(), RustCiStorageSelection::Ssd { .. }),
        "P10 must not fall back to an unapproved host-local Rustup/Cargo view"
    );
    let capability = prepared.into_capability();
    capability.revalidate_current()?;
    assert_eq!(capability.pinned_toolchain(), "1.98.0-aarch64-apple-darwin");
    Ok(capability)
}

pub(super) fn write_sealed_makefile(root: &Path) -> TestResult {
    // Only this new isolated fixture Workspace may receive a verification
    // target. Its immutable snapshot is published later by Forge itself.
    std::fs::write(
        root.join("source/workspace/Makefile"),
        "doctor:\n\t@echo source-e2e-approved\n",
    )?;
    Ok(())
}

pub(super) fn finish_isolated_fixture(
    root: &Path,
    immutable_verification: bool,
    original: &TestResult,
) -> TestResult {
    if immutable_verification {
        // Preserve the real source-published immutable input rather than
        // bypassing its protection by changing permissions or force deleting.
        return Ok(());
    }
    let expected_prefix = "cooperation-bootstrap-http-";
    if !root
        .file_name()
        .and_then(|value| value.to_str())
        .is_some_and(|name| name.starts_with(expected_prefix))
        || root.parent() != Some(std::env::temp_dir().as_path())
    {
        return Err("refusing to remove an unexpected test fixture path".into());
    }
    match std::fs::remove_dir_all(root) {
        Ok(()) => Ok(()),
        Err(cleanup_error) if original.is_err() => {
            eprintln!("isolated fixture cleanup also failed: {cleanup_error:?}");
            Ok(())
        }
        Err(cleanup_error) => {
            Err(format!("isolated E2E root cleanup failed: {cleanup_error:?}").into())
        }
    }
}

pub(super) async fn start_execution_coordinator(
    control: ForgeControlClient,
    root: &Path,
    boot_instance: RuntimeInstanceId,
    verified_ci: bool,
) -> TestResult<ForgeExecutionCoordinator> {
    if !verified_ci {
        return Ok(ForgeExecutionCoordinator::start(control, 2)?);
    }
    let root = root.to_path_buf();
    let capability = tokio::task::spawn_blocking(move || {
        prepare_host_capability(boot_instance, &root).map_err(|error| error.to_string())
    })
    .await??;
    Ok(ForgeExecutionCoordinator::start_with_rust_ci_capability(
        control,
        2,
        Some(capability),
    )?)
}

pub(super) async fn discover_project_command(
    source: &ForgeHost,
    activity_id: ActivityId,
) -> TestResult<(ExecutionRequest, ProjectCommandEvidence)> {
    let control = source.control_client.clone();
    let scope = source.scope.clone();
    let runtime_instance_id = source.boot_instance;
    tokio::task::spawn_blocking(move || {
        (|| -> TestResult<(ExecutionRequest, ProjectCommandEvidence)> {
            let catalog = control.project_command_list(ProjectCommandListRequest {
                scope,
                activity: FileActivityScope {
                    activity_id,
                    runtime_instance_id,
                    observed_at: TimestampMillis::new(current_test_time()?)?,
                },
            })?;
            let command = ProjectCommandId::new("make:doctor")?;
            let resolved =
                catalog.resolve_execution(&command, ExecutionId::from_uuid(Uuid::now_v7()))?;
            assert_eq!(resolved.class(), ProjectCommandClass::Verify);
            let evidence = ProjectCommandEvidence::new(resolved.class(), resolved.fingerprint());
            Ok((resolved.into_request(), evidence))
        })()
        .map_err(|error| error.to_string())
    })
    .await?
    .map_err(Into::into)
}

pub(super) fn source_submission(
    scope: ResolvedWorkspaceScope,
    activity: ActivityId,
    runtime: RuntimeInstanceId,
    request: ExecutionRequest,
    script: &str,
    project_command: Option<ProjectCommandEvidence>,
) -> TestResult<(forge_domain::ExecutionCommandId, SubmitExecutionForApproval)> {
    let (command_id, submitted) = if let Some(evidence) = project_command {
        (
            forge_domain::ExecutionCommandId::new("make:doctor".to_owned())?,
            SubmitExecutionForApproval::project_command(
                scope, activity, runtime, request, evidence,
            )?,
        )
    } else {
        (
            forge_control::analyze_shell_script(script.to_owned())?
                .intent_id()
                .clone(),
            SubmitExecutionForApproval::analyzed_shell(scope, activity, runtime, request)?,
        )
    };
    Ok((command_id, submitted))
}

pub(super) fn execution_mode(approved: &ApprovedExecutionFixture) -> serde_json::Value {
    if let Some(evidence) = approved.project_command {
        json!({
            "kind": "sealed_project_command",
            "command_id": "make:doctor",
            "expected_catalog_fingerprint": lower_hex_32(evidence.catalog_fingerprint().hash().as_bytes())
        })
    } else {
        json!({"kind": "analyzed_shell", "script": approved.script})
    }
}

fn lower_hex_32(value: &[u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(64);
    for byte in value {
        out.push(char::from(HEX[usize::from(byte >> 4)]));
        out.push(char::from(HEX[usize::from(byte & 15)]));
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires real isolated 3-node AGENT_BROKERD_BIN and SSD RustCI 1.98.0"]
async fn real_broker_forge_rustci_verification_run_end_to_end() -> TestResult {
    let cluster = broker_cluster::Cluster::start().await?;
    let result = bootstrap_fixture_with_broker(cluster.orchestration_config()?, true, true).await;
    drop(cluster);
    result
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires actual validated macOS RustCI SSD toolchain pair"]
async fn authentic_ssd_rustci_capability_is_discoverable() -> TestResult {
    let root = std::env::temp_dir().join(format!("p10-rustci-{}", Uuid::now_v7()));
    std::fs::create_dir(&root)?;
    let prepare_root = root.clone();
    let capability = tokio::task::spawn_blocking(move || {
        prepare_host_capability(RuntimeInstanceId::from_uuid(Uuid::now_v7()), &prepare_root)
            .map_err(|error| error.to_string())
    })
    .await??;
    assert_eq!(capability.pinned_toolchain(), "1.98.0-aarch64-apple-darwin");
    std::fs::remove_dir_all(root)?;
    Ok(())
}

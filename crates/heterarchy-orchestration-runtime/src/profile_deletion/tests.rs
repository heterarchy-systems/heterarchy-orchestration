use super::*;
use crate::AgentProfileCreateCommand;
use forge_domain::TimestampMillis;
use heterarchy_orchestration_operations::{
    AgentProfileProviderDeletionAction, AgentProfileProviderDeletionDeferredReason,
    AgentProfileProviderDeletionState,
};
use heterarchy_orchestration_sessions::{
    AgentProfileProviderFamily, AgentProfileRuntimeSessionId, CreateAgentProfileRuntimeSession,
};
use std::error::Error;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

fn open_runtime(label: &str) -> Result<(std::path::PathBuf, OrchestrationRuntime), Box<dyn Error>> {
    let root = std::env::temp_dir().join(format!("{label}-{}", Uuid::now_v7()));
    std::fs::create_dir_all(&root)?;
    Ok((root.clone(), OrchestrationRuntime::open(&root)?))
}

fn create_profile(
    runtime: &OrchestrationRuntime,
    request_id: Uuid,
) -> Result<crate::AgentProfileSummary, RuntimeError> {
    runtime
        .create_agent_profile(AgentProfileCreateCommand {
            request_id,
            stable_name: "deletion-test".to_owned(),
            display_name: "Deletion test".to_owned(),
            preferred_model: None,
            preferred_reasoning: None,
            base_capabilities: Vec::new(),
            created_at_ms: 1,
        })
        .map(|outcome| outcome.profile)
}

fn attach_ready_runtime(
    runtime: &OrchestrationRuntime,
    profile_id: Uuid,
    runtime_id: Uuid,
) -> Result<AgentProfileRuntimeSessionId, Box<dyn Error>> {
    let profile_id = AgentProfileId::from_uuid(profile_id)?;
    let runtime_id = AgentProfileRuntimeSessionId::from_uuid(runtime_id)?;
    let command = CreateAgentProfileRuntimeSession::new(
        runtime_id,
        Uuid::now_v7(),
        profile_id,
        AgentProfileProviderFamily::ChatGpt,
        TimestampMillis::new(2)?,
    )?;
    let mut catalog = runtime.profile_catalog_guard()?;
    let created = catalog.create_runtime_session(&command)?;
    let session = created.session();
    let _ready = catalog.confirm_runtime_session_ready(
        runtime_id,
        session.revision(),
        "provider-conversation-1",
        TimestampMillis::new(3)?,
    )?;
    Ok(runtime_id)
}

fn attach_unlinked_runtime(
    runtime: &OrchestrationRuntime,
    profile_id: Uuid,
    runtime_id: Uuid,
) -> Result<AgentProfileRuntimeSessionId, Box<dyn Error>> {
    let profile_id = AgentProfileId::from_uuid(profile_id)?;
    let runtime_id = AgentProfileRuntimeSessionId::from_uuid(runtime_id)?;
    let command = CreateAgentProfileRuntimeSession::new(
        runtime_id,
        Uuid::now_v7(),
        profile_id,
        AgentProfileProviderFamily::ChatGpt,
        TimestampMillis::new(2)?,
    )?;
    let _created = runtime
        .profile_catalog_guard()?
        .create_runtime_session(&command)?;
    Ok(runtime_id)
}

fn resume_to_delete_targets(
    runtime: &OrchestrationRuntime,
    request_id: Uuid,
) -> Result<(), Box<dyn Error>> {
    for observed_at_ms in 5..40 {
        let resumed = runtime.resume_agent_profile_deletion(request_id, observed_at_ms)?;
        if resumed.snapshot.deletion.is_some_and(|deletion| {
            deletion.stage == AgentProfileDeletionStage::DeletingConversations
        }) {
            return Ok(());
        }
    }
    Err("bounded resume calls did not reach provider deletion".into())
}

fn linked_deletion_fixture() -> TestResult<(
    std::path::PathBuf,
    OrchestrationRuntime,
    crate::AgentProfileSummary,
    AgentProfileRuntimeSessionId,
    Uuid,
)> {
    let (root, runtime) = open_runtime("profile-deletion-workflow")?;
    let profile = create_profile(&runtime, Uuid::from_u128(10))?;
    let runtime_session_id =
        attach_ready_runtime(&runtime, profile.profile_id, Uuid::from_u128(11))?;
    let request_id = Uuid::from_u128(12);
    Ok((root, runtime, profile, runtime_session_id, request_id))
}

fn assert_request_read_replay(
    runtime: &OrchestrationRuntime,
    profile: &crate::AgentProfileSummary,
    request_id: Uuid,
) -> TestResult {
    let requested = runtime.begin_agent_profile_deletion(
        request_id,
        profile.profile_id,
        profile.revision,
        4,
    )?;
    assert_eq!(
        requested.deletion.as_ref().map(|value| value.request_id),
        Some(request_id)
    );
    let replay = runtime.begin_agent_profile_deletion(
        request_id,
        profile.profile_id,
        profile.revision,
        5,
    )?;
    assert_eq!(replay, requested);
    assert_eq!(
        runtime.agent_profile_deletion_request(request_id)?,
        Some(requested.clone())
    );
    assert_eq!(
        runtime.agent_profile_deletion(profile.profile_id)?,
        Some(requested)
    );
    Ok(())
}

fn first_execute(
    runtime: &OrchestrationRuntime,
    request_id: Uuid,
    runtime_session_id: AgentProfileRuntimeSessionId,
) -> TestResult<AgentProfileProviderDeletionDispatchSummary> {
    resume_to_delete_targets(runtime, request_id)?;
    let execute = runtime
        .next_agent_profile_provider_deletion_dispatch(40)?
        .ok_or("expected one provider deletion dispatch")?;
    assert_eq!(
        execute.fence.action(),
        AgentProfileProviderDeletionAction::Execute
    );
    assert_eq!(execute.fence.runtime_session_id(), runtime_session_id);
    assert_eq!(
        execute.fence.provider_conversation_id(),
        "provider-conversation-1"
    );
    assert_eq!(
        execute.state,
        AgentProfileProviderDeletionState::EffectPending
    );
    let changed_fence = AgentProfileProviderDeletionFence::new(
        execute.fence.operation_id(),
        execute.fence.request_id(),
        execute.fence.profile_id().into_uuid(),
        execute.fence.runtime_session_id().into_uuid(),
        "different-conversation".to_owned(),
        execute.fence.expected_revision(),
        execute.fence.attempt_number(),
        AgentProfileProviderDeletionAction::Execute,
    )?;
    assert!(matches!(
        runtime.mark_agent_profile_provider_deletion_unknown(&changed_fence, 41),
        Err(RuntimeError::Conflict)
    ));
    Ok(execute)
}

fn prove_execute_replay_and_recover_inspect(
    runtime: &OrchestrationRuntime,
    execute: &AgentProfileProviderDeletionDispatchSummary,
) -> TestResult<(
    AgentProfileProviderDeletionFence,
    AgentProfileProviderDeletionReceiptInput,
    i64,
)> {
    let pre_click = AgentProfileProviderDeletionReceiptInput::NotStartedBeforeClick {
        reason: AgentProfileProviderDeletionDeferredReason::ProviderBusy,
        browser_session_id: None,
        tab_id: None,
        document_id: None,
    };
    let pre_click_ack =
        runtime.record_agent_profile_provider_deletion_receipt(&execute.fence, &pre_click, 41)?;
    assert_eq!(
        pre_click_ack.state,
        AgentProfileProviderDeletionState::Deferred
    );
    assert_eq!(
        runtime.record_agent_profile_provider_deletion_receipt(&execute.fence, &pre_click, 42)?,
        pre_click_ack
    );

    let retry_at = 41_i64 + 30_000;
    let next_execute = runtime
        .next_agent_profile_provider_deletion_dispatch(retry_at)?
        .ok_or("expected the pre-click target to become retryable")?;
    assert_eq!(
        next_execute.fence.action(),
        AgentProfileProviderDeletionAction::Execute
    );
    assert_eq!(
        next_execute.fence.attempt_number(),
        execute.fence.attempt_number() + 1
    );
    let no_effect = AgentProfileProviderDeletionReceiptInput::PositiveNoEffectReadback {
        reason: AgentProfileProviderDeletionDeferredReason::ProviderBusy,
        browser_session_id: Some("browser-session-1".to_owned()),
        tab_id: Some("tab-1".to_owned()),
        document_id: Some("document-1".to_owned()),
    };
    assert!(matches!(
        runtime.record_agent_profile_provider_deletion_receipt(
            &next_execute.fence,
            &no_effect,
            retry_at + 1
        ),
        Err(RuntimeError::Conflict)
    ));

    let unknown =
        runtime.mark_agent_profile_provider_deletion_unknown(&next_execute.fence, retry_at + 1)?;
    assert_eq!(
        unknown.state,
        AgentProfileProviderDeletionState::EffectUnknown
    );
    assert_eq!(
        runtime.mark_agent_profile_provider_deletion_unknown(&next_execute.fence, retry_at + 2)?,
        unknown
    );
    let inspect = runtime
        .next_agent_profile_provider_deletion_dispatch(retry_at + 3)?
        .ok_or("expected Inspect for an Unknown attempt")?;
    assert_eq!(inspect.fence.operation_id(), execute.fence.operation_id());
    assert_eq!(
        inspect.fence.attempt_number(),
        next_execute.fence.attempt_number()
    );
    assert_eq!(
        inspect.fence.action(),
        AgentProfileProviderDeletionAction::Inspect
    );
    assert_eq!(
        inspect.state,
        AgentProfileProviderDeletionState::EffectUnknown
    );
    Ok((inspect.fence, no_effect, retry_at))
}

fn prove_inspect_receipt_replay(
    runtime: &OrchestrationRuntime,
    inspect_fence: &AgentProfileProviderDeletionFence,
    no_effect: &AgentProfileProviderDeletionReceiptInput,
    retry_at: i64,
) -> TestResult<i64> {
    let no_effect_ack = runtime.record_agent_profile_provider_deletion_receipt(
        inspect_fence,
        no_effect,
        retry_at + 4,
    )?;
    assert_eq!(
        no_effect_ack.state,
        AgentProfileProviderDeletionState::Deferred
    );
    assert_eq!(
        runtime.record_agent_profile_provider_deletion_receipt(
            inspect_fence,
            no_effect,
            retry_at + 5
        )?,
        no_effect_ack
    );
    let changed_receipt = AgentProfileProviderDeletionReceiptInput::PositiveNoEffectReadback {
        reason: AgentProfileProviderDeletionDeferredReason::ProviderBusy,
        browser_session_id: Some("browser-session-1".to_owned()),
        tab_id: Some("tab-1".to_owned()),
        document_id: Some("different-document".to_owned()),
    };
    assert!(matches!(
        runtime.record_agent_profile_provider_deletion_receipt(
            inspect_fence,
            &changed_receipt,
            retry_at + 5
        ),
        Err(RuntimeError::Conflict)
    ));
    Ok(retry_at + 4 + 30_000)
}

fn prove_positive_confirmation(
    runtime: &OrchestrationRuntime,
    inspect_attempt: u64,
    final_retry_at: i64,
) -> TestResult<(AgentProfileProviderDeletionFence, i64)> {
    let final_execute = runtime
        .next_agent_profile_provider_deletion_dispatch(final_retry_at)?
        .ok_or("expected the deferred target to become retryable")?;
    assert_eq!(
        final_execute.fence.action(),
        AgentProfileProviderDeletionAction::Execute
    );
    assert_eq!(final_execute.fence.attempt_number(), inspect_attempt + 1);
    runtime
        .mark_agent_profile_provider_deletion_unknown(&final_execute.fence, final_retry_at + 1)?;
    let confirmation_fence = runtime
        .next_agent_profile_provider_deletion_dispatch(final_retry_at + 2)?
        .ok_or("expected Inspect before positive confirmation")?
        .fence;
    let missing_evidence = AgentProfileProviderDeletionReceiptInput::ProviderDeleteConfirmed {
        browser_session_id: "browser-session-1".to_owned(),
        tab_id: "tab-1".to_owned(),
        document_id: String::new(),
    };
    assert!(matches!(
        runtime.record_agent_profile_provider_deletion_receipt(
            &confirmation_fence,
            &missing_evidence,
            final_retry_at + 3
        ),
        Err(RuntimeError::InvalidInput)
    ));
    assert!(
        runtime
            .catalog()?
            .agent_profile_provider_deletion_receipt(
                confirmation_fence.operation_id(),
                confirmation_fence.attempt_number(),
            )?
            .is_none()
    );
    let confirmed = AgentProfileProviderDeletionReceiptInput::ProviderDeleteConfirmed {
        browser_session_id: "browser-session-1".to_owned(),
        tab_id: "tab-1".to_owned(),
        document_id: "document-1".to_owned(),
    };
    let confirmed_ack = runtime.record_agent_profile_provider_deletion_receipt(
        &confirmation_fence,
        &confirmed,
        final_retry_at + 3,
    )?;
    assert_eq!(
        confirmed_ack.state,
        AgentProfileProviderDeletionState::Deleted
    );
    assert_eq!(
        runtime.record_agent_profile_provider_deletion_receipt(
            &confirmation_fence,
            &confirmed,
            final_retry_at + 4
        )?,
        confirmed_ack
    );

    Ok((confirmation_fence, final_retry_at + 3))
}

fn assert_completed_deletion(
    runtime: &OrchestrationRuntime,
    request_id: Uuid,
    observed_at_ms: i64,
) -> TestResult {
    let _settled = runtime.resume_agent_profile_deletion(request_id, observed_at_ms + 2)?;
    let _session_finished =
        runtime.resume_agent_profile_deletion(request_id, observed_at_ms + 3)?;
    let complete = runtime.resume_agent_profile_deletion(request_id, observed_at_ms + 4)?;
    assert_eq!(complete.step, AgentProfileDeletionStepResult::Complete);
    let deletion = complete
        .snapshot
        .deletion
        .ok_or("deletion projection missing")?;
    assert_eq!(deletion.stage, AgentProfileDeletionStage::Completed);
    assert_eq!(deletion.linked_conversation_count, Some(1));
    assert_eq!(deletion.deleted_conversation_count, 1);
    assert!(deletion.completed_at_ms.is_some());
    Ok(())
}

fn corrupt_receipt_and_assert_terminal_read_fails(
    root: &std::path::Path,
    runtime: &OrchestrationRuntime,
    request_id: Uuid,
    runtime_session_id: AgentProfileRuntimeSessionId,
    fence: &AgentProfileProviderDeletionFence,
    observed_at_ms: i64,
) -> TestResult {
    let operations_db = rusqlite::Connection::open(root.join("agent-operations.sqlite3"))?;
    let original_runtime_revision = runtime
        .profile_catalog_guard()?
        .profile_deletion_runtime_entry(request_id, runtime_session_id)?
        .ok_or("frozen Runtime Session entry missing")?
        .original_revision()
        .get();
    let changed_metadata = operations_db.execute(
        "UPDATE ops_agent_profile_provider_deletions
         SET original_runtime_revision=?1 WHERE operation_id=?2",
        rusqlite::params![
            original_runtime_revision + 1,
            fence.operation_id().as_bytes(),
        ],
    )?;
    assert_eq!(changed_metadata, 1);
    assert!(runtime.agent_profile_deletion_request(request_id).is_err());
    let restored_metadata = operations_db.execute(
        "UPDATE ops_agent_profile_provider_deletions
         SET original_runtime_revision=?1 WHERE operation_id=?2",
        rusqlite::params![original_runtime_revision, fence.operation_id().as_bytes()],
    )?;
    assert_eq!(restored_metadata, 1);
    assert!(
        runtime
            .agent_profile_deletion_request(request_id)?
            .is_some_and(|snapshot| snapshot
                .deletion
                .is_some_and(|deletion| deletion.stage == AgentProfileDeletionStage::Completed))
    );

    let changed = operations_db.execute(
        "UPDATE ops_agent_profile_provider_deletion_receipts
         SET outcome_kind=1,deferred_reason=1,retry_after_ms=?1
         WHERE operation_id=?2 AND attempt_number=?3",
        rusqlite::params![
            observed_at_ms + 30_000,
            fence.operation_id().as_bytes(),
            i64::try_from(fence.attempt_number())?,
        ],
    )?;
    assert_eq!(changed, 1);
    assert!(runtime.agent_profile_deletion_request(request_id).is_err());
    Ok(())
}

#[test]
fn profile_deletion_request_is_readable_and_provider_effects_require_exact_receipts() -> TestResult
{
    let (root, runtime, profile, runtime_session_id, request_id) = linked_deletion_fixture()?;
    assert_request_read_replay(&runtime, &profile, request_id)?;
    let execute = first_execute(&runtime, request_id, runtime_session_id)?;
    let (inspect_fence, no_effect, retry_at) =
        prove_execute_replay_and_recover_inspect(&runtime, &execute)?;
    let final_retry_at =
        prove_inspect_receipt_replay(&runtime, &inspect_fence, &no_effect, retry_at)?;
    let (confirmation_fence, observed_at_ms) =
        prove_positive_confirmation(&runtime, inspect_fence.attempt_number(), final_retry_at)?;
    assert_completed_deletion(&runtime, request_id, observed_at_ms)?;
    corrupt_receipt_and_assert_terminal_read_fails(
        &root,
        &runtime,
        request_id,
        runtime_session_id,
        &confirmation_fence,
        observed_at_ms + 4,
    )?;

    drop(runtime);
    std::fs::remove_dir_all(root)?;
    Ok(())
}

#[test]
fn deletion_with_no_runtime_targets_completes_and_replays_from_readback()
-> Result<(), Box<dyn Error>> {
    let (root, runtime) = open_runtime("profile-deletion-empty-inventory")?;
    let profile = create_profile(&runtime, Uuid::from_u128(20))?;
    let request_id = Uuid::from_u128(21);
    let requested = runtime.begin_agent_profile_deletion(
        request_id,
        profile.profile_id,
        profile.revision,
        2,
    )?;
    assert_eq!(
        runtime.agent_profile_deletion_request(request_id)?,
        Some(requested)
    );

    let mut complete = None;
    for observed_at_ms in 3..32 {
        let resumed = runtime.resume_agent_profile_deletion(request_id, observed_at_ms)?;
        if resumed.step == AgentProfileDeletionStepResult::Complete {
            complete = Some(resumed);
            break;
        }
    }
    let complete = complete.ok_or("bounded resume calls did not finish empty inventory")?;
    let deletion = complete
        .snapshot
        .deletion
        .ok_or("deletion projection missing")?;
    assert_eq!(deletion.stage, AgentProfileDeletionStage::Completed);
    assert_eq!(deletion.linked_conversation_count, Some(0));
    assert_eq!(deletion.deleted_conversation_count, 0);
    assert_eq!(
        runtime.agent_profile_deletion_request(request_id)?,
        Some(complete.snapshot.clone())
    );
    assert!(
        runtime
            .next_agent_profile_provider_deletion_dispatch(33)?
            .is_none()
    );

    let replayed = runtime.begin_agent_profile_deletion(
        request_id,
        profile.profile_id,
        profile.revision,
        34,
    )?;
    assert_eq!(replayed, complete.snapshot);

    drop(runtime);
    std::fs::remove_dir_all(root)?;
    Ok(())
}

#[test]
fn omitted_no_linked_manifest_row_cannot_end_preparation() -> Result<(), Box<dyn Error>> {
    let (root, runtime) = open_runtime("profile-deletion-no-link-corruption")?;
    let profile = create_profile(&runtime, Uuid::from_u128(30))?;
    let request_id = Uuid::from_u128(31);
    attach_unlinked_runtime(&runtime, profile.profile_id, Uuid::from_u128(32))?;
    runtime.begin_agent_profile_deletion(request_id, profile.profile_id, profile.revision, 3)?;

    let mut prepare_at_ms = None;
    for candidate in 4..32 {
        let resumed = runtime.resume_agent_profile_deletion(request_id, candidate)?;
        if resumed.snapshot.deletion.is_some_and(|deletion| {
            deletion.stage == AgentProfileDeletionStage::ProviderDeletionPending
        }) {
            prepare_at_ms = Some(candidate + 1);
            break;
        }
    }
    let observed_at_ms = prepare_at_ms.ok_or("deletion did not reach the frozen manifest")?;

    let sessions_db = rusqlite::Connection::open(root.join("agent-profiles.sqlite3"))?;
    let unlinked_count: i64 = sessions_db.query_row(
        "SELECT COUNT(*) FROM agent_profile_deletion_runtime_entries
         WHERE request_id=?1 AND provider_conversation_id IS NULL",
        rusqlite::params![request_id.as_bytes()],
        |row| row.get(0),
    )?;
    assert_eq!(unlinked_count, 1);
    let removed = sessions_db.execute(
        "DELETE FROM agent_profile_deletion_runtime_entries WHERE request_id=?1",
        rusqlite::params![request_id.as_bytes()],
    )?;
    assert_eq!(removed, 1);

    assert!(
        runtime
            .resume_agent_profile_deletion(request_id, observed_at_ms)
            .is_err()
    );
    let current = runtime
        .agent_profile_deletion_request(request_id)?
        .ok_or("deletion request disappeared after manifest corruption")?;
    assert_eq!(
        current.deletion.map(|deletion| deletion.stage),
        Some(AgentProfileDeletionStage::ProviderDeletionPending)
    );

    drop(sessions_db);
    drop(runtime);
    std::fs::remove_dir_all(root)?;
    Ok(())
}

#[test]
fn changed_provider_receipt_blocks_delete_target_settlement() -> Result<(), Box<dyn Error>> {
    let (root, runtime) = open_runtime("profile-deletion-receipt-corruption")?;
    let profile = create_profile(&runtime, Uuid::from_u128(40))?;
    let runtime_session_id =
        attach_ready_runtime(&runtime, profile.profile_id, Uuid::from_u128(41))?;
    let request_id = Uuid::from_u128(42);
    runtime.begin_agent_profile_deletion(request_id, profile.profile_id, profile.revision, 4)?;
    resume_to_delete_targets(&runtime, request_id)?;
    let dispatch = runtime
        .next_agent_profile_provider_deletion_dispatch(40)?
        .ok_or("expected provider deletion dispatch")?;
    assert_eq!(dispatch.fence.runtime_session_id(), runtime_session_id);
    let confirmed = AgentProfileProviderDeletionReceiptInput::ProviderDeleteConfirmed {
        browser_session_id: "browser-session-2".to_owned(),
        tab_id: "tab-2".to_owned(),
        document_id: "document-2".to_owned(),
    };
    runtime.record_agent_profile_provider_deletion_receipt(&dispatch.fence, &confirmed, 41)?;

    let operations_db = rusqlite::Connection::open(root.join("agent-operations.sqlite3"))?;
    let changed = operations_db.execute(
        "UPDATE ops_agent_profile_provider_deletion_receipts
         SET outcome_kind=1,deferred_reason=1,retry_after_ms=?1
         WHERE operation_id=?2 AND attempt_number=?3",
        rusqlite::params![
            41_i64 + 30_000,
            dispatch.fence.operation_id().as_bytes(),
            i64::try_from(dispatch.fence.attempt_number())?,
        ],
    )?;
    assert_eq!(changed, 1);

    let resumed = runtime.resume_agent_profile_deletion(request_id, 42);
    assert!(resumed.is_err());
    assert_eq!(
        runtime
            .agent_profile_deletion_request(request_id)?
            .and_then(|snapshot| snapshot.deletion.map(|deletion| deletion.stage)),
        Some(AgentProfileDeletionStage::DeletingConversations)
    );
    assert!(
        runtime
            .agent_profile_deletion_request(request_id)?
            .is_some_and(|snapshot| snapshot
                .deletion
                .is_some_and(|deletion| deletion.stage != AgentProfileDeletionStage::Completed))
    );

    drop(operations_db);
    drop(runtime);
    std::fs::remove_dir_all(root)?;
    Ok(())
}

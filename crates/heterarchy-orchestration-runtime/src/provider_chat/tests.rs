//! Isolated catalog regressions; seeded import rows are not Source acceptance.

use super::*;
use crate::provisioning::AgentRequestSubmit;
use forge_domain::{AuthenticatedPrincipal, TimestampMillis, WorkspaceId};
use heterarchy_orchestration_operations::{
    AgentRequestEffectScope, AgentRequestIndependence, AppendWorkspaceMessageCommand,
    EnqueueProviderMessageCommand, MessageChannel, MessageContext, OperationalActor,
    ProviderMessageDeliveryReceipt, ProviderMessageTarget, WorkspaceParticipationId,
};
use heterarchy_orchestration_sessions::{
    ProfileConfiguration, ProfileMutation, ProfileMutationCommand,
};
use rusqlite::{Connection, params};
use std::{error::Error, path::PathBuf};
use uuid::Uuid;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
const OPERATOR: AuthenticatedPrincipal = AuthenticatedPrincipal::TrustedStdio;

struct Fixture {
    root: PathBuf,
    runtime: OrchestrationRuntime,
    target: ProviderMessageTarget,
    message_id: Uuid,
}

impl Fixture {
    fn ready() -> TestResult<Self> {
        let root =
            std::env::temp_dir().join(format!("provider-chat-observation-{}", Uuid::now_v7()));
        std::fs::create_dir_all(&root)?;
        let runtime = OrchestrationRuntime::open(&root)?;
        let workspace = WorkspaceId::from_uuid(Uuid::now_v7());
        let request_id = Uuid::now_v7();
        runtime.submit_agent_request(AgentRequestSubmit {
            workspace_id: workspace.into_uuid(),
            activity_id: Uuid::now_v7(),
            requester_agent_id: Uuid::now_v7(),
            requester_session_id: Uuid::now_v7(),
            coordinator_revision: 1,
            request_id,
            objective: "isolated provider chat".to_owned(),
            capability: "chat".to_owned(),
            count: 1,
            effect_scope: AgentRequestEffectScope::SourceWrite,
            independence: AgentRequestIndependence::None,
            reason: "fixture route".to_owned(),
            created_at_ms: 1,
        })?;
        runtime.reserve_agent_request_resolution(request_id, 2, 3)?;
        runtime.prepare_agent_request_effect_targets(request_id, 6, 7)?;
        let action = runtime
            .provider_worker_next_provision(OPERATOR, 10)?
            .ok_or("fixture dispatch")?;
        let operation = runtime.provider_worker_confirm_provision(
            OPERATOR,
            action.operation().operation_id(),
            action.operation().revision(),
            "conversation-test",
            11,
        )?;
        let profile = operation.profile_id().ok_or("profile binding")?;
        let runtime_id = operation.runtime_session_id().ok_or("runtime binding")?;
        let participation = WorkspaceParticipationId::from_uuid(Uuid::now_v7())?;
        let seed = Connection::open(root.join("agent-operations.sqlite3"))?;
        seed.execute(
            "INSERT INTO ops_imports(migration_id,workspace_id,destination_id,source_revision,phase,next_cursor,page_count,record_count,import_digest,verified_pages,verify_digest)
             VALUES(?1,?2,?3,1,2,NULL,5,0,?4,5,?4)",
            params![Uuid::now_v7().as_bytes(),workspace.into_uuid().as_bytes(),runtime.destination_id().as_bytes(),[0_u8;32].as_slice()])?;
        seed.execute(
            "INSERT INTO ops_workspace_participations(workspace_id,agent_profile_id,participation_id,created_at_ms,revision) VALUES(?1,?2,?3,0,1)",
            params![workspace.into_uuid().as_bytes(),profile.into_uuid().as_bytes(),participation.into_uuid().as_bytes()])?;
        drop(seed);
        let target = ProviderMessageTarget::new(
            workspace,
            profile,
            participation,
            runtime_id,
            "conversation-test".to_owned(),
        )?;
        let message_id = Uuid::now_v7();
        {
            let mut catalog = runtime.catalog()?;
            catalog.append_workspace_message(AppendWorkspaceMessageCommand {
                message_id,
                workspace_id: workspace,
                channel: MessageChannel::Workspace,
                actor: OperationalActor::LocalOperator,
                body: "original prompt".to_owned(),
                submitted_at: TimestampMillis::new(12)?,
                context: MessageContext::default(),
            })?;
            catalog.enqueue_provider_message(EnqueueProviderMessageCommand {
                message_id,
                target: target.clone(),
                queued_at: TimestampMillis::new(13)?,
                node_route_witness: None,
            })?;
            let _action = catalog
                .next_provider_message_dispatch(TimestampMillis::new(14)?)?
                .ok_or("outbox dispatch")?;
        }
        Ok(Self {
            root,
            runtime,
            target,
            message_id,
        })
    }
    fn command(
        &self,
        role: ProviderIncomingRole,
        turn: &str,
        body: &str,
    ) -> ProviderIncomingCommand {
        ProviderIncomingCommand {
            reply_to_message_id: self.message_id,
            provider_conversation_id: "conversation-test".to_owned(),
            provider_turn_id: turn.to_owned(),
            role,
            body: body.to_owned(),
        }
    }
    fn cleanup(self) -> TestResult {
        let Self { root, runtime, .. } = self;
        drop(runtime);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }
}

#[test]
fn incoming_readback_and_rename_replay_preserve_identity_and_never_write() -> TestResult {
    let fixture = Fixture::ready()?;
    let audit_connection = Connection::open(fixture.root.join("agent-operations.sqlite3"))?;
    let before: i64 =
        audit_connection.pragma_query_value(None, "data_version", |row| row.get(0))?;
    let command = fixture.command(ProviderIncomingRole::Assistant, "assistant-1", "answer");
    assert!(
        fixture
            .runtime
            .provider_worker_incoming_readback(OPERATOR, &command)?
            .is_none()
    );
    let after: i64 = audit_connection.pragma_query_value(None, "data_version", |row| row.get(0))?;
    assert_eq!(before, after, "absent readback must not write");
    let recorded =
        fixture
            .runtime
            .provider_worker_record_incoming(OPERATOR, command.clone(), 15)?;
    let before: i64 =
        audit_connection.pragma_query_value(None, "data_version", |row| row.get(0))?;
    let observed = fixture
        .runtime
        .provider_worker_incoming_readback(OPERATOR, &command)?
        .ok_or("turn readback")?;
    assert_eq!(&observed, recorded.message());
    let after: i64 = audit_connection.pragma_query_value(None, "data_version", |row| row.get(0))?;
    assert_eq!(before, after, "existing readback must not write");
    drop(audit_connection);
    {
        let mut profiles = fixture.runtime.profile_catalog_guard()?;
        let profile = profiles
            .resolve(fixture.target.profile_id())?
            .ok_or("profile")?;
        profiles.mutate_profile(&ProfileMutationCommand::new(
            Uuid::now_v7(),
            profile.id(),
            profile.revision(),
            ProfileMutation::Update(ProfileConfiguration::new(
                "renamed".to_owned(),
                None,
                None,
                vec![],
            )?),
            TimestampMillis::new(16)?,
        )?)?;
    }
    let replay = fixture
        .runtime
        .provider_worker_record_incoming(OPERATOR, command.clone(), 17)?;
    assert!(!replay.created());
    assert_eq!(replay.message(), recorded.message());
    let mut changed = command.clone();
    changed.role = ProviderIncomingRole::User;
    assert!(matches!(
        fixture
            .runtime
            .provider_worker_incoming_readback(OPERATOR, &changed),
        Err(RuntimeError::Conflict)
    ));
    changed = command;
    changed.body = "different".to_owned();
    assert!(matches!(
        fixture
            .runtime
            .provider_worker_incoming_readback(OPERATOR, &changed),
        Err(RuntimeError::Conflict)
    ));
    fixture.cleanup()
}

#[test]
fn delivered_user_turn_cannot_be_relabelled_as_assistant() -> TestResult {
    let fixture = Fixture::ready()?;
    let before = fixture
        .runtime
        .provider_worker_message_readback(OPERATOR, fixture.message_id)?;
    let body = before.body.ok_or("frozen provider body")?;
    let receipt = ProviderMessageDeliveryReceipt::new(
        fixture.target.clone(),
        "user-1".to_owned(),
        body.clone(),
    )?;
    fixture
        .runtime
        .catalog()?
        .confirm_provider_message_delivery(
            fixture.message_id,
            before.outbox.revision(),
            &receipt,
            TimestampMillis::new(15)?,
        )?;
    let user = fixture.command(ProviderIncomingRole::User, "user-1", &body);
    let observed = fixture
        .runtime
        .provider_worker_incoming_readback(OPERATOR, &user)?
        .ok_or("delivered user readback")?;
    assert_eq!(observed.message_id(), fixture.message_id);
    let assistant = fixture.command(ProviderIncomingRole::Assistant, "user-1", &body);
    assert!(matches!(
        fixture
            .runtime
            .provider_worker_record_incoming(OPERATOR, assistant, 16),
        Err(RuntimeError::Conflict)
    ));
    let replay = fixture
        .runtime
        .provider_worker_record_incoming(OPERATOR, user, 17)?;
    assert!(!replay.created());
    assert_eq!(replay.message().message_id(), fixture.message_id);
    fixture.cleanup()
}

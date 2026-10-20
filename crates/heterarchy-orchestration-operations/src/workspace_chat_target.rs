//! Confirmed provider bindings available for one Workspace Chat route.
//!
//! The route is an Operations projection only. It does not assert Forge
//! readiness or current logical membership; the native caller performs those
//! checks before sending. Every row is scoped by the requested Workspace and
//! Profile and must have a real `WorkspaceParticipation` anchor.

use forge_domain::{ActivityId, WorkspaceId};
use heterarchy_orchestration_sessions::{AgentProfileId, AgentProfileRuntimeSessionId};
use rusqlite::{Transaction, params};
use uuid::Uuid;

use crate::{
    ChatGptProjectId, OperationsCatalog, OperationsError, WorkspaceProjectBindingRevision,
    WorkspaceProjectReference, wire,
};

const MAX_CHAT_TARGETS: usize = 8;
const CONFIRMED_PROVISION_STATE: i64 = 5;
const CHATGPT_PROVIDER_FAMILY: i64 = 1;
const MAX_PROVIDER_CONVERSATION_ID_BYTES: usize = 512;

/// One confirmed provider binding for an exact Workspace/Profile route.
///
/// This record proves only that Operations has a confirmed provider binding;
/// it does not prove current Forge runtime readiness or logical membership.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceChatTarget {
    operation_id: Uuid,
    activity_id: ActivityId,
    profile_id: AgentProfileId,
    participation_id: crate::WorkspaceParticipationId,
    runtime_session_id: AgentProfileRuntimeSessionId,
    conversation: String,
    project: Option<WorkspaceProjectReference>,
}

impl WorkspaceChatTarget {
    #[must_use]
    pub const fn operation_id(&self) -> Uuid {
        self.operation_id
    }

    #[must_use]
    pub const fn activity_id(&self) -> ActivityId {
        self.activity_id
    }

    #[must_use]
    pub const fn profile_id(&self) -> AgentProfileId {
        self.profile_id
    }

    #[must_use]
    pub const fn participation_id(&self) -> crate::WorkspaceParticipationId {
        self.participation_id
    }

    #[must_use]
    pub const fn runtime_session_id(&self) -> AgentProfileRuntimeSessionId {
        self.runtime_session_id
    }

    #[must_use]
    pub fn provider_conversation_id(&self) -> &str {
        &self.conversation
    }

    #[must_use]
    pub const fn project(&self) -> Option<WorkspaceProjectReference> {
        self.project
    }
}

/// Bounded Chat route page. `truncated` is explicit so callers cannot present
/// a partial candidate set as a complete Workspace authority projection.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceChatTargetPage {
    targets: Vec<WorkspaceChatTarget>,
    truncated: bool,
}

impl WorkspaceChatTargetPage {
    pub fn targets(&self) -> &[WorkspaceChatTarget] {
        &self.targets
    }

    #[must_use]
    pub const fn truncated(&self) -> bool {
        self.truncated
    }
}

impl OperationsCatalog {
    /// Read confirmed provider bindings for one exact Workspace/Profile pair.
    ///
    /// The query joins the request's Workspace, the confirmed provision
    /// operation, and the real `WorkspaceParticipation` row in one `SQLite`
    /// snapshot. A Runtime Session shared by another Workspace is
    /// excluded as ambiguous, even if the requested Workspace/Profile row is
    /// otherwise valid.
    ///
    /// # Errors
    /// Rejects limits above the native bound, malformed persisted identities,
    /// invalid provider conversation codecs, and read failures.
    pub fn workspace_chat_targets(
        &self,
        workspace_id: WorkspaceId,
        profile_id: AgentProfileId,
        limit: usize,
    ) -> Result<WorkspaceChatTargetPage, OperationsError> {
        if !(1..=MAX_CHAT_TARGETS).contains(&limit) {
            return Err(OperationsError::Bounds("workspace chat target limit"));
        }
        let query_limit = i64::try_from(limit + 1)
            .map_err(|_| OperationsError::Bounds("workspace chat target query limit"))?;
        let tx = self.connection.unchecked_transaction()?;
        crate::operational_messages::require_verified_workspace(
            &tx,
            workspace_id,
            self.destination,
        )?;
        let result = read_workspace_chat_targets(&tx, workspace_id, profile_id, query_limit, limit);
        match result {
            Ok(page) => {
                crate::catalog::commit_read(tx, self.destination)?;
                Ok(page)
            }
            Err(error) => Err(error),
        }
    }
}

/// Read a selected route with the same bounded candidate and supersession rules
/// as the public projection, inside the caller's existing transaction.
pub(crate) fn read_workspace_chat_target_in_transaction(
    tx: &Transaction<'_>,
    workspace_id: WorkspaceId,
    profile_id: AgentProfileId,
    operation_id: Uuid,
) -> Result<Option<WorkspaceChatTarget>, OperationsError> {
    let query_limit = i64::try_from(MAX_CHAT_TARGETS + 1)
        .map_err(|_| OperationsError::Bounds("Workspace Chat target limit"))?;
    let page =
        read_workspace_chat_targets(tx, workspace_id, profile_id, query_limit, MAX_CHAT_TARGETS)?;
    if page.truncated {
        return Err(OperationsError::Bounds("Workspace Chat targets"));
    }
    Ok(page
        .targets
        .into_iter()
        .find(|route| route.operation_id == operation_id))
}

const WORKSPACE_CHAT_TARGETS_SQL: &str = "WITH confirmed AS (
             SELECT p.operation_id, r.workspace_id, p.runtime_session_id
             FROM ops_agent_provision_operations AS p
             JOIN ops_agent_requests AS r ON r.request_id=p.request_id
             WHERE p.provider_family=?1
               AND p.state=?2
               AND p.runtime_session_id IS NOT NULL
               AND p.provider_conversation_id IS NOT NULL
         )
         SELECT p.operation_id,
                r.activity_id,
                p.profile_id,
                wp.participation_id,
                p.runtime_session_id,
                p.provider_conversation_id,
                route.project_id,
                route.binding_revision
         FROM ops_agent_provision_operations AS p
         JOIN ops_agent_requests AS r
           ON r.request_id=p.request_id
          AND r.workspace_id=?3
         LEFT JOIN ops_agent_provision_project_routes AS route
           ON route.operation_id=p.operation_id
         JOIN ops_workspace_participations AS wp
           ON wp.workspace_id=r.workspace_id
          AND wp.agent_profile_id=p.profile_id
         WHERE p.provider_family=?1
           AND p.state=?2
           AND p.profile_id=?4
           AND p.runtime_session_id IS NOT NULL
           AND p.provider_conversation_id IS NOT NULL
           AND NOT EXISTS (
               SELECT 1
               FROM confirmed AS shared
               WHERE shared.runtime_session_id=p.runtime_session_id
                 AND shared.workspace_id <> r.workspace_id
           )
           AND NOT EXISTS (
               SELECT 1
               FROM ops_agent_provision_operations AS newer
               JOIN ops_agent_requests AS newer_request
                 ON newer_request.request_id=newer.request_id
               WHERE newer.provider_family=?1
                 AND newer.state=?2
                 AND newer.profile_id=p.profile_id
                 AND newer.runtime_session_id=p.runtime_session_id
                 AND newer_request.workspace_id=r.workspace_id
                 AND (
                     EXISTS (
                         SELECT 1
                         FROM ops_workspace_participation_mappings AS mapping
                         JOIN ops_logical_members AS member
                           ON member.workspace_id=mapping.workspace_id
                          AND member.participant_id=mapping.participant_id
                          AND member.session_id=mapping.session_id
                         JOIN ops_current_effect_bindings AS binding
                           ON binding.workspace_id=member.workspace_id
                          AND binding.participant_id=member.participant_id
                          AND binding.session_id=member.session_id
                          AND binding.activity_id=newer_request.activity_id
                         WHERE mapping.workspace_id=newer_request.workspace_id
                           AND mapping.participation_id=wp.participation_id
                           AND mapping.agent_profile_id=newer.profile_id
                           AND mapping.profile_mapping_request_id=newer.operation_id
                     )
                     OR EXISTS (
                         SELECT 1
                         FROM ops_workspace_participation_session_rebinds AS rebind
                         JOIN ops_logical_members AS member
                           ON member.workspace_id=rebind.workspace_id
                          AND member.participant_id=rebind.participant_id
                          AND member.session_id=rebind.new_session_id
                         JOIN ops_current_effect_bindings AS binding
                           ON binding.workspace_id=member.workspace_id
                          AND binding.participant_id=member.participant_id
                          AND binding.session_id=member.session_id
                          AND binding.activity_id=newer_request.activity_id
                         WHERE rebind.workspace_id=newer_request.workspace_id
                           AND rebind.participation_id=wp.participation_id
                           AND rebind.agent_profile_id=newer.profile_id
                           AND rebind.profile_mapping_request_id=newer.operation_id
                     )
                 )
                 AND (
                     newer.updated_at_ms > p.updated_at_ms
                     OR (
                         newer.updated_at_ms = p.updated_at_ms
                         AND newer.operation_id > p.operation_id
                     )
                 )
           )
         ORDER BY p.updated_at_ms DESC, p.operation_id DESC
         LIMIT ?5";

fn read_workspace_chat_targets(
    tx: &Transaction<'_>,
    workspace_id: WorkspaceId,
    profile_id: AgentProfileId,
    query_limit: i64,
    limit: usize,
) -> Result<WorkspaceChatTargetPage, OperationsError> {
    let mut statement = tx.prepare(WORKSPACE_CHAT_TARGETS_SQL)?;
    let mut rows = statement.query(params![
        CHATGPT_PROVIDER_FAMILY,
        CONFIRMED_PROVISION_STATE,
        workspace_id.into_uuid().as_bytes(),
        profile_id.into_uuid().as_bytes(),
        query_limit,
    ])?;
    let mut targets = Vec::new();
    while let Some(row) = rows.next()? {
        targets.push(decode_workspace_chat_target(row)?);
    }
    let truncated = targets.len() > limit;
    targets.truncate(limit);
    Ok(WorkspaceChatTargetPage { targets, truncated })
}

fn decode_workspace_chat_target(
    row: &rusqlite::Row<'_>,
) -> Result<WorkspaceChatTarget, OperationsError> {
    let operation_id = row.get::<_, Vec<u8>>(0)?;
    let activity_id = row.get::<_, Vec<u8>>(1)?;
    let profile_id = row.get::<_, Vec<u8>>(2)?;
    let participation_id = row.get::<_, Vec<u8>>(3)?;
    let runtime_session_id = row.get::<_, Vec<u8>>(4)?;
    let provider_conversation_id = row.get::<_, String>(5)?;
    let project_id = row.get::<_, Option<String>>(6)?;
    let binding_revision = row.get::<_, Option<i64>>(7)?;
    let project = match (project_id, binding_revision) {
        (None, None) => None,
        (Some(project_id), Some(binding_revision)) => {
            let project_id = ChatGptProjectId::try_from(project_id.as_str())
                .map_err(|_| OperationsError::Corrupt("workspace chat Project identity"))?;
            let binding_revision = WorkspaceProjectBindingRevision::new(binding_revision)
                .map_err(|_| OperationsError::Corrupt("workspace chat Project revision"))?;
            Some(WorkspaceProjectReference::new(project_id, binding_revision))
        }
        _ => {
            return Err(OperationsError::Corrupt(
                "workspace chat Project route shape",
            ));
        }
    };
    decode_workspace_chat_target_values(
        operation_id,
        activity_id,
        profile_id,
        participation_id,
        runtime_session_id,
        provider_conversation_id,
        project,
    )
}

fn decode_workspace_chat_target_values(
    operation_id: Vec<u8>,
    activity_id: Vec<u8>,
    profile_id: Vec<u8>,
    participation_id: Vec<u8>,
    runtime_session_id: Vec<u8>,
    provider_conversation_id: String,
    project: Option<WorkspaceProjectReference>,
) -> Result<WorkspaceChatTarget, OperationsError> {
    if provider_conversation_id.is_empty()
        || provider_conversation_id.len() > MAX_PROVIDER_CONVERSATION_ID_BYTES
    {
        return Err(OperationsError::Corrupt(
            "workspace chat provider conversation identity",
        ));
    }
    Ok(WorkspaceChatTarget {
        operation_id: Uuid::from_bytes(wire::id(operation_id)?),
        activity_id: ActivityId::from_uuid(Uuid::from_bytes(wire::id(activity_id)?)),
        profile_id: AgentProfileId::from_uuid(Uuid::from_bytes(wire::id(profile_id)?))
            .map_err(|_| OperationsError::Corrupt("workspace chat Profile identity"))?,
        participation_id: crate::WorkspaceParticipationId::from_uuid(Uuid::from_bytes(wire::id(
            participation_id,
        )?))?,
        runtime_session_id: AgentProfileRuntimeSessionId::from_uuid(Uuid::from_bytes(wire::id(
            runtime_session_id,
        )?))
        .map_err(|_| OperationsError::Corrupt("workspace chat Runtime Session identity"))?,
        conversation: provider_conversation_id,
        project,
    })
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::fs;
    use std::path::PathBuf;

    use forge_domain::WorkspaceId;
    use heterarchy_orchestration_sessions::{AgentProfileId, AgentProfileRuntimeSessionId};
    use rusqlite::params;

    use super::*;

    #[derive(Debug, Clone, Copy)]
    struct SeededTarget {
        activity: Uuid,
        session: Uuid,
        agent: Uuid,
        operation: Uuid,
    }

    fn catalog() -> Result<(PathBuf, OperationsCatalog), Box<dyn Error>> {
        let root = std::env::temp_dir().join(format!(
            "heterarchy-workspace-chat-targets-{}",
            Uuid::now_v7()
        ));
        fs::create_dir_all(&root)?;
        let catalog = OperationsCatalog::open(&root.join("operations.sqlite3"), Uuid::now_v7())?;
        Ok((root, catalog))
    }

    fn seed_target(
        catalog: &OperationsCatalog,
        workspace: WorkspaceId,
        profile: AgentProfileId,
        participation: Uuid,
        runtime: AgentProfileRuntimeSessionId,
        operation: Uuid,
        conversation: &str,
    ) -> Result<SeededTarget, Box<dyn Error>> {
        let request = Uuid::now_v7();
        let claim = Uuid::now_v7();
        let activity = Uuid::now_v7();
        let session = Uuid::now_v7();
        let agent = Uuid::now_v7();
        let connection = &catalog.connection;
        connection.execute(
            "INSERT INTO ops_imports(migration_id,workspace_id,destination_id,source_revision,phase,next_cursor,page_count,record_count,import_digest,verified_pages,verify_digest) SELECT ?1,?2,?3,1,2,NULL,5,0,?4,5,?4 WHERE NOT EXISTS(SELECT 1 FROM ops_imports WHERE workspace_id=?2 AND destination_id=?3)",
            params![Uuid::now_v7().as_bytes(), workspace.into_uuid().as_bytes(), catalog.destination.as_bytes(), &[0_u8; 32]],
        )?;
        connection.execute(
            "INSERT OR IGNORE INTO ops_workspace_participations(workspace_id,agent_profile_id,participation_id,created_at_ms,revision) VALUES(?1,?2,?3,1,1)",
            params![workspace.into_uuid().as_bytes(), profile.into_uuid().as_bytes(), participation.as_bytes()],
        )?;
        connection.execute(
            "INSERT INTO ops_agent_requests(request_id,workspace_id,activity_id,requester_agent_id,requester_session_id,coordinator_revision,objective,capability,requested_count,effect_scope,independence_requirement,reason,state,created_at_ms,revision) VALUES(?1,?2,?3,?4,?5,1,'objective','capability',1,0,0,'reason',1,1,1)",
            params![request.as_bytes(), workspace.into_uuid().as_bytes(), activity.as_bytes(), Uuid::now_v7().as_bytes(), session.as_bytes()],
        )?;
        connection.execute(
            "INSERT INTO ops_agent_request_resolutions(request_id,slot_ordinal,resolution_kind,claim_id,profile_id,profile_revision,runtime_session_id,runtime_session_revision,policy_revision,reserved_at_ms) VALUES(?1,0,2,?2,?3,1,?4,1,1,1)",
            params![request.as_bytes(), claim.as_bytes(), profile.into_uuid().as_bytes(), runtime.into_uuid().as_bytes()],
        )?;
        connection.execute(
            "INSERT INTO ops_agent_provision_operations(operation_id,request_id,slot_ordinal,effect_kind,provider_family,profile_id,runtime_session_id,state,provider_conversation_id,prepared_at_ms,updated_at_ms,revision) VALUES(?1,?2,0,1,1,?3,?4,5,?5,1,1,1)",
            params![operation.as_bytes(), request.as_bytes(), profile.into_uuid().as_bytes(), runtime.into_uuid().as_bytes(), conversation],
        )?;
        Ok(SeededTarget {
            activity,
            session,
            agent,
            operation,
        })
    }

    fn activate_target(
        catalog: &OperationsCatalog,
        workspace: WorkspaceId,
        profile: AgentProfileId,
        participation: Uuid,
        participant: Uuid,
        target: SeededTarget,
    ) -> Result<(), Box<dyn Error>> {
        let principal = Uuid::now_v7();
        catalog.connection.execute(
            "INSERT INTO ops_logical_members(workspace_id,participant_id,session_id,principal_kind,principal_id,display_name,declared_role,transport_kind,membership_revision,created_at_ms) VALUES(?1,?2,?3,1,?4,'member','researcher',2,1,1) ON CONFLICT(workspace_id,participant_id) DO UPDATE SET session_id=excluded.session_id",
            params![workspace.into_uuid().as_bytes(), participant.as_bytes(), target.session.as_bytes(), principal.as_bytes()],
        )?;
        catalog.connection.execute(
            "INSERT INTO ops_workspace_participation_mappings(request_id,workspace_id,participation_id,agent_profile_id,profile_revision,profile_mapping_request_id,session_id,session_revision,participant_id,participant_membership_revision,mapped_at_ms) VALUES(?1,?2,?3,?4,1,?1,?5,1,?6,1,1)",
            params![target.operation.as_bytes(), workspace.into_uuid().as_bytes(), participation.as_bytes(), profile.into_uuid().as_bytes(), target.session.as_bytes(), participant.as_bytes()],
        )?;
        catalog.connection.execute(
            "INSERT INTO ops_current_effect_bindings(workspace_id,participant_id,activity_id,session_id,agent_id,source_kind,source_request_id) VALUES(?1,?2,?3,?4,?5,0,?6)",
            params![workspace.into_uuid().as_bytes(), participant.as_bytes(), target.activity.as_bytes(), target.session.as_bytes(), target.agent.as_bytes(), target.operation.as_bytes()],
        )?;
        Ok(())
    }

    fn activate_replacement_target(
        catalog: &OperationsCatalog,
        workspace: WorkspaceId,
        profile: AgentProfileId,
        participation: Uuid,
        participant: Uuid,
        previous: SeededTarget,
        target: SeededTarget,
    ) -> Result<(), Box<dyn Error>> {
        catalog.connection.execute(
            "UPDATE ops_logical_members SET session_id=?1 WHERE workspace_id=?2 AND participant_id=?3 AND session_id=?4",
            params![target.session.as_bytes(), workspace.into_uuid().as_bytes(), participant.as_bytes(), previous.session.as_bytes()],
        )?;
        catalog.connection.execute(
            "UPDATE ops_workspace_participations SET revision=2 WHERE workspace_id=?1 AND agent_profile_id=?2 AND participation_id=?3 AND revision=1",
            params![workspace.into_uuid().as_bytes(), profile.into_uuid().as_bytes(), participation.as_bytes()],
        )?;
        catalog.connection.execute(
            "INSERT INTO ops_workspace_participation_session_rebinds(request_id,workspace_id,participation_id,agent_profile_id,profile_revision,profile_mapping_request_id,previous_session_id,new_session_id,new_session_revision,participant_id,participant_membership_revision,expected_participation_revision,result_participation_revision,bound_at_ms) VALUES(?1,?2,?3,?4,1,?1,?5,?6,1,?7,1,1,2,2)",
            params![target.operation.as_bytes(), workspace.into_uuid().as_bytes(), participation.as_bytes(), profile.into_uuid().as_bytes(), previous.session.as_bytes(), target.session.as_bytes(), participant.as_bytes()],
        )?;
        catalog.connection.execute(
            "INSERT INTO ops_current_effect_bindings(workspace_id,participant_id,activity_id,session_id,agent_id,source_kind,source_request_id) VALUES(?1,?2,?3,?4,?5,0,?6)",
            params![workspace.into_uuid().as_bytes(), participant.as_bytes(), target.activity.as_bytes(), target.session.as_bytes(), target.agent.as_bytes(), target.operation.as_bytes()],
        )?;
        Ok(())
    }

    fn ids(seed: u128) -> (WorkspaceId, AgentProfileId, AgentProfileRuntimeSessionId) {
        (
            WorkspaceId::from_uuid(Uuid::from_u128(seed)),
            AgentProfileId::from_uuid(Uuid::from_u128(seed + 1)).unwrap_or_else(|_| unreachable!()),
            AgentProfileRuntimeSessionId::from_uuid(Uuid::from_u128(seed + 2))
                .unwrap_or_else(|_| unreachable!()),
        )
    }

    #[test]
    fn exact_workspace_and_profile_scope_excludes_other_workspace() -> Result<(), Box<dyn Error>> {
        let (root, catalog) = catalog()?;
        let (workspace_a, profile, runtime_a) = ids(0x100);
        let (workspace_b, _, runtime_b) = ids(0x200);
        seed_target(
            &catalog,
            workspace_a,
            profile,
            Uuid::from_u128(0x301),
            runtime_a,
            Uuid::from_u128(0x401),
            "conversation-a",
        )?;
        seed_target(
            &catalog,
            workspace_b,
            profile,
            Uuid::from_u128(0x302),
            runtime_b,
            Uuid::from_u128(0x402),
            "conversation-b",
        )?;
        let page = catalog.workspace_chat_targets(workspace_a, profile, 8)?;
        assert_eq!(page.targets().len(), 1);
        assert_eq!(page.targets()[0].profile_id(), profile);
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn shared_runtime_is_excluded_but_distinct_runtime_is_allowed() -> Result<(), Box<dyn Error>> {
        let (root, catalog) = catalog()?;
        let (workspace_a, profile_a, runtime_shared) = ids(0x500);
        let (workspace_b, profile_b, _) = ids(0x600);
        seed_target(
            &catalog,
            workspace_a,
            profile_a,
            Uuid::from_u128(0x701),
            runtime_shared,
            Uuid::from_u128(0x801),
            "shared-a",
        )?;
        seed_target(
            &catalog,
            workspace_b,
            profile_b,
            Uuid::from_u128(0x702),
            runtime_shared,
            Uuid::from_u128(0x802),
            "shared-b",
        )?;
        let distinct = AgentProfileRuntimeSessionId::from_uuid(Uuid::from_u128(0x503))
            .unwrap_or_else(|_| unreachable!());
        seed_target(
            &catalog,
            workspace_a,
            profile_a,
            Uuid::from_u128(0x703),
            distinct,
            Uuid::from_u128(0x803),
            "distinct",
        )?;
        let page = catalog.workspace_chat_targets(workspace_a, profile_a, 8)?;
        assert_eq!(page.targets().len(), 1);
        assert_eq!(page.targets()[0].provider_conversation_id(), "distinct");
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn repeated_confirmed_runtime_in_one_workspace_uses_latest_operation()
    -> Result<(), Box<dyn Error>> {
        let (root, catalog) = catalog()?;
        let (workspace, profile, runtime) = ids(0xD00);
        let first_operation = Uuid::from_u128(0xD10);
        let latest_operation = Uuid::from_u128(0xD11);
        let participation = Uuid::from_u128(0xD20);
        seed_target(
            &catalog,
            workspace,
            profile,
            participation,
            runtime,
            first_operation,
            "old-conversation",
        )?;
        let latest = seed_target(
            &catalog,
            workspace,
            profile,
            participation,
            runtime,
            latest_operation,
            "latest-conversation",
        )?;
        activate_target(
            &catalog,
            workspace,
            profile,
            participation,
            Uuid::from_u128(0xD22),
            latest,
        )?;
        catalog.connection.execute(
            "UPDATE ops_agent_provision_operations SET updated_at_ms=2 WHERE operation_id=?1",
            params![latest_operation.as_bytes()],
        )?;
        let page = catalog.workspace_chat_targets(workspace, profile, 8)?;
        assert_eq!(page.targets().len(), 1);
        assert_eq!(page.targets()[0].operation_id(), latest_operation);
        assert_eq!(
            page.targets()[0].provider_conversation_id(),
            "latest-conversation"
        );
        assert!(!page.truncated());
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn reserved_newer_operation_does_not_suppress_the_activated_route() -> Result<(), Box<dyn Error>>
    {
        let (root, catalog) = catalog()?;
        let (workspace, profile, runtime) = ids(0xE00);
        let participation = Uuid::from_u128(0xE10);
        let participant = Uuid::from_u128(0xE11);
        let first_operation = Uuid::from_u128(0xE20);
        let newer_operation = Uuid::from_u128(0xE21);
        let first = seed_target(
            &catalog,
            workspace,
            profile,
            participation,
            runtime,
            first_operation,
            "old-conversation",
        )?;
        activate_target(
            &catalog,
            workspace,
            profile,
            participation,
            participant,
            first,
        )?;
        let newer = seed_target(
            &catalog,
            workspace,
            profile,
            participation,
            runtime,
            newer_operation,
            "same-conversation",
        )?;
        catalog.connection.execute(
            "UPDATE ops_agent_provision_operations SET updated_at_ms=2 WHERE operation_id=?1",
            params![newer_operation.as_bytes()],
        )?;

        let reserved = catalog.workspace_chat_targets(workspace, profile, 8)?;
        assert_eq!(reserved.targets().len(), 2);
        assert_eq!(reserved.targets()[0].operation_id(), newer_operation);
        assert_eq!(reserved.targets()[1].operation_id(), first_operation);

        catalog.connection.execute(
            "DELETE FROM ops_current_effect_bindings WHERE workspace_id=?1 AND participant_id=?2",
            params![workspace.into_uuid().as_bytes(), participant.as_bytes()],
        )?;
        activate_replacement_target(
            &catalog,
            workspace,
            profile,
            participation,
            participant,
            first,
            newer,
        )?;
        let activated = catalog.workspace_chat_targets(workspace, profile, 8)?;
        assert_eq!(activated.targets().len(), 1);
        assert_eq!(activated.targets()[0].operation_id(), newer_operation);
        assert!(!activated.truncated());
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn target_page_reports_bound_truncation() -> Result<(), Box<dyn Error>> {
        let (root, catalog) = catalog()?;
        let (workspace, profile, _) = ids(0x900);
        for index in 0..9_u128 {
            let runtime =
                AgentProfileRuntimeSessionId::from_uuid(Uuid::from_u128(0xA00 + index * 2 + 1))
                    .unwrap_or_else(|_| unreachable!());
            seed_target(
                &catalog,
                workspace,
                profile,
                Uuid::from_u128(0xB00 + index),
                runtime,
                Uuid::from_u128(0xC00 + index),
                &format!("conversation-{index}"),
            )?;
        }
        let page = catalog.workspace_chat_targets(workspace, profile, 8)?;
        assert_eq!(page.targets().len(), 8);
        assert!(page.truncated());
        fs::remove_dir_all(root)?;
        Ok(())
    }
}

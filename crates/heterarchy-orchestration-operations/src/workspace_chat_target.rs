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

use crate::{OperationsCatalog, OperationsError, wire};

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

fn read_workspace_chat_targets(
    tx: &Transaction<'_>,
    workspace_id: WorkspaceId,
    profile_id: AgentProfileId,
    query_limit: i64,
    limit: usize,
) -> Result<WorkspaceChatTargetPage, OperationsError> {
    let mut statement = tx.prepare(
        "WITH confirmed AS (
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
                p.provider_conversation_id
         FROM ops_agent_provision_operations AS p
         JOIN ops_agent_requests AS r
           ON r.request_id=p.request_id
          AND r.workspace_id=?3
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
                     newer.updated_at_ms > p.updated_at_ms
                     OR (
                         newer.updated_at_ms = p.updated_at_ms
                         AND newer.operation_id > p.operation_id
                     )
                 )
           )
         ORDER BY p.updated_at_ms DESC, p.operation_id DESC
         LIMIT ?5",
    )?;
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
    decode_workspace_chat_target_values(
        operation_id,
        activity_id,
        profile_id,
        participation_id,
        runtime_session_id,
        provider_conversation_id,
    )
}

fn decode_workspace_chat_target_values(
    operation_id: Vec<u8>,
    activity_id: Vec<u8>,
    profile_id: Vec<u8>,
    participation_id: Vec<u8>,
    runtime_session_id: Vec<u8>,
    provider_conversation_id: String,
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
    ) -> Result<(), Box<dyn Error>> {
        let request = Uuid::now_v7();
        let claim = Uuid::now_v7();
        let activity = Uuid::now_v7();
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
            params![request.as_bytes(), workspace.into_uuid().as_bytes(), activity.as_bytes(), Uuid::now_v7().as_bytes(), Uuid::now_v7().as_bytes()],
        )?;
        connection.execute(
            "INSERT INTO ops_agent_request_resolutions(request_id,slot_ordinal,resolution_kind,claim_id,profile_id,profile_revision,runtime_session_id,runtime_session_revision,policy_revision,reserved_at_ms) VALUES(?1,0,2,?2,?3,1,?4,1,1,1)",
            params![request.as_bytes(), claim.as_bytes(), profile.into_uuid().as_bytes(), runtime.into_uuid().as_bytes()],
        )?;
        connection.execute(
            "INSERT INTO ops_agent_provision_operations(operation_id,request_id,slot_ordinal,effect_kind,provider_family,profile_id,runtime_session_id,state,provider_conversation_id,prepared_at_ms,updated_at_ms,revision) VALUES(?1,?2,0,1,1,?3,?4,5,?5,1,1,1)",
            params![operation.as_bytes(), request.as_bytes(), profile.into_uuid().as_bytes(), runtime.into_uuid().as_bytes(), conversation],
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
        seed_target(
            &catalog,
            workspace,
            profile,
            Uuid::from_u128(0xD20),
            runtime,
            first_operation,
            "old-conversation",
        )?;
        seed_target(
            &catalog,
            workspace,
            profile,
            Uuid::from_u128(0xD21),
            runtime,
            latest_operation,
            "latest-conversation",
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

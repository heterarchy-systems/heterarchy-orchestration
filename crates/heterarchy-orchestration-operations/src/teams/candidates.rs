//! Bounded current logical members that an operator may select for Team formation.
//!
//! This read is an observation. Atomic formation revalidates every exact
//! Activity/Agent/Session binding before assigning roles.

use forge_domain::{ActivityId, AgentInstanceId, AgentSessionId, WorkspaceId};
use rusqlite::params;
use uuid::Uuid;

use crate::{OperationsCatalog, OperationsError, WorkspaceParticipationId, wire};

const MAX_CANDIDATES: u32 = 32;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TeamFormationCandidate {
    participation_id: WorkspaceParticipationId,
    participant_id: Uuid,
    profile_id: Uuid,
    membership_revision: u64,
    activity_id: ActivityId,
    agent_id: AgentInstanceId,
    session_id: AgentSessionId,
    display_name: String,
}

impl TeamFormationCandidate {
    #[must_use]
    pub const fn participation_id(&self) -> WorkspaceParticipationId {
        self.participation_id
    }
    #[must_use]
    pub const fn participant_id(&self) -> Uuid {
        self.participant_id
    }
    #[must_use]
    pub const fn profile_id(&self) -> Uuid {
        self.profile_id
    }
    #[must_use]
    pub const fn membership_revision(&self) -> u64 {
        self.membership_revision
    }
    #[must_use]
    pub const fn activity_id(&self) -> ActivityId {
        self.activity_id
    }
    #[must_use]
    pub const fn agent_id(&self) -> AgentInstanceId {
        self.agent_id
    }
    pub const fn session_id(&self) -> AgentSessionId {
        self.session_id
    }
    #[must_use]
    pub fn display_name(&self) -> &str {
        &self.display_name
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TeamFormationCandidatePage {
    candidates: Vec<TeamFormationCandidate>,
    next_participant: Option<Uuid>,
}

impl TeamFormationCandidatePage {
    #[must_use]
    pub fn candidates(&self) -> &[TeamFormationCandidate] {
        &self.candidates
    }
    #[must_use]
    pub const fn next_participant(&self) -> Option<Uuid> {
        self.next_participant
    }
}

impl OperationsCatalog {
    /// Read one logical-participant keyset page after filtering current Ready bindings.
    /// Multiple Activity bindings for the same logical member yield one candidate.
    ///
    /// # Errors
    /// Rejects invalid bounds, unverified Workspaces and corrupt identity projections.
    pub fn team_formation_candidates(
        &self,
        workspace: WorkspaceId,
        after_participant: Option<Uuid>,
        limit: u32,
    ) -> Result<TeamFormationCandidatePage, OperationsError> {
        if workspace.into_uuid().is_nil()
            || after_participant.is_some_and(|id| id.is_nil())
            || !(1..=MAX_CANDIDATES).contains(&limit)
        {
            return Err(OperationsError::Bounds("Team formation candidate page"));
        }
        let tx = self.connection.unchecked_transaction()?;
        super::ensure_verified_workspace(&tx, workspace)?;
        let mut statement = tx.prepare(
            "SELECT substr(m.participation_id,1,17),substr(l.participant_id,1,17),substr(m.agent_profile_id,1,17),l.membership_revision,substr(b.activity_id,1,17),substr(b.agent_id,1,17),substr(l.session_id,1,17),substr(l.display_name,1,129)
             FROM ops_workspace_participation_mappings m
             JOIN ops_workspace_participations p ON p.workspace_id=m.workspace_id AND p.participation_id=m.participation_id AND p.agent_profile_id=m.agent_profile_id
             JOIN ops_logical_members l ON l.workspace_id=m.workspace_id AND l.participant_id=m.participant_id
             JOIN ops_current_effect_bindings b ON b.workspace_id=l.workspace_id AND b.participant_id=l.participant_id AND b.session_id=l.session_id
             WHERE m.workspace_id=?1 AND (?2 IS NULL OR l.participant_id>?2)
               AND b.activity_id=(SELECT MIN(b2.activity_id) FROM ops_current_effect_bindings b2 WHERE b2.workspace_id=l.workspace_id AND b2.participant_id=l.participant_id AND b2.session_id=l.session_id)
             ORDER BY l.participant_id LIMIT ?3",
        )?;
        let rows = statement
            .query_map(
                params![
                    workspace.into_uuid().as_bytes(),
                    after_participant.map(Uuid::into_bytes),
                    i64::from(limit) + 1
                ],
                |row| {
                    Ok(RawCandidate {
                        participation: row.get(0)?,
                        participant: row.get(1)?,
                        profile: row.get(2)?,
                        revision: row.get(3)?,
                        activity: row.get(4)?,
                        agent: row.get(5)?,
                        session: row.get(6)?,
                        display_name: row.get(7)?,
                    })
                },
            )?
            .collect::<Result<Vec<_>, _>>()?;
        drop(statement);
        let has_more = rows.len()
            > usize::try_from(limit)
                .map_err(|_| OperationsError::Bounds("Team candidate limit"))?;
        let candidates = rows
            .into_iter()
            .take(
                usize::try_from(limit)
                    .map_err(|_| OperationsError::Bounds("Team candidate limit"))?,
            )
            .map(RawCandidate::decode)
            .collect::<Result<Vec<_>, _>>()?;
        let next_participant = has_more
            .then(|| {
                candidates
                    .last()
                    .map(TeamFormationCandidate::participant_id)
            })
            .flatten();
        tx.commit()?;
        Ok(TeamFormationCandidatePage {
            candidates,
            next_participant,
        })
    }
}

struct RawCandidate {
    participation: Vec<u8>,
    participant: Vec<u8>,
    profile: Vec<u8>,
    revision: i64,
    activity: Vec<u8>,
    agent: Vec<u8>,
    session: Vec<u8>,
    display_name: String,
}

impl RawCandidate {
    fn decode(self) -> Result<TeamFormationCandidate, OperationsError> {
        if self.display_name.is_empty() || self.display_name.len() > 128 || self.revision < 1 {
            return Err(OperationsError::Corrupt("Team formation candidate fields"));
        }
        Ok(TeamFormationCandidate {
            participation_id: WorkspaceParticipationId::from_uuid(decode_id(self.participation)?)?,
            participant_id: decode_id(self.participant)?,
            profile_id: decode_id(self.profile)?,
            membership_revision: u64::try_from(self.revision)
                .map_err(|_| OperationsError::Corrupt("Team candidate membership revision"))?,
            activity_id: ActivityId::from_uuid(decode_id(self.activity)?),
            agent_id: AgentInstanceId::from_uuid(decode_id(self.agent)?),
            session_id: AgentSessionId::from_uuid(decode_id(self.session)?),
            display_name: self.display_name,
        })
    }
}

fn decode_id(raw: Vec<u8>) -> Result<Uuid, OperationsError> {
    let id = Uuid::from_bytes(wire::id(raw)?);
    if id.is_nil() {
        return Err(OperationsError::Corrupt("Team candidate identity"));
    }
    Ok(id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;

    fn seed_candidate(
        catalog: &OperationsCatalog,
        workspace: WorkspaceId,
        ordinal: u128,
        ready: bool,
    ) -> Result<Uuid, Box<dyn Error>> {
        let participant = Uuid::from_u128(ordinal + 0x1000);
        let profile = Uuid::from_u128(ordinal + 0x2000);
        let participation = Uuid::from_u128(ordinal + 0x3000);
        let session = Uuid::from_u128(ordinal + 0x4000);
        let connection = &catalog.connection;
        connection.execute(
            "INSERT INTO ops_logical_members(workspace_id,participant_id,session_id,principal_kind,principal_id,display_name,declared_role,transport_kind,membership_revision,created_at_ms) VALUES(?1,?2,?3,0,NULL,'Candidate','worker',1,1,1)",
            params![workspace.into_uuid().as_bytes(), participant.as_bytes(), session.as_bytes()],
        )?;
        connection.execute(
            "INSERT INTO ops_workspace_participations(workspace_id,agent_profile_id,participation_id,created_at_ms,revision) VALUES(?1,?2,?3,1,1)",
            params![workspace.into_uuid().as_bytes(), profile.as_bytes(), participation.as_bytes()],
        )?;
        connection.execute(
            "INSERT INTO ops_workspace_participation_mappings(request_id,workspace_id,participation_id,agent_profile_id,profile_revision,profile_mapping_request_id,session_id,session_revision,participant_id,participant_membership_revision,mapped_at_ms) VALUES(?1,?2,?3,?4,1,?5,?6,1,?7,1,1)",
            params![Uuid::from_u128(ordinal+0x5000).as_bytes(), workspace.into_uuid().as_bytes(), participation.as_bytes(), profile.as_bytes(), Uuid::from_u128(ordinal+0x6000).as_bytes(), session.as_bytes(), participant.as_bytes()],
        )?;
        if ready {
            for activity_offset in [0x7000, 0x8000] {
                connection.execute(
                    "INSERT INTO ops_current_effect_bindings(workspace_id,participant_id,activity_id,session_id,agent_id,source_kind,source_request_id) VALUES(?1,?2,?3,?4,?5,0,?6)",
                    params![workspace.into_uuid().as_bytes(), participant.as_bytes(), Uuid::from_u128(ordinal+activity_offset).as_bytes(), session.as_bytes(), Uuid::from_u128(ordinal+activity_offset+0x2000).as_bytes(), Uuid::from_u128(ordinal+activity_offset+0x4000).as_bytes()],
                )?;
            }
        }
        Ok(participant)
    }

    #[test]
    fn candidates_page_filters_readiness_deduplicates_bindings_and_uses_current_session()
    -> Result<(), Box<dyn Error>> {
        let root = std::env::temp_dir().join(format!("team-candidates-{}", Uuid::now_v7()));
        std::fs::create_dir(&root)?;
        let destination = Uuid::from_u128(0x11);
        let workspace = WorkspaceId::from_uuid(Uuid::from_u128(0x12));
        let catalog = OperationsCatalog::open(&root.join("operations.sqlite3"), destination)?;
        catalog.connection.execute(
            "INSERT INTO ops_imports(migration_id,workspace_id,destination_id,source_revision,phase,next_cursor,page_count,record_count,import_digest,verified_pages,verify_digest) VALUES(?1,?2,?3,1,2,NULL,5,0,?4,5,?4)",
            params![Uuid::from_u128(0x13).as_bytes(), workspace.into_uuid().as_bytes(), destination.as_bytes(), [0_u8;32].as_slice()],
        )?;
        let _unready = seed_candidate(&catalog, workspace, 1, false)?;
        let first = seed_candidate(&catalog, workspace, 2, true)?;
        let second = seed_candidate(&catalog, workspace, 3, true)?;
        let replacement_session = Uuid::from_u128(0xE001);
        // The mapping remains immutable origin evidence after Session replacement.
        catalog.connection.execute("UPDATE ops_logical_members SET session_id=?1 WHERE workspace_id=?2 AND participant_id=?3", params![replacement_session.as_bytes(), workspace.into_uuid().as_bytes(), first.as_bytes()])?;
        catalog.connection.execute("UPDATE ops_current_effect_bindings SET session_id=?1 WHERE workspace_id=?2 AND participant_id=?3", params![replacement_session.as_bytes(), workspace.into_uuid().as_bytes(), first.as_bytes()])?;
        let page = catalog.team_formation_candidates(workspace, None, 1)?;
        assert_eq!(page.candidates().len(), 1);
        assert_eq!(page.candidates()[0].participant_id(), first);
        assert_eq!(
            page.candidates()[0].session_id().into_uuid(),
            replacement_session
        );
        assert_eq!(page.candidates()[0].membership_revision(), 1);
        assert_eq!(page.next_participant(), Some(first));
        let next = catalog.team_formation_candidates(workspace, page.next_participant(), 1)?;
        assert_eq!(next.candidates().len(), 1);
        assert_eq!(next.candidates()[0].participant_id(), second);
        assert_eq!(next.next_participant(), None);
        assert!(
            catalog
                .team_formation_candidates(workspace, None, 33)
                .is_err()
        );
        assert!(
            catalog
                .team_formation_candidates(WorkspaceId::from_uuid(Uuid::from_u128(0xDEAD)), None, 1)
                .is_err()
        );
        drop(catalog);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }
}

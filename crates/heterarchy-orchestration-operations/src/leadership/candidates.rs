//! Bounded candidate observations from the same electorate and credit owners.

use super::{LeadershipScope, election_subject, work_credit, work_credit_subject};
use crate::{OperationsCatalog, OperationsError};
use std::collections::BTreeSet;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeadershipCandidate {
    participant_id: Uuid,
    subject_id: Uuid,
    profile_id: Option<Uuid>,
    display_name: Option<String>,
    credits: i64,
    qualified: bool,
}
impl LeadershipCandidate {
    #[must_use]
    pub const fn participant_id(&self) -> Uuid {
        self.participant_id
    }
    #[must_use]
    pub const fn subject_id(&self) -> Uuid {
        self.subject_id
    }
    #[must_use]
    pub const fn profile_id(&self) -> Option<Uuid> {
        self.profile_id
    }
    #[must_use]
    pub fn display_name(&self) -> Option<&str> {
        self.display_name.as_deref()
    }
    #[must_use]
    pub const fn credits(&self) -> i64 {
        self.credits
    }
    #[must_use]
    pub const fn qualified(&self) -> bool {
        self.qualified
    }
}
impl OperationsCatalog {
    /// Observe current candidates; this grants no nomination or leader role.
    ///
    /// # Errors
    /// Rejects invalid scopes, oversized electorates and unavailable evidence.
    pub fn leadership_candidates(
        &self,
        scope: LeadershipScope,
        now_ms: i64,
    ) -> Result<Vec<LeadershipCandidate>, OperationsError> {
        let tx = self.connection.unchecked_transaction()?;
        let electorate = election_subject::snapshot_ready_subjects_for_scope(&tx, scope)?;
        let scores = match scope.team_id() {
            Some(team) => {
                work_credit::team_work_credit_scores_at(&tx, scope.workspace_id(), team, now_ms)?
            }
            None => work_credit::work_credit_scores_at(&tx, scope.workspace_id(), now_ms)?,
        };
        let mut seen = BTreeSet::new();
        let mut candidates = Vec::new();
        for participant in electorate.participant_ids() {
            let (subject, _, credits) = work_credit_subject::subject_score_for_participant(
                &tx,
                scope.workspace_id(),
                &scores,
                participant,
            )?;
            if !seen.insert((subject.kind(), subject.subject_id())) {
                continue;
            }
            candidates.push(LeadershipCandidate {
                participant_id: participant,
                subject_id: subject.subject_id(),
                profile_id: subject
                    .profile_id()
                    .map(heterarchy_orchestration_sessions::AgentProfileId::into_uuid),
                display_name: super::member_display_name(&tx, scope.workspace_id(), participant)?,
                credits,
                qualified: super::candidate_qualifies(&scores, credits)?,
            });
        }
        crate::catalog::commit_read(tx, self.destination)?;
        Ok(candidates)
    }
}

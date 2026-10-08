//! Deterministic admission policy for GPT-initiated Agent capability requests.
//!
//! This policy does not resolve, wake, provision, or create provider effects.
//! It only bounds how many additional Agents one request may ask Orchestration
//! to resolve. Durable request state is owned by the Operations catalog.

/// P0 safety ceiling for one `agent_request`.
///
/// The value is deliberately small to prevent one GPT tool call from creating
/// a wide fan-out. A future Workspace policy may configure a smaller value but
/// may never exceed this hard ceiling.
pub const MAX_AGENT_REQUEST_COUNT: u8 = 4;

/// Resolver-visible availability. These values are scheduling facts only and
/// never imply Workspace membership or effect authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentResolverAvailability {
    Ready,
    OfflineResumable,
    OfflineNeedsSession,
    Busy,
    Transitioning,
    Error,
}

/// One capability-compatible Profile candidate. The stable key is an opaque
/// 16-byte Profile identity used only for deterministic tie-breaking.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AgentResolverCandidate {
    profile_key: [u8; 16],
    current_scope: bool,
    availability: AgentResolverAvailability,
}

impl AgentResolverCandidate {
    #[must_use]
    pub const fn new(
        profile_key: [u8; 16],
        current_scope: bool,
        availability: AgentResolverAvailability,
    ) -> Self {
        Self {
            profile_key,
            current_scope,
            availability,
        }
    }
}

/// Deterministic resolver action. Provider effects are not executed by policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentResolverActionKind {
    ReuseCurrentScope,
    ReuseReadyRuntime,
    WakeExistingRuntime,
    WakeNewRuntime,
    ProvisionNew,
}

/// One requested Agent slot after deterministic resolution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AgentResolverSlotDecision {
    slot_ordinal: u8,
    action: AgentResolverActionKind,
    candidate_index: Option<usize>,
}

impl AgentResolverSlotDecision {
    #[must_use]
    pub const fn slot_ordinal(self) -> u8 {
        self.slot_ordinal
    }

    #[must_use]
    pub const fn action(self) -> AgentResolverActionKind {
        self.action
    }

    #[must_use]
    pub const fn candidate_index(self) -> Option<usize> {
        self.candidate_index
    }
}

/// Complete pure resolver result. Truncated observations fail closed instead
/// of silently selecting from an incomplete candidate set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentResolverDecision {
    Resolved(Vec<AgentResolverSlotDecision>),
    Indeterminate,
}

/// Select up to four unique reusable Profiles and fill remaining slots with
/// `ProvisionNew`. Within one action class, Profile identity bytes provide the
/// stable tie-break; no `WorkCredit`, recency, or affinity score is used yet.
#[must_use]
pub fn evaluate_agent_resolver(
    requested_count: u8,
    candidates: &[AgentResolverCandidate],
    candidates_truncated: bool,
    current_scope_truncated: bool,
) -> AgentResolverDecision {
    if requested_count == 0
        || requested_count > MAX_AGENT_REQUEST_COUNT
        || candidates_truncated
        || current_scope_truncated
    {
        return AgentResolverDecision::Indeterminate;
    }
    let mut remaining = (0..candidates.len()).collect::<Vec<_>>();
    remaining.sort_by_key(|index| {
        let candidate = candidates[*index];
        (candidate_rank(candidate), candidate.profile_key)
    });
    let mut slots = Vec::with_capacity(usize::from(requested_count));
    for slot_ordinal in 0..requested_count {
        let next = remaining
            .iter()
            .position(|index| candidate_action(candidates[*index]).is_some());
        let (action, candidate_index) = match next {
            Some(position) => {
                let index = remaining.remove(position);
                let Some(action) = candidate_action(candidates[index]) else {
                    return AgentResolverDecision::Indeterminate;
                };
                (action, Some(index))
            }
            None => (AgentResolverActionKind::ProvisionNew, None),
        };
        slots.push(AgentResolverSlotDecision {
            slot_ordinal,
            action,
            candidate_index,
        });
    }
    AgentResolverDecision::Resolved(slots)
}

const fn candidate_rank(candidate: AgentResolverCandidate) -> u8 {
    match candidate_action(candidate) {
        Some(AgentResolverActionKind::ReuseCurrentScope) => 0,
        Some(AgentResolverActionKind::ReuseReadyRuntime) => 1,
        Some(AgentResolverActionKind::WakeExistingRuntime) => 2,
        Some(AgentResolverActionKind::WakeNewRuntime) => 3,
        Some(AgentResolverActionKind::ProvisionNew) | None => 4,
    }
}

const fn candidate_action(candidate: AgentResolverCandidate) -> Option<AgentResolverActionKind> {
    if candidate.current_scope {
        match candidate.availability {
            AgentResolverAvailability::Ready => {
                return Some(AgentResolverActionKind::ReuseCurrentScope);
            }
            AgentResolverAvailability::OfflineResumable
            | AgentResolverAvailability::OfflineNeedsSession
            | AgentResolverAvailability::Busy
            | AgentResolverAvailability::Transitioning
            | AgentResolverAvailability::Error => {}
        }
    }
    match candidate.availability {
        AgentResolverAvailability::Ready => Some(AgentResolverActionKind::ReuseReadyRuntime),
        AgentResolverAvailability::OfflineResumable => {
            Some(AgentResolverActionKind::WakeExistingRuntime)
        }
        AgentResolverAvailability::OfflineNeedsSession => {
            Some(AgentResolverActionKind::WakeNewRuntime)
        }
        AgentResolverAvailability::Busy
        | AgentResolverAvailability::Transitioning
        | AgentResolverAvailability::Error => None,
    }
}

/// Deterministic admission facts for one capability request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AgentRequestAdmissionInput {
    requested_count: u8,
    configured_max_count: u8,
}

impl AgentRequestAdmissionInput {
    #[must_use]
    pub const fn new(requested_count: u8, configured_max_count: u8) -> Self {
        Self {
            requested_count,
            configured_max_count,
        }
    }
}

/// Admission result. Rejection is deterministic and carries no side effect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentRequestAdmissionDecision {
    Admit { count: u8 },
    Reject,
}

/// Evaluate one request count against the configured bound and hard ceiling.
#[must_use]
pub const fn evaluate_agent_request_admission(
    input: AgentRequestAdmissionInput,
) -> AgentRequestAdmissionDecision {
    if input.requested_count == 0
        || input.configured_max_count == 0
        || input.configured_max_count > MAX_AGENT_REQUEST_COUNT
        || input.requested_count > input.configured_max_count
    {
        AgentRequestAdmissionDecision::Reject
    } else {
        AgentRequestAdmissionDecision::Admit {
            count: input.requested_count,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolver_orders_reuse_then_wake_then_new_without_reusing_busy_profiles() {
        let candidates = [
            AgentResolverCandidate::new(
                [4; 16],
                false,
                AgentResolverAvailability::OfflineNeedsSession,
            ),
            AgentResolverCandidate::new([3; 16], false, AgentResolverAvailability::Ready),
            AgentResolverCandidate::new([9; 16], true, AgentResolverAvailability::Ready),
            AgentResolverCandidate::new(
                [2; 16],
                false,
                AgentResolverAvailability::OfflineResumable,
            ),
            AgentResolverCandidate::new([1; 16], true, AgentResolverAvailability::Busy),
        ];
        let first = evaluate_agent_resolver(4, &candidates, false, false);
        assert!(matches!(first, AgentResolverDecision::Resolved(_)));
        if let AgentResolverDecision::Resolved(slots) = first {
            assert_eq!(
                slots.iter().map(|slot| slot.action()).collect::<Vec<_>>(),
                [
                    AgentResolverActionKind::ReuseCurrentScope,
                    AgentResolverActionKind::ReuseReadyRuntime,
                    AgentResolverActionKind::WakeExistingRuntime,
                    AgentResolverActionKind::WakeNewRuntime,
                ]
            );
        }
        let reusable = &candidates[..3];
        let second = evaluate_agent_resolver(4, reusable, false, false);
        assert!(matches!(second, AgentResolverDecision::Resolved(_)));
        if let AgentResolverDecision::Resolved(slots) = second {
            assert_eq!(slots[3].action(), AgentResolverActionKind::ProvisionNew);
        }
    }
}

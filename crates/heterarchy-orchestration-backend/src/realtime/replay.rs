//! Pure replay / resume decision policy.
//!
//! Given a parsed cursor and a snapshot of the replay window, decide whether a
//! subscriber starts live, replays a bounded suffix, or must resync. The
//! decision is total: no input silently degrades to "start from now".

use super::cursor::EventCursor;

/// Snapshot of the hub replay window taken under the hub lock.
///
/// Invalid window states are unrepresentable: an empty window always has
/// `head == 0`, and a retained window always satisfies `1 <= oldest <= head`.
#[derive(Debug, Clone, Copy)]
pub(super) enum ReplayWindow {
    /// No event has been published in this epoch yet.
    Empty,
    /// At least one event is retained.
    Retained {
        /// Sequence of the most recently published event.
        head: u64,
        /// Sequence of the oldest retained event.
        oldest: u64,
    },
}

/// Why a client must discard its cursor and re-read canonical state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ResyncCause {
    /// The cursor names a different hub epoch. Epochs are process-local, so
    /// this is the guaranteed outcome after a backend restart.
    #[error("cursor epoch {cursor_epoch} does not match current epoch {current_epoch}")]
    EpochMismatch {
        /// Epoch carried by the rejected cursor.
        cursor_epoch: u64,
        /// Epoch of the current hub.
        current_epoch: u64,
    },
    /// A committed change was not fully represented by per-resource events.
    /// The boundary is an actual published head, including zero; no later
    /// retained event or invented sequence can stand in for the missed change.
    #[error("cursor sequence {cursor_sequence} crosses an observation gap at {barrier_sequence}")]
    ObservationGap {
        /// Sequence carried by the rejected cursor.
        cursor_sequence: u64,
        /// Last actual published sequence at the observation boundary.
        barrier_sequence: u64,
    },
    /// The cursor is older than the bounded replay window; the missed events
    /// no longer exist anywhere in this process.
    #[error(
        "cursor sequence {cursor_sequence} is older than the replay window (oldest retained {oldest_available})"
    )]
    RetentionGap {
        /// Sequence carried by the rejected cursor.
        cursor_sequence: u64,
        /// Oldest sequence still replayable.
        oldest_available: u64,
    },
}

/// A cursor that is not merely expired but invalid for the current stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CursorFault {
    /// Sequence beyond the hub head; the client claims an event this hub never
    /// published within its epoch.
    Future {
        /// Rejected cursor.
        cursor: EventCursor,
    },
}

/// What a newly admitted subscriber should receive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ReplayPlan {
    /// Attach to the live tail with nothing to replay.
    Live,
    /// Replay every retained event with sequence `> after_sequence`, then
    /// attach to the live tail.
    Replay {
        /// Replay starts strictly after this sequence.
        after_sequence: u64,
    },
    /// Reject the cursor: the client must re-read canonical state.
    Resync(ResyncCause),
}

/// Decide the resume outcome for one cursor against one window snapshot.
///
/// The invariant is: replay succeeds exactly when every event after the
/// cursor is still retained, i.e. when `cursor.sequence >= oldest - 1`.
/// A cursor exactly one behind the window (`oldest - 1`) is replayable, and
/// so is the origin cursor (`sequence 0`) whenever the whole history still
/// fits in the window.
pub(super) fn plan(
    cursor: Option<EventCursor>,
    epoch: u64,
    window: ReplayWindow,
) -> Result<ReplayPlan, CursorFault> {
    let Some(cursor) = cursor else {
        return Ok(ReplayPlan::Live);
    };
    if cursor.epoch != epoch {
        return Ok(ReplayPlan::Resync(ResyncCause::EpochMismatch {
            cursor_epoch: cursor.epoch,
            current_epoch: epoch,
        }));
    }
    if cursor.sequence > window.head() {
        return Err(CursorFault::Future { cursor });
    }
    match window {
        ReplayWindow::Empty => Ok(ReplayPlan::Live),
        ReplayWindow::Retained { head, oldest } => {
            // Invariant upheld by the hub: `oldest >= 1`, so `oldest - 1`
            // never underflows.
            if cursor.sequence < oldest - 1 {
                return Ok(ReplayPlan::Resync(ResyncCause::RetentionGap {
                    cursor_sequence: cursor.sequence,
                    oldest_available: oldest,
                }));
            }
            if cursor.sequence == head {
                return Ok(ReplayPlan::Live);
            }
            Ok(ReplayPlan::Replay {
                after_sequence: cursor.sequence,
            })
        }
    }
}

impl ReplayWindow {
    /// Sequence of the most recently published event in this window state.
    pub(super) const fn head(&self) -> u64 {
        match self {
            Self::Empty => 0,
            Self::Retained { head, .. } => *head,
        }
    }
}

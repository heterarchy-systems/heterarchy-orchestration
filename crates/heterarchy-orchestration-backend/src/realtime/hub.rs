//! Process-local realtime hub: bounded live fan-out plus a bounded replay
//! window.
//!
//! The hub is the single composition point that turns committed domain
//! changes into ordered observations. Sequence assignment, replay-window
//! insertion, and live broadcast share one short synchronous critical
//! section, so replay order, live order, and cursor order are one total
//! order by construction. No await ever runs under that lock; the broadcast
//! send is a bounded copy per receiver.

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context, Poll};
use std::time::{SystemTime, UNIX_EPOCH};

use futures_core::Stream as _;
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tokio_util::sync::CancellationToken;

use super::cursor::{EventCursor, MalformedCursor};
use super::event::RealtimeEvent;
use super::replay::{self, ReplayWindow, ResyncCause};

/// Retained events available for cursor replay.
///
/// 1024 compact envelopes occupy on the order of a few hundred kilobytes per
/// hub, which covers a reconnecting GUI catch-up burst without allowing
/// history to grow with traffic.
pub const REPLAY_WINDOW_CAPACITY: usize = 1024;

/// Per-subscriber live ring capacity before a subscriber is declared lagged.
///
/// A subscriber that stops polling for this many events is terminated with an
/// explicit `resync_required` instead of buffering without limit or silently
/// skipping; producers never block on a slow subscriber.
pub const LIVE_CHANNEL_CAPACITY: usize = 256;

/// Concurrently attached SSE subscribers admitted per hub.
///
/// The backend listener is loopback-only, so this bounds a single operator
/// UI plus a comfortable margin; excess connections are rejected with 503
/// instead of multiplying per-subscriber buffers without bound.
pub const MAX_ACTIVE_SUBSCRIBERS: usize = 64;

/// One published observation with its hub-assigned position.
#[derive(Debug)]
pub(crate) struct PublishedEvent {
    /// Hub epoch this event belongs to.
    pub(crate) epoch: u64,
    /// Hub-assigned monotonic sequence, starting at 1.
    pub(crate) sequence: u64,
    /// The committed-change notification.
    pub(crate) event: RealtimeEvent,
}

#[derive(Debug)]
struct HubState {
    next_sequence: u64,
    window: VecDeque<Arc<PublishedEvent>>,
    observation_gap: CancellationToken,
    replay_barrier: Option<u64>,
}

#[derive(Debug)]
struct HubInner {
    epoch: u64,
    live: broadcast::Sender<Arc<PublishedEvent>>,
    state: Mutex<HubState>,
    active_subscribers: AtomicUsize,
    replay_window_capacity: usize,
    max_active_subscribers: usize,
    shutdown: CancellationToken,
}

/// Shared handle to the process-local realtime hub.
///
/// Cloning is the intended sharing mechanism (one `Arc`); every handle
/// observes the same epoch, sequence, and replay window.
#[derive(Debug, Clone)]
pub struct RealtimeHub {
    inner: Arc<HubInner>,
}

impl RealtimeHub {
    /// Create the hub with the production bounds documented on
    /// [`REPLAY_WINDOW_CAPACITY`], [`LIVE_CHANNEL_CAPACITY`], and
    /// [`MAX_ACTIVE_SUBSCRIBERS`].
    #[must_use]
    pub fn new() -> Self {
        Self::with_capacities(
            REPLAY_WINDOW_CAPACITY,
            LIVE_CHANNEL_CAPACITY,
            MAX_ACTIVE_SUBSCRIBERS,
        )
    }

    /// Create a hub with explicit capacities; a zero capacity is raised to
    /// one. Test seam for exercising eviction and lag with small bounds.
    #[must_use]
    pub(super) fn with_capacities(
        replay_window: usize,
        live_channel: usize,
        max_active_subscribers: usize,
    ) -> Self {
        // `Receiver` creation from a `Sender` that outlives it in `HubInner`.
        let (live, _) = broadcast::channel::<Arc<PublishedEvent>>(live_channel.max(1));
        Self {
            inner: Arc::new(HubInner {
                epoch: allocate_epoch(),
                live,
                state: Mutex::new(HubState {
                    next_sequence: 0,
                    window: VecDeque::new(),
                    observation_gap: CancellationToken::new(),
                    replay_barrier: None,
                }),
                active_subscribers: AtomicUsize::new(0),
                replay_window_capacity: replay_window.max(1),
                max_active_subscribers: max_active_subscribers.max(1),
                shutdown: CancellationToken::new(),
            }),
        }
    }

    /// Epoch assigned to this hub instance.
    #[must_use]
    pub fn epoch(&self) -> u64 {
        self.inner.epoch
    }

    /// Publish one committed domain change and return its assigned sequence.
    ///
    /// Call this only after the owning domain transition has committed; the
    /// event is an observation of that commit, never its substitute.
    ///
    /// # Errors
    ///
    /// Returns [`PublishError::InvalidOccurredAt`] for a non-positive
    /// timestamp and [`PublishError::HubStatePoisoned`] when the hub internal
    /// state lock was poisoned by an earlier invariant breach; a poisoned hub
    /// refuses further work instead of guessing at its window.
    pub fn publish(&self, event: RealtimeEvent) -> Result<u64, PublishError> {
        if event.occurred_at_ms <= 0 {
            return Err(PublishError::InvalidOccurredAt {
                occurred_at_ms: event.occurred_at_ms,
            });
        }
        let mut state = self
            .inner
            .state
            .lock()
            .map_err(|_| PublishError::HubStatePoisoned)?;
        let sequence = state.next_sequence + 1;
        let published = Arc::new(PublishedEvent {
            epoch: self.inner.epoch,
            sequence,
            event,
        });
        state.next_sequence = sequence;
        state.window.push_back(Arc::clone(&published));
        while state.window.len() > self.inner.replay_window_capacity {
            state.window.pop_front();
        }
        // Sent under the state lock so live receivers observe the same total
        // order the replay window and cursors promise. The send is bounded
        // work (one `Arc` clone per receiver) and never awaits. An absent
        // audience is normal: the event stays replayable in the window.
        let _delivered = self.inner.live.send(published);
        Ok(sequence)
    }

    /// Attach a subscriber, honoring the `Last-Event-ID` resume contract.
    ///
    /// A `None` cursor attaches to the live tail only; a parsed cursor is
    /// either replayed from the bounded window or rejected explicitly.
    ///
    /// # Errors
    ///
    /// Returns [`SubscriptionError::InvalidCursor`] for a malformed or
    /// impossible cursor, [`SubscriptionError::ResyncRequired`] when the
    /// cursor names a foreign epoch or an evicted range,
    /// [`SubscriptionError::SubscriberLimitReached`] when admission is
    /// exhausted, and [`SubscriptionError::HubStatePoisoned`] when the hub
    /// internal state lock was poisoned.
    pub fn subscribe(
        &self,
        last_event_id: Option<&str>,
    ) -> Result<EventSubscription, SubscriptionError> {
        if self.inner.shutdown.is_cancelled() {
            return Err(SubscriptionError::ShuttingDown);
        }
        if self.inner.active_subscribers.fetch_add(1, Ordering::AcqRel)
            >= self.inner.max_active_subscribers
        {
            self.inner.active_subscribers.fetch_sub(1, Ordering::AcqRel);
            return Err(SubscriptionError::SubscriberLimitReached);
        }
        // Released by `Drop` on every path below, successful or not.
        let subscriber = ActiveSubscriber {
            inner: Arc::clone(&self.inner),
        };

        let cursor = match last_event_id {
            Some(raw) => Some(EventCursor::parse(raw).map_err(|error| {
                SubscriptionError::InvalidCursor(CursorInvalid::Malformed(error))
            })?),
            None => None,
        };

        let (replay, live, observation_gap) = {
            let state = self
                .inner
                .state
                .lock()
                .map_err(|_| SubscriptionError::HubStatePoisoned)?;
            if let Some(cursor) = cursor
                && cursor.epoch == self.inner.epoch
                && let Some(barrier) = state.replay_barrier
                && cursor.sequence <= barrier
            {
                return Err(SubscriptionError::ResyncRequired(
                    ResyncCause::ObservationGap {
                        cursor_sequence: cursor.sequence,
                        barrier_sequence: barrier,
                    },
                ));
            }
            let window = match state.window.front() {
                Some(oldest) => ReplayWindow::Retained {
                    head: state.next_sequence,
                    oldest: oldest.sequence,
                },
                // The hub keeps `next_sequence == 0` exactly while the window
                // is empty; any other combination is an invariant breach and
                // is refused instead of guessed at.
                None if state.next_sequence == 0 => ReplayWindow::Empty,
                None => return Err(SubscriptionError::HubStatePoisoned),
            };
            match replay::plan(cursor, self.inner.epoch, window) {
                Ok(replay::ReplayPlan::Live) => (
                    Vec::new(),
                    BroadcastStream::new(self.inner.live.subscribe()),
                    state.observation_gap.clone(),
                ),
                Ok(replay::ReplayPlan::Replay { after_sequence }) => {
                    // The receiver joins and the replay prefix is copied
                    // inside this same critical section, so replay and the
                    // live tail meet without a gap and without duplicates.
                    let live = BroadcastStream::new(self.inner.live.subscribe());
                    let owed = state
                        .window
                        .iter()
                        .filter(|event| event.sequence > after_sequence)
                        .cloned()
                        .collect::<Vec<_>>();
                    (owed, live, state.observation_gap.clone())
                }
                Ok(replay::ReplayPlan::Resync(cause)) => {
                    return Err(SubscriptionError::ResyncRequired(cause));
                }
                Err(replay::CursorFault::Future { cursor }) => {
                    return Err(SubscriptionError::InvalidCursor(CursorInvalid::Future {
                        cursor,
                    }));
                }
            }
        };
        Ok(EventSubscription {
            epoch: self.inner.epoch,
            replay: replay.into(),
            live,
            shutdown: self.inner.shutdown.clone(),
            observation_gap,
            subscriber,
        })
    }

    /// Stop stream admission and wake every attached stream for terminal close.
    pub fn shutdown(&self) {
        self.inner.shutdown.cancel();
    }

    /// A committed bulk change exceeded available per-resource observations.
    /// Existing streams must re-read canonical state, and old cursors cannot
    /// replay across this boundary. No resource revision or event is invented.
    ///
    /// # Errors
    /// Refuses a poisoned observation owner rather than guessing its cursor.
    pub fn require_snapshot_resync(&self) -> Result<(), PublishError> {
        let mut state = self
            .inner
            .state
            .lock()
            .map_err(|_| PublishError::HubStatePoisoned)?;
        state.replay_barrier = Some(state.next_sequence);
        state.observation_gap.cancel();
        state.observation_gap = CancellationToken::new();
        Ok(())
    }

    /// Bounded snapshot of hub counters for health/observability surfaces.
    #[must_use]
    pub fn snapshot(&self) -> HubSnapshot {
        let head = self.inner.state.lock();
        HubSnapshot {
            epoch: self.inner.epoch,
            head_sequence: head.as_ref().map_or(0, |state| state.next_sequence),
            active_subscribers: self.inner.active_subscribers.load(Ordering::Relaxed),
            state_poisoned: head.is_err(),
        }
    }
}

impl Default for RealtimeHub {
    fn default() -> Self {
        Self::new()
    }
}

/// A live subscription with its bounded replay prefix.
///
/// The replay prefix must be delivered before polling [`Self::poll_live`]; the
/// hub guarantees the two join without a gap or a duplicate.
#[derive(Debug)]
pub struct EventSubscription {
    /// Hub epoch every delivered event belongs to.
    epoch: u64,
    /// Retained events still owed to this subscriber, in sequence order.
    pub(crate) replay: VecDeque<Arc<PublishedEvent>>,
    /// Bounded live tail; lag on this channel is a terminal resync condition.
    live: BroadcastStream<Arc<PublishedEvent>>,
    pub(crate) shutdown: CancellationToken,
    pub(crate) observation_gap: CancellationToken,
    subscriber: ActiveSubscriber,
}

impl EventSubscription {
    /// The hub epoch this subscription belongs to.
    #[must_use]
    pub const fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Linearize final frame delivery with generation rotation. The closure
    /// only builds the selected frame; it never awaits, polls or does IO.
    pub(crate) fn deliver_current(
        &self,
        deliver: impl FnOnce() -> super::stream::StreamFrame,
    ) -> Result<super::stream::StreamFrame, super::stream::ResyncReason> {
        let _state = self
            .subscriber
            .inner
            .state
            .lock()
            .map_err(|_| super::stream::ResyncReason::AuthorityUnavailable)?;
        if self.shutdown.is_cancelled() {
            return Err(super::stream::ResyncReason::ServerShutdown);
        }
        if self.observation_gap.is_cancelled() {
            return Err(super::stream::ResyncReason::ObservationGap);
        }
        Ok(deliver())
    }

    /// Poll the bounded live tail for the next event.
    ///
    /// `Some(Err(Lagged(n)))` reports that `n` events were lost for this
    /// subscriber; the stream owner must surface an explicit resync and must
    /// not continue past the gap.
    pub(crate) fn poll_live(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Arc<PublishedEvent>, BroadcastStreamRecvError>>> {
        Pin::new(&mut self.live).poll_next(cx)
    }
}

#[derive(Debug)]
struct ActiveSubscriber {
    inner: Arc<HubInner>,
}

impl Drop for ActiveSubscriber {
    fn drop(&mut self) {
        self.inner.active_subscribers.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Counters describing one hub instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HubSnapshot {
    /// Epoch of the hub.
    pub epoch: u64,
    /// Sequence of the most recently published event, or `0` when poisoned.
    pub head_sequence: u64,
    /// Currently admitted live subscribers.
    pub active_subscribers: usize,
    /// The hub state lock is poisoned; every other counter is unreliable.
    pub state_poisoned: bool,
}

/// Why a publish was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum PublishError {
    /// The committing boundary supplied a non-positive timestamp.
    #[error("realtime event occurred_at_ms must be positive, got {occurred_at_ms}")]
    InvalidOccurredAt {
        /// The rejected timestamp.
        occurred_at_ms: i64,
    },
    /// Hub state was poisoned by an earlier invariant breach.
    #[error("realtime hub state is poisoned")]
    HubStatePoisoned,
}

/// Why a subscription was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SubscriptionError {
    /// The process owner has stopped realtime admission.
    #[error("realtime hub is shutting down")]
    ShuttingDown,
    /// The cursor could not be honored; the client must not stream with it.
    #[error("realtime cursor rejected: {0}")]
    InvalidCursor(#[from] CursorInvalid),
    /// The cursor names a foreign epoch or an evicted range.
    #[error("realtime resync required: {0}")]
    ResyncRequired(#[from] ResyncCause),
    /// Admission bound reached; retry after backing off.
    #[error("realtime subscriber admission limit reached")]
    SubscriberLimitReached,
    /// Hub state was poisoned by an earlier invariant breach.
    #[error("realtime hub state is poisoned")]
    HubStatePoisoned,
}

/// A cursor that cannot be honored on the current stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum CursorInvalid {
    /// The header value was not a well-formed cursor.
    #[error("{0}")]
    Malformed(#[from] MalformedCursor),
    /// The cursor claims a sequence this hub never published.
    #[error("cursor {cursor} is ahead of the stream head")]
    Future {
        /// Rejected cursor.
        cursor: EventCursor,
    },
}

/// Allocate a hub epoch: the process boot anchor in nanoseconds plus a
/// process-local counter.
///
/// Epochs are unique per hub within a process. Two distinct processes get
/// distinct anchors unless both booted within the same nanosecond, which
/// process startup time makes unreachable; this is the documented assumption
/// behind restart detection.
fn allocate_epoch() -> u64 {
    static BOOT_NANOS: OnceLock<u64> = OnceLock::new();
    static NEXT_IN_PROCESS: AtomicU64 = AtomicU64::new(0);
    let boot = *BOOT_NANOS.get_or_init(|| {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(1, |elapsed| {
                u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX)
            });
        if nanos == 0 { 1 } else { nanos }
    });
    boot.wrapping_add(NEXT_IN_PROCESS.fetch_add(1, Ordering::Relaxed))
}

#[cfg(test)]
mod tests {
    use std::task::{Context, Poll, Waker};

    use uuid::Uuid;

    use super::super::event::RealtimeEventKind;
    use super::*;

    fn team_event(revision: u64) -> RealtimeEvent {
        RealtimeEvent {
            kind: RealtimeEventKind::TeamChanged,
            resource_id: Uuid::nil(),
            resource_revision: revision,
            workspace_id: None,
            occurred_at_ms: 1,
        }
    }

    fn event_at(revision: u64, occurred_at_ms: i64) -> RealtimeEvent {
        RealtimeEvent {
            occurred_at_ms,
            ..team_event(revision)
        }
    }

    fn cursor(hub: &RealtimeHub, sequence: u64) -> String {
        EventCursor {
            epoch: hub.epoch(),
            sequence,
        }
        .encode()
    }

    fn expect_live(
        subscription: &mut EventSubscription,
        expected: &[u64],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        for expected_sequence in expected {
            match subscription.poll_live(&mut cx) {
                Poll::Ready(Some(Ok(event))) => assert_eq!(event.sequence, *expected_sequence),
                other => {
                    return Err(
                        format!("live event {expected_sequence} not delivered: {other:?}").into(),
                    );
                }
            }
        }
        Ok(())
    }

    #[test]
    fn replay_preserves_sequence_order_and_joins_live_without_gap_or_duplicate()
    -> Result<(), Box<dyn std::error::Error>> {
        let hub = RealtimeHub::with_capacities(8, 8, 4);
        for revision in 1..=5 {
            hub.publish(team_event(revision))?;
        }

        let mut from_origin = hub.subscribe(Some(&cursor(&hub, 0)))?;
        let origin_sequences: Vec<u64> = from_origin
            .replay
            .iter()
            .map(|event| event.sequence)
            .collect();
        assert_eq!(origin_sequences, vec![1, 2, 3, 4, 5]);

        // The live tail continues exactly after the replay prefix.
        hub.publish(team_event(6))?;
        expect_live(&mut from_origin, &[6])?;
        hub.publish(team_event(7))?;
        expect_live(&mut from_origin, &[7])?;

        // A mid-history cursor replays only its retained suffix, still in
        // sequence order.
        let resumed = hub.subscribe(Some(&cursor(&hub, 3)))?;
        let resumed_sequences: Vec<u64> =
            resumed.replay.iter().map(|event| event.sequence).collect();
        assert_eq!(resumed_sequences, vec![4, 5, 6, 7]);

        // A cursor at the head means live only.
        let at_head = hub.subscribe(Some(&cursor(&hub, 7)))?;
        assert!(at_head.replay.is_empty());
        Ok(())
    }

    #[test]
    fn faults_are_explicit_not_silent() -> Result<(), Box<dyn std::error::Error>> {
        let hub = RealtimeHub::with_capacities(4, 4, 8);
        assert!(matches!(
            hub.publish(event_at(1, 0)),
            Err(PublishError::InvalidOccurredAt { occurred_at_ms: 0 })
        ));
        for revision in 1..=8 {
            hub.publish(team_event(revision))?;
        }

        // Window retains 5..8; a cursor below that is a retention gap.
        match hub.subscribe(Some(&cursor(&hub, 1))) {
            Err(SubscriptionError::ResyncRequired(ResyncCause::RetentionGap {
                cursor_sequence: 1,
                oldest_available: 5,
            })) => {}
            other => return Err(format!("expected retention gap, got {other:?}").into()),
        }

        // A cursor from a foreign epoch (e.g. before a restart) must resync.
        let foreign = EventCursor {
            epoch: hub.epoch().wrapping_add(1),
            sequence: 1,
        }
        .encode();
        match hub.subscribe(Some(&foreign)) {
            Err(SubscriptionError::ResyncRequired(ResyncCause::EpochMismatch {
                current_epoch,
                ..
            })) => assert_eq!(current_epoch, hub.epoch()),
            other => return Err(format!("expected epoch mismatch, got {other:?}").into()),
        }

        // Malformed header values map to the invalid-cursor contract.
        match hub.subscribe(Some("not-a-cursor")) {
            Err(SubscriptionError::InvalidCursor(CursorInvalid::Malformed(_))) => {}
            other => return Err(format!("expected malformed cursor, got {other:?}").into()),
        }

        // Future sequences are invalid, never silently clamped to the tail.
        match hub.subscribe(Some(&cursor(&hub, 9))) {
            Err(SubscriptionError::InvalidCursor(CursorInvalid::Future { cursor })) => {
                assert_eq!(cursor.sequence, 9);
            }
            other => return Err(format!("expected future cursor, got {other:?}").into()),
        }
        Ok(())
    }

    #[test]
    fn subscriber_admission_is_bounded_and_released_on_drop()
    -> Result<(), Box<dyn std::error::Error>> {
        let hub = RealtimeHub::with_capacities(8, 8, 2);
        let first = hub.subscribe(None)?;
        let _second = hub.subscribe(None)?;
        assert_eq!(hub.snapshot().active_subscribers, 2);
        match hub.subscribe(None) {
            Err(SubscriptionError::SubscriberLimitReached) => {}
            other => return Err(format!("expected admission rejection, got {other:?}").into()),
        }
        drop(first);
        let _third = hub.subscribe(None)?;
        Ok(())
    }

    #[test]
    fn rejected_subscribes_release_their_admission_slot() -> Result<(), Box<dyn std::error::Error>>
    {
        let hub = RealtimeHub::with_capacities(4, 4, 8);
        for revision in 1..=8 {
            hub.publish(team_event(revision))?;
        }
        let before = hub.snapshot().active_subscribers;
        for rejected in [
            Some("not-a-cursor".to_owned()),
            Some(cursor(&hub, 1)),      // retention gap
            Some(cursor(&hub, 9)),      // future
            Some("12345:1".to_owned()), // foreign epoch
        ] {
            if hub.subscribe(rejected.as_deref()).is_ok() {
                return Err("expected the subscription to be rejected".into());
            }
        }
        assert_eq!(hub.snapshot().active_subscribers, before);
        Ok(())
    }
}

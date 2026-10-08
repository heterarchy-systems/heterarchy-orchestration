//! Ordered delivery stream for one subscription, independent of any
//! transport.
//!
//! The stream first drains the subscription's bounded replay prefix, then
//! tails the bounded live channel. Falling behind the live channel is a
//! terminal condition: the subscriber receives one [`ResyncNotice`] frame and
//! the stream ends, so loss is always explicit and never silently skipped.
//!
//! The stream owns its whole [`EventSubscription`], so the subscriber
//! admission slot is held exactly as long as the stream itself.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures_core::Stream;
use serde::Serialize;
use tokio::time::{Interval, MissedTickBehavior};
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tokio_util::sync::WaitForCancellationFutureOwned;
use uuid::Uuid;

use super::hub::{EventSubscription, PublishedEvent};

/// Maximum number of non-matching events examined in one poll.
const MAX_FILTER_CANDIDATES_PER_POLL: usize = 32;
const AUTHORIZATION_CHECK_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);

/// One ordered frame delivered to a subscriber.
#[derive(Debug)]
pub(crate) enum StreamFrame {
    /// A published observation.
    Event(Arc<PublishedEvent>),
    /// Terminal notice; the stream ends after it is delivered.
    Resync(ResyncNotice),
}

/// Why the subscriber must discard its cursor and re-read canonical state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResyncNotice {
    /// What triggered the resync.
    pub reason: ResyncReason,
    /// Epoch of the hub the subscriber was attached to.
    pub current_epoch: u64,
    /// Last sequence actually delivered on this stream (`0` if none).
    pub last_delivered_sequence: u64,
}

/// Trigger of a terminal resync.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case", rename_all_fields = "camelCase")]
pub enum ResyncReason {
    /// The subscriber fell behind the live channel; the skipped events may
    /// already have been overwritten in the ring.
    Lag {
        /// Number of events the receiver missed according to the channel.
        missed_events: u64,
    },
    /// The hub's live channel closed. Unreachable while this stream owns its
    /// subscription (the subscription's guard keeps the hub sender alive); the
    /// frame exists so a closed channel can never silently end the stream.
    HubClosed,
    /// The backend owner is draining existing connections for shutdown.
    ServerShutdown,
    /// The stream's authorization is no longer valid.
    AccessRevoked,
    /// The stream's authorization authority is temporarily unavailable.
    AuthorityUnavailable,
    /// A committed change cannot be represented completely by the bounded
    /// observation buffer. Re-read snapshots; do not replay a command.
    ObservationGap,
}

type AuthorizationFuture = Pin<Box<dyn Future<Output = Result<(), ResyncReason>> + Send + 'static>>;
type AuthorizationCallback = Box<dyn FnMut() -> AuthorizationFuture + Send + 'static>;

struct AuthorizationState {
    callback: AuthorizationCallback,
    interval: Interval,
    pending: Option<AuthorizationFuture>,
    authorized: bool,
}

impl AuthorizationState {
    fn new<F, Fut>(guard: F) -> Self
    where
        F: FnMut() -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), ResyncReason>> + Send + 'static,
    {
        let mut interval = tokio::time::interval_at(
            tokio::time::Instant::now() + AUTHORIZATION_CHECK_INTERVAL,
            AUTHORIZATION_CHECK_INTERVAL,
        );
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        Self {
            callback: Box::new({
                let mut guard = guard;
                move || Box::pin(guard())
            }),
            interval,
            pending: None,
            authorized: false,
        }
    }

    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), ResyncReason>> {
        if let Some(future) = self.pending.as_mut() {
            match future.as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(())) => {
                    self.pending = None;
                    self.authorized = true;
                }
                Poll::Ready(Err(reason)) => {
                    self.pending = None;
                    return Poll::Ready(Err(reason));
                }
            }
        }

        if !self.authorized {
            self.pending = Some((self.callback)());
            return self.poll(cx);
        }

        if self.interval.poll_tick(cx).is_ready() {
            self.pending = Some((self.callback)());
            return self.poll(cx);
        }

        Poll::Ready(Ok(()))
    }
}

/// Ordered [`StreamFrame`] source over one [`EventSubscription`].
pub(crate) struct EventStream {
    subscription: Option<EventSubscription>,
    replay: VecDeque<Arc<PublishedEvent>>,
    shutdown: Pin<Box<WaitForCancellationFutureOwned>>,
    observation_gap: Pin<Box<WaitForCancellationFutureOwned>>,
    current_epoch: u64,
    last_delivered_sequence: u64,
    workspace_id: Option<Uuid>,
    authorization: Option<AuthorizationState>,
}

impl EventStream {
    pub(crate) fn new(subscription: EventSubscription) -> Self {
        Self::new_with_workspace(subscription, None, None)
    }

    pub(crate) fn new_for_workspace(subscription: EventSubscription, workspace_id: Uuid) -> Self {
        Self::new_with_workspace(subscription, Some(workspace_id), None)
    }

    pub(crate) fn new_authorized<F, Fut>(
        subscription: EventSubscription,
        workspace_id: Option<Uuid>,
        guard: F,
    ) -> Self
    where
        F: FnMut() -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), ResyncReason>> + Send + 'static,
    {
        Self::new_with_workspace(
            subscription,
            workspace_id,
            Some(AuthorizationState::new(guard)),
        )
    }

    fn new_with_workspace(
        mut subscription: EventSubscription,
        workspace_id: Option<Uuid>,
        authorization: Option<AuthorizationState>,
    ) -> Self {
        let current_epoch = subscription.epoch();
        let replay = std::mem::take(&mut subscription.replay);
        let shutdown = Box::pin(subscription.shutdown.clone().cancelled_owned());
        let observation_gap = Box::pin(subscription.observation_gap.clone().cancelled_owned());
        Self {
            subscription: Some(subscription),
            replay,
            shutdown,
            observation_gap,
            current_epoch,
            last_delivered_sequence: 0,
            workspace_id,
            authorization,
        }
    }

    pub(crate) fn poll_frame(&mut self, cx: &mut Context<'_>) -> Poll<Option<StreamFrame>> {
        if self.subscription.is_none() {
            return Poll::Ready(None);
        }

        if let Some(frame) = self.poll_owner_notice(cx) {
            return Poll::Ready(Some(frame));
        }
        if let Some(outcome) = self.authorization.as_mut().map(|guard| guard.poll(cx)) {
            if let Some(frame) = self.poll_owner_notice(cx) {
                return Poll::Ready(Some(frame));
            }
            match outcome {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(())) => {}
                Poll::Ready(Err(reason)) => {
                    return Poll::Ready(Some(self.terminal_frame(reason)));
                }
            }
        }

        let mut examined = 0;
        loop {
            if let Some(frame) = self.poll_owner_notice(cx) {
                return Poll::Ready(Some(frame));
            }

            let event = if let Some(event) = self.replay.pop_front() {
                event
            } else {
                let outcome = match self.subscription.as_mut() {
                    Some(subscription) => subscription.poll_live(cx),
                    None => return Poll::Ready(None),
                };
                match outcome {
                    Poll::Ready(Some(Ok(event))) => event,
                    Poll::Ready(Some(Err(BroadcastStreamRecvError::Lagged(missed)))) => {
                        // The subscriber already lost events it was owed; the
                        // only honest frame left is an explicit resync.
                        // Skipping ahead to the newest event would hide the
                        // gap.
                        return Poll::Ready(Some(self.terminal_frame(ResyncReason::Lag {
                            missed_events: missed,
                        })));
                    }
                    Poll::Ready(None) => {
                        // The hub's live channel closed: the remaining history
                        // is undeliverable on this stream.
                        return Poll::Ready(Some(self.terminal_frame(ResyncReason::HubClosed)));
                    }
                    Poll::Pending => return Poll::Pending,
                }
            };

            examined += 1;
            if self
                .workspace_id
                .is_none_or(|workspace_id| event.event.workspace_id == Some(workspace_id))
            {
                return Poll::Ready(Some(self.deliver_event(event)));
            }
            if examined == MAX_FILTER_CANDIDATES_PER_POLL {
                // A foreign-event flood must yield even when the broadcast
                // receiver remains immediately ready.
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
        }
    }

    fn deliver_event(&mut self, event: Arc<PublishedEvent>) -> StreamFrame {
        let last_delivered = &mut self.last_delivered_sequence;
        let delivered = self.subscription.as_ref().map(|subscription| {
            subscription.deliver_current(|| {
                *last_delivered = event.sequence;
                StreamFrame::Event(event)
            })
        });
        match delivered {
            Some(Ok(frame)) => frame,
            Some(Err(reason)) => self.terminal_frame(reason),
            None => self.terminal_frame(ResyncReason::HubClosed),
        }
    }

    fn poll_owner_notice(&mut self, cx: &mut Context<'_>) -> Option<StreamFrame> {
        if self.shutdown.as_mut().poll(cx).is_ready() {
            Some(self.terminal_frame(ResyncReason::ServerShutdown))
        } else if self.observation_gap.as_mut().poll(cx).is_ready() {
            Some(self.terminal_frame(ResyncReason::ObservationGap))
        } else {
            None
        }
    }

    fn terminal_frame(&mut self, reason: ResyncReason) -> StreamFrame {
        self.subscription = None;
        self.replay.clear();
        self.authorization = None;
        StreamFrame::Resync(ResyncNotice {
            reason,
            current_epoch: self.current_epoch,
            last_delivered_sequence: self.last_delivered_sequence,
        })
    }
}

impl Stream for EventStream {
    type Item = StreamFrame;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().poll_frame(cx)
    }
}

#[cfg(test)]
mod tests {
    use std::task::{Context, Poll, Waker};

    #[test]
    fn committed_observation_gap_preempts_replay_and_rejects_old_cursors()
    -> Result<(), Box<dyn std::error::Error>> {
        let hub = RealtimeHub::with_capacities(8, 8, 2);
        hub.publish(team_event(1))?;
        let old_cursor = super::super::cursor::EventCursor {
            epoch: hub.epoch(),
            sequence: 1,
        }
        .encode();
        let origin = super::super::cursor::EventCursor {
            epoch: hub.epoch(),
            sequence: 0,
        }
        .encode();
        let mut replay = EventStream::new(hub.subscribe(Some(&origin))?);
        let mut live = EventStream::new(hub.subscribe(None)?);
        hub.require_snapshot_resync()?;
        assert_eq!(
            hub.snapshot().head_sequence,
            1,
            "resync must not invent an event or resource revision"
        );
        let mut cx = Context::from_waker(Waker::noop());
        for stream in [&mut replay, &mut live] {
            assert!(matches!(
                stream.poll_frame(&mut cx),
                Poll::Ready(Some(StreamFrame::Resync(ResyncNotice {
                    reason: ResyncReason::ObservationGap,
                    last_delivered_sequence: 0,
                    ..
                })))
            ));
            assert!(matches!(stream.poll_frame(&mut cx), Poll::Ready(None)));
        }
        assert_eq!(hub.snapshot().active_subscribers, 0);
        assert!(matches!(
            hub.subscribe(Some(&old_cursor)),
            Err(super::super::hub::SubscriptionError::ResyncRequired(_))
        ));
        let mut fresh = EventStream::new(hub.subscribe(None)?);
        hub.publish(team_event(2))?;
        assert!(
            matches!(fresh.poll_frame(&mut cx),Poll::Ready(Some(StreamFrame::Event(event))) if event.sequence==2)
        );
        let new_cursor = super::super::cursor::EventCursor {
            epoch: hub.epoch(),
            sequence: 2,
        }
        .encode();
        assert!(hub.subscribe(Some(&new_cursor)).is_ok());
        Ok(())
    }

    #[test]
    fn observation_gap_after_event_selection_prevents_delivery()
    -> Result<(), Box<dyn std::error::Error>> {
        let hub = RealtimeHub::with_capacities(8, 8, 1);
        hub.publish(team_event(1))?;
        let origin = super::super::cursor::EventCursor {
            epoch: hub.epoch(),
            sequence: 0,
        }
        .encode();
        let mut stream = EventStream::new(hub.subscribe(Some(&origin))?);
        // Force the exact interleaving: event selected, then gap committed,
        // then the final delivery step. No timing-dependent thread sleeps.
        let selected = stream
            .replay
            .pop_front()
            .ok_or("selected replay event missing")?;
        hub.require_snapshot_resync()?;
        assert!(matches!(
            stream.deliver_event(selected),
            StreamFrame::Resync(ResyncNotice {
                reason: ResyncReason::ObservationGap,
                last_delivered_sequence: 0,
                ..
            })
        ));
        assert_eq!(hub.snapshot().active_subscribers, 0);
        Ok(())
    }

    #[test]
    fn repeated_empty_head_gaps_preserve_generation_and_cursor_bounds()
    -> Result<(), Box<dyn std::error::Error>> {
        let hub = RealtimeHub::with_capacities(8, 8, 2);
        let origin = super::super::cursor::EventCursor {
            epoch: hub.epoch(),
            sequence: 0,
        }
        .encode();
        let mut cx = Context::from_waker(Waker::noop());
        for _ in 0..2 {
            let mut stream = EventStream::new(hub.subscribe(None)?);
            assert!(matches!(stream.poll_frame(&mut cx), Poll::Pending));
            hub.require_snapshot_resync()?;
            assert!(matches!(
                stream.poll_frame(&mut cx),
                Poll::Ready(Some(StreamFrame::Resync(ResyncNotice {
                    reason: ResyncReason::ObservationGap,
                    ..
                })))
            ));
            assert_eq!(hub.snapshot().head_sequence, 0);
            assert_eq!(hub.snapshot().active_subscribers, 0);
            assert!(matches!(
                hub.subscribe(Some(&origin)),
                Err(super::super::hub::SubscriptionError::ResyncRequired(_))
            ));
        }
        let mut fresh = EventStream::new(hub.subscribe(None)?);
        hub.publish(team_event(1))?;
        assert!(
            matches!(fresh.poll_frame(&mut cx), Poll::Ready(Some(StreamFrame::Event(event))) if event.sequence == 1)
        );
        Ok(())
    }

    use uuid::Uuid;

    use super::super::event::{RealtimeEvent, RealtimeEventKind};
    use super::super::hub::RealtimeHub;
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

    fn scoped_team_event(revision: u64, workspace_id: Option<Uuid>) -> RealtimeEvent {
        RealtimeEvent {
            resource_id: Uuid::from_u128(u128::from(revision)),
            workspace_id,
            ..team_event(revision)
        }
    }

    #[test]
    fn lagged_subscriber_receives_terminal_resync_and_stream_ends()
    -> Result<(), Box<dyn std::error::Error>> {
        let hub = RealtimeHub::with_capacities(64, 2, 8);
        let mut stream = EventStream::new(hub.subscribe(None)?);
        for revision in 1..=4 {
            hub.publish(team_event(revision))?;
        }

        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        match stream.poll_frame(&mut cx) {
            Poll::Ready(Some(StreamFrame::Resync(notice))) => {
                assert_eq!(notice.reason, ResyncReason::Lag { missed_events: 2 });
                assert_eq!(notice.current_epoch, hub.epoch());
                assert_eq!(notice.last_delivered_sequence, 0);
            }
            _ => return Err("expected a terminal resync frame".into()),
        }
        assert!(matches!(stream.poll_frame(&mut cx), Poll::Ready(None)));
        Ok(())
    }

    #[test]
    fn stream_drains_replay_prefix_in_order_before_live_tail()
    -> Result<(), Box<dyn std::error::Error>> {
        let hub = RealtimeHub::with_capacities(8, 8, 4);
        for revision in 1..=3 {
            hub.publish(team_event(revision))?;
        }
        let origin = super::super::cursor::EventCursor {
            epoch: hub.epoch(),
            sequence: 0,
        }
        .encode();
        let mut stream = EventStream::new(hub.subscribe(Some(&origin))?);
        hub.publish(team_event(4))?;

        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        let mut sequences = Vec::new();
        while let Poll::Ready(Some(frame)) = stream.poll_frame(&mut cx) {
            match frame {
                StreamFrame::Event(event) => sequences.push(event.sequence),
                StreamFrame::Resync(notice) => {
                    return Err(format!("unexpected resync: {notice:?}").into());
                }
            }
        }
        assert_eq!(sequences, vec![1, 2, 3, 4]);
        Ok(())
    }

    #[test]
    fn shutdown_closes_idle_and_replaying_streams_and_releases_admission()
    -> Result<(), Box<dyn std::error::Error>> {
        let hub = RealtimeHub::with_capacities(8, 8, 2);
        hub.publish(team_event(1))?;
        let cursor = super::super::cursor::EventCursor {
            epoch: hub.epoch(),
            sequence: 0,
        }
        .encode();
        let mut replaying = EventStream::new(hub.subscribe(Some(&cursor))?);
        let mut idle = EventStream::new(hub.subscribe(None)?);
        hub.shutdown();
        let mut cx = Context::from_waker(Waker::noop());
        for stream in [&mut replaying, &mut idle] {
            assert!(matches!(
                stream.poll_frame(&mut cx),
                Poll::Ready(Some(StreamFrame::Resync(ResyncNotice {
                    reason: ResyncReason::ServerShutdown,
                    ..
                })))
            ));
            assert!(matches!(stream.poll_frame(&mut cx), Poll::Ready(None)));
        }
        assert_eq!(hub.snapshot().active_subscribers, 0);
        assert!(matches!(
            hub.subscribe(None),
            Err(super::super::hub::SubscriptionError::ShuttingDown)
        ));
        Ok(())
    }

    #[test]
    fn scoped_stream_filters_replay_and_live_events_with_bounded_polling()
    -> Result<(), Box<dyn std::error::Error>> {
        let hub = RealtimeHub::with_capacities(128, 64, 4);
        let target_workspace = Uuid::from_u128(1);
        let foreign_workspace = Uuid::from_u128(2);
        for revision in 1..=32 {
            let workspace_id = if revision % 2 == 0 {
                None
            } else {
                Some(foreign_workspace)
            };
            hub.publish(scoped_team_event(revision, workspace_id))?;
        }
        hub.publish(scoped_team_event(33, Some(target_workspace)))?;

        let cursor = super::super::cursor::EventCursor {
            epoch: hub.epoch(),
            sequence: 0,
        }
        .encode();
        let mut stream =
            EventStream::new_for_workspace(hub.subscribe(Some(&cursor))?, target_workspace);
        let mut cx = Context::from_waker(Waker::noop());

        assert!(matches!(stream.poll_frame(&mut cx), Poll::Pending));
        match stream.poll_frame(&mut cx) {
            Poll::Ready(Some(StreamFrame::Event(event))) => {
                assert_eq!(event.sequence, 33);
                assert_eq!(event.event.resource_id, Uuid::from_u128(33));
                assert_eq!(event.event.workspace_id, Some(target_workspace));
            }
            other => return Err(format!("expected scoped replay event, got {other:?}").into()),
        }

        for revision in 34..=65 {
            let workspace_id = if revision % 2 == 0 {
                None
            } else {
                Some(foreign_workspace)
            };
            hub.publish(scoped_team_event(revision, workspace_id))?;
        }
        hub.publish(scoped_team_event(66, Some(target_workspace)))?;

        assert!(matches!(stream.poll_frame(&mut cx), Poll::Pending));
        match stream.poll_frame(&mut cx) {
            Poll::Ready(Some(StreamFrame::Event(event))) => {
                assert_eq!(event.sequence, 66);
                assert_eq!(event.event.resource_id, Uuid::from_u128(66));
                assert_eq!(event.event.workspace_id, Some(target_workspace));
            }
            other => return Err(format!("expected scoped live event, got {other:?}").into()),
        }

        hub.shutdown();
        match stream.poll_frame(&mut cx) {
            Poll::Ready(Some(StreamFrame::Resync(ResyncNotice {
                reason: ResyncReason::ServerShutdown,
                last_delivered_sequence,
                ..
            }))) => assert_eq!(last_delivered_sequence, 66),
            other => return Err(format!("expected scoped shutdown, got {other:?}").into()),
        }
        assert!(matches!(stream.poll_frame(&mut cx), Poll::Ready(None)));
        assert_eq!(hub.snapshot().active_subscribers, 0);
        Ok(())
    }

    #[tokio::test]
    async fn authorization_rejection_is_terminal_and_releases_subscription()
    -> Result<(), Box<dyn std::error::Error>> {
        for reason in [
            ResyncReason::AccessRevoked,
            ResyncReason::AuthorityUnavailable,
        ] {
            let hub = RealtimeHub::with_capacities(8, 8, 2);
            let mut stream =
                EventStream::new_authorized(hub.subscribe(None)?, None, move || async move {
                    Err::<(), _>(reason)
                });
            let mut cx = Context::from_waker(Waker::noop());
            match stream.poll_frame(&mut cx) {
                Poll::Ready(Some(StreamFrame::Resync(notice))) => {
                    assert_eq!(notice.reason, reason);
                    assert_eq!(notice.last_delivered_sequence, 0);
                }
                other => {
                    return Err(format!("expected authorization rejection, got {other:?}").into());
                }
            }
            assert_eq!(hub.snapshot().active_subscribers, 0);
            assert!(matches!(stream.poll_frame(&mut cx), Poll::Ready(None)));
        }
        Ok(())
    }

    #[tokio::test]
    async fn successful_authorization_allows_scoped_replay_and_live_events()
    -> Result<(), Box<dyn std::error::Error>> {
        let hub = RealtimeHub::with_capacities(8, 8, 2);
        let target_workspace = Uuid::from_u128(1);
        let foreign_workspace = Uuid::from_u128(2);
        hub.publish(scoped_team_event(1, Some(foreign_workspace)))?;
        hub.publish(scoped_team_event(2, Some(target_workspace)))?;
        let cursor = super::super::cursor::EventCursor {
            epoch: hub.epoch(),
            sequence: 0,
        }
        .encode();
        let mut stream = EventStream::new_authorized(
            hub.subscribe(Some(&cursor))?,
            Some(target_workspace),
            || async { Ok(()) },
        );
        let mut cx = Context::from_waker(Waker::noop());
        match stream.poll_frame(&mut cx) {
            Poll::Ready(Some(StreamFrame::Event(event))) => {
                assert_eq!(event.sequence, 2);
                assert_eq!(event.event.workspace_id, Some(target_workspace));
            }
            other => return Err(format!("expected authorized replay event, got {other:?}").into()),
        }

        hub.publish(scoped_team_event(3, Some(foreign_workspace)))?;
        hub.publish(scoped_team_event(4, Some(target_workspace)))?;
        match stream.poll_frame(&mut cx) {
            Poll::Ready(Some(StreamFrame::Event(event))) => {
                assert_eq!(event.sequence, 4);
                assert_eq!(event.event.workspace_id, Some(target_workspace));
            }
            other => return Err(format!("expected authorized live event, got {other:?}").into()),
        }
        drop(stream);
        assert_eq!(hub.snapshot().active_subscribers, 0);
        Ok(())
    }

    #[tokio::test]
    async fn shutdown_preempts_a_pending_authorization_guard()
    -> Result<(), Box<dyn std::error::Error>> {
        let hub = RealtimeHub::with_capacities(8, 8, 2);
        let mut stream = EventStream::new_authorized(hub.subscribe(None)?, None, || {
            std::future::pending::<Result<(), ResyncReason>>()
        });
        let mut cx = Context::from_waker(Waker::noop());
        assert!(matches!(stream.poll_frame(&mut cx), Poll::Pending));
        assert_eq!(hub.snapshot().active_subscribers, 1);

        hub.shutdown();
        match stream.poll_frame(&mut cx) {
            Poll::Ready(Some(StreamFrame::Resync(ResyncNotice {
                reason: ResyncReason::ServerShutdown,
                last_delivered_sequence,
                ..
            }))) => assert_eq!(last_delivered_sequence, 0),
            other => return Err(format!("expected shutdown during guard, got {other:?}").into()),
        }
        assert_eq!(hub.snapshot().active_subscribers, 0);
        assert!(matches!(stream.poll_frame(&mut cx), Poll::Ready(None)));
        Ok(())
    }
}

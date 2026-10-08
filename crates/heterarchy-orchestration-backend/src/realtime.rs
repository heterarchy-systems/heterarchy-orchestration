//! Bounded, resumable, observable realtime event subsystem for the
//! Orchestration backend.
//!
//! # Authority
//!
//! The realtime layer is an **observation boundary, not a state authority**.
//! Domain/catalog transitions commit first (SQLite, canonical state), then the
//! committing owner publishes a compact notification through
//! [`RealtimeHub::publish`]. The stream never becomes the source of truth:
//! clients reconcile observed events against the canonical HTTP queries, which
//! carry the authoritative revisions.
//!
//! # Delivery semantics (declared)
//!
//! `ordered live event + bounded process-local replay`, explicitly **not**
//! durable:
//!
//! * Ordering scope is the single hub-wide monotonic `sequence` inside one
//!   `epoch`; replay order, live order, and sequence assignment share one
//!   critical section, so every observer observes the same total order.
//! * Events are process-local. There is no durable event log behind this
//!   layer yet; a cursor from a previous process is rejected with
//!   `RESYNC_REQUIRED` instead of being silently honored.
//! * Losing the stream never loses state: clients re-read canonical state and
//!   re-attach with their last observed cursor.
//!
//! # Bounds (all explicit)
//!
//! | Bound | Value | Purpose |
//! | --- | --- | --- |
//! | Replay window | [`REPLAY_WINDOW_CAPACITY`] events | bounded catch-up for reconnecting clients |
//! | Per-subscriber live ring | [`LIVE_CHANNEL_CAPACITY`] events | a subscriber that stops polling is reported as lagged, never buffered without limit |
//! | Active subscribers | [`MAX_ACTIVE_SUBSCRIBERS`] | loopback backend admission bound; excess connections are rejected with 503 |
//! | Event payload | fixed fields, no free text | envelopes stay compact so fan-out clones are cheap |
//!
//! # Slow consumers
//!
//! A subscriber that falls more than [`LIVE_CHANNEL_CAPACITY`] events behind
//! receives one terminal `resync_required` SSE event and the stream ends. Old
//! events are never silently skipped, and producers are never blocked by a
//! slow subscriber.
//!
//! # Transport
//!
//! SSE (`axum::response::sse`) is the only transport. **A WebSocket transport
//! is introduced only when a true full-duplex requirement exists** (for
//! example an interactive terminal, which is likely Forge authority anyway).
//! Orchestration steering is `POST` command + SSE observation and does not
//! justify a bidirectional channel.

mod cursor;
mod event;
mod hub;
mod replay;
mod sse;
mod stream;

pub use cursor::{EventCursor, MalformedCursor};
pub use event::{RealtimeEvent, RealtimeEventKind};
pub use hub::{
    CursorInvalid, HubSnapshot, LIVE_CHANNEL_CAPACITY, MAX_ACTIVE_SUBSCRIBERS, PublishError,
    REPLAY_WINDOW_CAPACITY, RealtimeHub, SubscriptionError,
};
pub use replay::ResyncCause;
pub use sse::{
    RealtimeAdmissionError, events_handler, events_handler_authorized, events_handler_for_workspace,
};
pub use stream::{ResyncNotice, ResyncReason};

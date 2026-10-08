//! Typed resume cursor for the realtime observation stream.
//!
//! This module owns the entire `Last-Event-ID` encoding/parsing contract.
//! No other module interprets cursor text.

use std::fmt;

/// Maximum decimal digits considered for one cursor component.
///
/// `u64::MAX` renders in 20 digits; anything longer cannot be a valid
/// component and is rejected before parsing instead of overflowing.
const MAX_CURSOR_COMPONENT_DIGITS: usize = 20;

/// Resume cursor identifying the last event a client observed.
///
/// Wire form is `<epoch>:<sequence>`, both plain decimal digit runs without a
/// sign. `encode` emits unpadded values; `parse` tolerates zero padding in
/// input but accepts nothing beyond digits. `sequence` may be `0`, meaning
/// "start of this epoch" (no event carries sequence 0; published sequences
/// start at 1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EventCursor {
    /// Hub epoch the event belongs to. Epochs are process-local; every hub
    /// instance allocates a fresh epoch, so a pre-restart cursor never matches.
    pub epoch: u64,
    /// Monotonic sequence of the last observed event within `epoch`.
    pub sequence: u64,
}

impl EventCursor {
    /// Cursor for the origin of `epoch`: replay everything still retained.
    #[must_use]
    pub const fn origin(epoch: u64) -> Self {
        Self { epoch, sequence: 0 }
    }

    /// Parse the wire form of a cursor.
    ///
    /// # Errors
    ///
    /// Returns [`MalformedCursor`] for an absent `:`, non-decimal or signed
    /// components, oversized components, or values beyond `u64`.
    pub fn parse(raw: &str) -> Result<Self, MalformedCursor> {
        let (epoch_text, sequence_text) = raw.split_once(':').ok_or(MalformedCursor)?;
        let epoch = parse_component(epoch_text)?;
        let sequence = parse_component(sequence_text)?;
        Ok(Self { epoch, sequence })
    }

    /// Encode the wire form handed to clients as the SSE `id:` field.
    #[must_use]
    pub fn encode(self) -> String {
        format!("{}:{}", self.epoch, self.sequence)
    }
}

impl fmt::Display for EventCursor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}:{}", self.epoch, self.sequence)
    }
}

fn parse_component(text: &str) -> Result<u64, MalformedCursor> {
    if text.is_empty()
        || text.len() > MAX_CURSOR_COMPONENT_DIGITS
        || !text.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(MalformedCursor);
    }
    // Rejected oversized and non-digit input above; only magnitude can fail.
    text.parse::<u64>().map_err(|_| MalformedCursor)
}

/// A cursor string could not be interpreted at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("malformed realtime cursor; expected <epoch>:<sequence> in decimal")]
pub struct MalformedCursor;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_wire_form_round_trips_through_parse() {
        let cursor = EventCursor {
            epoch: 18_446_744_073_709_551_615,
            sequence: 42,
        };
        assert_eq!(EventCursor::parse(&cursor.encode()), Ok(cursor));
    }

    #[test]
    fn cursor_parse_rejects_signed_non_decimal_and_overflowing_components() {
        for raw in [
            "",
            "1",
            "1:",
            ":1",
            "+1:2",
            " 1:2",
            "1:2:3",
            "0x1:2",
            "1:+2",
            "9999999999999999999999:1",
            "1:99999999999999999999999",
        ] {
            assert_eq!(
                EventCursor::parse(raw),
                Err(MalformedCursor),
                "raw = {raw:?}"
            );
        }
        // Zero padding is tolerated on read and normalizes to the value.
        assert_eq!(
            EventCursor::parse("1:01"),
            Ok(EventCursor {
                epoch: 1,
                sequence: 1
            })
        );
    }
}

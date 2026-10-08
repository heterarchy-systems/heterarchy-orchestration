use crate::OperationsError;
use forge_persistence::{AgentOperationsExportCursor as Cursor, AgentOperationsInboxKey};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
#[derive(Serialize, Deserialize)]
enum StoredCursor {
    Participants(Option<[u8; 16]>),
    Presence(Option<[u8; 16]>),
    Events(Option<[u8; 16]>),
    Inbox(Option<([u8; 16], [u8; 16], [u8; 16])>),
    Coordinators(Option<[u8; 16]>),
}
pub(crate) fn encode(cursor: Cursor) -> Result<Vec<u8>, OperationsError> {
    let value = match cursor {
        Cursor::Participants(v) => {
            StoredCursor::Participants(v.map(|x| x.into_uuid().into_bytes()))
        }
        Cursor::Presence(v) => StoredCursor::Presence(v.map(|x| x.into_uuid().into_bytes())),
        Cursor::Events(v) => StoredCursor::Events(v.map(|x| x.into_uuid().into_bytes())),
        Cursor::Inbox(v) => StoredCursor::Inbox(v.map(|x| {
            (
                x.recipient_agent_id().into_uuid().into_bytes(),
                x.recipient_session_id().into_uuid().into_bytes(),
                x.event_id().into_uuid().into_bytes(),
            )
        })),
        Cursor::Coordinators(v) => {
            StoredCursor::Coordinators(v.map(|x| x.into_uuid().into_bytes()))
        }
    };
    Ok(serde_json::to_vec(&value)?)
}
pub(crate) fn decode(bytes: &[u8]) -> Result<Cursor, OperationsError> {
    if bytes.len() > 512 {
        return Err(OperationsError::Bounds("cursor"));
    }
    Ok(match serde_json::from_slice(bytes)? {
        StoredCursor::Participants(v) => Cursor::Participants(
            v.map(|x| forge_domain::AgentInstanceId::from_uuid(Uuid::from_bytes(x))),
        ),
        StoredCursor::Presence(v) => Cursor::Presence(
            v.map(|x| forge_domain::AgentInstanceId::from_uuid(Uuid::from_bytes(x))),
        ),
        StoredCursor::Events(v) => {
            Cursor::Events(v.map(|x| forge_domain::AgentEventId::from_uuid(Uuid::from_bytes(x))))
        }
        StoredCursor::Inbox(v) => Cursor::Inbox(v.map(|(a, s, e)| {
            AgentOperationsInboxKey::new(
                forge_domain::AgentInstanceId::from_uuid(Uuid::from_bytes(a)),
                forge_domain::AgentSessionId::from_uuid(Uuid::from_bytes(s)),
                forge_domain::AgentEventId::from_uuid(Uuid::from_bytes(e)),
            )
        })),
        StoredCursor::Coordinators(v) => Cursor::Coordinators(
            v.map(|x| forge_domain::ActivityId::from_uuid(Uuid::from_bytes(x))),
        ),
    })
}
pub(crate) const fn kind(cursor: Cursor) -> usize {
    match cursor {
        Cursor::Participants(_) => 0,
        Cursor::Presence(_) => 1,
        Cursor::Events(_) => 2,
        Cursor::Inbox(_) => 3,
        Cursor::Coordinators(_) => 4,
    }
}

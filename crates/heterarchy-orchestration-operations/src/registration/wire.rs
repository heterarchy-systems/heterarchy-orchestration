//! Private bounded binary codec for immutable registration requests.
//!
//! The journal stores this value as an opaque canonical payload so an exact
//! request replay can be compared byte-for-byte.  It intentionally encodes
//! only the registration proposal and Forge reservation request; it never
//! constructs or serializes a `RegisteredAgentInstance`.

use super::{MAX_REQUEST_WIRE_BYTES, RegistrationRequest};
use crate::OperationsError;
use forge_domain::{
    ActivityId, AgentDeclaredRole, AgentDisplayName, AgentInstanceId, AgentSessionId,
    AgentTransportKind, AuthenticatedPrincipal, DirectTokenId, OAuthGrantId, TimestampMillis,
};
use forge_persistence::AgentBindingReservationRequest;
use uuid::Uuid;

const MAGIC: [u8; 4] = *b"HREG";
const VERSION: u8 = 1;
const MAX_TEXT_BYTES: usize = 128;

pub(super) fn encode(value: &RegistrationRequest) -> Result<Vec<u8>, OperationsError> {
    let mut writer = Writer::new();
    writer.bytes(&MAGIC);
    writer.u8(VERSION);
    writer.uuid(value.proposed_participant_id());
    encode_binding_into(&mut writer, value.binding());
    writer.text(value.display_name().as_str())?;
    writer.text(value.declared_role().as_str())?;
    writer.u8(u8::try_from(encode_transport(value.transport()))
        .map_err(|_| OperationsError::Corrupt("registration transport"))?);
    let value = writer.finish();
    if value.len() > MAX_REQUEST_WIRE_BYTES {
        return Err(OperationsError::Bounds("registration request"));
    }
    Ok(value)
}

pub(super) fn decode(value: &[u8]) -> Result<RegistrationRequest, OperationsError> {
    if value.is_empty() || value.len() > MAX_REQUEST_WIRE_BYTES {
        return Err(OperationsError::Bounds("registration request"));
    }
    let mut reader = Reader::new(value);
    if reader.bytes(4)? != MAGIC {
        return Err(OperationsError::Corrupt("registration wire magic"));
    }
    if reader.u8()? != VERSION {
        return Err(OperationsError::Corrupt("registration wire version"));
    }
    let proposed_participant_id = reader.uuid()?;
    let binding = decode_binding(&mut reader)?;
    let display_name = AgentDisplayName::new(reader.text()?.to_owned())
        .map_err(|_| OperationsError::Corrupt("registration display name"))?;
    let declared_role = AgentDeclaredRole::new(reader.text()?.to_owned())
        .map_err(|_| OperationsError::Corrupt("registration declared role"))?;
    let transport = decode_transport(i64::from(reader.u8()?))?;
    reader.finish()?;
    RegistrationRequest::new(
        proposed_participant_id,
        binding,
        display_name,
        declared_role,
        transport,
    )
}

fn encode_binding_into(writer: &mut Writer, value: &AgentBindingReservationRequest) {
    writer.uuid(value.request_id());
    writer.uuid(value.activity_id().into_uuid());
    writer.uuid(value.agent_id().into_uuid());
    writer.uuid(value.session_id().into_uuid());
    encode_principal_into(writer, value.principal());
    writer.i64(value.requested_at().get());
}

fn decode_binding(
    reader: &mut Reader<'_>,
) -> Result<AgentBindingReservationRequest, OperationsError> {
    let request_id = reader.uuid()?;
    let activity_id = ActivityId::from_uuid(reader.uuid()?);
    let agent_id = AgentInstanceId::from_uuid(reader.uuid()?);
    let session_id = AgentSessionId::from_uuid(reader.uuid()?);
    let principal = decode_principal_wire(reader)?;
    let requested_at = TimestampMillis::new(reader.i64()?)
        .map_err(|_| OperationsError::Corrupt("registration requested timestamp"))?;
    AgentBindingReservationRequest::new(
        request_id,
        activity_id,
        agent_id,
        session_id,
        principal,
        requested_at,
    )
    .map_err(|_| OperationsError::Corrupt("registration binding request"))
}

pub(super) fn encode_principal(value: AuthenticatedPrincipal) -> (i64, Option<Vec<u8>>) {
    match value {
        AuthenticatedPrincipal::TrustedStdio => (0, None),
        AuthenticatedPrincipal::DirectToken(id) => (1, Some(id.into_uuid().as_bytes().to_vec())),
        AuthenticatedPrincipal::OAuthGrant(id) => (2, Some(id.into_uuid().as_bytes().to_vec())),
    }
}

fn encode_principal_into(writer: &mut Writer, value: AuthenticatedPrincipal) {
    let (kind, id) = encode_principal(value);
    writer.u8(u8::try_from(kind).unwrap_or_default());
    if let Some(id) = id {
        writer.bytes(&id);
    }
}

fn decode_principal_wire(
    reader: &mut Reader<'_>,
) -> Result<AuthenticatedPrincipal, OperationsError> {
    let kind = reader.u8()?;
    match kind {
        0 => Ok(AuthenticatedPrincipal::TrustedStdio),
        1 => Ok(AuthenticatedPrincipal::DirectToken(
            DirectTokenId::from_uuid(reader.uuid()?),
        )),
        2 => Ok(AuthenticatedPrincipal::OAuthGrant(OAuthGrantId::from_uuid(
            reader.uuid()?,
        ))),
        _ => Err(OperationsError::Corrupt("registration principal")),
    }
}

pub(super) fn decode_principal_fields(
    kind: i64,
    value: Option<&[u8]>,
) -> Result<AuthenticatedPrincipal, OperationsError> {
    let Some(value) = value else {
        return if kind == 0 {
            Ok(AuthenticatedPrincipal::TrustedStdio)
        } else {
            Err(OperationsError::Corrupt("pending principal"))
        };
    };
    if value.len() != 16 {
        return Err(OperationsError::Corrupt("pending principal identity"));
    }
    let id = Uuid::from_slice(value)
        .map_err(|_| OperationsError::Corrupt("pending principal identity"))?;
    match kind {
        1 => Ok(AuthenticatedPrincipal::DirectToken(
            DirectTokenId::from_uuid(id),
        )),
        2 => Ok(AuthenticatedPrincipal::OAuthGrant(OAuthGrantId::from_uuid(
            id,
        ))),
        _ => Err(OperationsError::Corrupt("pending principal")),
    }
}

pub(super) fn encode_transport(value: AgentTransportKind) -> i64 {
    match value {
        AgentTransportKind::Retired => 0,
        AgentTransportKind::Stdio => 1,
        AgentTransportKind::StreamableHttp => 2,
        AgentTransportKind::Other => 3,
    }
}

pub(super) fn decode_transport(value: i64) -> Result<AgentTransportKind, OperationsError> {
    match value {
        0 => Ok(AgentTransportKind::Retired),
        1 => Ok(AgentTransportKind::Stdio),
        2 => Ok(AgentTransportKind::StreamableHttp),
        3 => Ok(AgentTransportKind::Other),
        _ => Err(OperationsError::Corrupt("registration transport")),
    }
}

struct Writer {
    bytes: Vec<u8>,
}

impl Writer {
    fn new() -> Self {
        Self {
            bytes: Vec::with_capacity(256),
        }
    }

    fn bytes(&mut self, value: &[u8]) {
        self.bytes.extend_from_slice(value);
    }

    fn u8(&mut self, value: u8) {
        self.bytes.push(value);
    }

    fn i64(&mut self, value: i64) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    fn uuid(&mut self, value: Uuid) {
        self.bytes(value.as_bytes());
    }

    fn text(&mut self, value: &str) -> Result<(), OperationsError> {
        if value.is_empty() || value.len() > MAX_TEXT_BYTES {
            return Err(OperationsError::Bounds("registration text"));
        }
        let len =
            u16::try_from(value.len()).map_err(|_| OperationsError::Bounds("registration text"))?;
        self.bytes.extend_from_slice(&len.to_be_bytes());
        self.bytes(value.as_bytes());
        Ok(())
    }

    fn finish(self) -> Vec<u8> {
        self.bytes
    }
}

struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn bytes(&mut self, length: usize) -> Result<&'a [u8], OperationsError> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(OperationsError::Bounds("registration wire"))?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or(OperationsError::Corrupt("registration wire length"))?;
        self.offset = end;
        Ok(value)
    }

    fn u8(&mut self) -> Result<u8, OperationsError> {
        Ok(*self
            .bytes(1)?
            .first()
            .ok_or(OperationsError::Corrupt("registration wire length"))?)
    }

    fn i64(&mut self) -> Result<i64, OperationsError> {
        let value: [u8; 8] = self
            .bytes(8)?
            .try_into()
            .map_err(|_| OperationsError::Corrupt("registration wire integer"))?;
        Ok(i64::from_be_bytes(value))
    }

    fn uuid(&mut self) -> Result<Uuid, OperationsError> {
        Uuid::from_slice(self.bytes(16)?)
            .map_err(|_| OperationsError::Corrupt("registration wire UUID"))
    }

    fn text(&mut self) -> Result<&'a str, OperationsError> {
        let length =
            usize::from(u16::from_be_bytes(self.bytes(2)?.try_into().map_err(
                |_| OperationsError::Corrupt("registration text length"),
            )?));
        if length == 0 || length > MAX_TEXT_BYTES {
            return Err(OperationsError::Bounds("registration text"));
        }
        std::str::from_utf8(self.bytes(length)?)
            .map_err(|_| OperationsError::Corrupt("registration text UTF-8"))
    }

    fn finish(self) -> Result<(), OperationsError> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(OperationsError::Corrupt("registration wire trailing bytes"))
        }
    }
}

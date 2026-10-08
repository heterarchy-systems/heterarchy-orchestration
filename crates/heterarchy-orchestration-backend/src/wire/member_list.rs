//! Caller-scoped logical Workspace member-list query contract.

use serde::{Deserialize, Deserializer, Serialize};
use uuid::Uuid;

use super::WorkspaceQueryScope;
use super::membership::MemberTransportWire;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MemberListRequest {
    pub scope: WorkspaceQueryScope,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub session_id: Option<Uuid>,
    pub limit: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MemberListResponse {
    pub scope: WorkspaceQueryScope,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub session_id: Option<Uuid>,
    pub limit: u32,
    pub members: Vec<LogicalMemberWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LogicalMemberWire {
    pub workspace_id: Uuid,
    pub participant_id: Uuid,
    pub session_id: Uuid,
    pub display_name: String,
    pub declared_role: String,
    pub transport: MemberTransportWire,
    pub membership_revision: u64,
}

fn deserialize_required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scope() -> WorkspaceQueryScope {
        WorkspaceQueryScope {
            workspace_id: Uuid::from_u128(1),
            location_id: Uuid::from_u128(2),
            location_revision: 3,
            workspace_revision: 4,
            locator: "workspace".to_owned(),
        }
    }

    #[test]
    fn session_selector_is_required_nullable_on_both_contracts()
    -> Result<(), Box<dyn std::error::Error>> {
        let request = MemberListRequest {
            scope: scope(),
            session_id: None,
            limit: 64,
        };
        let encoded_request = serde_json::to_value(&request)?;
        assert!(encoded_request["sessionId"].is_null());
        let mut unknown_request = encoded_request.clone();
        unknown_request["unexpected"] = true.into();
        assert!(serde_json::from_value::<MemberListRequest>(unknown_request).is_err());
        let mut missing_request_session = encoded_request;
        missing_request_session
            .as_object_mut()
            .ok_or("member-list request object")?
            .remove("sessionId");
        assert!(serde_json::from_value::<MemberListRequest>(missing_request_session).is_err());

        let response = MemberListResponse {
            scope: scope(),
            session_id: None,
            limit: 64,
            members: Vec::new(),
        };
        let encoded_response = serde_json::to_value(response)?;
        assert!(encoded_response["sessionId"].is_null());
        let mut missing_response_session = encoded_response;
        missing_response_session
            .as_object_mut()
            .ok_or("member-list response object")?
            .remove("sessionId");
        assert!(serde_json::from_value::<MemberListResponse>(missing_response_session).is_err());
        Ok(())
    }

    #[test]
    fn member_fields_and_transport_vocabulary_are_closed() -> Result<(), Box<dyn std::error::Error>>
    {
        let response = MemberListResponse {
            scope: scope(),
            session_id: Some(Uuid::from_u128(3)),
            limit: 1,
            members: vec![LogicalMemberWire {
                workspace_id: scope().workspace_id,
                participant_id: Uuid::from_u128(4),
                session_id: Uuid::from_u128(3),
                display_name: "Ada".to_owned(),
                declared_role: "reviewer".to_owned(),
                transport: MemberTransportWire::Mcp,
                membership_revision: 1,
            }],
        };
        let baseline = serde_json::to_value(response)?;
        assert_eq!(
            baseline["members"][0]["participantId"],
            Uuid::from_u128(4).to_string()
        );
        assert_eq!(baseline["members"][0]["transport"], "mcp");

        let mut unknown_member_field = baseline.clone();
        unknown_member_field["members"][0]["future"] = true.into();
        assert!(serde_json::from_value::<MemberListResponse>(unknown_member_field).is_err());

        let mut unknown_transport = baseline;
        unknown_transport["members"][0]["transport"] = "future_transport".into();
        assert!(serde_json::from_value::<MemberListResponse>(unknown_transport).is_err());
        Ok(())
    }
}

//! Typed HTTP methods for configured Forge Mission-evidence workflows.

mod validation;

use hyper::Method;
use hyper::header::HeaderValue;
use uuid::Uuid;

use super::{ClientError, OrchestrationHttpClient, ReadbackTarget};
use crate::wire::mission_evidence::{
    MissionEvidenceBindingReadRequest, MissionEvidenceBindingReadResponse,
    MissionEvidenceManifestBindRequest, MissionEvidenceManifestBindResponse,
    MissionEvidenceManifestPrepareRequest, MissionEvidenceManifestPrepareResponse,
    MissionEvidenceMutationRequest, MissionEvidenceObservationReadRequest,
    MissionEvidenceObservationReadResponse, MissionEvidenceObservationRequest,
    MissionEvidenceObservationResponse, MissionEvidencePlanDigestsRequest,
    MissionEvidencePlanDigestsResponse, MissionEvidenceRequestReadActor,
    MissionEvidenceRequestReadRequest, MissionEvidenceRequestReadResponse,
};

use validation::{
    valid_bind_request, valid_bind_response, valid_binding_read_request,
    valid_binding_read_response, valid_mutation_readback, valid_observation_read_request,
    valid_observation_read_response, valid_observation_request, valid_observation_response,
    valid_plan_digests_request, valid_plan_digests_response, valid_prepare_request,
    valid_prepare_response, valid_readback_request,
};

/// Bounded full source-cut and provenance response allowance for this feature.
pub(super) const MAX_MISSION_EVIDENCE_RESPONSE_BYTES: usize =
    crate::wire::mission_evidence::MAX_MISSION_EVIDENCE_RESPONSE_BYTES;
/// Bounded maximal 256-partition candidate request and exact recovery envelope.
pub(super) const MAX_MISSION_EVIDENCE_REQUEST_BYTES: usize =
    crate::wire::mission_evidence::MAX_MISSION_EVIDENCE_BODY_BYTES;

/// Match only the exact Mission evidence route family for the larger bounded
/// source-cut response limit in the shared HTTP client.
pub(super) fn is_mission_evidence_path(path: &str) -> bool {
    let parts = path.split('/').collect::<Vec<_>>();
    if parts.len() < 8
        || !parts[0].is_empty()
        || parts[1] != "internal"
        || parts[2] != "v1"
        || parts[3] != "workspaces"
        || !canonical_uuid_segment(parts[4])
    {
        return false;
    }
    match parts[5] {
        "mission-evidence-requests" => {
            parts.len() == 8 && canonical_uuid_segment(parts[6]) && parts[7] == "query"
        }
        "mission-evidence" => match parts.get(6..) {
            Some(
                ["plan-digests", "query"]
                | ["manifests", "prepare" | "bind"]
                | ["observations", "record"],
            ) => true,
            Some(["plans", plan_id, "revisions", revision, "binding", "query"]) => {
                canonical_uuid_segment(plan_id) && canonical_revision_segment(revision)
            }
            Some(
                [
                    "plans",
                    plan_id,
                    "revisions",
                    revision,
                    "observations",
                    request_id,
                    "query",
                ],
            ) => {
                canonical_uuid_segment(plan_id)
                    && canonical_revision_segment(revision)
                    && canonical_uuid_segment(request_id)
            }
            None | Some(_) => false,
        },
        _ => false,
    }
}

fn canonical_uuid_segment(value: &str) -> bool {
    Uuid::parse_str(value).is_ok_and(|id| id.to_string() == value)
}

fn canonical_revision_segment(value: &str) -> bool {
    value
        .parse::<u64>()
        .is_ok_and(|revision| revision.to_string() == value && revision > 0)
}

impl OrchestrationHttpClient {
    /// Read locally derived Mission evaluation digests for one frozen plan.
    ///
    /// # Errors
    /// Rejects malformed selectors, bounded transport failures, and invalid
    /// or cross-scope responses.
    pub async fn mission_evidence_plan_digests(
        &self,
        authorization: &HeaderValue,
        request: &MissionEvidencePlanDigestsRequest,
    ) -> Result<MissionEvidencePlanDigestsResponse, ClientError> {
        if !valid_plan_digests_request(request) {
            return Err(ClientError::InvalidRequest);
        }
        let response = self
            .call(
                authorization,
                Method::POST,
                plan_digests_path(request.scope.workspace_id),
                Some(request),
                None,
            )
            .await?;
        if valid_plan_digests_response(&response, request) {
            Ok(response)
        } else {
            Err(ClientError::Protocol)
        }
    }

    /// Read-only preflight for a complete caller-selected source mapping.
    ///
    /// # Errors
    /// Rejects malformed or incomplete candidate selectors, bounded transport
    /// failures, and proposal readbacks that change candidate identities.
    pub async fn mission_evidence_manifest_prepare(
        &self,
        authorization: &HeaderValue,
        request: &MissionEvidenceManifestPrepareRequest,
    ) -> Result<MissionEvidenceManifestPrepareResponse, ClientError> {
        if !valid_prepare_request(request) {
            return Err(ClientError::InvalidRequest);
        }
        let response = self
            .call(
                authorization,
                Method::POST,
                manifest_prepare_path(request.context.scope.workspace_id),
                Some(request),
                None,
            )
            .await?;
        if valid_prepare_response(&response, request) {
            Ok(response)
        } else {
            Err(ClientError::Protocol)
        }
    }

    /// Fetch or seal source manifests and bind the complete bundle locally.
    ///
    /// No automatic retry is performed. A transport or malformed-response
    /// uncertainty retains the exact candidate mapping for actor-bound receipt
    /// readback.
    ///
    /// # Errors
    /// Rejects malformed requests, typed service rejections, bounded transport
    /// failures, or a response that cannot be correlated with the intent.
    pub async fn mission_evidence_manifest_bind(
        &self,
        authorization: &HeaderValue,
        request: &MissionEvidenceManifestBindRequest,
    ) -> Result<MissionEvidenceManifestBindResponse, ClientError> {
        if !valid_bind_request(request) {
            return Err(ClientError::InvalidRequest);
        }
        let intent = MissionEvidenceMutationRequest::Bind(request.clone());
        let target = mutation_readback(intent.clone());
        let response = self
            .call(
                authorization,
                Method::POST,
                manifest_bind_path(request.context.scope.workspace_id),
                Some(request),
                Some(target.clone()),
            )
            .await?;
        if valid_bind_response(&response, request) {
            Ok(response)
        } else {
            Err(ClientError::UnknownOutcome { readback: target })
        }
    }

    /// Read the canonical binding for one exact Mission plan revision.
    ///
    /// # Errors
    /// Rejects malformed selectors, bounded transport failures, and a foreign
    /// or malformed source binding projection.
    pub async fn mission_evidence_binding_read(
        &self,
        authorization: &HeaderValue,
        request: &MissionEvidenceBindingReadRequest,
    ) -> Result<MissionEvidenceBindingReadResponse, ClientError> {
        if !valid_binding_read_request(request) {
            return Err(ClientError::InvalidRequest);
        }
        let response = self
            .call(
                authorization,
                Method::POST,
                binding_read_path(
                    request.scope.workspace_id,
                    request.plan_id,
                    request.plan_revision,
                ),
                Some(request),
                None,
            )
            .await?;
        if valid_binding_read_response(&response, request) {
            Ok(response)
        } else {
            Err(ClientError::Protocol)
        }
    }

    /// Fetch exact Forge T1 and review cuts, then append one complete local
    /// observation at the expected Orchestration head.
    ///
    /// No automatic retry is performed. The exact original plan, expected head
    /// and request ID are retained for actor-bound receipt readback.
    ///
    /// # Errors
    /// Rejects malformed requests, typed source/native conflicts, bounded
    /// transport failures, or an uncorrelated observation response.
    pub async fn mission_evidence_observation_record(
        &self,
        authorization: &HeaderValue,
        request: &MissionEvidenceObservationRequest,
    ) -> Result<MissionEvidenceObservationResponse, ClientError> {
        if !valid_observation_request(request) {
            return Err(ClientError::InvalidRequest);
        }
        let intent = MissionEvidenceMutationRequest::Observe(request.clone());
        let target = mutation_readback(intent);
        let response = self
            .call(
                authorization,
                Method::POST,
                observation_record_path(request.context.scope.workspace_id),
                Some(request),
                Some(target.clone()),
            )
            .await?;
        if valid_observation_response(&response, request) {
            Ok(response)
        } else {
            Err(ClientError::UnknownOutcome { readback: target })
        }
    }

    /// Read one immutable observation by its exact local request identity.
    ///
    /// # Errors
    /// Rejects malformed selectors, bounded transport failures, and foreign or
    /// malformed readbacks.
    pub async fn mission_evidence_observation_read(
        &self,
        authorization: &HeaderValue,
        request: &MissionEvidenceObservationReadRequest,
    ) -> Result<MissionEvidenceObservationReadResponse, ClientError> {
        if !valid_observation_read_request(request) {
            return Err(ClientError::InvalidRequest);
        }
        let response = self
            .call(
                authorization,
                Method::POST,
                observation_read_path(
                    request.scope.workspace_id,
                    request.plan_id,
                    request.plan_revision,
                    request.request_id,
                ),
                Some(request),
                None,
            )
            .await?;
        if valid_observation_read_response(&response, request) {
            Ok(response)
        } else {
            Err(ClientError::Protocol)
        }
    }

    /// Read the durable mutation receipt for the original logical participant.
    ///
    /// `mutation` is retained as exact recovery context; `reader` selects the
    /// current authenticated Ready Session and Activity. An absent receipt is
    /// observational only and never authorizes a resend.
    ///
    /// # Errors
    /// Rejects malformed actor/intent selectors, bounded transport failures,
    /// and a receipt that does not match the retained intent.
    pub async fn mission_evidence_mutation_readback(
        &self,
        authorization: &HeaderValue,
        mutation: &MissionEvidenceMutationRequest,
        reader: MissionEvidenceRequestReadActor,
    ) -> Result<MissionEvidenceRequestReadResponse, ClientError> {
        let request = MissionEvidenceRequestReadRequest {
            scope: mutation.scope().clone(),
            session_id: reader.session_id,
            sender_activity_id: reader.sender_activity_id,
            mutation: mutation.clone(),
        };
        if !valid_readback_request(&request) {
            return Err(ClientError::InvalidRequest);
        }
        let response = self
            .call(
                authorization,
                Method::POST,
                mutation_read_path(request.scope.workspace_id, mutation.request_id()),
                Some(&request),
                Some(mutation_readback(mutation.clone())),
            )
            .await?;
        if valid_mutation_readback(&response, mutation, reader) {
            Ok(response)
        } else {
            Err(ClientError::Protocol)
        }
    }
}

fn mutation_readback(mutation: MissionEvidenceMutationRequest) -> ReadbackTarget {
    ReadbackTarget::MissionEvidenceMutation(Box::new(mutation))
}

fn plan_digests_path(workspace_id: Uuid) -> String {
    format!("/internal/v1/workspaces/{workspace_id}/mission-evidence/plan-digests/query")
}

fn manifest_prepare_path(workspace_id: Uuid) -> String {
    format!("/internal/v1/workspaces/{workspace_id}/mission-evidence/manifests/prepare")
}

fn manifest_bind_path(workspace_id: Uuid) -> String {
    format!("/internal/v1/workspaces/{workspace_id}/mission-evidence/manifests/bind")
}

fn binding_read_path(workspace_id: Uuid, plan_id: Uuid, revision: u64) -> String {
    format!(
        "/internal/v1/workspaces/{workspace_id}/mission-evidence/plans/{plan_id}/revisions/{revision}/binding/query"
    )
}

fn observation_record_path(workspace_id: Uuid) -> String {
    format!("/internal/v1/workspaces/{workspace_id}/mission-evidence/observations/record")
}

fn observation_read_path(
    workspace_id: Uuid,
    plan_id: Uuid,
    revision: u64,
    request_id: Uuid,
) -> String {
    format!(
        "/internal/v1/workspaces/{workspace_id}/mission-evidence/plans/{plan_id}/revisions/{revision}/observations/{request_id}/query"
    )
}

fn mutation_read_path(workspace_id: Uuid, request_id: Uuid) -> String {
    format!("/internal/v1/workspaces/{workspace_id}/mission-evidence-requests/{request_id}/query")
}

#[cfg(test)]
mod tests {
    use std::{error::Error, time::Duration};

    use hyper::StatusCode;
    use hyper::header::HeaderValue;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use uuid::Uuid;

    use super::*;
    use crate::wire::WorkspaceQueryScope;
    use crate::wire::mission_evaluation::MissionEvaluationMutationContextWire;
    use crate::wire::mission_evidence::{
        MissionEvidenceCandidateMemberWire, MissionEvidenceCandidatePartitionWire,
    };

    #[tokio::test]
    async fn malformed_bind_reply_retains_exact_intent_without_retry() -> Result<(), Box<dyn Error>>
    {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let client = OrchestrationHttpClient::new(listener.local_addr()?, Duration::from_secs(1))?;
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await?;
            let request = read_http_request(&mut socket).await?;
            write_json_response(&mut socket, StatusCode::OK, b"{}").await?;
            Ok::<_, std::io::Error>(request)
        });
        let request = bind_request();
        let result = client
            .mission_evidence_manifest_bind(&HeaderValue::from_static("Bearer fixture"), &request)
            .await;
        assert_eq!(
            result,
            Err(ClientError::UnknownOutcome {
                readback: ReadbackTarget::MissionEvidenceMutation(Box::new(
                    MissionEvidenceMutationRequest::Bind(request.clone())
                )),
            })
        );
        let captured = server.await??;
        assert!(captured.starts_with(b"POST /internal/v1/workspaces/"));
        Ok(())
    }

    #[tokio::test]
    async fn actor_bound_recovery_keeps_original_intent_across_session_rebind()
    -> Result<(), Box<dyn Error>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let client = OrchestrationHttpClient::new(listener.local_addr()?, Duration::from_secs(1))?;
        let mutation = MissionEvidenceMutationRequest::Observe(
            crate::wire::mission_evidence::MissionEvidenceObservationRequest {
                context: context(),
                request_id: id(8),
                mission_id: id(9),
                plan_id: id(10),
                plan_revision: 1,
                expected_head_revision: 0,
            },
        );
        let reader = MissionEvidenceRequestReadActor {
            sender_activity_id: id(6),
            session_id: id(11),
        };
        let expected_intent = mutation.clone();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await?;
            let raw = read_http_request(&mut socket).await?;
            let body = request_body(&raw)?;
            let request: MissionEvidenceRequestReadRequest =
                serde_json::from_slice(body).map_err(std::io::Error::other)?;
            if request.session_id != id(11)
                || request.sender_activity_id != id(6)
                || request.mutation != expected_intent
                || request.mutation.session_id() != id(3)
                || request.mutation.request_id() != id(8)
            {
                return Err(std::io::Error::other(
                    "readback changed its retained intent",
                ));
            }
            let response = MissionEvidenceRequestReadResponse {
                scope: request.scope,
                session_id: request.session_id,
                sender_activity_id: request.sender_activity_id,
                request_id: request.mutation.request_id(),
                mutation: None,
            };
            let body = serde_json::to_vec(&response).map_err(std::io::Error::other)?;
            write_json_response(&mut socket, StatusCode::OK, &body).await?;
            Ok::<_, std::io::Error>(())
        });
        let readback_result = client
            .mission_evidence_mutation_readback(
                &HeaderValue::from_static("Bearer fixture"),
                &mutation,
                reader,
            )
            .await;
        server.await??;
        let readback = readback_result?;
        assert_eq!(readback.session_id, reader.session_id);
        assert_eq!(readback.request_id, mutation.request_id());
        assert!(readback.mutation.is_none());
        Ok(())
    }

    async fn read_http_request(socket: &mut TcpStream) -> Result<Vec<u8>, std::io::Error> {
        let mut request = Vec::new();
        let mut chunk = [0_u8; 4_096];
        let header_end = loop {
            let read = socket.read(&mut chunk).await?;
            if read == 0 {
                return Err(std::io::Error::other(
                    "HTTP request ended before headers completed",
                ));
            }
            request.extend_from_slice(&chunk[..read]);
            if let Some(position) = request.windows(4).position(|window| window == b"\r\n\r\n") {
                break position;
            }
        };
        let header_text =
            std::str::from_utf8(&request[..header_end]).map_err(std::io::Error::other)?;
        let content_length = header_text
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().ok())
                    .flatten()
            })
            .ok_or_else(|| std::io::Error::other("HTTP request omitted Content-Length"))?;
        let request_end = header_end + 4 + content_length;
        while request.len() < request_end {
            let read = socket.read(&mut chunk).await?;
            if read == 0 {
                return Err(std::io::Error::other("HTTP request body was truncated"));
            }
            request.extend_from_slice(&chunk[..read]);
        }
        request.truncate(request_end);
        Ok(request)
    }

    fn request_body(request: &[u8]) -> Result<&[u8], std::io::Error> {
        let header_end = request
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .ok_or_else(|| std::io::Error::other("HTTP request omitted header terminator"))?;
        Ok(&request[header_end + 4..])
    }

    async fn write_json_response(
        socket: &mut TcpStream,
        status: StatusCode,
        body: &[u8],
    ) -> Result<(), std::io::Error> {
        let reason = status.canonical_reason().unwrap_or("Unknown");
        let headers = format!(
            "HTTP/1.1 {} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            status.as_u16(),
            reason,
            body.len()
        );
        socket.write_all(headers.as_bytes()).await?;
        socket.write_all(body).await
    }

    fn bind_request() -> MissionEvidenceManifestBindRequest {
        MissionEvidenceManifestBindRequest {
            context: context(),
            request_id: id(7),
            mission_id: id(8),
            plan_id: id(9),
            plan_revision: 1,
            partitions: vec![MissionEvidenceCandidatePartitionWire {
                activity_id: id(10),
                members: vec![MissionEvidenceCandidateMemberWire {
                    obligation_id: id(11),
                    assignment_id: id(12),
                    assignment_revision: 1,
                    target_agent_id: id(13),
                    target_session_id: id(3),
                }],
            }],
        }
    }

    fn context() -> MissionEvaluationMutationContextWire {
        MissionEvaluationMutationContextWire {
            scope: WorkspaceQueryScope {
                workspace_id: id(1),
                location_id: id(2),
                location_revision: 3,
                workspace_revision: 4,
                locator: "workspace".to_owned(),
            },
            session_id: id(3),
            sender_activity_id: id(6),
            expected_workspace_leadership_revision: 5,
        }
    }

    fn id(value: u128) -> Uuid {
        Uuid::from_u128(value)
    }
}

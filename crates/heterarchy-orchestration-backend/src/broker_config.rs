//! Operator-authenticated Broker connection inspection and dynamic reconfiguration.

use std::net::SocketAddr;

use axum::extract::{Extension, Query, State};
use axum::routing::get;
use axum::{Json, Router};
use forge_domain::AuthenticatedPrincipal;
use heterarchy_orchestration_operations::BrokerClusterConfig;
use serde::Deserialize;

use crate::app_support::LaneError;
use crate::wire::broker_config::{BrokerConfigResponse, BrokerConfigUpdateRequest};
use crate::{ApiError, BackendState};

pub(crate) fn routes() -> Router<BackendState> {
    Router::new().route(
        "/internal/v1/broker-config",
        get(get_broker_config).put(put_broker_config),
    )
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct BrokerConfigQuery {
    #[serde(default)]
    probe: bool,
}

async fn get_broker_config(
    State(state): State<BackendState>,
    Query(query): Query<BrokerConfigQuery>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
) -> Result<Json<BrokerConfigResponse>, LaneError> {
    let config = state
        .run_catalog(move |runtime| {
            runtime.require_operator(principal)?;
            Ok(runtime.broker_cluster_config())
        })
        .await?;
    let probe = if query.probe {
        Some(
            state
                .runtime
                .probe_broker_cluster()
                .await
                .map_err(ApiError::Runtime)?,
        )
    } else {
        None
    };
    Ok(Json(BrokerConfigResponse::from_config_and_probe(
        config, probe,
    )))
}

async fn put_broker_config(
    State(state): State<BackendState>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Json(body): Json<BrokerConfigUpdateRequest>,
) -> Result<Json<BrokerConfigResponse>, LaneError> {
    let config = parse_update_request(&body)?;
    state
        .run_catalog(move |runtime| runtime.require_operator(principal))
        .await?;
    let probe = state
        .runtime
        .reconfigure_broker_cluster(config)
        .await
        .map_err(ApiError::Runtime)?;
    Ok(Json(BrokerConfigResponse::from_config_and_probe(
        config,
        Some(probe),
    )))
}

fn parse_update_request(
    body: &BrokerConfigUpdateRequest,
) -> Result<BrokerClusterConfig, LaneError> {
    match (&body.broker_address, &body.nodes) {
        (Some(broker_raw), None) => {
            let broker_addr = broker_raw
                .trim()
                .parse::<SocketAddr>()
                .map_err(|_| LaneError::InvalidIdentity)?;
            if let Some(ops_raw) = &body.operations_address {
                let ops_addr = ops_raw
                    .trim()
                    .parse::<SocketAddr>()
                    .map_err(|_| LaneError::InvalidIdentity)?;
                BrokerClusterConfig::from_single_node(broker_addr, ops_addr)
                    .map_err(|_| LaneError::InvalidIdentity)
            } else {
                BrokerClusterConfig::from_broker_address(broker_addr)
                    .map_err(|_| LaneError::InvalidIdentity)
            }
        }
        (None, Some(nodes)) if body.operations_address.is_none() => {
            let mut pairs = [(
                SocketAddr::from(([127, 0, 0, 1], 8811)),
                SocketAddr::from(([127, 0, 0, 1], 9811)),
            ); 3];
            for (index, node) in nodes.iter().enumerate() {
                let broker_addr = node
                    .broker_address
                    .trim()
                    .parse::<SocketAddr>()
                    .map_err(|_| LaneError::InvalidIdentity)?;
                let ops_addr = if let Some(raw) = &node.operations_address {
                    raw.trim()
                        .parse::<SocketAddr>()
                        .map_err(|_| LaneError::InvalidIdentity)?
                } else {
                    let default_ops_port = broker_addr
                        .port()
                        .checked_add(1_000)
                        .or_else(|| broker_addr.port().checked_add(1))
                        .unwrap_or(broker_addr.port());
                    SocketAddr::new(broker_addr.ip(), default_ops_port)
                };
                pairs[index] = (broker_addr, ops_addr);
            }
            BrokerClusterConfig::from_node_pairs(pairs).map_err(|_| LaneError::InvalidIdentity)
        }
        _ => Err(LaneError::InvalidIdentity),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::broker_config::BrokerNodeAddressWire;

    #[test]
    fn parse_update_request_accepts_single_node_and_cluster_variants()
    -> Result<(), Box<dyn std::error::Error>> {
        let single = parse_update_request(&BrokerConfigUpdateRequest {
            broker_address: Some("127.0.0.1:8811".to_owned()),
            operations_address: None,
            nodes: None,
        })
        .map_err(|_| "single-node parse failed")?;
        assert!(single.is_single_node());
        assert_eq!(single.nodes[0].broker_address, "127.0.0.1:8811".parse()?);
        assert_eq!(
            single.nodes[0].operations_address,
            "127.0.0.1:9811".parse()?
        );

        let cluster = parse_update_request(&BrokerConfigUpdateRequest {
            broker_address: None,
            operations_address: None,
            nodes: Some([
                BrokerNodeAddressWire {
                    broker_address: "127.0.0.1:8811".to_owned(),
                    operations_address: Some("127.0.0.1:9811".to_owned()),
                },
                BrokerNodeAddressWire {
                    broker_address: "127.0.0.1:8812".to_owned(),
                    operations_address: Some("127.0.0.1:9812".to_owned()),
                },
                BrokerNodeAddressWire {
                    broker_address: "127.0.0.1:8813".to_owned(),
                    operations_address: Some("127.0.0.1:9813".to_owned()),
                },
            ]),
        })
        .map_err(|_| "cluster parse failed")?;
        assert!(!cluster.is_single_node());

        assert!(
            parse_update_request(&BrokerConfigUpdateRequest {
                broker_address: None,
                operations_address: None,
                nodes: None,
            })
            .is_err()
        );

        Ok(())
    }
}

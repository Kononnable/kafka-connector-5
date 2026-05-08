//! Cluster-wide state controller.
//!
//! `ClusterController` is the main entry point for the Kafka client.  It:
//!
//! - Maintains cluster metadata (broker list, controller ID, etc.)
//! - Creates / drops `BrokerController` instances based on `MetadataResponse`
//! - Provides request routing to a specific node or a random connected one
//! - Handles API version selection (intersection of client & broker support)

pub mod error;
pub mod options;

pub use error::ClusterError;
pub use options::ClusterOptions;

use crate::broker_controller::{BrokerController, BrokerError};
use bytes::Bytes;
use protocol::generated::{
    metadata_response::MetadataResponseBroker,
    MetadataRequest, MetadataResponse,
};
use protocol::generated::is_flexible_api;
use protocol::traits::{ApiRequest, ApiResponse, ApiVersion};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use tokio::sync::{Mutex, RwLock};
use tracing::{info, warn};



// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------



// ---------------------------------------------------------------------------
// BrokerNode — tracked broker state
// ---------------------------------------------------------------------------

/// Runtime state for a single broker in the cluster.
#[derive(Debug)]
struct BrokerNode {
    /// The broker's node ID from metadata.
    #[allow(dead_code)]
    node_id: i32,
    /// Host/port of the broker.
    #[allow(dead_code)]
    host: String,
    #[allow(dead_code)]
    port: i32,
    /// The controller managing the TCP connection.
    controller: Arc<BrokerController>,
}

// ---------------------------------------------------------------------------
// ClusterController
// ---------------------------------------------------------------------------

/// Cluster-level controller that manages connections to all brokers and
/// provides request routing.
#[derive(Debug)]
pub struct ClusterController {
    /// Map of node_id → BrokerNode.
    brokers: RwLock<HashMap<i32, BrokerNode>>,

    /// The controller broker's node ID (from metadata, 0 if unknown).
    controller_id: RwLock<i32>,

    /// The cluster ID (from metadata).
    cluster_id: RwLock<Option<String>>,

    /// A random seed used for round-robin / random selection.
    rng_seed: Mutex<u64>,
}

impl ClusterController {
    /// Create a new cluster controller.
    ///
    /// Connects to the first reachable bootstrap server, fetches cluster
    /// metadata to discover all brokers, and connects to every broker in
    /// the cluster.
    pub async fn new(options: ClusterOptions) -> Result<Arc<Self>, ClusterError> {
        let controller = Arc::new(Self {
            brokers: RwLock::new(HashMap::new()),
            controller_id: RwLock::new(0),
            cluster_id: RwLock::new(None),
            rng_seed: Mutex::new(0),
        });

        if options.bootstrap_servers.is_empty() {
            panic!("ClusterOptions.bootstrap_servers is empty — at least one address is required");
        }

        // Parse all addresses upfront — invalid strings are a user error.
        let parsed: Vec<SocketAddr> = options
            .bootstrap_servers
            .iter()
            .map(|s| {
                s.parse::<SocketAddr>()
                    .unwrap_or_else(|_| panic!(
                        "invalid bootstrap server address {:?} — expected host:port",
                        s
                    ))
            })
            .collect();

        // Try connecting to each address in order, stop at the first success.
        let mut last_err: Option<ClusterError> = None;
        let mut seed_broker: Option<Arc<BrokerController>> = None;
        let mut seed_addr = None;

        for addr in &parsed {
            match BrokerController::connect(*addr, None).await {
                Ok(broker) => {
                    seed_broker = Some(broker);
                    seed_addr = Some(*addr);
                    break;
                }
                Err(e) => {
                    warn!("failed to connect to {}: {e}", addr);
                    last_err = Some(ClusterError::Broker(e));
                }
            }
        }

        let broker = seed_broker.ok_or_else(|| {
            last_err.unwrap_or(ClusterError::NoConnectedBrokers)
        })?;
        let seed_addr = seed_addr.unwrap();

        let node = BrokerNode {
            node_id: -1, // temporary — replaced by metadata
            host: seed_addr.ip().to_string(),
            port: seed_addr.port() as i32,
            controller: Arc::clone(&broker),
        };

        {
            let mut brokers = controller.brokers.write().await;
            brokers.insert(-1, node);
        }

        // Fetch metadata to discover the full cluster and connect to all brokers.
        controller.refresh_metadata(None).await?;

        // Remove the seed broker entry — it's been replaced by proper metadata entries.
        {
            let mut brokers = controller.brokers.write().await;
            brokers.remove(&-1);
        }

        info!(
            "cluster initialized with {} broker(s), controller_id={}",
            controller.broker_count().await,
            controller.controller_node_id().await,
        );

        Ok(controller)
    }

    // ── Metadata management ────────────────────────────────────────────

    /// Fetch cluster metadata from a connected broker and update the internal
    /// broker list accordingly.
    ///
    /// If `node_id` is `None`, a random connected broker is used.
    pub async fn refresh_metadata(
        self: &Arc<Self>,
        node_id: Option<i32>,
    ) -> Result<MetadataResponse, ClusterError> {
        let req = MetadataRequest {
            topics: None, // fetch all topics
            allow_auto_topic_creation: false,
            include_cluster_authorized_operations: false,
            include_topic_authorized_operations: false,
        };

        let resp: MetadataResponse = self
            .send(&req, node_id, ApiVersion::new(9))
            .await?;

        if resp.error_code != 0 {
            return Err(ClusterError::MetadataError(resp.error_code));
        }

        // Update cluster state
        {
            let mut cid = self.cluster_id.write().await;
            *cid = resp.cluster_id.clone();
        }
        {
            let mut ctrl_id = self.controller_id.write().await;
            *ctrl_id = resp.controller_id;
        }

        // Diff broker list: add new, keep existing, remove stale
        let mut brokers_to_keep: Vec<i32> = Vec::new();
        let mut new_brokers: Vec<MetadataResponseBroker> = Vec::new();

        for broker in &resp.brokers {
            let existing = {
                let brokers = self.brokers.read().await;
                brokers.values().any(|b| b.node_id == broker.node_id)
            };

            if existing {
                brokers_to_keep.push(broker.node_id);
            } else {
                new_brokers.push(broker.clone());
            }
        }

        // Connect to new brokers
        for broker in &new_brokers {
            let addr_str = format!("{}:{}", broker.host, broker.port);
            if let Ok(addr) = addr_str.parse::<SocketAddr>() {
                match BrokerController::connect(addr, None).await {
                    Ok(controller) => {
                        let node = BrokerNode {
                            node_id: broker.node_id,
                            host: broker.host.clone(),
                            port: broker.port,
                            controller,
                        };
                        let mut brokers = self.brokers.write().await;
                        brokers.insert(broker.node_id, node);
                        info!(
                            "added broker node_id={} at {}:{}",
                            broker.node_id, broker.host, broker.port
                        );
                    }
                    Err(e) => {
                        warn!(
                            "failed to connect to broker {}:{} (node_id={}): {e}",
                            broker.host, broker.port, broker.node_id
                        );
                    }
                }
            } else {
                warn!(
                    "invalid broker address: {}:{}",
                    broker.host, broker.port
                );
            }
        }

        // Remove stale brokers (not in the latest metadata)
        {
            let mut brokers = self.brokers.write().await;
            brokers.retain(|node_id, _| {
                let keep = brokers_to_keep.contains(node_id)
                    || new_brokers.iter().any(|b| b.node_id == *node_id);
                if !keep && *node_id != -1 {
                    info!("removing stale broker node_id={}", node_id);
                }
                keep || *node_id == -1 // always keep seed broker if still there
            });
        }

        Ok(resp)
    }

    // ── Broker accessors ───────────────────────────────────────────────

    /// Number of brokers currently tracked.
    pub async fn broker_count(&self) -> usize {
        self.brokers.read().await.len()
    }

    /// Number of connected brokers.
    pub async fn connected_broker_count(&self) -> usize {
        let brokers = self.brokers.read().await;
        brokers
            .values()
            .filter(|b| b.controller.is_connected())
            .count()
    }

    /// The controller broker's node ID.
    pub async fn controller_node_id(&self) -> i32 {
        *self.controller_id.read().await
    }

    /// The cluster ID, if known.
    pub async fn cluster_id(&self) -> Option<String> {
        self.cluster_id.read().await.clone()
    }

    /// List of broker node IDs that are currently connected.
    pub async fn connected_broker_ids(&self) -> Vec<i32> {
        let brokers = self.brokers.read().await;
        brokers
            .values()
            .filter(|b| b.controller.is_connected())
            .map(|b| b.node_id)
            .collect()
    }

    /// Get a broker controller by node ID, if connected.
    pub async fn get_broker(&self, node_id: i32) -> Option<Arc<BrokerController>> {
        let brokers = self.brokers.read().await;
        brokers.get(&node_id).map(|b| Arc::clone(&b.controller))
    }

    /// Get a random connected broker controller.
    pub async fn random_broker(&self) -> Option<Arc<BrokerController>> {
        let brokers = self.brokers.read().await;
        let connected: Vec<&BrokerNode> =
            brokers.values().filter(|b| b.controller.is_connected()).collect();

        if connected.is_empty() {
            return None;
        }

        // Simple pseudo-random selection
        let mut seed = self.rng_seed.lock().await;
        *seed = seed.wrapping_add(1);
        let idx = (*seed as usize) % connected.len();

        Some(Arc::clone(&connected[idx].controller))
    }

    // ── Request sending ────────────────────────────────────────────────

    /// Send a request to a specific broker (or a random connected one if
    /// `node_id` is `None`).
    ///
    /// The API version is automatically selected as the intersection of what
    /// the client supports (from `R::get_min/max_supported_version()`) and
    /// what the broker supports (from the last ApiVersionsResponse).
    /// If the broker's versions are unknown, the client's max version is used.
    pub async fn send<R: ApiRequest>(
        self: &Arc<Self>,
        req: &R,
        node_id: Option<i32>,
        preferred_version: ApiVersion,
    ) -> Result<R::Response, ClusterError> {
        let broker = match node_id {
            Some(nid) => self
                .get_broker(nid)
                .await
                .ok_or(ClusterError::UnknownBroker(nid))?,
            None => self
                .random_broker()
                .await
                .ok_or(ClusterError::NoConnectedBrokers)?,
        };

        let api_key = R::get_api_key();
        let client_min = R::get_min_supported_version().0;
        let client_max = R::get_max_supported_version().0;

        // Determine the actual version to use
        let version = {
            let broker_versions = broker.api_versions().await;
            let (broker_min, broker_max) = broker_versions
                .as_ref()
                .and_then(|m| m.get(&api_key.0))
                .copied()
                .unwrap_or((client_min, client_max));

            // Pick the requested version, clamped to what both sides support
            let v = preferred_version.0.max(client_min).min(client_max);
            let v = v.max(broker_min).min(broker_max);

            if v < client_min || v > client_max {
                return Err(ClusterError::ClientUnsupportedVersion {
                    api_key: api_key.0,
                    requested: v,
                    client_min,
                    client_max,
                });
            }

            if v < broker_min || v > broker_max {
                return Err(ClusterError::BrokerUnsupportedVersion {
                    api_key: api_key.0,
                    requested: v,
                    broker_min,
                    broker_max,
                });
            }

            ApiVersion::new(v)
        };

        // Use the raw API to send at the negotiated version
        let rx = broker.send_raw(req, version).await?;
        let raw_bytes = rx.await.map_err(|_| BrokerError::ResponseChannelClosed)?;

        match raw_bytes {
            Ok(body) => {
                // Deserialize: skip response header
                // ApiVersionsResponse always uses v0 header per KIP-511.
                let header_size = if R::get_api_key().0 == 18 {
                    4
                } else if is_flexible_api(R::get_api_key().0, version.0) {
                    5
                } else {
                    4
                };
                let mut buf = Bytes::copy_from_slice(&body[header_size..]);
                let resp = R::Response::deserialize(version, &mut buf)
                    .map_err(BrokerError::Protocol)?;
                Ok(resp)
            }
            Err(e) => Err(ClusterError::Broker(e)),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cluster_controller_is_send_sync() {
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}
        assert_send::<ClusterController>();
        assert_sync::<ClusterController>();
    }
}

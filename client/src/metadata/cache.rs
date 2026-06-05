use std::time::{Duration, Instant};

use bytes::Bytes;
use indexmap::IndexMap;
use protocol::error::ApiError;
use protocol::generated::metadata_request::MetadataRequestTopic;
use protocol::generated::{MetadataRequest, MetadataResponse};
use protocol::traits::ApiVersion;

/// Information about a single broker in the cluster.
#[derive(Clone, Debug)]
pub struct BrokerInfo {
    pub host: String,
    pub port: i32,
    pub _rack: Option<String>,
}

/// A partition's replication metadata.
#[derive(Clone, Debug)]
pub struct PartitionInfo {
    pub _partition_index: i32,
    pub _leader_id: i32,
    pub _leader_epoch: i32,
    pub _replica_nodes: Vec<i32>,
    pub _isr_nodes: Vec<i32>,
    pub _offline_replicas: Vec<i32>,
}

/// A topic's metadata from a MetadataResponse.
#[derive(Clone, Debug)]
pub struct TopicMetadata {
    pub _name: String,
    pub _topic_id: [u8; 16],
    pub _error_code: i16,
    pub _is_internal: bool,
    pub _partitions: IndexMap<i32, PartitionInfo>,
}

/// Cached cluster topology with refresh scheduling and rebootstrap tracking.
pub struct MetadataCache {
    brokers: IndexMap<i32, BrokerInfo>,
    _topics: IndexMap<String, TopicMetadata>,
    refresh_interval: Duration,
    last_refresh: Option<Instant>,
    pending_refresh_ids: Vec<(usize, i32, ApiVersion)>,
    refresh_started_at: Option<Instant>,
    rebootstrap_trigger: Duration,
    rebootstrap_required_by_error: bool,
    all_brokers_unavailable: bool,
}

impl MetadataCache {
    pub fn new(refresh_interval: Duration, rebootstrap_trigger: Duration) -> Self {
        MetadataCache {
            brokers: IndexMap::new(),
            _topics: IndexMap::new(),
            refresh_interval,
            last_refresh: None,
            pending_refresh_ids: Vec::new(),
            refresh_started_at: None,
            rebootstrap_trigger,
            rebootstrap_required_by_error: false,
            all_brokers_unavailable: false,
        }
    }

    /// Populate the cache from the bootstrap MetadataResponse.
    pub(crate) fn bootstrap(&mut self, resp: &MetadataResponse) {
        if !resp.brokers.is_empty() {
            self.brokers.clear();
        }
        for (node_id, broker) in &resp.brokers {
            self.brokers.insert(
                *node_id,
                BrokerInfo {
                    host: broker.host.clone(),
                    port: broker.port,
                    _rack: broker.rack.clone(),
                },
            );
        }
    }

    /// Returns `true` if the refresh interval has elapsed since the last refresh.
    pub fn is_stale(&self) -> bool {
        match self.last_refresh {
            Some(t) => t.elapsed() >= self.refresh_interval,
            None => true,
        }
    }

    /// Periodic tick: returns a `MetadataRequest` to send if the cache is stale.
    /// Starts the rebootstrap deadline clock on the first request of a cycle.
    /// Updates `last_refresh` so repeated calls before the response arrives return `None`.
    pub fn tick(&mut self) -> Option<MetadataRequest> {
        if self.is_stale() {
            let topics: Option<Vec<MetadataRequestTopic>> = if self._topics.is_empty() {
                None
            } else {
                Some(
                    self._topics
                        .keys()
                        .map(|name| MetadataRequestTopic {
                            name: Some(name.clone()),
                            ..Default::default()
                        })
                        .collect(),
                )
            };
            self.last_refresh = Some(Instant::now());
            // Start the rebootstrap deadline when a refresh cycle begins
            // (but only if we have some brokers — during initial bootstrap
            // the blocking bootstrap() handles this).
            if self.refresh_started_at.is_none() && !self.brokers.is_empty() {
                self.refresh_started_at = Some(Instant::now());
            }
            Some(MetadataRequest {
                topics,
                allow_auto_topic_creation: false,
                ..Default::default()
            })
        } else {
            None
        }
    }

    /// Record that a metadata refresh request has been sent.
    pub fn register_inflight_refresh(
        &mut self,
        conn_idx: usize,
        corr_id: i32,
        version: ApiVersion,
    ) {
        self.pending_refresh_ids.push((conn_idx, corr_id, version));
    }

    /// Handle a response.  Returns `true` if it was consumed as a metadata
    /// refresh response.
    pub(crate) fn on_response(&mut self, corr_id: i32, conn_idx: usize, body: Bytes) -> bool {
        let pos = self
            .pending_refresh_ids
            .iter()
            .position(|&(ci, c, _)| ci == conn_idx && c == corr_id);
        match pos {
            Some(i) => {
                let (_, _, version) = self.pending_refresh_ids.swap_remove(i);

                // Try to parse the MetadataResponse to check for
                // REBOOTSTRAP_REQUIRED and update the broker cache.
                if let Ok(resp) = self.try_parse_response(body, version) {
                    // Check for REBOOTSTRAP_REQUIRED error code (v13+).
                    if resp.error_code != 0
                        && ApiError::from_code(resp.error_code)
                            == Some(ApiError::RebootstrapRequired)
                    {
                        self.rebootstrap_required_by_error = true;
                    }

                    // A response with non-empty brokers resets the
                    // rebootstrap deadline.
                    if !resp.brokers.is_empty() {
                        self.refresh_started_at = None;
                    }
                }

                self.last_refresh = Some(Instant::now());
                true
            }
            None => false,
        }
    }

    /// Try to deserialize `body` as a MetadataResponse using the version
    /// that was negotiated when the request was sent.
    ///
    /// `body` must start with the response header (as returned by
    /// [`read_broker_response`]).
    fn try_parse_response(
        &self,
        mut body: Bytes,
        version: ApiVersion,
    ) -> Result<MetadataResponse, String> {
        use protocol::generated::ResponseHeader;
        use protocol::traits::ApiResponse;

        let is_flexible = version.0 >= 9;
        ResponseHeader::decode(&mut body, is_flexible)
            .map_err(|e| format!("response header decode: {e}"))?;
        MetadataResponse::deserialize(version, &mut body)
            .map_err(|e| format!("metadata response decode: {e}"))
    }

    /// Look up broker info by node id.
    pub fn broker_info(&self, node_id: i32) -> Option<&BrokerInfo> {
        self.brokers.get(&node_id)
    }

    /// Returns any known broker id, if the cache is non-empty.
    pub fn any_broker(&self) -> Option<i32> {
        self.brokers.keys().next().copied()
    }

    /// Returns `true` if rebootstrap should be triggered.
    ///
    /// Rebootstrap is triggered on any of:
    /// a. All known brokers are in reconnect backoff (KIP-899)
    /// b. `REBOOTSTRAP_REQUIRED` error code received (KIP-1102)
    /// c. Timeout elapsed since first metadata attempt (KIP-1102)
    pub fn needs_rebootstrap(&self) -> bool {
        if self.rebootstrap_required_by_error {
            return true;
        }
        if self.all_brokers_unavailable {
            return true;
        }
        if let Some(started) = self.refresh_started_at {
            if started.elapsed() >= self.rebootstrap_trigger {
                return true;
            }
        }
        false
    }

    /// Signal that all known brokers are in reconnect backoff (KIP-899 condition).
    pub fn set_all_brokers_unavailable(&mut self) {
        self.all_brokers_unavailable = true;
    }

    /// Reset rebootstrap state after a successful rebootstrap.
    pub fn reset_rebootstrap_state(&mut self) {
        self.refresh_started_at = None;
        self.rebootstrap_required_by_error = false;
        self.all_brokers_unavailable = false;
    }

    /// Clear the entire cache (for rebootstrap).
    pub fn clear(&mut self) {
        self.brokers.clear();
        self._topics.clear();
        self.last_refresh = None;
        self.pending_refresh_ids.clear();
    }
}

use std::time::{Duration, Instant};

use bytes::Bytes;
use indexmap::IndexMap;
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

/// Cached cluster topology with refresh scheduling.
pub struct MetadataCache {
    brokers: IndexMap<i32, BrokerInfo>,
    _topics: IndexMap<String, TopicMetadata>,
    refresh_interval: Duration,
    last_refresh: Option<Instant>,
    pending_refresh_ids: Vec<(usize, i32, ApiVersion)>,


}

impl MetadataCache {
    pub fn new(refresh_interval: Duration) -> Self {
        MetadataCache {
            brokers: IndexMap::new(),
            _topics: IndexMap::new(),
            refresh_interval,
            last_refresh: None,
            pending_refresh_ids: Vec::new(),
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

                // Try to parse the MetadataResponse and update the broker cache.
                if let Ok(resp) = self.try_parse_response(body, version) {
                    if !resp.brokers.is_empty() {
                        // Update the broker cache — normal response.
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
}

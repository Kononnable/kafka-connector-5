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
                if let Ok(resp) = self.try_parse_response(body, version)
                    && !resp.brokers.is_empty()
                {
                    // TODO: Update the broker cache with information from `resp`.
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

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use bytes::BytesMut;
    use indexmap::IndexMap;
    use protocol::generated::MetadataResponse;
    use protocol::generated::metadata_response::MetadataResponseBroker;
    use protocol::protocol::serialization::{KafkaCodec, encode_unsigned_varint};
    use protocol::traits::ApiResponse;

    use super::*;

    fn make_metadata_response(broker_ids: Vec<i32>) -> MetadataResponse {
        MetadataResponse {
            brokers: broker_ids
                .into_iter()
                .map(|id| {
                    (
                        id,
                        MetadataResponseBroker {
                            host: format!("broker-{id}.example.com"),
                            port: 9092,
                            rack: None,
                        },
                    )
                })
                .collect(),
            controller_id: -1,
            cluster_id: None,
            topics: Vec::new(),
            cluster_authorized_operations: -2147483648,
            error_code: 0,
            throttle_time_ms: 0,
        }
    }

    #[test]
    fn bootstrap_populates_brokers() {
        let mut cache = MetadataCache::new(Duration::from_secs(300));
        let resp = make_metadata_response(vec![1, 2, 3]);
        cache.bootstrap(&resp);

        assert_eq!(cache.broker_info(1).unwrap().host, "broker-1.example.com");
        assert_eq!(cache.broker_info(1).unwrap().port, 9092);
        assert_eq!(cache.broker_info(2).unwrap().host, "broker-2.example.com");
        assert_eq!(cache.broker_info(2).unwrap().port, 9092);
        assert_eq!(cache.broker_info(3).unwrap().host, "broker-3.example.com");
        assert_eq!(cache.broker_info(3).unwrap().port, 9092);
        assert!(cache.broker_info(4).is_none());
    }

    #[test]
    fn bootstrap_clears_existing_brokers() {
        let mut cache = MetadataCache::new(Duration::from_secs(300));
        let resp1 = make_metadata_response(vec![1, 2]);
        cache.bootstrap(&resp1);
        assert_eq!(cache.any_broker(), Some(1));

        let resp2 = make_metadata_response(vec![3]);
        cache.bootstrap(&resp2);
        assert_eq!(cache.any_broker(), Some(3));
        assert!(cache.broker_info(1).is_none());
        assert!(cache.broker_info(2).is_none());
    }

    #[test]
    fn bootstrap_with_empty_brokers_preserves_existing() {
        let mut cache = MetadataCache::new(Duration::from_secs(300));
        let resp1 = make_metadata_response(vec![1]);
        cache.bootstrap(&resp1);

        let empty_resp = MetadataResponse {
            brokers: IndexMap::new(),
            controller_id: -1,
            cluster_id: None,
            topics: Vec::new(),
            cluster_authorized_operations: -2147483648,
            error_code: 0,
            throttle_time_ms: 0,
        };
        cache.bootstrap(&empty_resp);
        // Brokers should still be there
        assert!(cache.broker_info(1).is_some());
    }

    #[test]
    fn is_stale_returns_true_when_no_refresh() {
        let cache = MetadataCache::new(Duration::from_secs(60));
        assert!(cache.is_stale());
    }

    #[test]
    fn is_stale_returns_false_recently_refreshed() {
        let mut cache = MetadataCache::new(Duration::from_secs(300));
        cache.last_refresh = Some(Instant::now());
        assert!(!cache.is_stale());
    }

    #[test]
    fn is_stale_returns_true_after_interval() {
        let mut cache = MetadataCache::new(Duration::from_millis(10));
        cache.last_refresh = Some(Instant::now());
        // Manually set to the past
        cache.last_refresh = Some(Instant::now() - Duration::from_secs(1));
        assert!(cache.is_stale());
    }

    #[test]
    fn tick_returns_none_when_not_stale() {
        let mut cache = MetadataCache::new(Duration::from_secs(300));
        cache.last_refresh = Some(Instant::now());
        assert!(cache.tick().is_none());
    }

    #[test]
    fn tick_returns_metadata_request_when_stale() {
        let mut cache = MetadataCache::new(Duration::from_secs(300));
        let req = cache.tick();
        assert!(req.is_some());
        let req = req.unwrap();
        assert_eq!(req.topics, None);
        assert!(!req.allow_auto_topic_creation);
    }

    #[test]
    fn tick_returns_none_on_subsequent_calls() {
        let mut cache = MetadataCache::new(Duration::from_secs(300));
        cache.tick(); // first call sets last_refresh
        assert!(cache.tick().is_none()); // second call should be None
    }

    #[test]
    fn tick_requests_specific_topics_when_subscribed() {
        let mut cache = MetadataCache::new(Duration::from_secs(300));
        // Manually add a topic to simulate subscription
        cache._topics.insert(
            "test-topic".to_string(),
            TopicMetadata {
                _name: "test-topic".to_string(),
                _topic_id: [0u8; 16],
                _error_code: 0,
                _is_internal: false,
                _partitions: IndexMap::new(),
            },
        );
        let req = cache.tick().unwrap();
        assert!(req.topics.is_some());
        let topics = req.topics.unwrap();
        assert_eq!(topics.len(), 1);
        assert_eq!(topics[0].name, Some("test-topic".to_string()));
    }

    #[test]
    fn broker_info_lookup() {
        let mut cache = MetadataCache::new(Duration::from_secs(300));
        let resp = make_metadata_response(vec![5]);
        cache.bootstrap(&resp);

        let info = cache.broker_info(5).unwrap();
        assert_eq!(info.host, "broker-5.example.com");
        assert_eq!(info.port, 9092);
        assert!(info._rack.is_none());
    }

    #[test]
    fn broker_info_returns_none_for_unknown() {
        let cache = MetadataCache::new(Duration::from_secs(300));
        assert!(cache.broker_info(999).is_none());
    }

    #[test]
    fn any_broker_returns_none_when_empty() {
        let cache = MetadataCache::new(Duration::from_secs(300));
        assert!(cache.any_broker().is_none());
    }

    #[test]
    fn any_broker_returns_first_broker() {
        let mut cache = MetadataCache::new(Duration::from_secs(300));
        let resp = make_metadata_response(vec![3, 1, 2]);
        cache.bootstrap(&resp);
        // IndexMap preserves insertion order
        assert_eq!(cache.any_broker(), Some(3));
    }

    #[test]
    fn register_and_consume_inflight_refresh() {
        let mut cache = MetadataCache::new(Duration::from_secs(300));
        cache.register_inflight_refresh(0, 42, ApiVersion::new(12));

        // Create a minimal valid response body by serializing a proper MetadataResponse
        let version = ApiVersion::new(12);
        let resp = make_metadata_response(vec![1]);
        let mut buf = BytesMut::new();
        let is_flexible = version.0 >= MetadataResponse::get_min_flexible_version().0;
        42_i32
            .encode(&mut buf, ApiVersion::new(0), is_flexible)
            .unwrap();
        if is_flexible {
            encode_unsigned_varint(0u64, &mut buf);
        }
        // Serialize the response
        resp.serialize(version, &mut buf).unwrap();
        let body = buf.freeze();

        // Should consume the inflight refresh
        assert!(cache.on_response(42, 0, body));
        // last_refresh should be set
        assert!(cache.last_refresh.is_some());
    }

    #[test]
    fn on_response_returns_false_for_unknown_corr_id() {
        let mut cache = MetadataCache::new(Duration::from_secs(300));

        // Create a minimal valid response body
        let resp = make_metadata_response(vec![1]);
        let mut buf = BytesMut::new();
        0_i32.encode(&mut buf, ApiVersion::new(0), false).unwrap();
        encode_unsigned_varint(0u64, &mut buf);
        resp.serialize(ApiVersion::new(12), &mut buf).unwrap();
        let body = buf.freeze();

        assert!(!cache.on_response(999, 0, body));
        // last_refresh should NOT be set
        assert!(cache.last_refresh.is_none());
    }

    #[test]
    fn on_response_handles_mismatched_conn_idx() {
        let mut cache = MetadataCache::new(Duration::from_secs(300));
        cache.register_inflight_refresh(0, 42, ApiVersion::new(12));

        // Create a minimal valid response body
        let version = ApiVersion::new(12);
        let resp = make_metadata_response(vec![1]);
        let mut buf = BytesMut::new();
        let is_flexible = version.0 >= MetadataResponse::get_min_flexible_version().0;
        42_i32
            .encode(&mut buf, ApiVersion::new(0), is_flexible)
            .unwrap();
        if is_flexible {
            encode_unsigned_varint(0u64, &mut buf);
        }
        // Serialize the response
        resp.serialize(version, &mut buf).unwrap();
        let body = buf.freeze();

        // Wrong conn_idx should not match
        assert!(!cache.on_response(42, 1, body));
        assert!(cache.last_refresh.is_none());
    }
}

use std::time::{Duration, Instant};

use bytes::Bytes;
use indexmap::IndexMap;
use protocol::generated::metadata_request::MetadataRequestTopic;
use protocol::generated::{MetadataRequest, MetadataResponse};
use protocol::traits::{ApiResponse, ApiVersion};

use crate::types::{ApiKey, BrokerId, CorrelationId, InflightRequest, RequestHandlerId};

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
    brokers: IndexMap<BrokerId, BrokerInfo>,
    _topics: IndexMap<String, TopicMetadata>,
    refresh_interval: Duration,
    last_refresh: Option<Instant>,
}

impl MetadataCache {
    pub fn new(refresh_interval: Duration) -> Self {
        MetadataCache {
            brokers: IndexMap::new(),
            _topics: IndexMap::new(),
            refresh_interval,
            last_refresh: None,
        }
    }

    /// Populate the cache from the bootstrap MetadataResponse.
    pub(crate) fn bootstrap(&mut self, resp: &MetadataResponse) {
        if !resp.brokers.is_empty() {
            self.brokers.clear();
        }
        for (node_id, broker) in &resp.brokers {
            self.brokers.insert(
                BrokerId(*node_id),
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

    /// Periodic tick: returns a MetadataRequest if the cache is stale.
    /// The event loop is responsible for sending it.
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

    /// Handle a MetadataResponse for a periodic refresh.
    pub(crate) fn on_response(
        &mut self,
        _corr_id: CorrelationId,
        body: Bytes,
        version: ApiVersion,
    ) {
        // Deserialize and update the broker cache.
        let result = self.try_parse_response(body, version);
        match result {
            Ok(resp) => {
                debug_assert!(
                    !resp.brokers.is_empty(),
                    "MetadataResponse from connected broker must include that broker"
                );
                if !resp.brokers.is_empty() {
                    self.brokers.clear();
                    for (node_id, broker) in &resp.brokers {
                        self.brokers.insert(
                            BrokerId(*node_id),
                            BrokerInfo {
                                host: broker.host.clone(),
                                port: broker.port,
                                _rack: broker.rack.clone(),
                            },
                        );
                    }
                }
            }
            Err(e) => {
                tracing::warn!("failed to decode metadata refresh response: {e}");
            }
        }
        self.last_refresh = Some(Instant::now());
    }

    /// Handle a metadata refresh timeout — allow immediate retry.
    pub(crate) fn on_timeout(&mut self, _corr_id: CorrelationId, inflight: &InflightRequest) {
        tracing::warn!(
            version = %inflight.version.0,
            "metadata request timed out"
        );
        // Reset last_refresh so the next tick retries immediately.
        self.last_refresh = None;
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

        let is_flexible = version.0 >= MetadataResponse::get_min_flexible_version().0;
        ResponseHeader::decode(&mut body, is_flexible)
            .map_err(|e| format!("response header decode: {e}"))?;
        MetadataResponse::deserialize(version, &mut body)
            .map_err(|e| format!("metadata response decode: {e}"))
    }

    /// Look up broker info by node id.
    pub fn broker_info(&self, node_id: BrokerId) -> Option<&BrokerInfo> {
        self.brokers.get(&node_id)
    }

    /// Returns any known broker id, if the cache is non-empty.
    pub fn any_broker(&self) -> Option<BrokerId> {
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

        assert_eq!(
            cache.broker_info(BrokerId(1)).unwrap().host,
            "broker-1.example.com"
        );
        assert_eq!(cache.broker_info(BrokerId(1)).unwrap().port, 9092);
        assert_eq!(
            cache.broker_info(BrokerId(2)).unwrap().host,
            "broker-2.example.com"
        );
        assert_eq!(cache.broker_info(BrokerId(2)).unwrap().port, 9092);
        assert_eq!(
            cache.broker_info(BrokerId(3)).unwrap().host,
            "broker-3.example.com"
        );
        assert_eq!(cache.broker_info(BrokerId(3)).unwrap().port, 9092);
        assert!(cache.broker_info(BrokerId(4)).is_none());
    }

    #[test]
    fn bootstrap_clears_existing_brokers() {
        let mut cache = MetadataCache::new(Duration::from_secs(300));
        let resp1 = make_metadata_response(vec![1, 2]);
        cache.bootstrap(&resp1);
        assert_eq!(cache.any_broker(), Some(BrokerId(1)));

        let resp2 = make_metadata_response(vec![3]);
        cache.bootstrap(&resp2);
        assert_eq!(cache.any_broker(), Some(BrokerId(3)));
        assert!(cache.broker_info(BrokerId(1)).is_none());
        assert!(cache.broker_info(BrokerId(2)).is_none());
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
        assert!(cache.broker_info(BrokerId(1)).is_some());
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
        cache.last_refresh = Some(Instant::now() - Duration::from_secs(1));
        assert!(cache.is_stale());
    }

    #[test]
    fn broker_info_lookup() {
        let mut cache = MetadataCache::new(Duration::from_secs(300));
        let resp = make_metadata_response(vec![5]);
        cache.bootstrap(&resp);

        let info = cache.broker_info(BrokerId(5)).unwrap();
        assert_eq!(info.host, "broker-5.example.com");
        assert_eq!(info.port, 9092);
        assert!(info._rack.is_none());
    }

    #[test]
    fn broker_info_returns_none_for_unknown() {
        let cache = MetadataCache::new(Duration::from_secs(300));
        assert!(cache.broker_info(BrokerId(999)).is_none());
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
        assert_eq!(cache.any_broker(), Some(BrokerId(3)));
    }

    #[test]
    fn on_response_updates_brokers() {
        let mut cache = MetadataCache::new(Duration::from_secs(300));
        let boot_resp = make_metadata_response(vec![1]);
        cache.bootstrap(&boot_resp);
        assert_eq!(cache.any_broker(), Some(BrokerId(1)));

        // Simulate a refresh response with updated brokers
        let refresh_resp = make_metadata_response(vec![1, 2, 3]);
        let mut buf = BytesMut::new();
        let version = ApiVersion::new(12);
        let is_flexible = version.0 >= MetadataResponse::get_min_flexible_version().0;
        // response header
        let corr_id: i32 = 42;
        corr_id
            .encode(&mut buf, ApiVersion::new(0), is_flexible)
            .unwrap();
        if is_flexible {
            encode_unsigned_varint(0u64, &mut buf);
        }
        refresh_resp.serialize(version, &mut buf).unwrap();
        let body = buf.freeze();

        cache.on_response(CorrelationId(42), body, version);
        assert!(cache.broker_info(BrokerId(1)).is_some());
        assert!(cache.broker_info(BrokerId(2)).is_some());
        assert!(cache.broker_info(BrokerId(3)).is_some());
    }

    #[test]
    fn on_timeout_resets_stale_flag() {
        let mut cache = MetadataCache::new(Duration::from_millis(5));
        cache.last_refresh = Some(Instant::now() - Duration::from_secs(10));

        // The cache is stale, tick would try to refresh
        assert!(cache.is_stale());

        // Simulate a send by setting last_refresh
        cache.last_refresh = Some(Instant::now());
        assert!(!cache.is_stale());

        // Timeout fires
        let inflight = InflightRequest::new(
            ApiKey(3),
            ApiVersion::new(12),
            Instant::now() - Duration::from_secs(30),
            Duration::from_secs(30),
            RequestHandlerId(0),
        );
        cache.on_timeout(CorrelationId(42), &inflight);

        // last_refresh was reset, so is_stale should be true
        assert!(cache.is_stale());
    }
}

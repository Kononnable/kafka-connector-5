use std::time::Duration;

use bytes::Bytes;
use indexmap::IndexMap;
use mio::Registry;
use protocol::generated::MetadataResponse;

use crate::connection::ConnectionPool;

/// Information about a single broker in the cluster.
#[derive(Clone, Debug)]
pub struct BrokerInfo {
    pub host: String,
    pub port: i32,
    pub rack: Option<String>,
}

/// A partition's replication metadata.
#[derive(Clone, Debug)]
pub struct PartitionInfo {
    pub partition_index: i32,
    pub leader_id: i32,
    pub leader_epoch: i32,
    pub replica_nodes: Vec<i32>,
    pub isr_nodes: Vec<i32>,
    pub offline_replicas: Vec<i32>,
}

/// A topic's metadata from a MetadataResponse.
#[derive(Clone, Debug)]
pub struct TopicMetadata {
    pub name: String,
    pub topic_id: [u8; 16],
    pub error_code: i16,
    pub is_internal: bool,
    pub partitions: IndexMap<i32, PartitionInfo>,
}

/// Cached cluster topology with refresh scheduling.
pub struct MetadataCache {
    brokers: IndexMap<i32, BrokerInfo>,
    controller_id: i32,
    topics: IndexMap<String, TopicMetadata>,
    refresh_interval: Duration,
}

impl MetadataCache {
    pub fn new(refresh_interval: Duration) -> Self {
        MetadataCache {
            brokers: IndexMap::new(),
            controller_id: -1,
            topics: IndexMap::new(),
            refresh_interval,
        }
    }

    /// Populate the cache from the initial bootstrap MetadataResponse.
    /// Panics if the cache is not empty.
    pub(crate) fn bootstrap(&mut self, resp: &MetadataResponse) {
        assert!(self.brokers.is_empty(), "metadata cache already populated");
        self.controller_id = resp.controller_id;
        for (node_id, broker) in &resp.brokers {
            self.brokers.insert(
                *node_id,
                BrokerInfo {
                    host: broker.host.clone(),
                    port: broker.port,
                    rack: broker.rack.clone(),
                },
            );
        }
    }

    /// Stub — to be implemented when metadata request dispatch is integrated.
    pub(crate) fn tick(&mut self, _pool: &mut ConnectionPool, _registry: &Registry) {
        // TODO: implement metadata refresh tick logic
    }

    /// Stub — returns false to indicate the response was not handled.
    pub(crate) fn on_response(&mut self, _corr_id: i32, _conn_idx: usize, _body: Bytes) -> bool {
        false
    }
}

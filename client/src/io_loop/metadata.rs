use std::time::Duration;

use bytes::Bytes;
use indexmap::IndexMap;
use mio::Registry;
use protocol::generated::MetadataResponse;

use super::connection::ConnectionPool;

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
    pub brokers: IndexMap<i32, BrokerInfo>,
    pub controller_id: i32,
    pub topics: IndexMap<String, TopicMetadata>,
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

    /// Stub — to be implemented when metadata request dispatch is integrated.
    pub fn tick(&mut self, _pool: &mut ConnectionPool, _registry: &Registry) {
        // TODO: implement metadata refresh tick logic
    }

    /// Stub — returns false to indicate the response was not handled.
    pub fn on_response(&mut self, _corr_id: i32, _conn_idx: usize, _body: Bytes) -> bool {
        false
    }

    /// Apply a MetadataResponse to the cache, replacing existing broker/topic state.
    pub fn apply(&mut self, response: &MetadataResponse) {
        self.controller_id = response.controller_id;

        for (node_id, broker) in &response.brokers {
            self.brokers.insert(
                *node_id,
                BrokerInfo {
                    host: broker.host.clone(),
                    port: broker.port,
                    rack: broker.rack.clone(),
                },
            );
        }

        let mut new_topics = IndexMap::new();
        for topic in &response.topics {
            if let Some(ref name) = topic.name {
                let mut partitions = IndexMap::new();
                for part in &topic.partitions {
                    partitions.insert(
                        part.partition_index,
                        PartitionInfo {
                            partition_index: part.partition_index,
                            leader_id: part.leader_id,
                            leader_epoch: part.leader_epoch,
                            replica_nodes: part.replica_nodes.clone(),
                            isr_nodes: part.isr_nodes.clone(),
                            offline_replicas: part.offline_replicas.clone(),
                        },
                    );
                }
                let meta = TopicMetadata {
                    name: name.clone(),
                    topic_id: topic.topic_id,
                    error_code: topic.error_code,
                    is_internal: topic.is_internal,
                    partitions,
                };
                new_topics.insert(name.clone(), meta);
            }
        }
        self.topics = new_topics;
    }
}

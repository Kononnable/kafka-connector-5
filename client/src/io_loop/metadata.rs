use indexmap::IndexMap;

/// Information about a single broker in the cluster.
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct BrokerInfo {
    pub host: String,
    pub port: i32,
}

/// Cached cluster topology.
///
/// Key is the broker's node id (`i32`), matching the wire protocol.
#[derive(Clone, Debug, Default)]
pub struct MetadataCache {
    /// All known brokers, keyed by node id.
    pub brokers: IndexMap<i32, BrokerInfo>,
}

impl MetadataCache {
    pub fn new() -> Self {
        MetadataCache {
            brokers: IndexMap::new(),
        }
    }
}

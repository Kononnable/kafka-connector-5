use std::collections::HashMap;

struct BrokerInfo;

struct PartitionInfo;

pub struct MetadataCache {
    _brokers: HashMap<u32, BrokerInfo>,
    _partitions: HashMap<(String, u32), PartitionInfo>,
}

impl MetadataCache {
    pub fn new() -> Self {
        MetadataCache {
            _brokers: HashMap::new(),
            _partitions: HashMap::new(),
        }
    }
}

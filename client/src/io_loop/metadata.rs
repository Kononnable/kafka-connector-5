use std::collections::HashMap;

pub(crate) struct BrokerInfo;

pub(crate) struct PartitionInfo;

pub(crate) struct MetadataCache {
    _brokers: HashMap<u32, BrokerInfo>,
    _partitions: HashMap<(String, u32), PartitionInfo>,
}

impl MetadataCache {
    pub(crate) fn new() -> Self {
        MetadataCache {
            _brokers: HashMap::new(),
            _partitions: HashMap::new(),
        }
    }
}

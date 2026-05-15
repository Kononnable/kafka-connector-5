use std::collections::HashMap;

pub(crate) struct BrokerInfo;

pub(crate) struct PartitionInfo;

pub(crate) struct MetadataCache {
    brokers: HashMap<u32, BrokerInfo>,
    partitions: HashMap<(String, u32), PartitionInfo>,
}

impl MetadataCache {
    pub(crate) fn new() -> Self {
        MetadataCache {
            brokers: HashMap::new(),
            partitions: HashMap::new(),
        }
    }
}

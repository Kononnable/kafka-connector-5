#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVer, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// DescribeQuorumResponse
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct DescribeQuorumResponse {
    /// The top level error code.
    pub error_code: i16,
    /// The error message, or null if there was no error.
    /// Available in version 2+.
    pub error_message: Option<String>,
    /// The response from the describe quorum API.
    pub topics: Vec<TopicData>,
    /// The nodes in the quorum.
    /// Available in version 2+.
    pub nodes: Vec<Node>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Listener {
    /// The name of the endpoint.
    /// Available in version 2+.
    pub name: String,
    /// The hostname.
    /// Available in version 2+.
    pub host: String,
    /// The port.
    /// Available in version 2+.
    pub port: u16,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Node {
    /// The ID of the associated node.
    /// Available in version 2+.
    pub node_id: i32,
    /// The listeners of this controller.
    /// Available in version 2+.
    pub listeners: Vec<Listener>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct PartitionData {
    /// The partition index.
    pub partition_index: i32,
    /// The partition error code.
    pub error_code: i16,
    /// The error message, or null if there was no error.
    /// Available in version 2+.
    pub error_message: Option<String>,
    /// The ID of the current leader or -1 if the leader is unknown.
    pub leader_id: i32,
    /// The latest known leader epoch.
    pub leader_epoch: i32,
    /// The high water mark.
    pub high_watermark: i64,
    /// The current voters of the partition.
    pub current_voters: Vec<ReplicaState>,
    /// The observers of the partition.
    pub observers: Vec<ReplicaState>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ReplicaState {
    /// The ID of the replica.
    pub replica_id: i32,
    /// The replica directory ID of the replica.
    /// Available in version 2+.
    pub replica_directory_id: [u8; 16],
    /// The last known log end offset of the follower or -1 if it is unknown.
    pub log_end_offset: i64,
    /// The last known leader wall clock time time when a follower fetched from the leader. This is reported as -1 both for the current leader or if it is unknown for a voter.
    /// Available in version 1+.
    pub last_fetch_timestamp: i64,
    /// The leader wall clock append time of the offset for which the follower made the most recent fetch request. This is reported as the current time for the leader and -1 if unknown for a voter.
    /// Available in version 1+.
    pub last_caught_up_timestamp: i64,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct TopicData {
    /// The topic name.
    pub topic_name: String,
    /// The partition data.
    pub partitions: Vec<PartitionData>,
}

impl ApiResponse for DescribeQuorumResponse {
    type Request = crate::generated::DescribeQuorumRequest;
    fn get_api_key() -> ApiKey {
        ApiKey::new(55)
    }
    fn get_min_supported_version() -> ApiVer {
        ApiVer::new(0)
    }
    fn get_max_supported_version() -> ApiVer {
        ApiVer::new(2)
    }
    fn get_min_flexible_version() -> ApiVer {
        ApiVer::new(0)
    }
    fn serialize(&self, version: ApiVer, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            (0) <= version.0 && version.0 <= (2),
            "version {} is not supported by {} (supported: 0-2)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        self.error_code.encode(buf, version, is_flexible)?;
        if (2) <= version.0 {
            self.error_message.encode(buf, version, is_flexible)?;
        } else if self.error_message.is_some() {
            return Err(SerializationError::Encode(
                "field 'ErrorMessage' is not available in this version",
            ));
        }
        self.topics.encode(buf, version, is_flexible)?;
        if (2) <= version.0 {
            self.nodes.encode(buf, version, is_flexible)?;
        } else if !self.nodes.is_empty() {
            return Err(SerializationError::Encode(
                "field 'Nodes' is not available in this version",
            ));
        }
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(version: ApiVer, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let error_code = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let error_message = if (2) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let topics = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let nodes = if (2) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            error_code,
            error_message,
            topics,
            nodes,
        })
    }
}
impl KafkaSerialize for DescribeQuorumResponse {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.error_code.encode(buf, version, is_flexible)?;
        if (2) <= version.0 {
            self.error_message.encode(buf, version, is_flexible)?;
        }
        self.topics.encode(buf, version, is_flexible)?;
        if (2) <= version.0 {
            self.nodes.encode(buf, version, is_flexible)?;
        }
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for DescribeQuorumResponse {
    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let error_code = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let error_message = if (2) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let topics = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let nodes = if (2) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            error_code,
            error_message,
            topics,
            nodes,
        })
    }
}

impl KafkaSerialize for Listener {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        if (2) <= version.0 {
            self.name.encode(buf, version, is_flexible)?;
        }
        if (2) <= version.0 {
            self.host.encode(buf, version, is_flexible)?;
        }
        if (2) <= version.0 {
            self.port.encode(buf, version, is_flexible)?;
        }
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for Listener {
    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let name = if (2) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let host = if (2) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let port = if (2) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self { name, host, port })
    }
}

impl KafkaSerialize for Node {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        if (2) <= version.0 {
            self.node_id.encode(buf, version, is_flexible)?;
        }
        if (2) <= version.0 {
            self.listeners.encode(buf, version, is_flexible)?;
        }
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for Node {
    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let node_id = if (2) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let listeners = if (2) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self { node_id, listeners })
    }
}

impl KafkaSerialize for PartitionData {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.partition_index.encode(buf, version, is_flexible)?;
        self.error_code.encode(buf, version, is_flexible)?;
        if (2) <= version.0 {
            self.error_message.encode(buf, version, is_flexible)?;
        }
        self.leader_id.encode(buf, version, is_flexible)?;
        self.leader_epoch.encode(buf, version, is_flexible)?;
        self.high_watermark.encode(buf, version, is_flexible)?;
        self.current_voters.encode(buf, version, is_flexible)?;
        self.observers.encode(buf, version, is_flexible)?;
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for PartitionData {
    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let partition_index = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let error_code = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let error_message = if (2) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let leader_id = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let leader_epoch = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let high_watermark = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let current_voters = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let observers = KafkaDeserialize::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            partition_index,
            error_code,
            error_message,
            leader_id,
            leader_epoch,
            high_watermark,
            current_voters,
            observers,
        })
    }
}

impl KafkaSerialize for ReplicaState {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.replica_id.encode(buf, version, is_flexible)?;
        if (2) <= version.0 {
            self.replica_directory_id
                .encode(buf, version, is_flexible)?;
        }
        self.log_end_offset.encode(buf, version, is_flexible)?;
        if (1) <= version.0 {
            self.last_fetch_timestamp
                .encode(buf, version, is_flexible)?;
        }
        if (1) <= version.0 {
            self.last_caught_up_timestamp
                .encode(buf, version, is_flexible)?;
        }
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for ReplicaState {
    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let replica_id = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let replica_directory_id = if (2) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let log_end_offset = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let last_fetch_timestamp = if (1) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let last_caught_up_timestamp = if (1) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            replica_id,
            replica_directory_id,
            log_end_offset,
            last_fetch_timestamp,
            last_caught_up_timestamp,
        })
    }
}

impl KafkaSerialize for TopicData {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.topic_name.encode(buf, version, is_flexible)?;
        self.partitions.encode(buf, version, is_flexible)?;
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for TopicData {
    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let topic_name = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let partitions = KafkaDeserialize::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            topic_name,
            partitions,
        })
    }
}

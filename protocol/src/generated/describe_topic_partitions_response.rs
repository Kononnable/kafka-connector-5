#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{KafkaCodec, decode_unsigned_varint, encode_unsigned_varint};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVer, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use indexmap::IndexMap;

// -------------------------------------------------------
// DescribeTopicPartitionsResponse
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct DescribeTopicPartitionsResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// Each topic in the response.
    pub topics: Vec<DescribeTopicPartitionsResponseTopic>,
    /// The next topic and partition index to fetch details for.
    pub next_cursor: Option<Cursor>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Cursor {
    /// The name for the first topic to process.
    pub topic_name: String,
    /// The partition index to start with.
    pub partition_index: i32,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct DescribeTopicPartitionsResponsePartition {
    /// The partition error, or 0 if there was no error.
    pub error_code: i16,
    /// The partition index.
    pub partition_index: i32,
    /// The ID of the leader broker.
    pub leader_id: i32,
    /// The leader epoch of this partition.
    pub leader_epoch: i32,
    /// The set of all nodes that host this partition.
    pub replica_nodes: Vec<i32>,
    /// The set of nodes that are in sync with the leader for this partition.
    pub isr_nodes: Vec<i32>,
    /// The new eligible leader replicas otherwise.
    pub eligible_leader_replicas: Option<Vec<i32>>,
    /// The last known ELR.
    pub last_known_elr: Option<Vec<i32>>,
    /// The set of offline replicas of this partition.
    pub offline_replicas: Vec<i32>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct DescribeTopicPartitionsResponseTopic {
    /// The topic error, or 0 if there was no error.
    pub error_code: i16,
    /// The topic name.
    pub name: Option<String>,
    /// The topic id.
    pub topic_id: [u8; 16],
    /// True if the topic is internal.
    pub is_internal: bool,
    /// Each partition in the topic.
    pub partitions: Vec<DescribeTopicPartitionsResponsePartition>,
    /// 32-bit bitfield to represent authorized operations for this topic.
    pub topic_authorized_operations: i32,
}

impl ApiResponse for DescribeTopicPartitionsResponse {
    type Request = crate::generated::DescribeTopicPartitionsRequest;
    fn get_api_key() -> ApiKey {
        ApiKey::new(75)
    }
    fn get_min_supported_version() -> ApiVer {
        ApiVer::new(0)
    }
    fn get_max_supported_version() -> ApiVer {
        ApiVer::new(0)
    }
    fn get_min_flexible_version() -> ApiVer {
        ApiVer::new(0)
    }
    fn serialize(&self, version: ApiVer, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            0 <= version.0 && version.0 <= 0,
            "version {} is not supported by {} (supported: 0-0)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        self.throttle_time_ms.encode(buf, version, is_flexible)?;
        self.topics.encode(buf, version, is_flexible)?;
        if is_flexible {
            if let Some(ref val) = self.next_cursor {
                encode_unsigned_varint(1u64, buf);
                val.encode(buf, version, true)?;
            } else {
                encode_unsigned_varint(0u64, buf);
            }
        } else {
            if let Some(ref val) = self.next_cursor {
                val.encode(buf, version, false)?;
            }
        }
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(version: ApiVer, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let throttle_time_ms = KafkaCodec::decode(buf, version, is_flexible)?;
        let topics = KafkaCodec::decode(buf, version, is_flexible)?;
        let next_cursor = if is_flexible {
            let (present, _) = decode_unsigned_varint(buf)?;
            if present == 0 {
                None
            } else {
                Some(KafkaCodec::decode(buf, version, true)?)
            }
        } else {
            Some(KafkaCodec::decode(buf, version, false)?)
        };
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            throttle_time_ms,
            topics,
            next_cursor,
        })
    }
}
impl KafkaCodec for DescribeTopicPartitionsResponse {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.throttle_time_ms.encode(buf, version, is_flexible)?;
        self.topics.encode(buf, version, is_flexible)?;
        if is_flexible {
            if let Some(ref val) = self.next_cursor {
                encode_unsigned_varint(1u64, buf);
                val.encode(buf, version, true)?;
            } else {
                encode_unsigned_varint(0u64, buf);
            }
        } else {
            if let Some(ref val) = self.next_cursor {
                val.encode(buf, version, false)?;
            }
        }
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }

    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let throttle_time_ms = KafkaCodec::decode(buf, version, is_flexible)?;
        let topics = KafkaCodec::decode(buf, version, is_flexible)?;
        let next_cursor = if is_flexible {
            let (present, _) = decode_unsigned_varint(buf)?;
            if present == 0 {
                None
            } else {
                Some(KafkaCodec::decode(buf, version, true)?)
            }
        } else {
            Some(KafkaCodec::decode(buf, version, false)?)
        };
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            throttle_time_ms,
            topics,
            next_cursor,
        })
    }
}

impl KafkaCodec for Cursor {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.topic_name.encode(buf, version, is_flexible)?;
        self.partition_index.encode(buf, version, is_flexible)?;
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }

    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let topic_name = KafkaCodec::decode(buf, version, is_flexible)?;
        let partition_index = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            topic_name,
            partition_index,
        })
    }
}

impl KafkaCodec for DescribeTopicPartitionsResponsePartition {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.error_code.encode(buf, version, is_flexible)?;
        self.partition_index.encode(buf, version, is_flexible)?;
        self.leader_id.encode(buf, version, is_flexible)?;
        self.leader_epoch.encode(buf, version, is_flexible)?;
        self.replica_nodes.encode(buf, version, is_flexible)?;
        self.isr_nodes.encode(buf, version, is_flexible)?;
        self.eligible_leader_replicas
            .encode(buf, version, is_flexible)?;
        self.last_known_elr.encode(buf, version, is_flexible)?;
        self.offline_replicas.encode(buf, version, is_flexible)?;
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }

    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let error_code = KafkaCodec::decode(buf, version, is_flexible)?;
        let partition_index = KafkaCodec::decode(buf, version, is_flexible)?;
        let leader_id = KafkaCodec::decode(buf, version, is_flexible)?;
        let leader_epoch = KafkaCodec::decode(buf, version, is_flexible)?;
        let replica_nodes = KafkaCodec::decode(buf, version, is_flexible)?;
        let isr_nodes = KafkaCodec::decode(buf, version, is_flexible)?;
        let eligible_leader_replicas = KafkaCodec::decode(buf, version, is_flexible)?;
        let last_known_elr = KafkaCodec::decode(buf, version, is_flexible)?;
        let offline_replicas = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            error_code,
            partition_index,
            leader_id,
            leader_epoch,
            replica_nodes,
            isr_nodes,
            eligible_leader_replicas,
            last_known_elr,
            offline_replicas,
        })
    }
}

impl KafkaCodec for DescribeTopicPartitionsResponseTopic {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.error_code.encode(buf, version, is_flexible)?;
        self.name.encode(buf, version, is_flexible)?;
        self.topic_id.encode(buf, version, is_flexible)?;
        self.is_internal.encode(buf, version, is_flexible)?;
        self.partitions.encode(buf, version, is_flexible)?;
        self.topic_authorized_operations
            .encode(buf, version, is_flexible)?;
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }

    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let error_code = KafkaCodec::decode(buf, version, is_flexible)?;
        let name = KafkaCodec::decode(buf, version, is_flexible)?;
        let topic_id = KafkaCodec::decode(buf, version, is_flexible)?;
        let is_internal = KafkaCodec::decode(buf, version, is_flexible)?;
        let partitions = KafkaCodec::decode(buf, version, is_flexible)?;
        let topic_authorized_operations = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            error_code,
            name,
            topic_id,
            is_internal,
            partitions,
            topic_authorized_operations,
        })
    }
}

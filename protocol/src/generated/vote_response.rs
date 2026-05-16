#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{KafkaCodec, decode_unsigned_varint, encode_unsigned_varint};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVer, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use indexmap::IndexMap;

// -------------------------------------------------------
// VoteResponse
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct VoteResponse {
    /// The top level error code.
    pub error_code: i16,
    /// The results for each topic.
    pub topics: Vec<TopicData>,
    /// Endpoints for all current-leaders enumerated in PartitionData.
    /// IndexMap key `NodeId` (int32): The ID of the associated node.
    /// Available in version 1+.
    pub node_endpoints: IndexMap<i32, NodeEndpoint>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct NodeEndpoint {
    /// The node's hostname.
    /// Available in version 1+.
    pub host: String,
    /// The node's port.
    /// Available in version 1+.
    pub port: u16,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct PartitionData {
    /// The partition index.
    pub partition_index: i32,
    /// The partition level error code.
    pub error_code: i16,
    /// The ID of the current leader or -1 if the leader is unknown.
    pub leader_id: i32,
    /// The latest known leader epoch.
    pub leader_epoch: i32,
    /// True if the vote was granted and false otherwise.
    pub vote_granted: bool,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct TopicData {
    /// The topic name.
    pub topic_name: String,
    /// The results for each partition.
    pub partitions: Vec<PartitionData>,
}

impl ApiResponse for VoteResponse {
    type Request = crate::generated::VoteRequest;
    fn get_api_key() -> ApiKey {
        ApiKey::new(52)
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
            0 <= version.0 && version.0 <= 2,
            "version {} is not supported by {} (supported: 0-2)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        self.error_code.encode(buf, version, is_flexible)?;
        self.topics.encode(buf, version, is_flexible)?;
        if 1 <= version.0 {
            self.node_endpoints.encode(buf, version, is_flexible)?;
        } else if !self.node_endpoints.is_empty() {
            return Err(SerializationError::FieldNotAvailable {
                field: "NodeEndpoints",
                version,
                api_name: "VoteResponse",
            });
        }
        if is_flexible {
            let mut tag_count = 0u64;
            if !self.node_endpoints.is_empty() {
                tag_count += 1;
            }
            encode_unsigned_varint(tag_count, buf);
            if !self.node_endpoints.is_empty() {
                encode_unsigned_varint(0u64, buf);
                let mut tmp_buf = bytes::BytesMut::new();
                self.node_endpoints.encode(&mut tmp_buf, version, true)?;
                encode_unsigned_varint(tmp_buf.len() as u64, buf);
                buf.put_slice(&tmp_buf);
            }
        }
        Ok(())
    }
    fn deserialize(version: ApiVer, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let error_code = KafkaCodec::decode(buf, version, is_flexible)?;
        let topics = KafkaCodec::decode(buf, version, is_flexible)?;
        let mut node_endpoints = if 1 <= version.0 {
            if is_flexible {
                Default::default()
            } else {
                KafkaCodec::decode(buf, version, is_flexible)?
            }
        } else {
            Default::default()
        };
        if is_flexible {
            let (tag_count, _) = decode_unsigned_varint(buf)?;
            for _ in 0..tag_count {
                let (__tag_id, _) = decode_unsigned_varint(buf)?;
                let (__tag_len, _) = decode_unsigned_varint(buf)?;
                match __tag_id {
                    0 => {
                        node_endpoints = KafkaCodec::decode(buf, version, true)?;
                    }
                    _ => {
                        buf.advance(__tag_len as usize);
                    }
                }
            }
        }
        Ok(Self {
            error_code,
            topics,
            node_endpoints,
        })
    }
}
impl KafkaCodec for VoteResponse {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.error_code.encode(buf, version, is_flexible)?;
        self.topics.encode(buf, version, is_flexible)?;
        if 1 <= version.0 && !is_flexible {
            self.node_endpoints.encode(buf, version, is_flexible)?;
        }
        if is_flexible {
            let mut tag_count = 0u64;
            if !self.node_endpoints.is_empty() {
                tag_count += 1;
            }
            encode_unsigned_varint(tag_count, buf);
            if !self.node_endpoints.is_empty() {
                encode_unsigned_varint(0u64, buf);
                let mut tmp_buf = bytes::BytesMut::new();
                self.node_endpoints.encode(&mut tmp_buf, version, true)?;
                encode_unsigned_varint(tmp_buf.len() as u64, buf);
                buf.put_slice(&tmp_buf);
            }
        }
        Ok(())
    }

    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let error_code = KafkaCodec::decode(buf, version, is_flexible)?;
        let topics = KafkaCodec::decode(buf, version, is_flexible)?;
        let mut node_endpoints = if 1 <= version.0 {
            if is_flexible {
                Default::default()
            } else {
                KafkaCodec::decode(buf, version, is_flexible)?
            }
        } else {
            Default::default()
        };
        if is_flexible {
            let (tag_count, _) = decode_unsigned_varint(buf)?;
            for _ in 0..tag_count {
                let (__tag_id, _) = decode_unsigned_varint(buf)?;
                let (__tag_len, _) = decode_unsigned_varint(buf)?;
                match __tag_id {
                    0 => {
                        node_endpoints = KafkaCodec::decode(buf, version, true)?;
                    }
                    _ => {
                        buf.advance(__tag_len as usize);
                    }
                }
            }
        }
        Ok(Self {
            error_code,
            topics,
            node_endpoints,
        })
    }
}

impl KafkaCodec for NodeEndpoint {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        if 1 <= version.0 {
            self.host.encode(buf, version, is_flexible)?;
        }
        if 1 <= version.0 {
            self.port.encode(buf, version, is_flexible)?;
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
        let host = if 1 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let port = if 1 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self { host, port })
    }
}

impl KafkaCodec for PartitionData {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.partition_index.encode(buf, version, is_flexible)?;
        self.error_code.encode(buf, version, is_flexible)?;
        self.leader_id.encode(buf, version, is_flexible)?;
        self.leader_epoch.encode(buf, version, is_flexible)?;
        self.vote_granted.encode(buf, version, is_flexible)?;
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
        let partition_index = KafkaCodec::decode(buf, version, is_flexible)?;
        let error_code = KafkaCodec::decode(buf, version, is_flexible)?;
        let leader_id = KafkaCodec::decode(buf, version, is_flexible)?;
        let leader_epoch = KafkaCodec::decode(buf, version, is_flexible)?;
        let vote_granted = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            partition_index,
            error_code,
            leader_id,
            leader_epoch,
            vote_granted,
        })
    }
}

impl KafkaCodec for TopicData {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.topic_name.encode(buf, version, is_flexible)?;
        self.partitions.encode(buf, version, is_flexible)?;
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
        let partitions = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            topic_name,
            partitions,
        })
    }
}

#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{KafkaCodec, decode_unsigned_varint, encode_unsigned_varint};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVer, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use indexmap::IndexMap;

// -------------------------------------------------------
// ProduceResponse
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ProduceResponse {
    /// Each produce response.
    pub responses: Vec<TopicProduceResponse>,
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    /// Available in version 1+.
    pub throttle_time_ms: i32,
    /// Endpoints for all current-leaders enumerated in PartitionProduceResponses, with errors NOT_LEADER_OR_FOLLOWER.
    /// IndexMap key `NodeId` (int32): The ID of the associated node.
    /// Available in version 10+.
    pub node_endpoints: IndexMap<i32, NodeEndpoint>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct BatchIndexAndErrorMessage {
    /// The batch index of the record that caused the batch to be dropped.
    /// Available in version 8+.
    pub batch_index: i32,
    /// The error message of the record that caused the batch to be dropped.
    /// Available in version 8+.
    pub batch_index_error_message: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct LeaderIdAndEpoch {
    /// The ID of the current leader or -1 if the leader is unknown.
    /// Available in version 10+.
    pub leader_id: i32,
    /// The latest known leader epoch.
    /// Available in version 10+.
    pub leader_epoch: i32,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct NodeEndpoint {
    /// The node's hostname.
    /// Available in version 10+.
    pub host: String,
    /// The node's port.
    /// Available in version 10+.
    pub port: i32,
    /// The rack of the node, or null if it has not been assigned to a rack.
    /// Available in version 10+.
    pub rack: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct PartitionProduceResponse {
    /// The partition index.
    pub index: i32,
    /// The error code, or 0 if there was no error.
    pub error_code: i16,
    /// The base offset.
    pub base_offset: i64,
    /// The timestamp returned by broker after appending the messages. If CreateTime is used for the topic, the timestamp will be -1.  If LogAppendTime is used for the topic, the timestamp will be the broker local time when the messages are appended.
    /// Available in version 2+.
    pub log_append_time_ms: i64,
    /// The log start offset.
    /// Available in version 5+.
    pub log_start_offset: i64,
    /// The batch indices of records that caused the batch to be dropped.
    /// Available in version 8+.
    pub record_errors: Vec<BatchIndexAndErrorMessage>,
    /// The global error message summarizing the common root cause of the records that caused the batch to be dropped.
    /// Available in version 8+.
    pub error_message: Option<String>,
    /// The leader broker that the producer should use for future requests.
    /// Available in version 10+.
    pub current_leader: LeaderIdAndEpoch,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct TopicProduceResponse {
    /// The topic name.
    /// Available in version 0-12.
    pub name: String,
    /// The unique topic ID
    /// Available in version 13+.
    pub topic_id: [u8; 16],
    /// Each partition that we produced to within the topic.
    pub partition_responses: Vec<PartitionProduceResponse>,
}

impl ApiResponse for ProduceResponse {
    type Request = crate::generated::ProduceRequest;
    fn get_api_key() -> ApiKey {
        ApiKey::new(0)
    }
    fn get_min_supported_version() -> ApiVer {
        ApiVer::new(3)
    }
    fn get_max_supported_version() -> ApiVer {
        ApiVer::new(13)
    }
    fn get_min_flexible_version() -> ApiVer {
        ApiVer::new(9)
    }
    fn serialize(&self, version: ApiVer, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            3 <= version.0 && version.0 <= 13,
            "version {} is not supported by {} (supported: 3-13)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        self.responses.encode(buf, version, is_flexible)?;
        if 1 <= version.0 {
            self.throttle_time_ms.encode(buf, version, is_flexible)?;
        } else if self.throttle_time_ms != 0 {
            return Err(SerializationError::FieldNotAvailable {
                field: "ThrottleTimeMs",
                version,
                api_name: "ProduceResponse",
            });
        }
        if 10 <= version.0 {
            self.node_endpoints.encode(buf, version, is_flexible)?;
        } else if !self.node_endpoints.is_empty() {
            return Err(SerializationError::FieldNotAvailable {
                field: "NodeEndpoints",
                version,
                api_name: "ProduceResponse",
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
        let responses = KafkaCodec::decode(buf, version, is_flexible)?;
        let throttle_time_ms = if 1 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let mut node_endpoints = if 10 <= version.0 {
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
            responses,
            throttle_time_ms,
            node_endpoints,
        })
    }
}
impl KafkaCodec for ProduceResponse {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.responses.encode(buf, version, is_flexible)?;
        if 1 <= version.0 {
            self.throttle_time_ms.encode(buf, version, is_flexible)?;
        }
        if 10 <= version.0 && !is_flexible {
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
        let responses = KafkaCodec::decode(buf, version, is_flexible)?;
        let throttle_time_ms = if 1 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let mut node_endpoints = if 10 <= version.0 {
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
            responses,
            throttle_time_ms,
            node_endpoints,
        })
    }
}

impl KafkaCodec for BatchIndexAndErrorMessage {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        if 8 <= version.0 {
            self.batch_index.encode(buf, version, is_flexible)?;
        }
        if 8 <= version.0 {
            self.batch_index_error_message
                .encode(buf, version, is_flexible)?;
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
        let batch_index = if 8 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let batch_index_error_message = if 8 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            batch_index,
            batch_index_error_message,
        })
    }
}

impl KafkaCodec for LeaderIdAndEpoch {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        if 10 <= version.0 {
            self.leader_id.encode(buf, version, is_flexible)?;
        }
        if 10 <= version.0 {
            self.leader_epoch.encode(buf, version, is_flexible)?;
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
        let leader_id = if 10 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let leader_epoch = if 10 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            leader_id,
            leader_epoch,
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
        if 10 <= version.0 {
            self.host.encode(buf, version, is_flexible)?;
        }
        if 10 <= version.0 {
            self.port.encode(buf, version, is_flexible)?;
        }
        if 10 <= version.0 {
            self.rack.encode(buf, version, is_flexible)?;
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
        let host = if 10 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let port = if 10 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let rack = if 10 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self { host, port, rack })
    }
}

impl KafkaCodec for PartitionProduceResponse {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.index.encode(buf, version, is_flexible)?;
        self.error_code.encode(buf, version, is_flexible)?;
        self.base_offset.encode(buf, version, is_flexible)?;
        if 2 <= version.0 {
            self.log_append_time_ms.encode(buf, version, is_flexible)?;
        }
        if 5 <= version.0 {
            self.log_start_offset.encode(buf, version, is_flexible)?;
        }
        if 8 <= version.0 {
            self.record_errors.encode(buf, version, is_flexible)?;
        }
        if 8 <= version.0 {
            self.error_message.encode(buf, version, is_flexible)?;
        }
        if 10 <= version.0 && !is_flexible {
            self.current_leader.encode(buf, version, is_flexible)?;
        }
        if is_flexible {
            let mut tag_count = 0u64;
            if self.current_leader != Default::default() {
                tag_count += 1;
            }
            encode_unsigned_varint(tag_count, buf);
            if self.current_leader != Default::default() {
                encode_unsigned_varint(0u64, buf);
                let mut tmp_buf = bytes::BytesMut::new();
                self.current_leader.encode(&mut tmp_buf, version, true)?;
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
        let index = KafkaCodec::decode(buf, version, is_flexible)?;
        let error_code = KafkaCodec::decode(buf, version, is_flexible)?;
        let base_offset = KafkaCodec::decode(buf, version, is_flexible)?;
        let log_append_time_ms = if 2 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let log_start_offset = if 5 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let record_errors = if 8 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let error_message = if 8 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let mut current_leader = if 10 <= version.0 {
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
                        current_leader = KafkaCodec::decode(buf, version, true)?;
                    }
                    _ => {
                        buf.advance(__tag_len as usize);
                    }
                }
            }
        }
        Ok(Self {
            index,
            error_code,
            base_offset,
            log_append_time_ms,
            log_start_offset,
            record_errors,
            error_message,
            current_leader,
        })
    }
}

impl KafkaCodec for TopicProduceResponse {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        if 0 <= version.0 && version.0 <= 12 {
            self.name.encode(buf, version, is_flexible)?;
        }
        if 13 <= version.0 {
            self.topic_id.encode(buf, version, is_flexible)?;
        }
        self.partition_responses.encode(buf, version, is_flexible)?;
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
        let name = if 0 <= version.0 && version.0 <= 12 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let topic_id = if 13 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let partition_responses = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            name,
            topic_id,
            partition_responses,
        })
    }
}

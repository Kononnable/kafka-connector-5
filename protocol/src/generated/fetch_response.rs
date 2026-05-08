#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// FetchResponse
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct FetchResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    /// Available in version 1+.
    pub throttle_time_ms: i32,
    /// The top level response error code.
    /// Available in version 7+.
    pub error_code: i16,
    /// The fetch session ID, or 0 if this is not part of a fetch session.
    /// Available in version 7+.
    pub session_id: i32,
    /// The response topics.
    pub responses: Vec<FetchableTopicResponse>,
    /// Endpoints for all current-leaders enumerated in PartitionData, with errors NOT_LEADER_OR_FOLLOWER & FENCED_LEADER_EPOCH.
    /// Available in version 16+.
    pub node_endpoints: Vec<NodeEndpoint>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct AbortedTransaction {
    /// The producer id associated with the aborted transaction.
    /// Available in version 4+.
    pub producer_id: i64,
    /// The first offset in the aborted transaction.
    /// Available in version 4+.
    pub first_offset: i64,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct EpochEndOffset {
    /// The largest epoch.
    /// Available in version 12+.
    pub epoch: i32,
    /// The end offset of the epoch.
    /// Available in version 12+.
    pub end_offset: i64,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct FetchableTopicResponse {
    /// The topic name.
    /// Available in version 0-12.
    pub topic: String,
    /// The unique topic ID.
    /// Available in version 13+.
    pub topic_id: [u8; 16],
    /// The topic partitions.
    pub partitions: Vec<PartitionData>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct LeaderIdAndEpoch {
    /// The ID of the current leader or -1 if the leader is unknown.
    /// Available in version 12+.
    pub leader_id: i32,
    /// The latest known leader epoch.
    /// Available in version 12+.
    pub leader_epoch: i32,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct NodeEndpoint {
    /// The ID of the associated node.
    /// Available in version 16+.
    pub node_id: i32,
    /// The node's hostname.
    /// Available in version 16+.
    pub host: String,
    /// The node's port.
    /// Available in version 16+.
    pub port: i32,
    /// The rack of the node, or null if it has not been assigned to a rack.
    /// Available in version 16+.
    pub rack: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct PartitionData {
    /// The partition index.
    pub partition_index: i32,
    /// The error code, or 0 if there was no fetch error.
    pub error_code: i16,
    /// The current high water mark.
    pub high_watermark: i64,
    /// The last stable offset (or LSO) of the partition. This is the last offset such that the state of all transactional records prior to this offset have been decided (ABORTED or COMMITTED).
    /// Available in version 4+.
    pub last_stable_offset: i64,
    /// The current log start offset.
    /// Available in version 5+.
    pub log_start_offset: i64,
    /// In case divergence is detected based on the `LastFetchedEpoch` and `FetchOffset` in the request, this field indicates the largest epoch and its end offset such that subsequent records are known to diverge.
    /// Available in version 12+.
    pub diverging_epoch: EpochEndOffset,
    /// The current leader of the partition.
    /// Available in version 12+.
    pub current_leader: LeaderIdAndEpoch,
    /// In the case of fetching an offset less than the LogStartOffset, this is the end offset and epoch that should be used in the FetchSnapshot request.
    /// Available in version 12+.
    pub snapshot_id: SnapshotId,
    /// The aborted transactions.
    /// Available in version 4+.
    pub aborted_transactions: Option<Vec<AbortedTransaction>>,
    /// The preferred read replica for the consumer to use on its next fetch request.
    /// Available in version 11+.
    pub preferred_read_replica: i32,
    /// The record data.
    pub records: Option<Vec<u8>>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct SnapshotId {
    /// The end offset of the epoch.
    pub end_offset: i64,
    /// The largest epoch.
    pub epoch: i32,
}

impl ApiResponse for FetchResponse {
    type Request = crate::generated::FetchRequest;
    fn get_api_key() -> ApiKey {
        ApiKey::new(1)
    }
    fn get_min_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(4)
    }
    fn get_max_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(18)
    }
    fn get_min_flexible_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(12)
    }
    fn serialize(
        &self,
        version: crate::traits::ApiVersion,
        buf: &mut BytesMut,
    ) -> Result<(), SerializationError> {
        assert!(
            (4) <= version.0 && version.0 <= (18),
            "version {} is not supported by {} (supported: 4-18)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        if (1) <= version.0 {
            self.throttle_time_ms.encode(buf, version, is_flexible)?;
        } else if self.throttle_time_ms != 0 {
            return Err(SerializationError::Encode(
                "field 'ThrottleTimeMs' is not available in this version",
            ));
        }
        if (7) <= version.0 {
            self.error_code.encode(buf, version, is_flexible)?;
        } else if self.error_code != 0 {
            return Err(SerializationError::Encode(
                "field 'ErrorCode' is not available in this version",
            ));
        }
        if (7) <= version.0 {
            self.session_id.encode(buf, version, is_flexible)?;
        } else if self.session_id != 0 {
            return Err(SerializationError::Encode(
                "field 'SessionId' is not available in this version",
            ));
        }
        self.responses.encode(buf, version, is_flexible)?;
        if (16) <= version.0 {
            self.node_endpoints.encode(buf, version, is_flexible)?;
        } else if !self.node_endpoints.is_empty() {
            return Err(SerializationError::Encode(
                "field 'NodeEndpoints' is not available in this version",
            ));
        }
        if is_flexible {
            let mut __tag_count = 0u64;
            if !self.node_endpoints.is_empty() {
                __tag_count += 1;
            }
            crate::protocol::serialization::encode_unsigned_varint(__tag_count, buf);
            if !self.node_endpoints.is_empty() {
                crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
                let mut __tmp = bytes::BytesMut::new();
                self.node_endpoints.encode(&mut __tmp, version, true)?;
                crate::protocol::serialization::encode_unsigned_varint(__tmp.len() as u64, buf);
                buf.put_slice(&__tmp);
            }
        }
        Ok(())
    }
    fn deserialize(
        version: crate::traits::ApiVersion,
        buf: &mut Bytes,
    ) -> Result<Self, SerializationError> {
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let throttle_time_ms = if (1) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let error_code = if (7) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let session_id = if (7) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let responses = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let mut node_endpoints = if (16) <= version.0 {
            if is_flexible {
                Default::default()
            } else {
                KafkaDeserialize::decode(buf, version, is_flexible)?
            }
        } else {
            Default::default()
        };
        if is_flexible {
            let (__tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
            for _ in 0..__tag_count {
                let (__tag_id, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
                let (__tag_len, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
                match __tag_id {
                    0 => {
                        node_endpoints = KafkaDeserialize::decode(buf, version, true)?;
                    }
                    _ => {
                        buf.advance(__tag_len as usize);
                    }
                }
            }
        }
        Ok(Self {
            throttle_time_ms,
            error_code,
            session_id,
            responses,
            node_endpoints,
        })
    }
}
impl KafkaSerialize for FetchResponse {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), crate::traits::SerializationError> {
        if (1) <= version.0 {
            self.throttle_time_ms.encode(buf, version, is_flexible)?;
        }
        if (7) <= version.0 {
            self.error_code.encode(buf, version, is_flexible)?;
        }
        if (7) <= version.0 {
            self.session_id.encode(buf, version, is_flexible)?;
        }
        self.responses.encode(buf, version, is_flexible)?;
        if (16) <= version.0 && !is_flexible {
            self.node_endpoints.encode(buf, version, is_flexible)?;
        }
        if is_flexible {
            let mut __tag_count = 0u64;
            if !self.node_endpoints.is_empty() {
                __tag_count += 1;
            }
            crate::protocol::serialization::encode_unsigned_varint(__tag_count, buf);
            if !self.node_endpoints.is_empty() {
                crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
                let mut __tmp = bytes::BytesMut::new();
                self.node_endpoints.encode(&mut __tmp, version, true)?;
                crate::protocol::serialization::encode_unsigned_varint(__tmp.len() as u64, buf);
                buf.put_slice(&__tmp);
            }
        }
        Ok(())
    }
}

impl KafkaDeserialize for FetchResponse {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, crate::traits::SerializationError> {
        let throttle_time_ms = if (1) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let error_code = if (7) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let session_id = if (7) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let responses = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let mut node_endpoints = if (16) <= version.0 {
            if is_flexible {
                Default::default()
            } else {
                KafkaDeserialize::decode(buf, version, is_flexible)?
            }
        } else {
            Default::default()
        };
        if is_flexible {
            let (__tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
            for _ in 0..__tag_count {
                let (__tag_id, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
                let (__tag_len, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
                match __tag_id {
                    0 => {
                        node_endpoints = KafkaDeserialize::decode(buf, version, true)?;
                    }
                    _ => {
                        buf.advance(__tag_len as usize);
                    }
                }
            }
        }
        Ok(Self {
            throttle_time_ms,
            error_code,
            session_id,
            responses,
            node_endpoints,
        })
    }
}

impl KafkaSerialize for AbortedTransaction {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), crate::traits::SerializationError> {
        if (4) <= version.0 {
            self.producer_id.encode(buf, version, is_flexible)?;
        }
        if (4) <= version.0 {
            self.first_offset.encode(buf, version, is_flexible)?;
        }
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for AbortedTransaction {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, crate::traits::SerializationError> {
        let producer_id = if (4) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let first_offset = if (4) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            producer_id,
            first_offset,
        })
    }
}

impl KafkaSerialize for EpochEndOffset {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), crate::traits::SerializationError> {
        if (12) <= version.0 {
            self.epoch.encode(buf, version, is_flexible)?;
        }
        if (12) <= version.0 {
            self.end_offset.encode(buf, version, is_flexible)?;
        }
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for EpochEndOffset {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, crate::traits::SerializationError> {
        let epoch = if (12) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let end_offset = if (12) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self { epoch, end_offset })
    }
}

impl KafkaSerialize for FetchableTopicResponse {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), crate::traits::SerializationError> {
        if (0) <= version.0 && version.0 <= (12) {
            self.topic.encode(buf, version, is_flexible)?;
        }
        if (13) <= version.0 {
            self.topic_id.encode(buf, version, is_flexible)?;
        }
        self.partitions.encode(buf, version, is_flexible)?;
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for FetchableTopicResponse {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, crate::traits::SerializationError> {
        let topic = if (0) <= version.0 && version.0 <= (12) {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let topic_id = if (13) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let partitions = KafkaDeserialize::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            topic,
            topic_id,
            partitions,
        })
    }
}

impl KafkaSerialize for LeaderIdAndEpoch {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), crate::traits::SerializationError> {
        if (12) <= version.0 {
            self.leader_id.encode(buf, version, is_flexible)?;
        }
        if (12) <= version.0 {
            self.leader_epoch.encode(buf, version, is_flexible)?;
        }
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for LeaderIdAndEpoch {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, crate::traits::SerializationError> {
        let leader_id = if (12) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let leader_epoch = if (12) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            leader_id,
            leader_epoch,
        })
    }
}

impl KafkaSerialize for NodeEndpoint {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), crate::traits::SerializationError> {
        if (16) <= version.0 {
            self.node_id.encode(buf, version, is_flexible)?;
        }
        if (16) <= version.0 {
            self.host.encode(buf, version, is_flexible)?;
        }
        if (16) <= version.0 {
            self.port.encode(buf, version, is_flexible)?;
        }
        if (16) <= version.0 {
            self.rack.encode(buf, version, is_flexible)?;
        }
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for NodeEndpoint {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, crate::traits::SerializationError> {
        let node_id = if (16) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let host = if (16) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let port = if (16) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let rack = if (16) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            node_id,
            host,
            port,
            rack,
        })
    }
}

impl KafkaSerialize for PartitionData {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), crate::traits::SerializationError> {
        self.partition_index.encode(buf, version, is_flexible)?;
        self.error_code.encode(buf, version, is_flexible)?;
        self.high_watermark.encode(buf, version, is_flexible)?;
        if (4) <= version.0 {
            self.last_stable_offset.encode(buf, version, is_flexible)?;
        }
        if (5) <= version.0 {
            self.log_start_offset.encode(buf, version, is_flexible)?;
        }
        if (12) <= version.0 && !is_flexible {
            self.diverging_epoch.encode(buf, version, is_flexible)?;
        }
        if (12) <= version.0 && !is_flexible {
            self.current_leader.encode(buf, version, is_flexible)?;
        }
        if (12) <= version.0 && !is_flexible {
            self.snapshot_id.encode(buf, version, is_flexible)?;
        }
        if (4) <= version.0 {
            self.aborted_transactions
                .encode(buf, version, is_flexible)?;
        }
        if (11) <= version.0 {
            self.preferred_read_replica
                .encode(buf, version, is_flexible)?;
        }
        self.records.encode(buf, version, is_flexible)?;
        if is_flexible {
            let mut __tag_count = 0u64;
            if self.diverging_epoch != Default::default() {
                __tag_count += 1;
            }
            if self.current_leader != Default::default() {
                __tag_count += 1;
            }
            if self.snapshot_id != Default::default() {
                __tag_count += 1;
            }
            crate::protocol::serialization::encode_unsigned_varint(__tag_count, buf);
            if self.diverging_epoch != Default::default() {
                crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
                let mut __tmp = bytes::BytesMut::new();
                self.diverging_epoch.encode(&mut __tmp, version, true)?;
                crate::protocol::serialization::encode_unsigned_varint(__tmp.len() as u64, buf);
                buf.put_slice(&__tmp);
            }
            if self.current_leader != Default::default() {
                crate::protocol::serialization::encode_unsigned_varint(1u64, buf);
                let mut __tmp = bytes::BytesMut::new();
                self.current_leader.encode(&mut __tmp, version, true)?;
                crate::protocol::serialization::encode_unsigned_varint(__tmp.len() as u64, buf);
                buf.put_slice(&__tmp);
            }
            if self.snapshot_id != Default::default() {
                crate::protocol::serialization::encode_unsigned_varint(2u64, buf);
                let mut __tmp = bytes::BytesMut::new();
                self.snapshot_id.encode(&mut __tmp, version, true)?;
                crate::protocol::serialization::encode_unsigned_varint(__tmp.len() as u64, buf);
                buf.put_slice(&__tmp);
            }
        }
        Ok(())
    }
}

impl KafkaDeserialize for PartitionData {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, crate::traits::SerializationError> {
        let partition_index = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let error_code = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let high_watermark = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let last_stable_offset = if (4) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let log_start_offset = if (5) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let mut diverging_epoch = if (12) <= version.0 {
            if is_flexible {
                Default::default()
            } else {
                KafkaDeserialize::decode(buf, version, is_flexible)?
            }
        } else {
            Default::default()
        };
        let mut current_leader = if (12) <= version.0 {
            if is_flexible {
                Default::default()
            } else {
                KafkaDeserialize::decode(buf, version, is_flexible)?
            }
        } else {
            Default::default()
        };
        let mut snapshot_id = if (12) <= version.0 {
            if is_flexible {
                Default::default()
            } else {
                KafkaDeserialize::decode(buf, version, is_flexible)?
            }
        } else {
            Default::default()
        };
        let aborted_transactions = if (4) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let preferred_read_replica = if (11) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let records = KafkaDeserialize::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (__tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
            for _ in 0..__tag_count {
                let (__tag_id, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
                let (__tag_len, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
                match __tag_id {
                    0 => {
                        diverging_epoch = KafkaDeserialize::decode(buf, version, true)?;
                    }
                    1 => {
                        current_leader = KafkaDeserialize::decode(buf, version, true)?;
                    }
                    2 => {
                        snapshot_id = KafkaDeserialize::decode(buf, version, true)?;
                    }
                    _ => {
                        buf.advance(__tag_len as usize);
                    }
                }
            }
        }
        Ok(Self {
            partition_index,
            error_code,
            high_watermark,
            last_stable_offset,
            log_start_offset,
            diverging_epoch,
            current_leader,
            snapshot_id,
            aborted_transactions,
            preferred_read_replica,
            records,
        })
    }
}

impl KafkaSerialize for SnapshotId {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), crate::traits::SerializationError> {
        self.end_offset.encode(buf, version, is_flexible)?;
        self.epoch.encode(buf, version, is_flexible)?;
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for SnapshotId {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, crate::traits::SerializationError> {
        let end_offset = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let epoch = KafkaDeserialize::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self { end_offset, epoch })
    }
}

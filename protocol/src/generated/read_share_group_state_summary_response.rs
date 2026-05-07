#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{EncodeError, DecodeError, KafkaSerialize, KafkaDeserialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// ReadShareGroupStateSummaryResponse
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ReadShareGroupStateSummaryResponse {
    /// The read results.
    pub results: Vec<ReadStateSummaryResult>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct PartitionResult {
    /// The partition index.
    pub partition: i32,
    /// The error code, or 0 if there was no error.
    pub error_code: i16,
    /// The error message, or null if there was no error.
    pub error_message: Option<String>,
    /// The state epoch of the share-partition.
    pub state_epoch: i32,
    /// The leader epoch of the share-partition.
    pub leader_epoch: i32,
    /// The share-partition start offset.
    pub start_offset: i64,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ReadStateSummaryResult {
    /// The topic identifier.
    pub topic_id: [u8; 16],
    /// The results for the partitions.
    pub partitions: Vec<PartitionResult>,
}

impl ApiResponse for ReadShareGroupStateSummaryResponse {
    type Request = crate::generated::ReadShareGroupStateSummaryRequest;
    fn get_api_key() -> ApiKey { ApiKey::new(87) }
    fn get_min_supported_version() -> crate::traits::ApiVersion { crate::traits::ApiVersion::new(0) }
    fn get_max_supported_version() -> crate::traits::ApiVersion { crate::traits::ApiVersion::new(0) }
    fn serialize(&self, version: crate::traits::ApiVersion, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!((0) <= version.0 && version.0 <= (0), "version {} is not supported by {} (supported: 0-0)", version.0, stringify!(Self));
        let is_flexible = true;
        self.results.encode_flexible(buf, is_flexible).map_err(|_| SerializationError::Encode("failed to encode Results"))?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(version: crate::traits::ApiVersion, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let is_flexible = true;
        let results = <Vec<ReadStateSummaryResult> as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| SerializationError::Decode("failed to decode Results"))?;
        Ok(Self { results })
    }
}
impl KafkaSerialize for ReadShareGroupStateSummaryResponse {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.results.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Results".into() })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(&self, buf: &mut B, is_flexible: bool) -> Result<(), EncodeError> {
        self.results.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Results".into() })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for ReadShareGroupStateSummaryResponse {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let results = <Vec<ReadStateSummaryResult> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode Results".into() })?;
        Ok(Self { results })
    }
    fn decode_flexible<B: Buf>(buf: &mut B, is_flexible: bool) -> Result<Self, DecodeError> {
        let results = <Vec<ReadStateSummaryResult> as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode Results".into() })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self { results })
    }
}

impl KafkaSerialize for PartitionResult {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.partition.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Partition".into() })?;
        self.error_code.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode ErrorCode".into() })?;
        self.error_message.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode ErrorMessage".into() })?;
        self.state_epoch.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode StateEpoch".into() })?;
        self.leader_epoch.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode LeaderEpoch".into() })?;
        self.start_offset.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode StartOffset".into() })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(&self, buf: &mut B, is_flexible: bool) -> Result<(), EncodeError> {
        self.partition.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Partition".into() })?;
        self.error_code.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode ErrorCode".into() })?;
        if is_flexible {
            if let Some(ref __val) = self.error_message {
                crate::protocol::serialization::encode_unsigned_varint(1u64, buf);
                __val.encode_flexible(buf, true).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode ErrorMessage".into() })?;
            } else {
                crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
            }
        } else {
            if let Some(ref __val) = self.error_message {
                __val.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode ErrorMessage".into() })?;
            }
        }
        self.state_epoch.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode StateEpoch".into() })?;
        self.leader_epoch.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode LeaderEpoch".into() })?;
        self.start_offset.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode StartOffset".into() })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for PartitionResult {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let partition = <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode Partition".into() })?;
        let error_code = <i16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode ErrorCode".into() })?;
        let error_message = <Option<String> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode ErrorMessage".into() })?;
        let state_epoch = <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode StateEpoch".into() })?;
        let leader_epoch = <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode LeaderEpoch".into() })?;
        let start_offset = <i64 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode StartOffset".into() })?;
        Ok(Self { partition, error_code, error_message, state_epoch, leader_epoch, start_offset })
    }
    fn decode_flexible<B: Buf>(buf: &mut B, is_flexible: bool) -> Result<Self, DecodeError> {
        let partition = <i32 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode Partition".into() })?;
        let error_code = <i16 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode ErrorCode".into() })?;
        let error_message = if is_flexible {
            let (__present, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
            if __present == 0 {
                None
            } else {
                Some(<String as KafkaDeserialize>::decode_flexible(buf, true).map_err(|_| DecodeError::Protocol { message: "failed to decode ErrorMessage".into() })?)
            }
        } else {
            <Option<String> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode ErrorMessage".into() })?
        };
        let state_epoch = <i32 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode StateEpoch".into() })?;
        let leader_epoch = <i32 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode LeaderEpoch".into() })?;
        let start_offset = <i64 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode StartOffset".into() })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self { partition, error_code, error_message, state_epoch, leader_epoch, start_offset })
    }
}

impl KafkaSerialize for ReadStateSummaryResult {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.topic_id.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode TopicId".into() })?;
        self.partitions.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Partitions".into() })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(&self, buf: &mut B, is_flexible: bool) -> Result<(), EncodeError> {
        self.topic_id.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode TopicId".into() })?;
        self.partitions.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Partitions".into() })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for ReadStateSummaryResult {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let topic_id = <[u8; 16] as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode TopicId".into() })?;
        let partitions = <Vec<PartitionResult> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode Partitions".into() })?;
        Ok(Self { topic_id, partitions })
    }
    fn decode_flexible<B: Buf>(buf: &mut B, is_flexible: bool) -> Result<Self, DecodeError> {
        let topic_id = <[u8; 16] as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode TopicId".into() })?;
        let partitions = <Vec<PartitionResult> as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode Partitions".into() })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self { topic_id, partitions })
    }
}


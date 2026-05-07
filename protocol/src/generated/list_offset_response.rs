#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// ListOffsetResponse
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ListOffsetResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    /// Available in version 2+.
    pub throttle_time_ms: i32,
    /// Each topic in the response.
    pub topics: Vec<ListOffsetTopicResponse>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ListOffsetPartitionResponse {
    /// The partition index.
    pub partition_index: i32,
    /// The partition error code, or 0 if there was no error.
    pub error_code: i16,
    /// The result offsets.
    /// Available in version 0.
    pub old_style_offsets: Vec<i64>,
    /// The timestamp associated with the returned offset.
    /// Available in version 1+.
    pub timestamp: i64,
    /// The returned offset.
    /// Available in version 1+.
    pub offset: i64,
    /// LeaderEpoch. Type: int32.
    /// Available in version 4+.
    pub leader_epoch: i32,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ListOffsetTopicResponse {
    /// The topic name
    pub name: String,
    /// Each partition in the response.
    pub partitions: Vec<ListOffsetPartitionResponse>,
}

impl ApiResponse for ListOffsetResponse {
    type Request = crate::generated::ListOffsetRequest;
    fn get_api_key() -> ApiKey {
        ApiKey::new(2)
    }
    fn get_min_supported_version() -> ApiVersion {
        ApiVersion::new(0)
    }
    fn get_max_supported_version() -> ApiVersion {
        ApiVersion::new(5)
    }
    fn serialize(&self, version: ApiVersion, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            (0) <= version.0 && version.0 <= (5),
            "version {} is not supported by {} (supported: 0-5)",
            version.0,
            stringify!(Self)
        );
        if (2) <= version.0 {
            self.throttle_time_ms
                .encode(buf)
                .map_err(|_| SerializationError::Encode("failed to encode ThrottleTimeMs"))?;
        }
        self.topics
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode Topics"))?;
        Ok(())
    }
    fn deserialize(version: ApiVersion, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let throttle_time_ms = if (2) <= version.0 {
            <i32 as KafkaDeserialize>::decode(buf)
                .map_err(|_| SerializationError::Decode("failed to decode ThrottleTimeMs"))?
        } else {
            Default::default()
        };
        let topics = <Vec<ListOffsetTopicResponse> as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode Topics"))?;
        Ok(Self {
            throttle_time_ms,
            topics,
        })
    }
}
impl KafkaSerialize for ListOffsetResponse {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.throttle_time_ms
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ThrottleTimeMs".into(),
            })?;
        self.topics
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Topics".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for ListOffsetResponse {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let throttle_time_ms =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ThrottleTimeMs".into(),
            })?;
        let topics =
            <Vec<ListOffsetTopicResponse> as KafkaDeserialize>::decode(buf).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode Topics".into(),
                }
            })?;
        Ok(Self {
            throttle_time_ms,
            topics,
        })
    }
}

impl KafkaSerialize for ListOffsetPartitionResponse {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.partition_index
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode PartitionIndex".into(),
            })?;
        self.error_code
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ErrorCode".into(),
            })?;
        self.old_style_offsets
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode OldStyleOffsets".into(),
            })?;
        self.timestamp
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Timestamp".into(),
            })?;
        self.offset
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Offset".into(),
            })?;
        self.leader_epoch
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode LeaderEpoch".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for ListOffsetPartitionResponse {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let partition_index =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode PartitionIndex".into(),
            })?;
        let error_code =
            <i16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ErrorCode".into(),
            })?;
        let old_style_offsets =
            <Vec<i64> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode OldStyleOffsets".into(),
            })?;
        let timestamp =
            <i64 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode Timestamp".into(),
            })?;
        let offset = <i64 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
            message: "failed to decode Offset".into(),
        })?;
        let leader_epoch =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode LeaderEpoch".into(),
            })?;
        Ok(Self {
            partition_index,
            error_code,
            old_style_offsets,
            timestamp,
            offset,
            leader_epoch,
        })
    }
}

impl KafkaSerialize for ListOffsetTopicResponse {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.name
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Name".into(),
            })?;
        self.partitions
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Partitions".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for ListOffsetTopicResponse {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let name =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode Name".into(),
            })?;
        let partitions = <Vec<ListOffsetPartitionResponse> as KafkaDeserialize>::decode(buf)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode Partitions".into(),
            })?;
        Ok(Self { name, partitions })
    }
}

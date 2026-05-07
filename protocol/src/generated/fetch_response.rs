#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion, SerializationError};
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
    pub topics: Vec<FetchableTopicResponse>,
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
pub struct FetchablePartitionResponse {
    /// The partiiton index.
    pub partition_index: i32,
    /// The error code, or 0 if there was no fetch error.
    pub error_code: i16,
    /// The current high water mark.
    pub high_watermark: i64,
    /// The last stable offset (or LSO) of the partition. This is the last offset such that the state of all transactional records prior to this offset have been decided (ABORTED or COMMITTED)
    /// Available in version 4+.
    pub last_stable_offset: i64,
    /// The current log start offset.
    /// Available in version 5+.
    pub log_start_offset: i64,
    /// The aborted transactions.
    /// Available in version 4+.
    pub aborted: Option<Vec<AbortedTransaction>>,
    /// The record data.
    pub records: Option<Vec<u8>>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct FetchableTopicResponse {
    /// The topic name.
    pub name: String,
    /// The topic partitions.
    pub partitions: Vec<FetchablePartitionResponse>,
}

impl ApiResponse for FetchResponse {
    type Request = crate::generated::FetchRequest;
    fn get_api_key() -> ApiKey {
        ApiKey::new(1)
    }
    fn get_min_supported_version() -> ApiVersion {
        ApiVersion::new(0)
    }
    fn get_max_supported_version() -> ApiVersion {
        ApiVersion::new(10)
    }
    fn serialize(&self, version: ApiVersion, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            (0) <= version.0 && version.0 <= (10),
            "version {} is not supported by {} (supported: 0-10)",
            version.0,
            stringify!(Self)
        );
        if (1) <= version.0 {
            self.throttle_time_ms
                .encode(buf)
                .map_err(|_| SerializationError::Encode("failed to encode ThrottleTimeMs"))?;
        }
        if (7) <= version.0 {
            self.error_code
                .encode(buf)
                .map_err(|_| SerializationError::Encode("failed to encode ErrorCode"))?;
        }
        if (7) <= version.0 {
            self.session_id
                .encode(buf)
                .map_err(|_| SerializationError::Encode("failed to encode SessionId"))?;
        }
        self.topics
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode Topics"))?;
        Ok(())
    }
    fn deserialize(version: ApiVersion, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let throttle_time_ms = if (1) <= version.0 {
            <i32 as KafkaDeserialize>::decode(buf)
                .map_err(|_| SerializationError::Decode("failed to decode ThrottleTimeMs"))?
        } else {
            Default::default()
        };
        let error_code = if (7) <= version.0 {
            <i16 as KafkaDeserialize>::decode(buf)
                .map_err(|_| SerializationError::Decode("failed to decode ErrorCode"))?
        } else {
            Default::default()
        };
        let session_id = if (7) <= version.0 {
            <i32 as KafkaDeserialize>::decode(buf)
                .map_err(|_| SerializationError::Decode("failed to decode SessionId"))?
        } else {
            Default::default()
        };
        let topics = <Vec<FetchableTopicResponse> as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode Topics"))?;
        Ok(Self {
            throttle_time_ms,
            error_code,
            session_id,
            topics,
        })
    }
}
impl KafkaSerialize for FetchResponse {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.throttle_time_ms
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ThrottleTimeMs".into(),
            })?;
        self.error_code
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ErrorCode".into(),
            })?;
        self.session_id
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode SessionId".into(),
            })?;
        self.topics
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Topics".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for FetchResponse {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let throttle_time_ms =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ThrottleTimeMs".into(),
            })?;
        let error_code =
            <i16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ErrorCode".into(),
            })?;
        let session_id =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode SessionId".into(),
            })?;
        let topics =
            <Vec<FetchableTopicResponse> as KafkaDeserialize>::decode(buf).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode Topics".into(),
                }
            })?;
        Ok(Self {
            throttle_time_ms,
            error_code,
            session_id,
            topics,
        })
    }
}

impl KafkaSerialize for AbortedTransaction {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.producer_id
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ProducerId".into(),
            })?;
        self.first_offset
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode FirstOffset".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for AbortedTransaction {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let producer_id =
            <i64 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ProducerId".into(),
            })?;
        let first_offset =
            <i64 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode FirstOffset".into(),
            })?;
        Ok(Self {
            producer_id,
            first_offset,
        })
    }
}

impl KafkaSerialize for FetchablePartitionResponse {
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
        self.high_watermark
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode HighWatermark".into(),
            })?;
        self.last_stable_offset
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode LastStableOffset".into(),
            })?;
        self.log_start_offset
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode LogStartOffset".into(),
            })?;
        self.aborted
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Aborted".into(),
            })?;
        self.records
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Records".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for FetchablePartitionResponse {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let partition_index =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode PartitionIndex".into(),
            })?;
        let error_code =
            <i16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ErrorCode".into(),
            })?;
        let high_watermark =
            <i64 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode HighWatermark".into(),
            })?;
        let last_stable_offset =
            <i64 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode LastStableOffset".into(),
            })?;
        let log_start_offset =
            <i64 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode LogStartOffset".into(),
            })?;
        let aborted =
            <Option<Vec<AbortedTransaction>> as KafkaDeserialize>::decode(buf).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode Aborted".into(),
                }
            })?;
        let records = <Option<Vec<u8>> as KafkaDeserialize>::decode(buf).map_err(|_| {
            DecodeError::Protocol {
                message: "failed to decode Records".into(),
            }
        })?;
        Ok(Self {
            partition_index,
            error_code,
            high_watermark,
            last_stable_offset,
            log_start_offset,
            aborted,
            records,
        })
    }
}

impl KafkaSerialize for FetchableTopicResponse {
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

impl KafkaDeserialize for FetchableTopicResponse {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let name =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode Name".into(),
            })?;
        let partitions = <Vec<FetchablePartitionResponse> as KafkaDeserialize>::decode(buf)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode Partitions".into(),
            })?;
        Ok(Self { name, partitions })
    }
}

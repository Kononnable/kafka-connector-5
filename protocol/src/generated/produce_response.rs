#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// ProduceResponse
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ProduceResponse {
    /// Each produce response
    pub responses: Vec<TopicProduceResponse>,
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    /// Available in version 1+.
    pub throttle_time_ms: i32,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct BatchIndexAndErrorMessage {
    /// The batch index of the record that cause the batch to be dropped
    /// Available in version 8+.
    pub batch_index: i32,
    /// The error message of the record that caused the batch to be dropped
    /// Available in version 8+.
    pub batch_index_error_message: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct PartitionProduceResponse {
    /// The partition index.
    pub partition_index: i32,
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
    /// The batch indices of records that caused the batch to be dropped
    /// Available in version 8+.
    pub record_errors: Vec<BatchIndexAndErrorMessage>,
    /// The global error message summarizing the common root cause of the records that caused the batch to be dropped
    /// Available in version 8+.
    pub error_message: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct TopicProduceResponse {
    /// The topic name
    pub name: String,
    /// Each partition that we produced to within the topic.
    pub partitions: Vec<PartitionProduceResponse>,
}

impl ApiResponse for ProduceResponse {
    type Request = crate::generated::ProduceRequest;
    fn get_api_key() -> ApiKey {
        ApiKey::new(0)
    }
    fn get_min_supported_version() -> ApiVersion {
        ApiVersion::new(0)
    }
    fn get_max_supported_version() -> ApiVersion {
        ApiVersion::new(8)
    }
    fn serialize(&self, version: ApiVersion, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            (0) <= version.0 && version.0 <= (8),
            "version {} is not supported by {} (supported: 0-8)",
            version.0,
            stringify!(Self)
        );
        self.responses
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode Responses"))?;
        if (1) <= version.0 {
            self.throttle_time_ms
                .encode(buf)
                .map_err(|_| SerializationError::Encode("failed to encode ThrottleTimeMs"))?;
        }
        Ok(())
    }
    fn deserialize(version: ApiVersion, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let responses = <Vec<TopicProduceResponse> as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode Responses"))?;
        let throttle_time_ms = if (1) <= version.0 {
            <i32 as KafkaDeserialize>::decode(buf)
                .map_err(|_| SerializationError::Decode("failed to decode ThrottleTimeMs"))?
        } else {
            Default::default()
        };
        Ok(Self {
            responses,
            throttle_time_ms,
        })
    }
}
impl KafkaSerialize for ProduceResponse {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.responses
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Responses".into(),
            })?;
        self.throttle_time_ms
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ThrottleTimeMs".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for ProduceResponse {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let responses =
            <Vec<TopicProduceResponse> as KafkaDeserialize>::decode(buf).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode Responses".into(),
                }
            })?;
        let throttle_time_ms =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ThrottleTimeMs".into(),
            })?;
        Ok(Self {
            responses,
            throttle_time_ms,
        })
    }
}

impl KafkaSerialize for BatchIndexAndErrorMessage {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.batch_index
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode BatchIndex".into(),
            })?;
        self.batch_index_error_message
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode BatchIndexErrorMessage".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for BatchIndexAndErrorMessage {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let batch_index =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode BatchIndex".into(),
            })?;
        let batch_index_error_message =
            <Option<String> as KafkaDeserialize>::decode(buf).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode BatchIndexErrorMessage".into(),
                }
            })?;
        Ok(Self {
            batch_index,
            batch_index_error_message,
        })
    }
}

impl KafkaSerialize for PartitionProduceResponse {
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
        self.base_offset
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode BaseOffset".into(),
            })?;
        self.log_append_time_ms
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode LogAppendTimeMs".into(),
            })?;
        self.log_start_offset
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode LogStartOffset".into(),
            })?;
        self.record_errors
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode RecordErrors".into(),
            })?;
        self.error_message
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ErrorMessage".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for PartitionProduceResponse {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let partition_index =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode PartitionIndex".into(),
            })?;
        let error_code =
            <i16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ErrorCode".into(),
            })?;
        let base_offset =
            <i64 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode BaseOffset".into(),
            })?;
        let log_append_time_ms =
            <i64 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode LogAppendTimeMs".into(),
            })?;
        let log_start_offset =
            <i64 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode LogStartOffset".into(),
            })?;
        let record_errors = <Vec<BatchIndexAndErrorMessage> as KafkaDeserialize>::decode(buf)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode RecordErrors".into(),
            })?;
        let error_message = <Option<String> as KafkaDeserialize>::decode(buf).map_err(|_| {
            DecodeError::Protocol {
                message: "failed to decode ErrorMessage".into(),
            }
        })?;
        Ok(Self {
            partition_index,
            error_code,
            base_offset,
            log_append_time_ms,
            log_start_offset,
            record_errors,
            error_message,
        })
    }
}

impl KafkaSerialize for TopicProduceResponse {
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

impl KafkaDeserialize for TopicProduceResponse {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let name =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode Name".into(),
            })?;
        let partitions =
            <Vec<PartitionProduceResponse> as KafkaDeserialize>::decode(buf).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode Partitions".into(),
                }
            })?;
        Ok(Self { name, partitions })
    }
}

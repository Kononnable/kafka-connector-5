#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// AddPartitionsToTxnResponse
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct AddPartitionsToTxnResponse {
    /// Duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// The results for each topic.
    pub results: Vec<AddPartitionsToTxnTopicResult>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct AddPartitionsToTxnPartitionResult {
    /// The partition indexes.
    pub partition_index: i32,
    /// The response error code.
    pub error_code: i16,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct AddPartitionsToTxnTopicResult {
    /// The topic name.
    pub name: String,
    /// The results for each partition
    pub results: Vec<AddPartitionsToTxnPartitionResult>,
}

impl ApiResponse for AddPartitionsToTxnResponse {
    type Request = crate::generated::AddPartitionsToTxnRequest;
    fn get_api_key() -> ApiKey {
        ApiKey::new(24)
    }
    fn get_min_supported_version() -> ApiVersion {
        ApiVersion::new(0)
    }
    fn get_max_supported_version() -> ApiVersion {
        ApiVersion::new(1)
    }
    fn serialize(&self, version: ApiVersion, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            (0) <= version.0 && version.0 <= (1),
            "version {} is not supported by {} (supported: 0-1)",
            version.0,
            stringify!(Self)
        );
        self.throttle_time_ms
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode ThrottleTimeMs"))?;
        self.results
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode Results"))?;
        Ok(())
    }
    fn deserialize(version: ApiVersion, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let throttle_time_ms = <i32 as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode ThrottleTimeMs"))?;
        let results = <Vec<AddPartitionsToTxnTopicResult> as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode Results"))?;
        Ok(Self {
            throttle_time_ms,
            results,
        })
    }
}
impl KafkaSerialize for AddPartitionsToTxnResponse {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.throttle_time_ms
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ThrottleTimeMs".into(),
            })?;
        self.results
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Results".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for AddPartitionsToTxnResponse {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let throttle_time_ms =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ThrottleTimeMs".into(),
            })?;
        let results = <Vec<AddPartitionsToTxnTopicResult> as KafkaDeserialize>::decode(buf)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode Results".into(),
            })?;
        Ok(Self {
            throttle_time_ms,
            results,
        })
    }
}

impl KafkaSerialize for AddPartitionsToTxnPartitionResult {
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
        Ok(())
    }
}

impl KafkaDeserialize for AddPartitionsToTxnPartitionResult {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let partition_index =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode PartitionIndex".into(),
            })?;
        let error_code =
            <i16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ErrorCode".into(),
            })?;
        Ok(Self {
            partition_index,
            error_code,
        })
    }
}

impl KafkaSerialize for AddPartitionsToTxnTopicResult {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.name
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Name".into(),
            })?;
        self.results
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Results".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for AddPartitionsToTxnTopicResult {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let name =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode Name".into(),
            })?;
        let results = <Vec<AddPartitionsToTxnPartitionResult> as KafkaDeserialize>::decode(buf)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode Results".into(),
            })?;
        Ok(Self { name, results })
    }
}

#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// InitProducerIdResponse
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct InitProducerIdResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// The error code, or 0 if there was no error.
    pub error_code: i16,
    /// The current producer id.
    pub producer_id: i64,
    /// The current epoch associated with the producer id.
    pub producer_epoch: i16,
}

impl ApiResponse for InitProducerIdResponse {
    type Request = crate::generated::InitProducerIdRequest;
    fn get_api_key() -> ApiKey {
        ApiKey::new(22)
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
        self.error_code
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode ErrorCode"))?;
        self.producer_id
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode ProducerId"))?;
        self.producer_epoch
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode ProducerEpoch"))?;
        Ok(())
    }
    fn deserialize(version: ApiVersion, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let throttle_time_ms = <i32 as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode ThrottleTimeMs"))?;
        let error_code = <i16 as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode ErrorCode"))?;
        let producer_id = <i64 as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode ProducerId"))?;
        let producer_epoch = <i16 as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode ProducerEpoch"))?;
        Ok(Self {
            throttle_time_ms,
            error_code,
            producer_id,
            producer_epoch,
        })
    }
}
impl KafkaSerialize for InitProducerIdResponse {
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
        self.producer_id
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ProducerId".into(),
            })?;
        self.producer_epoch
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ProducerEpoch".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for InitProducerIdResponse {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let throttle_time_ms =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ThrottleTimeMs".into(),
            })?;
        let error_code =
            <i16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ErrorCode".into(),
            })?;
        let producer_id =
            <i64 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ProducerId".into(),
            })?;
        let producer_epoch =
            <i16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ProducerEpoch".into(),
            })?;
        Ok(Self {
            throttle_time_ms,
            error_code,
            producer_id,
            producer_epoch,
        })
    }
}

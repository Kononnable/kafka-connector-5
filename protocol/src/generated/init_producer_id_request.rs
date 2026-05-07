#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// InitProducerIdRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct InitProducerIdRequest {
    /// The transactional id, or null if the producer is not transactional.
    pub transactional_id: Option<String>,
    /// The time in ms to wait for before aborting idle transactions sent by this producer.
    pub transaction_timeout_ms: i32,
}

impl ApiRequest for InitProducerIdRequest {
    type Response = crate::generated::InitProducerIdResponse;
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
        self.transactional_id
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode TransactionalId"))?;
        self.transaction_timeout_ms
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode TransactionTimeoutMs"))?;
        Ok(())
    }
    fn deserialize(version: ApiVersion, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let transactional_id = <Option<String> as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode TransactionalId"))?;
        let transaction_timeout_ms = <i32 as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode TransactionTimeoutMs"))?;
        Ok(Self {
            transactional_id,
            transaction_timeout_ms,
        })
    }
}
impl KafkaSerialize for InitProducerIdRequest {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.transactional_id
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode TransactionalId".into(),
            })?;
        self.transaction_timeout_ms
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode TransactionTimeoutMs".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for InitProducerIdRequest {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let transactional_id = <Option<String> as KafkaDeserialize>::decode(buf).map_err(|_| {
            DecodeError::Protocol {
                message: "failed to decode TransactionalId".into(),
            }
        })?;
        let transaction_timeout_ms =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode TransactionTimeoutMs".into(),
            })?;
        Ok(Self {
            transactional_id,
            transaction_timeout_ms,
        })
    }
}

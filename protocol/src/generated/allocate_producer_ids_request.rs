#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{EncodeError, DecodeError, KafkaSerialize, KafkaDeserialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// AllocateProducerIdsRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct AllocateProducerIdsRequest {
    /// The ID of the requesting broker.
    pub broker_id: i32,
    /// The epoch of the requesting broker.
    pub broker_epoch: i64,
}

impl ApiRequest for AllocateProducerIdsRequest {
    type Response = crate::generated::AllocateProducerIdsResponse;
    fn get_api_key() -> ApiKey { ApiKey::new(67) }
    fn get_min_supported_version() -> crate::traits::ApiVersion { crate::traits::ApiVersion::new(0) }
    fn get_max_supported_version() -> crate::traits::ApiVersion { crate::traits::ApiVersion::new(0) }
    fn serialize(&self, version: crate::traits::ApiVersion, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!((0) <= version.0 && version.0 <= (0), "version {} is not supported by {} (supported: 0-0)", version.0, stringify!(Self));
        let is_flexible = true;
        self.broker_id.encode_flexible(buf, is_flexible).map_err(|_| SerializationError::Encode("failed to encode BrokerId"))?;
        self.broker_epoch.encode_flexible(buf, is_flexible).map_err(|_| SerializationError::Encode("failed to encode BrokerEpoch"))?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(version: crate::traits::ApiVersion, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let is_flexible = true;
        let broker_id = <i32 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| SerializationError::Decode("failed to decode BrokerId"))?;
        let broker_epoch = <i64 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| SerializationError::Decode("failed to decode BrokerEpoch"))?;
        Ok(Self { broker_id, broker_epoch })
    }
}
impl KafkaSerialize for AllocateProducerIdsRequest {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.broker_id.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode BrokerId".into() })?;
        self.broker_epoch.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode BrokerEpoch".into() })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(&self, buf: &mut B, is_flexible: bool) -> Result<(), EncodeError> {
        self.broker_id.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode BrokerId".into() })?;
        self.broker_epoch.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode BrokerEpoch".into() })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for AllocateProducerIdsRequest {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let broker_id = <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode BrokerId".into() })?;
        let broker_epoch = <i64 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode BrokerEpoch".into() })?;
        Ok(Self { broker_id, broker_epoch })
    }
    fn decode_flexible<B: Buf>(buf: &mut B, is_flexible: bool) -> Result<Self, DecodeError> {
        let broker_id = <i32 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode BrokerId".into() })?;
        let broker_epoch = <i64 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode BrokerEpoch".into() })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self { broker_id, broker_epoch })
    }
}


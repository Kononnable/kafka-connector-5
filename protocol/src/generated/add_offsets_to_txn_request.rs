#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// AddOffsetsToTxnRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct AddOffsetsToTxnRequest {
    /// The transactional id corresponding to the transaction.
    pub transactional_id: String,
    /// Current producer id in use by the transactional id.
    pub producer_id: i64,
    /// Current epoch associated with the producer id.
    pub producer_epoch: i16,
    /// The unique group identifier.
    pub group_id: String,
}

impl ApiRequest for AddOffsetsToTxnRequest {
    type Response = crate::generated::AddOffsetsToTxnResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(25)
    }
    fn get_min_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(0)
    }
    fn get_max_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(4)
    }
    fn get_min_flexible_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(3)
    }
    fn serialize(
        &self,
        version: crate::traits::ApiVersion,
        buf: &mut BytesMut,
    ) -> Result<(), SerializationError> {
        assert!(
            (0) <= version.0 && version.0 <= (4),
            "version {} is not supported by {} (supported: 0-4)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        self.transactional_id
            .encode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode TransactionalId"))?;
        self.producer_id
            .encode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode ProducerId"))?;
        self.producer_epoch
            .encode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode ProducerEpoch"))?;
        self.group_id
            .encode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode GroupId"))?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(
        version: crate::traits::ApiVersion,
        buf: &mut Bytes,
    ) -> Result<Self, SerializationError> {
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let transactional_id = <String as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode TransactionalId"))?;
        let producer_id = <i64 as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode ProducerId"))?;
        let producer_epoch = <i16 as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode ProducerEpoch"))?;
        let group_id = <String as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode GroupId"))?;
        Ok(Self {
            transactional_id,
            producer_id,
            producer_epoch,
            group_id,
        })
    }
}
impl KafkaSerialize for AddOffsetsToTxnRequest {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.transactional_id
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode TransactionalId".into(),
            })?;
        self.producer_id
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ProducerId".into(),
            })?;
        self.producer_epoch
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ProducerEpoch".into(),
            })?;
        self.group_id
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode GroupId".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for AddOffsetsToTxnRequest {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        let transactional_id = <String as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode TransactionalId".into(),
            })?;
        let producer_id =
            <i64 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode ProducerId".into(),
                }
            })?;
        let producer_epoch =
            <i16 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode ProducerEpoch".into(),
                }
            })?;
        let group_id =
            <String as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode GroupId".into(),
                }
            })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            transactional_id,
            producer_id,
            producer_epoch,
            group_id,
        })
    }
}

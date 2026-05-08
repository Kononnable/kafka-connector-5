#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{KafkaDeserialize, KafkaSerialize};
use crate::traits::{
    ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVersionTrait, SerializationError,
};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// EndTxnRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct EndTxnRequest {
    /// The ID of the transaction to end.
    pub transactional_id: String,
    /// The producer ID.
    pub producer_id: i64,
    /// The current epoch associated with the producer.
    pub producer_epoch: i16,
    /// True if the transaction was committed, false if it was aborted.
    pub committed: bool,
}

impl ApiRequest for EndTxnRequest {
    type Response = crate::generated::EndTxnResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(26)
    }
    fn get_min_supported_version() -> ApiVersionTrait {
        ApiVersionTrait::new(0)
    }
    fn get_max_supported_version() -> ApiVersionTrait {
        ApiVersionTrait::new(5)
    }
    fn get_min_flexible_version() -> ApiVersionTrait {
        ApiVersionTrait::new(3)
    }
    fn serialize(
        &self,
        version: ApiVersionTrait,
        buf: &mut BytesMut,
    ) -> Result<(), SerializationError> {
        assert!(
            (0) <= version.0 && version.0 <= (5),
            "version {} is not supported by {} (supported: 0-5)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        self.transactional_id.encode(buf, version, is_flexible)?;
        self.producer_id.encode(buf, version, is_flexible)?;
        self.producer_epoch.encode(buf, version, is_flexible)?;
        self.committed.encode(buf, version, is_flexible)?;
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(version: ApiVersionTrait, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let transactional_id = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let producer_id = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let producer_epoch = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let committed = KafkaDeserialize::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            transactional_id,
            producer_id,
            producer_epoch,
            committed,
        })
    }
}
impl KafkaSerialize for EndTxnRequest {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVersionTrait,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.transactional_id.encode(buf, version, is_flexible)?;
        self.producer_id.encode(buf, version, is_flexible)?;
        self.producer_epoch.encode(buf, version, is_flexible)?;
        self.committed.encode(buf, version, is_flexible)?;
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for EndTxnRequest {
    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVersionTrait,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let transactional_id = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let producer_id = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let producer_epoch = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let committed = KafkaDeserialize::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            transactional_id,
            producer_id,
            producer_epoch,
            committed,
        })
    }
}

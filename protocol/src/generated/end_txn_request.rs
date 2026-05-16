#![allow(unused_imports, unused_variables)]
use bytes::{Buf, BufMut, Bytes, BytesMut};
use indexmap::IndexMap;

use crate::protocol::serialization::{KafkaCodec, decode_unsigned_varint, encode_unsigned_varint};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVer, SerializationError};

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
    fn get_min_supported_version() -> ApiVer {
        ApiVer::new(0)
    }
    fn get_max_supported_version() -> ApiVer {
        ApiVer::new(5)
    }
    fn get_min_flexible_version() -> ApiVer {
        ApiVer::new(3)
    }
    fn serialize(&self, version: ApiVer, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            0 <= version.0 && version.0 <= 5,
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
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(version: ApiVer, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let transactional_id = KafkaCodec::decode(buf, version, is_flexible)?;
        let producer_id = KafkaCodec::decode(buf, version, is_flexible)?;
        let producer_epoch = KafkaCodec::decode(buf, version, is_flexible)?;
        let committed = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            transactional_id,
            producer_id,
            producer_epoch,
            committed,
        })
    }
}
impl KafkaCodec for EndTxnRequest {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.transactional_id.encode(buf, version, is_flexible)?;
        self.producer_id.encode(buf, version, is_flexible)?;
        self.producer_epoch.encode(buf, version, is_flexible)?;
        self.committed.encode(buf, version, is_flexible)?;
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }

    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let transactional_id = KafkaCodec::decode(buf, version, is_flexible)?;
        let producer_id = KafkaCodec::decode(buf, version, is_flexible)?;
        let producer_epoch = KafkaCodec::decode(buf, version, is_flexible)?;
        let committed = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            transactional_id,
            producer_id,
            producer_epoch,
            committed,
        })
    }
}

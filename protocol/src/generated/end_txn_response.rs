#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{KafkaDeserialize, KafkaSerialize};
use crate::traits::{
    ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVersionTrait, SerializationError,
};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// EndTxnResponse
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct EndTxnResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// The error code, or 0 if there was no error.
    pub error_code: i16,
    /// The producer ID.
    /// Available in version 5+.
    pub producer_id: i64,
    /// The current epoch associated with the producer.
    /// Available in version 5+.
    pub producer_epoch: i16,
}

impl ApiResponse for EndTxnResponse {
    type Request = crate::generated::EndTxnRequest;
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
        self.throttle_time_ms.encode(buf, version, is_flexible)?;
        self.error_code.encode(buf, version, is_flexible)?;
        if (5) <= version.0 {
            self.producer_id.encode(buf, version, is_flexible)?;
        } else if self.producer_id != 0 {
            return Err(SerializationError::Encode(
                "field 'ProducerId' is not available in this version",
            ));
        }
        if (5) <= version.0 {
            self.producer_epoch.encode(buf, version, is_flexible)?;
        } else if self.producer_epoch != 0 {
            return Err(SerializationError::Encode(
                "field 'ProducerEpoch' is not available in this version",
            ));
        }
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(version: ApiVersionTrait, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let throttle_time_ms = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let error_code = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let producer_id = if (5) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let producer_epoch = if (5) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            throttle_time_ms,
            error_code,
            producer_id,
            producer_epoch,
        })
    }
}
impl KafkaSerialize for EndTxnResponse {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVersionTrait,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.throttle_time_ms.encode(buf, version, is_flexible)?;
        self.error_code.encode(buf, version, is_flexible)?;
        if (5) <= version.0 {
            self.producer_id.encode(buf, version, is_flexible)?;
        }
        if (5) <= version.0 {
            self.producer_epoch.encode(buf, version, is_flexible)?;
        }
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for EndTxnResponse {
    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVersionTrait,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let throttle_time_ms = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let error_code = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let producer_id = if (5) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let producer_epoch = if (5) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            throttle_time_ms,
            error_code,
            producer_id,
            producer_epoch,
        })
    }
}

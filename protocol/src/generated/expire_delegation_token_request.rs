#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{KafkaDeserialize, KafkaSerialize};
use crate::traits::{
    ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVersionTrait, SerializationError,
};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// ExpireDelegationTokenRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ExpireDelegationTokenRequest {
    /// The HMAC of the delegation token to be expired.
    pub hmac: Vec<u8>,
    /// The expiry time period in milliseconds.
    pub expiry_time_period_ms: i64,
}

impl ApiRequest for ExpireDelegationTokenRequest {
    type Response = crate::generated::ExpireDelegationTokenResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(40)
    }
    fn get_min_supported_version() -> ApiVersionTrait {
        ApiVersionTrait::new(1)
    }
    fn get_max_supported_version() -> ApiVersionTrait {
        ApiVersionTrait::new(2)
    }
    fn get_min_flexible_version() -> ApiVersionTrait {
        ApiVersionTrait::new(2)
    }
    fn serialize(
        &self,
        version: ApiVersionTrait,
        buf: &mut BytesMut,
    ) -> Result<(), SerializationError> {
        assert!(
            (1) <= version.0 && version.0 <= (2),
            "version {} is not supported by {} (supported: 1-2)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        self.hmac.encode(buf, version, is_flexible)?;
        self.expiry_time_period_ms
            .encode(buf, version, is_flexible)?;
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(version: ApiVersionTrait, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let hmac = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let expiry_time_period_ms = KafkaDeserialize::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            hmac,
            expiry_time_period_ms,
        })
    }
}
impl KafkaSerialize for ExpireDelegationTokenRequest {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVersionTrait,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.hmac.encode(buf, version, is_flexible)?;
        self.expiry_time_period_ms
            .encode(buf, version, is_flexible)?;
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for ExpireDelegationTokenRequest {
    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVersionTrait,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let hmac = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let expiry_time_period_ms = KafkaDeserialize::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            hmac,
            expiry_time_period_ms,
        })
    }
}

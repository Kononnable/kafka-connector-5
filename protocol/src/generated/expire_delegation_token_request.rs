#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion, SerializationError};
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
        self.hmac
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode Hmac"))?;
        self.expiry_time_period_ms
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode ExpiryTimePeriodMs"))?;
        Ok(())
    }
    fn deserialize(version: ApiVersion, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let hmac = <Vec<u8> as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode Hmac"))?;
        let expiry_time_period_ms = <i64 as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode ExpiryTimePeriodMs"))?;
        Ok(Self {
            hmac,
            expiry_time_period_ms,
        })
    }
}
impl KafkaSerialize for ExpireDelegationTokenRequest {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.hmac
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Hmac".into(),
            })?;
        self.expiry_time_period_ms
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ExpiryTimePeriodMs".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for ExpireDelegationTokenRequest {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let hmac =
            <Vec<u8> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode Hmac".into(),
            })?;
        let expiry_time_period_ms =
            <i64 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ExpiryTimePeriodMs".into(),
            })?;
        Ok(Self {
            hmac,
            expiry_time_period_ms,
        })
    }
}

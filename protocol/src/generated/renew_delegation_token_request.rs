#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// RenewDelegationTokenRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RenewDelegationTokenRequest {
    /// The HMAC of the delegation token to be renewed.
    pub hmac: Vec<u8>,
    /// The renewal time period in milliseconds.
    pub renew_period_ms: i64,
}

impl ApiRequest for RenewDelegationTokenRequest {
    type Response = crate::generated::RenewDelegationTokenResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(39)
    }
    fn get_min_supported_version() -> ApiVersion {
        ApiVersion::new(0)
    }
    fn get_max_supported_version() -> ApiVersion {
        ApiVersion::new(2)
    }
    fn serialize(&self, version: ApiVersion, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            (0) <= version.0 && version.0 <= (2),
            "version {} is not supported by {} (supported: 0-2)",
            version.0,
            stringify!(Self)
        );
        self.hmac
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode Hmac"))?;
        self.renew_period_ms
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode RenewPeriodMs"))?;
        Ok(())
    }
    fn deserialize(version: ApiVersion, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let hmac = <Vec<u8> as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode Hmac"))?;
        let renew_period_ms = <i64 as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode RenewPeriodMs"))?;
        Ok(Self {
            hmac,
            renew_period_ms,
        })
    }
}
impl KafkaSerialize for RenewDelegationTokenRequest {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.hmac
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Hmac".into(),
            })?;
        self.renew_period_ms
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode RenewPeriodMs".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for RenewDelegationTokenRequest {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let hmac =
            <Vec<u8> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode Hmac".into(),
            })?;
        let renew_period_ms =
            <i64 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode RenewPeriodMs".into(),
            })?;
        Ok(Self {
            hmac,
            renew_period_ms,
        })
    }
}

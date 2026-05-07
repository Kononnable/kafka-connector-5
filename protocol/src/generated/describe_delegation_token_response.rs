#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// DescribeDelegationTokenResponse
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct DescribeDelegationTokenResponse {
    /// The error code, or 0 if there was no error.
    pub error_code: i16,
    /// The tokens.
    pub tokens: Vec<DescribedDelegationToken>,
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct DescribedDelegationToken {
    /// The token principal type.
    pub principal_type: String,
    /// The token principal name.
    pub principal_name: String,
    /// The token issue timestamp in milliseconds.
    pub issue_timestamp: i64,
    /// The token expiry timestamp in milliseconds.
    pub expiry_timestamp: i64,
    /// The token maximum timestamp length in milliseconds.
    pub max_timestamp: i64,
    /// The token ID.
    pub token_id: String,
    /// The token HMAC.
    pub hmac: Vec<u8>,
    /// Those who are able to renew this token before it expires.
    pub renewers: Vec<DescribedDelegationTokenRenewer>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct DescribedDelegationTokenRenewer {
    /// The renewer principal type
    pub principal_type: String,
    /// The renewer principal name
    pub principal_name: String,
}

impl ApiResponse for DescribeDelegationTokenResponse {
    type Request = crate::generated::DescribeDelegationTokenRequest;
    fn get_api_key() -> ApiKey {
        ApiKey::new(41)
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
        self.error_code
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode ErrorCode"))?;
        self.tokens
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode Tokens"))?;
        self.throttle_time_ms
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode ThrottleTimeMs"))?;
        Ok(())
    }
    fn deserialize(version: ApiVersion, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let error_code = <i16 as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode ErrorCode"))?;
        let tokens = <Vec<DescribedDelegationToken> as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode Tokens"))?;
        let throttle_time_ms = <i32 as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode ThrottleTimeMs"))?;
        Ok(Self {
            error_code,
            tokens,
            throttle_time_ms,
        })
    }
}
impl KafkaSerialize for DescribeDelegationTokenResponse {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.error_code
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ErrorCode".into(),
            })?;
        self.tokens
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Tokens".into(),
            })?;
        self.throttle_time_ms
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ThrottleTimeMs".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for DescribeDelegationTokenResponse {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let error_code =
            <i16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ErrorCode".into(),
            })?;
        let tokens =
            <Vec<DescribedDelegationToken> as KafkaDeserialize>::decode(buf).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode Tokens".into(),
                }
            })?;
        let throttle_time_ms =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ThrottleTimeMs".into(),
            })?;
        Ok(Self {
            error_code,
            tokens,
            throttle_time_ms,
        })
    }
}

impl KafkaSerialize for DescribedDelegationToken {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.principal_type
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode PrincipalType".into(),
            })?;
        self.principal_name
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode PrincipalName".into(),
            })?;
        self.issue_timestamp
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode IssueTimestamp".into(),
            })?;
        self.expiry_timestamp
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ExpiryTimestamp".into(),
            })?;
        self.max_timestamp
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode MaxTimestamp".into(),
            })?;
        self.token_id
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode TokenId".into(),
            })?;
        self.hmac
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Hmac".into(),
            })?;
        self.renewers
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Renewers".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for DescribedDelegationToken {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let principal_type =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode PrincipalType".into(),
            })?;
        let principal_name =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode PrincipalName".into(),
            })?;
        let issue_timestamp =
            <i64 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode IssueTimestamp".into(),
            })?;
        let expiry_timestamp =
            <i64 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ExpiryTimestamp".into(),
            })?;
        let max_timestamp =
            <i64 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode MaxTimestamp".into(),
            })?;
        let token_id =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode TokenId".into(),
            })?;
        let hmac =
            <Vec<u8> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode Hmac".into(),
            })?;
        let renewers = <Vec<DescribedDelegationTokenRenewer> as KafkaDeserialize>::decode(buf)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode Renewers".into(),
            })?;
        Ok(Self {
            principal_type,
            principal_name,
            issue_timestamp,
            expiry_timestamp,
            max_timestamp,
            token_id,
            hmac,
            renewers,
        })
    }
}

impl KafkaSerialize for DescribedDelegationTokenRenewer {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.principal_type
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode PrincipalType".into(),
            })?;
        self.principal_name
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode PrincipalName".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for DescribedDelegationTokenRenewer {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let principal_type =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode PrincipalType".into(),
            })?;
        let principal_name =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode PrincipalName".into(),
            })?;
        Ok(Self {
            principal_type,
            principal_name,
        })
    }
}

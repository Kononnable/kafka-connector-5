#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// CreateDelegationTokenResponse
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct CreateDelegationTokenResponse {
    /// The top-level error, or zero if there was no error.
    pub error_code: i16,
    /// The principal type of the token owner.
    pub principal_type: String,
    /// The name of the token owner.
    pub principal_name: String,
    /// The principal type of the requester of the token.
    /// Available in version 3+.
    pub token_requester_principal_type: String,
    /// The principal type of the requester of the token.
    /// Available in version 3+.
    pub token_requester_principal_name: String,
    /// When this token was generated.
    pub issue_timestamp_ms: i64,
    /// When this token expires.
    pub expiry_timestamp_ms: i64,
    /// The maximum lifetime of this token.
    pub max_timestamp_ms: i64,
    /// The token UUID.
    pub token_id: String,
    /// HMAC of the delegation token.
    pub hmac: Vec<u8>,
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
}

impl ApiResponse for CreateDelegationTokenResponse {
    type Request = crate::generated::CreateDelegationTokenRequest;
    fn get_api_key() -> ApiKey {
        ApiKey::new(38)
    }
    fn get_min_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(1)
    }
    fn get_max_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(3)
    }
    fn serialize(
        &self,
        version: crate::traits::ApiVersion,
        buf: &mut BytesMut,
    ) -> Result<(), SerializationError> {
        assert!(
            (1) <= version.0 && version.0 <= (3),
            "version {} is not supported by {} (supported: 1-3)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = (2) <= version.0;
        self.error_code
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode ErrorCode"))?;
        self.principal_type
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode PrincipalType"))?;
        self.principal_name
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode PrincipalName"))?;
        if (3) <= version.0 {
            self.token_requester_principal_type
                .encode_flexible(buf, is_flexible)
                .map_err(|_| {
                    SerializationError::Encode("failed to encode TokenRequesterPrincipalType")
                })?;
        }
        if (3) <= version.0 {
            self.token_requester_principal_name
                .encode_flexible(buf, is_flexible)
                .map_err(|_| {
                    SerializationError::Encode("failed to encode TokenRequesterPrincipalName")
                })?;
        }
        self.issue_timestamp_ms
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode IssueTimestampMs"))?;
        self.expiry_timestamp_ms
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode ExpiryTimestampMs"))?;
        self.max_timestamp_ms
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode MaxTimestampMs"))?;
        self.token_id
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode TokenId"))?;
        self.hmac
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode Hmac"))?;
        self.throttle_time_ms
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode ThrottleTimeMs"))?;
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
        let is_flexible = (2) <= version.0;
        let error_code = <i16 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode ErrorCode"))?;
        let principal_type =
            <String as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode PrincipalType"))?;
        let principal_name =
            <String as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode PrincipalName"))?;
        let token_requester_principal_type = if (3) <= version.0 {
            <String as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(
                |_| SerializationError::Decode("failed to decode TokenRequesterPrincipalType"),
            )?
        } else {
            Default::default()
        };
        let token_requester_principal_name = if (3) <= version.0 {
            <String as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(
                |_| SerializationError::Decode("failed to decode TokenRequesterPrincipalName"),
            )?
        } else {
            Default::default()
        };
        let issue_timestamp_ms =
            <i64 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode IssueTimestampMs"))?;
        let expiry_timestamp_ms =
            <i64 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode ExpiryTimestampMs"))?;
        let max_timestamp_ms =
            <i64 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode MaxTimestampMs"))?;
        let token_id = <String as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode TokenId"))?;
        let hmac = <Vec<u8> as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode Hmac"))?;
        let throttle_time_ms =
            <i32 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode ThrottleTimeMs"))?;
        Ok(Self {
            error_code,
            principal_type,
            principal_name,
            token_requester_principal_type,
            token_requester_principal_name,
            issue_timestamp_ms,
            expiry_timestamp_ms,
            max_timestamp_ms,
            token_id,
            hmac,
            throttle_time_ms,
        })
    }
}
impl KafkaSerialize for CreateDelegationTokenResponse {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.error_code
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ErrorCode".into(),
            })?;
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
        self.token_requester_principal_type
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode TokenRequesterPrincipalType".into(),
            })?;
        self.token_requester_principal_name
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode TokenRequesterPrincipalName".into(),
            })?;
        self.issue_timestamp_ms
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode IssueTimestampMs".into(),
            })?;
        self.expiry_timestamp_ms
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ExpiryTimestampMs".into(),
            })?;
        self.max_timestamp_ms
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode MaxTimestampMs".into(),
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
        self.throttle_time_ms
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ThrottleTimeMs".into(),
            })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(
        &self,
        buf: &mut B,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.error_code
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ErrorCode".into(),
            })?;
        self.principal_type
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode PrincipalType".into(),
            })?;
        self.principal_name
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode PrincipalName".into(),
            })?;
        self.token_requester_principal_type
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode TokenRequesterPrincipalType".into(),
            })?;
        self.token_requester_principal_name
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode TokenRequesterPrincipalName".into(),
            })?;
        self.issue_timestamp_ms
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode IssueTimestampMs".into(),
            })?;
        self.expiry_timestamp_ms
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ExpiryTimestampMs".into(),
            })?;
        self.max_timestamp_ms
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode MaxTimestampMs".into(),
            })?;
        self.token_id
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode TokenId".into(),
            })?;
        self.hmac
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Hmac".into(),
            })?;
        self.throttle_time_ms
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ThrottleTimeMs".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for CreateDelegationTokenResponse {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] classic decode field `ErrorCode` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let error_code =
            <i16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ErrorCode".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `PrincipalType` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let principal_type =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode PrincipalType".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `PrincipalName` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let principal_name =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode PrincipalName".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `TokenRequesterPrincipalType` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let token_requester_principal_type =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode TokenRequesterPrincipalType".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `TokenRequesterPrincipalName` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let token_requester_principal_name =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode TokenRequesterPrincipalName".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `IssueTimestampMs` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let issue_timestamp_ms =
            <i64 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode IssueTimestampMs".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `ExpiryTimestampMs` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let expiry_timestamp_ms =
            <i64 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ExpiryTimestampMs".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `MaxTimestampMs` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let max_timestamp_ms =
            <i64 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode MaxTimestampMs".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `TokenId` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let token_id =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode TokenId".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `Hmac` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let hmac =
            <Vec<u8> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode Hmac".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `ThrottleTimeMs` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let throttle_time_ms =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ThrottleTimeMs".into(),
            })?;
        Ok(Self {
            error_code,
            principal_type,
            principal_name,
            token_requester_principal_type,
            token_requester_principal_name,
            issue_timestamp_ms,
            expiry_timestamp_ms,
            max_timestamp_ms,
            token_id,
            hmac,
            throttle_time_ms,
        })
    }
    fn decode_flexible<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] decoding field `ErrorCode` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let error_code = <i16 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode ErrorCode".into(),
            })?;
        tracing::trace!(
            "  [{}] decoding field `PrincipalType` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let principal_type =
            <String as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode PrincipalType".into(),
                },
            )?;
        tracing::trace!(
            "  [{}] decoding field `PrincipalName` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let principal_name =
            <String as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode PrincipalName".into(),
                },
            )?;
        tracing::trace!(
            "  [{}] decoding field `TokenRequesterPrincipalType` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let token_requester_principal_type = if (3) <= version.0 {
            <String as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode TokenRequesterPrincipalType".into(),
                },
            )?
        } else {
            Default::default()
        };
        tracing::trace!(
            "  [{}] decoding field `TokenRequesterPrincipalName` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let token_requester_principal_name = if (3) <= version.0 {
            <String as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode TokenRequesterPrincipalName".into(),
                },
            )?
        } else {
            Default::default()
        };
        tracing::trace!(
            "  [{}] decoding field `IssueTimestampMs` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let issue_timestamp_ms =
            <i64 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode IssueTimestampMs".into(),
                },
            )?;
        tracing::trace!(
            "  [{}] decoding field `ExpiryTimestampMs` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let expiry_timestamp_ms =
            <i64 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode ExpiryTimestampMs".into(),
                },
            )?;
        tracing::trace!(
            "  [{}] decoding field `MaxTimestampMs` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let max_timestamp_ms =
            <i64 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode MaxTimestampMs".into(),
                },
            )?;
        tracing::trace!(
            "  [{}] decoding field `TokenId` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let token_id = <String as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode TokenId".into(),
            })?;
        tracing::trace!(
            "  [{}] decoding field `Hmac` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let hmac = <Vec<u8> as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode Hmac".into(),
            })?;
        tracing::trace!(
            "  [{}] decoding field `ThrottleTimeMs` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let throttle_time_ms =
            <i32 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode ThrottleTimeMs".into(),
                },
            )?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            error_code,
            principal_type,
            principal_name,
            token_requester_principal_type,
            token_requester_principal_name,
            issue_timestamp_ms,
            expiry_timestamp_ms,
            max_timestamp_ms,
            token_id,
            hmac,
            throttle_time_ms,
        })
    }
}

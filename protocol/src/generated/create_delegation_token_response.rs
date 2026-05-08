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
    fn get_min_flexible_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(2)
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
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        self.error_code
            .encode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode ErrorCode"))?;
        self.principal_type
            .encode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode PrincipalType"))?;
        self.principal_name
            .encode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode PrincipalName"))?;
        if (3) <= version.0 {
            self.token_requester_principal_type
                .encode(buf, version, is_flexible)
                .map_err(|_| {
                    SerializationError::Encode("failed to encode TokenRequesterPrincipalType")
                })?;
        } else if !self.token_requester_principal_type.is_empty() {
            return Err(SerializationError::Encode(
                "field 'TokenRequesterPrincipalType' is not available in this version",
            ));
        }
        if (3) <= version.0 {
            self.token_requester_principal_name
                .encode(buf, version, is_flexible)
                .map_err(|_| {
                    SerializationError::Encode("failed to encode TokenRequesterPrincipalName")
                })?;
        } else if !self.token_requester_principal_name.is_empty() {
            return Err(SerializationError::Encode(
                "field 'TokenRequesterPrincipalName' is not available in this version",
            ));
        }
        self.issue_timestamp_ms
            .encode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode IssueTimestampMs"))?;
        self.expiry_timestamp_ms
            .encode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode ExpiryTimestampMs"))?;
        self.max_timestamp_ms
            .encode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode MaxTimestampMs"))?;
        self.token_id
            .encode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode TokenId"))?;
        self.hmac
            .encode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode Hmac"))?;
        self.throttle_time_ms
            .encode(buf, version, is_flexible)
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
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let error_code = <i16 as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode ErrorCode"))?;
        let principal_type = <String as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode PrincipalType"))?;
        let principal_name = <String as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode PrincipalName"))?;
        let token_requester_principal_type = if (3) <= version.0 {
            <String as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                SerializationError::Decode("failed to decode TokenRequesterPrincipalType")
            })?
        } else {
            Default::default()
        };
        let token_requester_principal_name = if (3) <= version.0 {
            <String as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                SerializationError::Decode("failed to decode TokenRequesterPrincipalName")
            })?
        } else {
            Default::default()
        };
        let issue_timestamp_ms = <i64 as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode IssueTimestampMs"))?;
        let expiry_timestamp_ms = <i64 as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode ExpiryTimestampMs"))?;
        let max_timestamp_ms = <i64 as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode MaxTimestampMs"))?;
        let token_id = <String as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode TokenId"))?;
        let hmac = <Vec<u8> as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode Hmac"))?;
        let throttle_time_ms = <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)
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
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.error_code
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ErrorCode".into(),
            })?;
        self.principal_type
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode PrincipalType".into(),
            })?;
        self.principal_name
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode PrincipalName".into(),
            })?;
        if (3) <= version.0 {
            self.token_requester_principal_type
                .encode(buf, version, is_flexible)
                .map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode TokenRequesterPrincipalType".into(),
                })?;
        }
        if (3) <= version.0 {
            self.token_requester_principal_name
                .encode(buf, version, is_flexible)
                .map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode TokenRequesterPrincipalName".into(),
                })?;
        }
        self.issue_timestamp_ms
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode IssueTimestampMs".into(),
            })?;
        self.expiry_timestamp_ms
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ExpiryTimestampMs".into(),
            })?;
        self.max_timestamp_ms
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode MaxTimestampMs".into(),
            })?;
        self.token_id
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode TokenId".into(),
            })?;
        self.hmac
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Hmac".into(),
            })?;
        self.throttle_time_ms
            .encode(buf, version, is_flexible)
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
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        let error_code =
            <i16 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode ErrorCode".into(),
                }
            })?;
        let principal_type = <String as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode PrincipalType".into(),
            })?;
        let principal_name = <String as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode PrincipalName".into(),
            })?;
        let token_requester_principal_type = if (3) <= version.0 {
            <String as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode TokenRequesterPrincipalType".into(),
                }
            })?
        } else {
            Default::default()
        };
        let token_requester_principal_name = if (3) <= version.0 {
            <String as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode TokenRequesterPrincipalName".into(),
                }
            })?
        } else {
            Default::default()
        };
        let issue_timestamp_ms = <i64 as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode IssueTimestampMs".into(),
            })?;
        let expiry_timestamp_ms = <i64 as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode ExpiryTimestampMs".into(),
            })?;
        let max_timestamp_ms = <i64 as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode MaxTimestampMs".into(),
            })?;
        let token_id =
            <String as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode TokenId".into(),
                }
            })?;
        let hmac =
            <Vec<u8> as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode Hmac".into(),
                }
            })?;
        let throttle_time_ms = <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode ThrottleTimeMs".into(),
            })?;
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

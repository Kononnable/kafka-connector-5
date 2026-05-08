#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{KafkaCodec, decode_unsigned_varint, encode_unsigned_varint};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVer, SerializationError};
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
    fn get_min_supported_version() -> ApiVer {
        ApiVer::new(1)
    }
    fn get_max_supported_version() -> ApiVer {
        ApiVer::new(3)
    }
    fn get_min_flexible_version() -> ApiVer {
        ApiVer::new(2)
    }
    fn serialize(&self, version: ApiVer, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            1 <= version.0 && version.0 <= 3,
            "version {} is not supported by {} (supported: 1-3)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        self.error_code.encode(buf, version, is_flexible)?;
        self.principal_type.encode(buf, version, is_flexible)?;
        self.principal_name.encode(buf, version, is_flexible)?;
        if 3 <= version.0 {
            self.token_requester_principal_type
                .encode(buf, version, is_flexible)?;
        } else if !self.token_requester_principal_type.is_empty() {
            return Err(SerializationError::Encode(
                "field 'TokenRequesterPrincipalType' is not available in this version",
            ));
        }
        if 3 <= version.0 {
            self.token_requester_principal_name
                .encode(buf, version, is_flexible)?;
        } else if !self.token_requester_principal_name.is_empty() {
            return Err(SerializationError::Encode(
                "field 'TokenRequesterPrincipalName' is not available in this version",
            ));
        }
        self.issue_timestamp_ms.encode(buf, version, is_flexible)?;
        self.expiry_timestamp_ms.encode(buf, version, is_flexible)?;
        self.max_timestamp_ms.encode(buf, version, is_flexible)?;
        self.token_id.encode(buf, version, is_flexible)?;
        self.hmac.encode(buf, version, is_flexible)?;
        self.throttle_time_ms.encode(buf, version, is_flexible)?;
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(version: ApiVer, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let error_code = KafkaCodec::decode(buf, version, is_flexible)?;
        let principal_type = KafkaCodec::decode(buf, version, is_flexible)?;
        let principal_name = KafkaCodec::decode(buf, version, is_flexible)?;
        let token_requester_principal_type = if 3 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let token_requester_principal_name = if 3 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let issue_timestamp_ms = KafkaCodec::decode(buf, version, is_flexible)?;
        let expiry_timestamp_ms = KafkaCodec::decode(buf, version, is_flexible)?;
        let max_timestamp_ms = KafkaCodec::decode(buf, version, is_flexible)?;
        let token_id = KafkaCodec::decode(buf, version, is_flexible)?;
        let hmac = KafkaCodec::decode(buf, version, is_flexible)?;
        let throttle_time_ms = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
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
impl KafkaCodec for CreateDelegationTokenResponse {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.error_code.encode(buf, version, is_flexible)?;
        self.principal_type.encode(buf, version, is_flexible)?;
        self.principal_name.encode(buf, version, is_flexible)?;
        if 3 <= version.0 {
            self.token_requester_principal_type
                .encode(buf, version, is_flexible)?;
        }
        if 3 <= version.0 {
            self.token_requester_principal_name
                .encode(buf, version, is_flexible)?;
        }
        self.issue_timestamp_ms.encode(buf, version, is_flexible)?;
        self.expiry_timestamp_ms.encode(buf, version, is_flexible)?;
        self.max_timestamp_ms.encode(buf, version, is_flexible)?;
        self.token_id.encode(buf, version, is_flexible)?;
        self.hmac.encode(buf, version, is_flexible)?;
        self.throttle_time_ms.encode(buf, version, is_flexible)?;
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
        let error_code = KafkaCodec::decode(buf, version, is_flexible)?;
        let principal_type = KafkaCodec::decode(buf, version, is_flexible)?;
        let principal_name = KafkaCodec::decode(buf, version, is_flexible)?;
        let token_requester_principal_type = if 3 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let token_requester_principal_name = if 3 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let issue_timestamp_ms = KafkaCodec::decode(buf, version, is_flexible)?;
        let expiry_timestamp_ms = KafkaCodec::decode(buf, version, is_flexible)?;
        let max_timestamp_ms = KafkaCodec::decode(buf, version, is_flexible)?;
        let token_id = KafkaCodec::decode(buf, version, is_flexible)?;
        let hmac = KafkaCodec::decode(buf, version, is_flexible)?;
        let throttle_time_ms = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
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

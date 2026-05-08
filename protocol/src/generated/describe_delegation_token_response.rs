#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
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
    /// The principal type of the requester of the token.
    /// Available in version 3+.
    pub token_requester_principal_type: String,
    /// The principal type of the requester of the token.
    /// Available in version 3+.
    pub token_requester_principal_name: String,
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
    /// The renewer principal type.
    pub principal_type: String,
    /// The renewer principal name.
    pub principal_name: String,
}

impl ApiResponse for DescribeDelegationTokenResponse {
    type Request = crate::generated::DescribeDelegationTokenRequest;
    fn get_api_key() -> ApiKey {
        ApiKey::new(41)
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
        self.error_code.encode(buf, version, is_flexible)?;
        self.tokens.encode(buf, version, is_flexible)?;
        self.throttle_time_ms.encode(buf, version, is_flexible)?;
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
        let error_code = <i16 as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let tokens =
            <Vec<DescribedDelegationToken> as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let throttle_time_ms = <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        Ok(Self {
            error_code,
            tokens,
            throttle_time_ms,
        })
    }
}
impl KafkaSerialize for DescribeDelegationTokenResponse {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), crate::traits::SerializationError> {
        self.error_code.encode(buf, version, is_flexible)?;
        self.tokens.encode(buf, version, is_flexible)?;
        self.throttle_time_ms.encode(buf, version, is_flexible)?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for DescribeDelegationTokenResponse {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, crate::traits::SerializationError> {
        let error_code = <i16 as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let tokens =
            <Vec<DescribedDelegationToken> as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let throttle_time_ms = <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            error_code,
            tokens,
            throttle_time_ms,
        })
    }
}

impl KafkaSerialize for DescribedDelegationToken {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), crate::traits::SerializationError> {
        self.principal_type.encode(buf, version, is_flexible)?;
        self.principal_name.encode(buf, version, is_flexible)?;
        if (3) <= version.0 {
            self.token_requester_principal_type
                .encode(buf, version, is_flexible)?;
        }
        if (3) <= version.0 {
            self.token_requester_principal_name
                .encode(buf, version, is_flexible)?;
        }
        self.issue_timestamp.encode(buf, version, is_flexible)?;
        self.expiry_timestamp.encode(buf, version, is_flexible)?;
        self.max_timestamp.encode(buf, version, is_flexible)?;
        self.token_id.encode(buf, version, is_flexible)?;
        self.hmac.encode(buf, version, is_flexible)?;
        self.renewers.encode(buf, version, is_flexible)?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for DescribedDelegationToken {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, crate::traits::SerializationError> {
        let principal_type = <String as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let principal_name = <String as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let token_requester_principal_type = if (3) <= version.0 {
            <String as KafkaDeserialize>::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let token_requester_principal_name = if (3) <= version.0 {
            <String as KafkaDeserialize>::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let issue_timestamp = <i64 as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let expiry_timestamp = <i64 as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let max_timestamp = <i64 as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let token_id = <String as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let hmac = <Vec<u8> as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let renewers = <Vec<DescribedDelegationTokenRenewer> as KafkaDeserialize>::decode(
            buf,
            version,
            is_flexible,
        )?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            principal_type,
            principal_name,
            token_requester_principal_type,
            token_requester_principal_name,
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
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), crate::traits::SerializationError> {
        self.principal_type.encode(buf, version, is_flexible)?;
        self.principal_name.encode(buf, version, is_flexible)?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for DescribedDelegationTokenRenewer {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, crate::traits::SerializationError> {
        let principal_type = <String as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let principal_name = <String as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            principal_type,
            principal_name,
        })
    }
}

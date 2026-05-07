#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// CreateDelegationTokenRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct CreateDelegationTokenRequest {
    /// A list of those who are allowed to renew this token before it expires.
    pub renewers: Vec<CreatableRenewers>,
    /// The maximum lifetime of the token in milliseconds, or -1 to use the server side default.
    pub max_lifetime_ms: i64,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct CreatableRenewers {
    /// The type of the Kafka principal.
    pub principal_type: String,
    /// The name of the Kafka principal.
    pub principal_name: String,
}

impl ApiRequest for CreateDelegationTokenRequest {
    type Response = crate::generated::CreateDelegationTokenResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(38)
    }
    fn get_min_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(0)
    }
    fn get_max_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(2)
    }
    fn serialize(
        &self,
        version: crate::traits::ApiVersion,
        buf: &mut BytesMut,
    ) -> Result<(), SerializationError> {
        assert!(
            (0) <= version.0 && version.0 <= (2),
            "version {} is not supported by {} (supported: 0-2)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = (2) <= version.0;
        self.renewers
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode Renewers"))?;
        self.max_lifetime_ms
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode MaxLifetimeMs"))?;
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
        let renewers =
            <Vec<CreatableRenewers> as KafkaDeserialize>::decode_flexible(buf, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode Renewers"))?;
        let max_lifetime_ms = <i64 as KafkaDeserialize>::decode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode MaxLifetimeMs"))?;
        Ok(Self {
            renewers,
            max_lifetime_ms,
        })
    }
}
impl KafkaSerialize for CreateDelegationTokenRequest {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.renewers
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Renewers".into(),
            })?;
        self.max_lifetime_ms
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode MaxLifetimeMs".into(),
            })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(
        &self,
        buf: &mut B,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.renewers
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Renewers".into(),
            })?;
        self.max_lifetime_ms
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode MaxLifetimeMs".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for CreateDelegationTokenRequest {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let renewers = <Vec<CreatableRenewers> as KafkaDeserialize>::decode(buf).map_err(|_| {
            DecodeError::Protocol {
                message: "failed to decode Renewers".into(),
            }
        })?;
        let max_lifetime_ms =
            <i64 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode MaxLifetimeMs".into(),
            })?;
        Ok(Self {
            renewers,
            max_lifetime_ms,
        })
    }
    fn decode_flexible<B: Buf>(buf: &mut B, is_flexible: bool) -> Result<Self, DecodeError> {
        let renewers =
            <Vec<CreatableRenewers> as KafkaDeserialize>::decode_flexible(buf, is_flexible)
                .map_err(|_| DecodeError::Protocol {
                    message: "failed to decode Renewers".into(),
                })?;
        let max_lifetime_ms = <i64 as KafkaDeserialize>::decode_flexible(buf, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode MaxLifetimeMs".into(),
            })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            renewers,
            max_lifetime_ms,
        })
    }
}

impl KafkaSerialize for CreatableRenewers {
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
    fn encode_flexible<B: BufMut>(
        &self,
        buf: &mut B,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
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
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for CreatableRenewers {
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
    fn decode_flexible<B: Buf>(buf: &mut B, is_flexible: bool) -> Result<Self, DecodeError> {
        let principal_type = <String as KafkaDeserialize>::decode_flexible(buf, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode PrincipalType".into(),
            })?;
        let principal_name = <String as KafkaDeserialize>::decode_flexible(buf, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode PrincipalName".into(),
            })?;
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

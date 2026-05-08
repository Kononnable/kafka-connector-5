#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// CreateDelegationTokenRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct CreateDelegationTokenRequest {
    /// The principal type of the owner of the token. If it's null it defaults to the token request principal.
    /// Available in version 3+.
    pub owner_principal_type: Option<String>,
    /// The principal name of the owner of the token. If it's null it defaults to the token request principal.
    /// Available in version 3+.
    pub owner_principal_name: Option<String>,
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
        if (3) <= version.0 {
            self.owner_principal_type
                .encode(buf, version, is_flexible)
                .map_err(|_| SerializationError::Encode("failed to encode OwnerPrincipalType"))?;
        } else if self.owner_principal_type.is_some() {
            return Err(SerializationError::Encode(
                "field 'OwnerPrincipalType' is not available in this version",
            ));
        }
        if (3) <= version.0 {
            self.owner_principal_name
                .encode(buf, version, is_flexible)
                .map_err(|_| SerializationError::Encode("failed to encode OwnerPrincipalName"))?;
        } else if self.owner_principal_name.is_some() {
            return Err(SerializationError::Encode(
                "field 'OwnerPrincipalName' is not available in this version",
            ));
        }
        self.renewers
            .encode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode Renewers"))?;
        self.max_lifetime_ms
            .encode(buf, version, is_flexible)
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
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let owner_principal_type = if (3) <= version.0 {
            <Option<String> as KafkaDeserialize>::decode(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode OwnerPrincipalType"))?
        } else {
            Default::default()
        };
        let owner_principal_name = if (3) <= version.0 {
            <Option<String> as KafkaDeserialize>::decode(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode OwnerPrincipalName"))?
        } else {
            Default::default()
        };
        let renewers =
            <Vec<CreatableRenewers> as KafkaDeserialize>::decode(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode Renewers"))?;
        let max_lifetime_ms = <i64 as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode MaxLifetimeMs"))?;
        Ok(Self {
            owner_principal_type,
            owner_principal_name,
            renewers,
            max_lifetime_ms,
        })
    }
}
impl KafkaSerialize for CreateDelegationTokenRequest {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        if (3) <= version.0 {
            self.owner_principal_type
                .encode(buf, version, is_flexible)
                .map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode OwnerPrincipalType".into(),
                })?;
        }
        if (3) <= version.0 {
            self.owner_principal_name
                .encode(buf, version, is_flexible)
                .map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode OwnerPrincipalName".into(),
                })?;
        }
        self.renewers
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Renewers".into(),
            })?;
        self.max_lifetime_ms
            .encode(buf, version, is_flexible)
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
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        let owner_principal_type = if (3) <= version.0 {
            <Option<String> as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode OwnerPrincipalType".into(),
                },
            )?
        } else {
            Default::default()
        };
        let owner_principal_name = if (3) <= version.0 {
            <Option<String> as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode OwnerPrincipalName".into(),
                },
            )?
        } else {
            Default::default()
        };
        let renewers =
            <Vec<CreatableRenewers> as KafkaDeserialize>::decode(buf, version, is_flexible)
                .map_err(|_| DecodeError::Protocol {
                    message: "failed to decode Renewers".into(),
                })?;
        let max_lifetime_ms = <i64 as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode MaxLifetimeMs".into(),
            })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            owner_principal_type,
            owner_principal_name,
            renewers,
            max_lifetime_ms,
        })
    }
}

impl KafkaSerialize for CreatableRenewers {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
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
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for CreatableRenewers {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        let principal_type = <String as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode PrincipalType".into(),
            })?;
        let principal_name = <String as KafkaDeserialize>::decode(buf, version, is_flexible)
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

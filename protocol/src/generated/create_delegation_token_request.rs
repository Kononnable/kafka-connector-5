#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{
    KafkaDeserialize, KafkaSerialize, decode_unsigned_varint, encode_unsigned_varint,
};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVer, SerializationError};
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
            (1) <= version.0 && version.0 <= (3),
            "version {} is not supported by {} (supported: 1-3)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        if (3) <= version.0 {
            self.owner_principal_type
                .encode(buf, version, is_flexible)?;
        } else if self.owner_principal_type.is_some() {
            return Err(SerializationError::Encode(
                "field 'OwnerPrincipalType' is not available in this version",
            ));
        }
        if (3) <= version.0 {
            self.owner_principal_name
                .encode(buf, version, is_flexible)?;
        } else if self.owner_principal_name.is_some() {
            return Err(SerializationError::Encode(
                "field 'OwnerPrincipalName' is not available in this version",
            ));
        }
        self.renewers.encode(buf, version, is_flexible)?;
        self.max_lifetime_ms.encode(buf, version, is_flexible)?;
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(version: ApiVer, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let owner_principal_type = if (3) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let owner_principal_name = if (3) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let renewers = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let max_lifetime_ms = KafkaDeserialize::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
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
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        if (3) <= version.0 {
            self.owner_principal_type
                .encode(buf, version, is_flexible)?;
        }
        if (3) <= version.0 {
            self.owner_principal_name
                .encode(buf, version, is_flexible)?;
        }
        self.renewers.encode(buf, version, is_flexible)?;
        self.max_lifetime_ms.encode(buf, version, is_flexible)?;
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for CreateDelegationTokenRequest {
    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let owner_principal_type = if (3) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let owner_principal_name = if (3) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let renewers = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let max_lifetime_ms = KafkaDeserialize::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
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
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.principal_type.encode(buf, version, is_flexible)?;
        self.principal_name.encode(buf, version, is_flexible)?;
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for CreatableRenewers {
    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let principal_type = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let principal_name = KafkaDeserialize::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            principal_type,
            principal_name,
        })
    }
}

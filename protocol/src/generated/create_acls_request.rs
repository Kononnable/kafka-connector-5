#![allow(unused_imports, unused_variables)]
use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::protocol::serialization::{KafkaCodec, decode_unsigned_varint, encode_unsigned_varint};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVer, SerializationError};

// -------------------------------------------------------
// CreateAclsRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct CreateAclsRequest {
    /// The ACLs that we want to create.
    pub creations: Vec<AclCreation>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct AclCreation {
    /// The type of the resource.
    pub resource_type: i8,
    /// The resource name for the ACL.
    pub resource_name: String,
    /// The pattern type for the ACL.
    /// Available in version 1+.
    pub resource_pattern_type: i8,
    /// The principal for the ACL.
    pub principal: String,
    /// The host for the ACL.
    pub host: String,
    /// The operation type for the ACL (read, write, etc.).
    pub operation: i8,
    /// The permission type for the ACL (allow, deny, etc.).
    pub permission_type: i8,
}

impl ApiRequest for CreateAclsRequest {
    type Response = crate::generated::CreateAclsResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(30)
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
        self.creations.encode(buf, version, is_flexible)?;
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(version: ApiVer, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let creations = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self { creations })
    }
}
impl KafkaCodec for CreateAclsRequest {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.creations.encode(buf, version, is_flexible)?;
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
        let creations = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self { creations })
    }
}

impl KafkaCodec for AclCreation {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.resource_type.encode(buf, version, is_flexible)?;
        self.resource_name.encode(buf, version, is_flexible)?;
        if 1 <= version.0 {
            self.resource_pattern_type
                .encode(buf, version, is_flexible)?;
        }
        self.principal.encode(buf, version, is_flexible)?;
        self.host.encode(buf, version, is_flexible)?;
        self.operation.encode(buf, version, is_flexible)?;
        self.permission_type.encode(buf, version, is_flexible)?;
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
        let resource_type = KafkaCodec::decode(buf, version, is_flexible)?;
        let resource_name = KafkaCodec::decode(buf, version, is_flexible)?;
        let resource_pattern_type = if 1 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let principal = KafkaCodec::decode(buf, version, is_flexible)?;
        let host = KafkaCodec::decode(buf, version, is_flexible)?;
        let operation = KafkaCodec::decode(buf, version, is_flexible)?;
        let permission_type = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            resource_type,
            resource_name,
            resource_pattern_type,
            principal,
            host,
            operation,
            permission_type,
        })
    }
}

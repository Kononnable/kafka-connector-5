#![allow(unused_imports, unused_variables)]
use bytes::{Buf, BufMut, Bytes, BytesMut};
use indexmap::IndexMap;

use crate::protocol::serialization::{KafkaCodec, decode_unsigned_varint, encode_unsigned_varint};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVer, SerializationError};

// -------------------------------------------------------
// DescribeAclsResponse
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct DescribeAclsResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// The error code, or 0 if there was no error.
    pub error_code: i16,
    /// The error message, or null if there was no error.
    pub error_message: Option<String>,
    /// Each Resource that is referenced in an ACL.
    pub resources: Vec<DescribeAclsResource>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct AclDescription {
    /// The ACL principal.
    pub principal: String,
    /// The ACL host.
    pub host: String,
    /// The ACL operation.
    pub operation: i8,
    /// The ACL permission type.
    pub permission_type: i8,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DescribeAclsResource {
    /// The resource type.
    pub resource_type: i8,
    /// The resource name.
    pub resource_name: String,
    /// The resource pattern type.
    /// Available in version 1+.
    pub pattern_type: i8,
    /// The ACLs.
    pub acls: Vec<AclDescription>,
}
impl Default for DescribeAclsResource {
    fn default() -> Self {
        Self {
            resource_type: 0,
            resource_name: String::new(),
            pattern_type: 3,
            acls: Vec::new(),
        }
    }
}

impl ApiResponse for DescribeAclsResponse {
    type Request = crate::generated::DescribeAclsRequest;
    fn get_api_key() -> ApiKey {
        ApiKey::new(29)
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
        self.throttle_time_ms.encode(buf, version, is_flexible)?;
        self.error_code.encode(buf, version, is_flexible)?;
        self.error_message.encode(buf, version, is_flexible)?;
        self.resources.encode(buf, version, is_flexible)?;
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(version: ApiVer, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let throttle_time_ms = KafkaCodec::decode(buf, version, is_flexible)?;
        let error_code = KafkaCodec::decode(buf, version, is_flexible)?;
        let error_message = KafkaCodec::decode(buf, version, is_flexible)?;
        let resources = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            throttle_time_ms,
            error_code,
            error_message,
            resources,
        })
    }
}
impl KafkaCodec for DescribeAclsResponse {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.throttle_time_ms.encode(buf, version, is_flexible)?;
        self.error_code.encode(buf, version, is_flexible)?;
        self.error_message.encode(buf, version, is_flexible)?;
        self.resources.encode(buf, version, is_flexible)?;
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
        let throttle_time_ms = KafkaCodec::decode(buf, version, is_flexible)?;
        let error_code = KafkaCodec::decode(buf, version, is_flexible)?;
        let error_message = KafkaCodec::decode(buf, version, is_flexible)?;
        let resources = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            throttle_time_ms,
            error_code,
            error_message,
            resources,
        })
    }
}

impl KafkaCodec for AclDescription {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
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
        let principal = KafkaCodec::decode(buf, version, is_flexible)?;
        let host = KafkaCodec::decode(buf, version, is_flexible)?;
        let operation = KafkaCodec::decode(buf, version, is_flexible)?;
        let permission_type = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            principal,
            host,
            operation,
            permission_type,
        })
    }
}

impl KafkaCodec for DescribeAclsResource {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.resource_type.encode(buf, version, is_flexible)?;
        self.resource_name.encode(buf, version, is_flexible)?;
        if 1 <= version.0 {
            self.pattern_type.encode(buf, version, is_flexible)?;
        }
        self.acls.encode(buf, version, is_flexible)?;
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
        let pattern_type = if 1 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            3
        };
        let acls = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            resource_type,
            resource_name,
            pattern_type,
            acls,
        })
    }
}

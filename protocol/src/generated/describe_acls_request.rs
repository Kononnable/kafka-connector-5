#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{KafkaCodec, decode_unsigned_varint, encode_unsigned_varint};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVer, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use indexmap::IndexMap;

// -------------------------------------------------------
// DescribeAclsRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct DescribeAclsRequest {
    /// The resource type.
    pub resource_type_filter: i8,
    /// The resource name, or null to match any resource name.
    pub resource_name_filter: Option<String>,
    /// The resource pattern to match.
    /// Available in version 1+.
    pub pattern_type_filter: i8,
    /// The principal to match, or null to match any principal.
    pub principal_filter: Option<String>,
    /// The host to match, or null to match any host.
    pub host_filter: Option<String>,
    /// The operation to match.
    pub operation: i8,
    /// The permission type to match.
    pub permission_type: i8,
}

impl ApiRequest for DescribeAclsRequest {
    type Response = crate::generated::DescribeAclsResponse;
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
        self.resource_type_filter
            .encode(buf, version, is_flexible)?;
        self.resource_name_filter
            .encode(buf, version, is_flexible)?;
        if 1 <= version.0 {
            self.pattern_type_filter.encode(buf, version, is_flexible)?;
        } else if self.pattern_type_filter != 0 {
            return Err(SerializationError::FieldNotAvailable {
                field: "PatternTypeFilter",
                version,
                api_name: "DescribeAclsRequest",
            });
        }
        self.principal_filter.encode(buf, version, is_flexible)?;
        self.host_filter.encode(buf, version, is_flexible)?;
        self.operation.encode(buf, version, is_flexible)?;
        self.permission_type.encode(buf, version, is_flexible)?;
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(version: ApiVer, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let resource_type_filter = KafkaCodec::decode(buf, version, is_flexible)?;
        let resource_name_filter = KafkaCodec::decode(buf, version, is_flexible)?;
        let pattern_type_filter = if 1 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let principal_filter = KafkaCodec::decode(buf, version, is_flexible)?;
        let host_filter = KafkaCodec::decode(buf, version, is_flexible)?;
        let operation = KafkaCodec::decode(buf, version, is_flexible)?;
        let permission_type = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            resource_type_filter,
            resource_name_filter,
            pattern_type_filter,
            principal_filter,
            host_filter,
            operation,
            permission_type,
        })
    }
}
impl KafkaCodec for DescribeAclsRequest {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.resource_type_filter
            .encode(buf, version, is_flexible)?;
        self.resource_name_filter
            .encode(buf, version, is_flexible)?;
        if 1 <= version.0 {
            self.pattern_type_filter.encode(buf, version, is_flexible)?;
        }
        self.principal_filter.encode(buf, version, is_flexible)?;
        self.host_filter.encode(buf, version, is_flexible)?;
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
        let resource_type_filter = KafkaCodec::decode(buf, version, is_flexible)?;
        let resource_name_filter = KafkaCodec::decode(buf, version, is_flexible)?;
        let pattern_type_filter = if 1 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let principal_filter = KafkaCodec::decode(buf, version, is_flexible)?;
        let host_filter = KafkaCodec::decode(buf, version, is_flexible)?;
        let operation = KafkaCodec::decode(buf, version, is_flexible)?;
        let permission_type = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            resource_type_filter,
            resource_name_filter,
            pattern_type_filter,
            principal_filter,
            host_filter,
            operation,
            permission_type,
        })
    }
}

#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// DeleteAclsRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct DeleteAclsRequest {
    /// The filters to use when deleting ACLs.
    pub filters: Vec<DeleteAclsFilter>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct DeleteAclsFilter {
    /// The resource type.
    pub resource_type_filter: i8,
    /// The resource name.
    pub resource_name_filter: Option<String>,
    /// The pattern type.
    /// Available in version 1+.
    pub pattern_type_filter: i8,
    /// The principal filter, or null to accept all principals.
    pub principal_filter: Option<String>,
    /// The host filter, or null to accept all hosts.
    pub host_filter: Option<String>,
    /// The ACL operation.
    pub operation: i8,
    /// The permission type.
    pub permission_type: i8,
}

impl ApiRequest for DeleteAclsRequest {
    type Response = crate::generated::DeleteAclsResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(31)
    }
    fn get_min_supported_version() -> ApiVersion {
        ApiVersion::new(0)
    }
    fn get_max_supported_version() -> ApiVersion {
        ApiVersion::new(1)
    }
    fn serialize(&self, version: ApiVersion, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            (0) <= version.0 && version.0 <= (1),
            "version {} is not supported by {} (supported: 0-1)",
            version.0,
            stringify!(Self)
        );
        self.filters
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode Filters"))?;
        Ok(())
    }
    fn deserialize(version: ApiVersion, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let filters = <Vec<DeleteAclsFilter> as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode Filters"))?;
        Ok(Self { filters })
    }
}
impl KafkaSerialize for DeleteAclsRequest {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.filters
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Filters".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for DeleteAclsRequest {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let filters = <Vec<DeleteAclsFilter> as KafkaDeserialize>::decode(buf).map_err(|_| {
            DecodeError::Protocol {
                message: "failed to decode Filters".into(),
            }
        })?;
        Ok(Self { filters })
    }
}

impl KafkaSerialize for DeleteAclsFilter {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.resource_type_filter
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ResourceTypeFilter".into(),
            })?;
        self.resource_name_filter
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ResourceNameFilter".into(),
            })?;
        self.pattern_type_filter
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode PatternTypeFilter".into(),
            })?;
        self.principal_filter
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode PrincipalFilter".into(),
            })?;
        self.host_filter
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode HostFilter".into(),
            })?;
        self.operation
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Operation".into(),
            })?;
        self.permission_type
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode PermissionType".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for DeleteAclsFilter {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let resource_type_filter =
            <i8 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ResourceTypeFilter".into(),
            })?;
        let resource_name_filter =
            <Option<String> as KafkaDeserialize>::decode(buf).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode ResourceNameFilter".into(),
                }
            })?;
        let pattern_type_filter =
            <i8 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode PatternTypeFilter".into(),
            })?;
        let principal_filter = <Option<String> as KafkaDeserialize>::decode(buf).map_err(|_| {
            DecodeError::Protocol {
                message: "failed to decode PrincipalFilter".into(),
            }
        })?;
        let host_filter = <Option<String> as KafkaDeserialize>::decode(buf).map_err(|_| {
            DecodeError::Protocol {
                message: "failed to decode HostFilter".into(),
            }
        })?;
        let operation =
            <i8 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode Operation".into(),
            })?;
        let permission_type =
            <i8 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode PermissionType".into(),
            })?;
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

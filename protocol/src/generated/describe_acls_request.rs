#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// DescribeAclsRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct DescribeAclsRequest {
    /// The resource type.
    pub resource_type: i8,
    /// The resource name, or null to match any resource name.
    pub resource_name_filter: Option<String>,
    /// The resource pattern to match.
    /// Available in version 1+.
    pub resource_pattern_type: i8,
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
        self.resource_type
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode ResourceType"))?;
        self.resource_name_filter
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode ResourceNameFilter"))?;
        if (1) <= version.0 {
            self.resource_pattern_type
                .encode(buf)
                .map_err(|_| SerializationError::Encode("failed to encode ResourcePatternType"))?;
        }
        self.principal_filter
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode PrincipalFilter"))?;
        self.host_filter
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode HostFilter"))?;
        self.operation
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode Operation"))?;
        self.permission_type
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode PermissionType"))?;
        Ok(())
    }
    fn deserialize(version: ApiVersion, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let resource_type = <i8 as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode ResourceType"))?;
        let resource_name_filter = <Option<String> as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode ResourceNameFilter"))?;
        let resource_pattern_type = if (1) <= version.0 {
            <i8 as KafkaDeserialize>::decode(buf)
                .map_err(|_| SerializationError::Decode("failed to decode ResourcePatternType"))?
        } else {
            Default::default()
        };
        let principal_filter = <Option<String> as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode PrincipalFilter"))?;
        let host_filter = <Option<String> as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode HostFilter"))?;
        let operation = <i8 as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode Operation"))?;
        let permission_type = <i8 as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode PermissionType"))?;
        Ok(Self {
            resource_type,
            resource_name_filter,
            resource_pattern_type,
            principal_filter,
            host_filter,
            operation,
            permission_type,
        })
    }
}
impl KafkaSerialize for DescribeAclsRequest {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.resource_type
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ResourceType".into(),
            })?;
        self.resource_name_filter
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ResourceNameFilter".into(),
            })?;
        self.resource_pattern_type
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ResourcePatternType".into(),
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

impl KafkaDeserialize for DescribeAclsRequest {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let resource_type =
            <i8 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ResourceType".into(),
            })?;
        let resource_name_filter =
            <Option<String> as KafkaDeserialize>::decode(buf).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode ResourceNameFilter".into(),
                }
            })?;
        let resource_pattern_type =
            <i8 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ResourcePatternType".into(),
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
            resource_type,
            resource_name_filter,
            resource_pattern_type,
            principal_filter,
            host_filter,
            operation,
            permission_type,
        })
    }
}

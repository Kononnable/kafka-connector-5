#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

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
        self.resource_type_filter
            .encode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode ResourceTypeFilter"))?;
        self.resource_name_filter
            .encode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode ResourceNameFilter"))?;
        if (1) <= version.0 {
            self.pattern_type_filter
                .encode(buf, version, is_flexible)
                .map_err(|_| SerializationError::Encode("failed to encode PatternTypeFilter"))?;
        } else if self.pattern_type_filter != 0 {
            return Err(SerializationError::Encode(
                "field 'PatternTypeFilter' is not available in this version",
            ));
        }
        self.principal_filter
            .encode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode PrincipalFilter"))?;
        self.host_filter
            .encode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode HostFilter"))?;
        self.operation
            .encode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode Operation"))?;
        self.permission_type
            .encode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode PermissionType"))?;
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
        let resource_type_filter = <i8 as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode ResourceTypeFilter"))?;
        let resource_name_filter =
            <Option<String> as KafkaDeserialize>::decode(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode ResourceNameFilter"))?;
        let pattern_type_filter = if (1) <= version.0 {
            <i8 as KafkaDeserialize>::decode(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode PatternTypeFilter"))?
        } else {
            Default::default()
        };
        let principal_filter =
            <Option<String> as KafkaDeserialize>::decode(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode PrincipalFilter"))?;
        let host_filter = <Option<String> as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode HostFilter"))?;
        let operation = <i8 as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode Operation"))?;
        let permission_type = <i8 as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode PermissionType"))?;
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
impl KafkaSerialize for DescribeAclsRequest {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.resource_type_filter
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ResourceTypeFilter".into(),
            })?;
        self.resource_name_filter
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ResourceNameFilter".into(),
            })?;
        if (1) <= version.0 {
            self.pattern_type_filter
                .encode(buf, version, is_flexible)
                .map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode PatternTypeFilter".into(),
                })?;
        }
        self.principal_filter
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode PrincipalFilter".into(),
            })?;
        self.host_filter
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode HostFilter".into(),
            })?;
        self.operation
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Operation".into(),
            })?;
        self.permission_type
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode PermissionType".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for DescribeAclsRequest {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        let resource_type_filter = <i8 as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode ResourceTypeFilter".into(),
            })?;
        let resource_name_filter =
            <Option<String> as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode ResourceNameFilter".into(),
                },
            )?;
        let pattern_type_filter = if (1) <= version.0 {
            <i8 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode PatternTypeFilter".into(),
                }
            })?
        } else {
            Default::default()
        };
        let principal_filter =
            <Option<String> as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode PrincipalFilter".into(),
                },
            )?;
        let host_filter = <Option<String> as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
            message: "failed to decode HostFilter".into(),
        })?;
        let operation =
            <i8 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode Operation".into(),
                }
            })?;
        let permission_type =
            <i8 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode PermissionType".into(),
                }
            })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
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

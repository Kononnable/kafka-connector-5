#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

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
        self.creations
            .encode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode Creations"))?;
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
        let creations = <Vec<AclCreation> as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode Creations"))?;
        Ok(Self { creations })
    }
}
impl KafkaSerialize for CreateAclsRequest {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.creations
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Creations".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for CreateAclsRequest {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        let creations = <Vec<AclCreation> as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
            message: "failed to decode Creations".into(),
        })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self { creations })
    }
}

impl KafkaSerialize for AclCreation {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.resource_type
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ResourceType".into(),
            })?;
        self.resource_name
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ResourceName".into(),
            })?;
        if (1) <= version.0 {
            self.resource_pattern_type
                .encode(buf, version, is_flexible)
                .map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode ResourcePatternType".into(),
                })?;
        }
        self.principal
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Principal".into(),
            })?;
        self.host
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Host".into(),
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

impl KafkaDeserialize for AclCreation {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        let resource_type =
            <i8 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode ResourceType".into(),
                }
            })?;
        let resource_name = <String as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode ResourceName".into(),
            })?;
        let resource_pattern_type = if (1) <= version.0 {
            <i8 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode ResourcePatternType".into(),
                }
            })?
        } else {
            Default::default()
        };
        let principal =
            <String as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode Principal".into(),
                }
            })?;
        let host =
            <String as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode Host".into(),
                }
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

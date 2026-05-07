#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// CreateAclsRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct CreateAclsRequest {
    /// The ACLs that we want to create.
    pub creations: Vec<CreatableAcl>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct CreatableAcl {
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
        self.creations
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode Creations"))?;
        Ok(())
    }
    fn deserialize(version: ApiVersion, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let creations = <Vec<CreatableAcl> as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode Creations"))?;
        Ok(Self { creations })
    }
}
impl KafkaSerialize for CreateAclsRequest {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.creations
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Creations".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for CreateAclsRequest {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let creations = <Vec<CreatableAcl> as KafkaDeserialize>::decode(buf).map_err(|_| {
            DecodeError::Protocol {
                message: "failed to decode Creations".into(),
            }
        })?;
        Ok(Self { creations })
    }
}

impl KafkaSerialize for CreatableAcl {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.resource_type
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ResourceType".into(),
            })?;
        self.resource_name
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ResourceName".into(),
            })?;
        self.resource_pattern_type
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ResourcePatternType".into(),
            })?;
        self.principal
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Principal".into(),
            })?;
        self.host
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Host".into(),
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

impl KafkaDeserialize for CreatableAcl {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let resource_type =
            <i8 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ResourceType".into(),
            })?;
        let resource_name =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ResourceName".into(),
            })?;
        let resource_pattern_type =
            <i8 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ResourcePatternType".into(),
            })?;
        let principal =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode Principal".into(),
            })?;
        let host =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode Host".into(),
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
            resource_name,
            resource_pattern_type,
            principal,
            host,
            operation,
            permission_type,
        })
    }
}

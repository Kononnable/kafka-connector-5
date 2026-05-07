#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// IncrementalAlterConfigsRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct IncrementalAlterConfigsRequest {
    /// The incremental updates for each resource.
    pub resources: Vec<AlterConfigsResource>,
    /// True if we should validate the request, but not change the configurations.
    pub validate_only: bool,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct AlterConfigsResource {
    /// The resource type.
    pub resource_type: i8,
    /// The resource name.
    pub resource_name: String,
    /// The configurations.
    pub configs: Vec<AlterableConfig>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct AlterableConfig {
    /// The configuration key name.
    pub name: String,
    /// The type (Set, Delete, Append, Subtract) of operation.
    pub config_operation: i8,
    /// The value to set for the configuration key.
    pub value: Option<String>,
}

impl ApiRequest for IncrementalAlterConfigsRequest {
    type Response = crate::generated::IncrementalAlterConfigsResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(44)
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
        self.resources
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode Resources"))?;
        self.validate_only
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode ValidateOnly"))?;
        Ok(())
    }
    fn deserialize(version: ApiVersion, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let resources = <Vec<AlterConfigsResource> as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode Resources"))?;
        let validate_only = <bool as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode ValidateOnly"))?;
        Ok(Self {
            resources,
            validate_only,
        })
    }
}
impl KafkaSerialize for IncrementalAlterConfigsRequest {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.resources
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Resources".into(),
            })?;
        self.validate_only
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ValidateOnly".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for IncrementalAlterConfigsRequest {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let resources =
            <Vec<AlterConfigsResource> as KafkaDeserialize>::decode(buf).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode Resources".into(),
                }
            })?;
        let validate_only =
            <bool as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ValidateOnly".into(),
            })?;
        Ok(Self {
            resources,
            validate_only,
        })
    }
}

impl KafkaSerialize for AlterConfigsResource {
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
        self.configs
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Configs".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for AlterConfigsResource {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let resource_type =
            <i8 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ResourceType".into(),
            })?;
        let resource_name =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ResourceName".into(),
            })?;
        let configs = <Vec<AlterableConfig> as KafkaDeserialize>::decode(buf).map_err(|_| {
            DecodeError::Protocol {
                message: "failed to decode Configs".into(),
            }
        })?;
        Ok(Self {
            resource_type,
            resource_name,
            configs,
        })
    }
}

impl KafkaSerialize for AlterableConfig {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.name
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Name".into(),
            })?;
        self.config_operation
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ConfigOperation".into(),
            })?;
        self.value
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Value".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for AlterableConfig {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let name =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode Name".into(),
            })?;
        let config_operation =
            <i8 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ConfigOperation".into(),
            })?;
        let value = <Option<String> as KafkaDeserialize>::decode(buf).map_err(|_| {
            DecodeError::Protocol {
                message: "failed to decode Value".into(),
            }
        })?;
        Ok(Self {
            name,
            config_operation,
            value,
        })
    }
}

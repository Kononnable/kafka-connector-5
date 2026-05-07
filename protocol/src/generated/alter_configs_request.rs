#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{EncodeError, DecodeError, KafkaSerialize, KafkaDeserialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// AlterConfigsRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct AlterConfigsRequest {
    /// The updates for each resource.
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
    /// The value to set for the configuration key.
    pub value: Option<String>,
}

impl ApiRequest for AlterConfigsRequest {
    type Response = crate::generated::AlterConfigsResponse;
    fn get_api_key() -> ApiKey { ApiKey::new(33) }
    fn get_min_supported_version() -> crate::traits::ApiVersion { crate::traits::ApiVersion::new(0) }
    fn get_max_supported_version() -> crate::traits::ApiVersion { crate::traits::ApiVersion::new(2) }
    fn serialize(&self, version: crate::traits::ApiVersion, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!((0) <= version.0 && version.0 <= (2), "version {} is not supported by {} (supported: 0-2)", version.0, stringify!(Self));
        let is_flexible = (2) <= version.0;
        self.resources.encode_flexible(buf, is_flexible).map_err(|_| SerializationError::Encode("failed to encode Resources"))?;
        self.validate_only.encode_flexible(buf, is_flexible).map_err(|_| SerializationError::Encode("failed to encode ValidateOnly"))?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(version: crate::traits::ApiVersion, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let is_flexible = (2) <= version.0;
        let resources = <Vec<AlterConfigsResource> as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| SerializationError::Decode("failed to decode Resources"))?;
        let validate_only = <bool as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| SerializationError::Decode("failed to decode ValidateOnly"))?;
        Ok(Self { resources, validate_only })
    }
}
impl KafkaSerialize for AlterConfigsRequest {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.resources.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Resources".into() })?;
        self.validate_only.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode ValidateOnly".into() })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(&self, buf: &mut B, is_flexible: bool) -> Result<(), EncodeError> {
        self.resources.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Resources".into() })?;
        self.validate_only.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode ValidateOnly".into() })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for AlterConfigsRequest {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let resources = <Vec<AlterConfigsResource> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode Resources".into() })?;
        let validate_only = <bool as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode ValidateOnly".into() })?;
        Ok(Self { resources, validate_only })
    }
    fn decode_flexible<B: Buf>(buf: &mut B, is_flexible: bool) -> Result<Self, DecodeError> {
        let resources = <Vec<AlterConfigsResource> as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode Resources".into() })?;
        let validate_only = <bool as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode ValidateOnly".into() })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self { resources, validate_only })
    }
}

impl KafkaSerialize for AlterConfigsResource {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.resource_type.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode ResourceType".into() })?;
        self.resource_name.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode ResourceName".into() })?;
        self.configs.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Configs".into() })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(&self, buf: &mut B, is_flexible: bool) -> Result<(), EncodeError> {
        self.resource_type.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode ResourceType".into() })?;
        self.resource_name.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode ResourceName".into() })?;
        self.configs.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Configs".into() })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for AlterConfigsResource {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let resource_type = <i8 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode ResourceType".into() })?;
        let resource_name = <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode ResourceName".into() })?;
        let configs = <Vec<AlterableConfig> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode Configs".into() })?;
        Ok(Self { resource_type, resource_name, configs })
    }
    fn decode_flexible<B: Buf>(buf: &mut B, is_flexible: bool) -> Result<Self, DecodeError> {
        let resource_type = <i8 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode ResourceType".into() })?;
        let resource_name = <String as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode ResourceName".into() })?;
        let configs = <Vec<AlterableConfig> as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode Configs".into() })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self { resource_type, resource_name, configs })
    }
}

impl KafkaSerialize for AlterableConfig {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.name.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Name".into() })?;
        self.value.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Value".into() })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(&self, buf: &mut B, is_flexible: bool) -> Result<(), EncodeError> {
        self.name.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Name".into() })?;
        if is_flexible {
            if let Some(ref __val) = self.value {
                crate::protocol::serialization::encode_unsigned_varint(1u64, buf);
                __val.encode_flexible(buf, true).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Value".into() })?;
            } else {
                crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
            }
        } else {
            if let Some(ref __val) = self.value {
                __val.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Value".into() })?;
            }
        }
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for AlterableConfig {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let name = <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode Name".into() })?;
        let value = <Option<String> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode Value".into() })?;
        Ok(Self { name, value })
    }
    fn decode_flexible<B: Buf>(buf: &mut B, is_flexible: bool) -> Result<Self, DecodeError> {
        let name = <String as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode Name".into() })?;
        let value = if is_flexible {
            let (__present, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
            if __present == 0 {
                None
            } else {
                Some(<String as KafkaDeserialize>::decode_flexible(buf, true).map_err(|_| DecodeError::Protocol { message: "failed to decode Value".into() })?)
            }
        } else {
            <Option<String> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode Value".into() })?
        };
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self { name, value })
    }
}


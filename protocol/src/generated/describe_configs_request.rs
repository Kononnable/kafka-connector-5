#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// DescribeConfigsRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct DescribeConfigsRequest {
    /// The resources whose configurations we want to describe.
    pub resources: Vec<DescribeConfigsResource>,
    /// True if we should include all synonyms.
    /// Available in version 1+.
    pub include_synonyms: bool,
    /// True if we should include configuration documentation.
    /// Available in version 3+.
    pub include_documentation: bool,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct DescribeConfigsResource {
    /// The resource type.
    pub resource_type: i8,
    /// The resource name.
    pub resource_name: String,
    /// The configuration keys to list, or null to list all configuration keys.
    pub configuration_keys: Option<Vec<String>>,
}

impl ApiRequest for DescribeConfigsRequest {
    type Response = crate::generated::DescribeConfigsResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(32)
    }
    fn get_min_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(1)
    }
    fn get_max_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(4)
    }
    fn get_min_flexible_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(4)
    }
    fn serialize(
        &self,
        version: crate::traits::ApiVersion,
        buf: &mut BytesMut,
    ) -> Result<(), SerializationError> {
        assert!(
            (1) <= version.0 && version.0 <= (4),
            "version {} is not supported by {} (supported: 1-4)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        self.resources
            .encode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode Resources"))?;
        if (1) <= version.0 {
            self.include_synonyms
                .encode(buf, version, is_flexible)
                .map_err(|_| SerializationError::Encode("failed to encode IncludeSynonyms"))?;
        } else if self.include_synonyms {
            return Err(SerializationError::Encode(
                "field 'IncludeSynonyms' is not available in this version",
            ));
        }
        if (3) <= version.0 {
            self.include_documentation
                .encode(buf, version, is_flexible)
                .map_err(|_| SerializationError::Encode("failed to encode IncludeDocumentation"))?;
        } else if self.include_documentation {
            return Err(SerializationError::Encode(
                "field 'IncludeDocumentation' is not available in this version",
            ));
        }
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
        let resources =
            <Vec<DescribeConfigsResource> as KafkaDeserialize>::decode(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode Resources"))?;
        let include_synonyms = if (1) <= version.0 {
            <bool as KafkaDeserialize>::decode(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode IncludeSynonyms"))?
        } else {
            Default::default()
        };
        let include_documentation = if (3) <= version.0 {
            <bool as KafkaDeserialize>::decode(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode IncludeDocumentation"))?
        } else {
            Default::default()
        };
        Ok(Self {
            resources,
            include_synonyms,
            include_documentation,
        })
    }
}
impl KafkaSerialize for DescribeConfigsRequest {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.resources
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Resources".into(),
            })?;
        if (1) <= version.0 {
            self.include_synonyms
                .encode(buf, version, is_flexible)
                .map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode IncludeSynonyms".into(),
                })?;
        }
        if (3) <= version.0 {
            self.include_documentation
                .encode(buf, version, is_flexible)
                .map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode IncludeDocumentation".into(),
                })?;
        }
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for DescribeConfigsRequest {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        let resources =
            <Vec<DescribeConfigsResource> as KafkaDeserialize>::decode(buf, version, is_flexible)
                .map_err(|_| DecodeError::Protocol {
                message: "failed to decode Resources".into(),
            })?;
        let include_synonyms = if (1) <= version.0 {
            <bool as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode IncludeSynonyms".into(),
                }
            })?
        } else {
            Default::default()
        };
        let include_documentation = if (3) <= version.0 {
            <bool as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode IncludeDocumentation".into(),
                }
            })?
        } else {
            Default::default()
        };
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            resources,
            include_synonyms,
            include_documentation,
        })
    }
}

impl KafkaSerialize for DescribeConfigsResource {
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
        self.configuration_keys
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ConfigurationKeys".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for DescribeConfigsResource {
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
        let configuration_keys =
            <Option<Vec<String>> as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode ConfigurationKeys".into(),
                },
            )?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            resource_type,
            resource_name,
            configuration_keys,
        })
    }
}

#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// DescribeConfigsResponse
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct DescribeConfigsResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// The results for each resource.
    pub results: Vec<DescribeConfigsResult>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct DescribeConfigsResourceResult {
    /// The configuration name.
    pub name: String,
    /// The configuration value.
    pub value: Option<String>,
    /// True if the configuration is read-only.
    pub read_only: bool,
    /// True if the configuration is not set.
    /// Available in version 0.
    pub is_default: bool,
    /// The configuration source.
    /// Available in version 1+.
    pub config_source: i8,
    /// True if this configuration is sensitive.
    pub is_sensitive: bool,
    /// The synonyms for this configuration key.
    /// Available in version 1+.
    pub synonyms: Vec<DescribeConfigsSynonym>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct DescribeConfigsResult {
    /// The error code, or 0 if we were able to successfully describe the configurations.
    pub error_code: i16,
    /// The error message, or null if we were able to successfully describe the configurations.
    pub error_message: Option<String>,
    /// The resource type.
    pub resource_type: i8,
    /// The resource name.
    pub resource_name: String,
    /// Each listed configuration.
    pub configs: Vec<DescribeConfigsResourceResult>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct DescribeConfigsSynonym {
    /// The synonym name.
    /// Available in version 1+.
    pub name: String,
    /// The synonym value.
    /// Available in version 1+.
    pub value: Option<String>,
    /// The synonym source.
    /// Available in version 1+.
    pub source: i8,
}

impl ApiResponse for DescribeConfigsResponse {
    type Request = crate::generated::DescribeConfigsRequest;
    fn get_api_key() -> ApiKey {
        ApiKey::new(32)
    }
    fn get_min_supported_version() -> ApiVersion {
        ApiVersion::new(0)
    }
    fn get_max_supported_version() -> ApiVersion {
        ApiVersion::new(2)
    }
    fn serialize(&self, version: ApiVersion, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            (0) <= version.0 && version.0 <= (2),
            "version {} is not supported by {} (supported: 0-2)",
            version.0,
            stringify!(Self)
        );
        self.throttle_time_ms
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode ThrottleTimeMs"))?;
        self.results
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode Results"))?;
        Ok(())
    }
    fn deserialize(version: ApiVersion, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let throttle_time_ms = <i32 as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode ThrottleTimeMs"))?;
        let results = <Vec<DescribeConfigsResult> as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode Results"))?;
        Ok(Self {
            throttle_time_ms,
            results,
        })
    }
}
impl KafkaSerialize for DescribeConfigsResponse {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.throttle_time_ms
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ThrottleTimeMs".into(),
            })?;
        self.results
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Results".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for DescribeConfigsResponse {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let throttle_time_ms =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ThrottleTimeMs".into(),
            })?;
        let results =
            <Vec<DescribeConfigsResult> as KafkaDeserialize>::decode(buf).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode Results".into(),
                }
            })?;
        Ok(Self {
            throttle_time_ms,
            results,
        })
    }
}

impl KafkaSerialize for DescribeConfigsResourceResult {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.name
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Name".into(),
            })?;
        self.value
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Value".into(),
            })?;
        self.read_only
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ReadOnly".into(),
            })?;
        self.is_default
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode IsDefault".into(),
            })?;
        self.config_source
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ConfigSource".into(),
            })?;
        self.is_sensitive
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode IsSensitive".into(),
            })?;
        self.synonyms
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Synonyms".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for DescribeConfigsResourceResult {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let name =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode Name".into(),
            })?;
        let value = <Option<String> as KafkaDeserialize>::decode(buf).map_err(|_| {
            DecodeError::Protocol {
                message: "failed to decode Value".into(),
            }
        })?;
        let read_only =
            <bool as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ReadOnly".into(),
            })?;
        let is_default =
            <bool as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode IsDefault".into(),
            })?;
        let config_source =
            <i8 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ConfigSource".into(),
            })?;
        let is_sensitive =
            <bool as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode IsSensitive".into(),
            })?;
        let synonyms =
            <Vec<DescribeConfigsSynonym> as KafkaDeserialize>::decode(buf).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode Synonyms".into(),
                }
            })?;
        Ok(Self {
            name,
            value,
            read_only,
            is_default,
            config_source,
            is_sensitive,
            synonyms,
        })
    }
}

impl KafkaSerialize for DescribeConfigsResult {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.error_code
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ErrorCode".into(),
            })?;
        self.error_message
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ErrorMessage".into(),
            })?;
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

impl KafkaDeserialize for DescribeConfigsResult {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let error_code =
            <i16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ErrorCode".into(),
            })?;
        let error_message = <Option<String> as KafkaDeserialize>::decode(buf).map_err(|_| {
            DecodeError::Protocol {
                message: "failed to decode ErrorMessage".into(),
            }
        })?;
        let resource_type =
            <i8 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ResourceType".into(),
            })?;
        let resource_name =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ResourceName".into(),
            })?;
        let configs = <Vec<DescribeConfigsResourceResult> as KafkaDeserialize>::decode(buf)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode Configs".into(),
            })?;
        Ok(Self {
            error_code,
            error_message,
            resource_type,
            resource_name,
            configs,
        })
    }
}

impl KafkaSerialize for DescribeConfigsSynonym {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.name
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Name".into(),
            })?;
        self.value
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Value".into(),
            })?;
        self.source
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Source".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for DescribeConfigsSynonym {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let name =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode Name".into(),
            })?;
        let value = <Option<String> as KafkaDeserialize>::decode(buf).map_err(|_| {
            DecodeError::Protocol {
                message: "failed to decode Value".into(),
            }
        })?;
        let source = <i8 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
            message: "failed to decode Source".into(),
        })?;
        Ok(Self {
            name,
            value,
            source,
        })
    }
}

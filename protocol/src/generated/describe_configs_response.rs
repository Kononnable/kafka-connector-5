#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{KafkaCodec, decode_unsigned_varint, encode_unsigned_varint};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVer, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use indexmap::IndexMap;

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
    /// The configuration source.
    /// Available in version 1+.
    pub config_source: i8,
    /// True if this configuration is sensitive.
    pub is_sensitive: bool,
    /// The synonyms for this configuration key.
    /// Available in version 1+.
    pub synonyms: Vec<DescribeConfigsSynonym>,
    /// The configuration data type. Type can be one of the following values - BOOLEAN, STRING, INT, SHORT, LONG, DOUBLE, LIST, CLASS, PASSWORD.
    /// Available in version 3+.
    pub config_type: i8,
    /// The configuration documentation.
    /// Available in version 3+.
    pub documentation: Option<String>,
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
    fn get_min_supported_version() -> ApiVer {
        ApiVer::new(1)
    }
    fn get_max_supported_version() -> ApiVer {
        ApiVer::new(4)
    }
    fn get_min_flexible_version() -> ApiVer {
        ApiVer::new(4)
    }
    fn serialize(&self, version: ApiVer, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            1 <= version.0 && version.0 <= 4,
            "version {} is not supported by {} (supported: 1-4)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        self.throttle_time_ms.encode(buf, version, is_flexible)?;
        self.results.encode(buf, version, is_flexible)?;
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(version: ApiVer, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let throttle_time_ms = KafkaCodec::decode(buf, version, is_flexible)?;
        let results = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            throttle_time_ms,
            results,
        })
    }
}
impl KafkaCodec for DescribeConfigsResponse {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.throttle_time_ms.encode(buf, version, is_flexible)?;
        self.results.encode(buf, version, is_flexible)?;
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
        let throttle_time_ms = KafkaCodec::decode(buf, version, is_flexible)?;
        let results = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            throttle_time_ms,
            results,
        })
    }
}

impl KafkaCodec for DescribeConfigsResourceResult {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.name.encode(buf, version, is_flexible)?;
        self.value.encode(buf, version, is_flexible)?;
        self.read_only.encode(buf, version, is_flexible)?;
        if 1 <= version.0 {
            self.config_source.encode(buf, version, is_flexible)?;
        }
        self.is_sensitive.encode(buf, version, is_flexible)?;
        if 1 <= version.0 {
            self.synonyms.encode(buf, version, is_flexible)?;
        }
        if 3 <= version.0 {
            self.config_type.encode(buf, version, is_flexible)?;
        }
        if 3 <= version.0 {
            self.documentation.encode(buf, version, is_flexible)?;
        }
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
        let name = KafkaCodec::decode(buf, version, is_flexible)?;
        let value = KafkaCodec::decode(buf, version, is_flexible)?;
        let read_only = KafkaCodec::decode(buf, version, is_flexible)?;
        let config_source = if 1 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let is_sensitive = KafkaCodec::decode(buf, version, is_flexible)?;
        let synonyms = if 1 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let config_type = if 3 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let documentation = if 3 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            name,
            value,
            read_only,
            config_source,
            is_sensitive,
            synonyms,
            config_type,
            documentation,
        })
    }
}

impl KafkaCodec for DescribeConfigsResult {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.error_code.encode(buf, version, is_flexible)?;
        self.error_message.encode(buf, version, is_flexible)?;
        self.resource_type.encode(buf, version, is_flexible)?;
        self.resource_name.encode(buf, version, is_flexible)?;
        self.configs.encode(buf, version, is_flexible)?;
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
        let error_code = KafkaCodec::decode(buf, version, is_flexible)?;
        let error_message = KafkaCodec::decode(buf, version, is_flexible)?;
        let resource_type = KafkaCodec::decode(buf, version, is_flexible)?;
        let resource_name = KafkaCodec::decode(buf, version, is_flexible)?;
        let configs = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            error_code,
            error_message,
            resource_type,
            resource_name,
            configs,
        })
    }
}

impl KafkaCodec for DescribeConfigsSynonym {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        if 1 <= version.0 {
            self.name.encode(buf, version, is_flexible)?;
        }
        if 1 <= version.0 {
            self.value.encode(buf, version, is_flexible)?;
        }
        if 1 <= version.0 {
            self.source.encode(buf, version, is_flexible)?;
        }
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
        let name = if 1 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let value = if 1 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let source = if 1 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            name,
            value,
            source,
        })
    }
}

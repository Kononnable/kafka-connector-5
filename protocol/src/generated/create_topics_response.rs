#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{KafkaCodec, decode_unsigned_varint, encode_unsigned_varint};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVer, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use indexmap::IndexMap;

// -------------------------------------------------------
// CreateTopicsResponse
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct CreateTopicsResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    /// Available in version 2+.
    pub throttle_time_ms: i32,
    /// Results for each topic we tried to create.
    /// IndexMap key `Name` (string): The topic name.
    pub topics: IndexMap<String, CreatableTopicResult>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct CreatableTopicConfigs {
    /// The configuration name.
    /// Available in version 5+.
    pub name: String,
    /// The configuration value.
    /// Available in version 5+.
    pub value: Option<String>,
    /// True if the configuration is read-only.
    /// Available in version 5+.
    pub read_only: bool,
    /// The configuration source.
    /// Available in version 5+.
    pub config_source: i8,
    /// True if this configuration is sensitive.
    /// Available in version 5+.
    pub is_sensitive: bool,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct CreatableTopicResult {
    /// The unique topic ID.
    /// Available in version 7+.
    pub topic_id: [u8; 16],
    /// The error code, or 0 if there was no error.
    pub error_code: i16,
    /// The error message, or null if there was no error.
    /// Available in version 1+.
    pub error_message: Option<String>,
    /// Optional topic config error returned if configs are not returned in the response.
    /// Available in version 5+.
    pub topic_config_error_code: i16,
    /// Number of partitions of the topic.
    /// Available in version 5+.
    pub num_partitions: i32,
    /// Replication factor of the topic.
    /// Available in version 5+.
    pub replication_factor: i16,
    /// Configuration of the topic.
    /// Available in version 5+.
    pub configs: Option<Vec<CreatableTopicConfigs>>,
}

impl ApiResponse for CreateTopicsResponse {
    type Request = crate::generated::CreateTopicsRequest;
    fn get_api_key() -> ApiKey {
        ApiKey::new(19)
    }
    fn get_min_supported_version() -> ApiVer {
        ApiVer::new(2)
    }
    fn get_max_supported_version() -> ApiVer {
        ApiVer::new(7)
    }
    fn get_min_flexible_version() -> ApiVer {
        ApiVer::new(5)
    }
    fn serialize(&self, version: ApiVer, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            2 <= version.0 && version.0 <= 7,
            "version {} is not supported by {} (supported: 2-7)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        if 2 <= version.0 {
            self.throttle_time_ms.encode(buf, version, is_flexible)?;
        } else if self.throttle_time_ms != 0 {
            return Err(SerializationError::FieldNotAvailable {
                field: "ThrottleTimeMs",
                version,
                api_name: "CreateTopicsResponse",
            });
        }
        self.topics.encode(buf, version, is_flexible)?;
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(version: ApiVer, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let throttle_time_ms = if 2 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let topics = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            throttle_time_ms,
            topics,
        })
    }
}
impl KafkaCodec for CreateTopicsResponse {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        if 2 <= version.0 {
            self.throttle_time_ms.encode(buf, version, is_flexible)?;
        }
        self.topics.encode(buf, version, is_flexible)?;
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
        let throttle_time_ms = if 2 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let topics = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            throttle_time_ms,
            topics,
        })
    }
}

impl KafkaCodec for CreatableTopicConfigs {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        if 5 <= version.0 {
            self.name.encode(buf, version, is_flexible)?;
        }
        if 5 <= version.0 {
            self.value.encode(buf, version, is_flexible)?;
        }
        if 5 <= version.0 {
            self.read_only.encode(buf, version, is_flexible)?;
        }
        if 5 <= version.0 {
            self.config_source.encode(buf, version, is_flexible)?;
        }
        if 5 <= version.0 {
            self.is_sensitive.encode(buf, version, is_flexible)?;
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
        let name = if 5 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let value = if 5 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let read_only = if 5 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let config_source = if 5 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let is_sensitive = if 5 <= version.0 {
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
        })
    }
}

impl KafkaCodec for CreatableTopicResult {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        if 7 <= version.0 {
            self.topic_id.encode(buf, version, is_flexible)?;
        }
        self.error_code.encode(buf, version, is_flexible)?;
        if 1 <= version.0 {
            self.error_message.encode(buf, version, is_flexible)?;
        }
        if 5 <= version.0 && !is_flexible {
            self.topic_config_error_code
                .encode(buf, version, is_flexible)?;
        }
        if 5 <= version.0 {
            self.num_partitions.encode(buf, version, is_flexible)?;
        }
        if 5 <= version.0 {
            self.replication_factor.encode(buf, version, is_flexible)?;
        }
        if 5 <= version.0 {
            self.configs.encode(buf, version, is_flexible)?;
        }
        if is_flexible {
            let mut tag_count = 0u64;
            if self.topic_config_error_code != 0 {
                tag_count += 1;
            }
            encode_unsigned_varint(tag_count, buf);
            if self.topic_config_error_code != 0 {
                encode_unsigned_varint(0u64, buf);
                let mut tmp_buf = bytes::BytesMut::new();
                self.topic_config_error_code
                    .encode(&mut tmp_buf, version, true)?;
                encode_unsigned_varint(tmp_buf.len() as u64, buf);
                buf.put_slice(&tmp_buf);
            }
        }
        Ok(())
    }

    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let topic_id = if 7 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let error_code = KafkaCodec::decode(buf, version, is_flexible)?;
        let error_message = if 1 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let mut topic_config_error_code = if 5 <= version.0 {
            if is_flexible {
                Default::default()
            } else {
                KafkaCodec::decode(buf, version, is_flexible)?
            }
        } else {
            Default::default()
        };
        let num_partitions = if 5 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let replication_factor = if 5 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let configs = if 5 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        if is_flexible {
            let (tag_count, _) = decode_unsigned_varint(buf)?;
            for _ in 0..tag_count {
                let (__tag_id, _) = decode_unsigned_varint(buf)?;
                let (__tag_len, _) = decode_unsigned_varint(buf)?;
                match __tag_id {
                    0 => {
                        topic_config_error_code = KafkaCodec::decode(buf, version, true)?;
                    }
                    _ => {
                        buf.advance(__tag_len as usize);
                    }
                }
            }
        }
        Ok(Self {
            topic_id,
            error_code,
            error_message,
            topic_config_error_code,
            num_partitions,
            replication_factor,
            configs,
        })
    }
}

#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{EncodeError, DecodeError, KafkaSerialize, KafkaDeserialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// CreateTopicsResponse
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct CreateTopicsResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    /// Available in version 2+.
    pub throttle_time_ms: i32,
    /// Results for each topic we tried to create.
    pub topics: Vec<CreatableTopicResult>,
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
    /// The topic name.
    pub name: String,
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
    fn get_api_key() -> ApiKey { ApiKey::new(19) }
    fn get_min_supported_version() -> crate::traits::ApiVersion { crate::traits::ApiVersion::new(2) }
    fn get_max_supported_version() -> crate::traits::ApiVersion { crate::traits::ApiVersion::new(7) }
    fn serialize(&self, version: crate::traits::ApiVersion, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!((2) <= version.0 && version.0 <= (7), "version {} is not supported by {} (supported: 2-7)", version.0, stringify!(Self));
        let is_flexible = (5) <= version.0;
        if (2) <= version.0 {
            self.throttle_time_ms.encode_flexible(buf, is_flexible).map_err(|_| SerializationError::Encode("failed to encode ThrottleTimeMs"))?;
        }
        self.topics.encode_flexible(buf, is_flexible).map_err(|_| SerializationError::Encode("failed to encode Topics"))?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(version: crate::traits::ApiVersion, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let is_flexible = (5) <= version.0;
        let throttle_time_ms = if (2) <= version.0 {
            <i32 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| SerializationError::Decode("failed to decode ThrottleTimeMs"))?
        } else {
            Default::default()
        };
        let topics = <Vec<CreatableTopicResult> as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| SerializationError::Decode("failed to decode Topics"))?;
        Ok(Self { throttle_time_ms, topics })
    }
}
impl KafkaSerialize for CreateTopicsResponse {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.throttle_time_ms.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode ThrottleTimeMs".into() })?;
        self.topics.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Topics".into() })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(&self, buf: &mut B, is_flexible: bool) -> Result<(), EncodeError> {
        self.throttle_time_ms.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode ThrottleTimeMs".into() })?;
        self.topics.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Topics".into() })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for CreateTopicsResponse {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let throttle_time_ms = <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode ThrottleTimeMs".into() })?;
        let topics = <Vec<CreatableTopicResult> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode Topics".into() })?;
        Ok(Self { throttle_time_ms, topics })
    }
    fn decode_flexible<B: Buf>(buf: &mut B, is_flexible: bool) -> Result<Self, DecodeError> {
        let throttle_time_ms = <i32 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode ThrottleTimeMs".into() })?;
        let topics = <Vec<CreatableTopicResult> as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode Topics".into() })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self { throttle_time_ms, topics })
    }
}

impl KafkaSerialize for CreatableTopicConfigs {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.name.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Name".into() })?;
        self.value.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Value".into() })?;
        self.read_only.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode ReadOnly".into() })?;
        self.config_source.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode ConfigSource".into() })?;
        self.is_sensitive.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode IsSensitive".into() })?;
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
        self.read_only.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode ReadOnly".into() })?;
        self.config_source.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode ConfigSource".into() })?;
        self.is_sensitive.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode IsSensitive".into() })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for CreatableTopicConfigs {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let name = <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode Name".into() })?;
        let value = <Option<String> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode Value".into() })?;
        let read_only = <bool as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode ReadOnly".into() })?;
        let config_source = <i8 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode ConfigSource".into() })?;
        let is_sensitive = <bool as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode IsSensitive".into() })?;
        Ok(Self { name, value, read_only, config_source, is_sensitive })
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
        let read_only = <bool as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode ReadOnly".into() })?;
        let config_source = <i8 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode ConfigSource".into() })?;
        let is_sensitive = <bool as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode IsSensitive".into() })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self { name, value, read_only, config_source, is_sensitive })
    }
}

impl KafkaSerialize for CreatableTopicResult {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.name.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Name".into() })?;
        self.topic_id.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode TopicId".into() })?;
        self.error_code.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode ErrorCode".into() })?;
        self.error_message.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode ErrorMessage".into() })?;
        self.topic_config_error_code.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode TopicConfigErrorCode".into() })?;
        self.num_partitions.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode NumPartitions".into() })?;
        self.replication_factor.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode ReplicationFactor".into() })?;
        self.configs.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Configs".into() })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(&self, buf: &mut B, is_flexible: bool) -> Result<(), EncodeError> {
        self.name.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Name".into() })?;
        self.topic_id.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode TopicId".into() })?;
        self.error_code.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode ErrorCode".into() })?;
        if is_flexible {
            if let Some(ref __val) = self.error_message {
                crate::protocol::serialization::encode_unsigned_varint(1u64, buf);
                __val.encode_flexible(buf, true).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode ErrorMessage".into() })?;
            } else {
                crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
            }
        } else {
            if let Some(ref __val) = self.error_message {
                __val.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode ErrorMessage".into() })?;
            }
        }
        self.topic_config_error_code.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode TopicConfigErrorCode".into() })?;
        self.num_partitions.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode NumPartitions".into() })?;
        self.replication_factor.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode ReplicationFactor".into() })?;
        if is_flexible {
            if let Some(ref __val) = self.configs {
                crate::protocol::serialization::encode_unsigned_varint(1u64, buf);
                __val.encode_flexible(buf, true).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Configs".into() })?;
            } else {
                crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
            }
        } else {
            if let Some(ref __val) = self.configs {
                __val.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Configs".into() })?;
            }
        }
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for CreatableTopicResult {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let name = <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode Name".into() })?;
        let topic_id = <[u8; 16] as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode TopicId".into() })?;
        let error_code = <i16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode ErrorCode".into() })?;
        let error_message = <Option<String> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode ErrorMessage".into() })?;
        let topic_config_error_code = <i16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode TopicConfigErrorCode".into() })?;
        let num_partitions = <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode NumPartitions".into() })?;
        let replication_factor = <i16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode ReplicationFactor".into() })?;
        let configs = <Option<Vec<CreatableTopicConfigs>> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode Configs".into() })?;
        Ok(Self { name, topic_id, error_code, error_message, topic_config_error_code, num_partitions, replication_factor, configs })
    }
    fn decode_flexible<B: Buf>(buf: &mut B, is_flexible: bool) -> Result<Self, DecodeError> {
        let name = <String as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode Name".into() })?;
        let topic_id = <[u8; 16] as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode TopicId".into() })?;
        let error_code = <i16 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode ErrorCode".into() })?;
        let error_message = if is_flexible {
            let (__present, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
            if __present == 0 {
                None
            } else {
                Some(<String as KafkaDeserialize>::decode_flexible(buf, true).map_err(|_| DecodeError::Protocol { message: "failed to decode ErrorMessage".into() })?)
            }
        } else {
            <Option<String> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode ErrorMessage".into() })?
        };
        let topic_config_error_code = <i16 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode TopicConfigErrorCode".into() })?;
        let num_partitions = <i32 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode NumPartitions".into() })?;
        let replication_factor = <i16 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode ReplicationFactor".into() })?;
        let configs = if is_flexible {
            let (__present, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
            if __present == 0 {
                None
            } else {
                Some(<Vec<CreatableTopicConfigs> as KafkaDeserialize>::decode_flexible(buf, true).map_err(|_| DecodeError::Protocol { message: "failed to decode Configs".into() })?)
            }
        } else {
            <Option<Vec<CreatableTopicConfigs>> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode Configs".into() })?
        };
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self { name, topic_id, error_code, error_message, topic_config_error_code, num_partitions, replication_factor, configs })
    }
}


#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// DeleteTopicsResponse
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct DeleteTopicsResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    /// Available in version 1+.
    pub throttle_time_ms: i32,
    /// The results for each topic we tried to delete.
    pub responses: Vec<DeletableTopicResult>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct DeletableTopicResult {
    /// The topic name.
    pub name: Option<String>,
    /// The unique topic ID.
    /// Available in version 6+.
    pub topic_id: [u8; 16],
    /// The deletion error, or 0 if the deletion succeeded.
    pub error_code: i16,
    /// The error message, or null if there was no error.
    /// Available in version 5+.
    pub error_message: Option<String>,
}

impl ApiResponse for DeleteTopicsResponse {
    type Request = crate::generated::DeleteTopicsRequest;
    fn get_api_key() -> ApiKey {
        ApiKey::new(20)
    }
    fn get_min_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(1)
    }
    fn get_max_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(6)
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
            (1) <= version.0 && version.0 <= (6),
            "version {} is not supported by {} (supported: 1-6)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        if (1) <= version.0 {
            self.throttle_time_ms
                .encode(buf, version, is_flexible)
                .map_err(|_| SerializationError::Encode("failed to encode ThrottleTimeMs"))?;
        } else if self.throttle_time_ms != 0 {
            return Err(SerializationError::Encode(
                "field 'ThrottleTimeMs' is not available in this version",
            ));
        }
        self.responses
            .encode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode Responses"))?;
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
        let throttle_time_ms = if (1) <= version.0 {
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode ThrottleTimeMs"))?
        } else {
            Default::default()
        };
        let responses =
            <Vec<DeletableTopicResult> as KafkaDeserialize>::decode(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode Responses"))?;
        Ok(Self {
            throttle_time_ms,
            responses,
        })
    }
}
impl KafkaSerialize for DeleteTopicsResponse {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        if (1) <= version.0 {
            self.throttle_time_ms
                .encode(buf, version, is_flexible)
                .map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode ThrottleTimeMs".into(),
                })?;
        }
        self.responses
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Responses".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for DeleteTopicsResponse {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        let throttle_time_ms = if (1) <= version.0 {
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode ThrottleTimeMs".into(),
                }
            })?
        } else {
            Default::default()
        };
        let responses =
            <Vec<DeletableTopicResult> as KafkaDeserialize>::decode(buf, version, is_flexible)
                .map_err(|_| DecodeError::Protocol {
                    message: "failed to decode Responses".into(),
                })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            throttle_time_ms,
            responses,
        })
    }
}

impl KafkaSerialize for DeletableTopicResult {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.name
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Name".into(),
            })?;
        if (6) <= version.0 {
            self.topic_id
                .encode(buf, version, is_flexible)
                .map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode TopicId".into(),
                })?;
        }
        self.error_code
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ErrorCode".into(),
            })?;
        if (5) <= version.0 {
            self.error_message
                .encode(buf, version, is_flexible)
                .map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode ErrorMessage".into(),
                })?;
        }
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for DeletableTopicResult {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        let name = <Option<String> as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode Name".into(),
            })?;
        let topic_id = if (6) <= version.0 {
            <[u8; 16] as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode TopicId".into(),
                }
            })?
        } else {
            Default::default()
        };
        let error_code =
            <i16 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode ErrorCode".into(),
                }
            })?;
        let error_message = if (5) <= version.0 {
            <Option<String> as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode ErrorMessage".into(),
                },
            )?
        } else {
            Default::default()
        };
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            name,
            topic_id,
            error_code,
            error_message,
        })
    }
}

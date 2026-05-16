#![allow(unused_imports, unused_variables)]
use bytes::{Buf, BufMut, Bytes, BytesMut};
use indexmap::IndexMap;

use crate::protocol::serialization::{KafkaCodec, decode_unsigned_varint, encode_unsigned_varint};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVer, SerializationError};

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
    fn get_min_supported_version() -> ApiVer {
        ApiVer::new(1)
    }
    fn get_max_supported_version() -> ApiVer {
        ApiVer::new(6)
    }
    fn get_min_flexible_version() -> ApiVer {
        ApiVer::new(4)
    }
    fn serialize(&self, version: ApiVer, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            1 <= version.0 && version.0 <= 6,
            "version {} is not supported by {} (supported: 1-6)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        if 1 <= version.0 {
            self.throttle_time_ms.encode(buf, version, is_flexible)?;
        } else if self.throttle_time_ms != 0 {
            return Err(SerializationError::FieldNotAvailable {
                field: "ThrottleTimeMs",
                version,
                api_name: "DeleteTopicsResponse",
            });
        }
        self.responses.encode(buf, version, is_flexible)?;
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(version: ApiVer, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let throttle_time_ms = if 1 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            0
        };
        let responses = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            throttle_time_ms,
            responses,
        })
    }
}
impl KafkaCodec for DeleteTopicsResponse {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        if 1 <= version.0 {
            self.throttle_time_ms.encode(buf, version, is_flexible)?;
        }
        self.responses.encode(buf, version, is_flexible)?;
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
        let throttle_time_ms = if 1 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            0
        };
        let responses = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            throttle_time_ms,
            responses,
        })
    }
}

impl KafkaCodec for DeletableTopicResult {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.name.encode(buf, version, is_flexible)?;
        if 6 <= version.0 {
            self.topic_id.encode(buf, version, is_flexible)?;
        }
        self.error_code.encode(buf, version, is_flexible)?;
        if 5 <= version.0 {
            self.error_message.encode(buf, version, is_flexible)?;
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
        let topic_id = if 6 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            [0u8; 16]
        };
        let error_code = KafkaCodec::decode(buf, version, is_flexible)?;
        let error_message = if 5 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            None
        };
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            name,
            topic_id,
            error_code,
            error_message,
        })
    }
}

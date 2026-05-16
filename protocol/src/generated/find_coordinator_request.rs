#![allow(unused_imports, unused_variables)]
use bytes::{Buf, BufMut, Bytes, BytesMut};
use indexmap::IndexMap;

use crate::protocol::serialization::{KafkaCodec, decode_unsigned_varint, encode_unsigned_varint};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVer, SerializationError};

// -------------------------------------------------------
// FindCoordinatorRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct FindCoordinatorRequest {
    /// The coordinator key.
    /// Available in version 0-3.
    pub key: String,
    /// The coordinator key type. (group, transaction, share).
    /// Available in version 1+.
    pub key_type: i8,
    /// The coordinator keys.
    /// Available in version 4+.
    pub coordinator_keys: Vec<String>,
}

impl ApiRequest for FindCoordinatorRequest {
    type Response = crate::generated::FindCoordinatorResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(10)
    }
    fn get_min_supported_version() -> ApiVer {
        ApiVer::new(0)
    }
    fn get_max_supported_version() -> ApiVer {
        ApiVer::new(6)
    }
    fn get_min_flexible_version() -> ApiVer {
        ApiVer::new(3)
    }
    fn serialize(&self, version: ApiVer, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            0 <= version.0 && version.0 <= 6,
            "version {} is not supported by {} (supported: 0-6)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        if 0 <= version.0 && version.0 <= 3 {
            self.key.encode(buf, version, is_flexible)?;
        } else if !self.key.is_empty() {
            return Err(SerializationError::FieldNotAvailable {
                field: "Key",
                version,
                api_name: "FindCoordinatorRequest",
            });
        }
        if 1 <= version.0 {
            self.key_type.encode(buf, version, is_flexible)?;
        } else if self.key_type != 0 {
            return Err(SerializationError::FieldNotAvailable {
                field: "KeyType",
                version,
                api_name: "FindCoordinatorRequest",
            });
        }
        if 4 <= version.0 {
            self.coordinator_keys.encode(buf, version, is_flexible)?;
        } else if !self.coordinator_keys.is_empty() {
            return Err(SerializationError::FieldNotAvailable {
                field: "CoordinatorKeys",
                version,
                api_name: "FindCoordinatorRequest",
            });
        }
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(version: ApiVer, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let key = if 0 <= version.0 && version.0 <= 3 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            String::new()
        };
        let key_type = if 1 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            0
        };
        let coordinator_keys = if 4 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Vec::new()
        };
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            key,
            key_type,
            coordinator_keys,
        })
    }
}
impl KafkaCodec for FindCoordinatorRequest {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        if 0 <= version.0 && version.0 <= 3 {
            self.key.encode(buf, version, is_flexible)?;
        }
        if 1 <= version.0 {
            self.key_type.encode(buf, version, is_flexible)?;
        }
        if 4 <= version.0 {
            self.coordinator_keys.encode(buf, version, is_flexible)?;
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
        let key = if 0 <= version.0 && version.0 <= 3 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            String::new()
        };
        let key_type = if 1 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            0
        };
        let coordinator_keys = if 4 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Vec::new()
        };
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            key,
            key_type,
            coordinator_keys,
        })
    }
}

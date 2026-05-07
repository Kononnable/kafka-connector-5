#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

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
    fn get_min_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(0)
    }
    fn get_max_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(6)
    }
    fn serialize(
        &self,
        version: crate::traits::ApiVersion,
        buf: &mut BytesMut,
    ) -> Result<(), SerializationError> {
        assert!(
            (0) <= version.0 && version.0 <= (6),
            "version {} is not supported by {} (supported: 0-6)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = (3) <= version.0;
        if (0) <= version.0 && version.0 <= (3) {
            self.key
                .encode_flexible(buf, is_flexible)
                .map_err(|_| SerializationError::Encode("failed to encode Key"))?;
        }
        if (1) <= version.0 {
            self.key_type
                .encode_flexible(buf, is_flexible)
                .map_err(|_| SerializationError::Encode("failed to encode KeyType"))?;
        }
        if (4) <= version.0 {
            self.coordinator_keys
                .encode_flexible(buf, is_flexible)
                .map_err(|_| SerializationError::Encode("failed to encode CoordinatorKeys"))?;
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
        let is_flexible = (3) <= version.0;
        let key = if (0) <= version.0 && version.0 <= (3) {
            <String as KafkaDeserialize>::decode_flexible(buf, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode Key"))?
        } else {
            Default::default()
        };
        let key_type = if (1) <= version.0 {
            <i8 as KafkaDeserialize>::decode_flexible(buf, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode KeyType"))?
        } else {
            Default::default()
        };
        let coordinator_keys = if (4) <= version.0 {
            <Vec<String> as KafkaDeserialize>::decode_flexible(buf, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode CoordinatorKeys"))?
        } else {
            Default::default()
        };
        Ok(Self {
            key,
            key_type,
            coordinator_keys,
        })
    }
}
impl KafkaSerialize for FindCoordinatorRequest {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.key
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Key".into(),
            })?;
        self.key_type
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode KeyType".into(),
            })?;
        self.coordinator_keys
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode CoordinatorKeys".into(),
            })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(
        &self,
        buf: &mut B,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.key
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Key".into(),
            })?;
        self.key_type
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode KeyType".into(),
            })?;
        self.coordinator_keys
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode CoordinatorKeys".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for FindCoordinatorRequest {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let key = <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
            message: "failed to decode Key".into(),
        })?;
        let key_type =
            <i8 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode KeyType".into(),
            })?;
        let coordinator_keys =
            <Vec<String> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode CoordinatorKeys".into(),
            })?;
        Ok(Self {
            key,
            key_type,
            coordinator_keys,
        })
    }
    fn decode_flexible<B: Buf>(buf: &mut B, is_flexible: bool) -> Result<Self, DecodeError> {
        let key =
            <String as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode Key".into(),
                }
            })?;
        let key_type =
            <i8 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode KeyType".into(),
                }
            })?;
        let coordinator_keys = <Vec<String> as KafkaDeserialize>::decode_flexible(buf, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode CoordinatorKeys".into(),
            })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            key,
            key_type,
            coordinator_keys,
        })
    }
}

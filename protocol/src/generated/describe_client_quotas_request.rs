#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{EncodeError, DecodeError, KafkaSerialize, KafkaDeserialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// DescribeClientQuotasRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct DescribeClientQuotasRequest {
    /// Filter components to apply to quota entities.
    pub components: Vec<ComponentData>,
    /// Whether the match is strict, i.e. should exclude entities with unspecified entity types.
    pub strict: bool,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ComponentData {
    /// The entity type that the filter component applies to.
    pub entity_type: String,
    /// How to match the entity {0 = exact name, 1 = default name, 2 = any specified name}.
    pub match_type: i8,
    /// The string to match against, or null if unused for the match type.
    pub r#match: Option<String>,
}

impl ApiRequest for DescribeClientQuotasRequest {
    type Response = crate::generated::DescribeClientQuotasResponse;
    fn get_api_key() -> ApiKey { ApiKey::new(48) }
    fn get_min_supported_version() -> crate::traits::ApiVersion { crate::traits::ApiVersion::new(0) }
    fn get_max_supported_version() -> crate::traits::ApiVersion { crate::traits::ApiVersion::new(1) }
    fn serialize(&self, version: crate::traits::ApiVersion, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!((0) <= version.0 && version.0 <= (1), "version {} is not supported by {} (supported: 0-1)", version.0, stringify!(Self));
        let is_flexible = (1) <= version.0;
        self.components.encode_flexible(buf, is_flexible).map_err(|_| SerializationError::Encode("failed to encode Components"))?;
        self.strict.encode_flexible(buf, is_flexible).map_err(|_| SerializationError::Encode("failed to encode Strict"))?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(version: crate::traits::ApiVersion, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let is_flexible = (1) <= version.0;
        let components = <Vec<ComponentData> as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| SerializationError::Decode("failed to decode Components"))?;
        let strict = <bool as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| SerializationError::Decode("failed to decode Strict"))?;
        Ok(Self { components, strict })
    }
}
impl KafkaSerialize for DescribeClientQuotasRequest {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.components.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Components".into() })?;
        self.strict.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Strict".into() })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(&self, buf: &mut B, is_flexible: bool) -> Result<(), EncodeError> {
        self.components.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Components".into() })?;
        self.strict.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Strict".into() })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for DescribeClientQuotasRequest {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let components = <Vec<ComponentData> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode Components".into() })?;
        let strict = <bool as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode Strict".into() })?;
        Ok(Self { components, strict })
    }
    fn decode_flexible<B: Buf>(buf: &mut B, is_flexible: bool) -> Result<Self, DecodeError> {
        let components = <Vec<ComponentData> as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode Components".into() })?;
        let strict = <bool as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode Strict".into() })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self { components, strict })
    }
}

impl KafkaSerialize for ComponentData {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.entity_type.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode EntityType".into() })?;
        self.match_type.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode MatchType".into() })?;
        self.r#match.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Match".into() })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(&self, buf: &mut B, is_flexible: bool) -> Result<(), EncodeError> {
        self.entity_type.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode EntityType".into() })?;
        self.match_type.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode MatchType".into() })?;
        if is_flexible {
            if let Some(ref __val) = self.r#match {
                crate::protocol::serialization::encode_unsigned_varint(1u64, buf);
                __val.encode_flexible(buf, true).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Match".into() })?;
            } else {
                crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
            }
        } else {
            if let Some(ref __val) = self.r#match {
                __val.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Match".into() })?;
            }
        }
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for ComponentData {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let entity_type = <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode EntityType".into() })?;
        let match_type = <i8 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode MatchType".into() })?;
        let r#match = <Option<String> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode Match".into() })?;
        Ok(Self { entity_type, match_type, r#match })
    }
    fn decode_flexible<B: Buf>(buf: &mut B, is_flexible: bool) -> Result<Self, DecodeError> {
        let entity_type = <String as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode EntityType".into() })?;
        let match_type = <i8 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode MatchType".into() })?;
        let r#match = if is_flexible {
            let (__present, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
            if __present == 0 {
                None
            } else {
                Some(<String as KafkaDeserialize>::decode_flexible(buf, true).map_err(|_| DecodeError::Protocol { message: "failed to decode Match".into() })?)
            }
        } else {
            <Option<String> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode Match".into() })?
        };
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self { entity_type, match_type, r#match })
    }
}


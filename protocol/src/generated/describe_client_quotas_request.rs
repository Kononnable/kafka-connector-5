#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVer, SerializationError};
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
    fn get_api_key() -> ApiKey {
        ApiKey::new(48)
    }
    fn get_min_supported_version() -> ApiVer {
        ApiVer::new(0)
    }
    fn get_max_supported_version() -> ApiVer {
        ApiVer::new(1)
    }
    fn get_min_flexible_version() -> ApiVer {
        ApiVer::new(1)
    }
    fn serialize(&self, version: ApiVer, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            (0) <= version.0 && version.0 <= (1),
            "version {} is not supported by {} (supported: 0-1)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        self.components.encode(buf, version, is_flexible)?;
        self.strict.encode(buf, version, is_flexible)?;
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(version: ApiVer, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let components = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let strict = KafkaDeserialize::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self { components, strict })
    }
}
impl KafkaSerialize for DescribeClientQuotasRequest {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.components.encode(buf, version, is_flexible)?;
        self.strict.encode(buf, version, is_flexible)?;
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for DescribeClientQuotasRequest {
    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let components = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let strict = KafkaDeserialize::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self { components, strict })
    }
}

impl KafkaSerialize for ComponentData {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.entity_type.encode(buf, version, is_flexible)?;
        self.match_type.encode(buf, version, is_flexible)?;
        self.r#match.encode(buf, version, is_flexible)?;
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for ComponentData {
    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let entity_type = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let match_type = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let r#match = KafkaDeserialize::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            entity_type,
            match_type,
            r#match,
        })
    }
}

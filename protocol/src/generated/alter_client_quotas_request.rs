#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// AlterClientQuotasRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct AlterClientQuotasRequest {
    /// The quota configuration entries to alter.
    pub entries: Vec<EntryData>,
    /// Whether the alteration should be validated, but not performed.
    pub validate_only: bool,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct EntityData {
    /// The entity type.
    pub entity_type: String,
    /// The name of the entity, or null if the default.
    pub entity_name: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct EntryData {
    /// The quota entity to alter.
    pub entity: Vec<EntityData>,
    /// An individual quota configuration entry to alter.
    pub ops: Vec<OpData>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct OpData {
    /// The quota configuration key.
    pub key: String,
    /// The value to set, otherwise ignored if the value is to be removed.
    pub value: f64,
    /// Whether the quota configuration value should be removed, otherwise set.
    pub remove: bool,
}

impl ApiRequest for AlterClientQuotasRequest {
    type Response = crate::generated::AlterClientQuotasResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(49)
    }
    fn get_min_supported_version() -> ApiVersion {
        ApiVersion::new(0)
    }
    fn get_max_supported_version() -> ApiVersion {
        ApiVersion::new(0)
    }
    fn serialize(&self, version: ApiVersion, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            (0) <= version.0 && version.0 <= (0),
            "version {} is not supported by {} (supported: 0-0)",
            version.0,
            stringify!(Self)
        );
        self.entries
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode Entries"))?;
        self.validate_only
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode ValidateOnly"))?;
        Ok(())
    }
    fn deserialize(version: ApiVersion, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let entries = <Vec<EntryData> as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode Entries"))?;
        let validate_only = <bool as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode ValidateOnly"))?;
        Ok(Self {
            entries,
            validate_only,
        })
    }
}
impl KafkaSerialize for AlterClientQuotasRequest {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.entries
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Entries".into(),
            })?;
        self.validate_only
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ValidateOnly".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for AlterClientQuotasRequest {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let entries = <Vec<EntryData> as KafkaDeserialize>::decode(buf).map_err(|_| {
            DecodeError::Protocol {
                message: "failed to decode Entries".into(),
            }
        })?;
        let validate_only =
            <bool as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ValidateOnly".into(),
            })?;
        Ok(Self {
            entries,
            validate_only,
        })
    }
}

impl KafkaSerialize for EntityData {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.entity_type
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode EntityType".into(),
            })?;
        self.entity_name
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode EntityName".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for EntityData {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let entity_type =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode EntityType".into(),
            })?;
        let entity_name = <Option<String> as KafkaDeserialize>::decode(buf).map_err(|_| {
            DecodeError::Protocol {
                message: "failed to decode EntityName".into(),
            }
        })?;
        Ok(Self {
            entity_type,
            entity_name,
        })
    }
}

impl KafkaSerialize for EntryData {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.entity
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Entity".into(),
            })?;
        self.ops
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Ops".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for EntryData {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let entity = <Vec<EntityData> as KafkaDeserialize>::decode(buf).map_err(|_| {
            DecodeError::Protocol {
                message: "failed to decode Entity".into(),
            }
        })?;
        let ops =
            <Vec<OpData> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode Ops".into(),
            })?;
        Ok(Self { entity, ops })
    }
}

impl KafkaSerialize for OpData {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.key
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Key".into(),
            })?;
        self.value
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Value".into(),
            })?;
        self.remove
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Remove".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for OpData {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let key = <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
            message: "failed to decode Key".into(),
        })?;
        let value = <f64 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
            message: "failed to decode Value".into(),
        })?;
        let remove =
            <bool as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode Remove".into(),
            })?;
        Ok(Self { key, value, remove })
    }
}

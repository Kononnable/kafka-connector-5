#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// AlterClientQuotasResponse
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct AlterClientQuotasResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// The quota configuration entries to alter.
    pub entries: Vec<EntryData>,
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
    /// The error code, or `0` if the quota alteration succeeded.
    pub error_code: i16,
    /// The error message, or `null` if the quota alteration succeeded.
    pub error_message: Option<String>,
    /// The quota entity to alter.
    pub entity: Vec<EntityData>,
}

impl ApiResponse for AlterClientQuotasResponse {
    type Request = crate::generated::AlterClientQuotasRequest;
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
        self.throttle_time_ms
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode ThrottleTimeMs"))?;
        self.entries
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode Entries"))?;
        Ok(())
    }
    fn deserialize(version: ApiVersion, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let throttle_time_ms = <i32 as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode ThrottleTimeMs"))?;
        let entries = <Vec<EntryData> as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode Entries"))?;
        Ok(Self {
            throttle_time_ms,
            entries,
        })
    }
}
impl KafkaSerialize for AlterClientQuotasResponse {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.throttle_time_ms
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ThrottleTimeMs".into(),
            })?;
        self.entries
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Entries".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for AlterClientQuotasResponse {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let throttle_time_ms =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ThrottleTimeMs".into(),
            })?;
        let entries = <Vec<EntryData> as KafkaDeserialize>::decode(buf).map_err(|_| {
            DecodeError::Protocol {
                message: "failed to decode Entries".into(),
            }
        })?;
        Ok(Self {
            throttle_time_ms,
            entries,
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
        self.error_code
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ErrorCode".into(),
            })?;
        self.error_message
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ErrorMessage".into(),
            })?;
        self.entity
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Entity".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for EntryData {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let error_code =
            <i16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ErrorCode".into(),
            })?;
        let error_message = <Option<String> as KafkaDeserialize>::decode(buf).map_err(|_| {
            DecodeError::Protocol {
                message: "failed to decode ErrorMessage".into(),
            }
        })?;
        let entity = <Vec<EntityData> as KafkaDeserialize>::decode(buf).map_err(|_| {
            DecodeError::Protocol {
                message: "failed to decode Entity".into(),
            }
        })?;
        Ok(Self {
            error_code,
            error_message,
            entity,
        })
    }
}

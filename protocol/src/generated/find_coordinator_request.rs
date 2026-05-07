#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// FindCoordinatorRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct FindCoordinatorRequest {
    /// The coordinator key.
    pub key: String,
    /// The coordinator key type.  (Group, transaction, etc.)
    /// Available in version 1+.
    pub key_type: i8,
}

impl ApiRequest for FindCoordinatorRequest {
    type Response = crate::generated::FindCoordinatorResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(10)
    }
    fn get_min_supported_version() -> ApiVersion {
        ApiVersion::new(0)
    }
    fn get_max_supported_version() -> ApiVersion {
        ApiVersion::new(2)
    }
    fn serialize(&self, version: ApiVersion, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            (0) <= version.0 && version.0 <= (2),
            "version {} is not supported by {} (supported: 0-2)",
            version.0,
            stringify!(Self)
        );
        self.key
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode Key"))?;
        if (1) <= version.0 {
            self.key_type
                .encode(buf)
                .map_err(|_| SerializationError::Encode("failed to encode KeyType"))?;
        }
        Ok(())
    }
    fn deserialize(version: ApiVersion, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let key = <String as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode Key"))?;
        let key_type = if (1) <= version.0 {
            <i8 as KafkaDeserialize>::decode(buf)
                .map_err(|_| SerializationError::Decode("failed to decode KeyType"))?
        } else {
            Default::default()
        };
        Ok(Self { key, key_type })
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
        Ok(Self { key, key_type })
    }
}

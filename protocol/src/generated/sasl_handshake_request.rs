#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// SaslHandshakeRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SaslHandshakeRequest {
    /// The SASL mechanism chosen by the client.
    pub mechanism: String,
}

impl ApiRequest for SaslHandshakeRequest {
    type Response = crate::generated::SaslHandshakeResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(17)
    }
    fn get_min_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(0)
    }
    fn get_max_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(1)
    }
    fn get_min_flexible_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(32767)
    }
    fn serialize(
        &self,
        version: crate::traits::ApiVersion,
        buf: &mut BytesMut,
    ) -> Result<(), SerializationError> {
        assert!(
            (0) <= version.0 && version.0 <= (1),
            "version {} is not supported by {} (supported: 0-1)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        self.mechanism
            .encode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode Mechanism"))?;
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
        let mechanism = <String as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode Mechanism"))?;
        Ok(Self { mechanism })
    }
}
impl KafkaSerialize for SaslHandshakeRequest {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.mechanism
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Mechanism".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for SaslHandshakeRequest {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        let mechanism =
            <String as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode Mechanism".into(),
                }
            })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self { mechanism })
    }
}

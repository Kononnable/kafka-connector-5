#![allow(unused_imports, unused_variables)]
use bytes::{Buf, BufMut, Bytes, BytesMut};
use indexmap::IndexMap;

use crate::protocol::serialization::{KafkaCodec, decode_unsigned_varint, encode_unsigned_varint};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVer, SerializationError};

// -------------------------------------------------------
// SaslAuthenticateResponse
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SaslAuthenticateResponse {
    /// The error code, or 0 if there was no error.
    pub error_code: i16,
    /// The error message, or null if there was no error.
    pub error_message: Option<String>,
    /// The SASL authentication bytes from the server, as defined by the SASL mechanism.
    pub auth_bytes: Vec<u8>,
    /// Number of milliseconds after which only re-authentication over the existing connection to create a new session can occur.
    /// Available in version 1+.
    pub session_lifetime_ms: i64,
}

impl ApiResponse for SaslAuthenticateResponse {
    type Request = crate::generated::SaslAuthenticateRequest;
    fn get_api_key() -> ApiKey {
        ApiKey::new(36)
    }
    fn get_min_supported_version() -> ApiVer {
        ApiVer::new(0)
    }
    fn get_max_supported_version() -> ApiVer {
        ApiVer::new(2)
    }
    fn get_min_flexible_version() -> ApiVer {
        ApiVer::new(2)
    }
    fn serialize(&self, version: ApiVer, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            0 <= version.0 && version.0 <= 2,
            "version {} is not supported by {} (supported: 0-2)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        self.error_code.encode(buf, version, is_flexible)?;
        self.error_message.encode(buf, version, is_flexible)?;
        self.auth_bytes.encode(buf, version, is_flexible)?;
        if 1 <= version.0 {
            self.session_lifetime_ms.encode(buf, version, is_flexible)?;
        } else if self.session_lifetime_ms != 0 {
            return Err(SerializationError::FieldNotAvailable {
                field: "SessionLifetimeMs",
                version,
                api_name: "SaslAuthenticateResponse",
            });
        }
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(version: ApiVer, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let error_code = KafkaCodec::decode(buf, version, is_flexible)?;
        let error_message = KafkaCodec::decode(buf, version, is_flexible)?;
        let auth_bytes = KafkaCodec::decode(buf, version, is_flexible)?;
        let session_lifetime_ms = if 1 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            0
        };
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            error_code,
            error_message,
            auth_bytes,
            session_lifetime_ms,
        })
    }
}
impl KafkaCodec for SaslAuthenticateResponse {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.error_code.encode(buf, version, is_flexible)?;
        self.error_message.encode(buf, version, is_flexible)?;
        self.auth_bytes.encode(buf, version, is_flexible)?;
        if 1 <= version.0 {
            self.session_lifetime_ms.encode(buf, version, is_flexible)?;
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
        let error_code = KafkaCodec::decode(buf, version, is_flexible)?;
        let error_message = KafkaCodec::decode(buf, version, is_flexible)?;
        let auth_bytes = KafkaCodec::decode(buf, version, is_flexible)?;
        let session_lifetime_ms = if 1 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            0
        };
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            error_code,
            error_message,
            auth_bytes,
            session_lifetime_ms,
        })
    }
}

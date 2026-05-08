#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{
    KafkaDeserialize, KafkaSerialize, decode_unsigned_varint, encode_unsigned_varint,
};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVer, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// RequestHeader
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RequestHeader {
    /// The API key of this request.
    pub request_api_key: i16,
    /// The API version of this request.
    pub request_api_version: i16,
    /// The correlation ID of this request.
    pub correlation_id: i32,
    /// The client ID string.
    /// Available in version 1+.
    pub client_id: Option<String>,
}

impl KafkaSerialize for RequestHeader {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.request_api_key.encode(buf, version, is_flexible)?;
        self.request_api_version.encode(buf, version, is_flexible)?;
        self.correlation_id.encode(buf, version, is_flexible)?;
        if 1 <= version.0 {
            self.client_id.encode(buf, version, is_flexible)?;
        }
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for RequestHeader {
    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let request_api_key = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let request_api_version = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let correlation_id = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let client_id = if 1 <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            request_api_key,
            request_api_version,
            correlation_id,
            client_id,
        })
    }
}

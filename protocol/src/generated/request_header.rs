#![allow(unused_imports, unused_variables)]
use bytes::{Buf, BufMut, Bytes, BytesMut};
use indexmap::IndexMap;

use crate::protocol::serialization::{KafkaCodec, decode_unsigned_varint, encode_unsigned_varint};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVer, SerializationError};

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

impl RequestHeader {
    pub fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), SerializationError> {
        let version = ApiVer::new(self.request_api_version);
        // Per KIP-511, ApiVersions (key 18) always uses header v1 (non-flexible)
        let is_flexible = self.request_api_key != 18
            && crate::generated::is_flexible_api(self.request_api_key, self.request_api_version);
        self.request_api_key.encode(buf, version, is_flexible)?;
        self.request_api_version.encode(buf, version, is_flexible)?;
        self.correlation_id.encode(buf, version, is_flexible)?;
        if 1 <= version.0 {
            self.client_id.encode(buf, version, false)?;
        }
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    pub fn decode<B: Buf>(buf: &mut B) -> Result<Self, SerializationError> {
        let request_api_key = i16::decode(buf, ApiVer::new(0), false)?;
        let request_api_version = i16::decode(buf, ApiVer::new(0), false)?;
        // Header is flexible at v2+ (KIP-511: ApiVersions always uses v1)
        let is_flexible = request_api_key != 18
            && crate::generated::is_flexible_api(request_api_key, request_api_version);
        let correlation_id = i32::decode(buf, ApiVer::new(0), is_flexible)?;
        // ClientId is ALWAYS classic nullable string (KIP-511)
        let client_id = if 1 <= request_api_version {
            <Option<String> as KafkaCodec>::decode(buf, ApiVer::new(0), false)?
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

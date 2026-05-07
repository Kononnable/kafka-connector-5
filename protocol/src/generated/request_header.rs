#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{decode_unsigned_varint, encode_unsigned_varint, DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// RequestHeader
// -------------------------------------------------------
/// Request header.
///
/// v1 (non-flexible):
///   request_api_key:    int16
///   request_api_version: int16
///   correlation_id:     int32
///   client_id:          NULLABLE_STRING (2-byte length + UTF-8; -1 = null)
///
/// v2 (flexible):
///   request_api_key:    int16
///   request_api_version: int16
///   correlation_id:     int32
///   client_id:          COMPACT_NULLABLE_STRING (unsigned varint length + UTF-8; 0 = null)
///   _tag_buffer:        unsigned varint count + tagged fields
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
    /// Raw tagged field bytes (v2+ flexible encoding).
    pub _tag_buffer: Vec<u8>,
}

impl KafkaSerialize for RequestHeader {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.request_api_key.encode(buf)?;
        self.request_api_version.encode(buf)?;
        self.correlation_id.encode(buf)?;
        // For the base encode (non-flexible v1), client_id is nullable string
        self.client_id.encode(buf)?;
        Ok(())
    }
}

impl KafkaDeserialize for RequestHeader {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let request_api_key = i16::decode(buf)?;
        let request_api_version = i16::decode(buf)?;
        let correlation_id = i32::decode(buf)?;
        let client_id = Option::<String>::decode(buf)?;
        Ok(Self {
            request_api_key,
            request_api_version,
            correlation_id,
            client_id,
            _tag_buffer: Vec::new(),
        })
    }
}

// Flexible encode/decode (v2): compact nullable string + tag_buffer
impl RequestHeader {
    /// Encode as v2 (flexible) header.
    pub fn encode_v2<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.request_api_key.encode(buf)?;
        self.request_api_version.encode(buf)?;
        self.correlation_id.encode(buf)?;
        // client_id as compact nullable string
        match &self.client_id {
            None => { encode_unsigned_varint(0, buf); }
            Some(s) => {
                encode_unsigned_varint(s.len() as u64 + 1, buf);
                buf.put_slice(s.as_bytes());
            }
        }
        // Tag buffer
        if self._tag_buffer.is_empty() {
            encode_unsigned_varint(0, buf);
        } else {
            buf.put_slice(&self._tag_buffer);
        }
        Ok(())
    }

    /// Decode as v2 (flexible) header.
    pub fn decode_v2<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let request_api_key = i16::decode(buf)?;
        let request_api_version = i16::decode(buf)?;
        let correlation_id = i32::decode(buf)?;
        // client_id as compact nullable string
        let client_id = match decode_unsigned_varint(buf) {
            Ok((0, _)) => None,
            Ok((raw_len, _)) => {
                let len = (raw_len - 1) as usize;
                let mut data = vec![0u8; len];
                buf.copy_to_slice(&mut data);
                Some(String::from_utf8(data).map_err(|_| DecodeError::InvalidUtf8)?)
            }
            Err(e) => return Err(e),
        };
        // Capture remaining bytes as tag buffer
        let mut _tag_buffer = Vec::new();
        while buf.has_remaining() {
            _tag_buffer.push(buf.get_u8());
        }
        Ok(Self {
            request_api_key,
            request_api_version,
            correlation_id,
            client_id,
            _tag_buffer,
        })
    }
}

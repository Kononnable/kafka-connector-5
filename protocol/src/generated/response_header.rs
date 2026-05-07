#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{encode_unsigned_varint, DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// ResponseHeader
// -------------------------------------------------------
/// Response header.
///
/// v0 (non-flexible):
///   correlation_id: int32
///
/// v1 (flexible):
///   correlation_id: int32
///   _tag_buffer:    unsigned varint count + tagged fields
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ResponseHeader {
    /// The correlation ID of this response.
    pub correlation_id: i32,
    /// Raw tagged field bytes (v1+ flexible encoding).
    pub _tag_buffer: Vec<u8>,
}

impl KafkaSerialize for ResponseHeader {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.correlation_id.encode(buf)?;
        Ok(())
    }
}

impl KafkaDeserialize for ResponseHeader {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let correlation_id = i32::decode(buf)?;
        Ok(Self {
            correlation_id,
            _tag_buffer: Vec::new(),
        })
    }
}

// Flexible encode/decode (v1): includes tag_buffer
impl ResponseHeader {
    /// Encode as v1 (flexible) header.
    pub fn encode_v1<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.correlation_id.encode(buf)?;
        // Tag buffer
        if self._tag_buffer.is_empty() {
            encode_unsigned_varint(0, buf);
        } else {
            buf.put_slice(&self._tag_buffer);
        }
        Ok(())
    }

    /// Decode as v1 (flexible) header.
    pub fn decode_v1<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let correlation_id = i32::decode(buf)?;
        let mut _tag_buffer = Vec::new();
        while buf.has_remaining() {
            _tag_buffer.push(buf.get_u8());
        }
        Ok(Self {
            correlation_id,
            _tag_buffer,
        })
    }
}

#![allow(unused_imports, unused_variables)]
use bytes::{Buf, BufMut, Bytes, BytesMut};
use indexmap::IndexMap;

use crate::protocol::serialization::{KafkaCodec, decode_unsigned_varint, encode_unsigned_varint};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVer, SerializationError};

// -------------------------------------------------------
// ResponseHeader
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ResponseHeader {
    /// The correlation ID of this response.
    pub correlation_id: i32,
}

impl ResponseHeader {
    pub fn peek_correlation_id(buf: &[u8]) -> Result<i32, SerializationError> {
        let mut cur: &[u8] = buf;
        i32::decode(&mut cur, ApiVer::new(0), false)
    }
    pub fn decode<B: Buf>(buf: &mut B, is_flexible: bool) -> Result<Self, SerializationError> {
        let correlation_id = i32::decode(buf, ApiVer::new(0), false)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self { correlation_id })
    }
}

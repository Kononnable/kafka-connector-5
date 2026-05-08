#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{
    KafkaDeserialize, KafkaSerialize, decode_unsigned_varint, encode_unsigned_varint,
};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVer, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// ResponseHeader
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ResponseHeader {
    /// The correlation ID of this response.
    pub correlation_id: i32,
}

#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{
    KafkaDeserialize, KafkaSerialize, decode_unsigned_varint, encode_unsigned_varint,
};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVer, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// KRaftVersionRecord
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct KRaftVersionRecord {
    /// The version of the kraft version record.
    pub version: i16,
    /// The kraft protocol version.
    pub kraft_version: i16,
}

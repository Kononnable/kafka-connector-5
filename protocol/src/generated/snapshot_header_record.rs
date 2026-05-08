#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{
    KafkaDeserialize, KafkaSerialize, decode_unsigned_varint, encode_unsigned_varint,
};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVer, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// SnapshotHeaderRecord
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SnapshotHeaderRecord {
    /// The version of the snapshot header record.
    pub version: i16,
    /// The append time of the last record from the log contained in this snapshot.
    pub last_contained_log_timestamp: i64,
}

#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// SnapshotFooterRecord
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SnapshotFooterRecord {
    /// The version of the snapshot footer record
    pub version: i16,
}

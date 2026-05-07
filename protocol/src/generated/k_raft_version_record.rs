#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
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

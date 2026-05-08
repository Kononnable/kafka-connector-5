#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVer, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// EndTxnMarker
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct EndTxnMarker {
    /// The coordinator epoch when appending the record
    pub coordinator_epoch: i32,
}

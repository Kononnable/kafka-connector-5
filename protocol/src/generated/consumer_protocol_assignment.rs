#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// ConsumerProtocolAssignment
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ConsumerProtocolAssignment {
    /// AssignedPartitions. Type: []TopicPartition.
    pub assigned_partitions: Vec<TopicPartition>,
    /// UserData. Type: bytes.
    pub user_data: Option<Vec<u8>>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct TopicPartition {
    /// Topic. Type: string.
    pub topic: String,
    /// Partitions. Type: []int32.
    pub partitions: Vec<i32>,
}

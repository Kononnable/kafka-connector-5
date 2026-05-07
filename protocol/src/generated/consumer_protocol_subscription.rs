#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// ConsumerProtocolSubscription
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ConsumerProtocolSubscription {
    /// Topics. Type: []string.
    pub topics: Vec<String>,
    /// UserData. Type: bytes.
    pub user_data: Option<Vec<u8>>,
    /// OwnedPartitions. Type: []TopicPartition.
    /// Available in version 1+.
    pub owned_partitions: Vec<TopicPartition>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct TopicPartition {
    /// Topic. Type: string.
    /// Available in version 1+.
    pub topic: String,
    /// Partitions. Type: []int32.
    /// Available in version 1+.
    pub partitions: Vec<i32>,
}

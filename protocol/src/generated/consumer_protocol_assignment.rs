#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{
    KafkaDeserialize, KafkaSerialize, decode_unsigned_varint, encode_unsigned_varint,
};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVer, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// ConsumerProtocolAssignment
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ConsumerProtocolAssignment {
    /// The list of topics and partitions assigned to this consumer.
    pub assigned_partitions: Vec<TopicPartition>,
    /// User data.
    pub user_data: Option<Vec<u8>>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct TopicPartition {
    /// The topic name.
    pub topic: String,
    /// The list of partitions assigned to this consumer.
    pub partitions: Vec<i32>,
}

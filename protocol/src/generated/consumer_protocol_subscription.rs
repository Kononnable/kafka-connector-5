#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// ConsumerProtocolSubscription
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ConsumerProtocolSubscription {
    /// The topics that the member wants to consume.
    pub topics: Vec<String>,
    /// User data that will be passed back to the consumer.
    pub user_data: Option<Vec<u8>>,
    /// The partitions that the member owns.
    /// Available in version 1+.
    pub owned_partitions: Vec<TopicPartition>,
    /// The generation id of the member.
    /// Available in version 2+.
    pub generation_id: i32,
    /// The rack id of the member.
    /// Available in version 3+.
    pub rack_id: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct TopicPartition {
    /// The topic name.
    /// Available in version 1+.
    pub topic: String,
    /// The partition ids.
    /// Available in version 1+.
    pub partitions: Vec<i32>,
}

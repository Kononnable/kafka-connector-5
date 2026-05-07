#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// LeaderChangeMessage
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct LeaderChangeMessage {
    /// The ID of the newly elected leader
    pub leader_id: i32,
    /// The voters who voted for the current leader
    pub voters: Vec<Voter>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Voter {
    /// VoterId. Type: int32.
    pub voter_id: i32,
}

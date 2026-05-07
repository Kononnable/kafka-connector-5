#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// LeaderChangeMessage
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct LeaderChangeMessage {
    /// The version of the leader change message
    pub version: i16,
    /// The ID of the newly elected leader
    pub leader_id: i32,
    /// The set of voters in the quorum for this epoch
    pub voters: Vec<Voter>,
    /// The voters who voted for the leader at the time of election
    pub granting_voters: Vec<Voter>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Voter {
    /// VoterId. Type: int32.
    pub voter_id: i32,
}

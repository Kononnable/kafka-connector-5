#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{KafkaDeserialize, KafkaSerialize};
use crate::traits::{
    ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVersionTrait, SerializationError,
};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// VotersRecord
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct VotersRecord {
    /// The version of the voters record.
    pub version: i16,
    /// The set of voters in the quorum for this epoch.
    pub voters: Vec<Voter>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Endpoint {
    /// The name of the endpoint.
    pub name: String,
    /// The hostname.
    pub host: String,
    /// The port.
    pub port: u16,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct KRaftVersionFeature {
    /// The minimum supported KRaft protocol version.
    pub min_supported_version: i16,
    /// The maximum supported KRaft protocol version.
    pub max_supported_version: i16,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Voter {
    /// The replica id of the voter in the topic partition.
    pub voter_id: i32,
    /// The directory id of the voter in the topic partition.
    pub voter_directory_id: [u8; 16],
    /// The endpoint that can be used to communicate with the voter.
    pub endpoints: Vec<Endpoint>,
    /// The range of versions of the protocol that the replica supports.
    pub kraft_version_feature: KRaftVersionFeature,
}

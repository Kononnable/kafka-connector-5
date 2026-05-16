#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{KafkaCodec, decode_unsigned_varint, encode_unsigned_varint};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVer, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use indexmap::IndexMap;

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
    /// IndexMap key `Name` (string): The name of the endpoint.
    pub endpoints: IndexMap<String, Endpoint>,
    /// The range of versions of the protocol that the replica supports.
    pub kraft_version_feature: KRaftVersionFeature,
}

impl KafkaCodec for VotersRecord {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.version.encode(buf, version, is_flexible)?;
        self.voters.encode(buf, version, is_flexible)?;
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }

    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let version_val = KafkaCodec::decode(buf, version, is_flexible)?;
        let voters = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            version: version_val,
            voters,
        })
    }
}

impl KafkaCodec for Endpoint {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.host.encode(buf, version, is_flexible)?;
        self.port.encode(buf, version, is_flexible)?;
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }

    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let host = KafkaCodec::decode(buf, version, is_flexible)?;
        let port = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self { host, port })
    }
}

impl KafkaCodec for KRaftVersionFeature {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.min_supported_version
            .encode(buf, version, is_flexible)?;
        self.max_supported_version
            .encode(buf, version, is_flexible)?;
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }

    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let min_supported_version = KafkaCodec::decode(buf, version, is_flexible)?;
        let max_supported_version = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            min_supported_version,
            max_supported_version,
        })
    }
}

impl KafkaCodec for Voter {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.voter_id.encode(buf, version, is_flexible)?;
        self.voter_directory_id.encode(buf, version, is_flexible)?;
        self.endpoints.encode(buf, version, is_flexible)?;
        self.kraft_version_feature
            .encode(buf, version, is_flexible)?;
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }

    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let voter_id = KafkaCodec::decode(buf, version, is_flexible)?;
        let voter_directory_id = KafkaCodec::decode(buf, version, is_flexible)?;
        let endpoints = KafkaCodec::decode(buf, version, is_flexible)?;
        let kraft_version_feature = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            voter_id,
            voter_directory_id,
            endpoints,
            kraft_version_feature,
        })
    }
}

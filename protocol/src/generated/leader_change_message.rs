#![allow(unused_imports, unused_variables)]
use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::protocol::serialization::{KafkaCodec, decode_unsigned_varint, encode_unsigned_varint};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVer, SerializationError};

// -------------------------------------------------------
// LeaderChangeMessage
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct LeaderChangeMessage {
    /// The version of the leader change message.
    pub version: i16,
    /// The ID of the newly elected leader.
    pub leader_id: i32,
    /// The set of voters in the quorum for this epoch.
    pub voters: Vec<Voter>,
    /// The voters who voted for the leader at the time of election.
    pub granting_voters: Vec<Voter>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Voter {
    /// The ID of the voter.
    pub voter_id: i32,
    /// The directory id of the voter.
    /// Available in version 1+.
    pub voter_directory_id: [u8; 16],
}

impl KafkaCodec for LeaderChangeMessage {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.version.encode(buf, version, is_flexible)?;
        self.leader_id.encode(buf, version, is_flexible)?;
        self.voters.encode(buf, version, is_flexible)?;
        self.granting_voters.encode(buf, version, is_flexible)?;
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
        let leader_id = KafkaCodec::decode(buf, version, is_flexible)?;
        let voters = KafkaCodec::decode(buf, version, is_flexible)?;
        let granting_voters = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            version: version_val,
            leader_id,
            voters,
            granting_voters,
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
        if 1 <= version.0 {
            self.voter_directory_id.encode(buf, version, is_flexible)?;
        }
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
        let voter_directory_id = if 1 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            voter_id,
            voter_directory_id,
        })
    }
}

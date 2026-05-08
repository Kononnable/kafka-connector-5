#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{KafkaDeserialize, KafkaSerialize};
use crate::traits::{
    ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVersionTrait, SerializationError,
};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// RemoveRaftVoterRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RemoveRaftVoterRequest {
    /// The cluster id of the request.
    pub cluster_id: Option<String>,
    /// The replica id of the voter getting removed from the topic partition.
    pub voter_id: i32,
    /// The directory id of the voter getting removed from the topic partition.
    pub voter_directory_id: [u8; 16],
}

impl ApiRequest for RemoveRaftVoterRequest {
    type Response = crate::generated::RemoveRaftVoterResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(81)
    }
    fn get_min_supported_version() -> ApiVersionTrait {
        ApiVersionTrait::new(0)
    }
    fn get_max_supported_version() -> ApiVersionTrait {
        ApiVersionTrait::new(0)
    }
    fn get_min_flexible_version() -> ApiVersionTrait {
        ApiVersionTrait::new(0)
    }
    fn serialize(
        &self,
        version: ApiVersionTrait,
        buf: &mut BytesMut,
    ) -> Result<(), SerializationError> {
        assert!(
            (0) <= version.0 && version.0 <= (0),
            "version {} is not supported by {} (supported: 0-0)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        self.cluster_id.encode(buf, version, is_flexible)?;
        self.voter_id.encode(buf, version, is_flexible)?;
        self.voter_directory_id.encode(buf, version, is_flexible)?;
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(version: ApiVersionTrait, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let cluster_id = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let voter_id = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let voter_directory_id = KafkaDeserialize::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            cluster_id,
            voter_id,
            voter_directory_id,
        })
    }
}
impl KafkaSerialize for RemoveRaftVoterRequest {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVersionTrait,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.cluster_id.encode(buf, version, is_flexible)?;
        self.voter_id.encode(buf, version, is_flexible)?;
        self.voter_directory_id.encode(buf, version, is_flexible)?;
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for RemoveRaftVoterRequest {
    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVersionTrait,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let cluster_id = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let voter_id = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let voter_directory_id = KafkaDeserialize::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            cluster_id,
            voter_id,
            voter_directory_id,
        })
    }
}

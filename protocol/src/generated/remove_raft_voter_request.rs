#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
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
    fn get_min_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(0)
    }
    fn get_max_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(0)
    }
    fn get_min_flexible_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(0)
    }
    fn serialize(
        &self,
        version: crate::traits::ApiVersion,
        buf: &mut BytesMut,
    ) -> Result<(), SerializationError> {
        assert!(
            (0) <= version.0 && version.0 <= (0),
            "version {} is not supported by {} (supported: 0-0)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        self.cluster_id
            .encode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode ClusterId"))?;
        self.voter_id
            .encode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode VoterId"))?;
        self.voter_directory_id
            .encode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode VoterDirectoryId"))?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(
        version: crate::traits::ApiVersion,
        buf: &mut Bytes,
    ) -> Result<Self, SerializationError> {
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let cluster_id = <Option<String> as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode ClusterId"))?;
        let voter_id = <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode VoterId"))?;
        let voter_directory_id = <[u8; 16] as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode VoterDirectoryId"))?;
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
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.cluster_id
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ClusterId".into(),
            })?;
        self.voter_id
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode VoterId".into(),
            })?;
        self.voter_directory_id
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode VoterDirectoryId".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for RemoveRaftVoterRequest {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        let cluster_id = <Option<String> as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode ClusterId".into(),
            })?;
        let voter_id =
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode VoterId".into(),
                }
            })?;
        let voter_directory_id = <[u8; 16] as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode VoterDirectoryId".into(),
            })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            cluster_id,
            voter_id,
            voter_directory_id,
        })
    }
}

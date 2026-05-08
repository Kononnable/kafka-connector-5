#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// VoteRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct VoteRequest {
    /// The cluster id.
    pub cluster_id: Option<String>,
    /// The replica id of the voter receiving the request.
    /// Available in version 1+.
    pub voter_id: i32,
    /// The topic data.
    pub topics: Vec<TopicData>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct PartitionData {
    /// The partition index.
    pub partition_index: i32,
    /// The epoch of the voter sending the request
    pub replica_epoch: i32,
    /// The replica id of the voter sending the request
    pub replica_id: i32,
    /// The directory id of the voter sending the request
    /// Available in version 1+.
    pub replica_directory_id: [u8; 16],
    /// The directory id of the voter receiving the request
    /// Available in version 1+.
    pub voter_directory_id: [u8; 16],
    /// The epoch of the last record written to the metadata log.
    pub last_offset_epoch: i32,
    /// The log end offset of the metadata log of the voter sending the request.
    pub last_offset: i64,
    /// Whether the request is a PreVote request (not persisted) or not.
    /// Available in version 2+.
    pub pre_vote: bool,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct TopicData {
    /// The topic name.
    pub topic_name: String,
    /// The partition data.
    pub partitions: Vec<PartitionData>,
}

impl ApiRequest for VoteRequest {
    type Response = crate::generated::VoteResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(52)
    }
    fn get_min_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(0)
    }
    fn get_max_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(2)
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
            (0) <= version.0 && version.0 <= (2),
            "version {} is not supported by {} (supported: 0-2)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        self.cluster_id
            .encode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode ClusterId"))?;
        if (1) <= version.0 {
            self.voter_id
                .encode(buf, version, is_flexible)
                .map_err(|_| SerializationError::Encode("failed to encode VoterId"))?;
        } else if self.voter_id != 0 {
            return Err(SerializationError::Encode(
                "field 'VoterId' is not available in this version",
            ));
        }
        self.topics
            .encode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode Topics"))?;
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
        let voter_id = if (1) <= version.0 {
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode VoterId"))?
        } else {
            Default::default()
        };
        let topics = <Vec<TopicData> as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode Topics"))?;
        Ok(Self {
            cluster_id,
            voter_id,
            topics,
        })
    }
}
impl KafkaSerialize for VoteRequest {
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
        if (1) <= version.0 {
            self.voter_id
                .encode(buf, version, is_flexible)
                .map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode VoterId".into(),
                })?;
        }
        self.topics
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Topics".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for VoteRequest {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        let cluster_id = <Option<String> as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode ClusterId".into(),
            })?;
        let voter_id = if (1) <= version.0 {
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode VoterId".into(),
                }
            })?
        } else {
            Default::default()
        };
        let topics = <Vec<TopicData> as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode Topics".into(),
            })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            cluster_id,
            voter_id,
            topics,
        })
    }
}

impl KafkaSerialize for PartitionData {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.partition_index
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode PartitionIndex".into(),
            })?;
        self.replica_epoch
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ReplicaEpoch".into(),
            })?;
        self.replica_id
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ReplicaId".into(),
            })?;
        if (1) <= version.0 {
            self.replica_directory_id
                .encode(buf, version, is_flexible)
                .map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode ReplicaDirectoryId".into(),
                })?;
        }
        if (1) <= version.0 {
            self.voter_directory_id
                .encode(buf, version, is_flexible)
                .map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode VoterDirectoryId".into(),
                })?;
        }
        self.last_offset_epoch
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode LastOffsetEpoch".into(),
            })?;
        self.last_offset
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode LastOffset".into(),
            })?;
        if (2) <= version.0 {
            self.pre_vote
                .encode(buf, version, is_flexible)
                .map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode PreVote".into(),
                })?;
        }
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for PartitionData {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        let partition_index = <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode PartitionIndex".into(),
            })?;
        let replica_epoch =
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode ReplicaEpoch".into(),
                }
            })?;
        let replica_id =
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode ReplicaId".into(),
                }
            })?;
        let replica_directory_id = if (1) <= version.0 {
            <[u8; 16] as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode ReplicaDirectoryId".into(),
                }
            })?
        } else {
            Default::default()
        };
        let voter_directory_id = if (1) <= version.0 {
            <[u8; 16] as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode VoterDirectoryId".into(),
                }
            })?
        } else {
            Default::default()
        };
        let last_offset_epoch = <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode LastOffsetEpoch".into(),
            })?;
        let last_offset =
            <i64 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode LastOffset".into(),
                }
            })?;
        let pre_vote = if (2) <= version.0 {
            <bool as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode PreVote".into(),
                }
            })?
        } else {
            Default::default()
        };
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            partition_index,
            replica_epoch,
            replica_id,
            replica_directory_id,
            voter_directory_id,
            last_offset_epoch,
            last_offset,
            pre_vote,
        })
    }
}

impl KafkaSerialize for TopicData {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.topic_name
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode TopicName".into(),
            })?;
        self.partitions
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Partitions".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for TopicData {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        let topic_name =
            <String as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode TopicName".into(),
                }
            })?;
        let partitions =
            <Vec<PartitionData> as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode Partitions".into(),
                },
            )?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            topic_name,
            partitions,
        })
    }
}

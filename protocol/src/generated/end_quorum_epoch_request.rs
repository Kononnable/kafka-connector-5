#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// EndQuorumEpochRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct EndQuorumEpochRequest {
    /// The cluster id.
    pub cluster_id: Option<String>,
    /// The topics.
    pub topics: Vec<TopicData>,
    /// Endpoints for the leader.
    /// Available in version 1+.
    pub leader_endpoints: Vec<LeaderEndpoint>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct LeaderEndpoint {
    /// The name of the endpoint.
    /// Available in version 1+.
    pub name: String,
    /// The node's hostname.
    /// Available in version 1+.
    pub host: String,
    /// The node's port.
    /// Available in version 1+.
    pub port: u16,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct PartitionData {
    /// The partition index.
    pub partition_index: i32,
    /// The current leader ID that is resigning.
    pub leader_id: i32,
    /// The current epoch.
    pub leader_epoch: i32,
    /// A sorted list of preferred successors to start the election.
    /// Available in version 0.
    pub preferred_successors: Vec<i32>,
    /// A sorted list of preferred candidates to start the election.
    /// Available in version 1+.
    pub preferred_candidates: Vec<ReplicaInfo>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ReplicaInfo {
    /// The ID of the candidate replica.
    /// Available in version 1+.
    pub candidate_id: i32,
    /// The directory ID of the candidate replica.
    /// Available in version 1+.
    pub candidate_directory_id: [u8; 16],
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct TopicData {
    /// The topic name.
    pub topic_name: String,
    /// The partitions.
    pub partitions: Vec<PartitionData>,
}

impl ApiRequest for EndQuorumEpochRequest {
    type Response = crate::generated::EndQuorumEpochResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(54)
    }
    fn get_min_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(0)
    }
    fn get_max_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(1)
    }
    fn get_min_flexible_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(1)
    }
    fn serialize(
        &self,
        version: crate::traits::ApiVersion,
        buf: &mut BytesMut,
    ) -> Result<(), SerializationError> {
        assert!(
            (0) <= version.0 && version.0 <= (1),
            "version {} is not supported by {} (supported: 0-1)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        self.cluster_id.encode(buf, version, is_flexible)?;
        self.topics.encode(buf, version, is_flexible)?;
        if (1) <= version.0 {
            self.leader_endpoints.encode(buf, version, is_flexible)?;
        } else if !self.leader_endpoints.is_empty() {
            return Err(SerializationError::Encode(
                "field 'LeaderEndpoints' is not available in this version",
            ));
        }
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(
        version: crate::traits::ApiVersion,
        buf: &mut Bytes,
    ) -> Result<Self, SerializationError> {
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let cluster_id = <Option<String> as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let topics = <Vec<TopicData> as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let leader_endpoints = if (1) <= version.0 {
            <Vec<LeaderEndpoint> as KafkaDeserialize>::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            cluster_id,
            topics,
            leader_endpoints,
        })
    }
}
impl KafkaSerialize for EndQuorumEpochRequest {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), crate::traits::SerializationError> {
        self.cluster_id.encode(buf, version, is_flexible)?;
        self.topics.encode(buf, version, is_flexible)?;
        if (1) <= version.0 {
            self.leader_endpoints.encode(buf, version, is_flexible)?;
        }
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for EndQuorumEpochRequest {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, crate::traits::SerializationError> {
        let cluster_id = <Option<String> as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let topics = <Vec<TopicData> as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let leader_endpoints = if (1) <= version.0 {
            <Vec<LeaderEndpoint> as KafkaDeserialize>::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            cluster_id,
            topics,
            leader_endpoints,
        })
    }
}

impl KafkaSerialize for LeaderEndpoint {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), crate::traits::SerializationError> {
        if (1) <= version.0 {
            self.name.encode(buf, version, is_flexible)?;
        }
        if (1) <= version.0 {
            self.host.encode(buf, version, is_flexible)?;
        }
        if (1) <= version.0 {
            self.port.encode(buf, version, is_flexible)?;
        }
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for LeaderEndpoint {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, crate::traits::SerializationError> {
        let name = if (1) <= version.0 {
            <String as KafkaDeserialize>::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let host = if (1) <= version.0 {
            <String as KafkaDeserialize>::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let port = if (1) <= version.0 {
            <u16 as KafkaDeserialize>::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self { name, host, port })
    }
}

impl KafkaSerialize for PartitionData {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), crate::traits::SerializationError> {
        self.partition_index.encode(buf, version, is_flexible)?;
        self.leader_id.encode(buf, version, is_flexible)?;
        self.leader_epoch.encode(buf, version, is_flexible)?;
        if version.0 == (0) {
            self.preferred_successors
                .encode(buf, version, is_flexible)?;
        }
        if (1) <= version.0 {
            self.preferred_candidates
                .encode(buf, version, is_flexible)?;
        }
        if is_flexible {
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
    ) -> Result<Self, crate::traits::SerializationError> {
        let partition_index = <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let leader_id = <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let leader_epoch = <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let preferred_successors = if version.0 == (0) {
            <Vec<i32> as KafkaDeserialize>::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let preferred_candidates = if (1) <= version.0 {
            <Vec<ReplicaInfo> as KafkaDeserialize>::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            partition_index,
            leader_id,
            leader_epoch,
            preferred_successors,
            preferred_candidates,
        })
    }
}

impl KafkaSerialize for ReplicaInfo {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), crate::traits::SerializationError> {
        if (1) <= version.0 {
            self.candidate_id.encode(buf, version, is_flexible)?;
        }
        if (1) <= version.0 {
            self.candidate_directory_id
                .encode(buf, version, is_flexible)?;
        }
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for ReplicaInfo {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, crate::traits::SerializationError> {
        let candidate_id = if (1) <= version.0 {
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let candidate_directory_id = if (1) <= version.0 {
            <[u8; 16] as KafkaDeserialize>::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            candidate_id,
            candidate_directory_id,
        })
    }
}

impl KafkaSerialize for TopicData {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), crate::traits::SerializationError> {
        self.topic_name.encode(buf, version, is_flexible)?;
        self.partitions.encode(buf, version, is_flexible)?;
        if is_flexible {
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
    ) -> Result<Self, crate::traits::SerializationError> {
        let topic_name = <String as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let partitions =
            <Vec<PartitionData> as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            topic_name,
            partitions,
        })
    }
}

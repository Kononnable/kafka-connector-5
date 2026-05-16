#![allow(unused_imports, unused_variables)]
use bytes::{Buf, BufMut, Bytes, BytesMut};
use indexmap::IndexMap;

use crate::protocol::serialization::{KafkaCodec, decode_unsigned_varint, encode_unsigned_varint};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVer, SerializationError};

// -------------------------------------------------------
// BeginQuorumEpochRequest
// -------------------------------------------------------
#[derive(Clone, Debug, PartialEq)]
pub struct BeginQuorumEpochRequest {
    /// The cluster id.
    pub cluster_id: Option<String>,
    /// The replica id of the voter receiving the request.
    /// Available in version 1+.
    pub voter_id: i32,
    /// The topics.
    pub topics: Vec<TopicData>,
    /// Endpoints for the leader.
    /// IndexMap key `Name` (string): The name of the endpoint.
    /// Available in version 1+.
    pub leader_endpoints: IndexMap<String, LeaderEndpoint>,
}
impl Default for BeginQuorumEpochRequest {
    fn default() -> Self {
        Self {
            cluster_id: None,
            voter_id: -1,
            topics: Vec::new(),
            leader_endpoints: IndexMap::new(),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct LeaderEndpoint {
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
    /// The directory id of the receiving replica.
    /// Available in version 1+.
    pub voter_directory_id: [u8; 16],
    /// The ID of the newly elected leader.
    pub leader_id: i32,
    /// The epoch of the newly elected leader.
    pub leader_epoch: i32,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct TopicData {
    /// The topic name.
    pub topic_name: String,
    /// The partitions.
    pub partitions: Vec<PartitionData>,
}

impl ApiRequest for BeginQuorumEpochRequest {
    type Response = crate::generated::BeginQuorumEpochResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(53)
    }
    fn get_min_supported_version() -> ApiVer {
        ApiVer::new(0)
    }
    fn get_max_supported_version() -> ApiVer {
        ApiVer::new(1)
    }
    fn get_min_flexible_version() -> ApiVer {
        ApiVer::new(1)
    }
    fn serialize(&self, version: ApiVer, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            0 <= version.0 && version.0 <= 1,
            "version {} is not supported by {} (supported: 0-1)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        self.cluster_id.encode(buf, version, is_flexible)?;
        if 1 <= version.0 {
            self.voter_id.encode(buf, version, is_flexible)?;
        } else if self.voter_id != -1 {
            return Err(SerializationError::FieldNotAvailable {
                field: "VoterId",
                version,
                api_name: "BeginQuorumEpochRequest",
            });
        }
        self.topics.encode(buf, version, is_flexible)?;
        if 1 <= version.0 {
            self.leader_endpoints.encode(buf, version, is_flexible)?;
        } else if !self.leader_endpoints.is_empty() {
            return Err(SerializationError::FieldNotAvailable {
                field: "LeaderEndpoints",
                version,
                api_name: "BeginQuorumEpochRequest",
            });
        }
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(version: ApiVer, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let cluster_id = KafkaCodec::decode(buf, version, is_flexible)?;
        let voter_id = if 1 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            -1
        };
        let topics = KafkaCodec::decode(buf, version, is_flexible)?;
        let leader_endpoints = if 1 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            IndexMap::new()
        };
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            cluster_id,
            voter_id,
            topics,
            leader_endpoints,
        })
    }
}
impl KafkaCodec for BeginQuorumEpochRequest {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.cluster_id.encode(buf, version, is_flexible)?;
        if 1 <= version.0 {
            self.voter_id.encode(buf, version, is_flexible)?;
        }
        self.topics.encode(buf, version, is_flexible)?;
        if 1 <= version.0 {
            self.leader_endpoints.encode(buf, version, is_flexible)?;
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
        let cluster_id = KafkaCodec::decode(buf, version, is_flexible)?;
        let voter_id = if 1 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            -1
        };
        let topics = KafkaCodec::decode(buf, version, is_flexible)?;
        let leader_endpoints = if 1 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            IndexMap::new()
        };
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            cluster_id,
            voter_id,
            topics,
            leader_endpoints,
        })
    }
}

impl KafkaCodec for LeaderEndpoint {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        if 1 <= version.0 {
            self.host.encode(buf, version, is_flexible)?;
        }
        if 1 <= version.0 {
            self.port.encode(buf, version, is_flexible)?;
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
        let host = if 1 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            String::new()
        };
        let port = if 1 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            0
        };
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self { host, port })
    }
}

impl KafkaCodec for PartitionData {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.partition_index.encode(buf, version, is_flexible)?;
        if 1 <= version.0 {
            self.voter_directory_id.encode(buf, version, is_flexible)?;
        }
        self.leader_id.encode(buf, version, is_flexible)?;
        self.leader_epoch.encode(buf, version, is_flexible)?;
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
        let partition_index = KafkaCodec::decode(buf, version, is_flexible)?;
        let voter_directory_id = if 1 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            [0u8; 16]
        };
        let leader_id = KafkaCodec::decode(buf, version, is_flexible)?;
        let leader_epoch = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            partition_index,
            voter_directory_id,
            leader_id,
            leader_epoch,
        })
    }
}

impl KafkaCodec for TopicData {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.topic_name.encode(buf, version, is_flexible)?;
        self.partitions.encode(buf, version, is_flexible)?;
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
        let topic_name = KafkaCodec::decode(buf, version, is_flexible)?;
        let partitions = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            topic_name,
            partitions,
        })
    }
}

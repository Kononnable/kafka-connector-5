#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{
    KafkaDeserialize, KafkaSerialize, decode_unsigned_varint, encode_unsigned_varint,
};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVer, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// UpdateRaftVoterRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct UpdateRaftVoterRequest {
    /// The cluster id.
    pub cluster_id: Option<String>,
    /// The current leader epoch of the partition, -1 for unknown leader epoch.
    pub current_leader_epoch: i32,
    /// The replica id of the voter getting updated in the topic partition.
    pub voter_id: i32,
    /// The directory id of the voter getting updated in the topic partition.
    pub voter_directory_id: [u8; 16],
    /// The endpoint that can be used to communicate with the leader.
    pub listeners: Vec<Listener>,
    /// The range of versions of the protocol that the replica supports.
    pub kraft_version_feature: KRaftVersionFeature,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct KRaftVersionFeature {
    /// The minimum supported KRaft protocol version.
    pub min_supported_version: i16,
    /// The maximum supported KRaft protocol version.
    pub max_supported_version: i16,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Listener {
    /// The name of the endpoint.
    pub name: String,
    /// The hostname.
    pub host: String,
    /// The port.
    pub port: u16,
}

impl ApiRequest for UpdateRaftVoterRequest {
    type Response = crate::generated::UpdateRaftVoterResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(82)
    }
    fn get_min_supported_version() -> ApiVer {
        ApiVer::new(0)
    }
    fn get_max_supported_version() -> ApiVer {
        ApiVer::new(0)
    }
    fn get_min_flexible_version() -> ApiVer {
        ApiVer::new(0)
    }
    fn serialize(&self, version: ApiVer, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            (0) <= version.0 && version.0 <= (0),
            "version {} is not supported by {} (supported: 0-0)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        self.cluster_id.encode(buf, version, is_flexible)?;
        self.current_leader_epoch
            .encode(buf, version, is_flexible)?;
        self.voter_id.encode(buf, version, is_flexible)?;
        self.voter_directory_id.encode(buf, version, is_flexible)?;
        self.listeners.encode(buf, version, is_flexible)?;
        self.kraft_version_feature
            .encode(buf, version, is_flexible)?;
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(version: ApiVer, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let cluster_id = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let current_leader_epoch = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let voter_id = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let voter_directory_id = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let listeners = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let kraft_version_feature = KafkaDeserialize::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            cluster_id,
            current_leader_epoch,
            voter_id,
            voter_directory_id,
            listeners,
            kraft_version_feature,
        })
    }
}
impl KafkaSerialize for UpdateRaftVoterRequest {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.cluster_id.encode(buf, version, is_flexible)?;
        self.current_leader_epoch
            .encode(buf, version, is_flexible)?;
        self.voter_id.encode(buf, version, is_flexible)?;
        self.voter_directory_id.encode(buf, version, is_flexible)?;
        self.listeners.encode(buf, version, is_flexible)?;
        self.kraft_version_feature
            .encode(buf, version, is_flexible)?;
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for UpdateRaftVoterRequest {
    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let cluster_id = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let current_leader_epoch = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let voter_id = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let voter_directory_id = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let listeners = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let kraft_version_feature = KafkaDeserialize::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            cluster_id,
            current_leader_epoch,
            voter_id,
            voter_directory_id,
            listeners,
            kraft_version_feature,
        })
    }
}

impl KafkaSerialize for KRaftVersionFeature {
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
}

impl KafkaDeserialize for KRaftVersionFeature {
    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let min_supported_version = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let max_supported_version = KafkaDeserialize::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            min_supported_version,
            max_supported_version,
        })
    }
}

impl KafkaSerialize for Listener {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.name.encode(buf, version, is_flexible)?;
        self.host.encode(buf, version, is_flexible)?;
        self.port.encode(buf, version, is_flexible)?;
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for Listener {
    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let name = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let host = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let port = KafkaDeserialize::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self { name, host, port })
    }
}

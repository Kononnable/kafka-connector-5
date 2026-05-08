#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
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
        self.cluster_id.encode(buf, version, is_flexible)?;
        self.current_leader_epoch
            .encode(buf, version, is_flexible)?;
        self.voter_id.encode(buf, version, is_flexible)?;
        self.voter_directory_id.encode(buf, version, is_flexible)?;
        self.listeners.encode(buf, version, is_flexible)?;
        self.kraft_version_feature
            .encode(buf, version, is_flexible)?;
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
        let current_leader_epoch = <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let voter_id = <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let voter_directory_id = <[u8; 16] as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let listeners = <Vec<Listener> as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let kraft_version_feature =
            <KRaftVersionFeature as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
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
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), crate::traits::SerializationError> {
        self.cluster_id.encode(buf, version, is_flexible)?;
        self.current_leader_epoch
            .encode(buf, version, is_flexible)?;
        self.voter_id.encode(buf, version, is_flexible)?;
        self.voter_directory_id.encode(buf, version, is_flexible)?;
        self.listeners.encode(buf, version, is_flexible)?;
        self.kraft_version_feature
            .encode(buf, version, is_flexible)?;
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for UpdateRaftVoterRequest {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, crate::traits::SerializationError> {
        let cluster_id = <Option<String> as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let current_leader_epoch = <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let voter_id = <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let voter_directory_id = <[u8; 16] as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let listeners = <Vec<Listener> as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let kraft_version_feature =
            <KRaftVersionFeature as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
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
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), crate::traits::SerializationError> {
        self.min_supported_version
            .encode(buf, version, is_flexible)?;
        self.max_supported_version
            .encode(buf, version, is_flexible)?;
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for KRaftVersionFeature {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, crate::traits::SerializationError> {
        let min_supported_version = <i16 as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let max_supported_version = <i16 as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
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
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), crate::traits::SerializationError> {
        self.name.encode(buf, version, is_flexible)?;
        self.host.encode(buf, version, is_flexible)?;
        self.port.encode(buf, version, is_flexible)?;
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for Listener {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, crate::traits::SerializationError> {
        let name = <String as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let host = <String as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let port = <u16 as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self { name, host, port })
    }
}

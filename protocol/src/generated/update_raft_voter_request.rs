#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
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
        let is_flexible = true;
        self.cluster_id
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode ClusterId"))?;
        self.current_leader_epoch
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode CurrentLeaderEpoch"))?;
        self.voter_id
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode VoterId"))?;
        self.voter_directory_id
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode VoterDirectoryId"))?;
        self.listeners
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode Listeners"))?;
        self.kraft_version_feature
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode KRaftVersionFeature"))?;
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
        let is_flexible = true;
        let cluster_id =
            <Option<String> as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode ClusterId"))?;
        let current_leader_epoch =
            <i32 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode CurrentLeaderEpoch"))?;
        let voter_id = <i32 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode VoterId"))?;
        let voter_directory_id =
            <[u8; 16] as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode VoterDirectoryId"))?;
        let listeners =
            <Vec<Listener> as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode Listeners"))?;
        let kraft_version_feature =
            <KRaftVersionFeature as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode KRaftVersionFeature"))?;
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
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.cluster_id
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ClusterId".into(),
            })?;
        self.current_leader_epoch
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode CurrentLeaderEpoch".into(),
            })?;
        self.voter_id
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode VoterId".into(),
            })?;
        self.voter_directory_id
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode VoterDirectoryId".into(),
            })?;
        self.listeners
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Listeners".into(),
            })?;
        self.kraft_version_feature
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode KRaftVersionFeature".into(),
            })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(
        &self,
        buf: &mut B,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        if is_flexible {
            if let Some(ref __val) = self.cluster_id {
                crate::protocol::serialization::encode_unsigned_varint(1u64, buf);
                __val
                    .encode_flexible(buf, true)
                    .map_err(|_| EncodeError::ValueTooLarge {
                        message: "failed to encode ClusterId".into(),
                    })?;
            } else {
                crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
            }
        } else {
            if let Some(ref __val) = self.cluster_id {
                __val.encode(buf).map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode ClusterId".into(),
                })?;
            }
        }
        self.current_leader_epoch
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode CurrentLeaderEpoch".into(),
            })?;
        self.voter_id
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode VoterId".into(),
            })?;
        self.voter_directory_id
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode VoterDirectoryId".into(),
            })?;
        self.listeners
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Listeners".into(),
            })?;
        self.kraft_version_feature
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode KRaftVersionFeature".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for UpdateRaftVoterRequest {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] classic decode field `ClusterId` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let cluster_id = <Option<String> as KafkaDeserialize>::decode(buf).map_err(|_| {
            DecodeError::Protocol {
                message: "failed to decode ClusterId".into(),
            }
        })?;
        tracing::trace!(
            "  [{}] classic decode field `CurrentLeaderEpoch` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let current_leader_epoch =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode CurrentLeaderEpoch".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `VoterId` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let voter_id =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode VoterId".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `VoterDirectoryId` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let voter_directory_id =
            <[u8; 16] as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode VoterDirectoryId".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `Listeners` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let listeners = <Vec<Listener> as KafkaDeserialize>::decode(buf).map_err(|_| {
            DecodeError::Protocol {
                message: "failed to decode Listeners".into(),
            }
        })?;
        tracing::trace!(
            "  [{}] classic decode field `KRaftVersionFeature` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let kraft_version_feature = <KRaftVersionFeature as KafkaDeserialize>::decode(buf)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode KRaftVersionFeature".into(),
            })?;
        Ok(Self {
            cluster_id,
            current_leader_epoch,
            voter_id,
            voter_directory_id,
            listeners,
            kraft_version_feature,
        })
    }
    fn decode_flexible<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] decoding field `ClusterId` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let cluster_id = if is_flexible {
            <Option<String> as KafkaDeserialize>::decode_flexible(buf, version, true).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode ClusterId".into(),
                },
            )?
        } else {
            <Option<String> as KafkaDeserialize>::decode(buf).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode ClusterId".into(),
                }
            })?
        };
        tracing::trace!(
            "  [{}] decoding field `CurrentLeaderEpoch` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let current_leader_epoch =
            <i32 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode CurrentLeaderEpoch".into(),
                },
            )?;
        tracing::trace!(
            "  [{}] decoding field `VoterId` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let voter_id = <i32 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode VoterId".into(),
            })?;
        tracing::trace!(
            "  [{}] decoding field `VoterDirectoryId` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let voter_directory_id =
            <[u8; 16] as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode VoterDirectoryId".into(),
                },
            )?;
        tracing::trace!(
            "  [{}] decoding field `Listeners` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let listeners =
            <Vec<Listener> as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
                .map_err(|_| DecodeError::Protocol {
                    message: "failed to decode Listeners".into(),
                })?;
        tracing::trace!(
            "  [{}] decoding field `KRaftVersionFeature` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let kraft_version_feature =
            <KRaftVersionFeature as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
                .map_err(|_| DecodeError::Protocol {
                message: "failed to decode KRaftVersionFeature".into(),
            })?;
        if is_flexible {
            // Tagged fields (skip)
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
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.min_supported_version
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode MinSupportedVersion".into(),
            })?;
        self.max_supported_version
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode MaxSupportedVersion".into(),
            })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(
        &self,
        buf: &mut B,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.min_supported_version
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode MinSupportedVersion".into(),
            })?;
        self.max_supported_version
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode MaxSupportedVersion".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for KRaftVersionFeature {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] classic decode field `MinSupportedVersion` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let min_supported_version =
            <i16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode MinSupportedVersion".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `MaxSupportedVersion` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let max_supported_version =
            <i16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode MaxSupportedVersion".into(),
            })?;
        Ok(Self {
            min_supported_version,
            max_supported_version,
        })
    }
    fn decode_flexible<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] decoding field `MinSupportedVersion` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let min_supported_version =
            <i16 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode MinSupportedVersion".into(),
                },
            )?;
        tracing::trace!(
            "  [{}] decoding field `MaxSupportedVersion` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let max_supported_version =
            <i16 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode MaxSupportedVersion".into(),
                },
            )?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            min_supported_version,
            max_supported_version,
        })
    }
}

impl KafkaSerialize for Listener {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.name
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Name".into(),
            })?;
        self.host
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Host".into(),
            })?;
        self.port
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Port".into(),
            })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(
        &self,
        buf: &mut B,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.name
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Name".into(),
            })?;
        self.host
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Host".into(),
            })?;
        self.port
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Port".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for Listener {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] classic decode field `Name` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let name =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode Name".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `Host` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let host =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode Host".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `Port` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let port = <u16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
            message: "failed to decode Port".into(),
        })?;
        Ok(Self { name, host, port })
    }
    fn decode_flexible<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] decoding field `Name` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let name = <String as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode Name".into(),
            })?;
        tracing::trace!(
            "  [{}] decoding field `Host` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let host = <String as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode Host".into(),
            })?;
        tracing::trace!(
            "  [{}] decoding field `Port` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let port = <u16 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(
            |_| DecodeError::Protocol {
                message: "failed to decode Port".into(),
            },
        )?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self { name, host, port })
    }
}

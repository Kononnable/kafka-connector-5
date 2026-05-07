#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// AlterReplicaLogDirsRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct AlterReplicaLogDirsRequest {
    /// The alterations to make for each directory.
    pub dirs: Vec<AlterReplicaLogDir>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct AlterReplicaLogDir {
    /// The absolute directory path.
    pub path: String,
    /// The topics to add to the directory.
    pub topics: Vec<AlterReplicaLogDirTopic>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct AlterReplicaLogDirTopic {
    /// The topic name.
    pub name: String,
    /// The partition indexes.
    pub partitions: Vec<i32>,
}

impl ApiRequest for AlterReplicaLogDirsRequest {
    type Response = crate::generated::AlterReplicaLogDirsResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(34)
    }
    fn get_min_supported_version() -> ApiVersion {
        ApiVersion::new(0)
    }
    fn get_max_supported_version() -> ApiVersion {
        ApiVersion::new(1)
    }
    fn serialize(&self, version: ApiVersion, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            (0) <= version.0 && version.0 <= (1),
            "version {} is not supported by {} (supported: 0-1)",
            version.0,
            stringify!(Self)
        );
        self.dirs
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode Dirs"))?;
        Ok(())
    }
    fn deserialize(version: ApiVersion, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let dirs = <Vec<AlterReplicaLogDir> as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode Dirs"))?;
        Ok(Self { dirs })
    }
}
impl KafkaSerialize for AlterReplicaLogDirsRequest {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.dirs
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Dirs".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for AlterReplicaLogDirsRequest {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let dirs = <Vec<AlterReplicaLogDir> as KafkaDeserialize>::decode(buf).map_err(|_| {
            DecodeError::Protocol {
                message: "failed to decode Dirs".into(),
            }
        })?;
        Ok(Self { dirs })
    }
}

impl KafkaSerialize for AlterReplicaLogDir {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.path
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Path".into(),
            })?;
        self.topics
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Topics".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for AlterReplicaLogDir {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let path =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode Path".into(),
            })?;
        let topics =
            <Vec<AlterReplicaLogDirTopic> as KafkaDeserialize>::decode(buf).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode Topics".into(),
                }
            })?;
        Ok(Self { path, topics })
    }
}

impl KafkaSerialize for AlterReplicaLogDirTopic {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.name
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Name".into(),
            })?;
        self.partitions
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Partitions".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for AlterReplicaLogDirTopic {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let name =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode Name".into(),
            })?;
        let partitions =
            <Vec<i32> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode Partitions".into(),
            })?;
        Ok(Self { name, partitions })
    }
}

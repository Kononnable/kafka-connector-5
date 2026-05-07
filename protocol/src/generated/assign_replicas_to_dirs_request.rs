#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{EncodeError, DecodeError, KafkaSerialize, KafkaDeserialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// AssignReplicasToDirsRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct AssignReplicasToDirsRequest {
    /// The ID of the requesting broker.
    pub broker_id: i32,
    /// The epoch of the requesting broker.
    pub broker_epoch: i64,
    /// The directories to which replicas should be assigned.
    pub directories: Vec<DirectoryData>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct DirectoryData {
    /// The ID of the directory.
    pub id: [u8; 16],
    /// The topics assigned to the directory.
    pub topics: Vec<TopicData>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct PartitionData {
    /// The partition index.
    pub partition_index: i32,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct TopicData {
    /// The ID of the assigned topic.
    pub topic_id: [u8; 16],
    /// The partitions assigned to the directory.
    pub partitions: Vec<PartitionData>,
}

impl ApiRequest for AssignReplicasToDirsRequest {
    type Response = crate::generated::AssignReplicasToDirsResponse;
    fn get_api_key() -> ApiKey { ApiKey::new(73) }
    fn get_min_supported_version() -> crate::traits::ApiVersion { crate::traits::ApiVersion::new(0) }
    fn get_max_supported_version() -> crate::traits::ApiVersion { crate::traits::ApiVersion::new(0) }
    fn serialize(&self, version: crate::traits::ApiVersion, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!((0) <= version.0 && version.0 <= (0), "version {} is not supported by {} (supported: 0-0)", version.0, stringify!(Self));
        let is_flexible = true;
        self.broker_id.encode_flexible(buf, is_flexible).map_err(|_| SerializationError::Encode("failed to encode BrokerId"))?;
        self.broker_epoch.encode_flexible(buf, is_flexible).map_err(|_| SerializationError::Encode("failed to encode BrokerEpoch"))?;
        self.directories.encode_flexible(buf, is_flexible).map_err(|_| SerializationError::Encode("failed to encode Directories"))?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(version: crate::traits::ApiVersion, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let is_flexible = true;
        let broker_id = <i32 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| SerializationError::Decode("failed to decode BrokerId"))?;
        let broker_epoch = <i64 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| SerializationError::Decode("failed to decode BrokerEpoch"))?;
        let directories = <Vec<DirectoryData> as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| SerializationError::Decode("failed to decode Directories"))?;
        Ok(Self { broker_id, broker_epoch, directories })
    }
}
impl KafkaSerialize for AssignReplicasToDirsRequest {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.broker_id.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode BrokerId".into() })?;
        self.broker_epoch.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode BrokerEpoch".into() })?;
        self.directories.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Directories".into() })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(&self, buf: &mut B, is_flexible: bool) -> Result<(), EncodeError> {
        self.broker_id.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode BrokerId".into() })?;
        self.broker_epoch.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode BrokerEpoch".into() })?;
        self.directories.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Directories".into() })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for AssignReplicasToDirsRequest {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let broker_id = <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode BrokerId".into() })?;
        let broker_epoch = <i64 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode BrokerEpoch".into() })?;
        let directories = <Vec<DirectoryData> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode Directories".into() })?;
        Ok(Self { broker_id, broker_epoch, directories })
    }
    fn decode_flexible<B: Buf>(buf: &mut B, is_flexible: bool) -> Result<Self, DecodeError> {
        let broker_id = <i32 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode BrokerId".into() })?;
        let broker_epoch = <i64 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode BrokerEpoch".into() })?;
        let directories = <Vec<DirectoryData> as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode Directories".into() })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self { broker_id, broker_epoch, directories })
    }
}

impl KafkaSerialize for DirectoryData {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.id.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Id".into() })?;
        self.topics.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Topics".into() })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(&self, buf: &mut B, is_flexible: bool) -> Result<(), EncodeError> {
        self.id.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Id".into() })?;
        self.topics.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Topics".into() })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for DirectoryData {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let id = <[u8; 16] as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode Id".into() })?;
        let topics = <Vec<TopicData> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode Topics".into() })?;
        Ok(Self { id, topics })
    }
    fn decode_flexible<B: Buf>(buf: &mut B, is_flexible: bool) -> Result<Self, DecodeError> {
        let id = <[u8; 16] as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode Id".into() })?;
        let topics = <Vec<TopicData> as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode Topics".into() })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self { id, topics })
    }
}

impl KafkaSerialize for PartitionData {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.partition_index.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode PartitionIndex".into() })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(&self, buf: &mut B, is_flexible: bool) -> Result<(), EncodeError> {
        self.partition_index.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode PartitionIndex".into() })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for PartitionData {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let partition_index = <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode PartitionIndex".into() })?;
        Ok(Self { partition_index })
    }
    fn decode_flexible<B: Buf>(buf: &mut B, is_flexible: bool) -> Result<Self, DecodeError> {
        let partition_index = <i32 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode PartitionIndex".into() })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self { partition_index })
    }
}

impl KafkaSerialize for TopicData {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.topic_id.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode TopicId".into() })?;
        self.partitions.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Partitions".into() })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(&self, buf: &mut B, is_flexible: bool) -> Result<(), EncodeError> {
        self.topic_id.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode TopicId".into() })?;
        self.partitions.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Partitions".into() })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for TopicData {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let topic_id = <[u8; 16] as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode TopicId".into() })?;
        let partitions = <Vec<PartitionData> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode Partitions".into() })?;
        Ok(Self { topic_id, partitions })
    }
    fn decode_flexible<B: Buf>(buf: &mut B, is_flexible: bool) -> Result<Self, DecodeError> {
        let topic_id = <[u8; 16] as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode TopicId".into() })?;
        let partitions = <Vec<PartitionData> as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode Partitions".into() })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self { topic_id, partitions })
    }
}


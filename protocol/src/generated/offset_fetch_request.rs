#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// OffsetFetchRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct OffsetFetchRequest {
    /// The group to fetch offsets for.
    pub group_id: String,
    /// Each topic we would like to fetch offsets for, or null to fetch offsets for all topics.
    pub topics: Option<Vec<OffsetFetchRequestTopic>>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct OffsetFetchRequestTopic {
    /// The topic name.
    pub name: String,
    /// The partition indexes we would like to fetch offsets for.
    pub partition_indexes: Vec<i32>,
}

impl ApiRequest for OffsetFetchRequest {
    type Response = crate::generated::OffsetFetchResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(9)
    }
    fn get_min_supported_version() -> ApiVersion {
        ApiVersion::new(0)
    }
    fn get_max_supported_version() -> ApiVersion {
        ApiVersion::new(6)
    }
    fn serialize(&self, version: ApiVersion, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            (0) <= version.0 && version.0 <= (6),
            "version {} is not supported by {} (supported: 0-6)",
            version.0,
            stringify!(Self)
        );
        self.group_id
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode GroupId"))?;
        self.topics
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode Topics"))?;
        Ok(())
    }
    fn deserialize(version: ApiVersion, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let group_id = <String as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode GroupId"))?;
        let topics = <Option<Vec<OffsetFetchRequestTopic>> as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode Topics"))?;
        Ok(Self { group_id, topics })
    }
}
impl KafkaSerialize for OffsetFetchRequest {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.group_id
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode GroupId".into(),
            })?;
        self.topics
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Topics".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for OffsetFetchRequest {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let group_id =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode GroupId".into(),
            })?;
        let topics = <Option<Vec<OffsetFetchRequestTopic>> as KafkaDeserialize>::decode(buf)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode Topics".into(),
            })?;
        Ok(Self { group_id, topics })
    }
}

impl KafkaSerialize for OffsetFetchRequestTopic {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.name
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Name".into(),
            })?;
        self.partition_indexes
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode PartitionIndexes".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for OffsetFetchRequestTopic {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let name =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode Name".into(),
            })?;
        let partition_indexes =
            <Vec<i32> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode PartitionIndexes".into(),
            })?;
        Ok(Self {
            name,
            partition_indexes,
        })
    }
}

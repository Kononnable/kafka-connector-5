#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// ListPartitionReassignmentsRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ListPartitionReassignmentsRequest {
    /// The time in ms to wait for the request to complete.
    pub timeout_ms: i32,
    /// The topics to list partition reassignments for, or null to list everything.
    pub topics: Option<Vec<ListPartitionReassignmentsTopics>>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ListPartitionReassignmentsTopics {
    /// The topic name
    pub name: String,
    /// The partitions to list partition reassignments for.
    pub partition_indexes: Vec<i32>,
}

impl ApiRequest for ListPartitionReassignmentsRequest {
    type Response = crate::generated::ListPartitionReassignmentsResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(46)
    }
    fn get_min_supported_version() -> ApiVersion {
        ApiVersion::new(0)
    }
    fn get_max_supported_version() -> ApiVersion {
        ApiVersion::new(0)
    }
    fn serialize(&self, version: ApiVersion, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            (0) <= version.0 && version.0 <= (0),
            "version {} is not supported by {} (supported: 0-0)",
            version.0,
            stringify!(Self)
        );
        self.timeout_ms
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode TimeoutMs"))?;
        self.topics
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode Topics"))?;
        Ok(())
    }
    fn deserialize(version: ApiVersion, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let timeout_ms = <i32 as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode TimeoutMs"))?;
        let topics =
            <Option<Vec<ListPartitionReassignmentsTopics>> as KafkaDeserialize>::decode(buf)
                .map_err(|_| SerializationError::Decode("failed to decode Topics"))?;
        Ok(Self { timeout_ms, topics })
    }
}
impl KafkaSerialize for ListPartitionReassignmentsRequest {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.timeout_ms
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode TimeoutMs".into(),
            })?;
        self.topics
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Topics".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for ListPartitionReassignmentsRequest {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let timeout_ms =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode TimeoutMs".into(),
            })?;
        let topics =
            <Option<Vec<ListPartitionReassignmentsTopics>> as KafkaDeserialize>::decode(buf)
                .map_err(|_| DecodeError::Protocol {
                    message: "failed to decode Topics".into(),
                })?;
        Ok(Self { timeout_ms, topics })
    }
}

impl KafkaSerialize for ListPartitionReassignmentsTopics {
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

impl KafkaDeserialize for ListPartitionReassignmentsTopics {
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

#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// CreatePartitionsRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct CreatePartitionsRequest {
    /// Each topic that we want to create new partitions inside.
    pub topics: Vec<CreatePartitionsTopic>,
    /// The time in ms to wait for the partitions to be created.
    pub timeout_ms: i32,
    /// If true, then validate the request, but don't actually increase the number of partitions.
    pub validate_only: bool,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct CreatePartitionsAssignment {
    /// The assigned broker IDs.
    pub broker_ids: Vec<i32>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct CreatePartitionsTopic {
    /// The topic name.
    pub name: String,
    /// The new partition count.
    pub count: i32,
    /// The new partition assignments.
    pub assignments: Option<Vec<CreatePartitionsAssignment>>,
}

impl ApiRequest for CreatePartitionsRequest {
    type Response = crate::generated::CreatePartitionsResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(37)
    }
    fn get_min_supported_version() -> ApiVersion {
        ApiVersion::new(0)
    }
    fn get_max_supported_version() -> ApiVersion {
        ApiVersion::new(3)
    }
    fn serialize(&self, version: ApiVersion, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            (0) <= version.0 && version.0 <= (3),
            "version {} is not supported by {} (supported: 0-3)",
            version.0,
            stringify!(Self)
        );
        self.topics
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode Topics"))?;
        self.timeout_ms
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode TimeoutMs"))?;
        self.validate_only
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode ValidateOnly"))?;
        Ok(())
    }
    fn deserialize(version: ApiVersion, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let topics = <Vec<CreatePartitionsTopic> as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode Topics"))?;
        let timeout_ms = <i32 as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode TimeoutMs"))?;
        let validate_only = <bool as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode ValidateOnly"))?;
        Ok(Self {
            topics,
            timeout_ms,
            validate_only,
        })
    }
}
impl KafkaSerialize for CreatePartitionsRequest {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.topics
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Topics".into(),
            })?;
        self.timeout_ms
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode TimeoutMs".into(),
            })?;
        self.validate_only
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ValidateOnly".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for CreatePartitionsRequest {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let topics =
            <Vec<CreatePartitionsTopic> as KafkaDeserialize>::decode(buf).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode Topics".into(),
                }
            })?;
        let timeout_ms =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode TimeoutMs".into(),
            })?;
        let validate_only =
            <bool as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ValidateOnly".into(),
            })?;
        Ok(Self {
            topics,
            timeout_ms,
            validate_only,
        })
    }
}

impl KafkaSerialize for CreatePartitionsAssignment {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.broker_ids
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode BrokerIds".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for CreatePartitionsAssignment {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let broker_ids =
            <Vec<i32> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode BrokerIds".into(),
            })?;
        Ok(Self { broker_ids })
    }
}

impl KafkaSerialize for CreatePartitionsTopic {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.name
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Name".into(),
            })?;
        self.count
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Count".into(),
            })?;
        self.assignments
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Assignments".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for CreatePartitionsTopic {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let name =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode Name".into(),
            })?;
        let count = <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
            message: "failed to decode Count".into(),
        })?;
        let assignments = <Option<Vec<CreatePartitionsAssignment>> as KafkaDeserialize>::decode(
            buf,
        )
        .map_err(|_| DecodeError::Protocol {
            message: "failed to decode Assignments".into(),
        })?;
        Ok(Self {
            name,
            count,
            assignments,
        })
    }
}

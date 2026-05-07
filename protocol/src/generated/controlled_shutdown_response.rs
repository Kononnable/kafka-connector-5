#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// ControlledShutdownResponse
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ControlledShutdownResponse {
    /// The top-level error code.
    pub error_code: i16,
    /// The partitions that the broker still leads.
    pub remaining_partitions: Vec<RemainingPartition>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct RemainingPartition {
    /// The name of the topic.
    pub topic_name: String,
    /// The index of the partition.
    pub partition_index: i32,
}

impl ApiResponse for ControlledShutdownResponse {
    type Request = crate::generated::ControlledShutdownRequest;
    fn get_api_key() -> ApiKey {
        ApiKey::new(7)
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
        self.error_code
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode ErrorCode"))?;
        self.remaining_partitions
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode RemainingPartitions"))?;
        Ok(())
    }
    fn deserialize(version: ApiVersion, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let error_code = <i16 as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode ErrorCode"))?;
        let remaining_partitions = <Vec<RemainingPartition> as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode RemainingPartitions"))?;
        Ok(Self {
            error_code,
            remaining_partitions,
        })
    }
}
impl KafkaSerialize for ControlledShutdownResponse {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.error_code
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ErrorCode".into(),
            })?;
        self.remaining_partitions
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode RemainingPartitions".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for ControlledShutdownResponse {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let error_code =
            <i16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ErrorCode".into(),
            })?;
        let remaining_partitions = <Vec<RemainingPartition> as KafkaDeserialize>::decode(buf)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode RemainingPartitions".into(),
            })?;
        Ok(Self {
            error_code,
            remaining_partitions,
        })
    }
}

impl KafkaSerialize for RemainingPartition {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.topic_name
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode TopicName".into(),
            })?;
        self.partition_index
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode PartitionIndex".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for RemainingPartition {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let topic_name =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode TopicName".into(),
            })?;
        let partition_index =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode PartitionIndex".into(),
            })?;
        Ok(Self {
            topic_name,
            partition_index,
        })
    }
}

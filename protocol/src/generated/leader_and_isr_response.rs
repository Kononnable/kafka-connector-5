#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// LeaderAndIsrResponse
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct LeaderAndIsrResponse {
    /// The error code, or 0 if there was no error.
    pub error_code: i16,
    /// Each partition.
    pub partitions: Vec<LeaderAndIsrResponsePartition>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct LeaderAndIsrResponsePartition {
    /// The topic name.
    pub topic_name: String,
    /// The partition index.
    pub partition_index: i32,
    /// The partition error code, or 0 if there was no error.
    pub error_code: i16,
}

impl ApiResponse for LeaderAndIsrResponse {
    type Request = crate::generated::LeaderAndIsrRequest;
    fn get_api_key() -> ApiKey {
        ApiKey::new(4)
    }
    fn get_min_supported_version() -> ApiVersion {
        ApiVersion::new(0)
    }
    fn get_max_supported_version() -> ApiVersion {
        ApiVersion::new(2)
    }
    fn serialize(&self, version: ApiVersion, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            (0) <= version.0 && version.0 <= (2),
            "version {} is not supported by {} (supported: 0-2)",
            version.0,
            stringify!(Self)
        );
        self.error_code
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode ErrorCode"))?;
        self.partitions
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode Partitions"))?;
        Ok(())
    }
    fn deserialize(version: ApiVersion, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let error_code = <i16 as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode ErrorCode"))?;
        let partitions = <Vec<LeaderAndIsrResponsePartition> as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode Partitions"))?;
        Ok(Self {
            error_code,
            partitions,
        })
    }
}
impl KafkaSerialize for LeaderAndIsrResponse {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.error_code
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ErrorCode".into(),
            })?;
        self.partitions
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Partitions".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for LeaderAndIsrResponse {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let error_code =
            <i16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ErrorCode".into(),
            })?;
        let partitions = <Vec<LeaderAndIsrResponsePartition> as KafkaDeserialize>::decode(buf)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode Partitions".into(),
            })?;
        Ok(Self {
            error_code,
            partitions,
        })
    }
}

impl KafkaSerialize for LeaderAndIsrResponsePartition {
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
        self.error_code
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ErrorCode".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for LeaderAndIsrResponsePartition {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let topic_name =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode TopicName".into(),
            })?;
        let partition_index =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode PartitionIndex".into(),
            })?;
        let error_code =
            <i16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ErrorCode".into(),
            })?;
        Ok(Self {
            topic_name,
            partition_index,
            error_code,
        })
    }
}

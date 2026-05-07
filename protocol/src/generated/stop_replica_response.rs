#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// StopReplicaResponse
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct StopReplicaResponse {
    /// The top-level error code, or 0 if there was no top-level error.
    pub error_code: i16,
    /// The responses for each partition.
    pub partition_errors: Vec<StopReplicaPartitionError>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct StopReplicaPartitionError {
    /// The topic name.
    pub topic_name: String,
    /// The partition index.
    pub partition_index: i32,
    /// The partition error code, or 0 if there was no partition error.
    pub error_code: i16,
}

impl ApiResponse for StopReplicaResponse {
    type Request = crate::generated::StopReplicaRequest;
    fn get_api_key() -> ApiKey {
        ApiKey::new(5)
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
        self.partition_errors
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode PartitionErrors"))?;
        Ok(())
    }
    fn deserialize(version: ApiVersion, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let error_code = <i16 as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode ErrorCode"))?;
        let partition_errors = <Vec<StopReplicaPartitionError> as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode PartitionErrors"))?;
        Ok(Self {
            error_code,
            partition_errors,
        })
    }
}
impl KafkaSerialize for StopReplicaResponse {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.error_code
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ErrorCode".into(),
            })?;
        self.partition_errors
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode PartitionErrors".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for StopReplicaResponse {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let error_code =
            <i16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ErrorCode".into(),
            })?;
        let partition_errors = <Vec<StopReplicaPartitionError> as KafkaDeserialize>::decode(buf)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode PartitionErrors".into(),
            })?;
        Ok(Self {
            error_code,
            partition_errors,
        })
    }
}

impl KafkaSerialize for StopReplicaPartitionError {
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

impl KafkaDeserialize for StopReplicaPartitionError {
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

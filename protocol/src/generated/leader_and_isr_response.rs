#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// LeaderAndIsrResponse
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct LeaderAndIsrResponse {
    /// The error code, or 0 if there was no error.
    pub error_code: i16,
    /// Each partition in v0 to v4 message.
    /// Available in version 0-4.
    pub partition_errors: Vec<LeaderAndIsrPartitionError>,
    /// Each topic
    /// Available in version 5+.
    pub topics: Vec<LeaderAndIsrTopicError>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct LeaderAndIsrPartitionError {
    /// The topic name.
    /// Available in version 0-4.
    pub topic_name: String,
    /// The partition index.
    pub partition_index: i32,
    /// The partition error code, or 0 if there was no error.
    pub error_code: i16,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct LeaderAndIsrTopicError {
    /// The unique topic ID
    /// Available in version 5+.
    pub topic_id: [u8; 16],
    /// Each partition.
    /// Available in version 5+.
    pub partition_errors: Vec<LeaderAndIsrPartitionError>,
}

impl ApiResponse for LeaderAndIsrResponse {
    type Request = crate::generated::LeaderAndIsrRequest;
    fn get_api_key() -> ApiKey {
        ApiKey::new(4)
    }
    fn get_min_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(0)
    }
    fn get_max_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(5)
    }
    fn serialize(
        &self,
        version: crate::traits::ApiVersion,
        buf: &mut BytesMut,
    ) -> Result<(), SerializationError> {
        assert!(
            (0) <= version.0 && version.0 <= (5),
            "version {} is not supported by {} (supported: 0-5)",
            version.0,
            stringify!(Self)
        );
        self.error_code
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode ErrorCode"))?;
        if (0) <= version.0 && version.0 <= (4) {
            self.partition_errors
                .encode(buf)
                .map_err(|_| SerializationError::Encode("failed to encode PartitionErrors"))?;
        }
        if (5) <= version.0 {
            self.topics
                .encode(buf)
                .map_err(|_| SerializationError::Encode("failed to encode Topics"))?;
        }
        Ok(())
    }
    fn deserialize(
        version: crate::traits::ApiVersion,
        buf: &mut Bytes,
    ) -> Result<Self, SerializationError> {
        let error_code = <i16 as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode ErrorCode"))?;
        let partition_errors = if (0) <= version.0 && version.0 <= (4) {
            <Vec<LeaderAndIsrPartitionError> as KafkaDeserialize>::decode(buf)
                .map_err(|_| SerializationError::Decode("failed to decode PartitionErrors"))?
        } else {
            Default::default()
        };
        let topics = if (5) <= version.0 {
            <Vec<LeaderAndIsrTopicError> as KafkaDeserialize>::decode(buf)
                .map_err(|_| SerializationError::Decode("failed to decode Topics"))?
        } else {
            Default::default()
        };
        Ok(Self {
            error_code,
            partition_errors,
            topics,
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
        self.partition_errors
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode PartitionErrors".into(),
            })?;
        self.topics
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Topics".into(),
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
        let partition_errors = <Vec<LeaderAndIsrPartitionError> as KafkaDeserialize>::decode(buf)
            .map_err(|_| DecodeError::Protocol {
            message: "failed to decode PartitionErrors".into(),
        })?;
        let topics =
            <Vec<LeaderAndIsrTopicError> as KafkaDeserialize>::decode(buf).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode Topics".into(),
                }
            })?;
        Ok(Self {
            error_code,
            partition_errors,
            topics,
        })
    }
}

impl KafkaSerialize for LeaderAndIsrPartitionError {
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

impl KafkaDeserialize for LeaderAndIsrPartitionError {
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

impl KafkaSerialize for LeaderAndIsrTopicError {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.topic_id
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode TopicId".into(),
            })?;
        self.partition_errors
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode PartitionErrors".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for LeaderAndIsrTopicError {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let topic_id =
            <[u8; 16] as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode TopicId".into(),
            })?;
        let partition_errors = <Vec<LeaderAndIsrPartitionError> as KafkaDeserialize>::decode(buf)
            .map_err(|_| DecodeError::Protocol {
            message: "failed to decode PartitionErrors".into(),
        })?;
        Ok(Self {
            topic_id,
            partition_errors,
        })
    }
}

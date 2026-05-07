#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// ElectPreferredLeadersRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ElectPreferredLeadersRequest {
    /// The topic partitions to elect the preferred leader of.
    pub topic_partitions: Option<Vec<TopicPartitions>>,
    /// The time in ms to wait for the election to complete.
    pub timeout_ms: i32,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct TopicPartitions {
    /// The name of a topic.
    pub topic: String,
    /// The partitions of this topic whose preferred leader should be elected
    pub partition_id: Vec<i32>,
}

impl ApiRequest for ElectPreferredLeadersRequest {
    type Response = crate::generated::ElectPreferredLeadersResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(43)
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
        self.topic_partitions
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode TopicPartitions"))?;
        self.timeout_ms
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode TimeoutMs"))?;
        Ok(())
    }
    fn deserialize(version: ApiVersion, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let topic_partitions = <Option<Vec<TopicPartitions>> as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode TopicPartitions"))?;
        let timeout_ms = <i32 as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode TimeoutMs"))?;
        Ok(Self {
            topic_partitions,
            timeout_ms,
        })
    }
}
impl KafkaSerialize for ElectPreferredLeadersRequest {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.topic_partitions
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode TopicPartitions".into(),
            })?;
        self.timeout_ms
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode TimeoutMs".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for ElectPreferredLeadersRequest {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let topic_partitions = <Option<Vec<TopicPartitions>> as KafkaDeserialize>::decode(buf)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode TopicPartitions".into(),
            })?;
        let timeout_ms =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode TimeoutMs".into(),
            })?;
        Ok(Self {
            topic_partitions,
            timeout_ms,
        })
    }
}

impl KafkaSerialize for TopicPartitions {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.topic
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Topic".into(),
            })?;
        self.partition_id
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode PartitionId".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for TopicPartitions {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let topic =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode Topic".into(),
            })?;
        let partition_id =
            <Vec<i32> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode PartitionId".into(),
            })?;
        Ok(Self {
            topic,
            partition_id,
        })
    }
}

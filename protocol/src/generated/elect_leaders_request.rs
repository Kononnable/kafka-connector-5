#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// ElectLeadersRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ElectLeadersRequest {
    /// Type of elections to conduct for the partition. A value of '0' elects the preferred replica. A value of '1' elects the first live replica if there are no in-sync replica.
    /// Available in version 1+.
    pub election_type: i8,
    /// The topic partitions to elect leaders.
    pub topic_partitions: Option<Vec<TopicPartitions>>,
    /// The time in ms to wait for the election to complete.
    pub timeout_ms: i32,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct TopicPartitions {
    /// The name of a topic.
    pub topic: String,
    /// The partitions of this topic whose leader should be elected.
    pub partition_id: Vec<i32>,
}

impl ApiRequest for ElectLeadersRequest {
    type Response = crate::generated::ElectLeadersResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(43)
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
        if (1) <= version.0 {
            self.election_type
                .encode(buf)
                .map_err(|_| SerializationError::Encode("failed to encode ElectionType"))?;
        }
        self.topic_partitions
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode TopicPartitions"))?;
        self.timeout_ms
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode TimeoutMs"))?;
        Ok(())
    }
    fn deserialize(version: ApiVersion, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let election_type = if (1) <= version.0 {
            <i8 as KafkaDeserialize>::decode(buf)
                .map_err(|_| SerializationError::Decode("failed to decode ElectionType"))?
        } else {
            Default::default()
        };
        let topic_partitions = <Option<Vec<TopicPartitions>> as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode TopicPartitions"))?;
        let timeout_ms = <i32 as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode TimeoutMs"))?;
        Ok(Self {
            election_type,
            topic_partitions,
            timeout_ms,
        })
    }
}
impl KafkaSerialize for ElectLeadersRequest {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.election_type
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ElectionType".into(),
            })?;
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

impl KafkaDeserialize for ElectLeadersRequest {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let election_type =
            <i8 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ElectionType".into(),
            })?;
        let topic_partitions = <Option<Vec<TopicPartitions>> as KafkaDeserialize>::decode(buf)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode TopicPartitions".into(),
            })?;
        let timeout_ms =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode TimeoutMs".into(),
            })?;
        Ok(Self {
            election_type,
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

#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{EncodeError, DecodeError, KafkaSerialize, KafkaDeserialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
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
    pub partitions: Vec<i32>,
}

impl ApiRequest for ElectLeadersRequest {
    type Response = crate::generated::ElectLeadersResponse;
    fn get_api_key() -> ApiKey { ApiKey::new(43) }
    fn get_min_supported_version() -> crate::traits::ApiVersion { crate::traits::ApiVersion::new(0) }
    fn get_max_supported_version() -> crate::traits::ApiVersion { crate::traits::ApiVersion::new(2) }
    fn serialize(&self, version: crate::traits::ApiVersion, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!((0) <= version.0 && version.0 <= (2), "version {} is not supported by {} (supported: 0-2)", version.0, stringify!(Self));
        let is_flexible = (2) <= version.0;
        if (1) <= version.0 {
            self.election_type.encode_flexible(buf, is_flexible).map_err(|_| SerializationError::Encode("failed to encode ElectionType"))?;
        }
        self.topic_partitions.encode_flexible(buf, is_flexible).map_err(|_| SerializationError::Encode("failed to encode TopicPartitions"))?;
        self.timeout_ms.encode_flexible(buf, is_flexible).map_err(|_| SerializationError::Encode("failed to encode TimeoutMs"))?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(version: crate::traits::ApiVersion, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let is_flexible = (2) <= version.0;
        let election_type = if (1) <= version.0 {
            <i8 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| SerializationError::Decode("failed to decode ElectionType"))?
        } else {
            Default::default()
        };
        let topic_partitions = <Option<Vec<TopicPartitions>> as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| SerializationError::Decode("failed to decode TopicPartitions"))?;
        let timeout_ms = <i32 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| SerializationError::Decode("failed to decode TimeoutMs"))?;
        Ok(Self { election_type, topic_partitions, timeout_ms })
    }
}
impl KafkaSerialize for ElectLeadersRequest {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.election_type.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode ElectionType".into() })?;
        self.topic_partitions.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode TopicPartitions".into() })?;
        self.timeout_ms.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode TimeoutMs".into() })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(&self, buf: &mut B, is_flexible: bool) -> Result<(), EncodeError> {
        self.election_type.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode ElectionType".into() })?;
        if is_flexible {
            if let Some(ref __val) = self.topic_partitions {
                crate::protocol::serialization::encode_unsigned_varint(1u64, buf);
                __val.encode_flexible(buf, true).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode TopicPartitions".into() })?;
            } else {
                crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
            }
        } else {
            if let Some(ref __val) = self.topic_partitions {
                __val.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode TopicPartitions".into() })?;
            }
        }
        self.timeout_ms.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode TimeoutMs".into() })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for ElectLeadersRequest {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let election_type = <i8 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode ElectionType".into() })?;
        let topic_partitions = <Option<Vec<TopicPartitions>> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode TopicPartitions".into() })?;
        let timeout_ms = <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode TimeoutMs".into() })?;
        Ok(Self { election_type, topic_partitions, timeout_ms })
    }
    fn decode_flexible<B: Buf>(buf: &mut B, is_flexible: bool) -> Result<Self, DecodeError> {
        let election_type = <i8 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode ElectionType".into() })?;
        let topic_partitions = if is_flexible {
            let (__present, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
            if __present == 0 {
                None
            } else {
                Some(<Vec<TopicPartitions> as KafkaDeserialize>::decode_flexible(buf, true).map_err(|_| DecodeError::Protocol { message: "failed to decode TopicPartitions".into() })?)
            }
        } else {
            <Option<Vec<TopicPartitions>> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode TopicPartitions".into() })?
        };
        let timeout_ms = <i32 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode TimeoutMs".into() })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self { election_type, topic_partitions, timeout_ms })
    }
}

impl KafkaSerialize for TopicPartitions {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.topic.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Topic".into() })?;
        self.partitions.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Partitions".into() })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(&self, buf: &mut B, is_flexible: bool) -> Result<(), EncodeError> {
        self.topic.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Topic".into() })?;
        self.partitions.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Partitions".into() })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for TopicPartitions {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let topic = <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode Topic".into() })?;
        let partitions = <Vec<i32> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode Partitions".into() })?;
        Ok(Self { topic, partitions })
    }
    fn decode_flexible<B: Buf>(buf: &mut B, is_flexible: bool) -> Result<Self, DecodeError> {
        let topic = <String as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode Topic".into() })?;
        let partitions = <Vec<i32> as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode Partitions".into() })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self { topic, partitions })
    }
}


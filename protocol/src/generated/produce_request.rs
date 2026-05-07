#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// ProduceRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ProduceRequest {
    /// The transactional ID, or null if the producer is not transactional.
    /// Available in version 3+.
    pub transactional_id: Option<String>,
    /// The number of acknowledgments the producer requires the leader to have received before considering a request complete. Allowed values: 0 for no acknowledgments, 1 for only the leader and -1 for the full ISR.
    pub acks: i16,
    /// The timeout to await a response in miliseconds.
    pub timeout_ms: i32,
    /// Each topic to produce to.
    pub topic_data: Vec<TopicProduceData>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct PartitionProduceData {
    /// The partition index.
    pub index: i32,
    /// The record data to be produced.
    pub records: Option<Vec<u8>>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct TopicProduceData {
    /// The topic name.
    pub name: String,
    /// Each partition to produce to.
    pub partition_data: Vec<PartitionProduceData>,
}

impl ApiRequest for ProduceRequest {
    type Response = crate::generated::ProduceResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(0)
    }
    fn get_min_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(0)
    }
    fn get_max_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(9)
    }
    fn serialize(
        &self,
        version: crate::traits::ApiVersion,
        buf: &mut BytesMut,
    ) -> Result<(), SerializationError> {
        assert!(
            (0) <= version.0 && version.0 <= (9),
            "version {} is not supported by {} (supported: 0-9)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = (9) <= version.0;
        if (3) <= version.0 {
            self.transactional_id
                .encode_flexible(buf, is_flexible)
                .map_err(|_| SerializationError::Encode("failed to encode TransactionalId"))?;
        }
        self.acks
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode Acks"))?;
        self.timeout_ms
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode TimeoutMs"))?;
        self.topic_data
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode TopicData"))?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(
        version: crate::traits::ApiVersion,
        buf: &mut Bytes,
    ) -> Result<Self, SerializationError> {
        let is_flexible = (9) <= version.0;
        let transactional_id = if (3) <= version.0 {
            <Option<String> as KafkaDeserialize>::decode_flexible(buf, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode TransactionalId"))?
        } else {
            Default::default()
        };
        let acks = <i16 as KafkaDeserialize>::decode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode Acks"))?;
        let timeout_ms = <i32 as KafkaDeserialize>::decode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode TimeoutMs"))?;
        let topic_data =
            <Vec<TopicProduceData> as KafkaDeserialize>::decode_flexible(buf, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode TopicData"))?;
        Ok(Self {
            transactional_id,
            acks,
            timeout_ms,
            topic_data,
        })
    }
}
impl KafkaSerialize for ProduceRequest {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.transactional_id
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode TransactionalId".into(),
            })?;
        self.acks
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Acks".into(),
            })?;
        self.timeout_ms
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode TimeoutMs".into(),
            })?;
        self.topic_data
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode TopicData".into(),
            })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(
        &self,
        buf: &mut B,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.transactional_id
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode TransactionalId".into(),
            })?;
        self.acks
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Acks".into(),
            })?;
        self.timeout_ms
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode TimeoutMs".into(),
            })?;
        self.topic_data
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode TopicData".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for ProduceRequest {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let transactional_id = <Option<String> as KafkaDeserialize>::decode(buf).map_err(|_| {
            DecodeError::Protocol {
                message: "failed to decode TransactionalId".into(),
            }
        })?;
        let acks = <i16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
            message: "failed to decode Acks".into(),
        })?;
        let timeout_ms =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode TimeoutMs".into(),
            })?;
        let topic_data =
            <Vec<TopicProduceData> as KafkaDeserialize>::decode(buf).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode TopicData".into(),
                }
            })?;
        Ok(Self {
            transactional_id,
            acks,
            timeout_ms,
            topic_data,
        })
    }
    fn decode_flexible<B: Buf>(buf: &mut B, is_flexible: bool) -> Result<Self, DecodeError> {
        let transactional_id =
            <Option<String> as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode TransactionalId".into(),
                },
            )?;
        let acks = <i16 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| {
            DecodeError::Protocol {
                message: "failed to decode Acks".into(),
            }
        })?;
        let timeout_ms =
            <i32 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode TimeoutMs".into(),
                }
            })?;
        let topic_data =
            <Vec<TopicProduceData> as KafkaDeserialize>::decode_flexible(buf, is_flexible)
                .map_err(|_| DecodeError::Protocol {
                    message: "failed to decode TopicData".into(),
                })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            transactional_id,
            acks,
            timeout_ms,
            topic_data,
        })
    }
}

impl KafkaSerialize for PartitionProduceData {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.index
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Index".into(),
            })?;
        self.records
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Records".into(),
            })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(
        &self,
        buf: &mut B,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.index
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Index".into(),
            })?;
        self.records
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Records".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for PartitionProduceData {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let index = <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
            message: "failed to decode Index".into(),
        })?;
        let records = <Option<Vec<u8>> as KafkaDeserialize>::decode(buf).map_err(|_| {
            DecodeError::Protocol {
                message: "failed to decode Records".into(),
            }
        })?;
        Ok(Self { index, records })
    }
    fn decode_flexible<B: Buf>(buf: &mut B, is_flexible: bool) -> Result<Self, DecodeError> {
        let index = <i32 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| {
            DecodeError::Protocol {
                message: "failed to decode Index".into(),
            }
        })?;
        let records = <Option<Vec<u8>> as KafkaDeserialize>::decode_flexible(buf, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode Records".into(),
            })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self { index, records })
    }
}

impl KafkaSerialize for TopicProduceData {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.name
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Name".into(),
            })?;
        self.partition_data
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode PartitionData".into(),
            })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(
        &self,
        buf: &mut B,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.name
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Name".into(),
            })?;
        self.partition_data
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode PartitionData".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for TopicProduceData {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let name =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode Name".into(),
            })?;
        let partition_data =
            <Vec<PartitionProduceData> as KafkaDeserialize>::decode(buf).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode PartitionData".into(),
                }
            })?;
        Ok(Self {
            name,
            partition_data,
        })
    }
    fn decode_flexible<B: Buf>(buf: &mut B, is_flexible: bool) -> Result<Self, DecodeError> {
        let name =
            <String as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode Name".into(),
                }
            })?;
        let partition_data =
            <Vec<PartitionProduceData> as KafkaDeserialize>::decode_flexible(buf, is_flexible)
                .map_err(|_| DecodeError::Protocol {
                    message: "failed to decode PartitionData".into(),
                })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            name,
            partition_data,
        })
    }
}

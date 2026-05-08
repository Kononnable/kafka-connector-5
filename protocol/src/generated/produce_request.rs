#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{KafkaDeserialize, KafkaSerialize};
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
    /// The timeout to await a response in milliseconds.
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
    /// Available in version 0-12.
    pub name: String,
    /// The unique topic ID
    /// Available in version 13+.
    pub topic_id: [u8; 16],
    /// Each partition to produce to.
    pub partition_data: Vec<PartitionProduceData>,
}

impl ApiRequest for ProduceRequest {
    type Response = crate::generated::ProduceResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(0)
    }
    fn get_min_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(3)
    }
    fn get_max_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(13)
    }
    fn get_min_flexible_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(9)
    }
    fn serialize(
        &self,
        version: crate::traits::ApiVersion,
        buf: &mut BytesMut,
    ) -> Result<(), SerializationError> {
        assert!(
            (3) <= version.0 && version.0 <= (13),
            "version {} is not supported by {} (supported: 3-13)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        if (3) <= version.0 {
            self.transactional_id.encode(buf, version, is_flexible)?;
        } else if self.transactional_id.is_some() {
            return Err(SerializationError::Encode(
                "field 'TransactionalId' is not available in this version",
            ));
        }
        self.acks.encode(buf, version, is_flexible)?;
        self.timeout_ms.encode(buf, version, is_flexible)?;
        self.topic_data.encode(buf, version, is_flexible)?;
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(
        version: crate::traits::ApiVersion,
        buf: &mut Bytes,
    ) -> Result<Self, SerializationError> {
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let transactional_id = if (3) <= version.0 {
            <Option<String> as KafkaDeserialize>::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let acks = <i16 as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let timeout_ms = <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let topic_data =
            <Vec<TopicProduceData> as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        if is_flexible {
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
impl KafkaSerialize for ProduceRequest {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), crate::traits::SerializationError> {
        if (3) <= version.0 {
            self.transactional_id.encode(buf, version, is_flexible)?;
        }
        self.acks.encode(buf, version, is_flexible)?;
        self.timeout_ms.encode(buf, version, is_flexible)?;
        self.topic_data.encode(buf, version, is_flexible)?;
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for ProduceRequest {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, crate::traits::SerializationError> {
        let transactional_id = if (3) <= version.0 {
            <Option<String> as KafkaDeserialize>::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let acks = <i16 as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let timeout_ms = <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let topic_data =
            <Vec<TopicProduceData> as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        if is_flexible {
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
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), crate::traits::SerializationError> {
        self.index.encode(buf, version, is_flexible)?;
        self.records.encode(buf, version, is_flexible)?;
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for PartitionProduceData {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, crate::traits::SerializationError> {
        let index = <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let records = <Option<Vec<u8>> as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self { index, records })
    }
}

impl KafkaSerialize for TopicProduceData {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), crate::traits::SerializationError> {
        if (0) <= version.0 && version.0 <= (12) {
            self.name.encode(buf, version, is_flexible)?;
        }
        if (13) <= version.0 {
            self.topic_id.encode(buf, version, is_flexible)?;
        }
        self.partition_data.encode(buf, version, is_flexible)?;
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for TopicProduceData {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, crate::traits::SerializationError> {
        let name = if (0) <= version.0 && version.0 <= (12) {
            <String as KafkaDeserialize>::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let topic_id = if (13) <= version.0 {
            <[u8; 16] as KafkaDeserialize>::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let partition_data =
            <Vec<PartitionProduceData> as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            name,
            topic_id,
            partition_data,
        })
    }
}

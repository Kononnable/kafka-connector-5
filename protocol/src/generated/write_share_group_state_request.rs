#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{
    KafkaDeserialize, KafkaSerialize, decode_unsigned_varint, encode_unsigned_varint,
};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVer, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// WriteShareGroupStateRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct WriteShareGroupStateRequest {
    /// The group identifier.
    pub group_id: String,
    /// The data for the topics.
    pub topics: Vec<WriteStateData>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct PartitionData {
    /// The partition index.
    pub partition: i32,
    /// The state epoch of the share-partition.
    pub state_epoch: i32,
    /// The leader epoch of the share-partition.
    pub leader_epoch: i32,
    /// The share-partition start offset, or -1 if the start offset is not being written.
    pub start_offset: i64,
    /// The state batches for the share-partition.
    pub state_batches: Vec<StateBatch>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct StateBatch {
    /// The first offset of this state batch.
    pub first_offset: i64,
    /// The last offset of this state batch.
    pub last_offset: i64,
    /// The delivery state - 0:Available,2:Acked,4:Archived.
    pub delivery_state: i8,
    /// The delivery count.
    pub delivery_count: i16,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct WriteStateData {
    /// The topic identifier.
    pub topic_id: [u8; 16],
    /// The data for the partitions.
    pub partitions: Vec<PartitionData>,
}

impl ApiRequest for WriteShareGroupStateRequest {
    type Response = crate::generated::WriteShareGroupStateResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(85)
    }
    fn get_min_supported_version() -> ApiVer {
        ApiVer::new(0)
    }
    fn get_max_supported_version() -> ApiVer {
        ApiVer::new(0)
    }
    fn get_min_flexible_version() -> ApiVer {
        ApiVer::new(0)
    }
    fn serialize(&self, version: ApiVer, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            (0) <= version.0 && version.0 <= (0),
            "version {} is not supported by {} (supported: 0-0)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        self.group_id.encode(buf, version, is_flexible)?;
        self.topics.encode(buf, version, is_flexible)?;
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(version: ApiVer, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let group_id = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let topics = KafkaDeserialize::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self { group_id, topics })
    }
}
impl KafkaSerialize for WriteShareGroupStateRequest {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.group_id.encode(buf, version, is_flexible)?;
        self.topics.encode(buf, version, is_flexible)?;
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for WriteShareGroupStateRequest {
    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let group_id = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let topics = KafkaDeserialize::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self { group_id, topics })
    }
}

impl KafkaSerialize for PartitionData {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.partition.encode(buf, version, is_flexible)?;
        self.state_epoch.encode(buf, version, is_flexible)?;
        self.leader_epoch.encode(buf, version, is_flexible)?;
        self.start_offset.encode(buf, version, is_flexible)?;
        self.state_batches.encode(buf, version, is_flexible)?;
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for PartitionData {
    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let partition = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let state_epoch = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let leader_epoch = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let start_offset = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let state_batches = KafkaDeserialize::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            partition,
            state_epoch,
            leader_epoch,
            start_offset,
            state_batches,
        })
    }
}

impl KafkaSerialize for StateBatch {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.first_offset.encode(buf, version, is_flexible)?;
        self.last_offset.encode(buf, version, is_flexible)?;
        self.delivery_state.encode(buf, version, is_flexible)?;
        self.delivery_count.encode(buf, version, is_flexible)?;
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for StateBatch {
    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let first_offset = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let last_offset = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let delivery_state = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let delivery_count = KafkaDeserialize::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            first_offset,
            last_offset,
            delivery_state,
            delivery_count,
        })
    }
}

impl KafkaSerialize for WriteStateData {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.topic_id.encode(buf, version, is_flexible)?;
        self.partitions.encode(buf, version, is_flexible)?;
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for WriteStateData {
    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let topic_id = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let partitions = KafkaDeserialize::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            topic_id,
            partitions,
        })
    }
}

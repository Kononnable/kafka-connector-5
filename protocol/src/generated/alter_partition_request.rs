#![allow(unused_imports, unused_variables)]
use bytes::{Buf, BufMut, Bytes, BytesMut};
use indexmap::IndexMap;

use crate::protocol::serialization::{KafkaCodec, decode_unsigned_varint, encode_unsigned_varint};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVer, SerializationError};

// -------------------------------------------------------
// AlterPartitionRequest
// -------------------------------------------------------
#[derive(Clone, Debug, PartialEq)]
pub struct AlterPartitionRequest {
    /// The ID of the requesting broker.
    pub broker_id: i32,
    /// The epoch of the requesting broker.
    pub broker_epoch: i64,
    /// The topics to alter ISRs for.
    pub topics: Vec<TopicData>,
}
impl Default for AlterPartitionRequest {
    fn default() -> Self {
        Self {
            broker_id: 0,
            broker_epoch: -1,
            topics: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct BrokerState {
    /// The ID of the broker.
    /// Available in version 3+.
    pub broker_id: i32,
    /// The epoch of the broker. It will be -1 if the epoch check is not supported.
    /// Available in version 3+.
    pub broker_epoch: i64,
}
impl Default for BrokerState {
    fn default() -> Self {
        Self {
            broker_id: 0,
            broker_epoch: -1,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct PartitionData {
    /// The partition index.
    pub partition_index: i32,
    /// The leader epoch of this partition.
    pub leader_epoch: i32,
    /// The ISR for this partition. Deprecated since version 3.
    /// Available in version 0-2.
    pub new_isr: Vec<i32>,
    /// The ISR for this partition.
    /// Available in version 3+.
    pub new_isr_with_epochs: Vec<BrokerState>,
    /// 1 if the partition is recovering from an unclean leader election; 0 otherwise.
    /// Available in version 1+.
    pub leader_recovery_state: i8,
    /// The expected epoch of the partition which is being updated.
    pub partition_epoch: i32,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct TopicData {
    /// The ID of the topic to alter ISRs for.
    /// Available in version 2+.
    pub topic_id: [u8; 16],
    /// The partitions to alter ISRs for.
    pub partitions: Vec<PartitionData>,
}

impl ApiRequest for AlterPartitionRequest {
    type Response = crate::generated::AlterPartitionResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(56)
    }
    fn get_min_supported_version() -> ApiVer {
        ApiVer::new(2)
    }
    fn get_max_supported_version() -> ApiVer {
        ApiVer::new(3)
    }
    fn get_min_flexible_version() -> ApiVer {
        ApiVer::new(0)
    }
    fn serialize(&self, version: ApiVer, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            2 <= version.0 && version.0 <= 3,
            "version {} is not supported by {} (supported: 2-3)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        self.broker_id.encode(buf, version, is_flexible)?;
        self.broker_epoch.encode(buf, version, is_flexible)?;
        self.topics.encode(buf, version, is_flexible)?;
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(version: ApiVer, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let broker_id = KafkaCodec::decode(buf, version, is_flexible)?;
        let broker_epoch = KafkaCodec::decode(buf, version, is_flexible)?;
        let topics = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            broker_id,
            broker_epoch,
            topics,
        })
    }
}
impl KafkaCodec for AlterPartitionRequest {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.broker_id.encode(buf, version, is_flexible)?;
        self.broker_epoch.encode(buf, version, is_flexible)?;
        self.topics.encode(buf, version, is_flexible)?;
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }

    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let broker_id = KafkaCodec::decode(buf, version, is_flexible)?;
        let broker_epoch = KafkaCodec::decode(buf, version, is_flexible)?;
        let topics = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            broker_id,
            broker_epoch,
            topics,
        })
    }
}

impl KafkaCodec for BrokerState {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        if 3 <= version.0 {
            self.broker_id.encode(buf, version, is_flexible)?;
        }
        if 3 <= version.0 {
            self.broker_epoch.encode(buf, version, is_flexible)?;
        }
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }

    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let broker_id = if 3 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            0
        };
        let broker_epoch = if 3 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            -1
        };
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            broker_id,
            broker_epoch,
        })
    }
}

impl KafkaCodec for PartitionData {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.partition_index.encode(buf, version, is_flexible)?;
        self.leader_epoch.encode(buf, version, is_flexible)?;
        if 0 <= version.0 && version.0 <= 2 {
            self.new_isr.encode(buf, version, is_flexible)?;
        }
        if 3 <= version.0 {
            self.new_isr_with_epochs.encode(buf, version, is_flexible)?;
        }
        if 1 <= version.0 {
            self.leader_recovery_state
                .encode(buf, version, is_flexible)?;
        }
        self.partition_epoch.encode(buf, version, is_flexible)?;
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }

    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let partition_index = KafkaCodec::decode(buf, version, is_flexible)?;
        let leader_epoch = KafkaCodec::decode(buf, version, is_flexible)?;
        let new_isr = if 0 <= version.0 && version.0 <= 2 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Vec::new()
        };
        let new_isr_with_epochs = if 3 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Vec::new()
        };
        let leader_recovery_state = if 1 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            0
        };
        let partition_epoch = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            partition_index,
            leader_epoch,
            new_isr,
            new_isr_with_epochs,
            leader_recovery_state,
            partition_epoch,
        })
    }
}

impl KafkaCodec for TopicData {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        if 2 <= version.0 {
            self.topic_id.encode(buf, version, is_flexible)?;
        }
        self.partitions.encode(buf, version, is_flexible)?;
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }

    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let topic_id = if 2 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            [0u8; 16]
        };
        let partitions = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            topic_id,
            partitions,
        })
    }
}

#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{EncodeError, DecodeError, KafkaSerialize, KafkaDeserialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// AlterPartitionRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct AlterPartitionRequest {
    /// The ID of the requesting broker.
    pub broker_id: i32,
    /// The epoch of the requesting broker.
    pub broker_epoch: i64,
    /// The topics to alter ISRs for.
    pub topics: Vec<TopicData>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct BrokerState {
    /// The ID of the broker.
    /// Available in version 3+.
    pub broker_id: i32,
    /// The epoch of the broker. It will be -1 if the epoch check is not supported.
    /// Available in version 3+.
    pub broker_epoch: i64,
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
    fn get_api_key() -> ApiKey { ApiKey::new(56) }
    fn get_min_supported_version() -> crate::traits::ApiVersion { crate::traits::ApiVersion::new(2) }
    fn get_max_supported_version() -> crate::traits::ApiVersion { crate::traits::ApiVersion::new(3) }
    fn serialize(&self, version: crate::traits::ApiVersion, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!((2) <= version.0 && version.0 <= (3), "version {} is not supported by {} (supported: 2-3)", version.0, stringify!(Self));
        let is_flexible = true;
        self.broker_id.encode_flexible(buf, is_flexible).map_err(|_| SerializationError::Encode("failed to encode BrokerId"))?;
        self.broker_epoch.encode_flexible(buf, is_flexible).map_err(|_| SerializationError::Encode("failed to encode BrokerEpoch"))?;
        self.topics.encode_flexible(buf, is_flexible).map_err(|_| SerializationError::Encode("failed to encode Topics"))?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(version: crate::traits::ApiVersion, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let is_flexible = true;
        let broker_id = <i32 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| SerializationError::Decode("failed to decode BrokerId"))?;
        let broker_epoch = <i64 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| SerializationError::Decode("failed to decode BrokerEpoch"))?;
        let topics = <Vec<TopicData> as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| SerializationError::Decode("failed to decode Topics"))?;
        Ok(Self { broker_id, broker_epoch, topics })
    }
}
impl KafkaSerialize for AlterPartitionRequest {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.broker_id.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode BrokerId".into() })?;
        self.broker_epoch.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode BrokerEpoch".into() })?;
        self.topics.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Topics".into() })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(&self, buf: &mut B, is_flexible: bool) -> Result<(), EncodeError> {
        self.broker_id.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode BrokerId".into() })?;
        self.broker_epoch.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode BrokerEpoch".into() })?;
        self.topics.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Topics".into() })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for AlterPartitionRequest {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let broker_id = <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode BrokerId".into() })?;
        let broker_epoch = <i64 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode BrokerEpoch".into() })?;
        let topics = <Vec<TopicData> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode Topics".into() })?;
        Ok(Self { broker_id, broker_epoch, topics })
    }
    fn decode_flexible<B: Buf>(buf: &mut B, is_flexible: bool) -> Result<Self, DecodeError> {
        let broker_id = <i32 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode BrokerId".into() })?;
        let broker_epoch = <i64 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode BrokerEpoch".into() })?;
        let topics = <Vec<TopicData> as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode Topics".into() })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self { broker_id, broker_epoch, topics })
    }
}

impl KafkaSerialize for BrokerState {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.broker_id.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode BrokerId".into() })?;
        self.broker_epoch.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode BrokerEpoch".into() })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(&self, buf: &mut B, is_flexible: bool) -> Result<(), EncodeError> {
        self.broker_id.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode BrokerId".into() })?;
        self.broker_epoch.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode BrokerEpoch".into() })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for BrokerState {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let broker_id = <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode BrokerId".into() })?;
        let broker_epoch = <i64 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode BrokerEpoch".into() })?;
        Ok(Self { broker_id, broker_epoch })
    }
    fn decode_flexible<B: Buf>(buf: &mut B, is_flexible: bool) -> Result<Self, DecodeError> {
        let broker_id = <i32 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode BrokerId".into() })?;
        let broker_epoch = <i64 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode BrokerEpoch".into() })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self { broker_id, broker_epoch })
    }
}

impl KafkaSerialize for PartitionData {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.partition_index.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode PartitionIndex".into() })?;
        self.leader_epoch.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode LeaderEpoch".into() })?;
        self.new_isr.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode NewIsr".into() })?;
        self.new_isr_with_epochs.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode NewIsrWithEpochs".into() })?;
        self.leader_recovery_state.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode LeaderRecoveryState".into() })?;
        self.partition_epoch.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode PartitionEpoch".into() })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(&self, buf: &mut B, is_flexible: bool) -> Result<(), EncodeError> {
        self.partition_index.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode PartitionIndex".into() })?;
        self.leader_epoch.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode LeaderEpoch".into() })?;
        self.new_isr.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode NewIsr".into() })?;
        self.new_isr_with_epochs.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode NewIsrWithEpochs".into() })?;
        self.leader_recovery_state.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode LeaderRecoveryState".into() })?;
        self.partition_epoch.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode PartitionEpoch".into() })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for PartitionData {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let partition_index = <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode PartitionIndex".into() })?;
        let leader_epoch = <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode LeaderEpoch".into() })?;
        let new_isr = <Vec<i32> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode NewIsr".into() })?;
        let new_isr_with_epochs = <Vec<BrokerState> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode NewIsrWithEpochs".into() })?;
        let leader_recovery_state = <i8 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode LeaderRecoveryState".into() })?;
        let partition_epoch = <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode PartitionEpoch".into() })?;
        Ok(Self { partition_index, leader_epoch, new_isr, new_isr_with_epochs, leader_recovery_state, partition_epoch })
    }
    fn decode_flexible<B: Buf>(buf: &mut B, is_flexible: bool) -> Result<Self, DecodeError> {
        let partition_index = <i32 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode PartitionIndex".into() })?;
        let leader_epoch = <i32 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode LeaderEpoch".into() })?;
        let new_isr = <Vec<i32> as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode NewIsr".into() })?;
        let new_isr_with_epochs = <Vec<BrokerState> as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode NewIsrWithEpochs".into() })?;
        let leader_recovery_state = <i8 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode LeaderRecoveryState".into() })?;
        let partition_epoch = <i32 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode PartitionEpoch".into() })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self { partition_index, leader_epoch, new_isr, new_isr_with_epochs, leader_recovery_state, partition_epoch })
    }
}

impl KafkaSerialize for TopicData {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.topic_id.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode TopicId".into() })?;
        self.partitions.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Partitions".into() })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(&self, buf: &mut B, is_flexible: bool) -> Result<(), EncodeError> {
        self.topic_id.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode TopicId".into() })?;
        self.partitions.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Partitions".into() })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for TopicData {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let topic_id = <[u8; 16] as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode TopicId".into() })?;
        let partitions = <Vec<PartitionData> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode Partitions".into() })?;
        Ok(Self { topic_id, partitions })
    }
    fn decode_flexible<B: Buf>(buf: &mut B, is_flexible: bool) -> Result<Self, DecodeError> {
        let topic_id = <[u8; 16] as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode TopicId".into() })?;
        let partitions = <Vec<PartitionData> as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode Partitions".into() })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self { topic_id, partitions })
    }
}


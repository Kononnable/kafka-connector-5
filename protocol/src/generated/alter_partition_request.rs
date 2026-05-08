#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
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
    fn get_api_key() -> ApiKey {
        ApiKey::new(56)
    }
    fn get_min_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(2)
    }
    fn get_max_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(3)
    }
    fn get_min_flexible_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(0)
    }
    fn serialize(
        &self,
        version: crate::traits::ApiVersion,
        buf: &mut BytesMut,
    ) -> Result<(), SerializationError> {
        assert!(
            (2) <= version.0 && version.0 <= (3),
            "version {} is not supported by {} (supported: 2-3)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        self.broker_id
            .encode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode BrokerId"))?;
        self.broker_epoch
            .encode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode BrokerEpoch"))?;
        self.topics
            .encode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode Topics"))?;
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
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let broker_id = <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode BrokerId"))?;
        let broker_epoch = <i64 as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode BrokerEpoch"))?;
        let topics = <Vec<TopicData> as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode Topics"))?;
        Ok(Self {
            broker_id,
            broker_epoch,
            topics,
        })
    }
}
impl KafkaSerialize for AlterPartitionRequest {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.broker_id
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode BrokerId".into(),
            })?;
        self.broker_epoch
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode BrokerEpoch".into(),
            })?;
        self.topics
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Topics".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for AlterPartitionRequest {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        let broker_id =
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode BrokerId".into(),
                }
            })?;
        let broker_epoch =
            <i64 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode BrokerEpoch".into(),
                }
            })?;
        let topics = <Vec<TopicData> as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode Topics".into(),
            })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            broker_id,
            broker_epoch,
            topics,
        })
    }
}

impl KafkaSerialize for BrokerState {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        if (3) <= version.0 {
            self.broker_id
                .encode(buf, version, is_flexible)
                .map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode BrokerId".into(),
                })?;
        }
        if (3) <= version.0 {
            self.broker_epoch
                .encode(buf, version, is_flexible)
                .map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode BrokerEpoch".into(),
                })?;
        }
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for BrokerState {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        let broker_id = if (3) <= version.0 {
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode BrokerId".into(),
                }
            })?
        } else {
            Default::default()
        };
        let broker_epoch = if (3) <= version.0 {
            <i64 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode BrokerEpoch".into(),
                }
            })?
        } else {
            Default::default()
        };
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            broker_id,
            broker_epoch,
        })
    }
}

impl KafkaSerialize for PartitionData {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.partition_index
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode PartitionIndex".into(),
            })?;
        self.leader_epoch
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode LeaderEpoch".into(),
            })?;
        if (0) <= version.0 && version.0 <= (2) {
            self.new_isr
                .encode(buf, version, is_flexible)
                .map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode NewIsr".into(),
                })?;
        }
        if (3) <= version.0 {
            self.new_isr_with_epochs
                .encode(buf, version, is_flexible)
                .map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode NewIsrWithEpochs".into(),
                })?;
        }
        if (1) <= version.0 {
            self.leader_recovery_state
                .encode(buf, version, is_flexible)
                .map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode LeaderRecoveryState".into(),
                })?;
        }
        self.partition_epoch
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode PartitionEpoch".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for PartitionData {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        let partition_index = <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode PartitionIndex".into(),
            })?;
        let leader_epoch =
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode LeaderEpoch".into(),
                }
            })?;
        let new_isr = if (0) <= version.0 && version.0 <= (2) {
            <Vec<i32> as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode NewIsr".into(),
                }
            })?
        } else {
            Default::default()
        };
        let new_isr_with_epochs = if (3) <= version.0 {
            <Vec<BrokerState> as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode NewIsrWithEpochs".into(),
                },
            )?
        } else {
            Default::default()
        };
        let leader_recovery_state = if (1) <= version.0 {
            <i8 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode LeaderRecoveryState".into(),
                }
            })?
        } else {
            Default::default()
        };
        let partition_epoch = <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode PartitionEpoch".into(),
            })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
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

impl KafkaSerialize for TopicData {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        if (2) <= version.0 {
            self.topic_id
                .encode(buf, version, is_flexible)
                .map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode TopicId".into(),
                })?;
        }
        self.partitions
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Partitions".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for TopicData {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        let topic_id = if (2) <= version.0 {
            <[u8; 16] as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode TopicId".into(),
                }
            })?
        } else {
            Default::default()
        };
        let partitions =
            <Vec<PartitionData> as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode Partitions".into(),
                },
            )?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            topic_id,
            partitions,
        })
    }
}

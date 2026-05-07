#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// FetchRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct FetchRequest {
    /// The broker ID of the follower, of -1 if this request is from a consumer.
    pub replica_id: i32,
    /// The maximum time in milliseconds to wait for the response.
    pub max_wait: i32,
    /// The minimum bytes to accumulate in the response.
    pub min_bytes: i32,
    /// The maximum bytes to fetch.  See KIP-74 for cases where this limit may not be honored.
    /// Available in version 3+.
    pub max_bytes: i32,
    /// This setting controls the visibility of transactional records. Using READ_UNCOMMITTED (isolation_level = 0) makes all records visible. With READ_COMMITTED (isolation_level = 1), non-transactional and COMMITTED transactional records are visible. To be more concrete, READ_COMMITTED returns all data from offsets smaller than the current LSO (last stable offset), and enables the inclusion of the list of aborted transactions in the result, which allows consumers to discard ABORTED transactional records
    /// Available in version 4+.
    pub isolation_level: i8,
    /// The fetch session ID.
    /// Available in version 7+.
    pub session_id: i32,
    /// The epoch of the partition leader as known to the follower replica or a consumer.
    /// Available in version 7+.
    pub epoch: i32,
    /// The topics to fetch.
    pub topics: Vec<FetchableTopic>,
    /// In an incremental fetch request, the partitions to remove.
    /// Available in version 7+.
    pub forgotten: Vec<ForgottenTopic>,
    /// Rack ID of the consumer making this request
    /// Available in version 11+.
    pub rack_id: String,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct FetchPartition {
    /// The partition index.
    pub partition_index: i32,
    /// The current leader epoch of the partition.
    /// Available in version 9+.
    pub current_leader_epoch: i32,
    /// The message offset.
    pub fetch_offset: i64,
    /// The earliest available offset of the follower replica.  The field is only used when the request is sent by the follower.
    /// Available in version 5+.
    pub log_start_offset: i64,
    /// The maximum bytes to fetch from this partition.  See KIP-74 for cases where this limit may not be honored.
    pub max_bytes: i32,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct FetchableTopic {
    /// The name of the topic to fetch.
    pub name: String,
    /// The partitions to fetch.
    pub fetch_partitions: Vec<FetchPartition>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ForgottenTopic {
    /// The partition name.
    /// Available in version 7+.
    pub name: String,
    /// The partitions indexes to forget.
    /// Available in version 7+.
    pub forgotten_partition_indexes: Vec<i32>,
}

impl ApiRequest for FetchRequest {
    type Response = crate::generated::FetchResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(1)
    }
    fn get_min_supported_version() -> ApiVersion {
        ApiVersion::new(0)
    }
    fn get_max_supported_version() -> ApiVersion {
        ApiVersion::new(11)
    }
    fn serialize(&self, version: ApiVersion, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            (0) <= version.0 && version.0 <= (11),
            "version {} is not supported by {} (supported: 0-11)",
            version.0,
            stringify!(Self)
        );
        self.replica_id
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode ReplicaId"))?;
        self.max_wait
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode MaxWait"))?;
        self.min_bytes
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode MinBytes"))?;
        if (3) <= version.0 {
            self.max_bytes
                .encode(buf)
                .map_err(|_| SerializationError::Encode("failed to encode MaxBytes"))?;
        }
        if (4) <= version.0 {
            self.isolation_level
                .encode(buf)
                .map_err(|_| SerializationError::Encode("failed to encode IsolationLevel"))?;
        }
        if (7) <= version.0 {
            self.session_id
                .encode(buf)
                .map_err(|_| SerializationError::Encode("failed to encode SessionId"))?;
        }
        if (7) <= version.0 {
            self.epoch
                .encode(buf)
                .map_err(|_| SerializationError::Encode("failed to encode Epoch"))?;
        }
        self.topics
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode Topics"))?;
        if (7) <= version.0 {
            self.forgotten
                .encode(buf)
                .map_err(|_| SerializationError::Encode("failed to encode Forgotten"))?;
        }
        if (11) <= version.0 {
            self.rack_id
                .encode(buf)
                .map_err(|_| SerializationError::Encode("failed to encode RackId"))?;
        }
        Ok(())
    }
    fn deserialize(version: ApiVersion, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let replica_id = <i32 as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode ReplicaId"))?;
        let max_wait = <i32 as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode MaxWait"))?;
        let min_bytes = <i32 as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode MinBytes"))?;
        let max_bytes = if (3) <= version.0 {
            <i32 as KafkaDeserialize>::decode(buf)
                .map_err(|_| SerializationError::Decode("failed to decode MaxBytes"))?
        } else {
            Default::default()
        };
        let isolation_level = if (4) <= version.0 {
            <i8 as KafkaDeserialize>::decode(buf)
                .map_err(|_| SerializationError::Decode("failed to decode IsolationLevel"))?
        } else {
            Default::default()
        };
        let session_id = if (7) <= version.0 {
            <i32 as KafkaDeserialize>::decode(buf)
                .map_err(|_| SerializationError::Decode("failed to decode SessionId"))?
        } else {
            Default::default()
        };
        let epoch = if (7) <= version.0 {
            <i32 as KafkaDeserialize>::decode(buf)
                .map_err(|_| SerializationError::Decode("failed to decode Epoch"))?
        } else {
            Default::default()
        };
        let topics = <Vec<FetchableTopic> as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode Topics"))?;
        let forgotten = if (7) <= version.0 {
            <Vec<ForgottenTopic> as KafkaDeserialize>::decode(buf)
                .map_err(|_| SerializationError::Decode("failed to decode Forgotten"))?
        } else {
            Default::default()
        };
        let rack_id = if (11) <= version.0 {
            <String as KafkaDeserialize>::decode(buf)
                .map_err(|_| SerializationError::Decode("failed to decode RackId"))?
        } else {
            Default::default()
        };
        Ok(Self {
            replica_id,
            max_wait,
            min_bytes,
            max_bytes,
            isolation_level,
            session_id,
            epoch,
            topics,
            forgotten,
            rack_id,
        })
    }
}
impl KafkaSerialize for FetchRequest {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.replica_id
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ReplicaId".into(),
            })?;
        self.max_wait
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode MaxWait".into(),
            })?;
        self.min_bytes
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode MinBytes".into(),
            })?;
        self.max_bytes
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode MaxBytes".into(),
            })?;
        self.isolation_level
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode IsolationLevel".into(),
            })?;
        self.session_id
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode SessionId".into(),
            })?;
        self.epoch
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Epoch".into(),
            })?;
        self.topics
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Topics".into(),
            })?;
        self.forgotten
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Forgotten".into(),
            })?;
        self.rack_id
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode RackId".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for FetchRequest {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let replica_id =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ReplicaId".into(),
            })?;
        let max_wait =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode MaxWait".into(),
            })?;
        let min_bytes =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode MinBytes".into(),
            })?;
        let max_bytes =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode MaxBytes".into(),
            })?;
        let isolation_level =
            <i8 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode IsolationLevel".into(),
            })?;
        let session_id =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode SessionId".into(),
            })?;
        let epoch = <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
            message: "failed to decode Epoch".into(),
        })?;
        let topics = <Vec<FetchableTopic> as KafkaDeserialize>::decode(buf).map_err(|_| {
            DecodeError::Protocol {
                message: "failed to decode Topics".into(),
            }
        })?;
        let forgotten = <Vec<ForgottenTopic> as KafkaDeserialize>::decode(buf).map_err(|_| {
            DecodeError::Protocol {
                message: "failed to decode Forgotten".into(),
            }
        })?;
        let rack_id =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode RackId".into(),
            })?;
        Ok(Self {
            replica_id,
            max_wait,
            min_bytes,
            max_bytes,
            isolation_level,
            session_id,
            epoch,
            topics,
            forgotten,
            rack_id,
        })
    }
}

impl KafkaSerialize for FetchPartition {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.partition_index
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode PartitionIndex".into(),
            })?;
        self.current_leader_epoch
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode CurrentLeaderEpoch".into(),
            })?;
        self.fetch_offset
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode FetchOffset".into(),
            })?;
        self.log_start_offset
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode LogStartOffset".into(),
            })?;
        self.max_bytes
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode MaxBytes".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for FetchPartition {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let partition_index =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode PartitionIndex".into(),
            })?;
        let current_leader_epoch =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode CurrentLeaderEpoch".into(),
            })?;
        let fetch_offset =
            <i64 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode FetchOffset".into(),
            })?;
        let log_start_offset =
            <i64 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode LogStartOffset".into(),
            })?;
        let max_bytes =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode MaxBytes".into(),
            })?;
        Ok(Self {
            partition_index,
            current_leader_epoch,
            fetch_offset,
            log_start_offset,
            max_bytes,
        })
    }
}

impl KafkaSerialize for FetchableTopic {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.name
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Name".into(),
            })?;
        self.fetch_partitions
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode FetchPartitions".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for FetchableTopic {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let name =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode Name".into(),
            })?;
        let fetch_partitions =
            <Vec<FetchPartition> as KafkaDeserialize>::decode(buf).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode FetchPartitions".into(),
                }
            })?;
        Ok(Self {
            name,
            fetch_partitions,
        })
    }
}

impl KafkaSerialize for ForgottenTopic {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.name
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Name".into(),
            })?;
        self.forgotten_partition_indexes
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ForgottenPartitionIndexes".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for ForgottenTopic {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let name =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode Name".into(),
            })?;
        let forgotten_partition_indexes =
            <Vec<i32> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ForgottenPartitionIndexes".into(),
            })?;
        Ok(Self {
            name,
            forgotten_partition_indexes,
        })
    }
}

#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// FetchRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct FetchRequest {
    /// The clusterId if known. This is used to validate metadata fetches prior to broker registration.
    /// Available in version 12+.
    pub cluster_id: Option<String>,
    /// The broker ID of the follower, of -1 if this request is from a consumer.
    /// Available in version 0-14.
    pub replica_id: i32,
    /// The state of the replica in the follower.
    /// Available in version 15+.
    pub replica_state: ReplicaState,
    /// The maximum time in milliseconds to wait for the response.
    pub max_wait_ms: i32,
    /// The minimum bytes to accumulate in the response.
    pub min_bytes: i32,
    /// The maximum bytes to fetch.  See KIP-74 for cases where this limit may not be honored.
    /// Available in version 3+.
    pub max_bytes: i32,
    /// This setting controls the visibility of transactional records. Using READ_UNCOMMITTED (isolation_level = 0) makes all records visible. With READ_COMMITTED (isolation_level = 1), non-transactional and COMMITTED transactional records are visible. To be more concrete, READ_COMMITTED returns all data from offsets smaller than the current LSO (last stable offset), and enables the inclusion of the list of aborted transactions in the result, which allows consumers to discard ABORTED transactional records.
    /// Available in version 4+.
    pub isolation_level: i8,
    /// The fetch session ID.
    /// Available in version 7+.
    pub session_id: i32,
    /// The fetch session epoch, which is used for ordering requests in a session.
    /// Available in version 7+.
    pub session_epoch: i32,
    /// The topics to fetch.
    pub topics: Vec<FetchTopic>,
    /// In an incremental fetch request, the partitions to remove.
    /// Available in version 7+.
    pub forgotten_topics_data: Vec<ForgottenTopic>,
    /// Rack ID of the consumer making this request.
    /// Available in version 11+.
    pub rack_id: String,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct FetchPartition {
    /// The partition index.
    pub partition: i32,
    /// The current leader epoch of the partition.
    /// Available in version 9+.
    pub current_leader_epoch: i32,
    /// The message offset.
    pub fetch_offset: i64,
    /// The epoch of the last fetched record or -1 if there is none.
    /// Available in version 12+.
    pub last_fetched_epoch: i32,
    /// The earliest available offset of the follower replica.  The field is only used when the request is sent by the follower.
    /// Available in version 5+.
    pub log_start_offset: i64,
    /// The maximum bytes to fetch from this partition.  See KIP-74 for cases where this limit may not be honored.
    pub partition_max_bytes: i32,
    /// The directory id of the follower fetching.
    /// Available in version 17+.
    pub replica_directory_id: [u8; 16],
    /// The high-watermark known by the replica. -1 if the high-watermark is not known and 9223372036854775807 if the feature is not supported.
    /// Available in version 18+.
    pub high_watermark: i64,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct FetchTopic {
    /// The name of the topic to fetch.
    /// Available in version 0-12.
    pub topic: String,
    /// The unique topic ID.
    /// Available in version 13+.
    pub topic_id: [u8; 16],
    /// The partitions to fetch.
    pub partitions: Vec<FetchPartition>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ForgottenTopic {
    /// The topic name.
    /// Available in version 7-12.
    pub topic: String,
    /// The unique topic ID.
    /// Available in version 13+.
    pub topic_id: [u8; 16],
    /// The partitions indexes to forget.
    /// Available in version 7+.
    pub partitions: Vec<i32>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ReplicaState {
    /// The replica ID of the follower, or -1 if this request is from a consumer.
    /// Available in version 15+.
    pub replica_id: i32,
    /// The epoch of this follower, or -1 if not available.
    /// Available in version 15+.
    pub replica_epoch: i64,
}

impl ApiRequest for FetchRequest {
    type Response = crate::generated::FetchResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(1)
    }
    fn get_min_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(4)
    }
    fn get_max_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(18)
    }
    fn get_min_flexible_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(12)
    }
    fn serialize(
        &self,
        version: crate::traits::ApiVersion,
        buf: &mut BytesMut,
    ) -> Result<(), SerializationError> {
        assert!(
            (4) <= version.0 && version.0 <= (18),
            "version {} is not supported by {} (supported: 4-18)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        if (12) <= version.0 {
            self.cluster_id
                .encode(buf, version, is_flexible)
                .map_err(|_| SerializationError::Encode("failed to encode ClusterId"))?;
        } else if self.cluster_id.is_some() {
            return Err(SerializationError::Encode(
                "field 'ClusterId' is not available in this version",
            ));
        }
        if (0) <= version.0 && version.0 <= (14) {
            self.replica_id
                .encode(buf, version, is_flexible)
                .map_err(|_| SerializationError::Encode("failed to encode ReplicaId"))?;
        } else if self.replica_id != 0 {
            return Err(SerializationError::Encode(
                "field 'ReplicaId' is not available in this version",
            ));
        }
        if (15) <= version.0 {
            self.replica_state
                .encode(buf, version, is_flexible)
                .map_err(|_| SerializationError::Encode("failed to encode ReplicaState"))?;
        } else if self.replica_state != Default::default() {
            return Err(SerializationError::Encode(
                "field 'ReplicaState' is not available in this version",
            ));
        }
        self.max_wait_ms
            .encode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode MaxWaitMs"))?;
        self.min_bytes
            .encode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode MinBytes"))?;
        if (3) <= version.0 {
            self.max_bytes
                .encode(buf, version, is_flexible)
                .map_err(|_| SerializationError::Encode("failed to encode MaxBytes"))?;
        } else if self.max_bytes != 0 {
            return Err(SerializationError::Encode(
                "field 'MaxBytes' is not available in this version",
            ));
        }
        if (4) <= version.0 {
            self.isolation_level
                .encode(buf, version, is_flexible)
                .map_err(|_| SerializationError::Encode("failed to encode IsolationLevel"))?;
        } else if self.isolation_level != 0 {
            return Err(SerializationError::Encode(
                "field 'IsolationLevel' is not available in this version",
            ));
        }
        if (7) <= version.0 {
            self.session_id
                .encode(buf, version, is_flexible)
                .map_err(|_| SerializationError::Encode("failed to encode SessionId"))?;
        } else if self.session_id != 0 {
            return Err(SerializationError::Encode(
                "field 'SessionId' is not available in this version",
            ));
        }
        if (7) <= version.0 {
            self.session_epoch
                .encode(buf, version, is_flexible)
                .map_err(|_| SerializationError::Encode("failed to encode SessionEpoch"))?;
        } else if self.session_epoch != 0 {
            return Err(SerializationError::Encode(
                "field 'SessionEpoch' is not available in this version",
            ));
        }
        self.topics
            .encode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode Topics"))?;
        if (7) <= version.0 {
            self.forgotten_topics_data
                .encode(buf, version, is_flexible)
                .map_err(|_| SerializationError::Encode("failed to encode ForgottenTopicsData"))?;
        } else if !self.forgotten_topics_data.is_empty() {
            return Err(SerializationError::Encode(
                "field 'ForgottenTopicsData' is not available in this version",
            ));
        }
        if (11) <= version.0 {
            self.rack_id
                .encode(buf, version, is_flexible)
                .map_err(|_| SerializationError::Encode("failed to encode RackId"))?;
        } else if !self.rack_id.is_empty() {
            return Err(SerializationError::Encode(
                "field 'RackId' is not available in this version",
            ));
        }
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
        let cluster_id = if (12) <= version.0 {
            if is_flexible {
                Default::default()
            } else {
                <Option<String> as KafkaDeserialize>::decode(buf, version, is_flexible)
                    .map_err(|_| SerializationError::Decode("failed to decode ClusterId"))?
            }
        } else {
            Default::default()
        };
        let replica_id = if (0) <= version.0 && version.0 <= (14) {
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode ReplicaId"))?
        } else {
            Default::default()
        };
        let replica_state = if (15) <= version.0 {
            if is_flexible {
                Default::default()
            } else {
                <ReplicaState as KafkaDeserialize>::decode(buf, version, is_flexible)
                    .map_err(|_| SerializationError::Decode("failed to decode ReplicaState"))?
            }
        } else {
            Default::default()
        };
        let max_wait_ms = <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode MaxWaitMs"))?;
        let min_bytes = <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode MinBytes"))?;
        let max_bytes = if (3) <= version.0 {
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode MaxBytes"))?
        } else {
            Default::default()
        };
        let isolation_level = if (4) <= version.0 {
            <i8 as KafkaDeserialize>::decode(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode IsolationLevel"))?
        } else {
            Default::default()
        };
        let session_id = if (7) <= version.0 {
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode SessionId"))?
        } else {
            Default::default()
        };
        let session_epoch = if (7) <= version.0 {
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode SessionEpoch"))?
        } else {
            Default::default()
        };
        let topics = <Vec<FetchTopic> as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode Topics"))?;
        let forgotten_topics_data = if (7) <= version.0 {
            <Vec<ForgottenTopic> as KafkaDeserialize>::decode(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode ForgottenTopicsData"))?
        } else {
            Default::default()
        };
        let rack_id = if (11) <= version.0 {
            <String as KafkaDeserialize>::decode(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode RackId"))?
        } else {
            Default::default()
        };
        Ok(Self {
            cluster_id,
            replica_id,
            replica_state,
            max_wait_ms,
            min_bytes,
            max_bytes,
            isolation_level,
            session_id,
            session_epoch,
            topics,
            forgotten_topics_data,
            rack_id,
        })
    }
}
impl KafkaSerialize for FetchRequest {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        if (12) <= version.0 {
            self.cluster_id
                .encode(buf, version, is_flexible)
                .map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode ClusterId".into(),
                })?;
        }
        if (0) <= version.0 && version.0 <= (14) {
            self.replica_id
                .encode(buf, version, is_flexible)
                .map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode ReplicaId".into(),
                })?;
        }
        if (15) <= version.0 {
            self.replica_state
                .encode(buf, version, is_flexible)
                .map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode ReplicaState".into(),
                })?;
        }
        self.max_wait_ms
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode MaxWaitMs".into(),
            })?;
        self.min_bytes
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode MinBytes".into(),
            })?;
        if (3) <= version.0 {
            self.max_bytes
                .encode(buf, version, is_flexible)
                .map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode MaxBytes".into(),
                })?;
        }
        if (4) <= version.0 {
            self.isolation_level
                .encode(buf, version, is_flexible)
                .map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode IsolationLevel".into(),
                })?;
        }
        if (7) <= version.0 {
            self.session_id
                .encode(buf, version, is_flexible)
                .map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode SessionId".into(),
                })?;
        }
        if (7) <= version.0 {
            self.session_epoch
                .encode(buf, version, is_flexible)
                .map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode SessionEpoch".into(),
                })?;
        }
        self.topics
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Topics".into(),
            })?;
        if (7) <= version.0 {
            self.forgotten_topics_data
                .encode(buf, version, is_flexible)
                .map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode ForgottenTopicsData".into(),
                })?;
        }
        if (11) <= version.0 {
            self.rack_id
                .encode(buf, version, is_flexible)
                .map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode RackId".into(),
                })?;
        }
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for FetchRequest {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        let cluster_id = if (12) <= version.0 {
            if is_flexible {
                Default::default()
            } else {
                <Option<String> as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(
                    |_| DecodeError::Protocol {
                        message: "failed to decode ClusterId".into(),
                    },
                )?
            }
        } else {
            Default::default()
        };
        let replica_id = if (0) <= version.0 && version.0 <= (14) {
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode ReplicaId".into(),
                }
            })?
        } else {
            Default::default()
        };
        let replica_state = if (15) <= version.0 {
            if is_flexible {
                Default::default()
            } else {
                <ReplicaState as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(
                    |_| DecodeError::Protocol {
                        message: "failed to decode ReplicaState".into(),
                    },
                )?
            }
        } else {
            Default::default()
        };
        let max_wait_ms =
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode MaxWaitMs".into(),
                }
            })?;
        let min_bytes =
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode MinBytes".into(),
                }
            })?;
        let max_bytes = if (3) <= version.0 {
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode MaxBytes".into(),
                }
            })?
        } else {
            Default::default()
        };
        let isolation_level = if (4) <= version.0 {
            <i8 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode IsolationLevel".into(),
                }
            })?
        } else {
            Default::default()
        };
        let session_id = if (7) <= version.0 {
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode SessionId".into(),
                }
            })?
        } else {
            Default::default()
        };
        let session_epoch = if (7) <= version.0 {
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode SessionEpoch".into(),
                }
            })?
        } else {
            Default::default()
        };
        let topics = <Vec<FetchTopic> as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode Topics".into(),
            })?;
        let forgotten_topics_data = if (7) <= version.0 {
            <Vec<ForgottenTopic> as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode ForgottenTopicsData".into(),
                },
            )?
        } else {
            Default::default()
        };
        let rack_id = if (11) <= version.0 {
            <String as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode RackId".into(),
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
            cluster_id,
            replica_id,
            replica_state,
            max_wait_ms,
            min_bytes,
            max_bytes,
            isolation_level,
            session_id,
            session_epoch,
            topics,
            forgotten_topics_data,
            rack_id,
        })
    }
}

impl KafkaSerialize for FetchPartition {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.partition
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Partition".into(),
            })?;
        if (9) <= version.0 {
            self.current_leader_epoch
                .encode(buf, version, is_flexible)
                .map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode CurrentLeaderEpoch".into(),
                })?;
        }
        self.fetch_offset
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode FetchOffset".into(),
            })?;
        if (12) <= version.0 {
            self.last_fetched_epoch
                .encode(buf, version, is_flexible)
                .map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode LastFetchedEpoch".into(),
                })?;
        }
        if (5) <= version.0 {
            self.log_start_offset
                .encode(buf, version, is_flexible)
                .map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode LogStartOffset".into(),
                })?;
        }
        self.partition_max_bytes
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode PartitionMaxBytes".into(),
            })?;
        if (17) <= version.0 {
            self.replica_directory_id
                .encode(buf, version, is_flexible)
                .map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode ReplicaDirectoryId".into(),
                })?;
        }
        if (18) <= version.0 {
            self.high_watermark
                .encode(buf, version, is_flexible)
                .map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode HighWatermark".into(),
                })?;
        }
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for FetchPartition {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        let partition =
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode Partition".into(),
                }
            })?;
        let current_leader_epoch = if (9) <= version.0 {
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode CurrentLeaderEpoch".into(),
                }
            })?
        } else {
            Default::default()
        };
        let fetch_offset =
            <i64 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode FetchOffset".into(),
                }
            })?;
        let last_fetched_epoch = if (12) <= version.0 {
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode LastFetchedEpoch".into(),
                }
            })?
        } else {
            Default::default()
        };
        let log_start_offset = if (5) <= version.0 {
            <i64 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode LogStartOffset".into(),
                }
            })?
        } else {
            Default::default()
        };
        let partition_max_bytes = <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode PartitionMaxBytes".into(),
            })?;
        let replica_directory_id = if (17) <= version.0 {
            if is_flexible {
                Default::default()
            } else {
                <[u8; 16] as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                    DecodeError::Protocol {
                        message: "failed to decode ReplicaDirectoryId".into(),
                    }
                })?
            }
        } else {
            Default::default()
        };
        let high_watermark = if (18) <= version.0 {
            if is_flexible {
                Default::default()
            } else {
                <i64 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                    DecodeError::Protocol {
                        message: "failed to decode HighWatermark".into(),
                    }
                })?
            }
        } else {
            Default::default()
        };
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            partition,
            current_leader_epoch,
            fetch_offset,
            last_fetched_epoch,
            log_start_offset,
            partition_max_bytes,
            replica_directory_id,
            high_watermark,
        })
    }
}

impl KafkaSerialize for FetchTopic {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        if (0) <= version.0 && version.0 <= (12) {
            self.topic.encode(buf, version, is_flexible).map_err(|_| {
                EncodeError::ValueTooLarge {
                    message: "failed to encode Topic".into(),
                }
            })?;
        }
        if (13) <= version.0 {
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

impl KafkaDeserialize for FetchTopic {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        let topic = if (0) <= version.0 && version.0 <= (12) {
            <String as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode Topic".into(),
                }
            })?
        } else {
            Default::default()
        };
        let topic_id = if (13) <= version.0 {
            <[u8; 16] as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode TopicId".into(),
                }
            })?
        } else {
            Default::default()
        };
        let partitions =
            <Vec<FetchPartition> as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode Partitions".into(),
                },
            )?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            topic,
            topic_id,
            partitions,
        })
    }
}

impl KafkaSerialize for ForgottenTopic {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        if (7) <= version.0 && version.0 <= (12) {
            self.topic.encode(buf, version, is_flexible).map_err(|_| {
                EncodeError::ValueTooLarge {
                    message: "failed to encode Topic".into(),
                }
            })?;
        }
        if (13) <= version.0 {
            self.topic_id
                .encode(buf, version, is_flexible)
                .map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode TopicId".into(),
                })?;
        }
        if (7) <= version.0 {
            self.partitions
                .encode(buf, version, is_flexible)
                .map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode Partitions".into(),
                })?;
        }
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for ForgottenTopic {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        let topic = if (7) <= version.0 && version.0 <= (12) {
            <String as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode Topic".into(),
                }
            })?
        } else {
            Default::default()
        };
        let topic_id = if (13) <= version.0 {
            <[u8; 16] as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode TopicId".into(),
                }
            })?
        } else {
            Default::default()
        };
        let partitions = if (7) <= version.0 {
            <Vec<i32> as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode Partitions".into(),
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
            topic,
            topic_id,
            partitions,
        })
    }
}

impl KafkaSerialize for ReplicaState {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        if (15) <= version.0 {
            self.replica_id
                .encode(buf, version, is_flexible)
                .map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode ReplicaId".into(),
                })?;
        }
        if (15) <= version.0 {
            self.replica_epoch
                .encode(buf, version, is_flexible)
                .map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode ReplicaEpoch".into(),
                })?;
        }
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for ReplicaState {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        let replica_id = if (15) <= version.0 {
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode ReplicaId".into(),
                }
            })?
        } else {
            Default::default()
        };
        let replica_epoch = if (15) <= version.0 {
            <i64 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode ReplicaEpoch".into(),
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
            replica_id,
            replica_epoch,
        })
    }
}

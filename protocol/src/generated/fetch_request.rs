#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{KafkaCodec, decode_unsigned_varint, encode_unsigned_varint};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVer, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use indexmap::IndexMap;

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
    fn get_min_supported_version() -> ApiVer {
        ApiVer::new(4)
    }
    fn get_max_supported_version() -> ApiVer {
        ApiVer::new(18)
    }
    fn get_min_flexible_version() -> ApiVer {
        ApiVer::new(12)
    }
    fn serialize(&self, version: ApiVer, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            4 <= version.0 && version.0 <= 18,
            "version {} is not supported by {} (supported: 4-18)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        if 12 <= version.0 {
            self.cluster_id.encode(buf, version, is_flexible)?;
        } else if self.cluster_id.is_some() {
            return Err(SerializationError::FieldNotAvailable {
                field: "ClusterId",
                version,
                api_name: "FetchRequest",
            });
        }
        if 0 <= version.0 && version.0 <= 14 {
            self.replica_id.encode(buf, version, is_flexible)?;
        } else if self.replica_id != 0 {
            return Err(SerializationError::FieldNotAvailable {
                field: "ReplicaId",
                version,
                api_name: "FetchRequest",
            });
        }
        if 15 <= version.0 {
            self.replica_state.encode(buf, version, is_flexible)?;
        } else if self.replica_state != Default::default() {
            return Err(SerializationError::FieldNotAvailable {
                field: "ReplicaState",
                version,
                api_name: "FetchRequest",
            });
        }
        self.max_wait_ms.encode(buf, version, is_flexible)?;
        self.min_bytes.encode(buf, version, is_flexible)?;
        if 3 <= version.0 {
            self.max_bytes.encode(buf, version, is_flexible)?;
        } else if self.max_bytes != 0 {
            return Err(SerializationError::FieldNotAvailable {
                field: "MaxBytes",
                version,
                api_name: "FetchRequest",
            });
        }
        if 4 <= version.0 {
            self.isolation_level.encode(buf, version, is_flexible)?;
        } else if self.isolation_level != 0 {
            return Err(SerializationError::FieldNotAvailable {
                field: "IsolationLevel",
                version,
                api_name: "FetchRequest",
            });
        }
        if 7 <= version.0 {
            self.session_id.encode(buf, version, is_flexible)?;
        } else if self.session_id != 0 {
            return Err(SerializationError::FieldNotAvailable {
                field: "SessionId",
                version,
                api_name: "FetchRequest",
            });
        }
        if 7 <= version.0 {
            self.session_epoch.encode(buf, version, is_flexible)?;
        } else if self.session_epoch != 0 {
            return Err(SerializationError::FieldNotAvailable {
                field: "SessionEpoch",
                version,
                api_name: "FetchRequest",
            });
        }
        self.topics.encode(buf, version, is_flexible)?;
        if 7 <= version.0 {
            self.forgotten_topics_data
                .encode(buf, version, is_flexible)?;
        } else if !self.forgotten_topics_data.is_empty() {
            return Err(SerializationError::FieldNotAvailable {
                field: "ForgottenTopicsData",
                version,
                api_name: "FetchRequest",
            });
        }
        if 11 <= version.0 {
            self.rack_id.encode(buf, version, is_flexible)?;
        } else if !self.rack_id.is_empty() {
            return Err(SerializationError::FieldNotAvailable {
                field: "RackId",
                version,
                api_name: "FetchRequest",
            });
        }
        if is_flexible {
            let mut tag_count = 0u64;
            if self.cluster_id.is_some() {
                tag_count += 1;
            }
            if self.replica_state != Default::default() {
                tag_count += 1;
            }
            encode_unsigned_varint(tag_count, buf);
            if self.cluster_id.is_some() {
                encode_unsigned_varint(0u64, buf);
                let mut tmp_buf = bytes::BytesMut::new();
                self.cluster_id.encode(&mut tmp_buf, version, true)?;
                encode_unsigned_varint(tmp_buf.len() as u64, buf);
                buf.put_slice(&tmp_buf);
            }
            if self.replica_state != Default::default() {
                encode_unsigned_varint(1u64, buf);
                let mut tmp_buf = bytes::BytesMut::new();
                self.replica_state.encode(&mut tmp_buf, version, true)?;
                encode_unsigned_varint(tmp_buf.len() as u64, buf);
                buf.put_slice(&tmp_buf);
            }
        }
        Ok(())
    }
    fn deserialize(version: ApiVer, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let mut cluster_id = if 12 <= version.0 {
            if is_flexible {
                Default::default()
            } else {
                KafkaCodec::decode(buf, version, is_flexible)?
            }
        } else {
            Default::default()
        };
        let replica_id = if 0 <= version.0 && version.0 <= 14 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let mut replica_state = if 15 <= version.0 {
            if is_flexible {
                Default::default()
            } else {
                KafkaCodec::decode(buf, version, is_flexible)?
            }
        } else {
            Default::default()
        };
        let max_wait_ms = KafkaCodec::decode(buf, version, is_flexible)?;
        let min_bytes = KafkaCodec::decode(buf, version, is_flexible)?;
        let max_bytes = if 3 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let isolation_level = if 4 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let session_id = if 7 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let session_epoch = if 7 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let topics = KafkaCodec::decode(buf, version, is_flexible)?;
        let forgotten_topics_data = if 7 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let rack_id = if 11 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        if is_flexible {
            let (tag_count, _) = decode_unsigned_varint(buf)?;
            for _ in 0..tag_count {
                let (__tag_id, _) = decode_unsigned_varint(buf)?;
                let (__tag_len, _) = decode_unsigned_varint(buf)?;
                match __tag_id {
                    0 => {
                        cluster_id = KafkaCodec::decode(buf, version, true)?;
                    }
                    1 => {
                        replica_state = KafkaCodec::decode(buf, version, true)?;
                    }
                    _ => {
                        buf.advance(__tag_len as usize);
                    }
                }
            }
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
impl KafkaCodec for FetchRequest {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        if 12 <= version.0 && !is_flexible {
            self.cluster_id.encode(buf, version, is_flexible)?;
        }
        if 0 <= version.0 && version.0 <= 14 {
            self.replica_id.encode(buf, version, is_flexible)?;
        }
        if 15 <= version.0 && !is_flexible {
            self.replica_state.encode(buf, version, is_flexible)?;
        }
        self.max_wait_ms.encode(buf, version, is_flexible)?;
        self.min_bytes.encode(buf, version, is_flexible)?;
        if 3 <= version.0 {
            self.max_bytes.encode(buf, version, is_flexible)?;
        }
        if 4 <= version.0 {
            self.isolation_level.encode(buf, version, is_flexible)?;
        }
        if 7 <= version.0 {
            self.session_id.encode(buf, version, is_flexible)?;
        }
        if 7 <= version.0 {
            self.session_epoch.encode(buf, version, is_flexible)?;
        }
        self.topics.encode(buf, version, is_flexible)?;
        if 7 <= version.0 {
            self.forgotten_topics_data
                .encode(buf, version, is_flexible)?;
        }
        if 11 <= version.0 {
            self.rack_id.encode(buf, version, is_flexible)?;
        }
        if is_flexible {
            let mut tag_count = 0u64;
            if self.cluster_id.is_some() {
                tag_count += 1;
            }
            if self.replica_state != Default::default() {
                tag_count += 1;
            }
            encode_unsigned_varint(tag_count, buf);
            if self.cluster_id.is_some() {
                encode_unsigned_varint(0u64, buf);
                let mut tmp_buf = bytes::BytesMut::new();
                self.cluster_id.encode(&mut tmp_buf, version, true)?;
                encode_unsigned_varint(tmp_buf.len() as u64, buf);
                buf.put_slice(&tmp_buf);
            }
            if self.replica_state != Default::default() {
                encode_unsigned_varint(1u64, buf);
                let mut tmp_buf = bytes::BytesMut::new();
                self.replica_state.encode(&mut tmp_buf, version, true)?;
                encode_unsigned_varint(tmp_buf.len() as u64, buf);
                buf.put_slice(&tmp_buf);
            }
        }
        Ok(())
    }

    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let mut cluster_id = if 12 <= version.0 {
            if is_flexible {
                Default::default()
            } else {
                KafkaCodec::decode(buf, version, is_flexible)?
            }
        } else {
            Default::default()
        };
        let replica_id = if 0 <= version.0 && version.0 <= 14 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let mut replica_state = if 15 <= version.0 {
            if is_flexible {
                Default::default()
            } else {
                KafkaCodec::decode(buf, version, is_flexible)?
            }
        } else {
            Default::default()
        };
        let max_wait_ms = KafkaCodec::decode(buf, version, is_flexible)?;
        let min_bytes = KafkaCodec::decode(buf, version, is_flexible)?;
        let max_bytes = if 3 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let isolation_level = if 4 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let session_id = if 7 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let session_epoch = if 7 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let topics = KafkaCodec::decode(buf, version, is_flexible)?;
        let forgotten_topics_data = if 7 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let rack_id = if 11 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        if is_flexible {
            let (tag_count, _) = decode_unsigned_varint(buf)?;
            for _ in 0..tag_count {
                let (__tag_id, _) = decode_unsigned_varint(buf)?;
                let (__tag_len, _) = decode_unsigned_varint(buf)?;
                match __tag_id {
                    0 => {
                        cluster_id = KafkaCodec::decode(buf, version, true)?;
                    }
                    1 => {
                        replica_state = KafkaCodec::decode(buf, version, true)?;
                    }
                    _ => {
                        buf.advance(__tag_len as usize);
                    }
                }
            }
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

impl KafkaCodec for FetchPartition {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.partition.encode(buf, version, is_flexible)?;
        if 9 <= version.0 {
            self.current_leader_epoch
                .encode(buf, version, is_flexible)?;
        }
        self.fetch_offset.encode(buf, version, is_flexible)?;
        if 12 <= version.0 {
            self.last_fetched_epoch.encode(buf, version, is_flexible)?;
        }
        if 5 <= version.0 {
            self.log_start_offset.encode(buf, version, is_flexible)?;
        }
        self.partition_max_bytes.encode(buf, version, is_flexible)?;
        if 17 <= version.0 && !is_flexible {
            self.replica_directory_id
                .encode(buf, version, is_flexible)?;
        }
        if 18 <= version.0 && !is_flexible {
            self.high_watermark.encode(buf, version, is_flexible)?;
        }
        if is_flexible {
            let mut tag_count = 0u64;
            if self.replica_directory_id != [0u8; 16] {
                tag_count += 1;
            }
            if self.high_watermark != 0 {
                tag_count += 1;
            }
            encode_unsigned_varint(tag_count, buf);
            if self.replica_directory_id != [0u8; 16] {
                encode_unsigned_varint(0u64, buf);
                let mut tmp_buf = bytes::BytesMut::new();
                self.replica_directory_id
                    .encode(&mut tmp_buf, version, true)?;
                encode_unsigned_varint(tmp_buf.len() as u64, buf);
                buf.put_slice(&tmp_buf);
            }
            if self.high_watermark != 0 {
                encode_unsigned_varint(1u64, buf);
                let mut tmp_buf = bytes::BytesMut::new();
                self.high_watermark.encode(&mut tmp_buf, version, true)?;
                encode_unsigned_varint(tmp_buf.len() as u64, buf);
                buf.put_slice(&tmp_buf);
            }
        }
        Ok(())
    }

    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let partition = KafkaCodec::decode(buf, version, is_flexible)?;
        let current_leader_epoch = if 9 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let fetch_offset = KafkaCodec::decode(buf, version, is_flexible)?;
        let last_fetched_epoch = if 12 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let log_start_offset = if 5 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let partition_max_bytes = KafkaCodec::decode(buf, version, is_flexible)?;
        let mut replica_directory_id = if 17 <= version.0 {
            if is_flexible {
                Default::default()
            } else {
                KafkaCodec::decode(buf, version, is_flexible)?
            }
        } else {
            Default::default()
        };
        let mut high_watermark = if 18 <= version.0 {
            if is_flexible {
                Default::default()
            } else {
                KafkaCodec::decode(buf, version, is_flexible)?
            }
        } else {
            Default::default()
        };
        if is_flexible {
            let (tag_count, _) = decode_unsigned_varint(buf)?;
            for _ in 0..tag_count {
                let (__tag_id, _) = decode_unsigned_varint(buf)?;
                let (__tag_len, _) = decode_unsigned_varint(buf)?;
                match __tag_id {
                    0 => {
                        replica_directory_id = KafkaCodec::decode(buf, version, true)?;
                    }
                    1 => {
                        high_watermark = KafkaCodec::decode(buf, version, true)?;
                    }
                    _ => {
                        buf.advance(__tag_len as usize);
                    }
                }
            }
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

impl KafkaCodec for FetchTopic {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        if 0 <= version.0 && version.0 <= 12 {
            self.topic.encode(buf, version, is_flexible)?;
        }
        if 13 <= version.0 {
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
        let topic = if 0 <= version.0 && version.0 <= 12 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let topic_id = if 13 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let partitions = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            topic,
            topic_id,
            partitions,
        })
    }
}

impl KafkaCodec for ForgottenTopic {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        if 7 <= version.0 && version.0 <= 12 {
            self.topic.encode(buf, version, is_flexible)?;
        }
        if 13 <= version.0 {
            self.topic_id.encode(buf, version, is_flexible)?;
        }
        if 7 <= version.0 {
            self.partitions.encode(buf, version, is_flexible)?;
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
        let topic = if 7 <= version.0 && version.0 <= 12 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let topic_id = if 13 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let partitions = if 7 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            topic,
            topic_id,
            partitions,
        })
    }
}

impl KafkaCodec for ReplicaState {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        if 15 <= version.0 {
            self.replica_id.encode(buf, version, is_flexible)?;
        }
        if 15 <= version.0 {
            self.replica_epoch.encode(buf, version, is_flexible)?;
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
        let replica_id = if 15 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let replica_epoch = if 15 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            replica_id,
            replica_epoch,
        })
    }
}

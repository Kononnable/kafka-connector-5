#![allow(unused_imports, unused_variables)]
use bytes::{Buf, BufMut, Bytes, BytesMut};
use indexmap::IndexMap;

use crate::protocol::serialization::{KafkaCodec, decode_unsigned_varint, encode_unsigned_varint};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVer, SerializationError};

// -------------------------------------------------------
// ShareFetchRequest
// -------------------------------------------------------
#[derive(Clone, Debug, PartialEq)]
pub struct ShareFetchRequest {
    /// The group identifier.
    pub group_id: Option<String>,
    /// The member ID.
    pub member_id: Option<String>,
    /// The current share session epoch: 0 to open a share session; -1 to close it; otherwise increments for consecutive requests.
    pub share_session_epoch: i32,
    /// The maximum time in milliseconds to wait for the response.
    pub max_wait_ms: i32,
    /// The minimum bytes to accumulate in the response.
    pub min_bytes: i32,
    /// The maximum bytes to fetch. See KIP-74 for cases where this limit may not be honored.
    pub max_bytes: i32,
    /// The maximum number of records to fetch. This limit can be exceeded for alignment of batch boundaries.
    /// Available in version 1+.
    pub max_records: i32,
    /// The optimal number of records for batches of acquired records and acknowledgements.
    /// Available in version 1+.
    pub batch_size: i32,
    /// The topics to fetch.
    /// IndexMap key `TopicId` (uuid): The unique topic ID.
    pub topics: IndexMap<[u8; 16], FetchTopic>,
    /// The partitions to remove from this share session.
    pub forgotten_topics_data: Vec<ForgottenTopic>,
}
impl Default for ShareFetchRequest {
    fn default() -> Self {
        Self {
            group_id: None,
            member_id: None,
            share_session_epoch: 0,
            max_wait_ms: 0,
            min_bytes: 0,
            max_bytes: 2147483647,
            max_records: 0,
            batch_size: 0,
            topics: IndexMap::new(),
            forgotten_topics_data: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct AcknowledgementBatch {
    /// First offset of batch of records to acknowledge.
    pub first_offset: i64,
    /// Last offset (inclusive) of batch of records to acknowledge.
    pub last_offset: i64,
    /// Array of acknowledge types - 0:Gap,1:Accept,2:Release,3:Reject.
    pub acknowledge_types: Vec<i8>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct FetchPartition {
    /// The maximum bytes to fetch from this partition. 0 when only acknowledgement with no fetching is required. See KIP-74 for cases where this limit may not be honored.
    /// Available in version 0.
    pub partition_max_bytes: i32,
    /// Record batches to acknowledge.
    pub acknowledgement_batches: Vec<AcknowledgementBatch>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct FetchTopic {
    /// The partitions to fetch.
    /// IndexMap key `PartitionIndex` (int32): The partition index.
    pub partitions: IndexMap<i32, FetchPartition>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ForgottenTopic {
    /// The unique topic ID.
    pub topic_id: [u8; 16],
    /// The partitions indexes to forget.
    pub partitions: Vec<i32>,
}

impl ApiRequest for ShareFetchRequest {
    type Response = crate::generated::ShareFetchResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(78)
    }
    fn get_min_supported_version() -> ApiVer {
        ApiVer::new(1)
    }
    fn get_max_supported_version() -> ApiVer {
        ApiVer::new(1)
    }
    fn get_min_flexible_version() -> ApiVer {
        ApiVer::new(0)
    }
    fn serialize(&self, version: ApiVer, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            1 <= version.0 && version.0 <= 1,
            "version {} is not supported by {} (supported: 1-1)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        self.group_id.encode(buf, version, is_flexible)?;
        self.member_id.encode(buf, version, is_flexible)?;
        self.share_session_epoch.encode(buf, version, is_flexible)?;
        self.max_wait_ms.encode(buf, version, is_flexible)?;
        self.min_bytes.encode(buf, version, is_flexible)?;
        self.max_bytes.encode(buf, version, is_flexible)?;
        if 1 <= version.0 {
            self.max_records.encode(buf, version, is_flexible)?;
        } else if self.max_records != 0 {
            return Err(SerializationError::FieldNotAvailable {
                field: "MaxRecords",
                version,
                api_name: "ShareFetchRequest",
            });
        }
        if 1 <= version.0 {
            self.batch_size.encode(buf, version, is_flexible)?;
        } else if self.batch_size != 0 {
            return Err(SerializationError::FieldNotAvailable {
                field: "BatchSize",
                version,
                api_name: "ShareFetchRequest",
            });
        }
        self.topics.encode(buf, version, is_flexible)?;
        self.forgotten_topics_data
            .encode(buf, version, is_flexible)?;
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(version: ApiVer, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let group_id = KafkaCodec::decode(buf, version, is_flexible)?;
        let member_id = KafkaCodec::decode(buf, version, is_flexible)?;
        let share_session_epoch = KafkaCodec::decode(buf, version, is_flexible)?;
        let max_wait_ms = KafkaCodec::decode(buf, version, is_flexible)?;
        let min_bytes = KafkaCodec::decode(buf, version, is_flexible)?;
        let max_bytes = KafkaCodec::decode(buf, version, is_flexible)?;
        let max_records = if 1 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            0
        };
        let batch_size = if 1 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            0
        };
        let topics = KafkaCodec::decode(buf, version, is_flexible)?;
        let forgotten_topics_data = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            group_id,
            member_id,
            share_session_epoch,
            max_wait_ms,
            min_bytes,
            max_bytes,
            max_records,
            batch_size,
            topics,
            forgotten_topics_data,
        })
    }
}
impl KafkaCodec for ShareFetchRequest {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.group_id.encode(buf, version, is_flexible)?;
        self.member_id.encode(buf, version, is_flexible)?;
        self.share_session_epoch.encode(buf, version, is_flexible)?;
        self.max_wait_ms.encode(buf, version, is_flexible)?;
        self.min_bytes.encode(buf, version, is_flexible)?;
        self.max_bytes.encode(buf, version, is_flexible)?;
        if 1 <= version.0 {
            self.max_records.encode(buf, version, is_flexible)?;
        }
        if 1 <= version.0 {
            self.batch_size.encode(buf, version, is_flexible)?;
        }
        self.topics.encode(buf, version, is_flexible)?;
        self.forgotten_topics_data
            .encode(buf, version, is_flexible)?;
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
        let group_id = KafkaCodec::decode(buf, version, is_flexible)?;
        let member_id = KafkaCodec::decode(buf, version, is_flexible)?;
        let share_session_epoch = KafkaCodec::decode(buf, version, is_flexible)?;
        let max_wait_ms = KafkaCodec::decode(buf, version, is_flexible)?;
        let min_bytes = KafkaCodec::decode(buf, version, is_flexible)?;
        let max_bytes = KafkaCodec::decode(buf, version, is_flexible)?;
        let max_records = if 1 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            0
        };
        let batch_size = if 1 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            0
        };
        let topics = KafkaCodec::decode(buf, version, is_flexible)?;
        let forgotten_topics_data = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            group_id,
            member_id,
            share_session_epoch,
            max_wait_ms,
            min_bytes,
            max_bytes,
            max_records,
            batch_size,
            topics,
            forgotten_topics_data,
        })
    }
}

impl KafkaCodec for AcknowledgementBatch {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.first_offset.encode(buf, version, is_flexible)?;
        self.last_offset.encode(buf, version, is_flexible)?;
        self.acknowledge_types.encode(buf, version, is_flexible)?;
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
        let first_offset = KafkaCodec::decode(buf, version, is_flexible)?;
        let last_offset = KafkaCodec::decode(buf, version, is_flexible)?;
        let acknowledge_types = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            first_offset,
            last_offset,
            acknowledge_types,
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
        if version.0 == 0 {
            self.partition_max_bytes.encode(buf, version, is_flexible)?;
        }
        self.acknowledgement_batches
            .encode(buf, version, is_flexible)?;
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
        let partition_max_bytes = if version.0 == 0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            0
        };
        let acknowledgement_batches = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            partition_max_bytes,
            acknowledgement_batches,
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
        let partitions = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self { partitions })
    }
}

impl KafkaCodec for ForgottenTopic {
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

    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let topic_id = KafkaCodec::decode(buf, version, is_flexible)?;
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

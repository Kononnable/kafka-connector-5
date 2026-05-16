#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{KafkaCodec, decode_unsigned_varint, encode_unsigned_varint};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVer, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use indexmap::IndexMap;

// -------------------------------------------------------
// AddPartitionsToTxnResponse
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct AddPartitionsToTxnResponse {
    /// Duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// The response top level error code.
    /// Available in version 4+.
    pub error_code: i16,
    /// Results categorized by transactional ID.
    /// IndexMap key `TransactionalId` (string): The transactional id corresponding to the transaction.
    /// Available in version 4+.
    pub results_by_transaction: IndexMap<String, AddPartitionsToTxnResult>,
    /// The results for each topic.
    /// Available in version 0-3.
    pub results_by_topic_v3_and_below: Vec<AddPartitionsToTxnTopicResult>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct AddPartitionsToTxnPartitionResult {
    /// The partition indexes.
    pub partition_index: i32,
    /// The response error code.
    pub partition_error_code: i16,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct AddPartitionsToTxnResult {
    /// The results for each topic.
    /// Available in version 4+.
    pub topic_results: Vec<AddPartitionsToTxnTopicResult>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct AddPartitionsToTxnTopicResult {
    /// The topic name.
    pub name: String,
    /// The results for each partition.
    pub results_by_partition: Vec<AddPartitionsToTxnPartitionResult>,
}

impl ApiResponse for AddPartitionsToTxnResponse {
    type Request = crate::generated::AddPartitionsToTxnRequest;
    fn get_api_key() -> ApiKey {
        ApiKey::new(24)
    }
    fn get_min_supported_version() -> ApiVer {
        ApiVer::new(0)
    }
    fn get_max_supported_version() -> ApiVer {
        ApiVer::new(5)
    }
    fn get_min_flexible_version() -> ApiVer {
        ApiVer::new(3)
    }
    fn serialize(&self, version: ApiVer, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            0 <= version.0 && version.0 <= 5,
            "version {} is not supported by {} (supported: 0-5)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        self.throttle_time_ms.encode(buf, version, is_flexible)?;
        if 4 <= version.0 {
            self.error_code.encode(buf, version, is_flexible)?;
        } else if self.error_code != 0 {
            return Err(SerializationError::FieldNotAvailable {
                field: "ErrorCode",
                version,
                api_name: "AddPartitionsToTxnResponse",
            });
        }
        if 4 <= version.0 {
            self.results_by_transaction
                .encode(buf, version, is_flexible)?;
        } else if !self.results_by_transaction.is_empty() {
            return Err(SerializationError::FieldNotAvailable {
                field: "ResultsByTransaction",
                version,
                api_name: "AddPartitionsToTxnResponse",
            });
        }
        if 0 <= version.0 && version.0 <= 3 {
            self.results_by_topic_v3_and_below
                .encode(buf, version, is_flexible)?;
        } else if !self.results_by_topic_v3_and_below.is_empty() {
            return Err(SerializationError::FieldNotAvailable {
                field: "ResultsByTopicV3AndBelow",
                version,
                api_name: "AddPartitionsToTxnResponse",
            });
        }
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(version: ApiVer, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let throttle_time_ms = KafkaCodec::decode(buf, version, is_flexible)?;
        let error_code = if 4 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let results_by_transaction = if 4 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let results_by_topic_v3_and_below = if 0 <= version.0 && version.0 <= 3 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            throttle_time_ms,
            error_code,
            results_by_transaction,
            results_by_topic_v3_and_below,
        })
    }
}
impl KafkaCodec for AddPartitionsToTxnResponse {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.throttle_time_ms.encode(buf, version, is_flexible)?;
        if 4 <= version.0 {
            self.error_code.encode(buf, version, is_flexible)?;
        }
        if 4 <= version.0 {
            self.results_by_transaction
                .encode(buf, version, is_flexible)?;
        }
        if 0 <= version.0 && version.0 <= 3 {
            self.results_by_topic_v3_and_below
                .encode(buf, version, is_flexible)?;
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
        let throttle_time_ms = KafkaCodec::decode(buf, version, is_flexible)?;
        let error_code = if 4 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let results_by_transaction = if 4 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let results_by_topic_v3_and_below = if 0 <= version.0 && version.0 <= 3 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            throttle_time_ms,
            error_code,
            results_by_transaction,
            results_by_topic_v3_and_below,
        })
    }
}

impl KafkaCodec for AddPartitionsToTxnPartitionResult {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.partition_index.encode(buf, version, is_flexible)?;
        self.partition_error_code
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
        let partition_index = KafkaCodec::decode(buf, version, is_flexible)?;
        let partition_error_code = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            partition_index,
            partition_error_code,
        })
    }
}

impl KafkaCodec for AddPartitionsToTxnResult {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        if 4 <= version.0 {
            self.topic_results.encode(buf, version, is_flexible)?;
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
        let topic_results = if 4 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self { topic_results })
    }
}

impl KafkaCodec for AddPartitionsToTxnTopicResult {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.name.encode(buf, version, is_flexible)?;
        self.results_by_partition
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
        let name = KafkaCodec::decode(buf, version, is_flexible)?;
        let results_by_partition = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            name,
            results_by_partition,
        })
    }
}

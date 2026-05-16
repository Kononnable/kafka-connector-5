#![allow(unused_imports, unused_variables)]
use bytes::{Buf, BufMut, Bytes, BytesMut};
use indexmap::IndexMap;

use crate::protocol::serialization::{KafkaCodec, decode_unsigned_varint, encode_unsigned_varint};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVer, SerializationError};

// -------------------------------------------------------
// DescribeLogDirsResponse
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct DescribeLogDirsResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// The error code, or 0 if there was no error.
    /// Available in version 3+.
    pub error_code: i16,
    /// The log directories.
    pub results: Vec<DescribeLogDirsResult>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct DescribeLogDirsPartition {
    /// The partition index.
    pub partition_index: i32,
    /// The size of the log segments in this partition in bytes.
    pub partition_size: i64,
    /// The lag of the log's LEO w.r.t. partition's HW (if it is the current log for the partition) or current replica's LEO (if it is the future log for the partition).
    pub offset_lag: i64,
    /// True if this log is created by AlterReplicaLogDirsRequest and will replace the current log of the replica in the future.
    pub is_future_key: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DescribeLogDirsResult {
    /// The error code, or 0 if there was no error.
    pub error_code: i16,
    /// The absolute log directory path.
    pub log_dir: String,
    /// The topics.
    pub topics: Vec<DescribeLogDirsTopic>,
    /// The total size in bytes of the volume the log directory is in.
    /// Available in version 4+.
    pub total_bytes: i64,
    /// The usable size in bytes of the volume the log directory is in.
    /// Available in version 4+.
    pub usable_bytes: i64,
}
impl Default for DescribeLogDirsResult {
    fn default() -> Self {
        Self {
            error_code: 0,
            log_dir: String::new(),
            topics: Vec::new(),
            total_bytes: -1,
            usable_bytes: -1,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct DescribeLogDirsTopic {
    /// The topic name.
    pub name: String,
    /// The partitions.
    pub partitions: Vec<DescribeLogDirsPartition>,
}

impl ApiResponse for DescribeLogDirsResponse {
    type Request = crate::generated::DescribeLogDirsRequest;
    fn get_api_key() -> ApiKey {
        ApiKey::new(35)
    }
    fn get_min_supported_version() -> ApiVer {
        ApiVer::new(1)
    }
    fn get_max_supported_version() -> ApiVer {
        ApiVer::new(4)
    }
    fn get_min_flexible_version() -> ApiVer {
        ApiVer::new(2)
    }
    fn serialize(&self, version: ApiVer, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            1 <= version.0 && version.0 <= 4,
            "version {} is not supported by {} (supported: 1-4)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        self.throttle_time_ms.encode(buf, version, is_flexible)?;
        if 3 <= version.0 {
            self.error_code.encode(buf, version, is_flexible)?;
        } else if self.error_code != 0 {
            return Err(SerializationError::FieldNotAvailable {
                field: "ErrorCode",
                version,
                api_name: "DescribeLogDirsResponse",
            });
        }
        self.results.encode(buf, version, is_flexible)?;
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(version: ApiVer, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let throttle_time_ms = KafkaCodec::decode(buf, version, is_flexible)?;
        let error_code = if 3 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            0
        };
        let results = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            throttle_time_ms,
            error_code,
            results,
        })
    }
}
impl KafkaCodec for DescribeLogDirsResponse {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.throttle_time_ms.encode(buf, version, is_flexible)?;
        if 3 <= version.0 {
            self.error_code.encode(buf, version, is_flexible)?;
        }
        self.results.encode(buf, version, is_flexible)?;
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
        let error_code = if 3 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            0
        };
        let results = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            throttle_time_ms,
            error_code,
            results,
        })
    }
}

impl KafkaCodec for DescribeLogDirsPartition {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.partition_index.encode(buf, version, is_flexible)?;
        self.partition_size.encode(buf, version, is_flexible)?;
        self.offset_lag.encode(buf, version, is_flexible)?;
        self.is_future_key.encode(buf, version, is_flexible)?;
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
        let partition_size = KafkaCodec::decode(buf, version, is_flexible)?;
        let offset_lag = KafkaCodec::decode(buf, version, is_flexible)?;
        let is_future_key = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            partition_index,
            partition_size,
            offset_lag,
            is_future_key,
        })
    }
}

impl KafkaCodec for DescribeLogDirsResult {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.error_code.encode(buf, version, is_flexible)?;
        self.log_dir.encode(buf, version, is_flexible)?;
        self.topics.encode(buf, version, is_flexible)?;
        if 4 <= version.0 {
            self.total_bytes.encode(buf, version, is_flexible)?;
        }
        if 4 <= version.0 {
            self.usable_bytes.encode(buf, version, is_flexible)?;
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
        let error_code = KafkaCodec::decode(buf, version, is_flexible)?;
        let log_dir = KafkaCodec::decode(buf, version, is_flexible)?;
        let topics = KafkaCodec::decode(buf, version, is_flexible)?;
        let total_bytes = if 4 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            -1
        };
        let usable_bytes = if 4 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            -1
        };
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            error_code,
            log_dir,
            topics,
            total_bytes,
            usable_bytes,
        })
    }
}

impl KafkaCodec for DescribeLogDirsTopic {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.name.encode(buf, version, is_flexible)?;
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
        let name = KafkaCodec::decode(buf, version, is_flexible)?;
        let partitions = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self { name, partitions })
    }
}

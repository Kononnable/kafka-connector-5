#![allow(unused_imports, unused_variables)]
use bytes::{Buf, BufMut, Bytes, BytesMut};
use indexmap::IndexMap;

use crate::protocol::serialization::{KafkaCodec, decode_unsigned_varint, encode_unsigned_varint};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVer, SerializationError};

// -------------------------------------------------------
// BrokerHeartbeatRequest
// -------------------------------------------------------
#[derive(Clone, Debug, PartialEq)]
pub struct BrokerHeartbeatRequest {
    /// The broker ID.
    pub broker_id: i32,
    /// The broker epoch.
    pub broker_epoch: i64,
    /// The highest metadata offset which the broker has reached.
    pub current_metadata_offset: i64,
    /// True if the broker wants to be fenced, false otherwise.
    pub want_fence: bool,
    /// True if the broker wants to be shut down, false otherwise.
    pub want_shut_down: bool,
    /// Log directories that failed and went offline.
    /// Available in version 1+.
    pub offline_log_dirs: Vec<[u8; 16]>,
}
impl Default for BrokerHeartbeatRequest {
    fn default() -> Self {
        Self {
            broker_id: 0,
            broker_epoch: -1,
            current_metadata_offset: 0,
            want_fence: false,
            want_shut_down: false,
            offline_log_dirs: Vec::new(),
        }
    }
}

impl ApiRequest for BrokerHeartbeatRequest {
    type Response = crate::generated::BrokerHeartbeatResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(63)
    }
    fn get_min_supported_version() -> ApiVer {
        ApiVer::new(0)
    }
    fn get_max_supported_version() -> ApiVer {
        ApiVer::new(1)
    }
    fn get_min_flexible_version() -> ApiVer {
        ApiVer::new(0)
    }
    fn serialize(&self, version: ApiVer, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            0 <= version.0 && version.0 <= 1,
            "version {} is not supported by {} (supported: 0-1)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        self.broker_id.encode(buf, version, is_flexible)?;
        self.broker_epoch.encode(buf, version, is_flexible)?;
        self.current_metadata_offset
            .encode(buf, version, is_flexible)?;
        self.want_fence.encode(buf, version, is_flexible)?;
        self.want_shut_down.encode(buf, version, is_flexible)?;
        if 1 <= version.0 {
            self.offline_log_dirs.encode(buf, version, is_flexible)?;
        } else if !self.offline_log_dirs.is_empty() {
            return Err(SerializationError::FieldNotAvailable {
                field: "OfflineLogDirs",
                version,
                api_name: "BrokerHeartbeatRequest",
            });
        }
        if is_flexible {
            let mut tag_count = 0u64;
            if !self.offline_log_dirs.is_empty() {
                tag_count += 1;
            }
            encode_unsigned_varint(tag_count, buf);
            if !self.offline_log_dirs.is_empty() {
                encode_unsigned_varint(0u64, buf);
                let mut tmp_buf = bytes::BytesMut::new();
                self.offline_log_dirs.encode(&mut tmp_buf, version, true)?;
                encode_unsigned_varint(tmp_buf.len() as u64, buf);
                buf.put_slice(&tmp_buf);
            }
        }
        Ok(())
    }
    fn deserialize(version: ApiVer, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let broker_id = KafkaCodec::decode(buf, version, is_flexible)?;
        let broker_epoch = KafkaCodec::decode(buf, version, is_flexible)?;
        let current_metadata_offset = KafkaCodec::decode(buf, version, is_flexible)?;
        let want_fence = KafkaCodec::decode(buf, version, is_flexible)?;
        let want_shut_down = KafkaCodec::decode(buf, version, is_flexible)?;
        let mut offline_log_dirs = if 1 <= version.0 {
            if is_flexible {
                Vec::new()
            } else {
                KafkaCodec::decode(buf, version, is_flexible)?
            }
        } else {
            Vec::new()
        };
        if is_flexible {
            let (tag_count, _) = decode_unsigned_varint(buf)?;
            for _ in 0..tag_count {
                let (__tag_id, _) = decode_unsigned_varint(buf)?;
                let (__tag_len, _) = decode_unsigned_varint(buf)?;
                match __tag_id {
                    0 => {
                        offline_log_dirs = KafkaCodec::decode(buf, version, true)?;
                    }
                    _ => {
                        buf.advance(__tag_len as usize);
                    }
                }
            }
        }
        Ok(Self {
            broker_id,
            broker_epoch,
            current_metadata_offset,
            want_fence,
            want_shut_down,
            offline_log_dirs,
        })
    }
}
impl KafkaCodec for BrokerHeartbeatRequest {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.broker_id.encode(buf, version, is_flexible)?;
        self.broker_epoch.encode(buf, version, is_flexible)?;
        self.current_metadata_offset
            .encode(buf, version, is_flexible)?;
        self.want_fence.encode(buf, version, is_flexible)?;
        self.want_shut_down.encode(buf, version, is_flexible)?;
        if 1 <= version.0 && !is_flexible {
            self.offline_log_dirs.encode(buf, version, is_flexible)?;
        }
        if is_flexible {
            let mut tag_count = 0u64;
            if !self.offline_log_dirs.is_empty() {
                tag_count += 1;
            }
            encode_unsigned_varint(tag_count, buf);
            if !self.offline_log_dirs.is_empty() {
                encode_unsigned_varint(0u64, buf);
                let mut tmp_buf = bytes::BytesMut::new();
                self.offline_log_dirs.encode(&mut tmp_buf, version, true)?;
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
        let broker_id = KafkaCodec::decode(buf, version, is_flexible)?;
        let broker_epoch = KafkaCodec::decode(buf, version, is_flexible)?;
        let current_metadata_offset = KafkaCodec::decode(buf, version, is_flexible)?;
        let want_fence = KafkaCodec::decode(buf, version, is_flexible)?;
        let want_shut_down = KafkaCodec::decode(buf, version, is_flexible)?;
        let mut offline_log_dirs = if 1 <= version.0 {
            if is_flexible {
                Vec::new()
            } else {
                KafkaCodec::decode(buf, version, is_flexible)?
            }
        } else {
            Vec::new()
        };
        if is_flexible {
            let (tag_count, _) = decode_unsigned_varint(buf)?;
            for _ in 0..tag_count {
                let (__tag_id, _) = decode_unsigned_varint(buf)?;
                let (__tag_len, _) = decode_unsigned_varint(buf)?;
                match __tag_id {
                    0 => {
                        offline_log_dirs = KafkaCodec::decode(buf, version, true)?;
                    }
                    _ => {
                        buf.advance(__tag_len as usize);
                    }
                }
            }
        }
        Ok(Self {
            broker_id,
            broker_epoch,
            current_metadata_offset,
            want_fence,
            want_shut_down,
            offline_log_dirs,
        })
    }
}

#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// BrokerHeartbeatRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
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

impl ApiRequest for BrokerHeartbeatRequest {
    type Response = crate::generated::BrokerHeartbeatResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(63)
    }
    fn get_min_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(0)
    }
    fn get_max_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(1)
    }
    fn serialize(
        &self,
        version: crate::traits::ApiVersion,
        buf: &mut BytesMut,
    ) -> Result<(), SerializationError> {
        assert!(
            (0) <= version.0 && version.0 <= (1),
            "version {} is not supported by {} (supported: 0-1)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = true;
        self.broker_id
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode BrokerId"))?;
        self.broker_epoch
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode BrokerEpoch"))?;
        self.current_metadata_offset
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode CurrentMetadataOffset"))?;
        self.want_fence
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode WantFence"))?;
        self.want_shut_down
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode WantShutDown"))?;
        if (1) <= version.0 {
            self.offline_log_dirs
                .encode_flexible(buf, is_flexible)
                .map_err(|_| SerializationError::Encode("failed to encode OfflineLogDirs"))?;
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
        let is_flexible = true;
        let broker_id = <i32 as KafkaDeserialize>::decode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode BrokerId"))?;
        let broker_epoch = <i64 as KafkaDeserialize>::decode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode BrokerEpoch"))?;
        let current_metadata_offset = <i64 as KafkaDeserialize>::decode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode CurrentMetadataOffset"))?;
        let want_fence = <bool as KafkaDeserialize>::decode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode WantFence"))?;
        let want_shut_down = <bool as KafkaDeserialize>::decode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode WantShutDown"))?;
        let offline_log_dirs = if (1) <= version.0 {
            <Vec<[u8; 16]> as KafkaDeserialize>::decode_flexible(buf, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode OfflineLogDirs"))?
        } else {
            Default::default()
        };
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
impl KafkaSerialize for BrokerHeartbeatRequest {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.broker_id
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode BrokerId".into(),
            })?;
        self.broker_epoch
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode BrokerEpoch".into(),
            })?;
        self.current_metadata_offset
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode CurrentMetadataOffset".into(),
            })?;
        self.want_fence
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode WantFence".into(),
            })?;
        self.want_shut_down
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode WantShutDown".into(),
            })?;
        self.offline_log_dirs
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode OfflineLogDirs".into(),
            })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(
        &self,
        buf: &mut B,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.broker_id
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode BrokerId".into(),
            })?;
        self.broker_epoch
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode BrokerEpoch".into(),
            })?;
        self.current_metadata_offset
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode CurrentMetadataOffset".into(),
            })?;
        self.want_fence
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode WantFence".into(),
            })?;
        self.want_shut_down
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode WantShutDown".into(),
            })?;
        self.offline_log_dirs
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode OfflineLogDirs".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for BrokerHeartbeatRequest {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let broker_id =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode BrokerId".into(),
            })?;
        let broker_epoch =
            <i64 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode BrokerEpoch".into(),
            })?;
        let current_metadata_offset =
            <i64 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode CurrentMetadataOffset".into(),
            })?;
        let want_fence =
            <bool as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode WantFence".into(),
            })?;
        let want_shut_down =
            <bool as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode WantShutDown".into(),
            })?;
        let offline_log_dirs = <Vec<[u8; 16]> as KafkaDeserialize>::decode(buf).map_err(|_| {
            DecodeError::Protocol {
                message: "failed to decode OfflineLogDirs".into(),
            }
        })?;
        Ok(Self {
            broker_id,
            broker_epoch,
            current_metadata_offset,
            want_fence,
            want_shut_down,
            offline_log_dirs,
        })
    }
    fn decode_flexible<B: Buf>(buf: &mut B, is_flexible: bool) -> Result<Self, DecodeError> {
        let broker_id =
            <i32 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode BrokerId".into(),
                }
            })?;
        let broker_epoch =
            <i64 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode BrokerEpoch".into(),
                }
            })?;
        let current_metadata_offset = <i64 as KafkaDeserialize>::decode_flexible(buf, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode CurrentMetadataOffset".into(),
            })?;
        let want_fence =
            <bool as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode WantFence".into(),
                }
            })?;
        let want_shut_down = <bool as KafkaDeserialize>::decode_flexible(buf, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode WantShutDown".into(),
            })?;
        let offline_log_dirs =
            <Vec<[u8; 16]> as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode OfflineLogDirs".into(),
                },
            )?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
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

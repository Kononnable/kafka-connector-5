#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// InitProducerIdResponse
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct InitProducerIdResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// The error code, or 0 if there was no error.
    pub error_code: i16,
    /// The current producer id.
    pub producer_id: i64,
    /// The current epoch associated with the producer id.
    pub producer_epoch: i16,
    /// The producer id for ongoing transaction when KeepPreparedTxn is used, -1 if there is no transaction ongoing.
    /// Available in version 6+.
    pub ongoing_txn_producer_id: i64,
    /// The epoch associated with the  producer id for ongoing transaction when KeepPreparedTxn is used, -1 if there is no transaction ongoing.
    /// Available in version 6+.
    pub ongoing_txn_producer_epoch: i16,
}

impl ApiResponse for InitProducerIdResponse {
    type Request = crate::generated::InitProducerIdRequest;
    fn get_api_key() -> ApiKey {
        ApiKey::new(22)
    }
    fn get_min_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(0)
    }
    fn get_max_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(6)
    }
    fn get_min_flexible_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(2)
    }
    fn serialize(
        &self,
        version: crate::traits::ApiVersion,
        buf: &mut BytesMut,
    ) -> Result<(), SerializationError> {
        assert!(
            (0) <= version.0 && version.0 <= (6),
            "version {} is not supported by {} (supported: 0-6)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        self.throttle_time_ms
            .encode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode ThrottleTimeMs"))?;
        self.error_code
            .encode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode ErrorCode"))?;
        self.producer_id
            .encode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode ProducerId"))?;
        self.producer_epoch
            .encode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode ProducerEpoch"))?;
        if (6) <= version.0 {
            self.ongoing_txn_producer_id
                .encode(buf, version, is_flexible)
                .map_err(|_| SerializationError::Encode("failed to encode OngoingTxnProducerId"))?;
        } else if self.ongoing_txn_producer_id != 0 {
            return Err(SerializationError::Encode(
                "field 'OngoingTxnProducerId' is not available in this version",
            ));
        }
        if (6) <= version.0 {
            self.ongoing_txn_producer_epoch
                .encode(buf, version, is_flexible)
                .map_err(|_| {
                    SerializationError::Encode("failed to encode OngoingTxnProducerEpoch")
                })?;
        } else if self.ongoing_txn_producer_epoch != 0 {
            return Err(SerializationError::Encode(
                "field 'OngoingTxnProducerEpoch' is not available in this version",
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
        let throttle_time_ms = <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode ThrottleTimeMs"))?;
        let error_code = <i16 as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode ErrorCode"))?;
        let producer_id = <i64 as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode ProducerId"))?;
        let producer_epoch = <i16 as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode ProducerEpoch"))?;
        let ongoing_txn_producer_id = if (6) <= version.0 {
            <i64 as KafkaDeserialize>::decode(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode OngoingTxnProducerId"))?
        } else {
            Default::default()
        };
        let ongoing_txn_producer_epoch = if (6) <= version.0 {
            <i16 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                SerializationError::Decode("failed to decode OngoingTxnProducerEpoch")
            })?
        } else {
            Default::default()
        };
        Ok(Self {
            throttle_time_ms,
            error_code,
            producer_id,
            producer_epoch,
            ongoing_txn_producer_id,
            ongoing_txn_producer_epoch,
        })
    }
}
impl KafkaSerialize for InitProducerIdResponse {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.throttle_time_ms
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ThrottleTimeMs".into(),
            })?;
        self.error_code
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ErrorCode".into(),
            })?;
        self.producer_id
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ProducerId".into(),
            })?;
        self.producer_epoch
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ProducerEpoch".into(),
            })?;
        if (6) <= version.0 {
            self.ongoing_txn_producer_id
                .encode(buf, version, is_flexible)
                .map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode OngoingTxnProducerId".into(),
                })?;
        }
        if (6) <= version.0 {
            self.ongoing_txn_producer_epoch
                .encode(buf, version, is_flexible)
                .map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode OngoingTxnProducerEpoch".into(),
                })?;
        }
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for InitProducerIdResponse {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        let throttle_time_ms = <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode ThrottleTimeMs".into(),
            })?;
        let error_code =
            <i16 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode ErrorCode".into(),
                }
            })?;
        let producer_id =
            <i64 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode ProducerId".into(),
                }
            })?;
        let producer_epoch =
            <i16 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode ProducerEpoch".into(),
                }
            })?;
        let ongoing_txn_producer_id = if (6) <= version.0 {
            <i64 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode OngoingTxnProducerId".into(),
                }
            })?
        } else {
            Default::default()
        };
        let ongoing_txn_producer_epoch = if (6) <= version.0 {
            <i16 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode OngoingTxnProducerEpoch".into(),
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
            throttle_time_ms,
            error_code,
            producer_id,
            producer_epoch,
            ongoing_txn_producer_id,
            ongoing_txn_producer_epoch,
        })
    }
}

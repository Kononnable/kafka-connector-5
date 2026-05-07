#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// InitProducerIdRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct InitProducerIdRequest {
    /// The transactional id, or null if the producer is not transactional.
    pub transactional_id: Option<String>,
    /// The time in ms to wait before aborting idle transactions sent by this producer. This is only relevant if a TransactionalId has been defined.
    pub transaction_timeout_ms: i32,
    /// The producer id. This is used to disambiguate requests if a transactional id is reused following its expiration.
    /// Available in version 3+.
    pub producer_id: i64,
    /// The producer's current epoch. This will be checked against the producer epoch on the broker, and the request will return an error if they do not match.
    /// Available in version 3+.
    pub producer_epoch: i16,
    /// True if the client wants to enable two-phase commit (2PC) protocol for transactions.
    /// Available in version 6+.
    pub enable2_pc: bool,
    /// True if the client wants to keep the currently ongoing transaction instead of aborting it.
    /// Available in version 6+.
    pub keep_prepared_txn: bool,
}

impl ApiRequest for InitProducerIdRequest {
    type Response = crate::generated::InitProducerIdResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(22)
    }
    fn get_min_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(0)
    }
    fn get_max_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(6)
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
        let is_flexible = (2) <= version.0;
        self.transactional_id
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode TransactionalId"))?;
        self.transaction_timeout_ms
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode TransactionTimeoutMs"))?;
        if (3) <= version.0 {
            self.producer_id
                .encode_flexible(buf, is_flexible)
                .map_err(|_| SerializationError::Encode("failed to encode ProducerId"))?;
        }
        if (3) <= version.0 {
            self.producer_epoch
                .encode_flexible(buf, is_flexible)
                .map_err(|_| SerializationError::Encode("failed to encode ProducerEpoch"))?;
        }
        if (6) <= version.0 {
            self.enable2_pc
                .encode_flexible(buf, is_flexible)
                .map_err(|_| SerializationError::Encode("failed to encode Enable2Pc"))?;
        }
        if (6) <= version.0 {
            self.keep_prepared_txn
                .encode_flexible(buf, is_flexible)
                .map_err(|_| SerializationError::Encode("failed to encode KeepPreparedTxn"))?;
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
        let is_flexible = (2) <= version.0;
        let transactional_id =
            <Option<String> as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode TransactionalId"))?;
        let transaction_timeout_ms =
            <i32 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode TransactionTimeoutMs"))?;
        let producer_id = if (3) <= version.0 {
            <i64 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode ProducerId"))?
        } else {
            Default::default()
        };
        let producer_epoch = if (3) <= version.0 {
            <i16 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode ProducerEpoch"))?
        } else {
            Default::default()
        };
        let enable2_pc = if (6) <= version.0 {
            <bool as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode Enable2Pc"))?
        } else {
            Default::default()
        };
        let keep_prepared_txn = if (6) <= version.0 {
            <bool as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode KeepPreparedTxn"))?
        } else {
            Default::default()
        };
        Ok(Self {
            transactional_id,
            transaction_timeout_ms,
            producer_id,
            producer_epoch,
            enable2_pc,
            keep_prepared_txn,
        })
    }
}
impl KafkaSerialize for InitProducerIdRequest {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.transactional_id
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode TransactionalId".into(),
            })?;
        self.transaction_timeout_ms
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode TransactionTimeoutMs".into(),
            })?;
        self.producer_id
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ProducerId".into(),
            })?;
        self.producer_epoch
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ProducerEpoch".into(),
            })?;
        self.enable2_pc
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Enable2Pc".into(),
            })?;
        self.keep_prepared_txn
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode KeepPreparedTxn".into(),
            })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(
        &self,
        buf: &mut B,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        if is_flexible {
            if let Some(ref __val) = self.transactional_id {
                crate::protocol::serialization::encode_unsigned_varint(1u64, buf);
                __val
                    .encode_flexible(buf, true)
                    .map_err(|_| EncodeError::ValueTooLarge {
                        message: "failed to encode TransactionalId".into(),
                    })?;
            } else {
                crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
            }
        } else {
            if let Some(ref __val) = self.transactional_id {
                __val.encode(buf).map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode TransactionalId".into(),
                })?;
            }
        }
        self.transaction_timeout_ms
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode TransactionTimeoutMs".into(),
            })?;
        self.producer_id
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ProducerId".into(),
            })?;
        self.producer_epoch
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ProducerEpoch".into(),
            })?;
        self.enable2_pc
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Enable2Pc".into(),
            })?;
        self.keep_prepared_txn
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode KeepPreparedTxn".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for InitProducerIdRequest {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] classic decode field `TransactionalId` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let transactional_id = <Option<String> as KafkaDeserialize>::decode(buf).map_err(|_| {
            DecodeError::Protocol {
                message: "failed to decode TransactionalId".into(),
            }
        })?;
        tracing::trace!(
            "  [{}] classic decode field `TransactionTimeoutMs` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let transaction_timeout_ms =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode TransactionTimeoutMs".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `ProducerId` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let producer_id =
            <i64 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ProducerId".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `ProducerEpoch` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let producer_epoch =
            <i16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ProducerEpoch".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `Enable2Pc` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let enable2_pc =
            <bool as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode Enable2Pc".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `KeepPreparedTxn` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let keep_prepared_txn =
            <bool as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode KeepPreparedTxn".into(),
            })?;
        Ok(Self {
            transactional_id,
            transaction_timeout_ms,
            producer_id,
            producer_epoch,
            enable2_pc,
            keep_prepared_txn,
        })
    }
    fn decode_flexible<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] decoding field `TransactionalId` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let transactional_id = if is_flexible {
            <Option<String> as KafkaDeserialize>::decode_flexible(buf, version, true).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode TransactionalId".into(),
                },
            )?
        } else {
            <Option<String> as KafkaDeserialize>::decode(buf).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode TransactionalId".into(),
                }
            })?
        };
        tracing::trace!(
            "  [{}] decoding field `TransactionTimeoutMs` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let transaction_timeout_ms =
            <i32 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode TransactionTimeoutMs".into(),
                },
            )?;
        tracing::trace!(
            "  [{}] decoding field `ProducerId` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let producer_id = if (3) <= version.0 {
            <i64 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode ProducerId".into(),
                }
            })?
        } else {
            Default::default()
        };
        tracing::trace!(
            "  [{}] decoding field `ProducerEpoch` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let producer_epoch = if (3) <= version.0 {
            <i16 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode ProducerEpoch".into(),
                }
            })?
        } else {
            Default::default()
        };
        tracing::trace!(
            "  [{}] decoding field `Enable2Pc` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let enable2_pc = if (6) <= version.0 {
            <bool as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode Enable2Pc".into(),
                },
            )?
        } else {
            Default::default()
        };
        tracing::trace!(
            "  [{}] decoding field `KeepPreparedTxn` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let keep_prepared_txn = if (6) <= version.0 {
            <bool as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode KeepPreparedTxn".into(),
                },
            )?
        } else {
            Default::default()
        };
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            transactional_id,
            transaction_timeout_ms,
            producer_id,
            producer_epoch,
            enable2_pc,
            keep_prepared_txn,
        })
    }
}

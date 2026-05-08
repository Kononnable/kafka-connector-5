#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// DescribeTransactionsResponse
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct DescribeTransactionsResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// The current state of the transaction.
    pub transaction_states: Vec<TransactionState>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct TopicData {
    /// The topic name.
    pub topic: String,
    /// The partition ids included in the current transaction.
    pub partitions: Vec<i32>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct TransactionState {
    /// The error code.
    pub error_code: i16,
    /// The transactional id.
    pub transactional_id: String,
    /// The current transaction state of the producer.
    pub transaction_state: String,
    /// The timeout in milliseconds for the transaction.
    pub transaction_timeout_ms: i32,
    /// The start time of the transaction in milliseconds.
    pub transaction_start_time_ms: i64,
    /// The current producer id associated with the transaction.
    pub producer_id: i64,
    /// The current epoch associated with the producer id.
    pub producer_epoch: i16,
    /// The set of partitions included in the current transaction (if active). When a transaction is preparing to commit or abort, this will include only partitions which do not have markers.
    pub topics: Vec<TopicData>,
}

impl ApiResponse for DescribeTransactionsResponse {
    type Request = crate::generated::DescribeTransactionsRequest;
    fn get_api_key() -> ApiKey {
        ApiKey::new(65)
    }
    fn get_min_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(0)
    }
    fn get_max_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(0)
    }
    fn get_min_flexible_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(0)
    }
    fn serialize(
        &self,
        version: crate::traits::ApiVersion,
        buf: &mut BytesMut,
    ) -> Result<(), SerializationError> {
        assert!(
            (0) <= version.0 && version.0 <= (0),
            "version {} is not supported by {} (supported: 0-0)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        self.throttle_time_ms
            .encode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode ThrottleTimeMs"))?;
        self.transaction_states
            .encode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode TransactionStates"))?;
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
        let transaction_states =
            <Vec<TransactionState> as KafkaDeserialize>::decode(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode TransactionStates"))?;
        Ok(Self {
            throttle_time_ms,
            transaction_states,
        })
    }
}
impl KafkaSerialize for DescribeTransactionsResponse {
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
        self.transaction_states
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode TransactionStates".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for DescribeTransactionsResponse {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        let throttle_time_ms = <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode ThrottleTimeMs".into(),
            })?;
        let transaction_states =
            <Vec<TransactionState> as KafkaDeserialize>::decode(buf, version, is_flexible)
                .map_err(|_| DecodeError::Protocol {
                    message: "failed to decode TransactionStates".into(),
                })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            throttle_time_ms,
            transaction_states,
        })
    }
}

impl KafkaSerialize for TopicData {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.topic
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Topic".into(),
            })?;
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

impl KafkaDeserialize for TopicData {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        let topic =
            <String as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode Topic".into(),
                }
            })?;
        let partitions = <Vec<i32> as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode Partitions".into(),
            })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self { topic, partitions })
    }
}

impl KafkaSerialize for TransactionState {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.error_code
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ErrorCode".into(),
            })?;
        self.transactional_id
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode TransactionalId".into(),
            })?;
        self.transaction_state
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode TransactionState".into(),
            })?;
        self.transaction_timeout_ms
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode TransactionTimeoutMs".into(),
            })?;
        self.transaction_start_time_ms
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode TransactionStartTimeMs".into(),
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
        self.topics
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Topics".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for TransactionState {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        let error_code =
            <i16 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode ErrorCode".into(),
                }
            })?;
        let transactional_id = <String as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode TransactionalId".into(),
            })?;
        let transaction_state = <String as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode TransactionState".into(),
            })?;
        let transaction_timeout_ms = <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
            message: "failed to decode TransactionTimeoutMs".into(),
        })?;
        let transaction_start_time_ms =
            <i64 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode TransactionStartTimeMs".into(),
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
        let topics = <Vec<TopicData> as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode Topics".into(),
            })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            error_code,
            transactional_id,
            transaction_state,
            transaction_timeout_ms,
            transaction_start_time_ms,
            producer_id,
            producer_epoch,
            topics,
        })
    }
}

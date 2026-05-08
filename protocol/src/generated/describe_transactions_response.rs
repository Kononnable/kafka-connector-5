#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{KafkaDeserialize, KafkaSerialize};
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
        self.throttle_time_ms.encode(buf, version, is_flexible)?;
        self.transaction_states.encode(buf, version, is_flexible)?;
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(
        version: crate::traits::ApiVersion,
        buf: &mut Bytes,
    ) -> Result<Self, SerializationError> {
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let throttle_time_ms = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let transaction_states = KafkaDeserialize::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
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
    ) -> Result<(), crate::traits::SerializationError> {
        self.throttle_time_ms.encode(buf, version, is_flexible)?;
        self.transaction_states.encode(buf, version, is_flexible)?;
        if is_flexible {
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
    ) -> Result<Self, crate::traits::SerializationError> {
        let throttle_time_ms = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let transaction_states = KafkaDeserialize::decode(buf, version, is_flexible)?;
        if is_flexible {
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
    ) -> Result<(), crate::traits::SerializationError> {
        self.topic.encode(buf, version, is_flexible)?;
        self.partitions.encode(buf, version, is_flexible)?;
        if is_flexible {
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
    ) -> Result<Self, crate::traits::SerializationError> {
        let topic = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let partitions = KafkaDeserialize::decode(buf, version, is_flexible)?;
        if is_flexible {
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
    ) -> Result<(), crate::traits::SerializationError> {
        self.error_code.encode(buf, version, is_flexible)?;
        self.transactional_id.encode(buf, version, is_flexible)?;
        self.transaction_state.encode(buf, version, is_flexible)?;
        self.transaction_timeout_ms
            .encode(buf, version, is_flexible)?;
        self.transaction_start_time_ms
            .encode(buf, version, is_flexible)?;
        self.producer_id.encode(buf, version, is_flexible)?;
        self.producer_epoch.encode(buf, version, is_flexible)?;
        self.topics.encode(buf, version, is_flexible)?;
        if is_flexible {
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
    ) -> Result<Self, crate::traits::SerializationError> {
        let error_code = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let transactional_id = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let transaction_state = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let transaction_timeout_ms = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let transaction_start_time_ms = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let producer_id = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let producer_epoch = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let topics = KafkaDeserialize::decode(buf, version, is_flexible)?;
        if is_flexible {
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

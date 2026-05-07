#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{EncodeError, DecodeError, KafkaSerialize, KafkaDeserialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// ListTransactionsResponse
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ListTransactionsResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// The error code, or 0 if there was no error.
    pub error_code: i16,
    /// Set of state filters provided in the request which were unknown to the transaction coordinator.
    pub unknown_state_filters: Vec<String>,
    /// The current state of the transaction for the transactional id.
    pub transaction_states: Vec<TransactionState>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct TransactionState {
    /// The transactional id.
    pub transactional_id: String,
    /// The producer id.
    pub producer_id: i64,
    /// The current transaction state of the producer.
    pub transaction_state: String,
}

impl ApiResponse for ListTransactionsResponse {
    type Request = crate::generated::ListTransactionsRequest;
    fn get_api_key() -> ApiKey { ApiKey::new(66) }
    fn get_min_supported_version() -> crate::traits::ApiVersion { crate::traits::ApiVersion::new(0) }
    fn get_max_supported_version() -> crate::traits::ApiVersion { crate::traits::ApiVersion::new(2) }
    fn serialize(&self, version: crate::traits::ApiVersion, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!((0) <= version.0 && version.0 <= (2), "version {} is not supported by {} (supported: 0-2)", version.0, stringify!(Self));
        let is_flexible = true;
        self.throttle_time_ms.encode_flexible(buf, is_flexible).map_err(|_| SerializationError::Encode("failed to encode ThrottleTimeMs"))?;
        self.error_code.encode_flexible(buf, is_flexible).map_err(|_| SerializationError::Encode("failed to encode ErrorCode"))?;
        self.unknown_state_filters.encode_flexible(buf, is_flexible).map_err(|_| SerializationError::Encode("failed to encode UnknownStateFilters"))?;
        self.transaction_states.encode_flexible(buf, is_flexible).map_err(|_| SerializationError::Encode("failed to encode TransactionStates"))?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(version: crate::traits::ApiVersion, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let is_flexible = true;
        let throttle_time_ms = <i32 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| SerializationError::Decode("failed to decode ThrottleTimeMs"))?;
        let error_code = <i16 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| SerializationError::Decode("failed to decode ErrorCode"))?;
        let unknown_state_filters = <Vec<String> as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| SerializationError::Decode("failed to decode UnknownStateFilters"))?;
        let transaction_states = <Vec<TransactionState> as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| SerializationError::Decode("failed to decode TransactionStates"))?;
        Ok(Self { throttle_time_ms, error_code, unknown_state_filters, transaction_states })
    }
}
impl KafkaSerialize for ListTransactionsResponse {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.throttle_time_ms.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode ThrottleTimeMs".into() })?;
        self.error_code.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode ErrorCode".into() })?;
        self.unknown_state_filters.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode UnknownStateFilters".into() })?;
        self.transaction_states.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode TransactionStates".into() })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(&self, buf: &mut B, is_flexible: bool) -> Result<(), EncodeError> {
        self.throttle_time_ms.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode ThrottleTimeMs".into() })?;
        self.error_code.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode ErrorCode".into() })?;
        self.unknown_state_filters.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode UnknownStateFilters".into() })?;
        self.transaction_states.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode TransactionStates".into() })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for ListTransactionsResponse {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let throttle_time_ms = <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode ThrottleTimeMs".into() })?;
        let error_code = <i16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode ErrorCode".into() })?;
        let unknown_state_filters = <Vec<String> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode UnknownStateFilters".into() })?;
        let transaction_states = <Vec<TransactionState> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode TransactionStates".into() })?;
        Ok(Self { throttle_time_ms, error_code, unknown_state_filters, transaction_states })
    }
    fn decode_flexible<B: Buf>(buf: &mut B, is_flexible: bool) -> Result<Self, DecodeError> {
        let throttle_time_ms = <i32 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode ThrottleTimeMs".into() })?;
        let error_code = <i16 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode ErrorCode".into() })?;
        let unknown_state_filters = <Vec<String> as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode UnknownStateFilters".into() })?;
        let transaction_states = <Vec<TransactionState> as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode TransactionStates".into() })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self { throttle_time_ms, error_code, unknown_state_filters, transaction_states })
    }
}

impl KafkaSerialize for TransactionState {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.transactional_id.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode TransactionalId".into() })?;
        self.producer_id.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode ProducerId".into() })?;
        self.transaction_state.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode TransactionState".into() })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(&self, buf: &mut B, is_flexible: bool) -> Result<(), EncodeError> {
        self.transactional_id.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode TransactionalId".into() })?;
        self.producer_id.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode ProducerId".into() })?;
        self.transaction_state.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode TransactionState".into() })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for TransactionState {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let transactional_id = <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode TransactionalId".into() })?;
        let producer_id = <i64 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode ProducerId".into() })?;
        let transaction_state = <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode TransactionState".into() })?;
        Ok(Self { transactional_id, producer_id, transaction_state })
    }
    fn decode_flexible<B: Buf>(buf: &mut B, is_flexible: bool) -> Result<Self, DecodeError> {
        let transactional_id = <String as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode TransactionalId".into() })?;
        let producer_id = <i64 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode ProducerId".into() })?;
        let transaction_state = <String as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode TransactionState".into() })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self { transactional_id, producer_id, transaction_state })
    }
}


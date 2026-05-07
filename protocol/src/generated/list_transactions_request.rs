#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// ListTransactionsRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ListTransactionsRequest {
    /// The transaction states to filter by: if empty, all transactions are returned; if non-empty, then only transactions matching one of the filtered states will be returned.
    pub state_filters: Vec<String>,
    /// The producerIds to filter by: if empty, all transactions will be returned; if non-empty, only transactions which match one of the filtered producerIds will be returned.
    pub producer_id_filters: Vec<i64>,
    /// Duration (in millis) to filter by: if < 0, all transactions will be returned; otherwise, only transactions running longer than this duration will be returned.
    /// Available in version 1+.
    pub duration_filter: i64,
    /// The transactional ID regular expression pattern to filter by: if it is empty or null, all transactions are returned; Otherwise then only the transactions matching the given regular expression will be returned.
    /// Available in version 2+.
    pub transactional_id_pattern: Option<String>,
}

impl ApiRequest for ListTransactionsRequest {
    type Response = crate::generated::ListTransactionsResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(66)
    }
    fn get_min_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(0)
    }
    fn get_max_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(2)
    }
    fn serialize(
        &self,
        version: crate::traits::ApiVersion,
        buf: &mut BytesMut,
    ) -> Result<(), SerializationError> {
        assert!(
            (0) <= version.0 && version.0 <= (2),
            "version {} is not supported by {} (supported: 0-2)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = true;
        self.state_filters
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode StateFilters"))?;
        self.producer_id_filters
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode ProducerIdFilters"))?;
        if (1) <= version.0 {
            self.duration_filter
                .encode_flexible(buf, is_flexible)
                .map_err(|_| SerializationError::Encode("failed to encode DurationFilter"))?;
        }
        if (2) <= version.0 {
            self.transactional_id_pattern
                .encode_flexible(buf, is_flexible)
                .map_err(|_| {
                    SerializationError::Encode("failed to encode TransactionalIdPattern")
                })?;
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
        let state_filters = <Vec<String> as KafkaDeserialize>::decode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode StateFilters"))?;
        let producer_id_filters = <Vec<i64> as KafkaDeserialize>::decode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode ProducerIdFilters"))?;
        let duration_filter = if (1) <= version.0 {
            <i64 as KafkaDeserialize>::decode_flexible(buf, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode DurationFilter"))?
        } else {
            Default::default()
        };
        let transactional_id_pattern = if (2) <= version.0 {
            <Option<String> as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(
                |_| SerializationError::Decode("failed to decode TransactionalIdPattern"),
            )?
        } else {
            Default::default()
        };
        Ok(Self {
            state_filters,
            producer_id_filters,
            duration_filter,
            transactional_id_pattern,
        })
    }
}
impl KafkaSerialize for ListTransactionsRequest {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.state_filters
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode StateFilters".into(),
            })?;
        self.producer_id_filters
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ProducerIdFilters".into(),
            })?;
        self.duration_filter
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode DurationFilter".into(),
            })?;
        self.transactional_id_pattern
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode TransactionalIdPattern".into(),
            })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(
        &self,
        buf: &mut B,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.state_filters
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode StateFilters".into(),
            })?;
        self.producer_id_filters
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ProducerIdFilters".into(),
            })?;
        self.duration_filter
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode DurationFilter".into(),
            })?;
        if is_flexible {
            if let Some(ref __val) = self.transactional_id_pattern {
                crate::protocol::serialization::encode_unsigned_varint(1u64, buf);
                __val
                    .encode_flexible(buf, true)
                    .map_err(|_| EncodeError::ValueTooLarge {
                        message: "failed to encode TransactionalIdPattern".into(),
                    })?;
            } else {
                crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
            }
        } else {
            if let Some(ref __val) = self.transactional_id_pattern {
                __val.encode(buf).map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode TransactionalIdPattern".into(),
                })?;
            }
        }
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for ListTransactionsRequest {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] classic decode field `StateFilters` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let state_filters =
            <Vec<String> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode StateFilters".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `ProducerIdFilters` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let producer_id_filters =
            <Vec<i64> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ProducerIdFilters".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `DurationFilter` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let duration_filter =
            <i64 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode DurationFilter".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `TransactionalIdPattern` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let transactional_id_pattern =
            <Option<String> as KafkaDeserialize>::decode(buf).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode TransactionalIdPattern".into(),
                }
            })?;
        Ok(Self {
            state_filters,
            producer_id_filters,
            duration_filter,
            transactional_id_pattern,
        })
    }
    fn decode_flexible<B: Buf>(buf: &mut B, is_flexible: bool) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] decoding field `StateFilters` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let state_filters = <Vec<String> as KafkaDeserialize>::decode_flexible(buf, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode StateFilters".into(),
            })?;
        tracing::trace!(
            "  [{}] decoding field `ProducerIdFilters` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let producer_id_filters = <Vec<i64> as KafkaDeserialize>::decode_flexible(buf, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode ProducerIdFilters".into(),
            })?;
        tracing::trace!(
            "  [{}] decoding field `DurationFilter` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let duration_filter = <i64 as KafkaDeserialize>::decode_flexible(buf, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode DurationFilter".into(),
            })?;
        tracing::trace!(
            "  [{}] decoding field `TransactionalIdPattern` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let transactional_id_pattern = if is_flexible {
            <Option<String> as KafkaDeserialize>::decode_flexible(buf, true).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode TransactionalIdPattern".into(),
                }
            })?
        } else {
            <Option<String> as KafkaDeserialize>::decode(buf).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode TransactionalIdPattern".into(),
                }
            })?
        };
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            state_filters,
            producer_id_filters,
            duration_filter,
            transactional_id_pattern,
        })
    }
}

#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

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
    /// Available in version 4+.
    pub results_by_transaction: Vec<AddPartitionsToTxnResult>,
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
    /// The transactional id corresponding to the transaction.
    /// Available in version 4+.
    pub transactional_id: String,
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
    fn get_min_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(0)
    }
    fn get_max_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(5)
    }
    fn serialize(
        &self,
        version: crate::traits::ApiVersion,
        buf: &mut BytesMut,
    ) -> Result<(), SerializationError> {
        assert!(
            (0) <= version.0 && version.0 <= (5),
            "version {} is not supported by {} (supported: 0-5)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = (3) <= version.0;
        self.throttle_time_ms
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode ThrottleTimeMs"))?;
        if (4) <= version.0 {
            self.error_code
                .encode_flexible(buf, is_flexible)
                .map_err(|_| SerializationError::Encode("failed to encode ErrorCode"))?;
        } else if self.error_code != 0 {
            return Err(SerializationError::Encode(
                "field 'ErrorCode' is not available in this version",
            ));
        }
        if (4) <= version.0 {
            self.results_by_transaction
                .encode_flexible(buf, is_flexible)
                .map_err(|_| SerializationError::Encode("failed to encode ResultsByTransaction"))?;
        } else if !self.results_by_transaction.is_empty() {
            return Err(SerializationError::Encode(
                "field 'ResultsByTransaction' is not available in this version",
            ));
        }
        if (0) <= version.0 && version.0 <= (3) {
            self.results_by_topic_v3_and_below
                .encode_flexible(buf, is_flexible)
                .map_err(|_| {
                    SerializationError::Encode("failed to encode ResultsByTopicV3AndBelow")
                })?;
        } else if !self.results_by_topic_v3_and_below.is_empty() {
            return Err(SerializationError::Encode(
                "field 'ResultsByTopicV3AndBelow' is not available in this version",
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
        let is_flexible = (3) <= version.0;
        let throttle_time_ms =
            <i32 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode ThrottleTimeMs"))?;
        let error_code = if (4) <= version.0 {
            <i16 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode ErrorCode"))?
        } else {
            Default::default()
        };
        let results_by_transaction = if (4) <= version.0 {
            <Vec<AddPartitionsToTxnResult> as KafkaDeserialize>::decode_flexible(
                buf,
                version,
                is_flexible,
            )
            .map_err(|_| SerializationError::Decode("failed to decode ResultsByTransaction"))?
        } else {
            Default::default()
        };
        let results_by_topic_v3_and_below = if (0) <= version.0 && version.0 <= (3) {
            <Vec<AddPartitionsToTxnTopicResult> as KafkaDeserialize>::decode_flexible(
                buf,
                version,
                is_flexible,
            )
            .map_err(|_| SerializationError::Decode("failed to decode ResultsByTopicV3AndBelow"))?
        } else {
            Default::default()
        };
        Ok(Self {
            throttle_time_ms,
            error_code,
            results_by_transaction,
            results_by_topic_v3_and_below,
        })
    }
}
impl KafkaSerialize for AddPartitionsToTxnResponse {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.throttle_time_ms
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ThrottleTimeMs".into(),
            })?;
        self.error_code
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ErrorCode".into(),
            })?;
        self.results_by_transaction
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ResultsByTransaction".into(),
            })?;
        self.results_by_topic_v3_and_below
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ResultsByTopicV3AndBelow".into(),
            })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(
        &self,
        buf: &mut B,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.throttle_time_ms
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ThrottleTimeMs".into(),
            })?;
        self.error_code
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ErrorCode".into(),
            })?;
        self.results_by_transaction
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ResultsByTransaction".into(),
            })?;
        self.results_by_topic_v3_and_below
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ResultsByTopicV3AndBelow".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for AddPartitionsToTxnResponse {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] classic decode field `ThrottleTimeMs` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let throttle_time_ms =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ThrottleTimeMs".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `ErrorCode` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let error_code =
            <i16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ErrorCode".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `ResultsByTransaction` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let results_by_transaction =
            <Vec<AddPartitionsToTxnResult> as KafkaDeserialize>::decode(buf).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode ResultsByTransaction".into(),
                }
            })?;
        tracing::trace!(
            "  [{}] classic decode field `ResultsByTopicV3AndBelow` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let results_by_topic_v3_and_below =
            <Vec<AddPartitionsToTxnTopicResult> as KafkaDeserialize>::decode(buf).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode ResultsByTopicV3AndBelow".into(),
                },
            )?;
        Ok(Self {
            throttle_time_ms,
            error_code,
            results_by_transaction,
            results_by_topic_v3_and_below,
        })
    }
    fn decode_flexible<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] decoding field `ThrottleTimeMs` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let throttle_time_ms =
            <i32 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode ThrottleTimeMs".into(),
                },
            )?;
        tracing::trace!(
            "  [{}] decoding field `ErrorCode` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let error_code = if (4) <= version.0 {
            <i16 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode ErrorCode".into(),
                }
            })?
        } else {
            Default::default()
        };
        tracing::trace!(
            "  [{}] decoding field `ResultsByTransaction` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let results_by_transaction = if (4) <= version.0 {
            <Vec<AddPartitionsToTxnResult> as KafkaDeserialize>::decode_flexible(
                buf,
                version,
                is_flexible,
            )
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode ResultsByTransaction".into(),
            })?
        } else {
            Default::default()
        };
        tracing::trace!(
            "  [{}] decoding field `ResultsByTopicV3AndBelow` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let results_by_topic_v3_and_below = if (0) <= version.0 && version.0 <= (3) {
            <Vec<AddPartitionsToTxnTopicResult> as KafkaDeserialize>::decode_flexible(
                buf,
                version,
                is_flexible,
            )
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode ResultsByTopicV3AndBelow".into(),
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
            results_by_transaction,
            results_by_topic_v3_and_below,
        })
    }
}

impl KafkaSerialize for AddPartitionsToTxnPartitionResult {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.partition_index
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode PartitionIndex".into(),
            })?;
        self.partition_error_code
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode PartitionErrorCode".into(),
            })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(
        &self,
        buf: &mut B,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.partition_index
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode PartitionIndex".into(),
            })?;
        self.partition_error_code
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode PartitionErrorCode".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for AddPartitionsToTxnPartitionResult {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] classic decode field `PartitionIndex` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let partition_index =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode PartitionIndex".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `PartitionErrorCode` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let partition_error_code =
            <i16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode PartitionErrorCode".into(),
            })?;
        Ok(Self {
            partition_index,
            partition_error_code,
        })
    }
    fn decode_flexible<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] decoding field `PartitionIndex` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let partition_index = <i32 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode PartitionIndex".into(),
            })?;
        tracing::trace!(
            "  [{}] decoding field `PartitionErrorCode` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let partition_error_code =
            <i16 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode PartitionErrorCode".into(),
                },
            )?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            partition_index,
            partition_error_code,
        })
    }
}

impl KafkaSerialize for AddPartitionsToTxnResult {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.transactional_id
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode TransactionalId".into(),
            })?;
        self.topic_results
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode TopicResults".into(),
            })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(
        &self,
        buf: &mut B,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.transactional_id
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode TransactionalId".into(),
            })?;
        self.topic_results
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode TopicResults".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for AddPartitionsToTxnResult {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] classic decode field `TransactionalId` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let transactional_id =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode TransactionalId".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `TopicResults` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let topic_results = <Vec<AddPartitionsToTxnTopicResult> as KafkaDeserialize>::decode(buf)
            .map_err(|_| DecodeError::Protocol {
            message: "failed to decode TopicResults".into(),
        })?;
        Ok(Self {
            transactional_id,
            topic_results,
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
        let transactional_id = if (4) <= version.0 {
            <String as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode TransactionalId".into(),
                },
            )?
        } else {
            Default::default()
        };
        tracing::trace!(
            "  [{}] decoding field `TopicResults` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let topic_results = if (4) <= version.0 {
            <Vec<AddPartitionsToTxnTopicResult> as KafkaDeserialize>::decode_flexible(
                buf,
                version,
                is_flexible,
            )
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode TopicResults".into(),
            })?
        } else {
            Default::default()
        };
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            transactional_id,
            topic_results,
        })
    }
}

impl KafkaSerialize for AddPartitionsToTxnTopicResult {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.name
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Name".into(),
            })?;
        self.results_by_partition
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ResultsByPartition".into(),
            })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(
        &self,
        buf: &mut B,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.name
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Name".into(),
            })?;
        self.results_by_partition
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ResultsByPartition".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for AddPartitionsToTxnTopicResult {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] classic decode field `Name` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let name =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode Name".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `ResultsByPartition` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let results_by_partition =
            <Vec<AddPartitionsToTxnPartitionResult> as KafkaDeserialize>::decode(buf).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode ResultsByPartition".into(),
                },
            )?;
        Ok(Self {
            name,
            results_by_partition,
        })
    }
    fn decode_flexible<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] decoding field `Name` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let name = <String as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode Name".into(),
            })?;
        tracing::trace!(
            "  [{}] decoding field `ResultsByPartition` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let results_by_partition =
            <Vec<AddPartitionsToTxnPartitionResult> as KafkaDeserialize>::decode_flexible(
                buf,
                version,
                is_flexible,
            )
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode ResultsByPartition".into(),
            })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            name,
            results_by_partition,
        })
    }
}

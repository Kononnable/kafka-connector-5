#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// WriteTxnMarkersRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct WriteTxnMarkersRequest {
    /// The transaction markers to be written.
    pub markers: Vec<WritableTxnMarker>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct WritableTxnMarker {
    /// The current producer ID.
    pub producer_id: i64,
    /// The current epoch associated with the producer ID.
    pub producer_epoch: i16,
    /// The result of the transaction to write to the partitions (false = ABORT, true = COMMIT).
    pub transaction_result: bool,
    /// Each topic that we want to write transaction marker(s) for.
    pub topics: Vec<WritableTxnMarkerTopic>,
    /// Epoch associated with the transaction state partition hosted by this transaction coordinator.
    pub coordinator_epoch: i32,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct WritableTxnMarkerTopic {
    /// The topic name.
    pub name: String,
    /// The indexes of the partitions to write transaction markers for.
    pub partition_indexes: Vec<i32>,
}

impl ApiRequest for WriteTxnMarkersRequest {
    type Response = crate::generated::WriteTxnMarkersResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(27)
    }
    fn get_min_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(1)
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
            (1) <= version.0 && version.0 <= (1),
            "version {} is not supported by {} (supported: 1-1)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = (1) <= version.0;
        self.markers
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode Markers"))?;
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
        let is_flexible = (1) <= version.0;
        let markers = <Vec<WritableTxnMarker> as KafkaDeserialize>::decode_flexible(
            buf,
            version,
            is_flexible,
        )
        .map_err(|_| SerializationError::Decode("failed to decode Markers"))?;
        Ok(Self { markers })
    }
}
impl KafkaSerialize for WriteTxnMarkersRequest {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.markers
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Markers".into(),
            })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(
        &self,
        buf: &mut B,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.markers
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Markers".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for WriteTxnMarkersRequest {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] classic decode field `Markers` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let markers = <Vec<WritableTxnMarker> as KafkaDeserialize>::decode(buf).map_err(|_| {
            DecodeError::Protocol {
                message: "failed to decode Markers".into(),
            }
        })?;
        Ok(Self { markers })
    }
    fn decode_flexible<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] decoding field `Markers` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let markers = <Vec<WritableTxnMarker> as KafkaDeserialize>::decode_flexible(
            buf,
            version,
            is_flexible,
        )
        .map_err(|_| DecodeError::Protocol {
            message: "failed to decode Markers".into(),
        })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self { markers })
    }
}

impl KafkaSerialize for WritableTxnMarker {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
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
        self.transaction_result
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode TransactionResult".into(),
            })?;
        self.topics
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Topics".into(),
            })?;
        self.coordinator_epoch
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode CoordinatorEpoch".into(),
            })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(
        &self,
        buf: &mut B,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
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
        self.transaction_result
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode TransactionResult".into(),
            })?;
        self.topics
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Topics".into(),
            })?;
        self.coordinator_epoch
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode CoordinatorEpoch".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for WritableTxnMarker {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
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
            "  [{}] classic decode field `TransactionResult` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let transaction_result =
            <bool as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode TransactionResult".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `Topics` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let topics =
            <Vec<WritableTxnMarkerTopic> as KafkaDeserialize>::decode(buf).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode Topics".into(),
                }
            })?;
        tracing::trace!(
            "  [{}] classic decode field `CoordinatorEpoch` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let coordinator_epoch =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode CoordinatorEpoch".into(),
            })?;
        Ok(Self {
            producer_id,
            producer_epoch,
            transaction_result,
            topics,
            coordinator_epoch,
        })
    }
    fn decode_flexible<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] decoding field `ProducerId` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let producer_id = <i64 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode ProducerId".into(),
            })?;
        tracing::trace!(
            "  [{}] decoding field `ProducerEpoch` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let producer_epoch = <i16 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode ProducerEpoch".into(),
            })?;
        tracing::trace!(
            "  [{}] decoding field `TransactionResult` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let transaction_result =
            <bool as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode TransactionResult".into(),
                },
            )?;
        tracing::trace!(
            "  [{}] decoding field `Topics` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let topics = <Vec<WritableTxnMarkerTopic> as KafkaDeserialize>::decode_flexible(
            buf,
            version,
            is_flexible,
        )
        .map_err(|_| DecodeError::Protocol {
            message: "failed to decode Topics".into(),
        })?;
        tracing::trace!(
            "  [{}] decoding field `CoordinatorEpoch` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let coordinator_epoch =
            <i32 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode CoordinatorEpoch".into(),
                },
            )?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            producer_id,
            producer_epoch,
            transaction_result,
            topics,
            coordinator_epoch,
        })
    }
}

impl KafkaSerialize for WritableTxnMarkerTopic {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.name
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Name".into(),
            })?;
        self.partition_indexes
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode PartitionIndexes".into(),
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
        self.partition_indexes
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode PartitionIndexes".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for WritableTxnMarkerTopic {
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
            "  [{}] classic decode field `PartitionIndexes` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let partition_indexes =
            <Vec<i32> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode PartitionIndexes".into(),
            })?;
        Ok(Self {
            name,
            partition_indexes,
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
            "  [{}] decoding field `PartitionIndexes` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let partition_indexes =
            <Vec<i32> as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode PartitionIndexes".into(),
                },
            )?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            name,
            partition_indexes,
        })
    }
}

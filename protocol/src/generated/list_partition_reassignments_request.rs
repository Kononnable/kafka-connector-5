#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// ListPartitionReassignmentsRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ListPartitionReassignmentsRequest {
    /// The time in ms to wait for the request to complete.
    pub timeout_ms: i32,
    /// The topics to list partition reassignments for, or null to list everything.
    pub topics: Option<Vec<ListPartitionReassignmentsTopics>>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ListPartitionReassignmentsTopics {
    /// The topic name.
    pub name: String,
    /// The partitions to list partition reassignments for.
    pub partition_indexes: Vec<i32>,
}

impl ApiRequest for ListPartitionReassignmentsRequest {
    type Response = crate::generated::ListPartitionReassignmentsResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(46)
    }
    fn get_min_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(0)
    }
    fn get_max_supported_version() -> crate::traits::ApiVersion {
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
        let is_flexible = true;
        self.timeout_ms
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode TimeoutMs"))?;
        self.topics
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode Topics"))?;
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
        let timeout_ms = <i32 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode TimeoutMs"))?;
        let topics =
            <Option<Vec<ListPartitionReassignmentsTopics>> as KafkaDeserialize>::decode_flexible(
                buf,
                version,
                is_flexible,
            )
            .map_err(|_| SerializationError::Decode("failed to decode Topics"))?;
        Ok(Self { timeout_ms, topics })
    }
}
impl KafkaSerialize for ListPartitionReassignmentsRequest {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.timeout_ms
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode TimeoutMs".into(),
            })?;
        self.topics
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Topics".into(),
            })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(
        &self,
        buf: &mut B,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.timeout_ms
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode TimeoutMs".into(),
            })?;
        if is_flexible {
            if let Some(ref __val) = self.topics {
                crate::protocol::serialization::encode_unsigned_varint(1u64, buf);
                __val
                    .encode_flexible(buf, true)
                    .map_err(|_| EncodeError::ValueTooLarge {
                        message: "failed to encode Topics".into(),
                    })?;
            } else {
                crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
            }
        } else {
            if let Some(ref __val) = self.topics {
                __val.encode(buf).map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode Topics".into(),
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

impl KafkaDeserialize for ListPartitionReassignmentsRequest {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] classic decode field `TimeoutMs` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let timeout_ms =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode TimeoutMs".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `Topics` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let topics =
            <Option<Vec<ListPartitionReassignmentsTopics>> as KafkaDeserialize>::decode(buf)
                .map_err(|_| DecodeError::Protocol {
                    message: "failed to decode Topics".into(),
                })?;
        Ok(Self { timeout_ms, topics })
    }
    fn decode_flexible<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] decoding field `TimeoutMs` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let timeout_ms = <i32 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode TimeoutMs".into(),
            })?;
        tracing::trace!(
            "  [{}] decoding field `Topics` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let topics = if is_flexible {
            <Option<Vec<ListPartitionReassignmentsTopics>> as KafkaDeserialize>::decode_flexible(
                buf, version, true,
            )
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode Topics".into(),
            })?
        } else {
            <Option<Vec<ListPartitionReassignmentsTopics>> as KafkaDeserialize>::decode(buf)
                .map_err(|_| DecodeError::Protocol {
                    message: "failed to decode Topics".into(),
                })?
        };
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self { timeout_ms, topics })
    }
}

impl KafkaSerialize for ListPartitionReassignmentsTopics {
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

impl KafkaDeserialize for ListPartitionReassignmentsTopics {
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

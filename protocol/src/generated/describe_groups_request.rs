#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// DescribeGroupsRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct DescribeGroupsRequest {
    /// The names of the groups to describe.
    pub groups: Vec<String>,
    /// Whether to include authorized operations.
    /// Available in version 3+.
    pub include_authorized_operations: bool,
}

impl ApiRequest for DescribeGroupsRequest {
    type Response = crate::generated::DescribeGroupsResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(15)
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
        let is_flexible = (5) <= version.0;
        self.groups
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode Groups"))?;
        if (3) <= version.0 {
            self.include_authorized_operations
                .encode_flexible(buf, is_flexible)
                .map_err(|_| {
                    SerializationError::Encode("failed to encode IncludeAuthorizedOperations")
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
        let is_flexible = (5) <= version.0;
        let groups = <Vec<String> as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode Groups"))?;
        let include_authorized_operations = if (3) <= version.0 {
            <bool as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(
                |_| SerializationError::Decode("failed to decode IncludeAuthorizedOperations"),
            )?
        } else {
            Default::default()
        };
        Ok(Self {
            groups,
            include_authorized_operations,
        })
    }
}
impl KafkaSerialize for DescribeGroupsRequest {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.groups
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Groups".into(),
            })?;
        self.include_authorized_operations
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode IncludeAuthorizedOperations".into(),
            })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(
        &self,
        buf: &mut B,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.groups
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Groups".into(),
            })?;
        self.include_authorized_operations
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode IncludeAuthorizedOperations".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for DescribeGroupsRequest {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] classic decode field `Groups` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let groups =
            <Vec<String> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode Groups".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `IncludeAuthorizedOperations` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let include_authorized_operations =
            <bool as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode IncludeAuthorizedOperations".into(),
            })?;
        Ok(Self {
            groups,
            include_authorized_operations,
        })
    }
    fn decode_flexible<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] decoding field `Groups` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let groups = <Vec<String> as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode Groups".into(),
            })?;
        tracing::trace!(
            "  [{}] decoding field `IncludeAuthorizedOperations` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let include_authorized_operations = if (3) <= version.0 {
            <bool as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode IncludeAuthorizedOperations".into(),
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
            groups,
            include_authorized_operations,
        })
    }
}

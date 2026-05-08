#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// StreamsGroupDescribeRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct StreamsGroupDescribeRequest {
    /// The ids of the groups to describe
    pub group_ids: Vec<String>,
    /// Whether to include authorized operations.
    pub include_authorized_operations: bool,
}

impl ApiRequest for StreamsGroupDescribeRequest {
    type Response = crate::generated::StreamsGroupDescribeResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(89)
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
        self.group_ids
            .encode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode GroupIds"))?;
        self.include_authorized_operations
            .encode(buf, version, is_flexible)
            .map_err(|_| {
                SerializationError::Encode("failed to encode IncludeAuthorizedOperations")
            })?;
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
        let group_ids = <Vec<String> as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode GroupIds"))?;
        let include_authorized_operations =
            <bool as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                SerializationError::Decode("failed to decode IncludeAuthorizedOperations")
            })?;
        Ok(Self {
            group_ids,
            include_authorized_operations,
        })
    }
}
impl KafkaSerialize for StreamsGroupDescribeRequest {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.group_ids
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode GroupIds".into(),
            })?;
        self.include_authorized_operations
            .encode(buf, version, is_flexible)
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

impl KafkaDeserialize for StreamsGroupDescribeRequest {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        let group_ids = <Vec<String> as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode GroupIds".into(),
            })?;
        let include_authorized_operations =
            <bool as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode IncludeAuthorizedOperations".into(),
                }
            })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            group_ids,
            include_authorized_operations,
        })
    }
}

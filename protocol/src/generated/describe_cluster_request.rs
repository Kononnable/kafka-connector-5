#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// DescribeClusterRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct DescribeClusterRequest {
    /// Whether to include cluster authorized operations.
    pub include_cluster_authorized_operations: bool,
}

impl ApiRequest for DescribeClusterRequest {
    type Response = crate::generated::DescribeClusterResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(60)
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
        self.include_cluster_authorized_operations
            .encode_flexible(buf, is_flexible)
            .map_err(|_| {
                SerializationError::Encode("failed to encode IncludeClusterAuthorizedOperations")
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
        let is_flexible = true;
        let include_cluster_authorized_operations =
            <bool as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| {
                SerializationError::Decode("failed to decode IncludeClusterAuthorizedOperations")
            })?;
        Ok(Self {
            include_cluster_authorized_operations,
        })
    }
}
impl KafkaSerialize for DescribeClusterRequest {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.include_cluster_authorized_operations
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode IncludeClusterAuthorizedOperations".into(),
            })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(
        &self,
        buf: &mut B,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.include_cluster_authorized_operations
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode IncludeClusterAuthorizedOperations".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for DescribeClusterRequest {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let include_cluster_authorized_operations = <bool as KafkaDeserialize>::decode(buf)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode IncludeClusterAuthorizedOperations".into(),
            })?;
        Ok(Self {
            include_cluster_authorized_operations,
        })
    }
    fn decode_flexible<B: Buf>(buf: &mut B, is_flexible: bool) -> Result<Self, DecodeError> {
        let include_cluster_authorized_operations =
            <bool as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode IncludeClusterAuthorizedOperations".into(),
                }
            })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            include_cluster_authorized_operations,
        })
    }
}

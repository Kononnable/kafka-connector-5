#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// ConsumerGroupDescribeRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ConsumerGroupDescribeRequest {
    /// The ids of the groups to describe.
    pub group_ids: Vec<String>,
    /// Whether to include authorized operations.
    pub include_authorized_operations: bool,
}

impl ApiRequest for ConsumerGroupDescribeRequest {
    type Response = crate::generated::ConsumerGroupDescribeResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(69)
    }
    fn get_min_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(0)
    }
    fn get_max_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(1)
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
            (0) <= version.0 && version.0 <= (1),
            "version {} is not supported by {} (supported: 0-1)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        self.group_ids.encode(buf, version, is_flexible)?;
        self.include_authorized_operations
            .encode(buf, version, is_flexible)?;
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
        let group_ids = <Vec<String> as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let include_authorized_operations =
            <bool as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        Ok(Self {
            group_ids,
            include_authorized_operations,
        })
    }
}
impl KafkaSerialize for ConsumerGroupDescribeRequest {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), crate::traits::SerializationError> {
        self.group_ids.encode(buf, version, is_flexible)?;
        self.include_authorized_operations
            .encode(buf, version, is_flexible)?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for ConsumerGroupDescribeRequest {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, crate::traits::SerializationError> {
        let group_ids = <Vec<String> as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let include_authorized_operations =
            <bool as KafkaDeserialize>::decode(buf, version, is_flexible)?;
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

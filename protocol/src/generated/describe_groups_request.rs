#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// DescribeGroupsRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct DescribeGroupsRequest {
    /// The names of the groups to describe
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
    fn get_min_supported_version() -> ApiVersion {
        ApiVersion::new(0)
    }
    fn get_max_supported_version() -> ApiVersion {
        ApiVersion::new(3)
    }
    fn serialize(&self, version: ApiVersion, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            (0) <= version.0 && version.0 <= (3),
            "version {} is not supported by {} (supported: 0-3)",
            version.0,
            stringify!(Self)
        );
        self.groups
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode Groups"))?;
        if (3) <= version.0 {
            self.include_authorized_operations
                .encode(buf)
                .map_err(|_| {
                    SerializationError::Encode("failed to encode IncludeAuthorizedOperations")
                })?;
        }
        Ok(())
    }
    fn deserialize(version: ApiVersion, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let groups = <Vec<String> as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode Groups"))?;
        let include_authorized_operations = if (3) <= version.0 {
            <bool as KafkaDeserialize>::decode(buf).map_err(|_| {
                SerializationError::Decode("failed to decode IncludeAuthorizedOperations")
            })?
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
}

impl KafkaDeserialize for DescribeGroupsRequest {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let groups =
            <Vec<String> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode Groups".into(),
            })?;
        let include_authorized_operations =
            <bool as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode IncludeAuthorizedOperations".into(),
            })?;
        Ok(Self {
            groups,
            include_authorized_operations,
        })
    }
}

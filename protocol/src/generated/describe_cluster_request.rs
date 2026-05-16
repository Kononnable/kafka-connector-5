#![allow(unused_imports, unused_variables)]
use bytes::{Buf, BufMut, Bytes, BytesMut};
use indexmap::IndexMap;

use crate::protocol::serialization::{KafkaCodec, decode_unsigned_varint, encode_unsigned_varint};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVer, SerializationError};

// -------------------------------------------------------
// DescribeClusterRequest
// -------------------------------------------------------
#[derive(Clone, Debug, PartialEq)]
pub struct DescribeClusterRequest {
    /// Whether to include cluster authorized operations.
    pub include_cluster_authorized_operations: bool,
    /// The endpoint type to describe. 1=brokers, 2=controllers.
    /// Available in version 1+.
    pub endpoint_type: i8,
    /// Whether to include fenced brokers when listing brokers.
    /// Available in version 2+.
    pub include_fenced_brokers: bool,
}
impl Default for DescribeClusterRequest {
    fn default() -> Self {
        Self {
            include_cluster_authorized_operations: false,
            endpoint_type: 1,
            include_fenced_brokers: false,
        }
    }
}

impl ApiRequest for DescribeClusterRequest {
    type Response = crate::generated::DescribeClusterResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(60)
    }
    fn get_min_supported_version() -> ApiVer {
        ApiVer::new(0)
    }
    fn get_max_supported_version() -> ApiVer {
        ApiVer::new(2)
    }
    fn get_min_flexible_version() -> ApiVer {
        ApiVer::new(0)
    }
    fn serialize(&self, version: ApiVer, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            0 <= version.0 && version.0 <= 2,
            "version {} is not supported by {} (supported: 0-2)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        self.include_cluster_authorized_operations
            .encode(buf, version, is_flexible)?;
        if 1 <= version.0 {
            self.endpoint_type.encode(buf, version, is_flexible)?;
        } else if self.endpoint_type != 1 {
            return Err(SerializationError::FieldNotAvailable {
                field: "EndpointType",
                version,
                api_name: "DescribeClusterRequest",
            });
        }
        if 2 <= version.0 {
            self.include_fenced_brokers
                .encode(buf, version, is_flexible)?;
        } else if self.include_fenced_brokers {
            return Err(SerializationError::FieldNotAvailable {
                field: "IncludeFencedBrokers",
                version,
                api_name: "DescribeClusterRequest",
            });
        }
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(version: ApiVer, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let include_cluster_authorized_operations = KafkaCodec::decode(buf, version, is_flexible)?;
        let endpoint_type = if 1 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            1
        };
        let include_fenced_brokers = if 2 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            false
        };
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            include_cluster_authorized_operations,
            endpoint_type,
            include_fenced_brokers,
        })
    }
}
impl KafkaCodec for DescribeClusterRequest {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.include_cluster_authorized_operations
            .encode(buf, version, is_flexible)?;
        if 1 <= version.0 {
            self.endpoint_type.encode(buf, version, is_flexible)?;
        }
        if 2 <= version.0 {
            self.include_fenced_brokers
                .encode(buf, version, is_flexible)?;
        }
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }

    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let include_cluster_authorized_operations = KafkaCodec::decode(buf, version, is_flexible)?;
        let endpoint_type = if 1 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            1
        };
        let include_fenced_brokers = if 2 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            false
        };
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            include_cluster_authorized_operations,
            endpoint_type,
            include_fenced_brokers,
        })
    }
}

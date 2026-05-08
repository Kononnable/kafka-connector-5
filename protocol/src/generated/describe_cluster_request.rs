#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{KafkaDeserialize, KafkaSerialize};
use crate::traits::{
    ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVersionTrait, SerializationError,
};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// DescribeClusterRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
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

impl ApiRequest for DescribeClusterRequest {
    type Response = crate::generated::DescribeClusterResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(60)
    }
    fn get_min_supported_version() -> ApiVersionTrait {
        ApiVersionTrait::new(0)
    }
    fn get_max_supported_version() -> ApiVersionTrait {
        ApiVersionTrait::new(2)
    }
    fn get_min_flexible_version() -> ApiVersionTrait {
        ApiVersionTrait::new(0)
    }
    fn serialize(
        &self,
        version: ApiVersionTrait,
        buf: &mut BytesMut,
    ) -> Result<(), SerializationError> {
        assert!(
            (0) <= version.0 && version.0 <= (2),
            "version {} is not supported by {} (supported: 0-2)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        self.include_cluster_authorized_operations
            .encode(buf, version, is_flexible)?;
        if (1) <= version.0 {
            self.endpoint_type.encode(buf, version, is_flexible)?;
        } else if self.endpoint_type != 0 {
            return Err(SerializationError::Encode(
                "field 'EndpointType' is not available in this version",
            ));
        }
        if (2) <= version.0 {
            self.include_fenced_brokers
                .encode(buf, version, is_flexible)?;
        } else if self.include_fenced_brokers {
            return Err(SerializationError::Encode(
                "field 'IncludeFencedBrokers' is not available in this version",
            ));
        }
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(version: ApiVersionTrait, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let include_cluster_authorized_operations =
            KafkaDeserialize::decode(buf, version, is_flexible)?;
        let endpoint_type = if (1) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let include_fenced_brokers = if (2) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            include_cluster_authorized_operations,
            endpoint_type,
            include_fenced_brokers,
        })
    }
}
impl KafkaSerialize for DescribeClusterRequest {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVersionTrait,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.include_cluster_authorized_operations
            .encode(buf, version, is_flexible)?;
        if (1) <= version.0 {
            self.endpoint_type.encode(buf, version, is_flexible)?;
        }
        if (2) <= version.0 {
            self.include_fenced_brokers
                .encode(buf, version, is_flexible)?;
        }
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for DescribeClusterRequest {
    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVersionTrait,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let include_cluster_authorized_operations =
            KafkaDeserialize::decode(buf, version, is_flexible)?;
        let endpoint_type = if (1) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let include_fenced_brokers = if (2) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            include_cluster_authorized_operations,
            endpoint_type,
            include_fenced_brokers,
        })
    }
}

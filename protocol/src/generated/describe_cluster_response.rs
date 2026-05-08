#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// DescribeClusterResponse
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct DescribeClusterResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// The top-level error code, or 0 if there was no error.
    pub error_code: i16,
    /// The top-level error message, or null if there was no error.
    pub error_message: Option<String>,
    /// The endpoint type that was described. 1=brokers, 2=controllers.
    /// Available in version 1+.
    pub endpoint_type: i8,
    /// The cluster ID that responding broker belongs to.
    pub cluster_id: String,
    /// The ID of the controller broker.
    pub controller_id: i32,
    /// Each broker in the response.
    pub brokers: Vec<DescribeClusterBroker>,
    /// 32-bit bitfield to represent authorized operations for this cluster.
    pub cluster_authorized_operations: i32,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct DescribeClusterBroker {
    /// The broker ID.
    pub broker_id: i32,
    /// The broker hostname.
    pub host: String,
    /// The broker port.
    pub port: i32,
    /// The rack of the broker, or null if it has not been assigned to a rack.
    pub rack: Option<String>,
    /// Whether the broker is fenced
    /// Available in version 2+.
    pub is_fenced: bool,
}

impl ApiResponse for DescribeClusterResponse {
    type Request = crate::generated::DescribeClusterRequest;
    fn get_api_key() -> ApiKey {
        ApiKey::new(60)
    }
    fn get_min_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(0)
    }
    fn get_max_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(2)
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
            (0) <= version.0 && version.0 <= (2),
            "version {} is not supported by {} (supported: 0-2)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        self.throttle_time_ms.encode(buf, version, is_flexible)?;
        self.error_code.encode(buf, version, is_flexible)?;
        self.error_message.encode(buf, version, is_flexible)?;
        if (1) <= version.0 {
            self.endpoint_type.encode(buf, version, is_flexible)?;
        } else if self.endpoint_type != 0 {
            return Err(SerializationError::Encode(
                "field 'EndpointType' is not available in this version",
            ));
        }
        self.cluster_id.encode(buf, version, is_flexible)?;
        self.controller_id.encode(buf, version, is_flexible)?;
        self.brokers.encode(buf, version, is_flexible)?;
        self.cluster_authorized_operations
            .encode(buf, version, is_flexible)?;
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(
        version: crate::traits::ApiVersion,
        buf: &mut Bytes,
    ) -> Result<Self, SerializationError> {
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let throttle_time_ms = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let error_code = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let error_message = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let endpoint_type = if (1) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let cluster_id = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let controller_id = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let brokers = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let cluster_authorized_operations = KafkaDeserialize::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            throttle_time_ms,
            error_code,
            error_message,
            endpoint_type,
            cluster_id,
            controller_id,
            brokers,
            cluster_authorized_operations,
        })
    }
}
impl KafkaSerialize for DescribeClusterResponse {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), crate::traits::SerializationError> {
        self.throttle_time_ms.encode(buf, version, is_flexible)?;
        self.error_code.encode(buf, version, is_flexible)?;
        self.error_message.encode(buf, version, is_flexible)?;
        if (1) <= version.0 {
            self.endpoint_type.encode(buf, version, is_flexible)?;
        }
        self.cluster_id.encode(buf, version, is_flexible)?;
        self.controller_id.encode(buf, version, is_flexible)?;
        self.brokers.encode(buf, version, is_flexible)?;
        self.cluster_authorized_operations
            .encode(buf, version, is_flexible)?;
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for DescribeClusterResponse {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, crate::traits::SerializationError> {
        let throttle_time_ms = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let error_code = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let error_message = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let endpoint_type = if (1) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let cluster_id = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let controller_id = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let brokers = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let cluster_authorized_operations = KafkaDeserialize::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            throttle_time_ms,
            error_code,
            error_message,
            endpoint_type,
            cluster_id,
            controller_id,
            brokers,
            cluster_authorized_operations,
        })
    }
}

impl KafkaSerialize for DescribeClusterBroker {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), crate::traits::SerializationError> {
        self.broker_id.encode(buf, version, is_flexible)?;
        self.host.encode(buf, version, is_flexible)?;
        self.port.encode(buf, version, is_flexible)?;
        self.rack.encode(buf, version, is_flexible)?;
        if (2) <= version.0 {
            self.is_fenced.encode(buf, version, is_flexible)?;
        }
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for DescribeClusterBroker {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, crate::traits::SerializationError> {
        let broker_id = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let host = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let port = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let rack = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let is_fenced = if (2) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            broker_id,
            host,
            port,
            rack,
            is_fenced,
        })
    }
}

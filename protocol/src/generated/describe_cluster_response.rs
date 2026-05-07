#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{EncodeError, DecodeError, KafkaSerialize, KafkaDeserialize};
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
    fn get_api_key() -> ApiKey { ApiKey::new(60) }
    fn get_min_supported_version() -> crate::traits::ApiVersion { crate::traits::ApiVersion::new(0) }
    fn get_max_supported_version() -> crate::traits::ApiVersion { crate::traits::ApiVersion::new(2) }
    fn serialize(&self, version: crate::traits::ApiVersion, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!((0) <= version.0 && version.0 <= (2), "version {} is not supported by {} (supported: 0-2)", version.0, stringify!(Self));
        let is_flexible = true;
        self.throttle_time_ms.encode_flexible(buf, is_flexible).map_err(|_| SerializationError::Encode("failed to encode ThrottleTimeMs"))?;
        self.error_code.encode_flexible(buf, is_flexible).map_err(|_| SerializationError::Encode("failed to encode ErrorCode"))?;
        self.error_message.encode_flexible(buf, is_flexible).map_err(|_| SerializationError::Encode("failed to encode ErrorMessage"))?;
        if (1) <= version.0 {
            self.endpoint_type.encode_flexible(buf, is_flexible).map_err(|_| SerializationError::Encode("failed to encode EndpointType"))?;
        }
        self.cluster_id.encode_flexible(buf, is_flexible).map_err(|_| SerializationError::Encode("failed to encode ClusterId"))?;
        self.controller_id.encode_flexible(buf, is_flexible).map_err(|_| SerializationError::Encode("failed to encode ControllerId"))?;
        self.brokers.encode_flexible(buf, is_flexible).map_err(|_| SerializationError::Encode("failed to encode Brokers"))?;
        self.cluster_authorized_operations.encode_flexible(buf, is_flexible).map_err(|_| SerializationError::Encode("failed to encode ClusterAuthorizedOperations"))?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(version: crate::traits::ApiVersion, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let is_flexible = true;
        let throttle_time_ms = <i32 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| SerializationError::Decode("failed to decode ThrottleTimeMs"))?;
        let error_code = <i16 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| SerializationError::Decode("failed to decode ErrorCode"))?;
        let error_message = <Option<String> as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| SerializationError::Decode("failed to decode ErrorMessage"))?;
        let endpoint_type = if (1) <= version.0 {
            <i8 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| SerializationError::Decode("failed to decode EndpointType"))?
        } else {
            Default::default()
        };
        let cluster_id = <String as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| SerializationError::Decode("failed to decode ClusterId"))?;
        let controller_id = <i32 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| SerializationError::Decode("failed to decode ControllerId"))?;
        let brokers = <Vec<DescribeClusterBroker> as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| SerializationError::Decode("failed to decode Brokers"))?;
        let cluster_authorized_operations = <i32 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| SerializationError::Decode("failed to decode ClusterAuthorizedOperations"))?;
        Ok(Self { throttle_time_ms, error_code, error_message, endpoint_type, cluster_id, controller_id, brokers, cluster_authorized_operations })
    }
}
impl KafkaSerialize for DescribeClusterResponse {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.throttle_time_ms.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode ThrottleTimeMs".into() })?;
        self.error_code.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode ErrorCode".into() })?;
        self.error_message.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode ErrorMessage".into() })?;
        self.endpoint_type.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode EndpointType".into() })?;
        self.cluster_id.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode ClusterId".into() })?;
        self.controller_id.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode ControllerId".into() })?;
        self.brokers.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Brokers".into() })?;
        self.cluster_authorized_operations.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode ClusterAuthorizedOperations".into() })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(&self, buf: &mut B, is_flexible: bool) -> Result<(), EncodeError> {
        self.throttle_time_ms.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode ThrottleTimeMs".into() })?;
        self.error_code.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode ErrorCode".into() })?;
        if is_flexible {
            if let Some(ref __val) = self.error_message {
                crate::protocol::serialization::encode_unsigned_varint(1u64, buf);
                __val.encode_flexible(buf, true).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode ErrorMessage".into() })?;
            } else {
                crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
            }
        } else {
            if let Some(ref __val) = self.error_message {
                __val.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode ErrorMessage".into() })?;
            }
        }
        self.endpoint_type.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode EndpointType".into() })?;
        self.cluster_id.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode ClusterId".into() })?;
        self.controller_id.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode ControllerId".into() })?;
        self.brokers.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Brokers".into() })?;
        self.cluster_authorized_operations.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode ClusterAuthorizedOperations".into() })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for DescribeClusterResponse {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let throttle_time_ms = <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode ThrottleTimeMs".into() })?;
        let error_code = <i16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode ErrorCode".into() })?;
        let error_message = <Option<String> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode ErrorMessage".into() })?;
        let endpoint_type = <i8 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode EndpointType".into() })?;
        let cluster_id = <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode ClusterId".into() })?;
        let controller_id = <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode ControllerId".into() })?;
        let brokers = <Vec<DescribeClusterBroker> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode Brokers".into() })?;
        let cluster_authorized_operations = <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode ClusterAuthorizedOperations".into() })?;
        Ok(Self { throttle_time_ms, error_code, error_message, endpoint_type, cluster_id, controller_id, brokers, cluster_authorized_operations })
    }
    fn decode_flexible<B: Buf>(buf: &mut B, is_flexible: bool) -> Result<Self, DecodeError> {
        let throttle_time_ms = <i32 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode ThrottleTimeMs".into() })?;
        let error_code = <i16 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode ErrorCode".into() })?;
        let error_message = if is_flexible {
            let (__present, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
            if __present == 0 {
                None
            } else {
                Some(<String as KafkaDeserialize>::decode_flexible(buf, true).map_err(|_| DecodeError::Protocol { message: "failed to decode ErrorMessage".into() })?)
            }
        } else {
            <Option<String> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode ErrorMessage".into() })?
        };
        let endpoint_type = <i8 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode EndpointType".into() })?;
        let cluster_id = <String as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode ClusterId".into() })?;
        let controller_id = <i32 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode ControllerId".into() })?;
        let brokers = <Vec<DescribeClusterBroker> as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode Brokers".into() })?;
        let cluster_authorized_operations = <i32 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode ClusterAuthorizedOperations".into() })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self { throttle_time_ms, error_code, error_message, endpoint_type, cluster_id, controller_id, brokers, cluster_authorized_operations })
    }
}

impl KafkaSerialize for DescribeClusterBroker {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.broker_id.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode BrokerId".into() })?;
        self.host.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Host".into() })?;
        self.port.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Port".into() })?;
        self.rack.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Rack".into() })?;
        self.is_fenced.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode IsFenced".into() })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(&self, buf: &mut B, is_flexible: bool) -> Result<(), EncodeError> {
        self.broker_id.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode BrokerId".into() })?;
        self.host.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Host".into() })?;
        self.port.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Port".into() })?;
        if is_flexible {
            if let Some(ref __val) = self.rack {
                crate::protocol::serialization::encode_unsigned_varint(1u64, buf);
                __val.encode_flexible(buf, true).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Rack".into() })?;
            } else {
                crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
            }
        } else {
            if let Some(ref __val) = self.rack {
                __val.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Rack".into() })?;
            }
        }
        self.is_fenced.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode IsFenced".into() })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for DescribeClusterBroker {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let broker_id = <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode BrokerId".into() })?;
        let host = <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode Host".into() })?;
        let port = <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode Port".into() })?;
        let rack = <Option<String> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode Rack".into() })?;
        let is_fenced = <bool as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode IsFenced".into() })?;
        Ok(Self { broker_id, host, port, rack, is_fenced })
    }
    fn decode_flexible<B: Buf>(buf: &mut B, is_flexible: bool) -> Result<Self, DecodeError> {
        let broker_id = <i32 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode BrokerId".into() })?;
        let host = <String as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode Host".into() })?;
        let port = <i32 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode Port".into() })?;
        let rack = if is_flexible {
            let (__present, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
            if __present == 0 {
                None
            } else {
                Some(<String as KafkaDeserialize>::decode_flexible(buf, true).map_err(|_| DecodeError::Protocol { message: "failed to decode Rack".into() })?)
            }
        } else {
            <Option<String> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode Rack".into() })?
        };
        let is_fenced = <bool as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode IsFenced".into() })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self { broker_id, host, port, rack, is_fenced })
    }
}


#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{
    KafkaDeserialize, KafkaSerialize, decode_unsigned_varint, encode_unsigned_varint,
};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVer, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// PushTelemetryRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct PushTelemetryRequest {
    /// Unique id for this client instance.
    pub client_instance_id: [u8; 16],
    /// Unique identifier for the current subscription.
    pub subscription_id: i32,
    /// Client is terminating the connection.
    pub terminating: bool,
    /// Compression codec used to compress the metrics.
    pub compression_type: i8,
    /// Metrics encoded in OpenTelemetry MetricsData v1 protobuf format.
    pub metrics: Vec<u8>,
}

impl ApiRequest for PushTelemetryRequest {
    type Response = crate::generated::PushTelemetryResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(72)
    }
    fn get_min_supported_version() -> ApiVer {
        ApiVer::new(0)
    }
    fn get_max_supported_version() -> ApiVer {
        ApiVer::new(0)
    }
    fn get_min_flexible_version() -> ApiVer {
        ApiVer::new(0)
    }
    fn serialize(&self, version: ApiVer, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            0 <= version.0 && version.0 <= 0,
            "version {} is not supported by {} (supported: 0-0)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        self.client_instance_id.encode(buf, version, is_flexible)?;
        self.subscription_id.encode(buf, version, is_flexible)?;
        self.terminating.encode(buf, version, is_flexible)?;
        self.compression_type.encode(buf, version, is_flexible)?;
        self.metrics.encode(buf, version, is_flexible)?;
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(version: ApiVer, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let client_instance_id = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let subscription_id = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let terminating = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let compression_type = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let metrics = KafkaDeserialize::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            client_instance_id,
            subscription_id,
            terminating,
            compression_type,
            metrics,
        })
    }
}
impl KafkaSerialize for PushTelemetryRequest {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.client_instance_id.encode(buf, version, is_flexible)?;
        self.subscription_id.encode(buf, version, is_flexible)?;
        self.terminating.encode(buf, version, is_flexible)?;
        self.compression_type.encode(buf, version, is_flexible)?;
        self.metrics.encode(buf, version, is_flexible)?;
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for PushTelemetryRequest {
    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let client_instance_id = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let subscription_id = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let terminating = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let compression_type = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let metrics = KafkaDeserialize::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            client_instance_id,
            subscription_id,
            terminating,
            compression_type,
            metrics,
        })
    }
}

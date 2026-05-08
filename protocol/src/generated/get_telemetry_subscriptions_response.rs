#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// GetTelemetrySubscriptionsResponse
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct GetTelemetrySubscriptionsResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// The error code, or 0 if there was no error.
    pub error_code: i16,
    /// Assigned client instance id if ClientInstanceId was 0 in the request, else 0.
    pub client_instance_id: [u8; 16],
    /// Unique identifier for the current subscription set for this client instance.
    pub subscription_id: i32,
    /// Compression types that broker accepts for the PushTelemetryRequest.
    pub accepted_compression_types: Vec<i8>,
    /// Configured push interval, which is the lowest configured interval in the current subscription set.
    pub push_interval_ms: i32,
    /// The maximum bytes of binary data the broker accepts in PushTelemetryRequest.
    pub telemetry_max_bytes: i32,
    /// Flag to indicate monotonic/counter metrics are to be emitted as deltas or cumulative values.
    pub delta_temporality: bool,
    /// Requested metrics prefix string match. Empty array: No metrics subscribed, Array[0] empty string: All metrics subscribed.
    pub requested_metrics: Vec<String>,
}

impl ApiResponse for GetTelemetrySubscriptionsResponse {
    type Request = crate::generated::GetTelemetrySubscriptionsRequest;
    fn get_api_key() -> ApiKey {
        ApiKey::new(71)
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
        self.throttle_time_ms.encode(buf, version, is_flexible)?;
        self.error_code.encode(buf, version, is_flexible)?;
        self.client_instance_id.encode(buf, version, is_flexible)?;
        self.subscription_id.encode(buf, version, is_flexible)?;
        self.accepted_compression_types
            .encode(buf, version, is_flexible)?;
        self.push_interval_ms.encode(buf, version, is_flexible)?;
        self.telemetry_max_bytes.encode(buf, version, is_flexible)?;
        self.delta_temporality.encode(buf, version, is_flexible)?;
        self.requested_metrics.encode(buf, version, is_flexible)?;
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
        let throttle_time_ms = <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let error_code = <i16 as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let client_instance_id = <[u8; 16] as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let subscription_id = <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let accepted_compression_types =
            <Vec<i8> as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let push_interval_ms = <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let telemetry_max_bytes = <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let delta_temporality = <bool as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let requested_metrics =
            <Vec<String> as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        Ok(Self {
            throttle_time_ms,
            error_code,
            client_instance_id,
            subscription_id,
            accepted_compression_types,
            push_interval_ms,
            telemetry_max_bytes,
            delta_temporality,
            requested_metrics,
        })
    }
}
impl KafkaSerialize for GetTelemetrySubscriptionsResponse {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), crate::traits::SerializationError> {
        self.throttle_time_ms.encode(buf, version, is_flexible)?;
        self.error_code.encode(buf, version, is_flexible)?;
        self.client_instance_id.encode(buf, version, is_flexible)?;
        self.subscription_id.encode(buf, version, is_flexible)?;
        self.accepted_compression_types
            .encode(buf, version, is_flexible)?;
        self.push_interval_ms.encode(buf, version, is_flexible)?;
        self.telemetry_max_bytes.encode(buf, version, is_flexible)?;
        self.delta_temporality.encode(buf, version, is_flexible)?;
        self.requested_metrics.encode(buf, version, is_flexible)?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for GetTelemetrySubscriptionsResponse {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, crate::traits::SerializationError> {
        let throttle_time_ms = <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let error_code = <i16 as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let client_instance_id = <[u8; 16] as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let subscription_id = <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let accepted_compression_types =
            <Vec<i8> as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let push_interval_ms = <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let telemetry_max_bytes = <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let delta_temporality = <bool as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let requested_metrics =
            <Vec<String> as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            throttle_time_ms,
            error_code,
            client_instance_id,
            subscription_id,
            accepted_compression_types,
            push_interval_ms,
            telemetry_max_bytes,
            delta_temporality,
            requested_metrics,
        })
    }
}

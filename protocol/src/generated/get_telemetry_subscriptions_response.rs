#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
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
        self.throttle_time_ms
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode ThrottleTimeMs"))?;
        self.error_code
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode ErrorCode"))?;
        self.client_instance_id
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode ClientInstanceId"))?;
        self.subscription_id
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode SubscriptionId"))?;
        self.accepted_compression_types
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode AcceptedCompressionTypes"))?;
        self.push_interval_ms
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode PushIntervalMs"))?;
        self.telemetry_max_bytes
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode TelemetryMaxBytes"))?;
        self.delta_temporality
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode DeltaTemporality"))?;
        self.requested_metrics
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode RequestedMetrics"))?;
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
        let throttle_time_ms = <i32 as KafkaDeserialize>::decode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode ThrottleTimeMs"))?;
        let error_code = <i16 as KafkaDeserialize>::decode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode ErrorCode"))?;
        let client_instance_id = <[u8; 16] as KafkaDeserialize>::decode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode ClientInstanceId"))?;
        let subscription_id = <i32 as KafkaDeserialize>::decode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode SubscriptionId"))?;
        let accepted_compression_types =
            <Vec<i8> as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| {
                SerializationError::Decode("failed to decode AcceptedCompressionTypes")
            })?;
        let push_interval_ms = <i32 as KafkaDeserialize>::decode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode PushIntervalMs"))?;
        let telemetry_max_bytes = <i32 as KafkaDeserialize>::decode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode TelemetryMaxBytes"))?;
        let delta_temporality = <bool as KafkaDeserialize>::decode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode DeltaTemporality"))?;
        let requested_metrics =
            <Vec<String> as KafkaDeserialize>::decode_flexible(buf, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode RequestedMetrics"))?;
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
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.throttle_time_ms
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ThrottleTimeMs".into(),
            })?;
        self.error_code
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ErrorCode".into(),
            })?;
        self.client_instance_id
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ClientInstanceId".into(),
            })?;
        self.subscription_id
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode SubscriptionId".into(),
            })?;
        self.accepted_compression_types
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode AcceptedCompressionTypes".into(),
            })?;
        self.push_interval_ms
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode PushIntervalMs".into(),
            })?;
        self.telemetry_max_bytes
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode TelemetryMaxBytes".into(),
            })?;
        self.delta_temporality
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode DeltaTemporality".into(),
            })?;
        self.requested_metrics
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode RequestedMetrics".into(),
            })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(
        &self,
        buf: &mut B,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.throttle_time_ms
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ThrottleTimeMs".into(),
            })?;
        self.error_code
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ErrorCode".into(),
            })?;
        self.client_instance_id
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ClientInstanceId".into(),
            })?;
        self.subscription_id
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode SubscriptionId".into(),
            })?;
        self.accepted_compression_types
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode AcceptedCompressionTypes".into(),
            })?;
        self.push_interval_ms
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode PushIntervalMs".into(),
            })?;
        self.telemetry_max_bytes
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode TelemetryMaxBytes".into(),
            })?;
        self.delta_temporality
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode DeltaTemporality".into(),
            })?;
        self.requested_metrics
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode RequestedMetrics".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for GetTelemetrySubscriptionsResponse {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] classic decode field `ThrottleTimeMs` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let throttle_time_ms =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ThrottleTimeMs".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `ErrorCode` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let error_code =
            <i16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ErrorCode".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `ClientInstanceId` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let client_instance_id =
            <[u8; 16] as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ClientInstanceId".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `SubscriptionId` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let subscription_id =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode SubscriptionId".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `AcceptedCompressionTypes` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let accepted_compression_types =
            <Vec<i8> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode AcceptedCompressionTypes".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `PushIntervalMs` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let push_interval_ms =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode PushIntervalMs".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `TelemetryMaxBytes` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let telemetry_max_bytes =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode TelemetryMaxBytes".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `DeltaTemporality` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let delta_temporality =
            <bool as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode DeltaTemporality".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `RequestedMetrics` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let requested_metrics =
            <Vec<String> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode RequestedMetrics".into(),
            })?;
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
    fn decode_flexible<B: Buf>(buf: &mut B, is_flexible: bool) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] decoding field `ThrottleTimeMs` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let throttle_time_ms = <i32 as KafkaDeserialize>::decode_flexible(buf, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode ThrottleTimeMs".into(),
            })?;
        tracing::trace!(
            "  [{}] decoding field `ErrorCode` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let error_code =
            <i16 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode ErrorCode".into(),
                }
            })?;
        tracing::trace!(
            "  [{}] decoding field `ClientInstanceId` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let client_instance_id = <[u8; 16] as KafkaDeserialize>::decode_flexible(buf, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode ClientInstanceId".into(),
            })?;
        tracing::trace!(
            "  [{}] decoding field `SubscriptionId` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let subscription_id = <i32 as KafkaDeserialize>::decode_flexible(buf, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode SubscriptionId".into(),
            })?;
        tracing::trace!(
            "  [{}] decoding field `AcceptedCompressionTypes` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let accepted_compression_types =
            <Vec<i8> as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode AcceptedCompressionTypes".into(),
                }
            })?;
        tracing::trace!(
            "  [{}] decoding field `PushIntervalMs` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let push_interval_ms = <i32 as KafkaDeserialize>::decode_flexible(buf, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode PushIntervalMs".into(),
            })?;
        tracing::trace!(
            "  [{}] decoding field `TelemetryMaxBytes` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let telemetry_max_bytes = <i32 as KafkaDeserialize>::decode_flexible(buf, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode TelemetryMaxBytes".into(),
            })?;
        tracing::trace!(
            "  [{}] decoding field `DeltaTemporality` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let delta_temporality = <bool as KafkaDeserialize>::decode_flexible(buf, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode DeltaTemporality".into(),
            })?;
        tracing::trace!(
            "  [{}] decoding field `RequestedMetrics` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let requested_metrics =
            <Vec<String> as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode RequestedMetrics".into(),
                }
            })?;
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

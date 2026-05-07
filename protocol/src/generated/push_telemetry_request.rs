#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
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
        self.client_instance_id
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode ClientInstanceId"))?;
        self.subscription_id
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode SubscriptionId"))?;
        self.terminating
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode Terminating"))?;
        self.compression_type
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode CompressionType"))?;
        self.metrics
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode Metrics"))?;
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
        let client_instance_id = <[u8; 16] as KafkaDeserialize>::decode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode ClientInstanceId"))?;
        let subscription_id = <i32 as KafkaDeserialize>::decode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode SubscriptionId"))?;
        let terminating = <bool as KafkaDeserialize>::decode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode Terminating"))?;
        let compression_type = <i8 as KafkaDeserialize>::decode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode CompressionType"))?;
        let metrics = <Vec<u8> as KafkaDeserialize>::decode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode Metrics"))?;
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
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
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
        self.terminating
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Terminating".into(),
            })?;
        self.compression_type
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode CompressionType".into(),
            })?;
        self.metrics
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Metrics".into(),
            })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(
        &self,
        buf: &mut B,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
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
        self.terminating
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Terminating".into(),
            })?;
        self.compression_type
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode CompressionType".into(),
            })?;
        self.metrics
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Metrics".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for PushTelemetryRequest {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
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
            "  [{}] classic decode field `Terminating` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let terminating =
            <bool as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode Terminating".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `CompressionType` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let compression_type =
            <i8 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode CompressionType".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `Metrics` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let metrics =
            <Vec<u8> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode Metrics".into(),
            })?;
        Ok(Self {
            client_instance_id,
            subscription_id,
            terminating,
            compression_type,
            metrics,
        })
    }
    fn decode_flexible<B: Buf>(buf: &mut B, is_flexible: bool) -> Result<Self, DecodeError> {
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
            "  [{}] decoding field `Terminating` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let terminating =
            <bool as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode Terminating".into(),
                }
            })?;
        tracing::trace!(
            "  [{}] decoding field `CompressionType` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let compression_type = <i8 as KafkaDeserialize>::decode_flexible(buf, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode CompressionType".into(),
            })?;
        tracing::trace!(
            "  [{}] decoding field `Metrics` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let metrics =
            <Vec<u8> as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode Metrics".into(),
                }
            })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
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

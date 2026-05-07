#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// BrokerHeartbeatResponse
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct BrokerHeartbeatResponse {
    /// Duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// The error code, or 0 if there was no error.
    pub error_code: i16,
    /// True if the broker has approximately caught up with the latest metadata.
    pub is_caught_up: bool,
    /// True if the broker is fenced.
    pub is_fenced: bool,
    /// True if the broker should proceed with its shutdown.
    pub should_shut_down: bool,
}

impl ApiResponse for BrokerHeartbeatResponse {
    type Request = crate::generated::BrokerHeartbeatRequest;
    fn get_api_key() -> ApiKey {
        ApiKey::new(63)
    }
    fn get_min_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(0)
    }
    fn get_max_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(1)
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
        let is_flexible = true;
        self.throttle_time_ms
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode ThrottleTimeMs"))?;
        self.error_code
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode ErrorCode"))?;
        self.is_caught_up
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode IsCaughtUp"))?;
        self.is_fenced
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode IsFenced"))?;
        self.should_shut_down
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode ShouldShutDown"))?;
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
        let throttle_time_ms =
            <i32 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode ThrottleTimeMs"))?;
        let error_code = <i16 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode ErrorCode"))?;
        let is_caught_up =
            <bool as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode IsCaughtUp"))?;
        let is_fenced = <bool as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode IsFenced"))?;
        let should_shut_down =
            <bool as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode ShouldShutDown"))?;
        Ok(Self {
            throttle_time_ms,
            error_code,
            is_caught_up,
            is_fenced,
            should_shut_down,
        })
    }
}
impl KafkaSerialize for BrokerHeartbeatResponse {
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
        self.is_caught_up
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode IsCaughtUp".into(),
            })?;
        self.is_fenced
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode IsFenced".into(),
            })?;
        self.should_shut_down
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ShouldShutDown".into(),
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
        self.is_caught_up
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode IsCaughtUp".into(),
            })?;
        self.is_fenced
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode IsFenced".into(),
            })?;
        self.should_shut_down
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ShouldShutDown".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for BrokerHeartbeatResponse {
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
            "  [{}] classic decode field `IsCaughtUp` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let is_caught_up =
            <bool as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode IsCaughtUp".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `IsFenced` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let is_fenced =
            <bool as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode IsFenced".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `ShouldShutDown` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let should_shut_down =
            <bool as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ShouldShutDown".into(),
            })?;
        Ok(Self {
            throttle_time_ms,
            error_code,
            is_caught_up,
            is_fenced,
            should_shut_down,
        })
    }
    fn decode_flexible<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] decoding field `ThrottleTimeMs` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let throttle_time_ms =
            <i32 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode ThrottleTimeMs".into(),
                },
            )?;
        tracing::trace!(
            "  [{}] decoding field `ErrorCode` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let error_code = <i16 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode ErrorCode".into(),
            })?;
        tracing::trace!(
            "  [{}] decoding field `IsCaughtUp` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let is_caught_up = <bool as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
            message: "failed to decode IsCaughtUp".into(),
        })?;
        tracing::trace!(
            "  [{}] decoding field `IsFenced` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let is_fenced = <bool as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode IsFenced".into(),
            })?;
        tracing::trace!(
            "  [{}] decoding field `ShouldShutDown` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let should_shut_down =
            <bool as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode ShouldShutDown".into(),
                },
            )?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            throttle_time_ms,
            error_code,
            is_caught_up,
            is_fenced,
            should_shut_down,
        })
    }
}

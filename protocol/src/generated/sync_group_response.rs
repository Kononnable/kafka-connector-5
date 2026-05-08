#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// SyncGroupResponse
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SyncGroupResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    /// Available in version 1+.
    pub throttle_time_ms: i32,
    /// The error code, or 0 if there was no error.
    pub error_code: i16,
    /// The group protocol type.
    /// Available in version 5+.
    pub protocol_type: Option<String>,
    /// The group protocol name.
    /// Available in version 5+.
    pub protocol_name: Option<String>,
    /// The member assignment.
    pub assignment: Vec<u8>,
}

impl ApiResponse for SyncGroupResponse {
    type Request = crate::generated::SyncGroupRequest;
    fn get_api_key() -> ApiKey {
        ApiKey::new(14)
    }
    fn get_min_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(0)
    }
    fn get_max_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(5)
    }
    fn get_min_flexible_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(4)
    }
    fn serialize(
        &self,
        version: crate::traits::ApiVersion,
        buf: &mut BytesMut,
    ) -> Result<(), SerializationError> {
        assert!(
            (0) <= version.0 && version.0 <= (5),
            "version {} is not supported by {} (supported: 0-5)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        if (1) <= version.0 {
            self.throttle_time_ms
                .encode(buf, version, is_flexible)
                .map_err(|_| SerializationError::Encode("failed to encode ThrottleTimeMs"))?;
        } else if self.throttle_time_ms != 0 {
            return Err(SerializationError::Encode(
                "field 'ThrottleTimeMs' is not available in this version",
            ));
        }
        self.error_code
            .encode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode ErrorCode"))?;
        if (5) <= version.0 {
            self.protocol_type
                .encode(buf, version, is_flexible)
                .map_err(|_| SerializationError::Encode("failed to encode ProtocolType"))?;
        } else if self.protocol_type.is_some() {
            return Err(SerializationError::Encode(
                "field 'ProtocolType' is not available in this version",
            ));
        }
        if (5) <= version.0 {
            self.protocol_name
                .encode(buf, version, is_flexible)
                .map_err(|_| SerializationError::Encode("failed to encode ProtocolName"))?;
        } else if self.protocol_name.is_some() {
            return Err(SerializationError::Encode(
                "field 'ProtocolName' is not available in this version",
            ));
        }
        self.assignment
            .encode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode Assignment"))?;
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
        let throttle_time_ms = if (1) <= version.0 {
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode ThrottleTimeMs"))?
        } else {
            Default::default()
        };
        let error_code = <i16 as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode ErrorCode"))?;
        let protocol_type = if (5) <= version.0 {
            <Option<String> as KafkaDeserialize>::decode(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode ProtocolType"))?
        } else {
            Default::default()
        };
        let protocol_name = if (5) <= version.0 {
            <Option<String> as KafkaDeserialize>::decode(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode ProtocolName"))?
        } else {
            Default::default()
        };
        let assignment = <Vec<u8> as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode Assignment"))?;
        Ok(Self {
            throttle_time_ms,
            error_code,
            protocol_type,
            protocol_name,
            assignment,
        })
    }
}
impl KafkaSerialize for SyncGroupResponse {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        if (1) <= version.0 {
            self.throttle_time_ms
                .encode(buf, version, is_flexible)
                .map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode ThrottleTimeMs".into(),
                })?;
        }
        self.error_code
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ErrorCode".into(),
            })?;
        if (5) <= version.0 {
            self.protocol_type
                .encode(buf, version, is_flexible)
                .map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode ProtocolType".into(),
                })?;
        }
        if (5) <= version.0 {
            self.protocol_name
                .encode(buf, version, is_flexible)
                .map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode ProtocolName".into(),
                })?;
        }
        self.assignment
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Assignment".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for SyncGroupResponse {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        let throttle_time_ms = if (1) <= version.0 {
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode ThrottleTimeMs".into(),
                }
            })?
        } else {
            Default::default()
        };
        let error_code =
            <i16 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode ErrorCode".into(),
                }
            })?;
        let protocol_type = if (5) <= version.0 {
            <Option<String> as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode ProtocolType".into(),
                },
            )?
        } else {
            Default::default()
        };
        let protocol_name = if (5) <= version.0 {
            <Option<String> as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode ProtocolName".into(),
                },
            )?
        } else {
            Default::default()
        };
        let assignment =
            <Vec<u8> as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode Assignment".into(),
                }
            })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            throttle_time_ms,
            error_code,
            protocol_type,
            protocol_name,
            assignment,
        })
    }
}

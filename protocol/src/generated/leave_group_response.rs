#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// LeaveGroupResponse
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct LeaveGroupResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    /// Available in version 1+.
    pub throttle_time_ms: i32,
    /// The error code, or 0 if there was no error.
    pub error_code: i16,
    /// List of leaving member responses.
    /// Available in version 3+.
    pub members: Vec<MemberResponse>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct MemberResponse {
    /// The member ID to remove from the group.
    /// Available in version 3+.
    pub member_id: String,
    /// The group instance ID to remove from the group.
    /// Available in version 3+.
    pub group_instance_id: Option<String>,
    /// The error code, or 0 if there was no error.
    /// Available in version 3+.
    pub error_code: i16,
}

impl ApiResponse for LeaveGroupResponse {
    type Request = crate::generated::LeaveGroupRequest;
    fn get_api_key() -> ApiKey {
        ApiKey::new(13)
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
        if (3) <= version.0 {
            self.members
                .encode(buf, version, is_flexible)
                .map_err(|_| SerializationError::Encode("failed to encode Members"))?;
        } else if !self.members.is_empty() {
            return Err(SerializationError::Encode(
                "field 'Members' is not available in this version",
            ));
        }
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
        let members = if (3) <= version.0 {
            <Vec<MemberResponse> as KafkaDeserialize>::decode(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode Members"))?
        } else {
            Default::default()
        };
        Ok(Self {
            throttle_time_ms,
            error_code,
            members,
        })
    }
}
impl KafkaSerialize for LeaveGroupResponse {
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
        if (3) <= version.0 {
            self.members
                .encode(buf, version, is_flexible)
                .map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode Members".into(),
                })?;
        }
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for LeaveGroupResponse {
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
        let members = if (3) <= version.0 {
            <Vec<MemberResponse> as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode Members".into(),
                },
            )?
        } else {
            Default::default()
        };
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            throttle_time_ms,
            error_code,
            members,
        })
    }
}

impl KafkaSerialize for MemberResponse {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        if (3) <= version.0 {
            self.member_id
                .encode(buf, version, is_flexible)
                .map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode MemberId".into(),
                })?;
        }
        if (3) <= version.0 {
            self.group_instance_id
                .encode(buf, version, is_flexible)
                .map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode GroupInstanceId".into(),
                })?;
        }
        if (3) <= version.0 {
            self.error_code
                .encode(buf, version, is_flexible)
                .map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode ErrorCode".into(),
                })?;
        }
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for MemberResponse {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        let member_id = if (3) <= version.0 {
            <String as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode MemberId".into(),
                }
            })?
        } else {
            Default::default()
        };
        let group_instance_id = if (3) <= version.0 {
            <Option<String> as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode GroupInstanceId".into(),
                },
            )?
        } else {
            Default::default()
        };
        let error_code = if (3) <= version.0 {
            <i16 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode ErrorCode".into(),
                }
            })?
        } else {
            Default::default()
        };
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            member_id,
            group_instance_id,
            error_code,
        })
    }
}

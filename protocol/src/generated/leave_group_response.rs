#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion, SerializationError};
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
    fn get_min_supported_version() -> ApiVersion {
        ApiVersion::new(0)
    }
    fn get_max_supported_version() -> ApiVersion {
        ApiVersion::new(4)
    }
    fn serialize(&self, version: ApiVersion, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            (0) <= version.0 && version.0 <= (4),
            "version {} is not supported by {} (supported: 0-4)",
            version.0,
            stringify!(Self)
        );
        if (1) <= version.0 {
            self.throttle_time_ms
                .encode(buf)
                .map_err(|_| SerializationError::Encode("failed to encode ThrottleTimeMs"))?;
        }
        self.error_code
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode ErrorCode"))?;
        if (3) <= version.0 {
            self.members
                .encode(buf)
                .map_err(|_| SerializationError::Encode("failed to encode Members"))?;
        }
        Ok(())
    }
    fn deserialize(version: ApiVersion, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let throttle_time_ms = if (1) <= version.0 {
            <i32 as KafkaDeserialize>::decode(buf)
                .map_err(|_| SerializationError::Decode("failed to decode ThrottleTimeMs"))?
        } else {
            Default::default()
        };
        let error_code = <i16 as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode ErrorCode"))?;
        let members = if (3) <= version.0 {
            <Vec<MemberResponse> as KafkaDeserialize>::decode(buf)
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
        self.members
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Members".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for LeaveGroupResponse {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let throttle_time_ms =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ThrottleTimeMs".into(),
            })?;
        let error_code =
            <i16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ErrorCode".into(),
            })?;
        let members = <Vec<MemberResponse> as KafkaDeserialize>::decode(buf).map_err(|_| {
            DecodeError::Protocol {
                message: "failed to decode Members".into(),
            }
        })?;
        Ok(Self {
            throttle_time_ms,
            error_code,
            members,
        })
    }
}

impl KafkaSerialize for MemberResponse {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.member_id
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode MemberId".into(),
            })?;
        self.group_instance_id
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode GroupInstanceId".into(),
            })?;
        self.error_code
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ErrorCode".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for MemberResponse {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let member_id =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode MemberId".into(),
            })?;
        let group_instance_id =
            <Option<String> as KafkaDeserialize>::decode(buf).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode GroupInstanceId".into(),
                }
            })?;
        let error_code =
            <i16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ErrorCode".into(),
            })?;
        Ok(Self {
            member_id,
            group_instance_id,
            error_code,
        })
    }
}

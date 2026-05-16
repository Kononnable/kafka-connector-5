#![allow(unused_imports, unused_variables)]
use bytes::{Buf, BufMut, Bytes, BytesMut};
use indexmap::IndexMap;

use crate::protocol::serialization::{KafkaCodec, decode_unsigned_varint, encode_unsigned_varint};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVer, SerializationError};

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
    fn get_min_supported_version() -> ApiVer {
        ApiVer::new(0)
    }
    fn get_max_supported_version() -> ApiVer {
        ApiVer::new(5)
    }
    fn get_min_flexible_version() -> ApiVer {
        ApiVer::new(4)
    }
    fn serialize(&self, version: ApiVer, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            0 <= version.0 && version.0 <= 5,
            "version {} is not supported by {} (supported: 0-5)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        if 1 <= version.0 {
            self.throttle_time_ms.encode(buf, version, is_flexible)?;
        } else if self.throttle_time_ms != 0 {
            return Err(SerializationError::FieldNotAvailable {
                field: "ThrottleTimeMs",
                version,
                api_name: "LeaveGroupResponse",
            });
        }
        self.error_code.encode(buf, version, is_flexible)?;
        if 3 <= version.0 {
            self.members.encode(buf, version, is_flexible)?;
        } else if !self.members.is_empty() {
            return Err(SerializationError::FieldNotAvailable {
                field: "Members",
                version,
                api_name: "LeaveGroupResponse",
            });
        }
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(version: ApiVer, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let throttle_time_ms = if 1 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            0
        };
        let error_code = KafkaCodec::decode(buf, version, is_flexible)?;
        let members = if 3 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Vec::new()
        };
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            throttle_time_ms,
            error_code,
            members,
        })
    }
}
impl KafkaCodec for LeaveGroupResponse {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        if 1 <= version.0 {
            self.throttle_time_ms.encode(buf, version, is_flexible)?;
        }
        self.error_code.encode(buf, version, is_flexible)?;
        if 3 <= version.0 {
            self.members.encode(buf, version, is_flexible)?;
        }
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }

    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let throttle_time_ms = if 1 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            0
        };
        let error_code = KafkaCodec::decode(buf, version, is_flexible)?;
        let members = if 3 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Vec::new()
        };
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            throttle_time_ms,
            error_code,
            members,
        })
    }
}

impl KafkaCodec for MemberResponse {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        if 3 <= version.0 {
            self.member_id.encode(buf, version, is_flexible)?;
        }
        if 3 <= version.0 {
            self.group_instance_id.encode(buf, version, is_flexible)?;
        }
        if 3 <= version.0 {
            self.error_code.encode(buf, version, is_flexible)?;
        }
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }

    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let member_id = if 3 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            String::new()
        };
        let group_instance_id = if 3 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            None
        };
        let error_code = if 3 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            0
        };
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            member_id,
            group_instance_id,
            error_code,
        })
    }
}

#![allow(unused_imports, unused_variables)]
use bytes::{Buf, BufMut, Bytes, BytesMut};
use indexmap::IndexMap;

use crate::protocol::serialization::{KafkaCodec, decode_unsigned_varint, encode_unsigned_varint};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVer, SerializationError};

// -------------------------------------------------------
// LeaveGroupRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct LeaveGroupRequest {
    /// The ID of the group to leave.
    pub group_id: String,
    /// The member ID to remove from the group.
    /// Available in version 0-2.
    pub member_id: String,
    /// List of leaving member identities.
    /// Available in version 3+.
    pub members: Vec<MemberIdentity>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct MemberIdentity {
    /// The member ID to remove from the group.
    /// Available in version 3+.
    pub member_id: String,
    /// The group instance ID to remove from the group.
    /// Available in version 3+.
    pub group_instance_id: Option<String>,
    /// The reason why the member left the group.
    /// Available in version 5+.
    pub reason: Option<String>,
}

impl ApiRequest for LeaveGroupRequest {
    type Response = crate::generated::LeaveGroupResponse;
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
        self.group_id.encode(buf, version, is_flexible)?;
        if 0 <= version.0 && version.0 <= 2 {
            self.member_id.encode(buf, version, is_flexible)?;
        } else if !self.member_id.is_empty() {
            return Err(SerializationError::FieldNotAvailable {
                field: "MemberId",
                version,
                api_name: "LeaveGroupRequest",
            });
        }
        if 3 <= version.0 {
            self.members.encode(buf, version, is_flexible)?;
        } else if !self.members.is_empty() {
            return Err(SerializationError::FieldNotAvailable {
                field: "Members",
                version,
                api_name: "LeaveGroupRequest",
            });
        }
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(version: ApiVer, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let group_id = KafkaCodec::decode(buf, version, is_flexible)?;
        let member_id = if 0 <= version.0 && version.0 <= 2 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            String::new()
        };
        let members = if 3 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Vec::new()
        };
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            group_id,
            member_id,
            members,
        })
    }
}
impl KafkaCodec for LeaveGroupRequest {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.group_id.encode(buf, version, is_flexible)?;
        if 0 <= version.0 && version.0 <= 2 {
            self.member_id.encode(buf, version, is_flexible)?;
        }
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
        let group_id = KafkaCodec::decode(buf, version, is_flexible)?;
        let member_id = if 0 <= version.0 && version.0 <= 2 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            String::new()
        };
        let members = if 3 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Vec::new()
        };
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            group_id,
            member_id,
            members,
        })
    }
}

impl KafkaCodec for MemberIdentity {
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
        if 5 <= version.0 {
            self.reason.encode(buf, version, is_flexible)?;
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
        let reason = if 5 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            None
        };
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            member_id,
            group_instance_id,
            reason,
        })
    }
}

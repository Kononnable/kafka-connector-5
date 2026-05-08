#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

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
        self.group_id
            .encode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode GroupId"))?;
        if (0) <= version.0 && version.0 <= (2) {
            self.member_id
                .encode(buf, version, is_flexible)
                .map_err(|_| SerializationError::Encode("failed to encode MemberId"))?;
        } else if !self.member_id.is_empty() {
            return Err(SerializationError::Encode(
                "field 'MemberId' is not available in this version",
            ));
        }
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
        let group_id = <String as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode GroupId"))?;
        let member_id = if (0) <= version.0 && version.0 <= (2) {
            <String as KafkaDeserialize>::decode(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode MemberId"))?
        } else {
            Default::default()
        };
        let members = if (3) <= version.0 {
            <Vec<MemberIdentity> as KafkaDeserialize>::decode(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode Members"))?
        } else {
            Default::default()
        };
        Ok(Self {
            group_id,
            member_id,
            members,
        })
    }
}
impl KafkaSerialize for LeaveGroupRequest {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.group_id
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode GroupId".into(),
            })?;
        if (0) <= version.0 && version.0 <= (2) {
            self.member_id
                .encode(buf, version, is_flexible)
                .map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode MemberId".into(),
                })?;
        }
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

impl KafkaDeserialize for LeaveGroupRequest {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        let group_id =
            <String as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode GroupId".into(),
                }
            })?;
        let member_id = if (0) <= version.0 && version.0 <= (2) {
            <String as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode MemberId".into(),
                }
            })?
        } else {
            Default::default()
        };
        let members = if (3) <= version.0 {
            <Vec<MemberIdentity> as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(
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
            group_id,
            member_id,
            members,
        })
    }
}

impl KafkaSerialize for MemberIdentity {
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
        if (5) <= version.0 {
            self.reason.encode(buf, version, is_flexible).map_err(|_| {
                EncodeError::ValueTooLarge {
                    message: "failed to encode Reason".into(),
                }
            })?;
        }
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for MemberIdentity {
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
        let reason = if (5) <= version.0 {
            <Option<String> as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode Reason".into(),
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
            member_id,
            group_instance_id,
            reason,
        })
    }
}

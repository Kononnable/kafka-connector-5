#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// SyncGroupRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SyncGroupRequest {
    /// The unique group identifier.
    pub group_id: String,
    /// The generation of the group.
    pub generation_id: i32,
    /// The member ID assigned by the group.
    pub member_id: String,
    /// The unique identifier of the consumer instance provided by end user.
    /// Available in version 3+.
    pub group_instance_id: Option<String>,
    /// The group protocol type.
    /// Available in version 5+.
    pub protocol_type: Option<String>,
    /// The group protocol name.
    /// Available in version 5+.
    pub protocol_name: Option<String>,
    /// Each assignment.
    pub assignments: Vec<SyncGroupRequestAssignment>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct SyncGroupRequestAssignment {
    /// The ID of the member to assign.
    pub member_id: String,
    /// The member assignment.
    pub assignment: Vec<u8>,
}

impl ApiRequest for SyncGroupRequest {
    type Response = crate::generated::SyncGroupResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(14)
    }
    fn get_min_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(0)
    }
    fn get_max_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(5)
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
        let is_flexible = (4) <= version.0;
        self.group_id
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode GroupId"))?;
        self.generation_id
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode GenerationId"))?;
        self.member_id
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode MemberId"))?;
        if (3) <= version.0 {
            self.group_instance_id
                .encode_flexible(buf, is_flexible)
                .map_err(|_| SerializationError::Encode("failed to encode GroupInstanceId"))?;
        }
        if (5) <= version.0 {
            self.protocol_type
                .encode_flexible(buf, is_flexible)
                .map_err(|_| SerializationError::Encode("failed to encode ProtocolType"))?;
        }
        if (5) <= version.0 {
            self.protocol_name
                .encode_flexible(buf, is_flexible)
                .map_err(|_| SerializationError::Encode("failed to encode ProtocolName"))?;
        }
        self.assignments
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode Assignments"))?;
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
        let is_flexible = (4) <= version.0;
        let group_id = <String as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode GroupId"))?;
        let generation_id =
            <i32 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode GenerationId"))?;
        let member_id = <String as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode MemberId"))?;
        let group_instance_id = if (3) <= version.0 {
            <Option<String> as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode GroupInstanceId"))?
        } else {
            Default::default()
        };
        let protocol_type = if (5) <= version.0 {
            <Option<String> as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode ProtocolType"))?
        } else {
            Default::default()
        };
        let protocol_name = if (5) <= version.0 {
            <Option<String> as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode ProtocolName"))?
        } else {
            Default::default()
        };
        let assignments = <Vec<SyncGroupRequestAssignment> as KafkaDeserialize>::decode_flexible(
            buf,
            version,
            is_flexible,
        )
        .map_err(|_| SerializationError::Decode("failed to decode Assignments"))?;
        Ok(Self {
            group_id,
            generation_id,
            member_id,
            group_instance_id,
            protocol_type,
            protocol_name,
            assignments,
        })
    }
}
impl KafkaSerialize for SyncGroupRequest {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.group_id
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode GroupId".into(),
            })?;
        self.generation_id
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode GenerationId".into(),
            })?;
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
        self.protocol_type
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ProtocolType".into(),
            })?;
        self.protocol_name
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ProtocolName".into(),
            })?;
        self.assignments
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Assignments".into(),
            })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(
        &self,
        buf: &mut B,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.group_id
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode GroupId".into(),
            })?;
        self.generation_id
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode GenerationId".into(),
            })?;
        self.member_id
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode MemberId".into(),
            })?;
        if is_flexible {
            if let Some(ref __val) = self.group_instance_id {
                crate::protocol::serialization::encode_unsigned_varint(1u64, buf);
                __val
                    .encode_flexible(buf, true)
                    .map_err(|_| EncodeError::ValueTooLarge {
                        message: "failed to encode GroupInstanceId".into(),
                    })?;
            } else {
                crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
            }
        } else {
            if let Some(ref __val) = self.group_instance_id {
                __val.encode(buf).map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode GroupInstanceId".into(),
                })?;
            }
        }
        if is_flexible {
            if let Some(ref __val) = self.protocol_type {
                crate::protocol::serialization::encode_unsigned_varint(1u64, buf);
                __val
                    .encode_flexible(buf, true)
                    .map_err(|_| EncodeError::ValueTooLarge {
                        message: "failed to encode ProtocolType".into(),
                    })?;
            } else {
                crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
            }
        } else {
            if let Some(ref __val) = self.protocol_type {
                __val.encode(buf).map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode ProtocolType".into(),
                })?;
            }
        }
        if is_flexible {
            if let Some(ref __val) = self.protocol_name {
                crate::protocol::serialization::encode_unsigned_varint(1u64, buf);
                __val
                    .encode_flexible(buf, true)
                    .map_err(|_| EncodeError::ValueTooLarge {
                        message: "failed to encode ProtocolName".into(),
                    })?;
            } else {
                crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
            }
        } else {
            if let Some(ref __val) = self.protocol_name {
                __val.encode(buf).map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode ProtocolName".into(),
                })?;
            }
        }
        self.assignments
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Assignments".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for SyncGroupRequest {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] classic decode field `GroupId` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let group_id =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode GroupId".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `GenerationId` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let generation_id =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode GenerationId".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `MemberId` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let member_id =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode MemberId".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `GroupInstanceId` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let group_instance_id =
            <Option<String> as KafkaDeserialize>::decode(buf).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode GroupInstanceId".into(),
                }
            })?;
        tracing::trace!(
            "  [{}] classic decode field `ProtocolType` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let protocol_type = <Option<String> as KafkaDeserialize>::decode(buf).map_err(|_| {
            DecodeError::Protocol {
                message: "failed to decode ProtocolType".into(),
            }
        })?;
        tracing::trace!(
            "  [{}] classic decode field `ProtocolName` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let protocol_name = <Option<String> as KafkaDeserialize>::decode(buf).map_err(|_| {
            DecodeError::Protocol {
                message: "failed to decode ProtocolName".into(),
            }
        })?;
        tracing::trace!(
            "  [{}] classic decode field `Assignments` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let assignments = <Vec<SyncGroupRequestAssignment> as KafkaDeserialize>::decode(buf)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode Assignments".into(),
            })?;
        Ok(Self {
            group_id,
            generation_id,
            member_id,
            group_instance_id,
            protocol_type,
            protocol_name,
            assignments,
        })
    }
    fn decode_flexible<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] decoding field `GroupId` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let group_id = <String as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode GroupId".into(),
            })?;
        tracing::trace!(
            "  [{}] decoding field `GenerationId` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let generation_id = <i32 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
            message: "failed to decode GenerationId".into(),
        })?;
        tracing::trace!(
            "  [{}] decoding field `MemberId` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let member_id = <String as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode MemberId".into(),
            })?;
        tracing::trace!(
            "  [{}] decoding field `GroupInstanceId` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let group_instance_id = if is_flexible {
            <Option<String> as KafkaDeserialize>::decode_flexible(buf, version, true).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode GroupInstanceId".into(),
                },
            )?
        } else {
            <Option<String> as KafkaDeserialize>::decode(buf).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode GroupInstanceId".into(),
                }
            })?
        };
        tracing::trace!(
            "  [{}] decoding field `ProtocolType` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let protocol_type = if is_flexible {
            <Option<String> as KafkaDeserialize>::decode_flexible(buf, version, true).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode ProtocolType".into(),
                },
            )?
        } else {
            <Option<String> as KafkaDeserialize>::decode(buf).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode ProtocolType".into(),
                }
            })?
        };
        tracing::trace!(
            "  [{}] decoding field `ProtocolName` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let protocol_name = if is_flexible {
            <Option<String> as KafkaDeserialize>::decode_flexible(buf, version, true).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode ProtocolName".into(),
                },
            )?
        } else {
            <Option<String> as KafkaDeserialize>::decode(buf).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode ProtocolName".into(),
                }
            })?
        };
        tracing::trace!(
            "  [{}] decoding field `Assignments` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let assignments = <Vec<SyncGroupRequestAssignment> as KafkaDeserialize>::decode_flexible(
            buf,
            version,
            is_flexible,
        )
        .map_err(|_| DecodeError::Protocol {
            message: "failed to decode Assignments".into(),
        })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            group_id,
            generation_id,
            member_id,
            group_instance_id,
            protocol_type,
            protocol_name,
            assignments,
        })
    }
}

impl KafkaSerialize for SyncGroupRequestAssignment {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.member_id
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode MemberId".into(),
            })?;
        self.assignment
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Assignment".into(),
            })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(
        &self,
        buf: &mut B,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.member_id
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode MemberId".into(),
            })?;
        self.assignment
            .encode_flexible(buf, is_flexible)
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

impl KafkaDeserialize for SyncGroupRequestAssignment {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] classic decode field `MemberId` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let member_id =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode MemberId".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `Assignment` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let assignment =
            <Vec<u8> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode Assignment".into(),
            })?;
        Ok(Self {
            member_id,
            assignment,
        })
    }
    fn decode_flexible<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] decoding field `MemberId` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let member_id = <String as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode MemberId".into(),
            })?;
        tracing::trace!(
            "  [{}] decoding field `Assignment` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let assignment = <Vec<u8> as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode Assignment".into(),
            })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            member_id,
            assignment,
        })
    }
}

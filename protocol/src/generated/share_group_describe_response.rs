#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// ShareGroupDescribeResponse
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ShareGroupDescribeResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// Each described group.
    pub groups: Vec<DescribedGroup>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Assignment {
    /// The assigned topic-partitions to the member.
    pub topic_partitions: Vec<TopicPartitions>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct DescribedGroup {
    /// The describe error, or 0 if there was no error.
    pub error_code: i16,
    /// The top-level error message, or null if there was no error.
    pub error_message: Option<String>,
    /// The group ID string.
    pub group_id: String,
    /// The group state string, or the empty string.
    pub group_state: String,
    /// The group epoch.
    pub group_epoch: i32,
    /// The assignment epoch.
    pub assignment_epoch: i32,
    /// The selected assignor.
    pub assignor_name: String,
    /// The members.
    pub members: Vec<Member>,
    /// 32-bit bitfield to represent authorized operations for this group.
    pub authorized_operations: i32,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Member {
    /// The member ID.
    pub member_id: String,
    /// The member rack ID.
    pub rack_id: Option<String>,
    /// The current member epoch.
    pub member_epoch: i32,
    /// The client ID.
    pub client_id: String,
    /// The client host.
    pub client_host: String,
    /// The subscribed topic names.
    pub subscribed_topic_names: Vec<String>,
    /// The current assignment.
    pub assignment: Assignment,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct TopicPartitions {
    /// The topic ID.
    pub topic_id: [u8; 16],
    /// The topic name.
    pub topic_name: String,
    /// The partitions.
    pub partitions: Vec<i32>,
}

impl ApiResponse for ShareGroupDescribeResponse {
    type Request = crate::generated::ShareGroupDescribeRequest;
    fn get_api_key() -> ApiKey {
        ApiKey::new(77)
    }
    fn get_min_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(1)
    }
    fn get_max_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(1)
    }
    fn get_min_flexible_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(0)
    }
    fn serialize(
        &self,
        version: crate::traits::ApiVersion,
        buf: &mut BytesMut,
    ) -> Result<(), SerializationError> {
        assert!(
            (1) <= version.0 && version.0 <= (1),
            "version {} is not supported by {} (supported: 1-1)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        self.throttle_time_ms
            .encode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode ThrottleTimeMs"))?;
        self.groups
            .encode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode Groups"))?;
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
        let throttle_time_ms = <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode ThrottleTimeMs"))?;
        let groups = <Vec<DescribedGroup> as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode Groups"))?;
        Ok(Self {
            throttle_time_ms,
            groups,
        })
    }
}
impl KafkaSerialize for ShareGroupDescribeResponse {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.throttle_time_ms
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ThrottleTimeMs".into(),
            })?;
        self.groups
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Groups".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for ShareGroupDescribeResponse {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        let throttle_time_ms = <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode ThrottleTimeMs".into(),
            })?;
        let groups = <Vec<DescribedGroup> as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
            message: "failed to decode Groups".into(),
        })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            throttle_time_ms,
            groups,
        })
    }
}

impl KafkaSerialize for Assignment {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.topic_partitions
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode TopicPartitions".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for Assignment {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        let topic_partitions =
            <Vec<TopicPartitions> as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode TopicPartitions".into(),
                },
            )?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self { topic_partitions })
    }
}

impl KafkaSerialize for DescribedGroup {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.error_code
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ErrorCode".into(),
            })?;
        self.error_message
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ErrorMessage".into(),
            })?;
        self.group_id
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode GroupId".into(),
            })?;
        self.group_state
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode GroupState".into(),
            })?;
        self.group_epoch
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode GroupEpoch".into(),
            })?;
        self.assignment_epoch
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode AssignmentEpoch".into(),
            })?;
        self.assignor_name
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode AssignorName".into(),
            })?;
        self.members
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Members".into(),
            })?;
        self.authorized_operations
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode AuthorizedOperations".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for DescribedGroup {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        let error_code =
            <i16 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode ErrorCode".into(),
                }
            })?;
        let error_message = <Option<String> as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode ErrorMessage".into(),
            })?;
        let group_id =
            <String as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode GroupId".into(),
                }
            })?;
        let group_state =
            <String as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode GroupState".into(),
                }
            })?;
        let group_epoch =
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode GroupEpoch".into(),
                }
            })?;
        let assignment_epoch = <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode AssignmentEpoch".into(),
            })?;
        let assignor_name = <String as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode AssignorName".into(),
            })?;
        let members = <Vec<Member> as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode Members".into(),
            })?;
        let authorized_operations = <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode AuthorizedOperations".into(),
            })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            error_code,
            error_message,
            group_id,
            group_state,
            group_epoch,
            assignment_epoch,
            assignor_name,
            members,
            authorized_operations,
        })
    }
}

impl KafkaSerialize for Member {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.member_id
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode MemberId".into(),
            })?;
        self.rack_id
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode RackId".into(),
            })?;
        self.member_epoch
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode MemberEpoch".into(),
            })?;
        self.client_id
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ClientId".into(),
            })?;
        self.client_host
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ClientHost".into(),
            })?;
        self.subscribed_topic_names
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode SubscribedTopicNames".into(),
            })?;
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

impl KafkaDeserialize for Member {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        let member_id =
            <String as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode MemberId".into(),
                }
            })?;
        let rack_id = <Option<String> as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode RackId".into(),
            })?;
        let member_epoch =
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode MemberEpoch".into(),
                }
            })?;
        let client_id =
            <String as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode ClientId".into(),
                }
            })?;
        let client_host =
            <String as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode ClientHost".into(),
                }
            })?;
        let subscribed_topic_names =
            <Vec<String> as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode SubscribedTopicNames".into(),
                }
            })?;
        let assignment = <Assignment as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode Assignment".into(),
            })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            member_id,
            rack_id,
            member_epoch,
            client_id,
            client_host,
            subscribed_topic_names,
            assignment,
        })
    }
}

impl KafkaSerialize for TopicPartitions {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.topic_id
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode TopicId".into(),
            })?;
        self.topic_name
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode TopicName".into(),
            })?;
        self.partitions
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Partitions".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for TopicPartitions {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        let topic_id =
            <[u8; 16] as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode TopicId".into(),
                }
            })?;
        let topic_name =
            <String as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode TopicName".into(),
                }
            })?;
        let partitions = <Vec<i32> as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode Partitions".into(),
            })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            topic_id,
            topic_name,
            partitions,
        })
    }
}

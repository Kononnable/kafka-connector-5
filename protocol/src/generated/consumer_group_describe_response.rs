#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// ConsumerGroupDescribeResponse
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ConsumerGroupDescribeResponse {
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
    /// The member instance ID.
    pub instance_id: Option<String>,
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
    /// the subscribed topic regex otherwise or null of not provided.
    pub subscribed_topic_regex: Option<String>,
    /// The current assignment.
    pub assignment: Assignment,
    /// The target assignment.
    pub target_assignment: Assignment,
    /// -1 for unknown. 0 for classic member. +1 for consumer member.
    /// Available in version 1+.
    pub member_type: i8,
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

impl ApiResponse for ConsumerGroupDescribeResponse {
    type Request = crate::generated::ConsumerGroupDescribeRequest;
    fn get_api_key() -> ApiKey {
        ApiKey::new(69)
    }
    fn get_min_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(0)
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
            (0) <= version.0 && version.0 <= (1),
            "version {} is not supported by {} (supported: 0-1)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        self.throttle_time_ms.encode(buf, version, is_flexible)?;
        self.groups.encode(buf, version, is_flexible)?;
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(
        version: crate::traits::ApiVersion,
        buf: &mut Bytes,
    ) -> Result<Self, SerializationError> {
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let throttle_time_ms = <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let groups = <Vec<DescribedGroup> as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            throttle_time_ms,
            groups,
        })
    }
}
impl KafkaSerialize for ConsumerGroupDescribeResponse {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), crate::traits::SerializationError> {
        self.throttle_time_ms.encode(buf, version, is_flexible)?;
        self.groups.encode(buf, version, is_flexible)?;
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for ConsumerGroupDescribeResponse {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, crate::traits::SerializationError> {
        let throttle_time_ms = <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let groups = <Vec<DescribedGroup> as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        if is_flexible {
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
    ) -> Result<(), crate::traits::SerializationError> {
        self.topic_partitions.encode(buf, version, is_flexible)?;
        if is_flexible {
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
    ) -> Result<Self, crate::traits::SerializationError> {
        let topic_partitions =
            <Vec<TopicPartitions> as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        if is_flexible {
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
    ) -> Result<(), crate::traits::SerializationError> {
        self.error_code.encode(buf, version, is_flexible)?;
        self.error_message.encode(buf, version, is_flexible)?;
        self.group_id.encode(buf, version, is_flexible)?;
        self.group_state.encode(buf, version, is_flexible)?;
        self.group_epoch.encode(buf, version, is_flexible)?;
        self.assignment_epoch.encode(buf, version, is_flexible)?;
        self.assignor_name.encode(buf, version, is_flexible)?;
        self.members.encode(buf, version, is_flexible)?;
        self.authorized_operations
            .encode(buf, version, is_flexible)?;
        if is_flexible {
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
    ) -> Result<Self, crate::traits::SerializationError> {
        let error_code = <i16 as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let error_message =
            <Option<String> as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let group_id = <String as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let group_state = <String as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let group_epoch = <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let assignment_epoch = <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let assignor_name = <String as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let members = <Vec<Member> as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let authorized_operations = <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        if is_flexible {
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
    ) -> Result<(), crate::traits::SerializationError> {
        self.member_id.encode(buf, version, is_flexible)?;
        self.instance_id.encode(buf, version, is_flexible)?;
        self.rack_id.encode(buf, version, is_flexible)?;
        self.member_epoch.encode(buf, version, is_flexible)?;
        self.client_id.encode(buf, version, is_flexible)?;
        self.client_host.encode(buf, version, is_flexible)?;
        self.subscribed_topic_names
            .encode(buf, version, is_flexible)?;
        self.subscribed_topic_regex
            .encode(buf, version, is_flexible)?;
        self.assignment.encode(buf, version, is_flexible)?;
        self.target_assignment.encode(buf, version, is_flexible)?;
        if (1) <= version.0 {
            self.member_type.encode(buf, version, is_flexible)?;
        }
        if is_flexible {
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
    ) -> Result<Self, crate::traits::SerializationError> {
        let member_id = <String as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let instance_id = <Option<String> as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let rack_id = <Option<String> as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let member_epoch = <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let client_id = <String as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let client_host = <String as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let subscribed_topic_names =
            <Vec<String> as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let subscribed_topic_regex =
            <Option<String> as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let assignment = <Assignment as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let target_assignment =
            <Assignment as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let member_type = if (1) <= version.0 {
            <i8 as KafkaDeserialize>::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            member_id,
            instance_id,
            rack_id,
            member_epoch,
            client_id,
            client_host,
            subscribed_topic_names,
            subscribed_topic_regex,
            assignment,
            target_assignment,
            member_type,
        })
    }
}

impl KafkaSerialize for TopicPartitions {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), crate::traits::SerializationError> {
        self.topic_id.encode(buf, version, is_flexible)?;
        self.topic_name.encode(buf, version, is_flexible)?;
        self.partitions.encode(buf, version, is_flexible)?;
        if is_flexible {
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
    ) -> Result<Self, crate::traits::SerializationError> {
        let topic_id = <[u8; 16] as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let topic_name = <String as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let partitions = <Vec<i32> as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            topic_id,
            topic_name,
            partitions,
        })
    }
}

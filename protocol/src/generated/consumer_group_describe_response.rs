#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{KafkaCodec, decode_unsigned_varint, encode_unsigned_varint};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVer, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use indexmap::IndexMap;

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
    fn get_min_supported_version() -> ApiVer {
        ApiVer::new(0)
    }
    fn get_max_supported_version() -> ApiVer {
        ApiVer::new(1)
    }
    fn get_min_flexible_version() -> ApiVer {
        ApiVer::new(0)
    }
    fn serialize(&self, version: ApiVer, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            0 <= version.0 && version.0 <= 1,
            "version {} is not supported by {} (supported: 0-1)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        self.throttle_time_ms.encode(buf, version, is_flexible)?;
        self.groups.encode(buf, version, is_flexible)?;
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(version: ApiVer, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let throttle_time_ms = KafkaCodec::decode(buf, version, is_flexible)?;
        let groups = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            throttle_time_ms,
            groups,
        })
    }
}
impl KafkaCodec for ConsumerGroupDescribeResponse {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.throttle_time_ms.encode(buf, version, is_flexible)?;
        self.groups.encode(buf, version, is_flexible)?;
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
        let throttle_time_ms = KafkaCodec::decode(buf, version, is_flexible)?;
        let groups = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            throttle_time_ms,
            groups,
        })
    }
}

impl KafkaCodec for Assignment {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.topic_partitions.encode(buf, version, is_flexible)?;
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
        let topic_partitions = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self { topic_partitions })
    }
}

impl KafkaCodec for DescribedGroup {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
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
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }

    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let error_code = KafkaCodec::decode(buf, version, is_flexible)?;
        let error_message = KafkaCodec::decode(buf, version, is_flexible)?;
        let group_id = KafkaCodec::decode(buf, version, is_flexible)?;
        let group_state = KafkaCodec::decode(buf, version, is_flexible)?;
        let group_epoch = KafkaCodec::decode(buf, version, is_flexible)?;
        let assignment_epoch = KafkaCodec::decode(buf, version, is_flexible)?;
        let assignor_name = KafkaCodec::decode(buf, version, is_flexible)?;
        let members = KafkaCodec::decode(buf, version, is_flexible)?;
        let authorized_operations = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
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

impl KafkaCodec for Member {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
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
        if 1 <= version.0 {
            self.member_type.encode(buf, version, is_flexible)?;
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
        let member_id = KafkaCodec::decode(buf, version, is_flexible)?;
        let instance_id = KafkaCodec::decode(buf, version, is_flexible)?;
        let rack_id = KafkaCodec::decode(buf, version, is_flexible)?;
        let member_epoch = KafkaCodec::decode(buf, version, is_flexible)?;
        let client_id = KafkaCodec::decode(buf, version, is_flexible)?;
        let client_host = KafkaCodec::decode(buf, version, is_flexible)?;
        let subscribed_topic_names = KafkaCodec::decode(buf, version, is_flexible)?;
        let subscribed_topic_regex = KafkaCodec::decode(buf, version, is_flexible)?;
        let assignment = KafkaCodec::decode(buf, version, is_flexible)?;
        let target_assignment = KafkaCodec::decode(buf, version, is_flexible)?;
        let member_type = if 1 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
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

impl KafkaCodec for TopicPartitions {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.topic_id.encode(buf, version, is_flexible)?;
        self.topic_name.encode(buf, version, is_flexible)?;
        self.partitions.encode(buf, version, is_flexible)?;
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
        let topic_id = KafkaCodec::decode(buf, version, is_flexible)?;
        let topic_name = KafkaCodec::decode(buf, version, is_flexible)?;
        let partitions = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            topic_id,
            topic_name,
            partitions,
        })
    }
}

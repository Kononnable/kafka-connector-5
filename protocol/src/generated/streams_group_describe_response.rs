#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// StreamsGroupDescribeResponse
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct StreamsGroupDescribeResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// Each described group.
    pub groups: Vec<DescribedGroup>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Assignment {
    /// Active tasks for this client.
    pub active_tasks: Vec<TaskIds>,
    /// Standby tasks for this client.
    pub standby_tasks: Vec<TaskIds>,
    /// Warm-up tasks for this client.
    pub warmup_tasks: Vec<TaskIds>,
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
    /// The topology metadata currently initialized for the streams application. Can be null in case of a describe error.
    pub topology: Option<Topology>,
    /// The members.
    pub members: Vec<Member>,
    /// 32-bit bitfield to represent authorized operations for this group.
    pub authorized_operations: i32,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Endpoint {
    /// host of the endpoint
    pub host: String,
    /// port of the endpoint
    pub port: u16,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct KeyValue {
    /// key of the config
    pub key: String,
    /// value of the config
    pub value: String,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Member {
    /// The member ID.
    pub member_id: String,
    /// The member epoch.
    pub member_epoch: i32,
    /// The member instance ID for static membership.
    pub instance_id: Option<String>,
    /// The rack ID.
    pub rack_id: Option<String>,
    /// The client ID.
    pub client_id: String,
    /// The client host.
    pub client_host: String,
    /// The epoch of the topology on the client.
    pub topology_epoch: i32,
    /// Identity of the streams instance that may have multiple clients.
    pub process_id: String,
    /// User-defined endpoint for Interactive Queries. Null if not defined for this client.
    pub user_endpoint: Option<Endpoint>,
    /// Used for rack-aware assignment algorithm.
    pub client_tags: Vec<KeyValue>,
    /// Cumulative changelog offsets for tasks.
    pub task_offsets: Vec<TaskOffset>,
    /// Cumulative changelog end offsets for tasks.
    pub task_end_offsets: Vec<TaskOffset>,
    /// The current assignment.
    pub assignment: Assignment,
    /// The target assignment.
    pub target_assignment: Assignment,
    /// True for classic members that have not been upgraded yet.
    pub is_classic: bool,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Subtopology {
    /// String to uniquely identify the subtopology.
    pub subtopology_id: String,
    /// The topics the subtopology reads from.
    pub source_topics: Vec<String>,
    /// The repartition topics the subtopology writes to.
    pub repartition_sink_topics: Vec<String>,
    /// The set of state changelog topics associated with this subtopology. Created automatically.
    pub state_changelog_topics: Vec<TopicInfo>,
    /// The set of source topics that are internally created repartition topics. Created automatically.
    pub repartition_source_topics: Vec<TopicInfo>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct TaskIds {
    /// The subtopology identifier.
    pub subtopology_id: String,
    /// The partitions of the input topics processed by this member.
    pub partitions: Vec<i32>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct TaskOffset {
    /// The subtopology identifier.
    pub subtopology_id: String,
    /// The partition.
    pub partition: i32,
    /// The offset.
    pub offset: i64,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct TopicInfo {
    /// The name of the topic.
    pub name: String,
    /// The number of partitions in the topic. Can be 0 if no specific number of partitions is enforced. Always 0 for changelog topics.
    pub partitions: i32,
    /// The replication factor of the topic. Can be 0 if the default replication factor should be used.
    pub replication_factor: i16,
    /// Topic-level configurations as key-value pairs.
    pub topic_configs: Vec<KeyValue>,
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

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Topology {
    /// The epoch of the currently initialized topology for this group.
    pub epoch: i32,
    /// The subtopologies of the streams application. This contains the configured subtopologies, where the number of partitions are set and any regular expressions are resolved to actual topics. Null if the group is uninitialized, source topics are missing or incorrectly partitioned.
    pub subtopologies: Option<Vec<Subtopology>>,
}

impl ApiResponse for StreamsGroupDescribeResponse {
    type Request = crate::generated::StreamsGroupDescribeRequest;
    fn get_api_key() -> ApiKey {
        ApiKey::new(89)
    }
    fn get_min_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(0)
    }
    fn get_max_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(0)
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
            (0) <= version.0 && version.0 <= (0),
            "version {} is not supported by {} (supported: 0-0)",
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
impl KafkaSerialize for StreamsGroupDescribeResponse {
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

impl KafkaDeserialize for StreamsGroupDescribeResponse {
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
        self.active_tasks
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ActiveTasks".into(),
            })?;
        self.standby_tasks
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode StandbyTasks".into(),
            })?;
        self.warmup_tasks
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode WarmupTasks".into(),
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
        let active_tasks = <Vec<TaskIds> as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode ActiveTasks".into(),
            })?;
        let standby_tasks = <Vec<TaskIds> as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
            message: "failed to decode StandbyTasks".into(),
        })?;
        let warmup_tasks = <Vec<TaskIds> as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode WarmupTasks".into(),
            })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            active_tasks,
            standby_tasks,
            warmup_tasks,
        })
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
        if is_flexible {
            if let Some(ref __val) = self.topology {
                crate::protocol::serialization::encode_unsigned_varint(1u64, buf);
                __val
                    .encode(buf, version, true)
                    .map_err(|_| EncodeError::ValueTooLarge {
                        message: "failed to encode Topology".into(),
                    })?;
            } else {
                crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
            }
        } else {
            if let Some(ref __val) = self.topology {
                __val
                    .encode(buf, version, false)
                    .map_err(|_| EncodeError::ValueTooLarge {
                        message: "failed to encode Topology".into(),
                    })?;
            }
        }
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
        let topology = if is_flexible {
            let (__present, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
            if __present == 0 {
                None
            } else {
                Some(
                    <Topology as KafkaDeserialize>::decode(buf, version, true).map_err(|_| {
                        DecodeError::Protocol {
                            message: "failed to decode Topology".into(),
                        }
                    })?,
                )
            }
        } else {
            Some(
                <Topology as KafkaDeserialize>::decode(buf, version, false).map_err(|_| {
                    DecodeError::Protocol {
                        message: "failed to decode Topology".into(),
                    }
                })?,
            )
        };
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
            topology,
            members,
            authorized_operations,
        })
    }
}

impl KafkaSerialize for Endpoint {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.host
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Host".into(),
            })?;
        self.port
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Port".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for Endpoint {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        let host =
            <String as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode Host".into(),
                }
            })?;
        let port = <u16 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
            DecodeError::Protocol {
                message: "failed to decode Port".into(),
            }
        })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self { host, port })
    }
}

impl KafkaSerialize for KeyValue {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.key
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Key".into(),
            })?;
        self.value
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Value".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for KeyValue {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        let key =
            <String as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode Key".into(),
                }
            })?;
        let value =
            <String as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode Value".into(),
                }
            })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self { key, value })
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
        self.member_epoch
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode MemberEpoch".into(),
            })?;
        self.instance_id
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode InstanceId".into(),
            })?;
        self.rack_id
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode RackId".into(),
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
        self.topology_epoch
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode TopologyEpoch".into(),
            })?;
        self.process_id
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ProcessId".into(),
            })?;
        if is_flexible {
            if let Some(ref __val) = self.user_endpoint {
                crate::protocol::serialization::encode_unsigned_varint(1u64, buf);
                __val
                    .encode(buf, version, true)
                    .map_err(|_| EncodeError::ValueTooLarge {
                        message: "failed to encode UserEndpoint".into(),
                    })?;
            } else {
                crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
            }
        } else {
            if let Some(ref __val) = self.user_endpoint {
                __val
                    .encode(buf, version, false)
                    .map_err(|_| EncodeError::ValueTooLarge {
                        message: "failed to encode UserEndpoint".into(),
                    })?;
            }
        }
        self.client_tags
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ClientTags".into(),
            })?;
        self.task_offsets
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode TaskOffsets".into(),
            })?;
        self.task_end_offsets
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode TaskEndOffsets".into(),
            })?;
        self.assignment
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Assignment".into(),
            })?;
        self.target_assignment
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode TargetAssignment".into(),
            })?;
        self.is_classic
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode IsClassic".into(),
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
        let member_epoch =
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode MemberEpoch".into(),
                }
            })?;
        let instance_id = <Option<String> as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
            message: "failed to decode InstanceId".into(),
        })?;
        let rack_id = <Option<String> as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode RackId".into(),
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
        let topology_epoch =
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode TopologyEpoch".into(),
                }
            })?;
        let process_id =
            <String as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode ProcessId".into(),
                }
            })?;
        let user_endpoint = if is_flexible {
            let (__present, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
            if __present == 0 {
                None
            } else {
                Some(
                    <Endpoint as KafkaDeserialize>::decode(buf, version, true).map_err(|_| {
                        DecodeError::Protocol {
                            message: "failed to decode UserEndpoint".into(),
                        }
                    })?,
                )
            }
        } else {
            Some(
                <Endpoint as KafkaDeserialize>::decode(buf, version, false).map_err(|_| {
                    DecodeError::Protocol {
                        message: "failed to decode UserEndpoint".into(),
                    }
                })?,
            )
        };
        let client_tags = <Vec<KeyValue> as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode ClientTags".into(),
            })?;
        let task_offsets = <Vec<TaskOffset> as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode TaskOffsets".into(),
            })?;
        let task_end_offsets =
            <Vec<TaskOffset> as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode TaskEndOffsets".into(),
                },
            )?;
        let assignment = <Assignment as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode Assignment".into(),
            })?;
        let target_assignment = <Assignment as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode TargetAssignment".into(),
            })?;
        let is_classic =
            <bool as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode IsClassic".into(),
                }
            })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            member_id,
            member_epoch,
            instance_id,
            rack_id,
            client_id,
            client_host,
            topology_epoch,
            process_id,
            user_endpoint,
            client_tags,
            task_offsets,
            task_end_offsets,
            assignment,
            target_assignment,
            is_classic,
        })
    }
}

impl KafkaSerialize for Subtopology {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.subtopology_id
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode SubtopologyId".into(),
            })?;
        self.source_topics
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode SourceTopics".into(),
            })?;
        self.repartition_sink_topics
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode RepartitionSinkTopics".into(),
            })?;
        self.state_changelog_topics
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode StateChangelogTopics".into(),
            })?;
        self.repartition_source_topics
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode RepartitionSourceTopics".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for Subtopology {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        let subtopology_id = <String as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode SubtopologyId".into(),
            })?;
        let source_topics = <Vec<String> as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode SourceTopics".into(),
            })?;
        let repartition_sink_topics =
            <Vec<String> as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode RepartitionSinkTopics".into(),
                }
            })?;
        let state_changelog_topics =
            <Vec<TopicInfo> as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode StateChangelogTopics".into(),
                },
            )?;
        let repartition_source_topics =
            <Vec<TopicInfo> as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode RepartitionSourceTopics".into(),
                },
            )?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            subtopology_id,
            source_topics,
            repartition_sink_topics,
            state_changelog_topics,
            repartition_source_topics,
        })
    }
}

impl KafkaSerialize for TaskIds {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.subtopology_id
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode SubtopologyId".into(),
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

impl KafkaDeserialize for TaskIds {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        let subtopology_id = <String as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode SubtopologyId".into(),
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
            subtopology_id,
            partitions,
        })
    }
}

impl KafkaSerialize for TaskOffset {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.subtopology_id
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode SubtopologyId".into(),
            })?;
        self.partition
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Partition".into(),
            })?;
        self.offset
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Offset".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for TaskOffset {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        let subtopology_id = <String as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode SubtopologyId".into(),
            })?;
        let partition =
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode Partition".into(),
                }
            })?;
        let offset =
            <i64 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode Offset".into(),
                }
            })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            subtopology_id,
            partition,
            offset,
        })
    }
}

impl KafkaSerialize for TopicInfo {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.name
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Name".into(),
            })?;
        self.partitions
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Partitions".into(),
            })?;
        self.replication_factor
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ReplicationFactor".into(),
            })?;
        self.topic_configs
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode TopicConfigs".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for TopicInfo {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        let name =
            <String as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode Name".into(),
                }
            })?;
        let partitions =
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode Partitions".into(),
                }
            })?;
        let replication_factor = <i16 as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode ReplicationFactor".into(),
            })?;
        let topic_configs = <Vec<KeyValue> as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode TopicConfigs".into(),
            })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            name,
            partitions,
            replication_factor,
            topic_configs,
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

impl KafkaSerialize for Topology {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.epoch
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Epoch".into(),
            })?;
        self.subtopologies
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Subtopologies".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for Topology {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        let epoch = <i32 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
            DecodeError::Protocol {
                message: "failed to decode Epoch".into(),
            }
        })?;
        let subtopologies =
            <Option<Vec<Subtopology>> as KafkaDeserialize>::decode(buf, version, is_flexible)
                .map_err(|_| DecodeError::Protocol {
                    message: "failed to decode Subtopologies".into(),
                })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            epoch,
            subtopologies,
        })
    }
}

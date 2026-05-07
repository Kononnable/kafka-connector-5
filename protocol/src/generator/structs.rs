//! Module for representing Kafka protocol structures as Rust types.

use serde::{Deserialize, Serialize};

/// Represents a Kafka protocol message structure
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct MessageStruct {
    /// The API key for this message
    #[serde(rename = "apiKey", default)]
    pub api_key: Option<i16>,

    /// The type of message (request or response)
    #[serde(rename = "type")]
    pub message_type: MessageType,

    /// The name of the message
    pub name: String,

    /// The valid versions of this message
    #[serde(rename = "validVersions")]
    pub valid_versions: String,

    /// The versions that support flexible (varint) encoding
    #[serde(rename = "flexibleVersions", default)]
    pub flexible_versions: Option<String>,

    /// Shared struct definitions referenced by multiple messages
    #[serde(rename = "commonStructs", default)]
    pub common_structs: Vec<Field>,

    /// Fields in this message
    pub fields: Vec<Field>,
}

/// Represents the type of message
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum MessageType {
    #[serde(rename = "request")]
    Request,
    #[serde(rename = "response")]
    Response,
    #[serde(rename = "header")]
    Header,
    #[serde(rename = "data")]
    Data,
}

/// Represents a field in a Kafka message
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Field {
    /// The name of the field
    pub name: String,

    /// The type of the field (e.g., "int32", "string", "[]MetadataRequestTopic")
    /// Defaults to empty for common struct definitions.
    #[serde(rename = "type", default)]
    pub field_type: String,

    /// The versions this field is supported in
    pub versions: String,

    /// Whether this field is nullable
    #[serde(rename = "nullableVersions", default)]
    pub nullable_versions: Option<String>,

    /// Whether this field supports flexible (varint) encoding
    #[serde(rename = "flexibleVersions", default)]
    pub flexible_versions: Option<String>,

    /// Whether this field is ignorable
    #[serde(default = "default_true")]
    pub ignorable: bool,

    /// The default value for this field (if any)
    #[serde(default)]
    pub default: Option<FieldDefault>,

    /// About text for documentation
    #[serde(default)]
    pub about: Option<String>,

    /// Whether this field is a map key
    #[serde(rename = "mapKey", default)]
    pub map_key: bool,

    /// The entity type (e.g. "transactionalId")
    #[serde(rename = "entityType", default)]
    pub entity_type: Option<String>,

    /// Numeric tag for tagged fields (flexible encoding)
    #[serde(default)]
    pub tag: Option<i32>,

    /// Versions where the tagged field is present
    #[serde(rename = "taggedVersions", default)]
    pub tagged_versions: Option<String>,

    /// Whether this field supports zero-copy transfer
    #[serde(rename = "zeroCopy", default)]
    pub zero_copy: bool,

    /// Nested fields for complex types (like arrays)
    #[serde(default)]
    pub fields: Vec<Field>,
}

fn default_true() -> bool {
    true
}

/// Kafka protocol error codes.
///
/// Maps each i16 error code returned by the broker to a named variant with
/// a human-readable description. Unknown codes map to `UnknownServerError`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i16)]
pub enum ErrorCode {
    UnknownServerError = -1,
    None = 0,
    OffsetOutOfRange = 1,
    CorruptMessage = 2,
    UnknownTopicOrPartition = 3,
    InvalidFetchSize = 4,
    LeaderNotAvailable = 5,
    NotLeaderForPartition = 6,
    RequestTimedOut = 7,
    BrokerNotAvailable = 8,
    ReplicaNotAvailable = 9,
    MessageTooLarge = 10,
    StaleControllerEpoch = 11,
    OffsetMetadataTooLarge = 12,
    NetworkException = 13,
    CoordinatorLoadInProgress = 14,
    CoordinatorNotAvailable = 15,
    NotCoordinator = 16,
    InvalidTopicException = 17,
    RecordListTooLarge = 18,
    NotEnoughReplicas = 19,
    NotEnoughReplicasAfterAppend = 20,
    InvalidRequiredAcks = 21,
    IllegalGeneration = 22,
    InconsistentGroupProtocol = 23,
    InvalidGroupId = 24,
    UnknownMemberId = 25,
    InvalidSessionTimeout = 26,
    RebalanceInProgress = 27,
    InvalidCommitOffsetSize = 28,
    TopicAuthorizationFailed = 29,
    GroupAuthorizationFailed = 30,
    ClusterAuthorizationFailed = 31,
    InvalidTimestamp = 32,
    UnsupportedSaslMechanism = 33,
    IllegalSaslState = 34,
    UnsupportedVersion = 35,
    TopicAlreadyExists = 36,
    InvalidPartitions = 37,
    InvalidReplicationFactor = 38,
    InvalidReplicaAssignment = 39,
    InvalidConfig = 40,
    NotController = 41,
    InvalidRequest = 42,
    UnsupportedForMessageFormat = 43,
    PolicyViolation = 44,
    OutOfOrderSequenceNumber = 45,
    DuplicateSequenceNumber = 46,
    InvalidProducerEpoch = 47,
    InvalidTxnState = 48,
    InvalidProducerIdMapping = 49,
    InvalidTransactionTimeout = 50,
    ConcurrentTransactions = 51,
    TransactionCoordinatorFenced = 52,
    TransactionalIdAuthorizationFailed = 53,
    SecurityDisabled = 54,
    OperationNotAttempted = 55,
    KafkaStorageError = 56,
    LogDirNotFound = 57,
    SaslAuthenticationFailed = 58,
    UnknownProducerId = 59,
    ReassignmentInProgress = 60,
    DelegationTokenAuthDisabled = 61,
    DelegationTokenNotFound = 62,
    DelegationTokenOwnerMismatch = 63,
    DelegationTokenRequestNotAllowed = 64,
    DelegationTokenAuthorizationFailed = 65,
    DelegationTokenExpired = 66,
    InvalidPrincipalType = 67,
    NonEmptyGroup = 68,
    GroupIdNotFound = 69,
    FetchSessionIdNotFound = 70,
    InvalidFetchSessionEpoch = 71,
    ListenerNotFound = 72,
    TopicDeletionDisabled = 73,
    FencedLeaderEpoch = 74,
    UnknownLeaderEpoch = 75,
    UnsupportedCompressionType = 76,
    StaleBrokerEpoch = 77,
    OffsetNotAvailable = 78,
    MemberIdRequired = 79,
    PreferredLeaderNotAvailable = 80,
    GroupMaxSizeReached = 81,
}

impl ErrorCode {
    /// Convert an `i16` error code from the wire protocol into an `ErrorCode`.
    /// Returns `UnknownServerError` for unrecognised codes.
    pub fn from_code(code: i16) -> Self {
        match code {
            -1 => Self::UnknownServerError,
            0 => Self::None,
            1 => Self::OffsetOutOfRange,
            2 => Self::CorruptMessage,
            3 => Self::UnknownTopicOrPartition,
            4 => Self::InvalidFetchSize,
            5 => Self::LeaderNotAvailable,
            6 => Self::NotLeaderForPartition,
            7 => Self::RequestTimedOut,
            8 => Self::BrokerNotAvailable,
            9 => Self::ReplicaNotAvailable,
            10 => Self::MessageTooLarge,
            11 => Self::StaleControllerEpoch,
            12 => Self::OffsetMetadataTooLarge,
            13 => Self::NetworkException,
            14 => Self::CoordinatorLoadInProgress,
            15 => Self::CoordinatorNotAvailable,
            16 => Self::NotCoordinator,
            17 => Self::InvalidTopicException,
            18 => Self::RecordListTooLarge,
            19 => Self::NotEnoughReplicas,
            20 => Self::NotEnoughReplicasAfterAppend,
            21 => Self::InvalidRequiredAcks,
            22 => Self::IllegalGeneration,
            23 => Self::InconsistentGroupProtocol,
            24 => Self::InvalidGroupId,
            25 => Self::UnknownMemberId,
            26 => Self::InvalidSessionTimeout,
            27 => Self::RebalanceInProgress,
            28 => Self::InvalidCommitOffsetSize,
            29 => Self::TopicAuthorizationFailed,
            30 => Self::GroupAuthorizationFailed,
            31 => Self::ClusterAuthorizationFailed,
            32 => Self::InvalidTimestamp,
            33 => Self::UnsupportedSaslMechanism,
            34 => Self::IllegalSaslState,
            35 => Self::UnsupportedVersion,
            36 => Self::TopicAlreadyExists,
            37 => Self::InvalidPartitions,
            38 => Self::InvalidReplicationFactor,
            39 => Self::InvalidReplicaAssignment,
            40 => Self::InvalidConfig,
            41 => Self::NotController,
            42 => Self::InvalidRequest,
            43 => Self::UnsupportedForMessageFormat,
            44 => Self::PolicyViolation,
            45 => Self::OutOfOrderSequenceNumber,
            46 => Self::DuplicateSequenceNumber,
            47 => Self::InvalidProducerEpoch,
            48 => Self::InvalidTxnState,
            49 => Self::InvalidProducerIdMapping,
            50 => Self::InvalidTransactionTimeout,
            51 => Self::ConcurrentTransactions,
            52 => Self::TransactionCoordinatorFenced,
            53 => Self::TransactionalIdAuthorizationFailed,
            54 => Self::SecurityDisabled,
            55 => Self::OperationNotAttempted,
            56 => Self::KafkaStorageError,
            57 => Self::LogDirNotFound,
            58 => Self::SaslAuthenticationFailed,
            59 => Self::UnknownProducerId,
            60 => Self::ReassignmentInProgress,
            61 => Self::DelegationTokenAuthDisabled,
            62 => Self::DelegationTokenNotFound,
            63 => Self::DelegationTokenOwnerMismatch,
            64 => Self::DelegationTokenRequestNotAllowed,
            65 => Self::DelegationTokenAuthorizationFailed,
            66 => Self::DelegationTokenExpired,
            67 => Self::InvalidPrincipalType,
            68 => Self::NonEmptyGroup,
            69 => Self::GroupIdNotFound,
            70 => Self::FetchSessionIdNotFound,
            71 => Self::InvalidFetchSessionEpoch,
            72 => Self::ListenerNotFound,
            73 => Self::TopicDeletionDisabled,
            74 => Self::FencedLeaderEpoch,
            75 => Self::UnknownLeaderEpoch,
            76 => Self::UnsupportedCompressionType,
            77 => Self::StaleBrokerEpoch,
            78 => Self::OffsetNotAvailable,
            79 => Self::MemberIdRequired,
            80 => Self::PreferredLeaderNotAvailable,
            81 => Self::GroupMaxSizeReached,
            _ => Self::UnknownServerError,
        }
    }

    /// Human-readable description of this error.
    pub fn description(&self) -> &'static str {
        match self {
            Self::UnknownServerError => {
                "The server experienced an unexpected error when processing the request."
            }
            Self::None => "",
            Self::OffsetOutOfRange => {
                "The requested offset is not within the range of offsets maintained by the server."
            }
            Self::CorruptMessage => {
                "This message has failed its CRC checksum, exceeds the valid size, has a null key for a compacted topic, or is otherwise corrupt."
            }
            Self::UnknownTopicOrPartition => "This server does not host this topic-partition.",
            Self::InvalidFetchSize => "The requested fetch size is invalid.",
            Self::LeaderNotAvailable => {
                "There is no leader for this topic-partition as we are in the middle of a leadership election."
            }
            Self::NotLeaderForPartition => {
                "This server is not the leader for that topic-partition."
            }
            Self::RequestTimedOut => "The request timed out.",
            Self::BrokerNotAvailable => "The broker is not available.",
            Self::ReplicaNotAvailable => {
                "The replica is not available for the requested topic-partition."
            }
            Self::MessageTooLarge => {
                "The request included a message larger than the max message size the server will accept."
            }
            Self::StaleControllerEpoch => "The controller moved to another broker.",
            Self::OffsetMetadataTooLarge => {
                "The metadata field of the offset request was too large."
            }
            Self::NetworkException => "The server disconnected before a response was received.",
            Self::CoordinatorLoadInProgress => {
                "The coordinator is loading and hence can't process requests."
            }
            Self::CoordinatorNotAvailable => "The coordinator is not available.",
            Self::NotCoordinator => "This is not the correct coordinator.",
            Self::InvalidTopicException => {
                "The request attempted to perform an operation on an invalid topic."
            }
            Self::RecordListTooLarge => {
                "The request included message batch larger than the configured segment size on the server."
            }
            Self::NotEnoughReplicas => {
                "Messages are rejected since there are fewer in-sync replicas than required."
            }
            Self::NotEnoughReplicasAfterAppend => {
                "Messages are written to the log, but to fewer in-sync replicas than required."
            }
            Self::InvalidRequiredAcks => {
                "Produce request specified an invalid value for required acks."
            }
            Self::IllegalGeneration => "Specified group generation id is not valid.",
            Self::InconsistentGroupProtocol => {
                "The group member's supported protocols are incompatible with those of existing members or first group member tried to join with empty protocol type or empty protocol list."
            }
            Self::InvalidGroupId => "The configured groupId is invalid.",
            Self::UnknownMemberId => "The coordinator is not aware of this member.",
            Self::InvalidSessionTimeout => {
                "The session timeout is not within the range allowed by the broker."
            }
            Self::RebalanceInProgress => "The group is rebalancing, so a rejoin is needed.",
            Self::InvalidCommitOffsetSize => "The committing offset data size is not valid.",
            Self::TopicAuthorizationFailed => "Topic authorization failed.",
            Self::GroupAuthorizationFailed => "Group authorization failed.",
            Self::ClusterAuthorizationFailed => "Cluster authorization failed.",
            Self::InvalidTimestamp => "The timestamp of the message is out of acceptable range.",
            Self::UnsupportedSaslMechanism => {
                "The broker does not support the requested SASL mechanism."
            }
            Self::IllegalSaslState => "Request is not valid given the current SASL state.",
            Self::UnsupportedVersion => "The version of API is not supported.",
            Self::TopicAlreadyExists => "Topic with this name already exists.",
            Self::InvalidPartitions => "Number of partitions is below 1.",
            Self::InvalidReplicationFactor => {
                "Replication factor is below 1 or larger than the number of available brokers."
            }
            Self::InvalidReplicaAssignment => "Replica assignment is invalid.",
            Self::InvalidConfig => "Configuration is invalid.",
            Self::NotController => "This is not the correct controller for this cluster.",
            Self::InvalidRequest => {
                "This most likely occurs because of a request being malformed by the client library or the message was sent to an incompatible broker."
            }
            Self::UnsupportedForMessageFormat => {
                "The message format version on the broker does not support the request."
            }
            Self::PolicyViolation => "Request parameters do not satisfy the configured policy.",
            Self::OutOfOrderSequenceNumber => {
                "The broker received an out of order sequence number."
            }
            Self::DuplicateSequenceNumber => "The broker received a duplicate sequence number.",
            Self::InvalidProducerEpoch => {
                "Producer attempted an operation with an old epoch. Either there is a newer producer with the same transactionalId, or the producer's transaction has been expired by the broker."
            }
            Self::InvalidTxnState => {
                "The producer attempted a transactional operation in an invalid state."
            }
            Self::InvalidProducerIdMapping => {
                "The producer attempted to use a producer id which is not currently assigned to its transactional id."
            }
            Self::InvalidTransactionTimeout => {
                "The transaction timeout is larger than the maximum value allowed by the broker."
            }
            Self::ConcurrentTransactions => {
                "The producer attempted to update a transaction while another concurrent operation on the same transaction was ongoing."
            }
            Self::TransactionCoordinatorFenced => {
                "Indicates that the transaction coordinator sending a WriteTxnMarker is no longer the current coordinator for a given producer."
            }
            Self::TransactionalIdAuthorizationFailed => "Transactional Id authorization failed.",
            Self::SecurityDisabled => "Security features are disabled.",
            Self::OperationNotAttempted => "The broker did not attempt to execute this operation.",
            Self::KafkaStorageError => "Disk error when trying to access log file on the disk.",
            Self::LogDirNotFound => {
                "The user-specified log directory is not found in the broker config."
            }
            Self::SaslAuthenticationFailed => "SASL Authentication failed.",
            Self::UnknownProducerId => {
                "The broker could not locate the producer metadata associated with the producerId in question."
            }
            Self::ReassignmentInProgress => "A partition reassignment is in progress.",
            Self::DelegationTokenAuthDisabled => "Delegation Token feature is not enabled.",
            Self::DelegationTokenNotFound => "Delegation Token is not found on server.",
            Self::DelegationTokenOwnerMismatch => "Specified Principal is not valid Owner/Renewer.",
            Self::DelegationTokenRequestNotAllowed => {
                "Delegation Token requests are not allowed on PLAINTEXT/1-way SSL channels and on delegation token authenticated channels."
            }
            Self::DelegationTokenAuthorizationFailed => "Delegation Token authorization failed.",
            Self::DelegationTokenExpired => "Delegation Token is expired.",
            Self::InvalidPrincipalType => "Supplied principalType is not supported.",
            Self::NonEmptyGroup => "The group is not empty.",
            Self::GroupIdNotFound => "The group id does not exist.",
            Self::FetchSessionIdNotFound => "The fetch session ID was not found.",
            Self::InvalidFetchSessionEpoch => "The fetch session epoch is invalid.",
            Self::ListenerNotFound => {
                "There is no listener on the leader broker that matches the listener on which metadata request was processed."
            }
            Self::TopicDeletionDisabled => "Topic deletion is disabled.",
            Self::FencedLeaderEpoch => {
                "The leader epoch in the request is older than the epoch on the broker."
            }
            Self::UnknownLeaderEpoch => {
                "The leader epoch in the request is newer than the epoch on the broker."
            }
            Self::UnsupportedCompressionType => {
                "The requesting client does not support the compression type of given partition."
            }
            Self::StaleBrokerEpoch => "Broker epoch has changed.",
            Self::OffsetNotAvailable => {
                "The leader high watermark has not caught up from a recent leader election so the offsets cannot be guaranteed to be monotonically increasing."
            }
            Self::MemberIdRequired => {
                "The group member needs to have a valid member id before actually entering a consumer group."
            }
            Self::PreferredLeaderNotAvailable => "The preferred leader was not available.",
            Self::GroupMaxSizeReached => "The consumer group has reached its max size.",
        }
    }
}

/// Default value for a field - either a string literal or an integer.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum FieldDefault {
    Str(String),
    Int(i64),
}

impl Field {
    /// Create a new field
    pub fn new(name: String, field_type: String, versions: String) -> Self {
        Self {
            name,
            field_type,
            versions,
            nullable_versions: None,
            flexible_versions: None,
            ignorable: false,
            default: None,
            about: None,
            map_key: false,
            entity_type: None,
            tag: None,
            tagged_versions: None,
            zero_copy: false,
            fields: Vec::new(),
        }
    }
}

impl MessageStruct {
    /// Create a new message structure
    pub fn new(
        api_key: Option<i16>,
        message_type: MessageType,
        name: String,
        valid_versions: String,
    ) -> Self {
        Self {
            api_key,
            message_type,
            name,
            valid_versions,
            flexible_versions: None,
            common_structs: Vec::new(),
            fields: Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_struct_creation() {
        let field = Field::new(
            "TestField".to_string(),
            "int32".to_string(),
            "0+".to_string(),
        );
        assert_eq!(field.name, "TestField");
        assert_eq!(field.field_type, "int32");
    }

    #[test]
    fn test_message_struct_creation() {
        let message = MessageStruct::new(
            Some(3),
            MessageType::Request,
            "MetadataRequest".to_string(),
            "0-7".to_string(),
        );
        assert_eq!(message.api_key, Some(3));
        assert_eq!(message.name, "MetadataRequest");
    }

    #[test]
    fn test_error_code_from_code_none() {
        assert_eq!(ErrorCode::from_code(0), ErrorCode::None);
    }

    #[test]
    fn test_error_code_from_code_unknown() {
        assert_eq!(ErrorCode::from_code(-1), ErrorCode::UnknownServerError);
    }

    #[test]
    fn test_error_code_from_code_known() {
        assert_eq!(ErrorCode::from_code(3), ErrorCode::UnknownTopicOrPartition);
        assert_eq!(ErrorCode::from_code(7), ErrorCode::RequestTimedOut);
        assert_eq!(ErrorCode::from_code(81), ErrorCode::GroupMaxSizeReached);
    }

    #[test]
    fn test_error_code_from_code_unknown_code() {
        assert_eq!(ErrorCode::from_code(999), ErrorCode::UnknownServerError);
    }

    #[test]
    fn test_error_code_description() {
        assert_eq!(ErrorCode::None.description(), "");
        assert!(
            ErrorCode::UnknownServerError
                .description()
                .contains("unexpected error")
        );
        assert!(
            ErrorCode::RequestTimedOut
                .description()
                .contains("timed out")
        );
    }

    #[test]
    fn test_error_code_discriminant_values() {
        assert_eq!(ErrorCode::None as i16, 0);
        assert_eq!(ErrorCode::UnknownServerError as i16, -1);
        assert_eq!(ErrorCode::OffsetOutOfRange as i16, 1);
        assert_eq!(ErrorCode::GroupMaxSizeReached as i16, 81);
    }
}

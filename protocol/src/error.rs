//! Kafka protocol error codes.
//!
//! Reproduced from the [Kafka protocol guide](../../docs/04-protocol.md).

use std::fmt;

/// Kafka protocol error code.
///
/// Each error has a numeric code, a canonical name, and a retriable flag.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApiError {
    UnknownServerError = -1,
    OffsetOutOfRange = 1,
    CorruptMessage = 2,
    UnknownTopicOrPartition = 3,
    InvalidFetchSize = 4,
    LeaderNotAvailable = 5,
    NotLeaderOrFollower = 6,
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
    FencedInstanceId = 82,
    EligibleLeadersNotAvailable = 83,
    ElectionNotNeeded = 84,
    NoReassignmentInProgress = 85,
    GroupSubscribedToTopic = 86,
    InvalidRecord = 87,
    UnstableOffsetCommit = 88,
    ThrottlingQuotaExceeded = 89,
    ProducerFenced = 90,
    ResourceNotFound = 91,
    DuplicateResource = 92,
    UnacceptableCredential = 93,
    InconsistentVoterSet = 94,
    InvalidUpdateVersion = 95,
    FeatureUpdateFailed = 96,
    PrincipalDeserializationFailure = 97,
    SnapshotNotFound = 98,
    PositionOutOfRange = 99,
    UnknownTopicId = 100,
    DuplicateBrokerRegistration = 101,
    BrokerIdNotRegistered = 102,
    InconsistentTopicId = 103,
    InconsistentClusterId = 104,
    TransactionalIdNotFound = 105,
    FetchSessionTopicIdError = 106,
    IneligibleReplica = 107,
    NewLeaderElected = 108,
    OffsetMovedToTieredStorage = 109,
    FencedMemberEpoch = 110,
    UnreleasedInstanceId = 111,
    UnsupportedAssignor = 112,
    StaleMemberEpoch = 113,
    MismatchedEndpointType = 114,
    UnsupportedEndpointType = 115,
    UnknownControllerId = 116,
    UnknownSubscriptionId = 117,
    TelemetryTooLarge = 118,
    InvalidRegistration = 119,
    TransactionAbortable = 120,
    InvalidRecordState = 121,
    ShareSessionNotFound = 122,
    InvalidShareSessionEpoch = 123,
    FencedStateEpoch = 124,
    InvalidVoterKey = 125,
    DuplicateVoter = 126,
    VoterNotFound = 127,
    InvalidRegularExpression = 128,
    RebootstrapRequired = 129,
    StreamsInvalidTopology = 130,
    StreamsInvalidTopologyEpoch = 131,
    StreamsTopologyFenced = 132,
    ShareSessionLimitReached = 133,
}

impl ApiError {
    /// Returns the numeric error code as sent on the wire.
    pub fn code(self) -> i16 {
        self as i16
    }

    /// Returns `true` if the error is retriable.
    pub fn is_retriable(self) -> bool {
        matches!(
            self,
            Self::CorruptMessage
                | Self::UnknownTopicOrPartition
                | Self::LeaderNotAvailable
                | Self::NotLeaderOrFollower
                | Self::RequestTimedOut
                | Self::ReplicaNotAvailable
                | Self::NetworkException
                | Self::CoordinatorLoadInProgress
                | Self::CoordinatorNotAvailable
                | Self::NotCoordinator
                | Self::NotEnoughReplicas
                | Self::NotEnoughReplicasAfterAppend
                | Self::NotController
                | Self::KafkaStorageError
                | Self::FetchSessionIdNotFound
                | Self::InvalidFetchSessionEpoch
                | Self::ListenerNotFound
                | Self::FencedLeaderEpoch
                | Self::UnknownLeaderEpoch
                | Self::OffsetNotAvailable
                | Self::PreferredLeaderNotAvailable
                | Self::EligibleLeadersNotAvailable
                | Self::ElectionNotNeeded
                | Self::UnstableOffsetCommit
                | Self::ThrottlingQuotaExceeded
                | Self::UnknownTopicId
                | Self::InconsistentTopicId
                | Self::FetchSessionTopicIdError
                | Self::ShareSessionNotFound
                | Self::InvalidShareSessionEpoch
                | Self::ShareSessionLimitReached
        )
    }

    /// Resolve an `i16` wire code into an `ApiError`, or `None` for code 0.
    pub fn from_code(code: i16) -> Option<Self> {
        match code {
            0 => None,
            -1 => Some(Self::UnknownServerError),
            1 => Some(Self::OffsetOutOfRange),
            2 => Some(Self::CorruptMessage),
            3 => Some(Self::UnknownTopicOrPartition),
            4 => Some(Self::InvalidFetchSize),
            5 => Some(Self::LeaderNotAvailable),
            6 => Some(Self::NotLeaderOrFollower),
            7 => Some(Self::RequestTimedOut),
            8 => Some(Self::BrokerNotAvailable),
            9 => Some(Self::ReplicaNotAvailable),
            10 => Some(Self::MessageTooLarge),
            11 => Some(Self::StaleControllerEpoch),
            12 => Some(Self::OffsetMetadataTooLarge),
            13 => Some(Self::NetworkException),
            14 => Some(Self::CoordinatorLoadInProgress),
            15 => Some(Self::CoordinatorNotAvailable),
            16 => Some(Self::NotCoordinator),
            17 => Some(Self::InvalidTopicException),
            18 => Some(Self::RecordListTooLarge),
            19 => Some(Self::NotEnoughReplicas),
            20 => Some(Self::NotEnoughReplicasAfterAppend),
            21 => Some(Self::InvalidRequiredAcks),
            22 => Some(Self::IllegalGeneration),
            23 => Some(Self::InconsistentGroupProtocol),
            24 => Some(Self::InvalidGroupId),
            25 => Some(Self::UnknownMemberId),
            26 => Some(Self::InvalidSessionTimeout),
            27 => Some(Self::RebalanceInProgress),
            28 => Some(Self::InvalidCommitOffsetSize),
            29 => Some(Self::TopicAuthorizationFailed),
            30 => Some(Self::GroupAuthorizationFailed),
            31 => Some(Self::ClusterAuthorizationFailed),
            32 => Some(Self::InvalidTimestamp),
            33 => Some(Self::UnsupportedSaslMechanism),
            34 => Some(Self::IllegalSaslState),
            35 => Some(Self::UnsupportedVersion),
            36 => Some(Self::TopicAlreadyExists),
            37 => Some(Self::InvalidPartitions),
            38 => Some(Self::InvalidReplicationFactor),
            39 => Some(Self::InvalidReplicaAssignment),
            40 => Some(Self::InvalidConfig),
            41 => Some(Self::NotController),
            42 => Some(Self::InvalidRequest),
            43 => Some(Self::UnsupportedForMessageFormat),
            44 => Some(Self::PolicyViolation),
            45 => Some(Self::OutOfOrderSequenceNumber),
            46 => Some(Self::DuplicateSequenceNumber),
            47 => Some(Self::InvalidProducerEpoch),
            48 => Some(Self::InvalidTxnState),
            49 => Some(Self::InvalidProducerIdMapping),
            50 => Some(Self::InvalidTransactionTimeout),
            51 => Some(Self::ConcurrentTransactions),
            52 => Some(Self::TransactionCoordinatorFenced),
            53 => Some(Self::TransactionalIdAuthorizationFailed),
            54 => Some(Self::SecurityDisabled),
            55 => Some(Self::OperationNotAttempted),
            56 => Some(Self::KafkaStorageError),
            57 => Some(Self::LogDirNotFound),
            58 => Some(Self::SaslAuthenticationFailed),
            59 => Some(Self::UnknownProducerId),
            60 => Some(Self::ReassignmentInProgress),
            61 => Some(Self::DelegationTokenAuthDisabled),
            62 => Some(Self::DelegationTokenNotFound),
            63 => Some(Self::DelegationTokenOwnerMismatch),
            64 => Some(Self::DelegationTokenRequestNotAllowed),
            65 => Some(Self::DelegationTokenAuthorizationFailed),
            66 => Some(Self::DelegationTokenExpired),
            67 => Some(Self::InvalidPrincipalType),
            68 => Some(Self::NonEmptyGroup),
            69 => Some(Self::GroupIdNotFound),
            70 => Some(Self::FetchSessionIdNotFound),
            71 => Some(Self::InvalidFetchSessionEpoch),
            72 => Some(Self::ListenerNotFound),
            73 => Some(Self::TopicDeletionDisabled),
            74 => Some(Self::FencedLeaderEpoch),
            75 => Some(Self::UnknownLeaderEpoch),
            76 => Some(Self::UnsupportedCompressionType),
            77 => Some(Self::StaleBrokerEpoch),
            78 => Some(Self::OffsetNotAvailable),
            79 => Some(Self::MemberIdRequired),
            80 => Some(Self::PreferredLeaderNotAvailable),
            81 => Some(Self::GroupMaxSizeReached),
            82 => Some(Self::FencedInstanceId),
            83 => Some(Self::EligibleLeadersNotAvailable),
            84 => Some(Self::ElectionNotNeeded),
            85 => Some(Self::NoReassignmentInProgress),
            86 => Some(Self::GroupSubscribedToTopic),
            87 => Some(Self::InvalidRecord),
            88 => Some(Self::UnstableOffsetCommit),
            89 => Some(Self::ThrottlingQuotaExceeded),
            90 => Some(Self::ProducerFenced),
            91 => Some(Self::ResourceNotFound),
            92 => Some(Self::DuplicateResource),
            93 => Some(Self::UnacceptableCredential),
            94 => Some(Self::InconsistentVoterSet),
            95 => Some(Self::InvalidUpdateVersion),
            96 => Some(Self::FeatureUpdateFailed),
            97 => Some(Self::PrincipalDeserializationFailure),
            98 => Some(Self::SnapshotNotFound),
            99 => Some(Self::PositionOutOfRange),
            100 => Some(Self::UnknownTopicId),
            101 => Some(Self::DuplicateBrokerRegistration),
            102 => Some(Self::BrokerIdNotRegistered),
            103 => Some(Self::InconsistentTopicId),
            104 => Some(Self::InconsistentClusterId),
            105 => Some(Self::TransactionalIdNotFound),
            106 => Some(Self::FetchSessionTopicIdError),
            107 => Some(Self::IneligibleReplica),
            108 => Some(Self::NewLeaderElected),
            109 => Some(Self::OffsetMovedToTieredStorage),
            110 => Some(Self::FencedMemberEpoch),
            111 => Some(Self::UnreleasedInstanceId),
            112 => Some(Self::UnsupportedAssignor),
            113 => Some(Self::StaleMemberEpoch),
            114 => Some(Self::MismatchedEndpointType),
            115 => Some(Self::UnsupportedEndpointType),
            116 => Some(Self::UnknownControllerId),
            117 => Some(Self::UnknownSubscriptionId),
            118 => Some(Self::TelemetryTooLarge),
            119 => Some(Self::InvalidRegistration),
            120 => Some(Self::TransactionAbortable),
            121 => Some(Self::InvalidRecordState),
            122 => Some(Self::ShareSessionNotFound),
            123 => Some(Self::InvalidShareSessionEpoch),
            124 => Some(Self::FencedStateEpoch),
            125 => Some(Self::InvalidVoterKey),
            126 => Some(Self::DuplicateVoter),
            127 => Some(Self::VoterNotFound),
            128 => Some(Self::InvalidRegularExpression),
            129 => Some(Self::RebootstrapRequired),
            130 => Some(Self::StreamsInvalidTopology),
            131 => Some(Self::StreamsInvalidTopologyEpoch),
            132 => Some(Self::StreamsTopologyFenced),
            133 => Some(Self::ShareSessionLimitReached),
            _ => Some(Self::UnknownServerError),
        }
    }
}

impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::UnknownServerError => "UNKNOWN_SERVER_ERROR",
            Self::OffsetOutOfRange => "OFFSET_OUT_OF_RANGE",
            Self::CorruptMessage => "CORRUPT_MESSAGE",
            Self::UnknownTopicOrPartition => "UNKNOWN_TOPIC_OR_PARTITION",
            Self::InvalidFetchSize => "INVALID_FETCH_SIZE",
            Self::LeaderNotAvailable => "LEADER_NOT_AVAILABLE",
            Self::NotLeaderOrFollower => "NOT_LEADER_OR_FOLLOWER",
            Self::RequestTimedOut => "REQUEST_TIMED_OUT",
            Self::BrokerNotAvailable => "BROKER_NOT_AVAILABLE",
            Self::ReplicaNotAvailable => "REPLICA_NOT_AVAILABLE",
            Self::MessageTooLarge => "MESSAGE_TOO_LARGE",
            Self::StaleControllerEpoch => "STALE_CONTROLLER_EPOCH",
            Self::OffsetMetadataTooLarge => "OFFSET_METADATA_TOO_LARGE",
            Self::NetworkException => "NETWORK_EXCEPTION",
            Self::CoordinatorLoadInProgress => "COORDINATOR_LOAD_IN_PROGRESS",
            Self::CoordinatorNotAvailable => "COORDINATOR_NOT_AVAILABLE",
            Self::NotCoordinator => "NOT_COORDINATOR",
            Self::InvalidTopicException => "INVALID_TOPIC_EXCEPTION",
            Self::RecordListTooLarge => "RECORD_LIST_TOO_LARGE",
            Self::NotEnoughReplicas => "NOT_ENOUGH_REPLICAS",
            Self::NotEnoughReplicasAfterAppend => "NOT_ENOUGH_REPLICAS_AFTER_APPEND",
            Self::InvalidRequiredAcks => "INVALID_REQUIRED_ACKS",
            Self::IllegalGeneration => "ILLEGAL_GENERATION",
            Self::InconsistentGroupProtocol => "INCONSISTENT_GROUP_PROTOCOL",
            Self::InvalidGroupId => "INVALID_GROUP_ID",
            Self::UnknownMemberId => "UNKNOWN_MEMBER_ID",
            Self::InvalidSessionTimeout => "INVALID_SESSION_TIMEOUT",
            Self::RebalanceInProgress => "REBALANCE_IN_PROGRESS",
            Self::InvalidCommitOffsetSize => "INVALID_COMMIT_OFFSET_SIZE",
            Self::TopicAuthorizationFailed => "TOPIC_AUTHORIZATION_FAILED",
            Self::GroupAuthorizationFailed => "GROUP_AUTHORIZATION_FAILED",
            Self::ClusterAuthorizationFailed => "CLUSTER_AUTHORIZATION_FAILED",
            Self::InvalidTimestamp => "INVALID_TIMESTAMP",
            Self::UnsupportedSaslMechanism => "UNSUPPORTED_SASL_MECHANISM",
            Self::IllegalSaslState => "ILLEGAL_SASL_STATE",
            Self::UnsupportedVersion => "UNSUPPORTED_VERSION",
            Self::TopicAlreadyExists => "TOPIC_ALREADY_EXISTS",
            Self::InvalidPartitions => "INVALID_PARTITIONS",
            Self::InvalidReplicationFactor => "INVALID_REPLICATION_FACTOR",
            Self::InvalidReplicaAssignment => "INVALID_REPLICA_ASSIGNMENT",
            Self::InvalidConfig => "INVALID_CONFIG",
            Self::NotController => "NOT_CONTROLLER",
            Self::InvalidRequest => "INVALID_REQUEST",
            Self::UnsupportedForMessageFormat => "UNSUPPORTED_FOR_MESSAGE_FORMAT",
            Self::PolicyViolation => "POLICY_VIOLATION",
            Self::OutOfOrderSequenceNumber => "OUT_OF_ORDER_SEQUENCE_NUMBER",
            Self::DuplicateSequenceNumber => "DUPLICATE_SEQUENCE_NUMBER",
            Self::InvalidProducerEpoch => "INVALID_PRODUCER_EPOCH",
            Self::InvalidTxnState => "INVALID_TXN_STATE",
            Self::InvalidProducerIdMapping => "INVALID_PRODUCER_ID_MAPPING",
            Self::InvalidTransactionTimeout => "INVALID_TRANSACTION_TIMEOUT",
            Self::ConcurrentTransactions => "CONCURRENT_TRANSACTIONS",
            Self::TransactionCoordinatorFenced => "TRANSACTION_COORDINATOR_FENCED",
            Self::TransactionalIdAuthorizationFailed => "TRANSACTIONAL_ID_AUTHORIZATION_FAILED",
            Self::SecurityDisabled => "SECURITY_DISABLED",
            Self::OperationNotAttempted => "OPERATION_NOT_ATTEMPTED",
            Self::KafkaStorageError => "KAFKA_STORAGE_ERROR",
            Self::LogDirNotFound => "LOG_DIR_NOT_FOUND",
            Self::SaslAuthenticationFailed => "SASL_AUTHENTICATION_FAILED",
            Self::UnknownProducerId => "UNKNOWN_PRODUCER_ID",
            Self::ReassignmentInProgress => "REASSIGNMENT_IN_PROGRESS",
            Self::DelegationTokenAuthDisabled => "DELEGATION_TOKEN_AUTH_DISABLED",
            Self::DelegationTokenNotFound => "DELEGATION_TOKEN_NOT_FOUND",
            Self::DelegationTokenOwnerMismatch => "DELEGATION_TOKEN_OWNER_MISMATCH",
            Self::DelegationTokenRequestNotAllowed => "DELEGATION_TOKEN_REQUEST_NOT_ALLOWED",
            Self::DelegationTokenAuthorizationFailed => "DELEGATION_TOKEN_AUTHORIZATION_FAILED",
            Self::DelegationTokenExpired => "DELEGATION_TOKEN_EXPIRED",
            Self::InvalidPrincipalType => "INVALID_PRINCIPAL_TYPE",
            Self::NonEmptyGroup => "NON_EMPTY_GROUP",
            Self::GroupIdNotFound => "GROUP_ID_NOT_FOUND",
            Self::FetchSessionIdNotFound => "FETCH_SESSION_ID_NOT_FOUND",
            Self::InvalidFetchSessionEpoch => "INVALID_FETCH_SESSION_EPOCH",
            Self::ListenerNotFound => "LISTENER_NOT_FOUND",
            Self::TopicDeletionDisabled => "TOPIC_DELETION_DISABLED",
            Self::FencedLeaderEpoch => "FENCED_LEADER_EPOCH",
            Self::UnknownLeaderEpoch => "UNKNOWN_LEADER_EPOCH",
            Self::UnsupportedCompressionType => "UNSUPPORTED_COMPRESSION_TYPE",
            Self::StaleBrokerEpoch => "STALE_BROKER_EPOCH",
            Self::OffsetNotAvailable => "OFFSET_NOT_AVAILABLE",
            Self::MemberIdRequired => "MEMBER_ID_REQUIRED",
            Self::PreferredLeaderNotAvailable => "PREFERRED_LEADER_NOT_AVAILABLE",
            Self::GroupMaxSizeReached => "GROUP_MAX_SIZE_REACHED",
            Self::FencedInstanceId => "FENCED_INSTANCE_ID",
            Self::EligibleLeadersNotAvailable => "ELIGIBLE_LEADERS_NOT_AVAILABLE",
            Self::ElectionNotNeeded => "ELECTION_NOT_NEEDED",
            Self::NoReassignmentInProgress => "NO_REASSIGNMENT_IN_PROGRESS",
            Self::GroupSubscribedToTopic => "GROUP_SUBSCRIBED_TO_TOPIC",
            Self::InvalidRecord => "INVALID_RECORD",
            Self::UnstableOffsetCommit => "UNSTABLE_OFFSET_COMMIT",
            Self::ThrottlingQuotaExceeded => "THROTTLING_QUOTA_EXCEEDED",
            Self::ProducerFenced => "PRODUCER_FENCED",
            Self::ResourceNotFound => "RESOURCE_NOT_FOUND",
            Self::DuplicateResource => "DUPLICATE_RESOURCE",
            Self::UnacceptableCredential => "UNACCEPTABLE_CREDENTIAL",
            Self::InconsistentVoterSet => "INCONSISTENT_VOTER_SET",
            Self::InvalidUpdateVersion => "INVALID_UPDATE_VERSION",
            Self::FeatureUpdateFailed => "FEATURE_UPDATE_FAILED",
            Self::PrincipalDeserializationFailure => "PRINCIPAL_DESERIALIZATION_FAILURE",
            Self::SnapshotNotFound => "SNAPSHOT_NOT_FOUND",
            Self::PositionOutOfRange => "POSITION_OUT_OF_RANGE",
            Self::UnknownTopicId => "UNKNOWN_TOPIC_ID",
            Self::DuplicateBrokerRegistration => "DUPLICATE_BROKER_REGISTRATION",
            Self::BrokerIdNotRegistered => "BROKER_ID_NOT_REGISTERED",
            Self::InconsistentTopicId => "INCONSISTENT_TOPIC_ID",
            Self::InconsistentClusterId => "INCONSISTENT_CLUSTER_ID",
            Self::TransactionalIdNotFound => "TRANSACTIONAL_ID_NOT_FOUND",
            Self::FetchSessionTopicIdError => "FETCH_SESSION_TOPIC_ID_ERROR",
            Self::IneligibleReplica => "INELIGIBLE_REPLICA",
            Self::NewLeaderElected => "NEW_LEADER_ELECTED",
            Self::OffsetMovedToTieredStorage => "OFFSET_MOVED_TO_TIERED_STORAGE",
            Self::FencedMemberEpoch => "FENCED_MEMBER_EPOCH",
            Self::UnreleasedInstanceId => "UNRELEASED_INSTANCE_ID",
            Self::UnsupportedAssignor => "UNSUPPORTED_ASSIGNOR",
            Self::StaleMemberEpoch => "STALE_MEMBER_EPOCH",
            Self::MismatchedEndpointType => "MISMATCHED_ENDPOINT_TYPE",
            Self::UnsupportedEndpointType => "UNSUPPORTED_ENDPOINT_TYPE",
            Self::UnknownControllerId => "UNKNOWN_CONTROLLER_ID",
            Self::UnknownSubscriptionId => "UNKNOWN_SUBSCRIPTION_ID",
            Self::TelemetryTooLarge => "TELEMETRY_TOO_LARGE",
            Self::InvalidRegistration => "INVALID_REGISTRATION",
            Self::TransactionAbortable => "TRANSACTION_ABORTABLE",
            Self::InvalidRecordState => "INVALID_RECORD_STATE",
            Self::ShareSessionNotFound => "SHARE_SESSION_NOT_FOUND",
            Self::InvalidShareSessionEpoch => "INVALID_SHARE_SESSION_EPOCH",
            Self::FencedStateEpoch => "FENCED_STATE_EPOCH",
            Self::InvalidVoterKey => "INVALID_VOTER_KEY",
            Self::DuplicateVoter => "DUPLICATE_VOTER",
            Self::VoterNotFound => "VOTER_NOT_FOUND",
            Self::InvalidRegularExpression => "INVALID_REGULAR_EXPRESSION",
            Self::RebootstrapRequired => "REBOOTSTRAP_REQUIRED",
            Self::StreamsInvalidTopology => "STREAMS_INVALID_TOPOLOGY",
            Self::StreamsInvalidTopologyEpoch => "STREAMS_INVALID_TOPOLOGY_EPOCH",
            Self::StreamsTopologyFenced => "STREAMS_TOPOLOGY_FENCED",
            Self::ShareSessionLimitReached => "SHARE_SESSION_LIMIT_REACHED",
        };
        write!(f, "{name}({})", self.code())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip() {
        for code in -1..=133 {
            if code == 0 {
                assert!(ApiError::from_code(code).is_none());
            } else {
                let err = ApiError::from_code(code).unwrap();
                assert_eq!(err.code(), code, "mismatch for code {code}");
            }
        }
    }

    #[test]
    fn test_unknown_code_falls_back() {
        assert_eq!(ApiError::from_code(999).unwrap().code(), -1);
    }

    #[test]
    fn test_success() {
        assert!(ApiError::from_code(0).is_none());
    }

    #[test]
    fn test_retriable() {
        assert!(ApiError::CorruptMessage.is_retriable());
        assert!(!ApiError::UnknownServerError.is_retriable());
    }
}

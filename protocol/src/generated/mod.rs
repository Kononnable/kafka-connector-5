//! Auto-generated Kafka protocol structs.
//!
//! **Do not edit by hand.** Regenerate by running:
//! ```text
//! cargo test test_codegen_generated_structs -- --nocapture
//! UPDATE_EXPECT=1 cargo test test_codegen_generated_structs -- --nocapture
//! ```

pub mod add_offsets_to_txn_request;
pub mod add_offsets_to_txn_response;
pub mod add_partitions_to_txn_request;
pub mod add_partitions_to_txn_response;
pub mod add_raft_voter_request;
pub mod add_raft_voter_response;
pub mod allocate_producer_ids_request;
pub mod allocate_producer_ids_response;
pub mod alter_client_quotas_request;
pub mod alter_client_quotas_response;
pub mod alter_configs_request;
pub mod alter_configs_response;
pub mod alter_partition_reassignments_request;
pub mod alter_partition_reassignments_response;
pub mod alter_partition_request;
pub mod alter_partition_response;
pub mod alter_replica_log_dirs_request;
pub mod alter_replica_log_dirs_response;
pub mod alter_share_group_offsets_request;
pub mod alter_share_group_offsets_response;
pub mod alter_user_scram_credentials_request;
pub mod alter_user_scram_credentials_response;
pub mod api_versions_request;
pub mod api_versions_response;
pub mod assign_replicas_to_dirs_request;
pub mod assign_replicas_to_dirs_response;
pub mod begin_quorum_epoch_request;
pub mod begin_quorum_epoch_response;
pub mod broker_heartbeat_request;
pub mod broker_heartbeat_response;
pub mod broker_registration_request;
pub mod broker_registration_response;
pub mod consumer_group_describe_request;
pub mod consumer_group_describe_response;
pub mod consumer_group_heartbeat_request;
pub mod consumer_group_heartbeat_response;
pub mod consumer_protocol_assignment;
pub mod consumer_protocol_subscription;
pub mod controlled_shutdown_request;
pub mod controlled_shutdown_response;
pub mod controller_registration_request;
pub mod controller_registration_response;
pub mod create_acls_request;
pub mod create_acls_response;
pub mod create_delegation_token_request;
pub mod create_delegation_token_response;
pub mod create_partitions_request;
pub mod create_partitions_response;
pub mod create_topics_request;
pub mod create_topics_response;
pub mod default_principal_data;
pub mod delete_acls_request;
pub mod delete_acls_response;
pub mod delete_groups_request;
pub mod delete_groups_response;
pub mod delete_records_request;
pub mod delete_records_response;
pub mod delete_share_group_offsets_request;
pub mod delete_share_group_offsets_response;
pub mod delete_share_group_state_request;
pub mod delete_share_group_state_response;
pub mod delete_topics_request;
pub mod delete_topics_response;
pub mod describe_acls_request;
pub mod describe_acls_response;
pub mod describe_client_quotas_request;
pub mod describe_client_quotas_response;
pub mod describe_cluster_request;
pub mod describe_cluster_response;
pub mod describe_configs_request;
pub mod describe_configs_response;
pub mod describe_delegation_token_request;
pub mod describe_delegation_token_response;
pub mod describe_groups_request;
pub mod describe_groups_response;
pub mod describe_log_dirs_request;
pub mod describe_log_dirs_response;
pub mod describe_producers_request;
pub mod describe_producers_response;
pub mod describe_quorum_request;
pub mod describe_quorum_response;
pub mod describe_share_group_offsets_request;
pub mod describe_share_group_offsets_response;
pub mod describe_topic_partitions_request;
pub mod describe_topic_partitions_response;
pub mod describe_transactions_request;
pub mod describe_transactions_response;
pub mod describe_user_scram_credentials_request;
pub mod describe_user_scram_credentials_response;
pub mod elect_leaders_request;
pub mod elect_leaders_response;
pub mod end_quorum_epoch_request;
pub mod end_quorum_epoch_response;
pub mod end_txn_marker;
pub mod end_txn_request;
pub mod end_txn_response;
pub mod envelope_request;
pub mod envelope_response;
pub mod expire_delegation_token_request;
pub mod expire_delegation_token_response;
pub mod fetch_request;
pub mod fetch_response;
pub mod fetch_snapshot_request;
pub mod fetch_snapshot_response;
pub mod find_coordinator_request;
pub mod find_coordinator_response;
pub mod get_telemetry_subscriptions_request;
pub mod get_telemetry_subscriptions_response;
pub mod heartbeat_request;
pub mod heartbeat_response;
pub mod incremental_alter_configs_request;
pub mod incremental_alter_configs_response;
pub mod init_producer_id_request;
pub mod init_producer_id_response;
pub mod initialize_share_group_state_request;
pub mod initialize_share_group_state_response;
pub mod join_group_request;
pub mod join_group_response;
pub mod k_raft_version_record;
pub mod leader_and_isr_request;
pub mod leader_and_isr_response;
pub mod leader_change_message;
pub mod leave_group_request;
pub mod leave_group_response;
pub mod list_config_resources_request;
pub mod list_config_resources_response;
pub mod list_groups_request;
pub mod list_groups_response;
pub mod list_offsets_request;
pub mod list_offsets_response;
pub mod list_partition_reassignments_request;
pub mod list_partition_reassignments_response;
pub mod list_transactions_request;
pub mod list_transactions_response;
pub mod metadata_request;
pub mod metadata_response;
pub mod offset_commit_request;
pub mod offset_commit_response;
pub mod offset_delete_request;
pub mod offset_delete_response;
pub mod offset_fetch_request;
pub mod offset_fetch_response;
pub mod offset_for_leader_epoch_request;
pub mod offset_for_leader_epoch_response;
pub mod produce_request;
pub mod produce_response;
pub mod push_telemetry_request;
pub mod push_telemetry_response;
pub mod read_share_group_state_request;
pub mod read_share_group_state_response;
pub mod read_share_group_state_summary_request;
pub mod read_share_group_state_summary_response;
pub mod remove_raft_voter_request;
pub mod remove_raft_voter_response;
pub mod renew_delegation_token_request;
pub mod renew_delegation_token_response;
pub mod request_header;
pub mod response_header;
pub mod sasl_authenticate_request;
pub mod sasl_authenticate_response;
pub mod sasl_handshake_request;
pub mod sasl_handshake_response;
pub mod share_acknowledge_request;
pub mod share_acknowledge_response;
pub mod share_fetch_request;
pub mod share_fetch_response;
pub mod share_group_describe_request;
pub mod share_group_describe_response;
pub mod share_group_heartbeat_request;
pub mod share_group_heartbeat_response;
pub mod snapshot_footer_record;
pub mod snapshot_header_record;
pub mod stop_replica_request;
pub mod stop_replica_response;
pub mod streams_group_describe_request;
pub mod streams_group_describe_response;
pub mod streams_group_heartbeat_request;
pub mod streams_group_heartbeat_response;
pub mod sync_group_request;
pub mod sync_group_response;
pub mod txn_offset_commit_request;
pub mod txn_offset_commit_response;
pub mod unregister_broker_request;
pub mod unregister_broker_response;
pub mod update_features_request;
pub mod update_features_response;
pub mod update_metadata_request;
pub mod update_metadata_response;
pub mod update_raft_voter_request;
pub mod update_raft_voter_response;
pub mod vote_request;
pub mod vote_response;
pub mod voters_record;
pub mod write_share_group_state_request;
pub mod write_share_group_state_response;
pub mod write_txn_markers_request;
pub mod write_txn_markers_response;

// Re-exports
pub use add_offsets_to_txn_request::AddOffsetsToTxnRequest;
pub use add_offsets_to_txn_response::AddOffsetsToTxnResponse;
pub use add_partitions_to_txn_request::AddPartitionsToTxnRequest;
pub use add_partitions_to_txn_response::AddPartitionsToTxnResponse;
pub use add_raft_voter_request::AddRaftVoterRequest;
pub use add_raft_voter_response::AddRaftVoterResponse;
pub use allocate_producer_ids_request::AllocateProducerIdsRequest;
pub use allocate_producer_ids_response::AllocateProducerIdsResponse;
pub use alter_client_quotas_request::AlterClientQuotasRequest;
pub use alter_client_quotas_response::AlterClientQuotasResponse;
pub use alter_configs_request::AlterConfigsRequest;
pub use alter_configs_response::AlterConfigsResponse;
pub use alter_partition_reassignments_request::AlterPartitionReassignmentsRequest;
pub use alter_partition_reassignments_response::AlterPartitionReassignmentsResponse;
pub use alter_partition_request::AlterPartitionRequest;
pub use alter_partition_response::AlterPartitionResponse;
pub use alter_replica_log_dirs_request::AlterReplicaLogDirsRequest;
pub use alter_replica_log_dirs_response::AlterReplicaLogDirsResponse;
pub use alter_share_group_offsets_request::AlterShareGroupOffsetsRequest;
pub use alter_share_group_offsets_response::AlterShareGroupOffsetsResponse;
pub use alter_user_scram_credentials_request::AlterUserScramCredentialsRequest;
pub use alter_user_scram_credentials_response::AlterUserScramCredentialsResponse;
pub use api_versions_request::ApiVersionsRequest;
pub use api_versions_response::ApiVersionsResponse;
pub use assign_replicas_to_dirs_request::AssignReplicasToDirsRequest;
pub use assign_replicas_to_dirs_response::AssignReplicasToDirsResponse;
pub use begin_quorum_epoch_request::BeginQuorumEpochRequest;
pub use begin_quorum_epoch_response::BeginQuorumEpochResponse;
pub use broker_heartbeat_request::BrokerHeartbeatRequest;
pub use broker_heartbeat_response::BrokerHeartbeatResponse;
pub use broker_registration_request::BrokerRegistrationRequest;
pub use broker_registration_response::BrokerRegistrationResponse;
pub use consumer_group_describe_request::ConsumerGroupDescribeRequest;
pub use consumer_group_describe_response::ConsumerGroupDescribeResponse;
pub use consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest;
pub use consumer_group_heartbeat_response::ConsumerGroupHeartbeatResponse;
pub use consumer_protocol_assignment::ConsumerProtocolAssignment;
pub use consumer_protocol_subscription::ConsumerProtocolSubscription;
pub use controlled_shutdown_request::ControlledShutdownRequest;
pub use controlled_shutdown_response::ControlledShutdownResponse;
pub use controller_registration_request::ControllerRegistrationRequest;
pub use controller_registration_response::ControllerRegistrationResponse;
pub use create_acls_request::CreateAclsRequest;
pub use create_acls_response::CreateAclsResponse;
pub use create_delegation_token_request::CreateDelegationTokenRequest;
pub use create_delegation_token_response::CreateDelegationTokenResponse;
pub use create_partitions_request::CreatePartitionsRequest;
pub use create_partitions_response::CreatePartitionsResponse;
pub use create_topics_request::CreateTopicsRequest;
pub use create_topics_response::CreateTopicsResponse;
pub use default_principal_data::DefaultPrincipalData;
pub use delete_acls_request::DeleteAclsRequest;
pub use delete_acls_response::DeleteAclsResponse;
pub use delete_groups_request::DeleteGroupsRequest;
pub use delete_groups_response::DeleteGroupsResponse;
pub use delete_records_request::DeleteRecordsRequest;
pub use delete_records_response::DeleteRecordsResponse;
pub use delete_share_group_offsets_request::DeleteShareGroupOffsetsRequest;
pub use delete_share_group_offsets_response::DeleteShareGroupOffsetsResponse;
pub use delete_share_group_state_request::DeleteShareGroupStateRequest;
pub use delete_share_group_state_response::DeleteShareGroupStateResponse;
pub use delete_topics_request::DeleteTopicsRequest;
pub use delete_topics_response::DeleteTopicsResponse;
pub use describe_acls_request::DescribeAclsRequest;
pub use describe_acls_response::DescribeAclsResponse;
pub use describe_client_quotas_request::DescribeClientQuotasRequest;
pub use describe_client_quotas_response::DescribeClientQuotasResponse;
pub use describe_cluster_request::DescribeClusterRequest;
pub use describe_cluster_response::DescribeClusterResponse;
pub use describe_configs_request::DescribeConfigsRequest;
pub use describe_configs_response::DescribeConfigsResponse;
pub use describe_delegation_token_request::DescribeDelegationTokenRequest;
pub use describe_delegation_token_response::DescribeDelegationTokenResponse;
pub use describe_groups_request::DescribeGroupsRequest;
pub use describe_groups_response::DescribeGroupsResponse;
pub use describe_log_dirs_request::DescribeLogDirsRequest;
pub use describe_log_dirs_response::DescribeLogDirsResponse;
pub use describe_producers_request::DescribeProducersRequest;
pub use describe_producers_response::DescribeProducersResponse;
pub use describe_quorum_request::DescribeQuorumRequest;
pub use describe_quorum_response::DescribeQuorumResponse;
pub use describe_share_group_offsets_request::DescribeShareGroupOffsetsRequest;
pub use describe_share_group_offsets_response::DescribeShareGroupOffsetsResponse;
pub use describe_topic_partitions_request::DescribeTopicPartitionsRequest;
pub use describe_topic_partitions_response::DescribeTopicPartitionsResponse;
pub use describe_transactions_request::DescribeTransactionsRequest;
pub use describe_transactions_response::DescribeTransactionsResponse;
pub use describe_user_scram_credentials_request::DescribeUserScramCredentialsRequest;
pub use describe_user_scram_credentials_response::DescribeUserScramCredentialsResponse;
pub use elect_leaders_request::ElectLeadersRequest;
pub use elect_leaders_response::ElectLeadersResponse;
pub use end_quorum_epoch_request::EndQuorumEpochRequest;
pub use end_quorum_epoch_response::EndQuorumEpochResponse;
pub use end_txn_marker::EndTxnMarker;
pub use end_txn_request::EndTxnRequest;
pub use end_txn_response::EndTxnResponse;
pub use envelope_request::EnvelopeRequest;
pub use envelope_response::EnvelopeResponse;
pub use expire_delegation_token_request::ExpireDelegationTokenRequest;
pub use expire_delegation_token_response::ExpireDelegationTokenResponse;
pub use fetch_request::FetchRequest;
pub use fetch_response::FetchResponse;
pub use fetch_snapshot_request::FetchSnapshotRequest;
pub use fetch_snapshot_response::FetchSnapshotResponse;
pub use find_coordinator_request::FindCoordinatorRequest;
pub use find_coordinator_response::FindCoordinatorResponse;
pub use get_telemetry_subscriptions_request::GetTelemetrySubscriptionsRequest;
pub use get_telemetry_subscriptions_response::GetTelemetrySubscriptionsResponse;
pub use heartbeat_request::HeartbeatRequest;
pub use heartbeat_response::HeartbeatResponse;
pub use incremental_alter_configs_request::IncrementalAlterConfigsRequest;
pub use incremental_alter_configs_response::IncrementalAlterConfigsResponse;
pub use init_producer_id_request::InitProducerIdRequest;
pub use init_producer_id_response::InitProducerIdResponse;
pub use initialize_share_group_state_request::InitializeShareGroupStateRequest;
pub use initialize_share_group_state_response::InitializeShareGroupStateResponse;
pub use join_group_request::JoinGroupRequest;
pub use join_group_response::JoinGroupResponse;
pub use k_raft_version_record::KRaftVersionRecord;
pub use leader_and_isr_request::LeaderAndIsrRequest;
pub use leader_and_isr_response::LeaderAndIsrResponse;
pub use leader_change_message::LeaderChangeMessage;
pub use leave_group_request::LeaveGroupRequest;
pub use leave_group_response::LeaveGroupResponse;
pub use list_config_resources_request::ListConfigResourcesRequest;
pub use list_config_resources_response::ListConfigResourcesResponse;
pub use list_groups_request::ListGroupsRequest;
pub use list_groups_response::ListGroupsResponse;
pub use list_offsets_request::ListOffsetsRequest;
pub use list_offsets_response::ListOffsetsResponse;
pub use list_partition_reassignments_request::ListPartitionReassignmentsRequest;
pub use list_partition_reassignments_response::ListPartitionReassignmentsResponse;
pub use list_transactions_request::ListTransactionsRequest;
pub use list_transactions_response::ListTransactionsResponse;
pub use metadata_request::MetadataRequest;
pub use metadata_response::MetadataResponse;
pub use offset_commit_request::OffsetCommitRequest;
pub use offset_commit_response::OffsetCommitResponse;
pub use offset_delete_request::OffsetDeleteRequest;
pub use offset_delete_response::OffsetDeleteResponse;
pub use offset_fetch_request::OffsetFetchRequest;
pub use offset_fetch_response::OffsetFetchResponse;
pub use offset_for_leader_epoch_request::OffsetForLeaderEpochRequest;
pub use offset_for_leader_epoch_response::OffsetForLeaderEpochResponse;
pub use produce_request::ProduceRequest;
pub use produce_response::ProduceResponse;
pub use push_telemetry_request::PushTelemetryRequest;
pub use push_telemetry_response::PushTelemetryResponse;
pub use read_share_group_state_request::ReadShareGroupStateRequest;
pub use read_share_group_state_response::ReadShareGroupStateResponse;
pub use read_share_group_state_summary_request::ReadShareGroupStateSummaryRequest;
pub use read_share_group_state_summary_response::ReadShareGroupStateSummaryResponse;
pub use remove_raft_voter_request::RemoveRaftVoterRequest;
pub use remove_raft_voter_response::RemoveRaftVoterResponse;
pub use renew_delegation_token_request::RenewDelegationTokenRequest;
pub use renew_delegation_token_response::RenewDelegationTokenResponse;
pub use request_header::RequestHeader;
pub use response_header::ResponseHeader;
pub use sasl_authenticate_request::SaslAuthenticateRequest;
pub use sasl_authenticate_response::SaslAuthenticateResponse;
pub use sasl_handshake_request::SaslHandshakeRequest;
pub use sasl_handshake_response::SaslHandshakeResponse;
pub use share_acknowledge_request::ShareAcknowledgeRequest;
pub use share_acknowledge_response::ShareAcknowledgeResponse;
pub use share_fetch_request::ShareFetchRequest;
pub use share_fetch_response::ShareFetchResponse;
pub use share_group_describe_request::ShareGroupDescribeRequest;
pub use share_group_describe_response::ShareGroupDescribeResponse;
pub use share_group_heartbeat_request::ShareGroupHeartbeatRequest;
pub use share_group_heartbeat_response::ShareGroupHeartbeatResponse;
pub use snapshot_footer_record::SnapshotFooterRecord;
pub use snapshot_header_record::SnapshotHeaderRecord;
pub use stop_replica_request::StopReplicaRequest;
pub use stop_replica_response::StopReplicaResponse;
pub use streams_group_describe_request::StreamsGroupDescribeRequest;
pub use streams_group_describe_response::StreamsGroupDescribeResponse;
pub use streams_group_heartbeat_request::StreamsGroupHeartbeatRequest;
pub use streams_group_heartbeat_response::StreamsGroupHeartbeatResponse;
pub use sync_group_request::SyncGroupRequest;
pub use sync_group_response::SyncGroupResponse;
pub use txn_offset_commit_request::TxnOffsetCommitRequest;
pub use txn_offset_commit_response::TxnOffsetCommitResponse;
pub use unregister_broker_request::UnregisterBrokerRequest;
pub use unregister_broker_response::UnregisterBrokerResponse;
pub use update_features_request::UpdateFeaturesRequest;
pub use update_features_response::UpdateFeaturesResponse;
pub use update_metadata_request::UpdateMetadataRequest;
pub use update_metadata_response::UpdateMetadataResponse;
pub use update_raft_voter_request::UpdateRaftVoterRequest;
pub use update_raft_voter_response::UpdateRaftVoterResponse;
pub use vote_request::VoteRequest;
pub use vote_response::VoteResponse;
pub use voters_record::VotersRecord;
pub use write_share_group_state_request::WriteShareGroupStateRequest;
pub use write_share_group_state_response::WriteShareGroupStateResponse;
pub use write_txn_markers_request::WriteTxnMarkersRequest;
pub use write_txn_markers_response::WriteTxnMarkersResponse;

use crate::traits::ApiRequest;
/// Look up whether a given API key + version uses flexible (compact) wire encoding.
/// Generated from each message's `flexibleVersions` field.
pub fn is_flexible_api(api_key: i16, api_version: i16) -> bool {
    match api_key {
        25 => api_version >= AddOffsetsToTxnRequest::get_min_flexible_version().0,
        24 => api_version >= AddPartitionsToTxnRequest::get_min_flexible_version().0,
        80 => api_version >= AddRaftVoterRequest::get_min_flexible_version().0,
        67 => api_version >= AllocateProducerIdsRequest::get_min_flexible_version().0,
        49 => api_version >= AlterClientQuotasRequest::get_min_flexible_version().0,
        33 => api_version >= AlterConfigsRequest::get_min_flexible_version().0,
        45 => api_version >= AlterPartitionReassignmentsRequest::get_min_flexible_version().0,
        56 => api_version >= AlterPartitionRequest::get_min_flexible_version().0,
        34 => api_version >= AlterReplicaLogDirsRequest::get_min_flexible_version().0,
        91 => api_version >= AlterShareGroupOffsetsRequest::get_min_flexible_version().0,
        51 => api_version >= AlterUserScramCredentialsRequest::get_min_flexible_version().0,
        18 => api_version >= ApiVersionsRequest::get_min_flexible_version().0,
        73 => api_version >= AssignReplicasToDirsRequest::get_min_flexible_version().0,
        53 => api_version >= BeginQuorumEpochRequest::get_min_flexible_version().0,
        63 => api_version >= BrokerHeartbeatRequest::get_min_flexible_version().0,
        62 => api_version >= BrokerRegistrationRequest::get_min_flexible_version().0,
        69 => api_version >= ConsumerGroupDescribeRequest::get_min_flexible_version().0,
        68 => api_version >= ConsumerGroupHeartbeatRequest::get_min_flexible_version().0,
        7 => api_version >= ControlledShutdownRequest::get_min_flexible_version().0,
        70 => api_version >= ControllerRegistrationRequest::get_min_flexible_version().0,
        30 => api_version >= CreateAclsRequest::get_min_flexible_version().0,
        38 => api_version >= CreateDelegationTokenRequest::get_min_flexible_version().0,
        37 => api_version >= CreatePartitionsRequest::get_min_flexible_version().0,
        19 => api_version >= CreateTopicsRequest::get_min_flexible_version().0,
        31 => api_version >= DeleteAclsRequest::get_min_flexible_version().0,
        42 => api_version >= DeleteGroupsRequest::get_min_flexible_version().0,
        21 => api_version >= DeleteRecordsRequest::get_min_flexible_version().0,
        92 => api_version >= DeleteShareGroupOffsetsRequest::get_min_flexible_version().0,
        86 => api_version >= DeleteShareGroupStateRequest::get_min_flexible_version().0,
        20 => api_version >= DeleteTopicsRequest::get_min_flexible_version().0,
        29 => api_version >= DescribeAclsRequest::get_min_flexible_version().0,
        48 => api_version >= DescribeClientQuotasRequest::get_min_flexible_version().0,
        60 => api_version >= DescribeClusterRequest::get_min_flexible_version().0,
        32 => api_version >= DescribeConfigsRequest::get_min_flexible_version().0,
        41 => api_version >= DescribeDelegationTokenRequest::get_min_flexible_version().0,
        15 => api_version >= DescribeGroupsRequest::get_min_flexible_version().0,
        35 => api_version >= DescribeLogDirsRequest::get_min_flexible_version().0,
        61 => api_version >= DescribeProducersRequest::get_min_flexible_version().0,
        55 => api_version >= DescribeQuorumRequest::get_min_flexible_version().0,
        90 => api_version >= DescribeShareGroupOffsetsRequest::get_min_flexible_version().0,
        75 => api_version >= DescribeTopicPartitionsRequest::get_min_flexible_version().0,
        65 => api_version >= DescribeTransactionsRequest::get_min_flexible_version().0,
        50 => api_version >= DescribeUserScramCredentialsRequest::get_min_flexible_version().0,
        43 => api_version >= ElectLeadersRequest::get_min_flexible_version().0,
        54 => api_version >= EndQuorumEpochRequest::get_min_flexible_version().0,
        26 => api_version >= EndTxnRequest::get_min_flexible_version().0,
        58 => api_version >= EnvelopeRequest::get_min_flexible_version().0,
        40 => api_version >= ExpireDelegationTokenRequest::get_min_flexible_version().0,
        1 => api_version >= FetchRequest::get_min_flexible_version().0,
        59 => api_version >= FetchSnapshotRequest::get_min_flexible_version().0,
        10 => api_version >= FindCoordinatorRequest::get_min_flexible_version().0,
        71 => api_version >= GetTelemetrySubscriptionsRequest::get_min_flexible_version().0,
        12 => api_version >= HeartbeatRequest::get_min_flexible_version().0,
        44 => api_version >= IncrementalAlterConfigsRequest::get_min_flexible_version().0,
        22 => api_version >= InitProducerIdRequest::get_min_flexible_version().0,
        83 => api_version >= InitializeShareGroupStateRequest::get_min_flexible_version().0,
        11 => api_version >= JoinGroupRequest::get_min_flexible_version().0,
        4 => api_version >= LeaderAndIsrRequest::get_min_flexible_version().0,
        13 => api_version >= LeaveGroupRequest::get_min_flexible_version().0,
        74 => api_version >= ListConfigResourcesRequest::get_min_flexible_version().0,
        16 => api_version >= ListGroupsRequest::get_min_flexible_version().0,
        2 => api_version >= ListOffsetsRequest::get_min_flexible_version().0,
        46 => api_version >= ListPartitionReassignmentsRequest::get_min_flexible_version().0,
        66 => api_version >= ListTransactionsRequest::get_min_flexible_version().0,
        3 => api_version >= MetadataRequest::get_min_flexible_version().0,
        8 => api_version >= OffsetCommitRequest::get_min_flexible_version().0,
        47 => api_version >= OffsetDeleteRequest::get_min_flexible_version().0,
        9 => api_version >= OffsetFetchRequest::get_min_flexible_version().0,
        23 => api_version >= OffsetForLeaderEpochRequest::get_min_flexible_version().0,
        0 => api_version >= ProduceRequest::get_min_flexible_version().0,
        72 => api_version >= PushTelemetryRequest::get_min_flexible_version().0,
        84 => api_version >= ReadShareGroupStateRequest::get_min_flexible_version().0,
        87 => api_version >= ReadShareGroupStateSummaryRequest::get_min_flexible_version().0,
        81 => api_version >= RemoveRaftVoterRequest::get_min_flexible_version().0,
        39 => api_version >= RenewDelegationTokenRequest::get_min_flexible_version().0,
        36 => api_version >= SaslAuthenticateRequest::get_min_flexible_version().0,
        17 => api_version >= SaslHandshakeRequest::get_min_flexible_version().0,
        79 => api_version >= ShareAcknowledgeRequest::get_min_flexible_version().0,
        78 => api_version >= ShareFetchRequest::get_min_flexible_version().0,
        77 => api_version >= ShareGroupDescribeRequest::get_min_flexible_version().0,
        76 => api_version >= ShareGroupHeartbeatRequest::get_min_flexible_version().0,
        5 => api_version >= StopReplicaRequest::get_min_flexible_version().0,
        89 => api_version >= StreamsGroupDescribeRequest::get_min_flexible_version().0,
        88 => api_version >= StreamsGroupHeartbeatRequest::get_min_flexible_version().0,
        14 => api_version >= SyncGroupRequest::get_min_flexible_version().0,
        28 => api_version >= TxnOffsetCommitRequest::get_min_flexible_version().0,
        64 => api_version >= UnregisterBrokerRequest::get_min_flexible_version().0,
        57 => api_version >= UpdateFeaturesRequest::get_min_flexible_version().0,
        6 => api_version >= UpdateMetadataRequest::get_min_flexible_version().0,
        82 => api_version >= UpdateRaftVoterRequest::get_min_flexible_version().0,
        52 => api_version >= VoteRequest::get_min_flexible_version().0,
        85 => api_version >= WriteShareGroupStateRequest::get_min_flexible_version().0,
        27 => api_version >= WriteTxnMarkersRequest::get_min_flexible_version().0,
        _ => false,
    }
}

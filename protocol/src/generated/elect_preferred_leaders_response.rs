#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// ElectPreferredLeadersResponse
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ElectPreferredLeadersResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// The error code, or 0 if there was no error.
    pub replica_election_results: Vec<ReplicaElectionResult>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct PartitionResult {
    /// The partition id
    pub partition_id: i32,
    /// The result error, or zero if there was no error.
    pub error_code: i16,
    /// The result message, or null if there was no error.
    pub error_message: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ReplicaElectionResult {
    /// The topic name
    pub topic: String,
    /// The results for each partition
    pub partition_result: Vec<PartitionResult>,
}

impl ApiResponse for ElectPreferredLeadersResponse {
    type Request = crate::generated::ElectPreferredLeadersRequest;
    fn get_api_key() -> ApiKey {
        ApiKey::new(43)
    }
    fn get_min_supported_version() -> ApiVersion {
        ApiVersion::new(0)
    }
    fn get_max_supported_version() -> ApiVersion {
        ApiVersion::new(0)
    }
    fn serialize(&self, version: ApiVersion, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            (0) <= version.0 && version.0 <= (0),
            "version {} is not supported by {} (supported: 0-0)",
            version.0,
            stringify!(Self)
        );
        self.throttle_time_ms
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode ThrottleTimeMs"))?;
        self.replica_election_results
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode ReplicaElectionResults"))?;
        Ok(())
    }
    fn deserialize(version: ApiVersion, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let throttle_time_ms = <i32 as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode ThrottleTimeMs"))?;
        let replica_election_results =
            <Vec<ReplicaElectionResult> as KafkaDeserialize>::decode(buf).map_err(|_| {
                SerializationError::Decode("failed to decode ReplicaElectionResults")
            })?;
        Ok(Self {
            throttle_time_ms,
            replica_election_results,
        })
    }
}
impl KafkaSerialize for ElectPreferredLeadersResponse {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.throttle_time_ms
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ThrottleTimeMs".into(),
            })?;
        self.replica_election_results
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ReplicaElectionResults".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for ElectPreferredLeadersResponse {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let throttle_time_ms =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ThrottleTimeMs".into(),
            })?;
        let replica_election_results =
            <Vec<ReplicaElectionResult> as KafkaDeserialize>::decode(buf).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode ReplicaElectionResults".into(),
                }
            })?;
        Ok(Self {
            throttle_time_ms,
            replica_election_results,
        })
    }
}

impl KafkaSerialize for PartitionResult {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.partition_id
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode PartitionId".into(),
            })?;
        self.error_code
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ErrorCode".into(),
            })?;
        self.error_message
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ErrorMessage".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for PartitionResult {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let partition_id =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode PartitionId".into(),
            })?;
        let error_code =
            <i16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ErrorCode".into(),
            })?;
        let error_message = <Option<String> as KafkaDeserialize>::decode(buf).map_err(|_| {
            DecodeError::Protocol {
                message: "failed to decode ErrorMessage".into(),
            }
        })?;
        Ok(Self {
            partition_id,
            error_code,
            error_message,
        })
    }
}

impl KafkaSerialize for ReplicaElectionResult {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.topic
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Topic".into(),
            })?;
        self.partition_result
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode PartitionResult".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for ReplicaElectionResult {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let topic =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode Topic".into(),
            })?;
        let partition_result =
            <Vec<PartitionResult> as KafkaDeserialize>::decode(buf).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode PartitionResult".into(),
                }
            })?;
        Ok(Self {
            topic,
            partition_result,
        })
    }
}

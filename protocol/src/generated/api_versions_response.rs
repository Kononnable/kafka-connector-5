#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// ApiVersionsResponse
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ApiVersionsResponse {
    /// The top-level error code.
    pub error_code: i16,
    /// The APIs supported by the broker.
    pub api_keys: Vec<ApiVersionsResponseKey>,
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    /// Available in version 1+.
    pub throttle_time_ms: i32,
    /// Features supported by the broker.
    /// Available in version 3+.
    pub supported_features: Vec<SupportedFeatureKey>,
    /// The monotonically increasing epoch for the finalized features information. Valid values are >= 0. A value of -1 is special and represents unknown epoch.
    /// Available in version 3+.
    pub finalized_features_epoch: i64,
    /// List of cluster-wide finalized features. The information is valid only if FinalizedFeaturesEpoch >= 0.
    /// Available in version 3+.
    pub finalized_features: Vec<FinalizedFeatureKey>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ApiVersionsResponseKey {
    /// The API index.
    pub api_key: i16,
    /// The minimum supported version, inclusive.
    pub min_version: i16,
    /// The maximum supported version, inclusive.
    pub max_version: i16,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct FinalizedFeatureKey {
    /// The name of the feature.
    /// Available in version 3+.
    pub name: String,
    /// The cluster-wide finalized max version level for the feature.
    /// Available in version 3+.
    pub max_version_level: i16,
    /// The cluster-wide finalized min version level for the feature.
    /// Available in version 3+.
    pub min_version_level: i16,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct SupportedFeatureKey {
    /// The name of the feature.
    /// Available in version 3+.
    pub name: String,
    /// The minimum supported version for the feature.
    /// Available in version 3+.
    pub min_version: i16,
    /// The maximum supported version for the feature.
    /// Available in version 3+.
    pub max_version: i16,
}

impl ApiResponse for ApiVersionsResponse {
    type Request = crate::generated::ApiVersionsRequest;
    fn get_api_key() -> ApiKey {
        ApiKey::new(18)
    }
    fn get_min_supported_version() -> ApiVersion {
        ApiVersion::new(0)
    }
    fn get_max_supported_version() -> ApiVersion {
        ApiVersion::new(3)
    }
    fn serialize(&self, version: ApiVersion, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            (0) <= version.0 && version.0 <= (3),
            "version {} is not supported by {} (supported: 0-3)",
            version.0,
            stringify!(Self)
        );
        self.error_code
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode ErrorCode"))?;
        self.api_keys
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode ApiKeys"))?;
        if (1) <= version.0 {
            self.throttle_time_ms
                .encode(buf)
                .map_err(|_| SerializationError::Encode("failed to encode ThrottleTimeMs"))?;
        }
        if (3) <= version.0 {
            self.supported_features
                .encode(buf)
                .map_err(|_| SerializationError::Encode("failed to encode SupportedFeatures"))?;
        }
        if (3) <= version.0 {
            self.finalized_features_epoch.encode(buf).map_err(|_| {
                SerializationError::Encode("failed to encode FinalizedFeaturesEpoch")
            })?;
        }
        if (3) <= version.0 {
            self.finalized_features
                .encode(buf)
                .map_err(|_| SerializationError::Encode("failed to encode FinalizedFeatures"))?;
        }
        Ok(())
    }
    fn deserialize(version: ApiVersion, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let error_code = <i16 as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode ErrorCode"))?;
        let api_keys = <Vec<ApiVersionsResponseKey> as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode ApiKeys"))?;
        let throttle_time_ms = if (1) <= version.0 {
            <i32 as KafkaDeserialize>::decode(buf)
                .map_err(|_| SerializationError::Decode("failed to decode ThrottleTimeMs"))?
        } else {
            Default::default()
        };
        let supported_features = if (3) <= version.0 {
            <Vec<SupportedFeatureKey> as KafkaDeserialize>::decode(buf)
                .map_err(|_| SerializationError::Decode("failed to decode SupportedFeatures"))?
        } else {
            Default::default()
        };
        let finalized_features_epoch = if (3) <= version.0 {
            <i64 as KafkaDeserialize>::decode(buf).map_err(|_| {
                SerializationError::Decode("failed to decode FinalizedFeaturesEpoch")
            })?
        } else {
            Default::default()
        };
        let finalized_features = if (3) <= version.0 {
            <Vec<FinalizedFeatureKey> as KafkaDeserialize>::decode(buf)
                .map_err(|_| SerializationError::Decode("failed to decode FinalizedFeatures"))?
        } else {
            Default::default()
        };
        Ok(Self {
            error_code,
            api_keys,
            throttle_time_ms,
            supported_features,
            finalized_features_epoch,
            finalized_features,
        })
    }
}
impl KafkaSerialize for ApiVersionsResponse {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.error_code
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ErrorCode".into(),
            })?;
        self.api_keys
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ApiKeys".into(),
            })?;
        self.throttle_time_ms
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ThrottleTimeMs".into(),
            })?;
        self.supported_features
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode SupportedFeatures".into(),
            })?;
        self.finalized_features_epoch
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode FinalizedFeaturesEpoch".into(),
            })?;
        self.finalized_features
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode FinalizedFeatures".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for ApiVersionsResponse {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let error_code =
            <i16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ErrorCode".into(),
            })?;
        let api_keys =
            <Vec<ApiVersionsResponseKey> as KafkaDeserialize>::decode(buf).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode ApiKeys".into(),
                }
            })?;
        let throttle_time_ms =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ThrottleTimeMs".into(),
            })?;
        let supported_features = <Vec<SupportedFeatureKey> as KafkaDeserialize>::decode(buf)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode SupportedFeatures".into(),
            })?;
        let finalized_features_epoch =
            <i64 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode FinalizedFeaturesEpoch".into(),
            })?;
        let finalized_features = <Vec<FinalizedFeatureKey> as KafkaDeserialize>::decode(buf)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode FinalizedFeatures".into(),
            })?;
        Ok(Self {
            error_code,
            api_keys,
            throttle_time_ms,
            supported_features,
            finalized_features_epoch,
            finalized_features,
        })
    }
}

impl KafkaSerialize for ApiVersionsResponseKey {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.api_key
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ApiKey".into(),
            })?;
        self.min_version
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode MinVersion".into(),
            })?;
        self.max_version
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode MaxVersion".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for ApiVersionsResponseKey {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let api_key =
            <i16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ApiKey".into(),
            })?;
        let min_version =
            <i16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode MinVersion".into(),
            })?;
        let max_version =
            <i16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode MaxVersion".into(),
            })?;
        Ok(Self {
            api_key,
            min_version,
            max_version,
        })
    }
}

impl KafkaSerialize for FinalizedFeatureKey {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.name
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Name".into(),
            })?;
        self.max_version_level
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode MaxVersionLevel".into(),
            })?;
        self.min_version_level
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode MinVersionLevel".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for FinalizedFeatureKey {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let name =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode Name".into(),
            })?;
        let max_version_level =
            <i16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode MaxVersionLevel".into(),
            })?;
        let min_version_level =
            <i16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode MinVersionLevel".into(),
            })?;
        Ok(Self {
            name,
            max_version_level,
            min_version_level,
        })
    }
}

impl KafkaSerialize for SupportedFeatureKey {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.name
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Name".into(),
            })?;
        self.min_version
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode MinVersion".into(),
            })?;
        self.max_version
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode MaxVersion".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for SupportedFeatureKey {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let name =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode Name".into(),
            })?;
        let min_version =
            <i16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode MinVersion".into(),
            })?;
        let max_version =
            <i16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode MaxVersion".into(),
            })?;
        Ok(Self {
            name,
            min_version,
            max_version,
        })
    }
}

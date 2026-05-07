#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{EncodeError, DecodeError, KafkaSerialize, KafkaDeserialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// ApiVersionsResponse
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ApiVersionsResponse {
    /// The top-level error code.
    pub error_code: i16,
    /// The APIs supported by the broker.
    pub api_keys: Vec<ApiVersion>,
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    /// Available in version 1+.
    pub throttle_time_ms: i32,
    /// Features supported by the broker. Note: in v0-v3, features with MinSupportedVersion = 0 are omitted.
    /// Available in version 3+.
    pub supported_features: Vec<SupportedFeatureKey>,
    /// The monotonically increasing epoch for the finalized features information. Valid values are >= 0. A value of -1 is special and represents unknown epoch.
    /// Available in version 3+.
    pub finalized_features_epoch: i64,
    /// List of cluster-wide finalized features. The information is valid only if FinalizedFeaturesEpoch >= 0.
    /// Available in version 3+.
    pub finalized_features: Vec<FinalizedFeatureKey>,
    /// Set by a KRaft controller if the required configurations for ZK migration are present.
    /// Available in version 3+.
    pub zk_migration_ready: bool,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ApiVersion {
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
    fn get_api_key() -> ApiKey { ApiKey::new(18) }
    fn get_min_supported_version() -> crate::traits::ApiVersion { crate::traits::ApiVersion::new(0) }
    fn get_max_supported_version() -> crate::traits::ApiVersion { crate::traits::ApiVersion::new(4) }
    fn serialize(&self, version: crate::traits::ApiVersion, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!((0) <= version.0 && version.0 <= (4), "version {} is not supported by {} (supported: 0-4)", version.0, stringify!(Self));
        let is_flexible = (3) <= version.0;
        self.error_code.encode_flexible(buf, is_flexible).map_err(|_| SerializationError::Encode("failed to encode ErrorCode"))?;
        self.api_keys.encode_flexible(buf, is_flexible).map_err(|_| SerializationError::Encode("failed to encode ApiKeys"))?;
        if (1) <= version.0 {
            self.throttle_time_ms.encode_flexible(buf, is_flexible).map_err(|_| SerializationError::Encode("failed to encode ThrottleTimeMs"))?;
        }
        if (3) <= version.0 {
            self.supported_features.encode_flexible(buf, is_flexible).map_err(|_| SerializationError::Encode("failed to encode SupportedFeatures"))?;
        }
        if (3) <= version.0 {
            self.finalized_features_epoch.encode_flexible(buf, is_flexible).map_err(|_| SerializationError::Encode("failed to encode FinalizedFeaturesEpoch"))?;
        }
        if (3) <= version.0 {
            self.finalized_features.encode_flexible(buf, is_flexible).map_err(|_| SerializationError::Encode("failed to encode FinalizedFeatures"))?;
        }
        if (3) <= version.0 {
            self.zk_migration_ready.encode_flexible(buf, is_flexible).map_err(|_| SerializationError::Encode("failed to encode ZkMigrationReady"))?;
        }
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(version: crate::traits::ApiVersion, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let is_flexible = (3) <= version.0;
        let error_code = <i16 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| SerializationError::Decode("failed to decode ErrorCode"))?;
        let api_keys = <Vec<ApiVersion> as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| SerializationError::Decode("failed to decode ApiKeys"))?;
        let throttle_time_ms = if (1) <= version.0 {
            <i32 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| SerializationError::Decode("failed to decode ThrottleTimeMs"))?
        } else {
            Default::default()
        };
        let supported_features = if (3) <= version.0 {
            <Vec<SupportedFeatureKey> as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| SerializationError::Decode("failed to decode SupportedFeatures"))?
        } else {
            Default::default()
        };
        let finalized_features_epoch = if (3) <= version.0 {
            <i64 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| SerializationError::Decode("failed to decode FinalizedFeaturesEpoch"))?
        } else {
            Default::default()
        };
        let finalized_features = if (3) <= version.0 {
            <Vec<FinalizedFeatureKey> as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| SerializationError::Decode("failed to decode FinalizedFeatures"))?
        } else {
            Default::default()
        };
        let zk_migration_ready = if (3) <= version.0 {
            <bool as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| SerializationError::Decode("failed to decode ZkMigrationReady"))?
        } else {
            Default::default()
        };
        Ok(Self { error_code, api_keys, throttle_time_ms, supported_features, finalized_features_epoch, finalized_features, zk_migration_ready })
    }
}
impl KafkaSerialize for ApiVersionsResponse {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.error_code.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode ErrorCode".into() })?;
        self.api_keys.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode ApiKeys".into() })?;
        self.throttle_time_ms.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode ThrottleTimeMs".into() })?;
        self.supported_features.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode SupportedFeatures".into() })?;
        self.finalized_features_epoch.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode FinalizedFeaturesEpoch".into() })?;
        self.finalized_features.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode FinalizedFeatures".into() })?;
        self.zk_migration_ready.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode ZkMigrationReady".into() })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(&self, buf: &mut B, is_flexible: bool) -> Result<(), EncodeError> {
        self.error_code.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode ErrorCode".into() })?;
        self.api_keys.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode ApiKeys".into() })?;
        self.throttle_time_ms.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode ThrottleTimeMs".into() })?;
        self.supported_features.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode SupportedFeatures".into() })?;
        self.finalized_features_epoch.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode FinalizedFeaturesEpoch".into() })?;
        self.finalized_features.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode FinalizedFeatures".into() })?;
        self.zk_migration_ready.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode ZkMigrationReady".into() })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for ApiVersionsResponse {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let error_code = <i16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode ErrorCode".into() })?;
        let api_keys = <Vec<ApiVersion> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode ApiKeys".into() })?;
        let throttle_time_ms = <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode ThrottleTimeMs".into() })?;
        let supported_features = <Vec<SupportedFeatureKey> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode SupportedFeatures".into() })?;
        let finalized_features_epoch = <i64 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode FinalizedFeaturesEpoch".into() })?;
        let finalized_features = <Vec<FinalizedFeatureKey> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode FinalizedFeatures".into() })?;
        let zk_migration_ready = <bool as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode ZkMigrationReady".into() })?;
        Ok(Self { error_code, api_keys, throttle_time_ms, supported_features, finalized_features_epoch, finalized_features, zk_migration_ready })
    }
    fn decode_flexible<B: Buf>(buf: &mut B, is_flexible: bool) -> Result<Self, DecodeError> {
        let error_code = <i16 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode ErrorCode".into() })?;
        let api_keys = <Vec<ApiVersion> as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode ApiKeys".into() })?;
        let throttle_time_ms = <i32 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode ThrottleTimeMs".into() })?;
        let supported_features = <Vec<SupportedFeatureKey> as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode SupportedFeatures".into() })?;
        let finalized_features_epoch = <i64 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode FinalizedFeaturesEpoch".into() })?;
        let finalized_features = <Vec<FinalizedFeatureKey> as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode FinalizedFeatures".into() })?;
        let zk_migration_ready = <bool as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode ZkMigrationReady".into() })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self { error_code, api_keys, throttle_time_ms, supported_features, finalized_features_epoch, finalized_features, zk_migration_ready })
    }
}

impl KafkaSerialize for ApiVersion {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.api_key.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode ApiKey".into() })?;
        self.min_version.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode MinVersion".into() })?;
        self.max_version.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode MaxVersion".into() })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(&self, buf: &mut B, is_flexible: bool) -> Result<(), EncodeError> {
        self.api_key.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode ApiKey".into() })?;
        self.min_version.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode MinVersion".into() })?;
        self.max_version.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode MaxVersion".into() })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for ApiVersion {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let api_key = <i16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode ApiKey".into() })?;
        let min_version = <i16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode MinVersion".into() })?;
        let max_version = <i16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode MaxVersion".into() })?;
        Ok(Self { api_key, min_version, max_version })
    }
    fn decode_flexible<B: Buf>(buf: &mut B, is_flexible: bool) -> Result<Self, DecodeError> {
        let api_key = <i16 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode ApiKey".into() })?;
        let min_version = <i16 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode MinVersion".into() })?;
        let max_version = <i16 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode MaxVersion".into() })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self { api_key, min_version, max_version })
    }
}

impl KafkaSerialize for FinalizedFeatureKey {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.name.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Name".into() })?;
        self.max_version_level.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode MaxVersionLevel".into() })?;
        self.min_version_level.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode MinVersionLevel".into() })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(&self, buf: &mut B, is_flexible: bool) -> Result<(), EncodeError> {
        self.name.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Name".into() })?;
        self.max_version_level.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode MaxVersionLevel".into() })?;
        self.min_version_level.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode MinVersionLevel".into() })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for FinalizedFeatureKey {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let name = <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode Name".into() })?;
        let max_version_level = <i16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode MaxVersionLevel".into() })?;
        let min_version_level = <i16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode MinVersionLevel".into() })?;
        Ok(Self { name, max_version_level, min_version_level })
    }
    fn decode_flexible<B: Buf>(buf: &mut B, is_flexible: bool) -> Result<Self, DecodeError> {
        let name = <String as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode Name".into() })?;
        let max_version_level = <i16 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode MaxVersionLevel".into() })?;
        let min_version_level = <i16 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode MinVersionLevel".into() })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self { name, max_version_level, min_version_level })
    }
}

impl KafkaSerialize for SupportedFeatureKey {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.name.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Name".into() })?;
        self.min_version.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode MinVersion".into() })?;
        self.max_version.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode MaxVersion".into() })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(&self, buf: &mut B, is_flexible: bool) -> Result<(), EncodeError> {
        self.name.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Name".into() })?;
        self.min_version.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode MinVersion".into() })?;
        self.max_version.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode MaxVersion".into() })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for SupportedFeatureKey {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let name = <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode Name".into() })?;
        let min_version = <i16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode MinVersion".into() })?;
        let max_version = <i16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode MaxVersion".into() })?;
        Ok(Self { name, min_version, max_version })
    }
    fn decode_flexible<B: Buf>(buf: &mut B, is_flexible: bool) -> Result<Self, DecodeError> {
        let name = <String as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode Name".into() })?;
        let min_version = <i16 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode MinVersion".into() })?;
        let max_version = <i16 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode MaxVersion".into() })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self { name, min_version, max_version })
    }
}


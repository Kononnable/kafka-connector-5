#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{KafkaDeserialize, KafkaSerialize};
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
    fn get_api_key() -> ApiKey {
        ApiKey::new(18)
    }
    fn get_min_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(0)
    }
    fn get_max_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(4)
    }
    fn get_min_flexible_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(3)
    }
    fn serialize(
        &self,
        version: crate::traits::ApiVersion,
        buf: &mut BytesMut,
    ) -> Result<(), SerializationError> {
        assert!(
            (0) <= version.0 && version.0 <= (4),
            "version {} is not supported by {} (supported: 0-4)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        self.error_code.encode(buf, version, is_flexible)?;
        self.api_keys.encode(buf, version, is_flexible)?;
        if (1) <= version.0 {
            self.throttle_time_ms.encode(buf, version, is_flexible)?;
        } else if self.throttle_time_ms != 0 {
            return Err(SerializationError::Encode(
                "field 'ThrottleTimeMs' is not available in this version",
            ));
        }
        if (3) <= version.0 {
            self.supported_features.encode(buf, version, is_flexible)?;
        } else if !self.supported_features.is_empty() {
            return Err(SerializationError::Encode(
                "field 'SupportedFeatures' is not available in this version",
            ));
        }
        if (3) <= version.0 {
            self.finalized_features_epoch
                .encode(buf, version, is_flexible)?;
        } else if self.finalized_features_epoch != 0 {
            return Err(SerializationError::Encode(
                "field 'FinalizedFeaturesEpoch' is not available in this version",
            ));
        }
        if (3) <= version.0 {
            self.finalized_features.encode(buf, version, is_flexible)?;
        } else if !self.finalized_features.is_empty() {
            return Err(SerializationError::Encode(
                "field 'FinalizedFeatures' is not available in this version",
            ));
        }
        if (3) <= version.0 {
            self.zk_migration_ready.encode(buf, version, is_flexible)?;
        } else if self.zk_migration_ready {
            return Err(SerializationError::Encode(
                "field 'ZkMigrationReady' is not available in this version",
            ));
        }
        if is_flexible {
            let mut __tag_count = 0u64;
            if !self.supported_features.is_empty() {
                __tag_count += 1;
            }
            if self.finalized_features_epoch != 0 {
                __tag_count += 1;
            }
            if !self.finalized_features.is_empty() {
                __tag_count += 1;
            }
            if self.zk_migration_ready {
                __tag_count += 1;
            }
            crate::protocol::serialization::encode_unsigned_varint(__tag_count, buf);
            if !self.supported_features.is_empty() {
                crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
                let mut __tmp = bytes::BytesMut::new();
                self.supported_features.encode(&mut __tmp, version, true)?;
                crate::protocol::serialization::encode_unsigned_varint(__tmp.len() as u64, buf);
                buf.put_slice(&__tmp);
            }
            if self.finalized_features_epoch != 0 {
                crate::protocol::serialization::encode_unsigned_varint(1u64, buf);
                let mut __tmp = bytes::BytesMut::new();
                self.finalized_features_epoch
                    .encode(&mut __tmp, version, true)?;
                crate::protocol::serialization::encode_unsigned_varint(__tmp.len() as u64, buf);
                buf.put_slice(&__tmp);
            }
            if !self.finalized_features.is_empty() {
                crate::protocol::serialization::encode_unsigned_varint(2u64, buf);
                let mut __tmp = bytes::BytesMut::new();
                self.finalized_features.encode(&mut __tmp, version, true)?;
                crate::protocol::serialization::encode_unsigned_varint(__tmp.len() as u64, buf);
                buf.put_slice(&__tmp);
            }
            if self.zk_migration_ready {
                crate::protocol::serialization::encode_unsigned_varint(3u64, buf);
                let mut __tmp = bytes::BytesMut::new();
                self.zk_migration_ready.encode(&mut __tmp, version, true)?;
                crate::protocol::serialization::encode_unsigned_varint(__tmp.len() as u64, buf);
                buf.put_slice(&__tmp);
            }
        }
        Ok(())
    }
    fn deserialize(
        version: crate::traits::ApiVersion,
        buf: &mut Bytes,
    ) -> Result<Self, SerializationError> {
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let error_code = <i16 as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let api_keys = <Vec<ApiVersion> as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let throttle_time_ms = if (1) <= version.0 {
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let mut supported_features = if (3) <= version.0 {
            if is_flexible {
                Default::default()
            } else {
                <Vec<SupportedFeatureKey> as KafkaDeserialize>::decode(buf, version, is_flexible)?
            }
        } else {
            Default::default()
        };
        let mut finalized_features_epoch = if (3) <= version.0 {
            if is_flexible {
                Default::default()
            } else {
                <i64 as KafkaDeserialize>::decode(buf, version, is_flexible)?
            }
        } else {
            Default::default()
        };
        let mut finalized_features = if (3) <= version.0 {
            if is_flexible {
                Default::default()
            } else {
                <Vec<FinalizedFeatureKey> as KafkaDeserialize>::decode(buf, version, is_flexible)?
            }
        } else {
            Default::default()
        };
        let mut zk_migration_ready = if (3) <= version.0 {
            if is_flexible {
                Default::default()
            } else {
                <bool as KafkaDeserialize>::decode(buf, version, is_flexible)?
            }
        } else {
            Default::default()
        };
        if is_flexible {
            let (__tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
            for _ in 0..__tag_count {
                let (__tag_id, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
                let (__tag_len, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
                match __tag_id {
                    0 => {
                        supported_features =
                            <Vec<SupportedFeatureKey> as KafkaDeserialize>::decode(
                                buf, version, true,
                            )?;
                    }
                    1 => {
                        finalized_features_epoch =
                            <i64 as KafkaDeserialize>::decode(buf, version, true)?;
                    }
                    2 => {
                        finalized_features =
                            <Vec<FinalizedFeatureKey> as KafkaDeserialize>::decode(
                                buf, version, true,
                            )?;
                    }
                    3 => {
                        zk_migration_ready =
                            <bool as KafkaDeserialize>::decode(buf, version, true)?;
                    }
                    _ => {
                        buf.advance(__tag_len as usize);
                    }
                }
            }
        }
        Ok(Self {
            error_code,
            api_keys,
            throttle_time_ms,
            supported_features,
            finalized_features_epoch,
            finalized_features,
            zk_migration_ready,
        })
    }
}
impl KafkaSerialize for ApiVersionsResponse {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), crate::traits::SerializationError> {
        self.error_code.encode(buf, version, is_flexible)?;
        self.api_keys.encode(buf, version, is_flexible)?;
        if (1) <= version.0 {
            self.throttle_time_ms.encode(buf, version, is_flexible)?;
        }
        if (3) <= version.0 && !is_flexible {
            self.supported_features.encode(buf, version, is_flexible)?;
        }
        if (3) <= version.0 && !is_flexible {
            self.finalized_features_epoch
                .encode(buf, version, is_flexible)?;
        }
        if (3) <= version.0 && !is_flexible {
            self.finalized_features.encode(buf, version, is_flexible)?;
        }
        if (3) <= version.0 && !is_flexible {
            self.zk_migration_ready.encode(buf, version, is_flexible)?;
        }
        if is_flexible {
            let mut __tag_count = 0u64;
            if !self.supported_features.is_empty() {
                __tag_count += 1;
            }
            if self.finalized_features_epoch != 0 {
                __tag_count += 1;
            }
            if !self.finalized_features.is_empty() {
                __tag_count += 1;
            }
            if self.zk_migration_ready {
                __tag_count += 1;
            }
            crate::protocol::serialization::encode_unsigned_varint(__tag_count, buf);
            if !self.supported_features.is_empty() {
                crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
                let mut __tmp = bytes::BytesMut::new();
                self.supported_features.encode(&mut __tmp, version, true)?;
                crate::protocol::serialization::encode_unsigned_varint(__tmp.len() as u64, buf);
                buf.put_slice(&__tmp);
            }
            if self.finalized_features_epoch != 0 {
                crate::protocol::serialization::encode_unsigned_varint(1u64, buf);
                let mut __tmp = bytes::BytesMut::new();
                self.finalized_features_epoch
                    .encode(&mut __tmp, version, true)?;
                crate::protocol::serialization::encode_unsigned_varint(__tmp.len() as u64, buf);
                buf.put_slice(&__tmp);
            }
            if !self.finalized_features.is_empty() {
                crate::protocol::serialization::encode_unsigned_varint(2u64, buf);
                let mut __tmp = bytes::BytesMut::new();
                self.finalized_features.encode(&mut __tmp, version, true)?;
                crate::protocol::serialization::encode_unsigned_varint(__tmp.len() as u64, buf);
                buf.put_slice(&__tmp);
            }
            if self.zk_migration_ready {
                crate::protocol::serialization::encode_unsigned_varint(3u64, buf);
                let mut __tmp = bytes::BytesMut::new();
                self.zk_migration_ready.encode(&mut __tmp, version, true)?;
                crate::protocol::serialization::encode_unsigned_varint(__tmp.len() as u64, buf);
                buf.put_slice(&__tmp);
            }
        }
        Ok(())
    }
}

impl KafkaDeserialize for ApiVersionsResponse {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, crate::traits::SerializationError> {
        let error_code = <i16 as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let api_keys = <Vec<ApiVersion> as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let throttle_time_ms = if (1) <= version.0 {
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let mut supported_features = if (3) <= version.0 {
            if is_flexible {
                Default::default()
            } else {
                <Vec<SupportedFeatureKey> as KafkaDeserialize>::decode(buf, version, is_flexible)?
            }
        } else {
            Default::default()
        };
        let mut finalized_features_epoch = if (3) <= version.0 {
            if is_flexible {
                Default::default()
            } else {
                <i64 as KafkaDeserialize>::decode(buf, version, is_flexible)?
            }
        } else {
            Default::default()
        };
        let mut finalized_features = if (3) <= version.0 {
            if is_flexible {
                Default::default()
            } else {
                <Vec<FinalizedFeatureKey> as KafkaDeserialize>::decode(buf, version, is_flexible)?
            }
        } else {
            Default::default()
        };
        let mut zk_migration_ready = if (3) <= version.0 {
            if is_flexible {
                Default::default()
            } else {
                <bool as KafkaDeserialize>::decode(buf, version, is_flexible)?
            }
        } else {
            Default::default()
        };
        if is_flexible {
            let (__tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
            for _ in 0..__tag_count {
                let (__tag_id, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
                let (__tag_len, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
                match __tag_id {
                    0 => {
                        supported_features =
                            <Vec<SupportedFeatureKey> as KafkaDeserialize>::decode(
                                buf, version, true,
                            )?;
                    }
                    1 => {
                        finalized_features_epoch =
                            <i64 as KafkaDeserialize>::decode(buf, version, true)?;
                    }
                    2 => {
                        finalized_features =
                            <Vec<FinalizedFeatureKey> as KafkaDeserialize>::decode(
                                buf, version, true,
                            )?;
                    }
                    3 => {
                        zk_migration_ready =
                            <bool as KafkaDeserialize>::decode(buf, version, true)?;
                    }
                    _ => {
                        buf.advance(__tag_len as usize);
                    }
                }
            }
        }
        Ok(Self {
            error_code,
            api_keys,
            throttle_time_ms,
            supported_features,
            finalized_features_epoch,
            finalized_features,
            zk_migration_ready,
        })
    }
}

impl KafkaSerialize for ApiVersion {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), crate::traits::SerializationError> {
        self.api_key.encode(buf, version, is_flexible)?;
        self.min_version.encode(buf, version, is_flexible)?;
        self.max_version.encode(buf, version, is_flexible)?;
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for ApiVersion {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, crate::traits::SerializationError> {
        let api_key = <i16 as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let min_version = <i16 as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let max_version = <i16 as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            api_key,
            min_version,
            max_version,
        })
    }
}

impl KafkaSerialize for FinalizedFeatureKey {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), crate::traits::SerializationError> {
        if (3) <= version.0 {
            self.name.encode(buf, version, is_flexible)?;
        }
        if (3) <= version.0 {
            self.max_version_level.encode(buf, version, is_flexible)?;
        }
        if (3) <= version.0 {
            self.min_version_level.encode(buf, version, is_flexible)?;
        }
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for FinalizedFeatureKey {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, crate::traits::SerializationError> {
        let name = if (3) <= version.0 {
            <String as KafkaDeserialize>::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let max_version_level = if (3) <= version.0 {
            <i16 as KafkaDeserialize>::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let min_version_level = if (3) <= version.0 {
            <i16 as KafkaDeserialize>::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            name,
            max_version_level,
            min_version_level,
        })
    }
}

impl KafkaSerialize for SupportedFeatureKey {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), crate::traits::SerializationError> {
        if (3) <= version.0 {
            self.name.encode(buf, version, is_flexible)?;
        }
        if (3) <= version.0 {
            self.min_version.encode(buf, version, is_flexible)?;
        }
        if (3) <= version.0 {
            self.max_version.encode(buf, version, is_flexible)?;
        }
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for SupportedFeatureKey {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, crate::traits::SerializationError> {
        let name = if (3) <= version.0 {
            <String as KafkaDeserialize>::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let min_version = if (3) <= version.0 {
            <i16 as KafkaDeserialize>::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let max_version = if (3) <= version.0 {
            <i16 as KafkaDeserialize>::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            name,
            min_version,
            max_version,
        })
    }
}

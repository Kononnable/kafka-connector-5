#![allow(unused_imports, unused_variables)]
use bytes::{Buf, BufMut, Bytes, BytesMut};
use indexmap::IndexMap;

use crate::protocol::serialization::{KafkaCodec, decode_unsigned_varint, encode_unsigned_varint};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVer, SerializationError};

// -------------------------------------------------------
// BrokerRegistrationRequest
// -------------------------------------------------------
#[derive(Clone, Debug, PartialEq)]
pub struct BrokerRegistrationRequest {
    /// The broker ID.
    pub broker_id: i32,
    /// The cluster id of the broker process.
    pub cluster_id: String,
    /// The incarnation id of the broker process.
    pub incarnation_id: [u8; 16],
    /// The listeners of this broker.
    /// IndexMap key `Name` (string): The name of the endpoint.
    pub listeners: IndexMap<String, Listener>,
    /// The features on this broker. Note: in v0-v3, features with MinSupportedVersion = 0 are omitted.
    /// IndexMap key `Name` (string): The feature name.
    pub features: IndexMap<String, Feature>,
    /// The rack which this broker is in.
    pub rack: Option<String>,
    /// If the required configurations for ZK migration are present, this value is set to true.
    /// Available in version 1+.
    pub is_migrating_zk_broker: bool,
    /// Log directories configured in this broker which are available.
    /// Available in version 2+.
    pub log_dirs: Vec<[u8; 16]>,
    /// The epoch before a clean shutdown.
    /// Available in version 3+.
    pub previous_broker_epoch: i64,
}
impl Default for BrokerRegistrationRequest {
    fn default() -> Self {
        Self {
            broker_id: 0,
            cluster_id: String::new(),
            incarnation_id: [0u8; 16],
            listeners: IndexMap::new(),
            features: IndexMap::new(),
            rack: None,
            is_migrating_zk_broker: false,
            log_dirs: Vec::new(),
            previous_broker_epoch: -1,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Feature {
    /// The minimum supported feature level.
    pub min_supported_version: i16,
    /// The maximum supported feature level.
    pub max_supported_version: i16,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Listener {
    /// The hostname.
    pub host: String,
    /// The port.
    pub port: u16,
    /// The security protocol.
    pub security_protocol: i16,
}

impl ApiRequest for BrokerRegistrationRequest {
    type Response = crate::generated::BrokerRegistrationResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(62)
    }
    fn get_min_supported_version() -> ApiVer {
        ApiVer::new(0)
    }
    fn get_max_supported_version() -> ApiVer {
        ApiVer::new(4)
    }
    fn get_min_flexible_version() -> ApiVer {
        ApiVer::new(0)
    }
    fn serialize(&self, version: ApiVer, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            0 <= version.0 && version.0 <= 4,
            "version {} is not supported by {} (supported: 0-4)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        self.broker_id.encode(buf, version, is_flexible)?;
        self.cluster_id.encode(buf, version, is_flexible)?;
        self.incarnation_id.encode(buf, version, is_flexible)?;
        self.listeners.encode(buf, version, is_flexible)?;
        self.features.encode(buf, version, is_flexible)?;
        self.rack.encode(buf, version, is_flexible)?;
        if 1 <= version.0 {
            self.is_migrating_zk_broker
                .encode(buf, version, is_flexible)?;
        } else if self.is_migrating_zk_broker {
            return Err(SerializationError::FieldNotAvailable {
                field: "IsMigratingZkBroker",
                version,
                api_name: "BrokerRegistrationRequest",
            });
        }
        if 2 <= version.0 {
            self.log_dirs.encode(buf, version, is_flexible)?;
        } else if !self.log_dirs.is_empty() {
            return Err(SerializationError::FieldNotAvailable {
                field: "LogDirs",
                version,
                api_name: "BrokerRegistrationRequest",
            });
        }
        if 3 <= version.0 {
            self.previous_broker_epoch
                .encode(buf, version, is_flexible)?;
        } else if self.previous_broker_epoch != -1 {
            return Err(SerializationError::FieldNotAvailable {
                field: "PreviousBrokerEpoch",
                version,
                api_name: "BrokerRegistrationRequest",
            });
        }
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(version: ApiVer, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let broker_id = KafkaCodec::decode(buf, version, is_flexible)?;
        let cluster_id = KafkaCodec::decode(buf, version, is_flexible)?;
        let incarnation_id = KafkaCodec::decode(buf, version, is_flexible)?;
        let listeners = KafkaCodec::decode(buf, version, is_flexible)?;
        let features = KafkaCodec::decode(buf, version, is_flexible)?;
        let rack = KafkaCodec::decode(buf, version, is_flexible)?;
        let is_migrating_zk_broker = if 1 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            false
        };
        let log_dirs = if 2 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Vec::new()
        };
        let previous_broker_epoch = if 3 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            -1
        };
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            broker_id,
            cluster_id,
            incarnation_id,
            listeners,
            features,
            rack,
            is_migrating_zk_broker,
            log_dirs,
            previous_broker_epoch,
        })
    }
}
impl KafkaCodec for BrokerRegistrationRequest {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.broker_id.encode(buf, version, is_flexible)?;
        self.cluster_id.encode(buf, version, is_flexible)?;
        self.incarnation_id.encode(buf, version, is_flexible)?;
        self.listeners.encode(buf, version, is_flexible)?;
        self.features.encode(buf, version, is_flexible)?;
        self.rack.encode(buf, version, is_flexible)?;
        if 1 <= version.0 {
            self.is_migrating_zk_broker
                .encode(buf, version, is_flexible)?;
        }
        if 2 <= version.0 {
            self.log_dirs.encode(buf, version, is_flexible)?;
        }
        if 3 <= version.0 {
            self.previous_broker_epoch
                .encode(buf, version, is_flexible)?;
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
        let broker_id = KafkaCodec::decode(buf, version, is_flexible)?;
        let cluster_id = KafkaCodec::decode(buf, version, is_flexible)?;
        let incarnation_id = KafkaCodec::decode(buf, version, is_flexible)?;
        let listeners = KafkaCodec::decode(buf, version, is_flexible)?;
        let features = KafkaCodec::decode(buf, version, is_flexible)?;
        let rack = KafkaCodec::decode(buf, version, is_flexible)?;
        let is_migrating_zk_broker = if 1 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            false
        };
        let log_dirs = if 2 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Vec::new()
        };
        let previous_broker_epoch = if 3 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            -1
        };
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            broker_id,
            cluster_id,
            incarnation_id,
            listeners,
            features,
            rack,
            is_migrating_zk_broker,
            log_dirs,
            previous_broker_epoch,
        })
    }
}

impl KafkaCodec for Feature {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.min_supported_version
            .encode(buf, version, is_flexible)?;
        self.max_supported_version
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
        let min_supported_version = KafkaCodec::decode(buf, version, is_flexible)?;
        let max_supported_version = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            min_supported_version,
            max_supported_version,
        })
    }
}

impl KafkaCodec for Listener {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.host.encode(buf, version, is_flexible)?;
        self.port.encode(buf, version, is_flexible)?;
        self.security_protocol.encode(buf, version, is_flexible)?;
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
        let host = KafkaCodec::decode(buf, version, is_flexible)?;
        let port = KafkaCodec::decode(buf, version, is_flexible)?;
        let security_protocol = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            host,
            port,
            security_protocol,
        })
    }
}

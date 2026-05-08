#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVer, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// BrokerRegistrationRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct BrokerRegistrationRequest {
    /// The broker ID.
    pub broker_id: i32,
    /// The cluster id of the broker process.
    pub cluster_id: String,
    /// The incarnation id of the broker process.
    pub incarnation_id: [u8; 16],
    /// The listeners of this broker.
    pub listeners: Vec<Listener>,
    /// The features on this broker. Note: in v0-v3, features with MinSupportedVersion = 0 are omitted.
    pub features: Vec<Feature>,
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

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Feature {
    /// The feature name.
    pub name: String,
    /// The minimum supported feature level.
    pub min_supported_version: i16,
    /// The maximum supported feature level.
    pub max_supported_version: i16,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Listener {
    /// The name of the endpoint.
    pub name: String,
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
            (0) <= version.0 && version.0 <= (4),
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
        if (1) <= version.0 {
            self.is_migrating_zk_broker
                .encode(buf, version, is_flexible)?;
        } else if self.is_migrating_zk_broker {
            return Err(SerializationError::Encode(
                "field 'IsMigratingZkBroker' is not available in this version",
            ));
        }
        if (2) <= version.0 {
            self.log_dirs.encode(buf, version, is_flexible)?;
        } else if !self.log_dirs.is_empty() {
            return Err(SerializationError::Encode(
                "field 'LogDirs' is not available in this version",
            ));
        }
        if (3) <= version.0 {
            self.previous_broker_epoch
                .encode(buf, version, is_flexible)?;
        } else if self.previous_broker_epoch != 0 {
            return Err(SerializationError::Encode(
                "field 'PreviousBrokerEpoch' is not available in this version",
            ));
        }
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(version: ApiVer, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let broker_id = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let cluster_id = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let incarnation_id = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let listeners = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let features = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let rack = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let is_migrating_zk_broker = if (1) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let log_dirs = if (2) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let previous_broker_epoch = if (3) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
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
impl KafkaSerialize for BrokerRegistrationRequest {
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
        if (1) <= version.0 {
            self.is_migrating_zk_broker
                .encode(buf, version, is_flexible)?;
        }
        if (2) <= version.0 {
            self.log_dirs.encode(buf, version, is_flexible)?;
        }
        if (3) <= version.0 {
            self.previous_broker_epoch
                .encode(buf, version, is_flexible)?;
        }
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for BrokerRegistrationRequest {
    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let broker_id = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let cluster_id = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let incarnation_id = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let listeners = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let features = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let rack = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let is_migrating_zk_broker = if (1) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let log_dirs = if (2) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let previous_broker_epoch = if (3) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
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

impl KafkaSerialize for Feature {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.name.encode(buf, version, is_flexible)?;
        self.min_supported_version
            .encode(buf, version, is_flexible)?;
        self.max_supported_version
            .encode(buf, version, is_flexible)?;
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for Feature {
    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let name = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let min_supported_version = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let max_supported_version = KafkaDeserialize::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            name,
            min_supported_version,
            max_supported_version,
        })
    }
}

impl KafkaSerialize for Listener {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.name.encode(buf, version, is_flexible)?;
        self.host.encode(buf, version, is_flexible)?;
        self.port.encode(buf, version, is_flexible)?;
        self.security_protocol.encode(buf, version, is_flexible)?;
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for Listener {
    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let name = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let host = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let port = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let security_protocol = KafkaDeserialize::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            name,
            host,
            port,
            security_protocol,
        })
    }
}

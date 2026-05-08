#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// ControllerRegistrationRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ControllerRegistrationRequest {
    /// The ID of the controller to register.
    pub controller_id: i32,
    /// The controller incarnation ID, which is unique to each process run.
    pub incarnation_id: [u8; 16],
    /// Set if the required configurations for ZK migration are present.
    pub zk_migration_ready: bool,
    /// The listeners of this controller.
    pub listeners: Vec<Listener>,
    /// The features on this controller.
    pub features: Vec<Feature>,
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

impl ApiRequest for ControllerRegistrationRequest {
    type Response = crate::generated::ControllerRegistrationResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(70)
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
        self.controller_id
            .encode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode ControllerId"))?;
        self.incarnation_id
            .encode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode IncarnationId"))?;
        self.zk_migration_ready
            .encode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode ZkMigrationReady"))?;
        self.listeners
            .encode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode Listeners"))?;
        self.features
            .encode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode Features"))?;
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
        let controller_id = <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode ControllerId"))?;
        let incarnation_id = <[u8; 16] as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode IncarnationId"))?;
        let zk_migration_ready = <bool as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode ZkMigrationReady"))?;
        let listeners = <Vec<Listener> as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode Listeners"))?;
        let features = <Vec<Feature> as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode Features"))?;
        Ok(Self {
            controller_id,
            incarnation_id,
            zk_migration_ready,
            listeners,
            features,
        })
    }
}
impl KafkaSerialize for ControllerRegistrationRequest {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.controller_id
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ControllerId".into(),
            })?;
        self.incarnation_id
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode IncarnationId".into(),
            })?;
        self.zk_migration_ready
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ZkMigrationReady".into(),
            })?;
        self.listeners
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Listeners".into(),
            })?;
        self.features
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Features".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for ControllerRegistrationRequest {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        let controller_id =
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode ControllerId".into(),
                }
            })?;
        let incarnation_id = <[u8; 16] as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode IncarnationId".into(),
            })?;
        let zk_migration_ready = <bool as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode ZkMigrationReady".into(),
            })?;
        let listeners = <Vec<Listener> as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode Listeners".into(),
            })?;
        let features = <Vec<Feature> as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode Features".into(),
            })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            controller_id,
            incarnation_id,
            zk_migration_ready,
            listeners,
            features,
        })
    }
}

impl KafkaSerialize for Feature {
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
        self.min_supported_version
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode MinSupportedVersion".into(),
            })?;
        self.max_supported_version
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode MaxSupportedVersion".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for Feature {
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
        let min_supported_version = <i16 as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode MinSupportedVersion".into(),
            })?;
        let max_supported_version = <i16 as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode MaxSupportedVersion".into(),
            })?;
        if is_flexible {
            // Tagged fields (skip)
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
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.name
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Name".into(),
            })?;
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
        self.security_protocol
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode SecurityProtocol".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for Listener {
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
        let security_protocol = <i16 as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode SecurityProtocol".into(),
            })?;
        if is_flexible {
            // Tagged fields (skip)
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

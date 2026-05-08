#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
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
    fn get_min_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(0)
    }
    fn get_max_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(4)
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
        let is_flexible = true;
        self.broker_id
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode BrokerId"))?;
        self.cluster_id
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode ClusterId"))?;
        self.incarnation_id
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode IncarnationId"))?;
        self.listeners
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode Listeners"))?;
        self.features
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode Features"))?;
        self.rack
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode Rack"))?;
        if (1) <= version.0 {
            self.is_migrating_zk_broker
                .encode_flexible(buf, is_flexible)
                .map_err(|_| SerializationError::Encode("failed to encode IsMigratingZkBroker"))?;
        } else if self.is_migrating_zk_broker {
            return Err(SerializationError::Encode(
                "field 'IsMigratingZkBroker' is not available in this version",
            ));
        }
        if (2) <= version.0 {
            self.log_dirs
                .encode_flexible(buf, is_flexible)
                .map_err(|_| SerializationError::Encode("failed to encode LogDirs"))?;
        } else if !self.log_dirs.is_empty() {
            return Err(SerializationError::Encode(
                "field 'LogDirs' is not available in this version",
            ));
        }
        if (3) <= version.0 {
            self.previous_broker_epoch
                .encode_flexible(buf, is_flexible)
                .map_err(|_| SerializationError::Encode("failed to encode PreviousBrokerEpoch"))?;
        } else if self.previous_broker_epoch != 0 {
            return Err(SerializationError::Encode(
                "field 'PreviousBrokerEpoch' is not available in this version",
            ));
        }
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
        let is_flexible = true;
        let broker_id = <i32 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode BrokerId"))?;
        let cluster_id = <String as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode ClusterId"))?;
        let incarnation_id =
            <[u8; 16] as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode IncarnationId"))?;
        let listeners =
            <Vec<Listener> as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode Listeners"))?;
        let features =
            <Vec<Feature> as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode Features"))?;
        let rack = <Option<String> as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode Rack"))?;
        let is_migrating_zk_broker = if (1) <= version.0 {
            <bool as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode IsMigratingZkBroker"))?
        } else {
            Default::default()
        };
        let log_dirs = if (2) <= version.0 {
            <Vec<[u8; 16]> as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode LogDirs"))?
        } else {
            Default::default()
        };
        let previous_broker_epoch = if (3) <= version.0 {
            <i64 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode PreviousBrokerEpoch"))?
        } else {
            Default::default()
        };
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
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.broker_id
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode BrokerId".into(),
            })?;
        self.cluster_id
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ClusterId".into(),
            })?;
        self.incarnation_id
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode IncarnationId".into(),
            })?;
        self.listeners
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Listeners".into(),
            })?;
        self.features
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Features".into(),
            })?;
        self.rack
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Rack".into(),
            })?;
        self.is_migrating_zk_broker
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode IsMigratingZkBroker".into(),
            })?;
        self.log_dirs
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode LogDirs".into(),
            })?;
        self.previous_broker_epoch
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode PreviousBrokerEpoch".into(),
            })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(
        &self,
        buf: &mut B,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.broker_id
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode BrokerId".into(),
            })?;
        self.cluster_id
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ClusterId".into(),
            })?;
        self.incarnation_id
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode IncarnationId".into(),
            })?;
        self.listeners
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Listeners".into(),
            })?;
        self.features
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Features".into(),
            })?;
        if is_flexible {
            if let Some(ref __val) = self.rack {
                crate::protocol::serialization::encode_unsigned_varint(1u64, buf);
                __val
                    .encode_flexible(buf, true)
                    .map_err(|_| EncodeError::ValueTooLarge {
                        message: "failed to encode Rack".into(),
                    })?;
            } else {
                crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
            }
        } else {
            if let Some(ref __val) = self.rack {
                __val.encode(buf).map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode Rack".into(),
                })?;
            }
        }
        self.is_migrating_zk_broker
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode IsMigratingZkBroker".into(),
            })?;
        self.log_dirs
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode LogDirs".into(),
            })?;
        self.previous_broker_epoch
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode PreviousBrokerEpoch".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for BrokerRegistrationRequest {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] classic decode field `BrokerId` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let broker_id =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode BrokerId".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `ClusterId` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let cluster_id =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ClusterId".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `IncarnationId` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let incarnation_id =
            <[u8; 16] as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode IncarnationId".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `Listeners` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let listeners = <Vec<Listener> as KafkaDeserialize>::decode(buf).map_err(|_| {
            DecodeError::Protocol {
                message: "failed to decode Listeners".into(),
            }
        })?;
        tracing::trace!(
            "  [{}] classic decode field `Features` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let features =
            <Vec<Feature> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode Features".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `Rack` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let rack = <Option<String> as KafkaDeserialize>::decode(buf).map_err(|_| {
            DecodeError::Protocol {
                message: "failed to decode Rack".into(),
            }
        })?;
        tracing::trace!(
            "  [{}] classic decode field `IsMigratingZkBroker` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let is_migrating_zk_broker =
            <bool as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode IsMigratingZkBroker".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `LogDirs` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let log_dirs = <Vec<[u8; 16]> as KafkaDeserialize>::decode(buf).map_err(|_| {
            DecodeError::Protocol {
                message: "failed to decode LogDirs".into(),
            }
        })?;
        tracing::trace!(
            "  [{}] classic decode field `PreviousBrokerEpoch` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let previous_broker_epoch =
            <i64 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode PreviousBrokerEpoch".into(),
            })?;
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
    fn decode_flexible<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] decoding field `BrokerId` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let broker_id = <i32 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode BrokerId".into(),
            })?;
        tracing::trace!(
            "  [{}] decoding field `ClusterId` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let cluster_id = <String as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
            message: "failed to decode ClusterId".into(),
        })?;
        tracing::trace!(
            "  [{}] decoding field `IncarnationId` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let incarnation_id =
            <[u8; 16] as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode IncarnationId".into(),
                },
            )?;
        tracing::trace!(
            "  [{}] decoding field `Listeners` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let listeners =
            <Vec<Listener> as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
                .map_err(|_| DecodeError::Protocol {
                    message: "failed to decode Listeners".into(),
                })?;
        tracing::trace!(
            "  [{}] decoding field `Features` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let features =
            <Vec<Feature> as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
                .map_err(|_| DecodeError::Protocol {
                    message: "failed to decode Features".into(),
                })?;
        tracing::trace!(
            "  [{}] decoding field `Rack` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let rack = if is_flexible {
            <Option<String> as KafkaDeserialize>::decode_flexible(buf, version, true).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode Rack".into(),
                },
            )?
        } else {
            <Option<String> as KafkaDeserialize>::decode(buf).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode Rack".into(),
                }
            })?
        };
        tracing::trace!(
            "  [{}] decoding field `IsMigratingZkBroker` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let is_migrating_zk_broker = if (1) <= version.0 {
            <bool as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode IsMigratingZkBroker".into(),
                },
            )?
        } else {
            Default::default()
        };
        tracing::trace!(
            "  [{}] decoding field `LogDirs` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let log_dirs = if (2) <= version.0 {
            <Vec<[u8; 16]> as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
                .map_err(|_| DecodeError::Protocol {
                    message: "failed to decode LogDirs".into(),
                })?
        } else {
            Default::default()
        };
        tracing::trace!(
            "  [{}] decoding field `PreviousBrokerEpoch` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let previous_broker_epoch = if (3) <= version.0 {
            <i64 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode PreviousBrokerEpoch".into(),
                }
            })?
        } else {
            Default::default()
        };
        if is_flexible {
            // Tagged fields (skip)
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
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.name
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Name".into(),
            })?;
        self.min_supported_version
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode MinSupportedVersion".into(),
            })?;
        self.max_supported_version
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode MaxSupportedVersion".into(),
            })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(
        &self,
        buf: &mut B,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.name
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Name".into(),
            })?;
        self.min_supported_version
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode MinSupportedVersion".into(),
            })?;
        self.max_supported_version
            .encode_flexible(buf, is_flexible)
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
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] classic decode field `Name` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let name =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode Name".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `MinSupportedVersion` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let min_supported_version =
            <i16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode MinSupportedVersion".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `MaxSupportedVersion` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let max_supported_version =
            <i16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode MaxSupportedVersion".into(),
            })?;
        Ok(Self {
            name,
            min_supported_version,
            max_supported_version,
        })
    }
    fn decode_flexible<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] decoding field `Name` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let name = <String as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode Name".into(),
            })?;
        tracing::trace!(
            "  [{}] decoding field `MinSupportedVersion` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let min_supported_version =
            <i16 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode MinSupportedVersion".into(),
                },
            )?;
        tracing::trace!(
            "  [{}] decoding field `MaxSupportedVersion` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let max_supported_version =
            <i16 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode MaxSupportedVersion".into(),
                },
            )?;
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
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.name
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Name".into(),
            })?;
        self.host
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Host".into(),
            })?;
        self.port
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Port".into(),
            })?;
        self.security_protocol
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode SecurityProtocol".into(),
            })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(
        &self,
        buf: &mut B,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.name
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Name".into(),
            })?;
        self.host
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Host".into(),
            })?;
        self.port
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Port".into(),
            })?;
        self.security_protocol
            .encode_flexible(buf, is_flexible)
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
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] classic decode field `Name` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let name =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode Name".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `Host` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let host =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode Host".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `Port` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let port = <u16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
            message: "failed to decode Port".into(),
        })?;
        tracing::trace!(
            "  [{}] classic decode field `SecurityProtocol` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let security_protocol =
            <i16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode SecurityProtocol".into(),
            })?;
        Ok(Self {
            name,
            host,
            port,
            security_protocol,
        })
    }
    fn decode_flexible<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] decoding field `Name` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let name = <String as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode Name".into(),
            })?;
        tracing::trace!(
            "  [{}] decoding field `Host` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let host = <String as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode Host".into(),
            })?;
        tracing::trace!(
            "  [{}] decoding field `Port` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let port = <u16 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(
            |_| DecodeError::Protocol {
                message: "failed to decode Port".into(),
            },
        )?;
        tracing::trace!(
            "  [{}] decoding field `SecurityProtocol` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let security_protocol =
            <i16 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode SecurityProtocol".into(),
                },
            )?;
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

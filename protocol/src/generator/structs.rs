//! Module for representing Kafka protocol structures as Rust types.

/// Represents a Kafka protocol message structure
#[derive(Debug, Clone, PartialEq)]
pub struct MessageStruct {
    /// The API key for this message
    pub api_key: i16,
    
    /// The type of message (request or response)
    pub message_type: MessageType,
    
    /// The name of the message
    pub name: String,
    
    /// The valid versions of this message
    pub valid_versions: String,
    
    /// Fields in this message
    pub fields: Vec<Field>,
}

/// Represents the type of message
#[derive(Debug, Clone, PartialEq)]
pub enum MessageType {
    Request,
    Response,
}

/// Represents a field in a Kafka message
#[derive(Debug, Clone, PartialEq)]
pub struct Field {
    /// The name of the field
    pub name: String,
    
    /// The type of the field (e.g., "int32", "string", "[]MetadataRequestTopic")
    pub field_type: String,
    
    /// The versions this field is supported in
    pub versions: String,
    
    /// Whether this field is nullable
    pub nullable_versions: Option<String>,
    
    /// Whether this field is ignorable
    pub ignorable: bool,
    
    /// The default value for this field (if any)
    pub default: Option<String>,
    
    /// About text for documentation
    pub about: Option<String>,
    
    /// Nested fields for complex types (like arrays)
    pub fields: Vec<Field>,
}

impl Field {
    /// Create a new field
    pub fn new(name: String, field_type: String, versions: String) -> Self {
        Self {
            name,
            field_type,
            versions,
            nullable_versions: None,
            ignorable: false,
            default: None,
            about: None,
            fields: Vec::new(),
        }
    }
}

impl MessageStruct {
    /// Create a new message structure
    pub fn new(api_key: i16, message_type: MessageType, name: String, valid_versions: String) -> Self {
        Self {
            api_key,
            message_type,
            name,
            valid_versions,
            fields: Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_struct_creation() {
        let field = Field::new("TestField".to_string(), "int32".to_string(), "0+".to_string());
        assert_eq!(field.name, "TestField");
        assert_eq!(field.field_type, "int32");
    }

    #[test]
    fn test_message_struct_creation() {
        let message = MessageStruct::new(3, MessageType::Request, "MetadataRequest".to_string(), "0-7".to_string());
        assert_eq!(message.api_key, 3);
        assert_eq!(message.name, "MetadataRequest");
    }
}
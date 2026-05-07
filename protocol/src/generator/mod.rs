//! Protocol generator module for creating Rust structs from Kafka protocol JSON definitions.

pub mod codegen;
pub mod json_parser;
pub mod structs;

use std::fs;

/// Generate Rust structs from Kafka protocol JSON definitions
pub fn generate_structs() -> Result<Vec<structs::MessageStruct>, Box<dyn std::error::Error>> {
    let messages_dir = "messages/";
    let mut parsed_structs = Vec::new();

    // Read directory and process files
    if let Ok(entries) = std::fs::read_dir(messages_dir) {
        for entry in entries.flatten() {
            if let Some(extension) = entry.path().extension()
                && extension == "json"
            {
                // Read file content as string
                if let Ok(content) = fs::read_to_string(entry.path()) {
                    // Step 1: Remove comment lines (lines starting with //)
                    let cleaned_content = content
                        .lines()
                        .filter(|line| !line.trim_start().starts_with("//"))
                        .collect::<Vec<_>>()
                        .join("\n");

                    // Step 2: Parse cleaned JSON into MessageStruct
                    if let Ok(message) =
                        serde_json::from_str::<structs::MessageStruct>(&cleaned_content)
                    {
                        parsed_structs.push(message);
                    }
                }
            }
        }
    }

    Ok(parsed_structs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_protocol_generation() {
        let generated = generate_structs().unwrap();
        // Should have parsed at least some structs
        assert!(!generated.is_empty());
    }

    #[test]
    fn test_remove_comments() {
        let content = r#"// This is a comment
{
    // Another comment
    "apiKey": 3,
    "type": "request",
    // Final comment
    "name": "TestRequest",
    "validVersions": "0-7"
}"#;

        let cleaned = content
            .lines()
            .filter(|line| !line.trim_start().starts_with("//"))
            .collect::<Vec<_>>()
            .join("\n");

        assert!(!cleaned.contains("//"));
        assert!(cleaned.contains("\"apiKey\": 3"));
    }

    #[test]
    fn test_parse_simple_json() {
        let json = r#"{
    "apiKey": 3,
    "type": "request",
    "name": "MetadataRequest",
    "validVersions": "0-7",
    "fields": []
}"#;

        let result: Result<structs::MessageStruct, _> = serde_json::from_str(json);
        assert!(result.is_ok());
        let msg = result.unwrap();
        assert_eq!(msg.api_key, Some(3));
        assert_eq!(msg.name, "MetadataRequest");
        assert_eq!(msg.valid_versions, "0-7");
    }

    #[test]
    fn test_parse_actual_file() {
        let content = std::fs::read_to_string("messages/MetadataRequest.json").unwrap();
        let cleaned = content
            .lines()
            .filter(|line| !line.trim_start().starts_with("//"))
            .collect::<Vec<_>>()
            .join("\n");

        let result: Result<structs::MessageStruct, _> = serde_json::from_str(&cleaned);
        assert!(
            result.is_ok(),
            "Failed to parse MetadataRequest.json: {:?}",
            result.err()
        );
        let msg = result.unwrap();
        assert_eq!(msg.api_key, Some(3));
        assert_eq!(msg.message_type, structs::MessageType::Request);
        assert_eq!(msg.name, "MetadataRequest");
    }
}
